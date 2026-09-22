// Package httpexec forwards jobs over HTTP.
//
// It registers two executor types:
//
//   - "http" delivers every job of a queue to one endpoint configured by
//     the operator. This is the sidecar shape: mkqd owns the queue and
//     the application only has to answer an HTTP request, in whatever
//     language it is written.
//   - "webhook" delivers to a URL carried in the job payload, which is
//     what an outbound webhook queue needs.
//
// Import it for its side effects to make the types available to a
// configuration file:
//
//	import _ "github.com/shiroha-a/mkqd/executor/httpexec"
package httpexec

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/shiroha-a/mkq"
	"github.com/shiroha-a/mkqd"
	"github.com/shiroha-a/mkqd/internal/httpsend"
)

// Request headers mkqd sets on every dispatch.
const (
	HeaderQueue     = "X-Mkqd-Queue"
	HeaderJobID     = "X-Mkqd-Job-Id"
	HeaderJobName   = "X-Mkqd-Job-Name"
	HeaderAttempt   = "X-Mkqd-Attempt"
	HeaderTimestamp = "X-Mkqd-Timestamp"
	HeaderSignature = "X-Mkqd-Signature"
	// HeaderRetry is a response header: "no" fails the job for good
	// whatever the status code says.
	HeaderRetry = "X-Mkqd-Retry"
)

const (
	defaultTimeout          = 30 * time.Second
	defaultMaxResponseBytes = httpsend.DefaultMaxResponseBytes
	// failedReasonLimit caps how much of a response body ends up in
	// BullMQ's failedReason field, which dashboards render inline.
	failedReasonLimit = 1 << 10
	// headerValueLimit bounds the job metadata mkqd copies into request
	// headers, so an oversized job name cannot push a request past a
	// server's header limit.
	headerValueLimit = 256
)

// Options is the configuration block of the "http" executor.
type Options struct {
	// URL receives every job of the queue. Required.
	URL string `yaml:"url"`
	// Timeout bounds one dispatch. Zero means 30s.
	Timeout mkqd.Duration `yaml:"timeout"`
	// Secret keys the HMAC signature. Empty sends no signature, which
	// suits a loopback endpoint.
	Secret string `yaml:"secret"`
	// UserAgent overrides the default "mkqd/<version>".
	UserAgent string `yaml:"user_agent"`
	// Headers are added to every request. They cannot override the
	// X-Mkqd-* headers or Content-Type.
	Headers map[string]string `yaml:"headers"`
	// MaxResponseBytes caps how much of a response is read. Zero means
	// 64 KiB.
	MaxResponseBytes int64 `yaml:"max_response_bytes"`
	// AllowPrivateNetwork defaults to true here: the URL comes from the
	// operator, and pointing it at 127.0.0.1 is the normal case.
	AllowPrivateNetwork *bool `yaml:"allow_private_network"`
}

func (o Options) timeout() time.Duration {
	if o.Timeout <= 0 {
		return defaultTimeout
	}
	return o.Timeout.Duration()
}

func (o Options) maxResponseBytes() int64 {
	if o.MaxResponseBytes <= 0 {
		return defaultMaxResponseBytes
	}
	return o.MaxResponseBytes
}

func (o Options) allowPrivate() bool {
	if o.AllowPrivateNetwork == nil {
		return true
	}
	return *o.AllowPrivateNetwork
}

func init() {
	mkqd.RegisterExecutor("http", newHTTPExecutor)
	mkqd.RegisterExecutor("webhook", newWebhookExecutor)
}

func newHTTPExecutor(_ context.Context, bc mkqd.BuildContext, cfg mkqd.ExecutorConfig) (mkqd.Executor, error) {
	var opts Options
	if err := cfg.DecodeStrict(&opts); err != nil {
		return nil, err
	}
	if opts.URL == "" {
		return nil, fmt.Errorf("executor http: url is required")
	}
	if err := checkTarget(opts.URL); err != nil {
		return nil, fmt.Errorf("executor http: %w", err)
	}
	if err := checkHeaders(opts.Headers); err != nil {
		return nil, fmt.Errorf("executor http: %w", err)
	}
	return &httpExecutor{
		opts: opts,
		snd:  newSender(opts.allowPrivate(), opts.maxResponseBytes(), userAgent(opts.UserAgent), bc.Logger),
	}, nil
}

type httpExecutor struct {
	opts Options
	snd  *sender
}

// Execute wraps the job in the dispatch envelope and posts it.
func (e *httpExecutor) Execute(ctx context.Context, job *mkqd.Job) (any, error) {
	body, err := json.Marshal(envelope{
		Queue:        job.Queue,
		ID:           job.ID,
		Name:         job.Name,
		Data:         job.Data,
		AttemptsMade: job.AttemptsMade,
		Timestamp:    job.Timestamp,
	})
	if err != nil {
		return nil, permanent("encode dispatch envelope: %v", err)
	}
	ctx, cancel := context.WithTimeout(ctx, e.opts.timeout())
	defer cancel()
	return e.snd.send(ctx, e.opts.URL, body, e.opts.Secret, e.opts.Headers, job)
}

// envelope is the JSON body of a dispatch. It is the contract an
// application implements, so field names are stable.
type envelope struct {
	Queue        string          `json:"queue"`
	ID           string          `json:"id"`
	Name         string          `json:"name"`
	Data         json.RawMessage `json:"data"`
	AttemptsMade int             `json:"attemptsMade"`
	Timestamp    time.Time       `json:"timestamp"`
}

// sender holds the pieces both executors share.
type sender struct {
	client    *http.Client
	userAgent string
	maxBytes  int64
	log       *slog.Logger
}

func newSender(allowPrivate bool, maxBytes int64, ua string, log *slog.Logger) *sender {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &sender{
		client:    httpsend.NewClient(allowPrivate),
		userAgent: ua,
		maxBytes:  maxBytes,
		log:       log,
	}
}

// send posts body and turns the response into a handler outcome.
func (s *sender) send(ctx context.Context, target string, body []byte, secret string, extra map[string]string, job *mkqd.Job) (any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return nil, permanent("build request for %s: %v", target, err)
	}

	for k, v := range extra {
		if isReservedHeader(k) {
			continue
		}
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", s.userAgent)
	// job の名前と id は Redis 由来で、BullMQ の producer なら言語を問わず
	// 何でも入れられる。ヘッダに載せる前に無害化する — これは通知であって
	// 契約ではないので、変な名前のジョブを落とすより送り届けるほうがよい。
	req.Header.Set(HeaderQueue, sanitizeHeaderValue(job.Queue))
	req.Header.Set(HeaderJobID, sanitizeHeaderValue(job.ID))
	req.Header.Set(HeaderJobName, sanitizeHeaderValue(job.Name))
	req.Header.Set(HeaderAttempt, strconv.Itoa(job.AttemptsMade+1))

	ts := time.Now().Unix()
	req.Header.Set(HeaderTimestamp, strconv.FormatInt(ts, 10))
	if secret != "" {
		req.Header.Set(HeaderSignature, Sign(secret, ts, body))
	}

	resp, err := s.client.Do(req)
	if err != nil {
		// 接続先が公開アドレスでないなら、何度試しても同じ結果になる。
		if isBlocked(err) {
			return nil, permanent("%s is not reachable: %v", target, err)
		}
		return nil, fmt.Errorf("mkqd/http: POST %s: %w", target, err)
	}
	payload, readErr := httpsend.ReadBody(resp, s.maxBytes)
	if readErr != nil {
		return nil, fmt.Errorf("mkqd/http: POST %s: read response: %w", target, readErr)
	}

	if resp.StatusCode == http.StatusTooManyRequests {
		if ra := resp.Header.Get("Retry-After"); ra != "" {
			// mkq の WithBackoffStrategy はジョブ文脈を受け取らないので、
			// この値を次回遅延に反映する口がない。運用者が気づけるように
			// ログだけ残す。upstream 課題として記録済み。
			s.log.Warn("Retry-After is not applied to the retry delay",
				"job_id", job.ID, "retry_after", ra, "url", target)
		}
	}

	switch outcome := classify(resp.StatusCode, resp.Header); outcome {
	case outcomeSuccess:
		return returnValue(resp.Header, payload), nil
	case outcomePermanent:
		return nil, permanent("POST %s: %s: %s", target, resp.Status, detail(resp.Header, payload))
	default:
		return nil, fmt.Errorf("mkqd/http: POST %s: %s: %s", target, resp.Status, detail(resp.Header, payload))
	}
}

type outcome int

const (
	outcomeSuccess outcome = iota
	outcomeRetry
	outcomePermanent
)

// classify maps a response onto mkq's three outcomes.
//
// 2xx 以外で恒久的失敗に倒すのは、設定やリクエストを直さない限り成功しえない
// ものだけに絞ってある。未知のステータスは再試行側に倒す — 内部ディスパッチでは
// 仕事を落とさないほうが安全で、恒久的失敗にしてもジョブは failed set に残り
// `mkqd retry` で戻せる。
func classify(status int, header http.Header) outcome {
	// 2xx を先に見る。成功した仕事には再試行するものがないので、
	// 既定で X-Mkqd-Retry: no を付けているエンドポイントがあっても
	// 成功が失敗に化けてはいけない。
	if status >= 200 && status < 300 {
		return outcomeSuccess
	}
	if strings.EqualFold(header.Get(HeaderRetry), "no") {
		return outcomePermanent
	}
	switch {
	case status >= 300 && status < 400:
		// リダイレクトは追従しないので、追わない限り成功しない。
		return outcomePermanent
	case status == http.StatusBadRequest,
		status == http.StatusConflict,
		status == http.StatusGone,
		status == http.StatusUnprocessableEntity:
		return outcomePermanent
	default:
		return outcomeRetry
	}
}

// returnValue extracts the value stored in BullMQ's returnvalue field.
// Only a JSON response becomes one; anything else is dropped rather
// than stored as an opaque string.
func returnValue(header http.Header, payload []byte) any {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 {
		return nil
	}
	if ct := header.Get("Content-Type"); !strings.Contains(ct, "json") {
		return nil
	}
	if !json.Valid(trimmed) {
		return nil
	}
	return json.RawMessage(trimmed)
}

// detail renders the part of a failed response worth putting in
// failedReason: the application's own message when it sent one, and
// otherwise a bounded slice of the body.
func detail(header http.Header, payload []byte) string {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 {
		return "(empty response body)"
	}
	if strings.Contains(header.Get("Content-Type"), "json") {
		var obj struct {
			Error string `json:"error"`
		}
		if err := json.Unmarshal(trimmed, &obj); err == nil && obj.Error != "" {
			return httpsend.Truncate(obj.Error, failedReasonLimit)
		}
	}
	return httpsend.Truncate(string(trimmed), failedReasonLimit)
}

// sanitizeHeaderValue makes an arbitrary string safe to put in a header
// value, replacing anything a field value may not contain and bounding
// the length.
func sanitizeHeaderValue(v string) string {
	if len(v) > headerValueLimit {
		v = httpsend.Truncate(v, headerValueLimit)
	}
	var b strings.Builder
	for i := 0; i < len(v); i++ {
		if validHeaderValueByte(v[i]) {
			b.WriteByte(v[i])
			continue
		}
		b.WriteByte('_')
	}
	return b.String()
}

// validHeaderName reports whether s is an RFC 9110 field name (a token).
func validHeaderName(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !isTokenByte(s[i]) {
			return false
		}
	}
	return true
}

// validHeaderValue reports whether s may appear as a field value.
func validHeaderValue(s string) bool {
	for i := 0; i < len(s); i++ {
		if !validHeaderValueByte(s[i]) {
			return false
		}
	}
	return true
}

// validHeaderValueByte mirrors what net/http accepts: no controls other
// than tab, and no DEL.
func validHeaderValueByte(b byte) bool {
	return (b >= 0x20 || b == '\t') && b != 0x7f
}

func isTokenByte(b byte) bool {
	switch {
	case b >= 'a' && b <= 'z', b >= 'A' && b <= 'Z', b >= '0' && b <= '9':
		return true
	}
	return strings.IndexByte("!#$%&'*+-.^_`|~", b) >= 0
}

// checkHeaders rejects a header set that net/http would refuse at send
// time. Catching it here keeps the failure classifiable: an unsendable
// header can never become sendable, so it must not burn retries.
func checkHeaders(h map[string]string) error {
	for k, v := range h {
		if !validHeaderName(k) {
			return fmt.Errorf("invalid header name %q", k)
		}
		if !validHeaderValue(v) {
			return fmt.Errorf("invalid value for header %q", k)
		}
	}
	return nil
}

// Sign returns the value of the X-Mkqd-Signature header: an HMAC-SHA256
// over "v1:<unix timestamp>:<body>".
//
// The timestamp is inside the signed material so a captured request
// cannot be replayed indefinitely; receivers should reject a timestamp
// outside a few minutes of their own clock and compare in constant
// time.
func Sign(secret string, unixSeconds int64, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte("v1:"))
	mac.Write([]byte(strconv.FormatInt(unixSeconds, 10)))
	mac.Write([]byte(":"))
	mac.Write(body)
	return "v1=" + hex.EncodeToString(mac.Sum(nil))
}

// isReservedHeader reports whether a configured header would clobber
// one mkqd controls.
func isReservedHeader(name string) bool {
	if strings.EqualFold(name, "Content-Type") || strings.EqualFold(name, "User-Agent") {
		return true
	}
	return strings.HasPrefix(strings.ToLower(name), "x-mkqd-")
}

func userAgent(override string) string {
	if override != "" {
		return override
	}
	return mkqd.UserAgent()
}

// permanent marks an error as one mkq must not retry.
func permanent(format string, args ...any) error {
	return fmt.Errorf("mkqd/http: %w: %s", mkq.ErrUnrecoverable, fmt.Sprintf(format, args...))
}
