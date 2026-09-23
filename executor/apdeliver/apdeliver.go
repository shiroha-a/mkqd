// Package apdeliver delivers ActivityPub activities to remote inboxes.
//
// It registers the "activitypub_deliver" executor, which takes the
// whole job off an application's hands: it signs the request with an
// HTTP Signature, computes the body digest, applies the fediverse's
// retry conventions, and refuses destinations that are not on the
// public internet.
//
// Import it for its side effects to make the type available to a
// configuration file:
//
//	import _ "github.com/shiroha-a/mkqd/executor/apdeliver"
//
// # Keys
//
// An application that embeds mkqd does not have to hand over its
// private keys. Build the executor with a Signer of its own and the key
// never leaves the process:
//
//	ex, _ := apdeliver.New(apdeliver.Options{Signer: appSigner})
//	rt.HandleExecutor("deliver", ex)
//
// Standalone deployments configure a signer instead; see SignerConfig.
package apdeliver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/shiroha-a/mkq"
	"github.com/shiroha-a/mkqd"
	"github.com/shiroha-a/mkqd/httpsig"
	"github.com/shiroha-a/mkqd/internal/httpsend"
	"github.com/shiroha-a/mkqd/internal/safedial"
)

const (
	defaultTimeout = 30 * time.Second
	// defaultContentType is what ActivityPub servers expect on an
	// inbox POST.
	defaultContentType = `application/activity+json`
	// failedReasonLimit caps how much of a response body ends up in
	// BullMQ's failedReason, which dashboards render inline.
	failedReasonLimit = 1 << 10
)

// Payload is the job payload the executor expects.
type Payload struct {
	// Inbox is the destination. Required, http or https.
	Inbox string `json:"inbox"`
	// KeyID selects the signing key. Empty asks the signer for its
	// default, which is what a single-actor deployment wants.
	KeyID string `json:"keyId,omitempty"`
	// Activity is the activity to deliver. Exclusive with Body.
	Activity json.RawMessage `json:"activity,omitempty"`
	// Body fixes the exact bytes to send, for a caller that wants
	// control over the serialisation. Exclusive with Activity.
	Body string `json:"body,omitempty"`
	// ContentType overrides application/activity+json.
	ContentType string `json:"contentType,omitempty"`
	// Headers are added to the request. Reserved and signed headers
	// cannot be overridden.
	Headers map[string]string `json:"headers,omitempty"`
}

// Options configures an executor built in Go.
type Options struct {
	// Signer signs deliveries. Required.
	Signer httpsig.Signer
	// Timeout bounds one delivery. Zero means 30s.
	Timeout time.Duration
	// UserAgent overrides the default "mkqd/<version>".
	UserAgent string
	// MaxResponseBytes caps how much of a response is read. Zero means
	// 64 KiB.
	MaxResponseBytes int64
	// AllowPrivateNetwork disables the SSRF guard. The inbox comes from
	// the job payload, so this defaults to off and should stay off
	// outside tests.
	AllowPrivateNetwork bool
	// Logger receives operational records. Nil is fine.
	Logger *slog.Logger
}

// Executor delivers activities. Build one with New.
type Executor struct {
	signer      httpsig.Signer
	client      *http.Client
	timeout     time.Duration
	userAgent   string
	maxBytes    int64
	contentType string
	log         *slog.Logger
}

// New builds a delivery executor. It is the entry point for an
// application that holds its own keys.
func New(opts Options) (*Executor, error) {
	if opts.Signer == nil {
		return nil, errors.New("apdeliver: a signer is required")
	}
	if opts.Timeout <= 0 {
		opts.Timeout = defaultTimeout
	}
	if opts.MaxResponseBytes <= 0 {
		opts.MaxResponseBytes = httpsend.DefaultMaxResponseBytes
	}
	ua := opts.UserAgent
	if ua == "" {
		ua = mkqd.UserAgent()
	}
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Executor{
		signer:      opts.Signer,
		client:      httpsend.NewClient(opts.AllowPrivateNetwork),
		timeout:     opts.Timeout,
		userAgent:   ua,
		maxBytes:    opts.MaxResponseBytes,
		contentType: defaultContentType,
		log:         log,
	}, nil
}

// Execute implements mkqd.Executor.
//
// 配送できない payload は何度送っても配送できないので、payload の不備は
// すべて恒久的失敗にする。再試行で回復しうるのは相手側の状態だけ。
func (e *Executor) Execute(ctx context.Context, job *mkqd.Job) (any, error) {
	var p Payload
	if err := json.Unmarshal(job.Data, &p); err != nil {
		return nil, permanent("job %s: payload is not a delivery payload: %v", job.ID, err)
	}
	body, contentType, err := p.resolveBody(e.contentType)
	if err != nil {
		return nil, permanent("job %s: %v", job.ID, err)
	}
	// contentType も payload 由来なので、追加ヘッダと同じ検証を通す。
	// 送れない値は何度送っても送れないし、署名対象にも入る。
	if !validHeaderValue(contentType) {
		return nil, permanent("job %s: invalid contentType", job.ID)
	}
	if err := checkInbox(p.Inbox); err != nil {
		return nil, permanent("job %s: %v", job.ID, err)
	}

	ctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.Inbox, bytes.NewReader(body))
	if err != nil {
		return nil, permanent("job %s: build request: %v", job.ID, err)
	}
	for k, v := range p.Headers {
		if isSignedOrReserved(k) {
			continue
		}
		if !validHeaderName(k) || !validHeaderValue(v) {
			return nil, permanent("job %s: invalid header %q", job.ID, k)
		}
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("User-Agent", e.userAgent)
	req.Header.Set("Accept", `application/activity+json, application/ld+json`)

	// 署名は Content-Type と Digest を covered header に含むので、
	// それらを確定させた後で行う。
	if err := httpsig.SignRequest(ctx, req, body, p.KeyID, e.signer); err != nil {
		if errors.Is(err, httpsig.ErrUnknownKey) {
			return nil, permanent("job %s: %v", job.ID, err)
		}
		return nil, fmt.Errorf("apdeliver: job %s: sign: %w", job.ID, err)
	}

	resp, err := e.client.Do(req)
	if err != nil {
		if errors.Is(err, safedial.ErrBlocked) {
			return nil, permanent("job %s: %s is not reachable: %v", job.ID, p.Inbox, err)
		}
		return nil, fmt.Errorf("apdeliver: POST %s: %w", p.Inbox, err)
	}
	payload, readErr := httpsend.ReadBody(resp, e.maxBytes)
	if readErr != nil {
		return nil, fmt.Errorf("apdeliver: POST %s: read response: %w", p.Inbox, readErr)
	}

	switch classify(resp.StatusCode) {
	case outcomeSuccess:
		return nil, nil
	case outcomePermanent:
		return nil, permanent("POST %s: %s: %s", p.Inbox, resp.Status, detail(payload))
	default:
		err := fmt.Errorf("apdeliver: POST %s: %s: %s", p.Inbox, resp.Status, detail(payload))
		// **相手が「いつ来い」と言っているなら従う。** 連合先が 429 や 503 で
		// Retry-After を返すのは珍しくなく、指数バックオフの都合で早く叩き直す
		// のは相手にも自分にも損。runtime が取り出して遅延に使う。
		if ra := resp.Header.Get("Retry-After"); ra != "" {
			if d, ok := mkqd.ParseRetryAfter(ra, time.Now()); ok {
				return nil, mkqd.RetryAfter(d, err)
			}
			// 読めなかったことは runtime からは見えない (そこには何も
			// 届かないため)。連合先が独自形式を返しているのを見つける口。
			e.log.Debug("ignoring an unusable Retry-After",
				"job_id", job.ID, "inbox", p.Inbox, "retry_after", ra)
		}
		return nil, err
	}
}

// resolveBody picks the bytes to send. Activity and Body are exclusive:
// accepting both would leave the digest committing to one of them with
// no way for the caller to know which.
func (p Payload) resolveBody(defaultContentType string) ([]byte, string, error) {
	contentType := p.ContentType
	if contentType == "" {
		contentType = defaultContentType
	}
	activity := bytes.TrimSpace(p.Activity)
	hasActivity := len(activity) > 0
	switch {
	case hasActivity && p.Body != "":
		return nil, "", errors.New(`"activity" and "body" are exclusive`)
	case hasActivity && activity[0] != '{':
		// `null` や数値も JSON としては妥当なので、それだけでは弾けない。
		// 署名付きでゴミを remote inbox に送るより、ここで落とす。
		return nil, "", errors.New(`"activity" must be a JSON object`)
	case hasActivity:
		// mkqd 自身が送るバイト列に対して digest を計算するので、
		// ここで marshal し直しても受信側との不一致は起きない。
		return activity, contentType, nil
	case p.Body != "":
		return []byte(p.Body), contentType, nil
	default:
		return nil, "", errors.New(`one of "activity" or "body" is required`)
	}
}

// checkInbox rejects a destination that could never work.
func checkInbox(raw string) error {
	if raw == "" {
		return errors.New("inbox is required")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("inbox %q is not parseable: %v", raw, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("inbox %q must use http or https", raw)
	}
	if u.Host == "" {
		return fmt.Errorf("inbox %q has no host", raw)
	}
	return nil
}

type outcome int

const (
	outcomeSuccess outcome = iota
	outcomeRetry
	outcomePermanent
)

// classify decides what a delivery attempt means.
//
// 4xx を恒久的失敗にするのは Misskey と同じで、受け付けないものを送り続け
// ないという運用上の合意に従っている。3xx は POST のリダイレクト追従が署名を
// 壊すので追えず、追わない限り成功しない。
//
// **401 だけは Mastodon 側に合わせて再試行する。** inbox の 401 は多くの場合
// 署名検証の失敗で、その原因は時刻ずれや「相手がこちらの keyId を取りに来られ
// なかった」といった一過性のものが大半を占める。ここを恒久的失敗にすると、
// 相手側の一時的な不調でノートが永久に連合しなくなる。
func classify(status int) outcome {
	switch {
	case status >= 200 && status < 300:
		return outcomeSuccess
	case status == http.StatusUnauthorized,
		status == http.StatusRequestTimeout,
		status == http.StatusTooManyRequests:
		return outcomeRetry
	case status >= 300 && status < 500:
		return outcomePermanent
	default:
		return outcomeRetry
	}
}

// detail renders a bounded slice of a failed response for failedReason.
func detail(payload []byte) string {
	trimmed := strings.TrimSpace(string(payload))
	if trimmed == "" {
		return "(empty response body)"
	}
	return httpsend.Truncate(trimmed, failedReasonLimit)
}

// isSignedOrReserved reports whether a payload-supplied header would
// collide with one the executor controls. Letting a payload set Digest
// or Signature would let it decide what the signature covers.
func isSignedOrReserved(name string) bool {
	switch strings.ToLower(name) {
	case "host", "date", "digest", "signature", "content-type", "content-length", "user-agent", "accept":
		return true
	}
	return false
}

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

func validHeaderValue(s string) bool {
	for i := 0; i < len(s); i++ {
		if b := s[i]; (b < 0x20 && b != '\t') || b == 0x7f {
			return false
		}
	}
	return true
}

func isTokenByte(b byte) bool {
	switch {
	case b >= 'a' && b <= 'z', b >= 'A' && b <= 'Z', b >= '0' && b <= '9':
		return true
	}
	return strings.IndexByte("!#$%&'*+-.^_`|~", b) >= 0
}

func permanent(format string, args ...any) error {
	return fmt.Errorf("apdeliver: %w: %s", mkq.ErrUnrecoverable, fmt.Sprintf(format, args...))
}
