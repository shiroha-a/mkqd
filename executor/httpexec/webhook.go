package httpexec

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/shiroha-a/mkqd"
	"github.com/shiroha-a/mkqd/internal/safedial"
)

// WebhookOptions is the configuration block of the "webhook" executor.
type WebhookOptions struct {
	// Timeout bounds one delivery. Zero means 30s.
	Timeout mkqd.Duration `yaml:"timeout"`
	// Secret signs deliveries whose payload carries no secret of its
	// own. Empty means unsigned.
	Secret string `yaml:"secret"`
	// UserAgent overrides the default "mkqd/<version>".
	UserAgent string `yaml:"user_agent"`
	// Headers are added to every delivery; the payload may add more.
	Headers map[string]string `yaml:"headers"`
	// MaxResponseBytes caps how much of a response is read. Zero means
	// 64 KiB.
	MaxResponseBytes int64 `yaml:"max_response_bytes"`
	// AllowPrivateNetwork defaults to false here, unlike the "http"
	// executor: the destination arrives in the job payload, so anyone
	// who can enqueue could otherwise aim the worker at the private
	// network it runs in. Turn it on only for tests.
	AllowPrivateNetwork *bool `yaml:"allow_private_network"`
}

func (o WebhookOptions) timeout() time.Duration {
	if o.Timeout <= 0 {
		return defaultTimeout
	}
	return o.Timeout.Duration()
}

func (o WebhookOptions) maxResponseBytes() int64 {
	if o.MaxResponseBytes <= 0 {
		return defaultMaxResponseBytes
	}
	return o.MaxResponseBytes
}

func (o WebhookOptions) allowPrivate() bool {
	return o.AllowPrivateNetwork != nil && *o.AllowPrivateNetwork
}

// WebhookPayload is the job payload the "webhook" executor expects.
type WebhookPayload struct {
	// URL is the delivery destination. Required, http or https.
	URL string `json:"url"`
	// Secret overrides the configured signing secret for this one
	// delivery, which is how per-subscriber secrets are carried.
	Secret string `json:"secret,omitempty"`
	// Headers are added to this delivery. Reserved headers are
	// ignored.
	Headers map[string]string `json:"headers,omitempty"`
	// Body is sent verbatim as the request body. Absent means "null".
	Body json.RawMessage `json:"body,omitempty"`
}

func newWebhookExecutor(_ context.Context, bc mkqd.BuildContext, cfg mkqd.ExecutorConfig) (mkqd.Executor, error) {
	var opts WebhookOptions
	if err := cfg.DecodeStrict(&opts); err != nil {
		return nil, err
	}
	if err := checkHeaders(opts.Headers); err != nil {
		return nil, fmt.Errorf("executor webhook: %w", err)
	}
	return &webhookExecutor{
		opts: opts,
		snd:  newSender(opts.allowPrivate(), opts.maxResponseBytes(), userAgent(opts.UserAgent), bc.Logger),
	}, nil
}

type webhookExecutor struct {
	opts WebhookOptions
	snd  *sender
}

// Execute delivers the payload's body to the payload's URL.
//
// 壊れた payload は何度送っても直らないので、この executor では payload の
// 不備をすべて恒久的失敗にする。再試行で回復しうるのは相手側の状態だけ。
func (e *webhookExecutor) Execute(ctx context.Context, job *mkqd.Job) (any, error) {
	var p WebhookPayload
	if err := json.Unmarshal(job.Data, &p); err != nil {
		return nil, permanent("job %s: payload is not a webhook payload: %v", job.ID, err)
	}
	if err := checkTarget(p.URL); err != nil {
		return nil, permanent("job %s: %v", job.ID, err)
	}
	// net/http は送信時に不正なヘッダを拒否するが、その失敗は transport
	// エラーとして出てくるので再試行扱いになってしまう。送れないヘッダが
	// 送れるようになることはないので、ここで恒久的失敗に倒す。
	if err := checkHeaders(p.Headers); err != nil {
		return nil, permanent("job %s: %v", job.ID, err)
	}

	secret := p.Secret
	if secret == "" {
		secret = e.opts.Secret
	}

	body := []byte("null")
	if len(p.Body) > 0 {
		body = p.Body
	}

	ctx, cancel := context.WithTimeout(ctx, e.opts.timeout())
	defer cancel()
	return e.snd.send(ctx, p.URL, body, secret, mergeHeaders(e.opts.Headers, p.Headers), job)
}

// mergeHeaders overlays per-job headers on the configured ones.
func mergeHeaders(base, over map[string]string) map[string]string {
	if len(base) == 0 {
		return over
	}
	if len(over) == 0 {
		return base
	}
	out := make(map[string]string, len(base)+len(over))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range over {
		out[k] = v
	}
	return out
}

// checkTarget rejects a destination that could never work, before a
// request is built.
func checkTarget(raw string) error {
	if raw == "" {
		return errors.New("url is required")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("url %q is not parseable: %v", raw, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("url %q must use http or https", raw)
	}
	if u.Host == "" {
		return fmt.Errorf("url %q has no host", raw)
	}
	return nil
}

// isBlocked reports whether a transport error came from the SSRF
// guard, which no amount of retrying will change.
func isBlocked(err error) bool { return errors.Is(err, safedial.ErrBlocked) }
