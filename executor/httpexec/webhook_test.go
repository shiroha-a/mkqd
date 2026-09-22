package httpexec

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/shiroha-a/mkq"
	"github.com/shiroha-a/mkqd"
	"github.com/shiroha-a/mkqd/internal/safedial"
)

// webhookPayload renders a payload for the job data.
func webhookPayload(t *testing.T, p WebhookPayload) string {
	t.Helper()
	raw, err := json.Marshal(p)
	require.NoError(t, err)
	return string(raw)
}

// httptest binds to loopback, which the webhook executor blocks by
// default. Tests that exercise delivery therefore opt out explicitly —
// and one test below pins that the default really does block.
const allowLoopback = "type: webhook\nallow_private_network: true"

func TestWebhook_DeliversToPayloadURL(t *testing.T) {
	srv, rec := serve(t, http.StatusOK, nil, "")
	ex := buildWebhook(t, allowLoopback)

	_, err := ex.Execute(context.Background(), testJob(webhookPayload(t, WebhookPayload{
		URL:  srv.URL,
		Body: json.RawMessage(`{"event":"note.created"}`),
	})))
	require.NoError(t, err)

	method, header, body, hits := rec.snapshot()
	require.Equal(t, 1, hits)
	require.Equal(t, http.MethodPost, method)
	require.Equal(t, "application/json", header.Get("Content-Type"))
	require.Equal(t, "q", header.Get(HeaderQueue))
	// webhook は封筒に包まず、payload の body をそのまま送る。
	require.JSONEq(t, `{"event":"note.created"}`, string(body))
}

func TestWebhook_PerJobSecretBeatsConfiguredSecret(t *testing.T) {
	srv, rec := serve(t, http.StatusOK, nil, "")
	ex := buildWebhook(t, allowLoopback+"\nsecret: config-secret")

	_, err := ex.Execute(context.Background(), testJob(webhookPayload(t, WebhookPayload{
		URL:    srv.URL,
		Secret: "per-job-secret",
		Body:   json.RawMessage(`{"a":1}`),
	})))
	require.NoError(t, err)

	_, header, body, _ := rec.snapshot()
	ts, err := strconv.ParseInt(header.Get(HeaderTimestamp), 10, 64)
	require.NoError(t, err)
	require.Equal(t, Sign("per-job-secret", ts, body), header.Get(HeaderSignature))
	require.NotEqual(t, Sign("config-secret", ts, body), header.Get(HeaderSignature))
}

func TestWebhook_ConfiguredSecretIsTheFallback(t *testing.T) {
	srv, rec := serve(t, http.StatusOK, nil, "")
	ex := buildWebhook(t, allowLoopback+"\nsecret: config-secret")

	_, err := ex.Execute(context.Background(), testJob(webhookPayload(t, WebhookPayload{
		URL:  srv.URL,
		Body: json.RawMessage(`{"a":1}`),
	})))
	require.NoError(t, err)

	_, header, body, _ := rec.snapshot()
	ts, err := strconv.ParseInt(header.Get(HeaderTimestamp), 10, 64)
	require.NoError(t, err)
	require.Equal(t, Sign("config-secret", ts, body), header.Get(HeaderSignature))
}

func TestWebhook_HeadersMergeWithPayloadWinning(t *testing.T) {
	srv, rec := serve(t, http.StatusOK, nil, "")
	ex := buildWebhook(t, allowLoopback+"\nheaders:\n  X-Source: mkqd\n  X-Tier: default")

	_, err := ex.Execute(context.Background(), testJob(webhookPayload(t, WebhookPayload{
		URL:     srv.URL,
		Headers: map[string]string{"X-Tier": "per-job", "X-Mkqd-Queue": "spoofed"},
		Body:    json.RawMessage(`{}`),
	})))
	require.NoError(t, err)

	_, header, _, _ := rec.snapshot()
	require.Equal(t, "mkqd", header.Get("X-Source"))
	require.Equal(t, "per-job", header.Get("X-Tier"))
	require.Equal(t, "q", header.Get(HeaderQueue), "payload headers must not spoof mkqd's own")
}

func TestWebhook_AbsentBodyIsNull(t *testing.T) {
	srv, rec := serve(t, http.StatusOK, nil, "")
	ex := buildWebhook(t, allowLoopback)

	_, err := ex.Execute(context.Background(), testJob(webhookPayload(t, WebhookPayload{URL: srv.URL})))
	require.NoError(t, err)

	_, _, body, _ := rec.snapshot()
	require.Equal(t, "null", string(body))
}

// A payload that cannot be delivered will not become deliverable on a
// retry, so every payload defect is a permanent failure.
func TestWebhook_BadPayloadIsPermanent(t *testing.T) {
	ex := buildWebhook(t, allowLoopback)

	cases := []struct {
		name string
		data string
		want string
	}{
		{"not an object", `"just a string"`, "not a webhook payload"},
		{"missing url", `{"body":{}}`, "url is required"},
		{"unparseable url", "{\"url\":\"http://%zz\"}", "not parseable"},
		{"non-http scheme", `{"url":"file:///etc/passwd"}`, "must use http or https"},
		{"no host", `{"url":"http:///path"}`, "has no host"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ex.Execute(context.Background(), testJob(tc.data))
			require.ErrorIs(t, err, mkq.ErrUnrecoverable)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

// The destination comes from the payload, so anyone who can enqueue
// could otherwise aim the worker at the private network around it.
func TestWebhook_BlocksPrivateDestinationByDefault(t *testing.T) {
	srv, rec := serve(t, http.StatusOK, nil, "")
	ex := buildWebhook(t, "type: webhook")

	_, err := ex.Execute(context.Background(), testJob(webhookPayload(t, WebhookPayload{
		URL:  srv.URL,
		Body: json.RawMessage(`{}`),
	})))
	require.Error(t, err)
	require.ErrorIs(t, err, mkq.ErrUnrecoverable, "a blocked destination stays blocked on retry")
	require.ErrorContains(t, err, safedial.ErrBlocked.Error())

	_, _, _, hits := rec.snapshot()
	require.Equal(t, 0, hits, "the request must not reach the server")
}

func TestWebhook_StatusClassificationMatchesHTTP(t *testing.T) {
	cases := []struct {
		status    int
		permanent bool
	}{
		{http.StatusOK, false},
		{http.StatusGone, true},
		{http.StatusInternalServerError, false},
	}

	for _, tc := range cases {
		t.Run(http.StatusText(tc.status), func(t *testing.T) {
			srv, _ := serve(t, tc.status, nil, "")
			ex := buildWebhook(t, allowLoopback)
			_, err := ex.Execute(context.Background(), testJob(webhookPayload(t, WebhookPayload{
				URL:  srv.URL,
				Body: json.RawMessage(`{}`),
			})))
			switch {
			case tc.status == http.StatusOK:
				require.NoError(t, err)
			case tc.permanent:
				require.ErrorIs(t, err, mkq.ErrUnrecoverable)
			default:
				require.Error(t, err)
				require.NotErrorIs(t, err, mkq.ErrUnrecoverable)
			}
		})
	}
}

func TestWebhook_RejectsUnknownOption(t *testing.T) {
	_, err := newWebhookExecutor(context.Background(), mkqd.BuildContext{Queue: "q"},
		executorConfig(t, "type: webhook\nsecrets: oops"))
	require.ErrorContains(t, err, "secrets")
}

func TestExecutorTypesAreRegistered(t *testing.T) {
	require.Contains(t, mkqd.RegisteredExecutors(), "http")
	require.Contains(t, mkqd.RegisteredExecutors(), "webhook")
}

// net/http rejects an invalid header at send time, and that failure
// arrives as a transport error — which would otherwise be retried
// forever. A header that cannot be sent will never become sendable.
func TestWebhook_InvalidPayloadHeaderIsPermanent(t *testing.T) {
	srv, rec := serve(t, http.StatusOK, nil, "")
	ex := buildWebhook(t, allowLoopback)

	cases := []struct {
		name    string
		headers map[string]string
		want    string
	}{
		{"CRLF in value", map[string]string{"X-Bad": "a\r\nInjected: 1"}, "invalid value for header"},
		{"space in name", map[string]string{"Bad Name": "v"}, "invalid header name"},
		{"empty name", map[string]string{"": "v"}, "invalid header name"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ex.Execute(context.Background(), testJob(webhookPayload(t, WebhookPayload{
				URL:     srv.URL,
				Headers: tc.headers,
				Body:    json.RawMessage(`{}`),
			})))
			require.ErrorIs(t, err, mkq.ErrUnrecoverable)
			require.ErrorContains(t, err, tc.want)
		})
	}

	_, _, _, hits := rec.snapshot()
	require.Equal(t, 0, hits, "an unsendable request must not be attempted")
}

func TestWebhook_ConfiguredHeadersAreValidatedAtBuildTime(t *testing.T) {
	_, err := newWebhookExecutor(context.Background(), mkqd.BuildContext{Queue: "q"},
		executorConfig(t, "type: webhook\nheaders:\n  \"Bad Name\": v"))
	require.ErrorContains(t, err, "invalid header name")
}
