package httpexec

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/shiroha-a/mkq"
	"github.com/shiroha-a/mkqd"
)

// executorConfig builds an ExecutorConfig the way the runtime does, by
// going through the real config parser. That keeps the tests honest
// about DecodeStrict and about how options reach an executor.
func executorConfig(t *testing.T, options string) mkqd.ExecutorConfig {
	t.Helper()
	var b strings.Builder
	b.WriteString("redis:\n  addrs: [\"127.0.0.1:6379\"]\nqueues:\n  - name: q\n    executor:\n")
	for _, line := range strings.Split(strings.TrimRight(options, "\n"), "\n") {
		b.WriteString("      " + line + "\n")
	}
	cfg, err := mkqd.ParseConfig([]byte(b.String()))
	require.NoError(t, err)
	require.NotNil(t, cfg.Queues[0].Executor)
	return *cfg.Queues[0].Executor
}

func buildHTTP(t *testing.T, options string) mkqd.Executor {
	t.Helper()
	ex, err := newHTTPExecutor(context.Background(), mkqd.BuildContext{Queue: "q"}, executorConfig(t, options))
	require.NoError(t, err)
	return ex
}

func buildWebhook(t *testing.T, options string) mkqd.Executor {
	t.Helper()
	ex, err := newWebhookExecutor(context.Background(), mkqd.BuildContext{Queue: "q"}, executorConfig(t, options))
	require.NoError(t, err)
	return ex
}

func testJob(data string) *mkqd.Job {
	return &mkqd.Job{
		Queue:        "q",
		ID:           "42",
		Name:         "deliver",
		Data:         json.RawMessage(data),
		AttemptsMade: 1,
		Timestamp:    time.Unix(1758500000, 0).UTC(),
	}
}

// recorder captures the last request a handler saw.
type recorder struct {
	mu     sync.Mutex
	method string
	header http.Header
	body   []byte
	hits   int
}

func (r *recorder) capture(req *http.Request) {
	body, _ := io.ReadAll(req.Body)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.method = req.Method
	r.header = req.Header.Clone()
	r.body = body
	r.hits++
}

func (r *recorder) snapshot() (string, http.Header, []byte, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.method, r.header, r.body, r.hits
}

// serve starts a test server that records requests and replies with a
// fixed status, headers and body.
func serve(t *testing.T, status int, header map[string]string, body string) (*httptest.Server, *recorder) {
	t.Helper()
	rec := &recorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.capture(r)
		for k, v := range header {
			w.Header().Set(k, v)
		}
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv, rec
}

func TestHTTP_SendsEnvelopeAndHeaders(t *testing.T) {
	srv, rec := serve(t, http.StatusOK, nil, "")
	ex := buildHTTP(t, "type: http\nurl: "+srv.URL)

	_, err := ex.Execute(context.Background(), testJob(`{"to":"alice"}`))
	require.NoError(t, err)

	method, header, body, hits := rec.snapshot()
	require.Equal(t, 1, hits)
	require.Equal(t, http.MethodPost, method)
	require.Equal(t, "application/json", header.Get("Content-Type"))
	require.Equal(t, mkqd.UserAgent(), header.Get("User-Agent"))
	require.Equal(t, "q", header.Get(HeaderQueue))
	require.Equal(t, "42", header.Get(HeaderJobID))
	require.Equal(t, "deliver", header.Get(HeaderJobName))
	// attemptsMade=1 の試行は 2 回目。
	require.Equal(t, "2", header.Get(HeaderAttempt))

	var env struct {
		Queue        string          `json:"queue"`
		ID           string          `json:"id"`
		Name         string          `json:"name"`
		Data         json.RawMessage `json:"data"`
		AttemptsMade int             `json:"attemptsMade"`
		Timestamp    time.Time       `json:"timestamp"`
	}
	require.NoError(t, json.Unmarshal(body, &env))
	require.Equal(t, "q", env.Queue)
	require.Equal(t, "42", env.ID)
	require.Equal(t, "deliver", env.Name)
	require.Equal(t, 1, env.AttemptsMade)
	require.JSONEq(t, `{"to":"alice"}`, string(env.Data))
	require.True(t, env.Timestamp.Equal(time.Unix(1758500000, 0)))
}

func TestHTTP_SignatureCoversTimestampAndBody(t *testing.T) {
	srv, rec := serve(t, http.StatusOK, nil, "")
	ex := buildHTTP(t, "type: http\nurl: "+srv.URL+"\nsecret: topsecret")

	_, err := ex.Execute(context.Background(), testJob(`{"to":"alice"}`))
	require.NoError(t, err)

	_, header, body, _ := rec.snapshot()
	ts := header.Get(HeaderTimestamp)
	require.NotEmpty(t, ts)
	seconds, err := strconv.ParseInt(ts, 10, 64)
	require.NoError(t, err)

	mac := hmac.New(sha256.New, []byte("topsecret"))
	mac.Write([]byte("v1:" + ts + ":"))
	mac.Write(body)
	want := "v1=" + hex.EncodeToString(mac.Sum(nil))

	require.Equal(t, want, header.Get(HeaderSignature))
	require.Equal(t, want, Sign("topsecret", seconds, body))
}

func TestHTTP_NoSecretSendsNoSignature(t *testing.T) {
	srv, rec := serve(t, http.StatusOK, nil, "")
	ex := buildHTTP(t, "type: http\nurl: "+srv.URL)

	_, err := ex.Execute(context.Background(), testJob(`{}`))
	require.NoError(t, err)

	_, header, _, _ := rec.snapshot()
	require.Empty(t, header.Get(HeaderSignature))
}

func TestHTTP_ReturnValue(t *testing.T) {
	t.Run("json body becomes the return value", func(t *testing.T) {
		srv, _ := serve(t, http.StatusOK, map[string]string{"Content-Type": "application/json"}, `{"delivered":true}`)
		out, err := buildHTTP(t, "type: http\nurl: "+srv.URL).Execute(context.Background(), testJob(`{}`))
		require.NoError(t, err)
		raw, ok := out.(json.RawMessage)
		require.True(t, ok, "expected json.RawMessage, got %T", out)
		require.JSONEq(t, `{"delivered":true}`, string(raw))
	})

	t.Run("non-json content type is dropped even when the body parses", func(t *testing.T) {
		// 本文が JSON として妥当でも、Content-Type が JSON でなければ
		// returnvalue にしない。宣言された型を信じる。
		srv, _ := serve(t, http.StatusOK, map[string]string{"Content-Type": "text/plain"}, `{"looks":"like json"}`)
		out, err := buildHTTP(t, "type: http\nurl: "+srv.URL).Execute(context.Background(), testJob(`{}`))
		require.NoError(t, err)
		require.Nil(t, out)
	})

	t.Run("plain text body is dropped", func(t *testing.T) {
		srv, _ := serve(t, http.StatusOK, map[string]string{"Content-Type": "text/plain"}, "ok")
		out, err := buildHTTP(t, "type: http\nurl: "+srv.URL).Execute(context.Background(), testJob(`{}`))
		require.NoError(t, err)
		require.Nil(t, out)
	})

	t.Run("no content", func(t *testing.T) {
		srv, _ := serve(t, http.StatusNoContent, nil, "")
		out, err := buildHTTP(t, "type: http\nurl: "+srv.URL).Execute(context.Background(), testJob(`{}`))
		require.NoError(t, err)
		require.Nil(t, out)
	})

	t.Run("malformed json is dropped rather than stored", func(t *testing.T) {
		srv, _ := serve(t, http.StatusOK, map[string]string{"Content-Type": "application/json"}, `{"broken":`)
		out, err := buildHTTP(t, "type: http\nurl: "+srv.URL).Execute(context.Background(), testJob(`{}`))
		require.NoError(t, err)
		require.Nil(t, out)
	})
}

func TestHTTP_StatusClassification(t *testing.T) {
	cases := []struct {
		status    int
		permanent bool
		retry     bool
	}{
		{http.StatusOK, false, false},
		{http.StatusCreated, false, false},
		{http.StatusAccepted, false, false},
		{http.StatusNoContent, false, false},

		{http.StatusMovedPermanently, true, false},
		{http.StatusFound, true, false},
		{http.StatusBadRequest, true, false},
		{http.StatusConflict, true, false},
		{http.StatusGone, true, false},
		{http.StatusUnprocessableEntity, true, false},

		{http.StatusUnauthorized, false, true},
		{http.StatusForbidden, false, true},
		{http.StatusNotFound, false, true},
		{http.StatusRequestTimeout, false, true},
		{http.StatusTooManyRequests, false, true},
		{http.StatusInternalServerError, false, true},
		{http.StatusBadGateway, false, true},
		{http.StatusServiceUnavailable, false, true},
	}

	for _, tc := range cases {
		t.Run(http.StatusText(tc.status), func(t *testing.T) {
			srv, _ := serve(t, tc.status, nil, "")
			_, err := buildHTTP(t, "type: http\nurl: "+srv.URL).Execute(context.Background(), testJob(`{}`))

			switch {
			case tc.permanent:
				require.Error(t, err)
				require.ErrorIs(t, err, mkq.ErrUnrecoverable)
			case tc.retry:
				require.Error(t, err)
				require.NotErrorIs(t, err, mkq.ErrUnrecoverable)
			default:
				require.NoError(t, err)
			}
		})
	}
}

func TestHTTP_RetryHeaderOverridesStatus(t *testing.T) {
	// 500 は通常なら再試行だが、アプリが「もう送るな」と言ったら従う。
	srv, _ := serve(t, http.StatusInternalServerError, map[string]string{HeaderRetry: "no"}, "")
	_, err := buildHTTP(t, "type: http\nurl: "+srv.URL).Execute(context.Background(), testJob(`{}`))
	require.ErrorIs(t, err, mkq.ErrUnrecoverable)
}

// A redirect must not be followed: the destination would receive the
// signed body without having been vetted, and for the webhook executor
// it would bypass the SSRF guard entirely.
func TestHTTP_RedirectIsNotFollowed(t *testing.T) {
	target, targetRec := serve(t, http.StatusOK, nil, "")

	rec := &recorder{}
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.capture(r)
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(redirector.Close)

	_, err := buildHTTP(t, "type: http\nurl: "+redirector.URL).Execute(context.Background(), testJob(`{}`))
	require.ErrorIs(t, err, mkq.ErrUnrecoverable)

	_, _, _, hits := rec.snapshot()
	require.Equal(t, 1, hits)
	_, _, _, followed := targetRec.snapshot()
	require.Equal(t, 0, followed, "the redirect target must not be contacted")
}

func TestHTTP_FailureDetail(t *testing.T) {
	t.Run("json error field", func(t *testing.T) {
		srv, _ := serve(t, http.StatusBadRequest,
			map[string]string{"Content-Type": "application/json"}, `{"error":"unknown activity type"}`)
		_, err := buildHTTP(t, "type: http\nurl: "+srv.URL).Execute(context.Background(), testJob(`{}`))
		require.ErrorContains(t, err, "unknown activity type")
	})

	t.Run("plain body", func(t *testing.T) {
		srv, _ := serve(t, http.StatusBadRequest, nil, "nope")
		_, err := buildHTTP(t, "type: http\nurl: "+srv.URL).Execute(context.Background(), testJob(`{}`))
		require.ErrorContains(t, err, "nope")
	})

	t.Run("empty body", func(t *testing.T) {
		srv, _ := serve(t, http.StatusBadRequest, nil, "")
		_, err := buildHTTP(t, "type: http\nurl: "+srv.URL).Execute(context.Background(), testJob(`{}`))
		require.ErrorContains(t, err, "empty response body")
	})

	t.Run("long body is truncated", func(t *testing.T) {
		srv, _ := serve(t, http.StatusBadRequest, nil, strings.Repeat("x", 4096))
		_, err := buildHTTP(t, "type: http\nurl: "+srv.URL).Execute(context.Background(), testJob(`{}`))
		require.ErrorContains(t, err, "truncated")
		require.Less(t, len(err.Error()), 2048, "failedReason must stay dashboard-sized")
	})
}

func TestHTTP_MaxResponseBytesCapsTheRead(t *testing.T) {
	srv, _ := serve(t, http.StatusBadRequest, nil, strings.Repeat("y", 4096))
	_, err := buildHTTP(t, "type: http\nurl: "+srv.URL+"\nmax_response_bytes: 16").
		Execute(context.Background(), testJob(`{}`))
	require.Error(t, err)
	// 本文は 16 バイトで打ち切られる。17 個連続することはない。
	require.Contains(t, err.Error(), strings.Repeat("y", 16))
	require.NotContains(t, err.Error(), strings.Repeat("y", 17))
}

func TestHTTP_TimeoutIsRetryable(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		<-release
	}))
	t.Cleanup(func() { close(release); srv.Close() })

	_, err := buildHTTP(t, "type: http\nurl: "+srv.URL+"\ntimeout: 100ms").
		Execute(context.Background(), testJob(`{}`))
	require.Error(t, err)
	require.NotErrorIs(t, err, mkq.ErrUnrecoverable, "a timeout must leave the job retryable")
}

func TestHTTP_ConfiguredHeadersCannotOverrideReservedOnes(t *testing.T) {
	srv, rec := serve(t, http.StatusOK, nil, "")
	ex := buildHTTP(t, "type: http\nurl: "+srv.URL+
		"\nheaders:\n  X-Trace: abc\n  X-Mkqd-Queue: spoofed\n  X-Mkqd-Custom: mine\n  Content-Type: text/plain")

	_, err := ex.Execute(context.Background(), testJob(`{}`))
	require.NoError(t, err)

	_, header, _, _ := rec.snapshot()
	require.Equal(t, "abc", header.Get("X-Trace"), "ordinary headers pass through")
	require.Equal(t, "q", header.Get(HeaderQueue), "mkqd's own headers cannot be spoofed")
	require.Equal(t, "application/json", header.Get("Content-Type"))
	// X-Mkqd-* は mkqd の名前空間。mkqd 自身が設定しないものも含めて塞ぐ。
	require.Empty(t, header.Get("X-Mkqd-Custom"))
}

func TestHTTP_ConfigErrors(t *testing.T) {
	ctx := context.Background()
	bc := mkqd.BuildContext{Queue: "q"}

	_, err := newHTTPExecutor(ctx, bc, executorConfig(t, "type: http"))
	require.ErrorContains(t, err, "url is required")

	_, err = newHTTPExecutor(ctx, bc, executorConfig(t, "type: http\nurl: ftp://example.com"))
	require.ErrorContains(t, err, "must use http or https")

	// 綴り間違いを黙って無視しない。
	_, err = newHTTPExecutor(ctx, bc, executorConfig(t, "type: http\nurl: http://x/y\ntimeoout: 5s"))
	require.ErrorContains(t, err, "timeoout")
}

func TestHTTP_UserAgentOverride(t *testing.T) {
	srv, rec := serve(t, http.StatusOK, nil, "")
	ex := buildHTTP(t, "type: http\nurl: "+srv.URL+"\nuser_agent: my-app/2.0")
	_, err := ex.Execute(context.Background(), testJob(`{}`))
	require.NoError(t, err)
	_, header, _, _ := rec.snapshot()
	require.Equal(t, "my-app/2.0", header.Get("User-Agent"))
}

// A successful job has nothing to retry, so a blanket X-Mkqd-Retry: no
// on an endpoint must not turn every success into a failure.
func TestHTTP_RetryHeaderDoesNotSpoilSuccess(t *testing.T) {
	srv, _ := serve(t, http.StatusOK,
		map[string]string{HeaderRetry: "no", "Content-Type": "application/json"}, `{"ok":true}`)
	out, err := buildHTTP(t, "type: http\nurl: "+srv.URL).Execute(context.Background(), testJob(`{}`))
	require.NoError(t, err)
	require.NotNil(t, out)
}

// max_response_bytes must bound what crosses the wire, not only what is
// kept. The transport transparently inflates gzip, so an unbounded
// drain lets a small compressed response occupy a worker for the whole
// timeout.
func TestHTTP_OversizedResponseIsNotDrainedWhole(t *testing.T) {
	const bodySize = 8 << 20
	var written atomic.Int64

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		chunk := bytes.Repeat([]byte("z"), 32<<10)
		for sent := 0; sent < bodySize; sent += len(chunk) {
			n, err := w.Write(chunk)
			written.Add(int64(n))
			if err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)

	_, err := buildHTTP(t, "type: http\nurl: "+srv.URL+"\nmax_response_bytes: 1024\ntimeout: 10s").
		Execute(context.Background(), testJob(`{}`))
	require.Error(t, err)

	// 読み取り 1KiB + ドレイン上限 8KiB。書き込みは chunk 単位で進むので
	// 余裕を持たせつつ、本文全体 (8MiB) を読み切らないことを確かめる。
	require.Less(t, written.Load(), int64(bodySize/2),
		"the whole body was transferred despite max_response_bytes")
}

func TestHTTP_JobMetadataIsSanitizedIntoHeaders(t *testing.T) {
	srv, rec := serve(t, http.StatusOK, nil, "")
	ex := buildHTTP(t, "type: http\nurl: "+srv.URL)

	job := testJob(`{}`)
	// BullMQ の producer は言語を問わず任意の名前を付けられる。
	job.Name = "note\r\nInjected: 1"
	job.ID = "7\r\n"

	_, err := ex.Execute(context.Background(), job)
	require.NoError(t, err)

	_, header, _, _ := rec.snapshot()
	require.Equal(t, "note__Injected: 1", header.Get(HeaderJobName))
	require.Equal(t, "7__", header.Get(HeaderJobID))
	require.Empty(t, header.Get("Injected"), "CRLF in a job name must not inject a header")
}

func TestHTTP_LongJobNameIsBounded(t *testing.T) {
	srv, rec := serve(t, http.StatusOK, nil, "")
	job := testJob(`{}`)
	job.Name = strings.Repeat("n", 4096)

	_, err := buildHTTP(t, "type: http\nurl: "+srv.URL).Execute(context.Background(), job)
	require.NoError(t, err)

	_, header, _, _ := rec.snapshot()
	require.LessOrEqual(t, len(header.Get(HeaderJobName)), headerValueLimit+len("... (truncated)"))
}

func TestHTTP_ConfiguredHeadersAreValidatedAtBuildTime(t *testing.T) {
	ctx := context.Background()
	bc := mkqd.BuildContext{Queue: "q"}

	_, err := newHTTPExecutor(ctx, bc, executorConfig(t, "type: http\nurl: http://x/y\nheaders:\n  \"Bad Name\": v"))
	require.ErrorContains(t, err, "invalid header name")

	_, err = newHTTPExecutor(ctx, bc, executorConfig(t, "type: http\nurl: http://x/y\nheaders:\n  X-Ok: \"a\\rb\""))
	require.ErrorContains(t, err, "invalid value for header")
}

func TestTruncate_CutsOnARuneBoundary(t *testing.T) {
	// 日本語のエラーメッセージが壊れた UTF-8 になってダッシュボードに
	// 出ることがないようにする。
	s := strings.Repeat("あ", 500) // 1500 bytes
	got := truncate(s, failedReasonLimit)
	require.True(t, utf8.ValidString(got), "truncated text must stay valid UTF-8")
	require.True(t, strings.HasSuffix(got, "... (truncated)"))
	require.LessOrEqual(t, len(got)-len("... (truncated)"), failedReasonLimit)
}

func TestTruncate_ShortStringIsUntouched(t *testing.T) {
	require.Equal(t, "ありがとう", truncate("ありがとう", failedReasonLimit))
}
