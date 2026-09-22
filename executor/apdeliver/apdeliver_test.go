package apdeliver

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/shiroha-a/mkq"
	"github.com/shiroha-a/mkqd"
	"github.com/shiroha-a/mkqd/httpsig"
	"github.com/shiroha-a/mkqd/internal/safedial"
)

const testKeyID = "https://local.example/users/me#main-key"

func testSigner(t *testing.T) (*httpsig.LocalSigner, *rsa.PublicKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	s, err := httpsig.NewLocalSigner(testKeyID, key)
	require.NoError(t, err)
	return s, &key.PublicKey
}

// httptest binds to loopback, which the SSRF guard blocks. Delivery
// tests opt out explicitly; one test below pins that the default really
// does block.
func testExecutor(t *testing.T, signer httpsig.Signer) *Executor {
	t.Helper()
	ex, err := New(Options{Signer: signer, AllowPrivateNetwork: true})
	require.NoError(t, err)
	return ex
}

func job(t *testing.T, p Payload) *mkqd.Job {
	t.Helper()
	raw, err := json.Marshal(p)
	require.NoError(t, err)
	return &mkqd.Job{Queue: "deliver", ID: "1", Name: "deliver", Data: raw}
}

type received struct {
	mu     sync.Mutex
	req    *http.Request
	body   []byte
	hits   int
	verify error
}

func (r *received) snapshot() (*http.Request, []byte, int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.req, r.body, r.hits, r.verify
}

// inbox starts a server that verifies the HTTP Signature the way a real
// ActivityPub implementation would: rebuild the signing string from the
// request it received and check it against the public key.
func inbox(t *testing.T, pub *rsa.PublicKey, status int, respBody string) (*httptest.Server, *received) {
	t.Helper()
	rec := &received{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)

		rec.mu.Lock()
		rec.req = r.Clone(context.Background())
		rec.body = body
		rec.hits++
		if pub != nil {
			rec.verify = httpsig.Verify(r, body, pub)
		}
		rec.mu.Unlock()

		w.WriteHeader(status)
		_, _ = io.WriteString(w, respBody)
	}))
	t.Cleanup(srv.Close)
	return srv, rec
}

func TestDeliver_SignatureVerifiesAtTheInbox(t *testing.T) {
	signer, pub := testSigner(t)
	srv, rec := inbox(t, pub, http.StatusAccepted, "")

	activity := json.RawMessage(`{"@context":"https://www.w3.org/ns/activitystreams","type":"Create"}`)
	_, err := testExecutor(t, signer).Execute(context.Background(), job(t, Payload{
		Inbox:    srv.URL + "/users/alice/inbox",
		KeyID:    testKeyID,
		Activity: activity,
	}))
	require.NoError(t, err)

	req, body, hits, verr := rec.snapshot()
	require.Equal(t, 1, hits)
	require.NoError(t, verr, "the inbox must be able to verify the signature")
	require.Equal(t, http.MethodPost, req.Method)
	require.Equal(t, defaultContentType, req.Header.Get("Content-Type"))
	require.JSONEq(t, string(activity), string(body))

	// digest は実際に送ったバイト列に対するもの。
	require.Equal(t, httpsig.Digest(body), req.Header.Get("Digest"))

	parsed, err := httpsig.ParseSignatureHeader(req.Header.Get("Signature"))
	require.NoError(t, err)
	require.Equal(t, testKeyID, parsed.KeyID)
	require.Equal(t, httpsig.DefaultHeaders, parsed.Headers)
}

func TestDeliver_BodyIsSentVerbatim(t *testing.T) {
	signer, pub := testSigner(t)
	srv, rec := inbox(t, pub, http.StatusOK, "")

	// 送信バイト列を呼び出し側が固定したい場合の経路。空白の入り方まで保つ。
	raw := "{\n  \"type\": \"Follow\"\n}"
	_, err := testExecutor(t, signer).Execute(context.Background(), job(t, Payload{
		Inbox: srv.URL + "/inbox",
		Body:  raw,
	}))
	require.NoError(t, err)

	req, body, _, verr := rec.snapshot()
	require.NoError(t, verr)
	require.Equal(t, raw, string(body))
	require.Equal(t, httpsig.Digest([]byte(raw)), req.Header.Get("Digest"))
}

func TestDeliver_EmptyKeyIDUsesTheSignerDefault(t *testing.T) {
	signer, pub := testSigner(t)
	srv, rec := inbox(t, pub, http.StatusOK, "")

	_, err := testExecutor(t, signer).Execute(context.Background(), job(t, Payload{
		Inbox:    srv.URL + "/inbox",
		Activity: json.RawMessage(`{}`),
	}))
	require.NoError(t, err)

	req, _, _, verr := rec.snapshot()
	require.NoError(t, verr)
	parsed, err := httpsig.ParseSignatureHeader(req.Header.Get("Signature"))
	require.NoError(t, err)
	require.Equal(t, testKeyID, parsed.KeyID)
}

func TestDeliver_UnknownKeyIsPermanent(t *testing.T) {
	signer, pub := testSigner(t)
	srv, rec := inbox(t, pub, http.StatusOK, "")

	_, err := testExecutor(t, signer).Execute(context.Background(), job(t, Payload{
		Inbox:    srv.URL + "/inbox",
		KeyID:    "https://local.example/users/someone-else#main-key",
		Activity: json.RawMessage(`{}`),
	}))
	require.ErrorIs(t, err, mkq.ErrUnrecoverable, "a key that does not exist will not appear on a retry")

	_, _, hits, _ := rec.snapshot()
	require.Equal(t, 0, hits, "nothing should be sent unsigned")
}

func TestDeliver_ContentTypeOverride(t *testing.T) {
	signer, pub := testSigner(t)
	srv, rec := inbox(t, pub, http.StatusOK, "")

	_, err := testExecutor(t, signer).Execute(context.Background(), job(t, Payload{
		Inbox:       srv.URL + "/inbox",
		Activity:    json.RawMessage(`{}`),
		ContentType: "application/ld+json",
	}))
	require.NoError(t, err)

	req, _, _, verr := rec.snapshot()
	require.NoError(t, verr, "the override must be the value that was signed")
	require.Equal(t, "application/ld+json", req.Header.Get("Content-Type"))
}

func TestDeliver_ExtraHeadersArePassedButCannotTouchTheSignature(t *testing.T) {
	signer, pub := testSigner(t)
	srv, rec := inbox(t, pub, http.StatusOK, "")

	_, err := testExecutor(t, signer).Execute(context.Background(), job(t, Payload{
		Inbox:    srv.URL + "/inbox",
		Activity: json.RawMessage(`{}`),
		Headers: map[string]string{
			"X-Trace": "abc",
			"Digest":  "SHA-256=bogus",
			"Date":    "Thu, 01 Jan 1970 00:00:00 GMT",
		},
	}))
	require.NoError(t, err)

	req, body, _, verr := rec.snapshot()
	require.NoError(t, verr)
	require.Equal(t, "abc", req.Header.Get("X-Trace"))
	require.Equal(t, httpsig.Digest(body), req.Header.Get("Digest"), "a payload must not choose the digest")
	require.NotEqual(t, "Thu, 01 Jan 1970 00:00:00 GMT", req.Header.Get("Date"))
}

func TestDeliver_InvalidExtraHeaderIsPermanent(t *testing.T) {
	signer, _ := testSigner(t)
	srv, rec := inbox(t, nil, http.StatusOK, "")

	_, err := testExecutor(t, signer).Execute(context.Background(), job(t, Payload{
		Inbox:    srv.URL + "/inbox",
		Activity: json.RawMessage(`{}`),
		Headers:  map[string]string{"X-Bad": "a\r\nInjected: 1"},
	}))
	require.ErrorIs(t, err, mkq.ErrUnrecoverable)

	_, _, hits, _ := rec.snapshot()
	require.Equal(t, 0, hits)
}

func TestDeliver_StatusClassification(t *testing.T) {
	cases := []struct {
		status    int
		permanent bool
		retry     bool
	}{
		{http.StatusOK, false, false},
		{http.StatusAccepted, false, false},
		{http.StatusNoContent, false, false},

		{http.StatusMovedPermanently, true, false},
		{http.StatusFound, true, false},
		{http.StatusBadRequest, true, false},
		{http.StatusForbidden, true, false},
		{http.StatusNotFound, true, false},
		{http.StatusGone, true, false},
		{http.StatusUnprocessableEntity, true, false},

		// 401 は署名検証の失敗で、原因の大半は時刻ずれや相手がこちらの
		// keyId を取りに来られなかったといった一過性のもの。
		{http.StatusUnauthorized, false, true},
		{http.StatusRequestTimeout, false, true},
		{http.StatusTooManyRequests, false, true},
		{http.StatusInternalServerError, false, true},
		{http.StatusBadGateway, false, true},
		{http.StatusServiceUnavailable, false, true},
	}

	signer, _ := testSigner(t)
	for _, tc := range cases {
		t.Run(http.StatusText(tc.status), func(t *testing.T) {
			srv, _ := inbox(t, nil, tc.status, "")
			_, err := testExecutor(t, signer).Execute(context.Background(), job(t, Payload{
				Inbox:    srv.URL + "/inbox",
				Activity: json.RawMessage(`{}`),
			}))
			switch {
			case tc.permanent:
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

// Following a redirect would resend the signed body to a host the
// signature was not computed for, and for a payload-supplied inbox it
// would step around the SSRF guard.
func TestDeliver_RedirectIsNotFollowed(t *testing.T) {
	signer, pub := testSigner(t)
	target, targetRec := inbox(t, pub, http.StatusOK, "")

	rec := &received{}
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.mu.Lock()
		rec.hits++
		rec.mu.Unlock()
		http.Redirect(w, r, target.URL+"/inbox", http.StatusTemporaryRedirect)
	}))
	t.Cleanup(redirector.Close)

	_, err := testExecutor(t, signer).Execute(context.Background(), job(t, Payload{
		Inbox:    redirector.URL + "/inbox",
		Activity: json.RawMessage(`{}`),
	}))
	require.ErrorIs(t, err, mkq.ErrUnrecoverable)

	_, _, hits, _ := rec.snapshot()
	require.Equal(t, 1, hits)
	_, _, followed, _ := targetRec.snapshot()
	require.Equal(t, 0, followed, "the redirect target must not be contacted")
}

func TestDeliver_BadPayloadIsPermanent(t *testing.T) {
	signer, _ := testSigner(t)
	ex := testExecutor(t, signer)

	cases := []struct {
		name string
		data string
		want string
	}{
		{"not an object", `"a string"`, "not a delivery payload"},
		{"no inbox", `{"activity":{}}`, "inbox is required"},
		{"non-http inbox", `{"inbox":"file:///etc/passwd","activity":{}}`, "must use http or https"},
		{"inbox without host", `{"inbox":"http:///inbox","activity":{}}`, "has no host"},
		{"no body at all", `{"inbox":"https://remote.example/inbox"}`, `one of "activity" or "body" is required`},
		{"both body forms", `{"inbox":"https://remote.example/inbox","activity":{},"body":"x"}`, "are exclusive"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			j := &mkqd.Job{Queue: "deliver", ID: "1", Name: "deliver", Data: json.RawMessage(tc.data)}
			_, err := ex.Execute(context.Background(), j)
			require.ErrorIs(t, err, mkq.ErrUnrecoverable)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

// The inbox URL arrives in the job payload, so a job could otherwise
// aim the worker at the private network around it.
func TestDeliver_BlocksPrivateDestinationByDefault(t *testing.T) {
	signer, _ := testSigner(t)
	srv, rec := inbox(t, nil, http.StatusOK, "")

	ex, err := New(Options{Signer: signer})
	require.NoError(t, err)

	_, err = ex.Execute(context.Background(), job(t, Payload{
		Inbox:    srv.URL + "/inbox",
		Activity: json.RawMessage(`{}`),
	}))
	require.ErrorIs(t, err, mkq.ErrUnrecoverable, "a blocked destination stays blocked on retry")
	require.ErrorContains(t, err, safedial.ErrBlocked.Error())

	_, _, hits, _ := rec.snapshot()
	require.Equal(t, 0, hits)
}

func TestDeliver_FailureDetailIsBounded(t *testing.T) {
	signer, _ := testSigner(t)
	srv, _ := inbox(t, nil, http.StatusBadRequest, strings.Repeat("え", 2000))

	_, err := testExecutor(t, signer).Execute(context.Background(), job(t, Payload{
		Inbox:    srv.URL + "/inbox",
		Activity: json.RawMessage(`{}`),
	}))
	require.ErrorContains(t, err, "truncated")
	require.Less(t, len(err.Error()), 2048)
}

func TestDeliver_EmptyFailureBody(t *testing.T) {
	signer, _ := testSigner(t)
	srv, _ := inbox(t, nil, http.StatusBadRequest, "")
	_, err := testExecutor(t, signer).Execute(context.Background(), job(t, Payload{
		Inbox:    srv.URL + "/inbox",
		Activity: json.RawMessage(`{}`),
	}))
	require.ErrorContains(t, err, "empty response body")
}

func TestNew_RequiresASigner(t *testing.T) {
	_, err := New(Options{})
	require.ErrorContains(t, err, "signer is required")
}

func TestExecutorTypeIsRegistered(t *testing.T) {
	require.Contains(t, mkqd.RegisteredExecutors(), "activitypub_deliver")
}

func TestDeliver_InvalidContentTypeIsPermanent(t *testing.T) {
	signer, _ := testSigner(t)
	srv, rec := inbox(t, nil, http.StatusOK, "")

	_, err := testExecutor(t, signer).Execute(context.Background(), job(t, Payload{
		Inbox:       srv.URL + "/inbox",
		Activity:    json.RawMessage(`{}`),
		ContentType: "application/json\r\nX-Injected: 1",
	}))
	require.ErrorIs(t, err, mkq.ErrUnrecoverable)
	require.ErrorContains(t, err, "invalid contentType")

	_, _, hits, _ := rec.snapshot()
	require.Equal(t, 0, hits)
}

// `null`, a number and a bare string are all valid JSON, so "is it
// non-empty" is not enough: without a check, mkqd signs and delivers
// them to a remote inbox as if they were activities.
func TestDeliver_ActivityMustBeAnObject(t *testing.T) {
	signer, _ := testSigner(t)
	srv, rec := inbox(t, nil, http.StatusOK, "")
	ex := testExecutor(t, signer)

	for _, activity := range []string{`null`, `123`, `"a string"`, `[]`, `true`} {
		t.Run(activity, func(t *testing.T) {
			data := `{"inbox":"` + srv.URL + `/inbox","activity":` + activity + `}`
			j := &mkqd.Job{Queue: "deliver", ID: "1", Name: "deliver", Data: json.RawMessage(data)}
			_, err := ex.Execute(context.Background(), j)
			require.ErrorIs(t, err, mkq.ErrUnrecoverable)
			require.ErrorContains(t, err, "must be a JSON object")
		})
	}

	_, _, hits, _ := rec.snapshot()
	require.Equal(t, 0, hits, "nothing should reach a remote inbox")
}
