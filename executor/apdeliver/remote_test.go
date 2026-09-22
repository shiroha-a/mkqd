package apdeliver

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/shiroha-a/mkq"
	"github.com/shiroha-a/mkqd"
	"github.com/shiroha-a/mkqd/httpsig"
	"github.com/shiroha-a/mkqd/internal/httpsend"
)

// signingService stands in for the application: it holds the key,
// checks that the request really came from mkqd, and signs.
type signingService struct {
	mu        sync.Mutex
	key       *rsa.PrivateKey
	secret    string
	hits      int
	lastReq   RemoteRequest
	signature string
	hmacOK    bool
}

func (s *signingService) handler(t *testing.T) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)

		var req RemoteRequest
		if err := json.Unmarshal(body, &req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		raw, err := base64.StdEncoding.DecodeString(req.SigningString)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		ok := true
		if s.secret != "" {
			ts, _ := strconv.ParseInt(r.Header.Get(httpsend.HeaderTimestamp), 10, 64)
			want := httpsend.SignHMAC(s.secret, ts, body)
			ok = hmac.Equal([]byte(want), []byte(r.Header.Get(httpsend.HeaderSignature)))
		}

		local, err := httpsig.NewLocalSigner(testKeyID, s.key)
		require.NoError(t, err)
		sig, err := local.Sign(context.Background(), "", raw)
		require.NoError(t, err)

		s.mu.Lock()
		s.hits++
		s.lastReq = req
		s.hmacOK = ok
		s.signature = base64.StdEncoding.EncodeToString(sig.Bytes)
		s.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(RemoteResponse{
			KeyID:     testKeyID,
			Algorithm: httpsig.AlgorithmRSASHA256,
			Signature: base64.StdEncoding.EncodeToString(sig.Bytes),
		})
	}
}

func (s *signingService) snapshot() (int, RemoteRequest, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hits, s.lastReq, s.hmacOK
}

func newSigningService(t *testing.T, secret string) (*httptest.Server, *signingService, *rsa.PublicKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	svc := &signingService{key: key, secret: secret}
	srv := httptest.NewServer(svc.handler(t))
	t.Cleanup(srv.Close)
	return srv, svc, &key.PublicKey
}

// The point of the remote signer: mkqd never sees the private key, and
// the delivery still verifies at the receiving inbox.
func TestRemoteSigner_DeliveryVerifiesEndToEnd(t *testing.T) {
	signSrv, svc, pub := newSigningService(t, "signing-secret")

	signer, err := BuildSigner(SignerConfig{Type: "remote", URL: signSrv.URL, Secret: "signing-secret"})
	require.NoError(t, err)

	inboxSrv, rec := inbox(t, pub, http.StatusAccepted, "")
	ex, err := New(Options{Signer: signer, AllowPrivateNetwork: true})
	require.NoError(t, err)

	_, err = ex.Execute(context.Background(), job(t, Payload{
		Inbox:    inboxSrv.URL + "/users/alice/inbox",
		KeyID:    testKeyID,
		Activity: json.RawMessage(`{"type":"Create"}`),
	}))
	require.NoError(t, err)

	_, _, _, verr := rec.snapshot()
	require.NoError(t, verr, "the inbox must verify a remotely produced signature")

	hits, req, hmacOK := svc.snapshot()
	require.Equal(t, 1, hits)
	require.True(t, hmacOK, "the signing endpoint must be able to authenticate mkqd")
	require.Equal(t, testKeyID, req.KeyID)
	require.Equal(t, httpsig.AlgorithmRSASHA256, req.Algorithm)

	// 署名対象は base64。改行を含むのでそのまま JSON に載せたくない。
	raw, err := base64.StdEncoding.DecodeString(req.SigningString)
	require.NoError(t, err)
	require.Contains(t, string(raw), "(request-target): post /users/alice/inbox")
	require.Contains(t, string(raw), "digest: ")
}

func TestRemoteSigner_NoSecretSendsNoSignature(t *testing.T) {
	seen := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Header.Get(httpsend.HeaderSignature)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(RemoteResponse{KeyID: testKeyID, Signature: base64.StdEncoding.EncodeToString([]byte("x"))})
	}))
	t.Cleanup(srv.Close)

	signer, err := BuildSigner(SignerConfig{Type: "remote", URL: srv.URL})
	require.NoError(t, err)
	_, err = signer.Sign(context.Background(), testKeyID, []byte("msg"))
	require.NoError(t, err)
	require.Empty(t, <-seen)
}

func TestRemoteSigner_EmptyKeyIDAsksForTheDefault(t *testing.T) {
	signSrv, svc, _ := newSigningService(t, "")

	signer, err := BuildSigner(SignerConfig{Type: "remote", URL: signSrv.URL})
	require.NoError(t, err)

	sig, err := signer.Sign(context.Background(), "", []byte("msg"))
	require.NoError(t, err)
	// アプリが既定の鍵 ID を返すので、Signature ヘッダに載せる名前が決まる。
	require.Equal(t, testKeyID, sig.KeyID)

	_, req, _ := svc.snapshot()
	require.Empty(t, req.KeyID)
}

// The application saying "no such key" is a permanent condition: the
// activity is dropped rather than retried until attempts run out.
//
// **素の 404 はその意味にならない。** signer.url の綴り違い、まだ配備されて
// いないルート、/_mkqd/sign を通さない reverse proxy も 404 を返す。そこで
// 恒久的失敗にすると、設定を直したときには捨てた後になる。
func TestRemoteSigner_NoSuchKeyClassification(t *testing.T) {
	cases := []struct {
		name        string
		status      int
		contentType string
		unknownKey  bool
	}{
		{"410 Gone", http.StatusGone, "", true},
		{"410 with a body", http.StatusGone, "application/json", true},
		{"404 from the app", http.StatusNotFound, "application/json", true},
		{"bare 404", http.StatusNotFound, "", false},
		{"404 from a proxy", http.StatusNotFound, "text/html", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.contentType != "" {
					w.Header().Set("Content-Type", tc.contentType)
				}
				w.WriteHeader(tc.status)
			}))
			t.Cleanup(srv.Close)

			signer, err := BuildSigner(SignerConfig{Type: "remote", URL: srv.URL})
			require.NoError(t, err)
			_, err = signer.Sign(context.Background(), "https://local.example/users/nobody#main-key", []byte("msg"))
			require.Error(t, err)
			if tc.unknownKey {
				require.ErrorIs(t, err, httpsig.ErrUnknownKey)
			} else {
				require.NotErrorIs(t, err, httpsig.ErrUnknownKey,
					"a 404 that the application did not author must stay retryable")
			}
		})
	}
}

// Anything else is the signing side having a bad moment, which must not
// cost the activity.
func TestRemoteSigner_OtherFailuresStayRetryable(t *testing.T) {
	for _, status := range []int{http.StatusInternalServerError, http.StatusBadGateway,
		http.StatusServiceUnavailable, http.StatusUnauthorized, http.StatusBadRequest} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
				_, _ = io.WriteString(w, "signer said no")
			}))
			t.Cleanup(srv.Close)

			signer, err := BuildSigner(SignerConfig{Type: "remote", URL: srv.URL})
			require.NoError(t, err)
			_, err = signer.Sign(context.Background(), testKeyID, []byte("msg"))
			require.Error(t, err)
			require.NotErrorIs(t, err, httpsig.ErrUnknownKey)
			require.ErrorContains(t, err, "signer said no")
		})
	}
}

// A failing signer must leave the job retryable end to end, not just at
// the signer boundary.
func TestRemoteSigner_DeliveryStaysRetryableWhenSigningFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)

	signer, err := BuildSigner(SignerConfig{Type: "remote", URL: srv.URL})
	require.NoError(t, err)
	ex, err := New(Options{Signer: signer, AllowPrivateNetwork: true})
	require.NoError(t, err)

	_, err = ex.Execute(context.Background(), job(t, Payload{
		Inbox:    "http://127.0.0.1:1/inbox",
		Activity: json.RawMessage(`{}`),
	}))
	require.Error(t, err)
	require.NotErrorIs(t, err, mkq.ErrUnrecoverable)
}

// An unknown key must make the delivery permanent end to end. 410 is
// the unambiguous signal; see TestRemoteSigner_NoSuchKeyClassification
// for why a bare 404 is not.
func TestRemoteSigner_DeliveryIsPermanentForAnUnknownKey(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusGone)
	}))
	t.Cleanup(srv.Close)

	signer, err := BuildSigner(SignerConfig{Type: "remote", URL: srv.URL})
	require.NoError(t, err)
	ex, err := New(Options{Signer: signer, AllowPrivateNetwork: true})
	require.NoError(t, err)

	_, err = ex.Execute(context.Background(), job(t, Payload{
		Inbox:    "http://127.0.0.1:1/inbox",
		Activity: json.RawMessage(`{}`),
		KeyID:    "https://local.example/users/nobody#main-key",
	}))
	require.ErrorIs(t, err, mkq.ErrUnrecoverable)
}

func TestRemoteSigner_MalformedResponses(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{"not json", "hello", "not JSON"},
		{"signature not base64", `{"keyId":"k","signature":"!!!"}`, "not base64"},
		{"empty signature", `{"keyId":"k","signature":""}`, "empty signature"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, tc.body)
			}))
			t.Cleanup(srv.Close)

			signer, err := BuildSigner(SignerConfig{Type: "remote", URL: srv.URL})
			require.NoError(t, err)
			_, err = signer.Sign(context.Background(), testKeyID, []byte("msg"))
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestRemoteSigner_ResponseWithoutAKeyIDNeedsOneInTheRequest(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(RemoteResponse{Signature: base64.StdEncoding.EncodeToString([]byte("sig"))})
	}))
	t.Cleanup(srv.Close)

	signer, err := BuildSigner(SignerConfig{Type: "remote", URL: srv.URL})
	require.NoError(t, err)

	// 要求した keyId があればそれを使う。
	sig, err := signer.Sign(context.Background(), testKeyID, []byte("msg"))
	require.NoError(t, err)
	require.Equal(t, testKeyID, sig.KeyID)

	// どちらも空なら Signature ヘッダに載せる名前が決まらない。
	_, err = signer.Sign(context.Background(), "", []byte("msg"))
	require.ErrorContains(t, err, "no key id")
}

func TestRemoteSigner_Timeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		<-release
	}))
	t.Cleanup(func() { close(release); srv.Close() })

	signer, err := BuildSigner(SignerConfig{
		Type: "remote", URL: srv.URL, Timeout: mkqdDuration(100 * time.Millisecond),
	})
	require.NoError(t, err)

	_, err = signer.Sign(context.Background(), testKeyID, []byte("msg"))
	require.Error(t, err)
	require.NotErrorIs(t, err, httpsig.ErrUnknownKey, "a timeout must leave the delivery retryable")
}

func TestRemoteSigner_ConfigErrors(t *testing.T) {
	_, err := BuildSigner(SignerConfig{Type: "remote"})
	require.ErrorContains(t, err, "url is required")

	_, err = BuildSigner(SignerConfig{Type: "remote", URL: "file:///tmp/sign"})
	require.ErrorContains(t, err, "must use http or https")

	_, err = BuildSigner(SignerConfig{Type: "remote", URL: "http:///sign"})
	require.ErrorContains(t, err, "has no host")
}

// The signing endpoint is the operator's own, so loopback is allowed by
// default — unlike the inbox, which comes from the job payload.
func TestRemoteSigner_ReachesLoopbackByDefault(t *testing.T) {
	signSrv, svc, _ := newSigningService(t, "")
	signer, err := BuildSigner(SignerConfig{Type: "remote", URL: signSrv.URL})
	require.NoError(t, err)

	_, err = signer.Sign(context.Background(), testKeyID, []byte("msg"))
	require.NoError(t, err)

	hits, _, _ := svc.snapshot()
	require.Equal(t, 1, hits)
}

func TestRemoteSigner_GuardCanBeTurnedOn(t *testing.T) {
	signSrv, _, _ := newSigningService(t, "")
	off := false
	signer, err := BuildSigner(SignerConfig{Type: "remote", URL: signSrv.URL, AllowPrivateNetwork: &off})
	require.NoError(t, err)

	_, err = signer.Sign(context.Background(), testKeyID, []byte("msg"))
	require.ErrorContains(t, err, "not a public address")
}

// mkqdDuration adapts a time.Duration to the config type.
func mkqdDuration(d time.Duration) mkqd.Duration { return mkqd.Duration(d) }

// Any 2xx satisfies the contract; a gateway that answers 201 or 202
// should not look like a signer failure.
func TestRemoteSigner_AcceptsAny2xx(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusCreated, http.StatusAccepted} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				_ = json.NewEncoder(w).Encode(RemoteResponse{
					KeyID: testKeyID, Signature: base64.StdEncoding.EncodeToString([]byte("sig")),
				})
			}))
			t.Cleanup(srv.Close)

			signer, err := BuildSigner(SignerConfig{Type: "remote", URL: srv.URL})
			require.NoError(t, err)
			sig, err := signer.Sign(context.Background(), testKeyID, []byte("msg"))
			require.NoError(t, err)
			require.Equal(t, testKeyID, sig.KeyID)
		})
	}
}

// Signing with a different key than the one asked for produces a
// signature the destination will always reject, so stop with a reason
// rather than feeding 401s to the retry loop.
func TestRemoteSigner_RejectsAKeyIDMismatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(RemoteResponse{
			KeyID:     "https://local.example/users/someone-else#main-key",
			Signature: base64.StdEncoding.EncodeToString([]byte("sig")),
		})
	}))
	t.Cleanup(srv.Close)

	signer, err := BuildSigner(SignerConfig{Type: "remote", URL: srv.URL})
	require.NoError(t, err)
	_, err = signer.Sign(context.Background(), testKeyID, []byte("msg"))
	require.ErrorContains(t, err, "asked for key")
}

// A blocked signing endpoint is a configuration mistake: it cannot
// start working without a config change, so fail fast instead of
// burning every attempt.
func TestRemoteSigner_BlockedEndpointIsPermanent(t *testing.T) {
	signSrv, _, _ := newSigningService(t, "")
	off := false
	signer, err := BuildSigner(SignerConfig{Type: "remote", URL: signSrv.URL, AllowPrivateNetwork: &off})
	require.NoError(t, err)

	ex, err := New(Options{Signer: signer, AllowPrivateNetwork: true})
	require.NoError(t, err)
	_, err = ex.Execute(context.Background(), job(t, Payload{
		Inbox:    "http://127.0.0.1:1/inbox",
		Activity: json.RawMessage(`{}`),
	}))
	require.ErrorIs(t, err, mkq.ErrUnrecoverable)
	require.ErrorContains(t, err, "not a public address")
}

// The key id ends up inside keyId="..." of the Signature header, and
// with a remote signer it is the first place untrusted data can reach
// it.
func TestRemoteSigner_RejectsAnUnusableKeyID(t *testing.T) {
	for _, bad := range []string{`has"quote`, "has\r\nnewline", strings.Repeat("x", 600)} {
		t.Run(bad[:min(12, len(bad))], func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(RemoteResponse{
					KeyID: bad, Signature: base64.StdEncoding.EncodeToString([]byte("sig")),
				})
			}))
			t.Cleanup(srv.Close)

			signer, err := BuildSigner(SignerConfig{Type: "remote", URL: srv.URL})
			require.NoError(t, err)

			req, err := http.NewRequest(http.MethodPost, "https://remote.example/inbox", nil)
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/activity+json")
			err = httpsig.SignRequest(context.Background(), req, nil, "", signer)
			require.Error(t, err)
			require.Empty(t, req.Header.Get("Signature"), "nothing may be signed with an unusable key id")
		})
	}
}

// An unauthenticated signing endpoint will sign anything for anyone who
// can reach it. That is defensible on loopback but too heavy to pass in
// silence, so it must be said out loud at startup.
func TestRemoteSigner_WarnsWhenUnauthenticated(t *testing.T) {
	signSrv, _, _ := newSigningService(t, "")

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	cfg := executorConfig(t, "type: activitypub_deliver\n"+
		"signer:\n  type: remote\n  url: \""+signSrv.URL+"\"\n")

	_, err := newExecutor(context.Background(), mkqd.BuildContext{Queue: "deliver", Logger: logger}, cfg)
	require.NoError(t, err)
	require.Contains(t, buf.String(), "remote signer has no secret")

	// secret があれば黙っている。
	buf.Reset()
	cfg = executorConfig(t, "type: activitypub_deliver\n"+
		"signer:\n  type: remote\n  url: \""+signSrv.URL+"\"\n  secret: s3cret\n")
	_, err = newExecutor(context.Background(), mkqd.BuildContext{Queue: "deliver", Logger: logger}, cfg)
	require.NoError(t, err)
	require.NotContains(t, buf.String(), "remote signer has no secret")
}

// "No such key" must be decided from the status, not from whether the
// body happened to arrive. A connection that drops mid-response would
// otherwise turn a permanent condition into an endless retry.
func TestRemoteSigner_NoSuchKeySurvivesATruncatedResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hj, ok := w.(http.Hijacker)
		require.True(t, ok)
		conn, buf, err := hj.Hijack()
		require.NoError(t, err)
		// Content-Length より短い本文を書いて切る。
		_, _ = buf.WriteString("HTTP/1.1 410 Gone\r\nContent-Length: 100\r\n\r\npartial")
		_ = buf.Flush()
		_ = conn.Close()
	}))
	t.Cleanup(srv.Close)

	signer, err := BuildSigner(SignerConfig{Type: "remote", URL: srv.URL})
	require.NoError(t, err)
	_, err = signer.Sign(context.Background(), testKeyID, []byte("msg"))
	require.ErrorIs(t, err, httpsig.ErrUnknownKey)
}
