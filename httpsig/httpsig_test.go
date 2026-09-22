package httpsig

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func testKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	return key
}

func newRequest(t *testing.T, method, rawurl string, body []byte) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, rawurl, strings.NewReader(string(body)))
	require.NoError(t, err)
	return req
}

func TestDigest_KnownVector(t *testing.T) {
	// sha256("") = e3b0c442... ; base64 の既知値で固定する。
	require.Equal(t, "SHA-256=47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=", Digest(nil))
	require.Equal(t, "SHA-256=uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek=", Digest([]byte("hello world")))
}

func TestSigningString_CanonicalForm(t *testing.T) {
	req := newRequest(t, http.MethodPost, "https://remote.example/users/alice/inbox", []byte("{}"))
	req.Header.Set("Date", "Tue, 23 Sep 2026 00:00:00 GMT")
	req.Header.Set("Digest", Digest([]byte("{}")))
	req.Header.Set("Content-Type", "application/activity+json")

	got, err := SigningString(req, DefaultHeaders)
	require.NoError(t, err)

	want := strings.Join([]string{
		"(request-target): post /users/alice/inbox",
		"host: remote.example",
		"date: Tue, 23 Sep 2026 00:00:00 GMT",
		"digest: " + Digest([]byte("{}")),
		"content-type: application/activity+json",
	}, "\n")
	require.Equal(t, want, got)
}

func TestSigningString_RequestTargetKeepsTheQuery(t *testing.T) {
	req := newRequest(t, http.MethodPost, "https://remote.example/inbox?shared=1", nil)
	got, err := SigningString(req, []string{"(request-target)"})
	require.NoError(t, err)
	require.Equal(t, "(request-target): post /inbox?shared=1", got)
}

func TestSigningString_EmptyPathBecomesSlash(t *testing.T) {
	req := newRequest(t, http.MethodPost, "https://remote.example", nil)
	got, err := SigningString(req, []string{"(request-target)"})
	require.NoError(t, err)
	require.Equal(t, "(request-target): post /", got)
}

// The signed request-target must be byte-identical to what net/http
// writes on the wire, or the receiver rebuilds a different signing
// string and the delivery is rejected. A URL ending in a bare "?" is
// the case that catches a hand-rolled version.
func TestSigningString_RequestTargetMatchesTheWire(t *testing.T) {
	for _, raw := range []string{
		"https://remote.example/inbox",
		"https://remote.example/inbox?",
		"https://remote.example/inbox?shared=1",
		"https://remote.example/users/a%20b/inbox",
		"https://remote.example",
	} {
		t.Run(raw, func(t *testing.T) {
			req := newRequest(t, http.MethodPost, raw, nil)
			got, err := SigningString(req, []string{"(request-target)"})
			require.NoError(t, err)
			require.Equal(t, "(request-target): post "+req.URL.RequestURI(), got)
		})
	}
}

func TestSigningString_RefusesAnEmptyCoveredHeader(t *testing.T) {
	// 空のヘッダに署名すると、受信側が別の値を見て検証に失敗する。
	req := newRequest(t, http.MethodPost, "https://remote.example/inbox", nil)
	_, err := SigningString(req, []string{"digest"})
	require.ErrorContains(t, err, "cannot be signed")
}

func TestSignRequest_ProducesAVerifiableSignature(t *testing.T) {
	key := testKey(t)
	signer, err := NewLocalSigner("https://local.example/users/me#main-key", key)
	require.NoError(t, err)

	body := []byte(`{"type":"Create"}`)
	req := newRequest(t, http.MethodPost, "https://remote.example/users/alice/inbox", body)
	req.Header.Set("Content-Type", "application/activity+json")

	require.NoError(t, SignRequest(context.Background(), req, body, "", signer))

	require.NotEmpty(t, req.Header.Get("Date"))
	require.Equal(t, Digest(body), req.Header.Get("Digest"))
	require.NoError(t, Verify(req, body, &key.PublicKey), "the signature must verify against the public key")
}

func TestSignRequest_SignatureHeaderShape(t *testing.T) {
	key := testKey(t)
	signer, err := NewLocalSigner("https://local.example/users/me#main-key", key)
	require.NoError(t, err)

	body := []byte("{}")
	req := newRequest(t, http.MethodPost, "https://remote.example/inbox", body)
	req.Header.Set("Content-Type", "application/activity+json")
	require.NoError(t, SignRequest(context.Background(), req, body, "", signer))

	parsed, err := ParseSignatureHeader(req.Header.Get("Signature"))
	require.NoError(t, err)
	require.Equal(t, "https://local.example/users/me#main-key", parsed.KeyID)
	require.Equal(t, AlgorithmRSASHA256, parsed.Algorithm)
	require.Equal(t, DefaultHeaders, parsed.Headers)
	require.NotEmpty(t, parsed.Signature)
}

func TestVerify_RejectsATamperedBody(t *testing.T) {
	key := testKey(t)
	signer, err := NewLocalSigner("k", key)
	require.NoError(t, err)

	body := []byte(`{"type":"Create"}`)
	req := newRequest(t, http.MethodPost, "https://remote.example/inbox", body)
	req.Header.Set("Content-Type", "application/activity+json")
	require.NoError(t, SignRequest(context.Background(), req, body, "", signer))

	// 署名済みヘッダはそのままに本文だけ差し替える。digest と本文の突き合わせが
	// なければ、これが通ってしまう。
	require.Error(t, Verify(req, []byte(`{"type":"Delete"}`), &key.PublicKey))
}

func TestVerify_RejectsATamperedDigestHeader(t *testing.T) {
	key := testKey(t)
	signer, err := NewLocalSigner("k", key)
	require.NoError(t, err)

	body := []byte(`{"type":"Create"}`)
	req := newRequest(t, http.MethodPost, "https://remote.example/inbox", body)
	req.Header.Set("Content-Type", "application/activity+json")
	require.NoError(t, SignRequest(context.Background(), req, body, "", signer))

	// digest は署名対象なので、ヘッダの差し替えは署名検証で落ちる。
	req.Header.Set("Digest", Digest([]byte(`{"type":"Delete"}`)))
	require.Error(t, Verify(req, body, &key.PublicKey))
}

// A signature that covers only `date` — the draft's default when
// `headers=` is absent — says nothing about the host, path or body, so
// it can be replayed anywhere. Verify must refuse it however valid the
// signature itself is.
func TestVerify_RejectsInsufficientCoverage(t *testing.T) {
	key := testKey(t)
	signer, err := NewLocalSigner("k", key)
	require.NoError(t, err)

	body := []byte("{}")
	req := newRequest(t, http.MethodPost, "https://remote.example/inbox", body)
	req.Header.Set("Date", "Tue, 23 Sep 2026 00:00:00 GMT")
	req.Header.Set("Digest", Digest(body))

	signingString, err := SigningString(req, []string{"date"})
	require.NoError(t, err)
	sig, err := signer.Sign(context.Background(), "", []byte(signingString))
	require.NoError(t, err)
	req.Header.Set("Signature", `keyId="k",algorithm="rsa-sha256",headers="date",signature="`+
		base64.StdEncoding.EncodeToString(sig.Bytes)+`"`)

	err = Verify(req, body, &key.PublicKey)
	require.ErrorContains(t, err, "does not cover")
}

func TestVerify_RejectsAnotherKey(t *testing.T) {
	key := testKey(t)
	other := testKey(t)
	signer, err := NewLocalSigner("k", key)
	require.NoError(t, err)

	body := []byte("{}")
	req := newRequest(t, http.MethodPost, "https://remote.example/inbox", body)
	req.Header.Set("Content-Type", "application/activity+json")
	require.NoError(t, SignRequest(context.Background(), req, body, "", signer))

	require.Error(t, Verify(req, body, &other.PublicKey))
}

func TestLocalSigner_RejectsAKeyItDoesNotHold(t *testing.T) {
	signer, err := NewLocalSigner("mine", testKey(t))
	require.NoError(t, err)

	_, err = signer.Sign(context.Background(), "someone-elses", []byte("x"))
	require.ErrorIs(t, err, ErrUnknownKey)

	// 空の keyID は「既定の鍵で」の意味。
	sig, err := signer.Sign(context.Background(), "", []byte("x"))
	require.NoError(t, err)
	require.Equal(t, "mine", sig.KeyID)
}

func TestNewLocalSigner_Validates(t *testing.T) {
	_, err := NewLocalSigner("", testKey(t))
	require.ErrorContains(t, err, "key id is required")

	_, err = NewLocalSigner("k", nil)
	require.ErrorContains(t, err, "private key is required")
}

func TestSignRequest_RequiresASigner(t *testing.T) {
	req := newRequest(t, http.MethodPost, "https://remote.example/inbox", nil)
	require.ErrorContains(t, SignRequest(context.Background(), req, nil, "", nil), "no signer")
}

func TestSignRequest_RejectsASignerWithoutAKeyID(t *testing.T) {
	req := newRequest(t, http.MethodPost, "https://remote.example/inbox", nil)
	req.Header.Set("Content-Type", "application/activity+json")
	err := SignRequest(context.Background(), req, nil, "", signerFunc(func(context.Context, string, []byte) (Signature, error) {
		return Signature{Bytes: []byte("sig")}, nil
	}))
	require.ErrorContains(t, err, "no key id")
}

type signerFunc func(context.Context, string, []byte) (Signature, error)

func (f signerFunc) Sign(ctx context.Context, keyID string, s []byte) (Signature, error) {
	return f(ctx, keyID, s)
}

func TestParsePrivateKey(t *testing.T) {
	key := testKey(t)

	pkcs1 := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
	got, err := ParsePrivateKey(pkcs1)
	require.NoError(t, err)
	require.True(t, got.Equal(key))

	der, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	pkcs8 := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	got, err = ParsePrivateKey(pkcs8)
	require.NoError(t, err)
	require.True(t, got.Equal(key))
}

func TestParsePrivateKey_Errors(t *testing.T) {
	_, err := ParsePrivateKey([]byte("not pem"))
	require.ErrorContains(t, err, "no PEM block")

	_, err = ParsePrivateKey(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte("x")}))
	require.ErrorContains(t, err, "unsupported PEM block")

	_, err = ParsePrivateKey(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: []byte("garbage")}))
	require.ErrorContains(t, err, "parse PKCS#1")
}

func TestParseSignatureHeader(t *testing.T) {
	// keyId の URL にカンマが入っていても壊れない。
	h := `keyId="https://ex.ample/u/a,b#main-key",algorithm="rsa-sha256",headers="(request-target) host date",signature="YWJj"`
	parsed, err := ParseSignatureHeader(h)
	require.NoError(t, err)
	require.Equal(t, "https://ex.ample/u/a,b#main-key", parsed.KeyID)
	require.Equal(t, []string{"(request-target)", "host", "date"}, parsed.Headers)
	require.Equal(t, "YWJj", parsed.Signature)
}

func TestParseSignatureHeader_Errors(t *testing.T) {
	_, err := ParseSignatureHeader("")
	require.ErrorContains(t, err, "no Signature header")

	_, err = ParseSignatureHeader(`algorithm="rsa-sha256"`)
	require.ErrorContains(t, err, "missing keyId or signature")
}

func TestParseSignatureHeader_DefaultsToDate(t *testing.T) {
	parsed, err := ParseSignatureHeader(`keyId="k",signature="YWJj"`)
	require.NoError(t, err)
	require.Equal(t, []string{"date"}, parsed.Headers)
}
