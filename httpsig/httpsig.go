// Package httpsig signs outbound HTTP requests the way the fediverse
// does: the draft-cavage-http-signatures dialect, with a SHA-256
// Digest header over the body.
//
// RFC 9421 supersedes that draft, but ActivityPub implementations
// overwhelmingly still verify the draft form, and a request they cannot
// verify is a request that does not arrive. mkqd follows deployment,
// not the standards track.
//
// # Signing without holding the key
//
// The Signer interface is deliberately "sign these bytes", not "give me
// the private key". A multi-user ActivityPub server keeps per-actor
// keys in its database, and the natural way to reach them is to ask the
// application to sign — which means the key never has to leave it. A
// signer backed by a local PEM file is then just one implementation
// among others rather than the shape everything else has to fit.
package httpsig

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// AlgorithmRSASHA256 is the only algorithm the fediverse reliably
// verifies today.
const AlgorithmRSASHA256 = "rsa-sha256"

// DefaultHeaders is the covered-header list mkqd signs. It is what
// Mastodon, Misskey and the other large implementations expect.
var DefaultHeaders = []string{"(request-target)", "host", "date", "digest", "content-type"}

// Signature is what a Signer produces.
type Signature struct {
	// KeyID is the key actually used, which a Signer may resolve from
	// an empty request (a single-actor deployment has a default).
	KeyID string
	// Algorithm names the signature algorithm for the header.
	Algorithm string
	// Bytes is the raw signature, base64-encoded by this package.
	Bytes []byte
}

// Signer signs the canonical signing string of a request.
//
// keyID is the key the caller asked for; an empty keyID asks the signer
// for its default. Implementations must be safe for concurrent use.
type Signer interface {
	Sign(ctx context.Context, keyID string, signingString []byte) (Signature, error)
}

// ErrUnknownKey reports that a signer does not hold the requested key.
// A delivery for a key that does not exist will not start existing on a
// retry, so callers treat it as permanent.
var ErrUnknownKey = errors.New("httpsig: unknown key")

// SignRequest fills in Date, Digest and Signature on req.
//
// body must be exactly the bytes the request will send: the Digest
// header commits to them, and a receiver that recomputes the digest
// over different bytes rejects the delivery.
//
// The caller sets Content-Type first — DefaultHeaders covers it, and a
// header that is empty at signing time cannot be signed. Host is not
// set here: net/http sends it from req.Host or req.URL.Host, and the
// signing string reads it from the same place so the two cannot drift.
func SignRequest(ctx context.Context, req *http.Request, body []byte, keyID string, signer Signer) error {
	if signer == nil {
		return errors.New("httpsig: no signer")
	}
	if req.URL == nil {
		return errors.New("httpsig: request has no URL")
	}

	if req.Header.Get("Date") == "" {
		req.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	}
	// Host はヘッダマップではなく req.Host / URL.Host から送られるので、
	// ここでは設定しない。SigningString も同じところから取る — 署名した
	// 値と相手が受け取る値がずれてはいけない。
	req.Header.Set("Digest", Digest(body))

	signingString, err := SigningString(req, DefaultHeaders)
	if err != nil {
		return err
	}

	sig, err := signer.Sign(ctx, keyID, []byte(signingString))
	if err != nil {
		return err
	}
	if sig.KeyID == "" {
		return errors.New("httpsig: signer returned no key id")
	}
	algorithm := sig.Algorithm
	if algorithm == "" {
		algorithm = AlgorithmRSASHA256
	}

	req.Header.Set("Signature", fmt.Sprintf(
		`keyId="%s",algorithm="%s",headers="%s",signature="%s"`,
		sig.KeyID, algorithm, strings.Join(DefaultHeaders, " "),
		base64.StdEncoding.EncodeToString(sig.Bytes),
	))
	return nil
}

// Digest returns the value of the Digest header for a body.
func Digest(body []byte) string {
	sum := sha256.Sum256(body)
	return "SHA-256=" + base64.StdEncoding.EncodeToString(sum[:])
}

// SigningString builds the canonical string the signature covers: one
// "name: value" line per covered header, lowercase names, joined by
// newlines with no trailing newline.
//
// `(request-target)` is the pseudo-header "<lowercase method> <path>",
// where the path includes the query string.
func SigningString(req *http.Request, headers []string) (string, error) {
	if len(headers) == 0 {
		return "", errors.New("httpsig: no headers to sign")
	}
	var b strings.Builder
	for i, name := range headers {
		name = strings.ToLower(name)
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(name)
		b.WriteString(": ")

		if name == "(request-target)" {
			b.WriteString(requestTarget(req))
			continue
		}
		// Host は req.Header ではなく req.Host / URL に載るので、
		// net/http の扱いに合わせて個別に取る。
		if name == "host" {
			host := req.Host
			if host == "" {
				host = req.URL.Host
			}
			b.WriteString(host)
			continue
		}
		v := req.Header.Get(name)
		if v == "" {
			return "", fmt.Errorf("httpsig: header %q is empty and cannot be signed", name)
		}
		b.WriteString(v)
	}
	return b.String(), nil
}

// requestTarget is "<lowercase method> <request-uri>".
//
// **URL.RequestURI() を使うのは net/http がワイヤに書くのと同じ形だから。**
// 自前で EscapedPath + "?" + RawQuery を組み立てると、ForceQuery (末尾の "?"
// だけが付く URL) や Opaque で送信形とずれる。受信側はワイヤの形から署名対象を
// 組み立て直すので、ずれた瞬間に検証が落ちて配送が恒久的失敗として捨てられる。
func requestTarget(req *http.Request) string {
	return strings.ToLower(req.Method) + " " + req.URL.RequestURI()
}

// LocalSigner signs with an in-process RSA private key. It is the
// implementation a single-actor deployment and the tests use.
type LocalSigner struct {
	keyID string
	key   *rsa.PrivateKey
}

// NewLocalSigner wraps an RSA private key under a key id.
func NewLocalSigner(keyID string, key *rsa.PrivateKey) (*LocalSigner, error) {
	if keyID == "" {
		return nil, errors.New("httpsig: key id is required")
	}
	if key == nil {
		return nil, errors.New("httpsig: private key is required")
	}
	return &LocalSigner{keyID: keyID, key: key}, nil
}

// KeyID reports the key id this signer advertises.
func (s *LocalSigner) KeyID() string { return s.keyID }

// Sign implements Signer. A non-empty keyID that does not match this
// signer's is an ErrUnknownKey rather than a silent substitution.
func (s *LocalSigner) Sign(_ context.Context, keyID string, signingString []byte) (Signature, error) {
	if keyID != "" && keyID != s.keyID {
		return Signature{}, fmt.Errorf("%w: %s", ErrUnknownKey, keyID)
	}
	sum := sha256.Sum256(signingString)
	sig, err := rsa.SignPKCS1v15(rand.Reader, s.key, crypto.SHA256, sum[:])
	if err != nil {
		return Signature{}, fmt.Errorf("httpsig: sign: %w", err)
	}
	return Signature{KeyID: s.keyID, Algorithm: AlgorithmRSASHA256, Bytes: sig}, nil
}

// ParsePrivateKey reads an RSA private key from PEM, accepting both
// PKCS#1 ("RSA PRIVATE KEY") and PKCS#8 ("PRIVATE KEY") blocks, which
// is what actually turns up in ActivityPub deployments.
func ParsePrivateKey(pemBytes []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("httpsig: no PEM block found")
	}
	switch block.Type {
	case "RSA PRIVATE KEY":
		key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("httpsig: parse PKCS#1 key: %w", err)
		}
		return key, nil
	case "PRIVATE KEY":
		parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("httpsig: parse PKCS#8 key: %w", err)
		}
		key, ok := parsed.(*rsa.PrivateKey)
		if !ok {
			return nil, fmt.Errorf("httpsig: PKCS#8 key is %T, want RSA", parsed)
		}
		return key, nil
	default:
		return nil, fmt.Errorf("httpsig: unsupported PEM block %q", block.Type)
	}
}

// RequiredHeaders are the covered headers a signature must list to be
// accepted. Without them a valid signature says nothing useful: one
// that covers only `date` can be replayed against any host, path and
// body, which is the draft's default when `headers=` is absent.
var RequiredHeaders = []string{"(request-target)", "host", "date", "digest"}

// Verify checks a signature produced by SignRequest against a public
// key, and checks that the body is the one the signature commits to.
//
// **署名の検証だけでは足りない。** 署名が covered header を十分に含んで
// いなければ別の宛先へ使い回せるし、digest が本文と一致していなければ
// 署名済みのヘッダはそのままに本文だけ差し替えられる。ここで3つとも
// 確かめる。
func Verify(req *http.Request, body []byte, pub *rsa.PublicKey) error {
	parsed, err := ParseSignatureHeader(req.Header.Get("Signature"))
	if err != nil {
		return err
	}
	if err := requireCoverage(parsed.Headers); err != nil {
		return err
	}
	if got, want := req.Header.Get("Digest"), Digest(body); got != want {
		return fmt.Errorf("httpsig: digest %q does not match the body", got)
	}
	signingString, err := SigningString(req, parsed.Headers)
	if err != nil {
		return err
	}
	sum := sha256.Sum256([]byte(signingString))
	sig, err := base64.StdEncoding.DecodeString(parsed.Signature)
	if err != nil {
		return fmt.Errorf("httpsig: signature is not base64: %w", err)
	}
	return rsa.VerifyPKCS1v15(pub, crypto.SHA256, sum[:], sig)
}

func requireCoverage(covered []string) error {
	have := make(map[string]struct{}, len(covered))
	for _, h := range covered {
		have[strings.ToLower(h)] = struct{}{}
	}
	for _, want := range RequiredHeaders {
		if _, ok := have[want]; !ok {
			return fmt.Errorf("httpsig: signature does not cover %q", want)
		}
	}
	return nil
}

// ParsedSignature is the decomposed Signature header.
type ParsedSignature struct {
	KeyID     string
	Algorithm string
	Headers   []string
	Signature string
}

// ParseSignatureHeader splits a Signature header into its parameters.
func ParseSignatureHeader(v string) (ParsedSignature, error) {
	if v == "" {
		return ParsedSignature{}, errors.New("httpsig: no Signature header")
	}
	var out ParsedSignature
	for _, part := range splitParams(v) {
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		value = strings.Trim(strings.TrimSpace(value), `"`)
		switch strings.TrimSpace(key) {
		case "keyId":
			out.KeyID = value
		case "algorithm":
			out.Algorithm = value
		case "headers":
			out.Headers = strings.Fields(value)
		case "signature":
			out.Signature = value
		}
	}
	if out.KeyID == "" || out.Signature == "" {
		return ParsedSignature{}, errors.New("httpsig: Signature header is missing keyId or signature")
	}
	if len(out.Headers) == 0 {
		// draft の既定は date のみ。
		out.Headers = []string{"date"}
	}
	return out, nil
}

// splitParams splits on commas that are not inside a quoted value —
// base64 signatures never contain a comma, but a keyId URL might carry
// one and the cost of being careful is one pass.
func splitParams(v string) []string {
	var parts []string
	var cur strings.Builder
	inQuotes := false
	for i := 0; i < len(v); i++ {
		switch c := v[i]; {
		case c == '"':
			inQuotes = !inQuotes
			cur.WriteByte(c)
		case c == ',' && !inQuotes:
			parts = append(parts, cur.String())
			cur.Reset()
		default:
			cur.WriteByte(c)
		}
	}
	if cur.Len() > 0 {
		parts = append(parts, cur.String())
	}
	return parts
}
