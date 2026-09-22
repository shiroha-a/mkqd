package apdeliver

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/shiroha-a/mkqd"
	"github.com/shiroha-a/mkqd/httpsig"
	"github.com/shiroha-a/mkqd/internal/httpsend"
	"github.com/shiroha-a/mkqd/internal/safedial"
)

// remoteSigner asks the application to sign, instead of holding a key.
//
// **これが無いと、マルチユーザーの AP 実装は単体構成で使えない。** 鍵は
// ユーザごとに DB にあるので file / dir では届かず、かといって秘密鍵を
// mkqd に渡す理由もない。署名対象を送って署名を受け取れば、鍵はアプリの
// 中から一歩も出ない。
type remoteSigner struct {
	url       string
	secret    string
	timeout   time.Duration
	client    *http.Client
	userAgent string
}

// RemoteRequest is the body mkqd posts to the signing endpoint. It is
// the contract an application implements, so the field names are
// stable.
type RemoteRequest struct {
	// KeyID is the key to sign with. An empty string means "your
	// default key", which is what a single-actor deployment sends. The
	// field is always present, since applications validate it.
	KeyID string `json:"keyId"`
	// SigningString is the base64 of the bytes to sign.
	//
	// 署名文字列には改行が含まれる。base64 にしておけば JSON を通しても
	// 1 バイトも変わらない。
	SigningString string `json:"signingString"`
	// Algorithm names the expected signature algorithm.
	Algorithm string `json:"algorithm"`
}

// RemoteResponse is what the signing endpoint returns.
type RemoteResponse struct {
	// KeyID is the key that was used. Required when the request left it
	// empty, since the Signature header has to name a key.
	KeyID string `json:"keyId"`
	// Algorithm defaults to rsa-sha256 when empty.
	Algorithm string `json:"algorithm,omitempty"`
	// Signature is the base64 of the raw signature bytes.
	Signature string `json:"signature"`
}

const defaultRemoteTimeout = 5 * time.Second

func newRemoteSigner(cfg SignerConfig) (httpsig.Signer, error) {
	if cfg.URL == "" {
		return nil, errors.New("signer.url is required for a remote signer")
	}
	u, err := url.Parse(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("signer.url %q is not parseable: %v", cfg.URL, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("signer.url %q must use http or https", cfg.URL)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("signer.url %q has no host", cfg.URL)
	}

	timeout := cfg.Timeout.Duration()
	if timeout <= 0 {
		timeout = defaultRemoteTimeout
	}
	// 署名エンドポイントは運用者が決めるもので、loopback が通常構成。
	// payload 由来の宛先ではないのでガードは既定で外す。
	allowPrivate := true
	if cfg.AllowPrivateNetwork != nil {
		allowPrivate = *cfg.AllowPrivateNetwork
	}

	return &remoteSigner{
		url:       cfg.URL,
		secret:    cfg.Secret,
		timeout:   timeout,
		client:    httpsend.NewClient(allowPrivate),
		userAgent: mkqd.UserAgent(),
	}, nil
}

// isNoSuchKey reports whether a response means "this key does not
// exist" rather than "this URL does not exist".
//
// **素の 404 を恒久的失敗にしてはいけない。** signer.url の綴り違い、まだ
// 配備されていないルート、`/_mkqd/sign` を通さない reverse proxy —— どれも
// 404 を返すが、そこで全アクティビティを初回試行で捨ててしまうと、設定を
// 直しても手元には何も残らない。410 は「あったが無くなった」を明示する
// ステータスなのでそのまま信じ、404 はアプリ自身が JSON で答えたときだけ
// 鍵の不在として扱う。
func isNoSuchKey(resp *http.Response) bool {
	if resp.StatusCode == http.StatusGone {
		return true
	}
	if resp.StatusCode != http.StatusNotFound {
		return false
	}
	return strings.Contains(resp.Header.Get("Content-Type"), "json")
}

// Sign implements httpsig.Signer.
func (r *remoteSigner) Sign(ctx context.Context, keyID string, signingString []byte) (httpsig.Signature, error) {
	body, err := json.Marshal(RemoteRequest{
		KeyID:         keyID,
		SigningString: base64.StdEncoding.EncodeToString(signingString),
		Algorithm:     httpsig.AlgorithmRSASHA256,
	})
	if err != nil {
		return httpsig.Signature{}, fmt.Errorf("apdeliver: encode sign request: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.url, bytes.NewReader(body))
	if err != nil {
		return httpsig.Signature{}, fmt.Errorf("apdeliver: build sign request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", r.userAgent)
	ts := time.Now().Unix()
	req.Header.Set(httpsend.HeaderTimestamp, strconv.FormatInt(ts, 10))
	if r.secret != "" {
		req.Header.Set(httpsend.HeaderSignature, httpsend.SignHMAC(r.secret, ts, body))
	}

	resp, err := r.client.Do(req)
	if err != nil {
		if errors.Is(err, safedial.ErrBlocked) {
			// 設定の誤り。ガードを外すか URL を直すまで一度も成功しない。
			return httpsig.Signature{}, permanent("sign via %s: %v", r.url, err)
		}
		return httpsig.Signature{}, fmt.Errorf("apdeliver: sign via %s: %w", r.url, err)
	}

	// 「その鍵は無い」の判定は本文の読み取りに依存させない。接続が途中で
	// 切れただけで恒久的失敗が再試行に化けると、分類が運任せになる。
	if isNoSuchKey(resp) {
		return httpsig.Signature{}, fmt.Errorf("%w: %s said it has no key %q",
			httpsig.ErrUnknownKey, r.url, keyID)
	}

	payload, err := httpsend.ReadBody(resp, httpsend.DefaultMaxResponseBytes)
	if err != nil {
		return httpsig.Signature{}, fmt.Errorf("apdeliver: sign via %s: read response: %w", r.url, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// 署名側の一時的な不調で配送を捨てないよう、ここは再試行に倒す。
		return httpsig.Signature{}, fmt.Errorf("apdeliver: sign via %s: %s: %s",
			r.url, resp.Status, detail(payload))
	}

	var out RemoteResponse
	if err := json.Unmarshal(payload, &out); err != nil {
		return httpsig.Signature{}, fmt.Errorf("apdeliver: sign via %s: response is not JSON: %w", r.url, err)
	}
	sig, err := base64.StdEncoding.DecodeString(out.Signature)
	if err != nil {
		return httpsig.Signature{}, fmt.Errorf("apdeliver: sign via %s: signature is not base64: %w", r.url, err)
	}
	if len(sig) == 0 {
		return httpsig.Signature{}, fmt.Errorf("apdeliver: sign via %s: empty signature", r.url)
	}

	// 要求した鍵と違う鍵で署名されたら、その署名は宛先で必ず弾かれる。
	// 黙って進めて 401 を延々と食うより、ここで理由を出して止める。
	if keyID != "" && out.KeyID != "" && out.KeyID != keyID {
		return httpsig.Signature{}, fmt.Errorf("apdeliver: sign via %s: asked for key %q but the response used %q",
			r.url, keyID, out.KeyID)
	}
	resolved := out.KeyID
	if resolved == "" {
		resolved = keyID
	}
	if resolved == "" {
		return httpsig.Signature{}, fmt.Errorf("apdeliver: sign via %s: response has no key id", r.url)
	}
	algorithm := out.Algorithm
	if algorithm == "" {
		algorithm = httpsig.AlgorithmRSASHA256
	}
	return httpsig.Signature{KeyID: resolved, Algorithm: algorithm, Bytes: sig}, nil
}
