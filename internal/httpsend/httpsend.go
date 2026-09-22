// Package httpsend holds the outbound-HTTP pieces every mkqd executor
// that talks to the network needs: a transport wired to the SSRF guard,
// a bounded body read, and a truncation that does not break UTF-8.
//
// これらは executor ごとに書くと必ずどれかが抜ける類のもの
// (応答サイズの上限、リダイレクト方針、プロキシの扱い) なので、一箇所に
// まとめて同じ既定を共有する。
package httpsend

import (
	"io"
	"net/http"
	"time"
	"unicode/utf8"

	"github.com/shiroha-a/mkqd/internal/safedial"
)

const (
	// DefaultMaxResponseBytes is how much of a response is kept.
	DefaultMaxResponseBytes = 64 << 10
	// drainLimit bounds the read-and-discard that lets a connection be
	// reused. A response with more left over than this loses its
	// keep-alive, which is the cheaper outcome.
	drainLimit = 8 << 10
)

// NewClient builds the HTTP client an executor sends with.
//
// **リダイレクトは追わない。** 宛先が payload 由来の場合、SSRF ガードを通った
// 後で private 宛へ飛ばされうる。署名付きの配送では、そもそも転送先が署名を
// 検証できる保証もない。3xx は応答として返して設定ミスとして扱う。
//
// **ガードが有効なときは環境変数のプロキシを使わない。** プロキシ経由だと
// 接続先はプロキシのアドレスになり、safedial が検査するのも同じくプロキシに
// なるので、本来の宛先に対する保護が丸ごと無効になる。
func NewClient(allowPrivate bool) *http.Client {
	t := &http.Transport{
		DialContext:         safedial.NewDialer(allowPrivate).DialContext,
		MaxIdleConns:        256,
		MaxIdleConnsPerHost: 64,
		IdleConnTimeout:     90 * time.Second,
	}
	if allowPrivate {
		t.Proxy = http.ProxyFromEnvironment
	}
	return &http.Client{
		Transport: t,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// ReadBody keeps at most max bytes of a response, discards a bounded
// remainder so the connection can be reused, and closes the body.
//
// **上限は保持量ではなく転送量に効かせる必要がある。** transport は gzip を
// 透過的に展開するので、残りを無制限に読み捨てると、小さな圧縮ストリームが
// 巨大に展開される応答で worker slot と帯域を占有されうる。
func ReadBody(resp *http.Response, max int64) ([]byte, error) {
	defer func() {
		_, _ = io.CopyN(io.Discard, resp.Body, drainLimit)
		_ = resp.Body.Close()
	}()
	if max <= 0 {
		max = DefaultMaxResponseBytes
	}
	return io.ReadAll(io.LimitReader(resp.Body, max))
}

// Truncate cuts s to at most limit bytes without splitting a rune, so a
// non-ASCII failure message stays valid UTF-8 in a dashboard.
func Truncate(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "... (truncated)"
}
