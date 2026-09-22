package httpsend

import (
	"net/http"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

const limit = 1 << 10

func TestTruncate_CutsOnARuneBoundary(t *testing.T) {
	// 日本語のエラーメッセージが壊れた UTF-8 になってダッシュボードに
	// 出ることがないようにする。
	s := strings.Repeat("あ", 500) // 1500 bytes
	got := Truncate(s, limit)
	require.True(t, utf8.ValidString(got), "truncated text must stay valid UTF-8")
	require.True(t, strings.HasSuffix(got, "... (truncated)"))
	require.LessOrEqual(t, len(got)-len("... (truncated)"), limit)
}

func TestTruncate_ShortStringIsUntouched(t *testing.T) {
	require.Equal(t, "ありがとう", Truncate("ありがとう", limit))
}

func TestTruncate_ExactLimitIsUntouched(t *testing.T) {
	s := strings.Repeat("a", limit)
	require.Equal(t, s, Truncate(s, limit))
}

func TestNewClient_ProxyOnlyWhenGuardIsOff(t *testing.T) {
	// プロキシ経由だと safedial が検査するのはプロキシのアドレスになり、
	// 本来の宛先への判定が働かなくなる。ガードが有効なら使わない。
	guarded := NewClient(false).Transport.(*http.Transport)
	require.Nil(t, guarded.Proxy)

	open := NewClient(true).Transport.(*http.Transport)
	require.NotNil(t, open.Proxy)
}
