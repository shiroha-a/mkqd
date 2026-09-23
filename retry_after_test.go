package mkqd

import (
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/shiroha-a/mkq"
)

// TestParseRetryAfter_BothWireForms pins RFC 9110's two shapes.
//
// **秒数だけを見る実装は HTTP-date を落とす。** どちらも仕様どおりの
// Retry-After なので、片方しか読めないと相手によって効いたり効かなかったり
// する — いちばん分かりにくい壊れ方。
func TestParseRetryAfter_BothWireForms(t *testing.T) {
	now := time.Date(2026, 10, 21, 7, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		name string
		in   string
		want time.Duration
		ok   bool
	}{
		{"delta-seconds", "120", 2 * time.Minute, true},
		{"delta-seconds zero", "0", 0, true},
		{"HTTP-date in the future", "Wed, 21 Oct 2026 07:28:00 GMT", 28 * time.Minute, true},
		{"HTTP-date in the past", "Wed, 21 Oct 2026 06:00:00 GMT", 0, false},
		{"HTTP-date exactly now", "Wed, 21 Oct 2026 07:00:00 GMT", 0, false},
		{"empty", "", 0, false},
		{"garbage", "soon please", 0, false},
		{"negative seconds", "-30", 0, false},
		{"float is not delta-seconds", "1.5", 0, false},

		// **秒 -> Duration は 10^9 倍で、int64 は約 292 年しか持たない。**
		// 素直に掛けると符号が反転し、上限判定をすり抜けて 0 (即時再試行)
		// に化ける。防ぎたかった連打をこちらから始めることになる。
		{"int64 max seconds", "9223372036854775807", time.Duration(math.MaxInt64), true},
		{"seconds past the shift limit", "1000000000000000000", time.Duration(math.MaxInt64), true},
		{"more digits than int64 holds", "99999999999999999999", time.Duration(math.MaxInt64), true},
		{"a huge negative is still refused", "-99999999999999999999", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ParseRetryAfter(tc.in, now)
			assert.Equal(t, tc.ok, ok)
			if tc.ok {
				assert.Equal(t, tc.want, got)
			}
		})
	}
}

// RetryAfter must not wrap a non-positive delay: "retry after 0" says
// nothing the configured backoff does not already know, and wrapping it
// would replace a considered curve with an immediate retry.
func TestRetryAfter_IgnoresNonPositive(t *testing.T) {
	base := errors.New("boom")

	for _, d := range []time.Duration{0, -time.Second} {
		got := RetryAfter(d, base)
		assert.Same(t, base, got, "delay %v must pass the error through untouched", d)

		var ra *RetryAfterError
		assert.False(t, errors.As(got, &ra))
	}

	wrapped := RetryAfter(time.Minute, base)
	var ra *RetryAfterError
	require.True(t, errors.As(wrapped, &ra))
	assert.Equal(t, time.Minute, ra.After)
}

// The wrapper must stay transparent to errors.Is / errors.As, or an
// ErrUnrecoverable buried inside would stop meaning "do not retry".
func TestRetryAfterError_UnwrapsToTheCause(t *testing.T) {
	inner := fmt.Errorf("gone: %w", mkq.ErrUnrecoverable)
	got := RetryAfter(time.Minute, inner)

	assert.ErrorIs(t, got, mkq.ErrUnrecoverable,
		"a permanent failure must stay permanent even when a delay rides along")
	assert.Contains(t, got.Error(), "gone")
	assert.Contains(t, got.Error(), "1m0s")
}

// TestRuntime_RetryDelay covers the hook the Runtime hands to mkq.
func TestRuntime_RetryDelay(t *testing.T) {
	rt := newTestRuntime(t, testConfig(t))

	t.Run("declines when the error carries no hint", func(t *testing.T) {
		d, ok := rt.retryDelay(mkq.BackoffContext{Err: errors.New("boom")})
		assert.False(t, ok, "declining leaves the job to its configured backoff")
		assert.Zero(t, d)
	})

	t.Run("declines on a nil error", func(t *testing.T) {
		_, ok := rt.retryDelay(mkq.BackoffContext{})
		assert.False(t, ok)
	})

	t.Run("uses the hint", func(t *testing.T) {
		err := RetryAfter(90*time.Second, errors.New("429"))
		d, ok := rt.retryDelay(mkq.BackoffContext{Err: err})
		require.True(t, ok)
		assert.Equal(t, 90*time.Second, d)
	})

	t.Run("sees through wrapping", func(t *testing.T) {
		err := fmt.Errorf("deliver: %w", RetryAfter(30*time.Second, errors.New("429")))
		d, ok := rt.retryDelay(mkq.BackoffContext{Err: err})
		require.True(t, ok)
		assert.Equal(t, 30*time.Second, d)
	})

	// **相手の言い値をそのまま信じない。** `Retry-After: 86400` を返す実装は
	// 実在し、通すと 1 回の失敗でジョブが 1 日止まる。試行回数ぶん掛かる。
	t.Run("caps an unreasonable hint", func(t *testing.T) {
		err := RetryAfter(24*time.Hour, errors.New("429"))
		d, ok := rt.retryDelay(mkq.BackoffContext{Err: err})
		require.True(t, ok)
		assert.Equal(t, maxRetryAfter, d, "24h must be cut down to the cap")
		assert.Less(t, d, 24*time.Hour)
	})

	// 桁あふれ由来の巨大な値も、経路の最後で上限に落ちること。
	t.Run("an overflowing hint still lands on the cap", func(t *testing.T) {
		d, ok := ParseRetryAfter("9223372036854775807", time.Now())
		require.True(t, ok)
		d, ok = rt.retryDelay(mkq.BackoffContext{Err: RetryAfter(d, errors.New("429"))})
		require.True(t, ok)
		assert.Equal(t, maxRetryAfter, d)
		assert.Positive(t, d, "the cap must not be reached by wrapping through zero")
	})

	// **負は 0 にせず、黙って引き下がる。** RetryAfter() は負を弾くが、
	// 構造体は直に組める。mkq では負が「諦める」の意味になるので通せず、
	// 0 にすると即時再試行になって上限の意味が無くなる。設定どおりの
	// backoff に戻すのがいちばん被害が小さい。
	t.Run("a hand-built negative declines instead of retrying now", func(t *testing.T) {
		err := &RetryAfterError{After: -time.Hour, Err: errors.New("429")}
		d, ok := rt.retryDelay(mkq.BackoffContext{Err: err})
		assert.False(t, ok, "a negative must not become an immediate retry")
		assert.Zero(t, d)
	})
}
