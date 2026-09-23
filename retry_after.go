package mkqd

import (
	"errors"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/shiroha-a/mkq"
)

// maxRetryAfter bounds how long a server can push a job out.
//
// **相手の言い値をそのまま信じない。** `Retry-After: 86400` を返す実装は
// 実在し、それをそのまま通すと 1 回の失敗でジョブが 1 日止まる。試行回数の
// ぶんだけ掛かるので、5 回なら 5 日になる。
//
// 1 時間にしてあるのは、これが指数バックオフの上限としてよく使われる値で、
// 「相手の指定に従う」と「いつまでも待たない」の折り合いがつくため。これを
// 超える指定は切り詰めて、切り詰めたことをログに出す。
const maxRetryAfter = time.Hour

// maxDeltaSeconds is the largest delta-seconds that survives conversion
// to a Duration.
//
// **秒を Duration に直すと 10^9 倍される。** int64 に収まるのは約 292 年ぶん
// しかないので、`Retry-After: 9223372036854775807` のような値をそのまま掛け
// ると符号が反転する。負になれば上限判定をすり抜け、「上限で切り詰める」は
// ずが「即座に再試行」に化けて、防ぎたかった連打をこちらから始めてしまう。
const maxDeltaSeconds = int64(math.MaxInt64) / int64(time.Second)

// RetryAfterError carries a server's own instruction about when to come
// back, so the runtime can use it in place of the configured backoff.
//
// **executor から runtime へ遅延を伝える唯一の経路。** handler が返せるのは
// error だけなので、遅延は error に載せる。runtime は mkq の
// WithRetryDelayOverride に登録したフックで errors.As して取り出す。
//
// 独自の Executor からも使える。429 に限らず、相手が「いつ来い」を言って
// きたときに同じ形で伝えられる。
type RetryAfterError struct {
	// After is how long to wait before the next attempt. Negative
	// values are treated as zero by the runtime.
	After time.Duration
	// Err is the underlying failure. errors.Is / errors.As see through
	// to it, so mkq.ErrUnrecoverable inside still means "do not retry"
	// — the retry decision is made before the delay is.
	Err error
}

func (e *RetryAfterError) Error() string {
	if e.Err == nil {
		return "retry after " + e.After.String()
	}
	return e.Err.Error() + " (retry after " + e.After.String() + ")"
}

func (e *RetryAfterError) Unwrap() error { return e.Err }

// RetryAfter wraps err with the delay a server asked for.
//
// Returns err untouched when d is not positive: "retry after 0" carries
// no information the configured backoff does not already have, and
// wrapping it would override a considered curve with an immediate
// retry.
func RetryAfter(d time.Duration, err error) error {
	if d <= 0 {
		return err
	}
	return &RetryAfterError{After: d, Err: err}
}

// ParseRetryAfter reads an RFC 9110 `Retry-After` value relative to now.
//
// **2 つの形がある。** delta-seconds (`120`) と HTTP-date
// (`Wed, 21 Oct 2026 07:28:00 GMT`)。秒数だけを見る実装は後者を落とすので、
// 両方を受ける。
//
// Returns ok == false for an empty, unparseable, or past value — the
// caller then leaves the delay to the configured backoff rather than
// inventing one.
func ParseRetryAfter(v string, now time.Time) (time.Duration, bool) {
	if v == "" {
		return 0, false
	}
	secs, err := strconv.ParseInt(v, 10, 64)
	switch {
	case err == nil && secs < 0:
		return 0, false
	case err == nil && secs <= maxDeltaSeconds:
		return time.Duration(secs) * time.Second, true
	case err == nil, errors.Is(err, strconv.ErrRange) && v[0] != '-':
		// 桁が大きすぎるだけで、形としては delta-seconds。HTTP-date として
		// 読み直しても失敗するので、ここで「とても長い」として返す。
		// 呼び出し側が上限で切り詰めるため、捨てるより指定に近い。
		return time.Duration(math.MaxInt64), true
	}
	if t, err := http.ParseTime(v); err == nil {
		d := t.Sub(now)
		if d <= 0 {
			// 過去の時刻。「もう待たなくてよい」とも読めるが、相手が
			// 意図した遅延は分からないので設定どおりの backoff に委ねる。
			return 0, false
		}
		return d, true
	}
	return 0, false
}

// retryDelay is what the Runtime registers with mkq for every worker.
//
// Declining (ok == false) leaves the job to its configured backoff, so
// this only speaks up when a server actually said something.
func (rt *Runtime) retryDelay(bc mkq.BackoffContext) (time.Duration, bool) {
	var ra *RetryAfterError
	if !errors.As(bc.Err, &ra) {
		return 0, false
	}

	attrs := []any{
		"job_id", bc.JobID,
		"queue", bc.Name,
		"attempt", bc.AttemptsMade,
		"retry_after", ra.After.String(),
	}

	// **負の遅延は 0 にせず、黙って引き下がる。** mkq では負が「もう再試行
	// しない」の意味になるので通せない (BullMQ の -1 相当)。かといって 0 に
	// すると即時再試行になり、上限を設けて防いだはずの連打がここから漏れる。
	// executor 側の組み間違いなので、設定どおりの backoff に戻すのが被害が
	// 最も小さい。RetryAfter() は負を弾くが、構造体は直に組める。
	if ra.After < 0 {
		rt.log.Warn("ignoring a negative Retry-After", attrs...)
		return 0, false
	}

	d, capped := ra.After, false
	if d > maxRetryAfter {
		d, capped = maxRetryAfter, true
	}
	if capped {
		rt.log.Warn("honouring Retry-After, capped",
			append(attrs, "applied", d.String(), "cap", maxRetryAfter.String())...)
	} else {
		rt.log.Info("honouring Retry-After", attrs...)
	}
	return d, true
}
