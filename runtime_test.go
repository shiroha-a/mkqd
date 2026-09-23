package mkqd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/shiroha-a/mkq"
)

// Integration tests talk to a real Redis, as mkq's own suite does: the
// Lua scripts are the thing under test as much as the Go code, so a
// fake would test the wrong system.
func testRedisAddr() string {
	if a := os.Getenv("MKQD_TEST_REDIS_ADDR"); a != "" {
		return a
	}
	return "127.0.0.1:6379"
}

var prefixCounter atomic.Int64

// testConfig returns a config isolated from other tests (and from any
// real queue) by a unique BullMQ key prefix.
func testConfig(t *testing.T, queues ...QueueConfig) Config {
	t.Helper()
	prefix := fmt.Sprintf("mkqdtest:%d:%d", time.Now().UnixNano(), prefixCounter.Add(1))
	cfg := Config{
		Redis:     RedisConfig{Addrs: []string{testRedisAddr()}},
		KeyPrefix: prefix,
		Log:       LogConfig{Level: "error", Format: "text"},
		Server:    ServerConfig{Addr: serverOff},
		Defaults:  QueueDefaults{Concurrency: 2},
		Queues:    queues,
	}
	cfg.applyDefaults()
	t.Cleanup(func() { flushPrefix(t, prefix) })
	return cfg
}

func flushPrefix(t *testing.T, prefix string) {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: testRedisAddr()})
	defer func() { _ = rdb.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, prefix+"*", 500).Result()
		if err != nil {
			return
		}
		if len(keys) > 0 {
			_ = rdb.Del(ctx, keys...).Err()
		}
		if next == 0 {
			return
		}
		cursor = next
	}
}

func newTestRuntime(t *testing.T, cfg Config) *Runtime {
	t.Helper()
	rt, err := New(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = rt.Shutdown(ctx)
	})
	return rt
}

// registerTestExecutor registers an executor under a name unique to the
// test, because RegisterExecutor panics on a duplicate by design.
func registerTestExecutor(t *testing.T, ex Executor) string {
	t.Helper()
	name := fmt.Sprintf("test-%d-%d", time.Now().UnixNano(), prefixCounter.Add(1))
	RegisterExecutor(name, func(context.Context, BuildContext, ExecutorConfig) (Executor, error) {
		return ex, nil
	})
	return name
}

type email struct {
	To string `json:"to"`
}

func TestRuntime_TypedHandlerProcessesJob(t *testing.T) {
	rt := newTestRuntime(t, testConfig(t))

	got := make(chan email, 1)
	require.NoError(t, Handle(rt, "email", func(_ context.Context, job *mkq.Job[email]) (any, error) {
		got <- job.Data
		return "sent", nil
	}, WithConcurrency(2)))

	require.NoError(t, rt.Start(context.Background()))

	_, err := Producer[email](rt, "email").Add(context.Background(), email{To: "alice@example.com"})
	require.NoError(t, err)

	select {
	case v := <-got:
		require.Equal(t, "alice@example.com", v.To)
	case <-time.After(10 * time.Second):
		t.Fatal("handler was never invoked")
	}
}

func TestRuntime_ConfiguredExecutorProcessesRawJob(t *testing.T) {
	seen := make(chan *Job, 1)
	typ := registerTestExecutor(t, ExecutorFunc(func(_ context.Context, job *Job) (any, error) {
		seen <- job
		return nil, nil
	}))

	cfg := testConfig(t, QueueConfig{
		Name:     "inbox",
		Executor: &ExecutorConfig{Type: typ},
	})
	rt := newTestRuntime(t, cfg)
	require.NoError(t, rt.Start(context.Background()))

	payload := json.RawMessage(`{"actor":"https://example.com/users/a","type":"Follow"}`)
	_, err := Producer[json.RawMessage](rt, "inbox").Add(context.Background(), payload)
	require.NoError(t, err)

	select {
	case job := <-seen:
		require.Equal(t, "inbox", job.Queue)
		require.Equal(t, "inbox", job.Name)
		require.Equal(t, 0, job.AttemptsMade)
		require.JSONEq(t, string(payload), string(job.Data))
	case <-time.After(10 * time.Second):
		t.Fatal("executor was never invoked")
	}
}

func TestRuntime_CodeHandlerWinsOverConfiguredExecutor(t *testing.T) {
	var executorRan atomic.Bool
	typ := registerTestExecutor(t, ExecutorFunc(func(context.Context, *Job) (any, error) {
		executorRan.Store(true)
		return nil, nil
	}))

	cfg := testConfig(t, QueueConfig{
		Name:     "email",
		Executor: &ExecutorConfig{Type: typ},
	})
	rt := newTestRuntime(t, cfg)

	handled := make(chan struct{}, 1)
	require.NoError(t, Handle(rt, "email", func(context.Context, *mkq.Job[email]) (any, error) {
		handled <- struct{}{}
		return nil, nil
	}))

	require.NoError(t, rt.Start(context.Background()))
	_, err := Producer[email](rt, "email").Add(context.Background(), email{To: "b@example.com"})
	require.NoError(t, err)

	select {
	case <-handled:
	case <-time.After(10 * time.Second):
		t.Fatal("code handler was never invoked")
	}
	require.False(t, executorRan.Load(), "the configured executor must not run when code claims the queue")
}

func TestRuntime_YAMLTuningOverridesCodeOptions(t *testing.T) {
	seven := 7
	lock := Duration(12 * time.Second)
	cfg := testConfig(t, QueueConfig{
		Name:         "email",
		Concurrency:  &seven,
		LockDuration: &lock,
	})
	rt := newTestRuntime(t, cfg)

	// コード側は concurrency=1 / lock=90s を指定するが、運用者の YAML が勝つ。
	require.NoError(t, Handle(rt, "email", func(context.Context, *mkq.Job[email]) (any, error) {
		return nil, nil
	}, WithConcurrency(1), WithLockDuration(90*time.Second)))

	require.NoError(t, rt.Start(context.Background()))

	concurrency, lockDuration, ok := rt.QueueTuning("email")
	require.True(t, ok)
	require.Equal(t, 7, concurrency)
	require.Equal(t, 12*time.Second, lockDuration)
}

func TestRuntime_CodeOptionsOverrideDefaults(t *testing.T) {
	cfg := testConfig(t)
	rt := newTestRuntime(t, cfg)

	require.NoError(t, Handle(rt, "email", func(context.Context, *mkq.Job[email]) (any, error) {
		return nil, nil
	}, WithConcurrency(5)))
	require.NoError(t, rt.Start(context.Background()))

	concurrency, lockDuration, ok := rt.QueueTuning("email")
	require.True(t, ok)
	require.Equal(t, 5, concurrency, "Go option must beat defaults.concurrency")
	require.Equal(t, 30*time.Second, lockDuration, "unset knobs fall back to defaults")
}

// Shutdown cancels the running handler and then waits for it to return.
// Shutdown drains: the worker stops dequeueing but the handler already
// running keeps its context and finishes on its own.
//
// **再送の有無がここで決まる。** cancel して落とすと掴んでいたジョブは lock
// TTL が切れるまで active に残り、stalled detection が回収して再試行する。
// 配送系では「相手には届いていたのにもう一度送る」がデプロイのたびに出る。
// ジョブが completed に入っていることまで見るのは、handler が戻ったことと
// finalise が Redis に届いたことが別物だから — Drain の godoc が警告している
// のはまさにそこで、巻き取りの前に接続を閉じると active に取り残される。
func TestRuntime_ShutdownLetsInFlightJobFinish(t *testing.T) {
	cfg := testConfig(t)
	rt := newTestRuntime(t, cfg)

	entered := make(chan struct{})
	var sawCancel, returned atomic.Bool
	require.NoError(t, Handle(rt, "slow", func(ctx context.Context, _ *mkq.Job[email]) (any, error) {
		close(entered)
		// Shutdown が走りきるより長く居座る。cancel されないことの確認なので、
		// ctx.Done() を待つのではなく素直に眠る。
		select {
		case <-ctx.Done():
			sawCancel.Store(true)
		case <-time.After(500 * time.Millisecond):
		}
		returned.Store(true)
		return nil, nil
	}, WithConcurrency(1)))

	require.NoError(t, rt.Start(context.Background()))
	job, err := Producer[email](rt, "slow").Add(context.Background(), email{To: "c@example.com"})
	require.NoError(t, err)

	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never started")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	require.NoError(t, rt.Shutdown(ctx))

	require.False(t, sawCancel.Load(), "a drained handler must not be cancelled")
	require.True(t, returned.Load(), "Shutdown must wait for the handler to return")
	require.GreaterOrEqual(t, time.Since(start), 400*time.Millisecond,
		"Shutdown returned before the handler finished")

	rdb := redis.NewClient(&redis.Options{Addr: testRedisAddr()})
	defer func() { _ = rdb.Close() }()
	base := cfg.KeyPrefix + ":slow:"

	score, err := rdb.ZScore(context.Background(), base+"completed", job.ID).Result()
	require.NoError(t, err, "the job must have been finalised, not left locked in active")
	require.Positive(t, score)

	active, err := rdb.LLen(context.Background(), base+"active").Result()
	require.NoError(t, err)
	require.Zero(t, active, "nothing may be left in active for stalled recovery to redeliver")
}

// When the drain budget runs out the old behaviour applies to that case
// only: the handler is cancelled and then awaited on a separate grace,
// so its finalisation still lands before the Redis connections close.
func TestRuntime_ShutdownCancelsWhenDrainBudgetExpires(t *testing.T) {
	cfg := testConfig(t)
	rt := newTestRuntime(t, cfg)
	rt.unwindGrace = 5 * time.Second

	entered := make(chan struct{})
	var sawCancel, returned atomic.Bool
	require.NoError(t, Handle(rt, "slow", func(ctx context.Context, _ *mkq.Job[email]) (any, error) {
		close(entered)
		select {
		case <-ctx.Done():
			sawCancel.Store(true)
		case <-time.After(10 * time.Second):
		}
		// 巻き取りを模す。Shutdown はここが終わるまで返ってはいけない。
		time.Sleep(300 * time.Millisecond)
		returned.Store(true)
		return nil, nil
	}, WithConcurrency(1)))

	require.NoError(t, rt.Start(context.Background()))
	_, err := Producer[email](rt, "slow").Add(context.Background(), email{To: "e@example.com"})
	require.NoError(t, err)

	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never started")
	}

	// drain には足りない猶予を渡す。handler は 10 秒居座るつもりなので
	// 必ず budget 切れの経路に入る。
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	require.NoError(t, rt.Shutdown(ctx),
		"the unwind grace absorbs the cancellation, so Shutdown succeeds")

	require.True(t, sawCancel.Load(), "an over-budget handler must be cancelled")
	require.True(t, returned.Load(), "Shutdown must still wait for it to unwind")
}

// A handler that ignores its context must not hang the process past
// shutdown_timeout: Shutdown reports the deadline instead of blocking.
func TestRuntime_ShutdownDeadlineIsReported(t *testing.T) {
	rt := newTestRuntime(t, testConfig(t))
	// 既定の 5 秒を待たない。ここで見たいのは deadline が伝わることだけ。
	rt.unwindGrace = 200 * time.Millisecond

	entered := make(chan struct{})
	release := make(chan struct{})
	require.NoError(t, Handle(rt, "stubborn", func(context.Context, *mkq.Job[email]) (any, error) {
		close(entered)
		<-release
		return nil, nil
	}, WithConcurrency(1)))

	require.NoError(t, rt.Start(context.Background()))
	_, err := Producer[email](rt, "stubborn").Add(context.Background(), email{To: "d@example.com"})
	require.NoError(t, err)

	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never started")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err = rt.Shutdown(ctx)
	close(release)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestRuntime_ShutdownIsIdempotent(t *testing.T) {
	rt := newTestRuntime(t, testConfig(t))
	require.NoError(t, Handle(rt, "email", func(context.Context, *mkq.Job[email]) (any, error) {
		return nil, nil
	}))
	require.NoError(t, rt.Start(context.Background()))

	ctx := context.Background()
	require.NoError(t, rt.Shutdown(ctx))
	require.NoError(t, rt.Shutdown(ctx))
}

func TestRuntime_RegisterRejectsDuplicateAndPostStart(t *testing.T) {
	rt := newTestRuntime(t, testConfig(t))
	h := func(context.Context, *mkq.Job[email]) (any, error) { return nil, nil }

	require.NoError(t, Handle(rt, "email", h))
	require.ErrorContains(t, Handle(rt, "email", h), "already registered")
	require.ErrorContains(t, Handle(rt, "", h), "must not be empty")
	require.ErrorContains(t, Handle[email](rt, "nilcheck", nil), "handler is nil")

	require.NoError(t, rt.Start(context.Background()))
	require.ErrorContains(t, Handle(rt, "later", h), "after Start")
}

func TestRuntime_StartRejectsEmptyRegistry(t *testing.T) {
	rt := newTestRuntime(t, testConfig(t))
	require.ErrorContains(t, rt.Start(context.Background()), "no queues registered")
}

func TestRuntime_StartRejectsUnknownExecutorType(t *testing.T) {
	cfg := testConfig(t, QueueConfig{
		Name:     "inbox",
		Executor: &ExecutorConfig{Type: "definitely-not-registered"},
	})
	rt := newTestRuntime(t, cfg)
	err := rt.Start(context.Background())
	require.ErrorContains(t, err, "unknown executor type")
	require.ErrorContains(t, err, "definitely-not-registered")
}

func TestRuntime_StartRejectsQueueWithoutExecutorOrHandler(t *testing.T) {
	cfg := testConfig(t, QueueConfig{Name: "orphan"})
	rt := newTestRuntime(t, cfg)
	require.ErrorContains(t, rt.Start(context.Background()), "no executor in the config and no handler registered")
}

func TestRuntime_StartTwiceIsAnError(t *testing.T) {
	rt := newTestRuntime(t, testConfig(t))
	require.NoError(t, Handle(rt, "email", func(context.Context, *mkq.Job[email]) (any, error) {
		return nil, nil
	}))
	require.NoError(t, rt.Start(context.Background()))
	require.ErrorContains(t, rt.Start(context.Background()), "Start called twice")
}

func TestHealthServer_Endpoints(t *testing.T) {
	cfg := testConfig(t)
	cfg.Server = ServerConfig{Addr: "127.0.0.1:0", Metrics: true}
	rt := newTestRuntime(t, cfg)

	require.NoError(t, Handle(rt, "email", func(context.Context, *mkq.Job[email]) (any, error) {
		return nil, nil
	}))
	require.NoError(t, rt.Start(context.Background()))

	base := "http://" + rt.ServerAddr()
	require.Equal(t, http.StatusOK, statusOf(t, base+"/healthz"))
	require.Equal(t, http.StatusOK, statusOf(t, base+"/readyz"))

	body, code := getBody(t, base+"/metrics")
	require.Equal(t, http.StatusOK, code)
	require.Contains(t, body, "go_goroutines", "the Prometheus registry must expose collectors")
}

func TestHealthServer_ReadyzFailsAfterShutdown(t *testing.T) {
	cfg := testConfig(t)
	cfg.Server = ServerConfig{Addr: "127.0.0.1:0"}
	rt := newTestRuntime(t, cfg)

	require.NoError(t, Handle(rt, "email", func(context.Context, *mkq.Job[email]) (any, error) {
		return nil, nil
	}))
	require.NoError(t, rt.Start(context.Background()))

	base := "http://" + rt.ServerAddr()
	require.Equal(t, http.StatusOK, statusOf(t, base+"/readyz"))

	// Shutdown はリスナも落とすので、readyz の 503 は直接観測できない。
	// isServing のフラグだけを確認する。
	require.True(t, rt.isServing())
	require.NoError(t, rt.Shutdown(context.Background()))
	require.False(t, rt.isServing())
}

func TestHealthServer_DisabledWhenAddrIsOff(t *testing.T) {
	rt := newTestRuntime(t, testConfig(t))
	require.NoError(t, Handle(rt, "email", func(context.Context, *mkq.Job[email]) (any, error) {
		return nil, nil
	}))
	require.NoError(t, rt.Start(context.Background()))
	require.Equal(t, "", rt.ServerAddr())
}

func TestRuntime_CheckBuildsExecutorsWithoutStartingWorkers(t *testing.T) {
	var built atomic.Int32
	name := fmt.Sprintf("test-check-%d", prefixCounter.Add(1))
	RegisterExecutor(name, func(context.Context, BuildContext, ExecutorConfig) (Executor, error) {
		built.Add(1)
		return ExecutorFunc(func(context.Context, *Job) (any, error) { return nil, nil }), nil
	})

	cfg := testConfig(t, QueueConfig{Name: "inbox", Executor: &ExecutorConfig{Type: name}})
	rt := newTestRuntime(t, cfg)

	require.NoError(t, rt.Check(context.Background()))
	require.Equal(t, int32(1), built.Load())
	require.Equal(t, []string{"inbox"}, rt.Queues())
	require.Equal(t, "", rt.ServerAddr(), "Check must not start the health listener")
}

func TestJob_UpdateProgressAndLogReachRedis(t *testing.T) {
	done := make(chan error, 1)
	typ := registerTestExecutor(t, ExecutorFunc(func(ctx context.Context, job *Job) (any, error) {
		if err := job.UpdateProgress(ctx, 42); err != nil {
			done <- err
			return nil, err
		}
		if err := job.Log(ctx, "halfway"); err != nil {
			done <- err
			return nil, err
		}
		done <- nil
		return nil, nil
	}))

	cfg := testConfig(t, QueueConfig{Name: "prog", Executor: &ExecutorConfig{Type: typ}})
	rt := newTestRuntime(t, cfg)
	require.NoError(t, rt.Start(context.Background()))

	q := Producer[json.RawMessage](rt, "prog")
	_, err := q.Add(context.Background(), json.RawMessage(`{"x":1}`))
	require.NoError(t, err)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("executor was never invoked")
	}
}

func TestJob_MutationsRequireABackingJob(t *testing.T) {
	j := &Job{Queue: "q", ID: "1"}
	require.ErrorIs(t, j.UpdateProgress(context.Background(), 1), mkq.ErrJobDetached)
	require.ErrorIs(t, j.Log(context.Background(), "x"), mkq.ErrJobDetached)
}

func statusOf(t *testing.T, url string) int {
	t.Helper()
	_, code := getBody(t, url)
	return code
}

func getBody(t *testing.T, url string) (string, int) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(b), resp.StatusCode
}
