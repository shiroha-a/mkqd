package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/shiroha-a/mkq"
)

func (e *testEnv) rdb(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: testRedisAddr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func (e *testEnv) q(queue string) *mkq.Queue[json.RawMessage] {
	return mkq.Define[json.RawMessage](e.client, queue)
}

// TestAdmin_DrainRefusesWithoutYes is the guard that matters most.
//
// **`mkqd drain` は `mkqd run` のシャットダウンの drain と逆のことをする。**
// あちらは in-flight を完走させて何も消さない。こちらは待機中のジョブを
// 削除する。シャットダウンのログを読んだ運用者が「穏当な待避」と誤解して
// バックログを消す事故が起こり得るので、`--yes` 無しでは何もしない。
func TestAdmin_DrainRefusesWithoutYes(t *testing.T) {
	env := newTestEnv(t, "deliver")
	env.seed(t, "deliver", 3)

	before := env.keysUnder(t)

	out, err := capture(t, func() error {
		return cmdDrain([]string{"-c", env.configPath, "deliver"})
	})
	require.Error(t, err, "drain without --yes must refuse")
	require.Contains(t, err.Error(), "--yes")
	require.Contains(t, err.Error(), "not the graceful drain",
		"the refusal must say which drain this is, or the warning does not help")
	require.Empty(t, out)

	require.ElementsMatch(t, before, env.keysUnder(t),
		"a refused drain must not delete anything")

	counts, err := env.q("deliver").Counts(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 3, counts.Wait)
}

func TestAdmin_DrainRemovesPendingAndReportsCounts(t *testing.T) {
	env := newTestEnv(t, "deliver")
	env.seed(t, "deliver", 3)
	q := env.q("deliver")

	ctx := context.Background()
	_, err := q.Add(ctx, json.RawMessage(`{"p":1}`), mkq.WithPriority(5))
	require.NoError(t, err)
	_, err = q.Add(ctx, json.RawMessage(`{"d":1}`), mkq.WithDelay(time.Hour))
	require.NoError(t, err)

	out, err := capture(t, func() error {
		return cmdDrain([]string{"-c", env.configPath, "--json", "--yes", "deliver"})
	})
	require.NoError(t, err)

	var got drainResult
	require.NoError(t, json.Unmarshal([]byte(out), &got))
	require.True(t, got.OK)
	require.False(t, got.Delayed)
	require.EqualValues(t, 3, got.Removed["wait"])
	require.EqualValues(t, 1, got.Removed["prioritized"])
	require.NotContains(t, got.Removed, "delayed",
		"delayed is left alone unless -delayed is passed")

	after, err := q.Counts(ctx)
	require.NoError(t, err)
	require.Zero(t, after.Wait)
	require.Zero(t, after.Prioritized)
	require.EqualValues(t, 1, after.Delayed, "the delayed job must survive")
}

// The reported counts have to be what actually disappeared; a hard-coded
// or pre-drain number would make --yes theatre.
func TestAdmin_DrainCountsMatchReality(t *testing.T) {
	env := newTestEnv(t, "deliver")
	env.seed(t, "deliver", 7)

	out, err := capture(t, func() error {
		return cmdDrain([]string{"-c", env.configPath, "--json", "--yes", "deliver"})
	})
	require.NoError(t, err)

	var got drainResult
	require.NoError(t, json.Unmarshal([]byte(out), &got))
	require.EqualValues(t, 7, got.Removed["wait"])

	table, err := capture(t, func() error {
		return cmdDrain([]string{"-c", env.configPath, "--yes", "deliver"})
	})
	require.NoError(t, err)
	require.Contains(t, table, "nothing was queued",
		"a second drain has nothing left to remove and must say so")
}

// drain is "cancel everything pending", not a wipe: finished jobs stay.
func TestAdmin_DrainLeavesTerminalJobsAlone(t *testing.T) {
	env := newTestEnv(t, "deliver")
	q := env.q("deliver")
	ctx := context.Background()

	done, err := q.Add(ctx, json.RawMessage(`{"done":1}`))
	require.NoError(t, err)

	// completed へ入れるため、ワーカーを 1 本だけ回して待つ。
	worker, err := mkq.Process(q, func(context.Context, *mkq.Job[json.RawMessage]) (any, error) {
		return nil, nil
	}, mkq.WithConcurrency(1), mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)

	deadline := time.Now().Add(10 * time.Second)
	for {
		c, err := q.Counts(ctx, mkq.JobBucketCompleted)
		require.NoError(t, err)
		if c.Completed == 1 {
			break
		}
		require.False(t, time.Now().After(deadline), "job never completed")
		time.Sleep(20 * time.Millisecond)
	}
	require.NoError(t, worker.Stop(ctx))

	env.seed(t, "deliver", 2)

	_, err = capture(t, func() error {
		return cmdDrain([]string{"-c", env.configPath, "--yes", "deliver"})
	})
	require.NoError(t, err)

	after, err := q.Counts(ctx)
	require.NoError(t, err)
	require.Zero(t, after.Wait, "pending jobs are gone")
	require.EqualValues(t, 1, after.Completed, "a completed job is not pending and must survive")

	state, err := env.rdb(t).HGet(ctx, env.prefix+":deliver:"+done.ID, "finishedOn").Result()
	require.NoError(t, err, "the completed job HASH must still exist")
	require.NotEmpty(t, state)
}

// -delayed widens the drain, but scheduler iterations are protected by
// mkq's lua and must survive even then.
func TestAdmin_DrainDelayedKeepsSchedulerJobs(t *testing.T) {
	env := newTestEnv(t, "tick")
	q := env.q("tick")
	ctx := context.Background()

	// **最初の iteration は wait に入る。** `UpsertScheduleEvery` は 1 本目を
	// 即時に積むので、delayed に居る scheduler ジョブを作るには開始を先に
	// ずらす。時間で回るスケジュールの定常状態はこちら。
	require.NoError(t, q.UpsertScheduleEvery(ctx, "ticker", time.Hour,
		json.RawMessage(`{"t":1}`), mkq.WithScheduleStartDate(time.Now().Add(time.Hour))))
	_, err := q.Add(ctx, json.RawMessage(`{"ad-hoc":1}`), mkq.WithDelay(time.Hour))
	require.NoError(t, err)

	before, err := q.Counts(ctx, mkq.JobBucketDelayed)
	require.NoError(t, err)
	require.EqualValues(t, 2, before.Delayed, "one scheduler iteration plus one ad-hoc job")

	_, err = capture(t, func() error {
		return cmdDrain([]string{"-c", env.configPath, "--yes", "-delayed", "tick"})
	})
	require.NoError(t, err)

	after, err := q.Counts(ctx, mkq.JobBucketDelayed)
	require.NoError(t, err)
	require.EqualValues(t, 1, after.Delayed,
		"the ad-hoc delayed job goes, the scheduler iteration stays")

	ids, err := env.rdb(t).ZRange(ctx, env.prefix+":tick:delayed", 0, -1).Result()
	require.NoError(t, err)
	require.Len(t, ids, 1)
	require.True(t, strings.HasPrefix(ids[0], "repeat:ticker:"),
		"the survivor must be the scheduler iteration, got %q", ids[0])
}

// enqueue is the one command allowed to create a queue, and only when
// asked: a typo otherwise buries a job in a queue nothing works.
func TestAdmin_EnqueueRefusesUnknownQueueByDefault(t *testing.T) {
	env := newTestEnv(t, "deliver")
	env.seed(t, "deliver", 1)

	before := env.keysUnder(t)

	_, err := capture(t, func() error {
		return cmdEnqueue([]string{"-c", env.configPath, "delivr", `{"x":1}`})
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "--create")

	require.ElementsMatch(t, before, env.keysUnder(t),
		"a refused enqueue must not create the queue")
}

func TestAdmin_EnqueueCreatesWhenAsked(t *testing.T) {
	env := newTestEnv(t, "deliver")
	env.seed(t, "deliver", 1)

	out, err := capture(t, func() error {
		return cmdEnqueue([]string{"-c", env.configPath, "--json", "--create", "fresh", `{"x":1}`})
	})
	require.NoError(t, err)

	var got mutationResult
	require.NoError(t, json.Unmarshal([]byte(out), &got))
	require.True(t, got.OK)
	require.Equal(t, "fresh", got.Queue)
	require.NotEmpty(t, got.JobID)

	counts, err := env.q("fresh").Counts(context.Background(), mkq.JobBucketWait)
	require.NoError(t, err)
	require.EqualValues(t, 1, counts.Wait)
}

func TestAdmin_EnqueueRejectsNonJSONPayload(t *testing.T) {
	env := newTestEnv(t, "deliver")
	env.seed(t, "deliver", 1)

	before := env.keysUnder(t)
	_, err := capture(t, func() error {
		return cmdEnqueue([]string{"-c", env.configPath, "deliver", `not json`})
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "not valid JSON")
	require.ElementsMatch(t, before, env.keysUnder(t))
}

func TestAdmin_EnqueueOptionsReachRedis(t *testing.T) {
	env := newTestEnv(t, "deliver")
	env.seed(t, "deliver", 1)
	ctx := context.Background()

	_, err := capture(t, func() error {
		return cmdEnqueue([]string{
			"-c", env.configPath, "--json",
			"-id", "custom-1", "-name", "special", "-delay", "1h", "-attempts", "5",
			"deliver", `{"x":1}`,
		})
	})
	require.NoError(t, err)

	job, state, err := env.q("deliver").Get(ctx, "custom-1")
	require.NoError(t, err)
	require.Equal(t, "custom-1", job.ID, "-id must be honoured")
	require.Equal(t, "special", job.Name, "-name must be honoured")
	require.EqualValues(t, time.Hour.Milliseconds(), state.Delay, "-delay must be honoured")

	var opts map[string]any
	require.NoError(t, json.Unmarshal(state.Opts, &opts))
	require.EqualValues(t, 5, opts["attempts"], "-attempts must be honoured")

	score, err := env.rdb(t).ZScore(ctx, env.prefix+":deliver:delayed", "custom-1").Result()
	require.NoError(t, err, "a delayed job belongs in the delayed ZSET, not wait")
	require.Positive(t, score)
}

func TestAdmin_PauseResume(t *testing.T) {
	env := newTestEnv(t, "deliver")
	env.seed(t, "deliver", 1)
	ctx := context.Background()
	q := env.q("deliver")

	_, err := capture(t, func() error {
		return cmdPause([]string{"-c", env.configPath, "deliver"})
	})
	require.NoError(t, err)
	paused, err := q.IsPaused(ctx)
	require.NoError(t, err)
	require.True(t, paused)

	_, err = capture(t, func() error {
		return cmdResume([]string{"-c", env.configPath, "deliver"})
	})
	require.NoError(t, err)
	paused, err = q.IsPaused(ctx)
	require.NoError(t, err)
	require.False(t, paused)
}

func TestAdmin_PromoteMovesDelayedToWait(t *testing.T) {
	env := newTestEnv(t, "deliver")
	q := env.q("deliver")
	ctx := context.Background()

	job, err := q.Add(ctx, json.RawMessage(`{"x":1}`), mkq.WithDelay(time.Hour))
	require.NoError(t, err)

	_, err = capture(t, func() error {
		return cmdPromote([]string{"-c", env.configPath, "deliver", job.ID})
	})
	require.NoError(t, err)

	counts, err := q.Counts(ctx)
	require.NoError(t, err)
	require.Zero(t, counts.Delayed)
	require.EqualValues(t, 1, counts.Wait, "a promoted job is ready to run now")
}

func TestAdmin_RmDeletesTheJob(t *testing.T) {
	env := newTestEnv(t, "deliver")
	ids := env.seed(t, "deliver", 2)
	ctx := context.Background()

	_, err := capture(t, func() error {
		return cmdRm([]string{"-c", env.configPath, "deliver", ids[0]})
	})
	require.NoError(t, err)

	exists, err := env.rdb(t).Exists(ctx, env.prefix+":deliver:"+ids[0]).Result()
	require.NoError(t, err)
	require.Zero(t, exists, "the job HASH must be gone")

	counts, err := env.q("deliver").Counts(ctx, mkq.JobBucketWait)
	require.NoError(t, err)
	require.EqualValues(t, 1, counts.Wait, "the other job stays")
}

func TestAdmin_RetryMovesFailedBackToWait(t *testing.T) {
	env := newTestEnv(t, "deliver")
	q := env.q("deliver")
	ctx := context.Background()

	job, err := q.Add(ctx, json.RawMessage(`{"x":1}`))
	require.NoError(t, err)

	worker, err := mkq.Process(q, func(context.Context, *mkq.Job[json.RawMessage]) (any, error) {
		return nil, mkq.ErrUnrecoverable
	}, mkq.WithConcurrency(1), mkq.WithIdlePollInterval(20*time.Millisecond))
	require.NoError(t, err)

	deadline := time.Now().Add(10 * time.Second)
	for {
		c, err := q.Counts(ctx, mkq.JobBucketFailed)
		require.NoError(t, err)
		if c.Failed == 1 {
			break
		}
		require.False(t, time.Now().After(deadline), "job never failed")
		time.Sleep(20 * time.Millisecond)
	}
	require.NoError(t, worker.Stop(ctx))

	_, err = capture(t, func() error {
		return cmdRetry([]string{"-c", env.configPath, "deliver", job.ID})
	})
	require.NoError(t, err)

	counts, err := q.Counts(ctx)
	require.NoError(t, err)
	require.Zero(t, counts.Failed)
	require.EqualValues(t, 1, counts.Wait, "a retried job goes back to wait")
}

func TestAdmin_RejectsBadArguments(t *testing.T) {
	env := newTestEnv(t, "deliver")
	env.seed(t, "deliver", 1)

	for _, tc := range []struct {
		name string
		run  func() error
		want string
	}{
		{"retry from an impossible bucket", func() error {
			return cmdRetry([]string{"-c", env.configPath, "-from", "wait", "deliver", "1"})
		}, "-from must be"},
		{"pause with no queue", func() error {
			return cmdPause([]string{"-c", env.configPath})
		}, "usage"},
		{"rm on an unknown queue", func() error {
			return cmdRm([]string{"-c", env.configPath, "tyop", "1"})
		}, "does not exist"},
		{"promote on an unknown queue", func() error {
			return cmdPromote([]string{"-c", env.configPath, "tyop", "1"})
		}, "does not exist"},
		{"drain on an unknown queue", func() error {
			return cmdDrain([]string{"-c", env.configPath, "--yes", "tyop"})
		}, "does not exist"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := capture(t, tc.run)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.want)
		})
	}
}
