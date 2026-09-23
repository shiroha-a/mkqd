package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/shiroha-a/mkq"
)

// The inspect commands read a real Redis, as mkqd's library tests do.
func testRedisAddr() string {
	if a := os.Getenv("MKQD_TEST_REDIS_ADDR"); a != "" {
		return a
	}
	return "127.0.0.1:6379"
}

var prefixCounter atomic.Int64

// testEnv writes a config file for a prefix nothing else touches and
// hands back an mkq client on the same prefix for seeding.
type testEnv struct {
	prefix     string
	configPath string
	client     *mkq.Client
}

func newTestEnv(t *testing.T, configuredQueues ...string) *testEnv {
	t.Helper()
	prefix := fmt.Sprintf("mkqdcli:%d:%d", time.Now().UnixNano(), prefixCounter.Add(1))

	var b strings.Builder
	fmt.Fprintf(&b, "redis:\n  addrs: [%q]\n", testRedisAddr())
	fmt.Fprintf(&b, "key_prefix: %q\n", prefix)
	b.WriteString("log: { level: error, format: text }\n")
	b.WriteString("server: { addr: \"off\" }\n")
	if len(configuredQueues) > 0 {
		b.WriteString("queues:\n")
		for _, q := range configuredQueues {
			fmt.Fprintf(&b, "  - name: %q\n    executor: { type: log }\n", q)
		}
	}

	path := filepath.Join(t.TempDir(), "mkqd.yaml")
	require.NoError(t, os.WriteFile(path, []byte(b.String()), 0o600))

	client, err := mkq.NewClient(context.Background(), mkq.Config{
		Redis:     redis.UniversalOptions{Addrs: []string{testRedisAddr()}},
		KeyPrefix: prefix,
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = client.Close()
		flushPrefix(t, prefix)
	})
	return &testEnv{prefix: prefix, configPath: path, client: client}
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

// seed creates a queue and adds n jobs to it, returning their ids.
func (e *testEnv) seed(t *testing.T, queue string, n int) []string {
	t.Helper()
	q := mkq.Define[json.RawMessage](e.client, queue)
	ids := make([]string, 0, n)
	for i := range n {
		job, err := q.Add(context.Background(), json.RawMessage(fmt.Sprintf(`{"i":%d}`, i)))
		require.NoError(t, err)
		ids = append(ids, job.ID)
	}
	return ids
}

// keysUnder counts every Redis key under the test prefix, which is how
// the "inspecting must not create" assertions are made.
func (e *testEnv) keysUnder(t *testing.T) []string {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: testRedisAddr()})
	defer func() { _ = rdb.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var out []string
	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, e.prefix+"*", 500).Result()
		require.NoError(t, err)
		out = append(out, keys...)
		if next == 0 {
			return out
		}
		cursor = next
	}
}

// capture runs fn with os.Stdout redirected and returns what it wrote.
func capture(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	r, w, err := os.Pipe()
	require.NoError(t, err)

	orig := os.Stdout
	os.Stdout = w
	runErr := fn()
	os.Stdout = orig
	require.NoError(t, w.Close())

	var sb strings.Builder
	buf := make([]byte, 4096)
	for {
		n, readErr := r.Read(buf)
		sb.Write(buf[:n])
		if readErr != nil {
			break
		}
	}
	require.NoError(t, r.Close())
	return sb.String(), runErr
}

// TestInspect_UnknownQueueCreatesNothing is the acceptance criterion
// that shaped the design.
//
// **mkq の `Define` は書き込む。** `HSETNX meta version` と登録 SET への
// SADD を撃つので、素直に `Define(client, name)` すると打ち間違えた名前の
// キューが出来上がり、以降 `queues` にも出続ける。見るだけのコマンドが
// 対象を生んではいけないので、名前は `DiscoverQueues` の結果に対して
// 検証してから Define する。
func TestInspect_UnknownQueueCreatesNothing(t *testing.T) {
	env := newTestEnv(t)
	env.seed(t, "deliver", 1)

	before := env.keysUnder(t)

	for _, tc := range []struct {
		name string
		run  func() error
	}{
		{"counts", func() error { return cmdCounts([]string{"-c", env.configPath, "tyop"}) }},
		{"list", func() error { return cmdList([]string{"-c", env.configPath, "tyop", "wait"}) }},
		{"job", func() error { return cmdJob([]string{"-c", env.configPath, "tyop", "1"}) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := capture(t, tc.run)
			require.Error(t, err, "an unknown queue must be refused")
			require.Contains(t, err.Error(), "does not exist")
		})
	}

	require.ElementsMatch(t, before, env.keysUnder(t),
		"inspecting an unknown queue must not create any Redis key")
}

func TestInspect_QueuesMarksConfiguredAndStatus(t *testing.T) {
	env := newTestEnv(t, "deliver")
	env.seed(t, "deliver", 1)
	env.seed(t, "foreign", 1)

	out, err := capture(t, func() error {
		return cmdQueues([]string{"-c", env.configPath, "--json"})
	})
	require.NoError(t, err)

	var got queuesOutput
	require.NoError(t, json.Unmarshal([]byte(out), &got))
	require.Len(t, got.Queues, 2)

	byName := map[string]queueRow{}
	for _, r := range got.Queues {
		byName[r.Name] = r
	}
	require.True(t, byName["deliver"].Configured, "a queue in the config is marked configured")
	require.False(t, byName["foreign"].Configured,
		"a queue only present in Redis is not something this process works")
	require.False(t, byName["deliver"].Paused)
}

// A paused queue reports the same jobs under both wait and paused, by
// design (mkq v1.1.0). The table must not invite an operator to add
// the columns up, so it carries a status column and no paused column.
func TestInspect_CountsPausedOverlapsWait(t *testing.T) {
	env := newTestEnv(t, "deliver")
	env.seed(t, "deliver", 3)
	q := mkq.Define[json.RawMessage](env.client, "deliver")
	require.NoError(t, q.Pause(context.Background()))

	out, err := capture(t, func() error {
		return cmdCounts([]string{"-c", env.configPath, "--json", "deliver"})
	})
	require.NoError(t, err)

	var got countsOutput
	require.NoError(t, json.Unmarshal([]byte(out), &got))
	require.Len(t, got.Queues, 1)
	row := got.Queues[0]
	require.True(t, row.Paused)
	require.EqualValues(t, 3, row.Counts.Wait)
	require.EqualValues(t, 3, row.Counts.Paused,
		"paused restates wait while the queue is stopped")

	table, err := capture(t, func() error {
		return cmdCounts([]string{"-c", env.configPath, "deliver"})
	})
	require.NoError(t, err)
	require.NotContains(t, table, "PAUSED",
		"no paused column: adding it to WAIT would double count")
	require.Contains(t, table, "paused", "the status column must say so")
	require.Contains(t, table, "held back",
		"a paused queue needs the note explaining WAIT is not being worked")
}

// Paging boundaries: an empty bucket, a partial page, and a page that
// lands exactly on the end of the bucket.
func TestInspect_ListPagingBoundaries(t *testing.T) {
	env := newTestEnv(t, "deliver")
	ids := env.seed(t, "deliver", 4)

	listPage := func(t *testing.T, page, size int) []jobView {
		t.Helper()
		out, err := capture(t, func() error {
			return cmdList([]string{
				"-c", env.configPath, "--json",
				"-page", fmt.Sprint(page), "-size", fmt.Sprint(size),
				"deliver", "wait",
			})
		})
		require.NoError(t, err)
		var got listOutput
		require.NoError(t, json.Unmarshal([]byte(out), &got))
		return got.Jobs
	}

	require.Len(t, listPage(t, 0, 2), 2, "first page is full")
	require.Len(t, listPage(t, 1, 2), 2, "second page lands exactly on the end")
	require.Empty(t, listPage(t, 2, 2), "past the end is empty, not an error")
	require.Len(t, listPage(t, 0, 10), 4, "a page larger than the bucket returns all of it")
	require.Len(t, listPage(t, 0, 1), 1, "a single-job page")

	// 全件が昇順で、seed した順に並ぶこと。ページングが境界でジョブを
	// 落としたり重複させたりしていないことの確認でもある。
	var seen []string
	for page := 0; ; page++ {
		jobs := listPage(t, page, 2)
		if len(jobs) == 0 {
			break
		}
		for _, j := range jobs {
			seen = append(seen, j.ID)
		}
	}
	require.Equal(t, ids, seen, "paging must cover the bucket exactly once, oldest first")

	empty, err := capture(t, func() error {
		return cmdList([]string{"-c", env.configPath, "--json", "deliver", "failed"})
	})
	require.NoError(t, err)
	var got listOutput
	require.NoError(t, json.Unmarshal([]byte(empty), &got))
	require.NotNil(t, got.Jobs, "an empty bucket emits [] rather than null")
	require.Empty(t, got.Jobs)
}

func TestInspect_ListRejectsBadArguments(t *testing.T) {
	env := newTestEnv(t, "deliver")
	env.seed(t, "deliver", 1)

	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"unknown bucket", []string{"deliver", "nope"}, "unknown bucket"},
		{"negative page", []string{"-page", "-1", "deliver", "wait"}, "-page"},
		{"zero size", []string{"-size", "0", "deliver", "wait"}, "-size"},
		{"missing bucket", []string{"deliver"}, "usage"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := capture(t, func() error {
				return cmdList(append([]string{"-c", env.configPath}, tc.args...))
			})
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestInspect_JobShowsPayloadAndLogs(t *testing.T) {
	env := newTestEnv(t, "deliver")
	ids := env.seed(t, "deliver", 1)
	q := mkq.Define[json.RawMessage](env.client, "deliver")
	require.NoError(t, q.AppendJobLog(context.Background(), ids[0], "first line"))

	out, err := capture(t, func() error {
		return cmdJob([]string{"-c", env.configPath, "--json", "deliver", ids[0]})
	})
	require.NoError(t, err)

	var got jobOutput
	require.NoError(t, json.Unmarshal([]byte(out), &got))
	require.Equal(t, "deliver", got.Queue)
	require.Equal(t, ids[0], got.Job.ID)
	require.JSONEq(t, `{"i":0}`, string(got.Job.Data))
	require.Equal(t, []string{"first line"}, got.Logs.Lines)
	require.EqualValues(t, 1, got.Logs.Count)

	// 未処理のジョブに終了時刻は無い。ゼロ値の time をそのまま出すと
	// 1970 年として読まれるので、省かれていること。
	require.Nil(t, got.Job.State.FinishedOn)
	require.Nil(t, got.Job.State.ProcessedOn)

	table, err := capture(t, func() error {
		return cmdJob([]string{"-c", env.configPath, "deliver", ids[0]})
	})
	require.NoError(t, err)
	require.Contains(t, table, ids[0])
	require.Contains(t, table, `"i": 0`, "the payload is pretty-printed for a human")
	require.Contains(t, table, "first line")
	require.NotContains(t, table, "0001-01-01",
		"an unset timestamp must be omitted, not rendered as the zero time")
}

func TestInspect_TruncateIsRuneSafe(t *testing.T) {
	// 60 ルーンちょうどは切らない、61 ルーンで切る。バイト単位で切ると
	// マルチバイト文字が壊れて端末に化けた出力が出る。
	exact := strings.Repeat("あ", 60)
	require.Equal(t, exact, truncate(exact, 60))

	over := strings.Repeat("あ", 61)
	got := truncate(over, 60)
	require.Equal(t, strings.Repeat("あ", 60)+"...", got)
	require.True(t, len([]rune(got)) == 63)

	require.Equal(t, "a b", truncate("a\nb", 60), "newlines would break the table")
}
