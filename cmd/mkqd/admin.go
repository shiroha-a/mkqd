package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"slices"

	"github.com/shiroha-a/mkq"
)

// createQueue returns a handle for a queue that may not exist yet.
//
// **`Define` の副作用を許すのはここだけ。** 読み取り系は存在しない名前を
// 拒否する (inspector.queue)。enqueue はキューを生む行為そのものなので
// 作成を許すが、既定では拒否して `--create` を要求する。打ち間違えると
// 誰も処理しないキューにジョブが埋まるため。
func (in *inspector) createQueue(name string) *mkq.Queue[json.RawMessage] {
	return mkq.Define[json.RawMessage](in.client, name)
}

// mutationResult is the JSON shape every mutating command emits.
type mutationResult struct {
	Queue  string `json:"queue"`
	Action string `json:"action"`
	JobID  string `json:"job_id,omitempty"`
	OK     bool   `json:"ok"`
}

func emitMutation(asJSON bool, res mutationResult, human string) error {
	if asJSON {
		return emitJSON(res)
	}
	fmt.Println(human)
	return nil
}

// --- enqueue ----------------------------------------------------------

func cmdEnqueue(args []string) error {
	fs := flag.NewFlagSet("enqueue", flag.ContinueOnError)
	path := configFlag(fs)
	asJSON := jsonFlag(fs)
	create := fs.Bool("create", false, "create the queue if it does not exist yet")
	name := fs.String("name", "", "BullMQ job name (defaults to the queue name)")
	jobID := fs.String("id", "", "explicit job id instead of the queue's counter")
	delay := fs.Duration("delay", 0, "hold the job in delayed for this long")
	priority := fs.Uint("priority", 0, "BullMQ priority; higher runs first, 0 disables")
	attempts := fs.Int("attempts", 0, "total attempts before the job is failed for good")
	lifo := fs.Bool("lifo", false, "push to the head so this job is taken first")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return errors.New(`usage: mkqd enqueue [-c CONFIG] [--json] [--create] [flags] <queue> '<json payload>'`)
	}
	queueName, payload := fs.Arg(0), fs.Arg(1)

	if !json.Valid([]byte(payload)) {
		return fmt.Errorf("payload is not valid JSON: %s", truncate(payload, 60))
	}
	if *priority > uint(^uint32(0)) {
		return fmt.Errorf("-priority must fit in 32 bits")
	}

	ctx, cancel := context.WithTimeout(context.Background(), inspectTimeout)
	defer cancel()

	in, cleanup, err := openInspector(ctx, *path)
	if err != nil {
		return err
	}
	defer cleanup()

	var q *mkq.Queue[json.RawMessage]
	if slices.Contains(in.present, queueName) {
		q, err = in.queue(queueName)
		if err != nil {
			return err
		}
	} else {
		if !*create {
			return fmt.Errorf("queue %q does not exist in Redis (pass --create to make it)", queueName)
		}
		q = in.createQueue(queueName)
	}

	opts := []mkq.AddOption{}
	if *name != "" {
		opts = append(opts, mkq.WithJobName(*name))
	}
	if *jobID != "" {
		opts = append(opts, mkq.WithJobID(*jobID))
	}
	if *delay > 0 {
		opts = append(opts, mkq.WithDelay(*delay))
	}
	if *priority > 0 {
		opts = append(opts, mkq.WithPriority(uint32(*priority)))
	}
	if *attempts > 0 {
		opts = append(opts, mkq.WithAttempts(*attempts))
	}
	if *lifo {
		opts = append(opts, mkq.WithLifo(true))
	}

	job, err := q.Add(ctx, json.RawMessage(payload), opts...)
	if err != nil {
		return fmt.Errorf("queue %q: enqueue: %w", queueName, err)
	}

	return emitMutation(*asJSON,
		mutationResult{Queue: queueName, Action: "enqueue", JobID: job.ID, OK: true},
		fmt.Sprintf("enqueued %s job %s", queueName, job.ID))
}

// --- pause / resume ---------------------------------------------------

func cmdPause(args []string) error  { return pauseResumeCmd(args, "pause") }
func cmdResume(args []string) error { return pauseResumeCmd(args, "resume") }

func pauseResumeCmd(args []string, action string) error {
	fs := flag.NewFlagSet(action, flag.ContinueOnError)
	path := configFlag(fs)
	asJSON := jsonFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: mkqd %s [-c CONFIG] [--json] <queue>", action)
	}
	queueName := fs.Arg(0)

	ctx, cancel := context.WithTimeout(context.Background(), inspectTimeout)
	defer cancel()

	in, cleanup, err := openInspector(ctx, *path)
	if err != nil {
		return err
	}
	defer cleanup()

	q, err := in.queue(queueName)
	if err != nil {
		return err
	}

	if action == "pause" {
		err = q.Pause(ctx)
	} else {
		err = q.Resume(ctx)
	}
	if err != nil {
		return fmt.Errorf("queue %q: %s: %w", queueName, action, err)
	}

	return emitMutation(*asJSON,
		mutationResult{Queue: queueName, Action: action, OK: true},
		fmt.Sprintf("%sd %s", action, queueName))
}

// --- retry / promote / rm ---------------------------------------------

func cmdRetry(args []string) error {
	fs := flag.NewFlagSet("retry", flag.ContinueOnError)
	path := configFlag(fs)
	asJSON := jsonFlag(fs)
	from := fs.String("from", string(mkq.JobBucketFailed),
		"bucket the job is currently in: failed or completed")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return errors.New("usage: mkqd retry [-c CONFIG] [--json] [-from failed|completed] <queue> <jobId>")
	}
	source := mkq.JobBucket(*from)
	if source != mkq.JobBucketFailed && source != mkq.JobBucketCompleted {
		return fmt.Errorf("-from must be failed or completed, got %q", *from)
	}

	return withJob(fs, *path, *asJSON, "retry",
		func(ctx context.Context, q *mkq.Queue[json.RawMessage], jobID string) error {
			return q.RetryJob(ctx, jobID, mkq.WithRetryFromState(source))
		})
}

func cmdPromote(args []string) error {
	fs := flag.NewFlagSet("promote", flag.ContinueOnError)
	path := configFlag(fs)
	asJSON := jsonFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return errors.New("usage: mkqd promote [-c CONFIG] [--json] <queue> <jobId>")
	}
	return withJob(fs, *path, *asJSON, "promote",
		func(ctx context.Context, q *mkq.Queue[json.RawMessage], jobID string) error {
			return q.PromoteJob(ctx, jobID)
		})
}

func cmdRm(args []string) error {
	fs := flag.NewFlagSet("rm", flag.ContinueOnError)
	path := configFlag(fs)
	asJSON := jsonFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return errors.New("usage: mkqd rm [-c CONFIG] [--json] <queue> <jobId>")
	}
	// **rm に --yes は求めない。** ジョブ ID を明示した 1 件削除で誤爆の
	// 範囲が限定的であり、確認を挟むと運用の邪魔になる。バックログ全体を
	// 消す drain とは別扱い。
	return withJob(fs, *path, *asJSON, "rm",
		func(ctx context.Context, q *mkq.Queue[json.RawMessage], jobID string) error {
			return q.RemoveJob(ctx, jobID)
		})
}

// withJob runs a single-job mutation, sharing the queue lookup, the
// timeout and the result reporting.
func withJob(fs *flag.FlagSet, path string, asJSON bool, action string,
	fn func(context.Context, *mkq.Queue[json.RawMessage], string) error) error {
	queueName, jobID := fs.Arg(0), fs.Arg(1)

	ctx, cancel := context.WithTimeout(context.Background(), inspectTimeout)
	defer cancel()

	in, cleanup, err := openInspector(ctx, path)
	if err != nil {
		return err
	}
	defer cleanup()

	q, err := in.queue(queueName)
	if err != nil {
		return err
	}
	if err := fn(ctx, q, jobID); err != nil {
		return fmt.Errorf("queue %q job %q: %s: %w", queueName, jobID, action, err)
	}

	return emitMutation(asJSON,
		mutationResult{Queue: queueName, Action: action, JobID: jobID, OK: true},
		fmt.Sprintf("%s %s job %s", action, queueName, jobID))
}

// --- drain ------------------------------------------------------------

type drainResult struct {
	Queue string `json:"queue"`
	// Removed counts what actually disappeared, per bucket.
	Removed map[string]int64 `json:"removed"`
	Delayed bool             `json:"included_delayed"`
	OK      bool             `json:"ok"`
}

// cmdDrain deletes the pending backlog.
//
// **`mkqd run` のシャットダウンの drain とは逆のことをする。** あちらは
// in-flight を完走させて何も消さない。こちらは待機中のジョブを削除する。
// 名前は mkq のメソッド名と Roadmap に合わせたが、取り違えると復旧できない
// ので `--yes` を必須にし、実際に消えた件数を出す。
func cmdDrain(args []string) error {
	fs := flag.NewFlagSet("drain", flag.ContinueOnError)
	path := configFlag(fs)
	asJSON := jsonFlag(fs)
	yes := fs.Bool("yes", false, "required: confirm that queued jobs will be deleted")
	withDelayed := fs.Bool("delayed", false, "also delete delayed jobs (scheduler iterations are kept)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: mkqd drain [-c CONFIG] [--json] [-delayed] --yes <queue>")
	}
	queueName := fs.Arg(0)

	if !*yes {
		return fmt.Errorf("drain deletes the queued jobs in %q and cannot be undone; "+
			"pass --yes to proceed. This is not the graceful drain `mkqd run` does on shutdown, "+
			"which lets in-flight jobs finish and deletes nothing", queueName)
	}

	ctx, cancel := context.WithTimeout(context.Background(), inspectTimeout)
	defer cancel()

	in, cleanup, err := openInspector(ctx, *path)
	if err != nil {
		return err
	}
	defer cleanup()

	q, err := in.queue(queueName)
	if err != nil {
		return err
	}

	// 何を壊したかが見えないと --yes を求める意味が薄い。前後の差分を出す。
	before, err := q.Counts(ctx)
	if err != nil {
		return fmt.Errorf("queue %q: %w", queueName, err)
	}

	opts := []mkq.DrainOption{}
	if *withDelayed {
		opts = append(opts, mkq.WithDrainDelayed(true))
	}
	if err := q.DrainPending(ctx, opts...); err != nil {
		return fmt.Errorf("queue %q: drain: %w", queueName, err)
	}

	after, err := q.Counts(ctx)
	if err != nil {
		return fmt.Errorf("queue %q: %w", queueName, err)
	}

	// 差分を取るのは delayed のためでもある。`-delayed` を渡さなければ
	// delayed は残るので、drain 前の件数をそのまま出すと消していないものを
	// 消したと報告することになる。wait / prioritized は必ず空になるので
	// どちらの式でも同じ値になるが、同時に別の producer が積んだぶんを
	// 過大に数えないぶん差分のほうが正しい。
	removed := map[string]int64{}
	for name, delta := range map[string]int64{
		"wait":        before.Wait - after.Wait,
		"prioritized": before.Prioritized - after.Prioritized,
		"delayed":     before.Delayed - after.Delayed,
	} {
		if delta > 0 {
			removed[name] = delta
		}
	}

	if *asJSON {
		return emitJSON(drainResult{
			Queue: queueName, Removed: removed, Delayed: *withDelayed, OK: true,
		})
	}

	if len(removed) == 0 {
		fmt.Printf("drained %s: nothing was queued\n", queueName)
		return nil
	}
	fmt.Printf("drained %s:", queueName)
	for _, name := range []string{"wait", "prioritized", "delayed"} {
		if n, ok := removed[name]; ok {
			fmt.Printf(" %s=%d", name, n)
		}
	}
	fmt.Println()
	return nil
}
