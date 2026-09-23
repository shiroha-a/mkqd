package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/shiroha-a/mkq"
	"github.com/shiroha-a/mkqd"
)

// inspectTimeout bounds every read-only command. Long enough for a
// SCAN across a large keyspace, short enough that a wedged Redis
// fails the command instead of hanging a shell.
const inspectTimeout = 30 * time.Second

// buckets is the set of job states a queue can be listed by, in the
// order a dashboard reads them: what is coming, what is running, what
// is done.
var buckets = []mkq.JobBucket{
	mkq.JobBucketWait,
	mkq.JobBucketActive,
	mkq.JobBucketDelayed,
	mkq.JobBucketPrioritized,
	mkq.JobBucketCompleted,
	mkq.JobBucketFailed,
	mkq.JobBucketPaused,
}

func bucketNames() []string {
	names := make([]string, len(buckets))
	for i, b := range buckets {
		names[i] = string(b)
	}
	return names
}

// inspector holds what every read-only command needs: a client that
// never started a worker, and the queues that actually exist.
type inspector struct {
	rt        *mkqd.Runtime
	client    *mkq.Client
	present   []string
	confNames []string
}

// openInspector builds a runtime without starting workers, the way
// `mkqd check` does, and enumerates the queues present in Redis.
//
// **列挙を先にやるのは `Define` が書き込むから。** mkq の Define は
// `HSETNX meta version` と登録 SET への SADD を撃つので、打ち間違えた名前を
// そのまま渡すとその名前のキューが出来上がり、以降ずっと一覧に出続ける。
// 見るだけのコマンドが対象を生んではいけない。
func openInspector(ctx context.Context, path string) (*inspector, func(), error) {
	rt, err := mkqd.NewFromFile(ctx, path)
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), inspectTimeout)
		defer cancel()
		_ = rt.Shutdown(shutdownCtx)
	}

	client := rt.Client()
	present, err := client.DiscoverQueues(ctx)
	if err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("discover queues: %w", err)
	}

	// **`rt.Queues()` はここではまだ空。** 設定のキューを実際に組み立てる
	// のは Start (と Check) で、inspect はワーカーを立てない。設定ファイルを
	// 直接読めば副作用なく同じことが分かる。
	confNames := make([]string, 0, len(rt.Config().Queues))
	for _, q := range rt.Config().Queues {
		confNames = append(confNames, q.Name)
	}

	return &inspector{
		rt:        rt,
		client:    client,
		present:   present,
		confNames: confNames,
	}, cleanup, nil
}

// queue returns a handle for name, refusing names that are not already
// in Redis so that Define's write side effect cannot create one.
func (in *inspector) queue(name string) (*mkq.Queue[json.RawMessage], error) {
	if !slices.Contains(in.present, name) {
		return nil, fmt.Errorf("queue %q does not exist in Redis (run `mkqd queues` to list them)", name)
	}
	return mkq.Define[json.RawMessage](in.client, name), nil
}

// resolveQueues turns the positional arguments into queue names,
// defaulting to every queue present when none were given.
//
// Names are not checked here; queue() is the single gate that refuses
// one that does not exist, and every caller goes through it.
func (in *inspector) resolveQueues(args []string) []string {
	if len(args) == 0 {
		return in.present
	}
	return args
}

func emitJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// --- queues -----------------------------------------------------------

type queuesOutput struct {
	Queues []queueRow `json:"queues"`
}

type queueRow struct {
	Name string `json:"name"`
	// Configured reports whether this process would work the queue.
	// A queue can exist in Redis without appearing here: another
	// deployment, or a BullMQ worker in another language, owns it.
	Configured bool `json:"configured"`
	Paused     bool `json:"paused"`
}

func cmdQueues(args []string) error {
	fs := flag.NewFlagSet("queues", flag.ContinueOnError)
	path := configFlag(fs)
	asJSON := jsonFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("usage: mkqd queues [-c CONFIG] [--json]")
	}

	ctx, cancel := context.WithTimeout(context.Background(), inspectTimeout)
	defer cancel()

	in, cleanup, err := openInspector(ctx, *path)
	if err != nil {
		return err
	}
	defer cleanup()

	out := queuesOutput{Queues: make([]queueRow, 0, len(in.present))}
	for _, name := range in.present {
		q, err := in.queue(name)
		if err != nil {
			return err
		}
		paused, err := q.IsPaused(ctx)
		if err != nil {
			return fmt.Errorf("queue %q: %w", name, err)
		}
		out.Queues = append(out.Queues, queueRow{
			Name:       name,
			Configured: slices.Contains(in.confNames, name),
			Paused:     paused,
		})
	}

	if *asJSON {
		return emitJSON(out)
	}

	if len(out.Queues) == 0 {
		fmt.Println("no queues found under this key prefix")
		return nil
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "QUEUE\tCONFIGURED\tSTATUS")
	for _, r := range out.Queues {
		fmt.Fprintf(w, "%s\t%s\t%s\n", r.Name, yesNo(r.Configured), status(r.Paused))
	}
	return w.Flush()
}

// --- counts -----------------------------------------------------------

type countsOutput struct {
	Queues []countsRow `json:"queues"`
}

type countsRow struct {
	Name   string `json:"name"`
	Paused bool   `json:"paused"`
	Counts counts `json:"counts"`
}

// counts mirrors mkq.QueueCounts.
//
// **`paused` は `wait` の再掲であって別勘定ではない。** mkq v1.1.0 以降
// `QueueCounts.Paused` は「今なにが止められているか」を返すので、停止中の
// キューでは同じジョブが `wait` と `paused` の両方に数えられる。合計を取ると
// 二重になる。人間向けの表では `paused` 列を出さず、STATUS で示している。
type counts struct {
	Wait        int64 `json:"wait"`
	Active      int64 `json:"active"`
	Delayed     int64 `json:"delayed"`
	Prioritized int64 `json:"prioritized"`
	Completed   int64 `json:"completed"`
	Failed      int64 `json:"failed"`
	Paused      int64 `json:"paused"`
}

func cmdCounts(args []string) error {
	fs := flag.NewFlagSet("counts", flag.ContinueOnError)
	path := configFlag(fs)
	asJSON := jsonFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), inspectTimeout)
	defer cancel()

	in, cleanup, err := openInspector(ctx, *path)
	if err != nil {
		return err
	}
	defer cleanup()

	names := in.resolveQueues(fs.Args())

	out := countsOutput{Queues: make([]countsRow, 0, len(names))}
	for _, name := range names {
		q, err := in.queue(name)
		if err != nil {
			return err
		}
		c, err := q.Counts(ctx)
		if err != nil {
			return fmt.Errorf("queue %q: %w", name, err)
		}
		paused, err := q.IsPaused(ctx)
		if err != nil {
			return fmt.Errorf("queue %q: %w", name, err)
		}
		out.Queues = append(out.Queues, countsRow{
			Name:   name,
			Paused: paused,
			Counts: counts{
				Wait:        c.Wait,
				Active:      c.Active,
				Delayed:     c.Delayed,
				Prioritized: c.Prioritized,
				Completed:   c.Completed,
				Failed:      c.Failed,
				Paused:      c.Paused,
			},
		})
	}

	if *asJSON {
		return emitJSON(out)
	}

	if len(out.Queues) == 0 {
		fmt.Println("no queues found under this key prefix")
		return nil
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "QUEUE\tWAIT\tACTIVE\tDELAYED\tPRIO\tCOMPLETED\tFAILED\tSTATUS")
	anyPaused := false
	for _, r := range out.Queues {
		anyPaused = anyPaused || r.Paused
		fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%d\t%d\t%d\t%s\n",
			r.Name, r.Counts.Wait, r.Counts.Active, r.Counts.Delayed,
			r.Counts.Prioritized, r.Counts.Completed, r.Counts.Failed, status(r.Paused))
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if anyPaused {
		fmt.Println("\nWAIT on a paused queue is held back, not being worked.")
	}
	return nil
}

// --- list -------------------------------------------------------------

type listOutput struct {
	Queue  string    `json:"queue"`
	Bucket string    `json:"bucket"`
	Page   int       `json:"page"`
	Size   int       `json:"size"`
	Jobs   []jobView `json:"jobs"`
}

func cmdList(args []string) error {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	path := configFlag(fs)
	asJSON := jsonFlag(fs)
	page := fs.Int("page", 0, "zero-based page number")
	size := fs.Int("size", 20, "jobs per page")
	asc := fs.Bool("asc", true, "oldest first; -asc=false gives BullMQ's newest-first default")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return fmt.Errorf("usage: mkqd list [-c CONFIG] [--json] [-page N] [-size N] [-asc] <queue> <%s>",
			strings.Join(bucketNames(), "|"))
	}
	if *page < 0 {
		return errors.New("-page must not be negative")
	}
	if *size < 1 {
		return errors.New("-size must be at least 1")
	}

	name, bucketArg := fs.Arg(0), fs.Arg(1)
	bucket := mkq.JobBucket(bucketArg)
	if !slices.Contains(buckets, bucket) {
		return fmt.Errorf("unknown bucket %q (want one of: %s)", bucketArg, strings.Join(bucketNames(), ", "))
	}

	ctx, cancel := context.WithTimeout(context.Background(), inspectTimeout)
	defer cancel()

	in, cleanup, err := openInspector(ctx, *path)
	if err != nil {
		return err
	}
	defer cleanup()

	q, err := in.queue(name)
	if err != nil {
		return err
	}

	// mkq の ListJobs は [start, end] の閉区間。page/size はその shim
	// (godoc に書かれている変換式どおり)。
	start := int64(*page) * int64(*size)
	end := start + int64(*size) - 1
	listed, err := q.ListJobs(ctx, bucket, start, end, *asc)
	if err != nil {
		return fmt.Errorf("queue %q: %w", name, err)
	}

	out := listOutput{
		Queue:  name,
		Bucket: string(bucket),
		Page:   *page,
		Size:   *size,
		Jobs:   make([]jobView, 0, len(listed)),
	}
	for _, lj := range listed {
		out.Jobs = append(out.Jobs, newJobView(lj.Job, lj.State))
	}

	if *asJSON {
		return emitJSON(out)
	}

	if len(out.Jobs) == 0 {
		fmt.Printf("no jobs in %s/%s at page %d\n", name, bucket, *page)
		return nil
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tNAME\tCREATED\tATTEMPTS\tDETAIL")
	for _, j := range out.Jobs {
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\n",
			j.ID, j.Name, j.Timestamp.Format(time.RFC3339), j.AttemptsMade, summarise(j))
	}
	return w.Flush()
}

// --- job --------------------------------------------------------------

type jobOutput struct {
	Queue string   `json:"queue"`
	Job   jobView  `json:"job"`
	Logs  logsView `json:"logs"`
}

type logsView struct {
	Count int64    `json:"count"`
	Lines []string `json:"lines"`
}

func cmdJob(args []string) error {
	fs := flag.NewFlagSet("job", flag.ContinueOnError)
	path := configFlag(fs)
	asJSON := jsonFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return errors.New("usage: mkqd job [-c CONFIG] [--json] <queue> <jobId>")
	}
	name, jobID := fs.Arg(0), fs.Arg(1)

	ctx, cancel := context.WithTimeout(context.Background(), inspectTimeout)
	defer cancel()

	in, cleanup, err := openInspector(ctx, *path)
	if err != nil {
		return err
	}
	defer cleanup()

	q, err := in.queue(name)
	if err != nil {
		return err
	}

	job, state, err := q.Get(ctx, jobID)
	if err != nil {
		return fmt.Errorf("queue %q job %q: %w", name, jobID, err)
	}

	logs, err := q.GetJobLogs(ctx, jobID, 0, -1)
	if err != nil {
		return fmt.Errorf("queue %q job %q logs: %w", name, jobID, err)
	}

	out := jobOutput{
		Queue: name,
		Job:   newJobView(job, state),
		Logs:  logsView{Count: logs.Count, Lines: logs.Logs},
	}
	if out.Logs.Lines == nil {
		out.Logs.Lines = []string{}
	}

	if *asJSON {
		return emitJSON(out)
	}
	return printJob(out)
}

func printJob(out jobOutput) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "queue\t%s\n", out.Queue)
	fmt.Fprintf(w, "id\t%s\n", out.Job.ID)
	fmt.Fprintf(w, "name\t%s\n", out.Job.Name)
	fmt.Fprintf(w, "created\t%s\n", out.Job.Timestamp.Format(time.RFC3339))
	fmt.Fprintf(w, "attempts\tmade=%d started=%d stalled=%d\n",
		out.Job.AttemptsMade, out.Job.AttemptsStarted, out.Job.StalledCounter)
	if out.Job.ProcessedBy != "" {
		fmt.Fprintf(w, "processed by\t%s\n", out.Job.ProcessedBy)
	}
	if out.Job.State.ProcessedOn != nil {
		fmt.Fprintf(w, "processed on\t%s\n", out.Job.State.ProcessedOn.Format(time.RFC3339))
	}
	if out.Job.State.FinishedOn != nil {
		fmt.Fprintf(w, "finished on\t%s\n", out.Job.State.FinishedOn.Format(time.RFC3339))
	}
	if out.Job.State.FailedReason != "" {
		fmt.Fprintf(w, "failed reason\t%s\n", out.Job.State.FailedReason)
	}
	if err := w.Flush(); err != nil {
		return err
	}

	fmt.Printf("\ndata\n%s\n", indentJSON(out.Job.Data))
	if len(out.Job.State.Opts) > 0 {
		fmt.Printf("\nopts\n%s\n", indentJSON(out.Job.State.Opts))
	}
	if len(out.Job.State.ReturnValue) > 0 {
		fmt.Printf("\nreturn value\n%s\n", indentJSON(out.Job.State.ReturnValue))
	}
	if len(out.Job.State.Stacktrace) > 0 {
		fmt.Printf("\nstacktrace\n")
		for _, line := range out.Job.State.Stacktrace {
			fmt.Printf("  %s\n", line)
		}
	}
	if len(out.Logs.Lines) > 0 {
		fmt.Printf("\nlogs (%d)\n", out.Logs.Count)
		for _, line := range out.Logs.Lines {
			fmt.Printf("  %s\n", line)
		}
	}
	return nil
}

// --- shared views -----------------------------------------------------

// jobView is the JSON shape the CLI emits. It is a deliberate copy of
// mkq's Job / JobState rather than those types directly: this is the
// documented output contract of `mkqd list --json` and `mkqd job
// --json`, and it must not change shape just because mkq adds a field.
type jobView struct {
	ID              string          `json:"id"`
	Name            string          `json:"name"`
	Timestamp       time.Time       `json:"timestamp"`
	AttemptsMade    int             `json:"attempts_made"`
	AttemptsStarted int             `json:"attempts_started"`
	StalledCounter  int             `json:"stalled_counter"`
	ProcessedBy     string          `json:"processed_by,omitempty"`
	Data            json.RawMessage `json:"data"`
	State           stateView       `json:"state"`
}

type stateView struct {
	ProcessedOn  *time.Time      `json:"processed_on,omitempty"`
	FinishedOn   *time.Time      `json:"finished_on,omitempty"`
	ReturnValue  json.RawMessage `json:"return_value,omitempty"`
	FailedReason string          `json:"failed_reason,omitempty"`
	Stacktrace   []string        `json:"stacktrace,omitempty"`
	Progress     json.RawMessage `json:"progress,omitempty"`
	Opts         json.RawMessage `json:"opts,omitempty"`
	DelayMillis  int64           `json:"delay_millis,omitempty"`
	AttemptsAt   []int64         `json:"attempts_at,omitempty"`
}

func newJobView(job *mkq.Job[json.RawMessage], state *mkq.JobState) jobView {
	v := jobView{}
	if job != nil {
		v.ID = job.ID
		v.Name = job.Name
		v.Timestamp = job.Timestamp
		v.AttemptsMade = job.AttemptsMade
		v.AttemptsStarted = job.AttemptsStarted
		v.StalledCounter = job.StalledCounter
		v.ProcessedBy = job.ProcessedBy
		v.Data = job.Data
	}
	if v.Data == nil {
		v.Data = json.RawMessage("null")
	}
	if state != nil {
		v.State = stateView{
			ReturnValue:  state.ReturnValue,
			FailedReason: state.FailedReason,
			Stacktrace:   state.Stacktrace,
			Progress:     state.Progress,
			Opts:         state.Opts,
			DelayMillis:  state.Delay,
			AttemptsAt:   state.AttemptsAt,
		}
		// ゼロ値の time は「まだ起きていない」であって 1970 年ではない。
		// JSON に出すと後者に見えるので、ポインタにして省く。
		if !state.ProcessedOn.IsZero() {
			t := state.ProcessedOn
			v.State.ProcessedOn = &t
		}
		if !state.FinishedOn.IsZero() {
			t := state.FinishedOn
			v.State.FinishedOn = &t
		}
	}
	return v
}

// summarise picks the one thing worth showing per row: why it failed,
// or what it returned, or nothing.
func summarise(j jobView) string {
	if j.State.FailedReason != "" {
		return truncate(j.State.FailedReason, 60)
	}
	if len(j.State.ReturnValue) > 0 && string(j.State.ReturnValue) != "null" {
		return truncate(string(j.State.ReturnValue), 60)
	}
	return ""
}

// truncate shortens s to at most n characters, counting runes so a
// multi-byte payload is never cut mid-character.
func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "..."
}

func indentJSON(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "  (empty)"
	}
	var buf bytes.Buffer
	if err := json.Indent(&buf, raw, "  ", "  "); err != nil {
		// 不正な JSON でも運用者には見せる。握り潰すと読めない理由が
		// 分からなくなる。
		return "  " + string(raw)
	}
	return "  " + buf.String()
}

func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

func status(paused bool) string {
	if paused {
		return "paused"
	}
	return "running"
}

// jsonFlag wires the --json flag shared by the read-only commands.
func jsonFlag(fs *flag.FlagSet) *bool {
	return fs.Bool("json", false, "emit JSON instead of a table")
}
