package mkqd

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/shiroha-a/mkq"
)

// Executor runs a single job whose payload type is not known at compile
// time. It is the extension point the mkqd binary uses to turn a YAML
// queue declaration into running work.
//
// One Executor instance is shared by every worker goroutine of a queue,
// so implementations must be safe for concurrent use.
type Executor interface {
	// Execute runs the job. The returned value is stored in BullMQ's
	// `returnvalue` field. A non-nil error fails the attempt; wrap
	// mkq.ErrUnrecoverable to skip the remaining attempts.
	Execute(ctx context.Context, job *Job) (any, error)
}

// ExecutorFunc adapts a plain function to the Executor interface.
type ExecutorFunc func(ctx context.Context, job *Job) (any, error)

// Execute implements Executor.
func (f ExecutorFunc) Execute(ctx context.Context, job *Job) (any, error) { return f(ctx, job) }

// Job is the type-erased view of a job handed to an Executor. The
// payload stays as raw JSON so that executors which forward the job
// elsewhere (HTTP, ActivityPub delivery) never pay for a decode they
// do not need.
type Job struct {
	// Queue is the queue the job was dequeued from.
	Queue string
	// ID is the BullMQ job id.
	ID string
	// Name is the BullMQ job name; it defaults to the queue name and
	// is the conventional way to fan one queue across task types.
	Name string
	// Data is the job payload exactly as it sits in Redis.
	Data json.RawMessage
	// AttemptsMade is how many times this job failed before the
	// current attempt. The current attempt is AttemptsMade+1.
	AttemptsMade int
	// Timestamp is the job's creation time.
	Timestamp time.Time

	src *mkq.Job[json.RawMessage]
}

// UpdateProgress writes BullMQ's `progress` field for this job.
func (j *Job) UpdateProgress(ctx context.Context, v any) error {
	if j.src == nil {
		return mkq.ErrJobDetached
	}
	return j.src.UpdateProgress(ctx, v)
}

// Log appends a line to the job's BullMQ log list, which bull-board and
// other dashboards display.
func (j *Job) Log(ctx context.Context, line string) error {
	if j.src == nil {
		return mkq.ErrJobDetached
	}
	return j.src.Log(ctx, line)
}

// BuildContext is handed to an ExecutorFactory so it can read the
// surrounding configuration and log under the runtime's logger.
type BuildContext struct {
	// Queue is the queue this executor is being built for.
	Queue string
	// Logger is the runtime logger, already tagged with the queue.
	Logger *slog.Logger
	// Config is the whole mkqd configuration. Executors that need
	// process-wide settings read them from here.
	Config *Config
}

// ExecutorFactory builds an Executor from its configuration block.
type ExecutorFactory func(ctx context.Context, bc BuildContext, cfg ExecutorConfig) (Executor, error)

var (
	factoriesMu sync.RWMutex
	factories   = map[string]ExecutorFactory{}
)

// RegisterExecutor makes an executor type available to configuration
// files. Built-in types are registered by their own packages; embedding
// applications can register their own so that operators tune them from
// YAML without a rebuild.
//
// It panics on a duplicate registration, mirroring database/sql and
// similar registries: a collision is a programming error, not a
// runtime condition.
func RegisterExecutor(typ string, f ExecutorFactory) {
	if typ == "" {
		panic("mkqd: RegisterExecutor with empty type")
	}
	if f == nil {
		panic("mkqd: RegisterExecutor with nil factory")
	}
	factoriesMu.Lock()
	defer factoriesMu.Unlock()
	if _, dup := factories[typ]; dup {
		panic(fmt.Sprintf("mkqd: executor type %q registered twice", typ))
	}
	factories[typ] = f
}

// RegisteredExecutors lists the known executor types, sorted. It backs
// the error message shown for an unknown type and the `mkqd check`
// output.
func RegisteredExecutors() []string {
	factoriesMu.RLock()
	defer factoriesMu.RUnlock()
	out := make([]string, 0, len(factories))
	for k := range factories {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func lookupExecutorFactory(typ string) (ExecutorFactory, bool) {
	factoriesMu.RLock()
	defer factoriesMu.RUnlock()
	f, ok := factories[typ]
	return f, ok
}

// executorHandler adapts an Executor to mkq's typed handler signature
// by pinning T to json.RawMessage.
//
// json.RawMessage は Marshal/Unmarshal を素通しするので、mkq の JSON 経路を
// そのまま使って payload のバイト列を保てる。設定駆動で payload の Go 型を
// 知りえない単体ワーカーは、この型消去に乗る。
func executorHandler(queue string, ex Executor) mkq.Handler[json.RawMessage] {
	return func(ctx context.Context, job *mkq.Job[json.RawMessage]) (any, error) {
		return ex.Execute(ctx, &Job{
			Queue:        queue,
			ID:           job.ID,
			Name:         job.Name,
			Data:         job.Data,
			AttemptsMade: job.AttemptsMade,
			Timestamp:    job.Timestamp,
			src:          job,
		})
	}
}

// logExecutor is the built-in "log" executor: it records the job and
// succeeds. It exists so that a fresh deployment can prove the Redis
// wiring end to end before any application code is written.
func init() {
	RegisterExecutor("log", func(_ context.Context, bc BuildContext, _ ExecutorConfig) (Executor, error) {
		logger := bc.Logger
		return ExecutorFunc(func(_ context.Context, job *Job) (any, error) {
			logger.Info("job received",
				"job_id", job.ID,
				"job_name", job.Name,
				"attempt", job.AttemptsMade+1,
				"data", string(job.Data),
			)
			return nil, nil
		}), nil
	})
}

// builtinExecutorPackages maps the executor types mkqd ships to the
// package that registers them. They live in sub-packages so that an
// embedding application links only what it uses, which means a config
// can name a type whose package nobody imported. Naming the import in
// the error turns a dead end into a one-line fix.
var builtinExecutorPackages = map[string]string{
	"http":                "github.com/shiroha-a/mkqd/executor/httpexec",
	"webhook":             "github.com/shiroha-a/mkqd/executor/httpexec",
	"activitypub_deliver": "github.com/shiroha-a/mkqd/executor/apdeliver",
}

func unknownExecutorError(typ string) error {
	if pkg, ok := builtinExecutorPackages[typ]; ok {
		return fmt.Errorf("executor type %q is built in but not linked; add `import _ %q` (registered: %v)",
			typ, pkg, RegisteredExecutors())
	}
	return fmt.Errorf("unknown executor type %q (registered: %v)", typ, RegisteredExecutors())
}
