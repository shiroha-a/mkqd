package mkqd

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/shiroha-a/mkq"
)

// QueueOption is a per-queue tuning knob supplied at registration time
// by embedding code. Operators override any of these from YAML — see
// the precedence table in the package documentation.
type QueueOption func(*queueTuning)

// WithConcurrency sets how many jobs the queue processes in parallel.
func WithConcurrency(n int) QueueOption {
	return func(t *queueTuning) { t.concurrency = &n }
}

// WithLockDuration sets the BullMQ job lock TTL. mkq renews it at half
// the interval while a handler runs.
func WithLockDuration(d time.Duration) QueueOption {
	return func(t *queueTuning) { t.lockDuration = &d }
}

// WithStalledInterval sets how often the stalled-job scanner runs. Zero
// disables it for this queue.
func WithStalledInterval(d time.Duration) QueueOption {
	return func(t *queueTuning) { t.stalledInterval = &d }
}

// WithMaxStalledCount sets how many times a job may be recovered from
// the stalled state before it is failed outright.
func WithMaxStalledCount(n int) QueueOption {
	return func(t *queueTuning) { t.maxStalledCount = &n }
}

// WithRateLimit caps the queue at max jobs per window, shared across
// every worker on the queue (including BullMQ workers in other
// languages, since the window lives in Redis).
func WithRateLimit(max int, window time.Duration) QueueOption {
	return func(t *queueTuning) { t.rateLimit = &RateLimitConfig{Max: max, Duration: Duration(window)} }
}

// WithJobMetrics enables BullMQ's per-minute completed / failed metrics
// for this queue, keeping at most n one-minute buckets.
func WithJobMetrics(n int) QueueOption {
	return func(t *queueTuning) { t.jobMetrics = &n }
}

// queueTuning holds the knobs as "set or unset" so that three layers
// (built-in defaults, Go options, YAML) can be merged by precedence.
type queueTuning struct {
	concurrency     *int
	lockDuration    *time.Duration
	stalledInterval *time.Duration
	maxStalledCount *int
	jobMetrics      *int
	rateLimit       *RateLimitConfig
}

func tuningFromOptions(opts []QueueOption) queueTuning {
	var t queueTuning
	for _, o := range opts {
		if o != nil {
			o(&t)
		}
	}
	return t
}

func tuningFromConfig(q QueueConfig) queueTuning {
	t := queueTuning{
		concurrency:     q.Concurrency,
		maxStalledCount: q.MaxStalledCount,
		jobMetrics:      q.JobMetrics,
		rateLimit:       q.RateLimit,
	}
	if q.LockDuration != nil {
		d := q.LockDuration.Duration()
		t.lockDuration = &d
	}
	if q.StalledInterval != nil {
		d := q.StalledInterval.Duration()
		t.stalledInterval = &d
	}
	return t
}

// overlay returns t with every field that over sets replaced.
func (t queueTuning) overlay(over queueTuning) queueTuning {
	if over.concurrency != nil {
		t.concurrency = over.concurrency
	}
	if over.lockDuration != nil {
		t.lockDuration = over.lockDuration
	}
	if over.stalledInterval != nil {
		t.stalledInterval = over.stalledInterval
	}
	if over.maxStalledCount != nil {
		t.maxStalledCount = over.maxStalledCount
	}
	if over.jobMetrics != nil {
		t.jobMetrics = over.jobMetrics
	}
	if over.rateLimit != nil {
		t.rateLimit = over.rateLimit
	}
	return t
}

// resolved is the fully-determined tuning for one queue.
type resolved struct {
	concurrency     int
	lockDuration    time.Duration
	stalledInterval time.Duration
	maxStalledCount int
	jobMetrics      int
	rateLimit       *RateLimitConfig
}

func (t queueTuning) resolve(d QueueDefaults) resolved {
	r := resolved{
		concurrency:     d.Concurrency,
		lockDuration:    d.LockDuration.Duration(),
		stalledInterval: d.StalledInterval.Duration(),
		maxStalledCount: d.MaxStalledCount,
		jobMetrics:      d.JobMetrics,
		rateLimit:       t.rateLimit,
	}
	if t.concurrency != nil {
		r.concurrency = *t.concurrency
	}
	if t.lockDuration != nil {
		r.lockDuration = *t.lockDuration
	}
	if t.stalledInterval != nil {
		r.stalledInterval = *t.stalledInterval
	}
	if t.maxStalledCount != nil {
		r.maxStalledCount = *t.maxStalledCount
	}
	if t.jobMetrics != nil {
		r.jobMetrics = *t.jobMetrics
	}
	return r
}

func (r resolved) workerOptions(queue string) []mkq.WorkerOption {
	opts := []mkq.WorkerOption{
		mkq.WithConcurrency(r.concurrency),
		mkq.WithLockDuration(r.lockDuration),
		mkq.WithStalledInterval(r.stalledInterval),
		mkq.WithMaxStalledCount(r.maxStalledCount),
	}
	if r.rateLimit != nil {
		opts = append(opts, mkq.WithRateLimit(r.rateLimit.Max, r.rateLimit.Duration.Duration()))
	}
	if r.jobMetrics > 0 {
		opts = append(opts, mkq.WithJobMetrics(r.jobMetrics))
	}
	return opts
}

// workerOptionsFor is workerOptions plus the runtime-level hooks that
// every queue gets regardless of its tuning.
func (rt *Runtime) workerOptionsFor(r resolved, queue string) []mkq.WorkerOption {
	// **Retry-After は全キューで尊重する。** 相手が「いつ来い」と言っている
	// のに指数バックオフの都合で早く叩き直す理由が無い。口を出すのは executor
	// が RetryAfterError を返したときだけで、それ以外は設定どおりの backoff に
	// 委ねられるので、オプトインにする意味が薄い。
	return append(r.workerOptions(queue), mkq.WithRetryDelayOverride(rt.retryDelay))
}

// starter launches the mkq worker for one registered queue. The
// closure is what erases the payload type: Handle[T] captures T here,
// executor-backed queues pin it to json.RawMessage, and the Runtime
// only ever sees this signature.
type starter func(rt *Runtime, opts []mkq.WorkerOption) (*mkq.Worker, error)

type registration struct {
	queue string
	tune  queueTuning
	start starter
	// fromCode marks a queue registered through Handle / HandleExecutor
	// rather than built from the config file.
	fromCode bool
}

// Handle registers a typed handler for a queue. It is the main entry
// point when mkqd is embedded: the payload type is the application's
// own struct, and mkq's generic Job is handed to the handler
// unchanged.
//
//	mkqd.Handle(rt, "inbox", func(ctx context.Context, job *mkq.Job[Inbox]) (any, error) {
//	    return nil, process(ctx, job.Data)
//	})
//
// Registering the same queue twice, or registering after Start, is an
// error.
func Handle[T any](rt *Runtime, queue string, h mkq.Handler[T], opts ...QueueOption) error {
	if h == nil {
		return fmt.Errorf("mkqd: Handle(%q): handler is nil", queue)
	}
	return rt.register(registration{
		queue:    queue,
		tune:     tuningFromOptions(opts),
		fromCode: true,
		start: func(rt *Runtime, wo []mkq.WorkerOption) (*mkq.Worker, error) {
			return mkq.Process(mkq.Define[T](rt.client, queue), h, wo...)
		},
	})
}

// HandleExecutor registers a type-erased Executor for a queue. Embedded
// applications reach for Handle instead; this is what configuration
// files resolve to, and what an application uses when it wants to reuse
// a built-in executor under a queue name of its own.
func (rt *Runtime) HandleExecutor(queue string, ex Executor, opts ...QueueOption) error {
	if ex == nil {
		return fmt.Errorf("mkqd: HandleExecutor(%q): executor is nil", queue)
	}
	return rt.register(registration{
		queue:    queue,
		tune:     tuningFromOptions(opts),
		fromCode: true,
		start: func(rt *Runtime, wo []mkq.WorkerOption) (*mkq.Worker, error) {
			q := mkq.Define[json.RawMessage](rt.client, queue)
			return mkq.Process(q, executorHandler(queue, ex), wo...)
		},
	})
}

// Producer returns a typed producer handle for a queue. It is a thin
// pass-through to mkq.Define, so every mkq AddOption applies.
//
// The queue need not be registered for consumption: a producer-only
// process is a normal use of the runtime.
func Producer[T any](rt *Runtime, queue string) *mkq.Queue[T] {
	return mkq.Define[T](rt.client, queue)
}

// register records a queue registration, rejecting duplicates and
// post-Start additions.
func (rt *Runtime) register(r registration) error {
	if r.queue == "" {
		return fmt.Errorf("mkqd: queue name must not be empty")
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.started {
		return fmt.Errorf("mkqd: cannot register queue %q after Start", r.queue)
	}
	if _, dup := rt.registry[r.queue]; dup {
		return fmt.Errorf("mkqd: queue %q is already registered", r.queue)
	}
	rt.registry[r.queue] = r
	rt.order = append(rt.order, r.queue)
	return nil
}

// buildConfiguredQueues turns every YAML queue that code did not claim
// into an executor-backed registration.
//
// コードで登録済みのキューが YAML にも executor 付きで書かれている場合、
// 実行内容はコード側が持つべきなので executor 設定は無視して警告する。
// チューニング (concurrency 等) は YAML 側を採用する — 運用者が再ビルド
// なしに変えられることが単体ワーカーの価値の核になるため。
func (rt *Runtime) buildConfiguredQueues(ctx context.Context) error {
	for _, qc := range rt.cfg.Queues {
		existing, claimed := rt.registry[qc.Name]
		if claimed && existing.fromCode {
			if qc.Executor != nil {
				rt.log.Warn("queue has both a code handler and an executor config; the code handler wins",
					"queue", qc.Name, "executor", qc.Executor.Type)
			}
			continue
		}
		if qc.Executor == nil {
			return fmt.Errorf("mkqd: queue %q has no executor in the config and no handler registered in code", qc.Name)
		}
		factory, ok := lookupExecutorFactory(qc.Executor.Type)
		if !ok {
			return fmt.Errorf("mkqd: queue %q: %w", qc.Name, unknownExecutorError(qc.Executor.Type))
		}
		bc := BuildContext{
			Queue:  qc.Name,
			Logger: rt.log.With("queue", qc.Name),
			Config: &rt.cfg,
		}
		ex, err := factory(ctx, bc, *qc.Executor)
		if err != nil {
			return fmt.Errorf("mkqd: queue %q: build executor %q: %w", qc.Name, qc.Executor.Type, err)
		}
		name := qc.Name
		rt.registry[name] = registration{
			queue: name,
			start: func(rt *Runtime, wo []mkq.WorkerOption) (*mkq.Worker, error) {
				return mkq.Process(mkq.Define[json.RawMessage](rt.client, name), executorHandler(name, ex), wo...)
			},
		}
		rt.order = append(rt.order, name)
	}
	return nil
}

// resolveTuning applies the precedence rule: built-in defaults, then
// the Go QueueOptions given at registration, then the YAML queue block.
func (rt *Runtime) resolveTuning(r registration) resolved {
	t := r.tune
	if qc, ok := rt.cfg.queueConfig(r.queue); ok {
		t = t.overlay(tuningFromConfig(qc))
	}
	return t.resolve(rt.cfg.Defaults)
}

// registeredQueues returns the registration order with configured
// queues appended, deduplicated.
func (rt *Runtime) registeredQueues() []string {
	out := make([]string, 0, len(rt.order))
	seen := make(map[string]struct{}, len(rt.order))
	for _, n := range rt.order {
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		out = append(out, n)
	}
	return out
}

// sortedQueueNames is used by diagnostics where deterministic output
// matters more than registration order.
func sortedQueueNames(names []string) []string {
	out := append([]string(nil), names...)
	sort.Strings(out)
	return out
}
