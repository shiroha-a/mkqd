package mkqd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/redis/go-redis/v9"

	"github.com/shiroha-a/mkq"
	"github.com/shiroha-a/mkq/observability/promadapter"
	"github.com/shiroha-a/mkq/observability/slogadapter"
)

// Runtime owns everything a job-processing process needs: the Redis
// connection, the registered queues, the worker lifecycle, the health
// and metrics listener, and graceful shutdown.
//
// The mkqd binary is a thin front end over a Runtime; an embedding
// application drives the same type directly.
type Runtime struct {
	cfg    Config
	log    *slog.Logger
	client *mkq.Client
	rdb    redis.UniversalClient
	reg    *prometheus.Registry
	srv    *healthServer

	// unwindGrace は drain が猶予切れしたあとの巻き取り待ちの上限。
	// 既定は defaultUnwindGrace で、テストだけが縮める。
	unwindGrace time.Duration

	mu       sync.Mutex
	registry map[string]registration
	order    []string
	workers  []*mkq.Worker
	started  bool
	stopped  bool

	stopOnce sync.Once
	stopErr  error
}

// New builds a Runtime from an in-memory configuration and connects to
// Redis. Queues are registered afterwards with Handle / HandleExecutor;
// Start then launches them.
//
// Connecting here rather than in Start is deliberate: it lets Producer
// hand out working queue handles to application code that enqueues
// before (or without) consuming anything.
func New(ctx context.Context, cfg Config) (*Runtime, error) {
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	logger, err := newLogger(cfg.Log)
	if err != nil {
		return nil, err
	}

	rt := &Runtime{
		cfg:         cfg,
		log:         logger,
		registry:    map[string]registration{},
		unwindGrace: defaultUnwindGrace,
	}

	mkqCfg := mkq.Config{
		Redis:     cfg.Redis.universalOptions(cfg.autoPoolSize()),
		KeyPrefix: cfg.KeyPrefix,
		Logger:    slogadapter.New(logger.With("component", "mkq")),
	}
	if cfg.Server.Metrics {
		rt.reg = prometheus.NewRegistry()
		// プロセス自体の健康状態 (goroutine 数 / GC / fd) は job の増減と
		// 同じダッシュボードで見たいので、既定コレクタも同じ registry に載せる。
		rt.reg.MustRegister(collectors.NewGoCollector())
		rt.reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
		mkqCfg.Metrics = promadapter.New(rt.reg)
	}

	client, err := mkq.NewClient(ctx, mkqCfg)
	if err != nil {
		return nil, err
	}
	rt.client = client

	// health probe 用に小さな専用クライアントを持つ。worker の pool は
	// BZPopMin が slot ごとに接続を占有するので、/readyz の PING で
	// そこから 1 本借りると dispatch を細らせる。
	probeOpts := cfg.Redis.universalOptions(probePoolSize)
	probeOpts.PoolSize = probePoolSize
	rt.rdb = redis.NewUniversalClient(&probeOpts)

	return rt, nil
}

// NewFromFile loads a YAML configuration file and builds a Runtime
// from it.
func NewFromFile(ctx context.Context, path string) (*Runtime, error) {
	cfg, err := LoadConfig(path)
	if err != nil {
		return nil, err
	}
	return New(ctx, cfg)
}

// Config returns the effective configuration, defaults applied.
func (rt *Runtime) Config() Config { return rt.cfg }

// Logger returns the runtime logger, so embedding code can log under
// the same handler.
func (rt *Runtime) Logger() *slog.Logger { return rt.log }

// Client exposes the underlying mkq client for operations the runtime
// does not wrap (Inspector calls, schedules, QueueEvents).
func (rt *Runtime) Client() *mkq.Client { return rt.client }

// Start builds the configured queues, launches one mkq worker per
// registered queue, and starts the health listener. It does not block.
// Callers must pair it with Shutdown.
func (rt *Runtime) Start(ctx context.Context) error {
	rt.mu.Lock()
	if rt.started {
		rt.mu.Unlock()
		return errors.New("mkqd: Start called twice")
	}
	rt.started = true
	rt.mu.Unlock()

	if err := rt.buildConfiguredQueues(ctx); err != nil {
		return err
	}

	names := rt.registeredQueues()
	if len(names) == 0 {
		return errors.New("mkqd: no queues registered; declare queues in the config or call Handle")
	}
	rt.warnPoolSize(names)

	for _, name := range names {
		r := rt.registry[name]
		tune := rt.resolveTuning(r)
		w, err := r.start(rt, tune.workerOptions(name))
		if err != nil {
			// 起動途中で失敗した場合、既に立ち上がった worker を残すと
			// ジョブを掴んだまま宙に浮く。ここで巻き戻す。
			rt.stopWorkers(context.Background())
			return fmt.Errorf("mkqd: start worker for queue %q: %w", name, err)
		}
		rt.workers = append(rt.workers, w)
		rt.log.Info("queue started",
			"queue", name,
			"concurrency", tune.concurrency,
			"lock_duration", tune.lockDuration,
			"rate_limited", tune.rateLimit != nil,
		)
	}

	if rt.cfg.serverEnabled() {
		srv, err := newHealthServer(rt)
		if err != nil {
			rt.stopWorkers(context.Background())
			return err
		}
		rt.srv = srv
		rt.log.Info("health server listening", "addr", srv.Addr(), "metrics", rt.cfg.Server.Metrics)
	}

	return nil
}

// Run starts the runtime and blocks until ctx is cancelled or the
// process receives SIGINT / SIGTERM, then shuts down gracefully within
// shutdown_timeout.
func (rt *Runtime) Run(ctx context.Context) error {
	sigCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := rt.Start(sigCtx); err != nil {
		return err
	}
	rt.log.Info("mkqd started", "version", Version, "queues", sortedQueueNames(rt.registeredQueues()))

	<-sigCtx.Done()
	stop()
	rt.log.Info("shutting down; in-flight handlers are allowed to finish",
		"timeout", rt.cfg.ShutdownTimeout.Duration())

	// シャットダウンは親 ctx から切り離す。SIGTERM で cancel された ctx を
	// そのまま渡すと in-flight ジョブの猶予がゼロになる。
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rt.cfg.ShutdownTimeout.Duration())
	defer cancel()
	return rt.Shutdown(shutdownCtx)
}

// defaultUnwindGrace bounds the second wait that follows a drain which
// ran out of budget.
//
// **この猶予を削ると drain の意味が消える。** 猶予切れの Drain は handler を
// cancel した時点で戻る (mkq の契約)。handler が moveToFinished を撃つのは
// そのあとなので、ここで待たずに Redis を閉じると、ジョブは active に lock
// されたまま残り stalled recovery が再配送する — drain が避けたかったことが
// そのまま起きる。ShutdownTimeout とは別枠にしてあるのは、これが「運用者が
// 与えた猶予」ではなく「後始末に要る最低限」だから。
const defaultUnwindGrace = 5 * time.Second

// Shutdown stops the health listener, drains every worker, and closes
// the Redis connections. It is idempotent.
//
// **In-flight jobs are allowed to finish.** Workers stop dequeueing and
// the handlers already running keep their context and their lock until
// they return on their own; ctx bounds that wait. A handler that runs
// past the budget is then cancelled and awaited separately, which is
// the old behaviour for that case only.
//
// 配送系では、これが再送の有無を分ける。cancel して落とすと掴んでいた
// ジョブは lock TTL が切れるまで active に残り、stalled detection が回収して
// 再試行する。BullMQ の at-least-once 仕様どおりで正しさは保たれるが、
// 「相手には届いていたのにもう一度送る」がデプロイのたびに出る。
func (rt *Runtime) Shutdown(ctx context.Context) error {
	rt.stopOnce.Do(func() {
		var errs []error

		if rt.srv != nil {
			if err := rt.srv.Close(ctx); err != nil {
				errs = append(errs, fmt.Errorf("health server: %w", err))
			}
		}
		if err := rt.stopWorkers(ctx); err != nil {
			errs = append(errs, err)
		}
		if rt.client != nil {
			if err := rt.client.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close mkq client: %w", err))
			}
		}
		if rt.rdb != nil {
			if err := rt.rdb.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close redis client: %w", err))
			}
		}

		rt.mu.Lock()
		rt.stopped = true
		rt.mu.Unlock()

		rt.stopErr = errors.Join(errs...)
		if rt.stopErr == nil {
			rt.log.Info("mkqd stopped")
		}
	})
	return rt.stopErr
}

// stopWorkers drains every running worker concurrently. Workers are
// independent, so a slow queue must not serialise behind another.
func (rt *Runtime) stopWorkers(ctx context.Context) error {
	rt.mu.Lock()
	workers := rt.workers
	rt.workers = nil
	rt.mu.Unlock()

	if len(workers) == 0 {
		return nil
	}

	errs := make([]error, len(workers))
	var wg sync.WaitGroup
	for i, w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = rt.drainWorker(ctx, w)
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

// drainWorker lets a worker finish what it is holding, falling back to
// cancellation when the caller's budget runs out.
//
// mkq's Drain returns as soon as it cancels, so the wait for the
// handlers to unwind is a second, separate call. It gets its own
// context: ctx is already expired by then, and passing it would make
// Stop return immediately, leaving the finalisation racing the Redis
// close that follows.
func (rt *Runtime) drainWorker(ctx context.Context, w *mkq.Worker) error {
	if err := w.Drain(ctx); err == nil {
		return nil
	}

	rt.log.Warn("drain budget expired; cancelling in-flight handlers",
		"unwind_grace", rt.unwindGrace)

	unwindCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rt.unwindGrace)
	defer cancel()
	return w.Stop(unwindCtx)
}

// warnPoolSize flags the case where queues registered in code push the
// total concurrency past the connection pool sized from the config.
//
// mkq は BZPopMin で worker slot ごとに接続を占有するため、pool が足りないと
// dispatch が詰まる。設定に書かれていない (= コードだけで登録した) キューは
// 自動算出に乗らないので、ここで運用者に知らせる。
func (rt *Runtime) warnPoolSize(names []string) {
	total := 0
	for _, name := range names {
		total += rt.resolveTuning(rt.registry[name]).concurrency
	}
	pool := rt.cfg.Redis.PoolSize
	if pool == 0 {
		pool = rt.cfg.autoPoolSize()
	}
	if want := total + poolHeadroom; pool < want {
		rt.log.Warn("redis pool is smaller than the total worker concurrency; set redis.pool_size",
			"pool_size", pool, "total_concurrency", total, "recommended", want)
	}
}

// ping checks Redis reachability. It backs /readyz.
func (rt *Runtime) ping(ctx context.Context) error {
	if rt.rdb == nil {
		return errors.New("mkqd: runtime has no redis client")
	}
	return rt.rdb.Ping(ctx).Err()
}

// isServing reports whether workers are up and shutdown has not begun.
func (rt *Runtime) isServing() bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.started && !rt.stopped
}

// universalOptions maps the configured Redis settings onto go-redis.
func (r RedisConfig) universalOptions(autoPool int) redis.UniversalOptions {
	pool := r.PoolSize
	if pool == 0 {
		pool = autoPool
	}
	return redis.UniversalOptions{
		Addrs:      append([]string(nil), r.Addrs...),
		DB:         r.DB,
		Username:   r.Username,
		Password:   r.Password,
		MasterName: r.MasterName,
		PoolSize:   pool,
	}
}

const (
	// pingTimeout bounds the Redis check behind /readyz so a hung Redis
	// turns into a 503 instead of a hung probe.
	pingTimeout = 2 * time.Second
	// probePoolSize is the pool for the health-probe client; probes are
	// serialised by the HTTP handler, so two connections are plenty.
	probePoolSize = 2
)

// Check builds every configured executor and verifies that Redis
// answers, without starting a single worker. It is what `mkqd check`
// runs, so a bad config fails in CI or at deploy time rather than at
// 3am on the first job.
func (rt *Runtime) Check(ctx context.Context) error {
	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	if err := rt.ping(pingCtx); err != nil {
		return fmt.Errorf("mkqd: redis ping: %w", err)
	}
	return rt.buildConfiguredQueues(ctx)
}

// Queues reports the queues this runtime will consume, sorted. Valid
// after Check or Start; before either, it lists only the queues
// registered from code.
func (rt *Runtime) Queues() []string {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return sortedQueueNames(rt.registeredQueues())
}

// QueueTuning reports the resolved tuning for a queue, which is what
// `mkqd check` prints so operators can confirm the precedence rules
// landed where they expected.
func (rt *Runtime) QueueTuning(queue string) (concurrency int, lockDuration time.Duration, ok bool) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	r, ok := rt.registry[queue]
	if !ok {
		return 0, 0, false
	}
	t := rt.resolveTuning(r)
	return t.concurrency, t.lockDuration, true
}

// ServerAddr reports the address the health listener bound to, or an
// empty string when it is disabled. Tests bind port 0 and read the
// real port back from here.
func (rt *Runtime) ServerAddr() string {
	if rt.srv == nil {
		return ""
	}
	return rt.srv.Addr()
}
