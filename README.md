# mkqd

Standalone worker for [mkq](https://github.com/shiroha-a/mkq) — a
BullMQ-compatible Go job queue. mkqd is the part every deployment ends
up rewriting: configuration, worker lifecycle, graceful shutdown,
health and metrics endpoints, and the outbound plumbing an ActivityPub
server needs.

It has two shapes over one implementation.

**Embedded** — the application registers typed handlers and hands the
process over:

```go
rt, err := mkqd.NewFromFile(ctx, "mkqd.yaml")
if err != nil {
    log.Fatal(err)
}

mkqd.Handle(rt, "inbox", func(ctx context.Context, job *mkq.Job[Inbox]) (any, error) {
    return nil, processInbox(ctx, job.Data)
})

log.Fatal(rt.Run(ctx))
```

**Standalone** — one binary and a YAML file, no application code:

```sh
mkqd check -c mkqd.yaml   # validate the config and reach Redis
mkqd run   -c mkqd.yaml   # consume the configured queues
```

Because mkq speaks BullMQ's wire format, either shape shares queues
with BullMQ workers in other languages and with dashboards such as
bull-board — including a TypeScript Misskey handing its delivery queue
to a Go worker.

## Status

Early. The runtime, configuration, typed handlers, executor registry,
health endpoints and the `run` / `check` / `version` commands work and
are covered by tests against a real Redis. The HTTP-dispatch, webhook
and ActivityPub delivery executors, and the inspect / admin
subcommands, are the next slices — see the roadmap below.

## Install

```sh
go install github.com/shiroha-a/mkqd/cmd/mkqd@latest
```

Requires Go 1.26+ and Redis 7+ (the same floor as mkq).

## Configuration

`mkqd.example.yaml` is the annotated reference. The shape:

```yaml
redis:
  addrs: ["127.0.0.1:6379"]
  password: "${REDIS_PASSWORD:-}"
key_prefix: "bull"

log:   { level: info, format: text }
server: { addr: "127.0.0.1:9464", metrics: true }
shutdown_timeout: 30s

defaults:
  concurrency: 16
  lock_duration: 30s

queues:
  - name: smoke
    concurrency: 4
    executor:
      type: log
```

`${VAR}` and `${VAR:-fallback}` are expanded from the environment.
Expansion runs on parsed values, not on the raw file, so references in
comments are left alone and a value containing YAML punctuation cannot
break the document. A reference resolves its type the way a literal
would: `db: ${REDIS_DB}` is a number, `db: "${REDIS_DB}"` is a string
and therefore invalid, exactly as `db: "5"` would be. An undefined
variable with no fallback is a startup error rather than an empty
string.

### Who decides what

A queue's tuning is resolved in three layers, later winning over
earlier:

1. built-in defaults, then the config's `defaults` block;
2. the `QueueOption`s passed to `Handle` in Go;
3. the queue's own block in the config.

Execution is the other way round and always belongs to code: when a
queue has both a handler registered through `Handle` and an `executor`
declared in YAML, the handler wins and the declaration is logged as
ignored. Behaviour stays with the developer, tuning with the operator,
so concurrency changes without a rebuild.

### Connection pool

mkq's dispatcher parks a Redis connection per worker slot, so the pool
must exceed the total concurrency. Leaving `redis.pool_size` unset
sizes it as the sum of the declared queue concurrencies plus 8. Queues
registered only in Go are invisible to that sum; mkqd warns at startup
when the total outgrows the pool.

## Endpoints

| Path | Meaning |
|---|---|
| `GET /healthz` | the process is alive |
| `GET /readyz` | workers are up and Redis answers `PING`, else 503 |
| `GET /metrics` | Prometheus exposition, when `server.metrics` is true |

`/metrics` carries mkq's job counters and histograms alongside the
standard Go and process collectors. Set `server.addr: "off"` to skip
the listener entirely.

## Shutdown

SIGINT or SIGTERM stops the listener and the workers, bounded by
`shutdown_timeout`.

In-flight jobs are **cancelled, not drained**. mkq derives each job's
context from the worker's run context, so a handler receives a
cancellation and then has until the timeout to wind down and return.
Work cut short this way stays locked until the BullMQ lock expires and
is recovered by stalled detection — the at-least-once behaviour BullMQ
specifies. Handlers should treat context cancellation as "stop soon and
leave the job safe to retry".

## Executors

Configuration names an executor type; a factory turns it into running
work:

| Type | State |
|---|---|
| `log` | shipped — records each job and succeeds, for proving the wiring |
| `http` | planned — forward the job to the application's HTTP endpoint |
| `webhook` | planned — HMAC-signed POST to a URL carried in the payload |
| `activitypub_deliver` | planned — HTTP Signature delivery to a remote inbox |

An embedding application can register its own:

```go
mkqd.RegisterExecutor("my-thing", func(ctx context.Context, bc mkqd.BuildContext, cfg mkqd.ExecutorConfig) (mkqd.Executor, error) {
    var opts struct {
        Endpoint string `yaml:"endpoint"`
    }
    if err := cfg.Decode(&opts); err != nil {
        return nil, err
    }
    return mkqd.ExecutorFunc(func(ctx context.Context, job *mkqd.Job) (any, error) {
        return nil, send(ctx, opts.Endpoint, job.Data)
    }), nil
})
```

Executors see the payload as raw JSON, so one that forwards a job
elsewhere never pays for a decode it does not need.

## Roadmap

- `http` / `webhook` executors, with an SSRF-guarded dialer.
- `activitypub_deliver` and an HTTP Signature package, with file,
  directory and HTTP key stores.
- Inspect and admin subcommands (`counts`, `list`, `job`, `enqueue`,
  `pause`, `resume`, `retry`, `promote`, `rm`, `drain`).
- Container image, compose example and an embedded sample application.

## Development

```sh
go test ./... -race          # needs Redis on 127.0.0.1:6379, or MKQD_TEST_REDIS_ADDR
go run ./cmd/mkqd check -c mkqd.example.yaml
```

Contributions follow `CLAUDE.md`: branch off `develop`, one issue per
change, no direct commits to `main`.

## License

MIT. See `LICENSE`.
