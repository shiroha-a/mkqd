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
mkqd queues               # what is in Redis under this prefix
```

Because mkq speaks BullMQ's wire format, either shape shares queues
with BullMQ workers in other languages and with dashboards such as
bull-board — including a TypeScript Misskey handing its delivery queue
to a Go worker.

## Status

Early, but everything documented here works and is covered by tests:
the runtime, configuration, typed handlers, executor registry, health
endpoints, the `http`, `webhook` and `activitypub_deliver` executors,
and the full command set — `run`, `check`, the inspect commands
(`queues`, `counts`, `list`, `job`) and the admin ones (`enqueue`,
`pause`, `resume`, `retry`, `promote`, `rm`, `drain`), plus `keys` and
`version`.

## Install

```sh
go install github.com/shiroha-a/mkqd/cmd/mkqd@latest
```

Requires Go 1.27+ and Redis 7+ (the same floor as mkq).

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

## Inspecting queues

Four read-only commands, all taking `-c` for the config and `--json`
for scripted use:

```sh
mkqd queues                              # queues present in Redis
mkqd counts [queue...]                   # job counts per bucket
mkqd list <queue> <bucket> [-page N] [-size N] [-asc=false]
mkqd job  <queue> <jobId>                # one job with its state and logs
```

They open the Redis connection but start no worker, so running them
against a production deployment costs a few reads and nothing else.

`queues` reports every queue in Redis under the configured prefix, not
just the ones this process works — a queue created by a BullMQ worker
in another language shows up with `CONFIGURED = no`.

**A queue name that does not exist is refused rather than created.**
mkq's `Define` stamps `meta.version` on first use, so a typo would
otherwise leave a queue behind that shows up in every later listing.
The name is checked against what is actually in Redis first.

`list` pages with `-page` (zero-based) and `-size`, oldest first by
default; pass `-asc=false` for BullMQ's newest-first order.

## Administering queues

```sh
mkqd enqueue <queue> '<json>' [--create] [-name N] [-delay 5m] [-priority 3] [-attempts 5] [-id X] [-lifo]
mkqd pause   <queue>
mkqd resume  <queue>
mkqd retry   <queue> <jobId> [-from failed|completed]
mkqd promote <queue> <jobId>
mkqd rm      <queue> <jobId>
mkqd drain   <queue> --yes [-delayed]
```

`enqueue` will not create a queue that does not exist; pass `--create`
when you mean to. A typo otherwise buries the job in a queue nothing
works.

### `mkqd drain` is not the shutdown drain

There are two drains in mkqd and they do opposite things.

| | |
|---|---|
| the drain `mkqd run` does on SIGTERM | lets in-flight jobs **finish**. Deletes nothing |
| `mkqd drain <queue>` | **deletes** the queued jobs |

`--yes` is required, and the command prints what it removed:

```
$ mkqd drain deliver --yes
drained deliver: wait=120 prioritized=3
```

It removes `wait`, `paused` and `prioritized`. `delayed` only with
`-delayed`, and scheduler iterations survive even then. Active jobs and
finished ones are left alone — this is "cancel everything pending", not
a wipe.

### Counts on a paused queue

`counts` shows a `STATUS` column and no `paused` column, because under
BullMQ 6 a paused queue holds its jobs in `wait` — the same jobs would
appear under both headings and an operator adding the row up would
double count. `--json` carries both: `paused` for the queue status and
`counts.paused` for mkq's bucket count, which restates `counts.wait`
while the queue is stopped.

## Shutdown

SIGINT or SIGTERM stops the listener and the workers, bounded by
`shutdown_timeout`.

In-flight jobs are **drained, not cancelled**. Workers stop taking new
work; a handler that is already running keeps its context and its lock
and finishes normally. For a delivery worker this is the difference
between a clean deploy and re-sending everything that was in flight:
a cancelled job stays locked until the BullMQ lock expires, is then
recovered by stalled detection, and goes out a second time.

A handler still running when `shutdown_timeout` expires is cancelled
and given a further five seconds to unwind — long enough for it to
finalise the job before the Redis connections close, which is what
keeps that job out of stalled recovery too. Handlers should treat
context cancellation as "stop soon and leave the job safe to retry".

**Budget for `shutdown_timeout` + 5s when you set an outer deadline.**
Under Kubernetes that is `terminationGracePeriodSeconds`: leave it at
the default 30 with `shutdown_timeout: 30s` and SIGKILL lands during
the unwind, which puts the job back into stalled recovery — the thing
the drain exists to prevent. With the example config above, set it to
40 or higher.

## Executors

Configuration names an executor type; a factory turns it into running
work:

| Type | State | Package |
|---|---|---|
| `log` | shipped — records each job and succeeds, for proving the wiring | built in |
| `http` | shipped — forward the job to the application's HTTP endpoint | `executor/httpexec` |
| `webhook` | shipped — HMAC-signed POST to a URL carried in the payload | `executor/httpexec` |
| `activitypub_deliver` | shipped — HTTP Signature delivery to a remote inbox | `executor/apdeliver` |

Executors outside the core live in their own packages so an embedding
application links only what it uses. The `mkqd` binary links all of
them; an application embedding the runtime imports what it needs:

```go
import _ "github.com/shiroha-a/mkqd/executor/httpexec"
```

Naming a type whose package nobody imported is a startup error that
tells you which import to add.

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

## Retry-After

A server that answers 429 or 503 often says when to come back. Ignoring
it means retrying too early — which the remote rejects again — or too
late, once an exponential curve has overshot. Both executors read the
header and hand the delay to the runtime, which uses it for that one
retry instead of the configured backoff:

```
Retry-After: 900                              # delta-seconds
Retry-After: Wed, 21 Oct 2026 07:28:00 GMT    # HTTP-date
```

Both RFC 9110 forms are accepted. A missing, unparseable or already-past
value changes nothing and the configured backoff stands; a value that
was present but unusable is logged at debug level, which is the only
trace of it — nothing reaches the runtime to be logged there. **The delay is
capped at one hour**, so a remote answering `Retry-After: 86400` cannot
park a job for a day per attempt. The cap holds whatever the size: a
value too large to be a duration is treated as "very long" and capped
like any other, never wrapped into a short one.

The header only moves the delay; it never decides whether to retry at
all. A permanent failure stays permanent, and `mkq.ErrUnrecoverable`
still means "do not retry" when a delay rides along with it.

A custom executor gets the same treatment by wrapping its error:

```go
return nil, mkqd.RetryAfter(90*time.Second, fmt.Errorf("upstream is throttling"))
```

`mkqd.ParseRetryAfter` is exported for executors that have an HTTP
response in hand. `RetryAfter` returns the error untouched when the
delay is not positive, so there is no need to guard the call.

## The HTTP dispatch contract

The `http` executor turns a queue into an HTTP endpoint your
application implements, in any language. mkqd owns the queue, the
retries and the lock; the application answers one request per attempt.

```yaml
queues:
  - name: inbox
    concurrency: 32
    executor:
      type: http
      url: "http://127.0.0.1:3000/_mkqd/jobs"
      timeout: 60s
      secret: "${MKQD_HMAC_SECRET}"
```

Request:

```
POST /_mkqd/jobs
Content-Type: application/json
User-Agent: mkqd/<version>
X-Mkqd-Queue: inbox
X-Mkqd-Job-Id: 42
X-Mkqd-Job-Name: inbox
X-Mkqd-Attempt: 2
X-Mkqd-Timestamp: 1758500000
X-Mkqd-Signature: v1=<hex hmac-sha256>

{"queue":"inbox","id":"42","name":"inbox","data":{...},
 "attemptsMade":1,"timestamp":"2026-09-22T10:00:00Z"}
```

`X-Mkqd-Attempt` is which attempt this is; `attemptsMade` is how many
already failed. The signature is HMAC-SHA256 over
`v1:<timestamp>:<body>` keyed by `secret`. Verify it in constant time
and reject a timestamp more than a few minutes from your own clock.
Without a `secret` no signature is sent, which is reasonable on
loopback.

The response decides the job's fate:

| Status | Meaning |
|---|---|
| 2xx | success; a JSON body becomes BullMQ's `returnvalue` |
| 3xx | permanent failure — redirects are not followed |
| 400, 409, 410, 422 | permanent failure |
| anything else | retry |

A response header `X-Mkqd-Retry: no` fails the job permanently whatever
the status says. A JSON body of the form `{"error": "..."}` supplies
the text shown as the failure reason; otherwise the first kilobyte of
the body is used.

Unknown statuses retry rather than fail, because losing work is worse
than one wasted attempt. A permanent failure is not lost either: the
job lands in the failed set, and `mkqd retry` can send it back once the
cause is fixed.

A `Retry-After` on a retryable response sets that job's next delay in
place of the configured backoff. See [Retry-After](#retry-after).

## Outbound webhooks

The `webhook` executor takes its destination from the job instead of
the config, which is what an outbound webhook queue needs:

```json
{
  "url": "https://subscriber.example/hook",
  "secret": "per-subscriber-secret",
  "headers": {"X-Tier": "pro"},
  "body": {"event": "note.created"}
}
```

`body` is sent verbatim, signed the same way as a dispatch. Because the
destination comes from the payload, the SSRF guard is on by default
here: a job naming a loopback, private, link-local, carrier-grade NAT
or otherwise non-public address is rejected without a request being
made, and the check runs against the address actually being connected
to, so a hostname that re-resolves cannot slip past it. The `http`
executor has the guard off by default, since there the URL comes from
the operator and pointing it at `127.0.0.1` is the normal case.

Any defect in the payload — no URL, a non-HTTP scheme, malformed JSON,
a header name or value HTTP cannot carry — fails the job permanently
rather than retrying something that cannot start working.

Two things the destination cannot do to the worker: `max_response_bytes`
(64 KiB by default) bounds what crosses the wire and not only what is
kept, so a small gzip stream that inflates to gigabytes cannot hold a
worker slot open; and job metadata copied into `X-Mkqd-*` headers is
sanitized, so a job name containing CRLF cannot inject a header.

**Proxies.** The guarded executor ignores `HTTP_PROXY` / `HTTPS_PROXY`:
through a proxy the connection goes to the proxy's address, so that is
what the guard would check, and the real destination would be
unprotected. The `http` executor, whose destination the operator
chooses, honours the environment proxy as usual. Turning
`allow_private_network: true` on for `webhook` also turns the proxy
back on, for the same reason — you have taken the destination decision
back.

## ActivityPub delivery

The `activitypub_deliver` executor takes federation delivery off the
application entirely. Enqueue the activity and the inbox; mkqd signs,
digests, delivers and applies the fediverse's retry conventions.

```json
{
  "inbox": "https://remote.example/users/alice/inbox",
  "keyId": "https://local.example/users/me#main-key",
  "activity": {"@context": "https://www.w3.org/ns/activitystreams", "type": "Create"}
}
```

The request carries `Date`, `Host`, `Digest` and a `Signature` over
`(request-target) host date digest content-type` — the
draft-cavage dialect the fediverse actually verifies, rather than the
RFC 9421 form it mostly does not yet. `keyId` may be omitted when the
signer has a default, which is the single-actor case.

Use `body` instead of `activity` to fix the exact bytes sent; the two
are exclusive. Either way the digest is computed over what mkqd
actually sends, so re-serialisation cannot desynchronise it.

Delivery outcomes follow what the fediverse expects:

| Status | Meaning |
|---|---|
| 2xx | delivered |
| 3xx | permanent failure — a redirected POST cannot carry its signature |
| 4xx except 401, 408 and 429 | permanent failure |
| 401, 408, 429, 5xx, network errors | retry |

401 retries where Misskey would give up, following Mastodon instead: an
inbox answers 401 when signature verification failed, and the usual
causes — clock skew, or the remote not being able to fetch your key
endpoint just then — clear on their own. Treating it as permanent means
a post never federates because the other side had a bad minute.

A `Retry-After` from the remote sets that job's next delay in place of
the configured backoff, which is what a rate-limited instance is asking
for. See [Retry-After](#retry-after).

The inbox comes from the job, so the SSRF guard is on and redirects are
not followed. A payload that cannot be delivered — no inbox, both
`activity` and `body`, a header HTTP cannot carry, a key the signer does
not hold — fails permanently instead of burning attempts.

### Keys stay where they are

The signing interface is "sign these bytes", not "give me the private
key". An application that embeds mkqd therefore never hands its keys
over:

```go
ex, err := apdeliver.New(apdeliver.Options{Signer: appSigner})
if err != nil {
    log.Fatal(err)
}
rt.HandleExecutor("deliver", ex)
```

`appSigner` is anything with a `Sign(ctx, keyID, signingString)` method
— typically a lookup in the same database the actors live in.

A standalone mkqd configures a signer instead:

```yaml
queues:
  - name: deliver
    concurrency: 128
    rate_limit: { max: 300, duration: 1s }
    executor:
      type: activitypub_deliver
      signer:
        type: file           # one actor
        key_id: "https://local.example/users/me#main-key"
        private_key_path: /etc/mkqd/actor.pem
```

`type: dir` serves many actors from a directory, one PEM per key id.
The file is named after a hash of the key id, since a key id is a URL;
`mkqd keys -dir /etc/mkqd/keys <keyId>` prints where to put it. Parsed
keys are cached but revalidated against the file on every use, so
rotating a key in place takes effect without a restart — ActivityPub
rotation keeps the key id and replaces the material, which a cache with
no invalidation would never notice.

For a server whose keys live only in its database, `type: remote` keeps
the key there: mkqd sends the string to sign and the application returns
the signature.

```yaml
      signer:
        type: remote
        url: "http://127.0.0.1:3000/_mkqd/sign"
        secret: "${MKQD_SIGNER_SECRET}"
        timeout: 5s
```

The endpoint receives the same HMAC headers as an HTTP dispatch
(`v1:<timestamp>:<body>`, see the dispatch contract above) and answers:

```
POST /_mkqd/sign
{"keyId":"https://local.example/users/me#main-key",
 "signingString":"<base64>","algorithm":"rsa-sha256"}

200 {"keyId":"...","algorithm":"rsa-sha256","signature":"<base64>"}
```

The string to sign is base64 because it contains newlines, and a
round trip through JSON should not be able to change a single byte of
it. An empty `keyId` asks for the default key, and the response names
the key that was used.

Answer **410 Gone** for a key that does not exist: the delivery then
fails permanently instead of retrying for something that will never
appear. A **404 counts only when it carries a JSON body** — a bare 404
is what a typo in `signer.url`, a route that is not mounted yet, or a
proxy that does not forward the path all return, and discarding every
queued activity on one of those would leave nothing to recover once the
configuration is fixed. Any other failure — 5xx, a timeout — leaves the
delivery retryable, so a signer having a bad minute does not cost
activities.
Because the endpoint is the operator's own rather than payload-supplied,
loopback is allowed by default here — which also means an environment
proxy applies (see the proxy note above). Loopback is exempt from Go's
proxy rules, but a signer URL like `http://app.internal:3000/_mkqd/sign`
with `HTTP_PROXY` set would route signing requests, and the signatures
they return, through that proxy.

Leave `secret` out and mkqd signs nothing and logs a warning: an
unauthenticated signing endpoint will sign anything for anyone who can
reach it, which on a shared host means any local process can
impersonate any actor. Write it as `"${MKQD_SIGNER_SECRET}"` without a
`:-` fallback so an unset variable fails at startup rather than
silently disabling authentication.

## Docker

```sh
docker build -t mkqd:dev .
docker run --rm -v "$PWD/mkqd.yaml:/etc/mkqd/mkqd.yaml:ro" mkqd:dev
```

Distroless static base, non-root (uid 65532), one binary, ~23MB. The
image reads `MKQD_CONFIG` (default `/etc/mkqd/mkqd.yaml`), so every
subcommand works without repeating `-c`:

```sh
docker run --rm -v "$PWD/mkqd.yaml:/etc/mkqd/mkqd.yaml:ro" mkqd:dev counts
```

Pass `--build-arg VERSION=1.2.3` to stamp `mkqd version`; without it the
binary keeps the in-development marker from `version.go`.

### Compose

`deploy/` brings up Redis, mkqd and **bull-board** together:

```sh
cd deploy
docker compose up --build
docker compose run --rm mkqd enqueue smoke '{"hello":"world"}'
open http://127.0.0.1:3000
```

bull-board is there to be checked, not decorated with. It is the stock
BullMQ dashboard reading the same Redis keys mkqd writes — if mkqd ever
drifted from BullMQ's wire format, the stack would stop rendering.

**`stop_grace_period` is `shutdown_timeout` + 5s**, for the reason in
[Shutdown](#shutdown): a SIGKILL landing during the unwind puts the job
back into stalled recovery, which is what the drain exists to prevent.
Compose's default of 10s is not enough.

## Embedded example

`examples/embedded` is the README's opening snippet as something you can
run — a typed handler, a producer and the runtime, against a throwaway
key prefix:

```sh
go run ./examples/embedded
```

## Development

```sh
go test ./... -race          # needs Redis on 127.0.0.1:6379, or MKQD_TEST_REDIS_ADDR
go run ./cmd/mkqd check -c mkqd.example.yaml
golangci-lint run ./...      # CI gate; see .golangci.yml
```

`go run <pkg>@<version>` picks its toolchain from the tool's own `go`
directive, not this module's, so running a pinned tool that way needs
`GOTOOLCHAIN=go$(awk '/^go [0-9]/ {print $2; exit}' go.mod)`. The
workflows do this already.

Contributions follow `CLAUDE.md`: branch off `develop`, one issue per
change, no direct commits to `main`.

## License

MIT. See `LICENSE`.
