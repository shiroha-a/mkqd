# Changelog

All notable changes to mkqd are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-09-23

Honours a remote's `Retry-After`. Requires **mkq 1.3.0**.

### Added

- A remote's `Retry-After` now sets that job's next retry delay in place
  of the configured backoff. Both RFC 9110 forms are read (delta-seconds
  and HTTP-date), on any retryable response rather than 429 alone, since
  503 carries the header too. The delay is capped at one hour so a
  remote cannot park a job for a day per attempt — at any size, including
  values too large to hold as a duration — and the header never decides
  *whether* to retry: a permanent failure stays permanent.
  Applies to the `http`, `webhook` and `activitypub_deliver` executors.

- A `Retry-After` that was sent but could not be read is logged at debug
  level by the executor. Nothing reaches the runtime in that case, so
  this is the only trace of it.

- `RetryAfterError`, `RetryAfter` and `ParseRetryAfter` for custom
  executors to say the same thing. `RetryAfter` returns the error
  untouched for a non-positive delay, so callers need no guard.

### Changed

- Requires **mkq 1.3.0**, for `WithRetryDelayOverride`. The previous
  release built against mkq 1.1.1.

- The published image pins its build stage to the same Go patch release
  as `go.mod` (`golang:1.27.1`, not the floating `golang:1.27`). A
  floating tag changes under the build, and govulncheck reads `go.mod`
  rather than the Dockerfile — so a drift here would leave CI green
  while the shipped image went stale. CI now checks the two agree.

## [0.1.0] - 2026-09-23

First release. mkqd runs [mkq](https://github.com/shiroha-a/mkq) queues
as a standalone worker or as a runtime embedded in a Go application,
and owns the process concerns — configuration, worker lifecycle,
shutdown, health and metrics, outbound delivery — while mkq keeps
ownership of the BullMQ-compatible Redis layout.

Requires **Go 1.27**, matching mkq, and builds against **mkq 1.1.1**.
An application embedding mkqd needs the same toolchain: a module
declaring `go 1.27.1` cannot be built by an older one.

### Added

- Runtime: `New` / `NewFromFile` / `Start` / `Run` / `Shutdown` / `Check`,
  with signal handling and bounded graceful shutdown.

- Typed handler registration (`Handle[T]`) and producer access
  (`Producer[T]`) over mkq's generic API.

- Executor abstraction: `Executor`, type-erased `Job`, and a factory
  registry (`RegisterExecutor`) that configuration resolves against.
  Built-in `log` executor.

- YAML configuration with `${VAR}` / `${VAR:-default}` expansion over
  parsed values, strict unknown-field rejection, and validation.

- `ExecutorConfig.DecodeStrict`, so a typo in an executor's options
  fails at startup instead of being ignored.

- Naming a built-in executor type whose package is not linked now
  reports which import to add.

- Automatic Redis pool sizing from the declared queue concurrency, plus
  a startup warning when code-registered queues outgrow it.

- Health listener: `/healthz`, `/readyz`, `/metrics` (mkq's Prometheus
  adapter alongside the Go and process collectors).

- `mkqd` binary with `run`, `check` and `version` subcommands.

- `executor/httpexec`: the `http` executor, which forwards each job to a
  configured endpoint as a signed JSON envelope, and the `webhook`
  executor, which delivers a payload-supplied body to a payload-supplied
  URL. Both map response status onto retry / permanent failure, bound
  what a response may transfer (not only what is kept), refuse to follow
  redirects, reject headers HTTP cannot carry before a request is built,
  and sanitize job metadata on its way into `X-Mkqd-*` headers.

- `internal/safedial`: a dialer that rejects destinations outside the
  public internet, checked against the address actually being connected
  to so a re-resolving hostname cannot slip past. IPv6 forms that embed
  an IPv4 address are unwrapped and checked on the inner address. On by
  default for `webhook`, off by default for `http`; an environment proxy
  is honoured only when the guard is off, since a proxy would otherwise
  be all the guard ever sees.

- `httpsig`: draft-cavage HTTP Signature signing with a SHA-256 Digest,
  plus verification that also requires sufficient header coverage and
  checks the digest against the body. The `Signer` interface signs a
  string rather than surrendering a key, so an application can sign
  without its keys leaving the process.

- `executor/apdeliver`: the `activitypub_deliver` executor — signs,
  digests and delivers an activity to a remote inbox, classifies the
  outcome the way the fediverse expects, guards the payload-supplied
  destination, and refuses to follow redirects. Configured signers:
  `file` (one actor), `dir` (one PEM per key id, revalidated on use so an
  in-place key rotation is picked up without a restart) and `remote`,
  which asks the application to sign so that a multi-user server can run
  mkqd standalone without its keys ever leaving the application. A
  remote signer treats 410 (and a 404 the application authored) as a
  missing key, and everything else as retryable, so a signing endpoint
  that is merely unreachable cannot discard a queue.

- `mkqd keys` prints where a `dir` signer looks for a key id.

- Read-only inspect commands: `mkqd queues`, `mkqd counts`, `mkqd list`
  and `mkqd job`, each with `--json`. They open Redis but start no
  worker, so they are safe to point at a running deployment.

  `queues` uses mkq's `Client.DiscoverQueues`, so it reports queues
  created by BullMQ workers in other languages too, marking which ones
  this process is configured to work.

  **A queue name that does not exist is refused rather than created.**
  mkq's `Define` stamps `meta.version` and registers the name on first
  use, so a typo would otherwise leave a queue behind that shows up in
  every later listing. Names are checked against Redis first.

  `counts` prints a status column and no paused column: under BullMQ 6 a
  paused queue keeps its jobs in `wait`, so the two headings would show
  the same jobs and a reader adding the row up would double count. The
  JSON carries both numbers with that relationship documented.

- Admin commands: `mkqd enqueue`, `pause`, `resume`, `retry`, `promote`,
  `rm` and `drain`, each with `--json`.

  **`mkqd drain` is not the drain `mkqd run` does on shutdown.** That
  one lets in-flight jobs finish and deletes nothing; this one deletes
  the queued backlog. The names collide because the command follows
  mkq's `DrainPending`, so the command requires `--yes`, says which
  drain it is when refused, and prints what it actually removed:

  ```
  $ mkqd drain deliver --yes
  drained deliver: wait=120 prioritized=3
  ```

  It removes `wait`, `paused` and `prioritized`; `delayed` only with
  `-delayed`, and scheduler iterations survive even then. Active and
  finished jobs are untouched.

  `enqueue` refuses a queue that does not exist unless `--create` is
  passed, for the same reason the read-only commands do: a typo would
  otherwise bury the job in a queue nothing works. It is the only
  command allowed to create one.

- `Dockerfile`: distroless static, non-root (uid 65532), one binary at
  ~23MB. `MKQD_CONFIG` defaults to `/etc/mkqd/mkqd.yaml`, and
  `--build-arg VERSION=` stamps `mkqd version` at link time.

- `deploy/`: a compose stack of Redis, mkqd and bull-board. bull-board
  is the stock BullMQ dashboard pointed at the keys mkqd writes, so the
  stack is a check on the wire-format claim rather than an illustration
  of it.

  `stop_grace_period` is `shutdown_timeout` + 5s. A SIGKILL landing
  during the unwind puts the job back into stalled recovery, which is
  what the drain exists to prevent; compose's default of 10s is not
  enough.

  The dashboard declares `ioredis` explicitly: **BullMQ 6 moved it from
  a hard dependency to an optional peer dependency**, and npm does not
  install those on its own. Without it `new Queue(...)` fails at
  startup. mkq's own interop harness is affected too (shiroha-a/mkq#108).

- `examples/embedded`: the README's opening snippet as a runnable
  program, against a throwaway key prefix.

### Changed

- mkq 1.1.1, which fixes `ListJobs(ascending=true)` returning
  LIST-backed buckets newest-first. `mkqd list` pages oldest-first by
  default, so it was the caller that surfaced the bug.

- Shutdown drains instead of cancelling. Workers stop dequeueing and the
  handlers already running keep their context and their lock until they
  return on their own, using mkq 1.1.0's `Worker.Drain`.

  This is what `.tmp/design.md` §6 listed as upstream ask 3. Until mkq
  had the API, `Runtime.Shutdown` could only cancel, so a job cut
  mid-flight stayed locked until the BullMQ lock expired and was then
  redelivered by stalled detection. Correct per BullMQ's at-least-once
  contract, but for an ActivityPub delivery worker it meant re-sending
  in-flight posts on every deploy.

  A handler still running when `shutdown_timeout` expires is cancelled
  and then awaited on a separate five-second grace. That second wait is
  not optional: mkq's `Drain` returns as soon as it cancels, and the
  handler finalises its job after that, so closing Redis first would
  leave the job locked in `active` — exactly the redelivery the drain
  was meant to avoid.

  **Shutdown can now outlive the context it was given by up to five
  seconds.** Anything imposing an outer deadline has to budget for
  `shutdown_timeout` + 5s; under Kubernetes that is
  `terminationGracePeriodSeconds`, whose default of 30 is too small for
  the `shutdown_timeout: 30s` in the README's example config.

- Go 1.27.1, matching mkq. The `go` directive moves with it, so an
  application embedding mkqd needs a 1.27 toolchain: a module declaring
  `go 1.27.1` cannot be built by an older one.

[Unreleased]: https://github.com/shiroha-a/mkqd/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/shiroha-a/mkqd/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/shiroha-a/mkqd/releases/tag/v0.1.0
