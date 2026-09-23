# Changelog

All notable changes to mkqd are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- mkq 1.1.0.

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
- `ExecutorConfig.DecodeStrict`, so a typo in an executor's options
  fails at startup instead of being ignored.
- Naming a built-in executor type whose package is not linked now
  reports which import to add.
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
