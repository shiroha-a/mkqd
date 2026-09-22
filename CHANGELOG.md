# Changelog

All notable changes to mkqd are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
