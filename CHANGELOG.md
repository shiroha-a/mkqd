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
