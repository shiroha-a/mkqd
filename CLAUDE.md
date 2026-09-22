# mkqd

Standalone worker and embeddable runtime for
[mkq](https://github.com/shiroha-a/mkq), aimed at Go ActivityPub
servers. mkqd owns process concerns — configuration, worker lifecycle,
shutdown, health and metrics, outbound delivery — and never the Redis
wire format.

## Relationship to mkq

- mkq is the library and the sole owner of the BullMQ-compatible Redis
  layout. mkqd depends on it as a published module and must not reach
  around its API to touch Redis keys that mkq manages.
- A gap that can only be closed inside mkq is filed as an issue there,
  not worked around here. Known ones are listed in `.tmp/design.md` §6.
- Auxiliary keys mkqd owns (should any be needed) use the empty
  queue-name slot `{prefix}::mkqd:...`, which BullMQ can never produce.

## Workflow

Issue → branch → Pull Request. No direct commits to `main` or
`develop`.

1. Open or pick up an issue describing the scope.
2. Branch naming: `feature/<summary>` or `fix/<summary>`; PRs target
   `develop`.
3. Small, reviewable commits. Confirm with the user before
   `git commit`.
4. Every PR ships a working vertical slice — no stub PRs.
5. PR format follows the user-global guideline (Summary / Key Changes /
   Testing / Related Issue).

## Go conventions

- Go version matches mkq (see `go.mod`). Generics required.
- `gofmt` and `goimports` before every commit.
- Godoc: English.
- Inline comments: English for mechanical clarifications tied to Go
  idioms; Japanese for design rationale and non-obvious "why" notes.
- No emojis anywhere in code, docs, or commit messages.

## Design rules

- One runtime, two front ends. The binary is a thin shell over the same
  `Runtime` an embedding application drives; behaviour must not
  diverge between them.
- Tuning belongs to the operator (YAML wins), execution belongs to the
  developer (a code handler beats a configured executor).
- Executors receive raw JSON. Do not decode a payload the executor does
  not need.
- Anything reaching the network gets a timeout, a response size cap,
  and — where the destination comes from a payload — an SSRF guard.

## Testing

- `go test ./... -race` against a real Redis (`MKQD_TEST_REDIS_ADDR`,
  default `127.0.0.1:6379`). Fakes are not a substitute: mkq's Lua
  scripts are part of what these tests exercise.
- Every new test is mutation-verified — break the code under test and
  confirm the test fails — before it is committed. A test that cannot
  fail is removed or rewritten, not kept.
