# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A small Go HTTP service that forwards request bodies to Kafka topics. Topic comes
from the URL path, optional key from the `X-Kafka-Key` header, body becomes the
Kafka value verbatim. Produces are **synchronous** — the handler blocks until an
all-ISR ack (`acks=all`), so the HTTP status reflects the real produce result.
Intentionally minimal: no auth, no batching, no header-to-record-header mapping.

## Commands

```bash
go test ./...                                    # unit tests — fast, no deps
go test -race ./...                              # how CI runs unit tests
go vet ./... && gofmt -l .                       # vet + format check (should be silent)
go run ./cmd/app                                 # run the service

# integration tests (need a live broker)
docker compose -f compose.ci.yaml up -d --wait kafka
go test -tags=integration ./internal/producer/   # round-trips a message through Kafka
go test -tags=integration -run TestName ./internal/producer/   # single integration test
```

Integration tests are gated behind the `//go:build integration` tag so the
default `go test ./...` stays dependency-free. CI (`.github/workflows/test.yml`)
runs unit tests, then starts `compose.ci.yaml` (single-broker) and runs the
integration suite.

## Architecture

Wiring lives in `cmd/app/main.go`: `config.Load()` → `producer.New()` →
`server.New()` → `http.Server` with signal-driven graceful shutdown.

```
HTTP client → server → Producer interface → producer/franz-go → Kafka
                 ↑
             config (env vars), injected in cmd/app/main.go
```

| Path | Responsibility |
|---|---|
| `internal/config/` | `Config` + `Load()`: env vars, defaults, validation. Pure, unit-tested. Bad values fail fast at startup. |
| `internal/server/` | HTTP handlers + routing (`net/http` Go 1.22+ method+path patterns). |
| `internal/producer/` | franz-go wrapper. Owns ack/retry/timeout/partitioner semantics. |

**Key seam:** `server.Producer` is an interface declared in the `server` package;
`*producer.Producer` satisfies it structurally. So `server` never imports
`producer` — `main.go` injects the concrete type, and handlers stay unit-testable
with a fake. When adding a producer method the HTTP layer needs, add it to the
`server.Producer` interface, not just the concrete type.

## Behaviors that aren't obvious from a single file

- **Error → HTTP status mapping** lives in `server.produceErrorStatus`. It
  inspects franz-go's `kerr` sentinels: unknown topic → `404`, message too large
  → `413`, deadline exceeded → `504`, everything else → `502`. When changing
  error handling, keep client mistakes distinguishable from cluster outages.
- **Topics are never auto-created** — producing to an unknown topic returns `404`.
  Tests and local runs must create topics out of band first (see README).
- **Partitioner is deliberately `UniformBytesPartitioner`, not the default.** The
  default `StickyKeyPartitioner` would pin keyless records to one partition under
  this bridge's one-record-per-batch synchronous pattern. See the comment in
  `producer.New` before changing it.
- **Body size cap** (`BRIDGE_MAX_BODY_BYTES`, default 1 MiB) is enforced via
  `http.MaxBytesReader` *before* produce. Raising it above ~1 MiB also requires
  raising the broker/topic `max.message.bytes` and franz-go's batch limit in step.
- **Delivery is at-least-once.** Idempotent produce prevents broker-side dup, but
  an HTTP client that times out and retries can still create a duplicate.

## Conventions

- Standard library first (`net/http`, `log/slog` JSON logs). franz-go is the only
  substantive dependency; its diagnostics are routed into slog via the `kslog`
  plugin in `producer.New`.
- TDD: write the failing test, watch it fail, implement, watch it pass, commit.
- Frequent, scoped, conventional-ish commits (`feat:`, `fix:`, `test:`, `docs:`).

## Further reading

- `README.md` — user-facing API, full config table, run instructions.
- `ONBOARDING.md` — deeper walkthrough plus known gaps / follow-ups.
- `docs/superpowers/specs/` — the approved design and the *why* behind decisions.
- `docs/superpowers/plans/` — the task-by-task implementation plan.
