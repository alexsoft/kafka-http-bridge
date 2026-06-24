# Review of PR #1 — "Implementation" (`implementation` → `main`, +1929/−0, 16 files)

## Overview

The PR adds the full kafka-http-bridge: a Go service exposing `POST /topics/{topic}/messages` (body → Kafka value, `X-Kafka-Key` header → key), plus `/health` and `/ready`. It's built on stdlib `net/http` with three internal packages (`config`, `producer` on franz-go, `server`), graceful shutdown in `main.go`, unit + integration tests, CI workflows (tests, zizmor, dependabot), compose stack, README, and the design/plan docs.

Overall this is a clean, well-tested PR. The findings below are mostly hardening and Kafka-semantics gaps, not correctness bugs.

## Security (internal service)

The CI side is genuinely good: actions pinned to SHAs, `persist-credentials: false`, `permissions: contents: read`, zizmor in pedantic mode, dependabot with cooldown. Few internal tools bother with this.

On the service itself:

- ~~**Unbounded request body (the one real issue).**~~ **Fixed** (commit `a0848ae`): `handleProduce` now wraps the body with `http.MaxBytesReader` and returns `413`, capped by `BRIDGE_MAX_BODY_BYTES` (default 1 MiB), with the raise-in-step caveat documented in the README. Original finding: `internal/server/server.go` (`handleProduce`) did `io.ReadAll(r.Body)` with no cap. Any client can send a multi-gigabyte body (or many concurrent large ones) and the bridge buffers it all in memory before Kafka rejects it. Wrap the body with `http.MaxBytesReader(w, r.Body, max)` and return `413`. Make the limit configurable (e.g. `BRIDGE_MAX_BODY_BYTES`) with a 1 MiB default to mirror Kafka's default `max.message.bytes` — the right cap is a property of the cluster, not the bridge. If the limit is raised above ~1 MB, franz-go's `ProducerBatchMaxBytes` and the broker/topic `max.message.bytes` must be raised in step, otherwise requests pass the HTTP check and fail at produce time; document that in the README. Even internally, this protects you from a misconfigured client, not just a malicious one.
- **Internal error details are echoed to clients.** The `502` and `503` bodies include `err.Error()` from franz-go, which can contain broker addresses and cluster internals. For an internal tool this is usually acceptable (and aids debugging), but consider logging the detail and returning a generic message if the network isn't fully trusted.
- **No auth and no topic allowlist** — explicitly out of scope per the design doc, which is fine internally, but note the consequence: anyone who can reach the port can write arbitrary bytes to *any existing topic*. A simple optional topic allowlist env var would be cheap insurance.
- **Minor:** `http.Server` sets read/write timeouts but no `IdleTimeout` (keep-alive connections fall back to `ReadTimeout`, so this is tolerable) and no `MaxHeaderBytes` (1 MB default is fine). `0.0.0.0` default bind is reasonable for a containerized internal service.

## Performance

- The franz-go client is created once and shared, so concurrent HTTP requests get batched together by the client — that's the right shape. Per-request latency is necessarily one acks=all round trip since the handler is synchronous by design.
- `WriteTimeout` (15s) > `ProduceTimeout` (10s) is the correct ordering — produce deadline fires before the HTTP layer kills the response.
- `r.Context()` is passed through to `ProduceSync`, so a disconnecting client cancels the wait. Good. (Note the record may still be delivered after the client gives up — see the duplicates point under Kafka.)
- The body-size cap from the security section is also the main performance fix: today memory per request is unbounded.
- `/ready` issues a real `Ping` per call. Fine for probe traffic; just don't point a high-frequency monitor at it.

No other performance concerns — this is a thin bridge and it reads like one.

## Code organization

This is the strongest area:

- Clean dependency direction: `server` depends on a narrow `Producer` interface, so handlers are unit-tested with a fake; `producer` owns all kgo specifics; `config` is pure. `main.go` is just wiring.
- Tests cover the right things: defaults/overrides/invalid config, all handler outcomes including nil-key semantics, and an integration round-trip that creates its own topic (matching the "topics must pre-exist" contract).
- Good doc comments, bounded flush on `Close()` so shutdown can't hang, README matches the actual behavior.

Minor notes:

- The `docs/superpowers/` plan and spec (≈1000 of the 1929 added lines) are committed into the repo. Keeping the design doc is valuable; the step-by-step implementation plan with full code listings will drift from reality immediately — consider dropping it or moving it out of the repo.
- `Produce` in the interface returns `(int32, int64, error)`; the named results in the interface declaration help, but a small `ProduceResult` struct would read better if you ever add fields (e.g. timestamp).
- PR body is empty — for a 16-file PR a summary would help reviewers.

## Kafka — what's missing

1. ~~**Error mapping: everything is `502`.**~~ **Fixed** (commit `a9cdf14`): `produceErrorStatus` maps `kerr.UnknownTopicOrPartition`→`404`, `kerr.MessageTooLarge`→`413`, and `context.DeadlineExceeded`→`504`; everything else stays `502`. Covered by `TestProduceErrorMapping`.
2. ~~**Duplicate semantics aren't stated.**~~ **Fixed** (commit `31d7351`): the README now documents at-least-once HTTP delivery (idempotent produce avoids broker-side dupes; client retries / canceled requests can still duplicate) and advises idempotent consumers.
3. **No Kafka record headers.** Only the key is forwarded. For a bridge, propagating correlation IDs / trace context as record headers (e.g. `X-Kafka-Header-*` → headers) is the most commonly needed extension; it's listed as YAGNI, but I'd expect it to be the first feature request.
4. ~~**No client ID.**~~ **Fixed** (commit `3ce4d9e`): `kgo.ClientID("kafka-http-bridge")` is set.
5. ~~**kgo's internal logger isn't wired.**~~ **Fixed** (commit `cca512f`): a `kgo.Logger`→slog adapter (`internal/producer/kgolog.go`, no extra dependency) is wired via `kgo.WithLogger`. Covered by `TestSlogLevelMapping` / `TestKgoLoggerRoutesToSlog`.
6. **No metrics.** Produce latency, error counts by type, and in-flight requests are the obvious Prometheus surface for this service; franz-go has hook interfaces (`HookProduceBatchWritten` etc.) that make this cheap. Fine to defer, but it's the next thing an internal platform team will ask for.
7. **Compression/linger left at client defaults** and not configurable — acceptable, just be aware the config surface stops at retries/timeout.
8. ~~**Verify the Kafbat UI actually connects.** `compose.yaml` points the UI at `kafka:29092`, but the stock `apache/kafka` image's default listeners/advertised listeners are `localhost:9092` — inside the compose network, `kafka:29092` likely doesn't resolve to a working listener without explicit `KAFKA_LISTENERS`/`KAFKA_ADVERTISED_LISTENERS` env on the kafka service. The bridge and CI only use `localhost:9092` so tests pass either way, but the UI may be silently broken.~~ **Resolved.** `compose.yaml` was reworked into a 3-broker KRaft cluster (`kafka1/2/3`) with explicit `KAFKA_LISTENERS`/`KAFKA_ADVERTISED_LISTENERS`; the UI now bootstraps off the internal advertised listeners (`kafka1:19091,kafka2:19092,kafka3:19093`), which match. No longer a concern.

## New findings (post-review changes)

- ~~**README drifted from the multi-broker compose rewrite (real, broken commands).**~~ **Fixed** (commit `31d7351`): the README now uses `kafka1/kafka2/kafka3`, creates topics via the in-network listener `kafka1:19091`, and notes the `compose.ci.yaml` single-broker split. Original finding: `compose.yaml` defines `kafka1/2/3` (no `kafka` service), but the README told users to run `docker compose up -d kafka` / `exec kafka …`, which fail with "no such service: kafka".

## Verdict

Solid, well-organized PR — approve with changes. Before merge I'd fix the unbounded body read (#1 in security) and the 502-for-everything error mapping (#1 in Kafka); the rest can be follow-ups.
