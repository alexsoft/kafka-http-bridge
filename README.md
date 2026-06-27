# kafka-http-bridge

Produce to Kafka over plain HTTP. `POST` a body to a topic URL; the bridge
forwards the body to Kafka **verbatim**, waits for an all-ISR ack (`acks=all`),
and returns the partition and offset it landed on. The HTTP status reflects the
real produce result — no fire-and-forget.

```
POST /topics/orders/messages   ──▶   topic "orders", value = request body
X-Kafka-Key: customer-42                       key = "customer-42"
{"id": 99, "total": 12.50}     ◀──   200 {"topic":"orders","partition":1,"offset":847}
```

Intentionally minimal: no auth, no batching, no header→record-header mapping.
One HTTP request → one Kafka record, synchronously acked. It exists so services
that only speak HTTP (scripts, webhooks, languages without a good Kafka client)
can still produce reliably.

## Quick start

```bash
# 1. bring up a local 3-broker cluster (+ Kafbat UI on :8080)
docker compose up -d kafka1 kafka2 kafka3

# 2. run the bridge (8090 avoids colliding with the UI on 8080)
BRIDGE_PORT=8090 go run ./cmd/app

# 3. topics are never auto-created — make one. kafka1:19091 is the in-network
#    listener, reachable from inside any broker container.
docker compose exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:19091 --create --topic demo \
  --partitions 3 --replication-factor 3

# 4. produce
curl -X POST localhost:8090/topics/demo/messages \
  -H 'X-Kafka-Key: k1' --data-raw 'hello world'
# → {"topic":"demo","partition":0,"offset":0}
```

`docker compose up -d` with no service args also starts the **Kafbat UI** at
<http://localhost:8080> for eyeballing what landed. The bridge's default
`localhost:9092` reaches the `kafka2` host listener.

## API

### `POST /topics/{topic}/messages`

| | |
|---|---|
| **Topic** | from the URL path |
| **Key** | `X-Kafka-Key` header (optional). Absent → nil key, partitioner picks. |
| **Value** | the request body, sent unchanged. No content-type is assumed. |

| Status | Meaning |
|---|---|
| `200` | acked by all in-sync replicas → `{"topic","partition","offset"}` |
| `400` | empty topic, or the body could not be read |
| `404` | unknown topic/partition — the bridge does **not** auto-create topics |
| `413` | body over `BRIDGE_MAX_BODY_BYTES`, or Kafka rejected it as too large |
| `502` | produce failed after retries (e.g. cluster unreachable) |
| `504` | produce exceeded `KAFKA_PRODUCE_TIMEOUT` |

Errors come back as `{"error": "..."}`. Because this is an internal bridge the
message keeps the underlying broker detail to aid debugging — return a generic
message to clients before exposing it to untrusted callers.

### `GET /health`
Liveness — `200 {"status":"ok"}` while the process runs. Never touches Kafka.

### `GET /ready`
Readiness — pings the cluster: `200 {"status":"ready"}` if reachable, else `503`.
Wire this to your orchestrator's readiness probe; `/health` to liveness.

> **Two things that bite people:**
> - **Topics must already exist.** Producing to an unknown topic is a `404`, not
>   an auto-create. Create topics out of band first.
> - **Delivery is at-least-once.** Idempotent produce stops broker-side dupes,
>   but a client that times out and retries its `POST` can still create a
>   duplicate (or a record that lands after it gave up). Make consumers dedupe.

## Configuration

All via environment variables. Invalid values (bad port, unparseable duration,
empty broker list) fail fast at startup with a descriptive error.

| Var | Default | Purpose |
|---|---|---|
| `BRIDGE_HOST` | `0.0.0.0` | listen host |
| `BRIDGE_PORT` | `8080` | listen port |
| `KAFKA_BROKERS` | `localhost:9092` | comma-separated bootstrap brokers |
| `KAFKA_PRODUCE_RETRIES` | `2` | retry attempts on retriable errors |
| `KAFKA_PRODUCE_TIMEOUT` | `10s` | per-record delivery deadline → `504` |
| `HTTP_READ_TIMEOUT` | `15s` | server read timeout |
| `HTTP_WRITE_TIMEOUT` | `15s` | server write timeout |
| `SHUTDOWN_TIMEOUT` | `10s` | graceful drain on SIGINT/SIGTERM |
| `BRIDGE_MAX_BODY_BYTES` | `1048576` (1 MiB) | max request body; larger → `413` |

> **Raising `BRIDGE_MAX_BODY_BYTES`** above ~1 MiB requires raising the
> broker/topic `max.message.bytes` *and* franz-go's batch limit in step —
> otherwise oversized requests pass the HTTP check and fail at produce time.

## How it's built

Wiring lives in [`cmd/app/main.go`](cmd/app/main.go):
`config.Load()` → `producer.New()` → `server.New()` → `http.Server` with
signal-driven graceful shutdown.

```
HTTP client → [server] → Producer interface → [producer/franz-go] → Kafka
                  ↑                                    ↑
              config (env vars) ───────────────────────┘
                            injected in cmd/app/main.go
```

| Package | Responsibility |
|---|---|
| [`internal/config`](internal/config/) | `Config` + `Load()`: env vars, defaults, validation. Pure, unit-tested. |
| [`internal/server`](internal/server/) | HTTP handlers + routing (`net/http` Go 1.22+ method+path patterns). |
| [`internal/producer`](internal/producer/) | franz-go wrapper. Owns ack/retry/timeout/partitioner semantics. |

**The key seam:** `server.Producer` is an interface declared *in the `server`
package*; `*producer.Producer` satisfies it structurally. So `server` never
imports `producer` — `main.go` injects the concrete type, and handlers stay
unit-testable with a fake. When the HTTP layer needs a new producer method, add
it to the `server.Producer` interface, not just the concrete type.

### Design choices worth knowing before you change them

- **Synchronous produce.** `handleProduce` blocks on `ProduceSync` until the
  ack, so the HTTP status maps 1:1 to the produce outcome. The whole point.
- **Error → status mapping** lives in `server.produceErrorStatus`, switching on
  franz-go `kerr` sentinels: unknown topic → `404`, message too large → `413`,
  deadline → `504`, everything else → `502`. Keep client mistakes
  distinguishable from cluster outages.
- **`UniformBytesPartitioner`, not the default.** The default
  `StickyKeyPartitioner` would pin *keyless* records to one partition under this
  bridge's one-record-per-batch pattern. We hash keyed records (murmur2,
  Kafka-compatible) for per-key ordering and spread keyless ones. See the
  comment in `producer.New`.
- **Body cap before produce.** `http.MaxBytesReader` enforces
  `BRIDGE_MAX_BODY_BYTES` *before* anything reaches Kafka.
- **Bounded shutdown flush.** `Producer.Close` flushes within 5s so shutdown
  can't hang on unreachable brokers.

## Developing

```bash
go test ./...                 # unit tests — fast, no deps
go test -race ./...           # how CI runs them
go vet ./... && gofmt -l .    # vet + format check (should print nothing)
go run ./cmd/app              # run it
```

Integration tests round-trip a real message through Kafka and are gated behind
the `//go:build integration` tag so the default `go test ./...` stays
dependency-free:

```bash
docker compose -f compose.ci.yaml up -d --wait kafka   # single-broker, faster
go test -tags=integration ./internal/producer/         # creates a topic, produces, consumes back
```

CI ([`.github/workflows/test.yml`](.github/workflows/test.yml)) runs the unit
suite under `-race`, then brings up `compose.ci.yaml` and runs the integration
suite.

**Conventions:** standard library first (`net/http`, `log/slog` JSON logs);
franz-go is the only substantive dependency, with its diagnostics routed into
slog via the `kslog` plugin. TDD — failing test first. Small, scoped,
conventional-ish commits (`feat:`, `fix:`, `test:`, `docs:`).

## Further reading

- [CLAUDE.md](CLAUDE.md) — orientation for AI assistants working in the repo.
- `docs/superpowers/specs/` — the approved design and the *why* behind decisions.
- `docs/superpowers/plans/` — the task-by-task implementation plan.
