# kafka-http-bridge — Design

**Date:** 2026-06-09
**Status:** Approved

## Purpose

A small HTTP service that runs in the background and lets other services
produce messages to Kafka over HTTP. The request body is forwarded to Kafka
as-is; the topic and key are supplied out-of-band (path + header). Produce
failures are retried a small number of times, and if they still fail the
client receives an error response.

## Architecture

A single Go binary built on the standard library (`net/http`; Go 1.26
`http.ServeMux` path patterns provide routing). Three small, independently
testable packages plus wiring in `main.go`:

- **`config`** — loads and validates settings from environment variables and
  returns a typed `Config`. Pure, unit-testable.
- **`producer`** — thin wrapper over a `franz-go` (`github.com/twmb/franz-go`)
  client. Single method `Produce(ctx, topic, key, value) (partition, offset, error)`.
  Owns ack and retry semantics. Verified by an integration test against the
  compose Kafka.
- **`server`** — HTTP handlers and routing. Depends on a `Producer` interface
  so handlers can be unit-tested with a fake.

`main.go` constructs config → producer → server, starts the HTTP server, and
performs graceful shutdown on SIGINT/SIGTERM.

## API

### `POST /topics/{topic}/messages`
Produce one message.

- **Topic**: from the `{topic}` path segment (RESTful resource).
- **Key**: from the `X-Kafka-Key` header (optional; absent → nil key, so the
  partitioner chooses the partition).
- **Value**: the raw request body, forwarded byte-for-byte. No content-type
  interpretation.

Responses:
- `200 OK` → JSON `{"topic": "...", "partition": N, "offset": N}` after the
  message is ISR-acknowledged.
- `400 Bad Request` → empty/invalid topic.
- `502 Bad Gateway` → produce failed after all retries (body includes error
  detail).

**Topics must already exist.** The bridge does not auto-create topics; producing
to an unknown topic returns `502` (`UNKNOWN_TOPIC_OR_PARTITION`). This is a
deliberate choice to prevent typos from spawning junk topics — operators
pre-create topics out of band.

### `GET /health`
Liveness. Returns `200` whenever the process is up. No external dependency
check.

### `GET /ready`
Readiness. Pings Kafka broker metadata; `200` if reachable, `503` otherwise.

## Delivery & Retries

- franz-go configured with **`acks=all`** (wait for all in-sync replicas).
- On retriable produce errors, retry **2 times** (configurable via
  `KAFKA_PRODUCE_RETRIES`), bounded by a per-request produce timeout
  (`KAFKA_PRODUCE_TIMEOUT`).
- The handler waits synchronously for the broker acknowledgment before
  responding. After retries are exhausted, an error response is returned.

## Configuration (environment variables)

| Var | Default | Purpose |
|---|---|---|
| `BRIDGE_HOST` | `0.0.0.0` | listen host |
| `BRIDGE_PORT` | `8080` | listen port |
| `KAFKA_BROKERS` | `localhost:9092` | comma-separated broker list |
| `KAFKA_PRODUCE_RETRIES` | `2` | retry attempts on retriable errors |
| `KAFKA_PRODUCE_TIMEOUT` | `10s` | per-request produce deadline |
| `HTTP_READ_TIMEOUT` | `15s` | server read timeout |
| `HTTP_WRITE_TIMEOUT` | `15s` | server write timeout |
| `SHUTDOWN_TIMEOUT` | `10s` | graceful drain on shutdown signal |

Invalid values (unparseable durations, non-numeric port, empty brokers) fail
fast at startup with a clear error.

Note: the Kafbat UI in `compose.yaml` connects to `kafka:29092` on the docker
network; the bridge connects to `localhost:9092` when run locally against the
compose stack.

## Testing

- **Unit — `config`**: defaults applied, env overrides honored, invalid values
  rejected.
- **Unit — `server`**: handlers exercised with a fake `Producer`:
  - success → `200` with correct JSON body
  - missing/empty topic → `400`
  - producer error → `502`
  - `X-Kafka-Key` parsed and passed through; nil key when header absent
  - raw body passed through unmodified
  - `/health` and `/ready` behavior
- **Integration — `producer`**: against the compose Kafka, produce a message
  and consume it back; assert value, key, and that a partition/offset were
  returned. Gated behind a build tag or env check so the default `go test` run
  stays fast and dependency-free.

## Out of Scope (YAGNI)

- Authentication / authorization.
- Mapping arbitrary HTTP headers to Kafka record headers (only
  `X-Kafka-Key` → key, body → value).
- Bulk/multi-message endpoints or batching API.
- TLS/SASL to Kafka (can be added to `config` + `producer` later if needed).
