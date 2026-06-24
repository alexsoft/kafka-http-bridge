# kafka-http-bridge

A small HTTP service that forwards request bodies to Kafka topics. The topic is
taken from the URL path, the (optional) key from a header, and the body is sent
to Kafka as-is. Produces wait for acknowledgment from all in-sync replicas
(`acks=all`) and are retried before an error is returned.

## API

### `POST /topics/{topic}/messages`
- Body: the message value, sent to Kafka unchanged.
- Header `X-Kafka-Key` (optional): message key. Absent → nil key (the
  partitioner chooses the partition).
- `200` → `{"topic","partition","offset"}` once acked by all in-sync replicas.
- `400` → empty topic or unreadable body.
- `413` → request body exceeds `BRIDGE_MAX_BODY_BYTES`.
- `502` → produce failed after retries.

> **Topics must already exist.** The bridge does not auto-create topics —
> producing to an unknown topic returns `502`. Create topics out of band first.

### `GET /health`
Liveness — `200` `{"status":"ok"}` while the process runs.

### `GET /ready`
Readiness — `200` `{"status":"ready"}` if Kafka is reachable, else `503`.

## Configuration (environment variables)

| Var | Default | Purpose |
|---|---|---|
| `BRIDGE_HOST` | `0.0.0.0` | listen host |
| `BRIDGE_PORT` | `8080` | listen port |
| `KAFKA_BROKERS` | `localhost:9092` | comma-separated brokers |
| `KAFKA_PRODUCE_RETRIES` | `2` | retry attempts on retriable errors |
| `KAFKA_PRODUCE_TIMEOUT` | `10s` | per-record delivery deadline |
| `HTTP_READ_TIMEOUT` | `15s` | server read timeout |
| `HTTP_WRITE_TIMEOUT` | `15s` | server write timeout |
| `SHUTDOWN_TIMEOUT` | `10s` | graceful drain on SIGINT/SIGTERM |
| `BRIDGE_MAX_BODY_BYTES` | `1048576` (1 MiB) | max request body; larger → `413` |

> **Raising `BRIDGE_MAX_BODY_BYTES`** above ~1 MiB requires raising the
> broker/topic `max.message.bytes` (and franz-go's batch limit) in step —
> otherwise oversized requests pass the HTTP check and fail at produce time.

Invalid values (bad port range, unparseable durations, empty brokers) cause a
fast exit at startup with a clear error.

## Running locally

```bash
docker compose up -d kafka       # start Kafka (the Kafbat UI also uses 8080)
BRIDGE_PORT=8090 go run ./cmd/app # 8090 avoids colliding with the UI on 8080

# create a topic (the bridge does not auto-create)
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --topic demo \
  --partitions 1 --replication-factor 1

curl -X POST localhost:8090/topics/demo/messages \
  -H 'X-Kafka-Key: k1' --data-raw 'hello world'
# → {"topic":"demo","partition":0,"offset":0}
```

The full compose stack also starts the Kafbat UI at http://localhost:8080.

## Testing

```bash
go test ./...                                     # unit tests (fast, no deps)
docker compose up -d kafka
go test -tags=integration ./internal/producer/    # integration test (live Kafka)
```

The integration test creates its own topic, produces a message, and consumes it
back to verify the key and value round-trip.
