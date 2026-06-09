# kafka-http-bridge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an HTTP service that forwards request bodies to Kafka topics, with topic from the URL path and key from a header, retrying failed produces before returning an error.

**Architecture:** A single Go binary on the standard library `net/http` server. Three focused packages under `internal/`: `config` (env parsing), `producer` (franz-go wrapper, owns acks/retries), and `server` (HTTP handlers depending on a `Producer` interface). `main.go` wires them and handles graceful shutdown.

**Tech Stack:** Go 1.26, `net/http` (stdlib routing via `ServeMux` path patterns), `github.com/twmb/franz-go` Kafka client, `log/slog` for logging.

**Module path:** `github.com/alexsoft/kafka-http-bridge`

---

## File Structure

- `internal/config/config.go` — `Config` struct + `Load()` reading env vars with defaults/validation.
- `internal/config/config_test.go` — unit tests for parsing.
- `internal/producer/producer.go` — franz-go wrapper: `New`, `Produce`, `Ready`, `Close`.
- `internal/producer/producer_integration_test.go` — integration test (build tag `integration`) against compose Kafka.
- `internal/server/server.go` — `Producer` interface, `Server`, handlers, routing.
- `internal/server/server_test.go` — unit tests with a fake producer.
- `main.go` — wiring + graceful shutdown.
- `README.md` — usage/run instructions.

---

## Task 1: Config package

**Files:**
- Create: `internal/config/config.go`
- Test: `internal/config/config_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/config/config_test.go`:

```go
package config

import (
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	// Ensure no relevant env vars are set.
	for _, k := range []string{
		"BRIDGE_HOST", "BRIDGE_PORT", "KAFKA_BROKERS",
		"KAFKA_PRODUCE_RETRIES", "KAFKA_PRODUCE_TIMEOUT",
		"HTTP_READ_TIMEOUT", "HTTP_WRITE_TIMEOUT", "SHUTDOWN_TIMEOUT",
	} {
		t.Setenv(k, "")
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Host != "0.0.0.0" {
		t.Errorf("Host = %q, want 0.0.0.0", cfg.Host)
	}
	if cfg.Port != 8080 {
		t.Errorf("Port = %d, want 8080", cfg.Port)
	}
	if len(cfg.Brokers) != 1 || cfg.Brokers[0] != "localhost:9092" {
		t.Errorf("Brokers = %v, want [localhost:9092]", cfg.Brokers)
	}
	if cfg.ProduceRetries != 2 {
		t.Errorf("ProduceRetries = %d, want 2", cfg.ProduceRetries)
	}
	if cfg.ProduceTimeout != 10*time.Second {
		t.Errorf("ProduceTimeout = %v, want 10s", cfg.ProduceTimeout)
	}
	if cfg.ShutdownTimeout != 10*time.Second {
		t.Errorf("ShutdownTimeout = %v, want 10s", cfg.ShutdownTimeout)
	}
}

func TestLoadOverrides(t *testing.T) {
	t.Setenv("BRIDGE_HOST", "127.0.0.1")
	t.Setenv("BRIDGE_PORT", "9000")
	t.Setenv("KAFKA_BROKERS", "a:9092, b:9092 ,c:9092")
	t.Setenv("KAFKA_PRODUCE_RETRIES", "5")
	t.Setenv("KAFKA_PRODUCE_TIMEOUT", "3s")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Host != "127.0.0.1" {
		t.Errorf("Host = %q", cfg.Host)
	}
	if cfg.Port != 9000 {
		t.Errorf("Port = %d", cfg.Port)
	}
	if len(cfg.Brokers) != 3 || cfg.Brokers[0] != "a:9092" || cfg.Brokers[2] != "c:9092" {
		t.Errorf("Brokers = %v", cfg.Brokers)
	}
	if cfg.ProduceRetries != 5 {
		t.Errorf("ProduceRetries = %d", cfg.ProduceRetries)
	}
	if cfg.ProduceTimeout != 3*time.Second {
		t.Errorf("ProduceTimeout = %v", cfg.ProduceTimeout)
	}
}

func TestLoadInvalid(t *testing.T) {
	t.Run("bad port", func(t *testing.T) {
		t.Setenv("BRIDGE_PORT", "abc")
		if _, err := Load(); err == nil {
			t.Fatal("expected error for invalid port")
		}
	})
	t.Run("bad duration", func(t *testing.T) {
		t.Setenv("KAFKA_PRODUCE_TIMEOUT", "notaduration")
		if _, err := Load(); err == nil {
			t.Fatal("expected error for invalid duration")
		}
	})
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/config/`
Expected: FAIL — `config.go` does not exist / `Load` undefined (build error).

- [ ] **Step 3: Write minimal implementation**

Create `internal/config/config.go`:

```go
// Package config loads bridge settings from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all runtime settings for the bridge.
type Config struct {
	Host             string
	Port             int
	Brokers          []string
	ProduceRetries   int
	ProduceTimeout   time.Duration
	HTTPReadTimeout  time.Duration
	HTTPWriteTimeout time.Duration
	ShutdownTimeout  time.Duration
}

// Load reads configuration from environment variables, applying defaults
// and returning an error if any value is malformed or required value missing.
func Load() (Config, error) {
	cfg := Config{
		Host:    getEnvStr("BRIDGE_HOST", "0.0.0.0"),
		Brokers: getEnvList("KAFKA_BROKERS", []string{"localhost:9092"}),
	}

	var err error
	if cfg.Port, err = getEnvInt("BRIDGE_PORT", 8080); err != nil {
		return Config{}, err
	}
	if cfg.ProduceRetries, err = getEnvInt("KAFKA_PRODUCE_RETRIES", 2); err != nil {
		return Config{}, err
	}
	if cfg.ProduceTimeout, err = getEnvDur("KAFKA_PRODUCE_TIMEOUT", 10*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.HTTPReadTimeout, err = getEnvDur("HTTP_READ_TIMEOUT", 15*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.HTTPWriteTimeout, err = getEnvDur("HTTP_WRITE_TIMEOUT", 15*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.ShutdownTimeout, err = getEnvDur("SHUTDOWN_TIMEOUT", 10*time.Second); err != nil {
		return Config{}, err
	}

	if len(cfg.Brokers) == 0 {
		return Config{}, fmt.Errorf("KAFKA_BROKERS must not be empty")
	}
	return cfg, nil
}

func getEnvStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvList(key string, def []string) []string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func getEnvInt(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: invalid integer %q: %w", key, v, err)
	}
	return n, nil
}

func getEnvDur(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: invalid duration %q: %w", key, v, err)
	}
	return d, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/config/`
Expected: PASS (ok github.com/alexsoft/kafka-http-bridge/internal/config)

- [ ] **Step 5: Commit**

```bash
git add internal/config/
git commit -m "feat: add config package with env-var loading"
```

---

## Task 2: Server package (handlers with fake producer)

**Files:**
- Create: `internal/server/server.go`
- Test: `internal/server/server_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/server/server_test.go`:

```go
package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fakeProducer is a test double for the Producer interface.
type fakeProducer struct {
	gotTopic string
	gotKey   []byte
	gotValue []byte
	produceErr error
	readyErr   error
}

func (f *fakeProducer) Produce(ctx context.Context, topic string, key, value []byte) (int32, int64, error) {
	f.gotTopic = topic
	f.gotKey = key
	f.gotValue = value
	if f.produceErr != nil {
		return 0, 0, f.produceErr
	}
	return 7, 42, nil
}

func (f *fakeProducer) Ready(ctx context.Context) error { return f.readyErr }

func newTestServer(p Producer) *Server {
	return New(p, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestProduceSuccess(t *testing.T) {
	fp := &fakeProducer{}
	srv := newTestServer(fp)

	req := httptest.NewRequest(http.MethodPost, "/topics/orders/messages", strings.NewReader("hello-body"))
	req.Header.Set("X-Kafka-Key", "order-1")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Topic     string `json:"topic"`
		Partition int32  `json:"partition"`
		Offset    int64  `json:"offset"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Topic != "orders" || resp.Partition != 7 || resp.Offset != 42 {
		t.Errorf("resp = %+v", resp)
	}
	if fp.gotTopic != "orders" {
		t.Errorf("gotTopic = %q", fp.gotTopic)
	}
	if string(fp.gotKey) != "order-1" {
		t.Errorf("gotKey = %q", fp.gotKey)
	}
	if string(fp.gotValue) != "hello-body" {
		t.Errorf("gotValue = %q", fp.gotValue)
	}
}

func TestProduceNoKeyMeansNilKey(t *testing.T) {
	fp := &fakeProducer{}
	srv := newTestServer(fp)

	req := httptest.NewRequest(http.MethodPost, "/topics/orders/messages", strings.NewReader("x"))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if fp.gotKey != nil {
		t.Errorf("gotKey = %v, want nil", fp.gotKey)
	}
}

func TestProduceEmptyTopic(t *testing.T) {
	fp := &fakeProducer{}
	srv := newTestServer(fp)

	// Route requires a topic segment; invoke handler directly with empty path value.
	req := httptest.NewRequest(http.MethodPost, "/topics//messages", strings.NewReader("x"))
	req.SetPathValue("topic", "")
	rec := httptest.NewRecorder()
	srv.handleProduce(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestProduceProducerError(t *testing.T) {
	fp := &fakeProducer{produceErr: errors.New("broker down")}
	srv := newTestServer(fp)

	req := httptest.NewRequest(http.MethodPost, "/topics/orders/messages", strings.NewReader("x"))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
}

func TestHealth(t *testing.T) {
	srv := newTestServer(&fakeProducer{})
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

func TestReady(t *testing.T) {
	t.Run("ready", func(t *testing.T) {
		srv := newTestServer(&fakeProducer{})
		req := httptest.NewRequest(http.MethodGet, "/ready", nil)
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
	})
	t.Run("not ready", func(t *testing.T) {
		srv := newTestServer(&fakeProducer{readyErr: errors.New("no kafka")})
		req := httptest.NewRequest(http.MethodGet, "/ready", nil)
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", rec.Code)
		}
	})
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/server/`
Expected: FAIL — `server.go` does not exist / `New`, `Server`, `Producer` undefined.

- [ ] **Step 3: Write minimal implementation**

Create `internal/server/server.go`:

```go
// Package server provides the HTTP API for the Kafka bridge.
package server

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
)

// Producer is the subset of Kafka behavior the HTTP layer depends on.
type Producer interface {
	Produce(ctx context.Context, topic string, key, value []byte) (partition int32, offset int64, err error)
	Ready(ctx context.Context) error
}

// Server holds dependencies for the HTTP handlers.
type Server struct {
	producer Producer
	logger   *slog.Logger
}

// New constructs a Server.
func New(p Producer, logger *slog.Logger) *Server {
	return &Server{producer: p, logger: logger}
}

// Handler returns the configured HTTP router.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /topics/{topic}/messages", s.handleProduce)
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /ready", s.handleReady)
	return mux
}

type produceResponse struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func (s *Server) handleProduce(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	if topic == "" {
		writeError(w, http.StatusBadRequest, "topic must not be empty")
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "failed to read request body")
		return
	}

	var key []byte
	if k := r.Header.Get("X-Kafka-Key"); k != "" {
		key = []byte(k)
	}

	partition, offset, err := s.producer.Produce(r.Context(), topic, key, body)
	if err != nil {
		s.logger.Error("produce failed", "topic", topic, "err", err)
		writeError(w, http.StatusBadGateway, "failed to produce message: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, produceResponse{Topic: topic, Partition: partition, Offset: offset})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if err := s.producer.Ready(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "kafka not ready: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/server/`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/server/
git commit -m "feat: add HTTP server with produce, health, and ready handlers"
```

---

## Task 3: Producer package (franz-go wrapper)

**Files:**
- Modify: `go.mod`, `go.sum` (add franz-go dependency)
- Create: `internal/producer/producer.go`
- Test: `internal/producer/producer_integration_test.go`

- [ ] **Step 1: Add the franz-go dependency**

Run:
```bash
go get github.com/twmb/franz-go/pkg/kgo@latest
```
Expected: `go.mod`/`go.sum` updated with `github.com/twmb/franz-go`.

- [ ] **Step 2: Write the implementation**

Create `internal/producer/producer.go`:

```go
// Package producer wraps a franz-go Kafka client for synchronous, acked produces.
package producer

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer synchronously produces records to Kafka, waiting for ISR acks.
type Producer struct {
	client *kgo.Client
}

// New creates a Producer. retries is the number of retry attempts on
// retriable errors; timeout bounds how long a record may take to be delivered.
func New(brokers []string, retries int, timeout time.Duration) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordRetries(retries),
		kgo.RecordDeliveryTimeout(timeout),
	)
	if err != nil {
		return nil, err
	}
	return &Producer{client: client}, nil
}

// Produce sends value to topic with the given key (nil key allowed) and waits
// for the broker acknowledgment, returning the assigned partition and offset.
func (p *Producer) Produce(ctx context.Context, topic string, key, value []byte) (int32, int64, error) {
	rec := &kgo.Record{Topic: topic, Key: key, Value: value}
	results := p.client.ProduceSync(ctx, rec)
	r, err := results.First()
	if err != nil {
		return 0, 0, err
	}
	return r.Partition, r.Offset, nil
}

// Ready reports whether the Kafka cluster is reachable.
func (p *Producer) Ready(ctx context.Context) error {
	return p.client.Ping(ctx)
}

// Close flushes and closes the underlying client.
func (p *Producer) Close() {
	p.client.Close()
}
```

- [ ] **Step 3: Verify it compiles**

Run: `go build ./internal/producer/`
Expected: no output (success).

- [ ] **Step 4: Write the integration test**

Create `internal/producer/producer_integration_test.go`:

```go
//go:build integration

package producer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Run with: docker compose up -d kafka && go test -tags=integration ./internal/producer/
func TestProduceAndConsume(t *testing.T) {
	brokers := []string{"localhost:9092"}
	topic := fmt.Sprintf("bridge-it-%d", time.Now().UnixNano())

	p, err := New(brokers, 2, 10*time.Second)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := p.Ready(ctx); err != nil {
		t.Fatalf("Ready: %v", err)
	}

	key := []byte("k1")
	value := []byte("hello-integration")
	part, off, err := p.Produce(ctx, topic, key, value)
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	if off < 0 {
		t.Fatalf("offset = %d", off)
	}

	// Consume the message back from the assigned partition.
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {part: kgo.NewOffset().At(off)},
		}),
	)
	if err != nil {
		t.Fatalf("consumer New: %v", err)
	}
	defer consumer.Close()

	fetches := consumer.PollFetches(ctx)
	if errs := fetches.Errors(); len(errs) > 0 {
		t.Fatalf("poll errors: %v", errs)
	}

	var got *kgo.Record
	fetches.EachRecord(func(r *kgo.Record) {
		if got == nil {
			got = r
		}
	})
	if got == nil {
		t.Fatal("no record consumed")
	}
	if string(got.Key) != string(key) {
		t.Errorf("key = %q, want %q", got.Key, key)
	}
	if string(got.Value) != string(value) {
		t.Errorf("value = %q, want %q", got.Value, value)
	}
}
```

- [ ] **Step 5: Run unit build and (optionally) the integration test**

Run (always): `go test ./internal/producer/`
Expected: PASS with `[no test files]`-style ok (integration test excluded without the tag).

Run (requires Kafka up — `docker compose up -d kafka`): `go test -tags=integration ./internal/producer/`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum internal/producer/
git commit -m "feat: add franz-go producer with acks=all and retries"
```

---

## Task 4: Wire up main.go with graceful shutdown

**Files:**
- Modify: `main.go`

- [ ] **Step 1: Write the implementation**

Replace the contents of `main.go`:

```go
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/alexsoft/kafka-http-bridge/internal/config"
	"github.com/alexsoft/kafka-http-bridge/internal/producer"
	"github.com/alexsoft/kafka-http-bridge/internal/server"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load()
	if err != nil {
		logger.Error("failed to load config", "err", err)
		os.Exit(1)
	}

	prod, err := producer.New(cfg.Brokers, cfg.ProduceRetries, cfg.ProduceTimeout)
	if err != nil {
		logger.Error("failed to create producer", "err", err)
		os.Exit(1)
	}
	defer prod.Close()

	srv := server.New(prod, logger)
	httpServer := &http.Server{
		Addr:         net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
		Handler:      srv.Handler(),
		ReadTimeout:  cfg.HTTPReadTimeout,
		WriteTimeout: cfg.HTTPWriteTimeout,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("starting server", "addr", httpServer.Addr, "brokers", cfg.Brokers)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server error", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received, draining")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "err", err)
	}
	logger.Info("stopped")
}
```

- [ ] **Step 2: Verify the whole module builds and all unit tests pass**

Run: `go build ./... && go vet ./... && go test ./...`
Expected: build succeeds, vet clean, all tests PASS.

- [ ] **Step 3: Commit**

```bash
git add main.go
git commit -m "feat: wire config, producer, and server with graceful shutdown"
```

---

## Task 5: Manual end-to-end verification + README

**Files:**
- Create: `README.md`

- [ ] **Step 1: Start the stack and run the bridge**

Run:
```bash
docker compose up -d kafka
go run . &
```
Wait ~2s for startup.

- [ ] **Step 2: Exercise the endpoints**

Run:
```bash
curl -s localhost:8080/health
curl -s localhost:8080/ready
curl -s -i -X POST localhost:8080/topics/demo/messages \
  -H 'X-Kafka-Key: k1' --data-raw 'hello world'
```
Expected: `/health` → `{"status":"ok"}`; `/ready` → `{"status":"ready"}`; produce → `200` with JSON `{"topic":"demo","partition":...,"offset":...}`.

Verify the message arrived via the Kafbat UI at http://localhost:8080 — note: the UI also binds 8080 in `compose.yaml`. If the bridge and UI collide on 8080, run the bridge on another port: `BRIDGE_PORT=8090 go run .` and adjust the curl commands. (Consider documenting this in the README.)

- [ ] **Step 3: Stop the background bridge**

Run: `kill %1` (or find the PID and stop it). Then `docker compose down` when finished.

- [ ] **Step 4: Write README**

Create `README.md`:

````markdown
# kafka-http-bridge

A small HTTP service that forwards request bodies to Kafka topics. The topic is
taken from the URL path, the (optional) key from a header, and the body is sent
to Kafka as-is. Failed produces are retried before an error is returned.

## API

### `POST /topics/{topic}/messages`
- Body: the message value, sent to Kafka unchanged.
- Header `X-Kafka-Key` (optional): message key. Absent → nil key.
- `200` → `{"topic","partition","offset"}` once acked by all in-sync replicas.
- `400` → empty topic or unreadable body.
- `502` → produce failed after retries.

### `GET /health`
Liveness — `200` while the process runs.

### `GET /ready`
Readiness — `200` if Kafka is reachable, else `503`.

## Configuration (environment variables)

| Var | Default | Purpose |
|---|---|---|
| `BRIDGE_HOST` | `0.0.0.0` | listen host |
| `BRIDGE_PORT` | `8080` | listen port |
| `KAFKA_BROKERS` | `localhost:9092` | comma-separated brokers |
| `KAFKA_PRODUCE_RETRIES` | `2` | retry attempts |
| `KAFKA_PRODUCE_TIMEOUT` | `10s` | per-record delivery deadline |
| `HTTP_READ_TIMEOUT` | `15s` | server read timeout |
| `HTTP_WRITE_TIMEOUT` | `15s` | server write timeout |
| `SHUTDOWN_TIMEOUT` | `10s` | graceful drain on SIGINT/SIGTERM |

## Running locally

```bash
docker compose up -d kafka      # start Kafka
BRIDGE_PORT=8090 go run .        # 8090 avoids colliding with the Kafbat UI on 8080
curl -X POST localhost:8090/topics/demo/messages -H 'X-Kafka-Key: k1' --data-raw 'hi'
```

The Kafbat UI is available at http://localhost:8080.

## Testing

```bash
go test ./...                              # unit tests
docker compose up -d kafka
go test -tags=integration ./internal/producer/   # integration test
```
````

- [ ] **Step 5: Commit**

```bash
git add README.md
git commit -m "docs: add README with API, config, and run instructions"
```

---

## Done

All spec requirements are covered:
- HTTP bridge running in the background → `main.go` server + graceful shutdown (Task 4).
- RESTful endpoint, body as-is → `POST /topics/{topic}/messages` (Task 2).
- Topic from path, key from header → `handleProduce` (Task 2).
- Env-var config with host/port/kafka/timeouts → `config` package (Task 1).
- Retry then error on failure → franz-go `RecordRetries` + `acks=all`, `502` on exhaustion (Tasks 2 & 3).
- Tests → unit (config, server) + integration (producer).
