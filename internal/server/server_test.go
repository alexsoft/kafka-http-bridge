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
	gotTopic   string
	gotKey     []byte
	gotValue   []byte
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
