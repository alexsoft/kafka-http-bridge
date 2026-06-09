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
