package producer

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestSlogLevelMapping(t *testing.T) {
	cases := []struct {
		in   kgo.LogLevel
		want slog.Level
	}{
		{kgo.LogLevelError, slog.LevelError},
		{kgo.LogLevelWarn, slog.LevelWarn},
		{kgo.LogLevelInfo, slog.LevelInfo},
		{kgo.LogLevelDebug, slog.LevelDebug},
	}
	for _, tc := range cases {
		if got := slogLevel(tc.in); got != tc.want {
			t.Errorf("slogLevel(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestKgoLoggerRoutesToSlog(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	kl := newKgoLogger(logger)

	kl.Log(kgo.LogLevelError, "broker unreachable", "addr", "kafka:9092")

	out := buf.String()
	if !strings.Contains(out, "broker unreachable") || !strings.Contains(out, "addr=kafka:9092") {
		t.Errorf("log output missing message or attrs: %q", out)
	}
	if !strings.Contains(out, "level=ERROR") {
		t.Errorf("log output not at ERROR level: %q", out)
	}
}
