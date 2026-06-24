package producer

import (
	"context"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

// kgoLogger adapts the franz-go kgo.Logger interface onto an slog.Logger so
// the client's broker/connection/retry diagnostics flow into the same
// structured log as the rest of the service.
type kgoLogger struct {
	logger *slog.Logger
}

func newKgoLogger(l *slog.Logger) *kgoLogger {
	return &kgoLogger{logger: l}
}

// Level reports the maximum verbosity kgo should emit. Info keeps connection
// and metadata problems visible without the debug-level retry chatter.
func (k *kgoLogger) Level() kgo.LogLevel {
	return kgo.LogLevelInfo
}

func (k *kgoLogger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	k.logger.Log(context.Background(), slogLevel(level), msg, keyvals...)
}

func slogLevel(level kgo.LogLevel) slog.Level {
	switch level {
	case kgo.LogLevelError:
		return slog.LevelError
	case kgo.LogLevelWarn:
		return slog.LevelWarn
	case kgo.LogLevelDebug:
		return slog.LevelDebug
	default:
		return slog.LevelInfo
	}
}
