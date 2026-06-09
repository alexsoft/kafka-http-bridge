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
// and returning an error if any value is malformed or a required value is missing.
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
	if cfg.Port < 1 || cfg.Port > 65535 {
		return Config{}, fmt.Errorf("BRIDGE_PORT: %d out of range (1-65535)", cfg.Port)
	}
	if cfg.ProduceRetries < 0 {
		return Config{}, fmt.Errorf("KAFKA_PRODUCE_RETRIES: %d must not be negative", cfg.ProduceRetries)
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
