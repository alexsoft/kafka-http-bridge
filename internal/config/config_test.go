package config

import (
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
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
	if cfg.HTTPReadTimeout != 15*time.Second {
		t.Errorf("HTTPReadTimeout = %v, want 15s", cfg.HTTPReadTimeout)
	}
	if cfg.HTTPWriteTimeout != 15*time.Second {
		t.Errorf("HTTPWriteTimeout = %v, want 15s", cfg.HTTPWriteTimeout)
	}
	if cfg.ShutdownTimeout != 10*time.Second {
		t.Errorf("ShutdownTimeout = %v, want 10s", cfg.ShutdownTimeout)
	}
	if cfg.MaxBodyBytes != 1<<20 {
		t.Errorf("MaxBodyBytes = %d, want %d", cfg.MaxBodyBytes, 1<<20)
	}
}

func TestLoadMaxBodyBytes(t *testing.T) {
	t.Run("override", func(t *testing.T) {
		t.Setenv("BRIDGE_MAX_BODY_BYTES", "2048")
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if cfg.MaxBodyBytes != 2048 {
			t.Errorf("MaxBodyBytes = %d, want 2048", cfg.MaxBodyBytes)
		}
	})
	t.Run("not positive", func(t *testing.T) {
		t.Setenv("BRIDGE_MAX_BODY_BYTES", "0")
		if _, err := Load(); err == nil {
			t.Fatal("expected error for non-positive max body bytes")
		}
	})
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

func TestLoadEmptyBrokers(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", " , , ")
	if _, err := Load(); err == nil {
		t.Fatal("expected error for empty brokers list")
	}
}

func TestLoadRangeValidation(t *testing.T) {
	t.Run("port too low", func(t *testing.T) {
		t.Setenv("BRIDGE_PORT", "0")
		if _, err := Load(); err == nil {
			t.Fatal("expected error for port 0")
		}
	})
	t.Run("port too high", func(t *testing.T) {
		t.Setenv("BRIDGE_PORT", "70000")
		if _, err := Load(); err == nil {
			t.Fatal("expected error for port 70000")
		}
	})
	t.Run("negative retries", func(t *testing.T) {
		t.Setenv("KAFKA_PRODUCE_RETRIES", "-1")
		if _, err := Load(); err == nil {
			t.Fatal("expected error for negative retries")
		}
	})
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
