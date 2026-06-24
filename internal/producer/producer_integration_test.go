//go:build integration

package producer

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Run with: docker compose up -d kafka && go test -tags=integration ./internal/producer/
func TestProduceAndConsume(t *testing.T) {
	brokers := []string{"localhost:9092"}
	topic := fmt.Sprintf("bridge-it-%d", time.Now().UnixNano())

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// The bridge requires topics to already exist (no auto-creation), so the
	// test creates the topic up front, mirroring real operational setup.
	createTopic(ctx, t, brokers, topic, 1)

	p, err := New(brokers, 2, 10*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer p.Close()

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

// TestKeylessRecordsSpreadAcrossPartitions verifies that records produced
// without a key are distributed across partitions rather than pinned to one.
// This guards the UniformBytesPartitioner configuration in New: the franz-go
// default StickyKeyPartitioner pins keyless records to a single partition
// under the bridge's synchronous one-record-per-batch produce pattern.
func TestKeylessRecordsSpreadAcrossPartitions(t *testing.T) {
	brokers := []string{"localhost:9092"}
	topic := fmt.Sprintf("bridge-it-spread-%d", time.Now().UnixNano())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const partitions = 3
	createTopic(ctx, t, brokers, topic, partitions)

	p, err := New(brokers, 2, 10*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer p.Close()

	// With a random per-record partitioner over 3 partitions, the probability
	// that all 30 keyless records land on one partition is (1/3)^29 — vanishing,
	// so asserting we hit >= 2 partitions is not flaky in practice.
	const n = 30
	seen := make(map[int32]struct{})
	for i := 0; i < n; i++ {
		part, _, err := p.Produce(ctx, topic, nil, []byte(fmt.Sprintf("msg-%d", i)))
		if err != nil {
			t.Fatalf("Produce %d: %v", i, err)
		}
		seen[part] = struct{}{}
	}

	if len(seen) < 2 {
		t.Errorf("keyless records landed on %d partition(s) %v, want >= 2 (records should spread)", len(seen), seen)
	}
}

// createTopic creates a topic with the given partition count (replication
// factor 1) and fails the test on error.
func createTopic(ctx context.Context, t *testing.T, brokers []string, topic string, partitions int32) {
	t.Helper()
	admClient, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	defer admClient.Close()

	resp, err := kadm.NewClient(admClient).CreateTopics(ctx, partitions, 1, nil, topic)
	if err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if err := resp[topic].Err; err != nil {
		t.Fatalf("create topic %q: %v", topic, err)
	}
}
