//go:build integration

package producer

import (
	"context"
	"fmt"
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
	createTopic(ctx, t, brokers, topic)

	p, err := New(brokers, 2, 10*time.Second)
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

// createTopic creates a single-partition topic and fails the test on error.
func createTopic(ctx context.Context, t *testing.T, brokers []string, topic string) {
	t.Helper()
	admClient, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	defer admClient.Close()

	resp, err := kadm.NewClient(admClient).CreateTopics(ctx, 1, 1, nil, topic)
	if err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if err := resp[topic].Err; err != nil {
		t.Fatalf("create topic %q: %v", topic, err)
	}
}
