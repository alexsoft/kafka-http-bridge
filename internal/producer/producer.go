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
		// Identify the bridge in broker logs, quotas, and metrics.
		kgo.ClientID("kafka-http-bridge"),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordRetries(retries),
		kgo.RecordDeliveryTimeout(timeout),
		// Hash keyed records (murmur2, Kafka-compatible) so a given key is
		// ordered on one partition, but spread keyless records across all
		// partitions. The default StickyKeyPartitioner pins keyless records to
		// a single partition under this bridge's synchronous one-record-per-
		// batch pattern; a low byte threshold forces a fresh pick per record.
		kgo.RecordPartitioner(kgo.UniformBytesPartitioner(1, false, true, nil)),
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

// Close flushes any buffered records and closes the underlying client. The
// flush is bounded so shutdown cannot hang if the brokers are unreachable.
func (p *Producer) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = p.client.Flush(ctx)
	p.client.Close()
}
