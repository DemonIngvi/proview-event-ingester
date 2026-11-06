package consumer

import (
	"context"
	"errors"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaSource adapts a franz-go group consumer to Source.
type KafkaSource struct {
	client *kgo.Client
}

// NewKafkaSource creates a group consumer with manual commits and blocked
// rebalances during polling (see package doc).
func NewKafkaSource(connOpts []kgo.Opt, topic, group string) (*KafkaSource, error) {
	opts := append([]kgo.Opt{
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(500 * time.Millisecond),
		kgo.FetchMinBytes(256 << 10),
		kgo.FetchMaxBytes(16 << 20),
		kgo.RebalanceTimeout(90 * time.Second),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
	}, connOpts...)
	c, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &KafkaSource{client: c}, nil
}

// Poll returns up to max records.
func (k *KafkaSource) Poll(ctx context.Context, max int) ([]*kgo.Record, error) {
	fs := k.client.PollRecords(ctx, max)
	if fs.IsClientClosed() {
		return fs.Records(), ErrClosed
	}
	var firstErr error
	fs.EachError(func(_ string, _ int32, err error) {
		if firstErr == nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			firstErr = err
		}
	})
	return fs.Records(), firstErr
}

// Commit synchronously commits the offsets of recs.
func (k *KafkaSource) Commit(ctx context.Context, recs []*kgo.Record) error {
	return k.client.CommitRecords(ctx, recs...)
}

// AllowRebalance lets a pending rebalance proceed.
func (k *KafkaSource) AllowRebalance() { k.client.AllowRebalance() }

// Ping checks broker connectivity.
func (k *KafkaSource) Ping(ctx context.Context) error { return k.client.Ping(ctx) }

// Close leaves the group and closes the client.
func (k *KafkaSource) Close() { k.client.Close() }

// KafkaDLQ publishes invalid messages to a dead-letter topic, keeping the
// original key/value and adding the error and origin as headers.
type KafkaDLQ struct {
	client *kgo.Client
	topic  string
}

// NewKafkaDLQ creates a dead-letter producer.
func NewKafkaDLQ(connOpts []kgo.Opt, topic string) (*KafkaDLQ, error) {
	c, err := kgo.NewClient(append([]kgo.Opt{kgo.DefaultProduceTopic(topic)}, connOpts...)...)
	if err != nil {
		return nil, err
	}
	return &KafkaDLQ{client: c, topic: topic}, nil
}

// Send writes r to the DLQ and waits for the acknowledgement.
func (d *KafkaDLQ) Send(ctx context.Context, r *kgo.Record, reason error) error {
	out := &kgo.Record{Topic: d.topic, Key: r.Key, Value: r.Value, Headers: []kgo.RecordHeader{
		{Key: "error", Value: []byte(reason.Error())},
		{Key: "origin_topic", Value: []byte(r.Topic)},
	}}
	return d.client.ProduceSync(ctx, out).FirstErr()
}

// Close flushes and closes the producer.
func (d *KafkaDLQ) Close() { d.client.Close() }
