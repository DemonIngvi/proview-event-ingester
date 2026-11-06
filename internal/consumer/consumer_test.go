package consumer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/DemonIngvi/proview-event-ingester/internal/record"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

func value(i int) []byte {
	return fmt.Appendf(nil, `{"v":1,"event_id":"i%d:0","type":"impression","impression_id":"i%d","banner_id":1,"placement":"p","event_ts":1794000000000,"received_ts":1794000000000}`, i, i)
}

// fakeSource hands out queued records and records commits.
type fakeSource struct {
	mu        sync.Mutex
	queue     []*kgo.Record
	committed []*kgo.Record
	allows    int
	commitErr error
}

func (f *fakeSource) Poll(ctx context.Context, max int) ([]*kgo.Record, error) {
	f.mu.Lock()
	n := min(max, len(f.queue))
	out := f.queue[:n]
	f.queue = f.queue[n:]
	f.mu.Unlock()
	if n == 0 {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return out, nil
}

func (f *fakeSource) Commit(_ context.Context, recs []*kgo.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.commitErr != nil {
		return f.commitErr
	}
	f.committed = append(f.committed, recs...)
	return nil
}

func (f *fakeSource) AllowRebalance() { f.mu.Lock(); f.allows++; f.mu.Unlock() }

func (f *fakeSource) push(n int, start int64, partition int32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range n {
		f.queue = append(f.queue, &kgo.Record{Topic: "t", Partition: partition, Offset: start + int64(i), Value: value(int(start) + i)})
	}
}

type fakeSink struct {
	mu       sync.Mutex
	failures int
	batches  [][]record.Row
	tokens   []string
}

func (s *fakeSink) Insert(_ context.Context, token string, rows []record.Row) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokens = append(s.tokens, token)
	if s.failures > 0 {
		s.failures--
		return errors.New("clickhouse unavailable")
	}
	s.batches = append(s.batches, rows)
	return nil
}

type fakeDLQ struct {
	mu   sync.Mutex
	recs []*kgo.Record
}

func (d *fakeDLQ) Send(_ context.Context, r *kgo.Record, _ error) error {
	d.mu.Lock()
	d.recs = append(d.recs, r)
	d.mu.Unlock()
	return nil
}

func newConsumer(src Source, sink Sink, dlq DeadLetter, batch int) *Consumer {
	return New(Config{BatchSize: batch, FlushInterval: 50 * time.Millisecond, MaxBackoff: 20 * time.Millisecond, ShutdownTimeout: time.Second},
		src, sink, dlq, slog.New(slog.NewTextHandler(io.Discard, nil)), prometheus.NewRegistry())
}

func runFor(t *testing.T, c *Consumer, d time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	c.Run(ctx)
}

func TestBatchesBySizeAndCommitsAfterInsert(t *testing.T) {
	src, sink, dlq := &fakeSource{}, &fakeSink{}, &fakeDLQ{}
	src.push(25, 0, 0)
	runFor(t, newConsumer(src, sink, dlq, 10), 300*time.Millisecond)

	if len(sink.batches) != 3 || len(sink.batches[0]) != 10 || len(sink.batches[2]) != 5 {
		t.Fatalf("batches = %d (%v)", len(sink.batches), sink.tokens)
	}
	if len(src.committed) != 25 {
		t.Fatalf("committed = %d, want 25", len(src.committed))
	}
	if src.allows < 3 {
		t.Fatalf("AllowRebalance called %d times", src.allows)
	}
}

func TestRetriesInsertWithSameToken(t *testing.T) {
	src, sink := &fakeSource{}, &fakeSink{failures: 3}
	src.push(5, 100, 2)
	runFor(t, newConsumer(src, sink, &fakeDLQ{}, 100), 400*time.Millisecond)

	if len(sink.batches) != 1 || len(sink.tokens) != 4 {
		t.Fatalf("batches=%d attempts=%d", len(sink.batches), len(sink.tokens))
	}
	for _, tok := range sink.tokens {
		if tok != sink.tokens[0] {
			t.Fatal("dedup token changed between retries")
		}
	}
	if len(src.committed) != 5 {
		t.Fatalf("committed = %d", len(src.committed))
	}
}

func TestNoCommitWhenInsertNeverSucceeds(t *testing.T) {
	src, sink := &fakeSource{}, &fakeSink{failures: 1 << 30}
	src.push(3, 0, 0)
	runFor(t, newConsumer(src, sink, &fakeDLQ{}, 100), 200*time.Millisecond)
	if len(src.committed) != 0 {
		t.Fatalf("committed %d records without a successful insert", len(src.committed))
	}
}

func TestInvalidRecordsGoToDLQ(t *testing.T) {
	src, sink, dlq := &fakeSource{}, &fakeSink{}, &fakeDLQ{}
	src.push(2, 0, 0)
	src.mu.Lock()
	src.queue = append(src.queue, &kgo.Record{Topic: "t", Offset: 2, Value: []byte("garbage")})
	src.mu.Unlock()
	runFor(t, newConsumer(src, sink, dlq, 100), 200*time.Millisecond)

	if len(dlq.recs) != 1 || len(sink.batches) != 1 || len(sink.batches[0]) != 2 {
		t.Fatalf("dlq=%d batches=%v", len(dlq.recs), sink.batches)
	}
	if len(src.committed) != 3 {
		t.Fatalf("invalid record offset not committed: %d", len(src.committed))
	}
}

func TestDedupToken(t *testing.T) {
	a := []*kgo.Record{{Topic: "t", Partition: 0, Offset: 5}, {Topic: "t", Partition: 1, Offset: 9}, {Topic: "t", Partition: 0, Offset: 7}}
	b := []*kgo.Record{{Topic: "t", Partition: 1, Offset: 9}, {Topic: "t", Partition: 0, Offset: 7}, {Topic: "t", Partition: 0, Offset: 5}}
	if DedupToken(a) != DedupToken(b) {
		t.Fatal("token depends on record order")
	}
	c := []*kgo.Record{{Topic: "t", Partition: 0, Offset: 5}, {Topic: "t", Partition: 1, Offset: 10}}
	if DedupToken(a) == DedupToken(c) {
		t.Fatal("different offset ranges produced the same token")
	}
}
