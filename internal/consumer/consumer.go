// Package consumer moves events from Kafka to ClickHouse in batches with
// at-least-once delivery:
//
//  1. poll until BatchSize records or FlushInterval elapsed (rebalances are
//     blocked meanwhile, so the batch never spans a partition handover);
//  2. decode; invalid messages go to the dead-letter topic;
//  3. insert the batch with a deterministic deduplication token, retrying
//     with backoff until it succeeds;
//  4. commit offsets, then allow rebalancing.
//
// A crash between 3 and 4 re-delivers the batch; the token makes the retry
// a no-op in ClickHouse when the batch boundaries are the same.
package consumer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/DemonIngvi/proview-event-ingester/internal/record"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ErrClosed is returned by Source.Poll when the client is closed.
var ErrClosed = errors.New("source closed")

// Source is the Kafka side (implemented by KafkaSource).
type Source interface {
	Poll(ctx context.Context, max int) ([]*kgo.Record, error)
	Commit(ctx context.Context, recs []*kgo.Record) error
	AllowRebalance()
}

// Sink stores rows (implemented by chstore.Store).
type Sink interface {
	Insert(ctx context.Context, dedupToken string, rows []record.Row) error
}

// DeadLetter receives undecodable messages.
type DeadLetter interface {
	Send(ctx context.Context, r *kgo.Record, reason error) error
}

// Config tunes batching.
type Config struct {
	BatchSize       int
	FlushInterval   time.Duration
	MaxBackoff      time.Duration
	ShutdownTimeout time.Duration
}

// Consumer runs the poll-insert-commit loop.
type Consumer struct {
	cfg  Config
	src  Source
	sink Sink
	dlq  DeadLetter
	log  *slog.Logger

	rows        *prometheus.CounterVec
	insertDur   prometheus.Histogram
	batchRows   prometheus.Histogram
	errors      *prometheus.CounterVec
	lastSuccess prometheus.Gauge
}

// New creates a Consumer and registers its metrics.
func New(cfg Config, src Source, sink Sink, dlq DeadLetter, log *slog.Logger, reg prometheus.Registerer) *Consumer {
	c := &Consumer{cfg: cfg, src: src, sink: sink, dlq: dlq, log: log,
		rows: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "proview_ingester", Name: "records_total", Help: "Kafka records by outcome.",
		}, []string{"outcome"}),
		insertDur: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "proview_ingester", Name: "insert_duration_seconds", Help: "ClickHouse batch insert latency.",
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 12),
		}),
		batchRows: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "proview_ingester", Name: "batch_rows", Help: "Rows per insert.",
			Buckets: prometheus.ExponentialBuckets(10, 2, 14),
		}),
		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "proview_ingester", Name: "errors_total", Help: "Errors by stage.",
		}, []string{"stage"}),
		lastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "proview_ingester", Name: "last_commit_timestamp_seconds", Help: "Unix time of the last committed batch.",
		}),
	}
	reg.MustRegister(c.rows, c.insertDur, c.batchRows, c.errors, c.lastSuccess)
	return c
}

// Run processes batches until ctx is cancelled. The batch in flight at
// shutdown is still written and committed within ShutdownTimeout.
func (c *Consumer) Run(ctx context.Context) {
	for {
		recs, closed := c.collect(ctx)
		if len(recs) > 0 {
			pctx := ctx
			var cancel context.CancelFunc = func() {}
			if ctx.Err() != nil {
				pctx, cancel = context.WithTimeout(context.WithoutCancel(ctx), c.cfg.ShutdownTimeout)
			}
			c.process(pctx, recs)
			cancel()
		}
		c.src.AllowRebalance()
		if closed || ctx.Err() != nil {
			return
		}
	}
}

func (c *Consumer) collect(ctx context.Context) (recs []*kgo.Record, closed bool) {
	deadline := time.Now().Add(c.cfg.FlushInterval)
	for len(recs) < c.cfg.BatchSize {
		remaining := time.Until(deadline)
		if remaining <= 0 || ctx.Err() != nil {
			return recs, false
		}
		pctx, cancel := context.WithTimeout(ctx, remaining)
		got, err := c.src.Poll(pctx, c.cfg.BatchSize-len(recs))
		cancel()
		recs = append(recs, got...)
		switch {
		case errors.Is(err, ErrClosed):
			return recs, true
		case err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled):
			c.errors.WithLabelValues("poll").Inc()
			c.log.Warn("poll error", "err", err)
		}
	}
	return recs, false
}

func (c *Consumer) process(ctx context.Context, recs []*kgo.Record) {
	rows := make([]record.Row, 0, len(recs))
	for _, r := range recs {
		row, err := record.Decode(r.Value)
		if err != nil {
			c.rows.WithLabelValues("invalid").Inc()
			c.retry(ctx, "dlq", func() error { return c.dlq.Send(ctx, r, err) })
			continue
		}
		rows = append(rows, row)
	}
	if len(rows) > 0 {
		token := DedupToken(recs)
		ok := c.retry(ctx, "insert", func() error {
			start := time.Now()
			err := c.sink.Insert(ctx, token, rows)
			c.insertDur.Observe(time.Since(start).Seconds())
			return err
		})
		if !ok {
			c.log.Warn("shutdown before batch was stored; it will be re-delivered", "rows", len(rows))
			return
		}
		c.batchRows.Observe(float64(len(rows)))
		c.rows.WithLabelValues("inserted").Add(float64(len(rows)))
	}
	if err := c.src.Commit(ctx, recs); err != nil {
		// Rows are stored; the batch may be re-delivered after a rebalance.
		c.errors.WithLabelValues("commit").Inc()
		c.log.Warn("offset commit failed", "err", err)
		return
	}
	c.lastSuccess.SetToCurrentTime()
}

// retry calls fn with exponential backoff until it succeeds or ctx is done.
func (c *Consumer) retry(ctx context.Context, stage string, fn func() error) bool {
	backoff := 100 * time.Millisecond
	for {
		err := fn()
		if err == nil {
			return true
		}
		c.errors.WithLabelValues(stage).Inc()
		c.log.Error(stage+" failed, retrying", "err", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return false
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, c.cfg.MaxBackoff)
	}
}

// DedupToken is derived from the offset ranges of the batch, so the same
// batch always yields the same token.
func DedupToken(recs []*kgo.Record) string {
	type span struct{ lo, hi int64 }
	spans := map[string]*span{}
	for _, r := range recs {
		k := fmt.Sprintf("%s/%d", r.Topic, r.Partition)
		s, ok := spans[k]
		if !ok {
			spans[k] = &span{r.Offset, r.Offset}
			continue
		}
		s.lo, s.hi = min(s.lo, r.Offset), max(s.hi, r.Offset)
	}
	keys := make([]string, 0, len(spans))
	for k := range spans {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "%s:%d-%d;", k, spans[k].lo, spans[k].hi)
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:16])
}
