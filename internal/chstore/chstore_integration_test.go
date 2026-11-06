//go:build integration

package chstore

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/DemonIngvi/proview-event-ingester/internal/record"
)

// PROVIEW_TEST_CLICKHOUSE=localhost:9000 go test -tags integration ./internal/chstore/
func TestMigrateInsertDedupAggregate(t *testing.T) {
	addr := os.Getenv("PROVIEW_TEST_CLICKHOUSE")
	if addr == "" {
		t.Skip("PROVIEW_TEST_CLICKHOUSE not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	s, err := Open(ctx, Config{Addrs: strings.Split(addr, ","), Username: "default", Password: os.Getenv("PROVIEW_TEST_CLICKHOUSE_PASSWORD")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, q := range []string{"DROP DATABASE IF EXISTS proview_agg SYNC", "DROP DATABASE IF EXISTS proview SYNC"} {
		if err := s.conn.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		if err := s.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
	}

	ts := time.Date(2026, 11, 6, 10, 15, 30, 0, time.UTC).UnixMilli()
	rows := []record.Row{
		{EventID: "a:0", Type: "impression", ImpressionID: "a", BannerID: 1, Placement: "home_top", EventTS: ts, ReceivedTS: ts, VisitorID: "v1", Country: "RU", Device: "desktop"},
		{EventID: "b:0", Type: "impression", ImpressionID: "b", BannerID: 1, Placement: "home_top", EventTS: ts, ReceivedTS: ts, VisitorID: "v2", Country: "RU", Device: "desktop"},
		{EventID: "b:1", Type: "click", ImpressionID: "b", BannerID: 1, Placement: "home_top", EventTS: ts, ReceivedTS: ts, VisitorID: "v2", Country: "RU", Device: "desktop"},
		{EventID: "b:2", Type: "hover", ImpressionID: "b", BannerID: 1, Placement: "home_top", EventTS: ts, ReceivedTS: ts, DurationMS: 800, VisitorID: "v2", Country: "RU", Device: "desktop"},
	}
	// The same batch inserted twice with the same token must be stored once.
	for range 2 {
		if err := s.Insert(ctx, "batch-1", rows); err != nil {
			t.Fatal(err)
		}
	}

	var raw uint64
	if err := s.conn.QueryRow(ctx, "SELECT count() FROM proview.events_raw").Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if raw != 4 {
		t.Fatalf("raw rows = %d, want 4 (dedup failed)", raw)
	}
	for _, table := range []string{"banner_stats_1m", "banner_stats_1h"} {
		var impressions, clicks, hoverMS, visitors uint64
		err := s.conn.QueryRow(ctx, `SELECT sumIf(events, event_type='impression'), sumIf(events, event_type='click'),
			sumIf(duration_ms, event_type='hover'), uniqMergeIf(visitors, event_type='impression')
			FROM proview_agg.`+table).Scan(&impressions, &clicks, &hoverMS, &visitors)
		if err != nil {
			t.Fatal(err)
		}
		if impressions != 2 || clicks != 1 || hoverMS != 800 || visitors != 2 {
			t.Fatalf("%s: impressions=%d clicks=%d hover=%d visitors=%d", table, impressions, clicks, hoverMS, visitors)
		}
	}
}
