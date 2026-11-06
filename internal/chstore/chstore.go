// Package chstore writes event rows to ClickHouse and owns its schema.
package chstore

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/DemonIngvi/proview-event-ingester/internal/record"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Config describes the ClickHouse connection.
type Config struct {
	Addrs    []string
	Database string
	Username string
	Password string
	TLS      bool
	// DBEngine is used when creating the databases. "Atomic" for a single
	// node; Replicated('/clickhouse/databases/{db}', '{shard}', '{replica}')
	// on a cluster, which turns every MergeTree table into a replicated one.
	DBEngine string
}

// Store is a ClickHouse sink.
type Store struct {
	conn driver.Conn
	cfg  Config
}

// Open connects and pings ClickHouse.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	opts := &clickhouse.Options{
		Addr:             cfg.Addrs,
		Auth:             clickhouse.Auth{Database: "default", Username: cfg.Username, Password: cfg.Password},
		DialTimeout:      5 * time.Second,
		MaxOpenConns:     4,
		MaxIdleConns:     2,
		ConnMaxLifetime:  time.Hour,
		Compression:      &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
		ConnOpenStrategy: clickhouse.ConnOpenRoundRobin,
	}
	if cfg.TLS {
		opts.TLS = tlsConfig()
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, err
	}
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := conn.Ping(pctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	return &Store{conn: conn, cfg: cfg}, nil
}

// Close closes the connection pool.
func (s *Store) Close() error { return s.conn.Close() }

// Ping checks connectivity.
func (s *Store) Ping(ctx context.Context) error { return s.conn.Ping(ctx) }

const insertSQL = `INSERT INTO proview.events_raw
	(event_id, event_type, impression_id, banner_id, placement, event_time, received_at,
	 duration_ms, visitor_id, country, device, page_host)`

// Insert writes rows as one block. dedupToken makes a retried insert of the
// same batch idempotent (ClickHouse drops a block whose token it has seen
// within the deduplication window), including dependent materialized views.
func (s *Store) Insert(ctx context.Context, dedupToken string, rows []record.Row) error {
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"insert_deduplicate":                                 1,
		"insert_deduplication_token":                         dedupToken,
		"deduplicate_blocks_in_dependent_materialized_views": 1,
	}))
	batch, err := s.conn.PrepareBatch(ctx, insertSQL)
	if err != nil {
		return err
	}
	for i := range rows {
		r := &rows[i]
		if err := batch.Append(r.EventID, r.Type, r.ImpressionID, r.BannerID, r.Placement, r.EventTime(), r.ReceivedTime(),
			r.DurationMS, r.VisitorID, r.Country, r.Device, r.PageHost); err != nil {
			_ = batch.Abort()
			return err
		}
	}
	return batch.Send()
}

// Migrate creates the databases and applies embedded migrations in order.
// Every statement is idempotent (IF NOT EXISTS), so a re-run after a partial
// failure is safe even though ClickHouse DDL is not transactional.
func (s *Store) Migrate(ctx context.Context) error {
	engine := s.cfg.DBEngine
	if engine == "" {
		engine = "Atomic"
	}
	for _, db := range []string{"proview", "proview_agg"} {
		e := strings.ReplaceAll(engine, "{db}", db)
		if err := s.conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+db+" ENGINE = "+e); err != nil {
			return fmt.Errorf("create database %s: %w", db, err)
		}
	}
	if err := s.conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS proview.schema_migrations
		(version UInt32, name String, applied_at DateTime DEFAULT now()) ENGINE = MergeTree ORDER BY version`); err != nil {
		return err
	}
	applied := map[uint32]bool{}
	rows, err := s.conn.Query(ctx, "SELECT version FROM proview.schema_migrations")
	if err != nil {
		return err
	}
	for rows.Next() {
		var v uint32
		if err := rows.Scan(&v); err != nil {
			return err
		}
		applied[v] = true
	}
	if err := rows.Err(); err != nil {
		return err
	}

	files, err := fs.Glob(migrationsFS, "migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(files)
	for _, f := range files {
		name := strings.TrimPrefix(f, "migrations/")
		v64, err := strconv.ParseUint(strings.SplitN(name, "_", 2)[0], 10, 32)
		if err != nil {
			return fmt.Errorf("migration %s: bad version prefix", name)
		}
		version := uint32(v64)
		if applied[version] {
			continue
		}
		body, err := migrationsFS.ReadFile(f)
		if err != nil {
			return err
		}
		for _, stmt := range SplitStatements(string(body)) {
			if err := s.conn.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("apply %s: %w", name, err)
			}
		}
		if err := s.conn.Exec(ctx, "INSERT INTO proview.schema_migrations (version, name) VALUES (?, ?)", version, name); err != nil {
			return err
		}
	}
	return nil
}

// SplitStatements splits a migration file on ';' at end of line and drops
// comment-only chunks.
func SplitStatements(sql string) []string {
	var out []string
	var cur strings.Builder
	for _, line := range strings.Split(sql, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") || trimmed == "" {
			continue
		}
		cur.WriteString(line)
		cur.WriteByte('\n')
		if strings.HasSuffix(trimmed, ";") {
			stmt := strings.TrimSuffix(strings.TrimSpace(cur.String()), ";")
			out = append(out, stmt)
			cur.Reset()
		}
	}
	if rest := strings.TrimSpace(cur.String()); rest != "" {
		out = append(out, rest)
	}
	return out
}
