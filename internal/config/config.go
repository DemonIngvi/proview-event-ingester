// Package config loads event-ingester configuration from the environment.
package config

import (
	"errors"
	"strconv"
	"time"

	"github.com/DemonIngvi/proview-event-ingester/internal/chstore"
	"github.com/DemonIngvi/proview-event-ingester/internal/kafkaopt"
	"github.com/DemonIngvi/proview-event-ingester/internal/platform"
)

// Config is the event-ingester configuration.
type Config struct {
	OpsAddr         string
	Kafka           kafkaopt.Config
	Topic           string
	DLQTopic        string
	Group           string
	ClickHouse      chstore.Config
	BatchSize       int
	FlushInterval   time.Duration
	MaxBackoff      time.Duration
	ShutdownTimeout time.Duration
	LogLevel        string
}

// LoadClickHouse reads only the ClickHouse settings (used by "migrate").
func LoadClickHouse(lookup func(string) (string, bool)) (chstore.Config, error) {
	c := chstore.Config{
		Addrs:    platform.EnvList(lookup, "CLICKHOUSE_ADDRS", nil),
		Username: platform.Env(lookup, "CLICKHOUSE_USER", "default"),
		Password: platform.Env(lookup, "CLICKHOUSE_PASSWORD", ""),
		TLS:      platform.Env(lookup, "CLICKHOUSE_TLS", "false") == "true",
		DBEngine: platform.Env(lookup, "CLICKHOUSE_DB_ENGINE", "Atomic"),
	}
	if len(c.Addrs) == 0 {
		return c, errors.New("CLICKHOUSE_ADDRS is required")
	}
	return c, nil
}

// Load reads the full configuration.
func Load(lookup func(string) (string, bool)) (Config, error) {
	c := Config{
		OpsAddr:  platform.Env(lookup, "OPS_ADDR", ":9090"),
		Topic:    platform.Env(lookup, "KAFKA_TOPIC", "proview.events.v1"),
		DLQTopic: platform.Env(lookup, "KAFKA_DLQ_TOPIC", "proview.events.dlq"),
		Group:    platform.Env(lookup, "KAFKA_GROUP", "proview-event-ingester"),
		LogLevel: platform.Env(lookup, "LOG_LEVEL", "info"),
	}
	var errs []error
	var err error
	if c.Kafka, err = kafkaopt.Load(lookup, "proview-event-ingester"); err != nil {
		errs = append(errs, err)
	}
	if c.ClickHouse, err = LoadClickHouse(lookup); err != nil {
		errs = append(errs, err)
	}
	if c.BatchSize, err = strconv.Atoi(platform.Env(lookup, "BATCH_SIZE", "20000")); err != nil || c.BatchSize < 1 {
		errs = append(errs, errors.New("BATCH_SIZE must be a positive integer"))
	}
	if c.FlushInterval, err = platform.EnvDuration(lookup, "FLUSH_INTERVAL", time.Second); err != nil {
		errs = append(errs, err)
	}
	if c.MaxBackoff, err = platform.EnvDuration(lookup, "MAX_BACKOFF", 30*time.Second); err != nil {
		errs = append(errs, err)
	}
	if c.ShutdownTimeout, err = platform.EnvDuration(lookup, "SHUTDOWN_TIMEOUT", 20*time.Second); err != nil {
		errs = append(errs, err)
	}
	return c, errors.Join(errs...)
}
