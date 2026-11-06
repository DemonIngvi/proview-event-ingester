// Command event-ingester consumes banner events from Kafka and writes them to
// ClickHouse in batches.
//
// Usage:
//
//	event-ingester          run the consumer (default)
//	event-ingester migrate  create/upgrade the ClickHouse schema and exit
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/DemonIngvi/proview-event-ingester/internal/chstore"
	"github.com/DemonIngvi/proview-event-ingester/internal/config"
	"github.com/DemonIngvi/proview-event-ingester/internal/consumer"
	"github.com/DemonIngvi/proview-event-ingester/internal/platform"
)

var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log := platform.NewLogger(platform.Env(os.LookupEnv, "LOG_LEVEL", "info"), "event-ingester").With("version", version)

	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		chCfg, err := config.LoadClickHouse(os.LookupEnv)
		if err != nil {
			return err
		}
		st, err := chstore.Open(ctx, chCfg)
		if err != nil {
			return err
		}
		defer func() { _ = st.Close() }()
		if err := st.Migrate(ctx); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
		log.Info("clickhouse migrations applied")
		return nil
	}

	cfg, err := config.Load(os.LookupEnv)
	if err != nil {
		return err
	}
	st, err := chstore.Open(ctx, cfg.ClickHouse)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	src, err := consumer.NewKafkaSource(cfg.Kafka.Opts(), cfg.Topic, cfg.Group)
	if err != nil {
		return err
	}
	dlq, err := consumer.NewKafkaDLQ(cfg.Kafka.Opts(), cfg.DLQTopic)
	if err != nil {
		return err
	}
	defer dlq.Close()

	reg := platform.NewRegistry()
	c := consumer.New(consumer.Config{
		BatchSize: cfg.BatchSize, FlushInterval: cfg.FlushInterval, MaxBackoff: cfg.MaxBackoff, ShutdownTimeout: cfg.ShutdownTimeout,
	}, src, st, dlq, log, reg)

	ready := &platform.Readiness{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		log.Info("consuming", "topic", cfg.Topic, "group", cfg.Group, "batch", cfg.BatchSize)
		c.Run(ctx)
	}()

	err = platform.Serve(ctx, log, ready, 0, cfg.ShutdownTimeout, platform.NewServer(cfg.OpsAddr, platform.OpsHandler(reg, ready)))
	<-done
	src.Close() // leave the group only after the last batch is committed
	log.Info("stopped")
	return err
}
