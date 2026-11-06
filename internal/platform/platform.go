// Package platform contains process-level plumbing shared by every ProView
// service: structured logging, the operational HTTP endpoint (health,
// readiness, metrics) and graceful shutdown of HTTP servers.
package platform

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NewLogger returns a JSON slog logger writing to stdout.
func NewLogger(level string, service string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	return slog.New(h).With("service", service)
}

// NewRegistry returns a Prometheus registry with Go runtime and process
// collectors already registered.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return reg
}

// Readiness is a concurrency-safe readiness flag combined with optional
// dependency checks.
type Readiness struct {
	shuttingDown atomic.Bool
	checks       []func() error
}

// AddCheck registers a readiness check; the service is ready only when all
// checks return nil.
func (r *Readiness) AddCheck(fn func() error) { r.checks = append(r.checks, fn) }

// SetShuttingDown marks the service as not ready so that load balancers stop
// routing traffic before the listeners are closed.
func (r *Readiness) SetShuttingDown() { r.shuttingDown.Store(true) }

// Check returns nil when the service may receive traffic.
func (r *Readiness) Check() error {
	if r.shuttingDown.Load() {
		return errors.New("shutting down")
	}
	for _, c := range r.checks {
		if err := c(); err != nil {
			return err
		}
	}
	return nil
}

// OpsHandler serves /healthz (liveness), /readyz (readiness) and /metrics.
func OpsHandler(reg *prometheus.Registry, ready *Readiness) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if err := ready.Check(); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ready"))
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{EnableOpenMetrics: true}))
	return mux
}

// NewServer returns an http.Server with conservative timeouts suitable for
// public traffic.
func NewServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       90 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
}

// Serve runs all servers until ctx is cancelled, then performs a graceful
// shutdown: readiness goes false, the process waits shutdownDelay so that
// endpoints are removed from load balancers, and in-flight requests get
// shutdownTimeout to complete.
func Serve(ctx context.Context, log *slog.Logger, ready *Readiness, shutdownDelay, shutdownTimeout time.Duration, servers ...*http.Server) error {
	errCh := make(chan error, len(servers))
	for _, s := range servers {
		go func(s *http.Server) {
			log.Info("listening", "addr", s.Addr)
			if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- err
			}
		}(s)
	}

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-errCh:
		log.Error("server failed", "err", runErr)
	}

	ready.SetShuttingDown()
	if runErr == nil && shutdownDelay > 0 {
		log.Info("draining before shutdown", "delay", shutdownDelay)
		time.Sleep(shutdownDelay)
	}
	sctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	for _, s := range servers {
		if err := s.Shutdown(sctx); err != nil {
			log.Error("shutdown", "addr", s.Addr, "err", err)
		}
	}
	return runErr
}

// Env returns the value of key or def when unset or empty.
func Env(lookup func(string) (string, bool), key, def string) string {
	if v, ok := lookup(key); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return def
}

// EnvDuration parses a time.Duration from key.
func EnvDuration(lookup func(string) (string, bool), key string, def time.Duration) (time.Duration, error) {
	v := Env(lookup, key, "")
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, errors.New(key + ": " + err.Error())
	}
	return d, nil
}

// EnvList splits a comma-separated value, dropping empty items.
func EnvList(lookup func(string) (string, bool), key string, def []string) []string {
	v := Env(lookup, key, "")
	if v == "" {
		return def
	}
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
