// Command orchd is the control plane: API server, scheduler and state store.
//
// It never touches payloads. Model weights, scene files and results move
// through object storage between clients and agents; this process moves
// references only, which is what lets one modest control plane serve the fleet.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/alexk/orch/gen/orch/v1/orchv1connect"
	"github.com/alexk/orch/internal/api"
	"github.com/alexk/orch/internal/events"
	"github.com/alexk/orch/internal/scheduler"
	"github.com/alexk/orch/internal/store/pgstore"
	"github.com/alexk/orch/internal/ui"
)

func main() {
	var (
		dsn         = flag.String("dsn", envOr("ORCH_DSN", "postgres://localhost:5432/orch?sslmode=disable"), "Postgres connection string")
		listen      = flag.String("listen", envOr("ORCH_LISTEN", ":9443"), "listen address")
		interval    = flag.Duration("schedule-interval", 500*time.Millisecond, "placement loop interval")
		leaseTTL    = flag.Duration("lease-ttl", 30*time.Second, "lease TTL; agents renew on each heartbeat")
		hbTimeout   = flag.Duration("heartbeat-timeout", 15*time.Second, "mark a node offline after this long without a heartbeat")
		hbInterval  = flag.Duration("heartbeat-interval", 2*time.Second, "interval agents are told to heartbeat at")
		logLevel    = flag.String("log-level", envOr("ORCH_LOG_LEVEL", "info"), "debug, info, warn or error")
		taskTimeout = flag.Duration("task-timeout", 0, "default hard wall-clock limit per task, applied when a job does not set its own; 0 = none")

		s3Endpoint    = flag.String("s3-endpoint", envOr("ORCH_S3_ENDPOINT", ""), "object store endpoint (host:port); unset disables STS credential minting")
		s3STSEndpoint = flag.String("s3-sts-endpoint", envOr("ORCH_S3_STS_ENDPOINT", ""), "STS AssumeRole endpoint; defaults to s3-endpoint (MinIO serves both)")
		s3Bucket      = flag.String("s3-bucket", envOr("ORCH_S3_BUCKET", ""), "object store bucket")
		s3AccessKey   = flag.String("s3-access-key", envOr("ORCH_S3_ACCESS_KEY", ""), "broad admin credential used only to call AssumeRole; never sent to an agent")
		s3SecretKey   = flag.String("s3-secret-key", envOr("ORCH_S3_SECRET_KEY", ""), "broad admin credential used only to call AssumeRole; never sent to an agent")
		s3UseSSL      = flag.Bool("s3-use-ssl", envBoolOr("ORCH_S3_USE_SSL", false), "use TLS against the object store")
	)
	flag.Parse()

	log := newLogger(*logLevel)

	var s3 *api.S3Config
	if *s3Endpoint != "" && *s3Bucket != "" {
		stsEndpoint := *s3STSEndpoint
		if stsEndpoint == "" {
			stsEndpoint = *s3Endpoint
		}
		s3 = &api.S3Config{
			Endpoint:    *s3Endpoint,
			STSEndpoint: stsEndpoint,
			Bucket:      *s3Bucket,
			AccessKey:   *s3AccessKey,
			SecretKey:   *s3SecretKey,
			UseSSL:      *s3UseSSL,
		}
	}

	if err := run(runConfig{
		DSN:               *dsn,
		Listen:            *listen,
		Interval:          *interval,
		LeaseTTL:          *leaseTTL,
		HeartbeatTimeout:  *hbTimeout,
		HeartbeatInterval: *hbInterval,
		TaskTimeout:       *taskTimeout,
		S3:                s3,
		Log:               log,
	}); err != nil {
		log.Error("orchd exited", "err", err)
		os.Exit(1)
	}
}

type runConfig struct {
	DSN               string
	Listen            string
	Interval          time.Duration
	LeaseTTL          time.Duration
	HeartbeatTimeout  time.Duration
	HeartbeatInterval time.Duration
	TaskTimeout       time.Duration
	S3                *api.S3Config
	Log               *slog.Logger
}

func run(cfg runConfig) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Migrations are embedded and run here, so there is no separate migration
	// step and no ordering question between deploying and migrating.
	st, err := pgstore.Open(ctx, cfg.DSN)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer st.Close()
	cfg.Log.Info("connected to postgres, schema up to date")

	bus := events.NewBus()
	hub := api.NewHub(st, bus, cfg.Log.With("component", "hub"), cfg.HeartbeatInterval, cfg.S3)
	if cfg.S3 == nil {
		cfg.Log.Info("no ORCH_S3_* configuration found; agents will not receive per-lease upload credentials")
	}

	sched := scheduler.New(st, bus, cfg.Log.With("component", "scheduler"), scheduler.Config{
		Interval:         cfg.Interval,
		LeaseTTL:         cfg.LeaseTTL,
		HeartbeatTimeout: cfg.HeartbeatTimeout,
		QueueLimit:       256,
	})
	sched.SetDispatcher(hub)

	srv := api.NewServer(st, bus, hub, sched, cfg.Log.With("component", "api"), cfg.TaskTimeout)

	mux := http.NewServeMux()
	mux.Handle(orchv1connect.NewAgentServiceHandler(hub))
	mux.Handle(orchv1connect.NewOrchServiceHandler(srv))
	mux.HandleFunc("/events", api.SSEHandler(bus))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})
	mux.Handle("/", ui.Handler())

	// h2c: the agent stream is bidirectional and so needs HTTP/2, and this is a
	// trusted single-organization network rather than the open internet.
	handler := h2c.NewHandler(withCORS(mux), &http2.Server{})

	httpSrv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go sched.Run(ctx)

	errc := make(chan error, 1)
	go func() {
		cfg.Log.Info("listening", "addr", cfg.Listen, "ui", "http://localhost"+cfg.Listen)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		cfg.Log.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return httpSrv.Shutdown(shutdownCtx)
	}
}

// withCORS allows a separately-served UI during development.
func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "*")
		w.Header().Set("Access-Control-Expose-Headers", "*")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l}))
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envBoolOr(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}
