// Command orchd-agent runs on a node: inventory, health streaming, container
// lifecycle and artifact staging.
//
// Onboarding is discovery over declaration. The agent enumerates its own
// devices, probes host resources, reads driver and CUDA versions and registers
// itself. Nothing about the machine is typed by a human, because every fact a
// human types is a fact that goes stale.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/alexk/orch/internal/agent"
	"github.com/alexk/orch/internal/agent/hardware"
	"github.com/alexk/orch/internal/agent/sim"
)

func main() {
	var (
		server    = flag.String("server", envOr("ORCH_SERVER", "http://localhost:9443"), "control plane URL")
		token     = flag.String("token", os.Getenv("ORCH_JOIN_TOKEN"), "single-use join token")
		hostname  = flag.String("hostname", envOr("ORCH_HOSTNAME", ""), "hostname to register as (defaults to the system hostname)")
		simulated = flag.Bool("simulated", envBoolOr("ORCH_SIMULATED", false), "run with the simulated device, health and executor providers")
		profile   = flag.String("profile", envOr("ORCH_PROFILE", ""), "path to a simulated node profile (YAML)")
		cacheDir  = flag.String("cache-dir", envOr("ORCH_CACHE_DIR", ""), "content-addressed asset cache directory")
		workDir   = flag.String("work-dir", envOr("ORCH_WORK_DIR", ""), "per-lease scratch directory (task outputs, logs)")
		labels    = flag.String("labels", envOr("ORCH_LABELS", ""), "comma-separated key=value labels, e.g. location=lab-a,owner_team=cad")
		logLevel  = flag.String("log-level", envOr("ORCH_LOG_LEVEL", "info"), "debug, info, warn or error")

		containerdSocket = flag.String("containerd-socket", envOr("ORCH_CONTAINERD_SOCKET", "/run/containerd/containerd.sock"), "containerd gRPC socket (non-simulated only)")
		containerdNS     = flag.String("containerd-namespace", envOr("ORCH_CONTAINERD_NAMESPACE", "orch"), "containerd namespace for task containers (non-simulated only)")
		gpuMode          = flag.String("gpu-mode", envOr("ORCH_GPU_MODE", hardware.GPUModeCDI), "GPU attachment mode: cdi (default, needs `nvidia-ctk cdi generate`) or runtime-binary")
		nvidiaRuntimeBin = flag.String("nvidia-container-runtime-binary", envOr("ORCH_NVIDIA_CONTAINER_RUNTIME_BINARY", ""), "runc-compatible binary for gpu-mode=runtime-binary")
		devShmMB         = flag.Int64("dev-shm-mb", envInt64Or("ORCH_DEV_SHM_MB", 1024), "size of /dev/shm inside every task container, in MB")

		s3Endpoint  = flag.String("s3-endpoint", envOr("ORCH_S3_ENDPOINT", ""), "object store endpoint (host:port); unset disables uploads")
		s3Bucket    = flag.String("s3-bucket", envOr("ORCH_S3_BUCKET", ""), "object store bucket")
		s3Region    = flag.String("s3-region", envOr("ORCH_S3_REGION", ""), "object store region")
		s3AccessKey = flag.String("s3-access-key", envOr("ORCH_S3_ACCESS_KEY", ""), "object store access key (static fallback credential)")
		s3SecretKey = flag.String("s3-secret-key", envOr("ORCH_S3_SECRET_KEY", ""), "object store secret key (static fallback credential)")
		s3UseSSL    = flag.Bool("s3-use-ssl", envBoolOr("ORCH_S3_USE_SSL", false), "use TLS against the object store")
		s3PathStyle = flag.Bool("s3-path-style", envBoolOr("ORCH_S3_PATH_STYLE", true), "path-style bucket addressing (MinIO needs this)")

		taskMin  = flag.Duration("sim-task-min", 5*time.Second, "simulated task duration, lower bound")
		taskMax  = flag.Duration("sim-task-max", 15*time.Second, "simulated task duration, upper bound")
		failRate = flag.Float64("sim-failure-rate", 0, "simulated task failure probability, 0..1")
		staging  = flag.Duration("sim-staging-delay", 0, "simulated cold-cache staging delay")
	)
	flag.Parse()

	log := newLogger(*logLevel)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := agent.Config{
		ServerURL: *server,
		JoinToken: *token,
		Hostname:  *hostname,
		Simulated: *simulated,
		CacheDir:  *cacheDir,
		WorkDir:   *workDir,
		Labels:    parseLabels(*labels),
	}

	objectStore, err := agent.NewObjectStore(agent.ObjectStoreConfig{
		Endpoint:        *s3Endpoint,
		Bucket:          *s3Bucket,
		Region:          *s3Region,
		AccessKeyID:     *s3AccessKey,
		SecretAccessKey: *s3SecretKey,
		UseSSL:          *s3UseSSL,
		PathStyle:       *s3PathStyle,
	})
	if err != nil {
		log.Error("object store setup failed", "err", err)
		os.Exit(1)
	}
	if objectStore == nil {
		log.Info("no ORCH_S3_* configuration found; task outputs will not be uploaded")
	}

	var (
		devices agent.DeviceProvider
		health  agent.HealthSource
		exec    agent.Executor
		probe   agent.HostProbe
		closer  io.Closer
	)

	if *simulated {
		p, err := loadProfile(*profile)
		if err != nil {
			log.Error("could not load simulated profile", "err", err)
			os.Exit(1)
		}
		p.TaskDurationMin = *taskMin
		p.TaskDurationMax = *taskMax
		p.FailureRate = *failRate
		p.StagingDelay = *staging
		if cfg.Hostname == "" {
			cfg.Hostname = p.Hostname
		}
		if len(cfg.Labels) == 0 {
			cfg.Labels = p.Labels
		}

		// One object implements all three seams, and the control plane cannot
		// tell it apart from a real node.
		node := sim.NewNode(p)
		devices, health, exec = node, node, node
		probe = node.HostInfo

		log.Info("starting simulated node",
			"hostname", cfg.Hostname, "devices", len(p.Devices))
	} else {
		devices, health, exec, probe, closer, err = realProviders(hardware.Config{
			ContainerdSocket:             *containerdSocket,
			ContainerdNamespace:          *containerdNS,
			GPUMode:                      *gpuMode,
			NvidiaContainerRuntimeBinary: *nvidiaRuntimeBin,
			DevShmSizeBytes:              *devShmMB << 20,
			CacheDir:                     *cacheDir,
		}, log)
		if err != nil {
			log.Error("no real device backend available", "err", err)
			fmt.Fprintln(os.Stderr,
				"\nHint: run with --simulated to use the simulated providers.")
			os.Exit(1)
		}
		defer closer.Close()
	}

	a, err := agent.New(cfg, log, devices, health, exec, probe, objectStore)
	if err != nil {
		log.Error("agent setup failed", "err", err)
		os.Exit(1)
	}

	if err := a.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("agent exited", "err", err)
		os.Exit(1)
	}
}

// loadProfile reads a simulated node profile, or synthesises a default one.
func loadProfile(path string) (sim.Profile, error) {
	if path == "" {
		return defaultProfile(), nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return sim.Profile{}, err
	}
	var p sim.Profile
	if err := yaml.Unmarshal(raw, &p); err != nil {
		return sim.Profile{}, err
	}
	if len(p.Devices) == 0 {
		return sim.Profile{}, errors.New("profile declares no devices")
	}
	return p, nil
}

func defaultProfile() sim.Profile {
	host, _ := os.Hostname()
	return sim.Profile{
		Hostname: host,
		Devices: []sim.DeviceProfile{
			{VRAMGB: 24, Model: "sim-24", ComputeCapability: "8.9", EncodeSessions: 3, DecodeSessions: 3, PowerW: 300},
			{VRAMGB: 24, Model: "sim-24", ComputeCapability: "8.9", EncodeSessions: 3, DecodeSessions: 3, PowerW: 300},
		},
		Cores: 32, RAMGB: 128, ScratchGB: 1000, PCIeGen: 4,
		Driver: "580.95", CUDA: "13.0",
	}
}

func parseLabels(s string) map[string]string {
	if s == "" {
		return nil
	}
	out := map[string]string{}
	for _, pair := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(pair), "=")
		if ok && k != "" {
			out[k] = v
		}
	}
	return out
}

// realProviders resolves the hardware-backed seam implementations: NVML,
// DCGM and containerd, via internal/agent/hardware. It fails fast if any of
// the three is unavailable on this machine rather than silently reporting an
// empty or fabricated inventory -- a node that lies about its capabilities is
// worse than a node that refuses to start.
func realProviders(cfg hardware.Config, log *slog.Logger) (agent.DeviceProvider, agent.HealthSource, agent.Executor, agent.HostProbe, io.Closer, error) {
	return hardware.New(cfg, log)
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

func envInt64Or(key string, fallback int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return fallback
	}
	return n
}
