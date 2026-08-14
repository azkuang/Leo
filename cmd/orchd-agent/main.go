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
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/alexk/orch/internal/agent"
	"github.com/alexk/orch/internal/agent/sim"
)

func main() {
	var (
		server    = flag.String("server", envOr("ORCH_SERVER", "http://localhost:9443"), "control plane URL")
		token     = flag.String("token", os.Getenv("ORCH_JOIN_TOKEN"), "single-use join token")
		hostname  = flag.String("hostname", "", "hostname to register as (defaults to the system hostname)")
		simulated = flag.Bool("simulated", false, "run with the simulated device, health and executor providers")
		profile   = flag.String("profile", "", "path to a simulated node profile (YAML)")
		cacheDir  = flag.String("cache-dir", "", "content-addressed asset cache directory")
		labels    = flag.String("labels", "", "comma-separated key=value labels, e.g. location=lab-a,owner_team=cad")
		logLevel  = flag.String("log-level", envOr("ORCH_LOG_LEVEL", "info"), "debug, info, warn or error")

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
		Labels:    parseLabels(*labels),
	}

	var (
		devices agent.DeviceProvider
		health  agent.HealthSource
		exec    agent.Executor
		probe   agent.HostProbe
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
		var err error
		devices, health, exec, probe, err = realProviders()
		if err != nil {
			log.Error("no real device backend available", "err", err)
			fmt.Fprintln(os.Stderr,
				"\nHint: run with --simulated to use the simulated providers.")
			os.Exit(1)
		}
	}

	a, err := agent.New(cfg, log, devices, health, exec, probe)
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

// realProviders resolves the hardware-backed seam implementations.
//
// These are the NVML, DCGM and containerd backends. They are not built into
// this binary yet -- see internal/agent/hardware -- and the agent says so
// plainly rather than silently reporting an empty or fabricated inventory. A
// node that lies about its capabilities is worse than a node that refuses to
// start.
func realProviders() (agent.DeviceProvider, agent.HealthSource, agent.Executor, agent.HostProbe, error) {
	return nil, nil, nil, nil, errors.New(
		"the NVML/DCGM/containerd backends are not compiled into this build")
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
