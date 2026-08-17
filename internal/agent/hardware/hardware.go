// Package hardware provides the real, hardware-backed implementations of the
// agent's three extension seams -- NVML for device inventory, DCGM for health
// telemetry, and containerd for container execution -- plus a HostProbe that
// reads the machine's own non-GPU resources.
//
// Real and simulated nodes are meant to be indistinguishable to the control
// plane (see internal/agent/sim), so every field populated here must mean the
// same thing the simulator's fabricated version means.
//
// None of the three client libraries requires its target (a driver, a DCGM
// host engine, a containerd daemon) to be present at build time -- only at
// run time, via dlopen or a socket dial -- so this package compiles
// unconditionally, with no build tag, on any machine. New fails loudly at
// startup if a backend is unreachable rather than returning a provider that
// silently reports an empty or fabricated inventory.
package hardware

import (
	"fmt"
	"log/slog"

	"github.com/NVIDIA/go-nvml/pkg/nvml"

	"github.com/alexk/orch/internal/agent"
)

// Config configures the real, hardware-backed seam implementations.
type Config struct {
	// ContainerdSocket is the address of the containerd gRPC socket.
	ContainerdSocket string
	// ContainerdNamespace is the containerd namespace tasks are created in,
	// isolating orch's containers from anything else the daemon manages.
	ContainerdNamespace string
	// NvidiaRuntime is the runtime name registered in containerd's
	// config.toml for nvidia-container-toolkit (see:
	// nvidia-ctk runtime configure --runtime=containerd).
	NvidiaRuntime string
	// CacheDir is statfs'd for HostProbe's ScratchGB -- it should be the same
	// directory the agent's content-addressed cache uses, since that is the
	// disk a task's staged assets actually compete for.
	CacheDir string
}

func (c Config) withDefaults() Config {
	if c.ContainerdSocket == "" {
		c.ContainerdSocket = "/run/containerd/containerd.sock"
	}
	if c.ContainerdNamespace == "" {
		c.ContainerdNamespace = "orch"
	}
	if c.NvidiaRuntime == "" {
		c.NvidiaRuntime = "nvidia"
	}
	return c
}

// New resolves the hardware-backed seam implementations, initialising NVML,
// DCGM and containerd in turn. Each failure unwinds what was already
// initialised before returning, so a partial failure never leaves a backend
// half-started.
func New(cfg Config, log *slog.Logger) (agent.DeviceProvider, agent.HealthSource, agent.Executor, agent.HostProbe, error) {
	cfg = cfg.withDefaults()

	if ret := nvml.Init(); ret != nvml.SUCCESS {
		return nil, nil, nil, nil, fmt.Errorf("nvml: %v", nvml.ErrorString(ret))
	}

	devices := &NVMLProvider{}

	health, err := newDCGMSource(log)
	if err != nil {
		nvml.Shutdown()
		return nil, nil, nil, nil, fmt.Errorf("dcgm: %w", err)
	}

	exec, err := newContainerdExecutor(cfg, log)
	if err != nil {
		nvml.Shutdown()
		return nil, nil, nil, nil, fmt.Errorf("containerd: %w", err)
	}

	probe := newHostProbe(cfg.CacheDir, log)

	return devices, health, exec, probe, nil
}
