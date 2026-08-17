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
	"io"
	"log/slog"

	"github.com/NVIDIA/go-nvml/pkg/nvml"

	"github.com/alexk/orch/internal/agent"
)

// GPU device injection modes for ContainerdExecutor. See containerd.go.
const (
	// GPUModeCDI uses containerd's CDI device injection against
	// /etc/cdi/nvidia.yaml. This is the default: it works with the plain
	// runc.v2 runtime, so it needs no per-runtime registration in
	// containerd's config.toml. Node prerequisite:
	// `nvidia-ctk cdi generate --output=/etc/cdi/nvidia.yaml`.
	GPUModeCDI = "cdi"
	// GPUModeRuntimeBinary invokes nvidia-container-runtime directly as the
	// runc-compatible shim binary, keeping the NVIDIA_VISIBLE_DEVICES env
	// contract. Fallback for nodes where CDI has not been set up.
	GPUModeRuntimeBinary = "runtime-binary"
)

// defaultDevShmSize is 1GiB. Containerd's own default is 64MB, which breaks
// PyTorch DataLoader workers and NCCL -- both squarely in the target workload
// (batch LLM/diffusion inference).
const defaultDevShmSize = 1 << 30

// Config configures the real, hardware-backed seam implementations.
type Config struct {
	// ContainerdSocket is the address of the containerd gRPC socket.
	ContainerdSocket string
	// ContainerdNamespace is the containerd namespace tasks are created in,
	// isolating orch's containers from anything else the daemon manages.
	ContainerdNamespace string
	// GPUMode selects how GPU devices are attached to a container: GPUModeCDI
	// (default) or GPUModeRuntimeBinary.
	GPUMode string
	// NvidiaContainerRuntimeBinary is the runc-compatible binary invoked when
	// GPUMode is GPUModeRuntimeBinary.
	NvidiaContainerRuntimeBinary string
	// DevShmSizeBytes sizes /dev/shm inside every task container.
	DevShmSizeBytes int64
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
	if c.GPUMode == "" {
		c.GPUMode = GPUModeCDI
	}
	if c.NvidiaContainerRuntimeBinary == "" {
		c.NvidiaContainerRuntimeBinary = "/usr/bin/nvidia-container-runtime"
	}
	if c.DevShmSizeBytes == 0 {
		c.DevShmSizeBytes = defaultDevShmSize
	}
	return c
}

// closerFunc adapts a plain func() to io.Closer.
type closerFunc func() error

func (f closerFunc) Close() error { return f() }

// New resolves the hardware-backed seam implementations, initialising NVML,
// DCGM and containerd in turn. Each failure unwinds what was already
// initialised before returning, so a partial failure never leaves a backend
// half-started. The returned io.Closer tears every backend down in reverse
// order; the caller (cmd/orchd-agent) is expected to defer it.
func New(cfg Config, log *slog.Logger) (agent.DeviceProvider, agent.HealthSource, agent.Executor, agent.HostProbe, io.Closer, error) {
	cfg = cfg.withDefaults()

	if ret := nvml.Init(); ret != nvml.SUCCESS {
		return nil, nil, nil, nil, nil, fmt.Errorf("nvml: %v", nvml.ErrorString(ret))
	}

	devices := &NVMLProvider{}

	health, err := newDCGMSource(log)
	if err != nil {
		nvml.Shutdown()
		return nil, nil, nil, nil, nil, fmt.Errorf("dcgm: %w", err)
	}

	exec, err := newContainerdExecutor(cfg, log)
	if err != nil {
		_ = health.Close()
		nvml.Shutdown()
		return nil, nil, nil, nil, nil, fmt.Errorf("containerd: %w", err)
	}

	probe := newHostProbe(cfg.CacheDir, log)

	closer := closerFunc(func() error {
		var errs []error
		if err := exec.Close(); err != nil {
			errs = append(errs, err)
		}
		if err := health.Close(); err != nil {
			errs = append(errs, err)
		}
		if ret := nvml.Shutdown(); ret != nvml.SUCCESS {
			errs = append(errs, fmt.Errorf("nvml shutdown: %v", nvml.ErrorString(ret)))
		}
		return joinErrs(errs)
	})

	return devices, health, exec, probe, closer, nil
}

func joinErrs(errs []error) error {
	if len(errs) == 0 {
		return nil
	}
	msg := errs[0].Error()
	for _, e := range errs[1:] {
		msg += "; " + e.Error()
	}
	return fmt.Errorf("%s", msg)
}
