package hardware

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"syscall"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/pkg/oci"
	specs "github.com/opencontainers/runtime-spec/specs-go"

	"github.com/alexk/orch/internal/agent"
)

// cpuCFSPeriod is the cgroups CFS period used to translate a fractional core
// share into a CPU quota. 100ms is the Linux/Docker/containerd convention.
const cpuCFSPeriod = 100_000 // microseconds

// ContainerdExecutor runs tasks as containerd containers with the
// nvidia-container-toolkit runtime, so GPU visibility is enforced by the
// runtime hook rather than anything this code does directly.
type ContainerdExecutor struct {
	client *containerd.Client
	cfg    Config
	log    *slog.Logger

	mu      sync.Mutex
	running map[agent.Handle]*runningContainer
}

type runningContainer struct {
	container containerd.Container
	task      containerd.Task
	// killed is set by Stop, so Status can report ExecKilled instead of
	// reading a non-zero exit code as an ordinary failure.
	killed bool
}

func newContainerdExecutor(cfg Config, log *slog.Logger) (*ContainerdExecutor, error) {
	client, err := containerd.New(cfg.ContainerdSocket)
	if err != nil {
		return nil, fmt.Errorf("connect to containerd at %s: %w", cfg.ContainerdSocket, err)
	}
	return &ContainerdExecutor{
		client:  client,
		cfg:     cfg,
		log:     log,
		running: map[agent.Handle]*runningContainer{},
	}, nil
}

func (e *ContainerdExecutor) ns(ctx context.Context) context.Context {
	return namespaces.WithNamespace(ctx, e.cfg.ContainerdNamespace)
}

// Start pulls the task's image, creates a container under the nvidia runtime
// with the requested devices, host share and mounts, and starts it.
func (e *ContainerdExecutor) Start(ctx context.Context, spec agent.StartSpec) (agent.Handle, error) {
	ctx = e.ns(ctx)

	// spec.Image is expected to be a fully-qualified, resolvable reference
	// (registry/name@digest or registry/name:tag) -- the same assumption the
	// rest of the task pipeline already makes about ImageDigest.
	image, err := e.client.Pull(ctx, spec.Image, containerd.WithPullUnpack)
	if err != nil {
		return "", fmt.Errorf("pull %s: %w", spec.Image, err)
	}

	id := containerID(spec.LeaseID)

	specOpts := []oci.SpecOpts{
		oci.WithImageConfig(image),
		oci.WithEnv(taskEnv(spec)),
	}
	if len(spec.Command) > 0 {
		specOpts = append(specOpts, oci.WithProcessArgs(spec.Command...))
	}
	if len(spec.Mounts) > 0 {
		specOpts = append(specOpts, oci.WithMounts(ociMounts(spec.Mounts)))
	}
	// HostShareCores/RAMGB enforce a borrowed slot's proportional share of the
	// host through cgroups v2, same as the design intends (StartSpec doc
	// comment) -- the difference between borrowing being safe and being a
	// support burden.
	if spec.HostShareCores > 0 {
		quota := int64(spec.HostShareCores * float64(cpuCFSPeriod))
		specOpts = append(specOpts, oci.WithCPUCFS(quota, cpuCFSPeriod))
	}
	if spec.HostShareRAMGB > 0 {
		specOpts = append(specOpts, oci.WithMemoryLimit(uint64(spec.HostShareRAMGB)<<30))
	}

	container, err := e.client.NewContainer(ctx, id,
		containerd.WithNewSnapshot(id+"-rootfs", image),
		containerd.WithNewSpec(specOpts...),
		// The nvidia-container-toolkit runtime reads NVIDIA_VISIBLE_DEVICES
		// (set via taskEnv) and injects exactly those devices -- this is the
		// standard nvidia-container-runtime contract, not an orch-specific
		// mechanism.
		containerd.WithRuntime(e.cfg.NvidiaRuntime, nil),
	)
	if err != nil {
		return "", fmt.Errorf("create container %s: %w", id, err)
	}

	task, err := container.NewTask(ctx, cio.NewCreator(cio.WithStdio))
	if err != nil {
		_ = container.Delete(ctx, containerd.WithSnapshotCleanup)
		return "", fmt.Errorf("create task %s: %w", id, err)
	}

	if err := task.Start(ctx); err != nil {
		_, _ = task.Delete(ctx)
		_ = container.Delete(ctx, containerd.WithSnapshotCleanup)
		return "", fmt.Errorf("start task %s: %w", id, err)
	}

	h := agent.Handle(id)
	e.mu.Lock()
	e.running[h] = &runningContainer{container: container, task: task}
	e.mu.Unlock()

	return h, nil
}

// Stop sends SIGTERM (or SIGKILL if no grace period is given) to every
// process in the task, then escalates to SIGKILL after grace elapses.
// Preemption in v1 is kill-and-requeue (StartSpec doc comment), so there is
// no drain to wait for beyond that.
func (e *ContainerdExecutor) Stop(ctx context.Context, h agent.Handle, grace time.Duration) error {
	ctx = e.ns(ctx)

	e.mu.Lock()
	rc := e.running[h]
	if rc != nil {
		rc.killed = true
	}
	e.mu.Unlock()
	if rc == nil {
		return nil
	}

	sig := syscall.SIGTERM
	if grace <= 0 {
		sig = syscall.SIGKILL
	}
	if err := rc.task.Kill(ctx, sig, containerd.WithKillAll); err != nil {
		return fmt.Errorf("kill %s: %w", h, err)
	}
	if grace > 0 {
		time.AfterFunc(grace, func() {
			killCtx := e.ns(context.Background())
			_ = rc.task.Kill(killCtx, syscall.SIGKILL, containerd.WithKillAll)
		})
	}
	return nil
}

// Status reports the task's current lifecycle state. On first observing a
// terminal state it also tears the task and container down -- the Executor
// interface has no separate cleanup hook (sim's Forget is sim-only), so this
// is the one place real resources can be reclaimed without leaking a
// container per finished task.
func (e *ContainerdExecutor) Status(ctx context.Context, h agent.Handle) (agent.Status, error) {
	ctx = e.ns(ctx)

	e.mu.Lock()
	rc := e.running[h]
	e.mu.Unlock()
	if rc == nil {
		return agent.Status{}, fmt.Errorf("unknown handle %s", h)
	}

	st, err := rc.task.Status(ctx)
	if err != nil {
		return agent.Status{}, fmt.Errorf("task status %s: %w", h, err)
	}

	switch st.Status {
	case containerd.Running, containerd.Created, containerd.Paused, containerd.Pausing:
		return agent.Status{State: agent.ExecRunning}, nil
	case containerd.Stopped:
		out := terminalStatus(rc.killed, st.ExitStatus)
		e.cleanup(ctx, h, rc)
		return out, nil
	default:
		return agent.Status{State: agent.ExecRunning}, nil
	}
}

func terminalStatus(killed bool, exitStatus uint32) agent.Status {
	switch {
	case killed:
		return agent.Status{State: agent.ExecKilled, ExitCode: int(exitStatus)}
	case exitStatus == 0:
		return agent.Status{State: agent.ExecExited, ExitCode: 0, Progress: 1}
	default:
		return agent.Status{
			State:    agent.ExecFailed,
			ExitCode: int(exitStatus),
			Message:  fmt.Sprintf("exited with code %d", exitStatus),
		}
	}
}

func (e *ContainerdExecutor) cleanup(ctx context.Context, h agent.Handle, rc *runningContainer) {
	e.mu.Lock()
	delete(e.running, h)
	e.mu.Unlock()

	if _, err := rc.task.Delete(ctx); err != nil {
		e.log.Warn("task delete failed", "handle", h, "err", err)
	}
	if err := rc.container.Delete(ctx, containerd.WithSnapshotCleanup); err != nil {
		e.log.Warn("container delete failed", "handle", h, "err", err)
	}
}

// containerID derives a containerd container ID from a lease ID. Lease IDs
// are the fencing token already used to key everything else about this run
// (agent.go's runningTask map), so reusing it keeps one identifier across the
// whole stack instead of inventing a second one.
func containerID(leaseID string) string {
	return "orch-" + leaseID
}

// taskEnv builds the container's environment, including
// NVIDIA_VISIBLE_DEVICES -- DeviceIndices are NVML indices, which is exactly
// what the toolkit expects there.
func taskEnv(spec agent.StartSpec) []string {
	env := make([]string, 0, len(spec.Env)+1)
	for k, v := range spec.Env {
		env = append(env, k+"="+v)
	}
	if len(spec.DeviceIndices) > 0 {
		env = append(env, "NVIDIA_VISIBLE_DEVICES="+deviceList(spec.DeviceIndices))
	}
	return env
}

func deviceList(indices []int) string {
	out := ""
	for i, idx := range indices {
		if i > 0 {
			out += ","
		}
		out += fmt.Sprintf("%d", idx)
	}
	return out
}

func ociMounts(mounts []agent.Mount) []specs.Mount {
	out := make([]specs.Mount, 0, len(mounts))
	for _, m := range mounts {
		opts := []string{"rbind"}
		if m.ReadOnly {
			opts = append(opts, "ro")
		} else {
			opts = append(opts, "rw")
		}
		out = append(out, specs.Mount{
			Source:      m.HostPath,
			Destination: m.ContainerPath,
			Type:        "bind",
			Options:     opts,
		})
	}
	return out
}
