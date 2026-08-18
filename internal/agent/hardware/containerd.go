package hardware

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	runcoptions "github.com/containerd/containerd/api/types/runc/options"
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

// runcRuntime is containerd's plain shim, used for both GPU injection modes.
// The earlier code passed a bare "nvidia" runtime name to
// containerd.WithRuntime; in containerd v2's shim manager a runtime name
// without a dot is rejected outright ("invalid runtime name nvidia") because
// BinaryName() cannot derive a shim binary from it. That name was only ever
// meaningful to containerd's CRI plugin (via the config.toml stanza
// `nvidia-ctk runtime configure` writes), which this client-API executor
// does not go through -- so every container.NewTask call failed before this
// fix existed.
const runcRuntime = "io.containerd.runc.v2"

// containerIDPrefix identifies orch-owned containers in the containerd
// namespace, so adopt (reconciliation across an agent restart) can tell them
// apart from anything else that namespace might hold.
const containerIDPrefix = "orch-"

// ContainerdExecutor runs tasks as containerd containers, attaching GPUs via
// either CDI or the nvidia-container-runtime binary (Config.GPUMode).
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
	e := &ContainerdExecutor{
		client:  client,
		cfg:     cfg,
		log:     log,
		running: map[agent.Handle]*runningContainer{},
	}
	if err := e.adopt(context.Background()); err != nil {
		// Not fatal: a node starting for the first time, or one whose
		// previous agent process shut down cleanly, has nothing to adopt.
		log.Warn("could not reconcile pre-existing containers", "err", err)
	}
	return e, nil
}

// adopt lists every orch-owned container already in this namespace and loads
// its task, so a restarted agent process can find -- and, once the control
// plane's active-lease list says to, kill via RunningLeases/agent.Reconciler
// -- containers a crashed previous process left running, instead of leaking
// them (and the VRAM they hold) until the machine reboots.
func (e *ContainerdExecutor) adopt(ctx context.Context) error {
	ctx = e.ns(ctx)
	containers, err := e.client.Containers(ctx)
	if err != nil {
		return fmt.Errorf("list containers: %w", err)
	}
	for _, c := range containers {
		if !strings.HasPrefix(c.ID(), containerIDPrefix) {
			continue
		}
		task, err := c.Task(ctx, nil)
		if err != nil {
			e.log.Warn("orphaned container has no task, leaving it for manual cleanup",
				"container", c.ID(), "err", err)
			continue
		}
		e.running[agent.Handle(c.ID())] = &runningContainer{container: c, task: task}
		e.log.Info("adopted container from a previous agent process", "container", c.ID())
	}
	return nil
}

// RunningLeases implements agent.Reconciler: every lease this executor
// believes it is still running, independent of the agent process's own
// (restart-volatile) bookkeeping.
func (e *ContainerdExecutor) RunningLeases(context.Context) (map[string]agent.Handle, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[string]agent.Handle, len(e.running))
	for h := range e.running {
		out[strings.TrimPrefix(string(h), containerIDPrefix)] = h
	}
	return out, nil
}

// Close disconnects from containerd. It does not touch any running
// container: an agent process exiting is not a reason to kill GPU work, that
// is what Cleanup and preemption are for.
func (e *ContainerdExecutor) Close() error {
	return e.client.Close()
}

func (e *ContainerdExecutor) ns(ctx context.Context) context.Context {
	return namespaces.WithNamespace(ctx, e.cfg.ContainerdNamespace)
}

// Start pulls the task's image, creates a container with the requested
// devices, host share and mounts, and starts it.
func (e *ContainerdExecutor) Start(ctx context.Context, spec agent.StartSpec) (agent.Handle, error) {
	ctx = e.ns(ctx)

	// spec.Image is expected to be a fully-qualified, resolvable reference
	// (registry/name@digest or registry/name:tag) -- the same assumption the
	// rest of the task pipeline already makes about ImageDigest.
	image, err := e.client.GetImage(ctx, spec.Image)
	if err != nil {
		// A pre-pulled node should have no hard per-task registry
		// dependency; only pay for a pull when the image is not already
		// present.
		image, err = e.client.Pull(ctx, spec.Image, containerd.WithPullUnpack)
		if err != nil {
			return "", fmt.Errorf("pull %s: %w", spec.Image, err)
		}
	}

	id := containerID(spec.LeaseID)

	specOpts := []oci.SpecOpts{
		oci.WithImageConfig(image),
		oci.WithEnv(taskEnv(spec, e.cfg.GPUMode)),
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
	// The containerd default (64MB) breaks PyTorch DataLoader workers and
	// NCCL, both squarely in the target workload (batch LLM/diffusion
	// inference). WithDevShmSize takes kilobytes, not bytes.
	if shmKB := e.cfg.DevShmSizeBytes / 1024; shmKB > 0 {
		specOpts = append(specOpts, oci.WithDevShmSize(shmKB))
	}

	var runtimeOpts any
	switch {
	case e.cfg.GPUMode == GPUModeRuntimeBinary:
		runtimeOpts = &runcoptions.Options{BinaryName: e.cfg.NvidiaContainerRuntimeBinary}
	case len(spec.DeviceIndices) > 0:
		// CDI must be appended last: spec_opts.go documents that CDI
		// injection sets env, mounts and hooks that a later SpecOpt must not
		// silently reset.
		specOpts = append(specOpts, oci.WithCDIDevices(cdiNames(spec.DeviceIndices)...))
	}

	container, err := e.client.NewContainer(ctx, id,
		containerd.WithNewSnapshot(id+"-rootfs", image),
		containerd.WithNewSpec(specOpts...),
		containerd.WithRuntime(runcRuntime, runtimeOpts),
	)
	if err != nil {
		return "", fmt.Errorf("create container %s: %w", id, err)
	}

	// stdout/stderr goes to a per-attempt log file rather than the agent's
	// own stdio: without this, every task's output interleaves on one
	// stream, unattributable to any particular run, which is why a failed
	// real task was previously undiagnosable.
	ioCreator := cio.NewCreator(cio.WithStdio)
	if spec.LogPath != "" {
		ioCreator = cio.LogFile(spec.LogPath)
	}
	task, err := container.NewTask(ctx, ioCreator)
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

// Status reports the task's current lifecycle state. Unlike the previous
// version, it does not delete the task/container as a side effect of
// observing Stopped -- that tear-down happens in Cleanup, called explicitly
// once the caller is done with the terminal status. Doing it here meant a
// caller that stopped polling (e.g. after preemption cancelled the poll
// loop's context) would never see the Stopped state and would leak the
// container forever.
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
		return terminalStatus(rc.killed, st.ExitStatus), nil
	default:
		return agent.Status{State: agent.ExecRunning}, nil
	}
}

// Cleanup deletes the task and container (and its snapshot), and drops the
// executor's own bookkeeping. It is the one place real resources are
// reclaimed, called once a caller has recorded whatever terminal status it
// needed -- see Status's doc comment for why this used to be a Status side
// effect and why that leaked containers on preemption.
func (e *ContainerdExecutor) Cleanup(ctx context.Context, h agent.Handle) error {
	ctx = e.ns(ctx)

	e.mu.Lock()
	rc := e.running[h]
	delete(e.running, h)
	e.mu.Unlock()
	if rc == nil {
		return nil
	}

	var errs []error
	if _, err := rc.task.Delete(ctx); err != nil {
		errs = append(errs, fmt.Errorf("task delete: %w", err))
	}
	if err := rc.container.Delete(ctx, containerd.WithSnapshotCleanup); err != nil {
		errs = append(errs, fmt.Errorf("container delete: %w", err))
	}
	return errors.Join(errs...)
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

// containerID derives a containerd container ID from a lease ID. Lease IDs
// are the fencing token already used to key everything else about this run
// (agent.go's runningTask map), so reusing it keeps one identifier across the
// whole stack instead of inventing a second one.
func containerID(leaseID string) string {
	return containerIDPrefix + leaseID
}

// taskEnv builds the container's environment from spec.Env (already fully
// resolved by agent.go: job Env, ORCH_PARAM_* projections and identity vars)
// plus the GPU-specific variables only this executor knows about.
func taskEnv(spec agent.StartSpec, gpuMode string) []string {
	keys := make([]string, 0, len(spec.Env))
	for k := range spec.Env {
		keys = append(keys, k)
	}
	sort.Strings(keys) // deterministic order: easier to read in logs and to test

	env := make([]string, 0, len(keys)+2)
	for _, k := range keys {
		env = append(env, k+"="+spec.Env[k])
	}

	if len(spec.DeviceIndices) > 0 {
		// Without this the toolkit defaults to the "utility" capability and
		// injects nvidia-smi but no CUDA driver libraries -- nvidia-smi
		// works, CUDA programs fail with "no CUDA-capable device", which is
		// the most confusing failure mode this executor can produce.
		env = append(env, "NVIDIA_DRIVER_CAPABILITIES=compute,utility")

		// Only meaningful on the runtime-binary path: under CDI the variable
		// is ignored by the toolkit (CDI injection already picked the exact
		// devices) and leaving it in would be misleading about what actually
		// selected the devices.
		if gpuMode == GPUModeRuntimeBinary {
			env = append(env, "NVIDIA_VISIBLE_DEVICES="+deviceList(spec.DeviceIndices))
		}
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

// cdiNames renders device indices as CDI qualified device names for
// oci.WithCDIDevices, e.g. "nvidia.com/gpu=0". This is the mapping node
// operators configure via `nvidia-ctk cdi generate`, which names devices by
// their NVML index -- the same index DeviceIndices already carries.
func cdiNames(indices []int) []string {
	out := make([]string, len(indices))
	for i, idx := range indices {
		out[i] = fmt.Sprintf("nvidia.com/gpu=%d", idx)
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
