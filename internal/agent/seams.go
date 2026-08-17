// Package agent is the node-side runtime: inventory, health streaming,
// container lifecycle and artifact staging.
//
// Three of the system's four extension seams live here. They are Go interfaces
// resolved through a registry and compiled in. Go's `plugin` package is
// deliberately not used: dynamically loaded .so files require identical Go
// versions and dependency trees across every build, which would cost days for a
// benefit this project does not need.
//
// A consequence worth stating plainly: the simulated node is not a test
// fixture. It is a legitimate implementation of all three interfaces, and real
// and simulated nodes flow through identical code paths.
package agent

import (
	"context"
	"time"

	"github.com/alexk/orch/internal/domain"
)

// DeviceProvider enumerates the accelerators attached to this machine.
//
// Implementations: NVML, simulated.
type DeviceProvider interface {
	Enumerate(ctx context.Context) ([]domain.Device, error)
}

// HealthSource streams the fast-path telemetry the scheduler reads.
//
// Implementations: DCGM, simulated.
type HealthSource interface {
	// Stream emits one batch of samples per interval until the context ends.
	// The channel is closed when the source stops.
	Stream(ctx context.Context) (<-chan []domain.HealthSample, error)
}

// ExecState is the lifecycle of one container run.
type ExecState string

const (
	ExecStarting ExecState = "starting"
	ExecRunning  ExecState = "running"
	ExecExited   ExecState = "exited"
	ExecFailed   ExecState = "failed"
	ExecKilled   ExecState = "killed"
)

// Mount is a staged asset made visible inside the container.
type Mount struct {
	HostPath      string
	ContainerPath string
	ReadOnly      bool
}

// ContainerOutputDir is the single source of truth for the writable-output
// mount path inside every task container. Both agent.go (building the Mount)
// and hardware/containerd.go's taskEnv (building ORCH_OUTPUT_DIR) reference
// this rather than each hardcoding the same string.
const ContainerOutputDir = "/outputs"

// StartSpec is everything needed to run one task.
type StartSpec struct {
	// LeaseID is the fencing token. It travels with every status report so a
	// zombie from a preempted attempt is recognised and ignored.
	LeaseID string
	TaskID  string
	JobID   string
	// Index is the task's fan-out index, for identity env (ORCH_TASK_INDEX).
	Index int
	// Attempt is this task's attempt number, for identity env (ORCH_ATTEMPT).
	Attempt int

	Image   string
	Command []string
	// Env is the fully-resolved environment: job-level Env merged with
	// ORCH_PARAM_<KEY> projections and identity vars (ORCH_JOB_ID,
	// ORCH_TASK_ID, ...), built once in agent.go so real and simulated nodes
	// see identical values.
	Env map[string]string

	// DeviceIndices are the physical devices this task may use. Everything
	// else on the machine is off limits.
	DeviceIndices []int

	// HostShareCores and HostShareRAMGB are enforced through cgroups v2. A
	// borrowed slot carries its proportional share of the host and nothing
	// more -- the difference between borrowing being safe and being a support
	// burden.
	HostShareCores float64
	HostShareRAMGB int

	Mounts []Mount

	// OutputPrefix is unique per attempt, so a preempted attempt's writes are
	// simply orphaned rather than needing cleanup.
	OutputPrefix string
	// Params are the task's raw fan-out parameters, opaque to the
	// orchestrator -- kept distinct from Env because the simulator reads
	// specific keys (e.g. "sim_duration_ms") directly rather than through the
	// container-facing env projection.
	Params map[string]string

	// LogPath is where the executor should send the task's stdout/stderr, so
	// a failed run is attributable instead of interleaved into the agent's
	// own log.
	LogPath string
	// Timeout is a hard wall-clock limit on the run. Zero means no limit.
	Timeout time.Duration
}

// Handle identifies a running container to the executor that started it.
type Handle string

// Status is the current state of a started task.
type Status struct {
	State    ExecState
	ExitCode int
	Message  string
	// Progress is 0..1 where the executor can report it, for the UI only. The
	// scheduler never reads this.
	Progress float64
}

// Executor runs containers.
//
// Implementations: containerd, simulated. Rejected alternatives: Kubernetes,
// because the scheduler is the project and k8s would do the interesting part;
// SSH execution, because it offers no resource limits and no clean kill.
type Executor interface {
	Start(ctx context.Context, spec StartSpec) (Handle, error)
	Stop(ctx context.Context, h Handle, grace time.Duration) error
	Status(ctx context.Context, h Handle) (Status, error)
	// Cleanup releases whatever resources Start acquired for h (container,
	// snapshot, simulator bookkeeping). It is a separate call from Status so
	// that a caller which stops polling a task (e.g. after preemption) still
	// has a way to reclaim resources without depending on one more Status
	// call happening to observe the terminal state first.
	Cleanup(ctx context.Context, h Handle) error
}

// HostProbe reports the machine's own resources. Not a seam -- there are four
// of those and this is not one of them -- just the local half of building a
// capability document.
type HostProbe func() (domain.Host, domain.Software)
