// Package sim provides simulated implementations of the three node-side seams.
//
// This is not a test double bolted on the side. It implements DeviceProvider,
// HealthSource and Executor exactly as the NVML, DCGM and containerd backends
// do, and the control plane cannot tell the difference. Mixed real/simulated
// fleets are normal and respected -- Kubernetes ships kubemark -- and the UI
// badges simulated nodes visibly, because a demo that discloses "8 simulated,
// 1 real" up front is stronger than one where the audience finds out later.
//
// It exists because gang deadlocks, fragmentation behaviour and reclaim races
// are not testable on a single machine, and because a live demo needs
// scriptable failure injection rather than luck.
package sim

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/alexk/orch/internal/agent"
	"github.com/alexk/orch/internal/domain"
)

// DeviceProfile describes one simulated accelerator.
//
// If the real machines in a fleet are identical, every placement is arbitrary
// and the scheduler has nothing to show. Profiles are how heterogeneity is
// manufactured: smaller cards, older compute capability, thermal problems.
type DeviceProfile struct {
	VRAMGB            int    `json:"vram_gb" yaml:"vram_gb"`
	Model             string `json:"model" yaml:"model"`
	ComputeCapability string `json:"compute_capability" yaml:"compute_capability"`
	EncodeSessions    int    `json:"encode_sessions" yaml:"encode_sessions"`
	DecodeSessions    int    `json:"decode_sessions" yaml:"decode_sessions"`
	ECCPresent        bool   `json:"ecc_present" yaml:"ecc_present"`
	// IdleTempC and TempUnderLoadC shape the thermal curve. A desk-side
	// workstation runs far hotter than racked hardware, and the thermal scorer
	// is only interesting when some nodes actually throttle.
	IdleTempC      float64 `json:"idle_temp_c" yaml:"idle_temp_c"`
	TempUnderLoadC float64 `json:"temp_under_load_c" yaml:"temp_under_load_c"`
	PowerW         float64 `json:"power_w" yaml:"power_w"`
}

// Profile is a complete simulated machine.
type Profile struct {
	Hostname  string            `json:"hostname" yaml:"hostname"`
	Devices   []DeviceProfile   `json:"devices" yaml:"devices"`
	Cores     int               `json:"cores" yaml:"cores"`
	RAMGB     int               `json:"ram_gb" yaml:"ram_gb"`
	ScratchGB int               `json:"scratch_gb" yaml:"scratch_gb"`
	PCIeGen   int               `json:"pcie_gen" yaml:"pcie_gen"`
	Driver    string            `json:"driver" yaml:"driver"`
	CUDA      string            `json:"cuda" yaml:"cuda"`
	Labels    map[string]string `json:"labels" yaml:"labels"`

	// TaskDuration is how long a simulated task takes. The demo targets 5-15
	// seconds per task: short enough that progress visibly streams and
	// preemption is cheap, long enough that scheduling overhead is not the
	// dominant term.
	TaskDurationMin time.Duration `json:"-" yaml:"-"`
	TaskDurationMax time.Duration `json:"-" yaml:"-"`

	// FailureRate injects occasional task failures so the reliability scorer
	// and the retry path have something to act on.
	FailureRate float64 `json:"failure_rate" yaml:"failure_rate"`

	// StagingDelay models a cold content-addressed cache. Early tasks are slow
	// while assets stage, then throughput jumps as caches fill -- which is one
	// of the rare features that is both genuinely useful and visible.
	StagingDelay time.Duration `json:"-" yaml:"-"`
}

// Node is the shared state behind the three simulated seams. They have to agree
// with each other: utilization reported by the health source must reflect what
// the executor is actually running.
type Node struct {
	profile Profile
	rng     *rand.Rand

	mu      sync.Mutex
	running map[agent.Handle]*simTask
	// busy counts active tasks per device index, driving utilization and
	// therefore temperature.
	busy map[int]int

	// Injected conditions, set by the trigger console. Live demos fail waiting
	// for organic events, so they are scripted instead.
	injectedThrottle map[int]bool
	injectedXID      map[int]int
	tempOffset       float64
}

type simTask struct {
	spec     agent.StartSpec
	deadline time.Time
	started  time.Time
	staging  time.Duration
	cancel   context.CancelFunc

	mu     sync.Mutex
	status agent.Status
}

// NewNode creates a simulated node from a profile.
func NewNode(p Profile) *Node {
	if p.TaskDurationMin <= 0 {
		p.TaskDurationMin = 5 * time.Second
	}
	if p.TaskDurationMax < p.TaskDurationMin {
		p.TaskDurationMax = 15 * time.Second
	}
	if p.Driver == "" {
		p.Driver = "580.95"
	}
	if p.CUDA == "" {
		p.CUDA = "13.0"
	}
	if p.Cores == 0 {
		p.Cores = 16
	}
	if p.RAMGB == 0 {
		p.RAMGB = 64
	}
	if p.ScratchGB == 0 {
		p.ScratchGB = 500
	}
	if p.PCIeGen == 0 {
		p.PCIeGen = 4
	}

	return &Node{
		profile:          p,
		rng:              rand.New(rand.NewSource(time.Now().UnixNano())),
		running:          map[agent.Handle]*simTask{},
		busy:             map[int]int{},
		injectedThrottle: map[int]bool{},
		injectedXID:      map[int]int{},
	}
}

// Profile returns the node's configuration.
func (n *Node) Profile() Profile { return n.profile }

// ---------------------------------------------------------------------------
// DeviceProvider
// ---------------------------------------------------------------------------

// Enumerate reports the simulated devices.
func (n *Node) Enumerate(context.Context) ([]domain.Device, error) {
	out := make([]domain.Device, 0, len(n.profile.Devices))
	for i, d := range n.profile.Devices {
		cc := d.ComputeCapability
		if cc == "" {
			cc = "8.9"
		}
		out = append(out, domain.Device{
			Index:             i,
			Vendor:            "nvidia",
			Model:             d.Model,
			VRAMGB:            d.VRAMGB,
			ComputeCapability: cc,
			MIGCapable:        false,
			EncodeSessions:    d.EncodeSessions,
			DecodeSessions:    d.DecodeSessions,
			ECCPresent:        d.ECCPresent,
		})
	}
	return out, nil
}

// HostInfo reports the simulated host and software versions.
func (n *Node) HostInfo() (domain.Host, domain.Software) {
	return domain.Host{
			Cores:     n.profile.Cores,
			RAMGB:     n.profile.RAMGB,
			ScratchGB: n.profile.ScratchGB,
			PCIeGen:   n.profile.PCIeGen,
		}, domain.Software{
			Driver:  n.profile.Driver,
			CUDA:    n.profile.CUDA,
			Runtime: "simulated",
		}
}

// Labels returns the node's static labels.
func (n *Node) Labels() map[string]string { return n.profile.Labels }

// ---------------------------------------------------------------------------
// HealthSource
// ---------------------------------------------------------------------------

// Stream emits telemetry derived from what the executor is actually running, so
// the fast-path samples the scheduler reads stay consistent with reality.
func (n *Node) Stream(ctx context.Context) (<-chan []domain.HealthSample, error) {
	ch := make(chan []domain.HealthSample, 4)

	go func() {
		defer close(ch)
		t := time.NewTicker(time.Second)
		defer t.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				select {
				case ch <- n.sample():
				default:
					// Consumer is behind; the next tick carries fresher data
					// than this one anyway.
				}
			}
		}
	}()

	return ch, nil
}

func (n *Node) sample() []domain.HealthSample {
	n.mu.Lock()
	defer n.mu.Unlock()

	now := time.Now()
	out := make([]domain.HealthSample, 0, len(n.profile.Devices))

	for i, d := range n.profile.Devices {
		util := 0.0
		if n.busy[i] > 0 {
			util = 0.85 + n.rng.Float64()*0.14
		} else {
			util = n.rng.Float64() * 0.03
		}

		idle := d.IdleTempC
		if idle == 0 {
			idle = 38
		}
		loaded := d.TempUnderLoadC
		if loaded == 0 {
			loaded = 72
		}
		temp := idle + (loaded-idle)*util + n.tempOffset + n.rng.Float64()*2

		var reasons []string
		if n.injectedThrottle[i] {
			reasons = append(reasons, "sw_thermal_slowdown")
		}
		if temp >= 84 {
			reasons = append(reasons, "hw_thermal_slowdown")
		}
		if len(reasons) == 0 {
			reasons = append(reasons, "none")
		}

		freeVRAM := d.VRAMGB
		if n.busy[i] > 0 {
			freeVRAM = int(math.Max(0, float64(d.VRAMGB)*0.15))
		}

		s := domain.HealthSample{
			DeviceIndex:     i,
			Utilization:     util,
			FreeVRAMGB:      freeVRAM,
			TemperatureC:    temp,
			PowerW:          d.PowerW * (0.3 + 0.7*util),
			ThrottleReasons: reasons,
			SampledAt:       now,
			// ECC is present on professional cards and absent on consumer
			// ones. The simulator honours that rather than reporting it
			// unconditionally, so the "never assume ECC" rule gets exercised.
			ECCSupported: d.ECCPresent,
		}
		if xid, ok := n.injectedXID[i]; ok {
			s.XIDErrors = []int{xid}
		}
		out = append(out, s)
	}
	return out
}

// ---------------------------------------------------------------------------
// Injection (trigger console)
// ---------------------------------------------------------------------------

// InjectThrottle makes a device report thermal throttling.
func (n *Node) InjectThrottle(deviceIndex int, on bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if on {
		n.injectedThrottle[deviceIndex] = true
		n.tempOffset = 18
	} else {
		delete(n.injectedThrottle, deviceIndex)
		n.tempOffset = 0
	}
}

// InjectXID makes a device report a hardware error, which takes it out of
// scheduling without disturbing what it is already running.
func (n *Node) InjectXID(deviceIndex, code int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if code == 0 {
		delete(n.injectedXID, deviceIndex)
		return
	}
	n.injectedXID[deviceIndex] = code
}

// ClearConditions removes every injected fault.
func (n *Node) ClearConditions() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.injectedThrottle = map[int]bool{}
	n.injectedXID = map[int]int{}
	n.tempOffset = 0
}

// ---------------------------------------------------------------------------
// Executor
// ---------------------------------------------------------------------------

// Start begins a simulated task run.
func (n *Node) Start(ctx context.Context, spec agent.StartSpec) (agent.Handle, error) {
	n.mu.Lock()

	dur := n.taskDuration(spec)
	staging := n.stagingFor(spec)

	h := agent.Handle(fmt.Sprintf("sim-%s", spec.LeaseID))
	runCtx, cancel := context.WithCancel(context.Background())

	t := &simTask{
		spec:     spec,
		started:  time.Now(),
		staging:  staging,
		deadline: time.Now().Add(staging + dur),
		cancel:   cancel,
		status:   agent.Status{State: agent.ExecStarting},
	}
	n.running[h] = t
	for _, idx := range spec.DeviceIndices {
		n.busy[idx]++
	}
	fail := n.rng.Float64() < n.profile.FailureRate
	n.mu.Unlock()

	go n.run(runCtx, h, t, staging, dur, fail)
	return h, nil
}

func (n *Node) run(ctx context.Context, h agent.Handle, t *simTask, staging, dur time.Duration, fail bool) {
	// Staging first: a cold cache is visibly slower, which is the point of
	// showing cache residency at all.
	if staging > 0 {
		select {
		case <-ctx.Done():
			n.finish(h, t, agent.Status{State: agent.ExecKilled, Message: "preempted during staging"})
			return
		case <-time.After(staging):
		}
	}

	t.setStatus(agent.Status{State: agent.ExecRunning})

	select {
	case <-ctx.Done():
		n.finish(h, t, agent.Status{State: agent.ExecKilled, Message: "preempted"})
	case <-time.After(dur):
		if fail {
			n.finish(h, t, agent.Status{
				State: agent.ExecFailed, ExitCode: 1,
				Message: "simulated task failure", Progress: 1,
			})
			return
		}
		writeSimOutput(t.spec, dur)
		n.finish(h, t, agent.Status{State: agent.ExecExited, ExitCode: 0, Progress: 1})
	}
}

// writeSimOutput drops a small object into the task's writable output mount
// (if any) before it finishes, so the agent's uploader has real bytes to ship
// to the object store even on a simulated node. This is what lets the whole
// data plane -- staging, mounting, uploading, manifesting -- be developed and
// tested without a GPU: the simulator is not a test fixture, it exercises the
// same seam a real container does.
func writeSimOutput(spec agent.StartSpec, dur time.Duration) {
	var outDir string
	for _, m := range spec.Mounts {
		if !m.ReadOnly && m.ContainerPath == agent.ContainerOutputDir {
			outDir = m.HostPath
			break
		}
	}
	if outDir == "" {
		return
	}
	var b strings.Builder
	fmt.Fprintf(&b, "task_index=%d\n", spec.Index)
	fmt.Fprintf(&b, "attempt=%d\n", spec.Attempt)
	fmt.Fprintf(&b, "duration_ms=%d\n", dur.Milliseconds())
	keys := make([]string, 0, len(spec.Params))
	for k := range spec.Params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&b, "param.%s=%s\n", k, spec.Params[k])
	}
	_ = os.WriteFile(filepath.Join(outDir, "output.txt"), []byte(b.String()), 0o644)
}

func (n *Node) finish(h agent.Handle, t *simTask, st agent.Status) {
	t.setStatus(st)

	n.mu.Lock()
	defer n.mu.Unlock()
	for _, idx := range t.spec.DeviceIndices {
		if n.busy[idx] > 0 {
			n.busy[idx]--
		}
	}
}

// Stop kills a running task. Preemption in v1 is kill-and-requeue, so there is
// no drain to wait for -- grace is honoured but a batch task has nothing to
// checkpoint.
func (n *Node) Stop(_ context.Context, h agent.Handle, grace time.Duration) error {
	n.mu.Lock()
	t := n.running[h]
	n.mu.Unlock()

	if t == nil {
		return nil
	}
	if grace > 0 {
		time.AfterFunc(grace, t.cancel)
	} else {
		t.cancel()
	}
	return nil
}

// Status reports the current state of a task.
func (n *Node) Status(_ context.Context, h agent.Handle) (agent.Status, error) {
	n.mu.Lock()
	t := n.running[h]
	n.mu.Unlock()

	if t == nil {
		return agent.Status{}, fmt.Errorf("unknown handle %s", h)
	}

	st := t.getStatus()
	if st.State == agent.ExecRunning {
		total := t.deadline.Sub(t.started.Add(t.staging)).Seconds()
		if total > 0 {
			elapsed := time.Since(t.started.Add(t.staging)).Seconds()
			st.Progress = math.Min(1, math.Max(0, elapsed/total))
		}
	}
	return st, nil
}

// Cleanup drops finished bookkeeping so a long-lived agent does not grow
// without bound. It is the simulated half of the Executor.Cleanup contract;
// unlike a real container there is nothing external to tear down, so this is
// just the map delete that used to be named Forget and never called.
func (n *Node) Cleanup(_ context.Context, h agent.Handle) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.running, h)
	return nil
}

func (n *Node) taskDuration(spec agent.StartSpec) time.Duration {
	// A task may state its own duration, which is how the demo scripts pace
	// itself without the simulator knowing what the workload is.
	if v, ok := spec.Params["sim_duration_ms"]; ok {
		if ms, err := time.ParseDuration(v + "ms"); err == nil && ms > 0 {
			return ms
		}
	}
	span := n.profile.TaskDurationMax - n.profile.TaskDurationMin
	if span <= 0 {
		return n.profile.TaskDurationMin
	}
	return n.profile.TaskDurationMin + time.Duration(n.rng.Int63n(int64(span)))
}

func (n *Node) stagingFor(spec agent.StartSpec) time.Duration {
	if n.profile.StagingDelay <= 0 || len(spec.Mounts) == 0 {
		return 0
	}
	return n.profile.StagingDelay
}

func (t *simTask) setStatus(s agent.Status) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// Never walk backwards out of a terminal state; a late tick must not
	// resurrect a killed task.
	switch t.status.State {
	case agent.ExecExited, agent.ExecFailed, agent.ExecKilled:
		return
	}
	t.status = s
}

func (t *simTask) getStatus() agent.Status {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.status
}

// Compile-time proof that the simulated node really does implement all three
// seams, rather than merely resembling them.
var (
	_ agent.DeviceProvider = (*Node)(nil)
	_ agent.HealthSource   = (*Node)(nil)
	_ agent.Executor       = (*Node)(nil)
)
