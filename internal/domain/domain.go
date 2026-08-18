// Package domain holds the core types the control plane reasons about.
//
// Two rules govern this package:
//
//  1. Type names are vendor-neutral. `Device`, not `NvidiaGPU`. Costs nothing
//     now, keeps the door open.
//  2. Nothing here names a GPU model, and neither does anything that consumes
//     it. Placement reads capabilities only. A fleet with entirely different
//     hardware works with zero code change.
package domain

import (
	"time"
)

// ---------------------------------------------------------------------------
// States
// ---------------------------------------------------------------------------

// NodeState tracks a machine through its lifecycle. Workstations are not
// servers -- users reboot, sleep, and unplug them -- so churn between these
// states is routine, not an incident.
type NodeState string

const (
	NodeReady     NodeState = "ready"
	NodeDraining  NodeState = "draining"
	NodeUnhealthy NodeState = "unhealthy"
	NodeOffline   NodeState = "offline"
)

// SlotState is the allocation status of the unit the scheduler hands out.
type SlotState string

const (
	SlotFree      SlotState = "free"
	SlotAllocated SlotState = "allocated"
	SlotDraining  SlotState = "draining"
	SlotUnhealthy SlotState = "unhealthy"
)

// LeaseState tracks a claim on slots.
type LeaseState string

const (
	LeaseActive    LeaseState = "active"
	LeaseReleased  LeaseState = "released"
	LeasePreempted LeaseState = "preempted"
	LeaseExpired   LeaseState = "expired"
)

// LeaseMode determines how a lease ends and how preemption treats it.
type LeaseMode string

const (
	// ModeBatch ends when the container exits. Preemption kills and requeues
	// the task.
	ModeBatch LeaseMode = "batch"
	// ModeService ends on explicit release or TTL expiry without renewal.
	// Graceful drain is deferred past v1; the field exists so it is additive.
	ModeService LeaseMode = "service"
)

// TaskState is the task state machine. Tasks, not jobs, are the unit of
// requeue, which is what bounds the cost of preemption.
type TaskState string

const (
	TaskQueued    TaskState = "queued"
	TaskAssigned  TaskState = "assigned"
	TaskStaging   TaskState = "staging"
	TaskRunning   TaskState = "running"
	TaskDone      TaskState = "done"
	TaskFailed    TaskState = "failed"
	TaskPreempted TaskState = "preempted"
)

// Terminal reports whether a task will never transition again.
func (s TaskState) Terminal() bool { return s == TaskDone || s == TaskFailed }

// JobState summarises the tasks beneath a job.
type JobState string

const (
	JobPending   JobState = "pending"
	JobRunning   JobState = "running"
	JobCompleted JobState = "completed"
	JobFailed    JobState = "failed"
	JobCancelled JobState = "cancelled"
)

// SlotKind distinguishes a whole device from a partition of one. The scheduler
// does not branch on this; it exists for display and inventory.
type SlotKind string

const (
	KindGPU         SlotKind = "gpu"
	KindMIGInstance SlotKind = "mig_instance"
)

// ---------------------------------------------------------------------------
// Capability document (§6)
// ---------------------------------------------------------------------------

// Device is one schedulable piece of accelerator hardware as reported by the
// node that owns it.
type Device struct {
	Index int `json:"index"`

	Vendor string `json:"vendor"`
	// Model is informational. Displaying it is fine; branching on it is not.
	Model string `json:"model"`

	VRAMGB            int    `json:"vram_gb"`
	ComputeCapability string `json:"compute_capability"`

	MIGCapable bool   `json:"mig_capable"`
	MIGProfile string `json:"mig_profile,omitempty"`

	EncodeSessions int `json:"encode_sessions"`
	DecodeSessions int `json:"decode_sessions"`

	// ECC and row-remapping are present on professional cards and absent on
	// consumer ones. Treated as an optional per-device capability, never
	// assumed.
	ECCPresent bool `json:"ecc_present"`
}

// Host describes the machine the devices are attached to. A borrowed slot
// competes for these; the GPU is partitioned, the host is not.
type Host struct {
	Cores     int `json:"cores"`
	RAMGB     int `json:"ram_gb"`
	ScratchGB int `json:"scratch_gb"`
	PCIeGen   int `json:"pcie_gen"`
}

// Software carries the version floors a workload may require.
type Software struct {
	Driver  string `json:"driver"`
	CUDA    string `json:"cuda"`
	Runtime string `json:"runtime"`
}

// CapabilityDoc is the portability contract: an agent self-reports, and the
// scheduler consumes it generically.
type CapabilityDoc struct {
	Devices  []Device          `json:"devices"`
	Host     Host              `json:"host"`
	Software Software          `json:"software"`
	Labels   map[string]string `json:"labels,omitempty"`
}

// ---------------------------------------------------------------------------
// Health (§9)
// ---------------------------------------------------------------------------

// HealthSample is the fast-path telemetry struct the agent pushes on every
// heartbeat. This is what the scheduler reads -- never Prometheus, which would
// couple placement latency to the monitoring stack and schedule on data up to
// a scrape interval stale.
type HealthSample struct {
	DeviceIndex  int     `json:"device_index"`
	Utilization  float64 `json:"utilization"`
	FreeVRAMGB   int     `json:"free_vram_gb"`
	TemperatureC float64 `json:"temperature_c"`
	PowerW       float64 `json:"power_w"`

	// ThrottleReasons distinguish "slow because busy" from "slow because
	// degraded". A scheduler that keeps assigning to a thermally capped card
	// is actively making things worse.
	ThrottleReasons []string `json:"throttle_reasons,omitempty"`

	PCIeReplayCount uint64 `json:"pcie_replay_count"`
	XIDErrors       []int  `json:"xid_errors,omitempty"`

	ECCSupported      bool   `json:"ecc_supported"`
	ECCVolatileErrors uint64 `json:"ecc_volatile_errors"`
	RowRemapPending   uint64 `json:"row_remap_pending"`

	SampledAt time.Time `json:"sampled_at"`
}

// Throttled reports whether the device is being held back for any reason other
// than having work to do.
func (h HealthSample) Throttled() bool {
	for _, r := range h.ThrottleReasons {
		switch r {
		case "", "gpu_idle", "none":
			continue
		}
		return true
	}
	return false
}

// Degraded reports hardware trouble that should stop new placement onto this
// device. XID errors and pending row remaps are not transient.
func (h HealthSample) Degraded() bool {
	if len(h.XIDErrors) > 0 {
		return true
	}
	return h.ECCSupported && h.RowRemapPending > 0
}

// ---------------------------------------------------------------------------
// Fleet
// ---------------------------------------------------------------------------

// Slot is the unit of allocation: either a whole device or a partition of one.
// Each slot carries a derived share of host resources.
type Slot struct {
	SlotID      string   `json:"slot_id"`
	NodeID      string   `json:"node_id"`
	Kind        SlotKind `json:"kind"`
	DeviceIndex int      `json:"device_index"`

	VRAMGB            int    `json:"vram_gb"`
	ComputeCapability string `json:"compute_capability"`
	EncodeSessions    int    `json:"encode_sessions"`
	DecodeSessions    int    `json:"decode_sessions"`
	ECCPresent        bool   `json:"ecc_present"`

	State   SlotState `json:"state"`
	LeaseID string    `json:"lease_id,omitempty"`

	Health HealthSample `json:"health"`
}

// Node is a machine in the fleet. Pool ownership is at machine granularity;
// allocation is at slot granularity.
type Node struct {
	NodeID   string    `json:"node_id"`
	Hostname string    `json:"hostname"`
	PoolID   string    `json:"pool_id"`
	State    NodeState `json:"state"`

	Capabilities CapabilityDoc     `json:"capability_doc"`
	Labels       map[string]string `json:"labels,omitempty"`

	// Simulated nodes are a legitimate implementation of three interfaces, not
	// a test fixture -- but they are badged visibly in the UI regardless.
	Simulated bool `json:"simulated"`

	LastHeartbeat time.Time `json:"last_heartbeat"`

	HostCores     int `json:"host_cores"`
	HostRAMGB     int `json:"host_ram_gb"`
	HostScratchGB int `json:"host_scratch_gb"`

	// Lendable is cleared for the duration of a machine reclaim. A reclaim
	// racing against new placements will livelock, so the host stops accepting
	// borrowers before eviction begins.
	Lendable bool `json:"lendable"`

	LoadAvg1m     int `json:"-"`
	FreeScratchGB int `json:"free_scratch_gb"`

	Slots []Slot `json:"slots,omitempty"`
}

// Healthy reports whether the node may receive new placements.
func (n *Node) Healthy() bool { return n.State == NodeReady }

// ---------------------------------------------------------------------------
// Pools (§8)
// ---------------------------------------------------------------------------

// Pool is a named set of whole machines with a guaranteed quota, a lendable
// flag, and a borrow ceiling.
//
// Empty configuration is a valid state: one implicit `default` pool containing
// everything, no quotas, no borrowing. Pools are a refinement an admin opts
// into, never a prerequisite.
type Pool struct {
	PoolID string `json:"pool_id"`
	Name   string `json:"name"`

	// GuaranteedSlots is capacity this pool can always claim, by preempting
	// borrowers if necessary.
	GuaranteedSlots int `json:"guaranteed_slots"`

	// Lendable allows idle guaranteed capacity to be taken by other pools.
	Lendable bool `json:"lendable"`

	// BorrowCeiling caps how much this pool may take from others. Zero means
	// this pool never borrows.
	BorrowCeiling int `json:"borrow_ceiling"`
}

// DefaultPoolID is the implicit pool every node lands in until an admin says
// otherwise.
const DefaultPoolID = "default"

// ---------------------------------------------------------------------------
// Leases (§3)
// ---------------------------------------------------------------------------

// Lease is a time-bounded claim on one or more slots, held by a workload.
//
// The lease ID doubles as the fencing token: a status update quoting a lease
// the control plane no longer recognises is rejected at commit time, so a
// zombie worker from a preempted attempt cannot corrupt state.
type Lease struct {
	LeaseID string `json:"lease_id"`
	TaskID  string `json:"task_id"`
	JobID   string `json:"job_id"`
	PoolID  string `json:"pool_id"`
	NodeID  string `json:"node_id"`

	SlotIDs []string  `json:"slot_ids"`
	Mode    LeaseMode `json:"mode"`

	// Borrowed is what makes preemption ordering trivially correct: borrowed
	// leases are always evicted before native ones in the owning pool.
	Borrowed bool `json:"borrowed"`

	ExclusiveHost bool `json:"exclusive_host"`

	HostShareCores float64 `json:"host_share_cores"`
	HostShareRAMGB int     `json:"host_share_ram_gb"`

	GrantedAt time.Time     `json:"granted_at"`
	TTL       time.Duration `json:"ttl"`
	RenewedAt time.Time     `json:"renewed_at"`
	State     LeaseState    `json:"state"`
}

// Expired reports whether the lease has gone unrenewed past its TTL. Short TTLs
// with renewal are what make fast reschedule on disconnect safe.
func (l *Lease) Expired(now time.Time) bool {
	last := l.RenewedAt
	if last.IsZero() {
		last = l.GrantedAt
	}
	return now.Sub(last) > l.TTL
}

// ---------------------------------------------------------------------------
// Work
// ---------------------------------------------------------------------------

// ResourceRequest is the hard, boolean half of scheduling: does the request
// fit? Every field is a capability predicate.
type ResourceRequest struct {
	Slots int `json:"slots"`

	MinVRAMGB            int    `json:"min_vram_gb"`
	MinComputeCapability string `json:"min_compute_capability,omitempty"`
	MinDriver            string `json:"min_driver,omitempty"`
	MinCUDA              string `json:"min_cuda,omitempty"`
	MinEncodeSessions    int    `json:"min_encode_sessions"`
	MinDecodeSessions    int    `json:"min_decode_sessions"`
	RequireECC           bool   `json:"require_ecc"`

	RequiredLabels map[string]string `json:"required_labels,omitempty"`

	// ExclusiveHost is for workloads sensitive to host contention or using
	// peer-to-peer transfers between devices.
	ExclusiveHost bool `json:"exclusive_host"`
}

// AssetRef is a content-addressed input. The digest is also the node cache key,
// which is what makes cache residency a scoring signal.
type AssetRef struct {
	Digest    string `json:"digest"`
	URI       string `json:"uri"`
	SizeBytes uint64 `json:"size_bytes"`
	MountPath string `json:"mount_path"`
}

// Job is a submission. It may fan out into many tasks.
type Job struct {
	JobID  string `json:"job_id"`
	Name   string `json:"name"`
	Owner  string `json:"owner"`
	PoolID string `json:"pool_id"`

	ImageDigest string            `json:"image_digest"`
	Command     []string          `json:"command,omitempty"`
	Env         map[string]string `json:"env,omitempty"`
	Assets      []AssetRef        `json:"assets,omitempty"`

	Request     ResourceRequest `json:"request"`
	Mode        LeaseMode       `json:"mode"`
	Preemptible bool            `json:"preemptible"`

	// Timeout is a hard wall-clock limit per task. Zero means no limit --
	// nothing else stops a hung CUDA kernel from holding a slot forever.
	Timeout time.Duration `json:"timeout,omitempty"`

	State       JobState  `json:"state"`
	SubmittedAt time.Time `json:"submitted_at"`
}

// Task is the unit of work and of requeue. Preemption operates on tasks, so its
// cost is bounded to one task's recompute rather than a whole job.
type Task struct {
	TaskID string `json:"task_id"`
	JobID  string `json:"job_id"`
	Index  int    `json:"index"`

	// Params are opaque to the orchestrator. A render job puts a frame number
	// here; the scheduler never learns what a frame is.
	Params map[string]string `json:"params,omitempty"`

	Attempt int       `json:"attempt"`
	State   TaskState `json:"state"`

	AssignedLeaseID string `json:"assigned_lease_id,omitempty"`
	NodeID          string `json:"node_id,omitempty"`

	// Each attempt writes to a unique prefix, so a zombie worker from a
	// preempted attempt writes somewhere nobody reads and no storage-layer
	// fencing is required.
	OutputPrefix string `json:"output_prefix,omitempty"`

	Preemptions int `json:"preemptions"`

	// ExitCode is nil until the task finishes -- "not yet finished" and
	// "exited 0" are different facts, so this is a pointer rather than a
	// plain int, mirrored by a nullable store column.
	ExitCode *int `json:"exit_code,omitempty"`

	StartedAt  time.Time `json:"started_at,omitempty"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
	Message    string    `json:"message,omitempty"`
}

// AttemptPrefix returns the object-store prefix for a given attempt.
func AttemptPrefix(jobID, taskID string, attempt int) string {
	return "jobs/" + jobID + "/tasks/" + taskID + "/attempts/" + itoa(attempt) + "/"
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
