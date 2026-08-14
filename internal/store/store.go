// Package store defines the persistence boundary for the control plane.
//
// The scheduler reads a whole-fleet Snapshot, decides, and hands back a Plan
// that is applied in one transaction. That shape is deliberate: placement is
// inherently serialized against shared state, so rather than locking
// incrementally and hoping, the loop works from a consistent read and commits
// atomically with revalidation.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/alexk/orch/internal/domain"
)

// ErrNotFound is returned for a lookup by an ID nothing matches.
var ErrNotFound = errors.New("not found")

// ErrStaleLease is returned when an operation quotes a lease the control plane
// no longer recognises. The lease ID is the fencing token; a stale holder is
// rejected here rather than being allowed to mutate state.
var ErrStaleLease = errors.New("stale lease")

// ---------------------------------------------------------------------------
// Scheduling snapshot
// ---------------------------------------------------------------------------

// NodeView is a node plus everything placement needs to score it, gathered in
// one read so the scoring pass does no I/O.
type NodeView struct {
	domain.Node

	Slots []domain.Slot

	// Digests resident in this node's content-addressed cache (§11).
	CachedDigests []string

	// Host resources already committed to leases here. The GPU is partitioned;
	// the host is not, so this has to be tracked explicitly.
	AllocatedCores float64
	AllocatedRAMGB int

	// Tasks assigned or running on this node, for the queue_depth scorer.
	QueueDepth int

	// Decay-weighted outcome history behind the reliability scorer.
	RecentSuccesses float64
	RecentFailures  float64
}

// FreeSlots returns the slots available for allocation right now.
func (n *NodeView) FreeSlots() []domain.Slot {
	out := make([]domain.Slot, 0, len(n.Slots))
	for _, s := range n.Slots {
		if s.State == domain.SlotFree {
			out = append(out, s)
		}
	}
	return out
}

// HasLease reports whether any slot on this node is held by the given lease.
func (n *NodeView) HasLease(leaseID string) bool {
	for _, s := range n.Slots {
		if s.LeaseID == leaseID {
			return true
		}
	}
	return false
}

// QueuedTask pairs a task awaiting placement with the job that submitted it.
type QueuedTask struct {
	Task domain.Task
	Job  domain.Job
}

// Snapshot is the consistent read the placement loop works from.
type Snapshot struct {
	Nodes  []*NodeView
	Pools  []domain.Pool
	Leases []domain.Lease
	Queued []QueuedTask
	Now    time.Time
}

// NodeByID returns the node view with the given ID, or nil.
func (s *Snapshot) NodeByID(id string) *NodeView {
	for _, n := range s.Nodes {
		if n.NodeID == id {
			return n
		}
	}
	return nil
}

// PoolByID returns the pool with the given ID, or nil.
func (s *Snapshot) PoolByID(id string) *domain.Pool {
	for i := range s.Pools {
		if s.Pools[i].PoolID == id {
			return &s.Pools[i]
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Placement plan
// ---------------------------------------------------------------------------

// Placement is one lease to grant. For a multi-slot request every slot is on
// the same node, so granting it is one row plus N slot updates -- a gang
// reservation, not a distributed protocol.
type Placement struct {
	LeaseID string
	TaskID  string
	JobID   string
	// PoolID is the pool that requested the capacity, which is not necessarily
	// the pool owning the node.
	PoolID  string
	NodeID  string
	SlotIDs []string

	Mode          domain.LeaseMode
	Borrowed      bool
	ExclusiveHost bool

	HostShareCores float64
	HostShareRAMGB int

	TTL          time.Duration
	Attempt      int
	OutputPrefix string
}

// Eviction is one lease to revoke. Batch leases are killed and requeued at task
// granularity, so the cost is one task's recompute rather than a whole job.
type Eviction struct {
	LeaseID string
	Reason  string
	Requeue bool
	Grace   time.Duration
}

// Plan is the output of one placement pass.
type Plan struct {
	// NonLendable nodes are closed to new borrowers before any eviction runs.
	// A machine reclaim racing against new placements will livelock, so the
	// order here is load-bearing rather than cosmetic.
	NonLendable []string

	Evictions  []Eviction
	Placements []Placement
}

// Empty reports whether the plan would change nothing.
func (p *Plan) Empty() bool {
	return len(p.NonLendable) == 0 && len(p.Evictions) == 0 && len(p.Placements) == 0
}

// CommitResult reports what the transaction actually applied. A placement can
// be dropped if the slots it chose stopped being free between the snapshot and
// the commit; the scheduler simply retries it on the next pass.
type CommitResult struct {
	Placed  []Placement
	Evicted []Eviction
	// Rejected placements, keyed by lease ID, with the reason revalidation
	// failed.
	Rejected map[string]string
}

// ---------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------

// NodeRegistration is what an agent reports when it joins or reconnects.
type NodeRegistration struct {
	NodeID       string
	Hostname     string
	Capabilities domain.CapabilityDoc
	Simulated    bool
	JoinToken    string
}

// HeartbeatUpdate is the fast-path telemetry write on every heartbeat.
type HeartbeatUpdate struct {
	NodeID        string
	Samples       []domain.HealthSample
	CachedDigests []string
	LoadAvg1m     float64
	FreeScratchGB int
	At            time.Time
}

// TaskStatus is an agent-reported task transition, fenced by lease ID.
type TaskStatus struct {
	LeaseID      string
	TaskID       string
	State        domain.TaskState
	Message      string
	ExitCode     int
	OutputPrefix string
	At           time.Time
}

// Store is the whole persistence surface. It is an interface so the control
// plane can be tested without a database, not so it can be swapped in
// production -- Postgres is a deliberate choice, not an implementation detail.
type Store interface {
	// --- fleet ---

	// RegisterNode creates or reconciles a node and its slots from a
	// self-reported capability document, consuming the join token if the node
	// is new. Returns the assigned node ID and its currently active leases so
	// the agent can kill anything not in that set.
	RegisterNode(ctx context.Context, reg NodeRegistration) (nodeID string, activeLeases []string, err error)
	Heartbeat(ctx context.Context, up HeartbeatUpdate) error
	SetNodeState(ctx context.Context, nodeID string, state domain.NodeState) error
	SetNodeLendable(ctx context.Context, nodeID string, lendable bool) error
	AssignNodePool(ctx context.Context, nodeID, poolID string) error
	ListNodes(ctx context.Context) ([]*NodeView, error)
	GetNode(ctx context.Context, nodeID string) (*NodeView, error)

	// MarkStaleNodesOffline flags nodes that have missed heartbeats past the
	// cutoff and returns their IDs. No assumption is made that a node returns
	// with prior state.
	MarkStaleNodesOffline(ctx context.Context, cutoff time.Time) ([]string, error)

	// --- pools ---

	UpsertPool(ctx context.Context, p domain.Pool) error
	ListPools(ctx context.Context) ([]domain.Pool, error)

	// --- work ---

	CreateJob(ctx context.Context, job domain.Job, tasks []domain.Task) error
	ListJobs(ctx context.Context, owner string) ([]domain.Job, error)
	GetJob(ctx context.Context, jobID string) (domain.Job, []domain.Task, error)
	CancelJob(ctx context.Context, jobID string) ([]string, error)
	ListTasks(ctx context.Context, jobID string) ([]domain.Task, error)

	// ApplyTaskStatus records an agent-reported transition. It rejects updates
	// quoting a lease that is no longer active, releases the lease on terminal
	// states, and folds the outcome into the node's reliability counters.
	ApplyTaskStatus(ctx context.Context, st TaskStatus) error

	// --- leases ---

	ListActiveLeases(ctx context.Context) ([]domain.Lease, error)
	// RenewLeases extends the given leases held by one node, returning which
	// were accepted and which are no longer recognised.
	RenewLeases(ctx context.Context, nodeID string, leaseIDs []string, at time.Time) (renewed, rejected []string, err error)
	// ExpireLeases releases leases unrenewed past their TTL and requeues their
	// batch tasks. Returns the released leases.
	ExpireLeases(ctx context.Context, now time.Time) ([]domain.Lease, error)
	// ReleaseLeasesOnNode revokes every active lease on a node, for use when
	// the node disconnects or fails.
	ReleaseLeasesOnNode(ctx context.Context, nodeID, reason string, requeue bool) ([]domain.Lease, error)

	// --- placement ---

	// Snapshot is the consistent whole-fleet read the placement loop uses.
	Snapshot(ctx context.Context, queueLimit int) (*Snapshot, error)
	// Commit applies a plan in one transaction, revalidating every slot it
	// touches. Gang reservation is all-or-nothing per placement.
	Commit(ctx context.Context, plan Plan) (CommitResult, error)

	// --- policy ---

	GetPolicy(ctx context.Context) (Policy, error)
	SetPolicy(ctx context.Context, preset string, weights map[string]float64) (Policy, error)

	// --- onboarding ---

	IssueJoinToken(ctx context.Context, poolID string) (string, error)

	// --- observability ---

	SaveExplanation(ctx context.Context, taskID string, payload []byte) error
	GetExplanation(ctx context.Context, taskID string) ([]byte, error)

	Close()
}

// Policy is the scoring configuration. It lives in the database rather than in
// a config file so it can be edited live and reloaded without a restart.
type Policy struct {
	Preset           string             `json:"preset"`
	Weights          map[string]float64 `json:"weights"`
	ReloadGeneration int64              `json:"reload_generation"`
}
