package pgstore_test

// Integration tests against a real Postgres.
//
// These cover the properties that only exist at the SQL layer -- gang
// atomicity, lease fencing, requeue-on-preemption -- and that a fake store
// would assert about itself rather than about the system.
//
// Set ORCH_TEST_DSN to run them:
//
//	createdb orch_test
//	ORCH_TEST_DSN="postgres:///orch_test?host=/var/run/postgresql" go test ./internal/store/pgstore/

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/alexk/orch/internal/domain"
	"github.com/alexk/orch/internal/store"
	"github.com/alexk/orch/internal/store/pgstore"
)

func newStore(t *testing.T) *pgstore.Store {
	t.Helper()

	dsn := os.Getenv("ORCH_TEST_DSN")
	if dsn == "" {
		t.Skip("set ORCH_TEST_DSN to run Postgres integration tests")
	}

	ctx := context.Background()
	st, err := pgstore.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(st.Close)

	// Each test starts from a known fleet. Truncate rather than recreate: the
	// migrations have already run and the default pool must survive.
	_, err = st.Pool().Exec(ctx, `
		TRUNCATE leases, tasks, jobs, slots, nodes, placement_explanations,
		         join_tokens RESTART IDENTITY CASCADE;
		DELETE FROM pools WHERE pool_id <> 'default';`)
	if err != nil {
		t.Fatalf("reset: %v", err)
	}
	return st
}

// registerNode enrols a node with n identical devices.
func registerNode(t *testing.T, st *pgstore.Store, hostname string, devices int, vramGB int) string {
	t.Helper()

	doc := domain.CapabilityDoc{
		Host:     domain.Host{Cores: 32, RAMGB: 256, ScratchGB: 1000, PCIeGen: 5},
		Software: domain.Software{Driver: "580.95", CUDA: "13.0"},
	}
	for i := 0; i < devices; i++ {
		doc.Devices = append(doc.Devices, domain.Device{
			Index: i, Vendor: "test", Model: "test", VRAMGB: vramGB,
			ComputeCapability: "8.9", ECCPresent: true,
		})
	}

	nodeID, _, err := st.RegisterNode(context.Background(), store.NodeRegistration{
		Hostname: hostname, Capabilities: doc, Simulated: true,
	})
	if err != nil {
		t.Fatalf("register %s: %v", hostname, err)
	}
	return nodeID
}

func createJob(t *testing.T, st *pgstore.Store, poolID string, taskCount int) (domain.Job, []domain.Task) {
	t.Helper()

	job := domain.Job{
		JobID: "job-" + poolID, PoolID: poolID, ImageDigest: "sha256:test",
		Mode: domain.ModeBatch, State: domain.JobPending, SubmittedAt: time.Now(),
		Request: domain.ResourceRequest{Slots: 1},
	}
	var tasks []domain.Task
	for i := 0; i < taskCount; i++ {
		tasks = append(tasks, domain.Task{
			TaskID: fmt.Sprintf("task-%s-%d", poolID, i),
			JobID:  job.JobID, Index: i, State: domain.TaskQueued,
		})
	}
	if err := st.CreateJob(context.Background(), job, tasks); err != nil {
		t.Fatalf("create job: %v", err)
	}
	return job, tasks
}

func slotIDs(t *testing.T, st *pgstore.Store, nodeID string, n int) []string {
	t.Helper()

	node, err := st.GetNode(context.Background(), nodeID)
	if err != nil {
		t.Fatalf("get node: %v", err)
	}
	if len(node.Slots) < n {
		t.Fatalf("node has %d slots, need %d", len(node.Slots), n)
	}
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = node.Slots[i].SlotID
	}
	return out
}

// ---------------------------------------------------------------------------

func TestMigrationsSeedTheImplicitDefaultPool(t *testing.T) {
	st := newStore(t)

	pools, err := st.ListPools(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// An empty database is a valid state: one implicit pool containing
	// everything, so a fresh install schedules before anyone configures a pool.
	if len(pools) != 1 || pools[0].PoolID != domain.DefaultPoolID {
		t.Fatalf("expected only the default pool, got %+v", pools)
	}
}

func TestRegisterNodeDerivesSlotsFromCapabilities(t *testing.T) {
	st := newStore(t)
	nodeID := registerNode(t, st, "ws-01", 4, 48)

	node, err := st.GetNode(context.Background(), nodeID)
	if err != nil {
		t.Fatal(err)
	}
	if len(node.Slots) != 4 {
		t.Fatalf("expected 4 slots derived from the capability document, got %d", len(node.Slots))
	}
	if node.PoolID != domain.DefaultPoolID {
		t.Fatalf("a node with no join token should land in the default pool, got %s", node.PoolID)
	}
	for _, s := range node.Slots {
		if s.VRAMGB != 48 || s.State != domain.SlotFree {
			t.Fatalf("unexpected slot %+v", s)
		}
	}
}

// Re-registering must not duplicate slots or disturb running work.
func TestReRegisterIsIdempotent(t *testing.T) {
	st := newStore(t)
	nodeID := registerNode(t, st, "ws-01", 2, 48)
	again := registerNode(t, st, "ws-01", 2, 48)

	if again == nodeID {
		t.Log("same node ID reused")
	}
	node, err := st.GetNode(context.Background(), nodeID)
	if err != nil {
		t.Fatal(err)
	}
	if len(node.Slots) != 2 {
		t.Fatalf("re-registration duplicated slots: got %d", len(node.Slots))
	}
}

// A multi-slot request is all-or-nothing. Allocating one slot at a time
// deadlocks when two multi-slot jobs interleave.
func TestGangReservationIsAllOrNothing(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)

	nodeID := registerNode(t, st, "ws-01", 4, 48)
	job, tasks := createJob(t, st, "default", 2)
	slots := slotIDs(t, st, nodeID, 4)

	// Take two of the four slots.
	first, err := st.Commit(ctx, store.Plan{Placements: []store.Placement{{
		LeaseID: "lease-a", TaskID: tasks[0].TaskID, JobID: job.JobID,
		PoolID: "default", NodeID: nodeID, SlotIDs: slots[0:2],
		Mode: domain.ModeBatch, TTL: time.Minute,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Placed) != 1 {
		t.Fatalf("expected the first gang to be granted, rejected: %v", first.Rejected)
	}

	// Now ask for three slots, one of which is already taken. Nothing should be
	// granted -- not the two that happen to be free.
	second, err := st.Commit(ctx, store.Plan{Placements: []store.Placement{{
		LeaseID: "lease-b", TaskID: tasks[1].TaskID, JobID: job.JobID,
		PoolID: "default", NodeID: nodeID, SlotIDs: slots[1:4],
		Mode: domain.ModeBatch, TTL: time.Minute,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Placed) != 0 {
		t.Fatal("a gang overlapping an allocated slot must not be partially granted")
	}
	if _, ok := second.Rejected["lease-b"]; !ok {
		t.Fatalf("expected a rejection reason for lease-b, got %v", second.Rejected)
	}

	node, err := st.GetNode(ctx, nodeID)
	if err != nil {
		t.Fatal(err)
	}
	free := 0
	for _, s := range node.Slots {
		if s.State == domain.SlotFree {
			free++
		}
	}
	if free != 2 {
		t.Fatalf("rejected gang leaked an allocation: %d slots free, want 2", free)
	}
}

// Each attempt writes to its own prefix, so a preempted attempt's output is
// orphaned rather than overwritten.
func TestCommitAssignsUniqueAttemptPrefix(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)

	nodeID := registerNode(t, st, "ws-01", 2, 48)
	job, tasks := createJob(t, st, "default", 1)
	slots := slotIDs(t, st, nodeID, 2)

	res, err := st.Commit(ctx, store.Plan{Placements: []store.Placement{{
		LeaseID: "lease-a", TaskID: tasks[0].TaskID, JobID: job.JobID,
		PoolID: "default", NodeID: nodeID, SlotIDs: slots[0:1],
		Mode: domain.ModeBatch, TTL: time.Minute,
	}}})
	if err != nil || len(res.Placed) != 1 {
		t.Fatalf("first placement failed: %v %v", err, res.Rejected)
	}
	firstPrefix := res.Placed[0].OutputPrefix
	if firstPrefix == "" {
		t.Fatal("commit must report the output prefix back to the caller")
	}

	// Preempt and requeue, then place the retry.
	if _, err := st.Commit(ctx, store.Plan{Evictions: []store.Eviction{{
		LeaseID: "lease-a", Reason: "test", Requeue: true,
	}}}); err != nil {
		t.Fatal(err)
	}

	res2, err := st.Commit(ctx, store.Plan{Placements: []store.Placement{{
		LeaseID: "lease-b", TaskID: tasks[0].TaskID, JobID: job.JobID,
		PoolID: "default", NodeID: nodeID, SlotIDs: slots[0:1],
		Mode: domain.ModeBatch, TTL: time.Minute,
	}}})
	if err != nil || len(res2.Placed) != 1 {
		t.Fatalf("retry placement failed: %v %v", err, res2.Rejected)
	}
	if res2.Placed[0].OutputPrefix == firstPrefix {
		t.Fatalf("attempt 1 reused attempt 0's prefix (%s); a zombie writer would corrupt it",
			firstPrefix)
	}
}

// The lease ID is the fencing token. A worker from a preempted attempt is still
// alive and still reporting; its updates must be refused.
func TestStaleLeaseIsRejected(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)

	nodeID := registerNode(t, st, "ws-01", 2, 48)
	job, tasks := createJob(t, st, "default", 1)
	slots := slotIDs(t, st, nodeID, 2)

	if _, err := st.Commit(ctx, store.Plan{Placements: []store.Placement{{
		LeaseID: "lease-a", TaskID: tasks[0].TaskID, JobID: job.JobID,
		PoolID: "default", NodeID: nodeID, SlotIDs: slots[0:1],
		Mode: domain.ModeBatch, TTL: time.Minute,
	}}}); err != nil {
		t.Fatal(err)
	}

	// A live lease accepts updates.
	err := st.ApplyTaskStatus(ctx, store.TaskStatus{
		LeaseID: "lease-a", TaskID: tasks[0].TaskID,
		State: domain.TaskRunning, At: time.Now(),
	})
	if err != nil {
		t.Fatalf("running update on a live lease should succeed: %v", err)
	}

	if _, err := st.Commit(ctx, store.Plan{Evictions: []store.Eviction{{
		LeaseID: "lease-a", Reason: "preempted", Requeue: true,
	}}}); err != nil {
		t.Fatal(err)
	}

	// The zombie reports success after being preempted.
	err = st.ApplyTaskStatus(ctx, store.TaskStatus{
		LeaseID: "lease-a", TaskID: tasks[0].TaskID,
		State: domain.TaskDone, At: time.Now(),
	})
	if !errors.Is(err, store.ErrStaleLease) {
		t.Fatalf("expected ErrStaleLease from a preempted holder, got %v", err)
	}

	// And the task is still queued for its retry, not marked done.
	_, after, err := st.GetJob(ctx, job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if after[0].State != domain.TaskQueued {
		t.Fatalf("a stale holder changed task state to %s", after[0].State)
	}
}

func TestPreemptionRequeuesAndCountsAttempts(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)

	nodeID := registerNode(t, st, "ws-01", 2, 48)
	job, tasks := createJob(t, st, "default", 1)
	slots := slotIDs(t, st, nodeID, 2)

	if _, err := st.Commit(ctx, store.Plan{Placements: []store.Placement{{
		LeaseID: "lease-a", TaskID: tasks[0].TaskID, JobID: job.JobID,
		PoolID: "default", NodeID: nodeID, SlotIDs: slots[0:1],
		Mode: domain.ModeBatch, TTL: time.Minute,
	}}}); err != nil {
		t.Fatal(err)
	}

	if _, err := st.Commit(ctx, store.Plan{Evictions: []store.Eviction{{
		LeaseID: "lease-a", Reason: "owner reclaimed", Requeue: true,
	}}}); err != nil {
		t.Fatal(err)
	}

	_, after, err := st.GetJob(ctx, job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	task := after[0]
	if task.State != domain.TaskQueued {
		t.Fatalf("preempted batch task should requeue, got %s", task.State)
	}
	if task.Attempt != 1 || task.Preemptions != 1 {
		t.Fatalf("expected attempt=1 preemptions=1, got attempt=%d preemptions=%d",
			task.Attempt, task.Preemptions)
	}

	node, err := st.GetNode(ctx, nodeID)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range node.Slots {
		if s.State != domain.SlotFree {
			t.Fatalf("preemption left slot %s in state %s", s.SlotID, s.State)
		}
	}
}

// Machine reclaim closes the host to borrowers in the same transaction that
// evicts them, so the scheduler cannot refill the slots behind the reclaim.
func TestCommitMarksNonLendableBeforeEvicting(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)

	nodeID := registerNode(t, st, "ws-01", 2, 48)
	job, tasks := createJob(t, st, "default", 1)
	slots := slotIDs(t, st, nodeID, 2)

	if _, err := st.Commit(ctx, store.Plan{Placements: []store.Placement{{
		LeaseID: "lease-a", TaskID: tasks[0].TaskID, JobID: job.JobID,
		PoolID: "borrower", NodeID: nodeID, SlotIDs: slots[0:1],
		Mode: domain.ModeBatch, Borrowed: true, TTL: time.Minute,
	}}}); err != nil {
		t.Fatal(err)
	}

	if _, err := st.Commit(ctx, store.Plan{
		NonLendable: []string{nodeID},
		Evictions:   []store.Eviction{{LeaseID: "lease-a", Reason: "reclaim", Requeue: true}},
	}); err != nil {
		t.Fatal(err)
	}

	node, err := st.GetNode(ctx, nodeID)
	if err != nil {
		t.Fatal(err)
	}
	if node.Lendable {
		t.Fatal("reclaimed machine is still open to borrowers; the reclaim can livelock")
	}
}

func TestExpireLeasesRequeuesBatchWork(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)

	nodeID := registerNode(t, st, "ws-01", 2, 48)
	job, tasks := createJob(t, st, "default", 1)
	slots := slotIDs(t, st, nodeID, 2)

	// A TTL already in the past: the same state a workstation that was
	// unplugged mid-task leaves behind.
	if _, err := st.Commit(ctx, store.Plan{Placements: []store.Placement{{
		LeaseID: "lease-a", TaskID: tasks[0].TaskID, JobID: job.JobID,
		PoolID: "default", NodeID: nodeID, SlotIDs: slots[0:1],
		Mode: domain.ModeBatch, TTL: time.Millisecond,
	}}}); err != nil {
		t.Fatal(err)
	}

	expired, err := st.ExpireLeases(ctx, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(expired) != 1 {
		t.Fatalf("expected the lapsed lease to expire, got %d", len(expired))
	}

	_, after, err := st.GetJob(ctx, job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if after[0].State != domain.TaskQueued {
		t.Fatalf("expired batch task should requeue, got %s", after[0].State)
	}
	// An expiry is not the task's fault, so it must not count as a preemption
	// against it when reclaim later picks a victim.
	if after[0].Preemptions != 0 {
		t.Fatalf("expiry should not count as a preemption, got %d", after[0].Preemptions)
	}
}

func TestRenewLeasesRejectsUnknownLease(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)

	nodeID := registerNode(t, st, "ws-01", 2, 48)
	job, tasks := createJob(t, st, "default", 1)
	slots := slotIDs(t, st, nodeID, 2)

	if _, err := st.Commit(ctx, store.Plan{Placements: []store.Placement{{
		LeaseID: "lease-a", TaskID: tasks[0].TaskID, JobID: job.JobID,
		PoolID: "default", NodeID: nodeID, SlotIDs: slots[0:1],
		Mode: domain.ModeBatch, TTL: time.Minute,
	}}}); err != nil {
		t.Fatal(err)
	}

	renewed, rejected, err := st.RenewLeases(ctx, nodeID,
		[]string{"lease-a", "lease-ghost"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(renewed) != 1 || renewed[0] != "lease-a" {
		t.Fatalf("expected lease-a renewed, got %v", renewed)
	}
	if len(rejected) != 1 || rejected[0] != "lease-ghost" {
		t.Fatalf("expected lease-ghost rejected so the agent stops it, got %v", rejected)
	}
}

func TestHeartbeatMarksDegradedSlotUnhealthy(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	nodeID := registerNode(t, st, "ws-01", 2, 48)

	err := st.Heartbeat(ctx, store.HeartbeatUpdate{
		NodeID: nodeID, At: time.Now(),
		Samples: []domain.HealthSample{
			{DeviceIndex: 0, XIDErrors: []int{79}, TemperatureC: 60},
			{DeviceIndex: 1, TemperatureC: 55},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	node, err := st.GetNode(ctx, nodeID)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range node.Slots {
		want := domain.SlotFree
		if s.DeviceIndex == 0 {
			want = domain.SlotUnhealthy
		}
		if s.State != want {
			t.Fatalf("device %d: state %s, want %s", s.DeviceIndex, s.State, want)
		}
	}

	// And it recovers once the device stops reporting errors.
	if err := st.Heartbeat(ctx, store.HeartbeatUpdate{
		NodeID: nodeID, At: time.Now(),
		Samples: []domain.HealthSample{{DeviceIndex: 0, TemperatureC: 58}},
	}); err != nil {
		t.Fatal(err)
	}
	node, _ = st.GetNode(ctx, nodeID)
	for _, s := range node.Slots {
		if s.DeviceIndex == 0 && s.State != domain.SlotFree {
			t.Fatalf("recovered device should return to free, got %s", s.State)
		}
	}
}

func TestSnapshotReportsQueueAndAllocation(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)

	nodeID := registerNode(t, st, "ws-01", 2, 48)
	job, tasks := createJob(t, st, "default", 3)
	slots := slotIDs(t, st, nodeID, 2)

	if _, err := st.Commit(ctx, store.Plan{Placements: []store.Placement{{
		LeaseID: "lease-a", TaskID: tasks[0].TaskID, JobID: job.JobID,
		PoolID: "default", NodeID: nodeID, SlotIDs: slots[0:1],
		Mode: domain.ModeBatch, TTL: time.Minute,
		HostShareCores: 16, HostShareRAMGB: 128,
	}}}); err != nil {
		t.Fatal(err)
	}

	snap, err := st.Snapshot(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Queued) != 2 {
		t.Fatalf("expected 2 tasks still queued, got %d", len(snap.Queued))
	}
	if len(snap.Leases) != 1 {
		t.Fatalf("expected 1 active lease, got %d", len(snap.Leases))
	}

	n := snap.NodeByID(nodeID)
	if n == nil {
		t.Fatal("node missing from snapshot")
	}
	if n.AllocatedCores != 16 || n.AllocatedRAMGB != 128 {
		t.Fatalf("host share not reflected: %.0f cores, %d GB",
			n.AllocatedCores, n.AllocatedRAMGB)
	}
	if n.QueueDepth != 1 {
		t.Fatalf("expected queue depth 1, got %d", n.QueueDepth)
	}
}

func TestIssueJoinTokenUnknownPool(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)

	// The pool was never bootstrapped (no UpsertPool call). Asking for a
	// token against it must fail cleanly with ErrNotFound, not leak a raw
	// foreign-key violation from the join_tokens insert.
	_, err := st.IssueJoinToken(ctx, "cad")
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for unknown pool, got %v", err)
	}
}

func TestJoinTokenIsSingleUse(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)

	if err := st.UpsertPool(ctx, domain.Pool{
		PoolID: "cad", Name: "cad", Lendable: true,
	}); err != nil {
		t.Fatal(err)
	}
	token, err := st.IssueJoinToken(ctx, "cad")
	if err != nil {
		t.Fatal(err)
	}

	doc := domain.CapabilityDoc{
		Host:     domain.Host{Cores: 8, RAMGB: 32},
		Software: domain.Software{Driver: "580.95", CUDA: "13.0"},
		Devices:  []domain.Device{{Index: 0, VRAMGB: 24, ComputeCapability: "8.9"}},
	}

	nodeID, _, err := st.RegisterNode(ctx, store.NodeRegistration{
		Hostname: "ws-a", Capabilities: doc, JoinToken: token,
	})
	if err != nil {
		t.Fatalf("first join should succeed: %v", err)
	}
	node, _ := st.GetNode(ctx, nodeID)
	if node.PoolID != "cad" {
		t.Fatalf("token should place the node in cad, got %s", node.PoolID)
	}

	// A leaked token must not enrol a second machine.
	if _, _, err := st.RegisterNode(ctx, store.NodeRegistration{
		Hostname: "ws-b", Capabilities: doc, JoinToken: token,
	}); err == nil {
		t.Fatal("a spent join token must be refused")
	}
}

func TestCancelJobStopsQueuedWork(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)

	nodeID := registerNode(t, st, "ws-01", 2, 48)
	job, tasks := createJob(t, st, "default", 3)
	slots := slotIDs(t, st, nodeID, 2)

	if _, err := st.Commit(ctx, store.Plan{Placements: []store.Placement{{
		LeaseID: "lease-a", TaskID: tasks[0].TaskID, JobID: job.JobID,
		PoolID: "default", NodeID: nodeID, SlotIDs: slots[0:1],
		Mode: domain.ModeBatch, TTL: time.Minute,
	}}}); err != nil {
		t.Fatal(err)
	}

	leases, err := st.CancelJob(ctx, job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if len(leases) != 1 || leases[0] != "lease-a" {
		t.Fatalf("cancel should report the leases to revoke, got %v", leases)
	}

	_, after, err := st.GetJob(ctx, job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	for _, tk := range after {
		if tk.State != domain.TaskFailed {
			t.Fatalf("cancelled job left task %d in %s", tk.Index, tk.State)
		}
	}

	// The snapshot must not offer cancelled work back to the scheduler.
	snap, err := st.Snapshot(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Queued) != 0 {
		t.Fatalf("cancelled job still has %d queued tasks", len(snap.Queued))
	}
}

func TestPolicyRoundTripsAndBumpsGeneration(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)

	before, err := st.GetPolicy(ctx)
	if err != nil {
		t.Fatal(err)
	}

	after, err := st.SetPolicy(ctx, "latency", map[string]float64{"fit": 0.75})
	if err != nil {
		t.Fatal(err)
	}
	if after.Preset != "latency" || after.Weights["fit"] != 0.75 {
		t.Fatalf("policy did not round-trip: %+v", after)
	}
	// The running scheduler watches the generation, which is what makes weight
	// changes take effect without a restart.
	if after.ReloadGeneration <= before.ReloadGeneration {
		t.Fatalf("reload generation did not advance: %d -> %d",
			before.ReloadGeneration, after.ReloadGeneration)
	}
}

func TestMarkStaleNodesOffline(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	nodeID := registerNode(t, st, "ws-01", 2, 48)

	stale, err := st.MarkStaleNodesOffline(ctx, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(stale) != 1 || stale[0] != nodeID {
		t.Fatalf("expected the silent node to be marked offline, got %v", stale)
	}

	node, _ := st.GetNode(ctx, nodeID)
	if node.State != domain.NodeOffline {
		t.Fatalf("node state %s, want offline", node.State)
	}
}
