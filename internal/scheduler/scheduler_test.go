package scheduler

import (
	"testing"
	"time"

	"github.com/alexk/orch/internal/domain"
	"github.com/alexk/orch/internal/store"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

type nodeOpt func(*store.NodeView)

func withLabels(l map[string]string) nodeOpt {
	return func(n *store.NodeView) { n.Labels = l }
}

func withSoftware(driver, cuda string) nodeOpt {
	return func(n *store.NodeView) {
		n.Capabilities.Software = domain.Software{Driver: driver, CUDA: cuda}
	}
}

func withHealth(idx int, temp float64, throttle ...string) nodeOpt {
	return func(n *store.NodeView) {
		for i := range n.Slots {
			if n.Slots[i].DeviceIndex == idx {
				n.Slots[i].Health.TemperatureC = temp
				n.Slots[i].Health.ThrottleReasons = throttle
			}
		}
	}
}

func withCache(digests ...string) nodeOpt {
	return func(n *store.NodeView) { n.CachedDigests = digests }
}

func withReliability(ok, bad float64) nodeOpt {
	return func(n *store.NodeView) { n.RecentSuccesses, n.RecentFailures = ok, bad }
}

// mkNode builds a ready node whose slots all have the given VRAM.
func mkNode(id, pool string, vramGB []int, cc string, opts ...nodeOpt) *store.NodeView {
	n := &store.NodeView{
		Node: domain.Node{
			NodeID:   id,
			Hostname: id,
			PoolID:   pool,
			State:    domain.NodeReady,
			Lendable: true,
			// Generous host so tests exercise GPU predicates, not CPU limits,
			// unless a test deliberately shrinks it.
			HostCores: 64,
			HostRAMGB: 512,
			Capabilities: domain.CapabilityDoc{
				Software: domain.Software{Driver: "580.95", CUDA: "13.0"},
			},
		},
	}
	for i, v := range vramGB {
		n.Slots = append(n.Slots, domain.Slot{
			SlotID:            slotID(id, i),
			NodeID:            id,
			Kind:              domain.KindGPU,
			DeviceIndex:       i,
			VRAMGB:            v,
			ComputeCapability: cc,
			State:             domain.SlotFree,
		})
	}
	for _, o := range opts {
		o(n)
	}
	return n
}

func slotID(nodeID string, idx int) string {
	return nodeID + "-d" + string(rune('0'+idx))
}

func mkSnapshot(nodes []*store.NodeView, pools []domain.Pool, leases []domain.Lease) *store.Snapshot {
	return &store.Snapshot{Nodes: nodes, Pools: pools, Leases: leases, Now: time.Now()}
}

func mkJob(pool string, req domain.ResourceRequest) domain.Job {
	return domain.Job{
		JobID: "job-1", PoolID: pool, Mode: domain.ModeBatch,
		ImageDigest: "sha256:demo", Request: req,
	}
}

// occupy marks slots allocated by a lease, as the store would.
func occupy(n *store.NodeView, leaseID string, idxs ...int) domain.Lease {
	var slotIDs []string
	for _, i := range idxs {
		for j := range n.Slots {
			if n.Slots[j].DeviceIndex == i {
				n.Slots[j].State = domain.SlotAllocated
				n.Slots[j].LeaseID = leaseID
				slotIDs = append(slotIDs, n.Slots[j].SlotID)
			}
		}
	}
	return domain.Lease{
		LeaseID: leaseID, NodeID: n.NodeID, SlotIDs: slotIDs,
		Mode: domain.ModeBatch, State: domain.LeaseActive, GrantedAt: time.Now(),
	}
}

func candidateFor(t *testing.T, fs *fleetState, job domain.Job) []Candidate {
	t.Helper()
	cands, _ := filter(fs, job, job.Request)
	return cands
}

func nodeIDs(cands []Candidate) []string {
	out := make([]string, len(cands))
	for i, c := range cands {
		out[i] = c.Node.NodeID
	}
	return out
}

// ---------------------------------------------------------------------------
// Phase 1: capability predicates
// ---------------------------------------------------------------------------

func TestFilterExcludesInsufficientVRAM(t *testing.T) {
	fs := newFleetState(mkSnapshot(
		[]*store.NodeView{
			mkNode("small", "default", []int{8, 8}, "8.9"),
			mkNode("big", "default", []int{48, 48}, "8.9"),
		},
		[]domain.Pool{{PoolID: "default", Lendable: true}},
		nil,
	))

	job := mkJob("default", domain.ResourceRequest{Slots: 1, MinVRAMGB: 24})
	cands := candidateFor(t, fs, job)

	if len(cands) != 1 || cands[0].Node.NodeID != "big" {
		t.Fatalf("expected only 'big' to survive the VRAM floor, got %v", nodeIDs(cands))
	}
}

// The scheduler must compare compute capability numerically. Lexically "10.0"
// sorts below "8.9", which would silently exclude the newest hardware in the
// fleet from every job that states a floor.
func TestFilterComparesComputeCapabilityNumerically(t *testing.T) {
	fs := newFleetState(mkSnapshot(
		[]*store.NodeView{
			mkNode("blackwell", "default", []int{96}, "10.0"),
			mkNode("ada", "default", []int{24}, "8.9"),
			mkNode("turing", "default", []int{16}, "7.5"),
		},
		[]domain.Pool{{PoolID: "default", Lendable: true}},
		nil,
	))

	job := mkJob("default", domain.ResourceRequest{Slots: 1, MinComputeCapability: "8.9"})
	got := nodeIDs(candidateFor(t, fs, job))

	if len(got) != 2 {
		t.Fatalf("expected blackwell and ada to pass cc>=8.9, got %v", got)
	}
	for _, id := range got {
		if id == "turing" {
			t.Fatalf("turing (cc 7.5) should not satisfy cc>=8.9, got %v", got)
		}
	}
}

func TestFilterEnforcesDriverAndLabelPredicates(t *testing.T) {
	fs := newFleetState(mkSnapshot(
		[]*store.NodeView{
			mkNode("old", "default", []int{48}, "8.9", withSoftware("470.10", "11.4")),
			mkNode("labA", "default", []int{48}, "8.9", withLabels(map[string]string{"location": "lab-a"})),
			mkNode("labB", "default", []int{48}, "8.9", withLabels(map[string]string{"location": "lab-b"})),
		},
		[]domain.Pool{{PoolID: "default", Lendable: true}},
		nil,
	))

	job := mkJob("default", domain.ResourceRequest{
		Slots:          1,
		MinDriver:      "525.0",
		RequiredLabels: map[string]string{"location": "lab-a"},
	})
	got := nodeIDs(candidateFor(t, fs, job))

	if len(got) != 1 || got[0] != "labA" {
		t.Fatalf("expected only labA, got %v", got)
	}
}

func TestFilterExcludesDegradedSlots(t *testing.T) {
	bad := mkNode("bad", "default", []int{48}, "8.9")
	bad.Slots[0].Health.XIDErrors = []int{79}

	fs := newFleetState(mkSnapshot(
		[]*store.NodeView{bad, mkNode("good", "default", []int{48}, "8.9")},
		[]domain.Pool{{PoolID: "default", Lendable: true}},
		nil,
	))

	got := nodeIDs(candidateFor(t, fs, mkJob("default", domain.ResourceRequest{Slots: 1})))
	if len(got) != 1 || got[0] != "good" {
		t.Fatalf("a device reporting an XID error must not take new work, got %v", got)
	}
}

// A thermally capped card is still usable. Excluding it would be wrong: the
// distinction is soft, and belongs to the thermal scorer.
func TestFilterKeepsThrottledSlots(t *testing.T) {
	hot := mkNode("hot", "default", []int{48}, "8.9", withHealth(0, 88, "sw_thermal_slowdown"))
	fs := newFleetState(mkSnapshot(
		[]*store.NodeView{hot},
		[]domain.Pool{{PoolID: "default", Lendable: true}},
		nil,
	))

	if got := candidateFor(t, fs, mkJob("default", domain.ResourceRequest{Slots: 1})); len(got) != 1 {
		t.Fatalf("throttled card should remain a candidate, got %d candidates", len(got))
	}
}

// ---------------------------------------------------------------------------
// Gang scheduling
// ---------------------------------------------------------------------------

func TestGangRequestFitsWithinOneNode(t *testing.T) {
	fs := newFleetState(mkSnapshot(
		[]*store.NodeView{
			mkNode("one-gpu-a", "default", []int{48}, "8.9"),
			mkNode("one-gpu-b", "default", []int{48}, "8.9"),
			mkNode("four-gpu", "default", []int{48, 48, 48, 48}, "8.9"),
		},
		[]domain.Pool{{PoolID: "default", Lendable: true}},
		nil,
	))

	job := mkJob("default", domain.ResourceRequest{Slots: 4})
	cands := candidateFor(t, fs, job)

	// Two single-GPU machines must not be combined to satisfy one 4-slot
	// request: a multi-GPU request fits within one node by definition here.
	if len(cands) != 1 || cands[0].Node.NodeID != "four-gpu" {
		t.Fatalf("expected only four-gpu to satisfy a 4-slot gang, got %v", nodeIDs(cands))
	}
	if len(cands[0].Slots) != 4 {
		t.Fatalf("expected 4 slots reserved together, got %d", len(cands[0].Slots))
	}
}

func TestGangRejectsPartiallyOccupiedNode(t *testing.T) {
	n := mkNode("four-gpu", "default", []int{48, 48, 48, 48}, "8.9")
	lease := occupy(n, "lease-x", 0, 1, 2)

	fs := newFleetState(mkSnapshot(
		[]*store.NodeView{n},
		[]domain.Pool{{PoolID: "default", Lendable: true}},
		[]domain.Lease{lease},
	))

	if got := candidateFor(t, fs, mkJob("default", domain.ResourceRequest{Slots: 2})); len(got) != 0 {
		t.Fatalf("only one slot is free, a 2-slot gang must not be offered: %v", nodeIDs(got))
	}
}

// ---------------------------------------------------------------------------
// Phase 2: scoring
// ---------------------------------------------------------------------------

func TestFitPrefersSmallestAdequateSlot(t *testing.T) {
	fs := newFleetState(mkSnapshot(
		[]*store.NodeView{
			mkNode("huge", "default", []int{96}, "8.9"),
			mkNode("right", "default", []int{24}, "8.9"),
		},
		[]domain.Pool{{PoolID: "default", Lendable: true}},
		nil,
	))

	job := mkJob("default", domain.ResourceRequest{Slots: 1, MinVRAMGB: 20})
	cands := candidateFor(t, fs, job)
	info := buildFleetInfo(fs, time.Now())
	for i := range cands {
		score(&cands[i], ScoreInput{Job: job, Request: job.Request, Candidate: &cands[i], Fleet: info},
			map[string]float64{"fit": 1})
	}

	best := pick(cands)
	if best == nil || best.Node.NodeID != "right" {
		t.Fatalf("fit should avoid burning a 96GB card on a 20GB request, chose %v", best.Node.NodeID)
	}
}

// A slower idle GPU beats a faster busy one.
func TestQueueDepthPrefersIdleNode(t *testing.T) {
	busy := mkNode("busy", "default", []int{96}, "9.0")
	lease := occupy(busy, "lease-busy", 0)
	busy.Slots = append(busy.Slots, domain.Slot{
		SlotID: "busy-d1", NodeID: "busy", DeviceIndex: 1, VRAMGB: 96,
		ComputeCapability: "9.0", State: domain.SlotFree,
	})
	idle := mkNode("idle", "default", []int{24}, "8.6")

	fs := newFleetState(mkSnapshot(
		[]*store.NodeView{busy, idle},
		[]domain.Pool{{PoolID: "default", Lendable: true}},
		[]domain.Lease{lease},
	))

	job := mkJob("default", domain.ResourceRequest{Slots: 1})
	cands := candidateFor(t, fs, job)
	info := buildFleetInfo(fs, time.Now())
	for i := range cands {
		score(&cands[i], ScoreInput{Job: job, Request: job.Request, Candidate: &cands[i], Fleet: info},
			map[string]float64{"queue_depth": 1})
	}

	if best := pick(cands); best == nil || best.Node.NodeID != "idle" {
		t.Fatalf("expected the idle node to win on queue_depth, got %v", best)
	}
}

func TestThermalPenalisesThrottledCard(t *testing.T) {
	cool := mkNode("cool", "default", []int{48}, "8.9", withHealth(0, 45))
	hot := mkNode("hot", "default", []int{48}, "8.9", withHealth(0, 89, "hw_thermal_slowdown"))

	fs := newFleetState(mkSnapshot(
		[]*store.NodeView{cool, hot},
		[]domain.Pool{{PoolID: "default", Lendable: true}},
		nil,
	))

	job := mkJob("default", domain.ResourceRequest{Slots: 1})
	cands := candidateFor(t, fs, job)
	info := buildFleetInfo(fs, time.Now())
	for i := range cands {
		score(&cands[i], ScoreInput{Job: job, Request: job.Request, Candidate: &cands[i], Fleet: info},
			map[string]float64{"thermal": 1})
	}

	if best := pick(cands); best == nil || best.Node.NodeID != "cool" {
		t.Fatalf("thermal should steer away from a throttled card, chose %v", best)
	}
}

func TestCacheResidencyPrefersWarmNode(t *testing.T) {
	warm := mkNode("warm", "default", []int{48}, "8.9", withCache("sha256:scene"))
	cold := mkNode("cold", "default", []int{48}, "8.9")

	fs := newFleetState(mkSnapshot(
		[]*store.NodeView{warm, cold},
		[]domain.Pool{{PoolID: "default", Lendable: true}},
		nil,
	))

	job := mkJob("default", domain.ResourceRequest{Slots: 1})
	job.Assets = []domain.AssetRef{{Digest: "sha256:scene", SizeBytes: 1 << 30}}

	cands := candidateFor(t, fs, job)
	info := buildFleetInfo(fs, time.Now())
	for i := range cands {
		score(&cands[i], ScoreInput{Job: job, Request: job.Request, Candidate: &cands[i], Fleet: info},
			map[string]float64{"cache_residency": 1})
	}

	if best := pick(cands); best == nil || best.Node.NodeID != "warm" {
		t.Fatalf("expected the node already holding the asset to win, got %v", best)
	}
}

func TestReliabilityTreatsUnknownNodeAsTrusted(t *testing.T) {
	fresh := mkNode("fresh", "default", []int{48}, "8.9")
	flaky := mkNode("flaky", "default", []int{48}, "8.9", withReliability(2, 8))

	fs := newFleetState(mkSnapshot(
		[]*store.NodeView{fresh, flaky},
		[]domain.Pool{{PoolID: "default", Lendable: true}},
		nil,
	))

	job := mkJob("default", domain.ResourceRequest{Slots: 1})
	cands := candidateFor(t, fs, job)
	info := buildFleetInfo(fs, time.Now())
	for i := range cands {
		score(&cands[i], ScoreInput{Job: job, Request: job.Request, Candidate: &cands[i], Fleet: info},
			map[string]float64{"reliability": 1})
	}

	if best := pick(cands); best == nil || best.Node.NodeID != "fresh" {
		t.Fatalf("a node with no history should not rank below a flaky one, got %v", best)
	}
}

// ---------------------------------------------------------------------------
// Pools and borrowing
// ---------------------------------------------------------------------------

func TestPoolWithoutCeilingCannotBorrow(t *testing.T) {
	fs := newFleetState(mkSnapshot(
		[]*store.NodeView{mkNode("cad-01", "cad", []int{48}, "8.9")},
		[]domain.Pool{
			{PoolID: "cad", Lendable: true},
			{PoolID: "render", Lendable: true, BorrowCeiling: 0},
		},
		nil,
	))

	if got := candidateFor(t, fs, mkJob("render", domain.ResourceRequest{Slots: 1})); len(got) != 0 {
		t.Fatalf("a pool with a zero borrow ceiling must not borrow, got %v", nodeIDs(got))
	}
}

func TestBorrowCeilingCapsBorrowing(t *testing.T) {
	cad := mkNode("cad-01", "cad", []int{48, 48, 48, 48}, "8.9")
	// Two slots already borrowed by render.
	lease := occupy(cad, "lease-borrowed", 0, 1)
	lease.PoolID, lease.Borrowed = "render", true

	fs := newFleetState(mkSnapshot(
		[]*store.NodeView{cad},
		[]domain.Pool{
			{PoolID: "cad", Lendable: true},
			{PoolID: "render", Lendable: true, BorrowCeiling: 2},
		},
		[]domain.Lease{lease},
	))

	if got := candidateFor(t, fs, mkJob("render", domain.ResourceRequest{Slots: 1})); len(got) != 0 {
		t.Fatalf("render is at its ceiling of 2, must not borrow a third: %v", nodeIDs(got))
	}
}

func TestNonLendablePoolIsNotBorrowedFrom(t *testing.T) {
	fs := newFleetState(mkSnapshot(
		[]*store.NodeView{mkNode("cad-01", "cad", []int{48}, "8.9")},
		[]domain.Pool{
			{PoolID: "cad", Lendable: false},
			{PoolID: "render", Lendable: true, BorrowCeiling: 8},
		},
		nil,
	))

	if got := candidateFor(t, fs, mkJob("render", domain.ResourceRequest{Slots: 1})); len(got) != 0 {
		t.Fatalf("a pool that does not lend must not be borrowed from, got %v", nodeIDs(got))
	}
}

// A node closed to borrowers during a machine reclaim must not accept new
// borrowed work, or the reclaim never finishes.
func TestNodeClosedToBorrowersDuringReclaim(t *testing.T) {
	cad := mkNode("cad-01", "cad", []int{48, 48}, "8.9")
	cad.Lendable = false

	fs := newFleetState(mkSnapshot(
		[]*store.NodeView{cad},
		[]domain.Pool{
			{PoolID: "cad", Lendable: true},
			{PoolID: "render", Lendable: true, BorrowCeiling: 8},
		},
		nil,
	))

	if got := candidateFor(t, fs, mkJob("render", domain.ResourceRequest{Slots: 1})); len(got) != 0 {
		t.Fatalf("node under reclaim must be closed to borrowers, got %v", nodeIDs(got))
	}
	// The owner is unaffected.
	if got := candidateFor(t, fs, mkJob("cad", domain.ResourceRequest{Slots: 1})); len(got) != 1 {
		t.Fatalf("owner must still be able to place on its own node, got %v", nodeIDs(got))
	}
}

// ---------------------------------------------------------------------------
// Fragmentation
// ---------------------------------------------------------------------------

// Do not lend the last free slot on a machine that could still host a full-box
// job -- unless nothing else is available.
func TestBorrowAvoidsBreakingUpAWholeMachine(t *testing.T) {
	whole := mkNode("cad-whole", "cad", []int{48, 48, 48, 48}, "8.9")
	partial := mkNode("cad-partial", "cad", []int{48, 48, 48, 48}, "8.9")
	lease := occupy(partial, "lease-native", 0)
	lease.PoolID = "cad"

	fs := newFleetState(mkSnapshot(
		[]*store.NodeView{whole, partial},
		[]domain.Pool{
			{PoolID: "cad", Lendable: true},
			{PoolID: "render", Lendable: true, BorrowCeiling: 8},
		},
		[]domain.Lease{lease},
	))

	job := mkJob("render", domain.ResourceRequest{Slots: 1})
	cands := candidateFor(t, fs, job)
	info := buildFleetInfo(fs, time.Now())
	for i := range cands {
		score(&cands[i], ScoreInput{Job: job, Request: job.Request, Candidate: &cands[i], Fleet: info},
			map[string]float64{"fit": 1})
	}

	best := pick(cands)
	if best == nil || best.Node.NodeID != "cad-partial" {
		t.Fatalf("borrower should land on the already-broken machine, not carve up an intact one; chose %v", best)
	}
}

func TestBorrowTakesWholeMachineWhenNothingElseAvailable(t *testing.T) {
	whole := mkNode("cad-whole", "cad", []int{48, 48}, "8.9")

	fs := newFleetState(mkSnapshot(
		[]*store.NodeView{whole},
		[]domain.Pool{
			{PoolID: "cad", Lendable: true},
			{PoolID: "render", Lendable: true, BorrowCeiling: 8},
		},
		nil,
	))

	job := mkJob("render", domain.ResourceRequest{Slots: 1})
	cands := candidateFor(t, fs, job)
	if len(cands) != 1 || !cands[0].LendsLastSlot {
		t.Fatalf("expected the only candidate to be flagged as fragmenting, got %+v", cands)
	}
	if best := pick(cands); best == nil {
		t.Fatal("the rule is a preference, not a prohibition: it must still place when nothing else exists")
	}
}

// Borrowers pack onto the fewest lender machines, so reclaim empties one host
// rather than several.
func TestLenderConcentrationPacksBorrowers(t *testing.T) {
	occupied := mkNode("cad-01", "cad", []int{48, 48, 48, 48}, "8.9")
	borrowed := occupy(occupied, "lease-b1", 0)
	borrowed.PoolID, borrowed.Borrowed = "render", true

	empty := mkNode("cad-02", "cad", []int{48, 48, 48, 48}, "8.9")
	// Give cad-02 a native lease too, so neither node trips the
	// "don't fragment an intact machine" rule and concentration decides alone.
	native := occupy(empty, "lease-n1", 0)
	native.PoolID = "cad"

	fs := newFleetState(mkSnapshot(
		[]*store.NodeView{occupied, empty},
		[]domain.Pool{
			{PoolID: "cad", Lendable: true},
			{PoolID: "render", Lendable: true, BorrowCeiling: 8},
		},
		[]domain.Lease{borrowed, native},
	))

	job := mkJob("render", domain.ResourceRequest{Slots: 1})
	cands := candidateFor(t, fs, job)
	info := buildFleetInfo(fs, time.Now())
	for i := range cands {
		score(&cands[i], ScoreInput{Job: job, Request: job.Request, Candidate: &cands[i], Fleet: info},
			map[string]float64{"lender_concentration": 1})
	}

	if best := pick(cands); best == nil || best.Node.NodeID != "cad-01" {
		t.Fatalf("expected borrowers to concentrate on cad-01, chose %v", best)
	}
}

// ---------------------------------------------------------------------------
// Preemption and reclaim
// ---------------------------------------------------------------------------

func TestOwnerReclaimsBorrowedSlot(t *testing.T) {
	cad := mkNode("cad-01", "cad", []int{48, 48}, "8.9")
	borrowed := occupy(cad, "lease-borrowed", 0, 1)
	borrowed.PoolID, borrowed.Borrowed, borrowed.TaskID = "render", true, "task-render"

	fs := newFleetState(mkSnapshot(
		[]*store.NodeView{cad},
		[]domain.Pool{
			{PoolID: "cad", Lendable: true},
			{PoolID: "render", Lendable: true, BorrowCeiling: 8},
		},
		[]domain.Lease{borrowed},
	))

	req := domain.ResourceRequest{Slots: 1}
	// Nothing is free, so ordinary placement must find nothing.
	if got := candidateFor(t, fs, mkJob("cad", req)); len(got) != 0 {
		t.Fatalf("no slot is free, expected no candidates, got %v", nodeIDs(got))
	}

	evictions, why := reclaimForPool(fs, "cad", req, map[string]int{}, time.Now())
	if len(evictions) == 0 {
		t.Fatalf("owner should be able to reclaim its own machine: %s", why)
	}
	if evictions[0].LeaseID != "lease-borrowed" {
		t.Fatalf("expected the borrowed lease to be evicted, got %s", evictions[0].LeaseID)
	}
	if !evictions[0].Requeue {
		t.Fatal("a preempted batch task must be requeued, not dropped")
	}
}

// Borrowed leases are always evicted before native ones. A native lease in the
// owning pool is never a reclaim target.
func TestReclaimNeverEvictsNativeLeases(t *testing.T) {
	cad := mkNode("cad-01", "cad", []int{48, 48}, "8.9")
	native := occupy(cad, "lease-native", 0, 1)
	native.PoolID = "cad"

	fs := newFleetState(mkSnapshot(
		[]*store.NodeView{cad},
		[]domain.Pool{{PoolID: "cad", Lendable: true}},
		[]domain.Lease{native},
	))

	evictions, why := reclaimForPool(fs, "cad", domain.ResourceRequest{Slots: 1}, map[string]int{}, time.Now())
	if len(evictions) != 0 {
		t.Fatalf("native leases must never be preempted by their own pool, got %v (%s)", evictions, why)
	}
}

// Evict the borrower that loses the least work.
func TestReclaimEvictsCheapestBorrower(t *testing.T) {
	cad := mkNode("cad-01", "cad", []int{48, 48}, "8.9")

	old := occupy(cad, "lease-old", 0)
	old.PoolID, old.Borrowed, old.TaskID = "render", true, "task-old"
	old.GrantedAt = time.Now().Add(-10 * time.Minute)

	fresh := occupy(cad, "lease-fresh", 1)
	fresh.PoolID, fresh.Borrowed, fresh.TaskID = "render", true, "task-fresh"
	fresh.GrantedAt = time.Now().Add(-5 * time.Second)

	fs := newFleetState(mkSnapshot(
		[]*store.NodeView{cad},
		[]domain.Pool{
			{PoolID: "cad", Lendable: true},
			{PoolID: "render", Lendable: true, BorrowCeiling: 8},
		},
		[]domain.Lease{old, fresh},
	))

	evictions, _ := reclaimForPool(fs, "cad", domain.ResourceRequest{Slots: 1}, map[string]int{}, time.Now())
	if len(evictions) != 1 {
		t.Fatalf("expected exactly one eviction to free one slot, got %d", len(evictions))
	}
	if evictions[0].LeaseID != "lease-fresh" {
		t.Fatalf("expected the newest borrower (least work lost) to be evicted, got %s", evictions[0].LeaseID)
	}
}

// A task that keeps getting preempted must stop being the cheapest answer, or
// it starves while its peers run to completion.
func TestReclaimAvoidsRepeatedlyPreemptedTask(t *testing.T) {
	cad := mkNode("cad-01", "cad", []int{48, 48}, "8.9")

	unlucky := occupy(cad, "lease-unlucky", 0)
	unlucky.PoolID, unlucky.Borrowed, unlucky.TaskID = "render", true, "task-unlucky"
	unlucky.GrantedAt = time.Now().Add(-5 * time.Second)

	settled := occupy(cad, "lease-settled", 1)
	settled.PoolID, settled.Borrowed, settled.TaskID = "render", true, "task-settled"
	settled.GrantedAt = time.Now().Add(-90 * time.Second)

	fs := newFleetState(mkSnapshot(
		[]*store.NodeView{cad},
		[]domain.Pool{
			{PoolID: "cad", Lendable: true},
			{PoolID: "render", Lendable: true, BorrowCeiling: 8},
		},
		[]domain.Lease{unlucky, settled},
	))

	// Without history the fresh lease is cheapest; with three prior preemptions
	// it should no longer be.
	evictions, _ := reclaimForPool(fs, "cad",
		domain.ResourceRequest{Slots: 1}, map[string]int{"task-unlucky": 3}, time.Now())

	if len(evictions) != 1 {
		t.Fatalf("expected one eviction, got %d", len(evictions))
	}
	if evictions[0].LeaseID != "lease-settled" {
		t.Fatalf("expected the repeatedly-preempted task to be spared, evicted %s", evictions[0].LeaseID)
	}
}

// Machine reclaim closes the host to borrowers in the same plan that evicts
// them. Evicting first would let the loop refill the slots behind it.
func TestMachineReclaimMarksNodeNonLendableBeforeEvicting(t *testing.T) {
	cad := mkNode("cad-01", "cad", []int{48, 48}, "8.9")
	b1 := occupy(cad, "lease-b1", 0)
	b1.PoolID, b1.Borrowed = "render", true
	b2 := occupy(cad, "lease-b2", 1)
	b2.PoolID, b2.Borrowed = "render", true

	fs := newFleetState(mkSnapshot(
		[]*store.NodeView{cad},
		[]domain.Pool{
			{PoolID: "cad", Lendable: true},
			{PoolID: "render", Lendable: true, BorrowCeiling: 8},
		},
		[]domain.Lease{b1, b2},
	))

	plan := ReclaimMachine(fs, "cad-01", "CAD user logged in")

	if len(plan.NonLendable) != 1 || plan.NonLendable[0] != "cad-01" {
		t.Fatalf("reclaim must close the host to borrowers, got %v", plan.NonLendable)
	}
	if len(plan.Evictions) != 2 {
		t.Fatalf("expected both borrowers evicted, got %d", len(plan.Evictions))
	}
	for _, e := range plan.Evictions {
		if !e.Requeue {
			t.Fatalf("preempted batch work must requeue: %+v", e)
		}
	}
}

func TestMachineReclaimLeavesNativeWorkAlone(t *testing.T) {
	cad := mkNode("cad-01", "cad", []int{48, 48}, "8.9")
	native := occupy(cad, "lease-native", 0)
	native.PoolID = "cad"
	borrowed := occupy(cad, "lease-borrowed", 1)
	borrowed.PoolID, borrowed.Borrowed = "render", true

	fs := newFleetState(mkSnapshot(
		[]*store.NodeView{cad},
		[]domain.Pool{{PoolID: "cad", Lendable: true}, {PoolID: "render", Lendable: true, BorrowCeiling: 8}},
		[]domain.Lease{native, borrowed},
	))

	plan := ReclaimMachine(fs, "cad-01", "CAD user logged in")
	if len(plan.Evictions) != 1 || plan.Evictions[0].LeaseID != "lease-borrowed" {
		t.Fatalf("only borrowers should be evicted by a machine reclaim, got %v", plan.Evictions)
	}
}

// ---------------------------------------------------------------------------
// Host contention
// ---------------------------------------------------------------------------

// The GPU is partitioned; the host is not. A slot carries its proportional
// share of cores and RAM, and a node whose host is committed cannot take more
// work even though a GPU looks idle.
func TestHostShareLimitsPlacement(t *testing.T) {
	n := mkNode("thin-host", "default", []int{48, 48, 48, 48}, "8.9")
	n.HostCores, n.HostRAMGB = 8, 32
	// Three of four slots' worth of host already committed.
	n.AllocatedCores, n.AllocatedRAMGB = 6, 24
	occupy(n, "lease-x", 0, 1, 2)

	lease := domain.Lease{
		LeaseID: "lease-x", NodeID: n.NodeID, PoolID: "default",
		SlotIDs: []string{n.Slots[0].SlotID, n.Slots[1].SlotID, n.Slots[2].SlotID},
		State:   domain.LeaseActive, Mode: domain.ModeBatch,
		HostShareCores: 6, HostShareRAMGB: 24, GrantedAt: time.Now(),
	}

	fs := newFleetState(mkSnapshot(
		[]*store.NodeView{n},
		[]domain.Pool{{PoolID: "default", Lendable: true}},
		[]domain.Lease{lease},
	))

	// One slot free and exactly one slot's host share left: this must fit.
	if got := candidateFor(t, fs, mkJob("default", domain.ResourceRequest{Slots: 1})); len(got) != 1 {
		t.Fatalf("the last slot's host share is available, expected a candidate, got %v", nodeIDs(got))
	}

	// Now over-commit the host and the same free GPU must become unusable.
	fs.nodes["thin-host"].allocCores = 8
	fs.nodes["thin-host"].allocRAMGB = 32
	if got := candidateFor(t, fs, mkJob("default", domain.ResourceRequest{Slots: 1})); len(got) != 0 {
		t.Fatalf("host is fully committed, the free GPU must not be offered: %v", nodeIDs(got))
	}
}

func TestExclusiveHostRejectsOccupiedNode(t *testing.T) {
	n := mkNode("shared", "default", []int{48, 48}, "8.9")
	lease := occupy(n, "lease-x", 0)
	lease.PoolID = "default"

	fs := newFleetState(mkSnapshot(
		[]*store.NodeView{n},
		[]domain.Pool{{PoolID: "default", Lendable: true}},
		[]domain.Lease{lease},
	))

	job := mkJob("default", domain.ResourceRequest{Slots: 1, ExclusiveHost: true})
	if got := candidateFor(t, fs, job); len(got) != 0 {
		t.Fatalf("exclusive_host must not land on an occupied machine, got %v", nodeIDs(got))
	}
}

func TestExclusiveHostBlocksOtherWork(t *testing.T) {
	n := mkNode("solo", "default", []int{48, 48}, "8.9")
	lease := occupy(n, "lease-excl", 0)
	lease.PoolID, lease.ExclusiveHost = "default", true

	fs := newFleetState(mkSnapshot(
		[]*store.NodeView{n},
		[]domain.Pool{{PoolID: "default", Lendable: true}},
		[]domain.Lease{lease},
	))

	if got := candidateFor(t, fs, mkJob("default", domain.ResourceRequest{Slots: 1})); len(got) != 0 {
		t.Fatalf("a machine held exclusively must not accept co-tenants, got %v", nodeIDs(got))
	}
}

// ---------------------------------------------------------------------------
// Version comparison
// ---------------------------------------------------------------------------

func TestCompareVersions(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"10.0", "8.9", 1}, // the case a lexical compare gets wrong
		{"8.9", "10.0", -1},
		{"8.9", "8.9", 0},
		{"13", "13.0", 0},
		{"580.95", "580.9", 1},
		{"535-server", "535", 0},
		{"12.4", "12.10", -1},
	}
	for _, c := range cases {
		if got := domain.CompareVersions(c.a, c.b); got != c.want {
			t.Errorf("CompareVersions(%q, %q) = %d, want %d", c.a, c.b, got, c.want)
		}
	}
}

func TestVersionAtLeastTreatsUnreportedAsFailing(t *testing.T) {
	if domain.VersionAtLeast("", "525.0") {
		t.Error("a node that cannot report its driver must not be assumed to meet a floor")
	}
	if !domain.VersionAtLeast("", "") {
		t.Error("an empty floor is no constraint")
	}
}
