package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/alexk/orch/internal/domain"
	"github.com/alexk/orch/internal/events"
	"github.com/alexk/orch/internal/store"
)

// Dispatcher delivers decisions to the agents holding the affected nodes.
//
// The scheduler does not know how agents are connected; it hands off and moves
// on. A delivery failure is not fatal -- the lease TTL lapses and the task
// requeues, which is the same path a disconnected node already takes.
type Dispatcher interface {
	Assign(nodeID string, p store.Placement, task domain.Task, job domain.Job) error
	Preempt(nodeID, leaseID, reason string, grace time.Duration) error
}

// Config tunes the placement loop.
type Config struct {
	// Interval between passes.
	Interval time.Duration
	// LeaseTTL is short on purpose: a workstation that vanishes should free its
	// slots in seconds, not minutes.
	LeaseTTL time.Duration
	// HeartbeatTimeout after which a node is presumed gone.
	HeartbeatTimeout time.Duration
	// QueueLimit caps how many queued tasks one pass considers.
	QueueLimit int
}

// DefaultConfig returns settings suited to a 3-20 machine fleet.
func DefaultConfig() Config {
	return Config{
		Interval:         500 * time.Millisecond,
		LeaseTTL:         30 * time.Second,
		HeartbeatTimeout: 15 * time.Second,
		QueueLimit:       256,
	}
}

// Scheduler is the single-threaded placement loop.
//
// Placement decisions are inherently serialized against shared state: two
// concurrent scorers will select the same idle slot. Concurrency belongs in the
// API layer and the agents, not here. One leader, one loop, transactional
// commit -- which at this fleet size is over-provisioned by orders of
// magnitude.
type Scheduler struct {
	store store.Store
	bus   *events.Bus
	log   *slog.Logger
	cfg   Config

	dispatcher Dispatcher

	// Pending machine reclaims, set by the trigger console and drained by the
	// next pass.
	reclaims chan reclaimRequest
}

type reclaimRequest struct {
	NodeID string
	Reason string
	Done   chan []string
}

// New creates a scheduler.
func New(st store.Store, bus *events.Bus, log *slog.Logger, cfg Config) *Scheduler {
	return &Scheduler{
		store:    st,
		bus:      bus,
		log:      log,
		cfg:      cfg,
		reclaims: make(chan reclaimRequest, 32),
	}
}

// SetDispatcher wires the agent hub in. Done after construction because the hub
// needs the scheduler too.
func (s *Scheduler) SetDispatcher(d Dispatcher) { s.dispatcher = d }

// RequestMachineReclaim asks for a machine to be emptied of borrowers and
// returns the leases that were evicted.
//
// This is the "CAD user logs in" path: the owner wants an intact box back.
func (s *Scheduler) RequestMachineReclaim(ctx context.Context, nodeID, reason string) ([]string, error) {
	done := make(chan []string, 1)
	select {
	case s.reclaims <- reclaimRequest{NodeID: nodeID, Reason: reason, Done: done}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case ids := <-done:
		return ids, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Run drives the loop until the context is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()

	s.log.Info("scheduler started",
		"interval", s.cfg.Interval, "lease_ttl", s.cfg.LeaseTTL)

	for {
		select {
		case <-ctx.Done():
			s.log.Info("scheduler stopped")
			return
		case <-ticker.C:
			if err := s.pass(ctx); err != nil {
				s.log.Error("placement pass failed", "err", err)
			}
		}
	}
}

// pass is one iteration: reconcile, snapshot, decide, commit, dispatch.
func (s *Scheduler) pass(ctx context.Context) error {
	now := time.Now()

	if err := s.reconcile(ctx, now); err != nil {
		return err
	}

	snap, err := s.store.Snapshot(ctx, s.cfg.QueueLimit)
	if err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}

	pol, err := s.store.GetPolicy(ctx)
	if err != nil {
		return fmt.Errorf("policy: %w", err)
	}
	weights := ResolveWeights(pol.Preset, pol.Weights)

	fs := newFleetState(snap)

	// Operator-driven machine reclaims run before placement, so the freed slots
	// are visible to this same pass and the owner's work lands immediately.
	plan, waiters := s.drainReclaims(fs)

	decisions := s.place(fs, snap, weights, &plan)

	if plan.Empty() {
		s.notifyReclaimWaiters(waiters, nil)
		return nil
	}

	res, err := s.store.Commit(ctx, plan)
	if err != nil {
		s.notifyReclaimWaiters(waiters, nil)
		return fmt.Errorf("commit: %w", err)
	}

	s.notifyReclaimWaiters(waiters, res.Evicted)
	s.dispatch(ctx, snap, res, decisions)
	return nil
}

// reconcile handles the lifecycle events that happen without anyone asking:
// leases lapsing, and workstations disappearing.
func (s *Scheduler) reconcile(ctx context.Context, now time.Time) error {
	expired, err := s.store.ExpireLeases(ctx, now)
	if err != nil {
		return fmt.Errorf("expire leases: %w", err)
	}
	for _, l := range expired {
		s.bus.Emit(events.KindLeaseEnded,
			fmt.Sprintf("lease %s expired on %s, task requeued", short(l.LeaseID), l.NodeID),
			map[string]any{"lease_id": l.LeaseID, "node_id": l.NodeID, "reason": "ttl"})
	}

	offline, err := s.store.MarkStaleNodesOffline(ctx, now.Add(-s.cfg.HeartbeatTimeout))
	if err != nil {
		return fmt.Errorf("mark stale nodes: %w", err)
	}
	for _, nodeID := range offline {
		// No assumption that a node returns with prior state, so everything it
		// was running goes back on the queue.
		released, err := s.store.ReleaseLeasesOnNode(ctx, nodeID, "node offline", true)
		if err != nil {
			return fmt.Errorf("release leases on %s: %w", nodeID, err)
		}
		s.bus.Emit(events.KindNodeChanged,
			fmt.Sprintf("%s went offline, %d lease(s) requeued", nodeID, len(released)),
			map[string]any{"node_id": nodeID, "state": string(domain.NodeOffline)})
	}
	return nil
}

func (s *Scheduler) drainReclaims(fs *fleetState) (store.Plan, []reclaimRequest) {
	var (
		plan    store.Plan
		waiters []reclaimRequest
		seen    = map[string]bool{}
	)

	for {
		select {
		case r := <-s.reclaims:
			waiters = append(waiters, r)
			if seen[r.NodeID] {
				continue
			}
			seen[r.NodeID] = true

			sub := ReclaimMachine(fs, r.NodeID, r.Reason)
			plan.NonLendable = append(plan.NonLendable, sub.NonLendable...)
			plan.Evictions = append(plan.Evictions, sub.Evictions...)

			// Reflect the eviction in the working state so this pass can place
			// the owner's queued work onto the slots it just freed.
			for _, ev := range sub.Evictions {
				fs.release(ev.LeaseID)
			}
			if ns := fs.nodes[r.NodeID]; ns != nil {
				ns.lendable = false
			}
		default:
			return plan, waiters
		}
	}
}

func (s *Scheduler) notifyReclaimWaiters(waiters []reclaimRequest, evicted []store.Eviction) {
	ids := make([]string, 0, len(evicted))
	for _, e := range evicted {
		ids = append(ids, e.LeaseID)
	}
	for _, w := range waiters {
		select {
		case w.Done <- ids:
		default:
		}
	}
}

// decision records what the scheduler concluded about one task.
type decision struct {
	Task       domain.Task
	Job        domain.Job
	Chosen     *Candidate
	Candidates []Candidate
	Rejected   []rejection
	LeaseID    string
}

// place walks the queue in order and decides where each task goes.
func (s *Scheduler) place(
	fs *fleetState,
	snap *store.Snapshot,
	weights map[string]float64,
	plan *store.Plan,
) []decision {
	info := buildFleetInfo(fs, snap.Now)
	preemptions := map[string]int{}
	var out []decision

	for _, qt := range snap.Queued {
		job, task := qt.Job, qt.Task
		req := job.Request

		cands, rejected := filter(fs, job, req)

		// Nothing fits. If this pool owns machines occupied by borrowers, it is
		// entitled to take them back -- borrowing without reclaim is just a
		// slower way to violate capacity guarantees.
		if len(cands) == 0 {
			evictions, why := reclaimForPool(fs, job.PoolID, req, preemptions, snap.Now)
			if len(evictions) == 0 {
				out = append(out, decision{Task: task, Job: job, Rejected: rejected})
				if why != "" {
					s.log.Debug("task unplaceable", "task", task.TaskID, "why", why)
				}
				continue
			}

			for _, ev := range evictions {
				if l := fs.leases[ev.LeaseID]; l != nil {
					preemptions[l.TaskID]++
				}
				fs.release(ev.LeaseID)
				plan.Evictions = append(plan.Evictions, ev)
			}
			info = buildFleetInfo(fs, snap.Now)
			cands, rejected = filter(fs, job, req)
			if len(cands) == 0 {
				out = append(out, decision{Task: task, Job: job, Rejected: rejected})
				continue
			}
		}

		for i := range cands {
			score(&cands[i], ScoreInput{
				Job:       job,
				Request:   req,
				Candidate: &cands[i],
				Fleet:     info,
			}, weights)
		}

		best := pick(cands)
		if best == nil {
			out = append(out, decision{Task: task, Job: job, Candidates: cands, Rejected: rejected})
			continue
		}

		cores, ram := perSlotShare(best.Node)
		nSlots := len(best.Slots)
		leaseID := "lease-" + uuid.NewString()

		fs.take(best.Node.NodeID, best.SlotIDs(), job.PoolID, best.Borrowed, req.ExclusiveHost)
		info = buildFleetInfo(fs, snap.Now)

		mode := job.Mode
		if mode == "" {
			mode = domain.ModeBatch
		}

		plan.Placements = append(plan.Placements, store.Placement{
			LeaseID: leaseID,
			TaskID:  task.TaskID,
			JobID:   job.JobID,
			PoolID:  job.PoolID,
			NodeID:  best.Node.NodeID,
			SlotIDs: best.SlotIDs(),
			Mode:    mode,
			// A borrowed lease is preemptible whatever the job asked for. That
			// is the deal that makes lending safe for the owner.
			Borrowed:       best.Borrowed,
			ExclusiveHost:  req.ExclusiveHost,
			HostShareCores: cores * float64(nSlots),
			HostShareRAMGB: ram * nSlots,
			TTL:            s.cfg.LeaseTTL,
			Attempt:        task.Attempt,
		})

		out = append(out, decision{
			Task: task, Job: job, Chosen: best,
			Candidates: cands, Rejected: rejected, LeaseID: leaseID,
		})
	}

	return out
}

// pick selects the winning candidate.
//
// The fragmentation rule is applied here rather than as a score: candidates
// that would lend the last free slot on an otherwise-whole machine are set
// aside, and only consulted if nothing else is available.
func pick(cands []Candidate) *Candidate {
	var best, fallback *Candidate

	for i := range cands {
		c := &cands[i]
		if c.LendsLastSlot {
			if fallback == nil || c.Total > fallback.Total {
				fallback = c
			}
			continue
		}
		if best == nil || c.Total > best.Total {
			best = c
		}
	}

	if best != nil {
		return best
	}
	return fallback
}

func buildFleetInfo(fs *fleetState, now time.Time) *FleetInfo {
	info := &FleetInfo{BorrowersOnNode: map[string]int{}, Now: now}
	for _, l := range fs.leases {
		if l.Borrowed {
			info.BorrowersOnNode[l.NodeID]++
		}
	}
	for _, ns := range fs.nodes {
		if ns.queueDepth > info.MaxQueueDepth {
			info.MaxQueueDepth = ns.queueDepth
		}
	}
	return info
}

// dispatch tells the agents what changed and records why.
func (s *Scheduler) dispatch(ctx context.Context, snap *store.Snapshot, res store.CommitResult, decisions []decision) {
	placed := make(map[string]store.Placement, len(res.Placed))
	for _, p := range res.Placed {
		placed[p.TaskID] = p
	}

	for _, ev := range res.Evicted {
		nodeID := ""
		for _, l := range snap.Leases {
			if l.LeaseID == ev.LeaseID {
				nodeID = l.NodeID
				break
			}
		}
		s.bus.Emit(events.KindLeaseEnded,
			fmt.Sprintf("preempted %s on %s: %s", short(ev.LeaseID), nodeID, ev.Reason),
			map[string]any{"lease_id": ev.LeaseID, "node_id": nodeID, "reason": ev.Reason})

		if s.dispatcher != nil && nodeID != "" {
			if err := s.dispatcher.Preempt(nodeID, ev.LeaseID, ev.Reason, ev.Grace); err != nil {
				s.log.Warn("preempt dispatch failed", "lease", ev.LeaseID, "err", err)
			}
		}
	}

	for _, d := range decisions {
		p, ok := placed[d.Task.TaskID]
		if !ok {
			if reason, rejected := res.Rejected[d.LeaseID]; rejected {
				s.log.Debug("placement rejected at commit",
					"task", d.Task.TaskID, "reason", reason)
			}
			continue
		}

		s.saveExplanation(ctx, d, p)

		s.bus.Emit(events.KindLeaseGranted,
			fmt.Sprintf("placed task %s on %s%s", short(d.Task.TaskID), p.NodeID, borrowedSuffix(p.Borrowed)),
			map[string]any{
				"lease_id": p.LeaseID, "task_id": p.TaskID, "job_id": p.JobID,
				"node_id": p.NodeID, "slot_ids": p.SlotIDs, "borrowed": p.Borrowed,
			})

		if s.dispatcher != nil {
			if err := s.dispatcher.Assign(p.NodeID, p, d.Task, d.Job); err != nil {
				s.log.Warn("assignment dispatch failed", "task", p.TaskID, "err", err)
			}
		}
	}
}

// saveExplanation records the scoring breakdown so a placement can be shown
// rather than asserted.
func (s *Scheduler) saveExplanation(ctx context.Context, d decision, p store.Placement) {
	type candidateJSON struct {
		NodeID      string             `json:"node_id"`
		Total       float64            `json:"total"`
		PerScorer   map[string]float64 `json:"per_scorer"`
		WouldBorrow bool               `json:"would_borrow"`
		Chosen      bool               `json:"chosen"`
	}
	type explanationJSON struct {
		TaskID        string          `json:"task_id"`
		ChosenNodeID  string          `json:"chosen_node_id"`
		ChosenSlotIDs []string        `json:"chosen_slot_ids"`
		Candidates    []candidateJSON `json:"candidates"`
		FilteredOut   []string        `json:"filtered_out"`
		At            time.Time       `json:"at"`
	}

	exp := explanationJSON{
		TaskID:        d.Task.TaskID,
		ChosenNodeID:  p.NodeID,
		ChosenSlotIDs: p.SlotIDs,
		At:            time.Now(),
	}
	for i := range d.Candidates {
		c := &d.Candidates[i]
		exp.Candidates = append(exp.Candidates, candidateJSON{
			NodeID:      c.Node.NodeID,
			Total:       c.Total,
			PerScorer:   c.Scores,
			WouldBorrow: c.Borrowed,
			Chosen:      d.Chosen != nil && c.Node.NodeID == d.Chosen.Node.NodeID,
		})
	}
	for _, r := range d.Rejected {
		exp.FilteredOut = append(exp.FilteredOut, r.NodeID+": "+r.Reason)
	}

	raw, err := json.Marshal(exp)
	if err != nil {
		return
	}
	if err := s.store.SaveExplanation(ctx, d.Task.TaskID, raw); err != nil {
		s.log.Debug("save explanation failed", "task", d.Task.TaskID, "err", err)
	}
}

func borrowedSuffix(b bool) string {
	if b {
		return " (borrowed)"
	}
	return ""
}

func short(id string) string {
	if len(id) > 14 {
		return id[:14]
	}
	return id
}
