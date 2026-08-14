package scheduler

import (
	"fmt"
	"sort"

	"github.com/alexk/orch/internal/domain"
	"github.com/alexk/orch/internal/store"
)

// Candidate is one viable way to satisfy a request: a specific set of slots on
// a specific machine.
type Candidate struct {
	Node  *store.NodeView
	node  *nodeState
	Slots []domain.Slot

	// Borrowed is set when the node belongs to a pool other than the
	// requesting one. Borrowed leases run preemptible regardless of the job's
	// own preemptible flag.
	Borrowed bool

	// LendsLastSlot marks a borrow that would take the last free slot on a
	// machine that could otherwise still host a whole-box job. Allowed, but
	// only when nothing else will do.
	LendsLastSlot bool

	Scores map[string]float64
	Total  float64
}

// SlotIDs returns the chosen slot IDs.
func (c *Candidate) SlotIDs() []string {
	out := make([]string, len(c.Slots))
	for i, s := range c.Slots {
		out[i] = s.SlotID
	}
	return out
}

// rejection records why a node could not host a request, so the decision can be
// explained rather than merely asserted.
type rejection struct {
	NodeID string
	Reason string
}

// filter is Phase 1: the hard, boolean question of whether the request fits.
//
// Every predicate here reads the capability document. There is no list of GPU
// models in this file, or anywhere the scheduler can reach: a fleet with
// entirely different hardware works with zero code change.
func filter(fs *fleetState, job domain.Job, req domain.ResourceRequest) ([]Candidate, []rejection) {
	var (
		cands    []Candidate
		rejected []rejection
	)

	reject := func(nodeID, format string, args ...any) {
		rejected = append(rejected, rejection{NodeID: nodeID, Reason: fmt.Sprintf(format, args...)})
	}

	slots := req.Slots
	if slots < 1 {
		slots = 1
	}

	for _, ns := range fs.nodes {
		n := ns.view

		if !n.Healthy() {
			reject(n.NodeID, "node is %s", n.State)
			continue
		}

		// An exclusive-host lease owns the whole machine, and an exclusive
		// request needs a machine nobody else is on.
		if ns.exclusive {
			reject(n.NodeID, "host held exclusively")
			continue
		}
		if req.ExclusiveHost && len(fs.leasesByNode[n.NodeID]) > 0 {
			reject(n.NodeID, "exclusive host requested but node is occupied")
			continue
		}

		if k, ok := missingLabel(n.Labels, req.RequiredLabels); !ok {
			reject(n.NodeID, "label %q does not match", k)
			continue
		}

		sw := n.Capabilities.Software
		if !domain.VersionAtLeast(sw.Driver, req.MinDriver) {
			reject(n.NodeID, "driver %s < required %s", orNone(sw.Driver), req.MinDriver)
			continue
		}
		if !domain.VersionAtLeast(sw.CUDA, req.MinCUDA) {
			reject(n.NodeID, "cuda %s < required %s", orNone(sw.CUDA), req.MinCUDA)
			continue
		}

		borrowed, why := poolEligible(fs, job.PoolID, ns, slots)
		if why != "" {
			reject(n.NodeID, "%s", why)
			continue
		}

		if !ns.hostShareAvailable(slots) {
			reject(n.NodeID, "host share exhausted (%.1f/%d cores, %d/%d GB)",
				ns.allocCores, n.HostCores, ns.allocRAMGB, n.HostRAMGB)
			continue
		}

		fits := adequateSlots(ns, req)
		if len(fits) < slots {
			reject(n.NodeID, "%d slots meet the requirements, need %d", len(fits), slots)
			continue
		}

		// Prefer the smallest adequate slots. Burning a 96GB card on a request
		// that asked for 8GB is the waste the fit scorer exists to avoid, and
		// choosing the gang this way avoids creating it in the first place.
		sort.Slice(fits, func(i, j int) bool {
			if fits[i].VRAMGB != fits[j].VRAMGB {
				return fits[i].VRAMGB < fits[j].VRAMGB
			}
			return fits[i].SlotID < fits[j].SlotID
		})
		chosen := fits[:slots]

		cands = append(cands, Candidate{
			Node:          n,
			node:          ns,
			Slots:         chosen,
			Borrowed:      borrowed,
			LendsLastSlot: borrowed && lendsLastUsefulSlot(ns, slots),
			Scores:        map[string]float64{},
		})
	}

	sort.Slice(cands, func(i, j int) bool { return cands[i].Node.NodeID < cands[j].Node.NodeID })
	sort.Slice(rejected, func(i, j int) bool { return rejected[i].NodeID < rejected[j].NodeID })
	return cands, rejected
}

// adequateSlots returns the free slots on a node that satisfy every capability
// predicate in the request.
func adequateSlots(ns *nodeState, req domain.ResourceRequest) []domain.Slot {
	out := make([]domain.Slot, 0, len(ns.free))
	for _, s := range ns.free {
		if s.VRAMGB < req.MinVRAMGB {
			continue
		}
		if !domain.VersionAtLeast(s.ComputeCapability, req.MinComputeCapability) {
			continue
		}
		if s.EncodeSessions < req.MinEncodeSessions || s.DecodeSessions < req.MinDecodeSessions {
			continue
		}
		// ECC is present on professional cards and absent on consumer ones, so
		// a request may require it but nothing may assume it.
		if req.RequireECC && !s.ECCPresent {
			continue
		}
		// Hardware trouble is disqualifying; being merely hot is not, and is
		// left to the thermal scorer to weigh.
		if s.Health.Degraded() {
			continue
		}
		out = append(out, s)
	}
	return out
}

// poolEligible decides whether the requesting pool may use this machine, and
// whether doing so counts as borrowing.
//
// The model YARN, Borg and Kueue converged on: a pool always gets its own
// machines, idle capacity is lendable, and a ceiling caps how much any pool may
// take from others.
func poolEligible(fs *fleetState, poolID string, ns *nodeState, want int) (borrowed bool, reject string) {
	if ns.view.PoolID == poolID {
		return false, ""
	}

	owner := fs.pools[ns.view.PoolID]
	if owner != nil && !owner.Lendable {
		return false, "pool " + ns.view.PoolID + " does not lend"
	}
	// Cleared while a machine reclaim drains this host. Without it the reclaim
	// livelocks against the placements it is trying to make room for.
	if !ns.lendable {
		return false, "node is closed to borrowers"
	}

	borrower := fs.pools[poolID]
	ceiling := 0
	if borrower != nil {
		ceiling = borrower.BorrowCeiling
	}
	if ceiling == 0 {
		return false, "pool " + poolID + " has no borrow ceiling"
	}
	if fs.borrowedByPool[poolID]+want > ceiling {
		return false, fmt.Sprintf("borrow ceiling reached (%d/%d)", fs.borrowedByPool[poolID], ceiling)
	}

	// The owning pool keeps whatever it guaranteed itself. Lending is only
	// permitted out of capacity above that line.
	if owner != nil && owner.GuaranteedSlots > 0 {
		ownerFree := 0
		for _, other := range fs.nodes {
			if other.view.PoolID == ns.view.PoolID {
				ownerFree += len(other.free)
			}
		}
		if ownerFree-want < 0 {
			return false, "would breach owner's guarantee"
		}
	}

	return true, ""
}

// lendsLastUsefulSlot implements the one fragmentation rule worth having at
// this fleet size: do not lend the last free slot on a machine that could still
// host a whole-box job.
//
// It reports a violation rather than forbidding one. Selection prefers
// candidates without it and falls back to them only when nothing else is
// available.
func lendsLastUsefulSlot(ns *nodeState, want int) bool {
	total := len(ns.view.Slots)
	if total < 2 {
		// A single-slot machine can never host a multi-slot job, so lending it
		// fragments nothing.
		return false
	}
	// The machine is currently whole and the borrow would break that up.
	return len(ns.free) == total && want < total
}

func missingLabel(have, want map[string]string) (string, bool) {
	for k, v := range want {
		if have[k] != v {
			return k, false
		}
	}
	return "", true
}

func orNone(s string) string {
	if s == "" {
		return "(unreported)"
	}
	return s
}
