package scheduler

import (
	"sort"
	"time"

	"github.com/alexk/orch/internal/domain"
	"github.com/alexk/orch/internal/store"
)

// effectiveGuarantee is how many slots a pool may claim by preempting
// borrowers.
//
// An unset guarantee means "the machines I own", not "nothing". A pool that
// declared no quota still owns its hardware, and the zero-config default -- one
// implicit pool, no quotas -- has to keep working.
func effectiveGuarantee(fs *fleetState, poolID string) int {
	owned := fs.ownedSlots(poolID)
	if p := fs.pools[poolID]; p != nil && p.GuaranteedSlots > 0 && p.GuaranteedSlots < owned {
		return p.GuaranteedSlots
	}
	return owned
}

// entitledToReclaim reports whether a pool is below the capacity it can always
// claim, and so may evict borrowers to get there.
func entitledToReclaim(fs *fleetState, poolID string, want int) bool {
	inUse := fs.nativeByPool[poolID]
	return inUse+want <= effectiveGuarantee(fs, poolID)
}

// evictionCost orders borrowers by how much evicting them would cost.
//
// Elapsed runtime is the work that would be thrown away. Prior preemptions are
// added on top so a task that keeps getting chosen stops being the cheapest
// answer -- otherwise the same unlucky task is evicted forever while its peers
// run to completion.
func evictionCost(l *domain.Lease, now time.Time, preemptions int) float64 {
	started := l.GrantedAt
	elapsed := now.Sub(started).Seconds()
	if elapsed < 0 {
		elapsed = 0
	}

	cost := elapsed + float64(preemptions)*120

	// Service leases have no graceful drain in v1, so killing one is strictly
	// worse than killing a batch task that will simply requeue.
	if l.Mode == domain.ModeService {
		cost += 3600
	}
	return cost
}

// borrowerOnNode is a candidate eviction.
type borrowerOnNode struct {
	lease *domain.Lease
	cost  float64
}

// reclaimForPool finds the cheapest set of borrowers to evict so that a pool
// can place a request of `want` slots on machines it owns.
//
// Only borrowed leases are considered. That is what makes preemption ordering
// trivially correct: borrowed leases are always evicted before native ones in
// the owning pool, so there is never a question of which of two equal claims
// gives way.
func reclaimForPool(
	fs *fleetState,
	poolID string,
	req domain.ResourceRequest,
	preemptionsByTask map[string]int,
	now time.Time,
) ([]store.Eviction, string) {
	want := req.Slots
	if want < 1 {
		want = 1
	}
	if !entitledToReclaim(fs, poolID, want) {
		return nil, "pool is already at its guaranteed capacity"
	}

	// Group borrowers by the machine they sit on. Reclaim is per-host because
	// the slots a request needs must end up on one machine.
	byNode := map[string][]borrowerOnNode{}
	for _, l := range fs.leases {
		if !l.Borrowed || !fs.poolOwns(poolID, l.NodeID) {
			continue
		}
		ns := fs.nodes[l.NodeID]
		if ns == nil || !ns.view.Healthy() {
			continue
		}
		byNode[l.NodeID] = append(byNode[l.NodeID], borrowerOnNode{
			lease: l,
			cost:  evictionCost(l, now, preemptionsByTask[l.TaskID]),
		})
	}
	if len(byNode) == 0 {
		return nil, "no borrowers to reclaim on this pool's machines"
	}

	type plan struct {
		nodeID    string
		evictions []store.Eviction
		cost      float64
	}
	var best *plan

	for nodeID, borrowers := range byNode {
		ns := fs.nodes[nodeID]
		alreadyFree := 0
		for _, s := range ns.free {
			if slotSatisfies(s, req) {
				alreadyFree++
			}
		}
		if alreadyFree >= want {
			// Nothing to reclaim; the ordinary placement path will take these.
			continue
		}

		// Cheapest first, so the smallest amount of work is discarded.
		sort.Slice(borrowers, func(i, j int) bool {
			if borrowers[i].cost != borrowers[j].cost {
				return borrowers[i].cost < borrowers[j].cost
			}
			return borrowers[i].lease.LeaseID < borrowers[j].lease.LeaseID
		})

		freed := alreadyFree
		var (
			evictions []store.Eviction
			total     float64
		)
		for _, b := range borrowers {
			if freed >= want {
				break
			}
			// Only count slots that would actually satisfy the request.
			gained := 0
			for _, slotID := range b.lease.SlotIDs {
				for _, s := range ns.view.Slots {
					if s.SlotID == slotID && slotSatisfies(s, req) {
						gained++
					}
				}
			}
			if gained == 0 {
				continue
			}
			freed += gained
			total += b.cost
			evictions = append(evictions, store.Eviction{
				LeaseID: b.lease.LeaseID,
				Reason:  "slot reclaimed by owning pool " + poolID,
				Requeue: true,
				Grace:   5 * time.Second,
			})
		}

		if freed < want || len(evictions) == 0 {
			continue
		}
		if best == nil || total < best.cost {
			best = &plan{nodeID: nodeID, evictions: evictions, cost: total}
		}
	}

	if best == nil {
		return nil, "no single machine could be reclaimed to fit the request"
	}
	return best.evictions, ""
}

// slotSatisfies is the capability predicate, reused by reclaim so it does not
// evict borrowers off a card the waiting request could not have used anyway.
func slotSatisfies(s domain.Slot, req domain.ResourceRequest) bool {
	if s.VRAMGB < req.MinVRAMGB {
		return false
	}
	if !domain.VersionAtLeast(s.ComputeCapability, req.MinComputeCapability) {
		return false
	}
	if s.EncodeSessions < req.MinEncodeSessions || s.DecodeSessions < req.MinDecodeSessions {
		return false
	}
	if req.RequireECC && !s.ECCPresent {
		return false
	}
	return !s.Health.Degraded()
}

// ReclaimMachine empties a machine of borrowers so its owner gets an intact box.
//
// The node is marked non-lendable in the same plan, and Commit applies that
// before any eviction runs. Evicting first and closing the door afterwards
// livelocks: the loop refills slots behind the eviction as fast as they are
// freed.
func ReclaimMachine(fs *fleetState, nodeID, reason string) store.Plan {
	plan := store.Plan{NonLendable: []string{nodeID}}

	for _, l := range fs.leasesByNode[nodeID] {
		if !l.Borrowed {
			continue
		}
		plan.Evictions = append(plan.Evictions, store.Eviction{
			LeaseID: l.LeaseID,
			Reason:  reason,
			Requeue: true,
			Grace:   5 * time.Second,
		})
	}

	sort.Slice(plan.Evictions, func(i, j int) bool {
		return plan.Evictions[i].LeaseID < plan.Evictions[j].LeaseID
	})
	return plan
}
