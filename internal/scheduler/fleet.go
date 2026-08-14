package scheduler

import (
	"github.com/alexk/orch/internal/domain"
	"github.com/alexk/orch/internal/store"
)

// fleetState is the scheduler's working copy of the snapshot.
//
// A single pass places many tasks. Without in-pass bookkeeping every task in
// the batch would be scored against the same idle slots and they would all be
// sent to the same machine, only for all but one to be rejected at commit. This
// tracks tentative allocations so later tasks in the same pass see the fleet as
// the earlier ones left it.
type fleetState struct {
	nodes map[string]*nodeState
	// Slots each pool has taken from machines owned by another pool.
	borrowedByPool map[string]int
	// Slots each pool is using on its own machines.
	nativeByPool map[string]int
	pools        map[string]*domain.Pool
	leases       map[string]*domain.Lease
	// Active leases per node, including ones evicted or granted this pass.
	leasesByNode map[string][]*domain.Lease
}

type nodeState struct {
	view *store.NodeView

	// Free slot IDs, minus anything tentatively taken this pass.
	free map[string]domain.Slot

	allocCores float64
	allocRAMGB int

	// Cleared when a machine reclaim closes this host to borrowers.
	lendable bool

	// Set when an exclusive-host lease holds the whole machine.
	exclusive bool

	queueDepth int
}

func newFleetState(snap *store.Snapshot) *fleetState {
	fs := &fleetState{
		nodes:          make(map[string]*nodeState, len(snap.Nodes)),
		borrowedByPool: map[string]int{},
		nativeByPool:   map[string]int{},
		pools:          make(map[string]*domain.Pool, len(snap.Pools)),
		leases:         make(map[string]*domain.Lease, len(snap.Leases)),
		leasesByNode:   map[string][]*domain.Lease{},
	}

	for i := range snap.Pools {
		p := snap.Pools[i]
		fs.pools[p.PoolID] = &p
	}

	for _, n := range snap.Nodes {
		// queueDepth is derived from the lease list below rather than taken
		// from the node row. take() and release() adjust it as the pass
		// proceeds, so it has to start from the same source they mutate --
		// otherwise a denormalised count drifts against the leases it counts.
		ns := &nodeState{
			view:       n,
			free:       map[string]domain.Slot{},
			allocCores: n.AllocatedCores,
			allocRAMGB: n.AllocatedRAMGB,
			lendable:   n.Lendable,
		}
		for _, s := range n.Slots {
			if s.State == domain.SlotFree {
				ns.free[s.SlotID] = s
			}
		}
		fs.nodes[n.NodeID] = ns
	}

	for i := range snap.Leases {
		l := snap.Leases[i]
		fs.leases[l.LeaseID] = &l
		fs.leasesByNode[l.NodeID] = append(fs.leasesByNode[l.NodeID], &l)

		if ns := fs.nodes[l.NodeID]; ns != nil {
			ns.queueDepth++
		}

		if l.Borrowed {
			fs.borrowedByPool[l.PoolID] += len(l.SlotIDs)
		} else {
			fs.nativeByPool[l.PoolID] += len(l.SlotIDs)
		}
		if l.ExclusiveHost {
			if ns := fs.nodes[l.NodeID]; ns != nil {
				ns.exclusive = true
			}
		}
	}

	return fs
}

// perSlotShare derives the host resources one slot carries.
//
// Machine-owned / slot-lent means a borrowed workload runs on someone else's
// machine, sharing CPU, RAM, scratch and PCIe. The GPU is partitioned; the host
// is not. A slot's proportional share of the host -- and nothing more -- is
// what gets enforced through cgroups.
func perSlotShare(n *store.NodeView) (cores float64, ramGB int) {
	total := len(n.Slots)
	if total == 0 {
		return 0, 0
	}
	return float64(n.HostCores) / float64(total), n.HostRAMGB / total
}

// hostShareAvailable reports whether the node can still back `slots` more slots
// with host resources.
func (ns *nodeState) hostShareAvailable(slots int) bool {
	cores, ram := perSlotShare(ns.view)
	needCores := cores * float64(slots)
	needRAM := ram * slots
	// A small tolerance keeps floating-point division from rejecting the last
	// slot on a node whose cores do not divide evenly.
	const eps = 0.001
	return ns.allocCores+needCores <= float64(ns.view.HostCores)+eps &&
		ns.allocRAMGB+needRAM <= ns.view.HostRAMGB
}

// take tentatively allocates slots for the rest of this pass.
func (fs *fleetState) take(nodeID string, slotIDs []string, poolID string, borrowed, exclusive bool) {
	ns := fs.nodes[nodeID]
	if ns == nil {
		return
	}
	cores, ram := perSlotShare(ns.view)
	for _, id := range slotIDs {
		delete(ns.free, id)
	}
	ns.allocCores += cores * float64(len(slotIDs))
	ns.allocRAMGB += ram * len(slotIDs)
	ns.queueDepth++
	if exclusive {
		ns.exclusive = true
	}
	if borrowed {
		fs.borrowedByPool[poolID] += len(slotIDs)
	} else {
		fs.nativeByPool[poolID] += len(slotIDs)
	}
}

// release returns slots evicted this pass, so a reclaim and the placement that
// motivated it can be decided together.
func (fs *fleetState) release(leaseID string) {
	l := fs.leases[leaseID]
	if l == nil {
		return
	}
	ns := fs.nodes[l.NodeID]
	if ns == nil {
		return
	}

	for _, slotID := range l.SlotIDs {
		for _, s := range ns.view.Slots {
			if s.SlotID == slotID && s.State != domain.SlotUnhealthy {
				s.State = domain.SlotFree
				s.LeaseID = ""
				ns.free[slotID] = s
			}
		}
	}

	cores, ram := perSlotShare(ns.view)
	ns.allocCores -= cores * float64(len(l.SlotIDs))
	ns.allocRAMGB -= ram * len(l.SlotIDs)
	if ns.allocCores < 0 {
		ns.allocCores = 0
	}
	if ns.allocRAMGB < 0 {
		ns.allocRAMGB = 0
	}
	ns.queueDepth--
	if l.ExclusiveHost {
		ns.exclusive = false
	}

	if l.Borrowed {
		fs.borrowedByPool[l.PoolID] -= len(l.SlotIDs)
	} else {
		fs.nativeByPool[l.PoolID] -= len(l.SlotIDs)
	}

	delete(fs.leases, leaseID)
	remaining := fs.leasesByNode[l.NodeID][:0]
	for _, x := range fs.leasesByNode[l.NodeID] {
		if x.LeaseID != leaseID {
			remaining = append(remaining, x)
		}
	}
	fs.leasesByNode[l.NodeID] = remaining
}

// poolOwns reports whether a node belongs to a pool.
func (fs *fleetState) poolOwns(poolID, nodeID string) bool {
	ns := fs.nodes[nodeID]
	return ns != nil && ns.view.PoolID == poolID
}

// ownedSlots counts the slots on machines a pool owns -- its capacity floor
// before any borrowing.
func (fs *fleetState) ownedSlots(poolID string) int {
	n := 0
	for _, ns := range fs.nodes {
		if ns.view.PoolID == poolID {
			n += len(ns.view.Slots)
		}
	}
	return n
}

// lentSlots counts slots of a pool's machines currently held by other pools.
func (fs *fleetState) lentSlots(poolID string) int {
	n := 0
	for _, l := range fs.leases {
		if l.Borrowed && fs.poolOwns(poolID, l.NodeID) {
			n += len(l.SlotIDs)
		}
	}
	return n
}
