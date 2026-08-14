package pgstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/alexk/orch/internal/domain"
	"github.com/alexk/orch/internal/store"
)

// Snapshot is the consistent read the placement loop works from.
//
// It takes no locks. Holding row locks across the scoring pass would mean the
// database is locked for as long as scheduling takes; instead the loop decides
// optimistically and Commit revalidates everything it touches.
func (s *Store) Snapshot(ctx context.Context, queueLimit int) (*store.Snapshot, error) {
	snap := &store.Snapshot{Now: time.Now()}

	nodes, err := s.ListNodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("snapshot nodes: %w", err)
	}
	snap.Nodes = nodes

	if snap.Pools, err = s.ListPools(ctx); err != nil {
		return nil, fmt.Errorf("snapshot pools: %w", err)
	}
	if snap.Leases, err = s.ListActiveLeases(ctx); err != nil {
		return nil, fmt.Errorf("snapshot leases: %w", err)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT t.task_id, t.job_id, t.idx, t.params, t.attempt,
		       j.name, j.owner, j.pool_id, j.image_digest, j.spec, j.mode,
		       j.preemptible, j.submitted_at
		FROM tasks t
		JOIN jobs j ON j.job_id = t.job_id
		WHERE t.state = 'queued' AND j.state NOT IN ('cancelled')
		ORDER BY t.enqueued_at, t.idx
		LIMIT $1`, queueLimit)
	if err != nil {
		return nil, fmt.Errorf("snapshot queue: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			qt        store.QueuedTask
			paramsRaw []byte
			specRaw   []byte
			mode      string
		)
		err := rows.Scan(&qt.Task.TaskID, &qt.Task.JobID, &qt.Task.Index, &paramsRaw, &qt.Task.Attempt,
			&qt.Job.Name, &qt.Job.Owner, &qt.Job.PoolID, &qt.Job.ImageDigest, &specRaw, &mode,
			&qt.Job.Preemptible, &qt.Job.SubmittedAt)
		if err != nil {
			return nil, err
		}
		qt.Task.State = domain.TaskQueued
		if err := fromJSON(paramsRaw, &qt.Task.Params); err != nil {
			return nil, err
		}
		var spec jobSpec
		if err := fromJSON(specRaw, &spec); err != nil {
			return nil, err
		}
		qt.Job.JobID = qt.Task.JobID
		qt.Job.Mode = domain.LeaseMode(mode)
		qt.Job.Command, qt.Job.Env, qt.Job.Assets, qt.Job.Request =
			spec.Command, spec.Env, spec.Assets, spec.Request
		snap.Queued = append(snap.Queued, qt)
	}
	return snap, rows.Err()
}

// Commit applies a plan in one transaction.
//
// Order inside the transaction is load-bearing:
//
//  1. Close reclaimed hosts to borrowers. A reclaim that evicts first will
//     livelock, because the loop refills slots behind it as fast as they empty.
//  2. Evict, freeing slots.
//  3. Place, which may reuse slots freed in step 2 -- so a reclaim and the
//     replacement placement land together or not at all.
//
// Every placement is revalidated against the live row state. A gang of N slots
// is all-or-nothing: allocating them one at a time deadlocks when two
// multi-slot jobs interleave.
func (s *Store) Commit(ctx context.Context, plan store.Plan) (store.CommitResult, error) {
	res := store.CommitResult{Rejected: map[string]string{}}

	err := s.tx(ctx, func(tx pgx.Tx) error {
		if len(plan.NonLendable) > 0 {
			if _, err := tx.Exec(ctx,
				`UPDATE nodes SET lendable = FALSE WHERE node_id = ANY($1)`, plan.NonLendable,
			); err != nil {
				return err
			}
		}

		for _, ev := range plan.Evictions {
			var mode string
			err := tx.QueryRow(ctx,
				`SELECT mode FROM leases WHERE lease_id = $1 AND state = 'active' FOR UPDATE`,
				ev.LeaseID).Scan(&mode)
			if errors.Is(err, pgx.ErrNoRows) {
				// Already gone -- the workload finished on its own between the
				// snapshot and now, which is the good outcome.
				continue
			}
			if err != nil {
				return err
			}
			if err := closeLeaseTx(ctx, tx, ev.LeaseID, domain.LeasePreempted, ev.Reason); err != nil {
				return err
			}
			if ev.Requeue && domain.LeaseMode(mode) == domain.ModeBatch {
				if err := requeueTaskTx(ctx, tx, ev.LeaseID, true); err != nil {
					return err
				}
			}
			res.Evicted = append(res.Evicted, ev)
		}

		for i := range plan.Placements {
			p := &plan.Placements[i]
			ok, reason, err := commitPlacement(ctx, tx, p)
			if err != nil {
				return err
			}
			if !ok {
				res.Rejected[p.LeaseID] = reason
				continue
			}
			res.Placed = append(res.Placed, *p)
		}
		return nil
	})

	return res, err
}

// commitPlacement grants one lease, or reports why it could not.
//
// A rejection is not an error. The snapshot is a moment in time; if the world
// moved underneath it, the task stays queued and the next pass reconsiders it
// with fresh information.
func commitPlacement(ctx context.Context, tx pgx.Tx, p *store.Placement) (bool, string, error) {
	// Lock the task first. SKIP LOCKED means a second scheduler holding this
	// row is left alone rather than waited on -- the task is simply not ours
	// this pass.
	var (
		state   string
		attempt int
	)
	err := tx.QueryRow(ctx, `
		SELECT state, attempt FROM tasks
		WHERE task_id = $1 FOR UPDATE SKIP LOCKED`, p.TaskID).Scan(&state, &attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, "task locked by another scheduler or deleted", nil
	}
	if err != nil {
		return false, "", err
	}
	if domain.TaskState(state) != domain.TaskQueued {
		return false, "task no longer queued (" + state + ")", nil
	}

	// Lock every slot in the gang, then check them together. Partial success is
	// not a state this can end in.
	rows, err := tx.Query(ctx, `
		SELECT slot_id, node_id, state FROM slots
		WHERE slot_id = ANY($1) ORDER BY slot_id FOR UPDATE`, p.SlotIDs)
	if err != nil {
		return false, "", err
	}
	found := 0
	for rows.Next() {
		var slotID, nodeID, slotState string
		if err := rows.Scan(&slotID, &nodeID, &slotState); err != nil {
			rows.Close()
			return false, "", err
		}
		found++
		if domain.SlotState(slotState) != domain.SlotFree {
			rows.Close()
			return false, fmt.Sprintf("slot %s is %s", slotID, slotState), nil
		}
		if nodeID != p.NodeID {
			rows.Close()
			return false, fmt.Sprintf("slot %s moved to node %s", slotID, nodeID), nil
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return false, "", err
	}
	if found != len(p.SlotIDs) {
		return false, "slot disappeared from inventory", nil
	}

	// The node must still be healthy and, if this is a borrow, still lending.
	var (
		nodeState string
		lendable  bool
	)
	err = tx.QueryRow(ctx,
		`SELECT state, lendable FROM nodes WHERE node_id = $1 FOR UPDATE`, p.NodeID,
	).Scan(&nodeState, &lendable)
	if err != nil {
		return false, "", err
	}
	if domain.NodeState(nodeState) != domain.NodeReady {
		return false, "node is " + nodeState, nil
	}
	if p.Borrowed && !lendable {
		return false, "node closed to borrowers", nil
	}

	// Exclusivity is checked here rather than only at scoring time, because
	// another placement in this same plan may have taken the host.
	if p.ExclusiveHost {
		var others int
		if err := tx.QueryRow(ctx,
			`SELECT COUNT(*) FROM leases WHERE node_id = $1 AND state = 'active'`, p.NodeID,
		).Scan(&others); err != nil {
			return false, "", err
		}
		if others > 0 {
			return false, "exclusive host request but node is occupied", nil
		}
	} else {
		var exclusive int
		if err := tx.QueryRow(ctx,
			`SELECT COUNT(*) FROM leases WHERE node_id = $1 AND state = 'active' AND exclusive_host`, p.NodeID,
		).Scan(&exclusive); err != nil {
			return false, "", err
		}
		if exclusive > 0 {
			return false, "node held exclusively", nil
		}
	}

	// The attempt number comes from the locked row, not the snapshot, so the
	// output prefix is unique even if the task was requeued in between. The
	// caller reads it back off the placement to put in the agent assignment.
	prefix := domain.AttemptPrefix(p.JobID, p.TaskID, attempt)
	p.Attempt, p.OutputPrefix = attempt, prefix

	_, err = tx.Exec(ctx, `
		INSERT INTO leases (
			lease_id, task_id, job_id, pool_id, node_id, slot_ids, mode, borrowed,
			exclusive_host, host_share_cores, host_share_ram_gb, granted_at, ttl_ms, state)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, now(), $12, 'active')`,
		p.LeaseID, p.TaskID, p.JobID, p.PoolID, p.NodeID, p.SlotIDs, string(p.Mode),
		p.Borrowed, p.ExclusiveHost, p.HostShareCores, p.HostShareRAMGB,
		p.TTL.Milliseconds())
	if err != nil {
		return false, "", err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE slots SET state = 'allocated', lease_id = $2
		WHERE slot_id = ANY($1)`, p.SlotIDs, p.LeaseID); err != nil {
		return false, "", err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE tasks SET state = 'assigned', assigned_lease_id = $2, node_id = $3, output_prefix = $4
		WHERE task_id = $1`, p.TaskID, p.LeaseID, p.NodeID, prefix); err != nil {
		return false, "", err
	}

	return true, "", nil
}
