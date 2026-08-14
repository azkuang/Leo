package pgstore

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/alexk/orch/internal/domain"
)

// closeLeaseTx ends a lease and frees the slots it held.
//
// Slots return to 'free' only from 'allocated'. A slot marked unhealthy while
// occupied stays unhealthy, so releasing a lease never launders a degraded
// device back into the schedulable pool.
func closeLeaseTx(ctx context.Context, tx pgx.Tx, leaseID string, state domain.LeaseState, reason string) error {
	_, err := tx.Exec(ctx, `
		UPDATE leases SET state = $2, released_reason = $3
		WHERE lease_id = $1 AND state = 'active'`, leaseID, string(state), reason)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		UPDATE slots SET state = 'free', lease_id = NULL
		WHERE lease_id = $1 AND state = 'allocated'`, leaseID)
	return err
}

func releaseLeaseTx(ctx context.Context, tx pgx.Tx, leaseID, reason string) error {
	return closeLeaseTx(ctx, tx, leaseID, domain.LeaseReleased, reason)
}

// requeueTaskTx returns a task to the queue for another attempt.
//
// Each attempt gets a fresh output prefix, so the previous attempt's partial
// writes are simply orphaned rather than needing cleanup.
func requeueTaskTx(ctx context.Context, tx pgx.Tx, leaseID string, countPreemption bool) error {
	bump := 0
	if countPreemption {
		bump = 1
	}
	_, err := tx.Exec(ctx, `
		UPDATE tasks SET
			state = 'queued',
			attempt = attempt + 1,
			preemptions = preemptions + $2,
			assigned_lease_id = NULL,
			node_id = NULL,
			started_at = NULL,
			enqueued_at = now()
		WHERE assigned_lease_id = $1 AND state NOT IN ('done', 'failed')`,
		leaseID, bump)
	return err
}

const leaseSelect = `
	SELECT lease_id, task_id, job_id, pool_id, node_id, slot_ids, mode, borrowed,
	       exclusive_host, host_share_cores, host_share_ram_gb, granted_at,
	       ttl_ms, renewed_at, state
	FROM leases`

func scanLeases(rows pgx.Rows) ([]domain.Lease, error) {
	defer rows.Close()

	var out []domain.Lease
	for rows.Next() {
		var (
			l           domain.Lease
			mode, state string
			ttlMS       int64
			renewed     *time.Time
		)
		err := rows.Scan(&l.LeaseID, &l.TaskID, &l.JobID, &l.PoolID, &l.NodeID,
			&l.SlotIDs, &mode, &l.Borrowed, &l.ExclusiveHost, &l.HostShareCores,
			&l.HostShareRAMGB, &l.GrantedAt, &ttlMS, &renewed, &state)
		if err != nil {
			return nil, err
		}
		l.Mode = domain.LeaseMode(mode)
		l.State = domain.LeaseState(state)
		l.TTL = time.Duration(ttlMS) * time.Millisecond
		if renewed != nil {
			l.RenewedAt = *renewed
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

// ListActiveLeases returns every lease currently held.
func (s *Store) ListActiveLeases(ctx context.Context) ([]domain.Lease, error) {
	rows, err := s.pool.Query(ctx, leaseSelect+` WHERE state = 'active' ORDER BY granted_at`)
	if err != nil {
		return nil, err
	}
	return scanLeases(rows)
}

// RenewLeases extends leases a node still believes it holds.
//
// Rejected IDs are the fencing signal travelling back to the agent: the control
// plane no longer recognises that lease, so the agent must stop whatever is
// running under it.
func (s *Store) RenewLeases(ctx context.Context, nodeID string, leaseIDs []string, at time.Time) ([]string, []string, error) {
	if len(leaseIDs) == 0 {
		return nil, nil, nil
	}

	rows, err := s.pool.Query(ctx, `
		UPDATE leases SET renewed_at = $3
		WHERE node_id = $1 AND lease_id = ANY($2) AND state = 'active'
		RETURNING lease_id`, nodeID, leaseIDs, at)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	accepted := make(map[string]bool, len(leaseIDs))
	var renewed []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, nil, err
		}
		accepted[id] = true
		renewed = append(renewed, id)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	var rejected []string
	for _, id := range leaseIDs {
		if !accepted[id] {
			rejected = append(rejected, id)
		}
	}
	return renewed, rejected, nil
}

// ExpireLeases reclaims leases that stopped being renewed.
//
// Short TTLs plus renewal are what make a disconnected workstation cheap: the
// lease lapses, the slots free, and the task requeues without anyone deciding
// whether the machine is coming back.
func (s *Store) ExpireLeases(ctx context.Context, now time.Time) ([]domain.Lease, error) {
	var expired []domain.Lease

	err := s.tx(ctx, func(tx pgx.Tx) error {
		// The TTL is added to the last renewal rather than subtracted from now,
		// and ttl_ms is cast explicitly: bigint * interval is not a defined
		// operator, and leaving $1 untyped lets Postgres infer it as an
		// interval and compare the wrong things.
		rows, err := tx.Query(ctx, leaseSelect+`
			WHERE state = 'active'
			  AND COALESCE(renewed_at, granted_at)
			      + (ttl_ms::double precision * INTERVAL '1 millisecond') < $1::timestamptz
			FOR UPDATE`, now)
		if err != nil {
			return err
		}
		expired, err = scanLeases(rows)
		if err != nil {
			return err
		}

		for _, l := range expired {
			if err := closeLeaseTx(ctx, tx, l.LeaseID, domain.LeaseExpired, "ttl expired"); err != nil {
				return err
			}
			// An expired batch lease is indistinguishable from a lost node, so
			// the task goes back on the queue rather than being failed.
			if l.Mode == domain.ModeBatch {
				if err := requeueTaskTx(ctx, tx, l.LeaseID, false); err != nil {
					return err
				}
			}
		}
		return nil
	})
	return expired, err
}

// ReleaseLeasesOnNode revokes every lease on a node at once, for a disconnect
// or an operator-driven drain.
func (s *Store) ReleaseLeasesOnNode(ctx context.Context, nodeID, reason string, requeue bool) ([]domain.Lease, error) {
	var released []domain.Lease

	err := s.tx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, leaseSelect+` WHERE node_id = $1 AND state = 'active' FOR UPDATE`, nodeID)
		if err != nil {
			return err
		}
		released, err = scanLeases(rows)
		if err != nil {
			return err
		}

		for _, l := range released {
			if err := closeLeaseTx(ctx, tx, l.LeaseID, domain.LeaseReleased, reason); err != nil {
				return err
			}
			if requeue && l.Mode == domain.ModeBatch {
				if err := requeueTaskTx(ctx, tx, l.LeaseID, false); err != nil {
					return err
				}
			}
		}
		return nil
	})
	return released, err
}
