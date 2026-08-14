package pgstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/alexk/orch/internal/domain"
	"github.com/alexk/orch/internal/store"
)

// RegisterNode creates or reconciles a node from its self-reported capability
// document.
//
// Everything about the machine comes from the agent: devices, host resources,
// driver versions. Nothing is typed by a human, because every fact a human
// types is a fact that goes stale.
func (s *Store) RegisterNode(ctx context.Context, reg store.NodeRegistration) (string, []string, error) {
	var (
		nodeID string
		active []string
	)

	err := s.tx(ctx, func(tx pgx.Tx) error {
		nodeID = reg.NodeID

		// A node reconnecting keeps its identity; the control plane already
		// holds its leases and the agent needs them back.
		known := false
		if nodeID != "" {
			err := tx.QueryRow(ctx, `SELECT TRUE FROM nodes WHERE node_id = $1`, nodeID).Scan(&known)
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
		}

		poolID := domain.DefaultPoolID
		if !known {
			// A first join spends its token. Single-use, so a leaked token
			// cannot enrol a second machine.
			if reg.JoinToken != "" {
				err := tx.QueryRow(ctx, `
					UPDATE join_tokens SET used_at = now(), used_by = $2
					WHERE token = $1 AND used_at IS NULL
					RETURNING pool_id`,
					reg.JoinToken, reg.Hostname,
				).Scan(&poolID)
				if errors.Is(err, pgx.ErrNoRows) {
					return fmt.Errorf("join token invalid or already used")
				}
				if err != nil {
					return err
				}
			}
			if nodeID == "" {
				nodeID = "node-" + uuid.NewString()[:8]
			}
		}

		caps, err := toJSON(reg.Capabilities)
		if err != nil {
			return err
		}
		labels, err := toJSON(reg.Capabilities.Labels)
		if err != nil {
			return err
		}
		h := reg.Capabilities.Host

		if known {
			_, err = tx.Exec(ctx, `
				UPDATE nodes SET
					hostname = $2, state = 'ready', capability_doc = $3, labels = $4,
					simulated = $5, host_cores = $6, host_ram_gb = $7, host_scratch_gb = $8,
					last_heartbeat = now()
				WHERE node_id = $1`,
				nodeID, reg.Hostname, caps, labels, reg.Simulated, h.Cores, h.RAMGB, h.ScratchGB)
		} else {
			_, err = tx.Exec(ctx, `
				INSERT INTO nodes (
					node_id, hostname, pool_id, state, capability_doc, labels, simulated,
					host_cores, host_ram_gb, host_scratch_gb, free_scratch_gb, last_heartbeat)
				VALUES ($1, $2, $3, 'ready', $4, $5, $6, $7, $8, $9, $9, now())`,
				nodeID, reg.Hostname, poolID, caps, labels, reg.Simulated,
				h.Cores, h.RAMGB, h.ScratchGB)
		}
		if err != nil {
			return err
		}

		if err := reconcileSlots(ctx, tx, nodeID, reg.Capabilities.Devices); err != nil {
			return err
		}

		rows, err := tx.Query(ctx,
			`SELECT lease_id FROM leases WHERE node_id = $1 AND state = 'active'`, nodeID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return err
			}
			active = append(active, id)
		}
		return rows.Err()
	})

	return nodeID, active, err
}

// reconcileSlots makes the slot table match the devices the agent just
// reported. Devices that disappeared are dropped only if they were free --
// a device vanishing while it holds a lease is a health event, and the lease
// expiry path handles it rather than a cascading delete here.
func reconcileSlots(ctx context.Context, tx pgx.Tx, nodeID string, devices []domain.Device) error {
	seen := make([]int, 0, len(devices))

	for _, d := range devices {
		seen = append(seen, d.Index)
		slotID := fmt.Sprintf("%s-d%d", nodeID, d.Index)
		kind := domain.KindGPU
		if d.MIGProfile != "" {
			kind = domain.KindMIGInstance
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO slots (
				slot_id, node_id, kind, device_index, vram_gb, compute_capability,
				encode_sessions, decode_sessions, ecc_present, state)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'free')
			ON CONFLICT (node_id, device_index) DO UPDATE SET
				kind = EXCLUDED.kind,
				vram_gb = EXCLUDED.vram_gb,
				compute_capability = EXCLUDED.compute_capability,
				encode_sessions = EXCLUDED.encode_sessions,
				decode_sessions = EXCLUDED.decode_sessions,
				ecc_present = EXCLUDED.ecc_present`,
			slotID, nodeID, string(kind), d.Index, d.VRAMGB, d.ComputeCapability,
			d.EncodeSessions, d.DecodeSessions, d.ECCPresent)
		if err != nil {
			return err
		}
	}

	_, err := tx.Exec(ctx,
		`DELETE FROM slots WHERE node_id = $1 AND state = 'free' AND NOT (device_index = ANY($2))`,
		nodeID, seen)
	return err
}

// Heartbeat writes the fast-path telemetry the scheduler reads.
//
// This path is in-memory and sub-second by design. The scheduler never queries
// Prometheus: doing so would couple placement latency and availability to the
// monitoring stack and mean scheduling on data up to a scrape interval stale.
func (s *Store) Heartbeat(ctx context.Context, up store.HeartbeatUpdate) error {
	return s.tx(ctx, func(tx pgx.Tx) error {
		// A node with an empty cache sends no digests at all, which arrives as
		// a nil slice. COALESCE keeps that from violating the NOT NULL
		// constraint -- an empty cache is the normal state of a fresh node,
		// not an error.
		_, err := tx.Exec(ctx, `
			UPDATE nodes SET
				last_heartbeat = $2, cached_digests = COALESCE($3::text[], '{}'::text[]),
				load_avg_1m = $4, free_scratch_gb = $5,
				state = CASE WHEN state IN ('offline', 'unhealthy') THEN 'ready' ELSE state END
			WHERE node_id = $1`,
			up.NodeID, up.At, up.CachedDigests, up.LoadAvg1m, up.FreeScratchGB)
		if err != nil {
			return err
		}

		for _, sample := range up.Samples {
			raw, err := toJSON(sample)
			if err != nil {
				return err
			}
			// A degraded device stops taking new work but keeps whatever it is
			// already running; killing in-flight work on a single XID would
			// cost more than it saves.
			state := "state"
			if sample.Degraded() {
				state = `CASE WHEN state = 'free' THEN 'unhealthy' ELSE state END`
			} else {
				state = `CASE WHEN state = 'unhealthy' THEN 'free' ELSE state END`
			}
			_, err = tx.Exec(ctx, fmt.Sprintf(`
				UPDATE slots SET health = $3, state = %s
				WHERE node_id = $1 AND device_index = $2`, state),
				up.NodeID, sample.DeviceIndex, raw)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// SetNodeState moves a node through its lifecycle.
func (s *Store) SetNodeState(ctx context.Context, nodeID string, state domain.NodeState) error {
	_, err := s.pool.Exec(ctx, `UPDATE nodes SET state = $2 WHERE node_id = $1`, nodeID, string(state))
	return err
}

// SetNodeLendable opens or closes a machine to borrowers.
//
// Machine reclaim clears this before evicting anything. Doing it the other way
// round livelocks: the scheduler happily refills slots behind the eviction as
// fast as they are emptied.
func (s *Store) SetNodeLendable(ctx context.Context, nodeID string, lendable bool) error {
	_, err := s.pool.Exec(ctx, `UPDATE nodes SET lendable = $2 WHERE node_id = $1`, nodeID, lendable)
	return err
}

// AssignNodePool moves a node between pools. Ownership is at machine
// granularity, so this moves every slot on the box with it.
func (s *Store) AssignNodePool(ctx context.Context, nodeID, poolID string) error {
	_, err := s.pool.Exec(ctx, `UPDATE nodes SET pool_id = $2 WHERE node_id = $1`, nodeID, poolID)
	return err
}

// MarkStaleNodesOffline flags nodes that stopped heartbeating.
//
// Users reboot, sleep, and unplug workstations. Node churn is routine, not an
// incident, so this is an ordinary sweep rather than an alert path.
func (s *Store) MarkStaleNodesOffline(ctx context.Context, cutoff time.Time) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		UPDATE nodes SET state = 'offline'
		WHERE state <> 'offline' AND (last_heartbeat IS NULL OR last_heartbeat < $1)
		RETURNING node_id`, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

const nodeSelect = `
	SELECT n.node_id, n.hostname, n.pool_id, n.state, n.capability_doc, n.labels,
	       n.simulated, n.lendable, n.last_heartbeat, n.host_cores, n.host_ram_gb,
	       n.host_scratch_gb, n.free_scratch_gb, n.load_avg_1m, n.cached_digests,
	       n.recent_successes, n.recent_failures,
	       COALESCE(l.cores, 0), COALESCE(l.ram_gb, 0), COALESCE(l.depth, 0)
	FROM nodes n
	LEFT JOIN (
	    SELECT node_id,
	           SUM(host_share_cores)  AS cores,
	           SUM(host_share_ram_gb) AS ram_gb,
	           COUNT(*)               AS depth
	    FROM leases WHERE state = 'active' GROUP BY node_id
	) l ON l.node_id = n.node_id`

func scanNodes(rows pgx.Rows) ([]*store.NodeView, error) {
	defer rows.Close()

	var out []*store.NodeView
	for rows.Next() {
		var (
			n         store.NodeView
			capsRaw   []byte
			labelsRaw []byte
			lastHB    *time.Time
			state     string
		)
		err := rows.Scan(
			&n.NodeID, &n.Hostname, &n.PoolID, &state, &capsRaw, &labelsRaw,
			&n.Simulated, &n.Lendable, &lastHB, &n.HostCores, &n.HostRAMGB,
			&n.HostScratchGB, &n.FreeScratchGB, &n.LoadAvg1m, &n.CachedDigests,
			&n.RecentSuccesses, &n.RecentFailures,
			&n.AllocatedCores, &n.AllocatedRAMGB, &n.QueueDepth,
		)
		if err != nil {
			return nil, err
		}
		n.State = domain.NodeState(state)
		if lastHB != nil {
			n.LastHeartbeat = *lastHB
		}
		if err := fromJSON(capsRaw, &n.Capabilities); err != nil {
			return nil, err
		}
		if err := fromJSON(labelsRaw, &n.Labels); err != nil {
			return nil, err
		}
		out = append(out, &n)
	}
	return out, rows.Err()
}

func (s *Store) attachSlots(ctx context.Context, nodes []*store.NodeView) error {
	if len(nodes) == 0 {
		return nil
	}
	byID := make(map[string]*store.NodeView, len(nodes))
	ids := make([]string, 0, len(nodes))
	for _, n := range nodes {
		byID[n.NodeID] = n
		ids = append(ids, n.NodeID)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT slot_id, node_id, kind, device_index, vram_gb, compute_capability,
		       encode_sessions, decode_sessions, ecc_present, state,
		       COALESCE(lease_id, ''), health
		FROM slots WHERE node_id = ANY($1)
		ORDER BY node_id, device_index`, ids)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			sl        domain.Slot
			kind, st  string
			healthRaw []byte
		)
		err := rows.Scan(&sl.SlotID, &sl.NodeID, &kind, &sl.DeviceIndex, &sl.VRAMGB,
			&sl.ComputeCapability, &sl.EncodeSessions, &sl.DecodeSessions,
			&sl.ECCPresent, &st, &sl.LeaseID, &healthRaw)
		if err != nil {
			return err
		}
		sl.Kind = domain.SlotKind(kind)
		sl.State = domain.SlotState(st)
		if err := fromJSON(healthRaw, &sl.Health); err != nil {
			return err
		}
		if n := byID[sl.NodeID]; n != nil {
			n.Slots = append(n.Slots, sl)
		}
	}
	return rows.Err()
}

// ListNodes returns every node with its slots and derived allocation counters.
func (s *Store) ListNodes(ctx context.Context) ([]*store.NodeView, error) {
	rows, err := s.pool.Query(ctx, nodeSelect+` ORDER BY n.hostname`)
	if err != nil {
		return nil, err
	}
	nodes, err := scanNodes(rows)
	if err != nil {
		return nil, err
	}
	if err := s.attachSlots(ctx, nodes); err != nil {
		return nil, err
	}
	return nodes, nil
}

// GetNode returns one node with its slots.
func (s *Store) GetNode(ctx context.Context, nodeID string) (*store.NodeView, error) {
	rows, err := s.pool.Query(ctx, nodeSelect+` WHERE n.node_id = $1`, nodeID)
	if err != nil {
		return nil, err
	}
	nodes, err := scanNodes(rows)
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, store.ErrNotFound
	}
	if err := s.attachSlots(ctx, nodes); err != nil {
		return nil, err
	}
	return nodes[0], nil
}

// UpsertPool creates or updates a pool definition.
func (s *Store) UpsertPool(ctx context.Context, p domain.Pool) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO pools (pool_id, name, guaranteed_slots, lendable, borrow_ceiling)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (pool_id) DO UPDATE SET
			name = EXCLUDED.name,
			guaranteed_slots = EXCLUDED.guaranteed_slots,
			lendable = EXCLUDED.lendable,
			borrow_ceiling = EXCLUDED.borrow_ceiling`,
		p.PoolID, p.Name, p.GuaranteedSlots, p.Lendable, p.BorrowCeiling)
	return err
}

// ListPools returns every pool.
func (s *Store) ListPools(ctx context.Context) ([]domain.Pool, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT pool_id, name, guaranteed_slots, lendable, borrow_ceiling
		FROM pools ORDER BY pool_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []domain.Pool
	for rows.Next() {
		var p domain.Pool
		if err := rows.Scan(&p.PoolID, &p.Name, &p.GuaranteedSlots, &p.Lendable, &p.BorrowCeiling); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
