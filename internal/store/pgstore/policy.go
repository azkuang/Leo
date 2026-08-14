package pgstore

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/alexk/orch/internal/store"
)

// GetPolicy reads the current scoring configuration.
func (s *Store) GetPolicy(ctx context.Context) (store.Policy, error) {
	var (
		p          store.Policy
		weightsRaw []byte
	)
	err := s.pool.QueryRow(ctx,
		`SELECT preset, weights, reload_generation FROM policy WHERE id = 1`,
	).Scan(&p.Preset, &weightsRaw, &p.ReloadGeneration)
	if err != nil {
		return p, err
	}
	if err := fromJSON(weightsRaw, &p.Weights); err != nil {
		return p, err
	}
	if p.Weights == nil {
		p.Weights = map[string]float64{}
	}
	return p, nil
}

// SetPolicy replaces the scoring configuration and bumps the reload generation.
//
// The generation is what the running scheduler watches: policy is data, so
// changing weights takes effect on the next pass rather than on the next
// restart.
func (s *Store) SetPolicy(ctx context.Context, preset string, weights map[string]float64) (store.Policy, error) {
	raw, err := toJSON(weights)
	if err != nil {
		return store.Policy{}, err
	}
	var p store.Policy
	var weightsRaw []byte
	err = s.pool.QueryRow(ctx, `
		UPDATE policy SET preset = $1, weights = $2,
		       reload_generation = reload_generation + 1, updated_at = now()
		WHERE id = 1
		RETURNING preset, weights, reload_generation`, preset, raw,
	).Scan(&p.Preset, &weightsRaw, &p.ReloadGeneration)
	if err != nil {
		return p, err
	}
	err = fromJSON(weightsRaw, &p.Weights)
	return p, err
}

// IssueJoinToken mints a single-use token for enrolling one machine.
func (s *Store) IssueJoinToken(ctx context.Context, poolID string) (string, error) {
	if poolID == "" {
		poolID = "default"
	}
	token := uuid.NewString()
	_, err := s.pool.Exec(ctx,
		`INSERT INTO join_tokens (token, pool_id) VALUES ($1, $2)`, token, poolID)
	return token, err
}

// SaveExplanation records why the scheduler placed a task where it did.
func (s *Store) SaveExplanation(ctx context.Context, taskID string, payload []byte) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO placement_explanations (task_id, payload) VALUES ($1, $2)
		ON CONFLICT (task_id) DO UPDATE SET payload = EXCLUDED.payload, created_at = now()`,
		taskID, payload)
	return err
}

// GetExplanation returns the stored reasoning for a task's placement.
func (s *Store) GetExplanation(ctx context.Context, taskID string) ([]byte, error) {
	var payload []byte
	err := s.pool.QueryRow(ctx,
		`SELECT payload FROM placement_explanations WHERE task_id = $1`, taskID).Scan(&payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	return payload, err
}
