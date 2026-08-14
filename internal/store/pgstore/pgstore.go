// Package pgstore implements store.Store on Postgres.
//
// Postgres rather than etcd for one reason above the others: the gang
// reservation in §7 is a single transaction here, where etcd would need a
// hand-rolled multi-key compare-and-swap. Job history and scheduling inputs
// also live here, which etcd stores badly.
package pgstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/alexk/orch/internal/store"
	"github.com/alexk/orch/internal/store/migrations"
)

// Store is a Postgres-backed store.Store.
type Store struct {
	pool *pgxpool.Pool
}

var _ store.Store = (*Store)(nil)

// Open connects, waits for the server to accept queries, and applies any
// pending migrations.
func Open(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	cfg.MaxConns = 16
	cfg.MaxConnLifetime = time.Hour

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	// The control plane and the database usually start together, so a brief
	// unavailability at boot is expected rather than an error.
	if err := waitReady(ctx, pool, 30*time.Second); err != nil {
		pool.Close()
		return nil, err
	}

	if err := migrations.Run(ctx, func(c context.Context) (pgx.Tx, error) {
		return pool.Begin(c)
	}); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	return &Store{pool: pool}, nil
}

func waitReady(ctx context.Context, pool *pgxpool.Pool, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	var last error

	for time.Now().Before(deadline) {
		err := pool.Ping(ctx)
		if err == nil {
			return nil
		}
		last = err

		// Only a database that is not answering yet is worth waiting for. If
		// the server replied with an error -- bad password, no such database,
		// no permission -- retrying just hides a fixable mistake behind a long
		// silence and then reports the wrong cause.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			return fmt.Errorf("database refused the connection (%s): %w", pgErr.Code, err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("database not ready after %s: %w", limit, last)
}

// Close releases the connection pool.
func (s *Store) Close() { s.pool.Close() }

// Pool exposes the underlying pool for tests and for the leader-election
// advisory lock.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// tx runs fn inside a transaction, rolling back on error or panic.
func (s *Store) tx(ctx context.Context, fn func(pgx.Tx) error) error {
	t, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			_ = t.Rollback(ctx)
			panic(p)
		}
	}()
	if err := fn(t); err != nil {
		_ = t.Rollback(ctx)
		return err
	}
	return t.Commit(ctx)
}

func toJSON(v any) ([]byte, error) {
	if v == nil {
		return []byte("{}"), nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	if len(b) == 0 || string(b) == "null" {
		return []byte("{}"), nil
	}
	return b, nil
}

func fromJSON(raw []byte, dst any) error {
	if len(raw) == 0 {
		return nil
	}
	return json.Unmarshal(raw, dst)
}
