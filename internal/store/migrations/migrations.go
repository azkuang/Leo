// Package migrations embeds the SQL schema and applies it on startup.
//
// The files ship inside the binary and run when the control plane boots, so
// there is no separate migration step to forget and no ordering question
// between deploying the binary and migrating the database.
package migrations

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"

	"github.com/jackc/pgx/v5"
)

//go:embed *.sql
var files embed.FS

// Run applies every migration that has not been recorded yet, each in its own
// transaction alongside the row that records it. A partially applied migration
// therefore cannot be marked as done.
func Run(ctx context.Context, begin func(context.Context) (pgx.Tx, error)) error {
	names, err := list()
	if err != nil {
		return err
	}

	bootstrap, err := begin(ctx)
	if err != nil {
		return err
	}
	_, err = bootstrap.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		name       TEXT PRIMARY KEY,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`)
	if err != nil {
		_ = bootstrap.Rollback(ctx)
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	if err := bootstrap.Commit(ctx); err != nil {
		return err
	}

	for _, name := range names {
		tx, err := begin(ctx)
		if err != nil {
			return err
		}

		var applied bool
		err = tx.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE name = $1)`, name,
		).Scan(&applied)
		if err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("check migration %s: %w", name, err)
		}
		if applied {
			_ = tx.Rollback(ctx)
			continue
		}

		body, err := files.ReadFile(name)
		if err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if _, err := tx.Exec(ctx, string(body)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (name) VALUES ($1)`, name); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("record migration %s: %w", name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit migration %s: %w", name, err)
		}
	}
	return nil
}

func list() ([]string, error) {
	entries, err := fs.ReadDir(files, ".")
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	// Filenames are zero-padded and ordered, so lexical sort is apply order.
	sort.Strings(names)
	return names, nil
}
