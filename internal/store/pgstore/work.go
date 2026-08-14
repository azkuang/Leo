package pgstore

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/alexk/orch/internal/domain"
	"github.com/alexk/orch/internal/store"
)

// jobSpec is the part of a job that the scheduler reads only as capability
// predicates, plus payload references it never dereferences.
type jobSpec struct {
	Command []string               `json:"command,omitempty"`
	Env     map[string]string      `json:"env,omitempty"`
	Assets  []domain.AssetRef      `json:"assets,omitempty"`
	Request domain.ResourceRequest `json:"request"`
}

// CreateJob writes a job and its fan-out tasks in one transaction.
func (s *Store) CreateJob(ctx context.Context, job domain.Job, tasks []domain.Task) error {
	return s.tx(ctx, func(tx pgx.Tx) error {
		spec, err := toJSON(jobSpec{
			Command: job.Command,
			Env:     job.Env,
			Assets:  job.Assets,
			Request: job.Request,
		})
		if err != nil {
			return err
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO jobs (job_id, name, owner, pool_id, image_digest, spec, mode, preemptible, state, submitted_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending', $9)`,
			job.JobID, job.Name, job.Owner, job.PoolID, job.ImageDigest, spec,
			string(job.Mode), job.Preemptible, job.SubmittedAt)
		if err != nil {
			return err
		}

		rows := make([][]any, 0, len(tasks))
		for _, t := range tasks {
			params, err := toJSON(t.Params)
			if err != nil {
				return err
			}
			rows = append(rows, []any{t.TaskID, t.JobID, t.Index, params})
		}
		_, err = tx.CopyFrom(ctx,
			pgx.Identifier{"tasks"},
			[]string{"task_id", "job_id", "idx", "params"},
			pgx.CopyFromRows(rows))
		return err
	})
}

const jobSelect = `
	SELECT j.job_id, j.name, j.owner, j.pool_id, j.image_digest, j.spec, j.mode,
	       j.preemptible, j.state, j.submitted_at
	FROM jobs j`

func scanJobs(rows pgx.Rows) ([]domain.Job, error) {
	defer rows.Close()

	var out []domain.Job
	for rows.Next() {
		var (
			j           domain.Job
			specRaw     []byte
			mode, state string
		)
		err := rows.Scan(&j.JobID, &j.Name, &j.Owner, &j.PoolID, &j.ImageDigest,
			&specRaw, &mode, &j.Preemptible, &state, &j.SubmittedAt)
		if err != nil {
			return nil, err
		}
		j.Mode = domain.LeaseMode(mode)
		j.State = domain.JobState(state)

		var spec jobSpec
		if err := fromJSON(specRaw, &spec); err != nil {
			return nil, err
		}
		j.Command, j.Env, j.Assets, j.Request = spec.Command, spec.Env, spec.Assets, spec.Request
		out = append(out, j)
	}
	return out, rows.Err()
}

// ListJobs returns jobs, optionally filtered to one owner.
func (s *Store) ListJobs(ctx context.Context, owner string) ([]domain.Job, error) {
	q := jobSelect + ` ORDER BY j.submitted_at DESC LIMIT 200`
	args := []any{}
	if owner != "" {
		q = jobSelect + ` WHERE j.owner = $1 ORDER BY j.submitted_at DESC LIMIT 200`
		args = append(args, owner)
	}
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanJobs(rows)
}

// GetJob returns a job with all of its tasks.
func (s *Store) GetJob(ctx context.Context, jobID string) (domain.Job, []domain.Task, error) {
	rows, err := s.pool.Query(ctx, jobSelect+` WHERE j.job_id = $1`, jobID)
	if err != nil {
		return domain.Job{}, nil, err
	}
	jobs, err := scanJobs(rows)
	if err != nil {
		return domain.Job{}, nil, err
	}
	if len(jobs) == 0 {
		return domain.Job{}, nil, store.ErrNotFound
	}
	tasks, err := s.ListTasks(ctx, jobID)
	return jobs[0], tasks, err
}

// ListTasks returns every task of a job in submission order.
func (s *Store) ListTasks(ctx context.Context, jobID string) ([]domain.Task, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT task_id, job_id, idx, params, attempt, state,
		       COALESCE(assigned_lease_id, ''), COALESCE(node_id, ''),
		       output_prefix, preemptions, message, started_at, finished_at
		FROM tasks WHERE job_id = $1 ORDER BY idx`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []domain.Task
	for rows.Next() {
		var (
			t         domain.Task
			paramsRaw []byte
			state     string
			started   *time.Time
			finished  *time.Time
		)
		err := rows.Scan(&t.TaskID, &t.JobID, &t.Index, &paramsRaw, &t.Attempt, &state,
			&t.AssignedLeaseID, &t.NodeID, &t.OutputPrefix, &t.Preemptions, &t.Message,
			&started, &finished)
		if err != nil {
			return nil, err
		}
		t.State = domain.TaskState(state)
		if err := fromJSON(paramsRaw, &t.Params); err != nil {
			return nil, err
		}
		if started != nil {
			t.StartedAt = *started
		}
		if finished != nil {
			t.FinishedAt = *finished
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// CancelJob stops a job and returns the leases that must be revoked.
func (s *Store) CancelJob(ctx context.Context, jobID string) ([]string, error) {
	var leases []string
	err := s.tx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE jobs SET state = 'cancelled' WHERE job_id = $1`, jobID); err != nil {
			return err
		}

		rows, err := tx.Query(ctx,
			`SELECT lease_id FROM leases WHERE job_id = $1 AND state = 'active'`, jobID)
		if err != nil {
			return err
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			leases = append(leases, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}

		// Queued work simply stops being queued; running work is torn down by
		// the caller revoking the leases returned here.
		_, err = tx.Exec(ctx, `
			UPDATE tasks SET state = 'failed', message = 'job cancelled', finished_at = now()
			WHERE job_id = $1 AND state NOT IN ('done', 'failed')`, jobID)
		return err
	})
	return leases, err
}

// ApplyTaskStatus records an agent-reported transition, fenced by lease ID.
//
// A preempted attempt's worker may still be alive and reporting. Its lease is
// no longer active, so its updates are rejected here and its writes land under
// an attempt prefix nobody reads -- which is why no storage-layer fencing is
// needed.
func (s *Store) ApplyTaskStatus(ctx context.Context, st store.TaskStatus) error {
	return s.tx(ctx, func(tx pgx.Tx) error {
		var (
			nodeID string
			jobID  string
			mode   string
		)
		err := tx.QueryRow(ctx, `
			SELECT node_id, job_id, mode FROM leases
			WHERE lease_id = $1 AND state = 'active' FOR UPDATE`, st.LeaseID,
		).Scan(&nodeID, &jobID, &mode)
		if errors.Is(err, pgx.ErrNoRows) {
			return store.ErrStaleLease
		}
		if err != nil {
			return err
		}

		switch st.State {
		case domain.TaskRunning, domain.TaskStaging:
			_, err = tx.Exec(ctx, `
				UPDATE tasks SET state = $2, node_id = $3, message = $4,
				       started_at = COALESCE(started_at, $5)
				WHERE task_id = $1 AND assigned_lease_id = $6`,
				st.TaskID, string(st.State), nodeID, st.Message, st.At, st.LeaseID)
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `UPDATE jobs SET state = 'running' WHERE job_id = $1 AND state = 'pending'`, jobID)
			return err

		case domain.TaskDone, domain.TaskFailed:
			_, err = tx.Exec(ctx, `
				UPDATE tasks SET state = $2, message = $3, output_prefix = $4, finished_at = $5
				WHERE task_id = $1 AND assigned_lease_id = $6`,
				st.TaskID, string(st.State), st.Message, st.OutputPrefix, st.At, st.LeaseID)
			if err != nil {
				return err
			}
			if err := releaseLeaseTx(ctx, tx, st.LeaseID, string(st.State)); err != nil {
				return err
			}
			if err := recordOutcome(ctx, tx, nodeID, st.State == domain.TaskDone); err != nil {
				return err
			}
			return rollupJobTx(ctx, tx, jobID)
		}
		return nil
	})
}

// decayFactor ages the reliability counters on every write. A node that failed
// yesterday and has run cleanly since converges back toward trusted, without a
// cutoff that discards history in one step.
const decayFactor = 0.98

func recordOutcome(ctx context.Context, tx pgx.Tx, nodeID string, ok bool) error {
	// Decay and increment fold into one expression per column. Assigning a
	// column twice in a single UPDATE is a syntax error, and splitting this
	// into two statements would decay the untouched counter twice.
	success, failure := 0, 1
	if ok {
		success, failure = 1, 0
	}
	_, err := tx.Exec(ctx, `
		UPDATE nodes SET
			recent_successes = recent_successes * $2 + $3,
			recent_failures  = recent_failures  * $2 + $4
		WHERE node_id = $1`, nodeID, decayFactor, success, failure)
	return err
}

// rollupJobTx recomputes a job's summary state from its tasks.
func rollupJobTx(ctx context.Context, tx pgx.Tx, jobID string) error {
	_, err := tx.Exec(ctx, `
		UPDATE jobs SET state = CASE
			WHEN (SELECT COUNT(*) FROM tasks WHERE job_id = $1 AND state NOT IN ('done','failed')) > 0 THEN 'running'
			WHEN (SELECT COUNT(*) FROM tasks WHERE job_id = $1 AND state = 'failed') > 0 THEN 'failed'
			ELSE 'completed' END
		WHERE job_id = $1 AND state NOT IN ('cancelled')`, jobID)
	return err
}
