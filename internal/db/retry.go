package db

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func backoffForAttempt(attempt int) time.Duration {
	// attempt starts at 1
	// 1st failure => retry after 2s, then 4s, 8s, 16s... (cap 60s)
	secs := 1 << uint(min(attempt, 6)) // 2,4,8,16,32,64
	if secs > 60 {
		secs = 60
	}
	return time.Duration(secs) * time.Second
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ScheduleRetryOrFail decides whether to retry.
// max_retries means "extra tries after attempt=1".
func (s *Store) ScheduleRetryOrFail(ctx context.Context, jobID, runID uuid.UUID, reason string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Get attempt + job.max_retries
	var attempt int
	if err := tx.QueryRow(ctx, `select attempt from runs where run_id=$1`, runID).Scan(&attempt); err != nil {
		return err
	}

	var maxRetries int
	if err := tx.QueryRow(ctx, `select max_retries from jobs where job_id=$1`, jobID).Scan(&maxRetries); err != nil {
		return err
	}

	// total allowed attempts = 1 + max_retries
	if attempt < 1+maxRetries {
		delay := backoffForAttempt(attempt) // based on current failed attempt
		next := time.Now().UTC().Add(delay)

		// job back to pending with a delay
		_, err = tx.Exec(ctx, `
			update jobs
			set state='PENDING', next_run_at=$2
			where job_id=$1
		`, jobID, next)
		if err != nil {
			return err
		}

		// event
		_, _ = tx.Exec(ctx, `
		insert into events(job_id, run_id, type, payload)
		values ($1,$2,'RETRY_TRIGGERED',
			jsonb_build_object(
				'next_run_at',$3,
				'delay_seconds',$4,
				'attempt',$5,
				'job_state','PENDING',
				'run_state','FAILED',
				'reason',$6
			)
		)
	`, jobID, runID, next, int(delay.Seconds()), attempt, reason)

		return tx.Commit(ctx)
	}

	// No attempts left => job FAILED (run already FAILED)
	_, err = tx.Exec(ctx, `update jobs set state='FAILED' where job_id=$1`, jobID)
	if err != nil {
		return err
	}

	_, _ = tx.Exec(ctx, `
	insert into events(job_id, run_id, type, payload)
	values ($1,$2,'JOB_FAILED',
		jsonb_build_object(
			'final', true,
			'attempt', $3,
			'max_retries', $4,
			'reason', $5
		)
	)
    `, jobID, runID, attempt, maxRetries, reason)

	return tx.Commit(ctx)
}

// NextAttempt returns max(attempt)+1 for a job (inside tx for safety).
func nextAttemptTx(ctx context.Context, tx pgx.Tx, jobID uuid.UUID) (int, error) {
	var next int
	err := tx.QueryRow(ctx, `
		select coalesce(max(attempt),0)+1
		from runs
		where job_id=$1
	`, jobID).Scan(&next)
	return next, err
}

// Querier is implemented by pgx.Tx and pgxpool.Pool for QueryRow.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) RowScanner
}

// RowScanner minimal interface for Scan
type RowScanner interface {
	Scan(dest ...any) error
}

func (s *Store) CreateRunForJob(ctx context.Context, jobID uuid.UUID) (uuid.UUID, int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	attempt, err := nextAttemptTx(ctx, tx, jobID)
	if err != nil {
		return uuid.Nil, 0, err
	}

	runID := uuid.New()
	_, err = tx.Exec(ctx, `
		insert into runs(run_id, job_id, attempt, state)
		values ($1,$2,$3,'CREATED')
	`, runID, jobID, attempt)
	if err != nil {
		return uuid.Nil, 0, err
	}

	_, _ = tx.Exec(ctx, `insert into events(job_id, run_id, type, payload) values ($1,$2,'RUN_CREATED','{}'::jsonb)`, jobID, runID)

	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, 0, err
	}
	return runID, attempt, nil
}

func (s *Store) SetJobScheduled(ctx context.Context, jobID uuid.UUID, runID uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `update jobs set state='SCHEDULED' where job_id=$1`, jobID)
	if err != nil {
		return err
	}
	_, _ = s.pool.Exec(ctx, `insert into events(job_id, run_id, type, payload) values ($1,$2,'JOB_SCHEDULED','{}'::jsonb)`, jobID, runID)
	return nil
}

func (s *Store) MarkRunFailedOnly(ctx context.Context, runID uuid.UUID, reason string) error {
	_, err := s.pool.Exec(ctx, `
		update runs
		set state='FAILED', finished_at=now(), error_reason=$2
		where run_id=$1
	`, runID, reason)
	return err
}

func (s *Store) MarkRunSucceededOnly(ctx context.Context, runID uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `
		update runs
		set state='SUCCEEDED', finished_at=now()
		where run_id=$1
	`, runID)
	return err
}

func (s *Store) MarkJobSucceeded(ctx context.Context, jobID uuid.UUID, runID uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `update jobs set state='SUCCEEDED' where job_id=$1`, jobID)
	if err != nil {
		return err
	}
	_, _ = s.pool.Exec(ctx, `insert into events(job_id, run_id, type, payload) values ($1,$2,'JOB_SUCCEEDED','{}'::jsonb)`, jobID, runID)
	return nil
}

func (s *Store) MustHaveJob(ctx context.Context, jobID uuid.UUID) error {
	var x uuid.UUID
	if err := s.pool.QueryRow(ctx, `select job_id from jobs where job_id=$1`, jobID).Scan(&x); err != nil {
		return fmt.Errorf("job not found: %w", err)
	}
	return nil
}
