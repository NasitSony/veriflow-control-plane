package db

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

////////////////////////////////////////////////////////////////////////////////
// ⏱ BACKOFF LOGIC
////////////////////////////////////////////////////////////////////////////////

// Exponential backoff based on attempt number
func backoffForAttempt(attempt int) time.Duration {

	// attempt starts at 1
	// attempt=1 → 2s
	// attempt=2 → 4s
	// attempt=3 → 8s ...
	// capped at 60s

	secs := 1 << uint(min(attempt, 6)) // 2,4,8,16,32,64

	if secs > 60 {
		secs = 60
	}

	return time.Duration(secs) * time.Second
}

// helper
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

////////////////////////////////////////////////////////////////////////////////
// 🔁 RETRY DECISION ENGINE (MOST IMPORTANT FUNCTION)
////////////////////////////////////////////////////////////////////////////////

// Decides:
// - retry OR
// - permanently fail
func (s *Store) ScheduleRetryOrFail(ctx context.Context, jobID, runID uuid.UUID, reason string) error {

	// Start DB transaction (important for correctness)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	///////////////////////////////////////////////////////////
	// STEP 1: Get current attempt number
	///////////////////////////////////////////////////////////
	var attempt int
	if err := tx.QueryRow(ctx, `
		select attempt from runs where run_id=$1
	`, runID).Scan(&attempt); err != nil {
		return err
	}

	///////////////////////////////////////////////////////////
	// STEP 2: Get max retries allowed
	///////////////////////////////////////////////////////////
	var maxRetries int
	if err := tx.QueryRow(ctx, `
		select max_retries from jobs where job_id=$1
	`, jobID).Scan(&maxRetries); err != nil {
		return err
	}

	///////////////////////////////////////////////////////////
	// STEP 3: Get latest checkpoint (for resume)
	///////////////////////////////////////////////////////////
	var latestCheckpointURI string
	if err := tx.QueryRow(ctx, `
		select coalesce(latest_checkpoint_uri, '')
		from jobs
		where job_id=$1
	`, jobID).Scan(&latestCheckpointURI); err != nil {
		return err
	}

	log.Printf("schedule retry job_id=%s run_id=%s attempt=%d max_retries=%d checkpoint=%q reason=%s",
		jobID, runID, attempt, maxRetries, latestCheckpointURI, reason)

	///////////////////////////////////////////////////////////
	// STEP 4: Decide if retry is allowed
	///////////////////////////////////////////////////////////

	// total attempts allowed = 1 + maxRetries
	if attempt < 1+maxRetries {

		// compute delay (exponential backoff)
		delay := backoffForAttempt(attempt)

		// compute next run time
		next := time.Now().UTC().Add(delay)

		///////////////////////////////////////////////////////
		// STEP 5: Reset job → PENDING (for retry)
		///////////////////////////////////////////////////////

		if latestCheckpointURI != "" {
			// resume from checkpoint
			_, err = tx.Exec(ctx, `
				update jobs
				set state='PENDING',
				    next_run_at=$2,
				    spec = jsonb_set(spec, '{checkpointUri}', to_jsonb($3::text), true)
				where job_id=$1
			`, jobID, next, latestCheckpointURI)
		} else {
			// no checkpoint → normal retry
			_, err = tx.Exec(ctx, `
				update jobs
				set state='PENDING', next_run_at=$2
				where job_id=$1
			`, jobID, next)
		}

		if err != nil {
			return err
		}

		///////////////////////////////////////////////////////
		// STEP 6: Emit RETRY event
		///////////////////////////////////////////////////////

		_, err = tx.Exec(ctx, `
			insert into events(job_id, run_id, type, payload)
			values ($1,$2,'RETRY_TRIGGERED',
				jsonb_build_object(
					'next_run_at', to_jsonb($3::timestamptz),
					'delay_seconds', to_jsonb($4::int),
					'attempt', to_jsonb($5::int),
					'job_state', 'PENDING',
					'run_state', 'FAILED',
					'reason', to_jsonb($6::text),
					'checkpoint_uri', to_jsonb($7::text)
				)
			)
		`, jobID, runID, next, int(delay.Seconds()), attempt, reason, latestCheckpointURI)

		if err != nil {
			return fmt.Errorf("insert RETRY_TRIGGERED event: %w", err)
		}

		log.Printf("retry queued job_id=%s run_id=%s next_run_at=%s checkpoint=%q",
			jobID, runID, next.Format(time.RFC3339), latestCheckpointURI)

		return tx.Commit(ctx)
	}

	///////////////////////////////////////////////////////////
	// STEP 7: NO RETRIES LEFT → FINAL FAILURE
	///////////////////////////////////////////////////////////

	_, err = tx.Exec(ctx, `
		update jobs set state='FAILED' where job_id=$1
	`, jobID)

	if err != nil {
		return err
	}

	// emit final failure event
	_, err = tx.Exec(ctx, `
		insert into events(job_id, run_id, type, payload)
		values ($1,$2,'JOB_FAILED',
			jsonb_build_object(
				'final', true,
				'attempt', to_jsonb($3::int),
				'max_retries', to_jsonb($4::int),
				'reason', to_jsonb($5::text)
			)
		)
	`, jobID, runID, attempt, maxRetries, reason)

	if err != nil {
		return fmt.Errorf("insert JOB_FAILED event: %w", err)
	}

	return tx.Commit(ctx)
}

////////////////////////////////////////////////////////////////////////////////
// 🔁 ATTEMPT COUNTER
////////////////////////////////////////////////////////////////////////////////

// Calculates next attempt number safely inside transaction
func nextAttemptTx(ctx context.Context, tx pgx.Tx, jobID uuid.UUID) (int, error) {
	var next int
	err := tx.QueryRow(ctx, `
		select coalesce(max(attempt),0)+1
		from runs
		where job_id=$1
	`, jobID).Scan(&next)
	return next, err
}

////////////////////////////////////////////////////////////////////////////////
// 🚀 CREATE RUN (RETRY OR INITIAL)
////////////////////////////////////////////////////////////////////////////////

func (s *Store) CreateRunForJob(ctx context.Context, jobID uuid.UUID) (uuid.UUID, int, error) {

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// get next attempt
	attempt, err := nextAttemptTx(ctx, tx, jobID)
	if err != nil {
		return uuid.Nil, 0, err
	}

	runID := uuid.New()

	// insert new run
	_, err = tx.Exec(ctx, `
		insert into runs(run_id, job_id, attempt, state)
		values ($1,$2,$3,'CREATED')
	`, runID, jobID, attempt)

	// emit event
	_, _ = tx.Exec(ctx, `
		insert into events(job_id, run_id, type, payload)
		values ($1,$2,'RUN_CREATED','{}')
	`, jobID, runID)

	tx.Commit(ctx)
	return runID, attempt, nil
}

////////////////////////////////////////////////////////////////////////////////
// 🧱 SMALL HELPERS
////////////////////////////////////////////////////////////////////////////////

// mark job scheduled
func (s *Store) SetJobScheduled(ctx context.Context, jobID uuid.UUID, runID uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `update jobs set state='SCHEDULED' where job_id=$1`, jobID)
	_, _ = s.pool.Exec(ctx, `insert into events(job_id, run_id, type, payload) values ($1,$2,'JOB_SCHEDULED','{}')`, jobID, runID)
	return err
}

// mark only run failed (job not touched)
func (s *Store) MarkRunFailedOnly(ctx context.Context, runID uuid.UUID, reason string) error {
	_, err := s.pool.Exec(ctx, `
		update runs
		set state='FAILED', finished_at=now(), error_reason=$2
		where run_id=$1
	`, runID, reason)
	return err
}

// mark only run succeeded
func (s *Store) MarkRunSucceededOnly(ctx context.Context, runID uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `
		update runs
		set state='SUCCEEDED', finished_at=now()
		where run_id=$1
	`, runID)
	return err
}

// mark job succeeded
func (s *Store) MarkJobSucceeded(ctx context.Context, jobID uuid.UUID, runID uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `update jobs set state='SUCCEEDED' where job_id=$1`, jobID)
	_, _ = s.pool.Exec(ctx, `insert into events(job_id, run_id, type, payload) values ($1,$2,'JOB_SUCCEEDED','{}')`, jobID, runID)
	return err
}

// ensure job exists
func (s *Store) MustHaveJob(ctx context.Context, jobID uuid.UUID) error {
	var x uuid.UUID
	if err := s.pool.QueryRow(ctx, `select job_id from jobs where job_id=$1`, jobID).Scan(&x); err != nil {
		return fmt.Errorf("job not found: %w", err)
	}
	return nil
}
