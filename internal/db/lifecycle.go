package db

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

////////////////////////////////////////////////////////////////////////////////
// 🚀 MARK RUN DISPATCHED (Scheduler → K8s bridge)
////////////////////////////////////////////////////////////////////////////////

// Called right after Kubernetes Job is created
// Moves run from CREATED → STARTING
func (s *Store) MarkRunDispatched(ctx context.Context, runID uuid.UUID, k8sJobName string) error {

	// Update run state to STARTING and store K8s job name
	_, err := s.pool.Exec(ctx, `
		update runs
		set state='STARTING', k8s_job_name=$2
		where run_id=$1
	`, runID, k8sJobName)
	if err != nil {
		return err
	}

	// Emit event for observability
	// This represents: "Pod / Job has been created in Kubernetes"
	_, _ = s.pool.Exec(ctx, `
		insert into events(run_id, type, payload)
		values ($1,'POD_CREATED', jsonb_build_object('k8sJobName', $2))
	`, runID, k8sJobName)

	return nil
}

////////////////////////////////////////////////////////////////////////////////
// 🏃 MARK RUN RUNNING (Execution started)
////////////////////////////////////////////////////////////////////////////////

// Called when K8s shows the job is actually running
func (s *Store) MarkRunRunning(ctx context.Context, jobID, runID uuid.UUID) error {

	// Update run state to RUNNING
	// - started_at: set only once (coalesce prevents overwrite)
	// - heartbeat_at: updated every time we detect activity
	_, err := s.pool.Exec(ctx, `
		update runs
		set state='RUNNING', 
		    started_at=coalesce(started_at, now()), 
		    heartbeat_at=now()
		where run_id=$1
	`, runID)
	if err != nil {
		return err
	}

	// Also update job state → RUNNING (if not already)
	// Only transition from PENDING/SCHEDULED
	_, err = s.pool.Exec(ctx, `
		update jobs
		set state='RUNNING'
		where job_id=$1 and state in ('SCHEDULED','PENDING')
	`, jobID)
	if err != nil {
		return err
	}

	// Emit event
	_, _ = s.pool.Exec(ctx, `
		insert into events(job_id, run_id, type, payload)
		values ($1,$2,'POD_RUNNING','{}'::jsonb)
	`, jobID, runID)

	return nil
}

////////////////////////////////////////////////////////////////////////////////
// ✅ MARK RUN SUCCEEDED
////////////////////////////////////////////////////////////////////////////////

// Called when Kubernetes job completes successfully
func (s *Store) MarkRunSucceeded(ctx context.Context, jobID, runID uuid.UUID) error {

	// Mark run as SUCCEEDED
	_, err := s.pool.Exec(ctx, `
		update runs
		set state='SUCCEEDED', finished_at=now()
		where run_id=$1
	`, runID)
	if err != nil {
		return err
	}

	// Mark job as SUCCEEDED (final state)
	// No more retries after this
	_, err = s.pool.Exec(ctx, `
		update jobs
		set state='SUCCEEDED'
		where job_id=$1
	`, jobID)
	if err != nil {
		return err
	}

	// Emit event
	_, _ = s.pool.Exec(ctx, `
		insert into events(job_id, run_id, type, payload)
		values ($1,$2,'JOB_SUCCEEDED','{}'::jsonb)
	`, jobID, runID)

	return nil
}

////////////////////////////////////////////////////////////////////////////////
// ❌ MARK RUN FAILED
////////////////////////////////////////////////////////////////////////////////

// Called when failure detected (timeout, crash, etc.)
func (s *Store) MarkRunFailed(ctx context.Context, jobID, runID uuid.UUID, reason string) error {

	// Create JSON payload with failure reason
	payload, _ := json.Marshal(map[string]any{"reason": reason})

	// Mark run as FAILED
	_, err := s.pool.Exec(ctx, `
		update runs
		set state='FAILED', 
		    finished_at=now(), 
		    error_reason=$2
		where run_id=$1
	`, runID, reason)
	if err != nil {
		return err
	}

	// 🔴 Current version: immediately mark job FAILED
	// (later versions add retry logic instead of immediate failure)
	_, err = s.pool.Exec(ctx, `
		update jobs
		set state='FAILED'
		where job_id=$1
	`, jobID)
	if err != nil {
		return err
	}

	// Emit failure event
	_, _ = s.pool.Exec(ctx, `
		insert into events(job_id, run_id, type, payload)
		values ($1,$2,'JOB_FAILED',$3::jsonb)
	`, jobID, runID, string(payload))

	return nil
}

////////////////////////////////////////////////////////////////////////////////
// 🔄 LIST ACTIVE RUNS (Reconciler input)
////////////////////////////////////////////////////////////////////////////////

// Used by scheduler reconciler loop
// Returns runs that are still "alive"
func (s *Store) ListActiveRuns(ctx context.Context, limit int) ([]struct {
	RunID      uuid.UUID
	JobID      uuid.UUID
	K8sJobName string
	State      RunState
	CreatedAt  time.Time
}, error) {

	// Select runs that are:
	// - CREATED (not started yet)
	// - STARTING (K8s job created)
	// - RUNNING (actively executing)
	// AND have a Kubernetes job name (i.e., dispatched)
	rows, err := s.pool.Query(ctx, `
		select run_id, job_id, k8s_job_name, state, created_at
		from runs
		where state in ('CREATED','STARTING','RUNNING')
		  and k8s_job_name is not null
		order by created_at asc
		limit $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Result slice
	out := make([]struct {
		RunID      uuid.UUID
		JobID      uuid.UUID
		K8sJobName string
		State      RunState
		CreatedAt  time.Time
	}, 0)

	// Iterate rows and scan into struct
	for rows.Next() {
		var r struct {
			RunID      uuid.UUID
			JobID      uuid.UUID
			K8sJobName string
			State      RunState
			CreatedAt  time.Time
		}
		if err := rows.Scan(&r.RunID, &r.JobID, &r.K8sJobName, &r.State, &r.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}

	return out, rows.Err()
}
