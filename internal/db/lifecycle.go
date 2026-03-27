package db

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

func (s *Store) MarkRunDispatched(ctx context.Context, runID uuid.UUID, k8sJobName string) error {
	_, err := s.pool.Exec(ctx, `
		update runs
		set state='STARTING', k8s_job_name=$2
		where run_id=$1
	`, runID, k8sJobName)
	if err != nil {
		return err
	}

	_, _ = s.pool.Exec(ctx, `
		insert into events(run_id, type, payload)
		values ($1,'POD_CREATED', jsonb_build_object('k8sJobName', $2))
	`, runID, k8sJobName)

	return nil
}

func (s *Store) MarkRunRunning(ctx context.Context, jobID, runID uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `
		update runs
		set state='RUNNING', started_at=coalesce(started_at, now()), heartbeat_at=now()
		where run_id=$1
	`, runID)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		update jobs
		set state='RUNNING'
		where job_id=$1 and state in ('SCHEDULED','PENDING')
	`, jobID)
	if err != nil {
		return err
	}
	_, _ = s.pool.Exec(ctx, `
		insert into events(job_id, run_id, type, payload)
		values ($1,$2,'POD_RUNNING','{}'::jsonb)
	`, jobID, runID)
	return nil
}

func (s *Store) MarkRunSucceeded(ctx context.Context, jobID, runID uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `
		update runs
		set state='SUCCEEDED', finished_at=now()
		where run_id=$1
	`, runID)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		update jobs
		set state='SUCCEEDED'
		where job_id=$1
	`, jobID)
	if err != nil {
		return err
	}
	_, _ = s.pool.Exec(ctx, `
		insert into events(job_id, run_id, type, payload)
		values ($1,$2,'JOB_SUCCEEDED','{}'::jsonb)
	`, jobID, runID)
	return nil
}

func (s *Store) MarkRunFailed(ctx context.Context, jobID, runID uuid.UUID, reason string) error {
	payload, _ := json.Marshal(map[string]any{"reason": reason})

	_, err := s.pool.Exec(ctx, `
		update runs
		set state='FAILED', finished_at=now(), error_reason=$2
		where run_id=$1
	`, runID, reason)
	if err != nil {
		return err
	}

	// Day 4: mark job failed directly (Day 5 will add retries)
	_, err = s.pool.Exec(ctx, `
		update jobs
		set state='FAILED'
		where job_id=$1
	`, jobID)
	if err != nil {
		return err
	}

	_, _ = s.pool.Exec(ctx, `
		insert into events(job_id, run_id, type, payload)
		values ($1,$2,'JOB_FAILED',$3::jsonb)
	`, jobID, runID, string(payload))
	return nil
}

func (s *Store) ListActiveRuns(ctx context.Context, limit int) ([]struct {
	RunID      uuid.UUID
	JobID      uuid.UUID
	K8sJobName string
	State      RunState
	CreatedAt  time.Time
}, error) {
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

	out := make([]struct {
		RunID      uuid.UUID
		JobID      uuid.UUID
		K8sJobName string
		State      RunState
		CreatedAt  time.Time
	}, 0)

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
