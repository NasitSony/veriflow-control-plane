package db

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type JobState string

const (
	RunCreated   RunState = "CREATED"
	RunStarting  RunState = "STARTING"
	RunRunning   RunState = "RUNNING"
	RunSucceeded RunState = "SUCCEEDED"
	RunFailed    RunState = "FAILED"
)

type RunState string

type JobSpec struct {
	Image   string            `json:"image"`
	Command []string          `json:"command,omitempty"`
	Args    []string          `json:"args,omitempty"`
	Env     map[string]string `json:"env,omitempty"`

	Resources struct {
		CPU    string `json:"cpu,omitempty"`
		Memory string `json:"memory,omitempty"`
	} `json:"resources,omitempty"`

	Queue          string `json:"queue,omitempty"`
	Priority       int    `json:"priority,omitempty"`
	MaxRetries     int    `json:"max_retries,omitempty"`
	TimeoutSeconds int    `json:"timeoutSeconds,omitempty"`

	// 🔥 NEW AI FIELDS
	JobType       string `json:"jobType,omitempty"`  // training | batch-inference
	GPUCount      int    `json:"gpuCount,omitempty"` // number of GPUs
	DatasetURI    string `json:"datasetUri,omitempty"`
	CheckpointURI string `json:"checkpointUri,omitempty"`
	ArtifactURI   string `json:"artifactUri,omitempty"`
	Framework     string `json:"framework,omitempty"` // pytorch | custom
}

type Job struct {
	JobID      uuid.UUID  `json:"job_id"`
	CreatedAt  time.Time  `json:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at"`
	State      JobState   `json:"state"`
	Spec       JobSpec    `json:"spec"`
	Queue      string     `json:"queue"`
	Priority   int        `json:"priority"`
	MaxRetries int        `json:"max_retries"`
	NextRunAt  *time.Time `json:"nextRunAt,omitempty"`
}

type Run struct {
	RunID       uuid.UUID
	JobID       uuid.UUID
	Attempt     int
	State       RunState
	K8sJobName  string
	StartedAt   *time.Time
	FinishedAt  *time.Time
	ErrorReason *string
}

type Event struct {
	JobID     uuid.UUID
	RunID     *uuid.UUID
	Type      string
	Payload   []byte
	CreatedAt time.Time
}

type SystemSummary struct {
	TotalJobs            int `json:"totalJobs"`
	PendingJobs          int `json:"pendingJobs"`
	ScheduledJobs        int `json:"scheduledJobs"`
	RunningJobs          int `json:"runningJobs"`
	SucceededJobs        int `json:"succeededJobs"`
	FailedJobs           int `json:"failedJobs"`
	TotalRuns            int `json:"totalRuns"`
	RetryTriggeredEvents int `json:"retryTriggeredEvents"`
}

var ErrNotFound = errors.New("not found")

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// CreateJobIdempotent:
// - if idempotency_key exists: return existing job + replay=true
// - else: insert new job + replay=false
func (s *Store) CreateJobIdempotent(ctx context.Context, idemKey string, spec JobSpec) (job Job, replay bool, err error) {
	// normalize defaults
	queue := spec.Queue
	if queue == "" {
		queue = "default"
	}
	maxRetries := spec.MaxRetries
	if maxRetries < 0 || maxRetries > 10 {
		return Job{}, false, errors.New("spec.max_retries must be 0..10")
	}
	priority := spec.Priority

	spec.Queue = queue
	spec.Priority = priority
	spec.MaxRetries = maxRetries

	specJSON, err := json.Marshal(spec)
	if err != nil {
		return Job{}, false, err
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Job{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Check existing by idempotency key (stronger than relying on ON CONFLICT only for returning)
	var existingID uuid.UUID
	err = tx.QueryRow(ctx, `select job_id from jobs where idempotency_key=$1`, idemKey).Scan(&existingID)
	if err == nil {
		j, err2 := getJobByIDTx(ctx, tx, existingID)
		if err2 != nil {
			return Job{}, false, err2
		}
		if err := tx.Commit(ctx); err != nil {
			return Job{}, false, err
		}
		return j, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Job{}, false, err
	}

	jobID := uuid.New()

	_, err = tx.Exec(ctx, `
		insert into jobs(job_id, state, spec, queue, priority, max_retries, idempotency_key)
		values ($1, 'PENDING', $2, $3, $4, $5, $6)
	`, jobID, specJSON, queue, priority, maxRetries, idemKey)
	if err != nil {
		// if a race inserts same key, re-read
		var id uuid.UUID
		err2 := tx.QueryRow(ctx, `select job_id from jobs where idempotency_key=$1`, idemKey).Scan(&id)
		if err2 != nil {
			return Job{}, false, err
		}
		j, err3 := getJobByIDTx(ctx, tx, id)
		if err3 != nil {
			return Job{}, false, err3
		}
		if err := tx.Commit(ctx); err != nil {
			return Job{}, false, err
		}
		return j, true, nil
	}

	// event
	_, _ = tx.Exec(ctx, `
		insert into events(job_id, type, payload) values ($1, 'JOB_SUBMITTED', '{}'::jsonb)
	`, jobID)

	j, err := getJobByIDTx(ctx, tx, jobID)
	if err != nil {
		return Job{}, false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return Job{}, false, err
	}
	return j, false, nil
}

// ClaimNextPendingJob:
// Picks 1 pending job (highest priority, oldest first), locks it, moves to SCHEDULED,
// creates run attempt=1, writes events. Safe under concurrency due to SKIP LOCKED.
func (s *Store) ClaimNextPendingJob(ctx context.Context, queue string) (claimedJob Job, run Run, ok bool, err error) {
	if queue == "" {
		queue = "default"
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Job{}, Run{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Lock one pending job row without blocking other schedulers.
	row := tx.QueryRow(ctx, `
		select job_id
        from jobs
        where state='PENDING'
        and queue=$1
        and (next_run_at is null or next_run_at <= now())   
        order by priority desc, created_at asc
        for update skip locked
        limit 1
	`, queue)

	var jobID uuid.UUID
	if err := row.Scan(&jobID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Job{}, Run{}, false, nil
		}
		return Job{}, Run{}, false, err
	}

	// Update job -> SCHEDULED
	_, err = tx.Exec(ctx, `update jobs set state='SCHEDULED' where job_id=$1`, jobID)
	if err != nil {
		return Job{}, Run{}, false, err
	}

	// Create run attempt=1 (later: compute attempt = max(attempt)+1)
	runID := uuid.New()
	_, err = tx.Exec(ctx, `
    insert into runs(run_id, job_id, attempt, state)
    values ($1, $2,
    (select coalesce(max(attempt),0)+1 from runs where job_id=$2),
    'CREATED'
   )`, runID, jobID)
	if err != nil {
		return Job{}, Run{}, false, err
	}

	_, _ = tx.Exec(ctx, `insert into events(job_id, run_id, type, payload) values ($1,$2,'JOB_SCHEDULED','{}'::jsonb)`, jobID, runID)
	_, _ = tx.Exec(ctx, `insert into events(job_id, run_id, type, payload) values ($1,$2,'RUN_CREATED','{}'::jsonb)`, jobID, runID)

	j, err := getJobByIDTx(ctx, tx, jobID)
	if err != nil {
		return Job{}, Run{}, false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return Job{}, Run{}, false, err
	}

	return j, Run{RunID: runID, JobID: jobID, Attempt: 1, State: RunCreated}, true, nil
}

func getJobByIDTx(ctx context.Context, tx pgx.Tx, id uuid.UUID) (Job, error) {
	row := tx.QueryRow(ctx, `
		select job_id, created_at, updated_at, state, spec, queue, priority, max_retries
		from jobs where job_id=$1
	`, id)

	var j Job
	var specJSON []byte
	if err := row.Scan(&j.JobID, &j.CreatedAt, &j.UpdatedAt, &j.State, &specJSON, &j.Queue, &j.Priority, &j.MaxRetries); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Job{}, ErrNotFound
		}
		return Job{}, err
	}
	if err := json.Unmarshal(specJSON, &j.Spec); err != nil {
		return Job{}, err
	}
	return j, nil
}

func (s *Store) GetJob(ctx context.Context, jobID uuid.UUID) (Job, error) {
	var j Job
	err := s.pool.QueryRow(ctx, `
		select job_id, state, queue, priority, max_retries, next_run_at, spec, created_at
		from jobs
		where job_id=$1
	`, jobID).Scan(
		&j.JobID,
		&j.State,
		&j.Queue,
		&j.Priority,
		&j.MaxRetries,
		&j.NextRunAt,
		&j.Spec,
		&j.CreatedAt,
	)
	return j, err
}

func (s *Store) ListRunsForJob(ctx context.Context, jobID uuid.UUID) ([]Run, error) {
	rows, err := s.pool.Query(ctx, `
		select run_id, job_id, attempt, state, k8s_job_name, started_at, finished_at, error_reason
		from runs
		where job_id=$1
		order by created_at asc
	`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Run
	for rows.Next() {
		var r Run
		if err := rows.Scan(
			&r.RunID,
			&r.JobID,
			&r.Attempt,
			&r.State,
			&r.K8sJobName,
			&r.StartedAt,
			&r.FinishedAt,
			&r.ErrorReason,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) ListEventsForJob(ctx context.Context, jobID uuid.UUID, limit int) ([]Event, error) {
	rows, err := s.pool.Query(ctx, `
		select job_id, run_id, type, payload, ts
		from events
		where job_id=$1
		order by ts asc
		limit $2
	`, jobID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(
			&e.JobID,
			&e.RunID,
			&e.Type,
			&e.Payload,
			&e.CreatedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) AddJobEvent(ctx context.Context, jobID uuid.UUID, runID *uuid.UUID, eventType string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	if runID != nil {
		_, err = s.pool.Exec(ctx, `
			insert into events(job_id, run_id, type, payload)
			values ($1,$2,$3,$4::jsonb)
		`, jobID, *runID, eventType, string(data))
		return err
	}

	_, err = s.pool.Exec(ctx, `
		insert into events(job_id, type, payload)
		values ($1,$2,$3::jsonb)
	`, jobID, eventType, string(data))
	return err
}

func (s *Store) GetSystemSummary(ctx context.Context) (SystemSummary, error) {
	var out SystemSummary

	err := s.pool.QueryRow(ctx, `
		select
			(select count(*) from jobs),
			(select count(*) from jobs where state='PENDING'),
			(select count(*) from jobs where state='SCHEDULED'),
			(select count(*) from jobs where state='RUNNING'),
			(select count(*) from jobs where state='SUCCEEDED'),
			(select count(*) from jobs where state='FAILED'),
			(select count(*) from runs),
			(select count(*) from events where type='RETRY_TRIGGERED')
	`).Scan(
		&out.TotalJobs,
		&out.PendingJobs,
		&out.ScheduledJobs,
		&out.RunningJobs,
		&out.SucceededJobs,
		&out.FailedJobs,
		&out.TotalRuns,
		&out.RetryTriggeredEvents,
	)

	return out, err
}

func (s *Store) ListRecentEvents(ctx context.Context, limit int) ([]Event, error) {
	rows, err := s.pool.Query(ctx, `
		select job_id, run_id, type, payload, ts
		from events
		order by ts desc
		limit $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(
			&e.JobID,
			&e.RunID,
			&e.Type,
			&e.Payload,
			&e.CreatedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) GetActiveGPUUsageByNode(ctx context.Context) (map[string]int, error) {
	rows, err := s.pool.Query(ctx, `
		select
			coalesce(e.payload->>'node', '') as node_name,
			coalesce((e.payload->>'gpu_needed')::int, 0) as gpu_needed
		from runs r
		join lateral (
			select payload
			from events
			where run_id = r.run_id
			  and type = 'PLACEMENT_SELECTED'
			order by ts desc
			limit 1
		) e on true
		where r.state in ('CREATED','STARTING','RUNNING')
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]int{}
	for rows.Next() {
		var node string
		var used int
		if err := rows.Scan(&node, &used); err != nil {
			return nil, err
		}
		if node != "" {
			out[node] += used
		}
	}
	return out, rows.Err()
}

func (s *Store) ListTimedOutRuns(ctx context.Context) ([]struct {
	RunID          uuid.UUID
	JobID          uuid.UUID
	TimeoutSeconds int
	StartedAt      *time.Time
}, error) {
	rows, err := s.pool.Query(ctx, `
		select r.run_id, r.job_id, coalesce((j.spec->>'timeoutSeconds')::int, 0) as timeout_seconds, r.started_at
		from runs r
		join jobs j on j.job_id = r.job_id
		where r.state in ('STARTING','RUNNING')
		  and r.started_at is not null
		  and coalesce((j.spec->>'timeoutSeconds')::int, 0) > 0
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]struct {
		RunID          uuid.UUID
		JobID          uuid.UUID
		TimeoutSeconds int
		StartedAt      *time.Time
	}, 0)

	for rows.Next() {
		var r struct {
			RunID          uuid.UUID
			JobID          uuid.UUID
			TimeoutSeconds int
			StartedAt      *time.Time
		}
		if err := rows.Scan(&r.RunID, &r.JobID, &r.TimeoutSeconds, &r.StartedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) ListHeartbeatStaleRuns(ctx context.Context, staleAfter time.Duration) ([]struct {
	RunID       uuid.UUID
	JobID       uuid.UUID
	HeartbeatAt *time.Time
	State       RunState
}, error) {
	rows, err := s.pool.Query(ctx, `
		select run_id, job_id, heartbeat_at, state
		from runs
		where state in ('RUNNING')
		  and heartbeat_at is not null
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]struct {
		RunID       uuid.UUID
		JobID       uuid.UUID
		HeartbeatAt *time.Time
		State       RunState
	}, 0)

	cutoff := time.Now().Add(-staleAfter)

	for rows.Next() {
		var r struct {
			RunID       uuid.UUID
			JobID       uuid.UUID
			HeartbeatAt *time.Time
			State       RunState
		}
		if err := rows.Scan(&r.RunID, &r.JobID, &r.HeartbeatAt, &r.State); err != nil {
			return nil, err
		}
		if r.HeartbeatAt != nil && r.HeartbeatAt.Before(cutoff) {
			out = append(out, r)
		}
	}
	return out, rows.Err()
}

func (s *Store) RunAlreadyDispatched(ctx context.Context, runID uuid.UUID) (bool, string, error) {
	var k8sJobName *string
	err := s.pool.QueryRow(ctx, `
		select k8s_job_name
		from runs
		where run_id=$1
	`, runID).Scan(&k8sJobName)
	if err != nil {
		return false, "", err
	}
	if k8sJobName != nil && *k8sJobName != "" {
		return true, *k8sJobName, nil
	}
	return false, "", nil
}
