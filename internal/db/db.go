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
	Image     string            `json:"image"`
	Command   []string          `json:"command,omitempty"`
	Args      []string          `json:"args,omitempty"`
	Env       map[string]string `json:"env,omitempty"`
	Resources struct {
		CPU    string `json:"cpu,omitempty"`
		Memory string `json:"memory,omitempty"`
	} `json:"resources,omitempty"`
	Queue      string `json:"queue,omitempty"`
	Priority   int    `json:"priority,omitempty"`
	MaxRetries int    `json:"max_retries,omitempty"`
}

type Job struct {
	JobID      uuid.UUID `json:"job_id"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
	State      JobState  `json:"state"`
	Spec       JobSpec   `json:"spec"`
	Queue      string    `json:"queue"`
	Priority   int       `json:"priority"`
	MaxRetries int       `json:"max_retries"`
}

type Run struct {
	RunID   uuid.UUID `json:"run_id"`
	JobID   uuid.UUID `json:"job_id"`
	Attempt int       `json:"attempt"`
	State   RunState  `json:"state"`
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
		insert into events(job_id, type, payload) values ($1, 'JOB_CREATED', '{}'::jsonb)
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

func (s *Store) GetJob(ctx context.Context, id uuid.UUID) (Job, error) {
	row := s.pool.QueryRow(ctx, `
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
        and next_run_at <= now()   
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
