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

////////////////////////////////////////////////////////////////////////////////
// 🧠 STATE DEFINITIONS
////////////////////////////////////////////////////////////////////////////////

// JobState represents lifecycle of a JOB (long-lived logical unit)
type JobState string

// RunState represents lifecycle of a RUN (one execution attempt)
type RunState string

// Run lifecycle states
const (
	RunCreated   RunState = "CREATED"   // run created but not started
	RunStarting  RunState = "STARTING"  // dispatched, about to run
	RunRunning   RunState = "RUNNING"   // actively running
	RunSucceeded RunState = "SUCCEEDED" // completed successfully
	RunFailed    RunState = "FAILED"    // failed
)

////////////////////////////////////////////////////////////////////////////////
// 📦 JOB SPEC (WHAT TO RUN)
////////////////////////////////////////////////////////////////////////////////

// JobSpec is the user-provided specification of a job.
// This drives scheduling, execution, and retry behavior.
type JobSpec struct {
	Image   string            `json:"image"`             // container image
	Command []string          `json:"command,omitempty"` // entrypoint override
	Args    []string          `json:"args,omitempty"`
	Env     map[string]string `json:"env,omitempty"`

	// CPU / memory (not heavily used yet)
	Resources struct {
		CPU    string `json:"cpu,omitempty"`
		Memory string `json:"memory,omitempty"`
	} `json:"resources,omitempty"`

	// Scheduling policy
	Queue          string `json:"queue,omitempty"` // queue name
	Priority       int    `json:"priority,omitempty"`
	MaxRetries     int    `json:"max_retries,omitempty"`
	TimeoutSeconds int    `json:"timeoutSeconds,omitempty"`

	// 🔥 AI / GPU-specific fields
	JobType        string `json:"jobType,omitempty"`  // training | batch-inference | evaluation
	GPUCount       int    `json:"gpuCount,omitempty"` // required GPUs
	DatasetURI     string `json:"datasetUri,omitempty"`
	CheckpointURI  string `json:"checkpointUri,omitempty"`
	ArtifactURI    string `json:"artifactUri,omitempty"`
	Framework      string `json:"framework,omitempty"`
	GPUType        string `json:"gpuType,omitempty"`        // e.g., A100
	MinGPUMemoryMB int    `json:"minGpuMemoryMb,omitempty"` // constraint
}

////////////////////////////////////////////////////////////////////////////////
// 📊 CORE OBJECTS
////////////////////////////////////////////////////////////////////////////////

// Job = logical unit of work (persists across retries)
type Job struct {
	JobID               uuid.UUID  `json:"job_id"`
	CreatedAt           time.Time  `json:"created_at"`
	UpdatedAt           time.Time  `json:"updated_at"`
	State               JobState   `json:"state"` // PENDING → SCHEDULED → RUNNING → DONE
	Spec                JobSpec    `json:"spec"`
	Queue               string     `json:"queue"`
	Priority            int        `json:"priority"`
	MaxRetries          int        `json:"max_retries"`
	NextRunAt           *time.Time `json:"nextRunAt,omitempty"`
	LatestCheckpointURI string     `json:"latestCheckpointUri"` // used for resume
}

// Run = one execution attempt of a Job
type Run struct {
	RunID       uuid.UUID
	JobID       uuid.UUID
	Attempt     int      // retry number
	State       RunState // CREATED → RUNNING → SUCCEEDED/FAILED
	K8sJobName  string   // link to Kubernetes job
	StartedAt   *time.Time
	FinishedAt  *time.Time
	ErrorReason *string
}

// Event = audit log entry for everything happening in system
type Event struct {
	JobID     uuid.UUID
	RunID     *uuid.UUID
	Type      string
	Payload   []byte
	CreatedAt time.Time
}

////////////////////////////////////////////////////////////////////////////////
// 🏗 STORE
////////////////////////////////////////////////////////////////////////////////

// Store wraps PostgreSQL connection pool
type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

////////////////////////////////////////////////////////////////////////////////
// 🔥 CREATE JOB (IDEMPOTENT)
////////////////////////////////////////////////////////////////////////////////

// CreateJobIdempotent ensures duplicate submissions don't create new jobs
func (s *Store) CreateJobIdempotent(ctx context.Context, idemKey string, spec JobSpec) (job Job, replay bool, err error) {

	// Normalize defaults
	queue := spec.Queue
	if queue == "" {
		queue = "default"
	}
	maxRetries := spec.MaxRetries
	if maxRetries < 0 || maxRetries > 10 {
		return Job{}, false, errors.New("spec.max_retries must be 0..10")
	}

	spec.Queue = queue
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

	// 🔁 Check if job already exists (idempotency)
	var existingID uuid.UUID
	err = tx.QueryRow(ctx, `select job_id from jobs where idempotency_key=$1`, idemKey).Scan(&existingID)

	if err == nil {
		j, _ := getJobByIDTx(ctx, tx, existingID)
		_ = tx.Commit(ctx)
		return j, true, nil // replay
	}

	jobID := uuid.New()

	// Insert new job
	_, err = tx.Exec(ctx, `
		insert into jobs(job_id, state, spec, queue, priority, max_retries, idempotency_key)
		values ($1, 'PENDING', $2, $3, $4, $5, $6)
	`, jobID, specJSON, queue, spec.Priority, maxRetries, idemKey)

	// Emit event
	_, _ = tx.Exec(ctx, `
		insert into events(job_id, type, payload)
		values ($1, 'JOB_SUBMITTED', '{}'::jsonb)
	`, jobID)

	j, _ := getJobByIDTx(ctx, tx, jobID)
	_ = tx.Commit(ctx)

	return j, false, nil
}

////////////////////////////////////////////////////////////////////////////////
// 🔥 CLAIM JOB (SCHEDULER ENTRY POINT)
////////////////////////////////////////////////////////////////////////////////

// ClaimNextPendingJob picks ONE job safely across multiple schedulers
func (s *Store) ClaimNextPendingJob(ctx context.Context, queue string) (Job, Run, bool, error) {

	tx, _ := s.pool.BeginTx(ctx, pgx.TxOptions{})
	defer func() { _ = tx.Rollback(ctx) }()

	// 🔥 CRITICAL: SKIP LOCKED prevents race between schedulers
	row := tx.QueryRow(ctx, `
		select job_id
		from jobs
		where state='PENDING'
		and queue=$1
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

	// Move job → SCHEDULED
	tx.Exec(ctx, `update jobs set state='SCHEDULED' where job_id=$1`, jobID)

	// Create new run (attempt++)
	runID := uuid.New()
	tx.Exec(ctx, `
		insert into runs(run_id, job_id, attempt, state)
		values ($1, $2,
		(select coalesce(max(attempt),0)+1 from runs where job_id=$2),
		'CREATED'
	)`, runID, jobID)

	// Emit events
	tx.Exec(ctx, `insert into events(job_id, run_id, type, payload) values ($1,$2,'JOB_SCHEDULED','{}')`, jobID, runID)
	tx.Exec(ctx, `insert into events(job_id, run_id, type, payload) values ($1,$2,'RUN_CREATED','{}')`, jobID, runID)

	j, _ := getJobByIDTx(ctx, tx, jobID)
	tx.Commit(ctx)

	return j, Run{RunID: runID, JobID: jobID, Attempt: 1, State: RunCreated}, true, nil
}

////////////////////////////////////////////////////////////////////////////////
// 📊 EVENTS
////////////////////////////////////////////////////////////////////////////////

// AddJobEvent writes event to event log
func (s *Store) AddJobEvent(ctx context.Context, jobID uuid.UUID, runID *uuid.UUID, eventType string, payload any) error {
	data, _ := json.Marshal(payload)

	if runID != nil {
		_, err := s.pool.Exec(ctx, `
			insert into events(job_id, run_id, type, payload)
			values ($1,$2,$3,$4::jsonb)
		`, jobID, *runID, eventType, string(data))
		return err
	}

	_, err := s.pool.Exec(ctx, `
		insert into events(job_id, type, payload)
		values ($1,$2,$3::jsonb)
	`, jobID, eventType, string(data))
	return err
}

////////////////////////////////////////////////////////////////////////////////
// 🔁 RETRY / FAILURE HELPERS (USED BY SCHEDULER)
////////////////////////////////////////////////////////////////////////////////

// RunAlreadyDispatched prevents duplicate execution
func (s *Store) RunAlreadyDispatched(ctx context.Context, runID uuid.UUID) (bool, string, error) {
	var name *string
	s.pool.QueryRow(ctx, `select k8s_job_name from runs where run_id=$1`, runID).Scan(&name)

	if name != nil && *name != "" {
		return true, *name, nil
	}
	return false, "", nil
}

// UpdateLatestCheckpointURI enables resume
func (s *Store) UpdateLatestCheckpointURI(ctx context.Context, jobID uuid.UUID, checkpointURI string) error {
	_, err := s.pool.Exec(ctx, `
		update jobs set latest_checkpoint_uri=$2 where job_id=$1
	`, jobID, checkpointURI)
	return err
}

////////////////////////////////////////////////////////////////////////////////
// 📊 GPU / SYSTEM STATE
////////////////////////////////////////////////////////////////////////////////

// CurrentGPUUsageByQueue supports quota enforcement
func (s *Store) CurrentGPUUsageByQueue(ctx context.Context) (map[string]int, error) {
	rows, _ := s.pool.Query(ctx, `
		select queue, coalesce(sum((spec->>'gpuCount')::int), 0)
		from jobs
		where state in ('SCHEDULED','RUNNING')
		group by queue
	`)
	defer rows.Close()

	out := map[string]int{}
	for rows.Next() {
		var q string
		var used int
		rows.Scan(&q, &used)
		out[q] = used
	}
	return out, nil
}
