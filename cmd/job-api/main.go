package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/NasitSony/veriflow/internal/db"
)

// apiError is the standard JSON error payload returned by the API.
type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func main() {
	// Read server address and database DSN from environment, with defaults.
	addr := envOr("ADDR", ":8080")
	dsn := envOr("DATABASE_URL", "postgres://veriflow:veriflow@localhost:5436/veriflow?sslmode=disable")

	// Create a root context that is cancelled on Ctrl+C or SIGTERM.
	// This allows graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Open PostgreSQL connection pool.
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("db pool: %v", err)
	}
	defer pool.Close()

	// Quick startup connectivity check so the API does not start in a broken state.
	if err := pingDB(ctx, pool); err != nil {
		log.Fatalf("db ping: %v", err)
	}

	// Build store abstraction over DB pool.
	store := db.NewStore(pool)

	// HTTP router.
	mux := http.NewServeMux()

	// Liveness endpoint:
	// answers whether the process is up.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"ok": true})
	})

	// Readiness endpoint:
	// answers whether the service is ready to serve traffic.
	// Current readiness rule: DB must be reachable quickly.
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		c, cancel := context.WithTimeout(r.Context(), 8*time.Second)
		defer cancel()

		if err := pingDB(c, pool); err != nil {
			writeJSON(w, 503, map[string]any{
				"ready": false,
				"error": err.Error(),
			})
			return
		}

		writeJSON(w, 200, map[string]any{"ready": true})
	})

	// POST /v1/jobs
	// Submits a new job into the system.
	// Uses Idempotency-Key header to avoid duplicate submissions.
	mux.HandleFunc("/v1/jobs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, 405, "method_not_allowed", "Use POST")
			return
		}

		// Require an idempotency key so repeated client retries do not create duplicate jobs.
		idemKey := r.Header.Get("Idempotency-Key")
		if strings.TrimSpace(idemKey) == "" {
			writeError(w, 400, "missing_idempotency_key", "Provide Idempotency-Key header")
			return
		}

		// Decode request body into JobSpec.
		var spec db.JobSpec
		if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
			writeError(w, 400, "invalid_json", "Invalid JSON body")
			return
		}

		// Basic required validation.
		if spec.Image == "" {
			writeError(w, 400, "invalid_spec", "spec.image is required")
			return
		}

		// For AI workloads, datasetUri is required.
		if (spec.JobType == "training" || spec.JobType == "batch-inference" || spec.JobType == "evaluation") && spec.DatasetURI == "" {
			writeError(w, 400, "invalid_spec", "datasetUri is required for AI workloads")
			return
		}

		// GPU count must not be negative.
		if spec.GPUCount < 0 {
			writeError(w, 400, "invalid_spec", "gpuCount must be >= 0")
			return
		}

		// Log submission intent for observability.
		log.Printf(
			"submit job request: type=%s gpu=%d dataset=%s artifact=%s priority=%d image=%s",
			spec.JobType,
			spec.GPUCount,
			spec.DatasetURI,
			spec.ArtifactURI,
			spec.Priority,
			spec.Image,
		)

		// Validate job type.
		validJobType := spec.JobType == "training" || spec.JobType == "batch-inference" || spec.JobType == "evaluation"
		if spec.JobType == "" {
			writeError(w, 400, "invalid_spec", "jobType is required")
			return
		}

		if !validJobType {
			writeError(w, 400, "invalid_spec", "jobType must be one of: training, batch-inference, evaluation")
			return
		}

		// Create the job idempotently:
		// - if this key was already used, return the existing job with replay=true
		// - otherwise create a new job
		job, replay, err := store.CreateJobIdempotent(r.Context(), idemKey, spec)
		if err != nil {
			writeError(w, 400, "create_failed", err.Error())
			return
		}

		// New job => 201 Created
		// Replay => 200 OK
		status := 201
		if replay {
			status = 200
		}

		writeJSON(w, status, map[string]any{
			"job":    job,
			"replay": replay,
		})
	})

	// GET /v1/jobs/{job_id}
	// Returns job metadata, runs, and event history.
	mux.HandleFunc("/v1/jobs/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeError(w, 405, "method_not_allowed", "Use GET")
			return
		}

		// Extract UUID from path.
		jobIDStr := strings.TrimPrefix(r.URL.Path, "/v1/jobs/")
		id, err := uuid.Parse(jobIDStr)
		if err != nil {
			writeError(w, 400, "invalid_job_id", "job_id must be uuid")
			return
		}

		// Load job object.
		job, err := store.GetJob(r.Context(), id)
		if err != nil {
			if errors.Is(err, db.ErrNotFound) {
				writeError(w, 404, "not_found", "Job not found")
				return
			}
			writeError(w, 500, "db_error", err.Error())
			return
		}

		// Load all runs for the job.
		runs, err := store.ListRunsForJob(r.Context(), id)
		if err != nil {
			writeError(w, 500, "runs_error", err.Error())
			return
		}

		// Load recent events for the job.
		events, err := store.ListEventsForJob(r.Context(), id, 50)
		if err != nil {
			writeError(w, 500, "events_error", err.Error())
			return
		}

		// Return a combined job view.
		writeJSON(w, 200, map[string]any{
			"job":    job,
			"runs":   runs,
			"events": events,
		})
	})

	// GET /v1/system/scheduler
	// Returns scheduler status file, if available.
	mux.HandleFunc("/v1/system/scheduler", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeError(w, 405, "method_not_allowed", "Use GET")
			return
		}

		// Scheduler writes status JSON to this path.
		statusPath := envOr("SCHED_STATUS_PATH", "/tmp/veriflow-scheduler-status.json")

		// Read the scheduler snapshot file.
		data, err := os.ReadFile(statusPath)
		if err != nil {
			// If file is absent/unavailable, return a soft response instead of failing hard.
			writeJSON(w, 200, map[string]any{
				"scheduler": "not_available",
			})
			return
		}

		// This second check is redundant in current code because the previous err branch returns.
		// Kept here because it exists in the original file.
		if err != nil {
			writeError(w, 500, "status_read_error", err.Error())
			return
		}

		// Parse JSON into a generic map for passthrough response.
		var status map[string]any
		if err := json.Unmarshal(data, &status); err != nil {
			writeError(w, 500, "status_parse_error", err.Error())
			return
		}

		writeJSON(w, 200, map[string]any{
			"scheduler": status,
		})
	})

	// GET /v1/system/summary
	// Returns high-level system summary plus recent events.
	mux.HandleFunc("/v1/system/summary", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeError(w, 405, "method_not_allowed", "Use GET")
			return
		}

		// Fetch aggregate counts across jobs/runs/events.
		summary, err := store.GetSystemSummary(r.Context())
		if err != nil {
			writeError(w, 500, "summary_error", err.Error())
			return
		}

		// Fetch recent events for dashboard-style visibility.
		events, err := store.ListRecentEvents(r.Context(), 20)
		if err != nil {
			writeError(w, 500, "recent_events_error", err.Error())
			return
		}

		writeJSON(w, 200, map[string]any{
			"summary":      summary,
			"recentEvents": events,
		})
	})

	// Build HTTP server.
	srv := &http.Server{
		Addr:              addr,
		Handler:           logMiddleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Start server in background goroutine.
	go func() {
		log.Printf("job-api listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	}()

	// Wait for shutdown signal.
	<-ctx.Done()
	log.Printf("shutdown signal received")

	// Graceful shutdown with timeout.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	_ = srv.Shutdown(shutdownCtx)
	log.Printf("bye")
}

// pingDB checks whether the database is reachable within a bounded timeout.
func pingDB(ctx context.Context, pool *pgxpool.Pool) error {
	c, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	return pool.Ping(c)
}

// logMiddleware logs method, path, and request duration for every HTTP request.
func logMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start))
	})
}

// writeError wraps an API error into standard JSON format.
func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]any{
		"error": apiError{
			Code:    code,
			Message: msg,
		},
	})
}

// writeJSON writes any value as JSON with a given HTTP status.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// envOr returns environment variable value if set, else default.
func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
