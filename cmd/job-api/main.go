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

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func main() {
	addr := envOr("ADDR", ":8080")
	dsn := envOr("DATABASE_URL", "postgres://veriflow:veriflow@localhost:5436/veriflow?sslmode=disable")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("db pool: %v", err)
	}
	defer pool.Close()

	// quick connectivity check
	if err := pingDB(ctx, pool); err != nil {
		log.Fatalf("db ping: %v", err)
	}

	store := db.NewStore(pool)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"ok": true})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		// Day 2 readiness = can ping DB quickly
		c, cancel := context.WithTimeout(r.Context(), 8*time.Second)
		defer cancel()
		if err := pingDB(c, pool); err != nil {
			writeJSON(w, 503, map[string]any{"ready": false, "error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]any{"ready": true})
	})

	mux.HandleFunc("/v1/jobs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, 405, "method_not_allowed", "Use POST")
			return
		}

		idemKey := r.Header.Get("Idempotency-Key")
		if strings.TrimSpace(idemKey) == "" {
			writeError(w, 400, "missing_idempotency_key", "Provide Idempotency-Key header")
			return
		}

		var spec db.JobSpec
		if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
			writeError(w, 400, "invalid_json", "Invalid JSON body")
			return
		}
		if spec.Image == "" {
			writeError(w, 400, "invalid_spec", "spec.image is required")
			return
		}

		// 🔥 ADD THIS BLOCK
		if spec.JobType == "training" && spec.DatasetURI == "" {
			writeError(w, 400, "invalid_spec", "datasetUri required for training jobs")
			return
		}

		if spec.GPUCount < 0 {
			writeError(w, 400, "invalid_spec", "gpuCount must be >= 0")
			return
		}

		log.Printf(
			"submit job request: type=%s gpu=%d dataset=%s artifact=%s priority=%d image=%s",
			spec.JobType,
			spec.GPUCount,
			spec.DatasetURI,
			spec.ArtifactURI,
			spec.Priority,
			spec.Image,
		)

		job, replay, err := store.CreateJobIdempotent(r.Context(), idemKey, spec)
		if err != nil {
			writeError(w, 400, "create_failed", err.Error())
			return
		}

		status := 201
		if replay {
			status = 200
		}
		writeJSON(w, status, map[string]any{"job": job, "replay": replay})
	})

	mux.HandleFunc("/v1/jobs/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeError(w, 405, "method_not_allowed", "Use GET")
			return
		}
		jobIDStr := strings.TrimPrefix(r.URL.Path, "/v1/jobs/")
		id, err := uuid.Parse(jobIDStr)
		if err != nil {
			writeError(w, 400, "invalid_job_id", "job_id must be uuid")
			return
		}
		job, err := store.GetJob(r.Context(), id)
		if err != nil {
			if errors.Is(err, db.ErrNotFound) {
				writeError(w, 404, "not_found", "Job not found")
				return
			}
			writeError(w, 500, "db_error", err.Error())
			return
		}
		writeJSON(w, 200, map[string]any{"job": job})
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           logMiddleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("job-api listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("shutdown signal received")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	log.Printf("bye")
}

func pingDB(ctx context.Context, pool *pgxpool.Pool) error {
	c, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	return pool.Ping(c)
}

func logMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start))
	})
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]any{"error": apiError{Code: code, Message: msg}})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
