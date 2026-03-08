package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/NasitSony/veriflow/internal/db"

	"github.com/NasitSony/veriflow/internal/k8s"
)

func main() {

	k8sClient, err := k8s.NewClient()
	if err != nil {
		log.Fatalf("k8s client error: %v", err)
	}

	dsn := envOr("DATABASE_URL", "postgres://veriflow:veriflow@localhost:5436/veriflow?sslmode=disable")
	queue := envOr("QUEUE", "default")
	interval := envOrDuration("SCHED_INTERVAL", 700*time.Millisecond)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("db pool: %v", err)
	}
	defer pool.Close()

	store := db.NewStore(pool)

	go func() {
		t := time.NewTicker(1 * time.Second)
		defer t.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				runs, err := store.ListActiveRuns(ctx, 50)
				if err != nil {
					log.Printf("reconcile list runs error: %v", err)
					continue
				}

				for _, r := range runs {
					kjob, err := k8sClient.BatchV1().Jobs("default").Get(ctx, r.K8sJobName, metav1.GetOptions{})
					if err != nil {
						log.Printf("reconcile get job=%s err=%v", r.K8sJobName, err)
						continue
					}

					// If job has started pods, mark running
					if r.State != "RUNNING" && (kjob.Status.Active > 0 || kjob.Status.Succeeded > 0 || kjob.Status.Failed > 0) {
						_ = store.MarkRunRunning(ctx, r.JobID, r.RunID)
					}

					// Terminal states
					if kjob.Status.Succeeded > 0 {
						_ = store.MarkRunSucceededOnly(ctx, r.RunID)
						_ = store.MarkJobSucceeded(ctx, r.JobID, r.RunID)
						continue
					}
					if kjob.Status.Failed > 0 {
						//reason := "k8s_job_failed"
						//_ = store.MarkRunFailed(ctx, r.JobID, r.RunID, reason)
						_ = store.MarkRunFailedOnly(ctx, r.RunID, "k8s_job_failed")
						_ = store.ScheduleRetryOrFail(ctx, r.JobID, r.RunID)
						continue
					}
				}
			}
		}
	}()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("scheduler started queue=%s interval=%s", queue, interval)

	for {
		select {
		case <-ctx.Done():
			log.Printf("scheduler shutting down")
			return
		case <-ticker.C:
			j, run, ok, err := store.ClaimNextPendingJob(ctx, queue)
			if err != nil {
				log.Printf("claim error: %v", err)
				continue
			}
			if !ok {
				continue
			}
			jobName := fmt.Sprintf("run-%s", run.RunID.String())

			err = k8s.CreateJob(
				k8sClient,
				"default",
				jobName,
				j.Spec.Image,
				j.Spec.Command,
				j.JobID.String(),   // ✅ add this
				run.RunID.String(), // ✅ add this
			)

			if err := store.MarkRunDispatched(ctx, run.RunID, jobName); err != nil {
				log.Printf("mark dispatched error: %v", err)
			}

			//if err != nil {
			//log.Printf("k8s create error: %v", err)
			//continue
			//}

			_ = store.MarkRunDispatched(ctx, run.RunID, jobName)

			log.Printf("DISPATCHED job_id=%s run_id=%s k8s_job=%s",
				j.JobID, run.RunID, jobName)
		}
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envOrDuration(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
	}
	return def
}
