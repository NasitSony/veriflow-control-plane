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

type NodeCapacity struct {
	Name      string
	TotalGPUs int
	UsedGPUs  int
}

func (n NodeCapacity) FreeGPUs() int {
	return n.TotalGPUs - n.UsedGPUs
}

func pickNodeForJob(nodes []NodeCapacity, gpuCount int) (NodeCapacity, bool) {
	for _, n := range nodes {
		if n.FreeGPUs() >= gpuCount {
			return n, true
		}
	}
	return NodeCapacity{}, false
}

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
						log.Printf("run failed job_id=%s run_id=%s reason=%s", r.JobID, r.RunID, "k8s_job_failed")
						_ = store.MarkRunFailedOnly(ctx, r.RunID, "k8s_job_failed")
						_ = store.ScheduleRetryOrFail(ctx, r.JobID, r.RunID, "k8s_job_failed")
						continue
					}
				}
			}
		}
	}()

	// 🔥 Day 3: basic GPU inventory
	nodes := []NodeCapacity{
		{Name: "gpu-node-a", TotalGPUs: 2, UsedGPUs: 0},
		{Name: "gpu-node-b", TotalGPUs: 4, UsedGPUs: 0},
	}

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

			// 🔥 Day 3: GPU-aware fit check before dispatch
			gpuNeeded := j.Spec.GPUCount
			node, fit := pickNodeForJob(nodes, gpuNeeded)
			if !fit {
				log.Printf(
					"insufficient GPU capacity for job_id=%s run_id=%s need_gpu=%d",
					j.JobID,
					run.RunID,
					gpuNeeded,
				)
				continue
			}

			log.Printf(
				"selected node for job_id=%s run_id=%s node=%s need_gpu=%d free_gpu=%d job_type=%s",
				j.JobID,
				run.RunID,
				node.Name,
				gpuNeeded,
				node.FreeGPUs(),
				j.Spec.JobType,
			)

			jobName := fmt.Sprintf("run-%s", run.RunID.String())

			command := j.Spec.Command
			if j.Spec.JobType == "training" && len(command) == 0 {
				//resumeMsg := "echo no checkpoint provided; "
				//if j.Spec.CheckpointURI != "" {
				//resumeMsg = fmt.Sprintf("echo resuming from checkpoint=%s; ", j.Spec.CheckpointURI)
				//}

				//artifactMsg := "echo no artifact output configured; "
				//if j.Spec.ArtifactURI != "" {
				//artifactMsg = fmt.Sprintf("echo writing artifact to=%s; ", j.Spec.ArtifactURI)
				//}

				command = []string{
					"/bin/sh",
					"-c",
					"echo starting training job; " +
						"if [ -n \"$CHECKPOINT_URI\" ]; then echo resuming from checkpoint=$CHECKPOINT_URI; else echo no checkpoint provided; fi; " +
						"echo dataset=$DATASET_URI; " +
						"echo epoch=1 loss=0.84; sleep 2; " +
						"echo epoch=2 loss=0.61; sleep 2; " +
						"echo checkpoint saved path=/artifacts/ckpt-2; sleep 2; " +
						"if [ -n \"$ARTIFACT_URI\" ]; then echo writing artifact to=$ARTIFACT_URI; else echo no artifact output configured; fi; " +
						"echo epoch=3 loss=0.43; sleep 2; " +
						"echo training complete",
				}
			}

			log.Printf(
				"dispatching %s workload job_id=%s run_id=%s gpu=%d dataset=%s checkpoint=%s artifact=%s",
				j.Spec.JobType,
				j.JobID,
				run.RunID,
				j.Spec.GPUCount,
				j.Spec.DatasetURI,
				j.Spec.CheckpointURI,
				j.Spec.ArtifactURI,
			)

			env := map[string]string{
				"JOB_TYPE":       j.Spec.JobType,
				"DATASET_URI":    j.Spec.DatasetURI,
				"CHECKPOINT_URI": j.Spec.CheckpointURI,
				"ARTIFACT_URI":   j.Spec.ArtifactURI,
			}

			err = k8s.CreateJob(
				k8sClient,
				"default",
				jobName,
				j.Spec.Image,
				command,
				env,
				j.JobID.String(),
				run.RunID.String(),
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
