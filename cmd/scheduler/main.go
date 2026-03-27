package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/NasitSony/veriflow/internal/db"
	"github.com/NasitSony/veriflow/internal/k8s"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type NodeCapacity struct {
	Name      string `json:"name"`
	TotalGPUs int    `json:"totalGPUs"`
	UsedGPUs  int    `json:"usedGPUs"`
}

func (n NodeCapacity) FreeGPUs() int {
	return n.TotalGPUs - n.UsedGPUs
}

type SchedulerStatus struct {
	Queue        string         `json:"queue"`
	Nodes        []NodeCapacity `json:"nodes"`
	LastUpdated  time.Time      `json:"lastUpdated"`
	ActiveRuns   int            `json:"activeRuns"`
	PendingClaim bool           `json:"pendingClaim"`
}

func pickNodeForJob(nodes []NodeCapacity, gpuCount int) (NodeCapacity, bool) {
	for _, n := range nodes {
		if n.FreeGPUs() >= gpuCount {
			return n, true
		}
	}
	return NodeCapacity{}, false
}

func writeSchedulerStatus(path string, status SchedulerStatus) {
	data, err := json.MarshalIndent(status, "", "  ")
	if err != nil {
		log.Printf("marshal scheduler status error: %v", err)
		return
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		log.Printf("write scheduler status error: %v", err)
	}
}

func main() {

	k8sClient, err := k8s.NewClient()
	if err != nil {
		log.Fatalf("k8s client error: %v", err)
	}

	dsn := envOr("DATABASE_URL", "postgres://veriflow:veriflow@localhost:5436/veriflow?sslmode=disable")
	queues := parseQueues(envOr("QUEUES", "default"))
	queueIdx := 0
	interval := envOrDuration("SCHED_INTERVAL", 700*time.Millisecond)

	heartbeatStaleAfter := envOrDuration("HEARTBEAT_STALE_AFTER", 15*time.Second)

	statusPath := envOr("SCHED_STATUS_PATH", "/tmp/veriflow-scheduler-status.json")

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

				timedOutRuns, err := store.ListTimedOutRuns(ctx)
				if err != nil {
					log.Printf("list timed out runs error: %v", err)
				}
				timedOutMap := map[uuid.UUID]int{}
				for _, tr := range timedOutRuns {
					if tr.StartedAt != nil {
						deadline := tr.StartedAt.Add(time.Duration(tr.TimeoutSeconds) * time.Second)
						if time.Now().After(deadline) {
							timedOutMap[tr.RunID] = tr.TimeoutSeconds
						}
					}
				}

				staleRuns, err := store.ListHeartbeatStaleRuns(ctx, heartbeatStaleAfter)
				if err != nil {
					log.Printf("list heartbeat stale runs error: %v", err)
				}

				staleRunMap := map[uuid.UUID]struct{}{}
				for _, sr := range staleRuns {
					staleRunMap[sr.RunID] = struct{}{}
				}

				for _, r := range runs {

					//timedOutRuns, err := store.ListTimedOutRuns(ctx)

					if timeoutSec, ok := timedOutMap[r.RunID]; ok {
						log.Printf("run timeout job_id=%s run_id=%s timeout=%ds", r.JobID, r.RunID, timeoutSec)

						_ = store.AddJobEvent(ctx, r.JobID, &r.RunID, "JOB_TIMEOUT", map[string]any{
							"timeout_seconds": timeoutSec,
						})

						_ = store.MarkRunFailedOnly(ctx, r.RunID, "job_timeout")
						_ = store.ScheduleRetryOrFail(ctx, r.JobID, r.RunID, "job_timeout")

						continue
					}

					if _, ok := staleRunMap[r.RunID]; ok {
						log.Printf("heartbeat stale job_id=%s run_id=%s stale_after=%s", r.JobID, r.RunID, heartbeatStaleAfter)

						_ = store.AddJobEvent(ctx, r.JobID, &r.RunID, "HEARTBEAT_MISSED", map[string]any{
							"stale_after_seconds": int(heartbeatStaleAfter.Seconds()),
						})

						_ = store.MarkRunFailedOnly(ctx, r.RunID, "heartbeat_stale")
						_ = store.ScheduleRetryOrFail(ctx, r.JobID, r.RunID, "heartbeat_stale")

						continue
					}

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

	writeSchedulerStatus(statusPath, SchedulerStatus{
		Queue:       strings.Join(queues, ","),
		Nodes:       nodes,
		LastUpdated: time.Now().UTC(),
		ActiveRuns:  0,
	})

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("scheduler started queues=%v interval=%s", queues, interval)

	for {
		select {
		case <-ctx.Done():
			log.Printf("scheduler shutting down")
			return
		case <-ticker.C:

			usageByNode, err := store.GetActiveGPUUsageByNode(ctx)
			if err != nil {
				log.Printf("gpu usage query error: %v", err)
				continue
			}

			effectiveNodes := applyGPUUsage(nodes, usageByNode)

			writeSchedulerStatus(statusPath, SchedulerStatus{
				Queue:        strings.Join(queues, ","),
				Nodes:        effectiveNodes,
				LastUpdated:  time.Now().UTC(),
				ActiveRuns:   0,
				PendingClaim: true,
			})

			var (
				j   db.Job
				run db.Run
				ok  bool
			)

			queueOrder := nextQueueOrder(queues, queueIdx)

			for _, q := range queueOrder {
				j, run, ok, err = store.ClaimNextPendingJob(ctx, q)
				if err != nil {
					log.Printf("claim error queue=%s: %v", q, err)
					continue
				}
				if ok {
					log.Printf("claimed job from queue=%s job_id=%s run_id=%s", q, j.JobID, run.RunID)
					break
				}
			}

			queueIdx = (queueIdx + 1) % len(queues)

			if !ok {
				continue
			}

			writeSchedulerStatus(statusPath, SchedulerStatus{
				Queue:        strings.Join(queues, ","),
				Nodes:        effectiveNodes,
				LastUpdated:  time.Now().UTC(),
				ActiveRuns:   0,
				PendingClaim: false,
			})

			// 🔥 Day 3: GPU-aware fit check before dispatch
			gpuNeeded := j.Spec.GPUCount
			node, fit := pickNodeForJob(effectiveNodes, gpuNeeded)
			if !fit {
				log.Printf(
					"insufficient GPU capacity for job_id=%s run_id=%s need_gpu=%d",
					j.JobID,
					run.RunID,
					gpuNeeded,
				)

				_ = store.AddJobEvent(ctx, j.JobID, &run.RunID, "PLACEMENT_DEFERRED", map[string]any{
					"reason":     "insufficient_gpu_capacity",
					"gpu_needed": gpuNeeded,
					"job_type":   j.Spec.JobType,
					"queue":      j.Spec.Queue,
				})

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

			_ = store.AddJobEvent(ctx, j.JobID, &run.RunID, "PLACEMENT_SELECTED", map[string]any{
				"node":       node.Name,
				"gpu_needed": gpuNeeded,
				"free_gpu":   node.FreeGPUs(),
				"job_type":   j.Spec.JobType,
				"queue":      j.Spec.Queue,
				"checkpoint": j.Spec.CheckpointURI,
				"artifact":   j.Spec.ArtifactURI,
			})

			jobName := fmt.Sprintf("run-%s", run.RunID.String())

			command := j.Spec.Command
			if j.Spec.JobType == "training" && len(command) == 0 {

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

			_ = store.AddJobEvent(ctx, j.JobID, &run.RunID, "DISPATCH_REQUESTED", map[string]any{
				"k8s_job_name": jobName,
				"image":        j.Spec.Image,
				"job_type":     j.Spec.JobType,
			})

			alreadyDispatched, existingJobName, err := store.RunAlreadyDispatched(ctx, run.RunID)
			if err != nil {
				log.Printf("dispatch check error job_id=%s run_id=%s err=%v", j.JobID, run.RunID, err)
				continue
			}
			if alreadyDispatched {
				log.Printf("run already dispatched job_id=%s run_id=%s k8s_job=%s", j.JobID, run.RunID, existingJobName)
				_ = store.AddJobEvent(ctx, j.JobID, &run.RunID, "DISPATCH_SKIPPED_ALREADY_EXISTS", map[string]any{
					"k8s_job_name": existingJobName,
				})
				continue
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

			if err != nil {
				log.Printf("k8s create error job_id=%s run_id=%s err=%v", j.JobID, run.RunID, err)
				_ = store.AddJobEvent(ctx, j.JobID, &run.RunID, "DISPATCH_FAILED", map[string]any{
					"k8s_job_name": jobName,
					"error":        err.Error(),
				})
				continue
			}

			if err := store.MarkRunDispatched(ctx, run.RunID, jobName); err != nil {
				log.Printf("mark dispatched error: %v", err)
			}

			//if err != nil {
			//log.Printf("k8s create error: %v", err)
			//continue
			//}

			//_ = store.MarkRunDispatched(ctx, run.RunID, jobName)

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

func parseQueues(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		q := strings.TrimSpace(p)
		if q != "" {
			out = append(out, q)
		}
	}
	if len(out) == 0 {
		return []string{"default"}
	}
	return out
}

func applyGPUUsage(nodes []NodeCapacity, usage map[string]int) []NodeCapacity {
	out := make([]NodeCapacity, len(nodes))
	copy(out, nodes)

	for i := range out {
		out[i].UsedGPUs = usage[out[i].Name]
		if out[i].UsedGPUs < 0 {
			out[i].UsedGPUs = 0
		}
		if out[i].UsedGPUs > out[i].TotalGPUs {
			out[i].UsedGPUs = out[i].TotalGPUs
		}
	}
	return out
}

func nextQueueOrder(queues []string, start int) []string {
	if len(queues) == 0 {
		return []string{"default"}
	}
	out := make([]string, 0, len(queues))
	for i := 0; i < len(queues); i++ {
		idx := (start + i) % len(queues)
		out = append(out, queues[idx])
	}
	return out
}
