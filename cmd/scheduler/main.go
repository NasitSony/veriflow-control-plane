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
	runtimestatus "github.com/NasitSony/veriflow/internal/runtime"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type NodeCapacity struct {
	Name           string `json:"name"`
	GPUType        string `json:"gpuType"`
	TotalGPUs      int    `json:"totalGPUs"`
	UsedGPUs       int    `json:"usedGPUs"`
	MemoryPerGPUmb int    `json:"memoryPerGpuMb"`
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

	schedulerID := envOr("SCHEDULER_ID", "sched-unknown")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("db pool: %v", err)
	}
	defer pool.Close()

	store := db.NewStore(pool)

	lastSeenStatus := map[uuid.UUID]*runtimestatus.RunStatus{}

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

					statusFile := fmt.Sprintf("/tmp/veriflow-status/%s.json", r.RunID.String())

					rs, err := runtimestatus.ReadRunStatus(statusFile)
					statusLoaded := false

					if err != nil {
						// Missing status file is normal early in execution.
						// Malformed or unreadable status should not break reconciliation.

						if !os.IsNotExist(err) {
							log.Printf("read run status job_id=%s run_id=%s path=%s err=%v", r.JobID, r.RunID, statusFile, err)
						}
					} else {
						// Optional sanity checks

						statusLoaded = true
						prev := lastSeenStatus[r.RunID]

						if rs.RunID != "" && rs.RunID != r.RunID.String() {
							log.Printf("status run_id mismatch job_id=%s expected=%s got=%s", r.JobID, r.RunID, rs.RunID)
						} else {

							switch rs.JobType {

							case "training":
								if rs.Training != nil {
									curr := rs.Training

									var prevT *runtimestatus.TrainingStatus
									if prev != nil {
										prevT = prev.Training
									}

									phaseChanged := prev == nil || rs.Phase != prev.Phase
									messageChanged := prev == nil || rs.Message != prev.Message

									if prevT == nil ||
										curr.Epoch != prevT.Epoch ||
										curr.Step != prevT.Step ||
										!floatPtrEqual(curr.Loss, prevT.Loss) ||
										!floatPtrEqual(curr.Accuracy, prevT.Accuracy) || phaseChanged || messageChanged {

										_ = store.AddJobEvent(ctx, r.JobID, &r.RunID, "TRAINING_PROGRESS", map[string]any{
											"phase":      rs.Phase,
											"epoch":      curr.Epoch,
											"step":       curr.Step,
											"loss":       curr.Loss,
											"accuracy":   curr.Accuracy,
											"updated_at": rs.UpdatedAt,
											"message":    rs.Message,
										})
									}

									if curr.CheckpointPath != "" &&
										(prevT == nil || curr.CheckpointPath != prevT.CheckpointPath) {

										_ = store.AddJobEvent(ctx, r.JobID, &r.RunID, "CHECKPOINT_SAVED", map[string]any{
											"phase":           rs.Phase,
											"checkpoint_path": curr.CheckpointPath,
											"epoch":           curr.Epoch,
											"step":            curr.Step,
											"updated_at":      rs.UpdatedAt,
										})

										if err := store.UpdateLatestCheckpointURI(ctx, r.JobID, curr.CheckpointPath); err != nil {
											log.Printf("checkpoint update failed job_id=%s run_id=%s err=%v",
												r.JobID, r.RunID, err)
										} else {
											log.Printf("checkpoint updated job_id=%s run_id=%s path=%s",
												r.JobID, r.RunID, curr.CheckpointPath)
										}
									}

									if curr.ArtifactPath != "" &&
										(prevT == nil || curr.ArtifactPath != prevT.ArtifactPath) {

										_ = store.AddJobEvent(ctx, r.JobID, &r.RunID, "ARTIFACT_WRITTEN", map[string]any{
											"phase":         rs.Phase,
											"artifact_path": curr.ArtifactPath,
											"epoch":         curr.Epoch,
											"step":          curr.Step,
											"updated_at":    rs.UpdatedAt,
										})
									}
								}

							case "batch-inference":
								if rs.Inference != nil {
									curr := rs.Inference

									var prevI *runtimestatus.InferenceStatus
									if prev != nil {
										prevI = prev.Inference
									}

									if prevI == nil ||
										curr.ProcessedBatches != prevI.ProcessedBatches ||
										curr.LatencyMs != prevI.LatencyMs {

										_ = store.AddJobEvent(ctx, r.JobID, &r.RunID, "INFERENCE_PROGRESS", map[string]any{
											"phase":             rs.Phase,
											"processed_batches": curr.ProcessedBatches,
											"latency_ms":        curr.LatencyMs,
											"updated_at":        rs.UpdatedAt,
											"message":           rs.Message,
										})
									}

									if curr.ResultsPath != "" &&
										(prevI == nil || curr.ResultsPath != prevI.ResultsPath) {

										_ = store.AddJobEvent(ctx, r.JobID, &r.RunID, "RESULTS_WRITTEN", map[string]any{
											"phase":        rs.Phase,
											"results_path": curr.ResultsPath,
											"updated_at":   rs.UpdatedAt,
										})
									}

									if curr.ArtifactPath != "" &&
										(prevI == nil || curr.ArtifactPath != prevI.ArtifactPath) {

										_ = store.AddJobEvent(ctx, r.JobID, &r.RunID, "ARTIFACT_WRITTEN", map[string]any{
											"phase":         rs.Phase,
											"artifact_path": curr.ArtifactPath,
											"updated_at":    rs.UpdatedAt,
										})
									}
								}

							case "evaluation":
								if rs.Evaluation != nil {
									curr := rs.Evaluation

									var prevE *runtimestatus.EvaluationStatus
									if prev != nil {
										prevE = prev.Evaluation
									}

									if prevE == nil ||
										curr.Step != prevE.Step ||
										!floatPtrEqual(curr.Loss, prevE.Loss) ||
										!floatPtrEqual(curr.Accuracy, prevE.Accuracy) {

										_ = store.AddJobEvent(ctx, r.JobID, &r.RunID, "EVALUATION_PROGRESS", map[string]any{
											"phase":      rs.Phase,
											"step":       curr.Step,
											"loss":       curr.Loss,
											"accuracy":   curr.Accuracy,
											"updated_at": rs.UpdatedAt,
											"message":    rs.Message,
										})
									}

									if curr.ResultsPath != "" &&
										(prevE == nil || curr.ResultsPath != prevE.ResultsPath) {

										_ = store.AddJobEvent(ctx, r.JobID, &r.RunID, "RESULTS_WRITTEN", map[string]any{
											"phase":        rs.Phase,
											"results_path": curr.ResultsPath,
											"updated_at":   rs.UpdatedAt,
										})
									}
								}
							}
						}
					}

					kjob, err := k8sClient.BatchV1().Jobs("default").Get(ctx, r.K8sJobName, metav1.GetOptions{})
					if err != nil {
						log.Printf("reconcile get job=%s err=%v", r.K8sJobName, err)
						continue
					}

					// Pod-level failure detection (more reliable than only using Job status)
					pods, err := k8sClient.CoreV1().Pods("default").List(ctx, metav1.ListOptions{
						LabelSelector: fmt.Sprintf("job-name=%s", r.K8sJobName),
					})
					if err != nil {
						log.Printf("list pods error job=%s err=%v", r.K8sJobName, err)
						continue
					}

					podFailed := false
					podFailReason := ""

					for _, pod := range pods.Items {
						for _, cs := range pod.Status.ContainerStatuses {
							if cs.State.Terminated != nil {
								term := cs.State.Terminated
								if term.ExitCode != 0 {
									podFailed = true
									podFailReason = fmt.Sprintf("container_exit_%d", term.ExitCode)
									log.Printf(
										"pod failed job_id=%s run_id=%s pod=%s container=%s exit_code=%d",
										r.JobID,
										r.RunID,
										pod.Name,
										cs.Name,
										term.ExitCode,
									)
									break
								}
							}
						}
						if podFailed {
							break
						}
					}

					// If job has started pods, mark running
					if r.State != "RUNNING" && (kjob.Status.Active > 0 || kjob.Status.Succeeded > 0 || kjob.Status.Failed > 0) {
						_ = store.MarkRunRunning(ctx, r.JobID, r.RunID)
					}

					// Terminal states
					if podFailed {
						delete(lastSeenStatus, r.RunID)

						log.Printf("run failed job_id=%s run_id=%s reason=%s", r.JobID, r.RunID, podFailReason)

						_ = store.AddJobEvent(ctx, r.JobID, &r.RunID, "RUN_FAILED", map[string]any{
							"reason": podFailReason,
						})

						if err := store.MarkRunFailedOnly(ctx, r.RunID, podFailReason); err != nil {
							log.Printf("mark run failed error job_id=%s run_id=%s err=%v", r.JobID, r.RunID, err)
						}

						log.Printf("triggering retry job_id=%s run_id=%s", r.JobID, r.RunID)
						if err := store.ScheduleRetryOrFail(ctx, r.JobID, r.RunID, podFailReason); err != nil {
							log.Printf("retry scheduling failed job_id=%s run_id=%s err=%v", r.JobID, r.RunID, err)
						} else {
							log.Printf("retry scheduling succeeded job_id=%s run_id=%s", r.JobID, r.RunID)
						}

						continue
					}

					if kjob.Status.Succeeded > 0 {
						delete(lastSeenStatus, r.RunID)
						_ = store.MarkRunSucceededOnly(ctx, r.RunID)
						_ = store.MarkJobSucceeded(ctx, r.JobID, r.RunID)
						continue
					}

					if kjob.Status.Failed > 0 {
						delete(lastSeenStatus, r.RunID)
						log.Printf("run failed job_id=%s run_id=%s reason=%s", r.JobID, r.RunID, "k8s_job_failed")

						if r.State != "FAILED" {
							_ = store.AddJobEvent(ctx, r.JobID, &r.RunID, "RUN_FAILED", map[string]any{
								"reason": "k8s_job_failed",
							})

							_ = store.MarkRunFailedOnly(ctx, r.RunID, "k8s_job_failed")
							_ = store.ScheduleRetryOrFail(ctx, r.JobID, r.RunID, "k8s_job_failed")
						}
						continue
					}

					if statusLoaded {
						lastSeenStatus[r.RunID] = rs
					}
				}
			}
		}
	}()

	// 🔥 Day 3: basic GPU inventory
	nodes := []NodeCapacity{
		{
			Name:           "gpu-node-a",
			GPUType:        "A100",
			TotalGPUs:      4,
			UsedGPUs:       0,
			MemoryPerGPUmb: 40000,
		},
		{
			Name:           "gpu-node-b",
			GPUType:        "L4",
			TotalGPUs:      2,
			UsedGPUs:       0,
			MemoryPerGPUmb: 24000,
		},
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
					log.Printf("[%s] claimed job from queue=%s job_id=%s run_id=%s", schedulerID, q, j.JobID, run.RunID)
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

			gpuNeeded := j.Spec.GPUCount

			var selected *NodeCapacity
			var rejectReason string

			bestPostFree := int(^uint(0) >> 1) // max int
			bestMemoryHeadroom := int(^uint(0) >> 1)

			for i := range effectiveNodes {
				fits, reason := nodeFitsGPU(effectiveNodes[i], j.Spec)
				if !fits {
					log.Printf(
						"node rejected job_id=%s run_id=%s node=%s reason=%s gpu_needed=%d req_gpu_type=%s min_gpu_memory_mb=%d",
						j.JobID,
						run.RunID,
						effectiveNodes[i].Name,
						reason,
						gpuNeeded,
						j.Spec.GPUType,
						j.Spec.MinGPUMemoryMB,
					)
					rejectReason = reason
					continue
				}

				postFree := effectiveNodes[i].FreeGPUs() - gpuNeeded

				memoryHeadroom := 0
				if j.Spec.MinGPUMemoryMB > 0 {
					memoryHeadroom = effectiveNodes[i].MemoryPerGPUmb - j.Spec.MinGPUMemoryMB
				}

				log.Printf(
					"node candidate job_id=%s run_id=%s node=%s post_free_gpu=%d memory_headroom_mb=%d",
					j.JobID,
					run.RunID,
					effectiveNodes[i].Name,
					postFree,
					memoryHeadroom,
				)

				if selected == nil ||
					postFree < bestPostFree ||
					(postFree == bestPostFree && memoryHeadroom < bestMemoryHeadroom) {

					selected = &effectiveNodes[i]
					bestPostFree = postFree
					bestMemoryHeadroom = memoryHeadroom
				}
			}

			if selected == nil {
				log.Printf(
					"placement deferred job_id=%s run_id=%s reason=%s gpu_needed=%d gpu_type=%s min_gpu_memory_mb=%d",
					j.JobID,
					run.RunID,
					rejectReason,
					gpuNeeded,
					j.Spec.GPUType,
					j.Spec.MinGPUMemoryMB,
				)

				_ = store.AddJobEvent(ctx, j.JobID, &run.RunID, "PLACEMENT_DEFERRED", map[string]any{
					"reason":            rejectReason,
					"gpu_needed":        gpuNeeded,
					"gpu_type":          j.Spec.GPUType,
					"min_gpu_memory_mb": j.Spec.MinGPUMemoryMB,
					"job_type":          j.Spec.JobType,
					"queue":             j.Spec.Queue,
					"checkpoint":        j.Spec.CheckpointURI,
					"artifact":          j.Spec.ArtifactURI,
					"scheduler_id":      schedulerID,
					"placement_policy":  "best_fit_gpu",
				})

				continue
			}

			log.Printf(
				"selected node for job_id=%s run_id=%s node=%s need_gpu=%d free_gpu=%d post_free_gpu=%d node_gpu_type=%s req_gpu_type=%s min_gpu_memory_mb=%d job_type=%s policy=%s",
				j.JobID,
				run.RunID,
				selected.Name,
				gpuNeeded,
				selected.FreeGPUs(),
				bestPostFree,
				selected.GPUType,
				j.Spec.GPUType,
				j.Spec.MinGPUMemoryMB,
				j.Spec.JobType,
				"best_fit_gpu",
			)

			_ = store.AddJobEvent(ctx, j.JobID, &run.RunID, "PLACEMENT_SELECTED", map[string]any{
				"node":               selected.Name,
				"gpu_needed":         gpuNeeded,
				"free_gpu":           selected.FreeGPUs(),
				"post_free_gpu":      bestPostFree,
				"gpu_type":           selected.GPUType,
				"memory_per_gpu_mb":  selected.MemoryPerGPUmb,
				"requested_gpu_type": j.Spec.GPUType,
				"min_gpu_memory_mb":  j.Spec.MinGPUMemoryMB,
				"job_type":           j.Spec.JobType,
				"queue":              j.Spec.Queue,
				"checkpoint":         j.Spec.CheckpointURI,
				"artifact":           j.Spec.ArtifactURI,
				"scheduler_id":       schedulerID,
				"placement_policy":   "best_fit_gpu",
			})

			jobName := fmt.Sprintf("run-%s", run.RunID.String())
			command := j.Spec.Command

			if j.Spec.JobType == "training" {
				command = []string{
					"/bin/sh",
					"-c",
					`
echo "starting training job"
if [ -n "$CHECKPOINT_URI" ]; then
  echo "resuming from checkpoint=$CHECKPOINT_URI"
else
  echo "no checkpoint provided"
fi
echo "dataset=$DATASET_URI"

mkdir -p "$(dirname "$STATUS_PATH")"
mkdir -p /artifacts

cat <<EOF > "$STATUS_PATH"
{
  "runId": "$RUN_ID",
  "jobType": "training",
  "phase": "running",
  "updatedAt": "2026-03-28T16:10:00Z",
  "message": "training started",
  "training": {
    "epoch": 1,
    "step": 100,
    "loss": 0.84,
    "accuracy": 0.71
  }
}
EOF

echo "wrote training status epoch=1"
sleep 2

cat <<EOF > "$STATUS_PATH"
{
  "runId": "$RUN_ID",
  "jobType": "training",
  "phase": "running",
  "updatedAt": "2026-03-28T16:11:00Z",
  "message": "training progressing",
  "training": {
    "epoch": 2,
    "step": 200,
    "loss": 0.61,
    "accuracy": 0.79,
    "checkpointPath": "/artifacts/ckpt-2"
  }
}
EOF

echo "wrote training status epoch=2 checkpoint=/artifacts/ckpt-2"
sleep 2

# First attempt: fail after checkpoint so retry can resume
if [ -z "$CHECKPOINT_URI" ]; then
  echo "simulating failure after checkpoint"
  exit 1
fi

# Retry path: resume and finish
cat <<EOF > "$STATUS_PATH"
{
  "runId": "$RUN_ID",
  "jobType": "training",
  "phase": "running",
  "updatedAt": "2026-03-28T16:12:00Z",
  "message": "artifact written",
  "training": {
    "epoch": 3,
    "step": 300,
    "loss": 0.43,
    "accuracy": 0.86,
    "checkpointPath": "/artifacts/ckpt-2",
    "artifactPath": "/artifacts/model-final"
  }
}
EOF

echo "wrote training status epoch=3 artifact=/artifacts/model-final"
sleep 2

cat <<EOF > "$STATUS_PATH"
{
  "runId": "$RUN_ID",
  "jobType": "training",
  "phase": "succeeded",
  "updatedAt": "2026-03-28T16:13:00Z",
  "message": "training complete",
  "training": {
    "epoch": 3,
    "step": 300,
    "loss": 0.43,
    "accuracy": 0.86,
    "checkpointPath": "/artifacts/ckpt-2",
    "artifactPath": "/artifacts/model-final"
  }
}
EOF

echo "training complete"
`,
				}
			}

			if j.Spec.JobType == "batch-inference" && len(command) == 0 {
				command = []string{
					"/bin/sh",
					"-c",
					"echo starting batch inference job; " +
						"echo dataset=$DATASET_URI; " +
						"echo loading model artifact=$ARTIFACT_URI; sleep 2; " +
						"echo processed_batch=1 latency_ms=120; sleep 1; " +
						"echo processed_batch=2 latency_ms=110; sleep 1; " +
						"echo processed_batch=3 latency_ms=115; sleep 1; " +
						"echo batch inference complete",
				}
			}

			if j.Spec.JobType == "evaluation" && len(command) == 0 {
				command = []string{
					"/bin/sh",
					"-c",
					"echo starting evaluation job; " +
						"echo dataset=$DATASET_URI; sleep 2; " +
						"echo validation_loss=0.52 accuracy=0.82; sleep 2; " +
						"echo validation_loss=0.47 accuracy=0.85; sleep 2; " +
						"echo evaluation complete",
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
				"RUN_ID":         run.RunID.String(),
				"STATUS_PATH":    fmt.Sprintf("/status/%s.json", run.RunID.String()),
			}

			_ = store.AddJobEvent(ctx, j.JobID, &run.RunID, "DISPATCH_REQUESTED", map[string]any{
				"k8s_job_name": jobName,
				"image":        j.Spec.Image,
				"job_type":     j.Spec.JobType,
				"scheduler_id": schedulerID,
			})

			if j.Spec.JobType == "training" && j.Spec.CheckpointURI != "" {
				_ = store.AddJobEvent(ctx, j.JobID, &run.RunID, "TRAINING_RESUMED", map[string]any{
					"checkpoint_uri": j.Spec.CheckpointURI,
					"attempt":        run.Attempt,
				})
			}

			if j.Spec.JobType == "batch-inference" {
				_ = store.AddJobEvent(ctx, j.JobID, &run.RunID, "INFERENCE_STARTED", map[string]any{
					"dataset":   j.Spec.DatasetURI,
					"gpu_count": j.Spec.GPUCount,
					"artifact":  j.Spec.ArtifactURI,
				})
			}

			if j.Spec.JobType == "evaluation" {
				_ = store.AddJobEvent(ctx, j.JobID, &run.RunID, "EVALUATION_STARTED", map[string]any{
					"dataset":   j.Spec.DatasetURI,
					"gpu_count": j.Spec.GPUCount,
				})
			}

			alreadyDispatched, existingJobName, err := store.RunAlreadyDispatched(ctx, run.RunID)
			if err != nil {
				log.Printf("[%s] Dispatch check error job_id=%s run_id=%s err=%v", schedulerID, j.JobID, run.RunID, err)
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

			log.Printf("[%s] DISPATCHED job_id=%s run_id=%s k8s_job=%s",
				schedulerID, j.JobID, run.RunID, jobName)
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

func nodeFitsGPU(node NodeCapacity, spec db.JobSpec) (bool, string) {
	if node.FreeGPUs() < spec.GPUCount {
		return false, "insufficient_gpu_count"
	}
	if spec.GPUType != "" && node.GPUType != spec.GPUType {
		return false, "gpu_type_mismatch"
	}
	if spec.MinGPUMemoryMB > 0 && node.MemoryPerGPUmb < spec.MinGPUMemoryMB {
		return false, "insufficient_gpu_memory"
	}
	return true, ""
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

func floatPtrEqual(a, b *float64) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}
