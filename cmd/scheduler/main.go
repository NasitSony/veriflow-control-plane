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

	// Internal packages:
	// db: persistent store access
	// k8s: Kubernetes job launcher helper
	// runtimestatus: reads per-run status JSON files written by workers
	"github.com/NasitSony/veriflow/internal/db"
	"github.com/NasitSony/veriflow/internal/k8s"
	runtimestatus "github.com/NasitSony/veriflow/internal/runtime"

	// uuid: typed IDs for jobs/runs
	// pgxpool: PostgreSQL connection pool
	// metav1: Kubernetes list/get options and metadata types
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// GPUDevice models one GPU on a node.
// Allocated is the scheduler's current view of whether that GPU is in use.
type GPUDevice struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	MemoryMB  int    `json:"memoryMb"`
	Allocated bool   `json:"allocated"`
}

// NodeCapacity models one node and its GPU inventory.
type NodeCapacity struct {
	Name string      `json:"name"`
	GPUs []GPUDevice `json:"gpus"`
}

// FreeGPUs counts how many GPUs on a node are currently unallocated.
func (n NodeCapacity) FreeGPUs() int {
	count := 0
	for _, g := range n.GPUs {
		if !g.Allocated {
			count++
		}
	}
	return count
}

// Per-queue GPU quota policy.
// This is a simple fairness / multi-tenant control mechanism.
var queueGPUQuota = map[string]int{
	"default":  4,
	"research": 2,
	"batch":    1,
}

// countMatchingFreeGPUs counts free GPUs on a node that satisfy a job's GPU constraints:
// - type match if requested
// - minimum memory match if requested
func countMatchingFreeGPUs(node NodeCapacity, spec db.JobSpec) int {
	count := 0
	for _, g := range node.GPUs {
		if g.Allocated {
			continue
		}
		if spec.GPUType != "" && g.Type != spec.GPUType {
			continue
		}
		if spec.MinGPUMemoryMB > 0 && g.MemoryMB < spec.MinGPUMemoryMB {
			continue
		}
		count++
	}
	return count
}

// nodeFitsGPU answers:
// "Can this node satisfy the job's GPU request?"
// If not, it also returns a more specific reason string.
func nodeFitsGPU(node NodeCapacity, spec db.JobSpec) (bool, string) {
	matching := countMatchingFreeGPUs(node, spec)

	if matching < spec.GPUCount {
		// If total matching GPUs are insufficient, try to explain why.
		typeMismatch := true
		memMismatch := true

		for _, g := range node.GPUs {
			if g.Allocated {
				continue
			}
			if spec.GPUType == "" || g.Type == spec.GPUType {
				typeMismatch = false
			}
			if spec.MinGPUMemoryMB == 0 || g.MemoryMB >= spec.MinGPUMemoryMB {
				memMismatch = false
			}
		}

		if spec.GPUType != "" && typeMismatch {
			return false, "gpu_type_mismatch"
		}
		if spec.MinGPUMemoryMB > 0 && memMismatch {
			return false, "insufficient_gpu_memory"
		}
		// Otherwise, not enough matching devices together to satisfy the group request.
		return false, "gang_unsatisfied"
	}

	return true, ""
}

// selectGPUDevices performs the actual device selection after a node has been chosen.
// Current strategy: simple deterministic first-N matching GPUs.
func selectGPUDevices(node NodeCapacity, spec db.JobSpec) ([]GPUDevice, bool) {
	candidates := make([]GPUDevice, 0)

	for _, g := range node.GPUs {
		if g.Allocated {
			continue
		}
		if spec.GPUType != "" && g.Type != spec.GPUType {
			continue
		}
		if spec.MinGPUMemoryMB > 0 && g.MemoryMB < spec.MinGPUMemoryMB {
			continue
		}
		candidates = append(candidates, g)
	}

	if len(candidates) < spec.GPUCount {
		return nil, false
	}

	return candidates[:spec.GPUCount], true
}

// SchedulerStatus is a small JSON snapshot written to disk for observability/debugging.
type SchedulerStatus struct {
	Queue        string         `json:"queue"`
	Nodes        []NodeCapacity `json:"nodes"`
	LastUpdated  time.Time      `json:"lastUpdated"`
	ActiveRuns   int            `json:"activeRuns"`
	PendingClaim bool           `json:"pendingClaim"`
}

// Older/simple node picker: first node with enough free GPUs.
// Not the main placement policy anymore, but still present in the file.
func pickNodeForJob(nodes []NodeCapacity, gpuCount int) (NodeCapacity, bool) {
	for _, n := range nodes {
		if n.FreeGPUs() >= gpuCount {
			return n, true
		}
	}
	return NodeCapacity{}, false
}

// writeSchedulerStatus serializes scheduler state to a JSON file.
// Used for quick external introspection.
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
	// Create Kubernetes client. Fatal if unavailable.
	k8sClient, err := k8s.NewClient()
	if err != nil {
		log.Fatalf("k8s client error: %v", err)
	}

	// Read runtime configuration from env with defaults.
	dsn := envOr("DATABASE_URL", "postgres://veriflow:veriflow@localhost:5436/veriflow?sslmode=disable")
	queues := parseQueues(envOr("QUEUES", "default"))
	queueIdx := 0
	interval := envOrDuration("SCHED_INTERVAL", 700*time.Millisecond)

	heartbeatStaleAfter := envOrDuration("HEARTBEAT_STALE_AFTER", 15*time.Second)
	statusPath := envOr("SCHED_STATUS_PATH", "/tmp/veriflow-scheduler-status.json")
	schedulerID := envOr("SCHEDULER_ID", "sched-unknown")

	// Graceful shutdown context on Ctrl+C / SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Open database pool and wrap in store abstraction.
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("db pool: %v", err)
	}
	defer pool.Close()

	store := db.NewStore(pool)

	// Tracks last runtime status seen per run to avoid duplicate progress events.
	lastSeenStatus := map[uuid.UUID]*runtimestatus.RunStatus{}

	// Background reconciler loop:
	// - checks timeouts
	// - checks stale heartbeats
	// - reads status files
	// - checks K8s job/pod states
	// - triggers failure/retry/success transitions
	go func() {
		t := time.NewTicker(1 * time.Second)
		defer t.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				// Pull active runs to reconcile.
				runs, err := store.ListActiveRuns(ctx, 50)
				if err != nil {
					log.Printf("reconcile list runs error: %v", err)
					continue
				}

				// Build a set/map of timed-out runs.
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

				// Build a set/map of runs whose heartbeat has gone stale.
				staleRuns, err := store.ListHeartbeatStaleRuns(ctx, heartbeatStaleAfter)
				if err != nil {
					log.Printf("list heartbeat stale runs error: %v", err)
				}

				staleRunMap := map[uuid.UUID]struct{}{}
				for _, sr := range staleRuns {
					staleRunMap[sr.RunID] = struct{}{}
				}

				for _, r := range runs {
					// Timeout handling:
					// emit event, mark failed, schedule retry/failover.
					if timeoutSec, ok := timedOutMap[r.RunID]; ok {
						log.Printf("run timeout job_id=%s run_id=%s timeout=%ds", r.JobID, r.RunID, timeoutSec)

						_ = store.AddJobEvent(ctx, r.JobID, &r.RunID, "JOB_TIMEOUT", map[string]any{
							"timeout_seconds": timeoutSec,
						})

						_ = store.MarkRunFailedOnly(ctx, r.RunID, "job_timeout")
						_ = store.ScheduleRetryOrFail(ctx, r.JobID, r.RunID, "job_timeout")
						continue
					}

					// Heartbeat-stale handling:
					// emit event, mark failed, schedule retry/failover.
					if _, ok := staleRunMap[r.RunID]; ok {
						log.Printf("heartbeat stale job_id=%s run_id=%s stale_after=%s", r.JobID, r.RunID, heartbeatStaleAfter)

						_ = store.AddJobEvent(ctx, r.JobID, &r.RunID, "HEARTBEAT_MISSED", map[string]any{
							"stale_after_seconds": int(heartbeatStaleAfter.Seconds()),
						})

						_ = store.MarkRunFailedOnly(ctx, r.RunID, "heartbeat_stale")
						_ = store.ScheduleRetryOrFail(ctx, r.JobID, r.RunID, "heartbeat_stale")
						continue
					}

					// Read per-run status file written by the worker.
					statusFile := fmt.Sprintf("/tmp/veriflow-status/%s.json", r.RunID.String())

					rs, err := runtimestatus.ReadRunStatus(statusFile)
					statusLoaded := false

					if err != nil {
						// Missing file is normal early in execution.
						// Malformed file is logged but not fatal.
						if !os.IsNotExist(err) {
							log.Printf("read run status job_id=%s run_id=%s path=%s err=%v", r.JobID, r.RunID, statusFile, err)
						}
					} else {
						statusLoaded = true
						prev := lastSeenStatus[r.RunID]

						// Optional sanity check: runId inside file should match DB runId.
						if rs.RunID != "" && rs.RunID != r.RunID.String() {
							log.Printf("status run_id mismatch job_id=%s expected=%s got=%s", r.JobID, r.RunID, rs.RunID)
						} else {
							// Emit progress / artifact / checkpoint events depending on job type.
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

									// Emit TRAINING_PROGRESS only when something actually changed.
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

									// If checkpoint path changed, emit CHECKPOINT_SAVED and update latest checkpoint in DB.
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

									// If final artifact path changed, emit ARTIFACT_WRITTEN.
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

					// Reconcile against Kubernetes Job object.
					kjob, err := k8sClient.BatchV1().Jobs("default").Get(ctx, r.K8sJobName, metav1.GetOptions{})
					if err != nil {
						log.Printf("reconcile get job=%s err=%v", r.K8sJobName, err)
						continue
					}

					// Pod-level failure detection:
					// this can be more informative and immediate than only looking at Job.Status.
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

					// If K8s shows activity or terminal state, mark run RUNNING if not already.
					if r.State != "RUNNING" && (kjob.Status.Active > 0 || kjob.Status.Succeeded > 0 || kjob.Status.Failed > 0) {
						_ = store.MarkRunRunning(ctx, r.JobID, r.RunID)
					}

					// Pod-level failure path.
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

					// Success path.
					if kjob.Status.Succeeded > 0 {
						delete(lastSeenStatus, r.RunID)
						_ = store.MarkRunSucceededOnly(ctx, r.RunID)
						_ = store.MarkJobSucceeded(ctx, r.JobID, r.RunID)
						continue
					}

					// Job-controller failure path.
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

					// Save loaded status as last seen, so next pass can emit only diffs.
					if statusLoaded {
						lastSeenStatus[r.RunID] = rs
					}
				}
			}
		}
	}()

	// Main scheduler loop tick interval.
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("scheduler started queues=%v interval=%s", queues, interval)

	for {
		select {
		case <-ctx.Done():
			log.Printf("scheduler shutting down")
			return
		case <-ticker.C:
			// Mock/live candidate node inventory.
			// Right now this is hardcoded; later this would come from real cluster/device state.
			effectiveNodes := []NodeCapacity{
				{
					Name: "gpu-node-a",
					GPUs: []GPUDevice{
						{ID: "gpu0", Type: "A100", MemoryMB: 40000, Allocated: false},
						{ID: "gpu1", Type: "A100", MemoryMB: 40000, Allocated: false},
						{ID: "gpu2", Type: "A100", MemoryMB: 40000, Allocated: false},
						{ID: "gpu3", Type: "A100", MemoryMB: 40000, Allocated: false},
					},
				},
				{
					Name: "gpu-node-b",
					GPUs: []GPUDevice{
						{ID: "gpu0", Type: "L4", MemoryMB: 24000, Allocated: false},
						{ID: "gpu1", Type: "L4", MemoryMB: 24000, Allocated: false},
					},
				},
			}

			// Publish scheduler snapshot before claim attempt.
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

			// Fairness across queues: rotate starting queue each tick.
			queueOrder := nextQueueOrder(queues, queueIdx)

			// Try claiming one pending job from one of the queues.
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

			// Update status snapshot after claim.
			writeSchedulerStatus(statusPath, SchedulerStatus{
				Queue:        strings.Join(queues, ","),
				Nodes:        effectiveNodes,
				LastUpdated:  time.Now().UTC(),
				ActiveRuns:   0,
				PendingClaim: false,
			})

			// Enforce per-queue GPU quota before placement.
			usageByQueue, err := store.CurrentGPUUsageByQueue(ctx)
			if err != nil {
				log.Printf("gpu usage by queue error: %v", err)
				continue
			}

			queueQuota := queueQuotaFor(j.Spec.Queue)
			queueUsed := usageByQueue[j.Spec.Queue]

			if queueUsed+j.Spec.GPUCount > queueQuota {
				log.Printf(
					"quota deferred job_id=%s run_id=%s queue=%s used_gpu=%d request_gpu=%d quota_gpu=%d",
					j.JobID,
					run.RunID,
					j.Spec.Queue,
					queueUsed,
					j.Spec.GPUCount,
					queueQuota,
				)

				_ = store.AddJobEvent(ctx, j.JobID, &run.RunID, "PLACEMENT_DEFERRED", map[string]any{
					"reason":           "queue_gpu_quota_exceeded",
					"queue":            j.Spec.Queue,
					"queue_gpu_used":   queueUsed,
					"queue_gpu_quota":  queueQuota,
					"gpu_needed":       j.Spec.GPUCount,
					"scheduler_id":     schedulerID,
					"placement_policy": "best_fit_gpu_devices",
				})

				continue
			}

			gpuNeeded := j.Spec.GPUCount

			var selected *NodeCapacity
			var rejectReason string

			// Best-fit objective:
			// prefer node leaving the smallest remaining free GPU count after placement.
			// tie-break with smallest memory headroom.
			bestPostFree := int(^uint(0) >> 1)
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
				if j.Spec.MinGPUMemoryMB > 0 && len(effectiveNodes[i].GPUs) > 0 {
					// tie-break: smallest leftover memory above required minimum
					minHeadroom := int(^uint(0) >> 1)
					for _, g := range effectiveNodes[i].GPUs {
						if g.Allocated {
							continue
						}
						if j.Spec.GPUType != "" && g.Type != j.Spec.GPUType {
							continue
						}
						if j.Spec.MinGPUMemoryMB > 0 && g.MemoryMB < j.Spec.MinGPUMemoryMB {
							continue
						}
						headroom := g.MemoryMB - j.Spec.MinGPUMemoryMB
						if headroom < minHeadroom {
							minHeadroom = headroom
						}
					}
					if minHeadroom != int(^uint(0)>>1) {
						memoryHeadroom = minHeadroom
					}
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

			// No node satisfies placement constraints.
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
					"placement_policy":  "best_fit_gpu_devices",
				})

				continue
			}

			// Select exact GPU devices on the chosen node.
			selectedGPUs, ok := selectGPUDevices(*selected, j.Spec)
			if !ok {
				log.Printf(
					"placement deferred job_id=%s run_id=%s reason=%s",
					j.JobID,
					run.RunID,
					"insufficient_gpu_devices_after_selection",
				)

				log.Printf(
					"gang scheduling unsatisfied job_id=%s run_id=%s need_gpu=%d available=%d",
					j.JobID,
					run.RunID,
					gpuNeeded,
					countMatchingFreeGPUs(*selected, j.Spec),
				)

				_ = store.AddJobEvent(ctx, j.JobID, &run.RunID, "PLACEMENT_DEFERRED", map[string]any{
					"reason":           "gang_unsatisfied",
					"gpu_needed":       gpuNeeded,
					"scheduler_id":     schedulerID,
					"placement_policy": "best_fit_gpu_devices_gang",
				})

				continue
			}

			// Extract selected GPU IDs/types for logging and env injection.
			selectedIDs := make([]string, 0, len(selectedGPUs))
			selectedType := ""
			for _, g := range selectedGPUs {
				selectedIDs = append(selectedIDs, g.ID)
				if selectedType == "" {
					selectedType = g.Type
				}
			}

			log.Printf(
				"selected node for job_id=%s run_id=%s node=%s gpu_ids=%v need_gpu=%d free_gpu=%d post_free_gpu=%d selected_gpu_type=%s req_gpu_type=%s min_gpu_memory_mb=%d job_type=%s policy=%s",
				j.JobID,
				run.RunID,
				selected.Name,
				selectedIDs,
				gpuNeeded,
				selected.FreeGPUs(),
				bestPostFree,
				selectedType,
				j.Spec.GPUType,
				j.Spec.MinGPUMemoryMB,
				j.Spec.JobType,
				"best_fit_gpu_devices",
			)

			_ = store.AddJobEvent(ctx, j.JobID, &run.RunID, "PLACEMENT_SELECTED", map[string]any{
				"node":               selected.Name,
				"gpu_needed":         gpuNeeded,
				"free_gpu":           selected.FreeGPUs(),
				"post_free_gpu":      bestPostFree,
				"gpu_type":           selectedType,
				"requested_gpu_type": j.Spec.GPUType,
				"min_gpu_memory_mb":  j.Spec.MinGPUMemoryMB,
				"selected_gpu_ids":   selectedIDs,
				"selected_gpu_count": len(selectedIDs),
				"job_type":           j.Spec.JobType,
				"queue":              j.Spec.Queue,
				"checkpoint":         j.Spec.CheckpointURI,
				"artifact":           j.Spec.ArtifactURI,
				"scheduler_id":       schedulerID,
				"placement_policy":   "best_fit_gpu_devices",
			})

			jobName := fmt.Sprintf("run-%s", run.RunID.String())
			command := j.Spec.Command

			// Mock/demo training workload:
			// - writes status JSON
			// - writes checkpoint
			// - fails once if no checkpoint URI
			// - resumes and succeeds on retry
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

if [ -z "$CHECKPOINT_URI" ]; then
  echo "simulating failure after checkpoint"
  exit 1
fi

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

			// Mock batch inference workload if no custom command provided.
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

			// Mock evaluation workload if no custom command provided.
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

			// Env passed into the worker container.
			// STATUS_PATH is where worker writes runtime progress JSON.
			env := map[string]string{
				"JOB_TYPE":       j.Spec.JobType,
				"DATASET_URI":    j.Spec.DatasetURI,
				"CHECKPOINT_URI": j.Spec.CheckpointURI,
				"ARTIFACT_URI":   j.Spec.ArtifactURI,
				"RUN_ID":         run.RunID.String(),
				"STATUS_PATH":    fmt.Sprintf("/status/%s.json", run.RunID.String()),
			}

			// Pass exact selected GPU device IDs into container.
			if len(selectedIDs) > 0 {
				env["GPU_DEVICE_IDS"] = strings.Join(selectedIDs, ",")
			}

			log.Printf(
				"dispatch env job_id=%s run_id=%s gpu_ids=%s",
				j.JobID,
				run.RunID,
				env["GPU_DEVICE_IDS"],
			)

			// Emit dispatch-related lifecycle events.
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

			// Idempotency guard:
			// don't dispatch again if a K8s job was already created for this run.
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

			// Create Kubernetes Job.
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

			// Dispatch failure path.
			if err != nil {
				log.Printf("k8s create error job_id=%s run_id=%s err=%v", j.JobID, run.RunID, err)
				_ = store.AddJobEvent(ctx, j.JobID, &run.RunID, "DISPATCH_FAILED", map[string]any{
					"k8s_job_name": jobName,
					"error":        err.Error(),
				})
				continue
			}

			// Mark run dispatched in DB.
			if err := store.MarkRunDispatched(ctx, run.RunID, jobName); err != nil {
				log.Printf("mark dispatched error: %v", err)
			}

			log.Printf("[%s] DISPATCHED job_id=%s run_id=%s k8s_job=%s",
				schedulerID, j.JobID, run.RunID, jobName)
		}
	}
}

// envOr returns env var value or a default.
func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// envOrDuration parses duration env var or returns default.
func envOrDuration(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
	}
	return def
}

// parseQueues converts "a,b,c" -> []string{"a","b","c"} with trimming.
// Falls back to ["default"].
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

// nextQueueOrder rotates queue order for fairness.
// Example: queues=[a,b,c], start=1 => [b,c,a]
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

// queueQuotaFor returns configured queue quota,
// or a huge default if queue is unknown.
func queueQuotaFor(queue string) int {
	if q, ok := queueGPUQuota[queue]; ok {
		return q
	}
	return 1 << 30
}

// floatPtrEqual safely compares *float64 values, including nil cases.
func floatPtrEqual(a, b *float64) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}
