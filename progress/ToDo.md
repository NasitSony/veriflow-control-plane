# Veriflow — 21-Day AI Infrastructure Upgrade Plan (Strong Option 3)

## 🎯 Goal

Transform Veriflow into a true AI workload orchestration platform using:

* Runtime status reconciliation
* GPU-aware scheduling
* Checkpoint-aware recovery
* AI-native lifecycle + observability

---

# 📅 Week 1 — Runtime-Aware AI Workloads (Core Foundation)

## Day 1 — First-Class Workload Types ✅

* [ ] Validate `jobType` (training, batch-inference, evaluation)
* [ ] Branch scheduler behavior by jobType
* [ ] Add workload-specific fallback commands
* [ ] Emit:

  * [ ] TRAINING_STARTED
  * [ ] INFERENCE_STARTED
  * [ ] EVALUATION_STARTED

---

## Day 2 — Runtime Status Model

* [ ] Define `RunStatus` struct (Go)
* [ ] Fields:

  * [ ] runId
  * [ ] jobType
  * [ ] phase
  * [ ] epoch / step
  * [ ] loss / accuracy
  * [ ] processedBatches / latency
  * [ ] checkpointPath
  * [ ] artifactPath
  * [ ] resultsPath
  * [ ] updatedAt
* [ ] Define path convention:

  * [ ] `/status/<run-id>.json`
  * [ ] `/artifacts/<run-id>/`

---

## Day 3 — K8s Volume + Env Wiring

* [ ] Add volume mounts:

  * [ ] `/status`
  * [ ] `/artifacts`
* [ ] Pass env vars:

  * [ ] RUN_ID
  * [ ] JOB_TYPE
  * [ ] STATUS_PATH
  * [ ] ARTIFACT_DIR
* [ ] Ensure every job gets correct paths

---

## Day 4 — Training Runtime Status Writes

* [ ] Update training command/script:

  * [ ] Initialize status file
  * [ ] Update epoch/loss/accuracy
  * [ ] Write checkpointPath
  * [ ] Write artifactPath
* [ ] Write status multiple times during execution

---

## Day 5 — Status Reconciliation Loop

* [ ] Read `/status/<run-id>.json` for active runs
* [ ] Parse into `RunStatus`
* [ ] Maintain last-seen snapshot per run
* [ ] Implement diff detection logic

---

## Day 6 — Runtime-Derived Events (Training)

* [ ] Emit based on diff:

  * [ ] TRAINING_PROGRESS (epoch change)
  * [ ] CHECKPOINT_SAVED (checkpointPath change)
  * [ ] ARTIFACT_WRITTEN (artifactPath appears)
* [ ] Avoid duplicate event emission

---

## Day 7 — Inference + Evaluation Status

* [ ] Batch inference writes:

  * [ ] processedBatches
  * [ ] latency
  * [ ] resultsPath
* [ ] Emit:

  * [ ] INFERENCE_PROGRESS
  * [ ] RESULTS_WRITTEN
* [ ] Evaluation writes:

  * [ ] validation metrics
* [ ] Emit:

  * [ ] EVALUATION_PROGRESS
  * [ ] EVALUATION_COMPLETED

---

# 📅 Week 2 — AI Scheduling + Recovery

## Day 8 — GPU Class / Accelerator Support

* [ ] Add `gpuClass` to JobSpec
* [ ] Add node capability metadata
* [ ] Placement checks:

  * [ ] gpuCount
  * [ ] gpuClass
* [ ] Expose assigned GPU in API/status

---

## Day 9 — Placement Policy Upgrade

* [ ] Implement policy:

  * [ ] best-fit OR bin-pack
* [ ] Emit placement metadata:

  * [ ] node
  * [ ] gpuCount
  * [ ] gpuClass
* [ ] Show placement in `/v1/jobs/{id}`

---

## Day 10 — Queue Isolation

* [ ] Define queues:

  * [ ] training
  * [ ] inference
  * [ ] evaluation
* [ ] Apply queue-based scheduling
* [ ] Show queue in API

---

## Day 11 — Quotas / Admission Control

* [ ] Add limits:

  * [ ] max GPUs per queue
  * [ ] max concurrent jobs
* [ ] Emit:

  * [ ] ADMISSION_DENIED
  * [ ] QUOTA_DEFERRED

---

## Day 12 — Checkpoint-Aware Retry ⭐

* [ ] Read latest checkpoint from status/events
* [ ] Inject checkpoint into retry job
* [ ] Emit:

  * [ ] RETRY_FROM_CHECKPOINT

---

## Day 13 — Timeout + Stale Status Detection

* [ ] Use `updatedAt` from status
* [ ] Detect:

  * [ ] stale status
  * [ ] heartbeat timeout
* [ ] Emit failure reasons:

  * [ ] status_stale
  * [ ] job_timeout

---

## Day 14 — Failure Classification

* [ ] Classify failures:

  * [ ] infra_error
  * [ ] resource_unavailable
  * [ ] status_stale
  * [ ] heartbeat_stale
  * [ ] workload_error
* [ ] Attach classification to events + runs

---

# 📅 Week 3 — AI Platform Visibility + Packaging

## Day 15 — AI Run Inspection API ⭐

* [ ] Extend `/v1/jobs/{id}`:

  * [ ] latest epoch
  * [ ] loss / accuracy
  * [ ] processed batches
  * [ ] checkpointPath
  * [ ] artifactPath
  * [ ] resultsPath
  * [ ] assigned node / GPU
* [ ] Source from reconciled runtime status

---

## Day 16 — System Summary (AI-Aware)

* [ ] Add:

  * [ ] running training jobs
  * [ ] running inference jobs
  * [ ] GPUs requested / allocated
  * [ ] retries triggered
  * [ ] checkpoints saved
  * [ ] artifacts written
  * [ ] stale status incidents

---

## Day 17 — Demo Scripts

* [ ] demo-training.sh
* [ ] demo-inference.sh
* [ ] demo-failure-retry.sh
* [ ] Each demo shows:

  * [ ] job submission
  * [ ] status updates
  * [ ] events
  * [ ] K8s jobs/pods
  * [ ] logs

---

## Day 18 — README Rewrite ⭐

* [ ] Emphasize:

  * [ ] AI workload orchestration
  * [ ] runtime status reconciliation
  * [ ] GPU-aware scheduling
  * [ ] checkpoint + artifact lifecycle
* [ ] Add section:

  * [ ] "Runtime Status Reconciliation"

---

## Day 19 — Architecture + Screenshots

* [ ] Add diagram:

  * workload → status → reconciler → events/API
* [ ] Add screenshots:

  * [ ] job lifecycle API response
  * [ ] training progress
  * [ ] scheduler summary
  * [ ] Kubernetes job/pod

---

## Day 20 — Resume + Interview Story ⭐

* [ ] Create 2 resume bullets
* [ ] Prepare:

  * [ ] 30-second explanation
  * [ ] 2-minute deep explanation
* [ ] Key points:

  * [ ] control plane vs runtime separation
  * [ ] status reconciliation model
  * [ ] checkpoint-aware recovery

---

## Day 21 — Final Cleanup + Release

* [ ] Deduplicate events
* [ ] Handle malformed status files
* [ ] Stabilize demo flows
* [ ] Clean logs + code
* [ ] Tag version:

  * [ ] v2-ai-infra

---

# 🚀 End State

Veriflow becomes:

* AI workload orchestration platform
* GPU-aware scheduler
* runtime status reconciliation system
* checkpoint-aware recovery engine
* AI lifecycle observability platform

---
