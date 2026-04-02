# Veriflow — Runtime-Aware AI Workload Orchestrator

Veriflow is a Kubernetes-based AI workload orchestrator that implements a **control-plane + scheduler architecture** for running training-style jobs with runtime awareness, failure recovery, and checkpoint-aware retry.

It treats AI workloads as **distributed systems problems** — where scheduling, correctness, and failure handling are first-class concerns — rather than simple API execution.


## 🧠 Core Idea

Modern AI systems are not just model pipelines — they are **distributed systems**.

Veriflow models:

- job lifecycle as a **state machine**
- execution as **Kubernetes workloads**
- runtime signals as **control-plane inputs**
- recovery as **checkpoint-aware retry**


## ⚡ Key Features

- Idempotent job submission (``` Idempotency-Key ```)
- Concurrency-safe job claiming (``` FOR UPDATE SKIP LOCKED ```)
- Queue + priority-based scheduling
- GPU-aware placement decisions
- Retry with backoff (``` next_run_at ```)
- Timeout handling for long-running jobs
- Event-sourced lifecycle tracking (jobs, runs, events)
- Kubernetes-based execution (``` batch/v1.Job ```)
- **Runtime-aware reconciliation (training progress, checkpoints)**
- **Checkpoint-aware retry and resume**


## 🏗 Architecture

```
Client
  │  POST /v1/jobs  (Idempotency-Key)
  ▼
job-api (Go)
  │  writes jobs/spec to Postgres
  ▼
Postgres (jobs, runs, events)
  ▲
  │  claim (FOR UPDATE SKIP LOCKED)
  │  create run attempt
  │  dispatch → Kubernetes Job
  │  reconcile runtime + K8s state
  ▼
scheduler (Go) ───────────► Kubernetes Job / Pod
```

This mirrors real-world AI infra where:

- control plane = state + scheduling
- data plane = execution (K8s jobs)

## 🔄 Runtime-Aware Lifecycle

A training job now produces:

```
JOB_SUBMITTED
JOB_SCHEDULED
RUN_CREATED
PLACEMENT_SELECTED
DISPATCH_REQUESTED
POD_RUNNING

TRAINING_PROGRESS
CHECKPOINT_SAVED

RUN_FAILED
RETRY_TRIGGERED
RUN_CREATED (attempt 2)
TRAINING_RESUMED

...
JOB_SUCCEEDED
```

## 🧪 Evaluation

### 1. Multi-Scheduler Validation

Veriflow was tested with two concurrent scheduler instances (`sched-a`, `sched-b`) operating on the same Postgres-backed queue.

- Dispatch distribution:
  - sched-a: 3 dispatches
  - sched-b: 3 dispatches
- No duplicate dispatch of the same run observed

This demonstrates that `FOR UPDATE SKIP LOCKED` enables safe concurrent job claiming without duplicate execution, while allowing multiple schedulers to share work.

---

### 2. Burst Submission Handling
- Submitted **20 concurrent jobs**
- Scheduler continued:
  - claiming jobs
  - dispatching workloads
  - emitting runtime events
- No crashes or deadlocks observed

Veriflow maintains stable control-plane behavior under burst submission.

### 3. Runtime-Aware Failure Handling
- Jobs emit:
  - ```TRAINING_PROGRESS```
  - ```CHECKPOINT_SAVED```
- Failures detected via container exit codes
- Events show consistent failure propagation:
 
``` RUN_FAILED → (retry OR terminal failure) ```


### 4. Checkpoint-Aware Retry & Resume

For retry-enabled jobs:

- checkpoint persisted:

  ``` latest_checkpoint_uri = /artifacts/ckpt-2```

- retry scheduled:

  ``` RETRY_TRIGGERED```

- resumed execution:

 ``` TRAINING_RESUMED```

- final success:

  ``` JOB_SUCCEEDED```

Veriflow resumes failed training runs from the latest checkpoint instead of restarting from scratch.

### 5. Failure Storm Behavior
- Multiple jobs failed concurrently
- Scheduler:
  - continued processing
  - emitted consistent events
  - avoided duplicate claims

System remains consistent under failure bursts.

### GPU Placement Validation
- verified best-fit placement selects tighter-fit nodes when multiple nodes satisfy a request
- verified exact GPU type filtering (`requested_gpu_type = A100`)
- verified minimum per-GPU memory filtering (`min_gpu_memory_mb`)
- verified explicit deferred placement reasons for unsatisfied constraints

## 📊 Event Summary (example run)
- 40+ RUN_CREATED
- 30+ CHECKPOINT_SAVED
- 20+ RUN_FAILED
- successful retry + resume observed


## 🧬 Evolution

### v0.1 — Job API
- basic job submission
- Postgres persistence

### v0.2 — Scheduler + State Machine
- job claiming
- run lifecycle

### v0.3 — Kubernetes Execution
- dispatch as K8s Jobs
- reconcile job state

### v0.4 — Retry + Backoff
- next_run_at
- multi-attempt runs

### v0.5 — Observability
- event-sourced lifecycle
- logs + status tracking

### v0.6 — Runtime-Aware AI Infrastructure 🚀
- runtime status ingestion
- training progress tracking
- checkpoint persistence
- checkpoint-aware retry + resume

###  v0.7 - GPU-Aware Scheduling 🚀

Veriflow implements constraint-based GPU scheduling with device-level resource accounting and explainable placement decisions.

#### Job-Level GPU Constraints

Jobs can specify GPU requirements via the API:

- `gpuCount` — number of GPUs required
- `gpuType` (optional) — exact GPU type (e.g., A100, L4)
- `minGpuMemoryMb` (optional) — minimum per-GPU memory requirement

Example:

```json
{
  "gpuCount": 2,
  "gpuType": "A100",
  "minGpuMemoryMb": 30000
}```

## 🚀 One-Command Demo

This runs a full end-to-end workflow locally:

```bash
./scripts/demo.sh
```

This script will:
- submit a job
- Kubernetes execution
- log output
- DB lifecycle + event timeline

## ⚙️ Quickstart

```bash
make up
make api
make sched
make demo-success
make events
```

## 📡 API Example

```bash
curl -X POST http://localhost:8080/v1/jobs \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: test-1" \
  -d '{
    "image": "busybox",
    "jobType": "training",
    "gpuCount": 1,
    "datasetUri": "s3://data/sample"
  }'
```
---


## 📘 Operations Guide
See [RUNBOOK.md](RUNBOOK.md) for full operational steps.



## 🧠 What This Project Demonstrates

- Control-plane design for distributed systems
- Concurrency-safe scheduling using DB primitives
- Kubernetes-based workload orchestration
- Runtime-aware system reconciliation
- Failure detection and retry semantics
- Checkpoint-aware recovery
- AI infrastructure patterns (training workloads)

## 💥 Key Insight

AI systems should be treated as fault-tolerant distributed systems, not just model execution pipelines.

See **RUNBOOK.md** for full step-by-step instructions.
---


## License
MIT
