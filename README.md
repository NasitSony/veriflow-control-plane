# Veriflow — Kubernetes Job Orchestrator (Control Plane + Scheduler)

Veriflow is a small, runnable **AI/ML infrastructure-style job platform**: a control-plane HTTP API + a scheduler that dispatches workloads as **Kubernetes Jobs**, then reconciles runtime status back into Postgres with an **auditable event timeline**.

It’s intentionally minimal, but uses real production patterns: **idempotent submission**, **concurrency-safe claiming (SKIP LOCKED)**, **run attempts**, **retry backoff gating**, and **K8s reconciliation**.

## 🚀 One-Command Demo

Run a full end-to-end demo:

```bash
./scripts/demo.sh

---

## Architecture

```text
Client
  │  POST /v1/jobs  (Idempotency-Key)
  ▼
job-api (Go)
  │  writes jobs/spec to Postgres
  ▼
Postgres (jobs, runs, events)
  ▲
  │  claim (FOR UPDATE SKIP LOCKED) + create run attempt
  │  dispatch → create Kubernetes Job
  │  reconcile K8s Job status → RUNNING / SUCCEEDED / FAILED
  ▼
scheduler (Go)  ───────────────► Kubernetes Job / Pod
```

**Key tables**
- `jobs`: desired state, spec, priority, max retries, `next_run_at`
- `runs`: execution attempts (attempt=1,2,3...), lifecycle
- `events`: append-only timeline (`JOB_CREATED`, `RUN_RUNNING`, `RUN_SUCCEEDED`, …)

---

## Requirements

- Go (1.21+ recommended)
- Docker (for Postgres)
- `psql` (Postgres client)
- Kubernetes cluster + `kubectl`
  - On macOS: Colima works well: `brew install colima kubectl && colima start`

---

## Quickstart (5 minutes)

### 1) Start Postgres
```bash
make up
```

### 2) Run API + scheduler (two terminals)
Terminal A:
```bash
make api
```

Terminal B:
```bash
make sched
```

### 3) Submit a test job
```bash
make demo-success
```

### 4) Verify Kubernetes ran it
```bash
kubectl get jobs
kubectl get pods
# Replace JOB_NAME with the printed run-<uuid> job name:
kubectl logs -l job-name=JOB_NAME --tail=200
```

### 5) Verify DB lifecycle + events
```bash
make events
```

---

## API

### Create a job (idempotent)
`Idempotency-Key` ensures the same request won’t create duplicates.

```bash
curl -s -X POST localhost:8080/v1/jobs \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: hello-1" \
  -d '{
    "image":"busybox",
    "command":["sh","-c","echo hello-from-veriflow; sleep 1; echo done"],
    "max_retries": 0,
    "priority": 0,
    "queue": "default"
  }'
```

### Get a job
```bash
curl -s localhost:8080/v1/jobs/<job_id>
```

---

## Retries + backoff

Veriflow stores retry timing in `jobs.next_run_at` and only claims jobs whose `next_run_at <= now()`.

Try a failure demo (max_retries=2 ⇒ up to 3 attempts total):

```bash
make demo-fail
make events
make runs
```

Expected event shape:
- `RUN_FAILED`
- `JOB_RETRY_SCHEDULED` (payload includes delay/next_run_at/attempt)
- then a new `RUN_CREATED` for the next attempt

---

## Make targets

- `make up` / `make down` / `make reset`
- `make api` / `make sched`
- `make demo-success` / `make demo-fail`
- `make events` / `make runs` / `make jobs`

---

## Notes for hiring managers

This project demonstrates:
- **Control-plane design:** HTTP API + DB-backed state machine
- **Safe concurrency:** `FOR UPDATE SKIP LOCKED` claiming
- **Kubernetes execution:** workloads run as `batch/v1.Job`
- **Reconciliation loop:** K8s status → persisted lifecycle
- **Auditability:** append-only event timeline

---

## License
MIT
