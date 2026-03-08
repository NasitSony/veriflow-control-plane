**Veriflow Runbook**

This runbook describes how to start the Veriflow AI infrastructure control-plane, submit a job, and observe its lifecycle end-to-end.

**📦 Prerequisites**

Ensure the following are installed:
- Docker + Docker Compose
- kubectl configured to access your Kubernetes cluster
- Ports 8080 and 5436 available

**🚀 1. Start Infrastructure (Postgres)**
From the project root:

```bash
docker compose up -d
docker compose ps
```
Verify Postgres is healthy before proceeding.

**🧠 2. Run Job API Service**
```bash
docker run --rm -it \
  --network host \
  -v "$PWD":/app \
  -w /app \
  -e DATABASE_URL="postgres://veriflow:veriflow@localhost:5436/veriflow?sslmode=disable" \
  golang:1.25-bookworm \
  /usr/local/go/bin/go run ./cmd/job-api
```

Expected log:
- job-api listening on :8080

**⚙️ 3. Run Scheduler Service**

Open a new terminal:
```bash
docker run --rm -it \
  --network host \
  -v "$PWD":/app \
  -w /app \
  -v "$HOME/.kube:/root/.kube:ro" \
  -e DATABASE_URL="postgres://veriflow:veriflow@localhost:5436/veriflow?sslmode=disable" \
  golang:1.25-bookworm \
  /usr/local/go/bin/go run ./cmd/scheduler
```

Expected log:
- scheduler started queue=default interval=700ms

**📨 4. Submit a Job**

Open another terminal:
```bash
curl -X POST localhost:8080/v1/jobs \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: demo-run-1" \
  -d '{
    "image":"busybox",
    "command":["sh","-c","echo hello-from-veriflow; sleep 2; echo done"],
    "max_retries": 0
  }'
```

You should receive a JSON response containing the job ID.

**☸️ 5. Observe Kubernetes Execution**

List jobs:
- kubectl get jobs
- List pods:
```bash
kubectl get pods
```

View pod logs:
```bash
kubectl logs <pod-name>
```
Expected output:
```bash
hello-from-veriflow
done
bash

**🗄️ 6. Inspect Job Lifecycle in Database**

Recent Runs
```bash
docker compose exec -T postgres psql -U veriflow -d veriflow -c \
"select state, k8s_job_name, created_at from runs order by created_at desc limit 5;"
```

Lifecycle Events
```bash
docker compose exec -T postgres psql -U veriflow -d veriflow -c \
"select ts, type from events order by ts desc limit 10;"
```

Expected event chain:
```bash
JOB_CREATED
RUN_CREATED
JOB_SCHEDULED
RUN_RUNNING
JOB_SUCCEEDED
```


**🧹 7. Reset System State**

Clear old records:
```bash
docker compose exec -T postgres psql -U veriflow -d veriflow -c \
"truncate events, runs, jobs restart identity;"
```

**🛑 8. Stop Services**

Stop Postgres:
```bash
docker compose down
```

Stop API and scheduler with Ctrl + C.

**✅ End-to-End Success Criteria**

The system is functioning correctly when:
- API accepts job submission
- Scheduler dispatches job
- Kubernetes Job completes
- Database reflects correct lifecycle events

**🧭 Project Summary**

Veriflow is a control-plane service that:
- Accepts job submissions via REST API
- Persists lifecycle state in Postgres
- Schedules workloads via a dispatcher
- Executes workloads as Kubernetes Jobs
- Reconciles execution state to completion
