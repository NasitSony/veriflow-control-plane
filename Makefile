SHELL := /bin/bash

DATABASE_URL ?= postgres://veriflow:veriflow@localhost:5436/veriflow?sslmode=disable
ADDR ?= :8080
QUEUE ?= default
SCHED_INTERVAL ?= 700ms
K8S_NAMESPACE ?= default

.PHONY: up down reset migrate api sched demo-success demo-fail jobs runs events k8s-check

up:
	@docker compose up -d
	@$(MAKE) migrate

down:
	@docker compose down

reset:
	@docker compose down -v
	@docker compose up -d
	@$(MAKE) migrate

migrate:
	@echo "==> applying migrations"
	@psql "$(DATABASE_URL)" -f migrations/001_init.sql
	@psql "$(DATABASE_URL)" -f migrations/002_retry_backoff.sql

api:
	@DATABASE_URL="$(DATABASE_URL)" ADDR="$(ADDR)" go run ./cmd/job-api

sched:
	@DATABASE_URL="$(DATABASE_URL)" QUEUE="$(QUEUE)" SCHED_INTERVAL="$(SCHED_INTERVAL)" K8S_NAMESPACE="$(K8S_NAMESPACE)" go run ./cmd/scheduler

demo-success:
	@./scripts/demo_success.sh "$(ADDR)"

demo-fail:
	@./scripts/demo_fail.sh "$(ADDR)"

jobs:
	@psql "$(DATABASE_URL)" -c "select job_id,state,queue,priority,max_retries,next_run_at,created_at from jobs order by created_at desc limit 10;"

runs:
	@psql "$(DATABASE_URL)" -c "select run_id,job_id,attempt,state,k8s_job_name,error_reason,created_at from runs order by created_at desc limit 20;"

events:
	@psql "$(DATABASE_URL)" -c "select ts,type,payload from events order by ts desc limit 30;"

k8s-check:
	@kubectl get nodes
