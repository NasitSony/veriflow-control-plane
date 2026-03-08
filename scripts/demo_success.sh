#!/usr/bin/env bash
set -euo pipefail

ADDR="${1:-:8080}"
BASE="http://localhost${ADDR}"
KEY="demo-success-$(date +%s)"

echo "==> submitting success job (Idempotency-Key: ${KEY})"
curl -s -X POST "${BASE}/v1/jobs" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: ${KEY}" \
  -d '{
    "image":"busybox",
    "command":["sh","-c","echo hello-from-veriflow; sleep 1; echo done"],
    "max_retries": 0
  }' | (command -v jq >/dev/null 2>&1 && jq || cat)

echo ""
echo "==> watch scheduler output for: DISPATCHED ... k8s_job=run-<uuid>"
