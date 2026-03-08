#!/usr/bin/env bash
set -euo pipefail

ADDR="${1:-:8080}"
BASE="http://localhost${ADDR}"
KEY="demo-fail-$(date +%s)"

echo "==> submitting failing job (Idempotency-Key: ${KEY})"
curl -s -X POST "${BASE}/v1/jobs" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: ${KEY}" \
  -d '{
    "image":"busybox",
    "command":["sh","-c","echo will-fail; exit 1"],
    "max_retries": 2
  }' | (command -v jq >/dev/null 2>&1 && jq || cat)

echo ""
echo "==> run: make events  (expect RUN_FAILED + JOB_RETRY_SCHEDULED)"
echo "==> run: make runs    (attempt should increment)"
