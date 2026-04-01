# Day 1 — GPU Scheduling Foundation

## Goal
Define the GPU demand/supply model and basic placement constraints for Veriflow.

## Scope
This day is about **resource modeling**, not advanced scheduling policy.
The objective is to give jobs enough metadata to express GPU requirements and give the scheduler enough metadata to decide whether a workload fits a node.

---

## Tasks

### 1. Extend job spec
Add the following fields to job spec and API payloads:

- `gpu_count`
- `gpu_type` (optional)
- `min_gpu_memory_mb`

Notes:
- `gpu_type` should be optional so the scheduler can support either exact-match or memory-based placement.
- Prefer `min_gpu_memory_mb` over exact memory requirements so jobs express constraints rather than hardcoded device profiles.

---

### 2. Extend node / cluster GPU model
Add GPU resource metadata to the scheduler’s node model.

Minimum fields per node:
- node name
- available GPU count
- GPU type
- memory per GPU

Preferred future-proof model:
- per-GPU inventory
- GPU type per device
- memory per device
- allocation / free state

---

### 3. Update database / schema
Update DB schema or persisted scheduler metadata so GPU requirements can be stored and read consistently.

Checklist:
- job spec supports new GPU fields
- scheduler can read these fields from persisted job state
- existing jobs still work with defaults

---

### 4. Update API + internal structs
Update:
- request payload structs
- internal scheduler structs
- serialization / deserialization
- validation rules

Validation expectations:
- `gpu_count >= 0`
- `min_gpu_memory_mb >= 0`
- `gpu_type` optional

---

### 5. Define placement matching rules
Implement the first version of GPU placement rules:

1. enough free GPU count
2. if `gpu_type` is set, require exact type match
3. if `min_gpu_memory_mb` is set, require GPUs meeting minimum memory
4. if no node fits, defer placement with explicit reason

---

### 6. Add explicit placement failure reasons
When placement fails, emit a reason that explains why.

Examples:
- `insufficient_gpu_count`
- `gpu_type_mismatch`
- `insufficient_gpu_memory`

This should be visible in logs and/or job events.

---

### 7. Smoke test
Validate:
- job with no GPU requirements still works
- job with `gpu_count=1` schedules correctly
- job with impossible requirements is deferred
- scheduler emits meaningful placement failure reasons

---

## Deliverable
By end of day:

- jobs can express GPU requirements
- cluster/node state can express GPU capacity
- scheduler can decide fit / no-fit using structured GPU metadata
- failed placement has explicit reason
- foundation is ready for future best-fit or topology-aware scheduling

---

## Time Split

- Schema / API changes: 2.5h
- Resource model updates: 2.5h
- Matching logic: 2h
- Smoke test / cleanup: 1h

---

## Non-Goals (Day 1)
Do **not** implement these yet:

- NVLink / topology awareness
- gang scheduling
- distributed training coordination
- advanced bin-packing
- fairness / preemption policies

These depend on the resource model and belong to later phases.

---

## Success Criteria
Day 1 is successful if:

- scheduler understands job GPU requirements
- scheduler understands node GPU capacity
- placement decisions are explainable
- no existing non-GPU workload path is broken