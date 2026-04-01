# Day 2 — GPU Placement Policy Upgrade

## Goal
Upgrade placement from first-fit to a simple best-fit policy using the GPU resource model introduced in Day 1.

## Tasks
- Replace first-fit node selection with full candidate evaluation
- Score fitting nodes by post-placement free GPU count
- Add optional tie-break by memory headroom
- Emit placement policy metadata in `PLACEMENT_SELECTED`
- Add logs for rejected candidates and selected score
- Validate with small scenarios showing tighter-fit placement

## Deliverable
- Scheduler selects the best fitting node rather than the first fitting node
- Placement decisions are explainable via logs and event payloads
- System preserves larger-capacity nodes when smaller nodes can satisfy the request

## Success Criteria
- Same resource constraints from Day 1 still work
- Placement is deterministic and explainable
- Best-fit behavior is visible in event/log output

## Non-Goals
- NVLink / topology awareness
- Gang scheduling
- Per-device GPU assignment
- Preemption / fairness