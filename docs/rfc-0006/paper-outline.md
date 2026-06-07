1. Introduction
2. Motivation
Current AI orchestrators assume crash faults.
Schedulers may report incorrect lifecycle state.
False completion can suppress recovery.
False failure can trigger duplicate execution.
3. Threat Model
4. System Model
5. Protocol Design
Scheduler A proposes transition
        ↓
Validators verify pod state
        ↓
Votes collected
        ↓
Quorum reached
        ↓
Lifecycle committed
6. Implementation
✓ LifecycleTransition
✓ PodObservation
✓ HonestValidator
✓ ByzantineValidator
✓ Coordinator
✓ Quorum logic
✓ Pod adapter
7. Evaluation
✓ Valid transitions
✓ Invalid transitions
✓ False success protection
✓ False failure protection
✓ Unsafe baseline
✓ Threshold scaling
✓ Threshold violation
Valid transitions commit.
Invalid transitions rejected.
False success protection.
False failure protection.
Unsafe baseline comparison.
Threshold scaling.
Threshold violation behavior.
8. Limitations
9. Future Work
10. Conclusion

# Paper Writing Order

## 1. Introduction

Problem:

Current AI workload orchestrators typically assume crash-fault behavior from scheduler components.

Question:

What happens if a scheduler incorrectly reports workload completion, failure, or lifecycle state?

Observation:

Incorrect lifecycle transitions may suppress recovery, trigger duplicate execution, or create inconsistent orchestration state.

Contribution:

We explore quorum-backed lifecycle validation for AI workload orchestration and evaluate its ability to prevent incorrect lifecycle commits under Byzantine scheduler behavior.

---

## 2. Motivation

Example 1:

A scheduler reports a failed workload as SUCCEEDED.

Potential consequences:

* Recovery is skipped.
* Checkpoints may be deleted.
* Monitoring systems receive incorrect status.

Example 2:

A scheduler reports a successful workload as FAILED.

Potential consequences:

* Duplicate retries.
* Duplicate resource allocation.
* Additional infrastructure cost.

These examples motivate independent validation and quorum-backed lifecycle coordination.

---

## 3. Threat Model

Assumptions:

* Up to f scheduler validators may behave Byzantine.
* Honest validators independently observe workload state.
* Kubernetes control plane is non-Byzantine.
* Cryptographic identities are not forged.

Adversary capabilities:

* Propose invalid transitions.
* Vote inconsistently.
* Report incorrect lifecycle outcomes.
* Delay or omit responses.

Goal:

Prevent incorrect lifecycle commits while preserving correct lifecycle progress.
