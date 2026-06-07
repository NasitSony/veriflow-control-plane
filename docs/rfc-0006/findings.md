# Findings

## Finding 1: Valid lifecycle transitions commit within the Byzantine threshold

For 4-, 7-, and 10-node configurations, valid lifecycle transitions achieved 100% commit rates when Byzantine participation remained within the tolerated threshold.

## Finding 2: Invalid lifecycle transitions are rejected

Invalid completion proposals failed to commit across all tested configurations.

## Finding 3: False success attacks are prevented

Transitions that incorrectly marked failed workloads as SUCCEEDED were rejected, preserving workload recovery.

## Finding 4: False failure attacks are prevented

Transitions that incorrectly marked successful workloads as FAILED were rejected, preventing unnecessary retries and duplicate execution.

## Finding 5: Unsafe lifecycle coordination is vulnerable

In the unsafe baseline, invalid lifecycle transitions committed successfully, demonstrating the risk of single-scheduler lifecycle updates.

## Finding 6: Byzantine threshold violations prevent commit

When Byzantine participation exceeded the tolerated threshold, valid lifecycle transitions failed to reach quorum and were not committed.

Examples:

* 7 nodes, 3 Byzantine, quorum 5 → 0% commit rate
* 10 nodes, 4 Byzantine, quorum 7 → 0% commit rate

This behavior is consistent with the protocol's quorum assumptions.


# RFC-0006 Findings

## Overview

This document records observations from the Byzantine-Resilient Lifecycle Coordination prototype implemented in Veriflow.

The goal is to understand whether quorum-backed lifecycle validation can improve orchestration correctness under Byzantine scheduler behavior.

---

## Finding 1: Valid lifecycle transitions commit within the Byzantine threshold

Experiments with 4, 7, and 10 scheduler validators showed that valid lifecycle transitions successfully committed when Byzantine participation remained within the tolerated threshold.

Observed configurations:

* 4 validators, 1 Byzantine, quorum 3
* 7 validators, 2 Byzantine, quorum 5
* 10 validators, 3 Byzantine, quorum 7

Result:

* Valid transitions committed in all runs.

Observation:

The protocol preserved liveness for valid lifecycle updates when the number of Byzantine validators remained within the expected fault tolerance assumptions.

---

## Finding 2: Invalid lifecycle transitions are rejected

Experiments evaluated invalid completion proposals where Byzantine validators attempted to approve incorrect lifecycle transitions.

Result:

* Invalid transitions were rejected in all runs.
* No invalid completion proposal reached quorum.

Observation:

Independent validation by honest schedulers prevents incorrect lifecycle state from being committed.

---

## Finding 3: False success attacks are prevented

Scenario:

A Byzantine scheduler proposed:

RUNNING -> SUCCEEDED

while the workload observation indicated:

* PodFailed
* ExitCode = 1

Result:

* 0 successful commits
* 100 rejected proposals

Observation:

Quorum-backed validation prevented failed workloads from being incorrectly marked as completed.

Potential impact:

* Preserves recovery behavior
* Prevents premature cleanup
* Prevents incorrect success reporting

---

## Finding 4: False failure attacks are prevented

Scenario:

A Byzantine scheduler proposed:

RUNNING -> FAILED

while the workload observation indicated:

* PodSucceeded
* ExitCode = 0

Result:

* 0 successful commits
* 100 rejected proposals

Observation:

The protocol prevented successful workloads from being incorrectly marked as failed.

Potential impact:

* Prevents unnecessary retries
* Prevents duplicate execution
* Prevents duplicate resource allocation

---

## Finding 5: Unsafe lifecycle coordination is vulnerable

Baseline experiment:

A single scheduler was allowed to commit lifecycle transitions without quorum validation.

Result:

* False success proposal committed in 100 out of 100 runs.

Observation:

Without independent validation, incorrect lifecycle state can be committed directly.

This experiment motivates the need for quorum-backed lifecycle coordination.

---

## Finding 6: Byzantine threshold violations prevent quorum formation

Experiments intentionally exceeded the tolerated Byzantine threshold.

Observed configurations:

* 7 validators, 3 Byzantine
* 10 validators, 4 Byzantine

Result:

* Valid lifecycle transitions failed to commit.
* Quorum could not be reached.

Observation:

The current protocol relies on the configured quorum assumptions. When Byzantine participation exceeds the tolerated threshold, liveness is lost.

---

## Finding 7: Kubernetes integration path exists

A PodObservation adapter was implemented to derive protocol observations from workload state.

Observation:

The lifecycle validation protocol can consume Kubernetes-derived workload observations instead of relying solely on simulation inputs.

This provides a path toward future integration with the Veriflow scheduler and Kubernetes control plane.

---

## Current Limitations

The current prototype does not yet include:

* leader rotation
* Byzantine network communication
* cryptographic signatures
* HotStuff-style consensus
* MVBA integration
* distributed replica networking
* production Kubernetes deployment

The current implementation should be viewed as a lifecycle validation prototype rather than a full Byzantine consensus system.

---

## Future Work

Potential extensions include:

* replica-to-replica networking
* signed votes
* leader election
* HotStuff integration
* MVBA integration
* multi-cluster coordination
* large-scale workload experiments
* Kubernetes-backed evaluation
