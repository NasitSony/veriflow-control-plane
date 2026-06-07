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