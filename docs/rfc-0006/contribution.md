## 1 What problem does this solve?
Current AI workload orchestrators assume schedulers are crash-fault tolerant.
This work explores lifecycle coordination under Byzantine scheduler behavior.

## 2 What is the technical idea?

Lifecycle transitions require quorum-backed validation
based on independently observed pod state.

## 3 What evidence supports it?

Valid transitions commit.
Invalid transitions are rejected.
False success and false failure attacks are prevented.
Unsafe baselines commit incorrect lifecycle state.


Day 1:
Introduction
Motivation
Threat Model

Day 2:
System Model
Protocol Design

Day 3:
Implementation

Day 4:
Evaluation

Day 5:
Related Work
Conclusion

What is the overhead compared to ordinary scheduling?
How often would incorrect lifecycle decisions occur without quorum validation?

