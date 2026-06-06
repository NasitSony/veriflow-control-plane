package consensus

import (
	"fmt"
	"testing"
	"time"
)

func TestInvalidTransitionRejectedWithOneByzantineAlwaysYes(t *testing.T) {
	c := Coordinator{
		QuorumSize: 3,
		Validators: []Validator{
			HonestValidator{"v1"},
			HonestValidator{"v2"},
			HonestValidator{"v3"},
			ByzantineValidator{"v4", AlwaysYes},
		},
	}

	decision := c.Propose(LifecycleTransition{
		JobID: "job-1",
		From:  StatePending,
		To:    StateSucceeded,
	})

	if decision.Committed {
		t.Fatal("expected invalid transition to be rejected despite one Byzantine YES")
	}
}

func TestValidTransitionCommitsWithOneByzantineAlwaysNo(t *testing.T) {
	c := Coordinator{
		QuorumSize: 3,
		Validators: []Validator{
			HonestValidator{"v1"},
			HonestValidator{"v2"},
			HonestValidator{"v3"},
			ByzantineValidator{"v4", AlwaysNo},
		},
	}

	decision := c.Propose(LifecycleTransition{
		JobID: "job-1",
		From:  StateRunning,
		To:    StateSucceeded,
		Observation: PodObservation{
			Phase:    PodSucceeded,
			ExitCode: 0,
		},
	})

	if !decision.Committed {
		t.Fatal("expected valid transition to commit with 3 honest YES votes")
	}
}
func TestDelayedValidator(t *testing.T) {
	c := Coordinator{
		QuorumSize: 3,
		Validators: []Validator{
			HonestValidator{"v1"},
			HonestValidator{"v2"},
			HonestValidator{"v3"},
			ByzantineValidator{"v4", Delayed},
		},
	}

	start := time.Now()

	decision := c.Propose(LifecycleTransition{
		JobID: "job-1",
		From:  StateRunning,
		To:    StateSucceeded,
		Observation: PodObservation{
			Phase:    PodSucceeded,
			ExitCode: 0,
		},
	})

	duration := time.Since(start)

	if !decision.Committed {
		t.Fatal("expected valid transition to commit with 3 honest YES votes")
	}

	fmt.Printf(
		"latency=%v committed=%v yes=%d no=%d\n",
		duration,
		decision.Committed,
		decision.YesVotes,
		decision.NoVotes,
	)
}
