package consensus

import "testing"

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

	ok := c.Propose(LifecycleTransition{
		JobID: "job-1",
		From:  StatePending,
		To:    StateSucceeded,
	})

	if ok {
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

	ok := c.Propose(LifecycleTransition{
		JobID: "job-1",
		From:  StateRunning,
		To:    StateSucceeded,
	})

	if !ok {
		t.Fatal("expected valid transition to commit with 3 honest YES votes")
	}
}
