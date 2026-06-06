package consensus

import "testing"

func TestCoordinatorCommit(t *testing.T) {
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
		t.Fatal("expected commit")
	}
}

func TestCoordinatorReject(t *testing.T) {
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
		t.Fatal("expected rejection")
	}
}
