package consensus

import "testing"

func TestInvalidTransitionRejected(
	t *testing.T,
) {

	c := Coordinator{
		QuorumSize: 3,
		Validators: []Validator{
			HonestValidator{"v1"},
			HonestValidator{"v2"},
			HonestValidator{"v3"},
			HonestValidator{"v4"},
		},
	}

	decision := c.Propose(
		LifecycleTransition{
			JobID: "job-1",
			From:  StatePending,
			To:    StateSucceeded,
		},
	)

	if decision.Committed {
		t.Fatal("expected rejection")
	}
}
