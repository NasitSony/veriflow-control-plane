package consensus

import "testing"

func TestCoordinatorCommit(t *testing.T) {

	c := Coordinator{
		QuorumSize: 3,
		Validators: []Validator{
			StaticValidator{"v1", true},
			StaticValidator{"v2", true},
			StaticValidator{"v3", true},
			StaticValidator{"v4", false},
		},
	}

	ok := c.Propose(
		LifecycleTransition{
			JobID: "job-1",
		},
	)

	if !ok {
		t.Fatal("expected commit")
	}
}

func TestCoordinatorReject(t *testing.T) {

	c := Coordinator{
		QuorumSize: 3,
		Validators: []Validator{
			StaticValidator{"v1", true},
			StaticValidator{"v2", true},
			StaticValidator{"v3", false},
			StaticValidator{"v4", false},
		},
	}

	ok := c.Propose(
		LifecycleTransition{
			JobID: "job-1",
		},
	)

	if ok {
		t.Fatal("expected rejection")
	}
}
