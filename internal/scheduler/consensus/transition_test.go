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

	ok := c.Propose(
		LifecycleTransition{
			JobID: "job-1",
			From:  StatePending,
			To:    StateSucceeded,
		},
	)

	if ok {
		t.Fatal("expected rejection")
	}
}