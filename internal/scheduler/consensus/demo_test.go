package consensus

import "testing"

func TestProtocolDemo(t *testing.T) {

	c := Coordinator{
		QuorumSize: 3,
		Validators: []Validator{
			HonestValidator{"v1"},
			HonestValidator{"v2"},
			HonestValidator{"v3"},
			ByzantineValidator{"v4", AlwaysNo},
		},
	}

	decision := c.Propose(
		LifecycleTransition{
			JobID: "job-42",
			From:  StateRunning,
			To:    StateSucceeded,
			Observation: PodObservation{
				Phase:    PodSucceeded,
				ExitCode: 0,
			},
		},
	)

	LogDecision(decision)
}
