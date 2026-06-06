package consensus

import (
	"fmt"
	"testing"
)

func TestByzantineExperiment(t *testing.T) {

	total := 100
	committed := 0

	c := Coordinator{
		QuorumSize: 3,
		Validators: []Validator{
			HonestValidator{"v1"},
			HonestValidator{"v2"},
			HonestValidator{"v3"},
			ByzantineValidator{"v4", AlwaysYes},
		},
	}

	for i := 0; i < total; i++ {

		decision := c.Propose(
			LifecycleTransition{
				JobID: "job",
				From:  StatePending,
				To:    StateSucceeded,

				Observation: PodObservation{
					Phase:    PodFailed,
					ExitCode: 1,
				},
			},
		)

		if decision.Committed {
			committed++
		}
	}

	fmt.Printf(
		"\nInvalid proposal commit rate: %d/%d\n",
		committed,
		total,
	)
}
