package consensus

import "testing"

func TestValidPodObservation(t *testing.T) {

	transition := LifecycleTransition{
		JobID: "job-1",
		From:  StateRunning,
		To:    StateSucceeded,
		Observation: PodObservation{
			Phase:    PodSucceeded,
			ExitCode: 0,
		},
	}

	if !ValidatePodObservation(transition) {
		t.Fatal("expected observation to be valid")
	}
}
