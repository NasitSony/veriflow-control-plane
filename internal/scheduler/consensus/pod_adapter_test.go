package consensus

import "testing"

func TestBuildObservationFromSucceededPod(t *testing.T) {
	observation := BuildObservationFromPod("Succeeded", 0)

	if observation.Phase != PodSucceeded {
		t.Fatalf("expected PodSucceeded, got %s", observation.Phase)
	}

	if observation.ExitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", observation.ExitCode)
	}
}

func TestBuildObservationFromFailedPod(t *testing.T) {
	observation := BuildObservationFromPod("Failed", 1)

	if observation.Phase != PodFailed {
		t.Fatalf("expected PodFailed, got %s", observation.Phase)
	}
}
