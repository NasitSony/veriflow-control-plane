package consensus

func ValidatePodObservation(
	t LifecycleTransition,
) bool {

	switch t.To {

	case StateSucceeded:
		return t.Observation.Phase == PodSucceeded &&
			t.Observation.ExitCode == 0

	case StateFailed:
		return t.Observation.Phase == PodFailed

	default:
		return true
	}
}
