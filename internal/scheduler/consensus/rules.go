package consensus

func IsValidTransition(
	from LifecycleState,
	to LifecycleState,
) bool {

	switch from {

	case StatePending:
		return to == StateRunning

	case StateRunning:
		return to == StateSucceeded ||
			to == StateFailed

	default:
		return false
	}
}
