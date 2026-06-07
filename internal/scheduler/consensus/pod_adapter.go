package consensus

func BuildObservationFromPod(
	phase string,
	exitCode int,
) PodObservation {

	switch phase {

	case "Succeeded":
		return PodObservation{
			Phase:    PodSucceeded,
			ExitCode: exitCode,
		}

	case "Failed":
		return PodObservation{
			Phase:    PodFailed,
			ExitCode: exitCode,
		}
	}

	return PodObservation{
		Phase:    PodRunning,
		ExitCode: exitCode,
	}
}
