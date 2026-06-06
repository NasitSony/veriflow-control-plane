package consensus

import (
	"fmt"
	"testing"
	"time"
)

type ExperimentResult struct {
	Name       string
	Total      int
	Committed  int
	Rejected   int
	AvgLatency time.Duration
}

func runExperiment(
	name string,
	total int,
	coordinator Coordinator,
	transition LifecycleTransition,
) ExperimentResult {
	committed := 0
	rejected := 0
	var totalLatency time.Duration

	for i := 0; i < total; i++ {
		start := time.Now()
		decision := coordinator.Propose(transition)
		totalLatency += time.Since(start)

		if decision.Committed {
			committed++
		} else {
			rejected++
		}
	}

	return ExperimentResult{
		Name:       name,
		Total:      total,
		Committed:  committed,
		Rejected:   rejected,
		AvgLatency: totalLatency / time.Duration(total),
	}
}

func TestExperimentRunner(t *testing.T) {
	total := 100

	validTransition := LifecycleTransition{
		JobID: "job-valid",
		From:  StateRunning,
		To:    StateSucceeded,
		Observation: PodObservation{
			Phase:    PodSucceeded,
			ExitCode: 0,
		},
	}

	invalidTransition := LifecycleTransition{
		JobID: "job-invalid",
		From:  StateRunning,
		To:    StateSucceeded,
		Observation: PodObservation{
			Phase:    PodFailed,
			ExitCode: 1,
		},
	}

	results := []ExperimentResult{
		runExperiment(
			"valid_transition_7_nodes_2_byzantine_no",
			total,
			Coordinator{
				QuorumSize: 5,
				Validators: buildValidators(5, 2, AlwaysNo),
			},
			validTransition,
		),

		runExperiment(
			"valid_transition_10_nodes_3_byzantine_no",
			total,
			Coordinator{
				QuorumSize: 7,
				Validators: buildValidators(7, 3, AlwaysNo),
			},
			validTransition,
		),

		runExperiment(
			"invalid_completion_7_nodes_2_byzantine_yes",
			total,
			Coordinator{
				QuorumSize: 5,
				Validators: buildValidators(5, 2, AlwaysYes),
			},
			invalidTransition,
		),

		runExperiment(
			"invalid_completion_10_nodes_3_byzantine_yes",
			total,
			Coordinator{
				QuorumSize: 7,
				Validators: buildValidators(7, 3, AlwaysYes),
			},
			invalidTransition,
		),
	}

	fmt.Println("\nExperiment Results")
	fmt.Println("name,total,committed,rejected,avg_latency")

	for _, r := range results {
		fmt.Printf(
			"%s,%d,%d,%d,%v\n",
			r.Name,
			r.Total,
			r.Committed,
			r.Rejected,
			r.AvgLatency,
		)
	}
}
