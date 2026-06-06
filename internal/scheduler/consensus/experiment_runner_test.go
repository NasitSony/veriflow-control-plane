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
			"valid_transition_one_byzantine_no",
			total,
			Coordinator{
				QuorumSize: 3,
				Validators: []Validator{
					HonestValidator{"v1"},
					HonestValidator{"v2"},
					HonestValidator{"v3"},
					ByzantineValidator{"v4", AlwaysNo},
				},
			},
			validTransition,
		),

		runExperiment(
			"invalid_completion_one_byzantine_yes",
			total,
			Coordinator{
				QuorumSize: 3,
				Validators: []Validator{
					HonestValidator{"v1"},
					HonestValidator{"v2"},
					HonestValidator{"v3"},
					ByzantineValidator{"v4", AlwaysYes},
				},
			},
			invalidTransition,
		),

		runExperiment(
			"valid_transition_one_delayed",
			total,
			Coordinator{
				QuorumSize: 3,
				Validators: []Validator{
					HonestValidator{"v1"},
					HonestValidator{"v2"},
					HonestValidator{"v3"},
					ByzantineValidator{"v4", Delayed},
				},
			},
			validTransition,
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
