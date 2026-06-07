package consensus

import (
	"encoding/csv"
	"fmt"
	"os"
	"testing"
	"time"
)

type ExperimentResult struct {
	Name       string
	N          int
	Byzantine  int
	Quorum     int
	Total      int
	Committed  int
	Rejected   int
	AvgLatency time.Duration
}

type BaselineResult struct {
	Name      string
	Total     int
	Committed int
	Rejected  int
}

func runExperiment(
	name string,
	n int,
	byzantine int,
	quorum int,
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
		N:          n,
		Byzantine:  byzantine,
		Quorum:     quorum,
		Total:      total,
		Committed:  committed,
		Rejected:   rejected,
		AvgLatency: totalLatency / time.Duration(total),
	}
}

func writeResultsCSV(results []ExperimentResult) error {

	err := os.MkdirAll("docs/rfc-0006", 0755)
	if err != nil {
		return err
	}

	filename := fmt.Sprintf(
		"results_%s.csv",
		time.Now().Format("20060102_150405"),
	)

	file, err := os.Create(filename)

	if err != nil {
		return err
	}

	defer file.Close()

	writer := csv.NewWriter(file)

	if err := writer.Write([]string{
		"scenario",
		"n",
		"byzantine",
		"quorum",
		"total",
		"committed",
		"rejected",
		"avg_latency_ns",
	}); err != nil {
		return err
	}

	for _, r := range results {
		if err := writer.Write([]string{
			r.Name,
			fmt.Sprintf("%d", r.N),
			fmt.Sprintf("%d", r.Byzantine),
			fmt.Sprintf("%d", r.Quorum),
			fmt.Sprintf("%d", r.Total),
			fmt.Sprintf("%d", r.Committed),
			fmt.Sprintf("%d", r.Rejected),
			fmt.Sprintf("%d", r.AvgLatency.Nanoseconds()),
		}); err != nil {
			return err
		}
	}

	writer.Flush()

	if err := writer.Error(); err != nil {
		return err
	}

	return nil
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

	falseSuccessTransition := LifecycleTransition{
		JobID: "job-false-success",
		From:  StateRunning,
		To:    StateSucceeded,
		Observation: PodObservation{
			Phase:    PodFailed,
			ExitCode: 1,
		},
	}

	falseFailureTransition := LifecycleTransition{
		JobID: "job-false-failure",
		From:  StateRunning,
		To:    StateFailed,
		Observation: PodObservation{
			Phase:    PodSucceeded,
			ExitCode: 0,
		},
	}

	results := []ExperimentResult{
		runExperiment(
			"valid_transition_7_nodes_2_byzantine_no",
			7,
			2,
			5,
			total,
			Coordinator{
				QuorumSize: 5,
				Validators: buildValidators(5, 2, AlwaysNo),
			},
			validTransition,
		),

		runExperiment(
			"valid_transition_10_nodes_3_byzantine_no",
			10,
			3,
			7,
			total,
			Coordinator{
				QuorumSize: 7,
				Validators: buildValidators(7, 3, AlwaysNo),
			},
			validTransition,
		),

		runExperiment(
			"invalid_completion_7_nodes_2_byzantine_yes",
			7,
			2,
			5,
			total,
			Coordinator{
				QuorumSize: 5,
				Validators: buildValidators(5, 2, AlwaysYes),
			},
			invalidTransition,
		),

		runExperiment(
			"invalid_completion_10_nodes_3_byzantine_yes",
			10,
			3,
			7,
			total,
			Coordinator{
				QuorumSize: 7,
				Validators: buildValidators(7, 3, AlwaysYes),
			},
			invalidTransition,
		),

		runExperiment(
			"false_success_recovery_protected",
			4,
			1,
			3,
			total,
			Coordinator{
				QuorumSize: 3,
				Validators: buildValidators(3, 1, AlwaysYes),
			},
			falseSuccessTransition,
		),
		runExperiment(
			"false_failure_duplicate_retry_protected",
			4,
			1,
			3,
			total,
			Coordinator{
				QuorumSize: 3,
				Validators: buildValidators(3, 1, AlwaysYes),
			},
			falseFailureTransition,
		),
		runExperiment(
			"valid_transition_4_nodes_1_byzantine_no",
			4,
			1,
			3,
			total,
			Coordinator{
				QuorumSize: 3,
				Validators: buildValidators(3, 1, AlwaysNo),
			},
			validTransition,
		),
		runExperiment(
			"valid_transition_7_nodes_2_byzantine_no",
			7,
			2,
			5,
			total,
			Coordinator{
				QuorumSize: 5,
				Validators: buildValidators(5, 2, AlwaysNo),
			},
			validTransition,
		),
		runExperiment(
			"valid_transition_10_nodes_3_byzantine_no",
			10,
			3,
			7,
			total,
			Coordinator{
				QuorumSize: 7,
				Validators: buildValidators(7, 3, AlwaysNo),
			},
			validTransition,
		),
		runExperiment(
			"invalid_completion_4_nodes_1_byzantine_yes",
			4,
			1,
			3,
			total,
			Coordinator{
				QuorumSize: 3,
				Validators: buildValidators(3, 1, AlwaysYes),
			},
			invalidTransition,
		),
		runExperiment(
			"invalid_completion_7_nodes_2_byzantine_yes",
			7,
			2,
			5,
			total,
			Coordinator{
				QuorumSize: 5,
				Validators: buildValidators(5, 2, AlwaysYes),
			},
			invalidTransition,
		),
		runExperiment(
			"invalid_completion_10_nodes_3_byzantine_yes",
			10,
			3,
			7,
			total,
			Coordinator{
				QuorumSize: 7,
				Validators: buildValidators(7, 3, AlwaysYes),
			},
			invalidTransition,
		),
		runExperiment(
			"valid_transition_7_nodes_3_byzantine_no",
			7,
			3,
			5,
			total,
			Coordinator{
				QuorumSize: 5,
				Validators: buildValidators(4, 3, AlwaysNo),
			},
			validTransition,
		),
		runExperiment(
			"valid_transition_10_nodes_4_byzantine_no",
			10,
			4,
			7,
			total,
			Coordinator{
				QuorumSize: 7,
				Validators: buildValidators(6, 4, AlwaysNo),
			},
			validTransition,
		),
	}

	fmt.Println("\nExperiment Results")
	fmt.Println("scenario,n,byzantine,quorum,total,committed,rejected,avg_latency")

	for _, r := range results {
		fmt.Printf(
			"%s,%d,%d,%d,%d,%d,%d,%v\n",
			r.Name,
			r.N,
			r.Byzantine,
			r.Quorum,
			r.Total,
			r.Committed,
			r.Rejected,
			r.AvgLatency,
		)
	}

	fmt.Printf("writing %d results\n", len(results))
	err := writeResultsCSV(results)

	if err != nil {
		t.Fatal(err)
	}
}

func unsafeCommit(t LifecycleTransition) bool {
	return true
}

func runUnsafeBaseline(
	name string,
	total int,
	transition LifecycleTransition,
) BaselineResult {
	committed := 0
	rejected := 0

	for i := 0; i < total; i++ {
		if unsafeCommit(transition) {
			committed++
		} else {
			rejected++
		}
	}

	return BaselineResult{
		Name:      name,
		Total:     total,
		Committed: committed,
		Rejected:  rejected,
	}
}

func TestUnsafeBaseline(t *testing.T) {
	total := 100

	falseSuccessTransition := LifecycleTransition{
		JobID: "job-false-success",
		From:  StateRunning,
		To:    StateSucceeded,
		Observation: PodObservation{
			Phase:    PodFailed,
			ExitCode: 1,
		},
	}

	result := runUnsafeBaseline(
		"unsafe_false_success_committed",
		total,
		falseSuccessTransition,
	)

	fmt.Println("\nUnsafe Baseline Results")
	fmt.Println("name,total,committed,rejected")
	fmt.Printf(
		"%s,%d,%d,%d\n",
		result.Name,
		result.Total,
		result.Committed,
		result.Rejected,
	)

	if result.Committed != total {
		t.Fatal("expected unsafe baseline to commit every proposal")
	}
}
