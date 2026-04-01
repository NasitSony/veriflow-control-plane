package runtime

import "time"

type RunStatus struct {
	RunID     string    `json:"runId"`
	JobType   string    `json:"jobType"`
	Phase     string    `json:"phase"`
	Message   string    `json:"message,omitempty"`
	UpdatedAt time.Time `json:"updatedAt"`

	Training   *TrainingStatus   `json:"training,omitempty"`
	Inference  *InferenceStatus  `json:"inference,omitempty"`
	Evaluation *EvaluationStatus `json:"evaluation,omitempty"`
}

type TrainingStatus struct {
	Epoch          int      `json:"epoch,omitempty"`
	Step           int      `json:"step,omitempty"`
	Loss           *float64 `json:"loss,omitempty"`
	Accuracy       *float64 `json:"accuracy,omitempty"`
	CheckpointPath string   `json:"checkpointPath,omitempty"`
	ArtifactPath   string   `json:"artifactPath,omitempty"`
}

type InferenceStatus struct {
	ProcessedBatches int      `json:"processedBatches,omitempty"`
	LatencyMs        *float64 `json:"latencyMs,omitempty"`
	ResultsPath      string   `json:"resultsPath,omitempty"`
	ArtifactPath     string   `json:"artifactPath,omitempty"`
}

type EvaluationStatus struct {
	Step        int      `json:"step,omitempty"`
	Loss        *float64 `json:"loss,omitempty"`
	Accuracy    *float64 `json:"accuracy,omitempty"`
	ResultsPath string   `json:"resultsPath,omitempty"`
}
