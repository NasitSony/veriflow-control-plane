package consensus

type LifecycleState string

const (
	StateRunning   LifecycleState = "RUNNING"
	StateSucceeded LifecycleState = "SUCCEEDED"
	StateFailed    LifecycleState = "FAILED"
)

type LifecycleTransition struct {
	JobID         string
	PodName       string
	From          LifecycleState
	To            LifecycleState
	CheckpointURI string
}
