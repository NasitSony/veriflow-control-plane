package consensus

type Decision struct {
	Transition LifecycleTransition
	Committed  bool
	Votes      []Vote
	Reason     string
}
