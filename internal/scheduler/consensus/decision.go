package consensus

type Decision struct {
	Transition LifecycleTransition
	Committed  bool
	Votes      []Vote
	YesVotes   int
	NoVotes    int
	Reason     string
}
