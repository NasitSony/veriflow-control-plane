package consensus

type Coordinator struct {
	Validators []Validator
	QuorumSize int
}

func (c *Coordinator) Propose(t LifecycleTransition) Decision {
	var votes []Vote

	for _, validator := range c.Validators {
		votes = append(votes, validator.Validate(t))
	}

	committed := HasQuorum(votes, c.QuorumSize)

	reason := "quorum not reached"
	if committed {
		reason = "quorum reached"
	}

	return Decision{
		Transition: t,
		Committed:  committed,
		Votes:      votes,
		Reason:     reason,
	}
}
