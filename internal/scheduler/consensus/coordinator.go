package consensus

type Coordinator struct {
	Validators []Validator
	QuorumSize int
}

func (c *Coordinator) Propose(
	t LifecycleTransition,
) bool {

	var votes []Vote

	for _, validator := range c.Validators {
		votes = append(
			votes,
			validator.Validate(t),
		)
	}

	return HasQuorum(
		votes,
		c.QuorumSize,
	)
}
