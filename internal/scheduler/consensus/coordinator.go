package consensus

type Coordinator struct {
	Validators []Validator
	QuorumSize int
}

func (c *Coordinator) Propose(t LifecycleTransition) Decision {
	var votes []Vote

	for _, validator := range c.Validators {
		vote := validator.Validate(t)
		votes = append(votes, vote)

		if HasQuorum(votes, c.QuorumSize) {
			yes, no := countVotes(votes)

			return Decision{
				Transition: t,
				Committed:  true,
				Votes:      votes,
				YesVotes:   yes,
				NoVotes:    no,
				Reason:     "quorum reached",
			}
		}
	}

	yes, no := countVotes(votes)

	return Decision{
		Transition: t,
		Committed:  false,
		Votes:      votes,
		YesVotes:   yes,
		NoVotes:    no,
		Reason:     "quorum not reached",
	}
}

func countVotes(votes []Vote) (int, int) {
	yes := 0
	no := 0
	seen := map[string]bool{}

	for _, v := range votes {
		if seen[v.ValidatorID] {
			continue
		}

		seen[v.ValidatorID] = true

		if v.Accept {
			yes++
		} else {
			no++
		}
	}

	return yes, no
}
