package consensus

import "fmt"

func LogDecision(d Decision) {

	fmt.Printf(
		"\n[PROPOSAL] Job=%s %s->%s\n",
		d.Transition.JobID,
		d.Transition.From,
		d.Transition.To,
	)

	for _, vote := range d.Votes {

		fmt.Printf(
			"[VOTE] validator=%s accept=%v reason=%s\n",
			vote.ValidatorID,
			vote.Accept,
			vote.Reason,
		)
	}

	fmt.Printf(
		"[DECISION] committed=%v reason=%s\n",
		d.Committed,
		d.Reason,
	)
}
