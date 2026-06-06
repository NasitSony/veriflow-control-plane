package consensus

func HasQuorum(votes []Vote, quorum int) bool {
	yes := 0
	seen := map[string]bool{}

	for _, v := range votes {
		if seen[v.ValidatorID] {
			continue
		}
		seen[v.ValidatorID] = true

		if v.Accept {
			yes++
		}
	}

	return yes >= quorum
}
