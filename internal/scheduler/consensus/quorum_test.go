package consensus

import "testing"

func TestHasQuorumReturnsTrueWithEnoughYesVotes(t *testing.T) {
	votes := []Vote{
		{ValidatorID: "v1", JobID: "job-1", Accept: true},
		{ValidatorID: "v2", JobID: "job-1", Accept: true},
		{ValidatorID: "v3", JobID: "job-1", Accept: true},
		{ValidatorID: "v4", JobID: "job-1", Accept: false},
	}

	if !HasQuorum(votes, 3) {
		t.Fatal("expected quorum with 3 YES votes")
	}
}

func TestHasQuorumReturnsFalseWithoutEnoughYesVotes(t *testing.T) {
	votes := []Vote{
		{ValidatorID: "v1", JobID: "job-1", Accept: true},
		{ValidatorID: "v2", JobID: "job-1", Accept: true},
		{ValidatorID: "v3", JobID: "job-1", Accept: false},
		{ValidatorID: "v4", JobID: "job-1", Accept: false},
	}

	if HasQuorum(votes, 3) {
		t.Fatal("expected no quorum with only 2 YES votes")
	}
}

func TestHasQuorumIgnoresDuplicateValidatorVotes(t *testing.T) {
	votes := []Vote{
		{ValidatorID: "v1", JobID: "job-1", Accept: true},
		{ValidatorID: "v1", JobID: "job-1", Accept: true},
		{ValidatorID: "v2", JobID: "job-1", Accept: true},
	}

	if HasQuorum(votes, 3) {
		t.Fatal("expected duplicate validator votes to count only once")
	}
}
