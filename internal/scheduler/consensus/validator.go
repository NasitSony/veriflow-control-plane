package consensus

type Validator interface {
	ID() string
	Validate(t LifecycleTransition) Vote
}

type HonestValidator struct {
	ValidatorID string
}

func (v HonestValidator) ID() string {
	return v.ValidatorID
}

func (v HonestValidator) Validate(
	t LifecycleTransition,
) Vote {

	valid := IsValidTransition(
		t.From,
		t.To,
	) && ValidatePodObservation(t)

	reason := "valid"

	if !IsValidTransition(t.From, t.To) {
		reason = "invalid lifecycle transition"
	}

	if !ValidatePodObservation(t) {
		reason = "invalid pod observation"
	}

	return Vote{
		ValidatorID: v.ValidatorID,
		JobID:       t.JobID,
		Accept:      valid,
		Reason:      reason,
	}
}
