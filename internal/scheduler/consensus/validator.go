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
	)

	return Vote{
		ValidatorID: v.ValidatorID,
		JobID:       t.JobID,
		Accept:      valid,
	}
}
