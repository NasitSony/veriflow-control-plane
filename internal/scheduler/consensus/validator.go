package consensus

type Validator interface {
	ID() string
	Validate(t LifecycleTransition) Vote
}

type StaticValidator struct {
	ValidatorID string
	Accept      bool
}

func (v StaticValidator) ID() string {
	return v.ValidatorID
}

func (v StaticValidator) Validate(t LifecycleTransition) Vote {
	return Vote{
		ValidatorID: v.ValidatorID,
		JobID:       t.JobID,
		Accept:      v.Accept,
	}
}
