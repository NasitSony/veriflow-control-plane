package consensus

type Vote struct {
	ValidatorID string
	JobID       string
	Accept      bool
	Reason      string
}
