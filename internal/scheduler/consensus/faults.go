package consensus

import "time"

type ByzantineBehavior string

const (
	AlwaysYes ByzantineBehavior = "ALWAYS_YES"
	AlwaysNo  ByzantineBehavior = "ALWAYS_NO"
	Silent    ByzantineBehavior = "SILENT"
	Delayed   ByzantineBehavior = "DELAYED"
)

type ByzantineValidator struct {
	ValidatorID string
	Behavior    ByzantineBehavior
}

func (v ByzantineValidator) ID() string {
	return v.ValidatorID
}

func (v ByzantineValidator) Validate(t LifecycleTransition) Vote {
	switch v.Behavior {
	case AlwaysYes:
		return Vote{
			ValidatorID: v.ValidatorID,
			JobID:       t.JobID,
			Accept:      true,
			Reason:      "byzantine always yes",
		}

	case AlwaysNo:
		return Vote{
			ValidatorID: v.ValidatorID,
			JobID:       t.JobID,
			Accept:      false,
			Reason:      "byzantine always no",
		}

	case Silent:
		return Vote{
			ValidatorID: v.ValidatorID,
			JobID:       t.JobID,
			Accept:      false,
			Reason:      "byzantine silent",
		}

	case Delayed:

		time.Sleep(100 * time.Millisecond)

		return Vote{
			ValidatorID: v.ValidatorID,
			JobID:       t.JobID,
			Accept:      true,
			Reason:      "delayed vote",
		}

	default:
		return Vote{
			ValidatorID: v.ValidatorID,
			JobID:       t.JobID,
			Accept:      false,
			Reason:      "unknown byzantine behavior",
		}
	}
}
