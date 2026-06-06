package consensus

type ByzantineBehavior string

const (
	AlwaysYes ByzantineBehavior = "ALWAYS_YES"
	AlwaysNo  ByzantineBehavior = "ALWAYS_NO"
	Silent    ByzantineBehavior = "SILENT"
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

	default:
		return Vote{
			ValidatorID: v.ValidatorID,
			JobID:       t.JobID,
			Accept:      false,
			Reason:      "unknown byzantine behavior",
		}
	}
}
