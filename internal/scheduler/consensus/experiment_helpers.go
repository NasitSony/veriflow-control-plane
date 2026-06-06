package consensus

import "fmt"

func buildValidators(honest int, byzantine int, behavior ByzantineBehavior) []Validator {
	var validators []Validator

	for i := 1; i <= honest; i++ {
		validators = append(validators, HonestValidator{
			ValidatorID: fmt.Sprintf("h%d", i),
		})
	}

	for i := 1; i <= byzantine; i++ {
		validators = append(validators, ByzantineValidator{
			ValidatorID: fmt.Sprintf("b%d", i),
			Behavior:    behavior,
		})
	}

	return validators
}
