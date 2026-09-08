package node

import (
	"crypto/ed25519"
	"errors"
	"fmt"

	nodeconfig "github.com/xssnick/gton/cmd/node/config"
	"github.com/xssnick/gton/service/validator/keyring"
)

type collatorIdentity struct {
	keys  *keyring.Keyring
	keyID [32]byte
}

// consensusADNL selects the identity shared by validator and collator roles.
// A disabled or absent dedicated network retains the released node-identity mode.
func consensusADNL(cfg nodeconfig.Config) nodeconfig.ADNL {
	if cfg.ConsensusADNL != nil && cfg.ConsensusADNL.Enabled {
		return cfg.ConsensusADNL.ADNL
	}

	return cfg.ADNL
}

func configureCollatorIdentity(cfg nodeconfig.Config) (collatorIdentity, error) {
	seed := consensusADNL(cfg).Key
	if len(seed) != ed25519.SeedSize {
		return collatorIdentity{}, fmt.Errorf(
			"consensus ADNL key must contain a %d-byte Ed25519 seed for collation",
			ed25519.SeedSize,
		)
	}

	privateKey := ed25519.NewKeyFromSeed(seed)
	keys, err := keyring.New(privateKey)
	clear(privateKey)
	if err != nil {
		return collatorIdentity{}, fmt.Errorf("initialize collator keyring: %w", err)
	}
	ids := keys.KeyIDs()

	return collatorIdentity{keys: keys, keyID: ids[0]}, nil
}

type standaloneValidatorPolicy struct {
	allowed  map[[32]byte]struct{}
	allowAll bool
}

func configureStandaloneValidatorPolicy(
	config nodeconfig.CollatorValidatorAllowlist,
) (standaloneValidatorPolicy, error) {
	if !config.Enabled {
		return standaloneValidatorPolicy{allowAll: true}, nil
	}

	allowed := make(map[[32]byte]struct{}, len(config.ADNLIDs))
	for i := range config.ADNLIDs {
		value := config.ADNLIDs[i]
		if len(value) != 32 {
			return standaloneValidatorPolicy{}, fmt.Errorf(
				"collator.validator_allowlist.adnl_ids[%d] must contain 32 bytes",
				i,
			)
		}

		var id [32]byte
		copy(id[:], value)
		if id == ([32]byte{}) {
			return standaloneValidatorPolicy{}, fmt.Errorf(
				"collator.validator_allowlist.adnl_ids[%d] is zero",
				i,
			)
		}
		if _, duplicate := allowed[id]; duplicate {
			return standaloneValidatorPolicy{}, fmt.Errorf(
				"collator.validator_allowlist.adnl_ids[%d] is duplicated",
				i,
			)
		}
		allowed[id] = struct{}{}
	}
	if len(allowed) == 0 {
		return standaloneValidatorPolicy{}, errors.New(
			"collator.validator_allowlist.adnl_ids must not be empty when the allowlist is enabled",
		)
	}

	return standaloneValidatorPolicy{allowed: allowed}, nil
}
