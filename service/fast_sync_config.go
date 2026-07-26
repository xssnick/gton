package service

import (
	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/p2p"

	"github.com/xssnick/tonutils-go/tlb"
)

const fastSyncPlumtreeMinProtocolVersion = 2

type fastSyncBlockchainConfig struct {
	roster                     p2p.FastSyncValidatorRoster
	masterchainPlumtreeVersion uint8
	shardPlumtreeVersion       uint8
}

// fastSyncConfigFromConfig loads config-controlled FastSync state.
//
// Nothing here can fail. Param 30 picks a broadcast transport and the validator
// sets pick an overlay to join; neither takes part in validating a block, so a
// config this build cannot interpret must leave Plumtree off rather than stop
// the chain. That is also what the reference node does: every step of
// Config::get_new_consensus_config returns the default config on a null or
// unreadable cell, and FullNodeFastSyncOverlays skips a validator set it cannot
// read (`if (val_set.is_null()) continue`). The case that matters is a future
// consensus config variant: this node must ignore it, not halt on it.
//
// Receive-broadcast policy also depends on local validator and shard-monitoring
// state, so it is intentionally absent from the snapshot.
func fastSyncConfigFromConfig(cfg *tlb.BlockchainConfig) fastSyncBlockchainConfig {
	// An unreadable param 30 leaves both versions at 0, exactly as an absent one
	// does; the validator sets are independent and still load.
	consensus, err := cfg.GetNewConsensusConfig()
	if err != nil {
		consensus = tlb.NewConsensusConfigAll{}
	}

	return fastSyncBlockchainConfig{
		roster: p2p.NewFastSyncValidatorRoster(
			fastSyncValidators(cfg.GetPrevValidators()),
			fastSyncValidators(cfg.GetCurrentValidators()),
			fastSyncValidators(cfg.GetNextValidators()),
		),
		masterchainPlumtreeVersion: consensusPlumtreeProtocolVersion(consensus.Masterchain),
		shardPlumtreeVersion:       consensusPlumtreeProtocolVersion(consensus.Shard),
	}
}

// consensusPlumtreeProtocolVersion mirrors the reference unpack: only
// simplex_config_v2 carries a protocol version, and anything else - absent, the
// v1 record, or a variant this build does not know - leaves it at 0.
func consensusPlumtreeProtocolVersion(config any) uint8 {
	if value, isV2 := config.(tlb.NewConsensusConfigSimplexV2); isV2 {
		return value.ProtocolVersion
	}
	return 0
}

func (c fastSyncBlockchainConfig) plumtreeEnabled(workchain int32) bool {
	if workchain == -1 {
		return c.masterchainPlumtreeVersion >=
			fastSyncPlumtreeMinProtocolVersion
	}

	return c.shardPlumtreeVersion >= fastSyncPlumtreeMinProtocolVersion
}

// plumtreePolicy fails closed rather than failing: with no roster there are no
// authorized keys, and PlumtreePolicy.authorizes then rejects every source.
func (c fastSyncBlockchainConfig) plumtreePolicy() p2p.PlumtreePolicy {
	return p2p.NewPlumtreePolicy(
		c.masterchainPlumtreeVersion,
		c.shardPlumtreeVersion,
		c.roster.RootPublicKeyIDs(),
	)
}

// fastSyncValidators adapts one validator-set getter. A set that is absent or
// unreadable contributes nothing, matching the reference node's skip. Both
// fields are `bits 256` on the wire, so they always fill their arrays exactly.
func fastSyncValidators(
	set *tlb.ValidatorSetAny,
	err error,
) []p2p.FastSyncValidator {
	if err != nil {
		return nil
	}
	validators, err := blockproof.TotalValidators(*set)
	if err != nil {
		return nil
	}

	result := make([]p2p.FastSyncValidator, len(validators))
	for i, validator := range validators {
		copy(result[i].PublicKey[:], validator.PublicKey.Key)
		copy(result[i].ADNLID[:], validator.ADNLAddr)
	}
	return result
}
