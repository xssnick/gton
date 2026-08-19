package collator

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
)

const defaultCandidateSizeLimit = uint32(4 << 20)

// SizeLimitsConfigV1 predates the serialized defer threshold. The protocol
// initializes that field to 256 before unpacking the older constructor.
const legacyDeferOutQueueSizeLimit = uint64(256)

// Config contains immutable per-epoch data used by block collation. It is the
// single home for everything derived from one configuration root: parsed once
// when the epoch is prepared, shared across lanes and across blocks, never
// mutated by a consumer.
//
// The caveat that decides whether a field may live here: PrepareConfig runs
// twice in different worlds. From localConfigCache.prepare it runs on a
// materialized, untraced root, and from deriveMasterConfigTransition's fresh
// branch it runs on the configuration root reached through the block's accounts
// — that one IS under the block's read set, which the Merkle update descends and
// the collated-size estimate resolves membership against. So a field derived
// from cells the masterchain block already reads is free, and a field whose
// derivation reaches a cell nothing else on that path reads changes the
// masterchain block bytes. Nothing here may be rerouted onto the traced in-state
// configuration root for convenience.
type Config struct {
	execution          *tvm.PreparedBlockchainConfig
	globalVersion      uint32
	capabilities       uint64
	basechain          chainConfig
	basechainWorkchain workchainPolicy
	// workchains is config parameter 12 parsed once per config epoch, mirroring
	// block::Config::workchains_. The masterchain shard passes read it from here
	// instead of re-parsing the same immutable root on every block.
	workchains             map[int32]*masterShardWorkchainInfo
	masterchain            chainConfig
	burning                tlb.BurningConfig
	maxBlockBytes          uint32
	maxCollatedBytes       uint32
	deferOutQueueSizeLimit uint64
	// gas is the semantic gas allowance of config parameters 20/21 folded with
	// the epoch block limits, indexed like tvm's masterchain index: 0 basechain,
	// 1 masterchain. It is epoch data because blockLimitsAtTime rewrites only
	// ltDelta, never gas, so the hard gas threshold does not move within an epoch.
	gas [2]gasAccounting
	// specials is the masterchain fundamental-contract set of parameter 31 plus
	// the configuration contract of parameter 0.
	specials masterSpecials
	// fees names the destinations of the two masterchain special messages:
	// parameter 3 (fee collector, falling back to the elector of parameter 1)
	// and parameter 2 (minter, falling back to the configuration contract).
	fees feeDestinations
	// footprint is what parsing this configuration read, recorded when the
	// configuration was prepared. Master collation replays it instead of
	// re-parsing; a nil footprint simply means it re-parses.
	footprint *configFootprint
}

// gasAccounting is the per-chain gas allowance a transaction may consume before
// the semantic verifier rejects the block.
//
// err carries the overflow rejection instead of failing PrepareConfig, so an
// epoch stays preparable and the rejection still happens where it happens today:
// inside the verification of one block. Failing here would take the whole config
// epoch down, and with it shard collation, over a masterchain-shaped defect.
type gasAccounting struct {
	normal  uint64
	special uint64
	err     error
}

// masterSpecials is the identity list of the masterchain special accounts.
// Which of them actually execute, and at what logical time, stays per-block in
// processMasterTickTock and verifyMasterTickTock; only the identities are epoch
// data.
type masterSpecials struct {
	// ordered is parameter-31 dictionary order with the configuration contract
	// appended last when parameter 31 does not already list it.
	// processMasterTickTock executes in exactly this order, and the order decides
	// logical time assignment, so it is block bytes and not a presentation
	// detail.
	ordered [][32]byte
	// sorted is ordered by ascending account id, which is the order
	// verifyMasterTickTock already imposes on itself.
	sorted [][32]byte
	// set is the same identities as a membership test. It is shared by every
	// consumer of one epoch and must never be written to.
	set map[[32]byte]struct{}
	// err is the rejection the master paths raise today when the configuration
	// contract address is missing or malformed. It is carried rather than
	// returned for the same reason gasAccounting.err is.
	err error
}

// feeDestination is one masterchain special-message recipient.
//
// err is the exact error the tlb accessor returned, because the master paths
// embed it in their rejection and the wording is asserted by their tests.
type feeDestination struct {
	addr [32]byte
	ok   bool
	err  error
}

type feeDestinations struct {
	collector feeDestination
	minter    feeDestination
}

type workchainPolicy struct {
	present      bool
	enabledSince uint32
	basic        bool
	active       bool
}

type chainConfig struct {
	limits    blockLimits
	createFee tlb.Coins
	fwdPrices tlb.ConfigMsgForwardPrices
}

// PrepareConfig derives immutable collation data for one config epoch.
func PrepareConfig(execution *tvm.PreparedBlockchainConfig) (*Config, error) {
	raw := tlb.BlockchainConfig{Root: execution.Root()}

	globalVersion, err := raw.GetGlobalVersion()
	if err != nil {
		return nil, fmt.Errorf("%w: load global version: %v", ErrInvalidInput, err)
	}
	if _, err := raw.GetGlobalID(); err != nil {
		return nil, fmt.Errorf("%w: load blockchain global id: %v", ErrInvalidInput, err)
	}
	basechainLimits, err := prepareChainConfig(raw, false)
	if err != nil {
		return nil, err
	}
	createFees, err := raw.GetBlockCreateFees()
	if err != nil {
		return nil, fmt.Errorf("%w: load block creation fees: %v", ErrInvalidInput, err)
	}
	basechainLimits.createFee = createFees.BasechainBlockFee

	masterchainLimits, err := prepareChainConfig(raw, true)
	if err != nil {
		return nil, err
	}
	masterchainLimits.createFee = createFees.MasterchainBlockFee

	burning, err := raw.GetBurningConfig()
	if err != nil {
		return nil, fmt.Errorf("%w: load fee burning config: %v", ErrInvalidInput, err)
	}
	// An absent parameter 5 means the zero burning config: burn 0/1.
	// tonutils represents the absent denominator as zero, so normalize only
	// that exact absence shape; a present invalid fraction is rejected.
	if burning.FeeBurnDenom == 0 {
		if burning.FeeBurnNum != 0 || burning.BlackholeAddr != nil {
			return nil, fmt.Errorf("%w: fee burning denominator is zero", ErrInvalidInput)
		}
		burning.FeeBurnDenom = 1
	}
	if burning.FeeBurnNum > burning.FeeBurnDenom {
		return nil, fmt.Errorf("%w: fee burning numerator exceeds denominator", ErrInvalidInput)
	}
	consensus, err := raw.GetConsensusConfig()
	if err != nil {
		return nil, fmt.Errorf("%w: load consensus config: %v", ErrInvalidInput, err)
	}
	maxBlockBytes, maxCollatedBytes, err := candidateSizeLimits(consensus)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	deferOutQueueSizeLimit, err := configDeferOutQueueSizeLimit(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	workchains, err := loadMasterShardWorkchains(execution.Root())
	if err != nil {
		return nil, fmt.Errorf("%w: load workchain policies: %v", ErrInvalidInput, err)
	}
	var basechainWorkchain workchainPolicy
	if workchain := workchains[0]; workchain != nil {
		basechainWorkchain = workchainPolicy{
			present:      true,
			enabledSince: workchain.enabledSince,
			basic:        workchain.basic,
			active:       workchain.active,
		}
	}

	return &Config{
		execution:              execution,
		globalVersion:          globalVersion.Version,
		capabilities:           globalVersion.Capabilities,
		basechain:              basechainLimits,
		basechainWorkchain:     basechainWorkchain,
		workchains:             workchains,
		masterchain:            masterchainLimits,
		burning:                burning,
		maxBlockBytes:          maxBlockBytes,
		maxCollatedBytes:       maxCollatedBytes,
		deferOutQueueSizeLimit: deferOutQueueSizeLimit,
		gas: [2]gasAccounting{
			deriveGasAccounting(execution, basechainLimits.limits, false),
			deriveGasAccounting(execution, masterchainLimits.limits, true),
		},
		specials: deriveMasterSpecials(execution),
		fees:     deriveFeeDestinations(raw),
	}, nil
}

// deriveGasAccounting folds config parameter 20 or 21 into the epoch gas
// allowance the semantic verifier charges against.
//
// It reads the prices off the prepared execution config, which decoded them when
// the epoch was prepared, so this touches no cell at all.
func deriveGasAccounting(
	execution *tvm.PreparedBlockchainConfig,
	limits blockLimits,
	masterchain bool,
) gasAccounting {
	prices, ok := execution.GasPrices(masterchain)
	if !ok {
		// Unreachable through parseMasterConfigEpoch, whose strict preparation
		// requires both params; reachable only for a caller that prepares a
		// partial config for tooling.
		return gasAccounting{err: fmt.Errorf("%w: %s gas prices are absent from the configuration",
			ErrInvalidInput, chainName(masterchain))}
	}
	normal, err := semanticGasLimit(limits.gas[3], prices.GasLimit)
	if err != nil {
		return gasAccounting{err: err}
	}
	special, err := semanticGasLimit(limits.gas[3], prices.SpecialGasLimit)
	if err != nil {
		return gasAccounting{err: err}
	}
	return gasAccounting{normal: normal, special: special}
}

// deriveMasterSpecials resolves the masterchain special accounts once per config
// epoch: the fundamental smart contracts of parameter 31 and the configuration
// contract of parameter 0.
//
// The identities come off the prepared execution config, which walked parameter
// 31 and read parameter 0 when the epoch was prepared, so this touches no cell.
// That matters beyond cost: deriveMasterConfigTransition runs PrepareConfig
// under the block's read set when the candidate installs a configuration, and a
// parse added here that reads a cell nothing else on that path reads would change
// the masterchain block the collator produces. Rerouting any of this onto the
// in-state configuration root would do exactly that.
func deriveMasterSpecials(execution *tvm.PreparedBlockchainConfig) masterSpecials {
	if _, ok := execution.ConfigAddress(); !ok {
		// Master-only rejection, carried so the epoch stays preparable: shard
		// collation never asks about special accounts and must not die here.
		return masterSpecials{err: fmt.Errorf("%w: config smart contract address is malformed", ErrInvalidInput)}
	}
	// Cloned rather than aliased: the backing array belongs to the prepared
	// execution config, and nothing in this package may reach into it.
	return newMasterSpecials(slices.Clone(execution.SpecialAccounts()))
}

// newMasterSpecials derives the three views of one identity list together.
//
// It is the only constructor, including for tests: a replay that could be handed
// a set without the matching order would silently verify a masterchain block
// against an empty tick/tock list.
func newMasterSpecials(ordered [][32]byte) masterSpecials {
	sorted := slices.Clone(ordered)
	slices.SortFunc(sorted, func(left, right [32]byte) int {
		return bytes.Compare(left[:], right[:])
	})
	set := make(map[[32]byte]struct{}, len(ordered))
	for _, accountID := range ordered {
		set[accountID] = struct{}{}
	}

	return masterSpecials{ordered: ordered, sorted: sorted, set: set}
}

// deriveFeeDestinations resolves config parameters 3 and 2 with their fallbacks.
//
// Unlike the two above, this does parse the configuration root — but only cells
// validateMasterConfigData has already read on every masterchain block, since
// parameters 0 through 3 all go through its exactBits256 check before this runs.
// See deriveMasterSpecials for why that distinction decides block bytes.
func deriveFeeDestinations(raw tlb.BlockchainConfig) feeDestinations {
	return feeDestinations{
		collector: feeDestinationOf(raw.GetFeeCollectorAddress()),
		minter:    feeDestinationOf(raw.GetMinterAddress()),
	}
}

func feeDestinationOf(addr []byte, err error) feeDestination {
	if err != nil || len(addr) != 32 {
		return feeDestination{err: err}
	}
	return feeDestination{addr: [32]byte(addr), ok: true}
}

func configDeferOutQueueSizeLimit(raw tlb.BlockchainConfig) (uint64, error) {
	limits, err := raw.GetSizeLimitsConfig()
	if err != nil {
		return 0, fmt.Errorf("load size limits: %v", err)
	}

	switch value := limits.Config.(type) {
	case tlb.SizeLimitsConfigV1:
		return legacyDeferOutQueueSizeLimit, nil
	case tlb.SizeLimitsConfigV2:
		return uint64(value.DeferOutQueueSizeLimit), nil
	case tlb.SizeLimitsConfigV3:
		return uint64(value.DeferOutQueueSizeLimit), nil
	default:
		return 0, fmt.Errorf("unsupported size limits config %T", value)
	}
}

func prepareChainConfig(raw tlb.BlockchainConfig, masterchain bool) (chainConfig, error) {
	rawLimits, err := raw.GetBlockLimits(masterchain)
	if err != nil {
		return chainConfig{}, fmt.Errorf("%w: load %s block limits: %v", ErrInvalidInput, chainName(masterchain), err)
	}
	limits, err := parseBlockLimits(rawLimits)
	if err != nil {
		return chainConfig{}, err
	}
	forwardPrices, err := raw.GetMsgForwardPrices(masterchain)
	if err != nil {
		return chainConfig{}, fmt.Errorf("%w: load %s forwarding prices: %v", ErrInvalidInput, chainName(masterchain), err)
	}

	return chainConfig{limits: limits, fwdPrices: *forwardPrices}, nil
}

func chainName(masterchain bool) string {
	if masterchain {
		return "masterchain"
	}
	return "basechain"
}

func verifyBasechainWorkchain(config *Config, genUtime uint32) error {
	workchain := config.basechainWorkchain
	if !workchain.present {
		return fmt.Errorf("%w: basechain is absent from workchain config", ErrInvalidInput)
	}
	if !workchain.active || !workchain.basic {
		return fmt.Errorf("%w: basechain is inactive or uses an extended address format", ErrInvalidInput)
	}
	if genUtime < workchain.enabledSince {
		return fmt.Errorf("%w: basechain is not enabled at candidate generation time", ErrInvalidInput)
	}

	return nil
}

func candidateSizeLimits(config tlb.ConsensusConfig) (uint32, uint32, error) {
	switch value := config.Config.(type) {
	case nil:
		// Networks without consensus config use the protocol candidate cap.
		return defaultCandidateSizeLimit, defaultCandidateSizeLimit, nil
	case tlb.ConsensusConfigV1:
		return value.MaxBlockBytes, value.MaxCollatedBytes, nil
	case tlb.ConsensusConfigV2:
		return value.MaxBlockBytes, value.MaxCollatedBytes, nil
	case tlb.ConsensusConfigV3:
		return value.MaxBlockBytes, value.MaxCollatedBytes, nil
	case tlb.ConsensusConfigV4:
		return value.MaxBlockBytes, value.MaxCollatedBytes, nil
	default:
		return 0, 0, fmt.Errorf("unsupported consensus config %T", value)
	}
}
