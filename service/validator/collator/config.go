package collator

import (
	"fmt"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
)

const defaultCandidateSizeLimit = uint32(4 << 20)

// SizeLimitsConfigV1 predates the serialized defer threshold. The protocol
// initializes that field to 256 before unpacking the older constructor.
const legacyDeferOutQueueSizeLimit = uint64(256)

// Config contains immutable per-epoch data used by block collation.
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
	}, nil
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
