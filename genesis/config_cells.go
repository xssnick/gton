package genesis

import (
	"crypto/ed25519"
	"fmt"
	"math/big"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	capCreateStats         = uint64(2)
	capBounceMsgBody       = uint64(4)
	capReportVersion       = uint64(8)
	capShortDequeue        = uint64(32)
	capStoreOutQueueSize   = uint64(64)
	capMessageMetadata     = uint64(128)
	capDeferMessages       = uint64(256)
	capFullCollatedData    = uint64(512)
	defaultGlobalVersion   = uint32(14)
	maxValidatorValidUntil = ^uint32(0)
)

var (
	configAddress  = repeatedByte(0x55)
	electorAddress = repeatedByte(0x33)
	walletAddress  = [32]byte{}
)

func buildConfigRoot(spec validatedSpec, genesisTime uint32, baseRootHash, baseFileHash []byte) (*cell.Dictionary, uint32, error) {
	params := cell.NewDict(32)
	put := func(id uint32, value any) error {
		param, err := tlb.ToCell(value)
		if err != nil {
			return fmt.Errorf("serialize config parameter %d: %w", id, err)
		}
		return putConfigParam(params, id, param)
	}

	if err := put(tlb.ConfigParamConfigAddress, &tlb.ConfigParamAddress{Address: configAddress[:]}); err != nil {
		return nil, 0, err
	}
	if err := put(tlb.ConfigParamElectorAddress, &tlb.ConfigParamAddress{Address: electorAddress[:]}); err != nil {
		return nil, 0, err
	}
	if err := put(tlb.ConfigParamMinterAddress, &tlb.ConfigParamAddress{Address: walletAddress[:]}); err != nil {
		return nil, 0, err
	}

	toMint, err := buildToMintConfig()
	if err != nil {
		return nil, 0, err
	}
	if err = putConfigParam(params, tlb.ConfigParamExtraCurrencyToMint, toMint); err != nil {
		return nil, 0, err
	}

	capabilities := capCreateStats | capBounceMsgBody | capReportVersion | capShortDequeue |
		capStoreOutQueueSize | capMessageMetadata | capDeferMessages | capFullCollatedData
	if err = put(tlb.ConfigParamGlobalVersion, &tlb.GlobalVersion{Version: defaultGlobalVersion, Capabilities: capabilities}); err != nil {
		return nil, 0, err
	}

	mandatory, err := unitDictionary(32, []int64{0, 1, 9, 10, 12, 14, 15, 16, 17, 18, 20, 21, 22, 23, 24, 25, 28, 34})
	if err != nil {
		return nil, 0, err
	}
	if err = put(tlb.ConfigParamMandatoryParams, &tlb.MandatoryParamsConfig{Params: mandatory}); err != nil {
		return nil, 0, err
	}
	critical, err := unitDictionary(32, []int64{-999, -1000, -1001, 0, 1, 9, 10, 12, 14, 15, 16, 17, 32, 34, 36})
	if err != nil {
		return nil, 0, err
	}
	if err = put(tlb.ConfigParamCriticalParams, &tlb.CriticalParamsConfig{Params: critical}); err != nil {
		return nil, 0, err
	}

	if err = put(tlb.ConfigParamConfigVotingSetup, &tlb.ConfigVotingSetup{
		NormalParams: &tlb.ConfigProposalSetup{
			MinTotRounds: 2, MaxTotRounds: 3, MinWins: 2, MaxLosses: 2,
			MinStoreSec: 1_000_000, MaxStoreSec: 10_000_000, BitPrice: 1, CellPrice: 500,
		},
		CriticalParams: &tlb.ConfigProposalSetup{
			MinTotRounds: 4, MaxTotRounds: 7, MinWins: 4, MaxLosses: 2,
			MinStoreSec: 5_000_000, MaxStoreSec: 20_000_000, BitPrice: 2, CellPrice: 1_000,
		},
	}); err != nil {
		return nil, 0, err
	}

	workchains, err := buildWorkchainsConfig(genesisTime, baseRootHash, baseFileHash)
	if err != nil {
		return nil, 0, err
	}
	if err = putConfigParam(params, tlb.ConfigParamWorkchains, workchains); err != nil {
		return nil, 0, err
	}

	if err = put(tlb.ConfigParamComplaintPricing, &tlb.ComplaintPricing{
		Deposit: tonCoins(100), BitPrice: nanoCoins(1), CellPrice: nanoCoins(500),
	}); err != nil {
		return nil, 0, err
	}
	if err = put(tlb.ConfigParamBlockCreateFees, &tlb.BlockCreateFees{
		MasterchainBlockFee: nanoCoins(1_700_000_000), BasechainBlockFee: tonCoins(1),
	}); err != nil {
		return nil, 0, err
	}
	if err = put(tlb.ConfigParamValidatorElectionTimings, &tlb.ValidatorElectionTimings{
		ValidatorsElectedFor: 2400, ElectionsStartBefore: 800, ElectionsEndBefore: 60, StakeHeldFor: 300,
	}); err != nil {
		return nil, 0, err
	}
	count := uint16(len(spec.validators))
	if err = put(tlb.ConfigParamValidatorCountLimits, &tlb.ValidatorCountLimits{
		MaxValidators: count, MaxMainValidators: count, MinValidators: count,
	}); err != nil {
		return nil, 0, err
	}
	if err = put(tlb.ConfigParamValidatorStakeLimits, &tlb.ValidatorStakeLimits{
		MinStake: tonCoins(10_000), MaxStake: tonCoins(100_000), MinTotalStake: tonCoins(10_000), MaxStakeFactor: 10 << 16,
	}); err != nil {
		return nil, 0, err
	}

	storagePrices, err := buildStoragePrices()
	if err != nil {
		return nil, 0, err
	}
	if err = putConfigParam(params, tlb.ConfigParamStoragePrices, storagePrices); err != nil {
		return nil, 0, err
	}
	if err = put(tlb.ConfigParamGlobalID, &tlb.GlobalIDConfig{GlobalID: spec.spec.GlobalID}); err != nil {
		return nil, 0, err
	}

	for _, gas := range []struct {
		id      uint32
		special uint64
	}{
		{id: tlb.ConfigParamGasPricesMasterchain, special: 20_000_000},
		{id: tlb.ConfigParamGasPricesBasechain, special: 1_000_000},
	} {
		if err = put(gas.id, &tlb.ConfigGasLimitsPrices{
			HasFlatPricing: true, FlatGasLimit: 100, FlatGasPrice: 1000,
			HasSeparateSpecialLimit: true, GasPrice: 10 << 16, GasLimit: 1_000_000,
			SpecialGasLimit: gas.special, GasCredit: 10_000, BlockGasLimit: 1_000_000_000,
			FreezeDueLimit: 100_000_000, DeleteDueLimit: 1_000_000_000,
		}); err != nil {
			return nil, 0, err
		}
	}

	if err = put(tlb.ConfigParamBlockLimitsMasterchain, &tlb.BlockLimits{Limits: tlb.BlockLimitsV1{
		Bytes:   paramLimits(131_072, 524_288, 1_048_576),
		Gas:     paramLimits(200_000, 1_000_000, 2_500_000),
		LTDelta: paramLimits(1_000, 5_000, 10_000),
	}}); err != nil {
		return nil, 0, err
	}
	if err = put(tlb.ConfigParamBlockLimitsBasechain, &tlb.BlockLimits{Limits: tlb.BlockLimitsV1{
		Bytes:   paramLimits(262_144, 1_048_576, 2_097_152),
		Gas:     paramLimits(2_000_000, 10_000_000, 20_000_000),
		LTDelta: paramLimits(1_000, 5_000, 10_000),
	}}); err != nil {
		return nil, 0, err
	}

	forwardPrices := &tlb.ConfigMsgForwardPrices{
		LumpPrice: 100, BitPrice: 10 << 16, CellPrice: 10 << 16,
		IHRFactor: 98_304, FirstFrac: 21_845, NextFrac: 21_845,
	}
	if err = put(tlb.ConfigParamMsgForwardPricesMasterchain, forwardPrices); err != nil {
		return nil, 0, err
	}
	if err = put(tlb.ConfigParamMsgForwardPricesBasechain, forwardPrices); err != nil {
		return nil, 0, err
	}

	if err = put(tlb.ConfigParamCatchainConfig, &tlb.CatchainConfig{Config: tlb.CatchainConfigV2{
		ShuffleMcValidators:     true,
		McCatchainLifetime:      spec.spec.Consensus.MasterGroupLifetime,
		ShardCatchainLifetime:   spec.spec.Consensus.ShardGroupLifetime,
		ShardValidatorsLifetime: 1000,
		ShardValidatorsNum:      spec.spec.Consensus.ShardValidators,
	}}); err != nil {
		return nil, 0, err
	}

	if err = putConfigParam(params, tlb.ConfigParamConsensusConfig, buildLegacyConsensusConfig()); err != nil {
		return nil, 0, err
	}
	newConsensus, err := buildSimplexConfig(spec.spec.Consensus)
	if err != nil {
		return nil, 0, err
	}
	if err = putConfigParam(params, tlb.ConfigParamNewConsensusConfig, newConsensus); err != nil {
		return nil, 0, err
	}

	fundamental, err := unitDictionary(256, []int64{0})
	if err != nil {
		return nil, 0, err
	}
	if err = put(tlb.ConfigParamFundamentalSMCAddresses, &tlb.FundamentalSmartContractAddresses{Addresses: fundamental}); err != nil {
		return nil, 0, err
	}

	validators, validatorHash, err := buildValidatorSet(spec.validators, genesisTime)
	if err != nil {
		return nil, 0, err
	}
	if err = putConfigParam(params, tlb.ConfigParamCurrentValidators, validators); err != nil {
		return nil, 0, err
	}

	return params, validatorHash, nil
}

func buildValidatorSet(validators []validatorIdentity, genesisTime uint32) (*cell.Cell, uint32, error) {
	list := cell.NewDict(16)
	addresses := make([]*tlb.ValidatorAddr, len(validators))
	var totalWeight uint64
	for i, validator := range validators {
		address := &tlb.ValidatorAddr{
			PublicKey: tlb.SigPubKeyED25519{Key: validator.publicKey[:]},
			Weight:    validator.weight,
			ADNLAddr:  validator.adnlID[:],
		}
		value, err := tlb.ToCell(address)
		if err != nil {
			return nil, 0, err
		}
		if err = list.SetIntKey(big.NewInt(int64(i)), value); err != nil {
			return nil, 0, err
		}
		addresses[i] = address
		totalWeight += validator.weight
	}

	set := &tlb.ValidatorSetAny{Validators: tlb.ValidatorSetExt{
		UTimeSince:  genesisTime,
		UTimeUntil:  maxValidatorValidUntil,
		Total:       uint16(len(validators)),
		Main:        uint16(len(validators)),
		TotalWeight: totalWeight,
		List:        list,
	}}
	root, err := tlb.ToCell(set)
	if err != nil {
		return nil, 0, err
	}
	hash, err := blockproof.ValidatorSetHash(0, addresses)
	if err != nil {
		return nil, 0, err
	}
	return root, hash, nil
}

func buildSimplexConfig(config Consensus) (*cell.Cell, error) {
	params := cell.NewDict(8)
	if err := params.SetIntKey(big.NewInt(0), cell.BeginCell().MustStoreUInt(uint64(config.TargetBlockRateMS), 32).EndCell()); err != nil {
		return nil, err
	}
	if err := params.SetIntKey(big.NewInt(1), cell.BeginCell().MustStoreUInt(uint64(config.FirstBlockTimeoutMS), 32).EndCell()); err != nil {
		return nil, err
	}
	simplex := tlb.NewConsensusConfigSimplexV2{
		ProtocolVersion:      config.ProtocolVersion,
		UseQUIC:              true,
		SlotsPerLeaderWindow: config.SlotsPerLeaderWindow,
		NoncriticalParams:    params,
	}
	return tlb.ToCell(&tlb.NewConsensusConfigAll{Masterchain: simplex, Shard: simplex})
}

func buildLegacyConsensusConfig() *cell.Cell {
	return cell.BeginCell().
		MustStoreUInt(0xd9, 8).
		MustStoreUInt(0, 6).
		MustStoreBoolBit(true).
		MustStoreBoolBit(true).
		MustStoreUInt(3, 8).
		MustStoreUInt(2000, 32).
		MustStoreUInt(16000, 32).
		MustStoreUInt(3, 32).
		MustStoreUInt(8, 32).
		MustStoreUInt(4, 32).
		MustStoreUInt(2_097_152, 32).
		MustStoreUInt(10_485_760, 32).
		MustStoreUInt(5, 16).
		MustStoreUInt(10_000, 32).
		EndCell()
}

func buildWorkchainsConfig(genesisTime uint32, rootHash, fileHash []byte) (*cell.Cell, error) {
	value, err := tlb.ToCell(&tlb.WorkchainDescr{Descr: tlb.WorkchainDescrV2{
		WorkchainDescrFields: tlb.WorkchainDescrFields{
			EnabledSince:      genesisTime,
			ActualMinSplit:    0,
			MinSplit:          0,
			MaxSplit:          4,
			Basic:             true,
			Active:            true,
			AcceptMsgs:        true,
			ZeroStateRootHash: rootHash,
			ZeroStateFileHash: fileHash,
			Format: tlb.WorkchainFormatBasic{
				VMVersion: -1,
			},
		},
		SplitMergeTimings: tlb.WorkchainSplitMergeTimings{
			SplitMergeDelay:       20,
			SplitMergeInterval:    20,
			MinSplitMergeInterval: 10,
			MaxSplitMergeDelay:    1000,
		},
		PersistentStateSplitDepth: 0,
	}})
	if err != nil {
		return nil, err
	}
	workchains := cell.NewDict(32)
	if err := workchains.SetIntKey(big.NewInt(0), value); err != nil {
		return nil, err
	}
	return tlb.ToCell(&tlb.WorkchainsConfig{Workchains: workchains})
}

func buildStoragePrices() (*cell.Cell, error) {
	prices := cell.NewDict(32)
	value, err := tlb.ToCell(&tlb.ConfigStoragePrices{
		ValidSince: 0, BitPrice: 1, CellPrice: 500, MCBitPrice: 1000, MCCellPrice: 500_000,
	})
	if err != nil {
		return nil, err
	}
	if err = prices.SetIntKey(big.NewInt(0), value); err != nil {
		return nil, err
	}
	return tlb.ToCell(&tlb.StoragePricesConfig{Prices: prices})
}

func buildToMintConfig() (*cell.Cell, error) {
	entries := []cell.DictBulkKV{
		{Key: []byte{0xff, 0xff, 0xff, 0xef}, Value: cell.BeginCell().MustStoreBigVarUInt(big.NewInt(1_000_000_000_000), 32)},
		{Key: []byte{0, 0, 0, 239}, Value: cell.BeginCell().MustStoreBigVarUInt(big.NewInt(666_666_666_666), 32)},
	}
	dict, err := cell.NewDictFromItems(32, entries)
	if err != nil {
		return nil, err
	}
	return tlb.ToCell(&tlb.ExtraCurrencyToMintConfig{ToMint: dict})
}

func unitDictionary(keyBits uint, values []int64) (*cell.Dictionary, error) {
	dict := cell.NewDict(keyBits)
	for _, value := range values {
		key := big.NewInt(value)
		if value < 0 {
			key.Add(key, new(big.Int).Lsh(big.NewInt(1), keyBits))
		}
		if err := dict.SetIntKey(key, cell.BeginCell().EndCell()); err != nil {
			return nil, err
		}
	}
	return dict, nil
}

func putConfigParam(params *cell.Dictionary, id uint32, value *cell.Cell) error {
	return params.SetIntKey(new(big.Int).SetUint64(uint64(id)), cell.BeginCell().MustStoreRef(value).EndCell())
}

func paramLimits(underload, soft, hard uint32) tlb.ParamLimits {
	return tlb.ParamLimits{Underload: underload, SoftLimit: soft, HardLimit: hard}
}

func nanoCoins(value uint64) tlb.Coins {
	return tlb.FromNanoTONU(value)
}

func tonCoins(value uint64) tlb.Coins {
	return tlb.FromNanoTON(new(big.Int).Mul(new(big.Int).SetUint64(value), big.NewInt(1_000_000_000)))
}

func repeatedByte(value byte) [ed25519.PublicKeySize]byte {
	var result [ed25519.PublicKeySize]byte
	for i := range result {
		result[i] = value
	}
	return result
}
