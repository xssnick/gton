package collator

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

var errConfigAddressAbsent = errors.New("config address is absent")

var hardMandatoryConfigParameters = [...]uint32{
	18, 20, 21, 22, 23, 24, 25, 28, 34,
}

// validateMasterConfigData checks the configuration parts represented by
// tonutils. Every outer value must be an exact reference,
// known positive parameters are decoded by their protocol type, and both the
// old and resulting mandatory parameter sets are enforced.
func validateMasterConfigData(
	root *cell.Cell,
	accountID []byte,
	oldRoot *cell.Cell,
	relaxAddress bool,
) error {
	if root == nil || len(accountID) != 32 {
		return fmt.Errorf("%w: config root or account id is malformed", ErrInvalidInput)
	}
	dict := root.AsDict(32)
	if dict.GetKeySize() != 32 || dict.IsEmpty() {
		return fmt.Errorf("%w: config parameter dictionary is empty or has a wrong key size", ErrInvalidInput)
	}
	items, err := dict.LoadAll()
	if err != nil {
		return fmt.Errorf("%w: load config parameters: %v", ErrInvalidInput, err)
	}
	present := make(map[uint32]struct{}, len(items))
	raw := tlb.BlockchainConfig{Root: root}
	for i := range items {
		key, loadErr := items[i].Key.LoadInt(32)
		if loadErr != nil || items[i].Key.BitsLeft() != 0 {
			return fmt.Errorf("%w: config parameter %d key is malformed", ErrInvalidInput, i)
		}
		parameter, loadErr := items[i].Value.LoadRefCell()
		if loadErr != nil || items[i].Value.BitsLeft() != 0 || items[i].Value.RefsNum() != 0 {
			return fmt.Errorf("%w: config parameter %d is not an exact reference", ErrInvalidInput, key)
		}
		if key < 0 {
			continue
		}
		id := uint32(key)
		present[id] = struct{}{}
		if id == tlb.ConfigParamConfigAddress {
			address, addressErr := exactBits256(parameter)
			if addressErr != nil || !relaxAddress && !bytes.Equal(address, accountID) {
				return fmt.Errorf("%w: config parameter 0 does not name its contract", ErrInvalidInput)
			}
			continue
		}
		if err = validateKnownConfigParameter(raw, id); err != nil {
			return fmt.Errorf("%w: config parameter %d: %v", ErrInvalidInput, id, err)
		}
	}

	for _, id := range hardMandatoryConfigParameters {
		if _, exists := present[id]; !exists {
			return fmt.Errorf("%w: mandatory config parameter %d is absent", ErrInvalidInput, id)
		}
	}
	if err = requireConfigMandatorySet(raw, present); err != nil {
		return err
	}
	if oldRoot != nil {
		if err = requireConfigMandatorySet(tlb.BlockchainConfig{Root: oldRoot}, present); err != nil {
			return fmt.Errorf("old mandatory set: %w", err)
		}
	}
	return nil
}

func exactConfigAddress(root *cell.Cell) ([]byte, error) {
	parameter, err := (tlb.BlockchainConfig{Root: root}).GetParam(tlb.ConfigParamConfigAddress)
	if errors.Is(err, tlb.ErrBlockchainConfigParamAbsent) {
		return nil, errConfigAddressAbsent
	}
	if err != nil {
		return nil, err
	}
	return exactBits256(parameter)
}

func exactBits256(root *cell.Cell) ([]byte, error) {
	loader, err := root.BeginParse()
	if err != nil {
		return nil, err
	}
	value, err := loader.LoadSlice(256)
	if err != nil || loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		return nil, fmt.Errorf("value is not exactly 256 bits")
	}
	return value, nil
}

func requireConfigMandatorySet(raw tlb.BlockchainConfig, present map[uint32]struct{}) error {
	parameter, err := raw.GetParam(tlb.ConfigParamMandatoryParams)
	if errors.Is(err, tlb.ErrBlockchainConfigParamAbsent) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("%w: load mandatory config set: %v", ErrInvalidInput, err)
	}
	dict := parameter.AsDict(32)
	if dict.GetKeySize() != 32 {
		return fmt.Errorf("%w: mandatory config set has a wrong key size", ErrInvalidInput)
	}
	items, err := dict.LoadAll()
	if err != nil {
		return fmt.Errorf("%w: load mandatory config set: %v", ErrInvalidInput, err)
	}
	for i := range items {
		id, loadErr := items[i].Key.LoadUInt(32)
		if loadErr != nil || items[i].Key.BitsLeft() != 0 ||
			items[i].Value.BitsLeft() != 0 || items[i].Value.RefsNum() != 0 {
			return fmt.Errorf("%w: mandatory config entry %d is malformed", ErrInvalidInput, i)
		}
		if _, exists := present[uint32(id)]; !exists {
			return fmt.Errorf("%w: declared mandatory config parameter %d is absent", ErrInvalidInput, id)
		}
	}
	return nil
}

func validateKnownConfigParameter(raw tlb.BlockchainConfig, id uint32) (err error) {
	switch id {
	case tlb.ConfigParamElectorAddress,
		tlb.ConfigParamMinterAddress,
		tlb.ConfigParamFeeCollectorAddress,
		tlb.ConfigParamDNSRootAddress:
		var parameter *cell.Cell
		parameter, err = raw.GetParam(id)
		if err == nil {
			_, err = exactBits256(parameter)
		}
	case tlb.ConfigParamBurningConfig:
		_, err = raw.GetBurningConfig()
	case tlb.ConfigParamExtraCurrencyMintPrices:
		_, err = raw.GetExtraCurrencyMintPrices()
	case tlb.ConfigParamExtraCurrencyToMint:
		_, err = raw.GetExtraCurrencyToMint()
	case tlb.ConfigParamGlobalVersion:
		_, err = raw.GetGlobalVersion()
	case tlb.ConfigParamMandatoryParams:
		_, err = raw.GetMandatoryParams()
	case tlb.ConfigParamCriticalParams:
		_, err = raw.GetCriticalParams()
	case tlb.ConfigParamConfigVotingSetup:
		_, err = raw.GetConfigVotingSetup()
	case tlb.ConfigParamWorkchains:
		_, err = raw.GetWorkchains()
	case tlb.ConfigParamComplaintPricing:
		_, err = raw.GetComplaintPricing()
	case tlb.ConfigParamBlockCreateFees:
		_, err = raw.GetBlockCreateFees()
	case tlb.ConfigParamValidatorElectionTimings:
		_, err = raw.GetValidatorElectionTimings()
	case tlb.ConfigParamValidatorCountLimits:
		_, err = raw.GetValidatorCountLimits()
	case tlb.ConfigParamValidatorStakeLimits:
		_, err = raw.GetValidatorStakeLimits()
	case tlb.ConfigParamStoragePrices:
		_, err = raw.GetStoragePrices(0)
	case tlb.ConfigParamGlobalID:
		_, err = raw.GetGlobalID()
	case tlb.ConfigParamGasPricesMasterchain:
		_, err = raw.GetGasPrices(true)
	case tlb.ConfigParamGasPricesBasechain:
		_, err = raw.GetGasPrices(false)
	case tlb.ConfigParamBlockLimitsMasterchain:
		_, err = raw.GetBlockLimits(true)
	case tlb.ConfigParamBlockLimitsBasechain:
		_, err = raw.GetBlockLimits(false)
	case tlb.ConfigParamMsgForwardPricesMasterchain:
		_, err = raw.GetMsgForwardPrices(true)
	case tlb.ConfigParamMsgForwardPricesBasechain:
		_, err = raw.GetMsgForwardPrices(false)
	case tlb.ConfigParamCatchainConfig:
		_, err = raw.GetCatchainConfig()
	case tlb.ConfigParamConsensusConfig:
		_, err = raw.GetConsensusConfig()
	case tlb.ConfigParamNewConsensusConfig:
		_, err = raw.GetNewConsensusConfig()
	case tlb.ConfigParamFundamentalSMCAddresses:
		_, err = raw.GetFundamentalSmartContractAddresses()
	case tlb.ConfigParamPrevValidators:
		_, err = raw.GetPrevValidators()
	case tlb.ConfigParamPrevTempValidators:
		_, err = raw.GetPrevTempValidators()
	case tlb.ConfigParamCurrentValidators:
		_, err = raw.GetCurrentValidators()
	case tlb.ConfigParamCurrentTempValidators:
		_, err = raw.GetCurrentTempValidators()
	case tlb.ConfigParamNextValidators:
		_, err = raw.GetNextValidators()
	case tlb.ConfigParamNextTempValidators:
		_, err = raw.GetNextTempValidators()
	case tlb.ConfigParamValidatorTempKeys:
		_, err = raw.GetValidatorTempKeys()
	case tlb.ConfigParamMisbehaviourPunishment:
		_, err = raw.GetMisbehaviourPunishmentConfig()
	case tlb.ConfigParamSizeLimits:
		_, err = raw.GetSizeLimitsConfig()
	case tlb.ConfigParamSuspendedAddressList:
		_, err = raw.GetSuspendedAddressList()
	case tlb.ConfigParamPrecompiledContracts:
		_, err = raw.GetPrecompiledContractsConfig()
	case 71, 72, 73:
		err = validateOracleBridgeParameter(raw, id)
	case 79, 80, 81, 82:
		// The bridge ids are not modelled by tonutils, so they are listed by
		// number. 79/80/81 are the ETH/BNB/Polygon JettonBridgeParams of
		// crypto/block/block.tlb:780-782 at ton c7da81d4 (2023-03-23), all three
		// accepted by ConfigParam::get_tag there; 82 is newer than that checkout
		// and cannot be re-derived from it, so it stays on trust.
		// Anything not listed here fails the whole masterchain block, so this set
		// must be widened whenever governance introduces a parameter upstream
		// already accepts.
		err = validateJettonBridgeParameter(raw, id)
	default:
		return fmt.Errorf("unsupported positive parameter")
	}
	return err
}

func validateOracleBridgeParameter(raw tlb.BlockchainConfig, id uint32) error {
	parameter, err := raw.GetParam(id)
	if err != nil {
		return err
	}
	loader, err := parameter.BeginParse()
	if err != nil {
		return err
	}
	if _, err = loader.LoadSlice(512); err != nil {
		return err
	}
	oracles, err := loader.LoadDict(256)
	if err != nil {
		return err
	}
	if _, err = loader.LoadSlice(256); err != nil {
		return err
	}
	if loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		return fmt.Errorf("oracle bridge parameter has trailing data")
	}
	return validateOracleDictionary(oracles)
}

func validateJettonBridgeParameter(raw tlb.BlockchainConfig, id uint32) error {
	parameter, err := raw.GetParam(id)
	if err != nil {
		return err
	}
	loader, err := parameter.BeginParse()
	if err != nil {
		return err
	}
	version, err := loader.LoadUInt(8)
	if err != nil {
		return err
	}
	if _, err = loader.LoadSlice(512); err != nil {
		return err
	}
	oracles, err := loader.LoadDict(256)
	if err != nil {
		return err
	}
	if _, err = loader.LoadUInt(8); err != nil {
		return err
	}
	switch version {
	case 0:
		if _, err = loader.LoadBigCoins(); err != nil {
			return err
		}
	case 1:
		prices, loadErr := loader.LoadRefCell()
		if loadErr != nil {
			return loadErr
		}
		if err = validateJettonBridgePrices(prices); err != nil {
			return err
		}
		if _, err = loader.LoadSlice(256); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown jetton bridge version %d", version)
	}
	if loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		return fmt.Errorf("jetton bridge parameter has trailing data")
	}
	return validateOracleDictionary(oracles)
}

func validateJettonBridgePrices(root *cell.Cell) error {
	loader, err := root.BeginParse()
	if err != nil {
		return err
	}
	for range 6 {
		if _, err = loader.LoadBigCoins(); err != nil {
			return err
		}
	}
	if loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		return fmt.Errorf("jetton bridge prices have trailing data")
	}
	return nil
}

func validateOracleDictionary(dict *cell.Dictionary) error {
	if dict.GetKeySize() != 256 {
		return fmt.Errorf("oracle dictionary has a wrong key size")
	}
	items, err := dict.LoadAll()
	if err != nil {
		return err
	}
	for i := range items {
		if items[i].Key.BitsLeft() != 256 || items[i].Key.RefsNum() != 0 ||
			items[i].Value.BitsLeft() != 256 || items[i].Value.RefsNum() != 0 {
			return fmt.Errorf("oracle dictionary entry %d is malformed", i)
		}
	}
	return nil
}
