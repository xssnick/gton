package collator

import (
	"fmt"
	"math/bits"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// Configuration accessors intentionally allow unconsumed data and lazy
// dictionaries. Governance validation instead follows ConfigParam.validate_ref:
// every referenced record, dictionary fork and leaf must have the exact TL-B
// shape, within the reference implementation's 1024-cell budget per parameter.
type configParameterValidator struct{ remaining int }

func validateKnownConfigParameter(parameter *cell.Cell, id uint32) error {
	validator := configParameterValidator{remaining: 1024}
	return validator.ref(parameter, func(s *cell.Slice) error { return validator.parameter(s, id) })
}

func (v *configParameterValidator) ref(root *cell.Cell, decode func(*cell.Slice) error) error {
	v.remaining--
	if v.remaining < 0 {
		return fmt.Errorf("config parameter exceeds validation cell budget")
	}
	var s cell.Slice
	if err := root.BeginParseInto(&s); err != nil {
		return err
	}
	if s.IsSpecial() {
		return fmt.Errorf("config parameter contains an exotic cell")
	}
	if err := decode(&s); err != nil {
		return err
	}
	return configSliceEnd(&s)
}

func configSliceEnd(s *cell.Slice) error {
	if s.BitsLeft() != 0 || s.RefsNum() != 0 {
		return fmt.Errorf("trailing data: %d bits, %d refs", s.BitsLeft(), s.RefsNum())
	}
	return nil
}

func (v *configParameterValidator) parameter(s *cell.Slice, id uint32) error {
	switch id {
	case 1, 2, 3, 4:
		return s.SkipBits(256)
	case tlb.ConfigParamBurningConfig:
		var cfg tlb.BurningConfig
		if err := tlb.LoadFromCell(&cfg, s); err != nil {
			return err
		}
		if cfg.FeeBurnNum > cfg.FeeBurnDenom || cfg.FeeBurnDenom == 0 {
			return fmt.Errorf("invalid burning fraction")
		}
	case tlb.ConfigParamExtraCurrencyMintPrices:
		return configGrams(s, 2)
	case tlb.ConfigParamExtraCurrencyToMint:
		return v.hashmapE(s, 32, func(value *cell.Slice) error {
			if !validExtraCurrencyMintAmount(value) {
				return fmt.Errorf("invalid positive extra currency amount")
			}
			return nil
		})
	case tlb.ConfigParamGlobalVersion:
		return tlb.LoadFromCell(new(tlb.GlobalVersion), s)
	case tlb.ConfigParamMandatoryParams, tlb.ConfigParamCriticalParams:
		return v.hashmap(s, 32, configSliceEnd)
	case tlb.ConfigParamConfigVotingSetup:
		if err := configTag(s, 0x91, 8); err != nil {
			return err
		}
		for range 2 {
			root, err := s.LoadRefCell()
			if err != nil {
				return err
			}
			if err = v.ref(root, func(value *cell.Slice) error {
				var cfg tlb.ConfigProposalSetup
				if err := tlb.LoadFromCell(&cfg, value); err != nil {
					return err
				}
				if cfg.MinTotRounds > cfg.MaxTotRounds || cfg.MinStoreSec > cfg.MaxStoreSec {
					return fmt.Errorf("invalid configuration voting limits")
				}
				return nil
			}); err != nil {
				return err
			}
		}
	case tlb.ConfigParamWorkchains:
		return v.hashmapE(s, 32, validateConfigWorkchain)
	case tlb.ConfigParamComplaintPricing:
		if err := configTag(s, 0x1a, 8); err != nil {
			return err
		}
		return configGrams(s, 3)
	case tlb.ConfigParamBlockCreateFees:
		if err := configTag(s, 0x6b, 8); err != nil {
			return err
		}
		return configGrams(s, 2)
	case tlb.ConfigParamValidatorElectionTimings:
		return tlb.LoadFromCell(new(tlb.ValidatorElectionTimings), s)
	case tlb.ConfigParamValidatorCountLimits:
		var cfg tlb.ValidatorCountLimits
		if err := tlb.LoadFromCell(&cfg, s); err != nil {
			return err
		}
		if cfg.MaxValidators < cfg.MaxMainValidators || cfg.MaxMainValidators < cfg.MinValidators || cfg.MinValidators == 0 {
			return fmt.Errorf("invalid validator count limits")
		}
	case tlb.ConfigParamValidatorStakeLimits:
		if err := configGrams(s, 3); err != nil {
			return err
		}
		return s.SkipBits(32)
	case tlb.ConfigParamStoragePrices:
		return v.hashmap(s, 32, func(value *cell.Slice) error { return tlb.LoadFromCell(new(tlb.ConfigStoragePrices), value) })
	case tlb.ConfigParamGlobalID:
		return s.SkipBits(32)
	case tlb.ConfigParamGasPricesMasterchain, tlb.ConfigParamGasPricesBasechain:
		// GasLimitsPrices is recursive; the accessor only accepts one flat prefix
		// around the extended form, while TL-B also permits the plain form.
		for {
			tag, err := s.LoadUInt(8)
			if err != nil {
				return err
			}
			switch tag {
			case 0xd1:
				if err = s.SkipBits(128); err != nil {
					return err
				}
			case 0xdd:
				return s.SkipBits(6 * 64)
			case 0xde:
				return s.SkipBits(7 * 64)
			default:
				return fmt.Errorf("invalid gas prices constructor")
			}
		}
	case tlb.ConfigParamBlockLimitsMasterchain, tlb.ConfigParamBlockLimitsBasechain:
		var cfg tlb.BlockLimits
		if err := tlb.LoadFromCell(&cfg, s); err != nil {
			return err
		}
		switch limits := cfg.Limits.(type) {
		case tlb.BlockLimitsV1:
			return validateConfigLimits(limits.Bytes, limits.Gas, limits.LTDelta)
		case tlb.BlockLimitsV2:
			return validateConfigLimits(limits.Bytes, limits.Gas, limits.LTDelta, limits.CollatedData)
		}
	case tlb.ConfigParamMsgForwardPricesMasterchain, tlb.ConfigParamMsgForwardPricesBasechain:
		return tlb.LoadFromCell(new(tlb.ConfigMsgForwardPrices), s)
	case tlb.ConfigParamCatchainConfig:
		tag, err := s.LoadUInt(8)
		if err != nil {
			return err
		}
		switch tag {
		case 0xc1:
		case 0xc2:
			if err = configTag(s, 0, 7); err != nil {
				return err
			}
			if err = s.SkipBits(1); err != nil {
				return err
			}
		default:
			return fmt.Errorf("invalid catchain constructor")
		}
		for range 4 {
			value, err := s.LoadUInt(32)
			if err != nil {
				return err
			}
			if value == 0 {
				return fmt.Errorf("zero catchain lifetime or validator count")
			}
		}
	case tlb.ConfigParamConsensusConfig:
		return validateConfigConsensus(s)
	case tlb.ConfigParamNewConsensusConfig:
		if err := configTag(s, 0x10, 8); err != nil {
			return err
		}
		for range 2 {
			present, err := s.LoadBoolBit()
			if err != nil {
				return err
			}
			if !present {
				continue
			}
			root, err := s.LoadRefCell()
			if err != nil {
				return err
			}
			if err = v.ref(root, v.simplex); err != nil {
				return err
			}
		}
	case tlb.ConfigParamFundamentalSMCAddresses:
		return v.hashmapE(s, 256, configSliceEnd)
	case tlb.ConfigParamPrevValidators, tlb.ConfigParamPrevTempValidators,
		tlb.ConfigParamCurrentValidators, tlb.ConfigParamCurrentTempValidators,
		tlb.ConfigParamNextValidators, tlb.ConfigParamNextTempValidators:
		return v.validators(s)
	case tlb.ConfigParamValidatorTempKeys:
		return v.hashmapE(s, 256, v.signedTempKey)
	case tlb.ConfigParamMisbehaviourPunishment:
		if err := configTag(s, 1, 8); err != nil {
			return err
		}
		if err := configGrams(s, 1); err != nil {
			return err
		}
		return s.SkipBits(32 + 9*16)
	case tlb.ConfigParamSizeLimits:
		return tlb.LoadFromCell(new(tlb.SizeLimitsConfig), s)
	case tlb.ConfigParamSuspendedAddressList:
		if err := configTag(s, 0, 8); err != nil {
			return err
		}
		if err := v.hashmapE(s, 288, configSliceEnd); err != nil {
			return err
		}
		return s.SkipBits(32)
	case tlb.ConfigParamPrecompiledContracts:
		if err := configTag(s, 0xc0, 8); err != nil {
			return err
		}
		return v.hashmapE(s, 256, func(value *cell.Slice) error { return tlb.LoadFromCell(new(tlb.PrecompiledSmc), value) })
	case 46:
		if err := configTag(s, validatorRegistryConfigConstructor, 32); err != nil {
			return err
		}
		if err := s.SkipBits(256 + 32); err != nil {
			return err
		}
		present, err := s.LoadBoolBit()
		if err != nil {
			return err
		}
		if present {
			return s.SkipBits(256)
		}
	case 71, 72, 73:
		if err := s.SkipBits(512); err != nil {
			return err
		}
		if err := v.hashmapE(s, 256, func(value *cell.Slice) error { return value.SkipBits(256) }); err != nil {
			return err
		}
		return s.SkipBits(256)
	case 79, 81, 82:
		return v.jettonBridge(s)
	default:
		return fmt.Errorf("unsupported positive parameter")
	}
	return nil
}

func configTag(s *cell.Slice, want uint64, size uint) error {
	tag, err := s.LoadUInt(size)
	if err != nil {
		return err
	}
	if tag != want {
		return fmt.Errorf("unexpected constructor or flags: %x, want %x", tag, want)
	}
	return nil
}

func (v *configParameterValidator) hashmapE(s *cell.Slice, keyBits uint, leaf func(*cell.Slice) error) error {
	present, err := s.LoadBoolBit()
	if err != nil || !present {
		return err
	}
	root, err := s.LoadRefCell()
	if err != nil {
		return err
	}
	var node cell.Slice
	return v.hashmapRef(root, &node, keyBits, leaf)
}

func (v *configParameterValidator) hashmap(s *cell.Slice, keyBits uint, leaf func(*cell.Slice) error) error {
	// HmLabel permits short, long and same-bit encodings. Do not require the
	// canonical encoding: only the decoded length and exact node shape matter.
	first, err := s.LoadBoolBit()
	if err != nil {
		return err
	}
	var length uint64
	if !first {
		for {
			bit, err := s.LoadBoolBit()
			if err != nil {
				return err
			}
			if !bit {
				break
			}
			length++
			if length > uint64(keyBits) {
				return fmt.Errorf("dictionary label exceeds key length")
			}
		}
		if err = s.SkipBits(uint(length)); err != nil {
			return err
		}
	} else {
		same, err := s.LoadBoolBit()
		if err != nil {
			return err
		}
		if same {
			if err = s.SkipBits(1); err != nil {
				return err
			}
		}
		length, err = s.LoadUInt(uint(bits.Len(keyBits)))
		if err != nil {
			return err
		}
		if length > uint64(keyBits) {
			return fmt.Errorf("dictionary label exceeds key length")
		}
		if !same {
			if err = s.SkipBits(uint(length)); err != nil {
				return err
			}
		}
	}
	remaining := keyBits - uint(length)
	if remaining == 0 {
		if err = leaf(s); err != nil {
			return err
		}
		return configSliceEnd(s)
	}
	if s.BitsLeft() != 0 || s.RefsNum() != 2 {
		return fmt.Errorf("dictionary fork is not exactly two references")
	}
	left, err := s.LoadRefCell()
	if err != nil {
		return err
	}
	right, err := s.LoadRefCell()
	if err != nil {
		return err
	}
	// Both refs are retained before descending; the consumed parent slice can
	// now serve as scratch for every child, without a heap Slice per node.
	if err = v.hashmapRef(left, s, remaining-1, leaf); err != nil {
		return err
	}
	return v.hashmapRef(right, s, remaining-1, leaf)
}

func (v *configParameterValidator) hashmapRef(root *cell.Cell, s *cell.Slice, keyBits uint, leaf func(*cell.Slice) error) error {
	v.remaining--
	if v.remaining < 0 {
		return fmt.Errorf("config parameter exceeds validation cell budget")
	}
	if err := root.BeginParseInto(s); err != nil {
		return err
	}
	if s.IsSpecial() {
		return fmt.Errorf("config parameter contains an exotic cell")
	}
	return v.hashmap(s, keyBits, leaf)
}

func validateConfigWorkchain(s *cell.Slice) error {
	var cfg tlb.WorkchainDescr
	if err := tlb.LoadFromCell(&cfg, s); err != nil {
		return err
	}
	var fields tlb.WorkchainDescrFields
	switch descr := cfg.Descr.(type) {
	case tlb.WorkchainDescrV1:
		fields = descr.WorkchainDescrFields
	case tlb.WorkchainDescrV2:
		fields = descr.WorkchainDescrFields
		if descr.PersistentStateSplitDepth > 63 {
			return fmt.Errorf("persistent state split depth exceeds 63")
		}
	}
	if fields.ActualMinSplit > fields.MinSplit || fields.MinSplit > fields.MaxSplit || fields.MaxSplit > 60 || fields.Flags != 0 {
		return fmt.Errorf("invalid workchain split bounds or flags")
	}
	switch format := fields.Format.(type) {
	case tlb.WorkchainFormatBasic:
		if !fields.Basic {
			return fmt.Errorf("basic format in extended workchain")
		}
	case tlb.WorkchainFormatExtended:
		if fields.Basic || format.MinAddrLen < 64 || format.MinAddrLen > format.MaxAddrLen || format.MaxAddrLen > 1023 || format.AddrLenStep > 1023 || format.WorkchainTypeID == 0 {
			return fmt.Errorf("invalid extended workchain format")
		}
	}
	return nil
}

func validateConfigLimits(limits ...tlb.ParamLimits) error {
	for _, limit := range limits {
		if limit.Underload > limit.SoftLimit || limit.SoftLimit > limit.HardLimit {
			return fmt.Errorf("invalid block parameter limits")
		}
	}
	return nil
}

func validateConfigConsensus(s *cell.Slice) error {
	tag, err := s.LoadUInt(8)
	if err != nil {
		return err
	}
	candidateBits := uint(8)
	switch tag {
	case 0xd6:
		candidateBits = 32
	case 0xd7, 0xd8:
		if err = configTag(s, 0, 7); err != nil {
			return err
		}
		if err = s.SkipBits(1); err != nil {
			return err
		}
	case 0xd9:
		if err = configTag(s, 0, 6); err != nil {
			return err
		}
		if err = s.SkipBits(2); err != nil {
			return err
		}
	default:
		return fmt.Errorf("invalid consensus constructor")
	}
	candidates, err := s.LoadUInt(candidateBits)
	if err != nil {
		return err
	}
	if candidates == 0 {
		return fmt.Errorf("zero consensus round candidates")
	}
	if err = s.SkipBits(7 * 32); err != nil {
		return err
	}
	if tag >= 0xd8 {
		if err = s.SkipBits(16); err != nil {
			return err
		}
	}
	if tag == 0xd9 {
		return s.SkipBits(32)
	}
	return nil
}

func (v *configParameterValidator) simplex(s *cell.Slice) error {
	tag, err := s.LoadUInt(8)
	if err != nil {
		return err
	}
	if err = s.SkipBits(8); err != nil {
		return err
	}
	switch tag {
	case 0x21:
		if err = s.SkipBits(32); err != nil {
			return err
		}
	case 0x22:
	default:
		return fmt.Errorf("invalid simplex constructor")
	}
	slots, err := s.LoadUInt(32)
	if err != nil {
		return err
	}
	if slots == 0 {
		return fmt.Errorf("zero simplex slots per leader window")
	}
	if tag == 0x21 {
		return s.SkipBits(64)
	}
	return v.hashmapE(s, 8, func(value *cell.Slice) error { return value.SkipBits(32) })
}

func (v *configParameterValidator) validators(s *cell.Slice) error {
	tag, err := s.LoadUInt(8)
	if err != nil {
		return err
	}
	if tag != 0x11 && tag != 0x12 {
		return fmt.Errorf("invalid validator set constructor")
	}
	if err = s.SkipBits(64); err != nil {
		return err
	}
	total, err := s.LoadUInt(16)
	if err != nil {
		return err
	}
	main, err := s.LoadUInt(16)
	if err != nil {
		return err
	}
	if main == 0 || main > total {
		return fmt.Errorf("invalid main validator count")
	}
	leaf := func(value *cell.Slice) error {
		tag, err := value.LoadUInt(8)
		if err != nil {
			return err
		}
		if tag != 0x53 && tag != 0x73 {
			return fmt.Errorf("invalid validator constructor")
		}
		if err = configTag(value, 0x8e81278a, 32); err != nil {
			return err
		}
		if err = value.SkipBits(256 + 64); err != nil {
			return err
		}
		if tag == 0x73 {
			return value.SkipBits(256)
		}
		return nil
	}
	if tag == 0x11 {
		return v.hashmap(s, 16, leaf)
	}
	if err = s.SkipBits(64); err != nil {
		return err
	}
	return v.hashmapE(s, 16, leaf)
}

func (v *configParameterValidator) signedTempKey(s *cell.Slice) error {
	if err := configTag(s, 4, 4); err != nil {
		return err
	}
	root, err := s.LoadRefCell()
	if err != nil {
		return err
	}
	if err = v.ref(root, func(value *cell.Slice) error {
		if err := configTag(value, 3, 4); err != nil {
			return err
		}
		if err := value.SkipBits(256); err != nil {
			return err
		}
		if err := tlb.LoadFromCell(new(tlb.SigPubKeyED25519), value); err != nil {
			return err
		}
		return value.SkipBits(64)
	}); err != nil {
		return err
	}
	return v.signature(s)
}

func (v *configParameterValidator) signature(s *cell.Slice) error {
	tag, err := s.LoadUInt(4)
	if err != nil {
		return err
	}
	if tag == 5 {
		return s.SkipBits(512)
	}
	if tag != 15 {
		return fmt.Errorf("invalid crypto signature constructor")
	}
	root, err := s.LoadRefCell()
	if err != nil {
		return err
	}
	if err = v.ref(root, func(value *cell.Slice) error {
		if err := configTag(value, 4, 4); err != nil {
			return err
		}
		if err := tlb.LoadFromCell(new(tlb.SigPubKeyED25519), value); err != nil {
			return err
		}
		if err := value.SkipBits(64); err != nil {
			return err
		}
		return v.signature(value)
	}); err != nil {
		return err
	}
	if err = configTag(s, 5, 4); err != nil {
		return err
	}
	return s.SkipBits(512)
}

func (v *configParameterValidator) jettonBridge(s *cell.Slice) error {
	version, err := s.LoadUInt(8)
	if err != nil {
		return err
	}
	if err = s.SkipBits(512); err != nil {
		return err
	}
	if err = v.hashmapE(s, 256, func(value *cell.Slice) error { return value.SkipBits(256) }); err != nil {
		return err
	}
	if err = s.SkipBits(8); err != nil {
		return err
	}
	switch version {
	case 0:
		return configGrams(s, 1)
	case 1:
		prices, err := s.LoadRefCell()
		if err != nil {
			return err
		}
		if err = v.ref(prices, func(value *cell.Slice) error {
			return configGrams(value, 6)
		}); err != nil {
			return err
		}
		return s.SkipBits(256)
	default:
		return fmt.Errorf("unknown jetton bridge version %d", version)
	}
}

// Grams uses VarUInteger 16: zero has zero length, and every nonzero
// magnitude starts with a nonzero byte (VarUInteger::validate_skip).
func configGrams(s *cell.Slice, count int) error {
	for range count {
		length, err := s.LoadUInt(4)
		if err != nil {
			return err
		}
		if length == 0 {
			continue
		}
		first, err := s.LoadUInt(8)
		if err != nil {
			return err
		}
		if first == 0 {
			return fmt.Errorf("nonminimal Grams encoding")
		}
		if err = s.SkipBits(uint(length-1) * 8); err != nil {
			return err
		}
	}
	return nil
}
