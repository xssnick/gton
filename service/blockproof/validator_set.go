package blockproof

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"sort"

	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type validatorEntry struct {
	addr      *tlb.ValidatorAddr
	key       uint16
	cumWeight uint64
}

type validatorSetParams struct {
	entries     []validatorEntry
	main        int
	totalWeight uint64
}

type validatorWeightHole struct {
	start  uint64
	weight uint64
}

var crc32CastagnoliTable = crc32.MakeTable(crc32.Castagnoli)

func CurrentValidatorsForBlock(cfg *tlb.BlockchainConfig, block *ton.BlockIDExt, ccSeqno uint32) ([]*tlb.ValidatorAddr, error) {
	validatorsCfg, err := cfg.GetCurrentValidators()
	if err != nil {
		return nil, fmt.Errorf("load current validators: %w", err)
	}
	return ValidatorsForBlock(cfg, block, *validatorsCfg, ccSeqno)
}

func PrevValidatorsForBlock(cfg *tlb.BlockchainConfig, block *ton.BlockIDExt, ccSeqno uint32) ([]*tlb.ValidatorAddr, error) {
	validatorsCfg, err := cfg.GetPrevValidators()
	if err != nil {
		return nil, fmt.Errorf("load previous validators: %w", err)
	}
	return ValidatorsForBlock(cfg, block, *validatorsCfg, ccSeqno)
}

func NextValidatorsForBlock(cfg *tlb.BlockchainConfig, block *ton.BlockIDExt, ccSeqno uint32) ([]*tlb.ValidatorAddr, error) {
	validatorsCfg, err := cfg.GetNextValidators()
	if err != nil {
		return nil, fmt.Errorf("load next validators: %w", err)
	}
	return ValidatorsForBlock(cfg, block, *validatorsCfg, ccSeqno)
}

func ValidatorsForBlock(cfg *tlb.BlockchainConfig, block *ton.BlockIDExt, validatorConfig tlb.ValidatorSetAny, ccSeqno uint32) ([]*tlb.ValidatorAddr, error) {
	catchainCfg, err := cfg.GetCatchainConfig()
	if err != nil {
		return nil, fmt.Errorf("load catchain config: %w", err)
	}

	shuffleMasterchain, shardValidatorsNum, err := catchainValidatorOptions(catchainCfg)
	if err != nil {
		return nil, err
	}

	params, err := loadValidatorSetParams(validatorConfig)
	if err != nil {
		return nil, err
	}
	if len(params.entries) == 0 {
		return nil, fmt.Errorf("zero validators")
	}

	if block.Workchain == -1 {
		return masterchainValidatorsForSet(block, params, ccSeqno, shuffleMasterchain), nil
	}
	return shardValidatorsForSet(block, params, ccSeqno, shardValidatorsNum)
}

func ValidatorSetHash(ccSeqno uint32, validators []*tlb.ValidatorAddr) (uint32, error) {
	items := make([]ton.ValidatorItemHashable, len(validators))
	for i, validator := range validators {
		items[i] = ton.ValidatorItemHashable{
			Key:    bytes.Clone(validator.PublicKey.Key),
			Weight: validator.Weight,
			Addr:   normalizeADNLAddr(validator.ADNLAddr),
		}
	}

	data, err := tl.Serialize(ton.ValidatorSetHashable{
		CCSeqno:    ccSeqno,
		Validators: items,
	}, true)
	if err != nil {
		return 0, fmt.Errorf("serialize validator set hash payload: %w", err)
	}
	return crc32.Checksum(data, crc32CastagnoliTable), nil
}

func catchainValidatorOptions(cfg tlb.CatchainConfig) (bool, uint32, error) {
	switch c := cfg.Config.(type) {
	case tlb.CatchainConfigV1:
		return false, c.ShardValidatorsNum, nil
	case tlb.CatchainConfigV2:
		return c.ShuffleMcValidators, c.ShardValidatorsNum, nil
	default:
		return false, 0, fmt.Errorf("unknown catchain config type %T", cfg.Config)
	}
}

func loadValidatorSetParams(validatorConfig tlb.ValidatorSetAny) (validatorSetParams, error) {
	var total int
	var main int
	var totalWeight *uint64
	var list *cell.Dictionary

	switch v := validatorConfig.Validators.(type) {
	case tlb.ValidatorSet:
		total = int(v.Total)
		main = int(v.Main)
		list = v.List
	case tlb.ValidatorSetExt:
		if v.TotalWeight == 0 {
			return validatorSetParams{}, fmt.Errorf("validator set declares zero total weight")
		}
		total = int(v.Total)
		main = int(v.Main)
		totalWeight = &v.TotalWeight
		list = v.List
	default:
		return validatorSetParams{}, fmt.Errorf("unknown validator set type %T", validatorConfig.Validators)
	}
	if main <= 0 {
		main = total
	}

	entries, sumWeight, err := loadValidatorEntries(list, total)
	if err != nil {
		return validatorSetParams{}, err
	}
	if totalWeight != nil && sumWeight != *totalWeight {
		return validatorSetParams{}, fmt.Errorf("validator set declares incorrect total weight")
	}

	return validatorSetParams{
		entries:     entries,
		main:        main,
		totalWeight: sumWeight,
	}, nil
}

func loadValidatorEntries(dict *cell.Dictionary, total int) ([]validatorEntry, uint64, error) {
	if total <= 0 {
		return nil, 0, fmt.Errorf("validator set has invalid total %d", total)
	}
	if dict == nil {
		return nil, 0, fmt.Errorf("validator set dictionary is empty")
	}

	items, err := dict.LoadAll()
	if err != nil {
		return nil, 0, fmt.Errorf("load validator set dictionary: %w", err)
	}
	if len(items) != total {
		return nil, 0, fmt.Errorf("validator set dictionary has %d entries, expected %d", len(items), total)
	}

	entries := make([]validatorEntry, 0, len(items))
	var totalWeight uint64
	for _, item := range items {
		key, err := item.Key.LoadUInt(16)
		if err != nil {
			return nil, 0, fmt.Errorf("load validator index: %w", err)
		}
		if key >= uint64(total) {
			return nil, 0, fmt.Errorf("validator index %d is outside total %d", key, total)
		}

		addr, err := loadValidatorAddr(item.Value)
		if err != nil {
			return nil, 0, fmt.Errorf("load validator %d: %w", key, err)
		}
		if addr.Weight == 0 {
			return nil, 0, fmt.Errorf("validator %d has zero weight", key)
		}
		if ^uint64(0)-totalWeight < addr.Weight {
			return nil, 0, fmt.Errorf("total validator weight overflows uint64")
		}

		entries = append(entries, validatorEntry{
			addr:      addr,
			key:       uint16(key),
			cumWeight: totalWeight,
		})
		totalWeight += addr.Weight
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].key < entries[j].key
	})
	for i, entry := range entries {
		if int(entry.key) != i {
			return nil, 0, fmt.Errorf("validator set indices must be contiguous from 0")
		}
	}
	return entries, totalWeight, nil
}

func loadValidatorAddr(s *cell.Slice) (*tlb.ValidatorAddr, error) {
	var addr tlb.ValidatorAddr
	if err := tlb.LoadFromCell(&addr, s.Copy()); err == nil {
		addr.PublicKey.Key = bytes.Clone(addr.PublicKey.Key)
		addr.ADNLAddr = normalizeADNLAddr(addr.ADNLAddr)
		return &addr, nil
	}

	var validator tlb.Validator
	if err := tlb.LoadFromCell(&validator, s.Copy()); err != nil {
		return nil, err
	}
	return &tlb.ValidatorAddr{
		PublicKey: tlb.SigPubKeyED25519{Key: bytes.Clone(validator.PublicKey.Key)},
		Weight:    validator.Weight,
		ADNLAddr:  make([]byte, 32),
	}, nil
}

func masterchainValidatorsForSet(block *ton.BlockIDExt, params validatorSetParams, ccSeqno uint32, shuffle bool) []*tlb.ValidatorAddr {
	count := params.main
	if count > len(params.entries) {
		count = len(params.entries)
	}

	validators := make([]*tlb.ValidatorAddr, count)
	if shuffle {
		prng := ton.NewValidatorSetPRNG(block.Shard, block.Workchain, ccSeqno, nil)
		idx := make([]uint32, count)
		for i := 0; i < count; i++ {
			j := prng.NextRanged(uint64(i) + 1)
			idx[i] = idx[j]
			idx[j] = uint32(i)
		}
		for i := 0; i < count; i++ {
			validators[i] = cloneValidatorAddr(params.entries[idx[i]].addr, params.entries[idx[i]].addr.Weight)
		}
		return validators
	}

	for i := 0; i < count; i++ {
		validators[i] = cloneValidatorAddr(params.entries[i].addr, params.entries[i].addr.Weight)
	}
	return validators
}

func shardValidatorsForSet(block *ton.BlockIDExt, params validatorSetParams, ccSeqno uint32, shardValidatorsNum uint32) ([]*tlb.ValidatorAddr, error) {
	count := int(shardValidatorsNum)
	if count > len(params.entries) {
		count = len(params.entries)
	}
	if count == 0 {
		return nil, fmt.Errorf("zero shard validators for %s", tnstore.FormatBlockRef(*block))
	}

	prng := ton.NewValidatorSetPRNG(block.Shard, block.Workchain, ccSeqno, nil)
	holes := make([]validatorWeightHole, 0, count)
	totalWeight := params.totalWeight
	validators := make([]*tlb.ValidatorAddr, 0, count)
	for i := 0; i < count; i++ {
		if totalWeight == 0 {
			return nil, fmt.Errorf("validator set weight exhausted while choosing shard validators")
		}

		p := prng.NextRanged(totalWeight)
		for _, hole := range holes {
			if p < hole.start {
				break
			}
			p += hole.weight
		}

		entry, err := validatorAtWeight(params.entries, p)
		if err != nil {
			return nil, err
		}
		validators = append(validators, cloneValidatorAddr(entry.addr, 1))
		totalWeight -= entry.addr.Weight

		hole := validatorWeightHole{
			start:  entry.cumWeight,
			weight: entry.addr.Weight,
		}
		pos := sort.Search(len(holes), func(i int) bool {
			if holes[i].start != hole.start {
				return holes[i].start > hole.start
			}
			return holes[i].weight > hole.weight
		})
		holes = append(holes, validatorWeightHole{})
		copy(holes[pos+1:], holes[pos:])
		holes[pos] = hole
	}
	return validators, nil
}

func validatorAtWeight(entries []validatorEntry, weight uint64) (validatorEntry, error) {
	idx := sort.Search(len(entries), func(i int) bool {
		return weight < entries[i].cumWeight
	})
	if idx == 0 {
		return validatorEntry{}, fmt.Errorf("validator set has no entry for weight %d", weight)
	}
	return entries[idx-1], nil
}

func cloneValidatorAddr(addr *tlb.ValidatorAddr, weight uint64) *tlb.ValidatorAddr {
	return &tlb.ValidatorAddr{
		PublicKey: tlb.SigPubKeyED25519{Key: bytes.Clone(addr.PublicKey.Key)},
		Weight:    weight,
		ADNLAddr:  normalizeADNLAddr(addr.ADNLAddr),
	}
}

func normalizeADNLAddr(addr []byte) []byte {
	if len(addr) == 0 {
		return make([]byte, 32)
	}
	return bytes.Clone(addr)
}

func cloneSignatures(signatures []ton.Signature) []ton.Signature {
	if len(signatures) == 0 {
		return nil
	}

	cloned := make([]ton.Signature, len(signatures))
	for i, sig := range signatures {
		cloned[i] = ton.Signature{
			NodeIDShort: bytes.Clone(sig.NodeIDShort),
			Signature:   bytes.Clone(sig.Signature),
		}
	}
	return cloned
}
