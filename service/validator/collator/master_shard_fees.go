package collator

import (
	"fmt"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type masterShardFeesValidationInput struct {
	fees          *tlb.ShardFeesAugDict
	newRegistry   *ShardRegistry
	tops          []ShardTop
	newBlockSeqno uint32
}

// validateMasterShardFees checks the permissive ShardFees predicate candidate
// validation applies. In particular, it does not require the collator's
// canonical zero-valued entries to be present.
func validateMasterShardFees(input masterShardFeesValidationInput) (tlb.ShardFeeCreated, error) {
	if input.fees.GetKeySize() != masterShardFeesKeyBits {
		return tlb.ShardFeeCreated{}, fmt.Errorf("%w: ShardFees key size is %d, want %d",
			ErrInvalidInput, input.fees.GetKeySize(), masterShardFeesKeyBits)
	}

	present := make(map[shardRegistryKey]tlb.ShardFeeCreated, len(input.tops))
	valid, err := input.fees.ValidateCheckExtra(func(value, extra *cell.Slice, key *cell.Cell) (bool, error) {
		registryKey, err := loadMasterShardFeesKey(key)
		if err != nil {
			return false, fmt.Errorf("%w: invalid ShardFees key: %v", ErrInvalidInput, err)
		}
		if err = validateMasterShardFeeLeafExtra(value, extra); err != nil {
			return false, fmt.Errorf("%w: ShardFees entry %s: %v", ErrInvalidInput,
				formatShardRegistryKey(registryKey), err)
		}

		leaf, exists := input.newRegistry.leaves[registryKey]
		if !exists {
			return false, fmt.Errorf("%w: ShardFees entry %s has no new shard descriptor",
				ErrInvalidInput, formatShardRegistryKey(registryKey))
		}
		fields := leaf.fields
		if fields.regMCSeqno != input.newBlockSeqno {
			return false, fmt.Errorf("%w: ShardFees entry %s refers to descriptor registered by mc block %d, want %d",
				ErrInvalidInput, formatShardRegistryKey(registryKey), fields.regMCSeqno, input.newBlockSeqno)
		}

		record, err := loadMasterShardFeeCreated(value)
		if err != nil {
			return false, fmt.Errorf("%w: decode ShardFees entry %s: %v", ErrInvalidInput,
				formatShardRegistryKey(registryKey), err)
		}
		if !record.Fees.Equals(fields.fees) {
			return false, fmt.Errorf("%w: ShardFees entry %s has collected fees different from its descriptor",
				ErrInvalidInput, formatShardRegistryKey(registryKey))
		}
		if !record.Create.Equals(fields.created) {
			return false, fmt.Errorf("%w: ShardFees entry %s has created funds different from its descriptor",
				ErrInvalidInput, formatShardRegistryKey(registryKey))
		}
		present[registryKey] = record
		return true, nil
	}, false)
	if err != nil {
		return tlb.ShardFeeCreated{}, fmt.Errorf("%w: validate ShardFees dictionary: %v", ErrInvalidInput, err)
	}
	if !valid {
		return tlb.ShardFeeCreated{}, fmt.Errorf("%w: invalid ShardFees dictionary", ErrInvalidInput)
	}

	for i := range input.tops {
		key := shardTopKey(input.tops[i].Block)
		fields, err := parseShardDescriptorFields(input.tops[i].Descriptor)
		if err != nil {
			return tlb.ShardFeeCreated{}, fmt.Errorf("%w: parse accepted shard top %s: %v", ErrInvalidInput,
				formatShardRegistryKey(key), err)
		}
		record, exists := present[key]
		if !exists {
			// An entry may be omitted based only on zero fees_collected;
			// funds_created is deliberately not part of this absence predicate.
			if !currencyZero(fields.fees) {
				return tlb.ShardFeeCreated{}, fmt.Errorf(
					"%w: accepted shard top %s has non-zero fees but no ShardFees entry",
					ErrInvalidInput, formatShardRegistryKey(key),
				)
			}
			continue
		}
		if !record.Fees.Equals(fields.fees) || !record.Create.Equals(fields.created) {
			return tlb.ShardFeeCreated{}, fmt.Errorf(
				"%w: ShardFees entry %s differs from the accepted shard descriptor",
				ErrInvalidInput, formatShardRegistryKey(key),
			)
		}
	}

	var rootExtra cell.Slice
	if err := input.fees.LoadRootExtraInto(&rootExtra); err != nil {
		return tlb.ShardFeeCreated{}, fmt.Errorf("%w: load ShardFees root extra: %v", ErrInvalidInput, err)
	}
	total, err := loadMasterShardFeeCreated(&rootExtra)
	if err != nil {
		return tlb.ShardFeeCreated{}, fmt.Errorf("%w: decode ShardFees root extra: %v", ErrInvalidInput, err)
	}
	return total, nil
}

func loadMasterShardFeeCreated(loader *cell.Slice) (tlb.ShardFeeCreated, error) {
	check := *loader
	if err := validateMasterShardCurrencySlice(&check); err != nil {
		return tlb.ShardFeeCreated{}, fmt.Errorf("fees: %w", err)
	}
	if err := validateMasterShardCurrencySlice(&check); err != nil {
		return tlb.ShardFeeCreated{}, fmt.Errorf("created funds: %w", err)
	}
	if check.BitsLeft() != 0 || check.RefsNum() != 0 {
		return tlb.ShardFeeCreated{}, fmt.Errorf("trailing data: %d bits, %d refs",
			check.BitsLeft(), check.RefsNum())
	}

	var record tlb.ShardFeeCreated
	if err := loadExactSlice(&record, loader); err != nil {
		return tlb.ShardFeeCreated{}, err
	}
	return record, nil
}

func validateMasterShardCurrencySlice(loader *cell.Slice) error {
	length, err := loader.LoadUInt(4)
	if err != nil {
		return fmt.Errorf("load grams length: %w", err)
	}
	if length != 0 {
		grams, err := loader.LoadSlice(uint(length) * 8)
		if err != nil {
			return fmt.Errorf("load grams: %w", err)
		}
		if grams[0] == 0 {
			return fmt.Errorf("grams has a leading zero byte")
		}
	}

	extra, err := loader.LoadOptionalDict(32)
	if err != nil {
		return fmt.Errorf("load extra currencies: %w", err)
	}
	if extra == nil || extra.IsEmpty() {
		return nil
	}
	if !extra.ValidateAll() {
		return fmt.Errorf("extra currency dictionary is invalid")
	}
	items, err := extra.LoadAll()
	if err != nil {
		return fmt.Errorf("load extra currencies: %w", err)
	}
	for _, item := range items {
		if item.Key.BitsLeft() != 32 || item.Key.RefsNum() != 0 {
			return fmt.Errorf("extra currency key has invalid shape")
		}
		amountLength, err := item.Value.LoadUInt(5)
		if err != nil {
			return fmt.Errorf("load extra currency amount length: %w", err)
		}
		if amountLength == 0 || amountLength >= 32 {
			return fmt.Errorf("extra currency amount length %d is outside 1..31", amountLength)
		}
		amount, err := item.Value.LoadSlice(uint(amountLength) * 8)
		if err != nil {
			return fmt.Errorf("load extra currency amount: %w", err)
		}
		if amount[0] == 0 {
			return fmt.Errorf("extra currency amount has a leading zero byte")
		}
		if item.Value.BitsLeft() != 0 || item.Value.RefsNum() != 0 {
			return fmt.Errorf("extra currency amount has trailing data")
		}
	}
	return nil
}

func loadMasterShardFeesKey(key *cell.Cell) (shardRegistryKey, error) {
	var loader cell.Slice
	err := key.BeginParseInto(&loader)
	if err != nil {
		return shardRegistryKey{}, err
	}
	if loader.BitsLeft() != masterShardFeesKeyBits || loader.RefsNum() != 0 {
		return shardRegistryKey{}, fmt.Errorf("key has %d bits and %d refs", loader.BitsLeft(), loader.RefsNum())
	}
	workchain, err := loader.LoadInt(32)
	if err != nil {
		return shardRegistryKey{}, err
	}
	shardID, err := loader.LoadUInt(64)
	if err != nil {
		return shardRegistryKey{}, err
	}
	return shardRegistryKey{workchain: int32(workchain), shard: int64(shardID)}, nil
}

func validateMasterShardFeeLeafExtra(value, extra *cell.Slice) error {
	valueCell, err := value.ToCell()
	if err != nil {
		return fmt.Errorf("materialize value: %w", err)
	}
	extraCell, err := extra.ToCell()
	if err != nil {
		return fmt.Errorf("materialize leaf augmentation: %w", err)
	}
	if valueCell.HashKey() != extraCell.HashKey() {
		return fmt.Errorf("leaf augmentation differs from value")
	}
	return nil
}
