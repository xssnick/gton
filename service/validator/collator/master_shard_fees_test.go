package collator

import (
	"errors"
	"math/big"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/shard"
)

type masterShardFeesTestEntry struct {
	block   ton.BlockIDExt
	options masterShardTestDescriptorOptions
}

func TestValidateMasterShardFeesAcceptsExactEntriesAndReturnsTotals(t *testing.T) {
	first := masterShardTestBlock(0, shard.Root, 10, 0x10)
	second := masterShardTestBlock(1, shard.Root, 11, 0x11)
	registry := masterShardFeesTestRegistry(t,
		masterShardFeesTestEntry{
			block: first,
			options: masterShardTestDescriptorOptions{
				regMCSeqno: 7,
				fees:       5,
				created:    2,
			},
		},
		masterShardFeesTestEntry{
			block: second,
			options: masterShardTestDescriptorOptions{
				regMCSeqno: 7,
				fees:       8,
				created:    3,
			},
		},
	)
	fees := masterShardFeesTestDictionary(t, registry.leaves)

	total, err := validateMasterShardFees(masterShardFeesValidationInput{
		fees:          fees,
		newRegistry:   registry,
		tops:          registry.Tops(),
		newBlockSeqno: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
	if total.Fees.Coins.Nano().Int64() != 13 || total.Create.Coins.Nano().Int64() != 5 {
		t.Fatalf("root total = %s/%s, want 13/5", total.Fees.Coins.Nano(), total.Create.Coins.Nano())
	}
}

func TestValidateMasterShardFeesPermitsMissingZeroFeeEntryWithCreatedFunds(t *testing.T) {
	block := masterShardTestBlock(0, shard.Root, 10, 0x10)
	registry := masterShardFeesTestRegistry(t, masterShardFeesTestEntry{
		block: block,
		options: masterShardTestDescriptorOptions{
			regMCSeqno: 7,
			created:    9,
		},
	})
	fees := masterShardFeesTestDictionary(t, nil)

	if _, err := validateMasterShardFees(masterShardFeesValidationInput{
		fees:          fees,
		newRegistry:   registry,
		tops:          registry.Tops(),
		newBlockSeqno: 7,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestValidateMasterShardFeesRequiresEntryForNonZeroAcceptedFees(t *testing.T) {
	block := masterShardTestBlock(0, shard.Root, 10, 0x10)
	registry := masterShardFeesTestRegistry(t, masterShardFeesTestEntry{
		block: block,
		options: masterShardTestDescriptorOptions{
			regMCSeqno: 7,
			fees:       1,
		},
	})
	fees := masterShardFeesTestDictionary(t, nil)

	_, err := validateMasterShardFees(masterShardFeesValidationInput{
		fees:          fees,
		newRegistry:   registry,
		tops:          registry.Tops(),
		newBlockSeqno: 7,
	})
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("validation error = %v, want ErrInvalidInput", err)
	}
}

func TestValidateMasterShardFeesRejectsFundsCreatedMismatch(t *testing.T) {
	block := masterShardTestBlock(0, shard.Root, 10, 0x10)
	registry := masterShardFeesTestRegistry(t, masterShardFeesTestEntry{
		block: block,
		options: masterShardTestDescriptorOptions{
			regMCSeqno: 7,
			fees:       5,
			created:    2,
		},
	})
	feeLeaves := cloneShardRegistryLeaves(registry.leaves)
	key := shardRegistryKey{workchain: block.Workchain, shard: block.Shard}
	leaf := feeLeaves[key]
	leaf.fields.created = masterShardFeesTestCurrency(3)
	feeLeaves[key] = leaf
	fees := masterShardFeesTestDictionary(t, feeLeaves)

	_, err := validateMasterShardFees(masterShardFeesValidationInput{
		fees:          fees,
		newRegistry:   registry,
		tops:          registry.Tops(),
		newBlockSeqno: 7,
	})
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("validation error = %v, want ErrInvalidInput", err)
	}
}

func TestValidateMasterShardFeesRejectsNonCanonicalCurrency(t *testing.T) {
	block := masterShardTestBlock(0, shard.Root, 10, 0x10)
	registry := masterShardFeesTestRegistry(t, masterShardFeesTestEntry{
		block: block,
		options: masterShardTestDescriptorOptions{
			regMCSeqno: 7,
		},
	})
	fees, err := tlb.NewShardFeesAugDict()
	if err != nil {
		t.Fatal(err)
	}
	key := cell.BeginCell().
		MustStoreInt(int64(block.Workchain), 32).
		MustStoreUInt(uint64(block.Shard), 64).
		EndCell()
	// A one-byte zero Grams payload decodes numerically as zero, but strict
	// unpacking rejects its non-canonical leading zero.
	value := cell.BeginCell().
		MustStoreUInt(1, 4).
		MustStoreUInt(0, 8).
		MustStoreBoolBit(false).
		MustStoreUInt(0, 5).
		EndCell()
	if err = fees.Set(key, value); err != nil {
		t.Fatal(err)
	}

	_, err = validateMasterShardFees(masterShardFeesValidationInput{
		fees:          fees,
		newRegistry:   registry,
		tops:          registry.Tops(),
		newBlockSeqno: 7,
	})
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("validation error = %v, want ErrInvalidInput", err)
	}
}

func TestValidateMasterShardFeesRejectsExtraAndStaleEntries(t *testing.T) {
	currentBlock := masterShardTestBlock(0, shard.Root, 10, 0x10)
	current := masterShardFeesTestRegistry(t, masterShardFeesTestEntry{
		block: currentBlock,
		options: masterShardTestDescriptorOptions{
			regMCSeqno: 7,
			fees:       5,
		},
	})
	extraBlock := masterShardTestBlock(1, shard.Root, 11, 0x11)
	extra := masterShardFeesTestRegistry(t, masterShardFeesTestEntry{
		block: extraBlock,
		options: masterShardTestDescriptorOptions{
			regMCSeqno: 7,
			fees:       1,
		},
	})
	extraFees := masterShardFeesTestDictionary(t, extra.leaves)
	if _, err := validateMasterShardFees(masterShardFeesValidationInput{
		fees:          extraFees,
		newRegistry:   current,
		newBlockSeqno: 7,
	}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("extra entry error = %v, want ErrInvalidInput", err)
	}

	stale := masterShardFeesTestRegistry(t, masterShardFeesTestEntry{
		block: currentBlock,
		options: masterShardTestDescriptorOptions{
			regMCSeqno: 6,
			fees:       5,
		},
	})
	staleFees := masterShardFeesTestDictionary(t, stale.leaves)
	if _, err := validateMasterShardFees(masterShardFeesValidationInput{
		fees:          staleFees,
		newRegistry:   stale,
		newBlockSeqno: 7,
	}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("stale entry error = %v, want ErrInvalidInput", err)
	}
}

func masterShardFeesTestRegistry(t *testing.T, entries ...masterShardFeesTestEntry) *ShardRegistry {
	t.Helper()
	leaves := make(map[shardRegistryKey]shardRegistryLeaf, len(entries))
	for _, entry := range entries {
		descriptor := masterShardTestDescriptor(t, entry.block, entry.options)
		leaf, err := decodeShardRegistryLeaf(entry.block.Workchain, entry.block.Shard, descriptor)
		if err != nil {
			t.Fatal(err)
		}
		leaves[shardTopKey(entry.block)] = leaf
	}
	if _, err := buildShardHashesDictionary(leaves); err != nil {
		t.Fatal(err)
	}
	return &ShardRegistry{
		leaves:   leaves,
		accepted: make(map[shardRegistryKey]shardRegistryLeaf),
	}
}

func masterShardFeesTestDictionary(
	t *testing.T,
	leaves map[shardRegistryKey]shardRegistryLeaf,
) *tlb.ShardFeesAugDict {
	t.Helper()
	fees, err := buildShardFeesDictionary(leaves)
	if err != nil {
		t.Fatal(err)
	}
	return fees
}

func masterShardFeesTestCurrency(nano int64) tlb.CurrencyCollection {
	return tlb.CurrencyCollection{Coins: tlb.FromNanoTON(big.NewInt(nano))}
}
