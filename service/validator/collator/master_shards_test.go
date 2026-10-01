package collator

import (
	"bytes"
	"errors"
	"math"
	"math/big"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/shard"
)

type masterShardTestDescriptorOptions struct {
	legacy             bool
	beforeSplit        bool
	beforeMerge        bool
	wantSplit          bool
	wantMerge          bool
	genUtime           uint32
	regMCSeqno         uint32
	nextCatchainSeqno  uint32
	nextValidatorShard int64
	minRefMCSeqno      uint32
	splitMerge         any
	fees               int64
	created            int64
}

type masterShardTestWorkchain struct {
	workchain int32
	tree      *cell.Cell
}

func TestShardRegistryRoundTripPreservesDescriptorsAndCanonicalOrder(t *testing.T) {
	left, right := masterShardTestChildren(t, shard.Root)
	leftBlock := masterShardTestBlock(0, left, 11, 0x11)
	rightBlock := masterShardTestBlock(0, right, 12, 0x12)
	otherBlock := masterShardTestBlock(2, shard.Root, 7, 0x27)
	leftDesc := masterShardTestDescriptor(t, leftBlock, masterShardTestDescriptorOptions{fees: 3, created: 1})
	rightDesc := masterShardTestDescriptor(t, rightBlock, masterShardTestDescriptorOptions{legacy: true, fees: 5, created: 2})
	otherDesc := masterShardTestDescriptor(t, otherBlock, masterShardTestDescriptorOptions{fees: 8, created: 4})

	original := masterShardTestHashes(t,
		masterShardTestWorkchain{
			workchain: 2,
			tree:      masterShardTestLeaf(otherDesc),
		},
		masterShardTestWorkchain{
			workchain: 0,
			tree: masterShardTestFork(
				masterShardTestLeaf(leftDesc),
				masterShardTestLeaf(rightDesc),
			),
		},
	)
	registry, err := ParseShardRegistry(original)
	if err != nil {
		t.Fatal(err)
	}

	tops := registry.Tops()
	if len(tops) != 3 {
		t.Fatalf("top count = %d, want 3", len(tops))
	}
	wantBlocks := []ton.BlockIDExt{leftBlock, rightBlock, otherBlock}
	wantDescriptors := []*cell.Cell{leftDesc, rightDesc, otherDesc}
	for i := range tops {
		if !sameShardBlock(tops[i].Block, wantBlocks[i]) {
			t.Fatalf("top %d block = %+v, want %+v", i, tops[i].Block, wantBlocks[i])
		}
		if tops[i].Descriptor.HashKey() != wantDescriptors[i].HashKey() {
			t.Fatalf("top %d descriptor changed", i)
		}
	}

	rebuilt, fees, err := masterShardTestBuild(t, registry)
	if err != nil {
		t.Fatal(err)
	}
	if !sameDictionaryRoot(original, rebuilt) {
		t.Fatal("ShardHashes root changed without an update")
	}
	if !fees.IsEmpty() {
		t.Fatal("registry without Apply produced shard fees")
	}

	reloaded, err := ParseShardRegistry(rebuilt)
	if err != nil {
		t.Fatalf("parse rebuilt registry: %v", err)
	}
	reloadedTops := reloaded.Tops()
	for i := range reloadedTops {
		if reloadedTops[i].Descriptor.HashKey() != wantDescriptors[i].HashKey() {
			t.Fatalf("reloaded top %d descriptor changed", i)
		}
	}
}

func TestShardRegistryLoadsLazyTreeBranches(t *testing.T) {
	block := masterShardTestBlock(0, shard.Root, 1, 0x4c)
	tree := masterShardTestLeaf(masterShardTestDescriptor(t, block, masterShardTestDescriptorOptions{}))
	parent := cell.BeginCell().MustStoreRef(tree).EndCell()
	lazyParent, err := cell.CreateWithLazyRefsUnsafe(
		0x0100,
		nil,
		parent.Hash(),
		[]uint16{parent.Depth()},
		[]cell.LazyRef{{
			LevelMask: tree.LevelMask(),
			Hashes:    tree.Hash(),
			Depths:    []uint16{tree.Depth()},
		}},
		func(hash cell.Hash) (*cell.Cell, error) {
			if hash == tree.HashKey() {
				return tree, nil
			}
			return nil, errors.New("unexpected lazy shard tree hash")
		},
	)
	if err != nil {
		t.Fatalf("build lazy shard tree branch: %v", err)
	}

	dict := cell.NewDict(masterShardHashesKeyBits)
	key := cell.BeginCell().MustStoreInt(0, masterShardHashesKeyBits).EndCell()
	if err = dict.Set(key, lazyParent); err != nil {
		t.Fatalf("store lazy shard tree: %v", err)
	}
	registry, err := ParseShardRegistry(dict)
	if err != nil {
		t.Fatalf("parse lazy shard tree: %v", err)
	}
	tops := registry.Tops()
	if len(tops) != 1 || !sameShardBlock(tops[0].Block, block) {
		t.Fatalf("lazy shard registry tops = %+v, want %+v", tops, block)
	}
}

func TestShardRegistryApplyIsPermutationDeterministic(t *testing.T) {
	left, right := masterShardTestChildren(t, shard.Root)
	oldLeft := masterShardTestBlock(0, left, 20, 0x20)
	oldRight := masterShardTestBlock(0, right, 21, 0x21)
	current := masterShardTestHashes(t, masterShardTestWorkchain{
		workchain: 0,
		tree: masterShardTestFork(
			masterShardTestLeaf(masterShardTestDescriptor(t, oldLeft, masterShardTestDescriptorOptions{})),
			masterShardTestLeaf(masterShardTestDescriptor(t, oldRight, masterShardTestDescriptorOptions{legacy: true})),
		),
	})

	newLeft := masterShardTestBlock(0, left, 22, 0x22)
	newRight := masterShardTestBlock(0, right, 23, 0x23)
	leftTop := masterShardTestTop(t, newLeft, []ton.BlockIDExt{oldLeft}, masterShardTestDescriptorOptions{
		fees: 10, created: 3,
	})
	rightTop := masterShardTestTop(t, newRight, []ton.BlockIDExt{oldRight}, masterShardTestDescriptorOptions{
		legacy: true, fees: 7, created: 2,
	})

	orders := [][]ShardTop{
		{leftTop, rightTop},
		{rightTop, leftTop},
	}
	var wantHashes cell.Hash
	var wantFees cell.Hash
	for i, order := range orders {
		registry, err := ParseShardRegistry(current)
		if err != nil {
			t.Fatal(err)
		}
		if err = registry.Apply(order, math.MaxUint64); err != nil {
			t.Fatalf("apply permutation %d: %v", i, err)
		}
		hashes, fees, err := masterShardTestBuild(t, registry)
		if err != nil {
			t.Fatal(err)
		}
		hashesRoot := masterShardTestDictionaryRoot(t, hashes).HashKey()
		feesCell, err := fees.ToCell()
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			wantHashes = hashesRoot
			wantFees = feesCell.HashKey()
		} else if hashesRoot != wantHashes || feesCell.HashKey() != wantFees {
			t.Fatal("batch permutation changed ShardHashes or ShardFees")
		}
	}

	registry, err := ParseShardRegistry(current)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Apply([]ShardTop{rightTop, leftTop}, math.MaxUint64); err != nil {
		t.Fatal(err)
	}
	_, fees, err := masterShardTestBuild(t, registry)
	if err != nil {
		t.Fatal(err)
	}
	leftFee := masterShardTestLoadFee(t, fees, newLeft)
	if leftFee.Fees.Coins.Nano().Int64() != 10 || leftFee.Create.Coins.Nano().Int64() != 3 {
		t.Fatalf("left fees = %s/%s, want 10/3",
			leftFee.Fees.Coins.Nano(), leftFee.Create.Coins.Nano())
	}
	total := masterShardTestLoadFeeRoot(t, fees)
	if total.Fees.Coins.Nano().Int64() != 17 || total.Create.Coins.Nano().Int64() != 5 {
		t.Fatalf("total fees = %s/%s, want 17/5",
			total.Fees.Coins.Nano(), total.Create.Coins.Nano())
	}
}

func TestShardRegistrySplitIsAtomic(t *testing.T) {
	parent := masterShardTestBlock(0, shard.Root, 30, 0x30)
	current := masterShardTestHashes(t, masterShardTestWorkchain{
		workchain: 0,
		tree: masterShardTestLeaf(masterShardTestDescriptor(t, parent, masterShardTestDescriptorOptions{
			beforeSplit: true,
		})),
	})
	left, right := masterShardTestChildren(t, parent.Shard)
	leftBlock := masterShardTestBlock(0, left, 31, 0x31)
	rightBlock := masterShardTestBlock(0, right, 31, 0x32)
	leftTop := masterShardTestTop(t, leftBlock, []ton.BlockIDExt{parent}, masterShardTestDescriptorOptions{fees: 4})
	rightTop := masterShardTestTop(t, rightBlock, []ton.BlockIDExt{parent}, masterShardTestDescriptorOptions{
		legacy: true, fees: 6,
	})

	registry, err := ParseShardRegistry(current)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Apply([]ShardTop{leftTop}, math.MaxUint64); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("half split error = %v, want ErrInvalidInput", err)
	}
	masterShardTestAssertRegistryRoot(t, registry, current)
	if len(registry.Tops()) != 1 || !sameShardBlock(registry.Tops()[0].Block, parent) {
		t.Fatal("failed half split mutated the registry")
	}

	if err = registry.Apply([]ShardTop{rightTop, leftTop}, math.MaxUint64); err != nil {
		t.Fatalf("apply complete split: %v", err)
	}
	tops := registry.Tops()
	if len(tops) != 2 || !sameShardBlock(tops[0].Block, leftBlock) || !sameShardBlock(tops[1].Block, rightBlock) {
		t.Fatalf("split tops = %+v", tops)
	}
	hashes, fees, err := masterShardTestBuild(t, registry)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ParseShardRegistry(hashes); err != nil {
		t.Fatalf("parse split registry: %v", err)
	}
	if masterShardTestLoadFeeRoot(t, fees).Fees.Coins.Nano().Int64() != 10 {
		t.Fatal("split shard fees were not aggregated")
	}
}

func TestShardRegistryMergeRequiresExactChildren(t *testing.T) {
	left, right := masterShardTestChildren(t, shard.Root)
	leftBlock := masterShardTestBlock(0, left, 40, 0x40)
	rightBlock := masterShardTestBlock(0, right, 41, 0x41)
	current := masterShardTestHashes(t, masterShardTestWorkchain{
		workchain: 0,
		tree: masterShardTestFork(
			masterShardTestLeaf(masterShardTestDescriptor(t, leftBlock, masterShardTestDescriptorOptions{
				beforeMerge: true,
				splitMerge:  tlb.FutureMerge{MergeUtime: 40, Interval: 10},
			})),
			masterShardTestLeaf(masterShardTestDescriptor(t, rightBlock, masterShardTestDescriptorOptions{
				legacy: true, beforeMerge: true,
				splitMerge: tlb.FutureMerge{MergeUtime: 40, Interval: 10},
			})),
		),
	})
	parent := masterShardTestBlock(0, shard.Root, 42, 0x42)
	merged := masterShardTestTop(t, parent, []ton.BlockIDExt{rightBlock, leftBlock}, masterShardTestDescriptorOptions{
		genUtime: 45, nextCatchainSeqno: 1, fees: 12, created: 9,
	})

	registry, err := ParseShardRegistry(current)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Apply([]ShardTop{merged}, math.MaxUint64); err != nil {
		t.Fatal(err)
	}
	tops := registry.Tops()
	if len(tops) != 1 || !sameShardBlock(tops[0].Block, parent) {
		t.Fatalf("merge tops = %+v", tops)
	}
	hashes, fees, err := masterShardTestBuild(t, registry)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ParseShardRegistry(hashes); err != nil {
		t.Fatalf("parse merged registry: %v", err)
	}
	mergedFee := masterShardTestLoadFee(t, fees, parent)
	if mergedFee.Fees.Coins.Nano().Int64() != 12 || mergedFee.Create.Coins.Nano().Int64() != 9 {
		t.Fatal("merged tip fees changed")
	}

	staleRight := *rightBlock.Copy()
	staleRight.SeqNo--
	stale := masterShardTestTop(t, parent, []ton.BlockIDExt{leftBlock, staleRight}, masterShardTestDescriptorOptions{})
	registry, err = ParseShardRegistry(current)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Apply([]ShardTop{stale}, math.MaxUint64); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("stale merge error = %v, want ErrInvalidInput", err)
	}
	masterShardTestAssertRegistryRoot(t, registry, current)
}

func TestShardRegistryRejectsStaleDuplicateAndConflictingTransitions(t *testing.T) {
	left, right := masterShardTestChildren(t, shard.Root)
	oldLeft := masterShardTestBlock(0, left, 50, 0x50)
	oldRight := masterShardTestBlock(0, right, 51, 0x51)
	current := masterShardTestHashes(t, masterShardTestWorkchain{
		workchain: 0,
		tree: masterShardTestFork(
			masterShardTestLeaf(masterShardTestDescriptor(t, oldLeft, masterShardTestDescriptorOptions{})),
			masterShardTestLeaf(masterShardTestDescriptor(t, oldRight, masterShardTestDescriptorOptions{})),
		),
	})
	newLeft := masterShardTestBlock(0, left, 52, 0x52)
	staleLeft := *oldLeft.Copy()
	staleLeft.RootHash[0] ^= 0xff
	staleTop := masterShardTestTop(t, newLeft, []ton.BlockIDExt{staleLeft}, masterShardTestDescriptorOptions{})

	registry, err := ParseShardRegistry(current)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Apply([]ShardTop{staleTop}, math.MaxUint64); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("stale linear error = %v, want ErrInvalidInput", err)
	}
	masterShardTestAssertRegistryRoot(t, registry, current)

	first := masterShardTestTop(t, newLeft, []ton.BlockIDExt{oldLeft}, masterShardTestDescriptorOptions{})
	secondBlock := masterShardTestBlock(0, left, 53, 0x53)
	second := masterShardTestTop(t, secondBlock, []ton.BlockIDExt{oldLeft}, masterShardTestDescriptorOptions{})
	if err = registry.Apply([]ShardTop{first, second}, math.MaxUint64); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("duplicate target error = %v, want ErrInvalidInput", err)
	}
	masterShardTestAssertRegistryRoot(t, registry, current)

	parent := masterShardTestBlock(1, shard.Root, 60, 0x60)
	parentCurrent := masterShardTestHashes(t, masterShardTestWorkchain{
		workchain: 1,
		tree: masterShardTestLeaf(masterShardTestDescriptor(t, parent, masterShardTestDescriptorOptions{
			beforeSplit: true,
		})),
	})
	childLeft, childRight := masterShardTestChildren(t, parent.Shard)
	leftSplit := masterShardTestTop(t, masterShardTestBlock(1, childLeft, 61, 0x61),
		[]ton.BlockIDExt{parent}, masterShardTestDescriptorOptions{})
	rightSplit := masterShardTestTop(t, masterShardTestBlock(1, childRight, 61, 0x62),
		[]ton.BlockIDExt{parent}, masterShardTestDescriptorOptions{})
	linear := masterShardTestTop(t, masterShardTestBlock(1, shard.Root, 61, 0x63),
		[]ton.BlockIDExt{parent}, masterShardTestDescriptorOptions{})
	registry, err = ParseShardRegistry(parentCurrent)
	if err != nil {
		t.Fatal(err)
	}
	err = registry.Apply([]ShardTop{leftSplit, linear, rightSplit}, math.MaxUint64)
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "conflicting") {
		t.Fatalf("conflicting transition error = %v", err)
	}
	masterShardTestAssertRegistryRoot(t, registry, parentCurrent)
}

func TestShardRegistryApplyEnforcesDescriptorTransitionInvariants(t *testing.T) {
	oldBlock := masterShardTestBlock(0, shard.Root, 70, 0x70)
	oldDescriptor := masterShardTestDescriptor(t, oldBlock, masterShardTestDescriptorOptions{
		nextCatchainSeqno: 5,
		splitMerge:        tlb.FutureSplit{SplitUtime: 100, Interval: 50},
	})
	current := masterShardTestHashes(t, masterShardTestWorkchain{
		workchain: 0,
		tree:      masterShardTestLeaf(oldDescriptor),
	})
	newBlock := masterShardTestBlock(0, shard.Root, 71, 0x71)
	wrongValidatorShard, err := shard.Child(shard.Root, true)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		options masterShardTestDescriptorOptions
		ltLimit uint64
	}{
		{
			name: "logical time reaches master limit",
			options: masterShardTestDescriptorOptions{
				nextCatchainSeqno: 5,
			},
			ltLimit: uint64(newBlock.SeqNo)*1_000 + 999,
		},
		{
			name: "catchain seqno skips predecessor value",
			options: masterShardTestDescriptorOptions{
				nextCatchainSeqno: 6,
			},
			ltLimit: math.MaxUint64,
		},
		{
			name: "next validator shard differs from actual shard",
			options: masterShardTestDescriptorOptions{
				nextCatchainSeqno:  5,
				nextValidatorShard: wrongValidatorShard,
			},
			ltLimit: math.MaxUint64,
		},
		{
			name: "before split generated at interval end",
			options: masterShardTestDescriptorOptions{
				beforeSplit:       true,
				genUtime:          150,
				nextCatchainSeqno: 5,
			},
			ltLimit: math.MaxUint64,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registry, err := ParseShardRegistry(current)
			if err != nil {
				t.Fatal(err)
			}
			top := masterShardTestTop(t, newBlock, []ton.BlockIDExt{oldBlock}, test.options)
			if err = registry.Apply([]ShardTop{top}, test.ltLimit); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("apply error = %v, want ErrInvalidInput", err)
			}
			masterShardTestAssertRegistryRoot(t, registry, current)
		})
	}
}

func TestShardRegistryApplyInheritsLinearFSM(t *testing.T) {
	oldBlock := masterShardTestBlock(0, shard.Root, 80, 0x80)
	wantFSM := tlb.FutureSplit{SplitUtime: 100, Interval: 50}
	current := masterShardTestHashes(t, masterShardTestWorkchain{
		workchain: 0,
		tree: masterShardTestLeaf(masterShardTestDescriptor(t, oldBlock, masterShardTestDescriptorOptions{
			nextCatchainSeqno: 9,
			splitMerge:        wantFSM,
		})),
	})
	newBlock := masterShardTestBlock(0, shard.Root, 81, 0x81)
	top := masterShardTestTop(t, newBlock, []ton.BlockIDExt{oldBlock}, masterShardTestDescriptorOptions{
		beforeSplit:       true,
		genUtime:          149,
		nextCatchainSeqno: 9,
	})

	registry, err := ParseShardRegistry(current)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Apply([]ShardTop{top}, math.MaxUint64); err != nil {
		t.Fatal(err)
	}
	fields, err := parseShardDescriptorFields(registry.Tops()[0].Descriptor)
	if err != nil {
		t.Fatal(err)
	}
	masterShardFSMTestAssertState(t, fields.splitMerge, wantFSM)
}

func masterShardTestTop(
	t *testing.T,
	block ton.BlockIDExt,
	predecessors []ton.BlockIDExt,
	options masterShardTestDescriptorOptions,
) ShardTop {
	t.Helper()
	return ShardTop{
		Block:        block,
		Predecessors: predecessors,
		Descriptor:   masterShardTestDescriptor(t, block, options),
	}
}

func masterShardTestDescriptor(
	t *testing.T,
	block ton.BlockIDExt,
	options masterShardTestDescriptorOptions,
) *cell.Cell {
	t.Helper()
	fees := tlb.CurrencyCollection{Coins: tlb.FromNanoTON(big.NewInt(options.fees))}
	created := tlb.CurrencyCollection{Coins: tlb.FromNanoTON(big.NewInt(options.created))}
	splitMerge := options.splitMerge
	if splitMerge == nil {
		splitMerge = tlb.FutureSplitMergeNone{}
	}
	nextValidatorShard := options.nextValidatorShard
	if nextValidatorShard == 0 {
		nextValidatorShard = block.Shard
	}

	var value any
	if options.legacy {
		value = tlb.ShardDescB{
			SeqNo:              block.SeqNo,
			RegMcSeqno:         options.regMCSeqno,
			StartLT:            uint64(block.SeqNo) * 1_000,
			EndLT:              uint64(block.SeqNo)*1_000 + 999,
			RootHash:           append([]byte(nil), block.RootHash...),
			FileHash:           append([]byte(nil), block.FileHash...),
			BeforeSplit:        options.beforeSplit,
			BeforeMerge:        options.beforeMerge,
			WantSplit:          options.wantSplit,
			WantMerge:          options.wantMerge,
			NextCatchainSeqNo:  options.nextCatchainSeqno,
			NextValidatorShard: nextValidatorShard,
			MinRefMcSeqNo:      options.minRefMCSeqno,
			GenUTime:           options.genUtime,
			SplitMergeAt:       splitMerge,
			FeesCollected:      fees,
			FundsCreated:       created,
		}
	} else {
		description := tlb.ShardDesc{
			SeqNo:              block.SeqNo,
			RegMcSeqno:         options.regMCSeqno,
			StartLT:            uint64(block.SeqNo) * 1_000,
			EndLT:              uint64(block.SeqNo)*1_000 + 999,
			RootHash:           append([]byte(nil), block.RootHash...),
			FileHash:           append([]byte(nil), block.FileHash...),
			BeforeSplit:        options.beforeSplit,
			BeforeMerge:        options.beforeMerge,
			WantSplit:          options.wantSplit,
			WantMerge:          options.wantMerge,
			NextCatchainSeqNo:  options.nextCatchainSeqno,
			NextValidatorShard: nextValidatorShard,
			MinRefMcSeqNo:      options.minRefMCSeqno,
			GenUTime:           options.genUtime,
			SplitMergeAt:       splitMerge,
		}
		description.Currencies.FeesCollected = fees
		description.Currencies.FundsCreated = created
		value = description
	}
	descriptor, err := tlb.ToCell(value)
	if err != nil {
		t.Fatalf("serialize shard descriptor: %v", err)
	}
	return descriptor
}

func masterShardTestBlock(workchain int32, shardID int64, seqno uint32, fill byte) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: workchain,
		Shard:     shardID,
		SeqNo:     seqno,
		RootHash:  bytes.Repeat([]byte{fill}, 32),
		FileHash:  bytes.Repeat([]byte{fill ^ 0xff}, 32),
	}
}

func masterShardTestChildren(t *testing.T, parent int64) (int64, int64) {
	t.Helper()
	left, err := shard.Child(parent, true)
	if err != nil {
		t.Fatal(err)
	}
	right, err := shard.Child(parent, false)
	if err != nil {
		t.Fatal(err)
	}
	return left, right
}

func masterShardTestHashes(t *testing.T, workchains ...masterShardTestWorkchain) *cell.Dictionary {
	t.Helper()
	result := cell.NewDict(32)
	for _, workchain := range workchains {
		key := cell.BeginCell().MustStoreInt(int64(workchain.workchain), 32).EndCell()
		value := cell.BeginCell().MustStoreRef(workchain.tree).EndCell()
		if err := result.Set(key, value); err != nil {
			t.Fatal(err)
		}
	}
	return result
}

func masterShardTestLeaf(descriptor *cell.Cell) *cell.Cell {
	return cell.BeginCell().
		MustStoreBoolBit(false).
		MustStoreBuilder(descriptor.ToBuilder()).
		EndCell()
}

func masterShardTestFork(left, right *cell.Cell) *cell.Cell {
	return cell.BeginCell().
		MustStoreBoolBit(true).
		MustStoreRef(left).
		MustStoreRef(right).
		EndCell()
}

func masterShardTestDictionaryRoot(t *testing.T, dictionary *cell.Dictionary) *cell.Cell {
	t.Helper()
	root, err := dictionary.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	if root == nil {
		t.Fatal("dictionary root is nil")
	}
	return root
}

func masterShardTestAssertRegistryRoot(t *testing.T, registry *ShardRegistry, want *cell.Dictionary) {
	t.Helper()
	got, fees, err := masterShardTestBuild(t, registry)
	if err != nil {
		t.Fatal(err)
	}
	if !sameDictionaryRoot(got, want) {
		t.Fatal("failed Apply mutated ShardHashes")
	}
	if !fees.IsEmpty() {
		t.Fatal("failed Apply produced ShardFees")
	}
}

func masterShardTestLoadFee(t *testing.T, fees *tlb.ShardFeesAugDict, block ton.BlockIDExt) tlb.ShardFeeCreated {
	t.Helper()
	key := cell.BeginCell().
		MustStoreInt(int64(block.Workchain), 32).
		MustStoreUInt(uint64(block.Shard), 64).
		EndCell()
	value, err := fees.LoadValue(key)
	if err != nil {
		t.Fatal(err)
	}
	var decoded tlb.ShardFeeCreated
	if err = tlb.LoadFromCell(&decoded, value); err != nil {
		t.Fatal(err)
	}
	if value.BitsLeft() != 0 || value.RefsNum() != 0 {
		t.Fatal("ShardFeeCreated has trailing data")
	}
	return decoded
}

func masterShardTestLoadFeeRoot(t *testing.T, fees *tlb.ShardFeesAugDict) tlb.ShardFeeCreated {
	t.Helper()
	root := fees.GetRootExtra()
	if root == nil {
		t.Fatal("ShardFees root extra is nil")
	}
	loader := root.MustBeginParse()
	var decoded tlb.ShardFeeCreated
	if err := tlb.LoadFromCell(&decoded, loader); err != nil {
		t.Fatal(err)
	}
	if loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		t.Fatal("ShardFees root extra has trailing data")
	}
	return decoded
}

// masterShardTestBuild builds both dictionaries the way one masterchain
// collation does: ShardHashes from the committed leaves, ShardFees from the
// frozen accepted set.
func masterShardTestBuild(t *testing.T, registry *ShardRegistry) (*cell.Dictionary, *tlb.ShardFeesAugDict, error) {
	t.Helper()

	shardHashes, err := registry.Build()
	if err != nil {
		return nil, nil, err
	}
	shardFees, err := buildShardFeesDictionary(registry.accepted)
	if err != nil {
		return nil, nil, err
	}
	return shardHashes, shardFees, nil
}
