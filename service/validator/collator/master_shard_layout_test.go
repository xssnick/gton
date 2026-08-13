package collator

import (
	"bytes"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"

	"github.com/xssnick/gton/service/shard"
)

func TestValidateMasterShardLayoutAcceptsInheritedFSMAtExpiryBoundary(t *testing.T) {
	registry := masterShardFSMTestRegistry(t, masterShardFSMTestEntry{
		workchain: 0,
		shard:     shard.Root,
		options: masterShardFSMTestDescriptorOptions{
			wantSplit:  true,
			splitMerge: tlb.FutureSplit{SplitUtime: 10, Interval: 10},
		},
	})
	config := masterShardFSMTestConfig(t, map[int32]masterShardFSMTestWorkchain{
		0: {active: true, maxSplit: 2},
	})

	err := validateMasterShardLayout(masterShardLayoutValidationInput{
		oldRegistry: registry,
		newRegistry: masterShardLayoutTestClone(t, registry, nil),
		workchains:  masterShardTestWorkchainMap(t, config),
		now:         20,
		startLT:     1_000,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestValidateMasterShardLayoutRejectsInvalidFSMAndCatchainChanges(t *testing.T) {
	left, err := shard.Child(shard.Root, true)
	if err != nil {
		t.Fatal(err)
	}
	config := masterShardFSMTestConfig(t, map[int32]masterShardFSMTestWorkchain{
		0: {active: true, maxSplit: 2},
	})

	tests := []struct {
		name             string
		oldFSM           any
		newFSM           any
		newCatchainSeqno uint32
		nextValidator    int64
		updateCatchain   bool
	}{
		{
			name:             "live FSM is removed",
			oldFSM:           tlb.FutureSplit{SplitUtime: 100, Interval: 50},
			newFSM:           tlb.FutureSplitMergeNone{},
			newCatchainSeqno: 7,
		},
		{
			name:             "new interval is shorter than V1 minimum",
			oldFSM:           tlb.FutureSplitMergeNone{},
			newFSM:           tlb.FutureSplit{SplitUtime: 111, Interval: 29},
			newCatchainSeqno: 7,
		},
		{
			name:             "new interval ends after V1 maximum delay",
			oldFSM:           tlb.FutureSplitMergeNone{},
			newFSM:           tlb.FutureSplit{SplitUtime: 111, Interval: 1_000},
			newCatchainSeqno: 7,
		},
		{
			name:             "catchain increments without a boundary",
			oldFSM:           tlb.FutureSplitMergeNone{},
			newFSM:           tlb.FutureSplitMergeNone{},
			newCatchainSeqno: 8,
		},
		{
			name:             "mandatory catchain increment is omitted",
			oldFSM:           tlb.FutureSplitMergeNone{},
			newFSM:           tlb.FutureSplitMergeNone{},
			newCatchainSeqno: 7,
			updateCatchain:   true,
		},
		{
			name:             "next validator shard differs from actual shard",
			oldFSM:           tlb.FutureSplitMergeNone{},
			newFSM:           tlb.FutureSplitMergeNone{},
			newCatchainSeqno: 7,
			nextValidator:    left,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registry := masterShardFSMTestRegistry(t, masterShardFSMTestEntry{
				workchain: 0,
				shard:     shard.Root,
				options: masterShardFSMTestDescriptorOptions{
					wantSplit:         true,
					nextCatchainSeqno: 7,
					splitMerge:        test.oldFSM,
				},
			})
			candidate := masterShardLayoutTestClone(t, registry, func(_ shardRegistryKey, fields *shardDescriptorFields) {
				fields.splitMerge = test.newFSM
				fields.nextCatchainSeqno = test.newCatchainSeqno
				if test.nextValidator != 0 {
					fields.nextValidatorShard = test.nextValidator
				}
			})

			err := validateMasterShardLayout(masterShardLayoutValidationInput{
				oldRegistry:    registry,
				newRegistry:    candidate,
				workchains:     masterShardTestWorkchainMap(t, config),
				now:            110,
				startLT:        1_000,
				updateCatchain: test.updateCatchain,
			})
			if !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("validation error = %v, want ErrInvalidInput", err)
			}
		})
	}
}

func TestValidateMasterShardLayoutChecksWorkchainMembership(t *testing.T) {
	empty := &ShardRegistry{
		leaves:   make(map[shardRegistryKey]shardRegistryLeaf),
		accepted: make(map[shardRegistryKey]shardRegistryLeaf),
	}
	activeConfig := masterShardFSMTestConfig(t, map[int32]masterShardFSMTestWorkchain{
		0: {active: true, maxSplit: 2},
	})
	if err := validateMasterShardLayout(masterShardLayoutValidationInput{
		oldRegistry: empty,
		newRegistry: empty,
		workchains:  masterShardTestWorkchainMap(t, activeConfig),
		now:         100,
		startLT:     1_000,
	}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("missing active workchain error = %v, want ErrInvalidInput", err)
	}

	unknown := masterShardFSMTestRegistry(t, masterShardFSMTestEntry{
		workchain: 1,
		shard:     shard.Root,
	})
	if err := validateMasterShardLayout(masterShardLayoutValidationInput{
		oldRegistry: unknown,
		newRegistry: masterShardLayoutTestClone(t, unknown, nil),
		workchains:  masterShardTestWorkchainMap(t, activeConfig),
		now:         100,
		startLT:     1_000,
	}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("unknown workchain error = %v, want ErrInvalidInput", err)
	}
}

func TestValidateMasterShardLayoutAcceptsWorkchainActivation(t *testing.T) {
	const (
		rootFill = byte(0x51)
		fileFill = byte(0xa4)
	)
	empty := &ShardRegistry{
		leaves:   make(map[shardRegistryKey]shardRegistryLeaf),
		accepted: make(map[shardRegistryKey]shardRegistryLeaf),
	}
	config := masterShardFSMTestConfig(t, map[int32]masterShardFSMTestWorkchain{
		0: {
			active:   true,
			maxSplit: 2,
			rootFill: rootFill,
			fileFill: fileFill,
		},
	})
	candidate := masterShardLayoutTestActivationRegistry(t, rootFill, fileFill)
	input := masterShardLayoutValidationInput{
		oldRegistry:   empty,
		newRegistry:   candidate,
		workchains:    masterShardTestWorkchainMap(t, config),
		now:           100,
		startLT:       1_000,
		newBlockSeqno: 5,
	}
	if err := validateMasterShardLayout(input); err != nil {
		t.Fatal(err)
	}

	input.newRegistry = masterShardLayoutTestClone(t, candidate, func(_ shardRegistryKey, fields *shardDescriptorFields) {
		fields.seqno = 1
	})
	if err := validateMasterShardLayout(input); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("non-zero activation seqno error = %v, want ErrInvalidInput", err)
	}
}

func TestValidateMasterShardLayoutAcceptsVerifiedLinearUpdate(t *testing.T) {
	oldBlock := masterShardTestBlock(0, shard.Root, 90, 0x90)
	oldHashes := masterShardTestHashes(t, masterShardTestWorkchain{
		workchain: 0,
		tree:      masterShardTestLeaf(masterShardTestDescriptor(t, oldBlock, masterShardTestDescriptorOptions{})),
	})
	oldRegistry, err := ParseShardRegistry(oldHashes)
	if err != nil {
		t.Fatal(err)
	}
	newBlock := masterShardTestBlock(0, shard.Root, 91, 0x91)
	top := masterShardTestTop(t, newBlock, []ton.BlockIDExt{oldBlock}, masterShardTestDescriptorOptions{
		genUtime:   100,
		regMCSeqno: 5,
	})
	candidate := &ShardRegistry{
		leaves:   cloneShardRegistryLeaves(oldRegistry.leaves),
		accepted: make(map[shardRegistryKey]shardRegistryLeaf),
	}
	if err = candidate.Apply([]ShardTop{top}, 100_000); err != nil {
		t.Fatal(err)
	}
	config := masterShardFSMTestConfig(t, map[int32]masterShardFSMTestWorkchain{
		0: {active: true, maxSplit: 2},
	})

	err = validateMasterShardLayout(masterShardLayoutValidationInput{
		oldRegistry:   oldRegistry,
		newRegistry:   candidate,
		workchains:    masterShardTestWorkchainMap(t, config),
		tops:          []ShardTop{top},
		now:           100,
		startLT:       100_000,
		newBlockSeqno: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestValidateMasterShardLayoutAcceptsVerifiedSplit(t *testing.T) {
	left, right := masterShardTestChildren(t, shard.Root)
	parent := masterShardTestBlock(0, shard.Root, 90, 0x90)
	oldHashes := masterShardTestHashes(t, masterShardTestWorkchain{
		workchain: 0,
		tree: masterShardTestLeaf(masterShardTestDescriptor(t, parent, masterShardTestDescriptorOptions{
			beforeSplit:       true,
			nextCatchainSeqno: 3,
			splitMerge:        tlb.FutureSplit{SplitUtime: 90, Interval: 20},
		})),
	})
	oldRegistry, err := ParseShardRegistry(oldHashes)
	if err != nil {
		t.Fatal(err)
	}
	leftTop := masterShardTestTop(t, masterShardTestBlock(0, left, 91, 0x91), []ton.BlockIDExt{parent},
		masterShardTestDescriptorOptions{genUtime: 100, regMCSeqno: 5, nextCatchainSeqno: 3})
	rightTop := masterShardTestTop(t, masterShardTestBlock(0, right, 91, 0x92), []ton.BlockIDExt{parent},
		masterShardTestDescriptorOptions{genUtime: 100, regMCSeqno: 5, nextCatchainSeqno: 3})
	tops := []ShardTop{leftTop, rightTop}
	candidate := &ShardRegistry{
		leaves:   cloneShardRegistryLeaves(oldRegistry.leaves),
		accepted: make(map[shardRegistryKey]shardRegistryLeaf),
	}
	if err = candidate.Apply(tops, 100_000); err != nil {
		t.Fatal(err)
	}
	config := masterShardFSMTestConfig(t, map[int32]masterShardFSMTestWorkchain{
		0: {active: true, maxSplit: 2},
	})

	err = validateMasterShardLayout(masterShardLayoutValidationInput{
		oldRegistry:   oldRegistry,
		newRegistry:   candidate,
		workchains:    masterShardTestWorkchainMap(t, config),
		tops:          tops,
		now:           100,
		startLT:       100_000,
		newBlockSeqno: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestValidateMasterShardLayoutAcceptsVerifiedMergeAndCatchainBoundary(t *testing.T) {
	left, right := masterShardTestChildren(t, shard.Root)
	leftBlock := masterShardTestBlock(0, left, 90, 0x90)
	rightBlock := masterShardTestBlock(0, right, 90, 0x91)
	oldHashes := masterShardTestHashes(t, masterShardTestWorkchain{
		workchain: 0,
		tree: masterShardTestFork(
			masterShardTestLeaf(masterShardTestDescriptor(t, leftBlock, masterShardTestDescriptorOptions{
				beforeMerge:       true,
				nextCatchainSeqno: 4,
				splitMerge:        tlb.FutureMerge{MergeUtime: 90, Interval: 20},
			})),
			masterShardTestLeaf(masterShardTestDescriptor(t, rightBlock, masterShardTestDescriptorOptions{
				beforeMerge:       true,
				nextCatchainSeqno: 6,
				splitMerge:        tlb.FutureMerge{MergeUtime: 90, Interval: 20},
			})),
		),
	})
	oldRegistry, err := ParseShardRegistry(oldHashes)
	if err != nil {
		t.Fatal(err)
	}
	mergedTop := masterShardTestTop(t, masterShardTestBlock(0, shard.Root, 91, 0x92),
		[]ton.BlockIDExt{leftBlock, rightBlock}, masterShardTestDescriptorOptions{
			genUtime:          100,
			regMCSeqno:        5,
			nextCatchainSeqno: 7,
		})
	candidate := &ShardRegistry{
		leaves:   cloneShardRegistryLeaves(oldRegistry.leaves),
		accepted: make(map[shardRegistryKey]shardRegistryLeaf),
	}
	if err = candidate.Apply([]ShardTop{mergedTop}, 100_000); err != nil {
		t.Fatal(err)
	}
	candidate = masterShardLayoutTestClone(t, candidate, func(_ shardRegistryKey, fields *shardDescriptorFields) {
		fields.nextCatchainSeqno++
	})
	config := masterShardFSMTestConfig(t, map[int32]masterShardFSMTestWorkchain{
		0: {active: true, maxSplit: 2},
	})

	err = validateMasterShardLayout(masterShardLayoutValidationInput{
		oldRegistry:    oldRegistry,
		newRegistry:    candidate,
		workchains:     masterShardTestWorkchainMap(t, config),
		tops:           []ShardTop{mergedTop},
		now:            100,
		startLT:        100_000,
		newBlockSeqno:  5,
		updateCatchain: true,
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestValidateMasterShardLayoutFSMRequiresMatureBeforeMerge pins the three
// before_merge conditions C++ checks between "the sibling agrees" and "the merge
// conditions hold" (validate-query.cpp:2196-2210): the announced merge time must
// have arrived for this shard, the sibling must carry the announcement, and its
// time must have arrived too.
func TestValidateMasterShardLayoutFSMRequiresMatureBeforeMerge(t *testing.T) {
	left, _ := masterShardTestChildren(t, shard.Root)
	workchain := &masterShardWorkchainInfo{
		active:                true,
		maxSplit:              2,
		minSplitMergeInterval: 10,
		maxSplitMergeDelay:    1_000,
	}
	mature := tlb.FutureMerge{MergeUtime: 90, Interval: 20}
	premature := tlb.FutureMerge{MergeUtime: 150, Interval: 20}
	merging := func(splitMerge any) shardDescriptorFields {
		return shardDescriptorFields{
			beforeMerge:       true,
			wantMerge:         true,
			nextCatchainSeqno: 4,
			splitMerge:        splitMerge,
		}
	}

	tests := []struct {
		name      string
		own       any
		sibling   any
		wantError string
	}{
		{
			name:    "both announcements have arrived",
			own:     mature,
			sibling: mature,
		},
		{
			name:      "own announcement is still in the future",
			own:       premature,
			sibling:   mature,
			wantError: "before merge announcement has not matured",
		},
		{
			name:      "sibling never announced the merge",
			own:       mature,
			sibling:   tlb.FutureSplitMergeNone{},
			wantError: "no merge announcement in the sibling",
		},
		{
			name:      "sibling announcement is still in the future",
			own:       mature,
			sibling:   premature,
			wantError: "in the sibling has not matured",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fields := merging(test.own)
			// The announcement was raised in an earlier block and is inherited
			// unchanged here, which is what keeps the interval check above out of
			// the way -- exactly the state a shard is in when before_merge is set.
			previous := fields
			previous.beforeMerge = false
			sibling := merging(test.sibling)

			err := validateMasterShardLayoutFSM(masterShardLayoutFSMInput{
				key:               shardRegistryKey{workchain: 0, shard: left},
				fields:            fields,
				previous:          &previous,
				sibling:           &sibling,
				workchain:         workchain,
				baseCatchainSeqno: fields.nextCatchainSeqno,
				now:               100,
			})
			if test.wantError == "" {
				if err != nil {
					t.Fatalf("matured before merge rejected: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("before merge error = %v, want one containing %q", err, test.wantError)
			}
		})
	}
}

// TestValidateMasterShardLayoutRejectsPrematureBeforeMerge drives the same rule
// through the whole layout transition, where the announcement is inherited from
// the predecessor descriptor and therefore never reaches the interval check.
func TestValidateMasterShardLayoutRejectsPrematureBeforeMerge(t *testing.T) {
	left, right := masterShardTestChildren(t, shard.Root)
	announcement := tlb.FutureMerge{MergeUtime: 150, Interval: 20}
	oldHashes := masterShardTestHashes(t, masterShardTestWorkchain{
		workchain: 0,
		tree: masterShardTestFork(
			masterShardTestLeaf(masterShardTestDescriptor(t, masterShardTestBlock(0, left, 90, 0x90),
				masterShardTestDescriptorOptions{
					wantMerge:         true,
					nextCatchainSeqno: 4,
					splitMerge:        announcement,
				})),
			masterShardTestLeaf(masterShardTestDescriptor(t, masterShardTestBlock(0, right, 90, 0x91),
				masterShardTestDescriptorOptions{
					wantMerge:         true,
					nextCatchainSeqno: 4,
					splitMerge:        announcement,
				})),
		),
	})
	oldRegistry, err := ParseShardRegistry(oldHashes)
	if err != nil {
		t.Fatal(err)
	}
	config := masterShardFSMTestConfig(t, map[int32]masterShardFSMTestWorkchain{
		0: {active: true, maxSplit: 2},
	})
	input := masterShardLayoutValidationInput{
		oldRegistry:   oldRegistry,
		newRegistry:   masterShardLayoutTestClone(t, oldRegistry, nil),
		workchains:    masterShardTestWorkchainMap(t, config),
		now:           100,
		startLT:       100_000,
		newBlockSeqno: 5,
	}
	if err = validateMasterShardLayout(input); err != nil {
		t.Fatalf("inherited merge announcement rejected before it was raised: %v", err)
	}

	input.newRegistry = masterShardLayoutTestClone(t, oldRegistry, func(_ shardRegistryKey, fields *shardDescriptorFields) {
		fields.beforeMerge = true
	})
	err = validateMasterShardLayout(input)
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "has not matured") {
		t.Fatalf("premature before merge error = %v, want ErrInvalidInput containing %q", err, "has not matured")
	}
}

func masterShardLayoutTestClone(
	t *testing.T,
	registry *ShardRegistry,
	mutate func(shardRegistryKey, *shardDescriptorFields),
) *ShardRegistry {
	t.Helper()
	leaves := make(map[shardRegistryKey]shardRegistryLeaf, len(registry.leaves))
	for key, leaf := range registry.leaves {
		fields, err := parseShardDescriptorFields(leaf.top.Descriptor)
		if err != nil {
			t.Fatal(err)
		}
		if mutate != nil {
			mutate(key, &fields)
		}
		tag, err := loadMasterShardDescriptorTag(leaf.top.Descriptor)
		if err != nil {
			t.Fatal(err)
		}
		descriptor, err := storeMasterShardDescriptor(tag, fields)
		if err != nil {
			t.Fatal(err)
		}
		updated, err := decodeShardRegistryLeaf(key.workchain, key.shard, descriptor)
		if err != nil {
			t.Fatal(err)
		}
		leaves[key] = updated
	}
	return &ShardRegistry{
		leaves:   leaves,
		accepted: make(map[shardRegistryKey]shardRegistryLeaf),
	}
}

func masterShardLayoutTestActivationRegistry(t *testing.T, rootFill, fileFill byte) *ShardRegistry {
	t.Helper()
	zero := tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(0)}
	fields := shardDescriptorFields{
		regMCSeqno:         5,
		endLT:              10,
		genUtime:           90,
		rootHash:           bytes.Repeat([]byte{rootFill}, 32),
		fileHash:           bytes.Repeat([]byte{fileFill}, 32),
		nextValidatorShard: shard.Root,
		minRefMCSeqno:      math.MaxUint32,
		splitMerge:         tlb.FutureSplitMergeNone{},
		fees:               zero,
		created:            zero,
	}
	descriptor, err := storeMasterShardDescriptor(0xb, fields)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := decodeShardRegistryLeaf(0, shard.Root, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	return &ShardRegistry{
		leaves: map[shardRegistryKey]shardRegistryLeaf{
			{workchain: 0, shard: shard.Root}: leaf,
		},
		accepted: make(map[shardRegistryKey]shardRegistryLeaf),
	}
}
