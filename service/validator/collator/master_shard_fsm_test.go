package collator

import (
	"bytes"
	"errors"
	"math"
	"math/big"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/shard"
)

type masterShardFSMTestDescriptorOptions struct {
	tag               uint64
	wantSplit         bool
	wantMerge         bool
	beforeSplit       bool
	beforeMerge       bool
	nextCCUpdated     bool
	nextCatchainSeqno uint32
	splitMerge        any
	fill              byte
}

type masterShardFSMTestEntry struct {
	workchain int32
	shard     int64
	options   masterShardFSMTestDescriptorOptions
}

type masterShardFSMTestWorkchain struct {
	version                   uint8
	enabledSince              uint32
	monitorMinSplit           uint8
	minSplit                  uint8
	maxSplit                  uint8
	active                    bool
	rootFill                  byte
	fileFill                  byte
	splitMergeDelay           uint32
	splitMergeInterval        uint32
	minSplitMergeInterval     uint32
	maxSplitMergeDelay        uint32
	persistentStateSplitDepth uint8
}

func TestUpdateMasterShardFSMMatrix(t *testing.T) {
	workchain := &masterShardWorkchainInfo{
		minSplit:              0,
		maxSplit:              2,
		splitMergeDelay:       7,
		splitMergeInterval:    11,
		minSplitMergeInterval: 11,
		maxSplitMergeDelay:    1000,
	}
	siblingNoneWantMerge := shardDescriptorFields{
		wantMerge:  true,
		splitMerge: tlb.FutureSplitMergeNone{},
	}
	siblingBeforeSplit := shardDescriptorFields{
		beforeSplit: true,
		splitMerge:  tlb.FutureSplitMergeNone{},
	}
	siblingMatureMerge := shardDescriptorFields{
		wantMerge:  true,
		splitMerge: tlb.FutureMerge{MergeUtime: 10, Interval: 100},
	}

	tests := []struct {
		name              string
		fields            shardDescriptorFields
		sibling           *shardDescriptorFields
		workchain         *masterShardWorkchainInfo
		depth             uint32
		now               uint32
		updateCatchain    bool
		wantState         any
		wantBeforeMerge   bool
		wantCatchainSeqno uint32
	}{
		{
			name: "expired FSM is cleared at interval end",
			fields: shardDescriptorFields{
				splitMerge:        tlb.FutureSplit{SplitUtime: 10, Interval: 5},
				nextCatchainSeqno: 3,
			},
			now:               15,
			wantState:         tlb.FutureSplitMergeNone{},
			wantCatchainSeqno: 3,
		},
		{
			name: "before split clears a live FSM",
			fields: shardDescriptorFields{
				beforeSplit:       true,
				splitMerge:        tlb.FutureSplit{SplitUtime: 100, Interval: 20},
				nextCatchainSeqno: 4,
			},
			now:               50,
			wantState:         tlb.FutureSplitMergeNone{},
			wantCatchainSeqno: 4,
		},
		{
			name: "merge FSM without direct sibling is cleared",
			fields: shardDescriptorFields{
				splitMerge:        tlb.FutureMerge{MergeUtime: 100, Interval: 20},
				nextCatchainSeqno: 5,
			},
			now:               50,
			wantState:         tlb.FutureSplitMergeNone{},
			wantCatchainSeqno: 5,
		},
		{
			name: "merge FSM with splitting sibling is cleared",
			fields: shardDescriptorFields{
				splitMerge:        tlb.FutureMerge{MergeUtime: 100, Interval: 20},
				nextCatchainSeqno: 6,
			},
			sibling:           &siblingBeforeSplit,
			now:               50,
			wantState:         tlb.FutureSplitMergeNone{},
			wantCatchainSeqno: 6,
		},
		{
			name: "split is scheduled before merge",
			fields: shardDescriptorFields{
				wantSplit:         true,
				wantMerge:         true,
				splitMerge:        tlb.FutureSplitMergeNone{},
				nextCatchainSeqno: 7,
			},
			sibling:           &siblingNoneWantMerge,
			workchain:         workchain,
			depth:             1,
			now:               20,
			wantState:         tlb.FutureSplit{SplitUtime: 27, Interval: 11},
			wantCatchainSeqno: 7,
		},
		{
			name: "merge is scheduled with an idle willing sibling",
			fields: shardDescriptorFields{
				wantMerge:         true,
				splitMerge:        tlb.FutureSplitMergeNone{},
				nextCatchainSeqno: 8,
			},
			sibling:           &siblingNoneWantMerge,
			workchain:         workchain,
			depth:             1,
			now:               20,
			wantState:         tlb.FutureMerge{MergeUtime: 27, Interval: 11},
			wantCatchainSeqno: 8,
		},
		{
			name: "mature merge pair enters before merge",
			fields: shardDescriptorFields{
				wantMerge:         true,
				splitMerge:        tlb.FutureMerge{MergeUtime: 10, Interval: 100},
				nextCatchainSeqno: 9,
			},
			sibling:           &siblingMatureMerge,
			workchain:         workchain,
			depth:             1,
			now:               20,
			wantState:         tlb.FutureMerge{MergeUtime: 10, Interval: 100},
			wantBeforeMerge:   true,
			wantCatchainSeqno: 9,
		},
		{
			name: "old before merge removal advances catchain",
			fields: shardDescriptorFields{
				beforeMerge:       true,
				splitMerge:        tlb.FutureSplitMergeNone{},
				nextCatchainSeqno: 10,
			},
			now:               20,
			wantState:         tlb.FutureSplitMergeNone{},
			wantCatchainSeqno: 11,
		},
		{
			name: "unchanged old before merge does not advance catchain",
			fields: shardDescriptorFields{
				beforeMerge:       true,
				wantMerge:         true,
				splitMerge:        tlb.FutureMerge{MergeUtime: 10, Interval: 100},
				nextCatchainSeqno: 11,
			},
			sibling:           &siblingMatureMerge,
			workchain:         workchain,
			depth:             1,
			now:               20,
			wantState:         tlb.FutureMerge{MergeUtime: 10, Interval: 100},
			wantBeforeMerge:   true,
			wantCatchainSeqno: 11,
		},
		{
			name: "global catchain update advances every shard",
			fields: shardDescriptorFields{
				splitMerge:        tlb.FutureSplitMergeNone{},
				nextCatchainSeqno: 12,
			},
			now:               20,
			updateCatchain:    true,
			wantState:         tlb.FutureSplitMergeNone{},
			wantCatchainSeqno: 13,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fields := test.fields
			changed, err := updateMasterShardFSM(
				&fields,
				test.sibling,
				test.workchain,
				test.depth,
				test.now,
				test.updateCatchain,
			)
			if err != nil {
				t.Fatal(err)
			}
			if !changed {
				t.Fatal("FSM update reported no change")
			}
			masterShardFSMTestAssertState(t, fields.splitMerge, test.wantState)
			if fields.beforeMerge != test.wantBeforeMerge {
				t.Fatalf("before merge = %t, want %t", fields.beforeMerge, test.wantBeforeMerge)
			}
			if fields.nextCatchainSeqno != test.wantCatchainSeqno {
				t.Fatalf("next catchain seqno = %d, want %d", fields.nextCatchainSeqno, test.wantCatchainSeqno)
			}
		})
	}
}

func TestPostprocessMasterShardRegistryUsesSnapshotSiblings(t *testing.T) {
	left, right := masterShardFSMTestChildren(t, shard.Root)
	registry := masterShardFSMTestRegistry(t,
		masterShardFSMTestEntry{
			workchain: 0,
			shard:     left,
			options: masterShardFSMTestDescriptorOptions{
				tag:               0xa,
				wantMerge:         true,
				nextCatchainSeqno: 3,
				fill:              0x11,
			},
		},
		masterShardFSMTestEntry{
			workchain: 0,
			shard:     right,
			options: masterShardFSMTestDescriptorOptions{
				tag:               0xb,
				wantMerge:         true,
				nextCCUpdated:     true,
				nextCatchainSeqno: 4,
				fill:              0x22,
			},
		},
	)
	config := masterShardFSMTestConfig(t, map[int32]masterShardFSMTestWorkchain{
		0: {
			version:               2,
			minSplit:              0,
			maxSplit:              2,
			active:                true,
			splitMergeDelay:       7,
			splitMergeInterval:    11,
			minSplitMergeInterval: 11,
			maxSplitMergeDelay:    1000,
		},
	})

	err := postprocessMasterShardRegistry(masterShardRegistryPostprocessInput{
		registry:   registry,
		workchains: masterShardTestWorkchainMap(t, config),
		now:        20,
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, shardID := range []int64{left, right} {
		fields := masterShardFSMTestFields(t, registry, 0, shardID)
		masterShardFSMTestAssertState(t, fields.splitMerge, tlb.FutureMerge{MergeUtime: 27, Interval: 11})
		if fields.nextCCUpdated {
			t.Fatalf("shard %016x retained nx_cc_updated after rewrite", uint64(shardID))
		}
		tag, err := loadMasterShardDescriptorTag(registry.leaves[shardRegistryKey{workchain: 0, shard: shardID}].top.Descriptor)
		if err != nil {
			t.Fatal(err)
		}
		if tag != 0xa {
			t.Fatalf("shard %016x descriptor tag = %x, want a", uint64(shardID), tag)
		}
	}
}

func TestPostprocessMasterShardRegistryPreservesUnchangedDescriptor(t *testing.T) {
	registry := masterShardFSMTestRegistry(t, masterShardFSMTestEntry{
		workchain: 0,
		shard:     shard.Root,
		options: masterShardFSMTestDescriptorOptions{
			tag:           0xb,
			nextCCUpdated: true,
			fill:          0x2f,
		},
	})
	key := shardRegistryKey{workchain: 0, shard: shard.Root}
	before := registry.leaves[key].top.Descriptor.HashKey()
	config := masterShardFSMTestConfig(t, nil)

	if err := postprocessMasterShardRegistry(masterShardRegistryPostprocessInput{
		registry:   registry,
		workchains: masterShardTestWorkchainMap(t, config),
		now:        20,
	}); err != nil {
		t.Fatal(err)
	}
	if got := registry.leaves[key].top.Descriptor.HashKey(); got != before {
		t.Fatal("unchanged descriptor was reserialized")
	}
}

func TestPostprocessMasterShardRegistryUsesNewV1AndV2Timings(t *testing.T) {
	// Only the resulting workchain set controls the FSM; the predecessor's is
	// not an input at all.
	tests := []struct {
		name         string
		newWorkchain masterShardFSMTestWorkchain
		wantStart    uint32
		wantInterval uint32
	}{
		{
			name:         "V1 defaults",
			newWorkchain: masterShardFSMTestWorkchain{version: 1, maxSplit: 2},
			wantStart:    120,
			wantInterval: 100,
		},
		{
			name: "V2 configured timings",
			newWorkchain: masterShardFSMTestWorkchain{
				version:               2,
				maxSplit:              2,
				splitMergeDelay:       9,
				splitMergeInterval:    13,
				minSplitMergeInterval: 13,
				maxSplitMergeDelay:    1000,
			},
			wantStart:    29,
			wantInterval: 13,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registry := masterShardFSMTestRegistry(t, masterShardFSMTestEntry{
				workchain: 0,
				shard:     shard.Root,
				options: masterShardFSMTestDescriptorOptions{
					wantSplit: true,
					fill:      0x31,
				},
			})
			newConfig := masterShardFSMTestConfig(t, map[int32]masterShardFSMTestWorkchain{0: test.newWorkchain})

			err := postprocessMasterShardRegistry(masterShardRegistryPostprocessInput{
				registry:   registry,
				workchains: masterShardTestWorkchainMap(t, newConfig),
				now:        20,
			})
			if err != nil {
				t.Fatal(err)
			}

			fields := masterShardFSMTestFields(t, registry, 0, shard.Root)
			masterShardFSMTestAssertState(t, fields.splitMerge, tlb.FutureSplit{
				SplitUtime: test.wantStart,
				Interval:   test.wantInterval,
			})
		})
	}
}

func TestLoadMasterShardWorkchainsPreservesValidatorTimingBounds(t *testing.T) {
	config := masterShardFSMTestConfig(t, map[int32]masterShardFSMTestWorkchain{
		0: {version: 1},
		1: {
			version:               2,
			minSplitMergeInterval: 17,
			maxSplitMergeDelay:    211,
		},
	})
	workchains, err := loadMasterShardWorkchains(config)
	if err != nil {
		t.Fatal(err)
	}
	if workchains[0].minSplitMergeInterval != masterShardDefaultMinSplitMergeInterval ||
		workchains[0].maxSplitMergeDelay != masterShardDefaultMaxSplitMergeDelay {
		t.Fatalf("V1 validator timing bounds = %d/%d, want %d/%d",
			workchains[0].minSplitMergeInterval,
			workchains[0].maxSplitMergeDelay,
			masterShardDefaultMinSplitMergeInterval,
			masterShardDefaultMaxSplitMergeDelay,
		)
	}
	if workchains[1].minSplitMergeInterval != 17 || workchains[1].maxSplitMergeDelay != 211 {
		t.Fatalf("V2 validator timing bounds = %d/%d, want 17/211",
			workchains[1].minSplitMergeInterval, workchains[1].maxSplitMergeDelay)
	}
}

func TestPostprocessMasterShardRegistryMaturesMergePair(t *testing.T) {
	left, right := masterShardFSMTestChildren(t, shard.Root)
	merge := tlb.FutureMerge{MergeUtime: 10, Interval: 100}
	registry := masterShardFSMTestRegistry(t,
		masterShardFSMTestEntry{
			workchain: 0,
			shard:     left,
			options: masterShardFSMTestDescriptorOptions{
				wantMerge:         true,
				nextCatchainSeqno: 7,
				splitMerge:        merge,
				fill:              0x41,
			},
		},
		masterShardFSMTestEntry{
			workchain: 0,
			shard:     right,
			options: masterShardFSMTestDescriptorOptions{
				wantMerge:         true,
				nextCatchainSeqno: 8,
				splitMerge:        merge,
				fill:              0x42,
			},
		},
	)
	config := masterShardFSMTestConfig(t, map[int32]masterShardFSMTestWorkchain{
		0: {version: 2, minSplit: 0, maxSplit: 2, splitMergeDelay: 7, splitMergeInterval: 11},
	})

	if err := postprocessMasterShardRegistry(masterShardRegistryPostprocessInput{
		registry:   registry,
		workchains: masterShardTestWorkchainMap(t, config),
		now:        20,
	}); err != nil {
		t.Fatal(err)
	}

	for _, shardID := range []int64{left, right} {
		fields := masterShardFSMTestFields(t, registry, 0, shardID)
		if !fields.beforeMerge {
			t.Fatalf("shard %016x did not enter before merge", uint64(shardID))
		}
	}
	if got := masterShardFSMTestFields(t, registry, 0, left).nextCatchainSeqno; got != 7 {
		t.Fatalf("left next catchain seqno = %d, want 7", got)
	}
	if got := masterShardFSMTestFields(t, registry, 0, right).nextCatchainSeqno; got != 8 {
		t.Fatalf("right next catchain seqno = %d, want 8", got)
	}
}

func TestPostprocessMasterShardRegistryClearsMergeWithoutLeafSibling(t *testing.T) {
	left, right := masterShardFSMTestChildren(t, shard.Root)
	rightLeft, rightRight := masterShardFSMTestChildren(t, right)
	registry := masterShardFSMTestRegistry(t,
		masterShardFSMTestEntry{
			workchain: 0,
			shard:     left,
			options: masterShardFSMTestDescriptorOptions{
				splitMerge: tlb.FutureMerge{MergeUtime: 100, Interval: 20},
				fill:       0x51,
			},
		},
		masterShardFSMTestEntry{workchain: 0, shard: rightLeft, options: masterShardFSMTestDescriptorOptions{fill: 0x52}},
		masterShardFSMTestEntry{workchain: 0, shard: rightRight, options: masterShardFSMTestDescriptorOptions{fill: 0x53}},
	)
	config := masterShardFSMTestConfig(t, map[int32]masterShardFSMTestWorkchain{
		0: {version: 2, minSplit: 0, maxSplit: 2, splitMergeDelay: 7, splitMergeInterval: 11},
	})

	if err := postprocessMasterShardRegistry(masterShardRegistryPostprocessInput{
		registry:   registry,
		workchains: masterShardTestWorkchainMap(t, config),
		now:        50,
	}); err != nil {
		t.Fatal(err)
	}

	fields := masterShardFSMTestFields(t, registry, 0, left)
	masterShardFSMTestAssertState(t, fields.splitMerge, tlb.FutureSplitMergeNone{})
}

func TestPostprocessMasterShardRegistryCatchainUpdates(t *testing.T) {
	left, right := masterShardFSMTestChildren(t, shard.Root)
	registry := masterShardFSMTestRegistry(t,
		masterShardFSMTestEntry{
			workchain: 0,
			shard:     left,
			options: masterShardFSMTestDescriptorOptions{
				beforeMerge:       true,
				nextCatchainSeqno: 5,
				fill:              0x61,
			},
		},
		masterShardFSMTestEntry{
			workchain: 0,
			shard:     right,
			options: masterShardFSMTestDescriptorOptions{
				nextCatchainSeqno: 8,
				fill:              0x62,
			},
		},
	)
	config := masterShardFSMTestConfig(t, nil)

	if err := postprocessMasterShardRegistry(masterShardRegistryPostprocessInput{
		registry:       registry,
		workchains:     masterShardTestWorkchainMap(t, config),
		now:            20,
		updateCatchain: true,
	}); err != nil {
		t.Fatal(err)
	}

	leftFields := masterShardFSMTestFields(t, registry, 0, left)
	if leftFields.beforeMerge || leftFields.nextCatchainSeqno != 6 {
		t.Fatalf("left fields: before_merge=%t next_catchain=%d, want false/6",
			leftFields.beforeMerge, leftFields.nextCatchainSeqno)
	}
	rightFields := masterShardFSMTestFields(t, registry, 0, right)
	if rightFields.nextCatchainSeqno != 9 {
		t.Fatalf("right next catchain seqno = %d, want 9", rightFields.nextCatchainSeqno)
	}
}

func TestPostprocessMasterShardRegistryRejectsOverflowAtomically(t *testing.T) {
	tests := []struct {
		name           string
		options        masterShardFSMTestDescriptorOptions
		config         masterShardFSMTestWorkchain
		now            uint32
		updateCatchain bool
	}{
		{
			name: "catchain seqno",
			options: masterShardFSMTestDescriptorOptions{
				nextCatchainSeqno: math.MaxUint32,
				fill:              0x71,
			},
			config:         masterShardFSMTestWorkchain{version: 2, maxSplit: 1},
			updateCatchain: true,
		},
		{
			name: "existing interval end",
			options: masterShardFSMTestDescriptorOptions{
				splitMerge: tlb.FutureSplit{SplitUtime: math.MaxUint32 - 2, Interval: 3},
				fill:       0x73,
			},
			config: masterShardFSMTestWorkchain{version: 2, maxSplit: 1},
			now:    1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registry := masterShardFSMTestRegistry(t, masterShardFSMTestEntry{
				workchain: 0,
				shard:     shard.Root,
				options:   test.options,
			})
			key := shardRegistryKey{workchain: 0, shard: shard.Root}
			beforeDescriptor := registry.leaves[key].top.Descriptor.HashKey()
			registry.accepted[key] = registry.leaves[key]
			beforeAccepted := registry.accepted[key].top.Descriptor.HashKey()
			config := masterShardFSMTestConfig(t, map[int32]masterShardFSMTestWorkchain{0: test.config})

			err := postprocessMasterShardRegistry(masterShardRegistryPostprocessInput{
				registry:       registry,
				workchains:     masterShardTestWorkchainMap(t, config),
				now:            test.now,
				updateCatchain: test.updateCatchain,
			})
			if !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("error = %v, want ErrInvalidInput", err)
			}
			if registry.leaves[key].top.Descriptor.HashKey() != beforeDescriptor {
				t.Fatal("failed postprocess changed shard descriptor")
			}
			if len(registry.accepted) != 1 || registry.accepted[key].top.Descriptor.HashKey() != beforeAccepted {
				t.Fatal("failed postprocess changed accepted shard fees")
			}
		})
	}
}

// ValidateQuery rejects an announced split/merge window that is empty, shorter
// than min_split_merge_interval or ending past max_split_merge_delay. An
// inconsistent workchain configuration must therefore leave the shard
// unannounced instead of producing a masterchain block nobody can accept.
func TestPostprocessMasterShardRegistrySkipsUnschedulableAnnouncement(t *testing.T) {
	tests := []struct {
		name   string
		config masterShardFSMTestWorkchain
		now    uint32
	}{
		{
			name: "window overflows uint32",
			config: masterShardFSMTestWorkchain{
				version:            2,
				maxSplit:           1,
				splitMergeDelay:    10,
				splitMergeInterval: 1,
			},
			now: math.MaxUint32 - 5,
		},
		{
			name: "interval below the validator minimum",
			config: masterShardFSMTestWorkchain{
				version:               2,
				maxSplit:              1,
				splitMergeDelay:       7,
				splitMergeInterval:    11,
				minSplitMergeInterval: 12,
				maxSplitMergeDelay:    1000,
			},
			now: 20,
		},
		{
			name: "window ends past the validator maximum",
			config: masterShardFSMTestWorkchain{
				version:               2,
				maxSplit:              1,
				splitMergeDelay:       7,
				splitMergeInterval:    11,
				minSplitMergeInterval: 11,
				maxSplitMergeDelay:    17,
			},
			now: 20,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registry := masterShardFSMTestRegistry(t, masterShardFSMTestEntry{
				workchain: 0,
				shard:     shard.Root,
				options: masterShardFSMTestDescriptorOptions{
					wantSplit: true,
					fill:      0x72,
				},
			})
			config := masterShardFSMTestConfig(t, map[int32]masterShardFSMTestWorkchain{0: test.config})

			if err := postprocessMasterShardRegistry(masterShardRegistryPostprocessInput{
				registry:   registry,
				workchains: masterShardTestWorkchainMap(t, config),
				now:        test.now,
			}); err != nil {
				t.Fatal(err)
			}

			fields := masterShardFSMTestFields(t, registry, 0, shard.Root)
			masterShardFSMTestAssertState(t, fields.splitMerge, tlb.FutureSplitMergeNone{})
		})
	}
}

func TestActivateMasterShardWorkchains(t *testing.T) {
	registry := masterShardFSMTestRegistry(t, masterShardFSMTestEntry{
		workchain: 0,
		shard:     shard.Root,
		options:   masterShardFSMTestDescriptorOptions{fill: 0x81},
	})
	existingKey := shardRegistryKey{workchain: 0, shard: shard.Root}
	registry.accepted[existingKey] = registry.leaves[existingKey]
	existingDescriptor := registry.leaves[existingKey].top.Descriptor.HashKey()
	config := masterShardFSMTestConfig(t, map[int32]masterShardFSMTestWorkchain{
		0: {version: 1, active: true, maxSplit: 1, rootFill: 0x90, fileFill: 0x91},
		1: {
			version:      1,
			enabledSince: 50,
			active:       true,
			maxSplit:     1,
			rootFill:     0xa1,
			fileFill:     0xb1,
		},
		2: {version: 1, active: false, maxSplit: 1, rootFill: 0xa2, fileFill: 0xb2},
		3: {
			version:      1,
			enabledSince: 51,
			active:       true,
			maxSplit:     1,
			rootFill:     0xa3,
			fileFill:     0xb3,
		},
		4: {
			version:      2,
			enabledSince: 1,
			active:       true,
			maxSplit:     1,
			rootFill:     0xa4,
			fileFill:     0xb4,
		},
	})

	input := masterShardWorkchainActivationInput{
		registry:      registry,
		workchains:    masterShardTestWorkchainMap(t, config),
		now:           50,
		newBlockSeqno: 77,
	}
	if err := activateMasterShardWorkchains(input); err != nil {
		t.Fatal(err)
	}

	if len(registry.leaves) != 3 {
		t.Fatalf("leaf count = %d, want 3", len(registry.leaves))
	}
	if len(registry.accepted) != 3 {
		t.Fatalf("accepted fee count = %d, want 3", len(registry.accepted))
	}
	if registry.leaves[existingKey].top.Descriptor.HashKey() != existingDescriptor {
		t.Fatal("existing workchain descriptor changed")
	}
	for _, workchain := range []struct {
		id       int32
		rootFill byte
		fileFill byte
	}{
		{id: 1, rootFill: 0xa1, fileFill: 0xb1},
		{id: 4, rootFill: 0xa4, fileFill: 0xb4},
	} {
		key := shardRegistryKey{workchain: workchain.id, shard: shard.Root}
		leaf, exists := registry.leaves[key]
		if !exists {
			t.Fatalf("workchain %d was not activated", workchain.id)
		}
		fields := masterShardFSMTestFields(t, registry, workchain.id, shard.Root)
		if fields.seqno != 0 || fields.regMCSeqno != 77 || fields.startLT != 0 || fields.endLT != 0 || fields.genUtime != 0 {
			t.Fatalf("workchain %d initial descriptor counters are invalid: %+v", workchain.id, fields)
		}
		if !bytes.Equal(fields.rootHash, bytes.Repeat([]byte{workchain.rootFill}, 32)) ||
			!bytes.Equal(fields.fileHash, bytes.Repeat([]byte{workchain.fileFill}, 32)) {
			t.Fatalf("workchain %d initial hashes differ from zero state", workchain.id)
		}
		if fields.beforeSplit || fields.beforeMerge || fields.wantSplit || fields.wantMerge || fields.nextCCUpdated {
			t.Fatalf("workchain %d initial descriptor flags are set", workchain.id)
		}
		if fields.nextCatchainSeqno != 0 || fields.nextValidatorShard != shard.Root || fields.minRefMCSeqno != math.MaxUint32 {
			t.Fatalf("workchain %d initial validator fields are invalid", workchain.id)
		}
		masterShardFSMTestAssertState(t, fields.splitMerge, tlb.FutureSplitMergeNone{})
		zero := tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(0)}
		if !fields.fees.Equals(zero) || !fields.created.Equals(zero) {
			t.Fatalf("workchain %d initial currencies are not zero", workchain.id)
		}
		if _, exists = registry.accepted[key]; !exists {
			t.Fatalf("workchain %d has no zero ShardFees entry", workchain.id)
		}
		tag, err := loadMasterShardDescriptorTag(leaf.top.Descriptor)
		if err != nil {
			t.Fatal(err)
		}
		if tag != 0xb {
			t.Fatalf("workchain %d descriptor tag = %x, want b", workchain.id, tag)
		}
	}
	for _, workchainID := range []int32{2, 3} {
		if _, exists := registry.leaves[shardRegistryKey{workchain: workchainID, shard: shard.Root}]; exists {
			t.Fatalf("workchain %d was activated", workchainID)
		}
	}
	if _, err := registry.Build(); err != nil {
		t.Fatal(err)
	}

	beforeCount := len(registry.leaves)
	if err := activateMasterShardWorkchains(input); err != nil {
		t.Fatal(err)
	}
	if len(registry.leaves) != beforeCount || len(registry.accepted) != 3 {
		t.Fatal("repeated activation was not idempotent")
	}
}

func masterShardFSMTestRegistry(t *testing.T, entries ...masterShardFSMTestEntry) *ShardRegistry {
	t.Helper()
	leaves := make(map[shardRegistryKey]shardRegistryLeaf, len(entries))
	for i, entry := range entries {
		options := entry.options
		if options.fill == 0 {
			options.fill = byte(i + 1)
		}
		descriptor := masterShardFSMTestDescriptor(t, entry.workchain, entry.shard, options)
		leaf, err := decodeShardRegistryLeaf(entry.workchain, entry.shard, descriptor)
		if err != nil {
			t.Fatal(err)
		}
		key := shardRegistryKey{workchain: entry.workchain, shard: entry.shard}
		leaves[key] = leaf
	}
	if _, err := buildShardHashesDictionary(leaves); err != nil {
		t.Fatalf("build test shard registry: %v", err)
	}
	return &ShardRegistry{
		leaves:   leaves,
		accepted: make(map[shardRegistryKey]shardRegistryLeaf),
	}
}

func masterShardFSMTestDescriptor(
	t *testing.T,
	workchain int32,
	shardID int64,
	options masterShardFSMTestDescriptorOptions,
) *cell.Cell {
	t.Helper()
	if options.tag == 0 {
		options.tag = 0xb
	}
	if options.splitMerge == nil {
		options.splitMerge = tlb.FutureSplitMergeNone{}
	}
	block := ton.BlockIDExt{
		Workchain: workchain,
		Shard:     shardID,
		SeqNo:     1,
		RootHash:  bytes.Repeat([]byte{options.fill}, 32),
		FileHash:  bytes.Repeat([]byte{options.fill ^ 0xff}, 32),
	}
	zero := tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(0)}

	var value any
	switch options.tag {
	case 0xa:
		description := tlb.ShardDesc{
			SeqNo:              block.SeqNo,
			RegMcSeqno:         2,
			StartLT:            100,
			EndLT:              199,
			GenUTime:           10,
			RootHash:           block.RootHash,
			FileHash:           block.FileHash,
			BeforeSplit:        options.beforeSplit,
			BeforeMerge:        options.beforeMerge,
			WantSplit:          options.wantSplit,
			WantMerge:          options.wantMerge,
			NXCCUpdated:        options.nextCCUpdated,
			NextCatchainSeqNo:  options.nextCatchainSeqno,
			NextValidatorShard: shardID,
			MinRefMcSeqNo:      1,
			SplitMergeAt:       options.splitMerge,
		}
		description.Currencies.FeesCollected = zero
		description.Currencies.FundsCreated = zero
		value = description

	case 0xb:
		value = tlb.ShardDescB{
			SeqNo:              block.SeqNo,
			RegMcSeqno:         2,
			StartLT:            100,
			EndLT:              199,
			GenUTime:           10,
			RootHash:           block.RootHash,
			FileHash:           block.FileHash,
			BeforeSplit:        options.beforeSplit,
			BeforeMerge:        options.beforeMerge,
			WantSplit:          options.wantSplit,
			WantMerge:          options.wantMerge,
			NXCCUpdated:        options.nextCCUpdated,
			NextCatchainSeqNo:  options.nextCatchainSeqno,
			NextValidatorShard: shardID,
			MinRefMcSeqNo:      1,
			SplitMergeAt:       options.splitMerge,
			FeesCollected:      zero,
			FundsCreated:       zero,
		}

	default:
		t.Fatalf("unsupported test descriptor tag %x", options.tag)
	}
	descriptor, err := tlb.ToCell(value)
	if err != nil {
		t.Fatalf("serialize test descriptor: %v", err)
	}
	return descriptor
}

func masterShardFSMTestConfig(
	t *testing.T,
	workchains map[int32]masterShardFSMTestWorkchain,
) *cell.Cell {
	t.Helper()
	dictionary := cell.NewDict(32)
	for workchainID, options := range workchains {
		descriptor := masterShardFSMTestWorkchainDescriptor(options)
		if err := dictionary.SetIntKey(big.NewInt(int64(workchainID)), descriptor); err != nil {
			t.Fatal(err)
		}
	}
	parameter := cell.BeginCell().MustStoreDict(dictionary).EndCell()
	config := cell.NewDict(32)
	value := cell.BeginCell().MustStoreRef(parameter).EndCell()
	if err := config.SetIntKey(big.NewInt(int64(tlb.ConfigParamWorkchains)), value); err != nil {
		t.Fatal(err)
	}
	return config.AsCell()
}

func masterShardFSMTestWorkchainDescriptor(options masterShardFSMTestWorkchain) *cell.Cell {
	tag := uint64(0xa6)
	if options.version == 2 {
		tag = 0xa7
	}
	builder := cell.BeginCell().
		MustStoreUInt(tag, 8).
		MustStoreUInt(uint64(options.enabledSince), 32).
		MustStoreUInt(uint64(options.monitorMinSplit), 8).
		MustStoreUInt(uint64(options.minSplit), 8).
		MustStoreUInt(uint64(options.maxSplit), 8).
		MustStoreBoolBit(true).
		MustStoreBoolBit(options.active).
		MustStoreBoolBit(true).
		MustStoreUInt(0, 13).
		MustStoreSlice(bytes.Repeat([]byte{options.rootFill}, 32), 256).
		MustStoreSlice(bytes.Repeat([]byte{options.fileFill}, 32), 256).
		MustStoreUInt(0, 32).
		MustStoreUInt(1, 4).
		MustStoreInt(0, 32).
		MustStoreUInt(0, 64)
	if options.version == 2 {
		builder.MustStoreUInt(0, 4).
			MustStoreUInt(uint64(options.splitMergeDelay), 32).
			MustStoreUInt(uint64(options.splitMergeInterval), 32).
			MustStoreUInt(uint64(options.minSplitMergeInterval), 32).
			MustStoreUInt(uint64(options.maxSplitMergeDelay), 32).
			MustStoreUInt(uint64(options.persistentStateSplitDepth), 8)
	}
	return builder.EndCell()
}

func masterShardFSMTestFields(t *testing.T, registry *ShardRegistry, workchain int32, shardID int64) shardDescriptorFields {
	t.Helper()
	leaf, exists := registry.leaves[shardRegistryKey{workchain: workchain, shard: shardID}]
	if !exists {
		t.Fatalf("missing shard %d:%016x", workchain, uint64(shardID))
	}
	fields, err := parseShardDescriptorFields(leaf.top.Descriptor)
	if err != nil {
		t.Fatal(err)
	}
	return fields
}

func masterShardFSMTestAssertState(t *testing.T, got, want any) {
	t.Helper()
	switch want := want.(type) {
	case tlb.FutureSplitMergeNone:
		if _, ok := got.(tlb.FutureSplitMergeNone); !ok {
			t.Fatalf("split/merge state = %#v, want none", got)
		}
	case tlb.FutureSplit:
		value, ok := got.(tlb.FutureSplit)
		if !ok || value.SplitUtime != want.SplitUtime || value.Interval != want.Interval {
			t.Fatalf("split/merge state = %#v, want %#v", got, want)
		}
	case tlb.FutureMerge:
		value, ok := got.(tlb.FutureMerge)
		if !ok || value.MergeUtime != want.MergeUtime || value.Interval != want.Interval {
			t.Fatalf("split/merge state = %#v, want %#v", got, want)
		}
	default:
		t.Fatalf("unsupported expected split/merge state %T", want)
	}
}

func masterShardFSMTestChildren(t *testing.T, parent int64) (int64, int64) {
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

// masterShardTestWorkchainMap parses a test configuration the way PrepareConfig
// does, so tests feed the shard passes the same already-parsed map production
// takes from Config.workchains.
func masterShardTestWorkchainMap(t testing.TB, configRoot *cell.Cell) map[int32]*masterShardWorkchainInfo {
	t.Helper()

	workchains, err := loadMasterShardWorkchains(configRoot)
	if err != nil {
		t.Fatal(err)
	}
	return workchains
}
