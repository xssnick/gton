package collator

import (
	"bytes"
	"fmt"
	"maps"
	"math"
	"slices"

	"github.com/xssnick/tonutils-go/tlb"

	"github.com/xssnick/gton/service/shard"
)

type masterShardLayoutValidationInput struct {
	oldRegistry *ShardRegistry
	newRegistry *ShardRegistry
	// workchains is the resulting configuration's parameter 12, already parsed
	// by PrepareConfig.
	workchains     map[int32]*masterShardWorkchainInfo
	tops           []ShardTop
	now            uint32
	startLT        uint64
	newBlockSeqno  uint32
	updateCatchain bool
}

type masterShardLayoutFSMInput struct {
	key               shardRegistryKey
	fields            shardDescriptorFields
	previous          *shardDescriptorFields
	sibling           *shardDescriptorFields
	workchain         *masterShardWorkchainInfo
	baseCatchainSeqno uint32
	now               uint32
	updateCatchain    bool
}

type masterShardFSMKind uint8

const (
	masterShardFSMKindNone masterShardFSMKind = iota
	masterShardFSMKindSplit
	masterShardFSMKindMerge
)

// validateMasterShardLayout checks the admissible old-to-new ShardHashes
// transition. It intentionally validates the same range of FSM choices as
// the validation-side shard layout check instead of rebuilding the collator's one
// preferred result.
func validateMasterShardLayout(input masterShardLayoutValidationInput) error {
	if input.workchains == nil {
		return fmt.Errorf("%w: new workchain config is absent", ErrInvalidInput)
	}
	workchains := input.workchains

	expected := &ShardRegistry{
		leaves:   cloneShardRegistryLeaves(input.oldRegistry.leaves),
		accepted: make(map[shardRegistryKey]shardRegistryLeaf),
	}
	if err := expected.Apply(input.tops, input.startLT); err != nil {
		return fmt.Errorf("%w: apply verified shard tops: %v", ErrInvalidInput, err)
	}

	oldLeaves := input.oldRegistry.leaves
	newLeaves := input.newRegistry.leaves
	expectedLeaves := expected.leaves

	presentWorkchains := make(map[int32]struct{})
	for _, key := range sortedShardRegistryKeys(newLeaves) {
		item := newLeaves[key]
		workchain := workchains[key.workchain]
		if workchain == nil {
			return fmt.Errorf("%w: shard %s belongs to an unknown workchain", ErrInvalidInput,
				formatShardRegistryKey(key))
		}
		if item.fields.nextValidatorShard != key.shard {
			return fmt.Errorf("%w: shard %s has next validator shard %016x", ErrInvalidInput,
				formatShardRegistryKey(key), uint64(item.fields.nextValidatorShard))
		}
		presentWorkchains[key.workchain] = struct{}{}
		sibling, err := masterShardLayoutSibling(newLeaves, key)
		if err != nil {
			return fmt.Errorf("%w: shard %s: %v", ErrInvalidInput, formatShardRegistryKey(key), err)
		}

		expectedItem, exists := expectedLeaves[key]
		if !exists {
			if err = validateMasterShardActivation(key, item, workchain, input); err != nil {
				return fmt.Errorf("%w: %v", ErrInvalidInput, err)
			}
			if err = validateMasterShardLayoutFSM(masterShardLayoutFSMInput{
				key:            key,
				fields:         item.fields,
				sibling:        sibling,
				workchain:      workchain,
				now:            input.now,
				updateCatchain: input.updateCatchain,
			}); err != nil {
				return fmt.Errorf("%w: shard %s: %v", ErrInvalidInput, formatShardRegistryKey(key), err)
			}
			continue
		}

		if !sameShardBlock(item.top.Block, expectedItem.top.Block) {
			return fmt.Errorf("%w: shard %s has an unexpected top block", ErrInvalidInput,
				formatShardRegistryKey(key))
		}

		acceptedItem, imported := expected.accepted[key]
		previous, hasPrevious := oldLeaves[key]
		var baseCatchainSeqno uint32
		switch {
		case imported:
			if hasPrevious && sameShardBlock(previous.top.Block, item.top.Block) {
				return fmt.Errorf("%w: unchanged shard %s has a redundant top descriptor", ErrInvalidInput,
					formatShardRegistryKey(key))
			}
			if !sameMasterShardBasicInfo(item, acceptedItem, false) {
				return fmt.Errorf("%w: imported shard %s differs from its verified descriptor", ErrInvalidInput,
					formatShardRegistryKey(key))
			}
			if item.fields.regMCSeqno != input.newBlockSeqno {
				return fmt.Errorf("%w: imported shard %s has registration mc seqno %d, want %d", ErrInvalidInput,
					formatShardRegistryKey(key), item.fields.regMCSeqno, input.newBlockSeqno)
			}
			if item.fields.genUtime > input.now {
				return fmt.Errorf("%w: imported shard %s was generated in the future", ErrInvalidInput,
					formatShardRegistryKey(key))
			}
			baseCatchainSeqno = acceptedItem.fields.nextCatchainSeqno

		case hasPrevious:
			if !sameMasterShardBasicInfo(item, previous, true) {
				return fmt.Errorf("%w: unchanged shard %s changed immutable descriptor fields", ErrInvalidInput,
					formatShardRegistryKey(key))
			}
			baseCatchainSeqno = previous.fields.nextCatchainSeqno

		default:
			return fmt.Errorf("%w: shard %s has no predecessor", ErrInvalidInput,
				formatShardRegistryKey(key))
		}

		var previousFields *shardDescriptorFields
		if hasPrevious {
			previousFields = &previous.fields
		}
		if err = validateMasterShardLayoutFSM(masterShardLayoutFSMInput{
			key:               key,
			fields:            item.fields,
			previous:          previousFields,
			sibling:           sibling,
			workchain:         workchain,
			baseCatchainSeqno: baseCatchainSeqno,
			now:               input.now,
			updateCatchain:    input.updateCatchain,
		}); err != nil {
			return fmt.Errorf("%w: shard %s: %v", ErrInvalidInput, formatShardRegistryKey(key), err)
		}
	}

	for _, key := range sortedShardRegistryKeys(expectedLeaves) {
		if _, exists := newLeaves[key]; !exists {
			return fmt.Errorf("%w: previous or imported shard %s is absent from the new layout", ErrInvalidInput,
				formatShardRegistryKey(key))
		}
	}
	workchainIDs := slices.Sorted(maps.Keys(workchains))
	for _, workchainID := range workchainIDs {
		workchain := workchains[workchainID]
		if !workchain.active {
			continue
		}
		if _, exists := presentWorkchains[workchainID]; !exists {
			return fmt.Errorf("%w: active workchain %d is absent from the new layout", ErrInvalidInput, workchainID)
		}
	}

	return nil
}

func validateMasterShardActivation(
	key shardRegistryKey,
	item shardRegistryLeaf,
	workchain *masterShardWorkchainInfo,
	input masterShardLayoutValidationInput,
) error {
	if key.shard != shard.Root {
		return fmt.Errorf("new workchain %d starts with split shard %016x", key.workchain, uint64(key.shard))
	}
	if !workchain.active {
		return fmt.Errorf("new shard %s belongs to an inactive workchain", formatShardRegistryKey(key))
	}
	if item.fields.seqno != 0 {
		return fmt.Errorf("new shard %s starts with seqno %d", formatShardRegistryKey(key), item.fields.seqno)
	}
	if !bytes.Equal(item.fields.rootHash, workchain.zeroStateRootHash) ||
		!bytes.Equal(item.fields.fileHash, workchain.zeroStateFileHash) {
		return fmt.Errorf("new shard %s has incorrect zero-state hashes", formatShardRegistryKey(key))
	}
	if item.fields.endLT >= input.startLT {
		return fmt.Errorf("new shard %s has end lt %d, current limit is %d",
			formatShardRegistryKey(key), item.fields.endLT, input.startLT)
	}
	if item.fields.genUtime > input.now {
		return fmt.Errorf("new shard %s was generated in the future", formatShardRegistryKey(key))
	}
	if item.fields.beforeSplit || item.fields.beforeMerge || item.fields.wantSplit || item.fields.wantMerge {
		return fmt.Errorf("new shard %s has split/merge flags", formatShardRegistryKey(key))
	}
	if item.fields.minRefMCSeqno != math.MaxUint32 {
		return fmt.Errorf("new shard %s has finite min ref mc seqno", formatShardRegistryKey(key))
	}
	if item.fields.regMCSeqno != input.newBlockSeqno {
		return fmt.Errorf("new shard %s has registration mc seqno %d, want %d",
			formatShardRegistryKey(key), item.fields.regMCSeqno, input.newBlockSeqno)
	}
	if !currencyZero(item.fields.fees) {
		return fmt.Errorf("new shard %s has non-zero collected fees", formatShardRegistryKey(key))
	}
	return nil
}

func validateMasterShardLayoutFSM(input masterShardLayoutFSMInput) error {
	oldBeforeMerge := false
	fsmInherited := false
	if input.previous != nil {
		oldBeforeMerge = input.previous.beforeMerge
		previousKind, _, previousEnd, err := masterShardFSMInterval(input.previous.splitMerge)
		if err != nil {
			return err
		}
		fsmInherited = previousKind != masterShardFSMKindNone &&
			masterShardFSMEqual(input.previous.splitMerge, input.fields.splitMerge)
		if previousKind != masterShardFSMKindNone && !fsmInherited &&
			uint64(input.now) < previousEnd && !input.fields.beforeSplit {
			return fmt.Errorf("live split/merge announcement was changed without a topology reason")
		}
		if fsmInherited && (uint64(input.now) > previousEnd || input.fields.beforeSplit) {
			return fmt.Errorf("expired split/merge announcement was inherited")
		}
	} else if input.fields.beforeSplit {
		return fmt.Errorf("new, split, or merged shard is marked before split")
	}

	depth, err := shard.PrefixLength(input.key.shard)
	if err != nil {
		return fmt.Errorf("derive shard depth: %w", err)
	}
	splitCondition := (input.fields.wantSplit || depth < input.workchain.minSplit) &&
		depth < input.workchain.maxSplit && depth < masterShardMaxSplitDepth
	mergeCondition := !input.fields.beforeSplit && depth > input.workchain.minSplit &&
		(input.fields.wantMerge || depth > input.workchain.maxSplit) && input.sibling != nil &&
		!input.sibling.beforeSplit && (input.sibling.wantMerge || depth > input.workchain.maxSplit)

	kind, start, end, err := masterShardFSMInterval(input.fields.splitMerge)
	if err != nil {
		return err
	}
	if !fsmInherited && kind != masterShardFSMKindNone {
		minimumEnd := start + uint64(input.workchain.minSplitMergeInterval)
		maximumEnd := uint64(input.now) + uint64(input.workchain.maxSplitMergeDelay)
		if start < uint64(input.now) || end <= start || end < minimumEnd ||
			end > maximumEnd || end > math.MaxUint32 {
			return fmt.Errorf("split/merge interval %d..%d is outside the permitted range", start, end)
		}
		if kind == masterShardFSMKindSplit && !splitCondition {
			return fmt.Errorf("split announcement does not satisfy split conditions")
		}
		if kind == masterShardFSMKindMerge && !mergeCondition {
			return fmt.Errorf("merge announcement does not satisfy merge conditions")
		}
	}
	if kind == masterShardFSMKindMerge && (input.sibling == nil || input.sibling.beforeSplit) {
		return fmt.Errorf("merge announcement has no available sibling")
	}
	if input.fields.beforeMerge {
		if input.sibling == nil || !input.sibling.beforeMerge {
			return fmt.Errorf("before merge is not set for both siblings")
		}
		if kind != masterShardFSMKindMerge {
			return fmt.Errorf("before merge has no merge announcement")
		}
		// The announced merge time must already have arrived, for this shard and
		// for its sibling, and the sibling must carry the announcement too
		// (validate-query.cpp:2196-2210). The interval check above cannot stand
		// in for this: it is skipped once the announcement is inherited
		// unchanged, which is exactly the state a shard is in by the time
		// before_merge is raised in a later block.
		if !masterShardFSMMergeMature(input.fields.splitMerge, input.now) {
			return fmt.Errorf("before merge announcement has not matured")
		}
		if !masterShardFSMMerge(input.sibling.splitMerge) {
			return fmt.Errorf("before merge has no merge announcement in the sibling")
		}
		if !masterShardFSMMergeMature(input.sibling.splitMerge, input.now) {
			return fmt.Errorf("before merge announcement in the sibling has not matured")
		}
		if !mergeCondition {
			return fmt.Errorf("before merge does not satisfy merge conditions")
		}
	}

	catchainUpdated := input.fields.nextCatchainSeqno != input.baseCatchainSeqno
	expectedCatchainSeqno := input.baseCatchainSeqno
	if catchainUpdated {
		if expectedCatchainSeqno == math.MaxUint32 {
			return fmt.Errorf("catchain seqno overflows")
		}
		expectedCatchainSeqno++
	}
	if input.fields.nextCatchainSeqno != expectedCatchainSeqno {
		return fmt.Errorf("catchain seqno changed from %d to %d",
			input.baseCatchainSeqno, input.fields.nextCatchainSeqno)
	}
	if !catchainUpdated && input.updateCatchain {
		return fmt.Errorf("catchain seqno was not updated at a mandatory boundary")
	}
	beforeMergeCleared := !input.fields.beforeMerge && oldBeforeMerge
	if !catchainUpdated && beforeMergeCleared {
		return fmt.Errorf("catchain seqno was not updated after clearing before merge")
	}
	if catchainUpdated && !(input.updateCatchain || beforeMergeCleared) {
		return fmt.Errorf("catchain seqno was updated without a protocol reason")
	}
	return nil
}

func masterShardLayoutSibling(
	leaves map[shardRegistryKey]shardRegistryLeaf,
	key shardRegistryKey,
) (*shardDescriptorFields, error) {
	depth, err := shard.PrefixLength(key.shard)
	if err != nil {
		return nil, fmt.Errorf("derive shard depth: %w", err)
	}
	if depth == 0 {
		return nil, nil
	}
	siblingID, err := directMasterShardSibling(key.shard)
	if err != nil {
		return nil, fmt.Errorf("derive sibling: %w", err)
	}
	sibling, exists := leaves[shardRegistryKey{workchain: key.workchain, shard: siblingID}]
	if !exists {
		return nil, nil
	}
	return &sibling.fields, nil
}

func sameMasterShardBasicInfo(left, right shardRegistryLeaf, compareRegistration bool) bool {
	if !sameShardBlock(left.top.Block, right.top.Block) {
		return false
	}
	if left.fields.startLT != right.fields.startLT || left.fields.endLT != right.fields.endLT ||
		left.fields.genUtime != right.fields.genUtime || left.fields.minRefMCSeqno != right.fields.minRefMCSeqno ||
		left.fields.beforeSplit != right.fields.beforeSplit || left.fields.wantSplit != right.fields.wantSplit ||
		left.fields.wantMerge != right.fields.wantMerge {
		return false
	}
	if compareRegistration && left.fields.regMCSeqno != right.fields.regMCSeqno {
		return false
	}
	return left.fields.fees.Equals(right.fields.fees) && left.fields.created.Equals(right.fields.created)
}

func masterShardFSMInterval(state any) (masterShardFSMKind, uint64, uint64, error) {
	var kind masterShardFSMKind
	var start, interval uint32
	switch value := state.(type) {
	case tlb.FutureSplitMergeNone:
		return masterShardFSMKindNone, 0, 0, nil
	case tlb.FutureSplit:
		kind = masterShardFSMKindSplit
		start = value.SplitUtime
		interval = value.Interval
	case tlb.FutureMerge:
		kind = masterShardFSMKindMerge
		start = value.MergeUtime
		interval = value.Interval
	default:
		return masterShardFSMKindNone, 0, 0, fmt.Errorf("unsupported split/merge state %T", state)
	}
	return kind, uint64(start), uint64(start) + uint64(interval), nil
}

func masterShardFSMEqual(left, right any) bool {
	switch leftValue := left.(type) {
	case tlb.FutureSplitMergeNone:
		_, ok := right.(tlb.FutureSplitMergeNone)
		return ok
	case tlb.FutureSplit:
		rightValue, ok := right.(tlb.FutureSplit)
		return ok && leftValue.SplitUtime == rightValue.SplitUtime && leftValue.Interval == rightValue.Interval
	case tlb.FutureMerge:
		rightValue, ok := right.(tlb.FutureMerge)
		return ok && leftValue.MergeUtime == rightValue.MergeUtime && leftValue.Interval == rightValue.Interval
	default:
		return false
	}
}
