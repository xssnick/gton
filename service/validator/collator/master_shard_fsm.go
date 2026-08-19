package collator

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/shard"
)

const (
	masterShardDefaultSplitMergeDelay       = uint32(100)
	masterShardDefaultSplitMergeInterval    = uint32(100)
	masterShardDefaultMinSplitMergeInterval = uint32(30)
	masterShardDefaultMaxSplitMergeDelay    = uint32(1000)
	masterShardMaxSplitDepth                = uint32(60)
)

type masterShardRegistryPostprocessInput struct {
	registry *ShardRegistry
	// workchains is the resulting configuration's parameter 12, already parsed
	// by PrepareConfig. Only the new workchain set controls the FSM.
	workchains     map[int32]*masterShardWorkchainInfo
	now            uint32
	updateCatchain bool
}

type masterShardWorkchainInfo struct {
	enabledSince          uint32
	basic                 bool
	active                bool
	zeroStateRootHash     []byte
	zeroStateFileHash     []byte
	minSplit              uint32
	maxSplit              uint32
	splitMergeDelay       uint32
	splitMergeInterval    uint32
	minSplitMergeInterval uint32
	maxSplitMergeDelay    uint32
}

type masterShardWorkchainActivationInput struct {
	registry *ShardRegistry
	// workchains is the OLD configuration's parameter 12: activation runs
	// against the workchain set the predecessor state carried.
	workchains    map[int32]*masterShardWorkchainInfo
	now           uint32
	newBlockSeqno uint32
}

// activateMasterShardWorkchains inserts an initial root-shard descriptor and
// a zero ShardFees entry for each active, enabled workchain missing from the
// resulting registry. The step is performed against the old masterchain
// configuration.
func activateMasterShardWorkchains(input masterShardWorkchainActivationInput) error {
	if input.registry == nil {
		return fmt.Errorf("%w: shard registry is nil", ErrInvalidInput)
	}
	if input.workchains == nil {
		return fmt.Errorf("%w: old workchain config is absent", ErrInvalidInput)
	}
	workchains := input.workchains

	workchainIDs := slices.Sorted(maps.Keys(workchains))

	present := make(map[int32]struct{})
	for key := range input.registry.leaves {
		present[key.workchain] = struct{}{}
	}
	staged := cloneShardRegistryLeaves(input.registry.leaves)
	accepted := cloneShardRegistryLeaves(input.registry.accepted)
	zeroCurrency := tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(0)}
	for _, workchainID := range workchainIDs {
		workchain := workchains[workchainID]
		if !workchain.active || workchain.enabledSince > input.now {
			continue
		}
		if _, exists := present[workchainID]; exists {
			continue
		}

		fields := shardDescriptorFields{
			regMCSeqno:         input.newBlockSeqno,
			rootHash:           workchain.zeroStateRootHash,
			fileHash:           workchain.zeroStateFileHash,
			nextValidatorShard: shard.Root,
			minRefMCSeqno:      math.MaxUint32,
			splitMerge:         tlb.FutureSplitMergeNone{},
			fees:               zeroCurrency,
			created:            zeroCurrency,
		}
		descriptor, err := storeMasterShardDescriptor(0xb, fields)
		if err != nil {
			return fmt.Errorf("%w: serialize initial workchain %d descriptor: %v", ErrInvalidInput, workchainID, err)
		}
		leaf, err := decodeShardRegistryLeaf(workchainID, shard.Root, descriptor)
		if err != nil {
			return fmt.Errorf("%w: decode initial workchain %d descriptor: %v", ErrInvalidInput, workchainID, err)
		}
		key := shardRegistryKey{workchain: workchainID, shard: shard.Root}
		staged[key] = leaf
		accepted[key] = leaf
		present[workchainID] = struct{}{}
	}

	if _, err := buildShardHashesDictionary(staged); err != nil {
		return fmt.Errorf("%w: serialize activated shard registry: %v", ErrInvalidInput, err)
	}
	if _, err := buildShardFeesDictionary(accepted); err != nil {
		return fmt.Errorf("%w: serialize activated shard fees: %v", ErrInvalidInput, err)
	}

	input.registry.leaves = staged
	input.registry.accepted = accepted
	return nil
}

// postprocessMasterShardRegistry applies the masterchain shard FSM pass to the
// resulting shard registry. Both configuration roots are validated at the
// transition boundary, while only the new workchain set controls the FSM, as
// when the shard configuration is updated.
func postprocessMasterShardRegistry(input masterShardRegistryPostprocessInput) error {
	if input.registry == nil {
		return fmt.Errorf("%w: shard registry is nil", ErrInvalidInput)
	}
	if input.workchains == nil {
		return fmt.Errorf("%w: new workchain config is absent", ErrInvalidInput)
	}
	workchains := input.workchains

	keys := sortedShardRegistryKeys(input.registry.leaves)

	staged := cloneShardRegistryLeaves(input.registry.leaves)
	for _, key := range keys {
		item := input.registry.leaves[key]
		depth, err := shard.PrefixLength(key.shard)
		if err != nil {
			return fmt.Errorf("%w: derive depth for %s: %v", ErrInvalidInput, formatShardRegistryKey(key), err)
		}

		var sibling *shardDescriptorFields
		if depth > 0 {
			siblingID, err := directMasterShardSibling(key.shard)
			if err != nil {
				return fmt.Errorf("%w: derive sibling for %s: %v", ErrInvalidInput, formatShardRegistryKey(key), err)
			}
			siblingItem, exists := input.registry.leaves[shardRegistryKey{workchain: key.workchain, shard: siblingID}]
			if exists {
				sibling = &siblingItem.fields
			}
		}

		fields := item.fields
		workchain := workchains[key.workchain]
		changed, err := updateMasterShardFSM(
			&fields,
			sibling,
			workchain,
			depth,
			input.now,
			input.updateCatchain,
		)
		if err != nil {
			return fmt.Errorf("%w: update %s: %v", ErrInvalidInput, formatShardRegistryKey(key), err)
		}
		if !changed {
			continue
		}

		// Serialization canonicalizes every rewritten descriptor to #a and clears
		// the obsolete nx_cc_updated bit. Untouched leaves keep their bytes.
		fields.nextCCUpdated = false
		descriptor, err := storeMasterShardDescriptor(0xa, fields)
		if err != nil {
			return fmt.Errorf("%w: serialize %s: %v", ErrInvalidInput, formatShardRegistryKey(key), err)
		}
		leaf := item
		leaf.top.Descriptor = descriptor
		leaf.fields = fields
		staged[key] = leaf
	}

	// The key set is untouched here — every write above replaces a leaf that
	// already existed — so the partition this would validate is the one Apply and
	// activateMasterShardWorkchains already validated, and Build re-derives the
	// dictionary from these very leaves on the next statement of the only caller.
	input.registry.leaves = staged
	return nil
}

func updateMasterShardFSM(
	fields *shardDescriptorFields,
	sibling *shardDescriptorFields,
	workchain *masterShardWorkchainInfo,
	depth uint32,
	now uint32,
	updateCatchain bool,
) (bool, error) {
	changed := false
	oldBeforeMerge := fields.beforeMerge
	fields.beforeMerge = false

	expired, err := masterShardFSMExpired(fields.splitMerge, now)
	if err != nil {
		return false, err
	}
	if !masterShardFSMNone(fields.splitMerge) && (expired || fields.beforeSplit) {
		fields.splitMerge = tlb.FutureSplitMergeNone{}
		changed = true
	} else if masterShardFSMMerge(fields.splitMerge) && (sibling == nil || sibling.beforeSplit) {
		fields.splitMerge = tlb.FutureSplitMergeNone{}
		changed = true
	}

	if workchain != nil && !fields.beforeSplit {
		start, interval, schedulable := masterShardFSMSchedule(now, workchain)
		switch {
		case schedulable && masterShardFSMNone(fields.splitMerge) &&
			(fields.wantSplit || depth < workchain.minSplit) &&
			depth < workchain.maxSplit && depth < masterShardMaxSplitDepth:
			fields.splitMerge = tlb.FutureSplit{
				SplitUtime: start,
				Interval:   interval,
			}
			changed = true

		case schedulable && masterShardFSMNone(fields.splitMerge) &&
			depth > workchain.minSplit &&
			(fields.wantMerge || depth > workchain.maxSplit) &&
			sibling != nil && !sibling.beforeSplit &&
			masterShardFSMNone(sibling.splitMerge) &&
			(sibling.wantMerge || depth > workchain.maxSplit):
			fields.splitMerge = tlb.FutureMerge{
				MergeUtime: start,
				Interval:   interval,
			}
			changed = true

		case masterShardFSMMergeMature(fields.splitMerge, now) &&
			depth > workchain.minSplit &&
			sibling != nil && !sibling.beforeSplit &&
			masterShardFSMMergeMature(sibling.splitMerge, now) &&
			(depth > workchain.maxSplit || (fields.wantMerge && sibling.wantMerge)):
			fields.beforeMerge = true
			changed = true
		}
	}

	if fields.beforeMerge != oldBeforeMerge {
		updateCatchain = updateCatchain || oldBeforeMerge
		changed = true
	}
	if updateCatchain {
		if fields.nextCatchainSeqno == math.MaxUint32 {
			return false, fmt.Errorf("next catchain seqno overflows")
		}
		fields.nextCatchainSeqno++
		changed = true
	}

	return changed, nil
}

func masterShardFSMExpired(state any, now uint32) (bool, error) {
	var start, interval uint32
	switch value := state.(type) {
	case tlb.FutureSplitMergeNone:
		return false, nil
	case tlb.FutureSplit:
		start = value.SplitUtime
		interval = value.Interval
	case tlb.FutureMerge:
		start = value.MergeUtime
		interval = value.Interval
	default:
		return false, fmt.Errorf("unsupported split/merge state %T", state)
	}

	end := uint64(start) + uint64(interval)
	if end > math.MaxUint32 {
		return false, fmt.Errorf("split/merge interval end overflows")
	}
	return now >= uint32(end), nil
}

func masterShardFSMNone(state any) bool {
	_, ok := state.(tlb.FutureSplitMergeNone)
	return ok
}

func masterShardFSMMerge(state any) bool {
	_, ok := state.(tlb.FutureMerge)
	return ok
}

func masterShardFSMMergeMature(state any, now uint32) bool {
	merge, ok := state.(tlb.FutureMerge)
	return ok && now >= merge.MergeUtime
}

// masterShardFSMSchedule derives the announcement window from the workchain
// timings of configuration 12. Validation rejects an announced interval that is
// empty, shorter than min_split_merge_interval, or ends later than
// max_split_merge_delay from now, so inconsistent timings must leave the shard
// unannounced rather than produce a masterchain block that no validator can
// accept. Announcing unconditionally would instead rely on the configuration
// being self-consistent; a wrapped uint32 window is likewise not monotonic and
// is refused rather than wrapped.
func masterShardFSMSchedule(now uint32, workchain *masterShardWorkchainInfo) (start, interval uint32, ok bool) {
	begin := uint64(now) + uint64(workchain.splitMergeDelay)
	end := begin + uint64(workchain.splitMergeInterval)
	switch {
	case begin > math.MaxUint32 || end > math.MaxUint32,
		workchain.splitMergeInterval == 0,
		workchain.splitMergeInterval < workchain.minSplitMergeInterval,
		end > uint64(now)+uint64(workchain.maxSplitMergeDelay):
		return 0, 0, false
	}

	return uint32(begin), workchain.splitMergeInterval, true
}

func directMasterShardSibling(shardID int64) (int64, error) {
	parent, err := shard.Parent(shardID)
	if err != nil {
		return 0, err
	}
	left, err := shard.Child(parent, true)
	if err != nil {
		return 0, err
	}
	right, err := shard.Child(parent, false)
	if err != nil {
		return 0, err
	}

	switch shardID {
	case left:
		return right, nil
	case right:
		return left, nil
	default:
		return 0, fmt.Errorf("shard %016x is not a direct child of %016x", uint64(shardID), uint64(parent))
	}
}

func loadMasterShardDescriptorTag(descriptor *cell.Cell) (uint64, error) {
	loader, err := descriptor.BeginParse()
	if err != nil {
		return 0, err
	}
	tag, err := loader.LoadUInt(4)
	if err != nil {
		return 0, err
	}
	if tag != 0xa && tag != 0xb {
		return 0, fmt.Errorf("unknown descriptor tag %x", tag)
	}
	return tag, nil
}

func storeMasterShardDescriptor(tag uint64, fields shardDescriptorFields) (*cell.Cell, error) {
	switch tag {
	case 0xa:
		description := tlb.ShardDesc{
			SeqNo:              fields.seqno,
			RegMcSeqno:         fields.regMCSeqno,
			StartLT:            fields.startLT,
			EndLT:              fields.endLT,
			RootHash:           fields.rootHash,
			FileHash:           fields.fileHash,
			BeforeSplit:        fields.beforeSplit,
			BeforeMerge:        fields.beforeMerge,
			WantSplit:          fields.wantSplit,
			WantMerge:          fields.wantMerge,
			NXCCUpdated:        fields.nextCCUpdated,
			Flags:              fields.flags,
			NextCatchainSeqNo:  fields.nextCatchainSeqno,
			NextValidatorShard: fields.nextValidatorShard,
			MinRefMcSeqNo:      fields.minRefMCSeqno,
			GenUTime:           fields.genUtime,
			SplitMergeAt:       fields.splitMerge,
		}
		description.Currencies.FeesCollected = fields.fees
		description.Currencies.FundsCreated = fields.created
		return tlb.ToCell(description)

	case 0xb:
		return tlb.ToCell(tlb.ShardDescB{
			SeqNo:              fields.seqno,
			RegMcSeqno:         fields.regMCSeqno,
			StartLT:            fields.startLT,
			EndLT:              fields.endLT,
			RootHash:           fields.rootHash,
			FileHash:           fields.fileHash,
			BeforeSplit:        fields.beforeSplit,
			BeforeMerge:        fields.beforeMerge,
			WantSplit:          fields.wantSplit,
			WantMerge:          fields.wantMerge,
			NXCCUpdated:        fields.nextCCUpdated,
			Flags:              fields.flags,
			NextCatchainSeqNo:  fields.nextCatchainSeqno,
			NextValidatorShard: fields.nextValidatorShard,
			MinRefMcSeqNo:      fields.minRefMCSeqno,
			GenUTime:           fields.genUtime,
			SplitMergeAt:       fields.splitMerge,
			FeesCollected:      fields.fees,
			FundsCreated:       fields.created,
		})

	default:
		return nil, fmt.Errorf("unknown descriptor tag %x", tag)
	}
}

func loadMasterShardWorkchains(configRoot *cell.Cell) (map[int32]*masterShardWorkchainInfo, error) {
	parameter, err := (tlb.BlockchainConfig{Root: configRoot}).GetParam(tlb.ConfigParamWorkchains)
	if err != nil {
		if errors.Is(err, tlb.ErrBlockchainConfigParamAbsent) {
			return make(map[int32]*masterShardWorkchainInfo), nil
		}
		return nil, err
	}
	loader, err := parameter.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("parse config parameter 12: %w", err)
	}
	workchains, err := loader.LoadDict(32)
	if err != nil {
		return nil, fmt.Errorf("load workchain dictionary: %w", err)
	}
	if loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		return nil, fmt.Errorf("workchain config has %d trailing bits and %d refs", loader.BitsLeft(), loader.RefsNum())
	}
	if !workchains.ValidateAll() {
		return nil, fmt.Errorf("workchain dictionary is invalid")
	}
	items, err := workchains.LoadAll()
	if err != nil {
		return nil, fmt.Errorf("load workchain dictionary entries: %w", err)
	}

	result := make(map[int32]*masterShardWorkchainInfo, len(items))
	for _, item := range items {
		if item.Key.BitsLeft() != 32 || item.Key.RefsNum() != 0 {
			return nil, fmt.Errorf("workchain key has invalid shape")
		}
		workchain, err := item.Key.LoadInt(32)
		if err != nil {
			return nil, fmt.Errorf("load workchain key: %w", err)
		}
		if item.Key.BitsLeft() != 0 {
			return nil, fmt.Errorf("workchain key has trailing data")
		}
		workchainID := int32(workchain)
		if workchainID == invalidWorkchainID {
			return nil, fmt.Errorf("invalid workchain %d", workchainID)
		}

		info, err := loadMasterShardWorkchainInfo(item.Value)
		if err != nil {
			return nil, fmt.Errorf("workchain %d: %w", workchainID, err)
		}
		result[workchainID] = info
	}
	return result, nil
}

func loadMasterShardWorkchainInfo(loader *cell.Slice) (*masterShardWorkchainInfo, error) {
	tag, err := loader.LoadUInt(8)
	if err != nil {
		return nil, fmt.Errorf("load descriptor tag: %w", err)
	}
	if tag != 0xa6 && tag != 0xa7 {
		return nil, fmt.Errorf("unknown descriptor tag %x", tag)
	}
	enabledSince, err := loader.LoadUInt(32)
	if err != nil {
		return nil, fmt.Errorf("load enabled since: %w", err)
	}
	monitorMinSplit, err := loader.LoadUInt(8)
	if err != nil {
		return nil, fmt.Errorf("load monitor min split: %w", err)
	}
	minSplit, err := loader.LoadUInt(8)
	if err != nil {
		return nil, fmt.Errorf("load min split: %w", err)
	}
	maxSplit, err := loader.LoadUInt(8)
	if err != nil {
		return nil, fmt.Errorf("load max split: %w", err)
	}
	if monitorMinSplit > minSplit || minSplit > maxSplit || maxSplit > uint64(masterShardMaxSplitDepth) {
		return nil, fmt.Errorf("invalid split depths %d/%d/%d", monitorMinSplit, minSplit, maxSplit)
	}
	basic, err := loader.LoadBoolBit()
	if err != nil {
		return nil, fmt.Errorf("load basic flag: %w", err)
	}
	active, err := loader.LoadBoolBit()
	if err != nil {
		return nil, fmt.Errorf("load active flag: %w", err)
	}
	if _, err = loader.LoadBoolBit(); err != nil {
		return nil, fmt.Errorf("load accept messages flag: %w", err)
	}
	flags, err := loader.LoadUInt(13)
	if err != nil {
		return nil, fmt.Errorf("load flags: %w", err)
	}
	if flags != 0 {
		return nil, fmt.Errorf("flags are %d, want 0", flags)
	}
	zeroStateRootHash, err := loader.LoadSlice(256)
	if err != nil {
		return nil, fmt.Errorf("load zero-state root hash: %w", err)
	}
	zeroStateFileHash, err := loader.LoadSlice(256)
	if err != nil {
		return nil, fmt.Errorf("load zero-state file hash: %w", err)
	}
	if _, err = loader.LoadUInt(32); err != nil {
		return nil, fmt.Errorf("load version: %w", err)
	}

	formatTag, err := loader.LoadUInt(4)
	if err != nil {
		return nil, fmt.Errorf("load format tag: %w", err)
	}
	if basic {
		if formatTag != 1 {
			return nil, fmt.Errorf("basic workchain has format tag %x, want 1", formatTag)
		}
		if _, err = loader.LoadInt(32); err != nil {
			return nil, fmt.Errorf("load VM version: %w", err)
		}
		if _, err = loader.LoadUInt(64); err != nil {
			return nil, fmt.Errorf("load VM mode: %w", err)
		}
	} else {
		if formatTag != 0 {
			return nil, fmt.Errorf("extended workchain has format tag %x, want 0", formatTag)
		}
		minAddressLength, err := loader.LoadUInt(12)
		if err != nil {
			return nil, fmt.Errorf("load minimum address length: %w", err)
		}
		maxAddressLength, err := loader.LoadUInt(12)
		if err != nil {
			return nil, fmt.Errorf("load maximum address length: %w", err)
		}
		addressLengthStep, err := loader.LoadUInt(12)
		if err != nil {
			return nil, fmt.Errorf("load address length step: %w", err)
		}
		workchainTypeID, err := loader.LoadUInt(32)
		if err != nil {
			return nil, fmt.Errorf("load workchain type ID: %w", err)
		}
		if minAddressLength < 64 || minAddressLength > maxAddressLength ||
			maxAddressLength > 1023 || addressLengthStep > 1023 || workchainTypeID < 1 {
			return nil, fmt.Errorf("invalid extended workchain format")
		}
	}

	info := &masterShardWorkchainInfo{
		enabledSince:          uint32(enabledSince),
		basic:                 basic,
		active:                active,
		zeroStateRootHash:     zeroStateRootHash,
		zeroStateFileHash:     zeroStateFileHash,
		minSplit:              uint32(minSplit),
		maxSplit:              uint32(maxSplit),
		splitMergeDelay:       masterShardDefaultSplitMergeDelay,
		splitMergeInterval:    masterShardDefaultSplitMergeInterval,
		minSplitMergeInterval: masterShardDefaultMinSplitMergeInterval,
		maxSplitMergeDelay:    masterShardDefaultMaxSplitMergeDelay,
	}
	if tag == 0xa7 {
		timingTag, err := loader.LoadUInt(4)
		if err != nil {
			return nil, fmt.Errorf("load split/merge timings tag: %w", err)
		}
		if timingTag != 0 {
			return nil, fmt.Errorf("split/merge timings tag is %x, want 0", timingTag)
		}
		delay, err := loader.LoadUInt(32)
		if err != nil {
			return nil, fmt.Errorf("load split/merge delay: %w", err)
		}
		interval, err := loader.LoadUInt(32)
		if err != nil {
			return nil, fmt.Errorf("load split/merge interval: %w", err)
		}
		minimumInterval, err := loader.LoadUInt(32)
		if err != nil {
			return nil, fmt.Errorf("load minimum split/merge interval: %w", err)
		}
		maximumDelay, err := loader.LoadUInt(32)
		if err != nil {
			return nil, fmt.Errorf("load maximum split/merge delay: %w", err)
		}
		persistentStateSplitDepth, err := loader.LoadUInt(8)
		if err != nil {
			return nil, fmt.Errorf("load persistent-state split depth: %w", err)
		}
		if persistentStateSplitDepth > 63 {
			return nil, fmt.Errorf("persistent-state split depth is %d, want at most 63", persistentStateSplitDepth)
		}

		info.splitMergeDelay = uint32(delay)
		info.splitMergeInterval = uint32(interval)
		info.minSplitMergeInterval = uint32(minimumInterval)
		info.maxSplitMergeDelay = uint32(maximumDelay)
	}
	if loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		return nil, fmt.Errorf("descriptor has %d trailing bits and %d refs", loader.BitsLeft(), loader.RefsNum())
	}
	return info, nil
}
