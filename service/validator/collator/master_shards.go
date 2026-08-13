package collator

import (
	"bytes"
	"fmt"
	"maps"
	"math"
	"slices"
	"sort"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/shard"
)

const (
	masterShardHashesKeyBits = uint(32)
	masterShardFeesKeyBits   = uint(96)
	masterchainWorkchainID   = int32(-1)
	invalidWorkchainID       = int32(math.MinInt32)
)

// ShardTop is a verified shard tip proposed for the masterchain registry.
// Descriptor is the exact serialized ShardDescr value without the BinTree leaf
// tag. TopBlockDescr is the acquisition pipeline's exact, fully verified
// descriptor cell included in collated data. Predecessors contains one block
// for a linear/split transition and two blocks for a merge transition. Creators
// is ordered from the oldest accepted link to the tip — the order the reverse
// proof chain yields — and contains one creator ID for every accepted
// proof-chain link. The acquisition pipeline derives it while fully
// checking the proof chain; the deterministic core binds only its length.
type ShardTop struct {
	Block         ton.BlockIDExt
	Predecessors  []ton.BlockIDExt
	Descriptor    *cell.Cell
	TopBlockDescr *cell.Cell
	Creators      [][32]byte
}

// ShardRegistry owns an exact, mutable projection of masterchain ShardHashes.
// Apply is atomic and Build serializes a canonical ShardHashes dictionary;
// the ShardFees entries of the most recently applied batch are produced
// separately, by buildShardFeesDictionary over the accepted set.
type ShardRegistry struct {
	leaves   map[shardRegistryKey]shardRegistryLeaf
	accepted map[shardRegistryKey]shardRegistryLeaf
}

type shardRegistryKey struct {
	workchain int32
	shard     int64
}

// shardRegistryLeaf keeps the decoded descriptor next to the top it was
// projected from. The descriptor is parsed once, when the registry is built:
// every consumer of a flag, a fee or a seqno reads it from here instead of
// re-running the ~19-field decode over the same cell.
type shardRegistryLeaf struct {
	top    ShardTop
	fields shardDescriptorFields
}

type shardDescriptorFields struct {
	seqno              uint32
	regMCSeqno         uint32
	startLT            uint64
	endLT              uint64
	genUtime           uint32
	rootHash           []byte
	fileHash           []byte
	flags              uint8
	beforeSplit        bool
	beforeMerge        bool
	wantSplit          bool
	wantMerge          bool
	nextCCUpdated      bool
	nextCatchainSeqno  uint32
	nextValidatorShard int64
	minRefMCSeqno      uint32
	splitMerge         any
	fees               tlb.CurrencyCollection
	created            tlb.CurrencyCollection
}

type shardTransitionKind uint8

const (
	shardTransitionLinear shardTransitionKind = iota
	shardTransitionSplit
	shardTransitionMerge
)

type decodedShardTop struct {
	leaf        shardRegistryLeaf
	kind        shardTransitionKind
	predecessor []ton.BlockIDExt
}

type shardRegistryTransition struct {
	kind     shardTransitionKind
	outputs  []shardRegistryLeaf
	claims   []ton.BlockIDExt
	consumes []shardRegistryKey
}

// ParseShardRegistry strictly parses ShardHashes and retains each ShardDesc
// cell byte-for-byte. Tops are exposed in canonical (workchain, unsigned shard)
// order.
func ParseShardRegistry(shardHashes *cell.Dictionary) (*ShardRegistry, error) {
	if shardHashes == nil {
		return nil, fmt.Errorf("%w: shard hashes dictionary is nil", ErrInvalidInput)
	}
	if shardHashes.GetKeySize() != masterShardHashesKeyBits {
		return nil, fmt.Errorf("%w: shard hashes key size is %d, want %d",
			ErrInvalidInput, shardHashes.GetKeySize(), masterShardHashesKeyBits)
	}

	leaves, err := parseShardRegistryLeaves(shardHashes)
	if err != nil {
		return nil, fmt.Errorf("%w: parse shard hashes: %v", ErrInvalidInput, err)
	}
	rebuilt, err := buildShardHashesDictionary(leaves)
	if err != nil {
		return nil, fmt.Errorf("%w: rebuild shard hashes: %v", ErrInvalidInput, err)
	}
	if !sameDictionaryRoot(shardHashes, rebuilt) {
		return nil, fmt.Errorf("%w: shard hashes do not round-trip exactly", ErrInvalidInput)
	}

	return &ShardRegistry{
		leaves:   leaves,
		accepted: make(map[shardRegistryKey]shardRegistryLeaf),
	}, nil
}

// Tops returns the current registry leaves in canonical order. Descriptor cells
// are immutable and shared; block hash byte slices are copied at this boundary.
func (r *ShardRegistry) Tops() []ShardTop {
	leaves := sortedShardRegistryLeaves(r.leaves)
	tops := make([]ShardTop, len(leaves))
	for i := range leaves {
		tops[i] = ShardTop{
			Block:      *leaves[i].top.Block.Copy(),
			Descriptor: leaves[i].top.Descriptor,
		}
	}
	return tops
}

// Apply atomically applies a verified batch below the current masterchain
// logical-time limit. Batch order does not affect either resulting root.
func (r *ShardRegistry) Apply(tops []ShardTop, ltLimit uint64) error {
	decoded := make([]decodedShardTop, len(tops))
	for i := range tops {
		item, err := decodeShardTop(tops[i])
		if err != nil {
			return fmt.Errorf("%w: shard top %d: %v", ErrInvalidInput, i, err)
		}
		decoded[i] = item
	}

	transitions, err := planShardRegistryTransitions(decoded)
	if err != nil {
		return fmt.Errorf("%w: plan shard transitions: %v", ErrInvalidInput, err)
	}
	for i := range transitions {
		if err = verifyShardRegistryTransition(&transitions[i], r.leaves, ltLimit); err != nil {
			return fmt.Errorf("%w: verify shard transition: %v", ErrInvalidInput, err)
		}
	}

	staged := cloneShardRegistryLeaves(r.leaves)
	accepted := make(map[shardRegistryKey]shardRegistryLeaf, len(tops))
	for i := range transitions {
		transition := transitions[i]
		for _, consumed := range transition.consumes {
			delete(staged, consumed)
		}
		for _, output := range transition.outputs {
			key := shardTopKey(output.top.Block)
			if _, conflict := staged[key]; conflict {
				return fmt.Errorf("%w: transition output %s conflicts with an existing shard",
					ErrInvalidInput, formatShardRegistryKey(key))
			}
			staged[key] = output
			accepted[key] = output
		}
	}

	if _, err = buildShardHashesDictionary(staged); err != nil {
		return fmt.Errorf("%w: serialize applied shard registry: %v", ErrInvalidInput, err)
	}
	if _, err = buildShardFeesDictionary(accepted); err != nil {
		return fmt.Errorf("%w: serialize accepted shard fees: %v", ErrInvalidInput, err)
	}

	r.leaves = staged
	r.accepted = accepted
	return nil
}

// Build returns a canonical serialized ShardHashes dictionary for the current
// registry. It deliberately re-derives it from the committed r.leaves rather
// than reusing any snapshot staged during Apply or the FSM pass.
//
// ShardFees is not built here: it is a pure function of the frozen accepted set
// and is produced exactly once, by prepareMaster, before the state pass reads
// it — see buildShardFeesDictionary.
func (r *ShardRegistry) Build() (*cell.Dictionary, error) {
	shardHashes, err := buildShardHashesDictionary(r.leaves)
	if err != nil {
		return nil, fmt.Errorf("build shard hashes: %w", err)
	}
	return shardHashes, nil
}

func parseShardRegistryLeaves(shardHashes *cell.Dictionary) (map[shardRegistryKey]shardRegistryLeaf, error) {
	items, err := shardHashes.LoadAll()
	if err != nil {
		return nil, fmt.Errorf("load workchains: %w", err)
	}

	leaves := make(map[shardRegistryKey]shardRegistryLeaf)
	for _, item := range items {
		if item.Key.BitsLeft() != masterShardHashesKeyBits || item.Key.RefsNum() != 0 {
			return nil, fmt.Errorf("workchain key has invalid shape")
		}
		workchain, err := item.Key.LoadInt(masterShardHashesKeyBits)
		if err != nil {
			return nil, fmt.Errorf("load workchain key: %w", err)
		}
		if item.Key.BitsLeft() != 0 {
			return nil, fmt.Errorf("workchain key has trailing data")
		}
		workchainID := int32(workchain)
		if workchainID == masterchainWorkchainID || workchainID == invalidWorkchainID {
			return nil, fmt.Errorf("invalid workchain %d", workchainID)
		}
		if item.Value.BitsLeft() != 0 || item.Value.RefsNum() != 1 {
			return nil, fmt.Errorf("workchain %d value must contain exactly one tree reference", workchainID)
		}
		tree, err := item.Value.LoadRefCell()
		if err != nil {
			return nil, fmt.Errorf("load workchain %d tree: %w", workchainID, err)
		}
		if err = parseShardRegistryTree(workchainID, shard.Root, tree, leaves); err != nil {
			return nil, fmt.Errorf("workchain %d: %w", workchainID, err)
		}
	}
	return leaves, nil
}

func parseShardRegistryTree(
	workchain int32,
	shardID int64,
	node *cell.Cell,
	leaves map[shardRegistryKey]shardRegistryLeaf,
) error {
	if node == nil {
		return fmt.Errorf("shard %016x tree node is nil or special", uint64(shardID))
	}
	// State roots loaded from CellDB preserve untouched subtrees as lazy pruned
	// boundaries. A ShardHashes branch is a regular cell behind that boundary,
	// so load it before rejecting genuinely special proof cells.
	var err error
	node, err = node.Prewarm()
	if err != nil {
		return fmt.Errorf("materialize shard %016x tree node: %w", uint64(shardID), err)
	}
	if node.IsSpecial() {
		return fmt.Errorf("shard %016x tree node is nil or special", uint64(shardID))
	}
	loader, err := node.BeginParse()
	if err != nil {
		return fmt.Errorf("parse shard %016x tree node: %w", uint64(shardID), err)
	}
	fork, err := loader.LoadBoolBit()
	if err != nil {
		return fmt.Errorf("load shard %016x tree tag: %w", uint64(shardID), err)
	}
	if !fork {
		descriptor, err := loader.ToCell()
		if err != nil {
			return fmt.Errorf("load shard %016x descriptor: %w", uint64(shardID), err)
		}
		leaf, err := decodeShardRegistryLeaf(workchain, shardID, descriptor)
		if err != nil {
			return err
		}
		key := shardTopKey(leaf.top.Block)
		if _, duplicate := leaves[key]; duplicate {
			return fmt.Errorf("duplicate shard %s", formatShardRegistryKey(key))
		}
		leaves[key] = leaf
		return nil
	}
	if loader.BitsLeft() != 0 || loader.RefsNum() != 2 {
		return fmt.Errorf("shard %016x fork has %d trailing bits and %d refs",
			uint64(shardID), loader.BitsLeft(), loader.RefsNum())
	}

	leftID, err := shard.Child(shardID, true)
	if err != nil {
		return fmt.Errorf("derive left child of %016x: %w", uint64(shardID), err)
	}
	rightID, err := shard.Child(shardID, false)
	if err != nil {
		return fmt.Errorf("derive right child of %016x: %w", uint64(shardID), err)
	}
	left, err := loader.LoadRefCell()
	if err != nil {
		return fmt.Errorf("load left child of %016x: %w", uint64(shardID), err)
	}
	right, err := loader.LoadRefCell()
	if err != nil {
		return fmt.Errorf("load right child of %016x: %w", uint64(shardID), err)
	}
	if err = parseShardRegistryTree(workchain, leftID, left, leaves); err != nil {
		return err
	}
	return parseShardRegistryTree(workchain, rightID, right, leaves)
}

func decodeShardRegistryLeaf(workchain int32, shardID int64, descriptor *cell.Cell) (shardRegistryLeaf, error) {
	fields, err := parseShardDescriptorFields(descriptor)
	if err != nil {
		return shardRegistryLeaf{}, fmt.Errorf("shard %d:%016x descriptor: %w", workchain, uint64(shardID), err)
	}
	block := ton.BlockIDExt{
		Workchain: workchain,
		Shard:     shardID,
		SeqNo:     fields.seqno,
		RootHash:  fields.rootHash,
		FileHash:  fields.fileHash,
	}
	return shardRegistryLeaf{
		top: ShardTop{
			Block:      block,
			Descriptor: descriptor,
		},
		fields: fields,
	}, nil
}

func parseShardDescriptorFields(descriptor *cell.Cell) (shardDescriptorFields, error) {
	if descriptor == nil || descriptor.IsSpecial() {
		return shardDescriptorFields{}, fmt.Errorf("descriptor is nil or special")
	}
	loader, err := descriptor.BeginParse()
	if err != nil {
		return shardDescriptorFields{}, fmt.Errorf("parse descriptor: %w", err)
	}
	magic, err := loader.LoadUInt(4)
	if err != nil {
		return shardDescriptorFields{}, fmt.Errorf("load descriptor tag: %w", err)
	}

	var fields shardDescriptorFields
	switch magic {
	case 0xa:
		var value tlb.ShardDesc
		if err = tlb.LoadFromCell(&value, loader, true); err != nil {
			return shardDescriptorFields{}, fmt.Errorf("parse ShardDesc: %w", err)
		}
		fields = shardDescriptorFields{
			seqno:              value.SeqNo,
			regMCSeqno:         value.RegMcSeqno,
			startLT:            value.StartLT,
			endLT:              value.EndLT,
			genUtime:           value.GenUTime,
			rootHash:           value.RootHash,
			fileHash:           value.FileHash,
			flags:              value.Flags,
			beforeSplit:        value.BeforeSplit,
			beforeMerge:        value.BeforeMerge,
			wantSplit:          value.WantSplit,
			wantMerge:          value.WantMerge,
			nextCCUpdated:      value.NXCCUpdated,
			nextCatchainSeqno:  value.NextCatchainSeqNo,
			nextValidatorShard: value.NextValidatorShard,
			minRefMCSeqno:      value.MinRefMcSeqNo,
			splitMerge:         value.SplitMergeAt,
			fees:               value.Currencies.FeesCollected,
			created:            value.Currencies.FundsCreated,
		}
	case 0xb:
		var value tlb.ShardDescB
		if err = tlb.LoadFromCell(&value, loader, true); err != nil {
			return shardDescriptorFields{}, fmt.Errorf("parse ShardDescB: %w", err)
		}
		fields = shardDescriptorFields{
			seqno:              value.SeqNo,
			regMCSeqno:         value.RegMcSeqno,
			startLT:            value.StartLT,
			endLT:              value.EndLT,
			genUtime:           value.GenUTime,
			rootHash:           value.RootHash,
			fileHash:           value.FileHash,
			flags:              value.Flags,
			beforeSplit:        value.BeforeSplit,
			beforeMerge:        value.BeforeMerge,
			wantSplit:          value.WantSplit,
			wantMerge:          value.WantMerge,
			nextCCUpdated:      value.NXCCUpdated,
			nextCatchainSeqno:  value.NextCatchainSeqNo,
			nextValidatorShard: value.NextValidatorShard,
			minRefMCSeqno:      value.MinRefMcSeqNo,
			splitMerge:         value.SplitMergeAt,
			fees:               value.FeesCollected,
			created:            value.FundsCreated,
		}
	default:
		return shardDescriptorFields{}, fmt.Errorf("unknown descriptor tag %x", magic)
	}
	if loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		return shardDescriptorFields{}, fmt.Errorf("descriptor has %d trailing bits and %d refs",
			loader.BitsLeft(), loader.RefsNum())
	}
	if fields.flags != 0 {
		return shardDescriptorFields{}, fmt.Errorf("descriptor flags are %d, want 0", fields.flags)
	}
	if fields.beforeSplit && fields.beforeMerge {
		return shardDescriptorFields{}, fmt.Errorf("descriptor is both before split and before merge")
	}
	if len(fields.rootHash) != 32 || len(fields.fileHash) != 32 {
		return shardDescriptorFields{}, fmt.Errorf("descriptor block hashes are not 256 bits")
	}
	switch fields.splitMerge.(type) {
	case tlb.FutureSplitMergeNone, tlb.FutureSplit, tlb.FutureMerge:
	default:
		return shardDescriptorFields{}, fmt.Errorf("unsupported split/merge state %T", fields.splitMerge)
	}
	return fields, nil
}

func decodeShardTop(top ShardTop) (decodedShardTop, error) {
	if err := validateShardRegistryBlock(top.Block); err != nil {
		return decodedShardTop{}, fmt.Errorf("block: %w", err)
	}
	leaf, err := decodeShardRegistryLeaf(top.Block.Workchain, top.Block.Shard, top.Descriptor)
	if err != nil {
		return decodedShardTop{}, err
	}
	if !sameShardBlock(leaf.top.Block, top.Block) {
		return decodedShardTop{}, fmt.Errorf("descriptor block differs from declared block")
	}
	leaf.top.Block = *top.Block.Copy()
	leaf.top.Descriptor = top.Descriptor

	predecessors := make([]ton.BlockIDExt, len(top.Predecessors))
	for i := range top.Predecessors {
		if err = validateShardRegistryBlock(top.Predecessors[i]); err != nil {
			return decodedShardTop{}, fmt.Errorf("predecessor %d: %w", i, err)
		}
		if top.Predecessors[i].Workchain != top.Block.Workchain {
			return decodedShardTop{}, fmt.Errorf("predecessor %d belongs to workchain %d, want %d",
				i, top.Predecessors[i].Workchain, top.Block.Workchain)
		}
		predecessors[i] = *top.Predecessors[i].Copy()
	}

	item := decodedShardTop{leaf: leaf, predecessor: predecessors}
	switch len(predecessors) {
	case 1:
		switch {
		case predecessors[0].Shard == top.Block.Shard:
			item.kind = shardTransitionLinear
		case shard.IsDirectChild(predecessors[0].Shard, top.Block.Shard):
			item.kind = shardTransitionSplit
		default:
			return decodedShardTop{}, fmt.Errorf("single predecessor is neither the same shard nor its direct parent")
		}
	case 2:
		left, err := shard.Child(top.Block.Shard, true)
		if err != nil {
			return decodedShardTop{}, fmt.Errorf("derive merge left child: %w", err)
		}
		right, err := shard.Child(top.Block.Shard, false)
		if err != nil {
			return decodedShardTop{}, fmt.Errorf("derive merge right child: %w", err)
		}
		first := predecessors[0].Shard
		second := predecessors[1].Shard
		if !((first == left && second == right) || (first == right && second == left)) {
			return decodedShardTop{}, fmt.Errorf("merge predecessors are not the target's direct children")
		}
		if uint64(predecessors[0].Shard) > uint64(predecessors[1].Shard) {
			predecessors[0], predecessors[1] = predecessors[1], predecessors[0]
			item.predecessor = predecessors
		}
		item.kind = shardTransitionMerge
	default:
		return decodedShardTop{}, fmt.Errorf("predecessor count is %d, want 1 or 2", len(predecessors))
	}
	return item, nil
}

func planShardRegistryTransitions(decoded []decodedShardTop) ([]shardRegistryTransition, error) {
	sort.Slice(decoded, func(i, j int) bool {
		return shardRegistryKeyLess(shardTopKey(decoded[i].leaf.top.Block), shardTopKey(decoded[j].leaf.top.Block))
	})

	targets := make(map[shardRegistryKey]struct{}, len(decoded))
	splits := make(map[shardRegistryKey][]decodedShardTop)
	transitions := make([]shardRegistryTransition, 0, len(decoded))
	for _, item := range decoded {
		target := shardTopKey(item.leaf.top.Block)
		if _, duplicate := targets[target]; duplicate {
			return nil, fmt.Errorf("duplicate target %s", formatShardRegistryKey(target))
		}
		targets[target] = struct{}{}

		switch item.kind {
		case shardTransitionSplit:
			parent := shardTopKey(item.predecessor[0])
			splits[parent] = append(splits[parent], item)
		case shardTransitionLinear:
			transitions = append(transitions, shardRegistryTransition{
				kind:     shardTransitionLinear,
				outputs:  []shardRegistryLeaf{item.leaf},
				claims:   item.predecessor,
				consumes: []shardRegistryKey{shardTopKey(item.predecessor[0])},
			})
		case shardTransitionMerge:
			transitions = append(transitions, shardRegistryTransition{
				kind:    shardTransitionMerge,
				outputs: []shardRegistryLeaf{item.leaf},
				claims:  item.predecessor,
				consumes: []shardRegistryKey{
					shardTopKey(item.predecessor[0]),
					shardTopKey(item.predecessor[1]),
				},
			})
		}
	}

	splitParents := sortedShardRegistryKeys(splits)
	for _, parent := range splitParents {
		pair := splits[parent]
		if len(pair) != 2 {
			return nil, fmt.Errorf("split parent %s has %d child tips, want 2", formatShardRegistryKey(parent), len(pair))
		}
		left, err := shard.Child(parent.shard, true)
		if err != nil {
			return nil, fmt.Errorf("derive split children for %s: %w", formatShardRegistryKey(parent), err)
		}
		right, err := shard.Child(parent.shard, false)
		if err != nil {
			return nil, fmt.Errorf("derive split children for %s: %w", formatShardRegistryKey(parent), err)
		}
		if pair[0].leaf.top.Block.Shard != left || pair[1].leaf.top.Block.Shard != right {
			return nil, fmt.Errorf("split parent %s does not have the exact left/right tips", formatShardRegistryKey(parent))
		}
		transitions = append(transitions, shardRegistryTransition{
			kind:    shardTransitionSplit,
			outputs: []shardRegistryLeaf{pair[0].leaf, pair[1].leaf},
			claims: []ton.BlockIDExt{
				pair[0].predecessor[0],
				pair[1].predecessor[0],
			},
			consumes: []shardRegistryKey{parent},
		})
	}

	sort.Slice(transitions, func(i, j int) bool {
		return shardRegistryKeyLess(shardTopKey(transitions[i].outputs[0].top.Block),
			shardTopKey(transitions[j].outputs[0].top.Block))
	})
	consumed := make(map[shardRegistryKey]struct{})
	for _, transition := range transitions {
		for _, key := range transition.consumes {
			if _, conflict := consumed[key]; conflict {
				return nil, fmt.Errorf("predecessor %s is consumed by conflicting transitions",
					formatShardRegistryKey(key))
			}
			consumed[key] = struct{}{}
		}
	}
	return transitions, nil
}

func verifyShardRegistryTransition(
	transition *shardRegistryTransition,
	current map[shardRegistryKey]shardRegistryLeaf,
	ltLimit uint64,
) error {
	for _, claim := range transition.claims {
		key := shardTopKey(claim)
		leaf, exists := current[key]
		if !exists {
			return fmt.Errorf("predecessor %s is absent", formatShardRegistryKey(key))
		}
		if !sameShardBlock(leaf.top.Block, claim) {
			return fmt.Errorf("predecessor %s is stale", formatShardRegistryKey(key))
		}
	}

	oldCatchainSeqno := uint32(0)
	for _, key := range transition.consumes {
		oldCatchainSeqno = max(oldCatchainSeqno, current[key].fields.nextCatchainSeqno)
	}
	if transition.kind == shardTransitionMerge {
		if oldCatchainSeqno == math.MaxUint32 {
			return fmt.Errorf("merge catchain seqno overflows")
		}
		oldCatchainSeqno++
	}

	for i := range transition.outputs {
		output := &transition.outputs[i]
		fields := output.fields
		if fields.nextValidatorShard != output.top.Block.Shard {
			return fmt.Errorf("output %s has next validator shard %016x",
				formatShardRegistryKey(shardTopKey(output.top.Block)), uint64(fields.nextValidatorShard))
		}
		if fields.nextCatchainSeqno != oldCatchainSeqno {
			return fmt.Errorf("output %s has catchain seqno %d, want %d",
				formatShardRegistryKey(shardTopKey(output.top.Block)), fields.nextCatchainSeqno, oldCatchainSeqno)
		}
		if fields.endLT >= ltLimit {
			return fmt.Errorf("output %s has end lt %d, current limit is %d",
				formatShardRegistryKey(shardTopKey(output.top.Block)), fields.endLT, ltLimit)
		}
	}

	switch transition.kind {
	case shardTransitionLinear:
		currentLeaf := current[transition.consumes[0]]
		if currentLeaf.fields.beforeSplit || currentLeaf.fields.beforeMerge {
			return fmt.Errorf("linear predecessor %s is marked for a topology transition",
				formatShardRegistryKey(transition.consumes[0]))
		}
		if err := verifyShardBeforeSplit(currentLeaf.fields, transition.outputs[0].fields,
			transition.outputs[0], false); err != nil {
			return err
		}
		// inheritShardRegistryFSM rewrites outputs[0].fields, so it stays last:
		// the descriptor fields above are read as arguments before that write.
		if err := inheritShardRegistryFSM(&transition.outputs[0],
			transition.outputs[0].fields, currentLeaf.fields); err != nil {
			return err
		}
	case shardTransitionSplit:
		currentLeaf := current[transition.consumes[0]]
		if !currentLeaf.fields.beforeSplit {
			return fmt.Errorf("split predecessor %s is not marked before split",
				formatShardRegistryKey(transition.consumes[0]))
		}
		// The predecessor flags have to match the transition exactly, not just the
		// one that names it.
		if currentLeaf.fields.beforeMerge {
			return fmt.Errorf("split predecessor %s is also marked before merge",
				formatShardRegistryKey(transition.consumes[0]))
		}
		for _, output := range transition.outputs {
			if output.fields.beforeSplit {
				return fmt.Errorf("after-split output %s is itself marked before split",
					formatShardRegistryKey(shardTopKey(output.top.Block)))
			}
			if err := verifyShardBeforeSplit(currentLeaf.fields, output.fields, output, true); err != nil {
				return err
			}
		}
	case shardTransitionMerge:
		for _, key := range transition.consumes {
			predecessor := current[key]
			if !predecessor.fields.beforeMerge {
				return fmt.Errorf("merge predecessor %s is not marked before merge", formatShardRegistryKey(key))
			}
			if predecessor.fields.beforeSplit {
				return fmt.Errorf("merge predecessor %s is also marked before split", formatShardRegistryKey(key))
			}
			if err := verifyShardMergeWindow(predecessor.fields, transition.outputs[0].fields); err != nil {
				return fmt.Errorf("merge predecessor %s: %w", formatShardRegistryKey(key), err)
			}
		}
		if transition.outputs[0].fields.beforeSplit {
			return fmt.Errorf("after-merge output %s is marked before split",
				formatShardRegistryKey(shardTopKey(transition.outputs[0].top.Block)))
		}
	}
	return nil
}

func verifyShardBeforeSplit(
	predecessor shardDescriptorFields,
	outputFields shardDescriptorFields,
	output shardRegistryLeaf,
	topologySplit bool,
) error {
	if !outputFields.beforeSplit {
		return nil
	}
	if topologySplit {
		return fmt.Errorf("after-split output %s is itself marked before split",
			formatShardRegistryKey(shardTopKey(output.top.Block)))
	}

	split, ok := predecessor.splitMerge.(tlb.FutureSplit)
	if !ok {
		return fmt.Errorf("before-split output %s has no predecessor split announcement",
			formatShardRegistryKey(shardTopKey(output.top.Block)))
	}
	if !masterShardTimeInside(outputFields.genUtime, split.SplitUtime, split.Interval) {
		return fmt.Errorf("before-split output %s is outside predecessor split interval",
			formatShardRegistryKey(shardTopKey(output.top.Block)))
	}
	return nil
}

func verifyShardMergeWindow(predecessor, output shardDescriptorFields) error {
	merge, ok := predecessor.splitMerge.(tlb.FutureMerge)
	if !ok {
		return fmt.Errorf("predecessor has no merge announcement")
	}
	if !masterShardTimeInside(output.genUtime, merge.MergeUtime, merge.Interval) {
		return fmt.Errorf("output generation time is outside predecessor merge interval")
	}
	return nil
}

func inheritShardRegistryFSM(
	output *shardRegistryLeaf,
	fields shardDescriptorFields,
	predecessor shardDescriptorFields,
) error {
	if masterShardFSMNone(predecessor.splitMerge) {
		return nil
	}

	fields.splitMerge = predecessor.splitMerge
	tag, err := loadMasterShardDescriptorTag(output.top.Descriptor)
	if err != nil {
		return err
	}
	descriptor, err := storeMasterShardDescriptor(tag, fields)
	if err != nil {
		return err
	}
	output.top.Descriptor = descriptor
	output.fields = fields
	return nil
}

func masterShardTimeInside(now, start, interval uint32) bool {
	end := uint64(start) + uint64(interval)
	return end <= math.MaxUint32 && uint64(now) >= uint64(start) && uint64(now) < end
}

// sortedShardRegistryKeys orders the keys of any registry-keyed map. Map keys
// are unique, so the comparator is a strict total order over them.
func sortedShardRegistryKeys[V any](items map[shardRegistryKey]V) []shardRegistryKey {
	keys := make([]shardRegistryKey, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return shardRegistryKeyLess(keys[i], keys[j]) })

	return keys
}

func buildShardHashesDictionary(leaves map[shardRegistryKey]shardRegistryLeaf) (*cell.Dictionary, error) {
	byWorkchain := make(map[int32][]shardRegistryLeaf)
	for _, leaf := range leaves {
		workchain := leaf.top.Block.Workchain
		byWorkchain[workchain] = append(byWorkchain[workchain], leaf)
	}
	workchains := slices.Sorted(maps.Keys(byWorkchain))

	result := cell.NewDict(masterShardHashesKeyBits)
	for _, workchain := range workchains {
		workchainLeaves := byWorkchain[workchain]
		sort.Slice(workchainLeaves, func(i, j int) bool {
			return uint64(workchainLeaves[i].top.Block.Shard) < uint64(workchainLeaves[j].top.Block.Shard)
		})
		tree, err := buildWorkchainShardTree(shard.Root, workchainLeaves)
		if err != nil {
			return nil, fmt.Errorf("workchain %d: %w", workchain, err)
		}
		key := cell.BeginCell().MustStoreInt(int64(workchain), masterShardHashesKeyBits).EndCell()
		value := cell.BeginCell()
		if err = value.StoreRef(tree); err != nil {
			return nil, fmt.Errorf("store workchain %d tree reference: %w", workchain, err)
		}
		if err = result.Set(key, value.EndCell()); err != nil {
			return nil, fmt.Errorf("store workchain %d tree: %w", workchain, err)
		}
	}
	return result, nil
}

func buildWorkchainShardTree(shardID int64, leaves []shardRegistryLeaf) (*cell.Cell, error) {
	if len(leaves) == 0 {
		return nil, fmt.Errorf("shard %016x has no leaves", uint64(shardID))
	}
	if len(leaves) == 1 && leaves[0].top.Block.Shard == shardID {
		leaf := cell.BeginCell()
		if err := leaf.StoreBoolBit(false); err != nil {
			return nil, fmt.Errorf("store shard %016x leaf tag: %w", uint64(shardID), err)
		}
		if err := leaf.StoreBuilder(leaves[0].top.Descriptor.ToBuilder()); err != nil {
			return nil, fmt.Errorf("store shard %016x descriptor: %w", uint64(shardID), err)
		}
		return leaf.EndCell(), nil
	}
	for _, leaf := range leaves {
		if leaf.top.Block.Shard == shardID {
			return nil, fmt.Errorf("shard %016x is both a leaf and an ancestor", uint64(shardID))
		}
	}

	leftID, err := shard.Child(shardID, true)
	if err != nil {
		return nil, fmt.Errorf("split shard %016x: %w", uint64(shardID), err)
	}
	rightID, err := shard.Child(shardID, false)
	if err != nil {
		return nil, fmt.Errorf("split shard %016x: %w", uint64(shardID), err)
	}
	leftLeaves := make([]shardRegistryLeaf, 0, len(leaves))
	rightLeaves := make([]shardRegistryLeaf, 0, len(leaves))
	for _, leaf := range leaves {
		switch {
		case shard.Contains(leftID, leaf.top.Block.Shard):
			leftLeaves = append(leftLeaves, leaf)
		case shard.Contains(rightID, leaf.top.Block.Shard):
			rightLeaves = append(rightLeaves, leaf)
		default:
			return nil, fmt.Errorf("leaf %016x is outside shard %016x",
				uint64(leaf.top.Block.Shard), uint64(shardID))
		}
	}
	if len(leftLeaves) == 0 || len(rightLeaves) == 0 {
		return nil, fmt.Errorf("shard %016x has an incomplete child partition", uint64(shardID))
	}
	left, err := buildWorkchainShardTree(leftID, leftLeaves)
	if err != nil {
		return nil, err
	}
	right, err := buildWorkchainShardTree(rightID, rightLeaves)
	if err != nil {
		return nil, err
	}
	fork := cell.BeginCell()
	if err = fork.StoreBoolBit(true); err != nil {
		return nil, fmt.Errorf("store shard %016x fork tag: %w", uint64(shardID), err)
	}
	if err = fork.StoreRef(left); err != nil {
		return nil, fmt.Errorf("store shard %016x left branch: %w", uint64(shardID), err)
	}
	if err = fork.StoreRef(right); err != nil {
		return nil, fmt.Errorf("store shard %016x right branch: %w", uint64(shardID), err)
	}
	return fork.EndCell(), nil
}

func buildShardFeesDictionary(leaves map[shardRegistryKey]shardRegistryLeaf) (*tlb.ShardFeesAugDict, error) {
	fees, err := tlb.NewShardFeesAugDict()
	if err != nil {
		return nil, err
	}
	if fees.GetKeySize() != masterShardFeesKeyBits {
		return nil, fmt.Errorf("ShardFees key size is %d, want %d", fees.GetKeySize(), masterShardFeesKeyBits)
	}
	for _, leaf := range sortedShardRegistryLeaves(leaves) {
		key := cell.BeginCell().
			MustStoreInt(int64(leaf.top.Block.Workchain), 32).
			MustStoreUInt(uint64(leaf.top.Block.Shard), 64).
			EndCell()
		value, err := tlb.ToCell(tlb.ShardFeeCreated{
			Fees:   leaf.fields.fees,
			Create: leaf.fields.created,
		})
		if err != nil {
			return nil, fmt.Errorf("serialize fees for %s: %w", formatShardRegistryKey(shardTopKey(leaf.top.Block)), err)
		}
		if err = fees.Set(key, value); err != nil {
			return nil, fmt.Errorf("store fees for %s: %w", formatShardRegistryKey(shardTopKey(leaf.top.Block)), err)
		}
	}
	return fees, nil
}

func sortedShardRegistryLeaves(leaves map[shardRegistryKey]shardRegistryLeaf) []shardRegistryLeaf {
	result := make([]shardRegistryLeaf, 0, len(leaves))
	for _, leaf := range leaves {
		result = append(result, leaf)
	}
	sort.Slice(result, func(i, j int) bool {
		return shardRegistryKeyLess(shardTopKey(result[i].top.Block), shardTopKey(result[j].top.Block))
	})
	return result
}

func cloneShardRegistryLeaves(source map[shardRegistryKey]shardRegistryLeaf) map[shardRegistryKey]shardRegistryLeaf {
	cloned := make(map[shardRegistryKey]shardRegistryLeaf, len(source))
	for key, leaf := range source {
		cloned[key] = leaf
	}
	return cloned
}

func validateShardRegistryBlock(block ton.BlockIDExt) error {
	if block.Workchain == masterchainWorkchainID || block.Workchain == invalidWorkchainID {
		return fmt.Errorf("invalid workchain %d", block.Workchain)
	}
	if err := shard.Validate(block.Shard); err != nil {
		return fmt.Errorf("invalid shard %016x: %w", uint64(block.Shard), err)
	}
	if len(block.RootHash) != 32 || len(block.FileHash) != 32 {
		return fmt.Errorf("block hashes are not 256 bits")
	}
	return nil
}

func sameShardBlock(left, right ton.BlockIDExt) bool {
	return left.Workchain == right.Workchain && left.Shard == right.Shard && left.SeqNo == right.SeqNo &&
		bytes.Equal(left.RootHash, right.RootHash) && bytes.Equal(left.FileHash, right.FileHash)
}

func sameDictionaryRoot(left, right *cell.Dictionary) bool {
	leftRoot, leftErr := left.ToCell()
	rightRoot, rightErr := right.ToCell()
	if leftErr != nil || rightErr != nil {
		return false
	}
	if leftRoot == nil || rightRoot == nil {
		return leftRoot == nil && rightRoot == nil
	}
	return leftRoot.HashKey() == rightRoot.HashKey()
}

func shardTopKey(block ton.BlockIDExt) shardRegistryKey {
	return shardRegistryKey{workchain: block.Workchain, shard: block.Shard}
}

func shardRegistryKeyLess(left, right shardRegistryKey) bool {
	if left.workchain != right.workchain {
		return left.workchain < right.workchain
	}
	return uint64(left.shard) < uint64(right.shard)
}

func formatShardRegistryKey(key shardRegistryKey) string {
	return fmt.Sprintf("%d:%016x", key.workchain, uint64(key.shard))
}
