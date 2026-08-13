package collator

import (
	"bytes"
	"container/list"
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/shard"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	defaultShardTopInboxEntries      = 4096
	defaultShardTopInboxEntriesShard = 32
	shardTopSameTimestampVersion     = 13
)

// ShardTopValidatorSet identifies the validator set accepted for a shard in a
// particular masterchain view.
type ShardTopValidatorSet struct {
	CatchainSeqno    uint32
	ValidatorSetHash uint32
}

// ShardTopValidatorSets distinguishes the current set from the next set. A
// descriptor signed by the next set stays in the inbox but is not selected
// until that set becomes current.
type ShardTopValidatorSets struct {
	Current ShardTopValidatorSet
	Next    ShardTopValidatorSet
	HasNext bool
}

// ShardTopSelectionProvider supplies checks that must be evaluated against the
// same masterchain view as ShardTopSelection. ShardTopReady is where storage
// implementations preload and verify the exact block states and queue data
// needed by the latest top; false keeps the previous ready top available.
type ShardTopSelectionProvider interface {
	ValidatorSets(context.Context, ton.BlockIDExt, ton.BlockIDExt) (ShardTopValidatorSets, error)
	IsMasterchainAncestor(context.Context, ton.BlockIDExt, ton.BlockIDExt) (bool, error)
	ShardTopReady(context.Context, ton.BlockIDExt) (bool, error)
}

// ShardTopSelection is one coherent masterchain view used to choose shard tops.
// Select never mutates Registry.
type ShardTopSelection struct {
	Masterchain   ton.BlockIDExt
	VertSeqno     uint32
	GenUtime      uint32
	GlobalVersion uint32
	LTLimit       uint64
	Registry      *ShardRegistry
	Provider      ShardTopSelectionProvider
}

type ShardTopInboxOptions struct {
	MaxEntries         int
	MaxEntriesPerShard int
}

// ShardTopInbox retains fully verified TopBlockDescr cells in memory. Each
// (result shard, catchain) keeps the strictly highest received descriptor and
// the highest descriptor whose queue data is ready — the latest/ready pair the
// protocol's descriptor state machine is defined over.
type ShardTopInbox struct {
	mu sync.Mutex

	maxEntries         int
	maxEntriesPerShard int
	entries            map[cell.Hash]*shardTopInboxEntry
	shardEntries       map[shardRegistryKey]int
	groups             map[shardTopInboxGroupKey]*shardTopInboxGroup
	order              list.List
}

type shardTopInboxEntry struct {
	key         cell.Hash
	description *p2p.ShardBlockDescription
	root        *cell.Cell
	shard       shardRegistryKey
	group       shardTopInboxGroupKey
	checking    bool
	order       *list.Element
}

type shardTopInboxGroupKey struct {
	shard         shardRegistryKey
	catchainSeqno uint32
}

type shardTopInboxGroup struct {
	latest *shardTopInboxEntry
	ready  *shardTopInboxEntry
}

type shardTopInboxSnapshot struct {
	key         cell.Hash
	group       shardTopInboxGroupKey
	description *p2p.ShardBlockDescription
	root        *cell.Cell
}

type shardTopDisposition uint8

const (
	shardTopDispositionDeferred shardTopDisposition = iota
	shardTopDispositionDiscarded
	shardTopDispositionCandidate
)

type projectedShardTop struct {
	key   cell.Hash
	group shardTopInboxGroupKey
	top   ShardTop
}

type shardTopProjection struct {
	top         ShardTop
	disposition shardTopDisposition
}

func NewShardTopInbox(options ShardTopInboxOptions) (*ShardTopInbox, error) {
	if options.MaxEntries < 0 {
		return nil, fmt.Errorf("%w: shard top inbox entry limit is negative", ErrInvalidInput)
	}
	if options.MaxEntriesPerShard < 0 {
		return nil, fmt.Errorf("%w: shard top inbox per-shard limit is negative", ErrInvalidInput)
	}
	if options.MaxEntries == 0 {
		options.MaxEntries = defaultShardTopInboxEntries
	}
	if options.MaxEntriesPerShard == 0 {
		options.MaxEntriesPerShard = defaultShardTopInboxEntriesShard
	}

	return &ShardTopInbox{
		maxEntries:         options.MaxEntries,
		maxEntriesPerShard: options.MaxEntriesPerShard,
		entries:            make(map[cell.Hash]*shardTopInboxEntry, options.MaxEntries),
		shardEntries:       make(map[shardRegistryKey]int),
		groups:             make(map[shardTopInboxGroupKey]*shardTopInboxGroup),
	}, nil
}

// StoreShardTopDescription retains the exact outer root and a private semantic
// snapshot. Per-link proof fields are discarded after binding because the
// immutable outer root already owns the complete proof graph.
func (i *ShardTopInbox) StoreShardTopDescription(
	ctx context.Context,
	description *p2p.ShardBlockDescription,
	root *cell.Cell,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if description == nil {
		return fmt.Errorf("%w: shard top description is nil", ErrInvalidInput)
	}
	if root == nil {
		return fmt.Errorf("%w: shard top description root is nil", ErrInvalidInput)
	}
	shardKey := shardTopKey(description.Block)
	groupKey := shardTopInboxGroupKey{shard: shardKey, catchainSeqno: description.CatchainSeqno}

	i.mu.Lock()
	group := i.groups[groupKey]
	if group != nil && description.Block.SeqNo <= group.latest.description.Block.SeqNo {
		i.mu.Unlock()

		return nil
	}
	i.mu.Unlock()

	proofs, err := validateMasterTopBlockDescrBinding(root, description.Block, uint32(len(description.Chain)))
	if err != nil {
		return fmt.Errorf("%w: bind shard top description: %v", ErrInvalidInput, err)
	}
	if len(proofs) != len(description.Chain) {
		return fmt.Errorf("%w: shard top proof count is %d, want %d", ErrInvalidInput, len(proofs), len(description.Chain))
	}
	for index := range proofs {
		proof := description.Chain[index].TopBlockProof
		if proof == nil || proofs[index].HashKey() != proof.HashKey() {
			return fmt.Errorf("%w: shard top proof %d is not the proof decoded from the exact descriptor root", ErrInvalidInput, index)
		}
	}

	stored, err := cloneInboxShardBlockDescription(description)
	if err != nil {
		return fmt.Errorf("%w: clone shard top description: %v", ErrInvalidInput, err)
	}

	key := root.HashKey()

	i.mu.Lock()
	defer i.mu.Unlock()

	group = i.groups[groupKey]
	if group != nil && description.Block.SeqNo <= group.latest.description.Block.SeqNo {
		// The first installed descriptor wins at equal height. Besides being the
		// canonical rule, this prevents a same-seq fork from replacing a
		// descriptor while its queue readiness check is in flight.
		return nil
	}
	if existing := i.entries[key]; existing != nil {
		return fmt.Errorf("%w: shard top descriptor root belongs to another inbox key", ErrInvalidInput)
	}

	entry := &shardTopInboxEntry{
		key:         key,
		description: stored,
		root:        root,
		shard:       shardKey,
		group:       groupKey,
	}
	entry.order = i.order.PushBack(key)
	i.entries[key] = entry
	i.shardEntries[shardKey]++
	if group == nil {
		group = &shardTopInboxGroup{}
		i.groups[groupKey] = group
	}
	previousLatest := group.latest
	group.latest = entry
	if previousLatest != nil && previousLatest != group.ready && !previousLatest.checking {
		i.removeLocked(previousLatest.key, previousLatest)
	}

	i.pruneShardOverflowLocked(shardKey)
	for len(i.entries) > i.maxEntries {
		i.removeOldestLocked()
	}

	return nil
}

// Select returns a deterministic batch that ShardRegistry can atomically apply
// to the supplied view. Provider calls and proof readiness checks run without
// holding the inbox mutex.
func (i *ShardTopInbox) Select(ctx context.Context, input ShardTopSelection) ([]ShardTop, error) {
	if err := validateShardTopSelection(input); err != nil {
		return nil, err
	}

	latest := i.latestSnapshots()
	materialized := make(map[cell.Hash]struct{}, len(latest))
	projections := make(map[cell.Hash]shardTopProjection, len(latest))
	for _, snapshot := range latest {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !i.beginReadyCheck(snapshot) {
			continue
		}
		ready, err := input.Provider.ShardTopReady(ctx, snapshot.description.Block)
		if err != nil {
			i.finishReadyCheck(snapshot, false)
			block := snapshot.description.Block

			return nil, fmt.Errorf("check shard top %d:%016x:%d readiness: %w",
				block.Workchain, uint64(block.Shard), block.SeqNo, err)
		}
		if !ready {
			i.finishReadyCheck(snapshot, false)

			continue
		}
		top, disposition, projectErr := projectShardTop(ctx, input, snapshot)
		if projectErr != nil {
			i.finishReadyCheck(snapshot, false)

			return nil, projectErr
		}
		// Validity is re-checked after the queue preload. A permanently invalid
		// higher descriptor must not evict the lower ready one; a merely deferred
		// descriptor is valid and becomes the sole ready.
		i.finishReadyCheck(snapshot, disposition != shardTopDispositionDiscarded)
		if disposition == shardTopDispositionDiscarded {
			i.discardInvalidLatest(snapshot)

			continue
		}
		materialized[snapshot.key] = struct{}{}
		projections[snapshot.key] = shardTopProjection{top: top, disposition: disposition}
	}

	snapshots := i.readySnapshots()
	projected := make([]projectedShardTop, 0, len(snapshots))
	discarded := make([]cell.Hash, 0)
	for _, snapshot := range snapshots {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, loaded := materialized[snapshot.key]; !loaded {
			// ready_desc is persistent inbox state, while the provider is scoped
			// to this masterchain build. This materializes the exact block state
			// and queue view in the request-local provider. A temporary miss blocks
			// the ready descriptor without demoting it or exposing a fallback.
			ready, err := input.Provider.ShardTopReady(ctx, snapshot.description.Block)
			if err != nil {
				block := snapshot.description.Block

				return nil, fmt.Errorf("materialize ready shard top %d:%016x:%d: %w",
					block.Workchain, uint64(block.Shard), block.SeqNo, err)
			}
			if !ready {
				continue
			}
		}

		projection, hasProjection := projections[snapshot.key]
		if !hasProjection {
			top, disposition, err := projectShardTop(ctx, input, snapshot)
			if err != nil {
				return nil, err
			}
			projection = shardTopProjection{top: top, disposition: disposition}
		}
		switch projection.disposition {
		case shardTopDispositionDiscarded:
			discarded = append(discarded, snapshot.key)
		case shardTopDispositionCandidate:
			projected = append(projected, projectedShardTop{
				key:   snapshot.key,
				group: snapshot.group,
				top:   projection.top,
			})
		}
	}

	i.discard(discarded)
	projected = i.currentReady(projected)
	selected, err := selectShardTopTransitions(input.Registry, projected, input.LTLimit)
	if err != nil {
		return nil, err
	}

	return selected, nil
}

func (i *ShardTopInbox) Len() int {
	i.mu.Lock()
	defer i.mu.Unlock()

	return len(i.entries)
}

func validateShardTopSelection(input ShardTopSelection) error {
	if err := validateShardTopView(input); err != nil {
		return err
	}
	if input.LTLimit == 0 {
		return fmt.Errorf("%w: shard top selection lt limit is zero", ErrInvalidInput)
	}

	return nil
}

func validateShardTopView(input ShardTopSelection) error {
	if input.Masterchain.Workchain != -1 || input.Masterchain.Shard != shard.Root {
		return fmt.Errorf("%w: shard top selection masterchain head has invalid shard", ErrInvalidInput)
	}
	if len(input.Masterchain.RootHash) != 32 || len(input.Masterchain.FileHash) != 32 {
		return fmt.Errorf("%w: shard top selection masterchain head has invalid hashes", ErrInvalidInput)
	}
	if input.Registry == nil {
		return fmt.Errorf("%w: shard top selection registry is nil", ErrInvalidInput)
	}
	if input.Provider == nil {
		return fmt.Errorf("%w: shard top selection provider is nil", ErrInvalidInput)
	}

	return nil
}

func projectShardTop(
	ctx context.Context,
	input ShardTopSelection,
	snapshot shardTopInboxSnapshot,
) (ShardTop, shardTopDisposition, error) {
	description := snapshot.description
	head, disposition := classifyShardTopHeader(input, description)
	if disposition != shardTopDispositionCandidate {
		return ShardTop{}, disposition, nil
	}
	if head.GenUtime > input.GenUtime ||
		(input.GlobalVersion < shardTopSameTimestampVersion && head.GenUtime == input.GenUtime) {
		return ShardTop{}, shardTopDispositionDeferred, nil
	}

	predecessors, chainLength, disposition, err := classifyShardTopTopology(ctx, input, description)
	if err != nil {
		return ShardTop{}, disposition, err
	}
	if disposition != shardTopDispositionCandidate {
		return ShardTop{}, disposition, nil
	}
	if head.EndLT >= input.LTLimit {
		return ShardTop{}, shardTopDispositionDeferred, nil
	}

	disposition, err = classifyShardTopValidatorSet(ctx, input, description)
	if err != nil {
		return ShardTop{}, disposition, err
	}
	if disposition != shardTopDispositionCandidate {
		return ShardTop{}, disposition, nil
	}

	top, err := buildSelectedShardTop(snapshot.root, description, predecessors, chainLength)
	if err != nil {
		return ShardTop{}, shardTopDispositionDiscarded, nil
	}

	return top, shardTopDispositionCandidate, nil
}

func classifyShardTopHeader(
	input ShardTopSelection,
	description *p2p.ShardBlockDescription,
) (*p2p.ShardDescriptionLink, shardTopDisposition) {
	if description == nil || description.Block.Workchain == -1 ||
		len(description.Chain) == 0 || len(description.Chain) > int(maxShardTopChainLength) {
		return nil, shardTopDispositionDiscarded
	}
	head := &description.Chain[0]
	if !sameShardBlock(head.Block, description.Block) || head.VertSeqno < input.VertSeqno {
		return nil, shardTopDispositionDiscarded
	}
	if head.VertSeqno > input.VertSeqno {
		return nil, shardTopDispositionDeferred
	}

	return head, shardTopDispositionCandidate
}

func classifyShardTopTopology(
	ctx context.Context,
	input ShardTopSelection,
	description *p2p.ShardBlockDescription,
) ([]ton.BlockIDExt, int, shardTopDisposition, error) {
	if shardTopReferencesFutureMasterchain(description, input.Masterchain.SeqNo) {
		return nil, 0, shardTopDispositionDeferred, nil
	}

	ancestry, err := validateShardTopMasterchainAncestry(ctx, input, description)
	if err != nil {
		return nil, 0, shardTopDispositionDeferred, err
	}
	if !ancestry {
		return nil, 0, shardTopDispositionDiscarded, nil
	}

	predecessors, chainLength, disposition := shardTopRegistryAnchor(input.Registry, description)
	return predecessors, chainLength, disposition, nil
}

// classifyShardTopValidatorSet has no allow_next_vset mode on purpose: the C++
// collator omits it when importing shard tops (collator.cpp:1286), and only the
// descriptor-acceptance side passes it (top-shard-descr.cpp:271, :573) — here
// that side is service/blockproof/shard_top_validator_context.go. A descriptor
// signed by the next set is therefore always deferred, never selected.
func classifyShardTopValidatorSet(
	ctx context.Context,
	input ShardTopSelection,
	description *p2p.ShardBlockDescription,
) (shardTopDisposition, error) {
	validatorSets, err := input.Provider.ValidatorSets(ctx, input.Masterchain, description.Block)
	if err != nil {
		return shardTopDispositionDeferred, fmt.Errorf(
			"load validator sets for shard %d:%016x: %w",
			description.Block.Workchain,
			uint64(description.Block.Shard),
			err,
		)
	}
	declared := ShardTopValidatorSet{
		CatchainSeqno:    description.CatchainSeqno,
		ValidatorSetHash: description.ValidatorSetHash,
	}
	if declared == validatorSets.Current {
		return shardTopDispositionCandidate, nil
	}
	if validatorSets.HasNext && declared == validatorSets.Next {
		return shardTopDispositionDeferred, nil
	}

	return shardTopDispositionDiscarded, nil
}

func shardTopReferencesFutureMasterchain(description *p2p.ShardBlockDescription, masterchainSeqno uint32) bool {
	for index := range description.Chain {
		ref := description.Chain[index].MasterchainRef
		if ref != nil && ref.SeqNo > masterchainSeqno {
			return true
		}
	}

	return false
}

func validateShardTopMasterchainAncestry(
	ctx context.Context,
	input ShardTopSelection,
	description *p2p.ShardBlockDescription,
) (bool, error) {
	for index := range description.Chain {
		ref := description.Chain[index].MasterchainRef
		if ref == nil || ref.Workchain != -1 || ref.Shard != shard.Root {
			return false, nil
		}
		switch {
		case ref.SeqNo == input.Masterchain.SeqNo:
			if !ref.Equals(&input.Masterchain) {
				return false, nil
			}
		default:
			ancestor, err := input.Provider.IsMasterchainAncestor(ctx, *ref, input.Masterchain)
			if err != nil {
				return false, fmt.Errorf("check masterchain ancestor %d: %w", ref.SeqNo, err)
			}
			if !ancestor {
				return false, nil
			}
		}
	}

	return true, nil
}

func shardTopRegistryAnchor(
	registry *ShardRegistry,
	description *p2p.ShardBlockDescription,
) ([]ton.BlockIDExt, int, shardTopDisposition) {
	anchors := shardTopIntersectingLeaves(registry, description.Block)
	if len(anchors) == 0 {
		return nil, 0, shardTopDispositionDeferred
	}
	if len(anchors) > 2 {
		return nil, 0, shardTopDispositionDiscarded
	}
	if len(anchors) == 2 {
		left, err := shard.Child(description.Block.Shard, true)
		if err != nil || anchors[0].top.Block.Shard != left {
			return nil, 0, shardTopDispositionDiscarded
		}
		right, err := shard.Child(description.Block.Shard, false)
		if err != nil || anchors[1].top.Block.Shard != right {
			return nil, 0, shardTopDispositionDiscarded
		}
	}

	for index := range anchors {
		if anchors[index].top.Block.SeqNo >= description.Block.SeqNo {
			return nil, 0, shardTopDispositionDiscarded
		}
	}

	terminal := &description.Chain[len(description.Chain)-1]
	if len(terminal.PrevRefs) == 0 || len(terminal.PrevRefs) > 2 {
		return nil, 0, shardTopDispositionDiscarded
	}
	if shardTopAnchorBehindPredecessor(anchors, terminal.PrevRefs) {
		return nil, 0, shardTopDispositionDeferred
	}

	maxSeqno := anchors[0].top.Block.SeqNo
	for index := 1; index < len(anchors); index++ {
		maxSeqno = max(maxSeqno, anchors[index].top.Block.SeqNo)
	}
	chainLength := int(description.Block.SeqNo - maxSeqno)
	if chainLength <= 0 {
		return nil, 0, shardTopDispositionDiscarded
	}
	if chainLength > len(description.Chain) {
		return nil, 0, shardTopDispositionDeferred
	}

	if chainLength < len(description.Chain) {
		if len(anchors) != 1 || anchors[0].top.Block.Shard != description.Block.Shard ||
			!sameShardBlock(description.Chain[chainLength].Block, anchors[0].top.Block) {
			return nil, 0, shardTopDispositionDiscarded
		}
		return []ton.BlockIDExt{*description.Chain[chainLength].Block.Copy()}, chainLength, shardTopDispositionCandidate
	}

	if len(terminal.PrevRefs) != len(anchors) {
		return nil, 0, shardTopDispositionDiscarded
	}
	predecessors := make([]ton.BlockIDExt, len(anchors))
	for anchorIndex := range anchors {
		matched := false
		for predecessorIndex := range terminal.PrevRefs {
			if !sameShardBlock(terminal.PrevRefs[predecessorIndex], anchors[anchorIndex].top.Block) {
				continue
			}
			matched = true
			break
		}
		if !matched {
			return nil, 0, shardTopDispositionDiscarded
		}
		predecessors[anchorIndex] = *anchors[anchorIndex].top.Block.Copy()
	}

	return predecessors, chainLength, shardTopDispositionCandidate
}

func shardTopAnchorBehindPredecessor(anchors []shardRegistryLeaf, predecessors []ton.BlockIDExt) bool {
	for predecessorIndex := range predecessors {
		predecessor := &predecessors[predecessorIndex]
		for anchorIndex := range anchors {
			anchor := &anchors[anchorIndex].top.Block
			if anchor.Shard == predecessor.Shard && anchor.SeqNo < predecessor.SeqNo {
				return true
			}
		}
	}

	return false
}

func shardTopIntersectingLeaves(registry *ShardRegistry, block ton.BlockIDExt) []shardRegistryLeaf {
	result := make([]shardRegistryLeaf, 0, 2)
	for key, leaf := range registry.leaves {
		if key.workchain == block.Workchain && shard.Intersects(key.shard, block.Shard) {
			result = append(result, leaf)
		}
	}
	sort.Slice(result, func(left, right int) bool {
		return uint64(result[left].top.Block.Shard) < uint64(result[right].top.Block.Shard)
	})

	return result
}

func buildSelectedShardTop(
	root *cell.Cell,
	description *p2p.ShardBlockDescription,
	predecessors []ton.BlockIDExt,
	chainLength int,
) (ShardTop, error) {
	head := &description.Chain[0]
	fees := tlb.CurrencyCollection{}
	created := tlb.CurrencyCollection{}
	for index := 0; index < chainLength; index++ {
		var err error
		fees, err = fees.Add(description.Chain[index].FeesCollected)
		if err != nil {
			return ShardTop{}, fmt.Errorf("sum collected fees: %w", err)
		}
		created, err = created.Add(description.Chain[index].FundsCreated)
		if err != nil {
			return ShardTop{}, fmt.Errorf("sum created funds: %w", err)
		}
	}

	descriptor, err := storeMasterShardDescriptor(0xa, shardDescriptorFields{
		seqno:              description.Block.SeqNo,
		startLT:            head.StartLT,
		endLT:              head.EndLT,
		genUtime:           head.GenUtime,
		rootHash:           bytes.Clone(description.Block.RootHash),
		fileHash:           bytes.Clone(description.Block.FileHash),
		beforeSplit:        head.BeforeSplit,
		wantSplit:          head.WantSplit,
		wantMerge:          head.WantMerge,
		nextCatchainSeqno:  description.CatchainSeqno,
		nextValidatorShard: description.Block.Shard,
		minRefMCSeqno:      head.MinRefMCSeqno,
		splitMerge:         tlb.FutureSplitMergeNone{},
		fees:               fees,
		created:            created,
	})
	if err != nil {
		return ShardTop{}, err
	}

	creators := make([][32]byte, 0, chainLength)
	for index := chainLength - 1; index >= 0; index-- {
		creators = append(creators, description.Chain[index].CreatedBy)
	}

	return ShardTop{
		Block:         *description.Block.Copy(),
		Predecessors:  predecessors,
		Descriptor:    descriptor,
		TopBlockDescr: root,
		Creators:      creators,
	}, nil
}

type shardTopTransitionGroupKey struct {
	kind      shardTransitionKind
	workchain int32
	first     int64
	second    int64
}

type shardTopTransitionGroup struct {
	key        shardTopTransitionGroupKey
	output     shardRegistryKey
	candidates []projectedShardTop
	left       []projectedShardTop
	right      []projectedShardTop
}

func selectShardTopTransitions(
	registry *ShardRegistry,
	candidates []projectedShardTop,
	ltLimit uint64,
) ([]ShardTop, error) {
	sort.Slice(candidates, func(left, right int) bool {
		return shardTopCandidateLess(candidates[left], candidates[right])
	})

	groups := make(map[shardTopTransitionGroupKey]*shardTopTransitionGroup)
	for _, candidate := range candidates {
		decoded, err := decodeShardTop(candidate.top)
		if err != nil {
			continue
		}
		key := shardTopGroupKey(decoded)
		group := groups[key]
		if group == nil {
			group = &shardTopTransitionGroup{
				key:    key,
				output: shardTopKey(decoded.leaf.top.Block),
			}
			groups[key] = group
		}
		if decoded.kind != shardTransitionSplit {
			group.candidates = append(group.candidates, candidate)
			continue
		}

		left, err := shard.Child(decoded.predecessor[0].Shard, true)
		if err != nil {
			continue
		}
		if decoded.leaf.top.Block.Shard == left {
			group.left = append(group.left, candidate)
		} else {
			group.right = append(group.right, candidate)
		}
	}

	ordered := make([]*shardTopTransitionGroup, 0, len(groups))
	for _, group := range groups {
		ordered = append(ordered, group)
	}
	sort.Slice(ordered, func(left, right int) bool {
		return shardRegistryKeyLess(ordered[left].output, ordered[right].output)
	})

	consumed := make(map[shardRegistryKey]struct{})
	selected := make([]ShardTop, 0, len(ordered)+1)
	for _, group := range ordered {
		claims := shardTopGroupClaims(group.key)
		if shardTopClaimsConsumed(claims, consumed) {
			continue
		}

		batch := selectShardTopGroup(registry, group, ltLimit)
		if len(batch) == 0 {
			continue
		}
		selected = append(selected, batch...)
		for _, claim := range claims {
			consumed[claim] = struct{}{}
		}
	}

	if len(selected) == 0 {
		return nil, nil
	}
	staged := &ShardRegistry{
		leaves:   cloneShardRegistryLeaves(registry.leaves),
		accepted: make(map[shardRegistryKey]shardRegistryLeaf),
	}
	if err := staged.Apply(selected, ltLimit); err != nil {
		return nil, fmt.Errorf("select shard top batch: %w", err)
	}

	sort.Slice(selected, func(left, right int) bool {
		return shardRegistryKeyLess(shardTopKey(selected[left].Block), shardTopKey(selected[right].Block))
	})
	return selected, nil
}

func selectShardTopGroup(registry *ShardRegistry, group *shardTopTransitionGroup, ltLimit uint64) []ShardTop {
	if group.key.kind != shardTransitionSplit {
		for _, candidate := range group.candidates {
			batch := []ShardTop{candidate.top}
			if shardTopBatchValid(registry, batch, ltLimit) {
				return batch
			}
		}
		return nil
	}

	for _, left := range group.left {
		for _, right := range group.right {
			batch := []ShardTop{left.top, right.top}
			if shardTopBatchValid(registry, batch, ltLimit) {
				return batch
			}
		}
	}

	return nil
}

func shardTopBatchValid(registry *ShardRegistry, batch []ShardTop, ltLimit uint64) bool {
	decoded := make([]decodedShardTop, len(batch))
	for index := range batch {
		item, err := decodeShardTop(batch[index])
		if err != nil {
			return false
		}
		decoded[index] = item
	}
	transitions, err := planShardRegistryTransitions(decoded)
	if err != nil || len(transitions) != 1 {
		return false
	}

	return verifyShardRegistryTransition(&transitions[0], registry.leaves, ltLimit) == nil
}

func shardTopGroupKey(decoded decodedShardTop) shardTopTransitionGroupKey {
	key := shardTopTransitionGroupKey{
		kind:      decoded.kind,
		workchain: decoded.leaf.top.Block.Workchain,
		first:     decoded.predecessor[0].Shard,
	}
	if len(decoded.predecessor) == 2 {
		key.second = decoded.predecessor[1].Shard
	}

	return key
}

func shardTopGroupClaims(key shardTopTransitionGroupKey) []shardRegistryKey {
	claims := []shardRegistryKey{{workchain: key.workchain, shard: key.first}}
	if key.kind == shardTransitionMerge {
		claims = append(claims, shardRegistryKey{workchain: key.workchain, shard: key.second})
	}

	return claims
}

func shardTopClaimsConsumed(claims []shardRegistryKey, consumed map[shardRegistryKey]struct{}) bool {
	for _, claim := range claims {
		if _, exists := consumed[claim]; exists {
			return true
		}
	}

	return false
}

func shardTopCandidateLess(left, right projectedShardTop) bool {
	leftKey := shardTopKey(left.top.Block)
	rightKey := shardTopKey(right.top.Block)
	if leftKey != rightKey {
		return shardRegistryKeyLess(leftKey, rightKey)
	}
	if left.top.Block.SeqNo != right.top.Block.SeqNo {
		return left.top.Block.SeqNo > right.top.Block.SeqNo
	}
	if comparison := bytes.Compare(left.top.Block.RootHash, right.top.Block.RootHash); comparison != 0 {
		return comparison < 0
	}

	return bytes.Compare(left.key[:], right.key[:]) < 0
}

func cloneInboxShardBlockDescription(source *p2p.ShardBlockDescription) (*p2p.ShardBlockDescription, error) {
	cloned := &p2p.ShardBlockDescription{
		Block:            *source.Block.Copy(),
		CatchainSeqno:    source.CatchainSeqno,
		ValidatorSetHash: source.ValidatorSetHash,
		Chain:            make([]p2p.ShardDescriptionLink, len(source.Chain)),
	}
	for index := range source.Chain {
		link := &source.Chain[index]
		fees, err := link.FeesCollected.Add(tlb.CurrencyCollection{})
		if err != nil {
			return nil, fmt.Errorf("clone link %d fees: %w", index, err)
		}
		created, err := link.FundsCreated.Add(tlb.CurrencyCollection{})
		if err != nil {
			return nil, fmt.Errorf("clone link %d created funds: %w", index, err)
		}

		var masterchainRef *ton.BlockIDExt
		if link.MasterchainRef != nil {
			masterchainRef = link.MasterchainRef.Copy()
		}
		cloned.Chain[index] = p2p.ShardDescriptionLink{
			Block:          *link.Block.Copy(),
			PrevRefs:       cloneInboxBlockIDs(link.PrevRefs),
			MasterchainRef: masterchainRef,
			GenUtime:       link.GenUtime,
			VertSeqno:      link.VertSeqno,
			StartLT:        link.StartLT,
			EndLT:          link.EndLT,
			MinRefMCSeqno:  link.MinRefMCSeqno,
			BeforeSplit:    link.BeforeSplit,
			AfterSplit:     link.AfterSplit,
			AfterMerge:     link.AfterMerge,
			WantSplit:      link.WantSplit,
			WantMerge:      link.WantMerge,
			CreatedBy:      link.CreatedBy,
			FeesCollected:  fees,
			FundsCreated:   created,
		}
	}

	return cloned, nil
}

func cloneInboxBlockIDs(source []ton.BlockIDExt) []ton.BlockIDExt {
	if len(source) == 0 {
		return nil
	}

	cloned := make([]ton.BlockIDExt, len(source))
	for index := range source {
		cloned[index] = *source[index].Copy()
	}

	return cloned
}

func (i *ShardTopInbox) latestSnapshots() []shardTopInboxSnapshot {
	i.mu.Lock()
	defer i.mu.Unlock()

	snapshots := make([]shardTopInboxSnapshot, 0, len(i.groups))
	for element := i.order.Front(); element != nil; element = element.Next() {
		key := element.Value.(cell.Hash)
		entry := i.entries[key]
		group := i.groups[entry.group]
		if group == nil || group.latest != entry || group.ready == entry {
			continue
		}
		snapshots = append(snapshots, shardTopInboxSnapshot{
			key:         key,
			group:       entry.group,
			description: entry.description,
			root:        entry.root,
		})
	}

	return snapshots
}

func (i *ShardTopInbox) beginReadyCheck(snapshot shardTopInboxSnapshot) bool {
	i.mu.Lock()
	defer i.mu.Unlock()

	entry := i.entries[snapshot.key]
	group := i.groups[snapshot.group]
	if entry == nil || group == nil || group.latest != entry || group.ready == entry || entry.checking {
		return false
	}
	entry.checking = true

	return true
}

func (i *ShardTopInbox) finishReadyCheck(snapshot shardTopInboxSnapshot, ready bool) {
	i.mu.Lock()
	defer i.mu.Unlock()

	entry := i.entries[snapshot.key]
	if entry == nil || entry.group != snapshot.group {
		return
	}
	entry.checking = false
	group := i.groups[snapshot.group]
	if group == nil {
		i.removeLocked(entry.key, entry)

		return
	}
	if ready && (group.ready == nil ||
		group.ready.description.Block.SeqNo < entry.description.Block.SeqNo) {
		previousReady := group.ready
		group.ready = entry
		if previousReady != nil && previousReady != group.latest {
			i.removeLocked(previousReady.key, previousReady)
		}
	}
	if group.latest != entry && group.ready != entry {
		i.removeLocked(entry.key, entry)
	}
}

func (i *ShardTopInbox) discardInvalidLatest(snapshot shardTopInboxSnapshot) {
	i.mu.Lock()
	defer i.mu.Unlock()

	entry := i.entries[snapshot.key]
	group := i.groups[snapshot.group]
	if entry != nil && group != nil && group.latest == entry && group.ready != entry {
		i.removeLocked(entry.key, entry)
	}
}

func (i *ShardTopInbox) readySnapshots() []shardTopInboxSnapshot {
	i.mu.Lock()
	defer i.mu.Unlock()

	snapshots := make([]shardTopInboxSnapshot, 0, len(i.groups))
	for element := i.order.Front(); element != nil; element = element.Next() {
		key := element.Value.(cell.Hash)
		entry := i.entries[key]
		group := i.groups[entry.group]
		if group == nil || group.ready != entry {
			continue
		}
		snapshots = append(snapshots, shardTopInboxSnapshot{
			key:         key,
			group:       entry.group,
			description: entry.description,
			root:        entry.root,
		})
	}

	return snapshots
}

func (i *ShardTopInbox) currentReady(projected []projectedShardTop) []projectedShardTop {
	i.mu.Lock()
	defer i.mu.Unlock()

	current := projected[:0]
	for _, candidate := range projected {
		group := i.groups[candidate.group]
		if group != nil && group.ready != nil && group.ready.key == candidate.key {
			current = append(current, candidate)
		}
	}

	return current
}

func (i *ShardTopInbox) discard(keys []cell.Hash) {
	if len(keys) == 0 {
		return
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	for _, key := range keys {
		if entry := i.entries[key]; entry != nil {
			i.removeLocked(key, entry)
		}
	}
}

func (i *ShardTopInbox) pruneShardOverflowLocked(shardKey shardRegistryKey) {
	for i.shardEntries[shardKey] > i.maxEntriesPerShard {
		for element := i.order.Front(); element != nil; element = element.Next() {
			key := element.Value.(cell.Hash)
			entry := i.entries[key]
			if entry.shard != shardKey {
				continue
			}
			i.removeLocked(key, entry)
			break
		}
	}
}

func (i *ShardTopInbox) removeOldestLocked() {
	front := i.order.Front()
	if front == nil {
		return
	}
	key := front.Value.(cell.Hash)
	i.removeLocked(key, i.entries[key])
}

func (i *ShardTopInbox) removeLocked(key cell.Hash, entry *shardTopInboxEntry) {
	delete(i.entries, key)
	i.order.Remove(entry.order)
	i.shardEntries[entry.shard]--
	if i.shardEntries[entry.shard] == 0 {
		delete(i.shardEntries, entry.shard)
	}

	group := i.groups[entry.group]
	if group == nil {
		return
	}
	if group.ready == entry {
		group.ready = nil
	}
	if group.latest == entry {
		group.latest = group.ready
	}
	if group.latest == nil {
		delete(i.groups, entry.group)
	}
}
