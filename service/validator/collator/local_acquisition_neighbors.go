package collator

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/blockproof"
	sharddomain "github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
)

type localAcquiredMessages struct {
	cut        *msgpool.Cut
	neighbors  []Neighbor
	shardEndLT tlb.ShardEndLTFunc
	proofs     FullCollatedProofProvider
}

type localNeighborView struct {
	previous  PreviousBlock
	state     tlb.ShardStateUnsplit
	queue     tlb.OutMsgQueueInfo
	processed []tlb.ProcessedUptoRecord
	proof     *cell.MerkleProofBuilder
}

type localShardTopProvider struct {
	acquisition *LocalAcquisition
	master      *localMasterView
	views       map[[32]byte]*localNeighborView
}

func newLocalShardTopProvider(acquisition *LocalAcquisition, master *localMasterView) *localShardTopProvider {
	return &localShardTopProvider{
		acquisition: acquisition,
		master:      master,
		views:       make(map[[32]byte]*localNeighborView),
	}
}

func (p *localShardTopProvider) ValidatorSets(
	ctx context.Context,
	masterchain ton.BlockIDExt,
	shardTip ton.BlockIDExt,
) (ShardTopValidatorSets, error) {
	if err := ctx.Err(); err != nil {
		return ShardTopValidatorSets{}, err
	}
	if !masterchain.Equals(&p.master.context.ID) {
		return ShardTopValidatorSets{}, fmt.Errorf("%w: shard-top query uses another masterchain view", ErrInvalidInput)
	}
	target := groups.ShardID{Workchain: shardTip.Workchain, Shard: shardTip.Shard}
	current := localCurrentValidatorSet(p.master.context.Groups.Active, target)
	next, nextErr := localValidatorSetForShard(p.master.context.Groups.Future, target)
	if nextErr != nil && !errors.Is(nextErr, ErrNotFound) {
		return ShardTopValidatorSets{}, nextErr
	}

	sets := ShardTopValidatorSets{Current: current}
	if nextErr == nil {
		sets.Next = next
		sets.HasNext = true
	}

	return sets, nil
}

func localCurrentValidatorSet(sessions []groups.Session, target groups.ShardID) ShardTopValidatorSet {
	if exact, err := localValidatorSetForShard(sessions, target); err == nil {
		return exact
	}
	var related *groups.Session
	for i := range sessions {
		session := &sessions[i]
		if session.Shard.Workchain != target.Workchain ||
			!sharddomain.Intersects(session.Shard.Shard, target.Shard) {
			continue
		}
		if related != nil {
			// A merge announcement may intersect two current child groups. It is
			// signed by the exact future parent group, so no single current set
			// is authoritative for the comparison.
			return ShardTopValidatorSet{}
		}
		related = session
	}
	if related == nil {
		return ShardTopValidatorSet{}
	}

	return ShardTopValidatorSet{
		CatchainSeqno:    related.CatchainSeqno,
		ValidatorSetHash: related.ValidatorSetHash,
	}
}

func localValidatorSetForShard(sessions []groups.Session, target groups.ShardID) (ShardTopValidatorSet, error) {
	var matched *groups.Session
	for i := range sessions {
		session := &sessions[i]
		if session.Shard != target {
			continue
		}
		if matched != nil && matched.ID != session.ID {
			return ShardTopValidatorSet{}, fmt.Errorf("%w: multiple validator groups cover shard top", ErrInvalidInput)
		}
		matched = session
	}
	if matched == nil {
		return ShardTopValidatorSet{}, ErrNotFound
	}

	return ShardTopValidatorSet{
		CatchainSeqno:    matched.CatchainSeqno,
		ValidatorSetHash: matched.ValidatorSetHash,
	}, nil
}

func (p *localShardTopProvider) IsMasterchainAncestor(
	ctx context.Context,
	ancestor ton.BlockIDExt,
	head ton.BlockIDExt,
) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if !head.Equals(&p.master.context.ID) {
		return false, fmt.Errorf("%w: shard-top ancestry head differs from the selected view", ErrInvalidInput)
	}
	if err := p.master.requireAncestor(ancestor); err != nil {
		return false, nil
	}

	return true, nil
}

func (p *localShardTopProvider) ShardTopReady(ctx context.Context, block ton.BlockIDExt) (bool, error) {
	key, err := blockRootKey(block)
	if err != nil {
		return false, err
	}
	if p.views[key] != nil {
		return true, nil
	}
	view, err := p.acquisition.loadNeighborView(
		ctx,
		block,
		masterNeedsCollatedProofs(p.master),
		acquisitionReadImmediate,
	)
	if errors.Is(err, ErrAcquisitionNotReady) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	p.views[key] = view

	return true, nil
}

// masterNeedsCollatedProofs reports whether this collation is going to attach
// FullCollatedProofs, and therefore whether the neighbor views have to be
// traced. It deliberately takes the localMasterView rather than a config copy:
// AcquireShard reads managed.master and AcquireMaster reads a possibly
// re-derived master when it decides to set FullCollatedProofs, and a view that
// disagrees with the attach site would reach BuildFullCollatedProofs with a nil
// proof and fail the whole slot with ErrAcquisitionNotReady.
func masterNeedsCollatedProofs(master *localMasterView) bool {
	return master.context.Config.capabilities&capFullCollatedData != 0
}

func (a *LocalAcquisition) acquireShardMessages(
	ctx context.Context,
	branch *msgpool.Branch,
	master *localMasterView,
	target groups.ShardID,
	previous []PreviousBlock,
	candidateBase []PreviousBlock,
	candidateTip *[32]byte,
) (localAcquiredMessages, error) {
	expected, err := expectedShardNeighbors(master.context, target)
	if err != nil {
		return localAcquiredMessages{}, err
	}

	views := make(map[msgpool.ShardIdent]*localNeighborView, len(expected))
	neighbors, err := a.loadExpectedNeighbors(
		ctx,
		master,
		expected,
		previous,
		views,
		nil,
		masterNeedsCollatedProofs(master),
		acquisitionReadImmediate,
	)
	if err != nil {
		return localAcquiredMessages{}, err
	}
	destination := targetShardIdent(target)
	if err = prepareShardNeighborQueues(destination, neighbors, views, len(previous) == 2); err != nil {
		return localAcquiredMessages{}, err
	}
	messageViews := effectiveShardMessageViews(destination, previous, views)
	cut, err := a.cutCommittedViews(branch, destination, messageViews, candidateBase, candidateTip)
	if err != nil {
		return localAcquiredMessages{}, err
	}
	endLT, err := a.historicalShardEndLT(
		ctx,
		master,
		previous,
		neighbors,
		nil,
		nil,
		acquisitionReadImmediate,
	)
	if err != nil {
		return localAcquiredMessages{}, err
	}

	return localAcquiredMessages{
		cut:        cut,
		neighbors:  neighbors,
		shardEndLT: endLT,
		proofs:     &localFullProofProvider{views: views},
	}, nil
}

// prepareShardNeighborQueues performs the queue-bearing part of C++
// add_trivial_neighbor before collation starts. A still-registered ancestor is
// replaced with the half that belongs to our sibling, and constructing that
// half parses every retained EnqueuedMsg route. Besides rejecting an unusable
// local view before a candidate is published, this records the same reads in a
// traced neighbor state so FullCollatedData contains the exact validation
// proof. The effective queue itself is rebuilt later by deterministic
// collation; only its authenticated source reads matter here.
func prepareShardNeighborQueues(
	target msgpool.ShardIdent,
	neighbors []Neighbor,
	views map[msgpool.ShardIdent]*localNeighborView,
	afterMerge bool,
) error {
	entries, err := normalizeTrivialNeighbors(neighbors, target, Neighbor{Shard: target}, afterMerge)
	if err != nil {
		return err
	}
	for i := range entries {
		entry := &entries[i]
		if entry.origin < 0 {
			continue
		}
		source := neighbors[entry.origin].Shard
		if source == entry.Shard || source.Workchain != target.Workchain ||
			!sharddomain.Contains(int64(source.Shard), int64(target.Shard)) {
			continue
		}

		view := views[source]
		if view == nil {
			return fmt.Errorf("%w: queue view for ancestor %016x is unavailable", ErrAcquisitionNotReady, source.Shard)
		}
		if _, err = siblingQueueCut(view.queue.OutQueue, source, target, entry.Shard); err != nil {
			return fmt.Errorf("prepare virtual sibling queue: %w", err)
		}
	}

	return nil
}

// effectiveShardMessageViews mirrors the single-predecessor cases of C++
// add_trivial_neighbor. Registered states remain neighbor proof artifacts, but
// the inbound view for our own shard must come from the exact predecessor. The
// masterchain can additionally keep both child descriptors after the first
// merged block; those message views are replaced by the merged predecessor.
func effectiveShardMessageViews(
	target msgpool.ShardIdent,
	previous []PreviousBlock,
	registered map[msgpool.ShardIdent]*localNeighborView,
) map[msgpool.ShardIdent]*localNeighborView {
	if len(previous) != 1 || blockShardIdent(previous[0].ID) != target {
		return registered
	}

	children := 0
	for source := range registered {
		if source.Workchain == target.Workchain &&
			sharddomain.IsDirectChild(int64(target.Shard), int64(source.Shard)) {
			children++
		}
	}
	_, hasRegisteredTarget := registered[target]
	if !hasRegisteredTarget && children != 2 {
		return registered
	}

	effective := make(map[msgpool.ShardIdent]*localNeighborView, len(registered)+1)
	for source, view := range registered {
		if children == 2 && source.Workchain == target.Workchain &&
			sharddomain.IsDirectChild(int64(target.Shard), int64(source.Shard)) {
			continue
		}
		effective[source] = view
	}
	effective[target] = &localNeighborView{previous: previous[0]}

	return effective
}

func (a *LocalAcquisition) acquireMasterMessages(
	ctx context.Context,
	branch *msgpool.Branch,
	master *localMasterView,
	previous []PreviousBlock,
	candidateBase []PreviousBlock,
	candidateTip *[32]byte,
	tops []ShardTop,
	provider *localShardTopProvider,
) (localAcquiredMessages, error) {
	registry := &ShardRegistry{
		leaves:   cloneShardRegistryLeaves(master.registry.leaves),
		accepted: make(map[shardRegistryKey]shardRegistryLeaf),
	}
	var shardsMaxEndLT uint64
	for i := range tops {
		fields, err := parseShardDescriptorFields(tops[i].Descriptor)
		if err != nil {
			return localAcquiredMessages{}, fmt.Errorf("%w: selected shard top %d: %v", ErrInvalidInput, i, err)
		}
		shardsMaxEndLT = max(shardsMaxEndLT, fields.endLT)
	}
	startLT, err := nextMasterBlockStartLT(master.state.GenLT, shardsMaxEndLT)
	if err != nil {
		return localAcquiredMessages{}, err
	}
	if err = registry.Apply(tops, startLT); err != nil {
		return localAcquiredMessages{}, err
	}

	expected := expectedMasterNeighbors(registry, previous[0].ID)
	views := make(map[msgpool.ShardIdent]*localNeighborView, len(expected))
	for index := range tops {
		key, keyErr := blockRootKey(tops[index].Block)
		if keyErr != nil {
			return localAcquiredMessages{}, keyErr
		}
		view := provider.views[key]
		if view == nil {
			return localAcquiredMessages{}, fmt.Errorf("%w: selected shard top state is unavailable", ErrAcquisitionNotReady)
		}
		views[blockShardIdent(tops[index].Block)] = view
	}
	neighbors, err := a.loadExpectedNeighbors(
		ctx,
		master,
		expected,
		previous,
		views,
		nil,
		masterNeedsCollatedProofs(master),
		acquisitionReadImmediate,
	)
	if err != nil {
		return localAcquiredMessages{}, err
	}

	destination := targetShardIdent(groups.ShardID{Workchain: masterchainWorkchainID, Shard: sharddomain.Root})
	localSources := make(map[msgpool.ShardIdent]struct{}, len(tops))
	localRuns := make([][]*msgpool.InternalMessage, 0, len(tops))
	for i := range tops {
		source := blockShardIdent(tops[i].Block)
		view := views[source]
		localSources[source] = struct{}{}
		messages, seedErr := a.localSeedCut(destination, source, view)
		if seedErr != nil {
			return localAcquiredMessages{}, seedErr
		}
		if len(messages) > 0 {
			localRuns = append(localRuns, messages)
		}
	}
	committed, err := a.cutViews(branch, destination, views, candidateBase, candidateTip, localSources)
	if err != nil {
		return localAcquiredMessages{}, err
	}
	cut := mergeLocalCuts(committed, localRuns)
	endLT, err := a.historicalShardEndLT(
		ctx,
		master,
		previous,
		neighbors,
		nil,
		registry,
		acquisitionReadImmediate,
	)
	if err != nil {
		return localAcquiredMessages{}, err
	}

	return localAcquiredMessages{
		cut:        cut,
		neighbors:  neighbors,
		shardEndLT: endLT,
		proofs:     &localFullProofProvider{views: views},
	}, nil
}

// loadExpectedNeighbors materializes one view per expected neighbor. Both the
// collation and the validation path run it: views is the collation-side cache
// the proof builders are pinned to and is nil on validation, collated carries
// the transmitted neighbor states and is nil on collation. needProofs is the
// collation-side capFullCollatedData decision and must come from the same
// masterchain view that later decides whether to attach the proofs.
func (a *LocalAcquisition) loadExpectedNeighbors(
	ctx context.Context,
	master *localMasterView,
	expected map[neighborShardKey]ton.BlockIDExt,
	previous []PreviousBlock,
	views map[msgpool.ShardIdent]*localNeighborView,
	collated *verifiedCollatedData,
	needProofs bool,
	mode acquisitionReadMode,
) ([]Neighbor, error) {
	keys := make([]neighborShardKey, 0, len(expected))
	for key := range expected {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].workchain != keys[j].workchain {
			return keys[i].workchain < keys[j].workchain
		}
		return uint64(keys[i].shard) < uint64(keys[j].shard)
	})

	// The loop below reads one neighbour state after another, and each read is
	// storage I/O the next one does not depend on. Starting them together turns
	// the sum of those latencies into the longest of them; the loop then joins
	// whatever is still in flight through the block cache and is otherwise
	// untouched, so the order it verifies and fails in is unchanged.
	a.warmNeighborSources(ctx, keys, expected, previous, views, collated)

	neighbors := make([]Neighbor, 0, len(keys))
	for _, key := range keys {
		id := expected[key]
		source := blockShardIdent(id)
		if id.Workchain == masterchainWorkchainID {
			if !id.Equals(&master.context.ID) || !id.Equals(&master.previous.ID) {
				return nil, fmt.Errorf("%w: requested masterchain neighbor differs from the exact view", ErrInvalidInput)
			}
			// Built by hand rather than through localViewFromPrevious: the
			// masterchain state is already decoded on the view, so this skips a
			// redundant full ShardStateUnsplit parse.
			queue, err := parseQueueInfo(master.state.OutMsgQueueInfo)
			if err != nil {
				return nil, fmt.Errorf("decode masterchain neighbor %d outbound queue: %w", id.SeqNo, err)
			}
			records, err := tlb.LoadProcessedUptoRecords(queue.ProcInfo, source.Shard)
			if err != nil {
				return nil, fmt.Errorf("%w: decode masterchain processed frontier: %v", ErrInvalidInput, err)
			}
			view := &localNeighborView{
				previous:  master.previous,
				state:     master.state,
				queue:     queue,
				processed: records,
			}
			if views != nil {
				views[source] = view
			}
			neighbors = append(neighbors, localNeighbor(view, source))
			continue
		}

		var view *localNeighborView
		if cached := views[source]; cached != nil && cached.previous.ID.Equals(&id) {
			view = cached
		}
		// Full collated data is the protocol-authenticated neighbor view. Prefer
		// it to a session predecessor: the latter may be a narrower state proof
		// retained only to continue the candidate chain.
		if view == nil && collated != nil && collated.full {
			state, err := collated.StateRoot(id)
			if err != nil {
				return nil, fmt.Errorf("%w: load collated neighbor state for block %d: %v", ErrInvalidInput, id.SeqNo, err)
			}
			view, err = localViewFromPrevious(PreviousBlock{ID: id, State: state, Proven: true}, false, false)
			if err != nil {
				return nil, fmt.Errorf("decode collated neighbor %d: %w", id.SeqNo, err)
			}
		}
		// A shard is its own neighbor, so the session's own predecessor is in the
		// expected set — and it is already in memory, exactly as the node store
		// would hand it back. Prefer it: reading the same block back out of
		// storage costs a block read, a state read and a second full decode of a
		// tree we are holding.
		//
		// The one state that must not be taken here is a predecessor re-pointed
		// at the candidate's own collated proof, which covers what that
		// candidate's replay reads and stops at a pruned boundary anywhere else;
		// asking it for an outbound queue would reject a valid candidate.
		// Proven says so at the entry itself, so this branch is safe on its own
		// terms rather than by an argument about which caller reaches it. Every
		// other supplied predecessor is a full live tree — the validator applies
		// each accepted candidate to its own full parent, and collation carries
		// the state it just built — so it is decoded as a resident state, the
		// same treatment loadNeighborView gives the stored copy.
		//
		// It is deliberately never traced, whatever needProofs says. A view
		// taken here answers for a block in previous, and previous is exactly
		// the pair BuildFullCollatedProofs skips (ShardRequest.Previous and
		// Previous2 are that same pair): the predecessor state travels in the
		// candidate's own proof of its parent, not as a neighbour proof. A
		// builder here would therefore produce a proof that is discarded, after
		// traceInternalCut had walked the whole predecessor outbound queue to
		// fill it — measured at about a thousand extra cell loads per shard
		// collation on mainnet, spent on nothing.
		if view == nil {
			if supplied := suppliedNeighborState(previous, id); supplied != nil {
				var err error
				view, err = localViewFromPrevious(*supplied, true, false)
				if err != nil {
					return nil, fmt.Errorf("decode supplied neighbor %d: %w", id.SeqNo, err)
				}
			}
		}
		// Everything else comes from the node's canonical state: a neighbor
		// shard we do not collate, and a proven predecessor whose full tree this
		// node may still hold.
		if view == nil && a.store != nil {
			stored, err := a.loadNeighborView(ctx, id, needProofs, mode)
			switch {
			case err == nil:
				view = stored
			case errors.Is(err, ErrAcquisitionNotReady):
			default:
				return nil, err
			}
		}
		if view == nil {
			return nil, fmt.Errorf("%w: exact neighbor state for block %d is unavailable", ErrAcquisitionNotReady, id.SeqNo)
		}
		if views != nil {
			views[source] = view
		}
		neighbors = append(neighbors, localNeighbor(view, source))
	}

	return neighbors, nil
}

// warmNeighborSources starts the block-cache reads loadExpectedNeighbors is
// about to make, and returns without waiting for them: the loop it precedes
// asks for the same blocks in order, and the cache makes those calls join the
// read already in flight rather than start a second one.
//
// It is a cache warm and nothing more. Errors are dropped because the loop
// performs the same read authoritatively and reports whatever it finds, and
// nothing here can widen a proof: the block cache holds the untraced read, and
// every traced view is rebuilt per collation from the state root it caches.
//
// Which sources it warms is decided by pendingWarmSources.
func (a *LocalAcquisition) warmNeighborSources(
	ctx context.Context,
	keys []neighborShardKey,
	expected map[neighborShardKey]ton.BlockIDExt,
	previous []PreviousBlock,
	views map[msgpool.ShardIdent]*localNeighborView,
	collated *verifiedCollatedData,
) {
	pending := a.pendingWarmSources(keys, expected, previous, views, collated)
	// One neighbour is the loop's own read; there is nothing to overlap it with.
	if len(pending) < 2 {
		return
	}

	queue := make(chan ton.BlockIDExt, len(pending))
	for _, id := range pending {
		queue <- id
	}
	close(queue)
	for range min(len(pending), neighborWarmWorkers) {
		go func() {
			for id := range queue {
				if ctx.Err() != nil {
					return
				}
				_, _ = a.blockSource(ctx, id, acquisitionReadImmediate)
			}
		}()
	}
}

// pendingWarmSources selects the neighbour blocks warmNeighborSources will read
// ahead of the resolution loop, in the loop's own order.
//
// The skipped cases mirror the loop's preference order exactly: the masterchain
// neighbour is served from the master view, an entry already in views is reused,
// full collated data is the authenticated neighbour state that makes the
// resident read unnecessary, and a supplied predecessor that is not proven is
// the same tree the store would return. Warming a source the loop prefers
// something else for is not merely wasted work: it is precisely the storage read
// the preference exists to remove.
//
// It is a separate function so that this selection — the half of the preference
// that runs on goroutines nobody joins, and is therefore invisible to a test
// that watches a store counter — can be asserted directly and deterministically.
func (a *LocalAcquisition) pendingWarmSources(
	keys []neighborShardKey,
	expected map[neighborShardKey]ton.BlockIDExt,
	previous []PreviousBlock,
	views map[msgpool.ShardIdent]*localNeighborView,
	collated *verifiedCollatedData,
) []ton.BlockIDExt {
	if a.store == nil || (collated != nil && collated.full) {
		return nil
	}

	pending := make([]ton.BlockIDExt, 0, len(keys))
	for _, key := range keys {
		id := expected[key]
		if id.Workchain == masterchainWorkchainID {
			continue
		}
		if cached := views[blockShardIdent(id)]; cached != nil && cached.previous.ID.Equals(&id) {
			continue
		}
		if suppliedNeighborState(previous, id) != nil {
			continue
		}
		pending = append(pending, id)
	}

	return pending
}

// suppliedNeighborState returns the session predecessor that can answer for
// block id as a neighbour, or nil when none can. Both the resolution loop and
// its warm-up call it, which is what keeps the warm-up from starting the very
// storage read the loop is about to prefer a memory-resident state over.
//
// A proven entry is deliberately invisible here: its state is the candidate's
// own Merkle proof, complete for that candidate's replay and pruned everywhere
// else, so the neighbour view has to come from the node store instead.
func suppliedNeighborState(previous []PreviousBlock, id ton.BlockIDExt) *PreviousBlock {
	for i := range previous {
		if !previous[i].Proven && previous[i].ID.Equals(&id) {
			return &previous[i]
		}
	}

	return nil
}

func (a *LocalAcquisition) loadNeighborView(
	ctx context.Context,
	id ton.BlockIDExt,
	trace bool,
	mode acquisitionReadMode,
) (*localNeighborView, error) {
	previous, _, err := a.loadPrevious(ctx, id, mode)
	if err != nil {
		return nil, err
	}
	view, err := localViewFromPrevious(previous, true, trace)
	if err != nil {
		return nil, err
	}

	return view, nil
}

// localViewFromPrevious decodes one neighbor state. The two flags are
// orthogonal: local says the state is a resident full state rather than
// received collated data, trace says we are going to emit a Merkle proof of
// what the queue replay reads. Tracing is only ever asked for on local states,
// but the reverse is not true — the validation path and a collation on a
// network without capFullCollatedData both read local states without a proof,
// and a read set there costs an entry plus a pinned materialized cell for every
// queue cell the replay touches.
func localViewFromPrevious(previous PreviousBlock, local, trace bool) (*localNeighborView, error) {
	root := previous.State
	var builder *cell.MerkleProofBuilder
	if trace {
		builder = cell.NewMerkleProofBuilder(previous.State)
		root = builder.Root()
	}
	var state tlb.ShardStateUnsplit
	// A local state may be a lazy CellDB tree whose direct references look like
	// pruned cells until loaded. Proof decoding deliberately treats such a
	// reference as absent, so use ordinary decoding for it; this mirrors C++
	// ShardStateQ loading the master_ref from the auxiliary state. Received
	// collated data is a real partial Merkle proof and must keep proof-aware
	// decoding.
	var err error
	if local {
		err = parseExact(&state, root)
	} else {
		err = parseProofExact(&state, root)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: decode neighbor state: %v", ErrInvalidInput, err)
	}
	if local {
		// Read the auxiliary state only for a resident full state. C++ uses it
		// while initializing ShardStateQ, but may omit it from collated proofs
		// received from another validator because queue validation does not read
		// it. Requiring the cell on the validation path rejects valid C++ data.
		// This is gated on local rather than on trace so that a proof-less local
		// decode keeps the same validity check, and so that the cells it visits
		// still enter the read set whenever a proof is produced.
		var stats tlb.ShardStateStats
		if err := parseExact(&stats, state.Stats); err != nil {
			return nil, fmt.Errorf("%w: decode neighbor auxiliary state: %v", ErrInvalidInput, err)
		}
	}
	queue, err := parseNeighborQueueInfo(state.OutMsgQueueInfo)
	if err != nil {
		return nil, err
	}
	source := blockShardIdent(previous.ID)
	records, err := tlb.LoadProcessedUptoRecords(queue.ProcInfo, source.Shard)
	if err != nil {
		return nil, fmt.Errorf("%w: decode neighbor processed frontier: %v", ErrInvalidInput, err)
	}

	return &localNeighborView{
		previous:  previous,
		state:     state,
		queue:     queue,
		processed: records,
		proof:     builder,
	}, nil
}

func parseQueueInfo(root *cell.Cell) (tlb.OutMsgQueueInfo, error) {
	var queue tlb.OutMsgQueueInfo
	if err := parseExact(&queue, root); err != nil {
		return tlb.OutMsgQueueInfo{}, fmt.Errorf("%w: decode outbound queue: %v", ErrInvalidInput, err)
	}

	return queue, nil
}

// parseNeighborQueueInfo mirrors the narrow MessageQueue view used by the C++
// validator: neighbor proofs authenticate the complete OutMsgQueueInfo root,
// but only OutQueue and ProcessedInfo are materialized. OutMsgQueueExtra may
// therefore contain pruned DispatchQueue branches that are irrelevant here.
func parseNeighborQueueInfo(root *cell.Cell) (tlb.OutMsgQueueInfo, error) {
	if root == nil {
		return tlb.OutMsgQueueInfo{}, fmt.Errorf("%w: outbound queue is absent", ErrInvalidInput)
	}

	var loader cell.Slice
	if err := root.BeginParseInto(&loader); err != nil {
		return tlb.OutMsgQueueInfo{}, fmt.Errorf("%w: decode outbound queue: %v", ErrInvalidInput, err)
	}

	var outQueue tlb.OutMsgQueueAugDict
	if err := outQueue.LoadFromCellAsProof(&loader); err != nil {
		return tlb.OutMsgQueueInfo{}, fmt.Errorf("%w: decode outbound queue dictionary: %v", ErrInvalidInput, err)
	}
	processed, err := loader.LoadDict(processedInfoKeyBits)
	if err != nil {
		return tlb.OutMsgQueueInfo{}, fmt.Errorf("%w: decode outbound queue processed info: %v", ErrInvalidInput, err)
	}
	hasExtra, err := loader.LoadBoolBit()
	if err != nil {
		return tlb.OutMsgQueueInfo{}, fmt.Errorf("%w: decode outbound queue extra flag: %v", ErrInvalidInput, err)
	}
	if !hasExtra && (loader.BitsLeft() != 0 || loader.RefsNum() != 0) {
		return tlb.OutMsgQueueInfo{}, fmt.Errorf(
			"%w: outbound queue has trailing data without extra: %d bits, %d refs",
			ErrInvalidInput,
			loader.BitsLeft(),
			loader.RefsNum(),
		)
	}

	return tlb.OutMsgQueueInfo{OutQueue: &outQueue, ProcInfo: processed}, nil
}

func localNeighbor(view *localNeighborView, source msgpool.ShardIdent) Neighbor {
	return Neighbor{
		Block:           cloneBlockID(view.previous.ID),
		Shard:           source,
		EndLT:           view.state.GenLT,
		Processed:       view.processed,
		OutMsgQueueInfo: view.state.OutMsgQueueInfo,
	}
}

func (a *LocalAcquisition) cutCommittedViews(
	branch *msgpool.Branch,
	destination msgpool.ShardIdent,
	views map[msgpool.ShardIdent]*localNeighborView,
	candidateBase []PreviousBlock,
	candidateTip *[32]byte,
) (*msgpool.Cut, error) {
	return a.cutViews(branch, destination, views, candidateBase, candidateTip, nil)
}

func (a *LocalAcquisition) cutViews(
	branch *msgpool.Branch,
	destination msgpool.ShardIdent,
	views map[msgpool.ShardIdent]*localNeighborView,
	candidateBase []PreviousBlock,
	candidateTip *[32]byte,
	omit map[msgpool.ShardIdent]struct{},
) (*msgpool.Cut, error) {
	candidateSources := make([]msgpool.ShardIdent, 0, len(candidateBase)+1)
	if candidateTip != nil {
		candidateSources = append(candidateSources, destination)
	}
	for i := range candidateBase {
		source := blockShardIdent(candidateBase[i].ID)
		for previous := 0; previous < i; previous++ {
			if blockShardIdent(candidateBase[previous].ID) == source {
				return nil, fmt.Errorf("%w: candidate queue base repeats source", ErrInvalidInput)
			}
		}
		if source != destination {
			candidateSources = append(candidateSources, source)
		}
	}
	sources := make(map[msgpool.ShardIdent]msgpool.CutSource, len(views))
	for source, view := range views {
		if _, skipped := omit[source]; skipped {
			continue
		}
		ref, err := localSourceRef(view.previous.ID)
		if err != nil {
			return nil, err
		}
		candidateLineage := false
		for _, admitted := range candidateSources {
			candidateLineage = candidateLineage || admitted == source
		}
		if candidateTip != nil && candidateLineage {
			// CandidateTip replaces this lineage with the session-private
			// immutable base and deltas. The displayed neighbor can already be a
			// speculative successor which has no committed feed position.
			continue
		}
		if err = branch.PinSource(source, ref); err != nil {
			if !errors.Is(err, msgpool.ErrCutNotReady) && !errors.Is(err, msgpool.ErrCutStale) &&
				!errors.Is(err, msgpool.ErrNotFound) {
				return nil, fmt.Errorf("pin internal-message source: %w", err)
			}
			if _, err = branch.SeedSourceFromStateRoot(source, ref, view.previous.State); err != nil {
				return nil, fmt.Errorf("seed session internal-message source: %w", err)
			}
			if err = branch.PinSource(source, ref); err != nil {
				return nil, fmt.Errorf("pin seeded internal-message source: %w", err)
			}
		}
		sources[source] = msgpool.CutSource{Visible: ref}
	}
	cut, err := branch.Cut(msgpool.CutRequest{
		Sources:          sources,
		CandidateTip:     cloneHashPointer(candidateTip),
		CandidateSources: candidateSources,
	})
	if err != nil {
		return nil, fmt.Errorf("acquire exact internal-message cut: %w", err)
	}

	return cut, nil
}

func (a *LocalAcquisition) ensureInternalSource(
	destination, source msgpool.ShardIdent,
	ref msgpool.SourceRef,
	state *cell.Cell,
) error {
	err := a.messages.Internals().ValidateSourceRef(destination, source, ref)
	if err == nil {
		return nil
	}
	if errors.Is(err, msgpool.ErrCutStale) {
		// Never move a shared destination run backwards. Its bounded history
		// serves exact older predecessors. The caller may still read the exact
		// historical state into a request-local run, as the C++ collator does.
		return fmt.Errorf("%w: internal-message source position is stale: %w", ErrAcquisitionNotReady, err)
	}
	if !errors.Is(err, msgpool.ErrCutNotReady) && !errors.Is(err, msgpool.ErrNotFound) {
		return fmt.Errorf("read internal-message source: %w", err)
	}

	routed, total, err := a.messages.Internals().SeedsFromStateRoot(source, ref, state)
	if err != nil {
		return fmt.Errorf("derive internal-message seed: %w", err)
	}
	for i := range routed {
		if routed[i].Destination != destination {
			continue
		}
		if err = a.messages.Internals().Seed(destination, source, ref, routed[i].Messages, total); err != nil {
			return fmt.Errorf("seed internal-message source: %w", err)
		}
		return nil
	}

	return fmt.Errorf("%w: internal-message destination is absent from topology", ErrAcquisitionNotReady)
}

func (a *LocalAcquisition) localSeedCut(
	destination, source msgpool.ShardIdent,
	view *localNeighborView,
) ([]*msgpool.InternalMessage, error) {
	ref, err := localSourceRef(view.previous.ID)
	if err != nil {
		return nil, err
	}
	routed, _, err := a.messages.Internals().SeedsFromStateRoot(source, ref, view.previous.State)
	if err != nil {
		return nil, fmt.Errorf("derive request-local shard-top queue: %w", err)
	}
	for i := range routed {
		if routed[i].Destination == destination {
			return routed[i].Messages, nil
		}
	}

	return nil, fmt.Errorf("%w: masterchain destination is absent from internal-message topology", ErrAcquisitionNotReady)
}

// mergeLocalCuts interleaves the committed cut with the request-local
// shard-top runs in canonical order. There is no cut limit: a locally chosen
// bound would make the resulting block a function of node configuration, and
// the validation side carries no counterpart to check it against.
func mergeLocalCuts(committed *msgpool.Cut, local [][]*msgpool.InternalMessage) *msgpool.Cut {
	total := len(committed.Messages)
	for index := range local {
		total += len(local[index])
	}

	heap := make(internalMessageHeap, 0, len(local)+1)
	heap.pushRun(0, committed.Messages)
	for index := range local {
		heap.pushRun(index+1, local[index])
	}

	messages := make([]*msgpool.InternalMessage, 0, total)
	for len(heap) > 0 {
		cursor := heap.pop()
		messages = append(messages, cursor.messages[cursor.index])
		cursor.index++
		if cursor.index < len(cursor.messages) {
			heap.push(cursor)
		}
	}

	return &msgpool.Cut{Messages: messages, More: committed.More}
}

type internalMessageCursor struct {
	run      int
	index    int
	messages []*msgpool.InternalMessage
}

type internalMessageHeap []internalMessageCursor

func (h *internalMessageHeap) pushRun(run int, messages []*msgpool.InternalMessage) {
	if len(messages) == 0 {
		return
	}

	h.push(internalMessageCursor{run: run, messages: messages})
}

func (h *internalMessageHeap) push(cursor internalMessageCursor) {
	*h = append(*h, cursor)
	index := len(*h) - 1
	for index > 0 {
		parent := (index - 1) / 2
		if !internalCursorLess((*h)[index], (*h)[parent]) {
			break
		}
		(*h)[index], (*h)[parent] = (*h)[parent], (*h)[index]
		index = parent
	}
}

func (h *internalMessageHeap) pop() internalMessageCursor {
	root := (*h)[0]
	last := len(*h) - 1
	(*h)[0] = (*h)[last]
	*h = (*h)[:last]
	for index := 0; ; {
		left := index*2 + 1
		if left >= len(*h) {
			break
		}
		smallest := left
		if right := left + 1; right < len(*h) && internalCursorLess((*h)[right], (*h)[left]) {
			smallest = right
		}
		if !internalCursorLess((*h)[smallest], (*h)[index]) {
			break
		}
		(*h)[index], (*h)[smallest] = (*h)[smallest], (*h)[index]
		index = smallest
	}

	return root
}

func internalCursorLess(left, right internalMessageCursor) bool {
	order := msgpool.CompareLtHash(left.messages[left.index], right.messages[right.index])
	if order != 0 {
		return order < 0
	}

	return left.run < right.run
}

func traceInternalCut(
	scan FullCollatedQueueScan,
	messages []*msgpool.InternalMessage,
	views map[msgpool.ShardIdent]*localNeighborView,
) error {
	expected := make(map[msgpool.ShardIdent]map[cell.Hash]*msgpool.InternalMessage)
	for i := range messages {
		message := messages[i]
		view := views[message.Source]
		if view == nil || view.proof == nil {
			continue
		}
		// CandidateTip entries are authenticated by the candidate predecessor
		// proof, not by the older registered neighbor state for the same shard.
		if message.SourceSeqno > view.previous.ID.SeqNo {
			continue
		}
		byHash := expected[message.Source]
		if byHash == nil {
			byHash = make(map[cell.Hash]*msgpool.InternalMessage)
			expected[message.Source] = byHash
		}
		byHash[message.Root.HashKey()] = message
	}

	for source, view := range views {
		if view == nil || view.proof == nil {
			continue
		}
		byHash := expected[source]
		bound := semanticMessageBound{lt: scan.LT, hash: scan.Hash}
		err := walkSemanticQueuePrefix(view.queue.OutQueue, scan.Target, bound, func(entry semanticQueueEntry) error {
			message := byHash[entry.envelope.message.HashKey()]
			if message == nil {
				return nil
			}
			if entry.enqueued.Msg.HashKey() != message.EnvelopeCell.HashKey() {
				return fmt.Errorf("%w: traced internal message differs from acquired envelope", ErrInvalidInput)
			}
			delete(byHash, entry.envelope.message.HashKey())

			return nil
		})
		if err != nil {
			return fmt.Errorf(
				"%w: trace inbound queue from (%d,%016x): %v",
				ErrInvalidInput,
				source.Workchain,
				source.Shard,
				err,
			)
		}
		if len(byHash) != 0 {
			return fmt.Errorf(
				"%w: %d acquired internal messages are absent from source (%d,%016x)",
				ErrInvalidInput,
				len(byHash),
				source.Workchain,
				source.Shard,
			)
		}
	}

	return nil
}

func localSourceRef(id ton.BlockIDExt) (msgpool.SourceRef, error) {
	root, err := blockRootKey(id)
	if err != nil {
		return msgpool.SourceRef{}, err
	}

	return msgpool.SourceRef{Seqno: id.SeqNo, RootHash: root}, nil
}

func blockShardIdent(id ton.BlockIDExt) msgpool.ShardIdent {
	return msgpool.ShardIdentFrom(id.Workchain, id.Shard)
}

type localFullProofProvider struct {
	views map[msgpool.ShardIdent]*localNeighborView
}

func (p *localFullProofProvider) BuildFullCollatedProofs(
	ctx context.Context,
	request FullCollatedProofRequest,
) ([]*cell.Cell, error) {
	if request.QueueScan != nil {
		if err := traceInternalCut(*request.QueueScan, request.Internals, p.views); err != nil {
			return nil, err
		}
	}

	previousKey, err := blockRootKey(request.Previous.ID)
	if err != nil {
		return nil, err
	}
	var previous2Key [32]byte
	hasPrevious2 := request.Previous2 != nil
	if request.Previous2 != nil {
		previous2Key, err = blockRootKey(request.Previous2.ID)
		if err != nil {
			return nil, err
		}
	}

	proofs := make([]*cell.Cell, 0, len(request.Neighbors)*2)
	for i := range request.Neighbors {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		neighbor := &request.Neighbors[i]
		if neighbor.Block.Workchain == masterchainWorkchainID {
			continue
		}
		key, err := blockRootKey(neighbor.Block)
		if err != nil {
			return nil, err
		}
		if key == previousKey || hasPrevious2 && key == previous2Key {
			continue
		}
		view := p.views[neighbor.Shard]
		if view == nil || view.proof == nil || !view.previous.ID.Equals(&neighbor.Block) {
			return nil, fmt.Errorf("%w: proof view for neighbor %d is unavailable", ErrAcquisitionNotReady, i)
		}
		if neighbor.Block.SeqNo != 0 {
			if view.previous.Block == nil {
				return nil, fmt.Errorf("%w: block root for neighbor %d is unavailable", ErrAcquisitionNotReady, i)
			}
			blockProof, err := blockproof.BlockStateRootProof(view.previous.Block)
			if err != nil {
				return nil, fmt.Errorf("build neighbor %d block proof: %w", i, err)
			}
			proofs = append(proofs, blockProof)
		}
		stateProof, err := view.proof.CreateProof()
		if err != nil {
			return nil, fmt.Errorf("build neighbor %d state proof: %w", i, err)
		}
		proofs = append(proofs, stateProof)
	}

	return proofs, nil
}

func (a *LocalAcquisition) historicalShardEndLT(
	ctx context.Context,
	current *localMasterView,
	previous []PreviousBlock,
	neighbors []Neighbor,
	collated *verifiedCollatedData,
	nextRegistry *ShardRegistry,
	mode acquisitionReadMode,
) (tlb.ShardEndLTFunc, error) {
	seqnos := map[uint32]struct{}{current.context.ID.SeqNo: {}}
	nextSeqno := current.context.ID.SeqNo + 1
	if nextRegistry != nil {
		// Candidate ProcessedInfo is decoded later by semantic verification, so
		// its seqno cannot be discovered from predecessor or neighbor records.
		// C++ installs new_config_ for that frontier unconditionally.
		seqnos[nextSeqno] = struct{}{}
	}
	frontierSeqno := func(record uint32) uint32 {
		if nextRegistry != nil && record == nextSeqno {
			return record
		}

		return min(record, current.context.ID.SeqNo)
	}
	collect := func(records []tlb.ProcessedUptoRecord) {
		for i := range records {
			seqnos[frontierSeqno(records[i].MCSeqno)] = struct{}{}
		}
	}
	for i := range neighbors {
		collect(neighbors[i].Processed)
	}
	for i := range previous {
		alreadyLoaded := false
		for j := range neighbors {
			if neighbors[j].Block.Equals(&previous[i].ID) {
				alreadyLoaded = true
				break
			}
		}
		if alreadyLoaded {
			// The neighbor view authenticates the same exact BlockID and its
			// processed frontier was collected above. Avoid decoding the state a
			// second time; a full-collated masterchain candidate can carry the
			// predecessor as a narrower proof than the resident neighbor view.
			continue
		}
		predecessor := previous[i]
		if collated != nil && collated.full && predecessor.ID.Workchain != masterchainWorkchainID {
			state, err := collated.StateRoot(predecessor.ID)
			if err != nil {
				return nil, fmt.Errorf("load collated predecessor %d state: %w", predecessor.ID.SeqNo, err)
			}
			predecessor.State = state
			predecessor.Proven = true
		}
		view, err := localViewFromPrevious(predecessor, false, false)
		if err != nil {
			return nil, fmt.Errorf("decode predecessor %d processed frontier: %w", previous[i].ID.SeqNo, err)
		}
		collect(view.processed)
	}

	type historicalView struct {
		genLT  uint64
		shards []shardEndLT
	}
	history := make(map[uint32]historicalView, len(seqnos))
	for seqno := range seqnos {
		if nextRegistry != nil && seqno == nextSeqno {
			// C++ binds the new ProcessedInfo record of a masterchain candidate
			// to new_config_, whose ShardHashes already include the selected
			// TopBlockDescr transitions. All older frontiers remain clamped to
			// the predecessor masterchain state below.
			history[seqno] = historicalView{shards: shardEndLTs(nextRegistry)}
			continue
		}
		if seqno == current.context.ID.SeqNo {
			history[seqno] = historicalView{genLT: current.state.GenLT, shards: shardEndLTs(current.registry)}
			continue
		}
		// blockAt re-derives the seqno to block id mapping from the current
		// view's authenticated history on every call, so ancestry is re-proved
		// here and only the per-block derivation below is cached.
		id, err := current.blockAt(seqno)
		if err != nil {
			return nil, err
		}
		source, err := a.blockSource(ctx, id, mode)
		if err != nil {
			return nil, err
		}
		genLT, registry, err := source.masterShardRegistry()
		if err != nil {
			return nil, err
		}
		history[seqno] = historicalView{genLT: genLT, shards: shardEndLTs(registry)}
	}

	return func(mcSeqno uint32, workchain int32, prefix uint64) uint64 {
		view, exists := history[frontierSeqno(mcSeqno)]
		if !exists {
			return 0
		}
		if workchain == masterchainWorkchainID {
			return view.genLT
		}
		for i := range view.shards {
			if view.shards[i].shard.Contains(workchain, prefix) {
				return view.shards[i].endLT
			}
		}

		return 0
	}, nil
}

// shardEndLT is one registered shard and the end lt its descriptor carries.
type shardEndLT struct {
	shard msgpool.ShardIdent
	endLT uint64
}

// shardEndLTs flattens a registry once per history entry. The resolver behind it
// is called per queued internal message per processed-upto record, and Tops()
// copies the leaf map, sorts it and allocates two hashes per leaf on every call.
func shardEndLTs(registry *ShardRegistry) []shardEndLT {
	shards := make([]shardEndLT, 0, len(registry.leaves))
	for _, leaf := range registry.leaves {
		shards = append(shards, shardEndLT{
			shard: blockShardIdent(leaf.top.Block),
			endLT: leaf.fields.endLT,
		})
	}

	return shards
}
