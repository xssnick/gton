package liveview

import (
	"context"
	"fmt"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

func (s *Store) PublishLiveBlockArtifacts(artifacts storage.LiveBlockArtifacts) error {
	if err := s.publishLiveBlockArtifacts(artifacts, false); err != nil {
		return err
	}
	if s.nonFinalEnabled {
		s.promoteNonfinalWaiting()
	}
	return nil
}

// publishLiveBlockArtifacts is the one publish body. accepted marks a
// publication this node produced itself and has not yet committed anywhere, so
// it is additionally enrolled in the accepted-state bound — see
// accepted_state.go for why that enrolment is mandatory rather than an
// optimization. The non-final promotion the publication may unblock is left to
// the caller, so the accepted path can notify its observers first.
func (s *Store) publishLiveBlockArtifacts(artifacts storage.LiveBlockArtifacts, accepted bool) error {
	prepared, err := prepareLiveBlockArtifacts(artifacts)
	if err == nil {
		// Structural validation of the block root and state; a block whose view
		// cannot be built is not published at all.
		err = prepared.buildFragments()
	}
	if err != nil {
		return err
	}

	// The prewarm is the expensive half and the only deferrable one. Deferred,
	// it also installs the view itself, so the publish below must not.
	deferredPrewarm := prepared.fragments
	if s.fragmentPrewarmSlots(prepared.block) != nil {
		prepared.fragments = nil
	} else {
		deferredPrewarm = nil
		if prepared.fragments != nil {
			if err = prewarmFragments(prepared.fragments); err != nil {
				return err
			}
		}
	}
	cacheArtifacts := storage.LiveBlockCacheArtifacts{
		Block:           prepared.block,
		BlockData:       prepared.data,
		Meta:            prepared.meta,
		ArtifactFlushed: prepared.artifactFlushed,
	}
	cacheArtifacts.Proofs = prepared.proofs
	if err = s.liveBlockCache.PublishLiveBlockArtifacts(cacheArtifacts); err != nil {
		return err
	}

	s.mu.Lock()
	// Enrolled BEFORE the publish, because publishLiveBlockArtifactsPreparedLocked
	// trims the block cache and protectedLiveBlocksLocked is what keeps this entry
	// out of that trim.
	if accepted {
		s.rememberAcceptedStateLocked(prepared.block)
	}
	prepared.accepted = accepted
	published, ready := s.publishLiveBlockArtifactsPreparedLocked(prepared)
	// Exact block/state waiters are driven by sync publications, including a
	// non-current shard or a non-final candidate. They must not depend on the
	// publication also advancing CurrentState or the ready masterchain seqno.
	s.signalBlockArtifactsLocked()
	if published || ready {
		close(s.notify)
		s.notify = make(chan struct{})
	}
	s.mu.Unlock()

	// Strictly after the block is in s.blocks: the prewarm installs the view
	// through rememberBlockFragments, which drops it when the block is not there
	// yet. Also strictly outside s.mu — non-final state cells are lazy and their
	// loader re-enters the read lock.
	if deferredPrewarm != nil {
		s.scheduleFragmentPrewarm(prepared.block, deferredPrewarm)
	}
	return nil
}

// fragmentPrewarmSlots returns the semaphore a block's prewarm should occupy,
// or nil when the store prewarms inline. Masterchain blocks get their own slot
// because they are the ones that need the prewarm most (the config epoch,
// prev-blocks tuple and global libraries are masterchain-only) while shard
// publishes arrive in bursts from the shard apply workers and would otherwise
// crowd them out.
func (s *Store) fragmentPrewarmSlots(block ton.BlockIDExt) chan struct{} {
	if block.Workchain == masterchainID {
		return s.fragmentMasterSlots
	}
	return s.fragmentBuildSlots
}

func (s *Store) scheduleFragmentPrewarm(block ton.BlockIDExt, view *BlockView) {
	slots := s.fragmentPrewarmSlots(block)
	if slots == nil {
		s.prewarmAndInstallFragments(block, view)
		return
	}

	select {
	case slots <- struct{}{}:
	default:
		// Every slot is busy. Publish the cold view anyway: it is fully usable,
		// its cells just load lazily per query. Blocking the producer here would
		// put the prewarm cost straight back on the block apply path.
		s.rememberBlockFragments(block, view)
		return
	}
	go func() {
		defer func() { <-slots }()
		s.prewarmAndInstallFragments(block, view)
	}()
}

func (s *Store) prewarmAndInstallFragments(block ton.BlockIDExt, view *BlockView) {
	key, ok := liveBlockLookupKeyFromBlock(block)
	if !ok {
		return
	}

	// Through fragmentLoad so a query arriving mid-prewarm joins this call
	// instead of running a second full build from storage.
	_, err := s.fragmentLoad.do(context.Background(), key, func() (*BlockView, error) {
		if cached, err := s.cachedBlockFragments(block); err == nil {
			return cached, nil
		}
		if err := prewarmFragments(view); err != nil {
			return nil, err
		}
		return s.rememberBlockFragments(block, view), nil
	})
	if err != nil && s.onFragmentBuildError != nil {
		s.onFragmentBuildError(block, err)
	}
}

func prepareLiveBlockArtifacts(artifacts storage.LiveBlockArtifacts) (livePreparedBlockArtifacts, error) {
	block := artifacts.Block
	if !blockproof.IsFullBlockID(block) {
		return livePreparedBlockArtifacts{}, storage.ErrNotFound
	}

	root := artifacts.Root
	if root == nil && len(artifacts.BlockData) > 0 {
		parsed, err := ParseTrustedBlockBOC(block, artifacts.BlockData)
		if err != nil {
			return livePreparedBlockArtifacts{}, err
		}
		root = parsed
	}
	if root != nil {
		normalized, err := normalizeLiveBlockRoot(block, root)
		if err != nil {
			return livePreparedBlockArtifacts{}, err
		}
		root = normalized
	}

	var data []byte
	if len(artifacts.BlockData) > 0 {
		data = artifacts.BlockData
	}

	meta := artifacts.Meta.Clone()
	state := storage.CloneBlockState(artifacts.State)
	if state != nil {
		if !blockIDEqual(state.Block, block) {
			return livePreparedBlockArtifacts{}, fmt.Errorf("live block state mismatch: got %s want %s", storage.FormatBlockRef(state.Block), storage.FormatBlockRef(block))
		}
		stateMeta, err := storage.BuildBlockMetaFromState(*state)
		if err != nil {
			return livePreparedBlockArtifacts{}, err
		}
		meta = storage.MergeBlockMeta(meta, stateMeta)
	}
	if meta != nil {
		meta.ID = block
		if len(data) > 0 {
			meta.Mark(storage.BlockMetaHasBlockData)
		}
	}

	var proofs []storage.LiveBlockProofArtifact
	for _, proof := range artifacts.Proofs {
		if len(proof.Data) == 0 {
			continue
		}
		if meta != nil {
			meta.Mark(storage.BlockMetaFlagForProof(proof.Kind))
		}
		proofs = append(proofs, storage.LiveBlockProofArtifact{
			Kind: proof.Kind,
			Data: proof.Data,
		})
	}

	return livePreparedBlockArtifacts{
		block:           block,
		root:            root,
		data:            data,
		meta:            meta,
		state:           state,
		wantFragments:   !artifacts.AvailabilityOnly && root != nil && state != nil && state.Cell != nil,
		proofs:          proofs,
		artifactFlushed: artifacts.ArtifactFlushed,
		stateFlushed:    artifacts.StateFlushed,
	}, nil
}

// buildFragments constructs the block view. It stays in the caller's goroutine
// because it is also the structural validation of the block root and state:
// today a block whose view cannot be built is not published at all, and the
// non-final path additionally depends on that check before a reconstructed
// state becomes the base of later non-final blocks. Only the prewarm, which is
// the expensive half, is deferrable — see prewarmFragments.
func (p *livePreparedBlockArtifacts) buildFragments() error {
	if !p.wantFragments || p.fragments != nil {
		return nil
	}

	built, err := NewBlockView(p.block, p.root, p.state.Cell)
	if err != nil {
		return err
	}
	p.fragments = built
	return nil
}

// prewarmFragments does the lazy-cell work: the accounts-dict prewarm for shard
// blocks and, for masterchain blocks, the config epoch, prev-blocks tuple and
// global libraries. The view must not be shared yet.
func prewarmFragments(view *BlockView) error {
	if err := view.prewarmAccounts(); err != nil {
		return err
	}
	view.prewarmHotPath()
	return nil
}

func cloneLiveBlockArtifacts(artifacts storage.LiveBlockArtifacts) storage.LiveBlockArtifacts {
	cloned := storage.LiveBlockArtifacts{
		Block:            artifacts.Block,
		Root:             artifacts.Root,
		BlockData:        artifacts.BlockData,
		Meta:             artifacts.Meta.Clone(),
		State:            storage.CloneBlockState(artifacts.State),
		StateUpdate:      artifacts.StateUpdate,
		ArtifactFlushed:  artifacts.ArtifactFlushed,
		StateFlushed:     artifacts.StateFlushed,
		AvailabilityOnly: artifacts.AvailabilityOnly,
	}
	if len(artifacts.Proofs) > 0 {
		cloned.Proofs = make([]storage.LiveBlockProofArtifact, 0, len(artifacts.Proofs))
		for _, proof := range artifacts.Proofs {
			cloned.Proofs = append(cloned.Proofs, storage.LiveBlockProofArtifact{
				Kind: proof.Kind,
				Data: proof.Data,
			})
		}
	}
	return cloned
}

func (s *Store) publishLiveBlockArtifactsPreparedLocked(prepared livePreparedBlockArtifacts) (bool, bool) {
	key := storage.BlockKey(prepared.block)
	flushed := s.flushed[key]
	if flushed.artifact || flushed.state {
		delete(s.flushed, key)
	}
	s.putBlockLocked(key, &liveBlock{
		id:              prepared.block,
		root:            prepared.root,
		meta:            prepared.meta,
		artifactFlushed: prepared.artifactFlushed || flushed.artifact,
		stateFlushed:    prepared.stateFlushed || flushed.state,
		fragments:       prepared.fragments,
		acceptedOwner:   prepared.accepted,
		nonfinalOwner:   prepared.nonfinal,
	}, prepared.state)
	published := s.publishPendingCurrentLocked()
	if s.current != nil && blockIDEqual(s.current.Masterchain.Block, prepared.block) {
		s.updateMasterchainInfoLocked(s.current)
	}
	if s.nonFinalEnabled {
		s.cleanupNonfinalPendingLocked()
	}
	s.cleanupAcceptedStatesLocked()
	s.trimBlocksLocked(liveBlockKind(prepared.block))
	ready := s.updateReadyMasterSeqnoLocked()
	return published, ready
}

func (s *Store) MarkLiveBlockFlushed(block ton.BlockIDExt) {
	s.liveBlockCache.MarkBlockFlushed(block)

	s.mu.Lock()
	if s.markLiveBlockFlushedLocked(block) {
		s.finishLiveBlockFlushLocked(liveBlockKind(block) == liveBlockMaster, liveBlockKind(block) == liveBlockShard)
		s.signalBlockArtifactsLocked()
	}
	s.mu.Unlock()
	if s.nonFinalEnabled {
		s.promoteNonfinalWaiting()
	}
}

// MarkLiveBlocksFlushed releases a checkpoint's artifacts before trimming the
// live cache. During archive catch-up, many older blocks remain pinned until
// the same checkpoint commits, so trimming after each block repeatedly scans
// the entire pinned history.
func (s *Store) MarkLiveBlocksFlushed(blocks []ton.BlockIDExt) {
	if len(blocks) == 0 {
		return
	}

	for _, block := range blocks {
		s.liveBlockCache.MarkBlockFlushed(block)
	}

	s.mu.Lock()
	var master, shard bool
	for _, block := range blocks {
		if !s.markLiveBlockFlushedLocked(block) {
			continue
		}
		if liveBlockKind(block) == liveBlockMaster {
			master = true
		} else {
			shard = true
		}
	}
	if master || shard {
		s.finishLiveBlockFlushLocked(master, shard)
		s.signalBlockArtifactsLocked()
	}
	s.mu.Unlock()
	if s.nonFinalEnabled {
		s.promoteNonfinalWaiting()
	}
}

func (s *Store) markLiveBlockFlushedLocked(block ton.BlockIDExt) bool {
	key := storage.BlockKey(block)
	if cached := s.blocks[key]; cached != nil {
		evictableBefore := s.liveBlockEvictableLocked(cached)
		cached.artifactFlushed = true
		s.adjustLiveEvictableStateLocked(cached, evictableBefore)
		return true
	}
	if !s.coveredByCurrentStateLocked(block) {
		// Only a later publication of the block consumes the marker, and the
		// producers of an unflushed block publish it before the applied current
		// state passes it, so a marker for a covered block would never be consumed.
		flushed := s.flushed[key]
		flushed.artifact = true
		s.flushed[key] = flushed
	}
	return false
}

func (s *Store) finishLiveBlockFlushLocked(master, shard bool) {
	published := s.publishPendingCurrentLocked()
	if s.nonFinalEnabled {
		s.cleanupNonfinalPendingLocked()
	}
	s.cleanupAcceptedStatesLocked()
	if master {
		s.trimBlocksLocked(liveBlockMaster)
	}
	if shard {
		s.trimBlocksLocked(liveBlockShard)
	}
	ready := s.updateReadyMasterSeqnoLocked()
	if published || ready {
		close(s.notify)
		s.notify = make(chan struct{})
	}
}

func (s *Store) MarkLiveBlockStatesFlushed(blocks []ton.BlockIDExt) {
	if len(blocks) == 0 {
		return
	}

	s.mu.Lock()
	for _, block := range blocks {
		s.markLiveBlockStateFlushedLocked(block, false)
	}
	s.signalBlockArtifactsLocked()
	s.trimBlocksLocked(liveBlockMaster)
	s.trimBlocksLocked(liveBlockShard)
	s.mu.Unlock()
}

func (s *Store) MarkLiveCurrentStateFlushed(current *storage.CurrentState) {
	s.mu.Lock()
	s.markLiveBlockStateFlushedLocked(current.Masterchain.Block, true)
	for _, shard := range current.Shards {
		s.markLiveBlockStateFlushedLocked(shard.Block, true)
	}
	published := s.publishPendingCurrentLocked()
	s.trimBlocksLocked(liveBlockMaster)
	s.trimBlocksLocked(liveBlockShard)
	ready := s.updateReadyMasterSeqnoLocked()
	s.signalBlockArtifactsLocked()
	if published || ready {
		close(s.notify)
		s.notify = make(chan struct{})
	}
	s.mu.Unlock()
}
