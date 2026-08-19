package liveview

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

func (s *Store) loadInitialStoredCurrentState(ctx context.Context) {
	// Fresh nodes may not have a stored current state yet. Missing current is
	// allowed; real storage errors should fail during initialization.
	if _, err := s.loadStoredCurrentState(ctx); err != nil && !errors.Is(err, storage.ErrNotFound) {
		panic(fmt.Sprintf("load stored current state: %v", err))
	}
}

func (s *Store) loadStoredCurrentState(ctx context.Context) (*storage.CurrentState, error) {
	current, err := s.backing.CurrentState(ctx)
	if err != nil {
		return nil, err
	}

	loaded, blocks, err := s.prepareStoredCurrentState(ctx, current)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	prevSeqno := currentMasterchainSeqno(s.current)
	nextSeqno := currentMasterchainSeqno(loaded)
	if nextSeqno < prevSeqno {
		current = storage.CloneCurrentState(s.current)
		s.mu.Unlock()
		return current, nil
	}

	for _, block := range blocks {
		s.putBlockLocked(storage.BlockKey(block.state.Block), &liveBlock{
			id:              block.state.Block,
			meta:            block.meta,
			artifactFlushed: true,
			stateFlushed:    true,
		}, nil)
	}
	if len(blocks) > 0 {
		s.signalBlockArtifactsLocked()
	}

	next := storage.CloneCurrentState(loaded)
	s.releaseRetiredCurrentCachesLocked(s.current, next)
	s.current = next
	s.pendingCurrent = nil
	s.rememberCurrentBlockStatesLocked(s.current)
	s.updateMasterchainInfoLocked(s.current)
	if s.nonFinalEnabled {
		s.cleanupNonfinalPendingLocked()
	}
	s.cleanupAcceptedStatesLocked()
	ready := s.updateReadyMasterSeqnoLocked()
	if nextSeqno > prevSeqno || ready {
		close(s.notify)
		s.notify = make(chan struct{})
	}
	current = storage.CloneCurrentState(s.current)
	s.mu.Unlock()
	if s.nonFinalEnabled {
		s.promoteNonfinalWaiting()
	}
	return current, nil
}

func (s *Store) prepareStoredCurrentState(ctx context.Context, current *storage.CurrentState) (*storage.CurrentState, []storedCurrentBlock, error) {
	loaded := &storage.CurrentState{
		SyncedAt:         current.SyncedAt,
		ShardClientSeqno: current.ShardClientSeqno,
		Shards:           make(map[storage.ShardKey]storage.BlockState, len(current.Shards)),
	}
	blocks := make([]storedCurrentBlock, 0, 1+len(current.Shards))

	master, err := s.loadStoredCurrentBlock(ctx, current.Masterchain)
	if err != nil {
		return nil, nil, err
	}
	loaded.Masterchain = master.state
	blocks = append(blocks, master)

	for _, key := range storage.SortedShardKeys(current.Shards) {
		shard, err := s.loadStoredCurrentBlock(ctx, current.Shards[key])
		if err != nil {
			return nil, nil, err
		}
		loaded.Shards[key] = shard.state
		blocks = append(blocks, shard)
	}
	return loaded, blocks, nil
}

func (s *Store) loadStoredCurrentBlock(ctx context.Context, state storage.BlockState) (storedCurrentBlock, error) {
	block := state.Block
	if !blockproof.IsFullBlockID(block) {
		return storedCurrentBlock{}, storage.ErrNotFound
	}
	// A zero state is a state snapshot, not an ordinary block. It has no block
	// BOC to load, but it is still a complete current-state anchor for a fresh
	// network and must be published to extensions during startup.
	if block.SeqNo != 0 {
		if _, err := s.backing.BlockData(ctx, block); err != nil {
			return storedCurrentBlock{}, err
		}
	}

	loaded, err := s.backing.BlockState(ctx, block)
	if err != nil {
		return storedCurrentBlock{}, err
	}
	if !blockIDEqual(loaded.Block, block) {
		return storedCurrentBlock{}, fmt.Errorf("stored current block state mismatch: got %s want %s", storage.FormatBlockRef(loaded.Block), storage.FormatBlockRef(block))
	}
	if len(state.StateRootHash) > 0 && !bytes.Equal(loaded.StateRootHash, state.StateRootHash) {
		return storedCurrentBlock{}, fmt.Errorf("stored current state root mismatch for %s", storage.FormatBlockRef(block))
	}

	meta, err := s.backing.BlockMeta(ctx, block)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return storedCurrentBlock{}, err
	}
	loadedMeta, err := storage.BuildBlockMetaFromState(*loaded)
	if err != nil {
		return storedCurrentBlock{}, err
	}
	meta = storage.MergeBlockMeta(meta, loadedMeta)
	if meta != nil {
		meta.ID = block
	}

	return storedCurrentBlock{
		state: *storage.CloneBlockState(loaded),
		meta:  meta,
	}, nil
}

func (s *Store) SetLiveCurrentState(current *storage.CurrentState) {
	s.SetLiveCurrentStateSnapshot(storage.CloneCurrentState(current))
}

// SetLiveCurrentStateSnapshot publishes an already-private snapshot. The store
// takes ownership: the caller must not modify next after the call.
func (s *Store) SetLiveCurrentStateSnapshot(next *storage.CurrentState) {
	s.mu.Lock()
	prevSeqno := currentMasterchainSeqno(s.current)
	s.pendingCurrent = next
	published := s.publishPendingCurrentLocked()
	if s.nonFinalEnabled {
		s.cleanupNonfinalPendingLocked()
	}
	s.cleanupAcceptedStatesLocked()
	nextSeqno := currentMasterchainSeqno(s.current)
	ready := s.updateReadyMasterSeqnoLocked()
	// Every installer of a block state raises the artifacts edge, and this one
	// installs states too: publishPendingCurrentLocked remembers the state of every
	// block the adopted snapshot names. A block whose state becomes readable only
	// through the current snapshot therefore used to raise no edge at all, and an
	// edge-triggered reader — loadChainTip — waited blind for its 30 s backstop
	// instead of for the publication it was waiting on. Raised unconditionally, as
	// in every other installer: a signal that is not about the reader's own block
	// costs it one extra read, while a missing one costs it the backstop.
	s.signalBlockArtifactsLocked()
	if published || ready || nextSeqno > prevSeqno || currentMasterchainSeqno(next) > prevSeqno {
		close(s.notify)
		s.notify = make(chan struct{})
	}
	s.mu.Unlock()
	if s.nonFinalEnabled {
		s.promoteNonfinalWaiting()
	}
}

// CurrentState returns the published state snapshot. The snapshot is immutable
// once published and shared between callers; treat it as read-only.
func (s *Store) CurrentState(_ context.Context) (*storage.CurrentState, error) {
	s.mu.RLock()
	current := s.current
	s.mu.RUnlock()
	if current != nil {
		return current, nil
	}
	return nil, storage.ErrNotFound
}

func (s *Store) CurrentAccountBlocks(_ context.Context, workchain int32, account []byte) (CurrentAccountBlockIDs, error) {
	s.mu.RLock()
	blocks, err := currentAccountBlocksFromState(s.current, workchain, account)
	s.mu.RUnlock()
	return blocks, err
}

func (s *Store) CurrentMasterchainInfo(ctx context.Context) (ton.BlockIDExt, []byte, uint32, error) {
	s.mu.RLock()
	info := s.masterchainInfo
	s.mu.RUnlock()
	if info.valid {
		return info.block, info.stateRootHash, info.lastUTime, nil
	}

	current, err := s.CurrentState(ctx)
	if err != nil {
		return ton.BlockIDExt{}, nil, 0, err
	}

	block, stateRootHash, lastUTime, err := currentMasterchainInfo(current)
	if err != nil {
		return ton.BlockIDExt{}, nil, 0, err
	}
	if lastUTime == 0 {
		meta, err := s.BlockMeta(ctx, block)
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return ton.BlockIDExt{}, nil, 0, err
		}
		if meta != nil {
			lastUTime = meta.GenUTime
		}
	}
	return block, stateRootHash, lastUTime, nil
}
