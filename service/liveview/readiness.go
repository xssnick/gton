package liveview

import (
	"context"
	"encoding/binary"
	"errors"
	"time"

	sharddomain "github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

func (s *Store) MasterchainSeqnoReady(seqno uint32) bool {
	s.mu.RLock()
	ready := s.readyMasterSeqno
	s.mu.RUnlock()
	return ready >= seqno
}

func (s *Store) WaitMasterchainSeqno(ctx context.Context, seqno uint32, timeout time.Duration) error {
	if timeout > 10*time.Second {
		timeout = 10 * time.Second
	}
	if timeout < 0 {
		timeout = 0
	}

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for {
		currentSeqno, readySeqno, notify := s.waitSnapshot()
		if readySeqno >= seqno {
			return nil
		}
		if currentSeqno > 0 && seqno > currentSeqno+100 {
			return ErrWaitMasterchainTooFar
		}

		select {
		case <-notify:
		case <-waitCtx.Done():
			if errors.Is(waitCtx.Err(), context.DeadlineExceeded) {
				return ErrWaitMasterchainTimeout
			}
			return waitCtx.Err()
		}
	}
}

func (s *Store) publishPendingCurrentLocked() bool {
	if s.pendingCurrent == nil || !s.currentStateShardsReadyLocked(s.pendingCurrent) {
		return false
	}

	prevSeqno := currentMasterchainSeqno(s.current)
	nextSeqno := currentMasterchainSeqno(s.pendingCurrent)
	if nextSeqno < prevSeqno {
		s.pendingCurrent = nil
		return false
	}

	s.releaseRetiredCurrentCachesLocked(s.current, s.pendingCurrent)
	s.current = s.pendingCurrent
	s.pendingCurrent = nil
	s.rememberCurrentBlockStatesLocked(s.current)
	s.updateMasterchainInfoLocked(s.current)
	return nextSeqno > prevSeqno
}

func (s *Store) updateMasterchainInfoLocked(current *storage.CurrentState) {
	s.masterchainInfo = liveMasterchainInfo{}
	if current == nil {
		return
	}

	block := current.Masterchain.Block
	stateRootHash := current.Masterchain.StateRootHash
	lastUTime := uint32(0)
	if current.Masterchain.Parsed != nil {
		lastUTime = current.Masterchain.Parsed.GenUTime
	}
	if meta := s.metas[storage.BlockKey(block)]; meta != nil {
		if blockIDEqual(meta.ID, block) {
			if len(stateRootHash) == 0 {
				stateRootHash = meta.StateRootHash
			}
			if lastUTime == 0 {
				lastUTime = meta.GenUTime
			}
		}
	}

	if len(stateRootHash) != 32 {
		return
	}

	s.masterchainInfo = liveMasterchainInfo{
		block:         cloneBlockID(block),
		stateRootHash: stateRootHash,
		lastUTime:     lastUTime,
		valid:         true,
	}
}

func (s *Store) currentStateShardsReadyLocked(current *storage.CurrentState) bool {
	if current == nil {
		return false
	}
	if !s.currentBlockReadyLocked(current.Masterchain) {
		return false
	}
	for _, shard := range current.Shards {
		if !s.currentBlockReadyLocked(shard) {
			return false
		}
	}
	return true
}

func (s *Store) currentBlockReadyLocked(state storage.BlockState) bool {
	// A zerostate is the state snapshot itself and has no block artifact. State
	// sync deliberately publishes cell-less current metadata after its cell tree
	// is durable, so either the attached tree or that flush marker is sufficient.
	if state.Block.SeqNo == 0 {
		if state.Cell != nil {
			return true
		}

		key := storage.BlockKey(state.Block)
		if cached := s.blocks[key]; cached != nil && blockIDEqual(cached.id, state.Block) {
			return cached.stateFlushed
		}

		return s.flushed[key].state
	}

	return s.blockDataReadyLocked(state.Block)
}

func (s *Store) blockDataReadyLocked(block ton.BlockIDExt) bool {
	if currentHasBlockState(s.current, block) {
		return true
	}
	if s.liveBlockCache.HasBlockData(block) {
		return true
	}

	cached := s.blocks[storage.BlockKey(block)]
	return cached != nil && blockIDEqual(cached.id, block) && cached.artifactFlushed
}

func (s *Store) updateReadyMasterSeqnoLocked() bool {
	next := s.readyMasterSeqno
	if s.current != nil && s.current.Masterchain.Block.SeqNo > next {
		next = s.current.Masterchain.Block.SeqNo
	}

	if next <= s.readyMasterSeqno {
		return false
	}
	s.readyMasterSeqno = next
	return true
}

func (s *Store) waitSnapshot() (uint32, uint32, <-chan struct{}) {
	s.mu.RLock()
	currentSeqno := currentMasterchainSeqno(s.current)
	readySeqno := s.readyMasterSeqno
	notify := s.notify
	s.mu.RUnlock()
	return currentSeqno, readySeqno, notify
}

func currentMasterchainSeqno(current *storage.CurrentState) uint32 {
	if current == nil {
		return 0
	}
	return current.Masterchain.Block.SeqNo
}

func currentAccountBlocksFromState(current *storage.CurrentState, workchain int32, account []byte) (CurrentAccountBlockIDs, error) {
	if current == nil {
		return CurrentAccountBlockIDs{}, storage.ErrNotFound
	}

	master := current.Masterchain.Block
	if workchain == masterchainID {
		return CurrentAccountBlockIDs{Master: master, Account: master}, nil
	}
	if len(account) < 8 {
		return CurrentAccountBlockIDs{}, storage.ErrNotFound
	}

	prefix := binary.BigEndian.Uint64(account[:8])
	for length := uint32(60); length >= 1; length-- {
		shardID, err := sharddomain.FromAccountPrefix(prefix, length)
		if err != nil {
			return CurrentAccountBlockIDs{}, err
		}
		shard, ok := current.Shards[storage.ShardKey{Workchain: workchain, Shard: shardID}]
		if ok {
			return CurrentAccountBlockIDs{Master: master, Account: shard.Block}, nil
		}
	}

	shard, ok := current.Shards[storage.ShardKey{Workchain: workchain, Shard: masterchainShard}]
	if ok {
		return CurrentAccountBlockIDs{Master: master, Account: shard.Block}, nil
	}
	return CurrentAccountBlockIDs{}, storage.ErrNotFound
}

// cachedBlockState returns a copy of the cached state struct; the nested cells
// and byte slices are shared read-only with the cache.
