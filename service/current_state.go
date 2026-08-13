package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

func (s *SyncCoordinator) ensureCurrentState(ctx context.Context) error {
	s.state.lockCurrentStateTransition()
	current, err := s.storage.CurrentState(ctx)
	s.state.unlockCurrentStateTransition()

	if err == nil {
		s.log.Debug().
			Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
			Uint32("shard_client_seqno", current.ShardClientSeqno).
			Int("shards", len(current.Shards)).
			Msg("loaded current state from storage")
		return s.catchUpCurrentState(ctx)
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return fmt.Errorf("load current state: %w", err)
	}

	var snapshot *storage.CurrentState
	if s.archiveFromZero {
		s.log.Info().Msg("current state is missing, starting archive sync from zero state")
		snapshot, err = s.stateSync.SyncZeroStateCurrent(ctx)
		if err != nil {
			return fmt.Errorf("sync current state from zero state: %w", err)
		}
	} else {
		s.log.Info().Msg("current state is missing, starting state snapshot sync")
		snapshot, err = s.stateSync.SyncCurrent(ctx)
		if err != nil {
			return fmt.Errorf("sync current state: %w", err)
		}
	}

	s.log.Info().
		Str("masterchain", storage.FormatBlockRef(snapshot.Masterchain.Block)).
		Uint32("shard_client_seqno", snapshot.ShardClientSeqno).
		Int("shards", len(snapshot.Shards)).
		Msg("synced current state")
	s.publishCommittedCurrentState(snapshot)

	return s.catchUpCurrentState(ctx)
}

func (s *SyncCoordinator) catchUpCurrentState(ctx context.Context) error {
	s.state.lockCurrentStateTransition()
	defer s.state.unlockCurrentStateTransition()

	current, err := s.storage.CurrentState(ctx)
	if err != nil {
		return fmt.Errorf("load current state: %w", err)
	}
	if err = s.checkCurrentBeforeSyncUntil(ctx, current); err != nil {
		return err
	}

	for {
		if s.syncUntilFrozen() {
			return nil
		}

		if current.ShardClientSeqno == 0 {
			current.ShardClientSeqno = current.Masterchain.Block.SeqNo
		}
		if s.archiveFromZero && current.Masterchain.Block.SeqNo == 0 && current.ShardClientSeqno == 0 {
			archiveTarget := current.Masterchain.Block
			archiveTarget.SeqNo = ^uint32(0)
			s.log.Info().
				Str("current", storage.FormatBlockRef(current.Masterchain.Block)).
				Str("archive_target", storage.FormatBlockRef(archiveTarget)).
				Msg("starting archive catch-up from zero state")

			var err error
			current, err = s.archive.CatchUp(ctx, current, archiveTarget)
			if err != nil {
				return err
			}
			continue
		}
		if s.state.cellGenerationSwitchRequestActive() || s.state.cellGenerationSwitchActive() {
			s.log.Debug().
				Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
				Uint32("shard_client_seqno", current.ShardClientSeqno).
				Msg("cell generation switch is requested, delaying current state sync")
			return nil
		}

		nowUnix := time.Now().Unix()
		masterUTime := blockStateUtime(ctx, s.storage, &current.Masterchain)
		hasMasterLag := masterUTime != 0
		lagSeconds := nowUnix - masterUTime
		knownTarget, err := s.knownMasterchainTarget(current.Masterchain.Block.SeqNo)
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return err
		}
		preferNext := err == nil && shouldPreferNextBlockTarget(current.Masterchain.Block.SeqNo, knownTarget.SeqNo)
		if hasMasterLag && shouldSwitchNextToArchiveByLag(lagSeconds) && !preferNext && current.Masterchain.Block.SeqNo != ^uint32(0) {
			archiveTarget := current.Masterchain.Block
			archiveTarget.SeqNo = ^uint32(0)
			event := s.log.Info().
				Str("current", storage.FormatBlockRef(current.Masterchain.Block)).
				Str("archive_target", storage.FormatBlockRef(archiveTarget)).
				Int64("master_lag_seconds", lagSeconds).
				Int64("remaining_lag_seconds", remainingLagSeconds(lagSeconds)).
				Int64("switch_to_archive_lag_seconds", nextToArchiveLagSeconds).
				Int64("switch_to_next_lag_seconds", archiveToNextLagSeconds).
				Uint32("lookahead_blocks", archiveTarget.SeqNo-current.Masterchain.Block.SeqNo)
			if err == nil {
				event.Str("known_target", storage.FormatBlockRef(knownTarget))
			}
			event.Msg("switching from next-block pipeline to archive catch-up")

			current, err = s.archive.CatchUp(ctx, current, archiveTarget)
			if err != nil {
				return err
			}
			if s.state.cellGenerationSwitchRequestActive() {
				s.log.Info().
					Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
					Uint32("shard_client_seqno", current.ShardClientSeqno).
					Msg("yielding archive catch-up loop for cell generation switch")
				return nil
			}
			continue
		}

		if err == nil {
			if knownTarget.SeqNo > current.Masterchain.Block.SeqNo {
				next, err := s.runNextSyncToTarget(ctx, current, knownTarget)
				if err != nil {
					return err
				}
				current = next
				if s.state.cellGenerationSwitchRequestActive() {
					s.log.Info().
						Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
						Uint32("shard_client_seqno", current.ShardClientSeqno).
						Msg("yielding targeted next-block catch-up loop for cell generation switch")
					return nil
				}
				continue
			}
		}

		s.node.SetRebroadcastQuiet(false)
		next, err := s.runNextSyncBootstrap(ctx, current)
		if err != nil {
			return err
		}
		if s.syncUntilFrozen() {
			return nil
		}
		if !next.changed {
			woken, err := s.waitCurrentStatePersistOrWake(ctx)
			if err != nil {
				return err
			}
			if woken {
				continue
			}
			s.log.Debug().
				Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
				Uint32("shard_client_seqno", current.ShardClientSeqno).
				Int("shards", len(current.Shards)).
				Msg("current state caught up")
			return nil
		}
		current = next.current
		if s.state.cellGenerationSwitchRequestActive() {
			s.log.Info().
				Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
				Uint32("shard_client_seqno", current.ShardClientSeqno).
				Msg("yielding next-block bootstrap loop for cell generation switch")
			return nil
		}
	}
}

func shouldPreferNextBlockTarget(currentSeqno, targetSeqno uint32) bool {
	// A nearby verified head is stronger recovery evidence than wall-clock lag:
	// a deliberately halted private network has old block times but no newer
	// archive to download. Distant heads still take the archive path.
	return targetSeqno > currentSeqno && targetSeqno-currentSeqno <= nextMasterchainPrefetchBlocks
}

func (s *SyncCoordinator) knownMasterchainTarget(currentSeqno uint32) (ton.BlockIDExt, error) {
	latest, err := s.node.ObservedMasterchainBlock()
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if latest.SeqNo <= currentSeqno {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	return latest, nil
}

func (s *SyncCoordinator) rememberSeenMasterchainBlock(block ton.BlockIDExt) {
	if block.Workchain != -1 || block.Shard != topShard {
		return
	}

	s.node.RememberSeenMasterchainBlock(block)
}

type queuedMasterchainBlock struct {
	block        PreparedBlock
	sourcePeerID p2p.PeerID
	queuedAt     time.Time
	bytes        int64
}

type queuedMasterchainCandidate struct {
	block        VerifiedBlock
	sourcePeerID p2p.PeerID
	queuedAt     time.Time
	bytes        int64
}

type queuedMasterchainFuture struct {
	block              ton.BlockIDExt
	sourcePeerID       p2p.PeerID
	lowestMissingSeqno uint32
}

type cachedMasterchainBlockForApply struct {
	block          PreparedBlock
	source         SyncBlockSource
	prepareElapsed time.Duration
}

func (s *SyncCoordinator) queueMasterchainBroadcastCandidateFromSource(block VerifiedBlock, sourcePeerID p2p.PeerID) {
	if !masterchainBroadcastCandidateCacheable(block) {
		return
	}
	prev := block.Meta.PrevRefs[0]

	s.nextMasterchainMx.Lock()
	defer s.nextMasterchainMx.Unlock()

	if _, ok := s.nextMasterchainQueue[storage.BlockKey(prev)]; ok {
		return
	}

	now := time.Now()
	s.pruneQueuedMasterchainBlocksLocked(now)
	bytes := queuedMasterchainCandidateBytes(block)
	if bytes > nextMasterchainQueueMaxBytes {
		return
	}
	if s.queuedMasterchainBlockTooFarFromCurrent(prev.SeqNo) {
		return
	}

	key := storage.BlockKey(prev)
	if existingKey, ok := s.nextMasterchainBySeqno[block.ID.SeqNo]; ok {
		if _, exists := s.nextMasterchainQueue[existingKey]; exists {
			return
		}
		deleteQueuedMasterchainSeqnoIndex(s.nextMasterchainBySeqno, block.ID.SeqNo, existingKey)
	}
	if existingKey, ok := s.nextMasterchainCandidateBySeqno[block.ID.SeqNo]; ok && existingKey != key {
		s.deleteQueuedMasterchainCandidateLocked(existingKey)
	}

	if _, ok := s.nextMasterchainCandidates[key]; ok {
		s.deleteQueuedMasterchainCandidateLocked(key)
	} else if queuedMasterchainBlockTooFar(s.nextMasterchainQueue, s.nextMasterchainCandidates, prev.SeqNo) {
		return
	}

	for s.queuedMasterchainItemsLocked() >= nextMasterchainQueueLimit || s.queuedMasterchainBytesLocked()+bytes > nextMasterchainQueueMaxBytes {
		if !s.evictFarthestQueuedMasterchainBlockLocked() {
			return
		}
	}

	entry := queuedMasterchainCandidate{
		block:        block,
		sourcePeerID: sourcePeerID,
		queuedAt:     now,
		bytes:        bytes,
	}
	s.nextMasterchainCandidates[key] = entry
	s.nextMasterchainCandidateBySeqno[block.ID.SeqNo] = key
	s.nextMasterchainCandidateBytes += bytes
}

func (s *SyncCoordinator) queuePreparedMasterchainBlockFromSource(block PreparedBlock, sourcePeerID p2p.PeerID) {
	if block.ID.Workchain != -1 || block.ID.Shard != topShard || len(block.Meta.PrevRefs) != 1 {
		return
	}
	prev := block.Meta.PrevRefs[0]
	if prev.Workchain != -1 || prev.Shard != topShard {
		return
	}

	s.nextMasterchainMx.Lock()
	defer s.nextMasterchainMx.Unlock()

	now := time.Now()
	s.pruneQueuedMasterchainBlocksLocked(now)
	bytes := queuedMasterchainBlockBytes(block)
	if bytes > nextMasterchainQueueMaxBytes {
		return
	}
	if s.queuedMasterchainBlockTooFarFromCurrent(prev.SeqNo) {
		return
	}

	key := storage.BlockKey(prev)
	s.deleteQueuedMasterchainCandidateLocked(key)
	if existingKey, ok := s.nextMasterchainCandidateBySeqno[block.ID.SeqNo]; ok && existingKey != key {
		s.deleteQueuedMasterchainCandidateLocked(existingKey)
	}
	if existingKey, ok := s.nextMasterchainBySeqno[block.ID.SeqNo]; ok && existingKey != key {
		s.deleteQueuedMasterchainBlockLocked(existingKey)
	}

	if _, ok := s.nextMasterchainQueue[key]; ok {
		s.deleteQueuedMasterchainBlockLocked(key)
	} else if queuedMasterchainBlockTooFar(s.nextMasterchainQueue, s.nextMasterchainCandidates, prev.SeqNo) {
		return
	}

	for s.queuedMasterchainItemsLocked() >= nextMasterchainQueueLimit || s.queuedMasterchainBytesLocked()+bytes > nextMasterchainQueueMaxBytes {
		if !s.evictFarthestQueuedMasterchainBlockLocked() {
			return
		}
	}

	entry := queuedMasterchainBlock{
		block:        block,
		sourcePeerID: sourcePeerID,
		queuedAt:     now,
		bytes:        bytes,
	}
	s.nextMasterchainQueue[key] = entry
	s.nextMasterchainBySeqno[block.ID.SeqNo] = key
	s.nextMasterchainBytes += bytes
}

func masterchainBroadcastCandidateCacheable(block VerifiedBlock) bool {
	if block.ID.Workchain != -1 || block.ID.Shard != topShard {
		return false
	}
	if len(block.Meta.PrevRefs) != 1 {
		return false
	}
	prev := block.Meta.PrevRefs[0]
	if prev.Workchain != -1 || prev.Shard != topShard {
		return false
	}
	if block.consensus == nil || (block.Source != SyncBlockSourceInternal && !masterchainBroadcastBlockKind(block.Kind)) {
		return false
	}
	if block.IsLink || len(block.BlockBOC) == 0 || len(block.ProofBOC) == 0 {
		return false
	}
	return true
}

func masterchainBroadcastBlockKind(kind string) bool {
	switch kind {
	case "tonNode.blockBroadcast", "tonNode.blockBroadcastCompressed", "tonNode.blockBroadcastCompressedV2", "tonNode.blockFinalityBroadcast":
		return true
	default:
		return false
	}
}

func queuedMasterchainBlockBytes(block PreparedBlock) int64 {
	// Count BlockBOC twice: bytes plus an approximate decoded BlockRoot budget.
	bytes := int64(len(block.BlockBOC)*2 + len(block.ProofBOC) + 256)
	bytes += int64(block.StateUpdateToCells.ByteSize())
	return bytes
}

func queuedMasterchainCandidateBytes(block VerifiedBlock) int64 {
	// Count BlockBOC twice: bytes plus an approximate decoded BlockRoot budget.
	return int64(len(block.BlockBOC)*2 + len(block.ProofBOC) + 256)
}

func deleteQueuedMasterchainSeqnoIndex(bySeqno map[uint32]storage.BlockRootHash, seqno uint32, key storage.BlockRootHash) {
	if bySeqno[seqno] == key {
		delete(bySeqno, seqno)
	}
}

func queuedMasterchainPrevSeqno(entry queuedMasterchainBlock) uint32 {
	return entry.block.Meta.PrevRefs[0].SeqNo
}

func queuedMasterchainCandidatePrevSeqno(entry queuedMasterchainCandidate) uint32 {
	return entry.block.Meta.PrevRefs[0].SeqNo
}

func queuedMasterchainBlockTooFar(queue map[storage.BlockRootHash]queuedMasterchainBlock, candidates map[storage.BlockRootHash]queuedMasterchainCandidate, prevSeqno uint32) bool {
	var minSeqno uint32
	first := true

	for _, entry := range queue {
		seqno := queuedMasterchainPrevSeqno(entry)
		if first || seqno < minSeqno {
			minSeqno = seqno
			first = false
		}
	}
	for _, entry := range candidates {
		seqno := queuedMasterchainCandidatePrevSeqno(entry)
		if first || seqno < minSeqno {
			minSeqno = seqno
			first = false
		}
	}
	if first || prevSeqno <= minSeqno {
		return false
	}
	return prevSeqno-minSeqno >= nextMasterchainQueueLimit
}

func (s *SyncCoordinator) pruneQueuedMasterchainBlocksLocked(now time.Time) {
	for key, entry := range s.nextMasterchainQueue {
		if now.Sub(entry.queuedAt) >= nextMasterchainQueueTTL {
			s.deleteQueuedMasterchainBlockLocked(key)
		}
	}
	for key, entry := range s.nextMasterchainCandidates {
		if now.Sub(entry.queuedAt) >= nextMasterchainQueueTTL {
			s.deleteQueuedMasterchainCandidateLocked(key)
		}
	}
}

func (s *SyncCoordinator) deleteQueuedMasterchainBlockLocked(key storage.BlockRootHash) {
	entry, ok := s.nextMasterchainQueue[key]
	if !ok {
		return
	}
	delete(s.nextMasterchainQueue, key)
	deleteQueuedMasterchainSeqnoIndex(s.nextMasterchainBySeqno, entry.block.ID.SeqNo, key)
	s.nextMasterchainBytes -= entry.bytes
}

func (s *SyncCoordinator) deleteQueuedMasterchainCandidateLocked(key storage.BlockRootHash) {
	entry, ok := s.nextMasterchainCandidates[key]
	if !ok {
		return
	}
	delete(s.nextMasterchainCandidates, key)
	deleteQueuedMasterchainSeqnoIndex(s.nextMasterchainCandidateBySeqno, entry.block.ID.SeqNo, key)
	s.nextMasterchainCandidateBytes -= entry.bytes
}

func (s *SyncCoordinator) queuedMasterchainItemsLocked() int {
	return len(s.nextMasterchainQueue) + len(s.nextMasterchainCandidates)
}

func (s *SyncCoordinator) queuedMasterchainBytesLocked() int64 {
	return s.nextMasterchainBytes + s.nextMasterchainCandidateBytes
}

func (s *SyncCoordinator) evictFarthestQueuedMasterchainBlockLocked() bool {
	var evictKey storage.BlockRootHash
	var evictSeqno uint32
	evictCandidate := false
	first := true

	for key, entry := range s.nextMasterchainQueue {
		seqno := queuedMasterchainPrevSeqno(entry)
		if first || seqno > evictSeqno {
			evictKey = key
			evictSeqno = seqno
			evictCandidate = false
			first = false
		}
	}
	for key, entry := range s.nextMasterchainCandidates {
		seqno := queuedMasterchainCandidatePrevSeqno(entry)
		if first || seqno > evictSeqno {
			evictKey = key
			evictSeqno = seqno
			evictCandidate = true
			first = false
		}
	}

	if first {
		return false
	}
	if evictCandidate {
		s.deleteQueuedMasterchainCandidateLocked(evictKey)
		return true
	}
	s.deleteQueuedMasterchainBlockLocked(evictKey)
	return true
}

func (s *SyncCoordinator) queuedMasterchainBlockTooFarFromCurrent(prevSeqno uint32) bool {
	currentSeqno, ok := s.status.currentMasterchainSeqno()
	if !ok {
		return false
	}
	if prevSeqno <= currentSeqno {
		return false
	}
	return prevSeqno-currentSeqno >= nextMasterchainQueueLimit
}

func (s *SyncCoordinator) takeQueuedMasterchainBlock(prev, target ton.BlockIDExt) (PreparedBlock, error) {
	if prev.Workchain != -1 || prev.Shard != topShard {
		return PreparedBlock{}, storage.ErrNotFound
	}

	key := storage.BlockKey(prev)

	s.nextMasterchainMx.Lock()
	s.pruneQueuedMasterchainBlocksLocked(time.Now())
	entry, ok := s.nextMasterchainQueue[key]
	if ok {
		s.deleteQueuedMasterchainBlockLocked(key)
	}
	s.nextMasterchainMx.Unlock()
	if !ok {
		return PreparedBlock{}, storage.ErrNotFound
	}

	block := entry.block
	if block.ID.Workchain != target.Workchain || block.ID.Shard != target.Shard || block.ID.SeqNo <= prev.SeqNo || block.ID.SeqNo > target.SeqNo {
		return PreparedBlock{}, storage.ErrNotFound
	}
	return block, nil
}

func (s *SyncCoordinator) peekQueuedMasterchainCandidate(prev, target ton.BlockIDExt) (queuedMasterchainCandidate, error) {
	if prev.Workchain != -1 || prev.Shard != topShard {
		return queuedMasterchainCandidate{}, storage.ErrNotFound
	}

	key := storage.BlockKey(prev)

	s.nextMasterchainMx.Lock()
	s.pruneQueuedMasterchainBlocksLocked(time.Now())
	entry, ok := s.nextMasterchainCandidates[key]
	s.nextMasterchainMx.Unlock()
	if !ok {
		return queuedMasterchainCandidate{}, storage.ErrNotFound
	}

	block := entry.block
	if block.ID.Workchain != target.Workchain || block.ID.Shard != target.Shard || block.ID.SeqNo <= prev.SeqNo || block.ID.SeqNo > target.SeqNo {
		return queuedMasterchainCandidate{}, storage.ErrNotFound
	}
	return entry, nil
}

func (s *SyncCoordinator) dropQueuedMasterchainCandidate(prev ton.BlockIDExt) {
	if prev.Workchain != -1 || prev.Shard != topShard {
		return
	}

	s.nextMasterchainMx.Lock()
	s.deleteQueuedMasterchainCandidateLocked(storage.BlockKey(prev))
	s.nextMasterchainMx.Unlock()
}

func (s *SyncCoordinator) promoteQueuedMasterchainBroadcastCandidate(ctx context.Context, prev, target ton.BlockIDExt) (cachedMasterchainBlockForApply, error) {
	entry, err := s.peekQueuedMasterchainCandidate(prev, target)
	if err != nil {
		return cachedMasterchainBlockForApply{}, err
	}

	started := time.Now()
	block := entry.block
	checked, err := s.validateVerifiedMasterchainBlock(ctx, block)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return cachedMasterchainBlockForApply{}, err
		}
		if errors.Is(err, storage.ErrNotFound) {
			return cachedMasterchainBlockForApply{}, storage.ErrNotFound
		}
		s.dropQueuedMasterchainCandidate(prev)
		event := s.log.Debug().
			Err(err).
			Str("block", block.BlockRef()).
			Str("prev", storage.FormatBlockRef(prev))
		if !entry.sourcePeerID.IsZero() {
			event.Str("source_peer_id", entry.sourcePeerID.String())
		}
		event.Msg("dropping invalid queued masterchain broadcast candidate")
		return cachedMasterchainBlockForApply{}, storage.ErrNotFound
	}
	block.consensusChecked = checked

	prepared, err := s.prepareVerifiedMasterchainBlockShared(ctx, block)
	if err != nil {
		if ctx.Err() != nil {
			// The shared preparation can hand back this caller's own context
			// error; that says nothing about the candidate, so keep it queued.
			return cachedMasterchainBlockForApply{}, err
		}
		s.dropQueuedMasterchainCandidate(prev)
		event := s.log.Debug().
			Err(err).
			Str("block", block.BlockRef()).
			Str("prev", storage.FormatBlockRef(prev))
		if !entry.sourcePeerID.IsZero() {
			event.Str("source_peer_id", entry.sourcePeerID.String())
		}
		event.Msg("dropping unprepared queued masterchain broadcast candidate")
		return cachedMasterchainBlockForApply{}, storage.ErrNotFound
	}

	s.dropQueuedMasterchainCandidate(prev)
	prepared.PrepareElapsed = time.Since(started)
	source := SyncBlockSourceBroadcastCandidate
	if prepared.Source == SyncBlockSourceInternal {
		source = SyncBlockSourceInternal
	}
	return cachedMasterchainBlockForApply{
		block:          prepared,
		source:         source,
		prepareElapsed: prepared.PrepareElapsed,
	}, nil
}

func (s *SyncCoordinator) takeCachedMasterchainBlockForApply(ctx context.Context, prev, target ton.BlockIDExt) (cachedMasterchainBlockForApply, error) {
	downloaded, err := s.takeQueuedMasterchainBlock(prev, target)
	if err == nil {
		source := SyncBlockSourceBroadcastQueue
		if downloaded.Source == SyncBlockSourceInternal {
			source = SyncBlockSourceInternal
		}
		return cachedMasterchainBlockForApply{block: downloaded, source: source}, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return cachedMasterchainBlockForApply{}, err
	}
	return s.promoteQueuedMasterchainBroadcastCandidate(ctx, prev, target)
}

func (s *SyncCoordinator) queuedMasterchainFuture(prev ton.BlockIDExt) (queuedMasterchainFuture, error) {
	if prev.Workchain != -1 || prev.Shard != topShard {
		return queuedMasterchainFuture{}, storage.ErrNotFound
	}
	if prev.SeqNo == ^uint32(0) {
		return queuedMasterchainFuture{}, storage.ErrNotFound
	}

	s.nextMasterchainMx.Lock()
	defer s.nextMasterchainMx.Unlock()
	s.pruneQueuedMasterchainBlocksLocked(time.Now())

	var future queuedMasterchainFuture
	future.lowestMissingSeqno = prev.SeqNo + 1
	for {
		key, ok := s.nextMasterchainBySeqno[future.lowestMissingSeqno]
		candidateKey, candidateOK := s.nextMasterchainCandidateBySeqno[future.lowestMissingSeqno]
		if !ok && !candidateOK {
			break
		}
		if ok {
			if _, ok := s.nextMasterchainQueue[key]; !ok {
				delete(s.nextMasterchainBySeqno, future.lowestMissingSeqno)
				break
			}
		}
		if candidateOK {
			if _, ok := s.nextMasterchainCandidates[candidateKey]; !ok {
				delete(s.nextMasterchainCandidateBySeqno, future.lowestMissingSeqno)
				break
			}
		}
		if future.lowestMissingSeqno == ^uint32(0) {
			break
		}
		future.lowestMissingSeqno++
	}

	hasAhead := false
	for seqno, key := range s.nextMasterchainBySeqno {
		entry, ok := s.nextMasterchainQueue[key]
		if !ok {
			delete(s.nextMasterchainBySeqno, seqno)
			continue
		}
		block := entry.block.ID
		if block.Workchain != -1 || block.Shard != topShard || block.SeqNo <= prev.SeqNo {
			continue
		}
		if !hasAhead || block.SeqNo > future.block.SeqNo {
			future.block = block
			future.sourcePeerID = entry.sourcePeerID
			hasAhead = true
		}
	}
	for seqno, key := range s.nextMasterchainCandidateBySeqno {
		entry, ok := s.nextMasterchainCandidates[key]
		if !ok {
			delete(s.nextMasterchainCandidateBySeqno, seqno)
			continue
		}
		block := entry.block.ID
		if block.Workchain != -1 || block.Shard != topShard || block.SeqNo <= prev.SeqNo {
			continue
		}
		if !hasAhead || block.SeqNo > future.block.SeqNo {
			future.block = block
			future.sourcePeerID = entry.sourcePeerID
			hasAhead = true
		}
	}
	if !hasAhead {
		return queuedMasterchainFuture{}, storage.ErrNotFound
	}
	return future, nil
}

func masterchainCatchUpTarget() ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: -1,
		Shard:     topShard,
		SeqNo:     ^uint32(0),
	}
}

func (s *SyncCoordinator) currentStateForNextMasterState(ctx context.Context, current *storage.CurrentState, masterState *storage.BlockState, targets []ton.BlockIDExt, resolver *shardStateResolver) (nextState *storage.CurrentState, stats nextShardClientApplyStats, err error) {
	started := time.Now()
	// The caller may hand in a recorder that already collected downloads for
	// this master block (shard apply-ahead); reuse it so the reported obtain
	// stats stay complete no matter which stage did the work.
	obtainRecorder := shardObtainRecorderFromContext(ctx)
	if obtainRecorder == nil {
		obtainRecorder = newShardObtainRecorder()
	}
	defer func() {
		stats.wall = time.Since(started)
		stats.obtain = obtainRecorder.duration()
		stats.downloaded = obtainRecorder.count()
	}()

	next := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: masterState.Block.SeqNo,
		Masterchain:      *storage.CloneBlockState(masterState),
		Shards:           make(map[storage.ShardKey]storage.BlockState, len(targets)),
	}
	if len(targets) == 0 {
		return next, stats, nil
	}

	type nextShardJob struct {
		target ton.BlockIDExt
	}
	jobsList := make([]nextShardJob, 0, len(targets))

	for _, target := range targets {
		key := storage.ShardKeyFromBlock(target)
		if existing, ok := current.Shards[key]; ok {
			if existing.Block.Equals(&target) {
				next.Shards[key] = *storage.CloneBlockState(&existing)
				stats.reused++
				continue
			}

			jobsList = append(jobsList, nextShardJob{target: target})
			continue
		}

		jobsList = append(jobsList, nextShardJob{target: target})
	}
	if len(jobsList) == 0 {
		return next, stats, nil
	}
	workers := archiveShardApplyParallelism
	if len(jobsList) < workers {
		workers = len(jobsList)
	}

	type shardResult struct {
		target ton.BlockIDExt
		state  *storage.BlockState
		wait   time.Duration
		err    error
	}

	jobs := make(chan nextShardJob)
	results := make(chan shardResult, len(jobsList))
	applyCtx, cancelApply := context.WithCancel(ctx)
	applyCtx = contextWithShardObtainRecorder(applyCtx, obtainRecorder)
	// Every shard block resolved for this master is stamped with it as the
	// including master, so the resolution carries the state explicitly.
	applyCtx = contextWithShardApplyMaster(applyCtx, masterState)
	defer cancelApply()

	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				waitStarted := time.Now()
				state, err := resolver.resolveWithContext(applyCtx, job.target)
				result := shardResult{
					target: job.target,
					state:  state,
					wait:   time.Since(waitStarted),
					err:    err,
				}
				select {
				case results <- result:
				case <-applyCtx.Done():
					return
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, job := range jobsList {
			select {
			case jobs <- job:
			case <-applyCtx.Done():
				return
			}
		}
	}()
	go func() {
		wg.Wait()
		close(results)
	}()

	var firstErr error
	for res := range results {
		if res.err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("apply next-block shard target %s: %w", storage.FormatBlockRef(res.target), res.err)
				cancelApply()
			}
			continue
		}
		stats.resolverWait += res.wait
		shard := *storage.CloneBlockState(res.state)
		setShardStateMasterchainRef(&shard, masterState.Block)
		next.Shards[storage.ShardKeyFromBlock(res.target)] = shard
	}

	if firstErr != nil {
		return nil, stats, firstErr
	}
	if len(next.Shards) != len(targets) {
		if err := ctx.Err(); err != nil {
			return nil, stats, err
		}
		return nil, stats, fmt.Errorf("loaded %d next-block shard states, expected %d", len(next.Shards), len(targets))
	}
	return next, stats, nil
}

func (s *SyncCoordinator) loadOrDownloadBlockForApply(ctx context.Context, block ton.BlockIDExt) (PreparedBlock, error) {
	prepared, err := s.preparedShardBlocks.take(block)
	if err == nil {
		return prepared, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return PreparedBlock{}, err
	}

	downloaded, err := loadStoredBlockForApply(ctx, s.storage, block)
	if err == nil {
		return downloaded, nil
	}
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return PreparedBlock{}, err
	}
	downloadStarted := time.Now()
	raw, err := s.downloadExactChainBlockWithRetry(ctx, block)
	observeShardBlockObtain(ctx, downloadStarted)
	if err != nil {
		return PreparedBlock{}, err
	}
	downloaded, err = s.prepareDownloadedBlockForApply(raw)
	if err != nil {
		return PreparedBlock{}, err
	}
	return downloaded, nil
}

func (s *SyncCoordinator) loadBlockStateForApply(ctx context.Context, state storage.BlockState) (*storage.BlockState, error) {
	if state.Cell != nil && state.Parsed != nil {
		return storage.CloneBlockState(&state), nil
	}

	loaded, err := s.storage.BlockState(ctx, state.Block)
	if err != nil {
		return nil, err
	}
	return loaded, nil
}
