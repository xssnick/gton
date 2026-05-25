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

func (s *Service) ensureCurrentState(ctx context.Context) error {
	s.stateMu.Lock()
	current, err := s.storage.CurrentState(ctx)
	s.stateMu.Unlock()

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

	s.log.Info().Msg("current state is missing, starting state snapshot sync")
	snapshot, err := s.stateSync.SyncCurrent(ctx)
	if err != nil {
		return fmt.Errorf("sync current state: %w", err)
	}

	s.log.Info().
		Str("masterchain", storage.FormatBlockRef(snapshot.Masterchain.Block)).
		Uint32("shard_client_seqno", snapshot.ShardClientSeqno).
		Int("shards", len(snapshot.Shards)).
		Msg("synced current state")
	return s.catchUpCurrentState(ctx)
}

func (s *Service) catchUpCurrentState(ctx context.Context) error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	current, err := s.storage.CurrentState(ctx)
	if err != nil {
		return fmt.Errorf("load current state: %w", err)
	}

	for {
		if current.ShardClientSeqno == 0 {
			current.ShardClientSeqno = current.Masterchain.Block.SeqNo
		}
		if s.cellGenerationSwitchRequestActive() || s.cellGenerationSwitchActive() {
			s.log.Debug().
				Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
				Uint32("shard_client_seqno", current.ShardClientSeqno).
				Msg("cell generation switch is requested, delaying current state sync")
			return nil
		}

		nowUnix := time.Now().Unix()
		masterUTime := blockStateUtime(ctx, s.storage, &current.Masterchain)
		lagSeconds, hasMasterLag := masterchainBlockLagSeconds(masterUTime, nowUnix)
		knownTarget, err := s.knownMasterchainTarget(current.Masterchain.Block.SeqNo)
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return err
		}

		if hasMasterLag && shouldSwitchNextToArchiveByLag(lagSeconds) {
			archiveTarget, ok := archiveCatchUpTargetByLag(current, lagSeconds)
			if ok {
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

				current, err = s.catchUpShardClientFromArchives(ctx, current, archiveTarget)
				if err != nil {
					return err
				}
				if s.cellGenerationSwitchRequestActive() {
					s.log.Info().
						Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
						Uint32("shard_client_seqno", current.ShardClientSeqno).
						Msg("yielding archive catch-up loop for cell generation switch")
					return nil
				}
				continue
			}
		}

		if err == nil {
			if knownTarget.SeqNo > current.Masterchain.Block.SeqNo {
				next, err := s.runNextSyncToTarget(ctx, current, knownTarget)
				if err != nil {
					return err
				}
				current = next
				if s.cellGenerationSwitchRequestActive() {
					s.log.Info().
						Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
						Uint32("shard_client_seqno", current.ShardClientSeqno).
						Msg("yielding targeted next-block catch-up loop for cell generation switch")
					return nil
				}
				continue
			}
		}

		if s.node != nil {
			s.node.SetRebroadcastQuiet(false)
		}
		next, err := s.runNextSyncBootstrap(ctx, current)
		if err != nil {
			return err
		}
		if !next.changed {
			if err = s.waitCurrentStatePersist(ctx); err != nil {
				return err
			}
			s.log.Debug().
				Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
				Uint32("shard_client_seqno", current.ShardClientSeqno).
				Int("shards", len(current.Shards)).
				Msg("current state caught up")
			return nil
		}
		current = next.current
		if s.cellGenerationSwitchRequestActive() {
			s.log.Info().
				Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
				Uint32("shard_client_seqno", current.ShardClientSeqno).
				Msg("yielding next-block bootstrap loop for cell generation switch")
			return nil
		}
	}
}

func (s *Service) knownMasterchainTarget(currentSeqno uint32) (ton.BlockIDExt, error) {
	var target ton.BlockIDExt
	hasTarget := false

	if s.node != nil {
		latest, err := s.node.ObservedMasterchainBlock()
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return ton.BlockIDExt{}, err
		}
		if err == nil && latest.SeqNo > currentSeqno {
			target = latest
			hasTarget = true
		}
	}

	if !hasTarget {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	return target, nil
}

func (s *Service) rememberSeenMasterchainBlock(block ton.BlockIDExt) {
	if block.Workchain != -1 || block.Shard != topShard {
		return
	}

	if s.node != nil {
		s.node.RememberSeenMasterchainBlock(block)
	}
}

type queuedMasterchainBlock struct {
	downloaded p2p.DownloadedBlock
	sourceKey  string
	queuedAt   time.Time
	bytes      int64
	verified   bool
}

type queuedMasterchainFuture struct {
	block              ton.BlockIDExt
	sourceKey          string
	lowestMissingSeqno uint32
}

func (s *Service) queueMasterchainBlockCandidateFromSource(downloaded p2p.DownloadedBlock, sourceKey string) {
	s.queueMasterchainBlockFromSource(downloaded, sourceKey, true)
}

func (s *Service) queueMasterchainBroadcastCandidateFromSource(downloaded p2p.DownloadedBlock, sourceKey string) {
	if !masterchainBroadcastCandidateCacheable(downloaded) {
		return
	}
	s.queueMasterchainBlockFromSource(downloaded, sourceKey, false)
}

func (s *Service) queueMasterchainBlockFromSource(downloaded p2p.DownloadedBlock, sourceKey string, verified bool) {
	if downloaded.ID.Workchain != -1 || downloaded.ID.Shard != topShard || downloaded.Meta == nil || len(downloaded.Meta.PrevRefs) != 1 {
		return
	}
	prev := downloaded.Meta.PrevRefs[0]
	if prev.Workchain != -1 || prev.Shard != topShard {
		return
	}

	s.nextMasterchainMx.Lock()
	defer s.nextMasterchainMx.Unlock()

	if s.nextMasterchainQueue == nil {
		s.nextMasterchainQueue = make(map[string]queuedMasterchainBlock)
	}
	if s.nextMasterchainBySeqno == nil {
		s.nextMasterchainBySeqno = make(map[uint32]string)
	}

	now := time.Now()
	s.pruneQueuedMasterchainBlocksLocked(now)
	bytes := queuedMasterchainBlockBytes(downloaded)
	if bytes > nextMasterchainQueueMaxBytes {
		return
	}
	if s.queuedMasterchainBlockTooFarFromCurrent(prev.SeqNo) {
		return
	}

	key := storage.BlockKey(prev)
	if existingKey, ok := s.nextMasterchainBySeqno[downloaded.ID.SeqNo]; ok && existingKey != key {
		s.deleteQueuedMasterchainBlockLocked(existingKey)
	}

	if _, ok := s.nextMasterchainQueue[key]; ok {
		s.deleteQueuedMasterchainBlockLocked(key)
	} else {
		if queuedMasterchainBlockTooFar(s.nextMasterchainQueue, prev.SeqNo) {
			return
		}
	}

	for len(s.nextMasterchainQueue) >= nextMasterchainQueueLimit || s.nextMasterchainBytes+bytes > nextMasterchainQueueMaxBytes {
		if !s.evictFarthestQueuedMasterchainBlockLocked() {
			return
		}
	}

	entry := queuedMasterchainBlock{
		downloaded: downloaded,
		sourceKey:  sourceKey,
		queuedAt:   now,
		bytes:      bytes,
		verified:   verified,
	}
	s.nextMasterchainQueue[key] = entry
	s.nextMasterchainBySeqno[downloaded.ID.SeqNo] = key
	s.nextMasterchainBytes += bytes
}

func masterchainBroadcastCandidateCacheable(downloaded p2p.DownloadedBlock) bool {
	if downloaded.ID.Workchain != -1 || downloaded.ID.Shard != topShard {
		return false
	}
	if downloaded.Meta == nil || len(downloaded.Meta.PrevRefs) != 1 {
		return false
	}
	prev := downloaded.Meta.PrevRefs[0]
	if prev.Workchain != -1 || prev.Shard != topShard {
		return false
	}
	if downloaded.BroadcastSignatures == nil {
		return false
	}
	if !downloaded.VerifiedRootHash || !downloaded.VerifiedFileHash {
		return false
	}
	if downloaded.IsLink || len(downloaded.BlockBOC) == 0 || len(downloaded.ProofBOC) == 0 {
		return false
	}
	return true
}

func queuedMasterchainBlockBytes(downloaded p2p.DownloadedBlock) int64 {
	bytes := int64(len(downloaded.BlockBOC) + len(downloaded.ProofBOC) + 256)
	for _, data := range downloaded.StateUpdateToCells {
		bytes += int64(len(data))
	}
	if bytes <= 0 {
		return 1
	}
	return bytes
}

func deleteQueuedMasterchainSeqnoIndex(bySeqno map[uint32]string, seqno uint32, key string) {
	if bySeqno == nil {
		return
	}
	if bySeqno[seqno] == key {
		delete(bySeqno, seqno)
	}
}

func queuedMasterchainPrevSeqno(entry queuedMasterchainBlock) uint32 {
	if entry.downloaded.Meta != nil && len(entry.downloaded.Meta.PrevRefs) == 1 {
		return entry.downloaded.Meta.PrevRefs[0].SeqNo
	}
	return entry.downloaded.ID.SeqNo
}

func queuedMasterchainMinPrevSeqno(queue map[string]queuedMasterchainBlock) (uint32, bool) {
	var minSeqno uint32
	first := true

	for _, entry := range queue {
		seqno := queuedMasterchainPrevSeqno(entry)
		if first || seqno < minSeqno {
			minSeqno = seqno
			first = false
		}
	}
	return minSeqno, !first
}

func queuedMasterchainBlockTooFar(queue map[string]queuedMasterchainBlock, prevSeqno uint32) bool {
	minSeqno, ok := queuedMasterchainMinPrevSeqno(queue)
	if !ok || prevSeqno <= minSeqno {
		return false
	}
	return prevSeqno-minSeqno >= nextMasterchainQueueLimit
}

func (s *Service) pruneQueuedMasterchainBlocksLocked(now time.Time) {
	for key, entry := range s.nextMasterchainQueue {
		if now.Sub(entry.queuedAt) >= nextMasterchainQueueTTL {
			s.deleteQueuedMasterchainBlockLocked(key)
		}
	}
}

func (s *Service) deleteQueuedMasterchainBlockLocked(key string) {
	entry, ok := s.nextMasterchainQueue[key]
	if !ok {
		return
	}
	delete(s.nextMasterchainQueue, key)
	deleteQueuedMasterchainSeqnoIndex(s.nextMasterchainBySeqno, entry.downloaded.ID.SeqNo, key)
	s.nextMasterchainBytes -= entry.bytes
	if s.nextMasterchainBytes < 0 {
		s.nextMasterchainBytes = 0
	}
}

func (s *Service) evictFarthestQueuedMasterchainBlockLocked() bool {
	var evictKey string
	var evictSeqno uint32
	first := true

	for key, entry := range s.nextMasterchainQueue {
		seqno := queuedMasterchainPrevSeqno(entry)
		if first || seqno > evictSeqno {
			evictKey = key
			evictSeqno = seqno
			first = false
		}
	}

	if first {
		return false
	}
	s.deleteQueuedMasterchainBlockLocked(evictKey)
	return true
}

func (s *Service) queuedMasterchainBlockTooFarFromCurrent(prevSeqno uint32) bool {
	s.currentStatusMu.RLock()
	current := storage.CloneCurrentState(s.currentStatus)
	s.currentStatusMu.RUnlock()
	if current == nil || current.Masterchain.Block.Workchain != -1 || current.Masterchain.Block.Shard != topShard {
		return false
	}
	if prevSeqno <= current.Masterchain.Block.SeqNo {
		return false
	}
	return prevSeqno-current.Masterchain.Block.SeqNo >= nextMasterchainQueueLimit
}

func (s *Service) takeQueuedMasterchainBlock(prev, target ton.BlockIDExt) (p2p.DownloadedBlock, bool) {
	entry, ok := s.takeQueuedMasterchainEntry(prev, target, true)
	if !ok || !entry.verified {
		return p2p.DownloadedBlock{}, false
	}
	return entry.downloaded, true
}

func (s *Service) peekQueuedMasterchainCandidate(prev, target ton.BlockIDExt) (queuedMasterchainBlock, bool) {
	if prev.Workchain != -1 || prev.Shard != topShard {
		return queuedMasterchainBlock{}, false
	}

	key := storage.BlockKey(prev)

	s.nextMasterchainMx.Lock()
	s.pruneQueuedMasterchainBlocksLocked(time.Now())
	entry, ok := s.nextMasterchainQueue[key]
	s.nextMasterchainMx.Unlock()
	if !ok || entry.verified {
		return queuedMasterchainBlock{}, false
	}

	downloaded := entry.downloaded
	if downloaded.ID.Workchain != target.Workchain || downloaded.ID.Shard != target.Shard || downloaded.ID.SeqNo <= prev.SeqNo || downloaded.ID.SeqNo > target.SeqNo {
		return queuedMasterchainBlock{}, false
	}
	if downloaded.Meta == nil || len(downloaded.Meta.PrevRefs) != 1 || !downloaded.Meta.PrevRefs[0].Equals(&prev) {
		return queuedMasterchainBlock{}, false
	}
	return entry, true
}

func (s *Service) dropQueuedMasterchainBlock(prev ton.BlockIDExt) {
	if prev.Workchain != -1 || prev.Shard != topShard {
		return
	}

	s.nextMasterchainMx.Lock()
	s.deleteQueuedMasterchainBlockLocked(storage.BlockKey(prev))
	s.nextMasterchainMx.Unlock()
}

func (s *Service) promoteQueuedMasterchainBroadcastCandidate(ctx context.Context, prev, target ton.BlockIDExt) (p2p.DownloadedBlock, bool, error) {
	entry, ok := s.peekQueuedMasterchainCandidate(prev, target)
	if !ok {
		return p2p.DownloadedBlock{}, false, nil
	}

	downloaded := entry.downloaded
	if err := s.validateSyncedMasterchainBlock(ctx, downloaded); err != nil {
		if errors.Is(err, context.Canceled) {
			return p2p.DownloadedBlock{}, false, err
		}
		if errors.Is(err, storage.ErrNotFound) {
			return p2p.DownloadedBlock{}, false, nil
		}
		s.dropQueuedMasterchainBlock(prev)
		s.log.Debug().
			Err(err).
			Str("block", downloaded.BlockRef()).
			Str("prev", storage.FormatBlockRef(prev)).
			Str("source", entry.sourceKey).
			Msg("dropping invalid queued masterchain broadcast candidate")
		return p2p.DownloadedBlock{}, false, nil
	}

	prepared, err := prepareDownloadedBlockStateCells(downloaded)
	if err != nil {
		s.dropQueuedMasterchainBlock(prev)
		s.log.Debug().
			Err(err).
			Str("block", downloaded.BlockRef()).
			Str("prev", storage.FormatBlockRef(prev)).
			Str("source", entry.sourceKey).
			Msg("dropping unprepared queued masterchain broadcast candidate")
		return p2p.DownloadedBlock{}, false, nil
	}

	s.dropQueuedMasterchainBlock(prev)
	return prepared, true, nil
}

func (s *Service) takeCachedMasterchainBlockForApply(ctx context.Context, prev, target ton.BlockIDExt) (p2p.DownloadedBlock, string, bool, error) {
	if downloaded, ok := s.takeQueuedMasterchainBlock(prev, target); ok {
		return downloaded, "broadcast_queue", true, nil
	}

	downloaded, ok, err := s.promoteQueuedMasterchainBroadcastCandidate(ctx, prev, target)
	if err != nil || !ok {
		return p2p.DownloadedBlock{}, "", false, err
	}
	return downloaded, "broadcast_candidate", true, nil
}

func (s *Service) takeQueuedMasterchainEntry(prev, target ton.BlockIDExt, requireVerified bool) (queuedMasterchainBlock, bool) {
	if prev.Workchain != -1 || prev.Shard != topShard {
		return queuedMasterchainBlock{}, false
	}

	key := storage.BlockKey(prev)

	s.nextMasterchainMx.Lock()
	s.pruneQueuedMasterchainBlocksLocked(time.Now())
	entry, ok := s.nextMasterchainQueue[key]
	if ok && (!requireVerified || entry.verified) {
		s.deleteQueuedMasterchainBlockLocked(key)
	}
	s.nextMasterchainMx.Unlock()
	if !ok {
		return queuedMasterchainBlock{}, false
	}
	if requireVerified && !entry.verified {
		return queuedMasterchainBlock{}, false
	}

	downloaded := entry.downloaded
	if downloaded.ID.Workchain != target.Workchain || downloaded.ID.Shard != target.Shard || downloaded.ID.SeqNo <= prev.SeqNo || downloaded.ID.SeqNo > target.SeqNo {
		return queuedMasterchainBlock{}, false
	}
	if downloaded.Meta == nil || len(downloaded.Meta.PrevRefs) != 1 || !downloaded.Meta.PrevRefs[0].Equals(&prev) {
		return queuedMasterchainBlock{}, false
	}
	return entry, true
}

func (s *Service) queuedMasterchainBlockAhead(prev ton.BlockIDExt) (ton.BlockIDExt, bool) {
	future, ok := s.queuedMasterchainFuture(prev)
	return future.block, ok
}

func (s *Service) queuedMasterchainFuture(prev ton.BlockIDExt) (queuedMasterchainFuture, bool) {
	if prev.Workchain != -1 || prev.Shard != topShard {
		return queuedMasterchainFuture{}, false
	}
	if prev.SeqNo == ^uint32(0) {
		return queuedMasterchainFuture{}, false
	}

	s.nextMasterchainMx.Lock()
	defer s.nextMasterchainMx.Unlock()
	s.pruneQueuedMasterchainBlocksLocked(time.Now())

	var future queuedMasterchainFuture
	future.lowestMissingSeqno = prev.SeqNo + 1
	for {
		key, ok := s.nextMasterchainBySeqno[future.lowestMissingSeqno]
		if !ok {
			break
		}
		if _, ok := s.nextMasterchainQueue[key]; !ok {
			delete(s.nextMasterchainBySeqno, future.lowestMissingSeqno)
			break
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
		block := entry.downloaded.ID
		if block.Workchain != -1 || block.Shard != topShard || block.SeqNo <= prev.SeqNo {
			continue
		}
		if !hasAhead || block.SeqNo > future.block.SeqNo {
			future.block = block
			future.sourceKey = entry.sourceKey
			hasAhead = true
		}
	}
	return future, hasAhead
}

func masterchainSeqnoTarget(seqno uint32) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: -1,
		Shard:     topShard,
		SeqNo:     seqno,
	}
}

func (s *Service) currentStateForNextMasterState(ctx context.Context, current *storage.CurrentState, masterState *storage.BlockState, targets []ton.BlockIDExt, resolver *shardStateResolver) (nextState *storage.CurrentState, stats nextShardClientApplyStats, err error) {
	started := time.Now()
	defer func() {
		stats.wall = time.Since(started)
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

func (s *Service) loadOrDownloadBlockForApply(ctx context.Context, block ton.BlockIDExt) (p2p.DownloadedBlock, error) {
	downloaded, err := loadStoredBlockForApply(ctx, s.storage, block, true)
	if err == nil {
		return downloaded, nil
	}
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return p2p.DownloadedBlock{}, err
	}

	downloaded, err = s.downloadExactChainBlockWithRetry(ctx, block)
	if err != nil {
		return p2p.DownloadedBlock{}, err
	}
	downloaded, err = prepareDownloadedBlockStateCells(downloaded)
	if err != nil {
		return p2p.DownloadedBlock{}, err
	}
	return downloaded, nil
}

func (s *Service) loadBlockStateForApply(ctx context.Context, state storage.BlockState) (*storage.BlockState, error) {
	if state.Cell != nil && state.Parsed != nil {
		return storage.CloneBlockState(&state), nil
	}
	return s.storage.BlockState(ctx, state.Block)
}
