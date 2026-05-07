package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"flexserver/service/p2p"
	"flexserver/service/state"
	"flexserver/service/storage"

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

		nowUnix := time.Now().Unix()
		masterUTime := blockStateUtime(ctx, s.storage, &current.Masterchain)
		knownTarget, err := s.knownMasterchainTarget(ctx, current.Masterchain.Block.SeqNo)
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return err
		}
		if err == nil {
			archiveTarget, lagSeconds, ok := archiveCatchUpTargetByKnownTargetTime(current, knownTarget, masterUTime, nowUnix)
			if ok {
				s.log.Info().
					Str("current", storage.FormatBlockRef(current.Masterchain.Block)).
					Str("target", storage.FormatBlockRef(knownTarget)).
					Str("archive_target", storage.FormatBlockRef(archiveTarget)).
					Int64("lag_seconds", lagSeconds).
					Int64("switch_to_archive_lag_seconds", nextToArchiveLagSeconds).
					Int64("switch_to_next_lag_seconds", archiveToNextLagSeconds).
					Msg("starting archive catch-up because masterchain time is stale")

				current, err = s.catchUpShardClientFromArchives(ctx, current, archiveTarget)
				if err != nil {
					return err
				}
				continue
			}

			if knownTarget.SeqNo > current.Masterchain.Block.SeqNo {
				next, err := s.catchUpShardClientWithNextBlocks(ctx, current, knownTarget)
				if err != nil {
					return err
				}
				current = next
				continue
			}
		}

		if archiveTarget, lagSeconds, ok := archiveCatchUpTargetByBlockTime(current, masterUTime, nowUnix); ok {
			s.log.Info().
				Str("current", storage.FormatBlockRef(current.Masterchain.Block)).
				Str("archive_target", storage.FormatBlockRef(archiveTarget)).
				Int64("lag_seconds", lagSeconds).
				Int64("switch_to_archive_lag_seconds", nextToArchiveLagSeconds).
				Int64("switch_to_next_lag_seconds", archiveToNextLagSeconds).
				Uint32("lookahead_blocks", archiveTarget.SeqNo-current.Masterchain.Block.SeqNo).
				Msg("starting archive catch-up because current masterchain time is stale")

			current, err = s.catchUpShardClientFromArchives(ctx, current, archiveTarget)
			if err != nil {
				return err
			}
			continue
		}

		if s.node != nil {
			s.node.SetRebroadcastQuiet(false)
		}
		next, changed, err := s.catchUpShardClientBootstrap(ctx, current)
		if err != nil {
			return err
		}
		if !changed {
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
		current = next
	}
}

func (s *Service) knownMasterchainTarget(ctx context.Context, currentSeqno uint32) (ton.BlockIDExt, error) {
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

	stored, err := s.storage.SeenMasterchainBlock(ctx)
	if err == nil {
		if stored.SeqNo > currentSeqno && (!hasTarget || stored.SeqNo > target.SeqNo) {
			target = stored
			hasTarget = true
		}
	} else if !errors.Is(err, storage.ErrNotFound) {
		return ton.BlockIDExt{}, err
	}

	if !hasTarget {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	return target, nil
}

func (s *Service) masterchainLagFromKnownTarget(ctx context.Context, current ton.BlockIDExt) (ton.BlockIDExt, uint32, bool) {
	target, err := s.knownMasterchainTarget(ctx, current.SeqNo)
	if errors.Is(err, storage.ErrNotFound) {
		return current, 0, true
	}
	if err != nil {
		s.log.Debug().
			Err(err).
			Str("current", storage.FormatBlockRef(current)).
			Msg("failed to load latest masterchain lag")
		return current, 0, false
	}
	return target, target.SeqNo - current.SeqNo, true
}

func (s *Service) rememberSeenMasterchainBlock(ctx context.Context, block ton.BlockIDExt) error {
	if block.Workchain != -1 || block.Shard != topShard {
		return nil
	}

	if s.node != nil {
		s.node.RememberSeenMasterchainBlock(block)
	}
	if err := s.storage.SaveSeenMasterchainBlock(ctx, block); err != nil {
		return fmt.Errorf("save seen masterchain block %s: %w", storage.FormatBlockRef(block), err)
	}
	return nil
}

func (s *Service) queueVerifiedMasterchainBlock(downloaded p2p.DownloadedBlock) {
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
		s.nextMasterchainQueue = make(map[string]p2p.DownloadedBlock)
	}
	if len(s.nextMasterchainQueue) >= nextMasterchainQueueLimit {
		evictQueuedMasterchainBlock(s.nextMasterchainQueue)
	}
	s.nextMasterchainQueue[storage.BlockKey(prev)] = downloaded
}

func evictQueuedMasterchainBlock(queue map[string]p2p.DownloadedBlock) {
	var evictKey string
	var evictSeqno uint32
	first := true

	for key, downloaded := range queue {
		seqno := downloaded.ID.SeqNo
		if downloaded.Meta != nil && len(downloaded.Meta.PrevRefs) == 1 {
			seqno = downloaded.Meta.PrevRefs[0].SeqNo
		}
		if first || seqno < evictSeqno {
			evictKey = key
			evictSeqno = seqno
			first = false
		}
	}

	delete(queue, evictKey)
}

func (s *Service) takeQueuedMasterchainBlock(prev, target ton.BlockIDExt) (p2p.DownloadedBlock, bool) {
	if prev.Workchain != -1 || prev.Shard != topShard {
		return p2p.DownloadedBlock{}, false
	}

	key := storage.BlockKey(prev)

	s.nextMasterchainMx.Lock()
	downloaded, ok := s.nextMasterchainQueue[key]
	if ok {
		delete(s.nextMasterchainQueue, key)
	}
	s.nextMasterchainMx.Unlock()
	if !ok {
		return p2p.DownloadedBlock{}, false
	}

	if downloaded.ID.Workchain != target.Workchain || downloaded.ID.Shard != target.Shard || downloaded.ID.SeqNo <= prev.SeqNo || downloaded.ID.SeqNo > target.SeqNo {
		return p2p.DownloadedBlock{}, false
	}
	if downloaded.Meta == nil || len(downloaded.Meta.PrevRefs) != 1 || !downloaded.Meta.PrevRefs[0].Equals(&prev) {
		return p2p.DownloadedBlock{}, false
	}
	return downloaded, true
}

func masterchainSeqnoTarget(seqno uint32) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: -1,
		Shard:     topShard,
		SeqNo:     seqno,
	}
}

func (s *Service) currentStateForMasterState(ctx context.Context, current *storage.CurrentState, masterState *storage.BlockState) (currentStateResult, error) {
	targets, err := state.ShardBlocksFromMasterState(masterState)
	if err != nil {
		return currentStateResult{}, err
	}

	next := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: masterState.Block.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(masterState),
		Shards:           make(map[storage.ShardKey]storage.BlockState, len(targets)),
	}

	changed := !current.Masterchain.Block.Equals(&masterState.Block) || current.ShardClientSeqno != masterState.Block.SeqNo || len(current.Shards) != len(targets)
	if len(targets) == 0 {
		return currentStateResult{state: next, changed: changed}, nil
	}

	type shardResult struct {
		key   storage.ShardKey
		state *storage.BlockState
		err   error
	}

	workers := shardStateCatchUpParallelism
	if len(targets) < workers {
		workers = len(targets)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan shardResult, len(targets))
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	resolver := newShardStateResolver(ctx, shardStateResolverConfig{
		current:   current.Shards,
		loadState: s.loadBlockStateForApply,
		loadBlock: s.loadOrDownloadBlockForApply,
		apply: func(ctx context.Context, target ton.BlockIDExt, previous []*storage.BlockState, downloaded p2p.DownloadedBlock) (*storage.BlockState, error) {
			return s.applyResolvedShardBlock(ctx, target, previous, downloaded, nil)
		},
		save: s.storage.SaveBlockState,
	})

	for _, target := range targets {
		target := target
		key := storage.ShardKeyFromBlock(target)
		if existing, ok := current.Shards[key]; ok {
			if existing.Block.Equals(&target) {
				next.Shards[key] = storage.BlockStateWithoutCells(&existing)
				continue
			}
		}
		changed = true

		wg.Add(1)
		go func() {
			defer wg.Done()

			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				results <- shardResult{key: key, err: ctx.Err()}
				return
			}
			defer func() { <-sem }()

			shardState, err := resolver.resolve(target)
			if err != nil {
				cancel()
			}
			results <- shardResult{key: key, state: shardState, err: err}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var firstErr error
	for res := range results {
		if res.err != nil {
			if errors.Is(res.err, context.Canceled) {
				if firstErr == nil && ctx.Err() != nil && !errors.Is(ctx.Err(), context.Canceled) {
					firstErr = ctx.Err()
				}
				continue
			}
			if firstErr == nil || errors.Is(firstErr, context.Canceled) {
				firstErr = res.err
			}
			continue
		}
		next.Shards[res.key] = storage.BlockStateWithoutCells(res.state)
	}
	if firstErr != nil {
		return currentStateResult{}, firstErr
	}
	if len(next.Shards) != len(targets) {
		if ctx.Err() != nil {
			return currentStateResult{}, ctx.Err()
		}
		return currentStateResult{}, fmt.Errorf("loaded %d shard states, expected %d", len(next.Shards), len(targets))
	}

	return currentStateResult{state: next, changed: changed}, nil
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
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				waitStarted := time.Now()
				state, err := resolver.resolve(job.target)
				results <- shardResult{
					target: job.target,
					state:  state,
					wait:   time.Since(waitStarted),
					err:    err,
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, job := range jobsList {
			select {
			case jobs <- job:
			case <-ctx.Done():
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
			}
			continue
		}
		stats.resolverWait += res.wait
		next.Shards[storage.ShardKeyFromBlock(res.target)] = *storage.CloneBlockState(res.state)
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
		s.publishLiveBlock(downloaded, true)
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
	s.publishLiveBlock(downloaded, false)
	return downloaded, nil
}

func (s *Service) loadBlockStateForApply(ctx context.Context, state storage.BlockState) (*storage.BlockState, error) {
	if state.Cell != nil && state.Parsed != nil {
		return storage.CloneBlockState(&state), nil
	}
	return s.storage.BlockState(ctx, state.Block)
}
