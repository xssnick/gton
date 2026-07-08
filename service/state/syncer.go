package state

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	storage2 "github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
)

const stateSnapshotRetryDelay = time.Second

const shardStateDownloadParallelism = 4

type Syncer struct {
	source     Source
	storage    storage2.StateStorage
	importer   *stateImportCoordinator
	log        zerolog.Logger
	syncBefore time.Duration
	syncUntil  uint32
}

type blockStateSnapshot struct {
	state    *storage2.BlockState
	artifact *storage2.ServedBlockFull
}

type SyncerOptions struct {
	SyncBefore time.Duration
	SyncUntil  uint32
}

func NewSyncer(source Source, storage storage2.StateStorage, opts SyncerOptions, logger ...*zerolog.Logger) *Syncer {
	log := zerolog.Nop()
	if len(logger) > 0 && logger[0] != nil {
		log = logger[0].With().Str("component", "state").Logger()
	}
	syncBefore := opts.SyncBefore
	if syncBefore <= 0 {
		syncBefore = DefaultSyncBefore
	}

	return &Syncer{
		source:     source,
		storage:    storage,
		importer:   newStateImportCoordinator(log),
		log:        log,
		syncBefore: syncBefore,
		syncUntil:  opts.SyncUntil,
	}
}

func (s *Syncer) SyncCurrent(ctx context.Context) (*storage2.CurrentState, error) {
	progress, err := s.storage.StateSyncProgress(ctx)
	switch {
	case err == nil:
		master := progress.Masterchain.Block
		s.log.Info().
			Str("masterchain", storage2.FormatBlockRef(master)).
			Int("completed_shards", len(progress.Shards)).
			Msg("resuming pending state snapshot sync")

		masterState, err := s.storage.BlockState(ctx, master)
		if err != nil {
			return nil, fmt.Errorf("load pending masterchain state %s: %w", storage2.FormatBlockRef(master), err)
		}
		return s.shardStatesStage(ctx, blockStateSnapshot{state: masterState}, progress)
	case errors.Is(err, storage2.ErrNotFound):
	default:
		return nil, fmt.Errorf("load pending state snapshot sync progress: %w", err)
	}

	masterState, err := s.masterchainStateStage(ctx)
	if err != nil {
		return nil, err
	}
	return s.shardStatesStage(ctx, masterState, nil)
}

func (s *Syncer) masterchainStateStage(ctx context.Context) (blockStateSnapshot, error) {
	master, err := s.persistentMasterchainBlock(ctx)
	if err != nil {
		return blockStateSnapshot{}, fmt.Errorf("choose persistent masterchain state: %w", err)
	}

	s.log.Info().
		Str("masterchain", storage2.FormatBlockRef(master)).
		Msg("downloading selected masterchain state snapshot")
	if state, err := s.storage.BlockState(ctx, master); err == nil {
		s.log.Info().
			Str("masterchain", storage2.FormatBlockRef(master)).
			Msg("using stored masterchain state snapshot")
		return blockStateSnapshot{state: state}, nil
	} else if !errors.Is(err, storage2.ErrNotFound) {
		return blockStateSnapshot{}, fmt.Errorf("load stored masterchain state %s: %w", storage2.FormatBlockRef(master), err)
	}

	for {
		masterState, err := s.downloadBlockState(ctx, master, master, 0)
		if err == nil {
			s.log.Info().
				Str("masterchain", storage2.FormatBlockRef(masterState.state.Block)).
				Msg("masterchain state snapshot stage completed")
			return masterState, nil
		}
		if errors.Is(err, context.Canceled) {
			return blockStateSnapshot{}, err
		}
		if !errors.Is(err, ErrStateNotAvailable) {
			return blockStateSnapshot{}, err
		}

		s.log.Debug().
			Err(err).
			Str("masterchain", storage2.FormatBlockRef(master)).
			Dur("retry_in", stateSnapshotRetryDelay).
			Msg("masterchain state snapshot is not available, retrying selected masterchain state")

		if err = waitStateSnapshotRetry(ctx); err != nil {
			return blockStateSnapshot{}, err
		}
	}
}

func (s *Syncer) shardStatesStage(ctx context.Context, masterSnapshot blockStateSnapshot, progress *storage2.CurrentState) (*storage2.CurrentState, error) {
	masterState := masterSnapshot.state
	master := masterState.Block
	if progress != nil && !progress.Masterchain.Block.Equals(&master) {
		return nil, fmt.Errorf(
			"pending state sync masterchain mismatch: progress=%s selected=%s",
			storage2.FormatBlockRef(progress.Masterchain.Block),
			storage2.FormatBlockRef(master),
		)
	}

	next := &storage2.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.SeqNo,
		Masterchain:      storage2.BlockStateWithoutCells(masterState),
		Shards:           map[storage2.ShardKey]storage2.BlockState{},
	}
	if progress != nil {
		next.SyncedAt = progress.SyncedAt
	}
	if err := s.storage.SaveStateSyncProgress(ctx, next); err != nil {
		return nil, fmt.Errorf("persist pending state sync progress for %s: %w", storage2.FormatBlockRef(master), err)
	}

	artifacts := map[storage2.BlockRootHash]*storage2.ServedBlockFull{}
	if masterSnapshot.artifact != nil {
		artifacts[storage2.BlockKey(masterSnapshot.artifact.ID)] = masterSnapshot.artifact.Clone()
	}

	s.log.Info().
		Str("masterchain", storage2.FormatBlockRef(master)).
		Msg("loading shard list from masterchain state")
	shardBlocks, err := ShardBlocksFromMasterState(masterState)
	if err != nil {
		return nil, fmt.Errorf("load shard blocks for %s: %w", storage2.FormatBlockRef(master), err)
	}

	type shardPlan struct {
		block      ton.BlockIDExt
		key        storage2.ShardKey
		splitDepth uint32
	}
	splitDepths, err := PersistentStateSplitDepths(masterState, shardBlocks)
	if err != nil {
		return nil, fmt.Errorf("load persistent state split depths for %s: %w", storage2.FormatBlockRef(master), err)
	}

	plans := make([]shardPlan, 0, len(shardBlocks))
	planByKey := make(map[storage2.ShardKey]shardPlan, len(shardBlocks))
	for _, shardBlock := range shardBlocks {
		plan := shardPlan{
			block:      shardBlock,
			key:        storage2.ShardKeyFromBlock(shardBlock),
			splitDepth: splitDepths[shardBlock.Workchain],
		}
		plans = append(plans, plan)
		planByKey[plan.key] = plan
	}

	if progress != nil {
		for _, key := range storage2.SortedShardKeys(progress.Shards) {
			shard := progress.Shards[key]
			plan, ok := planByKey[key]
			if !ok || !shard.Block.Equals(&plan.block) {
				continue
			}

			stored, err := s.storage.BlockState(ctx, shard.Block)
			if err != nil {
				return nil, fmt.Errorf("load pending shard state %s: %w", storage2.FormatBlockRef(shard.Block), err)
			}
			storedShard := storage2.BlockStateWithoutCells(stored)
			setShardMasterchainRef(&storedShard, master)
			next.Shards[key] = storedShard
		}
	}

	masterState.Cell = nil
	masterState.Parsed = nil

	pendingPlans := make([]shardPlan, 0, len(plans))
	for _, plan := range plans {
		if state, ok := next.Shards[plan.key]; ok && state.Block.Equals(&plan.block) {
			s.log.Info().
				Str("block", storage2.FormatBlockRef(plan.block)).
				Str("masterchain", storage2.FormatBlockRef(master)).
				Msg("shard state already completed in pending sync")
			continue
		}
		pendingPlans = append(pendingPlans, plan)
	}
	if err := s.storage.SaveStateSyncProgress(ctx, next); err != nil {
		return nil, fmt.Errorf("persist pending state sync progress for %s: %w", storage2.FormatBlockRef(master), err)
	}

	workers := shardStateDownloadParallelism
	if len(pendingPlans) < workers {
		workers = len(pendingPlans)
	}
	if workers < 1 {
		workers = 1
	}

	s.log.Info().
		Str("masterchain", storage2.FormatBlockRef(master)).
		Int("shards", len(plans)).
		Int("pending_shards", len(pendingPlans)).
		Int("workers", workers).
		Msg("syncing shard states")

	type shardStateResult struct {
		snapshot blockStateSnapshot
		err      error
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan shardStateResult, len(pendingPlans))
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup

	for _, plan := range pendingPlans {
		plan := plan
		wg.Add(1)
		go func() {
			defer wg.Done()

			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				results <- shardStateResult{err: ctx.Err()}
				return
			}
			defer func() { <-sem }()

			s.log.Info().
				Str("block", storage2.FormatBlockRef(plan.block)).
				Str("masterchain", storage2.FormatBlockRef(master)).
				Uint32("split_depth", plan.splitDepth).
				Msg("syncing shard state")

			shardState, err := s.storage.BlockState(ctx, plan.block)
			if err == nil {
				s.log.Info().
					Str("block", storage2.FormatBlockRef(plan.block)).
					Str("masterchain", storage2.FormatBlockRef(master)).
					Msg("using stored shard state")
				results <- shardStateResult{snapshot: blockStateSnapshot{state: shardState}}
				return
			}
			if !errors.Is(err, storage2.ErrNotFound) {
				err = fmt.Errorf("load stored shard state %s: %w", storage2.FormatBlockRef(plan.block), err)
			} else {
				snapshot, err := s.downloadShardState(ctx, plan.block, master, plan.splitDepth)
				if err != nil && !errors.Is(err, context.Canceled) {
					cancel()
				}
				results <- shardStateResult{snapshot: snapshot, err: err}
				return
			}
			if err != nil && !errors.Is(err, context.Canceled) {
				cancel()
			}
			results <- shardStateResult{err: err}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var firstErr error
	for res := range results {
		switch {
		case res.err == nil:
			shard := storage2.BlockStateWithoutCells(res.snapshot.state)
			setShardMasterchainRef(&shard, master)
			next.Shards[storage2.ShardKeyFromBlock(res.snapshot.state.Block)] = shard
			if res.snapshot.artifact != nil {
				artifacts[storage2.BlockKey(res.snapshot.artifact.ID)] = res.snapshot.artifact.Clone()
			}
			res.snapshot.state.Cell = nil
			res.snapshot.state.Parsed = nil
			if err = s.storage.SaveStateSyncProgress(ctx, next); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("persist pending state sync progress for %s: %w", storage2.FormatBlockRef(master), err)
				cancel()
			}
		case errors.Is(res.err, context.Canceled):
			if firstErr == nil && ctx.Err() != nil {
				firstErr = ctx.Err()
			}
		case firstErr == nil:
			firstErr = res.err
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}

	next.SyncedAt = time.Now()
	entries := make([]storage2.StateCheckpointBlock, 0, 1+len(next.Shards))
	if err = s.appendSnapshotCheckpointEntry(ctx, &entries, &next.Masterchain, artifacts); err != nil {
		return nil, fmt.Errorf("prepare current masterchain checkpoint artifact: %w", err)
	}
	for _, key := range storage2.SortedShardKeys(next.Shards) {
		shard := next.Shards[key]
		if err = s.appendSnapshotCheckpointEntry(ctx, &entries, &shard, artifacts); err != nil {
			return nil, fmt.Errorf("prepare current shard checkpoint artifact %s: %w", storage2.FormatBlockRef(shard.Block), err)
		}
	}
	if _, err = s.storage.SaveStateCheckpointEntries(ctx, entries, storage2.StateCellRecords{}, next); err != nil {
		return nil, fmt.Errorf("persist current state: %w", err)
	}
	if err = s.storage.ClearStateSyncProgress(ctx); err != nil {
		return nil, fmt.Errorf("clear pending state sync progress: %w", err)
	}

	return next, nil
}

func (s *Syncer) appendSnapshotCheckpointEntry(ctx context.Context, entries *[]storage2.StateCheckpointBlock, state *storage2.BlockState, artifacts map[storage2.BlockRootHash]*storage2.ServedBlockFull) error {
	if state.Block.SeqNo == 0 {
		*entries = append(*entries, storage2.StateCheckpointBlock{State: state})
		return nil
	}

	artifact := artifacts[storage2.BlockKey(state.Block)]
	if artifact == nil {
		var err error
		artifact, err = s.source.BlockFull(ctx, state.Block)
		if err != nil {
			return err
		}
	}
	if artifact == nil {
		return fmt.Errorf("block artifact %s is missing", storage2.FormatBlockRef(state.Block))
	}

	*entries = append(*entries, storage2.StateCheckpointBlock{
		State:    state,
		Artifact: artifact,
	})
	return nil
}

func (s *Syncer) downloadShardState(ctx context.Context, shardBlock ton.BlockIDExt, master ton.BlockIDExt, splitDepth uint32) (blockStateSnapshot, error) {
	for {
		shardState, err := s.downloadBlockState(ctx, shardBlock, master, splitDepth)
		if err == nil {
			return shardState, nil
		}
		if errors.Is(err, context.Canceled) {
			return blockStateSnapshot{}, err
		}
		if !errors.Is(err, ErrStateNotAvailable) {
			return blockStateSnapshot{}, err
		}

		s.log.Debug().
			Err(err).
			Str("block", storage2.FormatBlockRef(shardBlock)).
			Str("masterchain", storage2.FormatBlockRef(master)).
			Dur("retry_in", stateSnapshotRetryDelay).
			Msg("shard state snapshot is not available, retrying shard state")

		if err = waitStateSnapshotRetry(ctx); err != nil {
			return blockStateSnapshot{}, err
		}
	}
}

func waitStateSnapshotRetry(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(stateSnapshotRetryDelay):
		return nil
	}
}

func (s *Syncer) downloadBlockState(ctx context.Context, block ton.BlockIDExt, master ton.BlockIDExt, splitDepth uint32) (blockStateSnapshot, error) {
	state, err := s.storage.BlockState(ctx, block)
	if err == nil {
		s.log.Info().
			Str("block", storage2.FormatBlockRef(block)).
			Msg("using stored block state")
		return blockStateSnapshot{state: state}, nil
	}
	if !errors.Is(err, storage2.ErrNotFound) {
		return blockStateSnapshot{}, fmt.Errorf("load block state %s: %w", storage2.FormatBlockRef(block), err)
	}

	downloaded, err := s.source.DownloadState(ctx, block, master, splitDepth)
	if err != nil {
		return blockStateSnapshot{}, err
	}
	artifact := downloaded.BlockArtifact()
	defer func() {
		if cleanupErr := downloaded.Cleanup(); cleanupErr != nil {
			s.log.Warn().
				Err(cleanupErr).
				Str("block", storage2.FormatBlockRef(block)).
				Msg("failed to cleanup temporary state snapshot")
		}
	}()

	s.log.Info().
		Str("block", storage2.FormatBlockRef(block)).
		Msg("importing downloaded state snapshot")
	state, err = s.importer.ImportCells(ctx, downloaded, s.storage, master)
	if err != nil {
		return blockStateSnapshot{}, fmt.Errorf("import block state %s: %w", storage2.FormatBlockRef(block), err)
	}
	if err = s.publishDownloadedBlockState(ctx, state, artifact); err != nil {
		return blockStateSnapshot{}, fmt.Errorf("publish block state %s: %w", storage2.FormatBlockRef(block), err)
	}
	s.log.Info().
		Str("block", storage2.FormatBlockRef(block)).
		Msg("block state published")
	return blockStateSnapshot{
		state:    state,
		artifact: artifact,
	}, nil
}

func (s *Syncer) publishDownloadedBlockState(ctx context.Context, state *storage2.BlockState, artifact *storage2.ServedBlockFull) error {
	if state.Block.SeqNo == 0 {
		return s.storage.SaveZeroStateCheckpoint(ctx, []*storage2.BlockState{state}, nil)
	}
	if artifact == nil {
		return fmt.Errorf("downloaded state source did not include full block artifact")
	}
	_, err := s.storage.SaveStateCheckpointEntries(ctx, []storage2.StateCheckpointBlock{{
		State:    state,
		Artifact: artifact,
	}}, storage2.StateCellRecords{}, nil)
	return err
}

func setShardMasterchainRef(state *storage2.BlockState, master ton.BlockIDExt) {
	if state == nil || state.Block.Workchain == -1 {
		return
	}

	// Match cppnode: this is the including master, not the shard header MasterRef.
	ref := master
	state.MasterchainRef = &ref
}
