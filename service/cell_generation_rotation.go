package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/p2p"
	state2 "github.com/xssnick/gton/service/state"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

var errCellGenerationMigrationRunning = errors.New("cell generation migration is running")

type cellGenerationRotationStore interface {
	ActiveCellGeneration(ctx context.Context) (storage.CellGenerationInfo, error)
	PendingCellGenerationMigration(ctx context.Context) (storage.CellGenerationInfo, error)
	BeginCellGeneration(ctx context.Context, origin ton.BlockIDExt) (uint64, error)
	AbortCellGeneration(ctx context.Context, generation uint64) error
	CleanupCellGeneration(ctx context.Context, generation uint64) error
	ImportStateCellTreeInGeneration(ctx context.Context, generation uint64, block ton.BlockIDExt, root *cell.Cell, parsedCells []cell.Cell, totalCells uint64) (*cell.Cell, error)
	LazyCellLoaderInGeneration(generation uint64) cell.LazyCellLoader
	SaveEncodedCellsInGeneration(ctx context.Context, generation uint64, records []storage.EncodedCellRecord, sync bool) error
	SwitchCellGeneration(ctx context.Context, generation uint64, origin ton.BlockIDExt, expectedCurrent ton.BlockIDExt, current *storage.CurrentState) (uint64, error)
	PersistentStateFile(ctx context.Context, block ton.BlockIDExt, masterchainBlock ton.BlockIDExt, effectiveShard int64) (*storage.PersistentStateFile, error)
}

type cellGenerationCandidate struct {
	generation uint64
	current    *storage.CurrentState
	cells      *stateCellWindowCache
}

type generationStateCellImporter struct {
	store      cellGenerationRotationStore
	generation uint64
}

func (i generationStateCellImporter) ImportStateCellTree(ctx context.Context, block ton.BlockIDExt, root *cell.Cell, parsedCells []cell.Cell, totalCells uint64) (*cell.Cell, error) {
	return i.store.ImportStateCellTreeInGeneration(ctx, i.generation, block, root, parsedCells, totalCells)
}

func (i generationStateCellImporter) LoadStateCellTree(context.Context, ton.BlockIDExt, []byte) (*cell.Cell, error) {
	return nil, storage.ErrNotFound
}

func (s *Service) afterPersistentStateSerialized(ctx context.Context, persistent ton.BlockIDExt, scope PersistentStateSerializationScope) {
	if scope != PersistentStateSerializationAll {
		s.log.Debug().
			Str("persistent_state", storage.FormatBlockRef(persistent)).
			Str("scope", scope.String()).
			Msg("skipping cell generation migration check for partial persistent state serialization")
		return
	}

	store, ok := s.storage.(cellGenerationRotationStore)
	if !ok {
		s.log.Debug().
			Str("storage", fmt.Sprintf("%T", s.storage)).
			Msg("storage does not support cell generation migration")
		return
	}

	needed, err := s.shouldStartCellGenerationMigration(ctx, store, persistent)
	if err != nil {
		s.log.Error().
			Err(err).
			Str("persistent_state", storage.FormatBlockRef(persistent)).
			Msg("failed to check cell generation migration need")
		return
	}
	if !needed {
		return
	}

	if err := s.queueCellGenerationMigration(ctx, store, persistent, "automatic"); err != nil {
		s.log.Info().
			Err(err).
			Str("persistent_state", storage.FormatBlockRef(persistent)).
			Msg("cell generation migration cannot be queued")
	}
}

func (s *Service) StartCellGenerationMigration(ctx context.Context, masterSeqno uint32) error {
	store, ok := s.storage.(cellGenerationRotationStore)
	if !ok {
		return fmt.Errorf("storage does not support cell generation migration")
	}

	migrationLease, err := s.beginExclusiveServiceTask(ctx, exclusiveServiceTaskCellGenerationMigration)
	if err != nil {
		return err
	}

	persistent, err := s.durableMasterchainBlockForMigration(ctx, masterSeqno)
	if err != nil {
		migrationLease.release()
		return err
	}

	active, err := store.ActiveCellGeneration(ctx)
	if err != nil {
		migrationLease.release()
		return err
	}
	if active.OriginPersistentState.Equals(&persistent) {
		migrationLease.release()
		return fmt.Errorf("active cell generation %d already starts from persistent state %s", active.ID, storage.FormatBlockRef(persistent))
	}

	return s.startCellGenerationMigrationWithLease(store, persistent, "manual", migrationLease)
}

func (s *Service) durableMasterchainBlockForMigration(ctx context.Context, masterSeqno uint32) (ton.BlockIDExt, error) {
	return s.durableMasterchainBlock(ctx, masterSeqno, "cell generation migration")
}

func (s *Service) queueCellGenerationMigration(ctx context.Context, store cellGenerationRotationStore, persistent ton.BlockIDExt, source string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	generation, err := store.BeginCellGeneration(s.currentStatePersistContext(), persistent)
	if err != nil {
		return fmt.Errorf("persist cell generation migration intent: %w", err)
	}

	s.log.Info().
		Uint64("cell_generation", generation).
		Str("persistent_state", storage.FormatBlockRef(persistent)).
		Str("source", source).
		Msg("cell generation migration queued")
	s.wakeServiceMaintenance()
	return nil
}

func (s *Service) startCellGenerationMigrationWithLease(store cellGenerationRotationStore, persistent ton.BlockIDExt, source string, migrationLease *exclusiveServiceTaskLease) error {
	runCtx := s.currentStatePersistContext()
	generation, err := store.BeginCellGeneration(runCtx, persistent)
	if err != nil {
		migrationLease.release()
		return fmt.Errorf("persist cell generation migration intent: %w", err)
	}

	s.runAsync(func() {
		defer migrationLease.release()

		started := time.Now()
		if err := s.runCellGenerationMigration(runCtx, store, persistent); err != nil {
			if errors.Is(err, context.Canceled) {
				s.log.Info().
					Uint64("cell_generation", generation).
					Str("persistent_state", storage.FormatBlockRef(persistent)).
					Str("source", source).
					Msg("cell generation migration stopped")
				return
			}
			s.log.Error().
				Err(err).
				Uint64("cell_generation", generation).
				Str("persistent_state", storage.FormatBlockRef(persistent)).
				Str("source", source).
				Dur("elapsed", time.Since(started)).
				Msg("cell generation migration failed")
			return
		}

		s.log.Info().
			Uint64("cell_generation", generation).
			Str("persistent_state", storage.FormatBlockRef(persistent)).
			Str("source", source).
			Dur("elapsed", time.Since(started)).
			Msg("cell generation migration finished")
	})
	return nil
}

func (s *Service) shouldStartCellGenerationMigration(ctx context.Context, store cellGenerationRotationStore, persistent ton.BlockIDExt) (bool, error) {
	if s.stateTTL <= 0 {
		return false, nil
	}

	active, err := store.ActiveCellGeneration(ctx)
	if err != nil {
		return false, err
	}
	if active.OriginPersistentState.Equals(&persistent) {
		s.log.Info().
			Uint64("cell_generation", active.ID).
			Str("persistent_state", storage.FormatBlockRef(persistent)).
			Msg("skipping cell generation migration because active generation already starts from this persistent state")
		return false, nil
	}

	persistentMeta, err := s.storage.BlockMeta(ctx, persistent)
	if err != nil {
		return false, fmt.Errorf("load persistent state block meta %s: %w", storage.FormatBlockRef(persistent), err)
	}
	if persistentMeta.GenUTime == 0 {
		return false, fmt.Errorf("persistent state block %s has no gen utime", storage.FormatBlockRef(persistent))
	}

	if !emptyBlockID(active.OriginPersistentState) {
		originMeta, err := s.storage.BlockMeta(ctx, active.OriginPersistentState)
		if err != nil {
			return false, fmt.Errorf("load active cell generation origin meta %s: %w", storage.FormatBlockRef(active.OriginPersistentState), err)
		}
		if originMeta.GenUTime == 0 {
			return false, fmt.Errorf("active cell generation origin %s has no gen utime", storage.FormatBlockRef(active.OriginPersistentState))
		}
		if persistentMeta.GenUTime <= originMeta.GenUTime {
			return false, nil
		}

		age := time.Duration(persistentMeta.GenUTime-originMeta.GenUTime) * time.Second
		if age < s.stateTTL {
			s.log.Info().
				Uint64("cell_generation", active.ID).
				Str("origin_persistent_state", storage.FormatBlockRef(active.OriginPersistentState)).
				Str("persistent_state", storage.FormatBlockRef(persistent)).
				Dur("generation_age", age).
				Dur("state_ttl", s.stateTTL).
				Msg("skipping cell generation migration because state ttl boundary is not reached")
			return false, nil
		}
	}

	s.log.Info().
		Uint64("cell_generation", active.ID).
		Str("origin_persistent_state", storage.FormatBlockRef(active.OriginPersistentState)).
		Str("persistent_state", storage.FormatBlockRef(persistent)).
		Dur("state_ttl", s.stateTTL).
		Msg("cell generation migration is needed after persistent state serialization")
	return true, nil
}

func (s *Service) beginCellGenerationMigration(ctx context.Context) (*exclusiveServiceTaskLease, error) {
	return s.beginExclusiveServiceTask(ctx, exclusiveServiceTaskCellGenerationMigration)
}

func (s *Service) cellGenerationMigrationActive() bool {
	return s.exclusiveServiceTaskActive(exclusiveServiceTaskCellGenerationMigration)
}

func (s *Service) beginCellGenerationSwitch() bool {
	s.cellMigrationMu.Lock()
	defer s.cellMigrationMu.Unlock()

	if s.cellGenerationSwitching {
		return false
	}
	s.cellGenerationSwitching = true
	return true
}

func (s *Service) finishCellGenerationSwitch() {
	s.cellMigrationMu.Lock()
	s.cellGenerationSwitching = false
	s.cellMigrationMu.Unlock()
}

func (s *Service) cellGenerationSwitchActive() bool {
	s.cellMigrationMu.Lock()
	defer s.cellMigrationMu.Unlock()
	return s.cellGenerationSwitching
}

func (s *Service) checkCurrentStatePersistAllowed() error {
	if err := s.takeCurrentStatePersistError(); err != nil {
		return err
	}
	if s.cellGenerationSwitchActive() {
		return errCellGenerationMigrationRunning
	}
	return nil
}

func (s *Service) runCellGenerationMigration(ctx context.Context, store cellGenerationRotationStore, origin ton.BlockIDExt) (err error) {
	if err := s.waitCurrentStatePersist(ctx); err != nil {
		return err
	}

	generation, err := store.BeginCellGeneration(ctx, origin)
	if err != nil {
		return fmt.Errorf("begin candidate cell generation: %w", err)
	}
	abort := true
	defer func() {
		if !abort {
			return
		}
		if ctxErr := ctx.Err(); errors.Is(err, context.Canceled) || ctxErr != nil {
			if ctxErr != nil && !errors.Is(err, ctxErr) {
				err = errors.Join(err, ctxErr)
			}
			s.log.Info().
				Uint64("cell_generation", generation).
				Str("persistent_state", storage.FormatBlockRef(origin)).
				Msg("leaving pending cell generation migration for startup resume")
			return
		}
		if err := store.AbortCellGeneration(context.Background(), generation); err != nil {
			s.log.Warn().
				Err(err).
				Uint64("cell_generation", generation).
				Msg("failed to remove aborted cell generation")
		}
	}()

	candidate, err := s.importSerializedPersistentCurrent(ctx, store, generation, origin)
	if err != nil {
		return err
	}

	for {
		target, err := s.storage.CurrentState(ctx)
		if err != nil {
			return fmt.Errorf("load durable current state: %w", err)
		}
		if target.Masterchain.Block.SeqNo < candidate.current.Masterchain.Block.SeqNo {
			return fmt.Errorf("durable current state %s is before migration persistent state %s", storage.FormatBlockRef(target.Masterchain.Block), storage.FormatBlockRef(candidate.current.Masterchain.Block))
		}

		if err = s.catchUpCellGenerationCandidate(ctx, store, candidate, target); err != nil {
			return err
		}
		if err = s.flushCellGenerationCandidate(ctx, store, candidate); err != nil {
			return err
		}

		switched, latest, oldGeneration, err := s.trySwitchCellGenerationCandidate(ctx, store, candidate, origin)
		if err != nil {
			return err
		}
		if !switched {
			s.log.Info().
				Uint64("cell_generation", generation).
				Str("candidate", storage.FormatBlockRef(candidate.current.Masterchain.Block)).
				Str("latest", storage.FormatBlockRef(latest.Masterchain.Block)).
				Msg("durable current state advanced during cell generation migration, continuing candidate catch-up")
			continue
		}

		abort = false
		if oldGeneration != 0 {
			if err := store.CleanupCellGeneration(ctx, oldGeneration); err != nil {
				return fmt.Errorf("cleanup old cell generation %d: %w", oldGeneration, err)
			}
			s.log.Info().
				Uint64("old_cell_generation", oldGeneration).
				Msg("old cell generation cleaned up")
		}
		return nil
	}
}

func (s *Service) importSerializedPersistentCurrent(ctx context.Context, store cellGenerationRotationStore, generation uint64, master ton.BlockIDExt) (*cellGenerationCandidate, error) {
	masterState, err := s.storage.BlockState(ctx, master)
	if err != nil {
		return nil, fmt.Errorf("load serialized masterchain state %s: %w", storage.FormatBlockRef(master), err)
	}

	importer := generationStateCellImporter{store: store, generation: generation}
	importedMaster, err := s.importSerializedPersistentBlockState(ctx, store, importer, generation, masterState.Block, master, 0, masterState.StateRootHash)
	if err != nil {
		return nil, fmt.Errorf("import serialized masterchain persistent state: %w", err)
	}

	shards, err := state2.ShardBlocksFromMasterState(importedMaster)
	if err != nil {
		return nil, fmt.Errorf("load shard list from imported persistent state %s: %w", storage.FormatBlockRef(master), err)
	}

	current := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: importedMaster.Block.SeqNo,
		Masterchain:      *storage.CloneBlockState(importedMaster),
		Shards:           make(map[storage.ShardKey]storage.BlockState, len(shards)),
	}
	splitDepths, err := state2.PersistentStateSplitDepths(importedMaster, shards)
	if err != nil {
		return nil, fmt.Errorf("load persistent state split depths for imported persistent state %s: %w", storage.FormatBlockRef(master), err)
	}

	for _, shard := range shards {
		canonicalShard, err := s.storage.BlockState(ctx, shard)
		if err != nil {
			return nil, fmt.Errorf("load serialized shard state metadata %s: %w", storage.FormatBlockRef(shard), err)
		}
		importedShard, err := s.importSerializedPersistentBlockState(ctx, store, importer, generation, shard, master, splitDepths[shard.Workchain], canonicalShard.StateRootHash)
		if err != nil {
			return nil, fmt.Errorf("import serialized shard persistent state %s: %w", storage.FormatBlockRef(shard), err)
		}
		current.Shards[storage.ShardKeyFromBlock(shard)] = *storage.CloneBlockState(importedShard)
	}

	s.log.Info().
		Uint64("cell_generation", generation).
		Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
		Int("shards", len(current.Shards)).
		Msg("imported serialized persistent state into candidate cell generation")

	candidate := &cellGenerationCandidate{
		generation: generation,
		current:    current,
		cells:      newStateCellWindowCache(store.LazyCellLoaderInGeneration(generation)),
	}
	return candidate, nil
}

func (s *Service) importSerializedPersistentBlockState(
	ctx context.Context,
	store cellGenerationRotationStore,
	importer generationStateCellImporter,
	generation uint64,
	block ton.BlockIDExt,
	master ton.BlockIDExt,
	splitDepth uint32,
	stateRootHash []byte,
) (*storage.BlockState, error) {
	artifact, err := s.serializedPersistentStateArtifact(ctx, store, block, master, splitDepth, stateRootHash)
	if err != nil {
		return nil, err
	}
	state, err := artifact.ImportCells(ctx, importer)
	if err != nil {
		return nil, err
	}
	state.CellGeneration = generation
	return state, nil
}

func (s *Service) serializedPersistentStateArtifact(ctx context.Context, store cellGenerationRotationStore, block ton.BlockIDExt, master ton.BlockIDExt, splitDepth uint32, stateRootHash []byte) (storage.DownloadedState, error) {
	if block.Workchain == -1 || splitDepth <= uint32(state2.ShardPrefixLength(block.Shard)) {
		file, err := store.PersistentStateFile(ctx, block, master, 0)
		if err != nil {
			return nil, err
		}
		return p2p.NewPersistentStateSnapshotArtifactFromFile(s.node, file)
	}

	return p2p.NewSplitPersistentStateSnapshotArtifactFromStoredFiles(ctx, s.node, block, master, splitDepth, stateRootHash, func(effectiveShard int64) (*storage.PersistentStateFile, error) {
		return store.PersistentStateFile(ctx, block, master, effectiveShard)
	})
}

func (s *Service) catchUpCellGenerationCandidate(ctx context.Context, store cellGenerationRotationStore, candidate *cellGenerationCandidate, target *storage.CurrentState) error {
	if target == nil {
		return fmt.Errorf("target current state is nil")
	}

	shardCache := map[string]*storage.BlockState{}
	resolver := s.newCellGenerationShardResolver(ctx, candidate, shardCache)
	processed := uint32(0)
	for candidate.current.Masterchain.Block.SeqNo < target.Masterchain.Block.SeqNo {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		downloaded, err := s.loadCandidateNextMasterBlock(ctx, candidate.current.Masterchain.Block)
		if err != nil {
			return err
		}
		nextMaster, _, err := s.applyMasterchainTransitionWithConsensusProof(&candidate.current.Masterchain, downloaded, nil, candidate.cells)
		if err != nil {
			return err
		}
		nextMaster.CellGeneration = candidate.generation

		targets, err := state2.ShardBlocksFromMasterState(nextMaster)
		if err != nil {
			return fmt.Errorf("load shard targets from candidate master state %s: %w", storage.FormatBlockRef(nextMaster.Block), err)
		}
		nextCurrent, _, err := s.currentStateForNextMasterState(ctx, candidate.current, nextMaster, targets, resolver)
		if err != nil {
			return err
		}
		setCurrentCellGeneration(nextCurrent, candidate.generation)

		candidate.current = nextCurrent
		resolver.updateCurrent(candidate.current.Shards)
		processed++

		if processed%nextBlockCatchUpCheckpointBlocks == 0 {
			if err = s.flushCellGenerationCandidate(ctx, store, candidate); err != nil {
				return err
			}
		}
	}

	if !candidate.current.Masterchain.Block.Equals(&target.Masterchain.Block) {
		return fmt.Errorf("candidate masterchain caught up to %s, durable target is %s", storage.FormatBlockRef(candidate.current.Masterchain.Block), storage.FormatBlockRef(target.Masterchain.Block))
	}
	return nil
}

func (s *Service) newCellGenerationShardResolver(ctx context.Context, candidate *cellGenerationCandidate, cache map[string]*storage.BlockState) *shardStateResolver {
	return newShardStateResolver(ctx, shardStateResolverConfig{
		current: candidate.current.Shards,
		cache:   cache,
		loadState: func(_ context.Context, state storage.BlockState) (*storage.BlockState, error) {
			if state.Cell != nil {
				return storage.CloneBlockState(&state), nil
			}
			return nil, storage.ErrNotFound
		},
		loadBlock: s.loadCandidateBlockForApply,
		apply: func(ctx context.Context, target ton.BlockIDExt, previous []*storage.BlockState, downloaded p2p.DownloadedBlock) (*storage.BlockState, error) {
			next, err := s.applyResolvedShardBlock(ctx, target, previous, downloaded, candidate.cells)
			if err != nil {
				return nil, err
			}
			next.CellGeneration = candidate.generation
			return next, nil
		},
		afterApplyState: func(_ context.Context, state *storage.BlockState, _ p2p.DownloadedBlock, _ time.Duration) error {
			return nil
		},
	})
}

func (s *Service) loadCandidateNextMasterBlock(ctx context.Context, prev ton.BlockIDExt) (p2p.DownloadedBlock, error) {
	next, err := s.storage.LookupBlockBySeqNo(ctx, storage.BlockHistoryKey{Workchain: -1, Shard: topShard}, prev.SeqNo+1)
	if err != nil {
		return p2p.DownloadedBlock{}, fmt.Errorf("load stored next masterchain block after %s: %w", storage.FormatBlockRef(prev), err)
	}
	return s.loadCandidateBlockForApply(ctx, next)
}

func (s *Service) loadCandidateBlockForApply(ctx context.Context, block ton.BlockIDExt) (p2p.DownloadedBlock, error) {
	downloaded, err := loadStoredBlockForApply(ctx, s.storage, block, true)
	if err != nil {
		return p2p.DownloadedBlock{}, fmt.Errorf("load stored candidate block %s: %w", storage.FormatBlockRef(block), err)
	}
	return downloaded, nil
}

func (s *Service) flushCellGenerationCandidate(ctx context.Context, store cellGenerationRotationStore, candidate *cellGenerationCandidate) error {
	checkpoint := candidate.cells.beginCheckpoint()
	if checkpoint == nil {
		return nil
	}

	records := checkpoint.records()
	if len(records) > 0 {
		if err := store.SaveEncodedCellsInGeneration(ctx, candidate.generation, records, true); err != nil {
			return err
		}
	}
	checkpoint.complete()
	return nil
}

func (s *Service) trySwitchCellGenerationCandidate(ctx context.Context, store cellGenerationRotationStore, candidate *cellGenerationCandidate, origin ton.BlockIDExt) (bool, *storage.CurrentState, uint64, error) {
	s.stateMu.Lock()
	s.currentStatePersistMu.Lock()

	if err := s.checkCurrentStatePersistAllowed(); err != nil {
		s.currentStatePersistMu.Unlock()
		s.stateMu.Unlock()
		return false, nil, 0, err
	}

	latest, err := s.storage.CurrentState(ctx)
	if err != nil {
		s.currentStatePersistMu.Unlock()
		s.stateMu.Unlock()
		return false, nil, 0, fmt.Errorf("load durable current state before cell generation switch: %w", err)
	}
	if latest.Masterchain.Block.SeqNo > candidate.current.Masterchain.Block.SeqNo {
		s.currentStatePersistMu.Unlock()
		s.stateMu.Unlock()
		return false, latest, 0, nil
	}
	if !latest.Masterchain.Block.Equals(&candidate.current.Masterchain.Block) {
		s.currentStatePersistMu.Unlock()
		s.stateMu.Unlock()
		return false, nil, 0, fmt.Errorf("candidate current state %s does not match durable current state %s", storage.FormatBlockRef(candidate.current.Masterchain.Block), storage.FormatBlockRef(latest.Masterchain.Block))
	}
	if !s.beginCellGenerationSwitch() {
		s.currentStatePersistMu.Unlock()
		s.stateMu.Unlock()
		return false, nil, 0, errCellGenerationMigrationRunning
	}
	s.currentStatePersistMu.Unlock()
	s.stateMu.Unlock()
	defer s.finishCellGenerationSwitch()

	oldGeneration, err := store.SwitchCellGeneration(ctx, candidate.generation, origin, candidate.current.Masterchain.Block, currentStateWithoutCells(candidate.current))
	if err != nil {
		if errors.Is(err, storage.ErrCurrentStateAdvanced) {
			latest, loadErr := s.storage.CurrentState(ctx)
			if loadErr != nil {
				return false, nil, 0, fmt.Errorf("load durable current state after rejected cell generation switch: %w", loadErr)
			}
			return false, latest, 0, nil
		}
		return false, nil, 0, err
	}
	s.publishCommittedCurrentState(candidate.current)
	return true, latest, oldGeneration, nil
}

func setCurrentCellGeneration(current *storage.CurrentState, generation uint64) {
	if current == nil {
		return
	}
	current.Masterchain.CellGeneration = generation
	for key, shard := range current.Shards {
		shard.CellGeneration = generation
		current.Shards[key] = shard
	}
}

func emptyBlockID(block ton.BlockIDExt) bool {
	return block.Workchain == 0 &&
		block.Shard == 0 &&
		block.SeqNo == 0 &&
		zeroHash(block.RootHash) &&
		zeroHash(block.FileHash)
}

func zeroHash(hash []byte) bool {
	for _, b := range hash {
		if b != 0 {
			return false
		}
	}
	return true
}
