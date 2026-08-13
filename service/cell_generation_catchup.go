package service

import (
	"context"
	"fmt"
	"time"

	state2 "github.com/xssnick/gton/service/state"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type cellGenerationCandidateBlockLoad struct {
	block PreparedBlock
	err   error
}

func (s *StateLifecycle) catchUpAndFlushCellGenerationCandidate(ctx context.Context, store cellGenerationStore, candidate *cellGenerationCandidate, target *storage.CurrentState) error {
	release, err := s.throttleActiveCellGenerationCompactions(ctx, store, candidate, target)
	if err != nil {
		return err
	}
	defer release()

	if err := s.catchUpCellGenerationCandidate(ctx, store, candidate, target); err != nil {
		return fmt.Errorf("catch up cell generation candidate: %w", err)
	}
	if err := s.flushCellGenerationCandidate(ctx, candidate); err != nil {
		return fmt.Errorf("flush cell generation candidate: %w", err)
	}
	if err := store.SaveCellGenerationMigrationProgress(ctx, candidate.generation, candidate.current); err != nil {
		return fmt.Errorf("save cell generation migration progress: %w", err)
	}
	return nil
}

func (s *StateLifecycle) catchUpCellGenerationCandidate(ctx context.Context, store cellGenerationStore, candidate *cellGenerationCandidate, target *storage.CurrentState) error {
	if target.Masterchain.Block.SeqNo < candidate.current.Masterchain.Block.SeqNo {
		return fmt.Errorf("durable current state %s is before candidate current state %s", storage.FormatBlockRef(target.Masterchain.Block), storage.FormatBlockRef(candidate.current.Masterchain.Block))
	}

	shardCache := map[storage.BlockRootHash]*storage.BlockState{}
	resolver := s.newCellGenerationShardResolver(ctx, candidate, shardCache)
	processed := uint32(0)
	checkpointBlocks := uint32(0)
	started := time.Now()
	startSeqno := candidate.current.Masterchain.Block.SeqNo
	lastLog := started
	lastLogProcessed := processed
	shardBlocksApplied := uint64(0)
	shardBlocksReused := uint64(0)
	workCtx, cancelWork := context.WithCancel(ctx)
	var prefetched <-chan cellGenerationCandidateBlockLoad
	var checkpointFlush <-chan error
	defer func() {
		cancelWork()
		if prefetched != nil {
			<-prefetched
		}
		if checkpointFlush != nil {
			<-checkpointFlush
		}
	}()

	if target.Masterchain.Block.SeqNo > startSeqno {
		s.log.Info().
			Uint64("cell_generation", candidate.generation).
			Str("from", storage.FormatBlockRef(candidate.current.Masterchain.Block)).
			Str("target", storage.FormatBlockRef(target.Masterchain.Block)).
			Str("catchup_method", "cell_generation_migration").
			Uint32("total_masterchain_blocks", target.Masterchain.Block.SeqNo-startSeqno).
			Uint32("remaining", target.Masterchain.Block.SeqNo-startSeqno).
			Int("shards", len(candidate.current.Shards)).
			Msg("catching up next celldb generation from stored blocks")
	}

	var downloaded PreparedBlock
	if candidate.current.Masterchain.Block.SeqNo < target.Masterchain.Block.SeqNo {
		var err error
		downloaded, err = s.loadCandidateNextMasterBlock(ctx, candidate.current.Masterchain.Block)
		if err != nil {
			return err
		}
	}

	for candidate.current.Masterchain.Block.SeqNo < target.Masterchain.Block.SeqNo {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if downloaded.ID.SeqNo < target.Masterchain.Block.SeqNo {
			prefetched = s.prefetchCellGenerationCandidateBlock(workCtx, downloaded.ID)
		}

		nextMaster, _, err := s.blockTransitions.applyMasterchainTransition(ctx, &candidate.current.Masterchain, downloaded, nil, candidate.cells, nil)
		if err != nil {
			return fmt.Errorf("apply candidate masterchain transition after %s: %w", storage.FormatBlockRef(candidate.current.Masterchain.Block), err)
		}
		targets, err := state2.ShardBlocksFromMasterState(nextMaster)
		if err != nil {
			return fmt.Errorf("load shard targets from candidate master state %s: %w", storage.FormatBlockRef(nextMaster.Block), err)
		}
		nextCurrent, shardStats, err := s.blockTransitions.currentStateForNextMasterState(ctx, candidate.current, nextMaster, targets, resolver)
		if err != nil {
			return fmt.Errorf("apply candidate shard states for masterchain %s: %w", storage.FormatBlockRef(nextMaster.Block), err)
		}
		candidate.current = nextCurrent
		resolver.updateCurrent(candidate.current.Shards)
		shardBlocksApplied += uint64(shardStats.applied)
		shardBlocksReused += uint64(shardStats.reused)
		processed++
		checkpointBlocks++

		now := time.Now()
		if now.Sub(lastLog) >= cellGenerationMigrationProgressInterval && candidate.current.Masterchain.Block.SeqNo < target.Masterchain.Block.SeqNo {
			s.logCellGenerationCandidateCatchUpProgress(
				candidate, target, startSeqno, processed, checkpointBlocks, lastLogProcessed,
				started, lastLog, now, shardBlocksApplied, shardBlocksReused, false,
			)
			lastLog = now
			lastLogProcessed = processed
		}

		if checkpointBlocks >= s.nextBlockCheckpointBlocks || candidate.cells.activeByteSize() >= s.checkpointBytes {
			if checkpointFlush != nil {
				if err = <-checkpointFlush; err != nil {
					checkpointFlush = nil
					return err
				}
				checkpointFlush = nil
			}
			checkpointFlush = s.startCellGenerationCandidateCheckpoint(workCtx, store, candidate)
			checkpointBlocks = 0
		}

		if prefetched != nil {
			loaded := <-prefetched
			prefetched = nil
			if loaded.err != nil {
				return loaded.err
			}
			downloaded = loaded.block
		}
	}

	if checkpointFlush != nil {
		if err := <-checkpointFlush; err != nil {
			checkpointFlush = nil
			return err
		}
		checkpointFlush = nil
	}

	if !candidate.current.Masterchain.Block.Equals(&target.Masterchain.Block) {
		return fmt.Errorf("candidate masterchain caught up to %s, durable target is %s", storage.FormatBlockRef(candidate.current.Masterchain.Block), storage.FormatBlockRef(target.Masterchain.Block))
	}
	if processed > 0 {
		now := time.Now()
		s.logCellGenerationCandidateCatchUpProgress(
			candidate, target, startSeqno, processed, checkpointBlocks, lastLogProcessed,
			started, lastLog, now, shardBlocksApplied, shardBlocksReused, true,
		)
	}
	return nil
}

func (s *StateLifecycle) prefetchCellGenerationCandidateBlock(ctx context.Context, previous ton.BlockIDExt) <-chan cellGenerationCandidateBlockLoad {
	done := make(chan cellGenerationCandidateBlockLoad, 1)
	s.runAsync(func() {
		block, err := s.loadCandidateNextMasterBlock(ctx, previous)
		done <- cellGenerationCandidateBlockLoad{block: block, err: err}
	})
	return done
}

func (s *StateLifecycle) startCellGenerationCandidateCheckpoint(ctx context.Context, store cellGenerationStore, candidate *cellGenerationCandidate) <-chan error {
	checkpoint := candidate.cells.beginCheckpoint()
	if len(checkpoint.caches) == 0 {
		return nil
	}

	generation := candidate.generation
	current := storage.CloneCurrentState(candidate.current)
	done := make(chan error, 1)
	s.runAsync(func() {
		records := checkpoint.records()
		if len(records) > 0 {
			if err := candidate.generationCells.SaveEncoded(ctx, records, true); err != nil {
				done <- fmt.Errorf("flush cell generation candidate checkpoint at %s: %w", storage.FormatBlockRef(current.Masterchain.Block), err)
				return
			}
		}
		if err := store.SaveCellGenerationMigrationProgress(ctx, generation, current); err != nil {
			done <- fmt.Errorf("save cell generation migration progress at %s: %w", storage.FormatBlockRef(current.Masterchain.Block), err)
			return
		}

		checkpoint.complete()
		done <- nil
	})
	return done
}

func (s *StateLifecycle) logCellGenerationCandidateCatchUpProgress(
	candidate *cellGenerationCandidate,
	target *storage.CurrentState,
	startSeqno uint32,
	processed uint32,
	checkpointBlocks uint32,
	lastLogProcessed uint32,
	started time.Time,
	lastLog time.Time,
	now time.Time,
	shardBlocksApplied uint64,
	shardBlocksReused uint64,
	done bool,
) {
	total := target.Masterchain.Block.SeqNo - startSeqno
	remaining := target.Masterchain.Block.SeqNo - candidate.current.Masterchain.Block.SeqNo
	windowBlocks := processed - lastLogProcessed
	windowElapsed := now.Sub(lastLog)
	elapsed := now.Sub(started)

	event := s.log.Info().
		Uint64("cell_generation", candidate.generation).
		Str("current", storage.FormatBlockRef(candidate.current.Masterchain.Block)).
		Str("target", storage.FormatBlockRef(target.Masterchain.Block)).
		Str("catchup_method", "cell_generation_migration").
		Uint32("processed_masterchain_blocks", processed).
		Uint32("total_masterchain_blocks", total).
		Uint32("remaining", remaining).
		Uint32("pending_checkpoint_blocks", checkpointBlocks).
		Uint64("pending_checkpoint_bytes", candidate.cells.byteSize()).
		Uint64("applied_shard_blocks", shardBlocksApplied).
		Uint64("reused_shard_blocks", shardBlocksReused).
		Dur("elapsed", elapsed).
		Str("progress", formatCatchUpProgress(processed, total)).
		Str("speed", formatBlockRate(processed, elapsed)).
		Str("window_speed", formatBlockRate(windowBlocks, windowElapsed)).
		Str("eta", formatCatchUpETA(processed, total, elapsed))
	if done {
		event.Msg("cell generation migration catch-up finished")
		return
	}
	event.Msg("cell generation migration catch-up progress")
}

func (s *StateLifecycle) newCellGenerationShardResolver(ctx context.Context, candidate *cellGenerationCandidate, cache map[storage.BlockRootHash]*storage.BlockState) *shardStateResolver {
	return newShardStateResolver(ctx, shardStateResolverConfig{
		current: candidate.current.Shards,
		cache:   cache,
		loadState: func(_ context.Context, state storage.BlockState) (*storage.BlockState, error) {
			if state.Cell != nil {
				return storage.CloneBlockState(&state), nil
			}
			if len(state.StateRootHash) == 0 {
				// Block-only lookups are resolver cache probes. Candidate
				// generation must apply the stored block instead of reusing
				// active-generation state metadata.
				return nil, storage.ErrNotFound
			}
			if len(state.StateRootHash) == 32 {
				var hash cell.Hash
				copy(hash[:], state.StateRootHash)
				root, err := candidate.loader(hash)
				if err != nil {
					return nil, err
				}
				state.Cell = root
				return storage.CloneBlockState(&state), nil
			}
			return nil, fmt.Errorf("candidate shard state %s root hash size is %d", storage.FormatBlockRef(state.Block), len(state.StateRootHash))
		},
		loadBlock: s.loadCandidateBlockForApply,
		apply: func(ctx context.Context, target ton.BlockIDExt, previous []*storage.BlockState, downloaded PreparedBlock) (*storage.BlockState, error) {
			next, err := s.blockTransitions.applyResolvedShardBlock(ctx, target, previous, downloaded, candidate.cells, nil)
			if err != nil {
				return nil, err
			}
			return next, nil
		},
		afterApplyState: func(_ context.Context, state *storage.BlockState, _ PreparedBlock, _ time.Duration) error {
			return nil
		},
	})
}

func (s *StateLifecycle) loadCandidateNextMasterBlock(ctx context.Context, prev ton.BlockIDExt) (PreparedBlock, error) {
	next, err := s.blockLoader.loadNext(ctx, prev)
	if err != nil {
		return PreparedBlock{}, fmt.Errorf("load stored next masterchain block after %s: %w", storage.FormatBlockRef(prev), err)
	}
	return next, nil
}

func (s *StateLifecycle) loadCandidateBlockForApply(ctx context.Context, block ton.BlockIDExt) (PreparedBlock, error) {
	downloaded, err := s.blockLoader.load(ctx, block)
	if err != nil {
		return PreparedBlock{}, fmt.Errorf("load stored candidate block %s: %w", storage.FormatBlockRef(block), err)
	}
	return downloaded, nil
}

func (s *StateLifecycle) flushCellGenerationCandidate(ctx context.Context, candidate *cellGenerationCandidate) error {
	checkpoint := candidate.cells.beginCheckpoint()
	if len(checkpoint.caches) == 0 {
		return nil
	}

	records := checkpoint.records()
	if len(records) > 0 {
		if err := candidate.generationCells.SaveEncoded(ctx, records, true); err != nil {
			return err
		}
	}
	checkpoint.complete()
	return nil
}
