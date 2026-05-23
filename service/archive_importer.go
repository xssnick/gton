package service

import (
	"context"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

const (
	archiveCatchUpProgressInterval = 5 * time.Second

	DefaultArchiveCatchUpCheckpointBlocks = 2000
	DefaultArchiveCatchUpCheckpointPeriod = 2 * time.Minute
	DefaultArchiveCatchUpPrefetchWindows  = 8

	archiveShardApplyParallelism              = 8
	archiveMasterConsensusPrecheckParallelism = 8
	archiveMasterLookaheadWindows             = 16
	archiveHotLookaheadWindows                = 2
	archiveReadyWindowBacklog                 = 4
	archiveDownloadWorkerMin                  = 16
	archiveDownloadWorkerMax                  = 64
	archiveDownloadWorkerMultiplier           = 4
	archiveDownloadHotWorkerMin               = 2
	archiveDownloadHotWorkerMax               = 8
	archivePrepareHotWorkerMin                = 1
	archivePrepareHotWorkerMax                = 2
	archiveCatchUpMaxAdaptiveCheckpointBlocks = 4000
	archiveCatchUpCheckpointSlowThreshold     = time.Second
	archiveCatchUpCheckpointFastThreshold     = 400 * time.Millisecond
	archiveCheckpointWaitLogInterval          = 10 * time.Second
)

type archiveCatchUpRunner struct {
	service *Service
	ctx     context.Context
	current *storage.CurrentState
	target  ton.BlockIDExt

	importCache *archiveImportCache
	pipeline    *archiveWindowPipeline

	started                        time.Time
	startSeqno                     uint32
	lastProgress                   time.Time
	lastProgressSeqno              uint32
	lastCheckpoint                 time.Time
	lastCheckpointSeqno            uint32
	checkpointBlocksTarget         uint32
	startRemainingLagSeconds       int64
	hasStartRemainingLag           bool
	shardBlocksApplied             uint64
	shardBlocksReused              uint64
	lastProgressShardBlocksApplied uint64
	lastProgressShardBlocksReused  uint64
	progressStats                  archiveCatchUpProgressStats
	lastProgressStats              archiveCatchUpProgressStats
	pipelineWaitStarted            time.Time
	checkpointDone                 chan archiveCheckpointResult
	checkpointStates               appliedStateSet
	stateCells                     *archiveStateCellOverlay
}

func (s *Service) catchUpShardClientFromArchives(ctx context.Context, current *storage.CurrentState, target ton.BlockIDExt) (*storage.CurrentState, error) {
	current = storage.CloneCurrentState(current)
	if current.ShardClientSeqno == 0 {
		current.ShardClientSeqno = current.Masterchain.Block.SeqNo
	}
	if current.Masterchain.Block.SeqNo != current.ShardClientSeqno {
		return nil, fmt.Errorf("current masterchain seqno %d differs from shard client seqno %d", current.Masterchain.Block.SeqNo, current.ShardClientSeqno)
	}
	if err := s.waitSyncDiskSpace(ctx, "archive_catchup"); err != nil {
		return nil, err
	}

	started := time.Now()
	runner := &archiveCatchUpRunner{
		service:                s,
		ctx:                    ctx,
		current:                current,
		target:                 target,
		importCache:            newArchiveImportCache(),
		started:                started,
		startSeqno:             current.ShardClientSeqno,
		lastProgress:           started,
		lastProgressSeqno:      current.ShardClientSeqno,
		lastCheckpoint:         started,
		lastCheckpointSeqno:    current.ShardClientSeqno,
		checkpointBlocksTarget: s.archiveCatchUpCheckpointBlocks,
		stateCells:             newArchiveStateCellOverlay(s.stateCellLoader()),
	}
	runner.startRemainingLagSeconds, runner.hasStartRemainingLag = runner.archiveRemainingLagSeconds()

	return runner.run()
}

func (r *archiveCatchUpRunner) run() (*storage.CurrentState, error) {
	s := r.service
	s.log.Info().
		Str("from", storage.FormatBlockRef(r.current.Masterchain.Block)).
		Str("target", storage.FormatBlockRef(r.target)).
		Uint32("shard_client_seqno", r.current.ShardClientSeqno).
		Uint32("total_masterchain_blocks", r.target.SeqNo-r.current.ShardClientSeqno).
		Msg("starting archive shard-client catch-up")

	r.pipeline = r.startShardClientArchiveWindowPipeline()
	defer func() {
		if r.pipeline != nil {
			r.pipeline.cancel()
		}
	}()

	handoffToNext := false
	yieldToCellGenerationSwitch := false
	handoffCheckpointBlocks := uint32(0)
	for r.current.ShardClientSeqno < r.target.SeqNo {
		before := r.current.ShardClientSeqno
		window, err := r.nextArchiveWindowWithProgress()
		if err != nil {
			if isArchiveCatchUpRetryError(err) {
				if err = r.restartPipeline(err); err != nil {
					return nil, err
				}
				continue
			}
			return nil, r.returnWithProgress(err)
		}
		if window.startSeqno != r.current.ShardClientSeqno+1 {
			return nil, fmt.Errorf("archive pipeline returned window #%d after current seqno %d", window.startSeqno, r.current.ShardClientSeqno)
		}
		if len(window.masterStates) == 0 {
			return nil, fmt.Errorf("archive window #%d did not provide next masterchain blocks", window.startSeqno)
		}

		applyStarted := time.Now()
		next, err := r.applyShardClientArchiveWindow(r.ctx, r.current, window)
		if err != nil {
			if isArchiveCatchUpRetryError(err) {
				if err = r.restartPipeline(err); err != nil {
					return nil, err
				}
				continue
			}
			return nil, r.returnWithProgress(err)
		}
		applyElapsed := time.Since(applyStarted)
		if err = r.storeArchiveWindow(window); err != nil {
			return nil, r.returnWithProgress(err)
		}
		window.stateCells.copyRecordsTo(r.stateCells)
		s.log.Debug().
			Uint32("start_seqno", window.startSeqno).
			Int("master_blocks", len(window.masterStates)).
			Int("archive_blocks", len(window.archiveBlocks)).
			Int("shard_archives", window.shardArchives).
			Int("shard_blocks_applied", window.shardBlocksApplied).
			Int("shard_blocks_reused", window.shardBlocksReused).
			Dur("elapsed", applyElapsed).
			Dur("master_apply_elapsed", window.masterApplyElapsed).
			Dur("master_prepare_elapsed", window.masterPrepareElapsed).
			Dur("master_consensus_elapsed", window.masterConsensusElapsed).
			Dur("master_state_update_elapsed", window.masterStateUpdateElapsed).
			Dur("shard_target_parse_elapsed", window.shardTargetElapsed).
			Dur("shard_apply_elapsed", window.shardApplyElapsed).
			Msg("archive shard-client window applied")

		if next.ShardClientSeqno <= before {
			return nil, fmt.Errorf("archive window #%d did not advance shard client seqno %d", window.startSeqno, before)
		}
		r.current = next
		r.importCache.dropBefore(r.current.ShardClientSeqno + 1)
		r.shardBlocksApplied += uint64(window.shardBlocksApplied)
		r.shardBlocksReused += uint64(window.shardBlocksReused)
		r.recordArchiveWindowProgress(window, applyElapsed)
		r.checkpointStates.rememberAllEntries(window.appliedStates.takeEntries())
		window.releaseImportedData()

		if _, err = r.finishCheckpoint(false); err != nil {
			return nil, err
		}
		if s.cellGenerationSwitchRequestActive() {
			if r.checkpointDone != nil || r.current.ShardClientSeqno > r.lastCheckpointSeqno {
				s.log.Info().
					Str("masterchain", storage.FormatBlockRef(r.current.Masterchain.Block)).
					Uint32("shard_client_seqno", r.current.ShardClientSeqno).
					Uint32("persisted_masterchain_seqno", r.lastCheckpointSeqno).
					Uint32("pending_checkpoint_blocks", r.current.ShardClientSeqno-r.lastCheckpointSeqno).
					Uint64("pending_checkpoint_bytes", r.pendingArchiveCheckpointBytes()).
					Uint32("checkpoint_target_blocks", r.checkpointBlocksTarget).
					Uint64("checkpoint_target_bytes", s.checkpointBytesTarget()).
					Bool("checkpoint_in_flight", r.checkpointDone != nil).
					Msg("persisting archive shard-client checkpoint before cell generation switch")
				if _, err = r.persistCheckpoint("cell_generation_switch"); err != nil {
					return nil, err
				}
			}
			yieldToCellGenerationSwitch = true
			s.log.Info().
				Str("current", storage.FormatBlockRef(r.current.Masterchain.Block)).
				Str("target", storage.FormatBlockRef(r.target)).
				Msg("yielding archive shard-client catch-up for cell generation switch")
			break
		}
		if lagSeconds, ok := r.archiveLiveTailLagSeconds(); ok && shouldSwitchArchiveToNextByLag(lagSeconds) {
			if r.checkpointDone != nil || r.current.ShardClientSeqno > r.lastCheckpointSeqno {
				s.log.Info().
					Str("masterchain", storage.FormatBlockRef(r.current.Masterchain.Block)).
					Uint32("shard_client_seqno", r.current.ShardClientSeqno).
					Uint32("persisted_masterchain_seqno", r.lastCheckpointSeqno).
					Uint32("pending_checkpoint_blocks", r.current.ShardClientSeqno-r.lastCheckpointSeqno).
					Uint64("pending_checkpoint_bytes", r.pendingArchiveCheckpointBytes()).
					Uint32("checkpoint_target_blocks", r.checkpointBlocksTarget).
					Uint64("checkpoint_target_bytes", s.checkpointBytesTarget()).
					Uint32("checkpoint_backpressure_blocks", r.archiveCheckpointBackpressureBlocks()).
					Uint64("checkpoint_backpressure_bytes", r.archiveCheckpointBackpressureBytes()).
					Bool("checkpoint_in_flight", r.checkpointDone != nil).
					Msg("persisting archive shard-client checkpoint before next-block handoff")
				handoffCheckpointBlocks, err = r.persistCheckpoint("handoff")
				if err != nil {
					return nil, err
				}
			}
			handoffToNext = true
			s.log.Info().
				Str("current", storage.FormatBlockRef(r.current.Masterchain.Block)).
				Str("target", storage.FormatBlockRef(r.target)).
				Int64("lag_seconds", lagSeconds).
				Int64("switch_to_next_lag_seconds", archiveToNextLagSeconds).
				Uint32("checkpoint_blocks", handoffCheckpointBlocks).
				Bool("checkpoint_in_flight", r.checkpointDone != nil).
				Msg("switching from archive catch-up to next-block pipeline")
			break
		}
		if r.checkpointDone == nil && s.shouldPersistArchiveCatchUpCheckpoint(r.current.ShardClientSeqno, r.target.SeqNo, r.lastCheckpointSeqno, r.lastCheckpoint, r.checkpointBlocksTarget, r.pendingArchiveCheckpointBytes()) {
			if _, err = r.startCheckpoint("interval"); err != nil {
				return nil, err
			}
		}
		if r.shouldWaitArchiveCheckpointBackpressure() {
			backpressureBlocks := r.archiveCheckpointBackpressureBlocks()
			pendingBytes := r.pendingArchiveCheckpointBytes()
			s.log.Info().
				Str("masterchain", storage.FormatBlockRef(r.current.Masterchain.Block)).
				Uint32("shard_client_seqno", r.current.ShardClientSeqno).
				Uint32("persisted_masterchain_seqno", r.lastCheckpointSeqno).
				Uint32("pending_checkpoint_blocks", r.current.ShardClientSeqno-r.lastCheckpointSeqno).
				Uint64("pending_checkpoint_bytes", pendingBytes).
				Uint32("checkpoint_target_blocks", r.checkpointBlocksTarget).
				Uint64("checkpoint_target_bytes", s.checkpointBytesTarget()).
				Uint32("checkpoint_backpressure_blocks", backpressureBlocks).
				Uint64("checkpoint_backpressure_bytes", r.archiveCheckpointBackpressureBytes()).
				Bool("checkpoint_in_flight", r.checkpointDone != nil).
				Msg("waiting for archive shard-client checkpoint backpressure")
			if _, err = r.finishCheckpoint(true); err != nil {
				return nil, err
			}
		}

		if err = r.logProgress(); err != nil {
			return nil, err
		}
	}

	if r.current.ShardClientSeqno > r.lastCheckpointSeqno {
		s.log.Info().
			Str("masterchain", storage.FormatBlockRef(r.current.Masterchain.Block)).
			Uint32("shard_client_seqno", r.current.ShardClientSeqno).
			Uint32("persisted_masterchain_seqno", r.lastCheckpointSeqno).
			Uint32("pending_checkpoint_blocks", r.current.ShardClientSeqno-r.lastCheckpointSeqno).
			Uint64("pending_checkpoint_bytes", r.pendingArchiveCheckpointBytes()).
			Uint32("checkpoint_target_blocks", r.checkpointBlocksTarget).
			Uint64("checkpoint_target_bytes", s.checkpointBytesTarget()).
			Uint32("checkpoint_backpressure_blocks", r.archiveCheckpointBackpressureBlocks()).
			Uint64("checkpoint_backpressure_bytes", r.archiveCheckpointBackpressureBytes()).
			Bool("checkpoint_in_flight", r.checkpointDone != nil).
			Msg("persisting final archive shard-client checkpoint")
		if _, err := r.persistCheckpoint("final"); err != nil {
			return nil, err
		}
	}

	doneMsg := "archive shard-client catch-up completed"
	if handoffToNext {
		doneMsg = "archive shard-client catch-up handed off to next-block pipeline"
	}
	if yieldToCellGenerationSwitch {
		doneMsg = "archive shard-client catch-up yielded for cell generation switch"
	}
	s.log.Info().
		Str("masterchain", storage.FormatBlockRef(r.current.Masterchain.Block)).
		Uint32("shard_client_seqno", r.current.ShardClientSeqno).
		Int("shards", len(r.current.Shards)).
		Msg(doneMsg)
	return r.current, nil
}
