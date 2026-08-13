package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

const (
	archiveCatchUpProgressInterval = 5 * time.Second

	DefaultArchiveCatchUpCheckpointBlocks = 2000
	DefaultArchiveCatchUpCheckpointPeriod = 2 * time.Minute
	// Deliberately conservative: each pending window pins its imported pack
	// payloads (blocks + prepared state-update cells) until the runner applies
	// it, and archive packs can be huge. Operators with RAM to spare raise the
	// ArchiveCatchUpPrefetchWindows option instead.
	DefaultArchiveCatchUpPrefetchWindows = 2

	archiveShardApplyParallelism              = 8
	archiveMasterConsensusPrecheckParallelism = 8
	archiveMasterLookaheadWindows             = 16
	archiveHotLookaheadWindows                = 2
	archiveReadyWindowBacklog                 = 1
	archiveShardArchiveImportInFlight         = 4
	archiveDownloadWorkerMin                  = 16
	archiveDownloadWorkerMax                  = 20
	archiveDownloadWorkerMultiplier           = 4
	archiveCatchUpMaxAdaptiveCheckpointBlocks = 4000
	archiveCatchUpCheckpointSlowThreshold     = time.Second
	archiveCatchUpCheckpointFastThreshold     = 400 * time.Millisecond
	archiveCheckpointWaitLogInterval          = 10 * time.Second
)

type archiveCatchUpRun struct {
	archive *ArchiveRunner
	ctx     context.Context
	current *storage.CurrentState
	target  ton.BlockIDExt

	archiveSession  *p2p.ArchiveSession
	archiveImporter *archive.Importer
	importCache     *archiveImportCache
	pipeline        *archiveWindowPipeline
	downloadGate    archiveDownloadBackpressureGate

	started                        time.Time
	startSeqno                     uint32
	lastProgress                   time.Time
	lastProgressSeqno              uint32
	lastCheckpoint                 time.Time
	lastCheckpointSeqno            uint32
	checkpointBlocksTarget         uint32
	startProgressGoal              archiveProgressGoal
	shardBlocksApplied             uint64
	shardBlocksReused              uint64
	lastProgressShardBlocksApplied uint64
	lastProgressShardBlocksReused  uint64
	progressStats                  archiveCatchUpProgressStats
	lastProgressStats              archiveCatchUpProgressStats
	pipelineWaitStarted            time.Time
	checkpointDone                 chan archiveCheckpointResult
	checkpointJoined               chan struct{}
	checkpointStates               appliedStateSet
	stateCells                     *stateCellWindowCache
}

func (r *archiveCatchUpRun) run() (result *storage.CurrentState, runErr error) {
	a := r.archive
	r.archiveImporter = archive.NewImporter()

	r.archiveSession = a.network.BeginArchiveSession()
	defer func() {
		if shutdownErr := r.shutdown(); shutdownErr != nil {
			runErr = errors.Join(runErr, shutdownErr)
			result = nil
		}
	}()

	a.log.Info().
		Str("from", storage.FormatBlockRef(r.current.Masterchain.Block)).
		Str("target", storage.FormatBlockRef(r.target)).
		Uint32("shard_client_seqno", r.current.ShardClientSeqno).
		Uint32("total_masterchain_blocks", r.target.SeqNo-r.current.ShardClientSeqno).
		Msg("starting archive shard-client catch-up")

	r.pipeline = r.startShardClientArchiveWindowPipeline()

	handoffToNext := false
	yieldToCellGenerationSwitch := false
	handoffCheckpointBlocks := uint32(0)
	for r.current.ShardClientSeqno < r.target.SeqNo {
		before := r.current.ShardClientSeqno
		window, err := r.nextArchiveWindowWithProgress()
		if err != nil {
			if errors.Is(err, errArchiveNextBlockReady) {
				handoffToNext = true
				break
			}
			if window != nil {
				r.dropArchiveWindowShardImportCache(window)
				window.releaseImportedData()
			}
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
			if window.syncUntilReached {
				window.releaseImportedData()
				a.syncUntilTransitions.enterSyncUntilOffline(r.current, PreparedBlock{})
				break
			}
			window.releaseImportedData()
			return nil, fmt.Errorf("archive window #%d did not provide next masterchain blocks", window.startSeqno)
		}

		applyStarted := time.Now()
		next, err := r.applyShardClientArchiveWindow(r.ctx, r.current, window)
		if err != nil {
			r.dropArchiveWindowShardImportCache(window)
			window.releaseImportedData()
			if isArchiveCatchUpRetryError(err) {
				if err = r.restartPipeline(err); err != nil {
					return nil, err
				}
				continue
			}
			return nil, r.returnWithProgress(err)
		}
		applyElapsed := time.Since(applyStarted)
		r.stateCells.adoptRecordsFrom(window.stateCells)
		window.stateCells.releaseRecordsToBase(r.stateCells.loader())
		a.log.Debug().
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
			if window.syncUntilReached {
				a.syncUntilTransitions.enterSyncUntilOffline(r.current, PreparedBlock{})
				break
			}
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
		if window.syncUntilReached {
			if r.checkpointDone != nil || r.current.ShardClientSeqno > r.lastCheckpointSeqno {
				if _, err = r.persistCheckpoint("sync_until"); err != nil {
					return nil, err
				}
			}
			a.syncUntilTransitions.enterSyncUntilOffline(r.current, PreparedBlock{})
			break
		}
		if a.state.cellGenerationSwitchRequestActive() {
			if r.checkpointDone != nil || r.current.ShardClientSeqno > r.lastCheckpointSeqno {
				a.log.Info().
					Str("masterchain", storage.FormatBlockRef(r.current.Masterchain.Block)).
					Uint32("shard_client_seqno", r.current.ShardClientSeqno).
					Uint32("persisted_masterchain_seqno", r.lastCheckpointSeqno).
					Uint32("pending_checkpoint_blocks", r.current.ShardClientSeqno-r.lastCheckpointSeqno).
					Uint64("pending_checkpoint_bytes", r.pendingArchiveCheckpointBytes()).
					Uint32("checkpoint_target_blocks", r.checkpointBlocksTarget).
					Uint64("checkpoint_target_bytes", a.state.checkpointTargetBytes()).
					Bool("checkpoint_in_flight", r.checkpointDone != nil).
					Msg("persisting archive shard-client checkpoint before cell generation switch")
				if _, err = r.persistCheckpoint("cell_generation_switch"); err != nil {
					return nil, err
				}
			}
			yieldToCellGenerationSwitch = true
			a.log.Info().
				Str("current", storage.FormatBlockRef(r.current.Masterchain.Block)).
				Str("target", storage.FormatBlockRef(r.target)).
				Msg("yielding archive shard-client catch-up for cell generation switch")
			break
		}
		blockUTime := blockStateUtime(r.ctx, a.storage, &r.current.Masterchain)
		if lagSeconds := time.Now().Unix() - blockUTime; blockUTime != 0 && shouldSwitchArchiveToNextByLag(lagSeconds) {
			if r.checkpointDone != nil || r.current.ShardClientSeqno > r.lastCheckpointSeqno {
				a.log.Info().
					Str("masterchain", storage.FormatBlockRef(r.current.Masterchain.Block)).
					Uint32("shard_client_seqno", r.current.ShardClientSeqno).
					Uint32("persisted_masterchain_seqno", r.lastCheckpointSeqno).
					Uint32("pending_checkpoint_blocks", r.current.ShardClientSeqno-r.lastCheckpointSeqno).
					Uint64("pending_checkpoint_bytes", r.pendingArchiveCheckpointBytes()).
					Uint32("checkpoint_target_blocks", r.checkpointBlocksTarget).
					Uint64("checkpoint_target_bytes", a.state.checkpointTargetBytes()).
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
			a.log.Info().
				Str("current", storage.FormatBlockRef(r.current.Masterchain.Block)).
				Str("target", storage.FormatBlockRef(r.target)).
				Int64("lag_seconds", lagSeconds).
				Int64("switch_to_next_lag_seconds", archiveToNextLagSeconds).
				Uint32("checkpoint_blocks", handoffCheckpointBlocks).
				Bool("checkpoint_in_flight", r.checkpointDone != nil).
				Msg("switching from archive catch-up to next-block pipeline")
			break
		}
		if r.checkpointDone == nil && a.shouldPersistArchiveCatchUpCheckpoint(r.current.ShardClientSeqno, r.target.SeqNo, r.lastCheckpointSeqno, r.lastCheckpoint, r.checkpointBlocksTarget, r.pendingArchiveCheckpointBytes()) {
			if err = r.startCheckpoint("interval"); err != nil {
				return nil, err
			}
		}
		if r.shouldWaitArchiveCheckpointBackpressure() {
			backpressureBlocks := r.archiveCheckpointBackpressureBlocks()
			pendingBytes := r.pendingArchiveCheckpointBytes()
			a.log.Info().
				Str("masterchain", storage.FormatBlockRef(r.current.Masterchain.Block)).
				Uint32("shard_client_seqno", r.current.ShardClientSeqno).
				Uint32("persisted_masterchain_seqno", r.lastCheckpointSeqno).
				Uint32("pending_checkpoint_blocks", r.current.ShardClientSeqno-r.lastCheckpointSeqno).
				Uint64("pending_checkpoint_bytes", pendingBytes).
				Uint32("checkpoint_target_blocks", r.checkpointBlocksTarget).
				Uint64("checkpoint_target_bytes", a.state.checkpointTargetBytes()).
				Uint32("checkpoint_backpressure_blocks", backpressureBlocks).
				Uint64("checkpoint_backpressure_bytes", r.archiveCheckpointBackpressureBytes()).
				Bool("checkpoint_in_flight", r.checkpointDone != nil).
				Msg("waiting for archive shard-client checkpoint backpressure")
			resumeDownloads := r.pauseArchiveDownloadsForCheckpointBackpressure()
			if _, err = r.finishCheckpoint(true); err != nil {
				resumeDownloads()
				return nil, err
			}
			resumeDownloads()
		}

		if err = r.logProgress(); err != nil {
			return nil, err
		}
	}

	if r.current.ShardClientSeqno > r.lastCheckpointSeqno {
		a.log.Info().
			Str("masterchain", storage.FormatBlockRef(r.current.Masterchain.Block)).
			Uint32("shard_client_seqno", r.current.ShardClientSeqno).
			Uint32("persisted_masterchain_seqno", r.lastCheckpointSeqno).
			Uint32("pending_checkpoint_blocks", r.current.ShardClientSeqno-r.lastCheckpointSeqno).
			Uint64("pending_checkpoint_bytes", r.pendingArchiveCheckpointBytes()).
			Uint32("checkpoint_target_blocks", r.checkpointBlocksTarget).
			Uint64("checkpoint_target_bytes", a.state.checkpointTargetBytes()).
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
	a.log.Info().
		Str("masterchain", storage.FormatBlockRef(r.current.Masterchain.Block)).
		Uint32("shard_client_seqno", r.current.ShardClientSeqno).
		Int("shards", len(r.current.Shards)).
		Msg(doneMsg)
	return r.current, nil
}

func (r *archiveCatchUpRun) shouldHandoffToNextBlock() bool {
	latest, err := r.archive.network.ObservedMasterchainBlock()
	return err == nil && shouldPreferNextBlockTarget(r.current.Masterchain.Block.SeqNo, latest.SeqNo)
}

func (r *archiveCatchUpRun) shutdown() error {
	if r.pipeline != nil {
		r.pipeline.stop()
	}
	_, checkpointErr := r.finishCheckpoint(true)
	if r.archiveSession != nil {
		r.archiveSession.Close()
	}
	if r.archiveImporter != nil {
		r.archiveImporter.Close()
	}
	return checkpointErr
}

func (r *archiveCatchUpRun) dropArchiveWindowShardImportCache(window *shardClientArchiveWindow) {
	for _, imported := range window.archiveImports {
		if imported.cacheKey.shard.IsMasterchain() {
			continue
		}
		r.importCache.drop(imported.cacheKey)
	}
}
