package service

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xssnick/gton/internal/logutil"
	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/gton/service/p2p"
)

type archiveWindowPipelineProgress struct {
	mu             sync.RWMutex
	stage          string
	frontSeqno     uint32
	pendingWindows int
	readyWindows   int
	frontTask      *archiveWindowShardImportTask
	pendingTasks   []*archiveWindowShardImportTask
	queue          *archiveImportQueue
}

type archiveWindowPipelineSnapshot struct {
	stage                  string
	frontSeqno             uint32
	pendingWindows         int
	readyWindows           int
	activeDownload         int64
	activePrepare          int64
	downloadHotQueued      int
	downloadPrefetchQueued int
	prepareHotQueued       int
	preparePrefetchQueued  int
}

type archiveImportQueueSnapshot struct {
	activeDownload         int64
	activePrepare          int64
	downloadHotQueued      int
	downloadPrefetchQueued int
	prepareHotQueued       int
	preparePrefetchQueued  int
}

type archiveCatchUpProgressStats struct {
	windows            uint64
	shardArchives      uint64
	checkpoints        uint64
	bytes              int64
	blocks             uint64
	entries            uint64
	pipelineWait       time.Duration
	masterPrefetchWait time.Duration
	archiveDownload    time.Duration
	archiveImport      time.Duration
	applyWall          time.Duration
	masterApply        time.Duration
	masterPrecheck     time.Duration
	masterPrepare      time.Duration
	masterConsensus    time.Duration
	masterStateUpdate  time.Duration
	shardTargetParse   time.Duration
	shardApply         time.Duration
	stateCells         uint64
	stateCellBytes     uint64
	stateCellPrepare   time.Duration
	checkpointPersist  time.Duration
}

type archiveCatchUpStageTiming struct {
	name    string
	elapsed time.Duration
}

func (s archiveCatchUpProgressStats) since(previous archiveCatchUpProgressStats) archiveCatchUpProgressStats {
	return archiveCatchUpProgressStats{
		windows:            s.windows - previous.windows,
		shardArchives:      s.shardArchives - previous.shardArchives,
		checkpoints:        s.checkpoints - previous.checkpoints,
		bytes:              s.bytes - previous.bytes,
		blocks:             s.blocks - previous.blocks,
		entries:            s.entries - previous.entries,
		pipelineWait:       s.pipelineWait - previous.pipelineWait,
		masterPrefetchWait: s.masterPrefetchWait - previous.masterPrefetchWait,
		archiveDownload:    s.archiveDownload - previous.archiveDownload,
		archiveImport:      s.archiveImport - previous.archiveImport,
		applyWall:          s.applyWall - previous.applyWall,
		masterApply:        s.masterApply - previous.masterApply,
		masterPrecheck:     s.masterPrecheck - previous.masterPrecheck,
		masterPrepare:      s.masterPrepare - previous.masterPrepare,
		masterConsensus:    s.masterConsensus - previous.masterConsensus,
		masterStateUpdate:  s.masterStateUpdate - previous.masterStateUpdate,
		shardTargetParse:   s.shardTargetParse - previous.shardTargetParse,
		shardApply:         s.shardApply - previous.shardApply,
		stateCells:         s.stateCells - previous.stateCells,
		stateCellBytes:     s.stateCellBytes - previous.stateCellBytes,
		stateCellPrepare:   s.stateCellPrepare - previous.stateCellPrepare,
		checkpointPersist:  s.checkpointPersist - previous.checkpointPersist,
	}
}

func newArchiveWindowPipelineProgress() *archiveWindowPipelineProgress {
	return &archiveWindowPipelineProgress{stage: "starting"}
}

func (p *archiveWindowPipelineProgress) setQueue(queue *archiveImportQueue) {
	p.mu.Lock()
	p.queue = queue
	p.mu.Unlock()
}

func (p *archiveWindowPipelineProgress) setPlanning(seqno uint32, stage string) {
	p.mu.Lock()
	p.frontSeqno = seqno
	p.stage = stage
	p.frontTask = nil
	p.pendingTasks = nil
	p.pendingWindows = 0
	p.readyWindows = 0
	p.mu.Unlock()
}

func (p *archiveWindowPipelineProgress) setPending(pending []archivePendingWindow, planningSeqno uint32, planningStage string) {
	ready := 0
	var frontSeqno uint32
	var frontTask *archiveWindowShardImportTask
	tasks := make([]*archiveWindowShardImportTask, 0, len(pending))
	for idx, item := range pending {
		if item.shards != nil {
			tasks = append(tasks, item.shards)
		}
		if item.shards != nil && item.shards.finishedSnapshot() {
			ready++
		}
		if idx == 0 {
			frontTask = item.shards
			if item.window != nil {
				frontSeqno = item.window.startSeqno
			}
		}
	}
	if frontSeqno == 0 {
		frontSeqno = planningSeqno
	}

	p.mu.Lock()
	p.frontSeqno = frontSeqno
	p.stage = planningStage
	p.frontTask = frontTask
	p.pendingTasks = tasks
	p.pendingWindows = len(pending)
	p.readyWindows = ready
	p.mu.Unlock()
}

func (p *archiveWindowPipelineProgress) snapshot() archiveWindowPipelineSnapshot {
	p.mu.RLock()
	snapshot := archiveWindowPipelineSnapshot{
		stage:          p.stage,
		frontSeqno:     p.frontSeqno,
		pendingWindows: p.pendingWindows,
		readyWindows:   p.readyWindows,
	}
	frontTask := p.frontTask
	pendingTasks := append([]*archiveWindowShardImportTask(nil), p.pendingTasks...)
	queue := p.queue
	p.mu.RUnlock()

	ready := 0
	for _, task := range pendingTasks {
		if task.finishedSnapshot() {
			ready++
		}
	}
	snapshot.readyWindows = ready
	if frontTask != nil {
		if stage := frontTask.stageSnapshot(); stage != "" {
			snapshot.stage = stage
		}
	}
	if snapshot.stage == "" {
		snapshot.stage = "idle"
	}
	if queue != nil {
		queueSnapshot := queue.snapshot()
		snapshot.activeDownload = queueSnapshot.activeDownload
		snapshot.activePrepare = queueSnapshot.activePrepare
		snapshot.downloadHotQueued = queueSnapshot.downloadHotQueued
		snapshot.downloadPrefetchQueued = queueSnapshot.downloadPrefetchQueued
		snapshot.prepareHotQueued = queueSnapshot.prepareHotQueued
		snapshot.preparePrefetchQueued = queueSnapshot.preparePrefetchQueued
	}
	return snapshot
}

func (s archiveWindowPipelineSnapshot) activeString() string {
	return fmt.Sprintf("download=%d prepare=%d", s.activeDownload, s.activePrepare)
}

func (s archiveWindowPipelineSnapshot) queueString() string {
	return fmt.Sprintf("download(h/p)=%d/%d prepare(h/p)=%d/%d", s.downloadHotQueued, s.downloadPrefetchQueued, s.prepareHotQueued, s.preparePrefetchQueued)
}

func (s archiveWindowPipelineSnapshot) windowsString() string {
	return fmt.Sprintf("pending=%d ready=%d", s.pendingWindows, s.readyWindows)
}

func (r *archiveCatchUpRun) startPipelineWaitProgress() {
	if r.pipelineWaitStarted.IsZero() {
		r.pipelineWaitStarted = time.Now()
	}
}

func (r *archiveCatchUpRun) accountPipelineWaitProgress(now time.Time) {
	if r.pipelineWaitStarted.IsZero() {
		return
	}
	if elapsed := now.Sub(r.pipelineWaitStarted); elapsed > 0 {
		r.progressStats.pipelineWait += elapsed
	}
	r.pipelineWaitStarted = now
}

func (r *archiveCatchUpRun) stopPipelineWaitProgress(now time.Time) {
	r.accountPipelineWaitProgress(now)
	r.pipelineWaitStarted = time.Time{}
}

func (r *archiveCatchUpRun) recordArchiveWindowProgress(window *shardClientArchiveWindow, applyElapsed time.Duration) {
	stats := &r.progressStats
	stats.windows++
	stats.masterPrefetchWait += window.masterWait
	stats.masterApply += window.masterApplyElapsed
	stats.masterPrecheck += window.masterPrecheckElapsed
	stats.masterPrepare += window.masterPrepareElapsed
	stats.masterConsensus += window.masterConsensusElapsed
	stats.masterStateUpdate += window.masterStateUpdateElapsed
	stats.shardTargetParse += window.shardTargetElapsed
	stats.shardApply += window.shardApplyElapsed
	stats.applyWall += applyElapsed
	stats.bytes += window.totalStats.Bytes
	stats.blocks += uint64(window.totalStats.Blocks)
	stats.entries += uint64(window.totalStats.Entries)
	stats.archiveDownload += window.totalStats.DownloadElapsed
	stats.archiveImport += window.totalStats.ImportElapsed
	stats.stateCells += window.totalStats.StateUpdateCells
	stats.stateCellBytes += window.totalStats.StateUpdateCellBytes
	stats.stateCellPrepare += window.totalStats.StateUpdateCellPrepare

	stats.shardArchives += uint64(window.totalStats.ShardArchives)
}

func (w *shardClientArchiveWindow) releaseImportedData() {
	w.masterStats = nil
	w.totalStats = nil
	w.masterStates = nil
	w.masterBlocks = nil
	w.masterSequence = nil
	w.masterProofs = nil
	w.archiveBlocks = nil
	w.archiveImports = nil
	w.shardTargets = nil
	w.stateCells = nil
	w.appliedStates = appliedStateSet{}
}

func (r *archiveCatchUpRun) recordArchiveCheckpointProgress(checkpointBlocks uint32, elapsed time.Duration) {
	if checkpointBlocks == 0 {
		return
	}

	r.progressStats.checkpoints++
	r.progressStats.checkpointPersist += elapsed
}

func archiveCatchUpDominantStage(stages ...archiveCatchUpStageTiming) string {
	var best archiveCatchUpStageTiming
	for _, stage := range stages {
		if stage.elapsed > best.elapsed {
			best = stage
		}
	}
	if best.elapsed <= 0 {
		return "none"
	}
	return best.name
}

func (r *archiveCatchUpRun) logProgress() error {
	if _, err := r.finishCheckpoint(false); err != nil {
		return err
	}

	now := time.Now()
	if now.Sub(r.lastProgress) < archiveCatchUpProgressInterval && r.current.ShardClientSeqno < r.target.SeqNo {
		return nil
	}

	r.accountPipelineWaitProgress(now)

	done := r.current.ShardClientSeqno - r.startSeqno
	total := r.target.SeqNo - r.startSeqno
	targetRemainingBlocks := r.target.SeqNo - r.current.ShardClientSeqno
	windowBlocks := r.current.ShardClientSeqno - r.lastProgressSeqno
	windowShardBlocksApplied := r.shardBlocksApplied - r.lastProgressShardBlocksApplied
	windowElapsed := now.Sub(r.lastProgress)
	stats := r.progressStats
	windowStats := stats.since(r.lastProgressStats)
	progress := formatCatchUpProgress(done, total)
	eta := formatCatchUpETA(done, total, time.Since(r.started))
	progressGoal := r.archiveProgressGoalAt(now)

	event := r.archive.log.Info().
		Uint32("current", r.current.Masterchain.Block.SeqNo).
		Uint32("target", r.target.SeqNo).
		Str("catchup_method", "archive_shard_client").
		Uint32("pending_checkpoint_blocks", r.current.ShardClientSeqno-r.lastCheckpointSeqno).
		Uint64("pending_checkpoint_bytes", r.pendingArchiveCheckpointBytes()).
		Uint64("window_shard_archives", windowStats.shardArchives).
		Int64("window_archive_bytes", windowStats.bytes).
		Uint64("window_archive_blocks", windowStats.blocks)
	if progressGoal.kind == archiveProgressGoalSyncUntil {
		event = event.Uint32("sync_until", r.archive.syncUntil)
		if progressGoal.knownRemaining() {
			event = event.Int64("remaining_sync_until_seconds", progressGoal.remainingSeconds)
		}
		if r.startProgressGoal.kind == archiveProgressGoalSyncUntil &&
			r.startProgressGoal.knownRemaining() &&
			progressGoal.knownRemaining() {
			progress = formatLagCatchUpProgress(r.startProgressGoal.remainingSeconds, progressGoal.remainingSeconds)
			eta = formatLagCatchUpETA(r.startProgressGoal.remainingSeconds, progressGoal.remainingSeconds, time.Since(r.started))
		} else {
			eta = "unknown"
		}
	} else if blockUTime := blockStateUtime(r.ctx, r.archive.storage, &r.current.Masterchain); blockUTime != 0 {
		lagSeconds := now.Unix() - blockUTime
		remainingLag := remainingLagSeconds(lagSeconds)
		event = event.
			Int64("master_lag_seconds", lagSeconds).
			Int64("remaining_lag_seconds", remainingLag)
		if r.startProgressGoal.kind == archiveProgressGoalLiveTail && r.startProgressGoal.knownRemaining() {
			progress = formatLagCatchUpProgress(r.startProgressGoal.remainingSeconds, remainingLag)
			eta = formatLagCatchUpETA(r.startProgressGoal.remainingSeconds, remainingLag, time.Since(r.started))
		}
	} else {
		event = event.Int64("remaining", int64(targetRemainingBlocks))
	}
	event = event.
		Dur("window_pipeline_wait", windowStats.pipelineWait).
		Dur("window_master_prefetch_wait", windowStats.masterPrefetchWait).
		Dur("window_archive_download", windowStats.archiveDownload).
		Dur("window_apply_wall", windowStats.applyWall).
		Dur("window_master_apply", windowStats.masterApply).
		Dur("window_master_precheck", windowStats.masterPrecheck).
		Dur("window_master_prepare", windowStats.masterPrepare).
		Dur("window_master_consensus", windowStats.masterConsensus).
		Dur("window_master_state_update", windowStats.masterStateUpdate).
		Dur("window_shard_target_parse", windowStats.shardTargetParse).
		Dur("window_shard_apply", windowStats.shardApply).
		Dur("window_checkpoint_persist", windowStats.checkpointPersist).
		Str("progress", progress).
		Str("speed", formatBlockRate(done, time.Since(r.started))).
		Str("window_speed", formatBlockRate(windowBlocks, windowElapsed)).
		Str("shard_apply_speed", formatBlockRate64(windowShardBlocksApplied, windowElapsed)).
		Str("archive_download_speed", logutil.FormatByteRate(stats.bytes, stats.archiveDownload)).
		Str("window_archive_download_speed", logutil.FormatByteRate(windowStats.bytes, windowStats.archiveDownload)).
		Str("window_bottleneck", archiveCatchUpDominantStage(
			archiveCatchUpStageTiming{name: "pipeline_wait", elapsed: windowStats.pipelineWait},
			archiveCatchUpStageTiming{name: "apply_wall", elapsed: windowStats.applyWall},
			archiveCatchUpStageTiming{name: "checkpoint_persist", elapsed: windowStats.checkpointPersist},
		)).
		Str("window_pipeline_bottleneck", archiveCatchUpDominantStage(
			archiveCatchUpStageTiming{name: "master_prefetch_wait", elapsed: windowStats.masterPrefetchWait},
			archiveCatchUpStageTiming{name: "archive_download", elapsed: windowStats.archiveDownload},
			archiveCatchUpStageTiming{name: "archive_import", elapsed: windowStats.archiveImport},
			archiveCatchUpStageTiming{name: "master_precheck", elapsed: windowStats.masterPrecheck},
			archiveCatchUpStageTiming{name: "master_apply", elapsed: windowStats.masterApply},
		)).
		Str("window_master_apply_bottleneck", archiveCatchUpDominantStage(
			archiveCatchUpStageTiming{name: "prepare", elapsed: windowStats.masterPrepare},
			archiveCatchUpStageTiming{name: "consensus", elapsed: windowStats.masterConsensus},
			archiveCatchUpStageTiming{name: "state_update", elapsed: windowStats.masterStateUpdate},
		)).
		Str("eta", eta)
	if r.pipeline != nil && r.pipeline.progress != nil {
		pipeline := r.pipeline.progress.snapshot()
		event = event.
			Uint32("pipeline_front_seqno", pipeline.frontSeqno).
			Str("pipeline_stage", pipeline.stage).
			Str("pipeline_windows", pipeline.windowsString()).
			Str("pipeline_active", pipeline.activeString()).
			Str("pipeline_queues", pipeline.queueString())
	}
	event.Msg("archive shard-client catch-up progress")

	r.lastProgress = now
	r.lastProgressSeqno = r.current.ShardClientSeqno
	r.lastProgressShardBlocksApplied = r.shardBlocksApplied
	r.lastProgressShardBlocksReused = r.shardBlocksReused
	r.lastProgressStats = stats
	return nil
}

func isArchiveCatchUpRetryError(err error) bool {
	return errors.Is(err, archive.ErrNotAvailable) ||
		errors.Is(err, p2p.ErrNoArchivePeers) ||
		isExpectedRetryError(err)
}

func (a *ArchiveRunner) shouldPersistArchiveCatchUpCheckpoint(seqno uint32, targetSeqno uint32, lastCheckpointSeqno uint32, lastCheckpoint time.Time, checkpointBlocks uint32, pendingBytes uint64) bool {
	if seqno <= lastCheckpointSeqno {
		return false
	}
	if seqno >= targetSeqno {
		return true
	}
	if seqno-lastCheckpointSeqno >= checkpointBlocks {
		return true
	}
	if pendingBytes >= a.state.checkpointTargetBytes() {
		return true
	}
	return time.Since(lastCheckpoint) >= a.checkpointPeriod
}
