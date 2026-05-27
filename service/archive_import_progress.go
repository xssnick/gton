package service

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xssnick/gton/internal/logutil"
	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/gton/service/storage"
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
	if p == nil {
		return
	}

	p.mu.Lock()
	p.queue = queue
	p.mu.Unlock()
}

func (p *archiveWindowPipelineProgress) setPlanning(seqno uint32, stage string) {
	if p == nil {
		return
	}

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
	if p == nil {
		return
	}

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
	if p == nil {
		return archiveWindowPipelineSnapshot{}
	}

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

func (r *archiveCatchUpRunner) startPipelineWaitProgress() {
	if r.pipelineWaitStarted.IsZero() {
		r.pipelineWaitStarted = time.Now()
	}
}

func (r *archiveCatchUpRunner) accountPipelineWaitProgress(now time.Time) {
	if r.pipelineWaitStarted.IsZero() {
		return
	}
	if elapsed := now.Sub(r.pipelineWaitStarted); elapsed > 0 {
		r.progressStats.pipelineWait += elapsed
	}
	r.pipelineWaitStarted = now
}

func (r *archiveCatchUpRunner) stopPipelineWaitProgress(now time.Time) {
	r.accountPipelineWaitProgress(now)
	r.pipelineWaitStarted = time.Time{}
}

func (r *archiveCatchUpRunner) recordArchiveWindowProgress(window *shardClientArchiveWindow, applyElapsed time.Duration) {
	if window == nil {
		return
	}

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
	if window.totalStats == nil {
		if window.shardArchives > 0 {
			stats.shardArchives += uint64(window.shardArchives)
		}
		return
	}

	stats.bytes += window.totalStats.Bytes
	if window.totalStats.Blocks > 0 {
		stats.blocks += uint64(window.totalStats.Blocks)
	}
	if window.totalStats.Entries > 0 {
		stats.entries += uint64(window.totalStats.Entries)
	}
	stats.archiveDownload += window.totalStats.DownloadElapsed
	stats.archiveImport += window.totalStats.ImportElapsed
	stats.stateCells += window.totalStats.StateUpdateCells
	stats.stateCellBytes += window.totalStats.StateUpdateCellBytes
	stats.stateCellPrepare += window.totalStats.StateUpdateCellPrepare

	shardArchives := window.totalStats.ShardArchives
	if shardArchives == 0 {
		shardArchives = window.shardArchives
	}
	if shardArchives > 0 {
		stats.shardArchives += uint64(shardArchives)
	}
}

func (w *shardClientArchiveWindow) releaseImportedData() {
	if w == nil {
		return
	}

	w.masterStats = nil
	w.totalStats = nil
	w.masterStates = nil
	w.masterBlocks = nil
	w.masterSequence = nil
	w.masterProofs = nil
	w.archiveBlocks = nil
	w.archiveImports = nil
	w.stateCells = nil
	w.appliedStates = appliedStateSet{}
}

func (r *archiveCatchUpRunner) storeArchiveWindow(window *shardClientArchiveWindow) error {
	if window == nil || len(window.archiveImports) == 0 {
		return nil
	}

	stored := storage.ServedArchiveImport{}
	seenFull := map[string]struct{}{}
	seenLink := map[string]struct{}{}
	for _, imported := range window.archiveImports {
		if imported == nil {
			continue
		}
		appendArchiveImport(&stored, &imported.stored, seenFull, seenLink)
	}
	if len(stored.FullBlocks) == 0 && len(stored.BlockData) == 0 && len(stored.Proofs) == 0 && len(stored.Links) == 0 {
		return nil
	}

	started := time.Now()
	if err := r.service.storage.SaveArchiveImport(&stored); err != nil {
		return fmt.Errorf("store archive blocks: %w", err)
	}
	r.service.log.Debug().
		Int("full_blocks", len(stored.FullBlocks)).
		Int("block_data", len(stored.BlockData)).
		Int("proofs", len(stored.Proofs)).
		Int("links", len(stored.Links)).
		Dur("elapsed", time.Since(started)).
		Msg("stored archive block refs")
	return nil
}

func appendArchiveImport(dst *storage.ServedArchiveImport, src *storage.ServedArchiveImport, seenFull map[string]struct{}, seenLink map[string]struct{}) {
	if dst == nil || src == nil {
		return
	}

	for _, full := range src.FullBlocks {
		if full == nil {
			continue
		}
		key := storage.BlockKey(full.ID)
		if _, exists := seenFull[key]; exists {
			continue
		}
		seenFull[key] = struct{}{}
		cloned := *full
		cloned.ProofRef = full.ProofRef.Clone()
		cloned.BlockRef = full.BlockRef.Clone()
		cloned.Meta = full.Meta.Clone()
		dst.FullBlocks = append(dst.FullBlocks, &cloned)
	}

	for _, block := range src.BlockData {
		dst.BlockData = append(dst.BlockData, cloneServedBlockData(block))
	}

	for _, proof := range src.Proofs {
		dst.Proofs = append(dst.Proofs, cloneServedBlockProof(proof))
	}

	for _, link := range src.Links {
		prevKey := storage.BlockKey(link.Prev)
		nextKey := storage.BlockKey(link.Next)
		linkKey := prevKey + ">" + nextKey
		if _, exists := seenLink[linkKey]; exists {
			continue
		}
		seenLink[linkKey] = struct{}{}
		dst.Links = append(dst.Links, link)
	}
}

func (r *archiveCatchUpRunner) recordArchiveCheckpointProgress(checkpointBlocks uint32, elapsed time.Duration) {
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

func (r *archiveCatchUpRunner) logProgress() error {
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
	windowShardBlocksReused := r.shardBlocksReused - r.lastProgressShardBlocksReused
	windowElapsed := now.Sub(r.lastProgress)
	stats := r.progressStats
	windowStats := stats.since(r.lastProgressStats)
	progress := formatCatchUpProgress(done, total)
	eta := formatCatchUpETA(done, total, time.Since(r.started))

	event := r.service.log.Info().
		Str("current", storage.FormatBlockRef(r.current.Masterchain.Block)).
		Str("target", storage.FormatBlockRef(r.target)).
		Str("catchup_method", "archive_shard_client").
		Bool("checkpoint_in_flight", r.checkpointDone != nil).
		Uint32("persisted_masterchain_seqno", r.lastCheckpointSeqno).
		Uint32("pending_checkpoint_blocks", r.current.ShardClientSeqno-r.lastCheckpointSeqno).
		Uint64("pending_checkpoint_bytes", r.pendingArchiveCheckpointBytes()).
		Uint32("processed_masterchain_blocks", done).
		Uint64("applied_shard_blocks", r.shardBlocksApplied).
		Uint32("total_masterchain_blocks", total).
		Uint64("window_archive_windows", windowStats.windows).
		Uint64("window_shard_archives", windowStats.shardArchives).
		Int64("window_archive_bytes", windowStats.bytes).
		Uint64("window_archive_blocks", windowStats.blocks).
		Uint64("window_archive_entries", windowStats.entries)
	if lagSeconds, ok := r.archiveLiveTailLagSeconds(); ok {
		remainingLag := remainingLagSeconds(lagSeconds)
		event = event.
			Int64("master_lag_seconds", lagSeconds).
			Int64("remaining_lag_seconds", remainingLag)
		if r.hasStartRemainingLag {
			progress = formatLagCatchUpProgress(r.startRemainingLagSeconds, remainingLag)
			eta = formatLagCatchUpETA(r.startRemainingLagSeconds, remainingLag, time.Since(r.started))
		}
	} else {
		event = event.Int64("remaining", int64(targetRemainingBlocks))
	}
	event = event.
		Uint64("checkpoints", stats.checkpoints).
		Uint64("window_checkpoints", windowStats.checkpoints).
		Dur("window_pipeline_wait", windowStats.pipelineWait).
		Dur("window_master_prefetch_wait", windowStats.masterPrefetchWait).
		Dur("window_archive_download", windowStats.archiveDownload).
		Dur("window_archive_import", windowStats.archiveImport).
		Dur("window_apply_wall", windowStats.applyWall).
		Dur("window_master_apply", windowStats.masterApply).
		Dur("window_master_precheck", windowStats.masterPrecheck).
		Dur("window_master_prepare", windowStats.masterPrepare).
		Dur("window_master_consensus", windowStats.masterConsensus).
		Dur("window_master_state_update", windowStats.masterStateUpdate).
		Dur("window_shard_target_parse", windowStats.shardTargetParse).
		Dur("window_shard_apply", windowStats.shardApply).
		Uint64("state_cells", stats.stateCells).
		Uint64("window_state_cells", windowStats.stateCells).
		Uint64("state_cell_bytes", stats.stateCellBytes).
		Uint64("window_state_cell_bytes", windowStats.stateCellBytes).
		Dur("window_state_cell_prepare", windowStats.stateCellPrepare).
		Dur("window_checkpoint_persist", windowStats.checkpointPersist).
		Str("progress", progress).
		Str("speed", formatBlockRate(done, time.Since(r.started))).
		Str("window_speed", formatBlockRate(windowBlocks, windowElapsed)).
		Str("shard_apply_speed", formatBlockRate64(windowShardBlocksApplied, windowElapsed)).
		Str("shard_seen_speed", formatBlockRate64(windowShardBlocksApplied+windowShardBlocksReused, windowElapsed)).
		Str("archive_download_speed", logutil.FormatByteRate(stats.bytes, stats.archiveDownload)).
		Str("window_archive_download_speed", logutil.FormatByteRate(windowStats.bytes, windowStats.archiveDownload)).
		Str("window_archive_ingest_speed", logutil.FormatByteRate(windowStats.bytes, windowElapsed)).
		Str("archive_import_block_speed", formatBlockRate64(stats.blocks, stats.archiveImport)).
		Str("window_archive_import_block_speed", formatBlockRate64(windowStats.blocks, windowStats.archiveImport)).
		Str("state_cell_prepare_speed", logutil.FormatCellRate(stats.stateCells, stats.stateCellPrepare)).
		Str("window_state_cell_prepare_speed", logutil.FormatCellRate(windowStats.stateCells, windowStats.stateCellPrepare)).
		Str("state_cell_prepare_byte_speed", logutil.FormatByteRate(int64(stats.stateCellBytes), stats.stateCellPrepare)).
		Str("window_state_cell_prepare_byte_speed", logutil.FormatByteRate(int64(windowStats.stateCellBytes), windowStats.stateCellPrepare)).
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
	return errors.Is(err, archive.ErrNotAvailable) || isExpectedRetryError(err)
}

func (s *Service) shouldPersistArchiveCatchUpCheckpoint(seqno uint32, targetSeqno uint32, lastCheckpointSeqno uint32, lastCheckpoint time.Time, checkpointBlocks uint32, pendingBytes uint64) bool {
	if seqno <= lastCheckpointSeqno {
		return false
	}
	if seqno >= targetSeqno {
		return true
	}
	if checkpointBlocks == 0 {
		checkpointBlocks = s.archiveCatchUpCheckpointBlocks
	}
	if seqno-lastCheckpointSeqno >= checkpointBlocks {
		return true
	}
	if pendingBytes >= s.checkpointBytesTarget() {
		return true
	}
	return time.Since(lastCheckpoint) >= s.archiveCatchUpCheckpointPeriod
}
