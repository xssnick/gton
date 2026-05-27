package service

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/gton/service/blockproof"
	state2 "github.com/xssnick/gton/service/state"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	archiveBackfillGuard            = 30 * time.Minute
	archiveBackfillProgressInterval = 5 * time.Second
	archiveBackfillRetryDelay       = 5 * time.Minute
	archiveBackfillIdlePollDelay    = 10 * time.Minute
)

var (
	errArchiveBackfillDisabled = errors.New("archive backfill is disabled")
	errArchiveBackfillComplete = errors.New("archive backfill is complete")
)

type archiveBackfillStore interface {
	ActiveCellGeneration(ctx context.Context) (storage.CellGenerationInfo, error)
	BlockMeta(ctx context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error)
	ArchiveBackfillProgress(ctx context.Context) (storage.ArchiveBackfillProgress, error)
	SaveArchiveBackfillProgress(ctx context.Context, progress storage.ArchiveBackfillProgress) error
	LoadStateCellTree(ctx context.Context, block ton.BlockIDExt, rootHash []byte) (*cell.Cell, error)
}

type archiveBackfillRetention struct {
	origin      ton.BlockIDExt
	originUTime uint32
	gcCutoff    uint32
	target      uint32
}

type archiveBackfillRunner struct {
	service    *Service
	store      archiveBackfillStore
	retention  archiveBackfillRetention
	progress   storage.ArchiveBackfillProgress
	stateRoot  *cell.Cell
	splitDepth uint32

	started        time.Time
	startRemaining int64
	logMu          sync.Mutex
	stage          string
	shardDone      int
	shardTotal     int
}

type archiveBackfillMasterWindow struct {
	imported     *archiveImportResult
	blocks       []PreparedBlock
	shardTargets []ton.BlockIDExt
	floorSeq     uint32
	floorTime    uint32
}

func (s *Service) runArchiveBackfillOnce(ctx context.Context) (bool, error) {
	if s.archiveTTL <= archiveBackfillGuard {
		return false, nil
	}
	if s.node == nil {
		return false, nil
	}

	store, ok := s.storage.(archiveBackfillStore)
	if !ok {
		s.log.Debug().
			Str("storage", fmt.Sprintf("%T", s.storage)).
			Msg("storage does not support archive backfill")
		return false, nil
	}

	retention, err := s.archiveBackfillRetention(ctx, store)
	if errors.Is(err, errArchiveBackfillDisabled) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	progress, err := s.archiveBackfillProgress(ctx, store, retention)
	if errors.Is(err, errArchiveBackfillComplete) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	lease, err := s.beginExclusiveServiceTask(ctx, exclusiveServiceTaskArchiveBackfill)
	if err != nil {
		return false, fmt.Errorf("start archive backfill: %w", err)
	}
	defer lease.release()

	startRemaining := archiveBackfillRemainingSeconds(progress.VerifiedFloorUTime, retention.target)
	s.log.Info().
		Str("origin_persistent_state", storage.FormatBlockRef(retention.origin)).
		Uint32("verified_floor_seqno", progress.VerifiedFloorSeqno).
		Uint32("verified_floor_utime", progress.VerifiedFloorUTime).
		Uint32("target_unix", retention.target).
		Uint32("gc_cutoff_unix", retention.gcCutoff).
		Int64("remaining_backfill_seconds", startRemaining).
		Str("eta", formatLagCatchUpETA(startRemaining, startRemaining, 0)).
		Msg("archive backfill started")

	stateRoot, splitDepth, err := s.archiveBackfillStateRoot(ctx, store, retention.origin)
	if err != nil {
		return false, err
	}

	runner := &archiveBackfillRunner{
		service:        s,
		store:          store,
		retention:      retention,
		progress:       progress,
		stateRoot:      stateRoot,
		splitDepth:     splitDepth,
		started:        time.Now(),
		startRemaining: startRemaining,
		stage:          "starting",
	}
	return runner.runWindow(ctx)
}

func (s *Service) archiveBackfillRetention(ctx context.Context, store archiveBackfillStore) (archiveBackfillRetention, error) {
	active, err := store.ActiveCellGeneration(ctx)
	if err != nil {
		return archiveBackfillRetention{}, fmt.Errorf("load active cell generation: %w", err)
	}
	if emptyBlockID(active.OriginPersistentState) {
		return archiveBackfillRetention{}, errArchiveBackfillDisabled
	}

	originMeta, err := store.BlockMeta(ctx, active.OriginPersistentState)
	if err != nil {
		return archiveBackfillRetention{}, fmt.Errorf("load active cell generation origin meta %s: %w", storage.FormatBlockRef(active.OriginPersistentState), err)
	}
	if originMeta.GenUTime == 0 {
		return archiveBackfillRetention{}, errArchiveBackfillDisabled
	}

	ttlSeconds := uint64(s.archiveTTL / time.Second)
	guardSeconds := uint64(archiveBackfillGuard / time.Second)
	if ttlSeconds <= guardSeconds || ttlSeconds > uint64(^uint32(0)) {
		return archiveBackfillRetention{}, errArchiveBackfillDisabled
	}
	if uint64(originMeta.GenUTime) <= ttlSeconds {
		return archiveBackfillRetention{}, errArchiveBackfillDisabled
	}

	gcCutoff := originMeta.GenUTime - uint32(ttlSeconds)
	target := gcCutoff + uint32(guardSeconds)
	if target >= originMeta.GenUTime {
		return archiveBackfillRetention{}, errArchiveBackfillDisabled
	}
	return archiveBackfillRetention{
		origin:      active.OriginPersistentState,
		originUTime: originMeta.GenUTime,
		gcCutoff:    gcCutoff,
		target:      target,
	}, nil
}

func (s *Service) archiveBackfillProgress(ctx context.Context, store archiveBackfillStore, retention archiveBackfillRetention) (storage.ArchiveBackfillProgress, error) {
	progress, err := store.ArchiveBackfillProgress(ctx)
	if errors.Is(err, storage.ErrNotFound) {
		progress = storage.ArchiveBackfillProgress{
			OriginPersistentState: retention.origin,
			OriginGenUTime:        retention.originUTime,
			GCCutoffUnix:          retention.gcCutoff,
			TargetUnix:            retention.target,
			VerifiedFloorSeqno:    retention.origin.SeqNo,
			VerifiedFloorUTime:    retention.originUTime,
		}
	} else if err != nil {
		return storage.ArchiveBackfillProgress{}, fmt.Errorf("load archive backfill progress: %w", err)
	} else if !progress.OriginPersistentState.Equals(&retention.origin) {
		progress = storage.ArchiveBackfillProgress{
			OriginPersistentState: retention.origin,
			OriginGenUTime:        retention.originUTime,
			GCCutoffUnix:          retention.gcCutoff,
			TargetUnix:            retention.target,
			VerifiedFloorSeqno:    retention.origin.SeqNo,
			VerifiedFloorUTime:    retention.originUTime,
		}
	}

	progress.OriginGenUTime = retention.originUTime
	progress.GCCutoffUnix = retention.gcCutoff
	progress.TargetUnix = retention.target
	if progress.VerifiedFloorSeqno <= 1 || progress.VerifiedFloorUTime <= retention.target {
		return storage.ArchiveBackfillProgress{}, errArchiveBackfillComplete
	}
	return progress, nil
}

func (s *Service) archiveBackfillStateRoot(ctx context.Context, store archiveBackfillStore, origin ton.BlockIDExt) (*cell.Cell, uint32, error) {
	meta, err := store.BlockMeta(ctx, origin)
	if err != nil {
		return nil, 0, fmt.Errorf("load archive backfill origin meta %s: %w", storage.FormatBlockRef(origin), err)
	}
	if len(meta.StateRootHash) != 32 {
		return nil, 0, fmt.Errorf("archive backfill origin %s has no state root hash", storage.FormatBlockRef(origin))
	}

	root, err := store.LoadStateCellTree(ctx, origin, meta.StateRootHash)
	if err != nil {
		return nil, 0, fmt.Errorf("load archive backfill origin state %s: %w", storage.FormatBlockRef(origin), err)
	}
	parsed, err := storage.ParseStateCell(&origin, root, nil, meta.StateRootHash, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("parse archive backfill origin state %s: %w", storage.FormatBlockRef(origin), err)
	}
	splitDepth, err := monitorMinSplitDepth(parsed, 0)
	if err != nil {
		return nil, 0, err
	}
	if splitDepth > maxArchiveMonitorSplitDepth {
		return nil, 0, fmt.Errorf("monitor split depth %d exceeds supported archive prefix fanout %d", splitDepth, maxArchiveMonitorSplitDepth)
	}
	return root, splitDepth, nil
}

func (r *archiveBackfillRunner) runWindow(ctx context.Context) (bool, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	done := make(chan struct{})
	go r.logProgressLoop(ctx, done)
	defer func() {
		close(done)
	}()

	if r.progress.VerifiedFloorSeqno == 0 {
		return false, nil
	}
	windowEnd := r.progress.VerifiedFloorSeqno - 1
	r.setStage("downloading master archive", 0, 0)
	r.logProgress()

	masterWindow, err := r.downloadMasterWindow(ctx, windowEnd)
	if err != nil {
		return false, err
	}
	if masterWindow.floorTime <= r.retention.gcCutoff {
		r.service.log.Info().
			Uint32("window_floor_utime", masterWindow.floorTime).
			Uint32("gc_cutoff_unix", r.retention.gcCutoff).
			Uint32("target_unix", r.retention.target).
			Msg("archive backfill reached gc guard boundary")
		return false, nil
	}

	r.setStage("saving master archive metadata", 0, 0)
	if err = r.service.storage.SaveArchiveImport(&masterWindow.imported.stored); err != nil {
		return false, fmt.Errorf("save archive backfill master window: %w", err)
	}

	if err = r.downloadShardWindows(ctx, windowEnd, masterWindow); err != nil {
		return false, err
	}

	r.progress.VerifiedFloorSeqno = masterWindow.floorSeq
	r.progress.VerifiedFloorUTime = masterWindow.floorTime
	r.progress.GCCutoffUnix = r.retention.gcCutoff
	r.progress.TargetUnix = r.retention.target
	r.progress.UpdatedAtUnix = uint64(time.Now().Unix())

	r.setStage("saving checkpoint", r.shardDone, r.shardTotal)
	if err = r.store.SaveArchiveBackfillProgress(ctx, r.progress); err != nil {
		return false, fmt.Errorf("save archive backfill progress: %w", err)
	}

	r.service.log.Info().
		Str("origin_persistent_state", storage.FormatBlockRef(r.retention.origin)).
		Uint32("verified_floor_seqno", r.progress.VerifiedFloorSeqno).
		Uint32("verified_floor_utime", r.progress.VerifiedFloorUTime).
		Uint32("target_unix", r.retention.target).
		Int64("remaining_backfill_seconds", r.remainingSeconds()).
		Str("eta", r.eta()).
		Msg("archive backfill checkpoint saved")
	return true, nil
}

func (r *archiveBackfillRunner) downloadMasterWindow(ctx context.Context, windowEnd uint32) (archiveBackfillMasterWindow, error) {
	downloaded, err := r.service.node.DownloadArchive(ctx, windowEnd, archive.ShardID{Workchain: -1, Shard: topShard}, "")
	if err != nil {
		return archiveBackfillMasterWindow{}, fmt.Errorf("download archive backfill master window #%d: %w", windowEnd, err)
	}
	imported, err := r.service.importArchiveBlocks(ctx, downloaded, r.splitDepth)
	if err != nil {
		return archiveBackfillMasterWindow{}, fmt.Errorf("import archive backfill master window #%d: %w", windowEnd, err)
	}

	blocks, err := r.masterWindowBlocks(imported, windowEnd)
	if err != nil {
		return archiveBackfillMasterWindow{}, err
	}
	if err = r.validateMasterWindow(blocks, windowEnd); err != nil {
		return archiveBackfillMasterWindow{}, err
	}

	shardTargets, err := archiveBackfillChangedShardTargets(blocks)
	if err != nil {
		return archiveBackfillMasterWindow{}, err
	}

	floor := blocks[len(blocks)-1]
	return archiveBackfillMasterWindow{
		imported:     imported,
		blocks:       blocks,
		shardTargets: shardTargets,
		floorSeq:     floor.ID.SeqNo,
		floorTime:    floor.Meta.GenUTime,
	}, nil
}

func (r *archiveBackfillRunner) masterWindowBlocks(imported *archiveImportResult, windowEnd uint32) ([]PreparedBlock, error) {
	if imported == nil {
		return nil, fmt.Errorf("archive backfill master import is empty")
	}

	bySeq := make(map[uint32]PreparedBlock)
	var seqnos []uint32
	for _, block := range imported.blocks {
		if block.ID.Workchain != -1 || block.ID.Shard != topShard {
			continue
		}
		if block.ID.SeqNo >= r.progress.VerifiedFloorSeqno {
			continue
		}
		if block.Meta == nil {
			return nil, fmt.Errorf("archive backfill master block %s has no meta", storage.FormatBlockRef(block.ID))
		}
		if block.Meta.GenUTime == 0 {
			return nil, fmt.Errorf("archive backfill master block %s has no gen utime", storage.FormatBlockRef(block.ID))
		}
		bySeq[block.ID.SeqNo] = block
		seqnos = append(seqnos, block.ID.SeqNo)
	}
	if len(seqnos) == 0 {
		return nil, fmt.Errorf("archive backfill master window #%d has no masterchain blocks", windowEnd)
	}
	sort.Slice(seqnos, func(i, j int) bool { return seqnos[i] > seqnos[j] })
	if seqnos[0] != windowEnd {
		return nil, fmt.Errorf("archive backfill master window has gap at top: got=%d want=%d", seqnos[0], windowEnd)
	}

	blocks := make([]PreparedBlock, 0, len(seqnos))
	expected := windowEnd
	for _, seqno := range seqnos {
		if seqno != expected {
			return nil, fmt.Errorf("archive backfill master window has gap: got=%d want=%d", seqno, expected)
		}
		block := bySeq[seqno]
		blocks = append(blocks, block)
		if block.Meta.GenUTime <= r.retention.target || seqno == 0 {
			break
		}
		expected--
	}
	return blocks, nil
}

func (r *archiveBackfillRunner) validateMasterWindow(blocks []PreparedBlock, windowEnd uint32) error {
	if len(blocks) == 0 {
		return fmt.Errorf("archive backfill master window #%d is empty", windowEnd)
	}

	var child *PreparedBlock
	for i := range blocks {
		block := blocks[i]
		if block.IsLink {
			return fmt.Errorf("archive backfill master block %s has proof link instead of full proof", storage.FormatBlockRef(block.ID))
		}
		parsed, err := blockproof.ParseBOC(block.ID, block.ProofBOC)
		if err != nil {
			return fmt.Errorf("validate archive backfill master proof %s: %w", storage.FormatBlockRef(block.ID), err)
		}
		if parsed.Proof.Signatures == nil {
			return fmt.Errorf("archive backfill master block %s has no validator signatures", storage.FormatBlockRef(block.ID))
		}
		old, err := blockproof.OldMasterBlockIDFromState(r.stateRoot, block.ID.SeqNo)
		if err != nil {
			return fmt.Errorf("validate archive backfill old master reference %s: %w", storage.FormatBlockRef(block.ID), err)
		}
		if !old.Equals(&block.ID) {
			return fmt.Errorf("archive backfill old master reference mismatch: state=%s archive=%s", storage.FormatBlockRef(old), storage.FormatBlockRef(block.ID))
		}
		if child != nil {
			if len(child.Meta.PrevRefs) != 1 || !child.Meta.PrevRefs[0].Equals(&block.ID) {
				return fmt.Errorf("archive backfill master chain gap: child=%s expected_prev=%s", storage.FormatBlockRef(child.ID), storage.FormatBlockRef(block.ID))
			}
		}
		child = &blocks[i]
	}
	return nil
}

func archiveBackfillChangedShardTargets(blocks []PreparedBlock) ([]ton.BlockIDExt, error) {
	changed := make(map[string]ton.BlockIDExt)
	var previous map[storage.ShardKey]ton.BlockIDExt
	for i := len(blocks) - 1; i >= 0; i-- {
		block := blocks[i]
		parsed, err := parsePreparedBlock(block)
		if err != nil {
			return nil, err
		}
		shards, err := state2.ShardBlocksFromMasterBlock(block.ID, parsed)
		if err != nil {
			return nil, err
		}

		current := make(map[storage.ShardKey]ton.BlockIDExt, len(shards))
		for _, shard := range shards {
			current[storage.ShardKeyFromBlock(shard)] = shard
			if previous == nil || shard.SeqNo == 0 {
				continue
			}
			old, ok := previous[storage.ShardKeyFromBlock(shard)]
			if !ok || !old.Equals(&shard) {
				changed[storage.BlockKey(shard)] = shard
			}
		}
		previous = current
	}

	targets := make([]ton.BlockIDExt, 0, len(changed))
	for _, block := range changed {
		targets = append(targets, block)
	}
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].Workchain != targets[j].Workchain {
			return targets[i].Workchain < targets[j].Workchain
		}
		if targets[i].Shard != targets[j].Shard {
			return targets[i].Shard < targets[j].Shard
		}
		return targets[i].SeqNo < targets[j].SeqNo
	})
	return targets, nil
}

func (r *archiveBackfillRunner) downloadShardWindows(ctx context.Context, windowEnd uint32, masterWindow archiveBackfillMasterWindow) error {
	if masterWindow.imported.stats.ContainsShardBlocks {
		r.setStage("validating shard archive coverage", 0, 0)
		return r.validateShardCoverage(ctx, masterWindow.shardTargets)
	}

	shards := archiveBackfillShardPrefixes(r.splitDepth)
	r.setStage("downloading shard archives", 0, len(shards))
	for idx, shard := range shards {
		r.setStage("downloading shard archives", idx, len(shards))
		downloaded, err := r.service.node.DownloadArchive(ctx, windowEnd, shard, "")
		if errors.Is(err, archive.ErrNotAvailable) {
			r.service.log.Debug().
				Uint32("masterchain_seqno", windowEnd).
				Int32("workchain", shard.Workchain).
				Str("shard", fmt.Sprintf("%016x", uint64(shard.Shard))).
				Msg("archive backfill shard archive is not available")
			continue
		}
		if err != nil {
			return fmt.Errorf("download archive backfill shard window #%d %s: %w", windowEnd, shard.String(), err)
		}

		imported, err := r.service.importArchiveBlocks(ctx, downloaded, r.splitDepth)
		if err != nil {
			return fmt.Errorf("import archive backfill shard window #%d %s: %w", windowEnd, shard.String(), err)
		}
		if err = validateArchiveBackfillShardImport(imported, shard); err != nil {
			return err
		}
		if err = r.service.storage.SaveArchiveImport(&imported.stored); err != nil {
			return fmt.Errorf("save archive backfill shard window #%d %s: %w", windowEnd, shard.String(), err)
		}
		r.setStage("downloading shard archives", idx+1, len(shards))
	}

	r.setStage("validating shard archive coverage", len(shards), len(shards))
	return r.validateShardCoverage(ctx, masterWindow.shardTargets)
}

func validateArchiveBackfillShardImport(imported *archiveImportResult, shard archive.ShardID) error {
	if imported == nil {
		return fmt.Errorf("archive backfill shard import is empty")
	}
	for _, block := range imported.blocks {
		if block.ID.Workchain == -1 && block.ID.Shard == topShard {
			continue
		}
		if !shard.ContainsBlock(block.ID) {
			return fmt.Errorf("archive backfill shard archive %s contains block %s", shard.String(), storage.FormatBlockRef(block.ID))
		}
		if !block.IsLink {
			return fmt.Errorf("archive backfill shard block %s has full proof instead of proof link", storage.FormatBlockRef(block.ID))
		}
		if _, err := blockproof.ParseBOC(block.ID, block.ProofBOC); err != nil {
			return fmt.Errorf("validate archive backfill shard proof %s: %w", storage.FormatBlockRef(block.ID), err)
		}
	}
	return nil
}

func (r *archiveBackfillRunner) validateShardCoverage(ctx context.Context, targets []ton.BlockIDExt) error {
	for _, target := range targets {
		meta, err := r.store.BlockMeta(ctx, target)
		if err != nil {
			return fmt.Errorf("archive backfill missing changed shard block %s: %w", storage.FormatBlockRef(target), err)
		}
		if meta == nil || !meta.Has(storage.BlockMetaHasServedFull) || !meta.Has(storage.BlockMetaHasBlockData) {
			return fmt.Errorf("archive backfill changed shard block %s is not stored as full block", storage.FormatBlockRef(target))
		}
	}
	return nil
}

func archiveBackfillShardPrefixes(splitDepth uint32) []archive.ShardID {
	count := 1 << splitDepth
	shards := make([]archive.ShardID, 0, count)
	for i := 0; i < count; i++ {
		shard := uint64(i*2+1) << (64 - splitDepth - 1)
		shards = append(shards, archive.ShardID{
			Workchain: 0,
			Shard:     int64(shard),
		})
	}
	return shards
}

func (r *archiveBackfillRunner) logProgressLoop(ctx context.Context, done <-chan struct{}) {
	ticker := time.NewTicker(archiveBackfillProgressInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.logProgress()
		case <-done:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (r *archiveBackfillRunner) setStage(stage string, shardDone int, shardTotal int) {
	r.logMu.Lock()
	r.stage = stage
	r.shardDone = shardDone
	r.shardTotal = shardTotal
	r.logMu.Unlock()
}

func (r *archiveBackfillRunner) logProgress() {
	r.logMu.Lock()
	stage := r.stage
	shardDone := r.shardDone
	shardTotal := r.shardTotal
	r.logMu.Unlock()

	r.service.log.Info().
		Str("origin_persistent_state", storage.FormatBlockRef(r.retention.origin)).
		Uint32("verified_floor_seqno", r.progress.VerifiedFloorSeqno).
		Uint32("verified_floor_utime", r.progress.VerifiedFloorUTime).
		Uint32("target_unix", r.retention.target).
		Uint32("gc_cutoff_unix", r.retention.gcCutoff).
		Int64("remaining_backfill_seconds", r.remainingSeconds()).
		Str("eta", r.eta()).
		Str("stage", stage).
		Int("shard_archives_done", shardDone).
		Int("shard_archives_total", shardTotal).
		Msg("archive backfill progress")
}

func (r *archiveBackfillRunner) remainingSeconds() int64 {
	return archiveBackfillRemainingSeconds(r.progress.VerifiedFloorUTime, r.retention.target)
}

func (r *archiveBackfillRunner) eta() string {
	return formatLagCatchUpETA(r.startRemaining, r.remainingSeconds(), time.Since(r.started))
}

func archiveBackfillRemainingSeconds(verifiedFloorUTime uint32, target uint32) int64 {
	if verifiedFloorUTime <= target {
		return 0
	}
	return int64(verifiedFloorUTime - target)
}
