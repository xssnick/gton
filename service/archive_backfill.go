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
	LookupBlockBySeqNo(ctx context.Context, key storage.BlockHistoryKey, seqno uint32) (ton.BlockIDExt, error)
	BlockData(ctx context.Context, block ton.BlockIDExt) ([]byte, error)
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
	imported  *archiveImportResult
	loaded    *archive.Imported
	stored    storage.ServedArchiveImport
	floorSeq  uint32
	floorTime uint32
}

type archiveBackfillStoredMasterWindow struct {
	shardTargets    []ton.BlockIDExt
	shardMasterRefs map[storage.BlockRootHash]ton.BlockIDExt
	shardPlans      []archiveShardImportPlan
	splitDepth      uint32
	floorSeq        uint32
	floorTime       uint32
}

type archiveBackfillStoredMasterBlock struct {
	ID     ton.BlockIDExt
	Meta   *storage.BlockMeta
	Shards []ton.BlockIDExt
}

type archiveBackfillShardTargetRef struct {
	target ton.BlockIDExt
	master ton.BlockIDExt
}

func (s *Service) runArchiveBackfillOnce(ctx context.Context) (bool, error) {
	if s.disableArchiveBackfill {
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
	if s.disableArchiveBackfill {
		return archiveBackfillRetention{}, errArchiveBackfillDisabled
	}

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
	if ttlSeconds == 0 || uint64(originMeta.GenUTime) <= ttlSeconds {
		return archiveBackfillRetention{
			origin:      active.OriginPersistentState,
			originUTime: originMeta.GenUTime,
		}, nil
	}

	gcCutoff := originMeta.GenUTime - uint32(ttlSeconds)
	target := uint64(gcCutoff) + guardSeconds
	if target >= uint64(originMeta.GenUTime) {
		target = uint64(originMeta.GenUTime)
	}

	return archiveBackfillRetention{
		origin:      active.OriginPersistentState,
		originUTime: originMeta.GenUTime,
		gcCutoff:    gcCutoff,
		target:      uint32(target),
	}, nil
}

func (s *Service) archiveBackfillProgress(ctx context.Context, store archiveBackfillStore, retention archiveBackfillRetention) (storage.ArchiveBackfillProgress, error) {
	progress, err := store.ArchiveBackfillProgress(ctx)
	if errors.Is(err, storage.ErrNotFound) {
		progress = newArchiveBackfillProgress(retention)
	} else if err != nil {
		return storage.ArchiveBackfillProgress{}, fmt.Errorf("load archive backfill progress: %w", err)
	} else if !progress.OriginPersistentState.Equals(&retention.origin) {
		if canCarryArchiveBackfillProgress(progress, retention) {
			progress.OriginPersistentState = retention.origin
		} else {
			// Reinitialize only when the stored checkpoint is not monotonic relative to the active origin.
			progress = newArchiveBackfillProgress(retention)
		}
	}

	progress.OriginGenUTime = retention.originUTime
	progress.GCCutoffUnix = retention.gcCutoff
	progress.TargetUnix = retention.target
	if progress.VerifiedFloorSeqno == 0 {
		return storage.ArchiveBackfillProgress{}, errArchiveBackfillComplete
	}
	if retention.target > 0 && (progress.VerifiedFloorSeqno <= 1 || progress.VerifiedFloorUTime <= retention.target) {
		return storage.ArchiveBackfillProgress{}, errArchiveBackfillComplete
	}
	return progress, nil
}

func newArchiveBackfillProgress(retention archiveBackfillRetention) storage.ArchiveBackfillProgress {
	return storage.ArchiveBackfillProgress{
		OriginPersistentState: retention.origin,
		OriginGenUTime:        retention.originUTime,
		GCCutoffUnix:          retention.gcCutoff,
		TargetUnix:            retention.target,
		VerifiedFloorSeqno:    retention.origin.SeqNo,
		VerifiedFloorUTime:    retention.originUTime,
	}
}

func canCarryArchiveBackfillProgress(progress storage.ArchiveBackfillProgress, retention archiveBackfillRetention) bool {
	if emptyBlockID(progress.OriginPersistentState) {
		return false
	}
	if progress.OriginPersistentState.Workchain != retention.origin.Workchain ||
		progress.OriginPersistentState.Shard != retention.origin.Shard {
		return false
	}
	if progress.OriginPersistentState.SeqNo == retention.origin.SeqNo {
		return false
	}
	if progress.OriginPersistentState.SeqNo > retention.origin.SeqNo {
		return false
	}
	if progress.OriginGenUTime != 0 && retention.originUTime != 0 && progress.OriginGenUTime > retention.originUTime {
		return false
	}
	if progress.VerifiedFloorSeqno != 0 && progress.VerifiedFloorSeqno > progress.OriginPersistentState.SeqNo {
		return false
	}
	if progress.VerifiedFloorSeqno > retention.origin.SeqNo {
		return false
	}
	if progress.VerifiedFloorUTime != 0 && progress.OriginGenUTime != 0 && progress.VerifiedFloorUTime > progress.OriginGenUTime {
		return false
	}
	if progress.VerifiedFloorUTime != 0 && retention.originUTime != 0 && progress.VerifiedFloorUTime > retention.originUTime {
		return false
	}
	return true
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
	if r.retention.gcCutoff != 0 && masterWindow.floorTime <= r.retention.gcCutoff {
		r.service.log.Info().
			Uint32("window_floor_utime", masterWindow.floorTime).
			Uint32("gc_cutoff_unix", r.retention.gcCutoff).
			Uint32("target_unix", r.retention.target).
			Msg("archive backfill reached gc guard boundary")
		return false, nil
	}

	r.setStage("saving master archive metadata", 0, 0)
	if err = r.service.storage.SaveArchiveImport(&masterWindow.stored); err != nil {
		return false, fmt.Errorf("save archive backfill master window: %w", err)
	}
	masterWindow.stored = storage.ServedArchiveImport{}

	storedWindow, err := r.loadStoredMasterWindow(ctx, windowEnd, masterWindow.floorSeq)
	if err != nil {
		return false, err
	}
	if storedWindow.floorSeq != masterWindow.floorSeq || storedWindow.floorTime != masterWindow.floorTime {
		return false, fmt.Errorf("archive backfill stored master window floor mismatch: stored=(%d,%d) imported=(%d,%d)",
			storedWindow.floorSeq, storedWindow.floorTime, masterWindow.floorSeq, masterWindow.floorTime)
	}

	if archiveBackfillImportHasShardBlocks(masterWindow.imported) {
		if err = r.saveMasterArchiveShardBlocks(masterWindow, storedWindow); err != nil {
			return false, err
		}
		storedWindow.shardPlans = nil
	}
	masterWindow.imported = nil
	masterWindow.loaded = nil

	if err = r.downloadShardWindows(ctx, windowEnd, storedWindow); err != nil {
		return false, err
	}

	r.progress.VerifiedFloorSeqno = storedWindow.floorSeq
	r.progress.VerifiedFloorUTime = storedWindow.floorTime
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
	downloaded, err := r.service.node.DownloadArchive(ctx, windowEnd, archive.ShardID{Workchain: -1, Shard: topShard})
	if err != nil {
		return archiveBackfillMasterWindow{}, fmt.Errorf("download archive backfill master window #%d: %w", windowEnd, err)
	}
	loaded, err := loadDownloadedArchive(ctx, downloaded)
	if err != nil {
		return archiveBackfillMasterWindow{}, fmt.Errorf("import archive backfill master window #%d: %w", windowEnd, err)
	}
	imported, err := r.service.prepareImportedArchiveBlocks(loaded, r.splitDepth)
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
	stored, err := servedArchiveImportFromImported(loaded, r.splitDepth, nil, archiveBackfillIncludeBlocks(blocks))
	if err != nil {
		return archiveBackfillMasterWindow{}, fmt.Errorf("prepare archive backfill master window storage #%d: %w", windowEnd, err)
	}

	floor := blocks[len(blocks)-1]
	return archiveBackfillMasterWindow{
		imported:  imported,
		loaded:    loaded,
		stored:    stored,
		floorSeq:  floor.ID.SeqNo,
		floorTime: floor.Meta.GenUTime,
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

func archiveBackfillIncludeBlocks(blocks []PreparedBlock) map[storage.BlockRootHash]bool {
	include := make(map[storage.BlockRootHash]bool, len(blocks))
	for _, block := range blocks {
		include[storage.BlockKey(block.ID)] = true
	}
	return include
}

func archiveBackfillImportHasShardBlocks(imported *archiveImportResult) bool {
	if imported == nil {
		return false
	}
	for _, block := range imported.blocks {
		if block.ID.Workchain != -1 || block.ID.Shard != topShard {
			return true
		}
	}
	return false
}

func (r *archiveBackfillRunner) downloadShardWindows(ctx context.Context, windowEnd uint32, masterWindow archiveBackfillStoredMasterWindow) error {
	if len(masterWindow.shardPlans) == 0 {
		r.setStage("validating shard archive coverage", 0, 0)
		return r.validateShardCoverage(ctx, masterWindow.shardTargets)
	}

	r.setStage("downloading shard archives", 0, len(masterWindow.shardPlans))
	for idx, plan := range masterWindow.shardPlans {
		r.setStage("downloading shard archives", idx, len(masterWindow.shardPlans))
		shard := plan.shard
		downloaded, err := r.service.node.DownloadArchive(ctx, windowEnd, shard)
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

		loaded, err := loadDownloadedArchive(ctx, downloaded)
		if err != nil {
			return fmt.Errorf("import archive backfill shard window #%d %s: %w", windowEnd, shard.String(), err)
		}
		imported, err := r.service.prepareImportedArchiveBlocks(loaded, plan.splitDepth)
		if err != nil {
			return fmt.Errorf("import archive backfill shard window #%d %s: %w", windowEnd, shard.String(), err)
		}
		if err = validateArchiveImportCoversPlan(imported, plan); err != nil {
			return err
		}
		shardRefs, includeBlocks, err := archiveBackfillShardImportMasterRefs(imported.blocks, plan.needed, masterWindow.shardMasterRefs)
		if err != nil {
			return err
		}
		if err = validateArchiveBackfillShardImport(imported, includeBlocks); err != nil {
			return err
		}
		stored, err := servedArchiveImportFromImported(loaded, plan.splitDepth, shardRefs, includeBlocks)
		if err != nil {
			return err
		}
		if err = r.service.storage.SaveArchiveImport(&stored); err != nil {
			return fmt.Errorf("save archive backfill shard window #%d %s: %w", windowEnd, shard.String(), err)
		}
		r.setStage("downloading shard archives", idx+1, len(masterWindow.shardPlans))
	}

	r.setStage("validating shard archive coverage", len(masterWindow.shardPlans), len(masterWindow.shardPlans))
	return r.validateShardCoverage(ctx, masterWindow.shardTargets)
}

func (r *archiveBackfillRunner) saveMasterArchiveShardBlocks(masterWindow archiveBackfillMasterWindow, storedWindow archiveBackfillStoredMasterWindow) error {
	if len(storedWindow.shardPlans) == 0 {
		return nil
	}

	r.setStage("saving shard blocks from master archive", 0, 1)
	stored, err := archiveBackfillShardImportFromCommonMasterArchive(masterWindow.loaded, masterWindow.imported, storedWindow.splitDepth, storedWindow.shardPlans, storedWindow.shardMasterRefs)
	if err != nil {
		return err
	}
	if err = r.service.storage.SaveArchiveImport(&stored); err != nil {
		return fmt.Errorf("save archive backfill shard blocks from master archive: %w", err)
	}
	r.setStage("saving shard blocks from master archive", 1, 1)
	return nil
}

func archiveBackfillShardImportFromCommonMasterArchive(loaded *archive.Imported, imported *archiveImportResult, splitDepth uint32, plans []archiveShardImportPlan, masterRefs map[storage.BlockRootHash]ton.BlockIDExt) (storage.ServedArchiveImport, error) {
	targets := archiveBackfillShardPlanTargets(plans)
	if len(targets) == 0 {
		return storage.ServedArchiveImport{}, nil
	}
	for _, plan := range plans {
		if err := validateArchiveImportCoversPlan(imported, plan); err != nil {
			return storage.ServedArchiveImport{}, fmt.Errorf("archive backfill common master archive: %w", err)
		}
	}

	shardRefs, includeBlocks, err := archiveBackfillShardImportMasterRefs(imported.blocks, targets, masterRefs)
	if err != nil {
		return storage.ServedArchiveImport{}, err
	}
	if err = validateArchiveBackfillShardImport(imported, includeBlocks); err != nil {
		return storage.ServedArchiveImport{}, err
	}
	return servedArchiveImportFromImported(loaded, splitDepth, shardRefs, includeBlocks)
}

func archiveBackfillShardPlanTargets(plans []archiveShardImportPlan) []ton.BlockIDExt {
	var total int
	for _, plan := range plans {
		total += len(plan.needed)
	}
	targets := make([]ton.BlockIDExt, 0, total)
	for _, plan := range plans {
		targets = append(targets, plan.needed...)
	}
	return targets
}

func (r *archiveBackfillRunner) loadStoredMasterWindow(ctx context.Context, windowEnd uint32, floorSeq uint32) (archiveBackfillStoredMasterWindow, error) {
	blocks, err := r.loadStoredMasterWindowBlocks(ctx, windowEnd, floorSeq)
	if err != nil {
		return archiveBackfillStoredMasterWindow{}, err
	}
	if err = r.validateStoredMasterWindow(blocks, windowEnd); err != nil {
		return archiveBackfillStoredMasterWindow{}, err
	}

	shardTargets, shardMasterRefs, err := archiveBackfillStoredShardTargets(blocks)
	if err != nil {
		return archiveBackfillStoredMasterWindow{}, err
	}
	splitDepth, err := archiveBackfillStoredWindowSplitDepth(r.splitDepth, blocks)
	if err != nil {
		return archiveBackfillStoredMasterWindow{}, err
	}
	shardPlans, err := r.missingArchiveBackfillShardPlans(ctx, splitDepth, shardTargets)
	if err != nil {
		return archiveBackfillStoredMasterWindow{}, err
	}

	floor := blocks[len(blocks)-1]
	return archiveBackfillStoredMasterWindow{
		shardTargets:    shardTargets,
		shardMasterRefs: shardMasterRefs,
		shardPlans:      shardPlans,
		splitDepth:      splitDepth,
		floorSeq:        floor.ID.SeqNo,
		floorTime:       floor.Meta.GenUTime,
	}, nil
}

func (r *archiveBackfillRunner) loadStoredMasterWindowBlocks(ctx context.Context, windowEnd uint32, floorSeq uint32) ([]archiveBackfillStoredMasterBlock, error) {
	if floorSeq > windowEnd {
		return nil, fmt.Errorf("archive backfill stored master window floor %d is above top %d", floorSeq, windowEnd)
	}

	key := storage.BlockHistoryKey{Workchain: -1, Shard: topShard}
	blocks := make([]archiveBackfillStoredMasterBlock, 0, 128)
	for seqno := windowEnd; ; seqno-- {
		id, err := r.store.LookupBlockBySeqNo(ctx, key, seqno)
		if err != nil {
			return nil, fmt.Errorf("load archive backfill stored master block #%d: %w", seqno, err)
		}
		if id.Workchain != -1 || id.Shard != topShard {
			return nil, fmt.Errorf("archive backfill master seqno index #%d points to %s", seqno, storage.FormatBlockRef(id))
		}

		meta, err := r.store.BlockMeta(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("load archive backfill stored master meta %s: %w", storage.FormatBlockRef(id), err)
		}
		if meta.GenUTime == 0 {
			return nil, fmt.Errorf("archive backfill stored master block %s has no gen utime", storage.FormatBlockRef(id))
		}

		data, err := r.store.BlockData(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("load archive backfill stored master block data %s: %w", storage.FormatBlockRef(id), err)
		}
		root, err := cell.FromBOC(data)
		if err != nil {
			return nil, fmt.Errorf("parse archive backfill stored master block data %s: %w", storage.FormatBlockRef(id), err)
		}
		parsed, err := storage.ParseVerifiedBlockCell(id, root)
		if err != nil {
			return nil, fmt.Errorf("parse archive backfill stored master block %s: %w", storage.FormatBlockRef(id), err)
		}
		shards, err := state2.ShardBlocksFromMasterBlock(id, parsed)
		if err != nil {
			return nil, err
		}

		blocks = append(blocks, archiveBackfillStoredMasterBlock{
			ID:     id,
			Meta:   meta,
			Shards: shards,
		})
		if seqno == floorSeq {
			break
		}
	}
	return blocks, nil
}

func (r *archiveBackfillRunner) validateStoredMasterWindow(blocks []archiveBackfillStoredMasterBlock, windowEnd uint32) error {
	if len(blocks) == 0 {
		return fmt.Errorf("archive backfill stored master window #%d is empty", windowEnd)
	}
	if blocks[0].ID.SeqNo != windowEnd {
		return fmt.Errorf("archive backfill stored master window has gap at top: got=%d want=%d", blocks[0].ID.SeqNo, windowEnd)
	}

	var child *archiveBackfillStoredMasterBlock
	for i := range blocks {
		block := &blocks[i]
		old, err := blockproof.OldMasterBlockIDFromState(r.stateRoot, block.ID.SeqNo)
		if err != nil {
			return fmt.Errorf("validate archive backfill stored old master reference %s: %w", storage.FormatBlockRef(block.ID), err)
		}
		if !old.Equals(&block.ID) {
			return fmt.Errorf("archive backfill stored old master reference mismatch: state=%s archive=%s", storage.FormatBlockRef(old), storage.FormatBlockRef(block.ID))
		}
		if child != nil {
			if len(child.Meta.PrevRefs) != 1 || !child.Meta.PrevRefs[0].Equals(&block.ID) {
				return fmt.Errorf("archive backfill stored master chain gap: child=%s expected_prev=%s", storage.FormatBlockRef(child.ID), storage.FormatBlockRef(block.ID))
			}
		}
		child = block
	}
	return nil
}

func archiveBackfillStoredShardTargets(blocks []archiveBackfillStoredMasterBlock) ([]ton.BlockIDExt, map[storage.BlockRootHash]ton.BlockIDExt, error) {
	changed := make(map[storage.BlockRootHash]ton.BlockIDExt)
	masterRefs := make(map[storage.BlockRootHash]ton.BlockIDExt)
	var previous map[storage.ShardKey]ton.BlockIDExt
	for i := len(blocks) - 1; i >= 0; i-- {
		block := blocks[i]
		current := make(map[storage.ShardKey]ton.BlockIDExt, len(block.Shards))
		for _, shard := range block.Shards {
			key := storage.BlockKey(shard)
			if _, ok := masterRefs[key]; !ok {
				masterRefs[key] = block.ID
			}

			shardKey := storage.ShardKeyFromBlock(shard)
			current[shardKey] = shard
			if previous == nil || shard.SeqNo == 0 {
				continue
			}
			old, ok := previous[shardKey]
			if !ok || !old.Equals(&shard) {
				changed[key] = shard
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
	return targets, masterRefs, nil
}

func archiveBackfillStoredWindowSplitDepth(base uint32, blocks []archiveBackfillStoredMasterBlock) (uint32, error) {
	if base > maxArchiveMonitorSplitDepth {
		return 0, fmt.Errorf("archive backfill split depth %d exceeds supported archive prefix fanout %d", base, maxArchiveMonitorSplitDepth)
	}

	splitDepth := base
	for _, block := range blocks {
		for _, shard := range block.Shards {
			if shard.Workchain != 0 {
				continue
			}

			depth := uint32(archiveShardPrefixLength(shard.Shard))
			if depth > splitDepth {
				splitDepth = depth
			}
		}
	}
	if splitDepth > maxArchiveMonitorSplitDepth {
		return 0, fmt.Errorf("archive backfill stored shard split depth %d exceeds supported archive prefix fanout %d", splitDepth, maxArchiveMonitorSplitDepth)
	}
	return splitDepth, nil
}

func (r *archiveBackfillRunner) missingArchiveBackfillShardPlans(ctx context.Context, splitDepth uint32, targets []ton.BlockIDExt) ([]archiveShardImportPlan, error) {
	plansByShard := make(map[archive.ShardID]*archiveShardImportPlan)
	for _, target := range targets {
		stored, err := r.archiveBackfillShardTargetStored(ctx, target)
		if err != nil {
			return nil, err
		}
		if stored {
			continue
		}

		shard, err := archiveBackfillShardPrefixForBlock(splitDepth, target)
		if err != nil {
			return nil, err
		}
		plan := plansByShard[shard]
		if plan == nil {
			plan = &archiveShardImportPlan{shard: shard, splitDepth: splitDepth}
			plansByShard[shard] = plan
		}
		plan.needed = append(plan.needed, target)
	}

	plans := make([]archiveShardImportPlan, 0, len(plansByShard))
	for _, plan := range plansByShard {
		plans = append(plans, *plan)
	}
	sort.Slice(plans, func(i, j int) bool {
		if plans[i].shard.Workchain != plans[j].shard.Workchain {
			return plans[i].shard.Workchain < plans[j].shard.Workchain
		}
		return plans[i].shard.Shard < plans[j].shard.Shard
	})
	return plans, nil
}

func (r *archiveBackfillRunner) archiveBackfillShardTargetStored(ctx context.Context, target ton.BlockIDExt) (bool, error) {
	meta, err := r.store.BlockMeta(ctx, target)
	if errors.Is(err, storage.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load archive backfill shard target meta %s: %w", storage.FormatBlockRef(target), err)
	}
	if !meta.Has(storage.BlockMetaHasServedFull) || !meta.Has(storage.BlockMetaHasBlockData) {
		return false, nil
	}
	return target.Workchain == -1 || meta.MasterchainRef != nil, nil
}

func archiveBackfillShardPrefixForBlock(splitDepth uint32, block ton.BlockIDExt) (archive.ShardID, error) {
	if block.Workchain != 0 {
		return archive.ShardID{}, fmt.Errorf("archive backfill shard target %s is not a basechain block", storage.FormatBlockRef(block))
	}
	if splitDepth > 62 {
		return archive.ShardID{}, fmt.Errorf("archive backfill split depth %d is too large", splitDepth)
	}
	if splitDepth == 0 {
		return archive.ShardID{Workchain: 0, Shard: topShard}, nil
	}

	idx := uint64(block.Shard) >> (64 - splitDepth)
	shard := (idx*2 + 1) << (64 - splitDepth - 1)
	return archive.ShardID{Workchain: 0, Shard: int64(shard)}, nil
}

func archiveBackfillShardImportMasterRefs(blocks map[storage.BlockRootHash]PreparedBlock, targets []ton.BlockIDExt, targetMasterRefs map[storage.BlockRootHash]ton.BlockIDExt) (map[storage.BlockRootHash]ton.BlockIDExt, map[storage.BlockRootHash]bool, error) {
	ordered, err := archiveBackfillOrderedShardTargets(blocks, targets, targetMasterRefs)
	if err != nil {
		return nil, nil, err
	}

	refs := make(map[storage.BlockRootHash]ton.BlockIDExt)
	include := make(map[storage.BlockRootHash]bool)
	for _, item := range ordered {
		if err := assignArchiveBackfillShardImportMasterRef(blocks, item.target, item.master, refs, include); err != nil {
			return nil, nil, err
		}
	}
	return refs, include, nil
}

func archiveBackfillOrderedShardTargets(blocks map[storage.BlockRootHash]PreparedBlock, targets []ton.BlockIDExt, targetMasterRefs map[storage.BlockRootHash]ton.BlockIDExt) ([]archiveBackfillShardTargetRef, error) {
	ordered := make([]archiveBackfillShardTargetRef, 0, len(targets))
	for _, target := range targets {
		key := storage.BlockKey(target)
		master, ok := targetMasterRefs[key]
		if !ok {
			return nil, fmt.Errorf("archive backfill shard target %s has no masterchain reference in stored master window", storage.FormatBlockRef(target))
		}
		if _, ok = blocks[key]; !ok {
			return nil, fmt.Errorf("archive backfill shard import is missing planned block %s", storage.FormatBlockRef(target))
		}
		ordered = append(ordered, archiveBackfillShardTargetRef{
			target: target,
			master: master,
		})
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		left := ordered[i]
		right := ordered[j]
		if left.master.SeqNo != right.master.SeqNo {
			return left.master.SeqNo < right.master.SeqNo
		}
		if left.target.Workchain != right.target.Workchain {
			return left.target.Workchain < right.target.Workchain
		}
		if left.target.Shard != right.target.Shard {
			return left.target.Shard < right.target.Shard
		}
		return left.target.SeqNo < right.target.SeqNo
	})
	return ordered, nil
}

func assignArchiveBackfillShardImportMasterRef(blocks map[storage.BlockRootHash]PreparedBlock, start ton.BlockIDExt, master ton.BlockIDExt, refs map[storage.BlockRootHash]ton.BlockIDExt, include map[storage.BlockRootHash]bool) error {
	stack := []ton.BlockIDExt{start}
	for len(stack) > 0 {
		block := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		key := storage.BlockKey(block)
		if existing, ok := refs[key]; ok {
			if !existing.Equals(&master) && existing.SeqNo >= master.SeqNo {
				return fmt.Errorf("archive backfill shard block %s was visited with later masterchain reference %s before %s",
					storage.FormatBlockRef(block), storage.FormatBlockRef(existing), storage.FormatBlockRef(master))
			}
			continue
		}

		prepared, ok := blocks[key]
		if !ok {
			continue
		}
		if !prepared.ID.Equals(&block) {
			return fmt.Errorf("archive backfill shard block key collision: got=%s want=%s", storage.FormatBlockRef(prepared.ID), storage.FormatBlockRef(block))
		}
		if prepared.Meta == nil {
			return fmt.Errorf("archive backfill shard block %s has no meta", storage.FormatBlockRef(block))
		}

		refs[key] = master
		include[key] = true
		for _, prev := range prepared.Meta.PrevRefs {
			if prev.Workchain == block.Workchain && prev.Shard == block.Shard {
				stack = append(stack, prev)
			}
		}
	}
	return nil
}

func validateArchiveBackfillShardImport(imported *archiveImportResult, includeBlocks map[storage.BlockRootHash]bool) error {
	if imported == nil {
		return fmt.Errorf("archive backfill shard import is empty")
	}
	for _, block := range imported.blocks {
		if !includeBlocks[storage.BlockKey(block.ID)] {
			continue
		}
		if block.ID.Workchain == -1 && block.ID.Shard == topShard {
			continue
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
		if target.Workchain != -1 && meta.MasterchainRef == nil {
			return fmt.Errorf("archive backfill changed shard block %s has no masterchain reference", storage.FormatBlockRef(target))
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
