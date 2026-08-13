package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/p2p"
	state2 "github.com/xssnick/gton/service/state"
	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
)

var (
	errArchiveRunnerAlreadyBound = errors.New("archive runner transitions are already bound")
	errArchiveRunnerNotBound     = errors.New("archive runner transitions are not bound")
	errArchiveRunnerStarted      = errors.New("archive runner is already started")
	errArchiveNextBlockReady     = errors.New("nearby masterchain target is ready for next-block sync")
)

// ArchiveRunnerOptions configures archive catch-up policy.
type ArchiveRunnerOptions struct {
	CheckpointBlocks     uint32
	CheckpointPeriod     time.Duration
	PrefetchWindows      int
	SyncBackpressure     uint32
	SyncUntil            uint32
	StorageDir           string
	MinSyncDiskFreeBytes uint64
}

// ArchiveNetwork is the cold-path network surface required by ArchiveRunner.
// Keeping this contract here lets the composition root provide p2p.Node without
// making archive catch-up depend on the rest of the node API.
type ArchiveNetwork interface {
	BeginArchiveSession() *p2p.ArchiveSession
	ObservedMasterchainBlock() (ton.BlockIDExt, error)
}

// ArchiveStore is the persisted read surface used by archive catch-up. Writes
// are committed through StateLifecycle, so archive orchestration cannot reach
// peer serving, maintenance, serialization, generations, or store lifecycle.
type ArchiveStore interface {
	BlockState(context.Context, ton.BlockIDExt) (*storage.BlockState, error)
	BlockMeta(context.Context, ton.BlockIDExt) (*storage.BlockMeta, error)
}

// archiveMasterTransitions is owned by ArchiveRunner. It combines application
// and the post-apply publication policy so archive code does not depend on the
// coordinator's individual caches and publishers.
type archiveMasterTransitions interface {
	masterchainValidatorsForConsensus(*storage.BlockState, ton.BlockIDExt, masterchainValidatorCacheKey) (*blockproof.PreparedValidatorSet, error)
	applyArchiveMasterBlock(context.Context, *storage.BlockState, *PreparedBlock, *masterchainConsensusProof, stateUpdateApplier) (*storage.BlockState, masterchainApplyTiming, time.Duration, error)
}

type archiveShardTransitions interface {
	applyArchiveShardBlock(context.Context, ton.BlockIDExt, []*storage.BlockState, PreparedBlock, stateUpdateApplier, *storage.BlockState) (*storage.BlockState, error)
}

type archiveCheckpointTransitions interface {
	archiveCheckpointCommitted(*storage.CurrentState, []storage.StateCheckpointBlock)
}

type archiveSyncUntilTransitions interface {
	enterSyncUntilOffline(*storage.CurrentState, PreparedBlock)
}

// ArchiveRunner owns archive catch-up configuration and dependencies. Every
// CatchUp call creates one scoped archiveCatchUpRun which owns and joins its
// pipeline, workers, checkpoint, network session, and importer.
type ArchiveRunner struct {
	log                   zerolog.Logger
	network               ArchiveNetwork
	storage               ArchiveStore
	stateSync             *state2.Syncer
	state                 *StateLifecycle
	blockReceivedObserver *blockReceivedObserverRunner
	maintenance           *MaintenanceRunner

	checkpointBlocks     uint32
	checkpointPeriod     time.Duration
	prefetchWindows      int
	syncBackpressure     uint32
	syncUntil            uint32
	syncDiskSpacePath    string
	minSyncDiskFreeBytes uint64

	bindMu                sync.Mutex
	isBound               bool
	isStarted             bool
	masterTransitions     archiveMasterTransitions
	shardTransitions      archiveShardTransitions
	checkpointTransitions archiveCheckpointTransitions
	syncUntilTransitions  archiveSyncUntilTransitions

	runMu sync.Mutex
}

func NewArchiveRunner(
	logger zerolog.Logger,
	network ArchiveNetwork,
	store ArchiveStore,
	stateSync *state2.Syncer,
	state *StateLifecycle,
	observer BlockReceivedObserver,
	maintenance *MaintenanceRunner,
	opts ArchiveRunnerOptions,
) *ArchiveRunner {
	if opts.CheckpointBlocks == 0 {
		opts.CheckpointBlocks = DefaultArchiveCatchUpCheckpointBlocks
	}
	if opts.CheckpointPeriod == 0 {
		opts.CheckpointPeriod = DefaultArchiveCatchUpCheckpointPeriod
	}
	if opts.PrefetchWindows == 0 {
		opts.PrefetchWindows = DefaultArchiveCatchUpPrefetchWindows
	}
	if opts.SyncBackpressure == 0 {
		opts.SyncBackpressure = DefaultSyncBackpressureWindows
	}
	if opts.MinSyncDiskFreeBytes == 0 {
		opts.MinSyncDiskFreeBytes = defaultMinSyncDiskFreeBytes
	}

	return &ArchiveRunner{
		log:                   logger,
		network:               network,
		storage:               store,
		stateSync:             stateSync,
		state:                 state,
		blockReceivedObserver: newBlockReceivedObserverRunner(logger, observer),
		maintenance:           maintenance,
		checkpointBlocks:      opts.CheckpointBlocks,
		checkpointPeriod:      opts.CheckpointPeriod,
		prefetchWindows:       opts.PrefetchWindows,
		syncBackpressure:      opts.SyncBackpressure,
		syncUntil:             opts.SyncUntil,
		syncDiskSpacePath:     opts.StorageDir,
		minSyncDiskFreeBytes:  opts.MinSyncDiskFreeBytes,
	}
}

// Bind completes the construction cycle with the coordinator's semantic
// transition policies. Binding is one-shot and must happen before CatchUp.
func (a *ArchiveRunner) Bind(
	master archiveMasterTransitions,
	shard archiveShardTransitions,
	checkpoint archiveCheckpointTransitions,
	syncUntil archiveSyncUntilTransitions,
) error {
	a.bindMu.Lock()
	defer a.bindMu.Unlock()

	if a.isStarted {
		return errArchiveRunnerStarted
	}
	if a.isBound {
		return errArchiveRunnerAlreadyBound
	}
	if master == nil || shard == nil || checkpoint == nil || syncUntil == nil {
		return errArchiveRunnerNotBound
	}

	a.masterTransitions = master
	a.shardTransitions = shard
	a.checkpointTransitions = checkpoint
	a.syncUntilTransitions = syncUntil
	a.isBound = true
	return nil
}

// CatchUp advances current through archive windows up to target.
func (a *ArchiveRunner) CatchUp(ctx context.Context, current *storage.CurrentState, target ton.BlockIDExt) (*storage.CurrentState, error) {
	a.bindMu.Lock()
	if !a.isBound {
		a.bindMu.Unlock()
		return nil, errArchiveRunnerNotBound
	}
	a.isStarted = true
	a.bindMu.Unlock()

	a.runMu.Lock()
	defer a.runMu.Unlock()

	current = storage.CloneCurrentState(current)
	if current.ShardClientSeqno == 0 {
		current.ShardClientSeqno = current.Masterchain.Block.SeqNo
	}
	if current.Masterchain.Block.SeqNo != current.ShardClientSeqno {
		return nil, fmt.Errorf("current masterchain seqno %d differs from shard client seqno %d", current.Masterchain.Block.SeqNo, current.ShardClientSeqno)
	}
	if err := a.waitSyncDiskSpace(ctx); err != nil {
		return nil, err
	}

	started := time.Now()
	run := &archiveCatchUpRun{
		archive:                a,
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
		checkpointBlocksTarget: a.checkpointBlocks,
		stateCells:             a.state.newArchiveStateCellWindow(a.state.stateCellLoader()),
	}
	run.startProgressGoal = run.archiveProgressGoalAt(started)

	return run.run()
}

func (a *ArchiveRunner) waitSyncDiskSpace(ctx context.Context) error {
	path := strings.TrimSpace(a.syncDiskSpacePath)
	if path == "" {
		return nil
	}

	for {
		status, err := statFSSyncDiskSpace(path)
		if err == nil && status.AvailableBytes >= a.minSyncDiskFreeBytes {
			return nil
		}

		event := a.log.Error().
			Str("flow", "archive_catchup").
			Str("path", path).
			Uint64("required_bytes", a.minSyncDiskFreeBytes).
			Dur("retry_delay", syncDiskSpaceRetryDelay)
		if err != nil {
			event.Err(err).Msg("failed to check free disk space before sync")
		} else {
			event.
				Err(errSyncDiskSpaceInsufficient).
				Uint64("available_bytes", status.AvailableBytes).
				Msg("not enough free disk space before sync")
		}

		if a.maintenance != nil {
			a.maintenance.wake()
		}
		if !sleepContext(ctx, syncDiskSpaceRetryDelay) {
			return ctx.Err()
		}
	}
}

func (a *ArchiveRunner) preparedMasterBlockAfterSyncUntil(block PreparedBlock) bool {
	return a.syncUntil != 0 && block.Meta.GenUTime > a.syncUntil
}

func (a *ArchiveRunner) loadBlockStateForApply(ctx context.Context, state storage.BlockState) (*storage.BlockState, error) {
	if state.Cell != nil && state.Parsed != nil {
		return storage.CloneBlockState(&state), nil
	}

	return a.storage.BlockState(ctx, state.Block)
}

func (s *SyncCoordinator) applyArchiveMasterBlock(ctx context.Context, current *storage.BlockState, block *PreparedBlock, proof *masterchainConsensusProof, applier stateUpdateApplier) (*storage.BlockState, masterchainApplyTiming, time.Duration, error) {
	block.consensus = proof

	consensusStarted := time.Now()
	checked, err := s.checkMasterchainBlockConsensusWithProof(current, proof)
	consensusElapsed := time.Since(consensusStarted)
	if err != nil {
		return nil, masterchainApplyTiming{}, consensusElapsed, err
	}
	block.consensusChecked = checked

	next, timing, err := s.applyMasterchainTransition(ctx, current, *block, checked, applier, &blockAppliedObserverMeta{})
	if err != nil {
		return nil, timing, consensusElapsed, err
	}
	if err = s.updateMasterDependentCachesForKeyBlock(next, block); err != nil {
		return nil, timing, consensusElapsed, err
	}
	if err = s.publishShardTopValidationView(next); err != nil {
		return nil, timing, consensusElapsed, err
	}

	s.publishLiveBlockArtifacts(*block, next, liveBlockPublishOptions{availabilityOnly: true})
	s.rememberAppliedMasterchainState(next)
	s.rememberSeenMasterchainBlock(next.Block)
	s.rememberMasterState(ctx, next, block)
	return next, timing, consensusElapsed, nil
}

func (s *SyncCoordinator) applyArchiveShardBlock(ctx context.Context, target ton.BlockIDExt, previous []*storage.BlockState, block PreparedBlock, applier stateUpdateApplier, master *storage.BlockState) (*storage.BlockState, error) {
	return s.applyResolvedShardBlock(ctx, target, previous, block, applier, &blockAppliedObserverMeta{
		InclusionMasterRef:   &master.Block,
		InclusionMasterState: master.Cell,
	})
}

func (s *SyncCoordinator) archiveCheckpointCommitted(current *storage.CurrentState, entries []storage.StateCheckpointBlock) {
	s.markLiveCheckpointStatesFlushed(entries)
	s.publishLiveCurrentBlockMarkers(current)
	s.publishCommittedCurrentState(current)
}
