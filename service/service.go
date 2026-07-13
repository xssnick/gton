package service

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xssnick/gton/service/blocksync"
	"github.com/xssnick/gton/service/externalmsg"
	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/state"
	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	topShard                           = int64(-1 << 63)
	shardStateCatchUpParallelism       = 4
	shardStateDownloadBuffer           = 16
	shardStateDownloadWorkers          = 4
	nextBlockDescriptionLookahead      = shardStateDownloadBuffer
	shardStateCatchUpRetryDelay        = 500 * time.Millisecond
	currentStateLivePollDelay          = 300 * time.Millisecond
	syncDiskSpaceRetryDelay            = 5 * time.Second
	defaultMinSyncDiskFreeBytes        = 10 << 30
	stateSerializationMinDiskFreeBytes = 30 << 30
	nextBlockBootstrapBlocks           = 0
	nextBlockBootstrapProbeTimeout     = 2 * time.Second
	nextBlockBootstrapLiveProbeTimeout = 5 * time.Second
	nextBlockBootstrapLiveStageDelay   = 250 * time.Millisecond
	nextBlockBootstrapProbePeers       = 3
	nextBlockBootstrapUrgentPeers      = 8
	nextBlockBootstrapWidePeers        = 16
	nextBlockBootstrapUrgentMisses     = 1
	nextBlockBootstrapWideMisses       = 2
	nextBlockBootstrapUrgentLagSeconds = 3
	nextBlockBootstrapWideLagSeconds   = 10
	nextBlockBootstrapWideGapBlocks    = 8
	nextBlockBootstrapLiveLagSeconds   = 10
	exactBlockDownloadProbePeers       = 4
	exactBlockDownloadWaitLogDelay     = time.Second
	exactBlockDownloadWaitLogEvery     = 2 * time.Second
	chainBlockDownloadRetries          = 3
	chainBlockDownloadRetryDelay       = 500 * time.Millisecond
	nextMasterchainQueueLimit          = 64
	nextMasterchainQueueTTL            = 3 * time.Minute
	nextMasterchainQueueMaxBytes       = 512 << 20
	masterStateCacheLimit              = 2048
	syncedBlockProcessorWorkers        = 8
	syncedBlockPriorityQueueLimit      = 64
	syncedBlockMasterHotWorkers        = 2
	syncedBlockMasterHotQueueLimit     = 64
	statusTPSMasterWindow              = 10
)

const (
	DefaultNextBlockCheckpointBlocks = 400
	DefaultCheckpointBytes           = uint64(512 << 20)
	DefaultSyncBackpressureWindows   = 4
)

var (
	errMasterchainPrevMismatch = errors.New("masterchain previous state is not current")
	errSyncedBlockDeferred     = errors.New("synced block was deferred")
	errShardDescriptionTooOld  = errors.New("shard block description is older than current state")
	errShardDescriptionTooNew  = errors.New("shard block description is too far ahead of current state")
)
var errShardCatchUpNeedsSnapshot = errors.New("shard catch-up requires state snapshot")

type shardBlockLoader func(context.Context, ton.BlockIDExt) (PreparedBlock, error)

type timeoutError interface {
	Timeout() bool
}

type shardStateDownload struct {
	prev            ton.BlockIDExt
	block           PreparedBlock
	err             error
	source          SyncBlockSource
	downloadElapsed time.Duration
	prepareElapsed  time.Duration
}

type shardStateDownloadJob struct {
	prev  ton.BlockIDExt
	block ton.BlockIDExt
}

type catchUpTiming struct {
	windowStarted time.Time
	blocks        uint32
	downloadWait  time.Duration
	apply         time.Duration
	persist       time.Duration
	checkpoints   uint32
}

type nextShardClientApplyStats struct {
	wall         time.Duration
	targetParse  time.Duration
	apply        time.Duration
	obtain       time.Duration
	resolverWait time.Duration
	applied      int
	downloaded   int
	reused       int
}

func newCatchUpTiming(now time.Time) catchUpTiming {
	return catchUpTiming{windowStarted: now}
}

func (t *catchUpTiming) reset(now time.Time) {
	*t = newCatchUpTiming(now)
}

func avgDuration(total time.Duration, count uint32) time.Duration {
	if count == 0 {
		return 0
	}
	return total / time.Duration(count)
}

type Service struct {
	log                     zerolog.Logger
	node                    *p2p.Node
	blockSync               *blocksync.Service
	storage                 storage.Storage
	stateSync               *state.Syncer
	liveState               CurrentStatePublisher
	liveBlockCache          *storage.LiveBlockCache
	liveStateUsesBlockCache bool
	sync                    SyncObserver
	applyHooks              *blockApplyHookRunner
	externalMessageChecker  *externalmsg.Checker
	externalMessageHooks    *externalMessageHookRunner
	blockReceivedHooks      *blockReceivedHookRunner

	stateCellPrewrite *stateCellPrewriter
	artifactPrewrite  *artifactPrewriter

	archiveCatchUpCheckpointBlocks uint32
	archiveCatchUpCheckpointPeriod time.Duration
	archiveCatchUpPrefetchWindows  int
	nextBlockCheckpointBlocks      uint32
	checkpointBytes                uint64
	syncBackpressureWindows        uint32
	shutdownContext                context.Context
	broadcastAdmissionMu           sync.Mutex
	broadcastAdmissionInitialized  bool
	broadcastAdmissionClosed       bool
	broadcastAdmissionClosedAtomic atomic.Bool
	broadcastLiveMasterSeqno       uint32
	broadcastFlushedMasterSeqno    uint32

	stateMu sync.Mutex

	currentStateWake      chan struct{}
	shardDescriptionWake  chan struct{}
	shardDescriptionMu    sync.Mutex
	shardDescriptionHints map[storage.BlockRootHash]shardDescriptionHint
	shardDescriptionOrder []storage.BlockRootHash

	preparedShardBlocks     preparedShardBlockCache
	preparedShardBlockSlots chan struct{}

	nextMasterchainMx               sync.Mutex
	nextMasterchainQueue            map[storage.BlockRootHash]queuedMasterchainBlock
	nextMasterchainBySeqno          map[uint32]storage.BlockRootHash
	nextMasterchainBytes            int64
	nextMasterchainCandidates       map[storage.BlockRootHash]queuedMasterchainCandidate
	nextMasterchainCandidateBySeqno map[uint32]storage.BlockRootHash
	nextMasterchainCandidateBytes   int64
	validatorCache                  masterchainValidatorCache
	broadcastValidatorCache         broadcastValidatorCache
	masterStateCacheMu              sync.Mutex
	masterStateCache                map[storage.BlockRootHash]*storage.BlockState
	masterStateCacheKeys            []storage.BlockRootHash
	masterStateCacheHead            int
	compressedStateMu               sync.Mutex
	compressedStateCache            map[storage.BlockRootHash]compressedBlockStateEntry
	compressedStateOrder            []compressedBlockStateOrderEntry
	compressedStateOrderHead        int
	compressedStateVersion          uint64
	monitorSplitDepthMu             sync.Mutex
	monitorSplitDepth               map[monitorSplitDepthKey]uint32

	currentStatePersistMu              sync.Mutex
	currentStatePersistErrMu           sync.Mutex
	currentStatePersistErr             error
	stateCellLoaderMu                  sync.RWMutex
	stateCellLoaders                   map[uint64]cell.LazyCellLoader
	stateCellLoaderSnapshot            atomic.Value
	lazyCellLoads                      lazyCellLoadCounters
	nextStateCellLoaderID              uint64
	stateSerializer                    *stateSerializer
	automaticStateSerializationReady   atomic.Bool
	maintenanceWake                    chan struct{}
	stateTTL                           time.Duration
	archiveTTL                         time.Duration
	archiveFromZero                    bool
	syncUntil                          uint32
	syncDiskSpacePath                  string
	minSyncDiskFreeBytes               uint64
	minStateSerializationDiskFreeBytes uint64
	syncDiskSpaceProbe                 syncDiskSpaceProbe
	syncDiskSpaceRetryDelay            time.Duration
	exclusiveTaskMu                    sync.Mutex
	exclusiveTask                      exclusiveServiceTask
	cellMigrationMu                    sync.Mutex
	cellGenerationMigrationRun         *cellGenerationMigrationRun
	cellGenerationMigrationStopping    bool
	cellGenerationSwitchRequested      bool
	cellGenerationNextBlockYieldAt     time.Time
	cellGenerationSwitching            bool
	currentStatusMu                    sync.RWMutex
	currentStatus                      *storage.CurrentState
	appliedMasterchainStatus           *storage.BlockState

	startOnce sync.Once
	wg        sync.WaitGroup
}

type Options struct {
	ArchiveCatchUpCheckpointBlocks          uint32
	ArchiveCatchUpCheckpointPeriod          time.Duration
	ArchiveCatchUpPrefetchWindows           int
	NextBlockCheckpointBlocks               uint32
	CheckpointBytes                         uint64
	SyncBackpressureWindows                 uint32
	CurrentStatePublisher                   CurrentStatePublisher
	LiveBlockCache                          *storage.LiveBlockCache
	CurrentStatePublisherUsesLiveBlockCache bool
	ShutdownContext                         context.Context
	StateFilesDir                           string
	StateTTL                                time.Duration
	ArchiveTTL                              time.Duration
	ArchiveFromZero                         bool
	SyncUntil                               uint32
	StorageDir                              string
	MinSyncDiskFreeBytes                    uint64
	MinStateSerializationDiskFreeBytes      uint64
	DisableStateSerialization               bool
	PersistentStateLargeBOCBatchSize        int
	StateSerializeOnePass                   bool
	SyncObserver                            SyncObserver
	Extension                               hooks.Extension
	ExternalMessageChecker                  *externalmsg.Checker
}

type CurrentStatePublisher interface {
	SetLiveCurrentState(*storage.CurrentState)
	// SetLiveCurrentStateSnapshot takes ownership of an already-private
	// snapshot instead of cloning the state like SetLiveCurrentState does.
	SetLiveCurrentStateSnapshot(*storage.CurrentState)
	MarkLiveCurrentStateFlushed(*storage.CurrentState)
	MarkLiveBlockStatesFlushed([]ton.BlockIDExt)
	MarkLiveBlockFlushed(ton.BlockIDExt)
	PublishLiveBlockArtifacts(storage.LiveBlockArtifacts) error
	NonfinalBlockCacheEnabled() bool
	PublishNonfinalBlockArtifacts(storage.LiveBlockArtifacts, storage.LiveBlockNonfinalKind) error
	SetNonfinalCellLoader(cell.LazyCellLoader)
	BlockState(context.Context, ton.BlockIDExt) (*storage.BlockState, error)
	LoadStateCellTree(context.Context, ton.BlockIDExt, []byte) (*cell.Cell, error)
}

type SyncObserver interface {
	ObserveSyncBlock(SyncBlockObservation)
	ObserveSyncObtain(SyncObtainObservation)
	ObserveSyncPersist(SyncPersistObservation)
}

type SyncBlockSource string

const (
	SyncBlockSourceUnknown            SyncBlockSource = "unknown"
	SyncBlockSourceBroadcast          SyncBlockSource = "broadcast"
	SyncBlockSourceBroadcastQueue     SyncBlockSource = "broadcast_queue"
	SyncBlockSourceBroadcastCandidate SyncBlockSource = "broadcast_candidate"
	SyncBlockSourceBroadcastCache     SyncBlockSource = "broadcast_cache"
	SyncBlockSourceBroadcastHint      SyncBlockSource = "broadcast_hint"
	SyncBlockSourceQueue              SyncBlockSource = "queue"
	SyncBlockSourcePeerProbe          SyncBlockSource = "peer_probe"
	SyncBlockSourceNextBlock          SyncBlockSource = "next_block"
	SyncBlockSourceIndexed            SyncBlockSource = "indexed"
	SyncBlockSourceNextDescription    SyncBlockSource = "next_description"
	SyncBlockSourcePeerCatchUp        SyncBlockSource = "peer_catch_up"
	SyncBlockSourceCatchUp            SyncBlockSource = "catch_up"
	SyncBlockSourceProbe              SyncBlockSource = "probe"
	SyncBlockSourceStored             SyncBlockSource = "stored"
)

type SyncBlockOrigin string

const (
	SyncBlockOriginUnknown   SyncBlockOrigin = "unknown"
	SyncBlockOriginBroadcast SyncBlockOrigin = "broadcast"
	SyncBlockOriginDownload  SyncBlockOrigin = "download"
	SyncBlockOriginStored    SyncBlockOrigin = "stored"
	SyncBlockOriginOther     SyncBlockOrigin = "other"
)

type SyncBlockObservation struct {
	Pipeline         string
	Chain            string
	Shard            string
	Source           SyncBlockSource
	Origin           SyncBlockOrigin
	Result           string
	CatchUp          bool
	DownloadDuration time.Duration
	PrepareDuration  time.Duration
	ApplyDuration    time.Duration
}

type SyncObtainObservation struct {
	Pipeline string
	Stage    string
	Result   string
	CatchUp  bool
	Duration time.Duration
}

type SyncPersistObservation struct {
	Mode          string
	Result        string
	QueueDuration time.Duration
	Duration      time.Duration
	States        int
	Stages        []SyncPersistStageObservation
}

type SyncPersistStageObservation struct {
	Stage    string
	Duration time.Duration
}

func (s *Service) observeSyncBlock(observation SyncBlockObservation) {
	if s.sync == nil {
		return
	}
	if observation.Pipeline == "" {
		observation.Pipeline = "unknown"
	}
	if observation.Chain == "" {
		observation.Chain = "unknown"
	}
	if observation.Shard == "" {
		observation.Shard = "unknown"
	}
	if observation.Source == "" {
		observation.Source = SyncBlockSourceUnknown
	}
	if observation.Origin == "" {
		observation.Origin = SyncBlockOriginUnknown
	}
	if observation.Result == "" {
		observation.Result = "unknown"
	}
	s.sync.ObserveSyncBlock(observation)
}

func (s *Service) observeSyncObtain(observation SyncObtainObservation) {
	if s.sync == nil {
		return
	}
	if observation.Pipeline == "" {
		observation.Pipeline = "unknown"
	}
	if observation.Stage == "" {
		observation.Stage = "unknown"
	}
	if observation.Result == "" {
		observation.Result = "unknown"
	}
	s.sync.ObserveSyncObtain(observation)
}

func syncBlockOriginForSource(source SyncBlockSource) SyncBlockOrigin {
	switch source {
	case SyncBlockSourceBroadcast, SyncBlockSourceBroadcastQueue, SyncBlockSourceBroadcastCandidate, SyncBlockSourceBroadcastCache, SyncBlockSourceBroadcastHint, SyncBlockSourceQueue:
		return SyncBlockOriginBroadcast
	case SyncBlockSourcePeerProbe, SyncBlockSourceNextBlock, SyncBlockSourceIndexed, SyncBlockSourceNextDescription, SyncBlockSourcePeerCatchUp, SyncBlockSourceCatchUp, SyncBlockSourceProbe:
		return SyncBlockOriginDownload
	case SyncBlockSourceStored:
		return SyncBlockOriginStored
	case "":
		return SyncBlockOriginUnknown
	default:
		return SyncBlockOriginOther
	}
}

func syncBlockOriginForKind(kind string) SyncBlockOrigin {
	switch kind {
	case "tonNode.blockBroadcast", "tonNode.blockBroadcastCompressed", "tonNode.blockBroadcastCompressedV2", "tonNode.newShardBlockBroadcast":
		return SyncBlockOriginBroadcast
	case "local full block cache", "local next block cache", "stored block":
		return SyncBlockOriginStored
	default:
		return SyncBlockOriginDownload
	}
}

func syncBlockSourceForKind(defaultSource SyncBlockSource, kind string) SyncBlockSource {
	switch kind {
	case "tonNode.blockBroadcast", "tonNode.blockBroadcastCompressed", "tonNode.blockBroadcastCompressedV2":
		return SyncBlockSourceBroadcastCache
	case "tonNode.newShardBlockBroadcast":
		return SyncBlockSourceBroadcastHint
	case "local full block cache", "local next block cache", "stored block":
		return SyncBlockSourceStored
	default:
		return defaultSource
	}
}

func syncBlockResultForError(err error) string {
	if err == nil {
		return "success"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if isTimeoutError(err) {
		return "timeout"
	}
	if errors.Is(err, p2p.ErrBlockNotAvailable) || errors.Is(err, p2p.ErrStateNotAvailable) {
		return "miss"
	}
	if isExpectedRetryError(err) {
		return "retry"
	}
	return "error"
}

func (s *Service) observeSyncPersist(observation SyncPersistObservation) {
	if s.sync == nil {
		return
	}
	if observation.Mode == "" {
		observation.Mode = "unknown"
	}
	if observation.Result == "" {
		observation.Result = "unknown"
	}
	for i := range observation.Stages {
		if observation.Stages[i].Stage == "" {
			observation.Stages[i].Stage = "unknown"
		}
		if observation.Stages[i].Duration < 0 {
			observation.Stages[i].Duration = 0
		}
	}
	s.sync.ObserveSyncPersist(observation)
}

func syncChainLabel(block ton.BlockIDExt) string {
	if block.Workchain == -1 && block.Shard == topShard {
		return "masterchain"
	}
	if block.Workchain == 0 {
		return "shardchain"
	}
	return fmt.Sprintf("workchain_%d", block.Workchain)
}

func syncShardLabel(block ton.BlockIDExt) string {
	if block.Workchain == -1 && block.Shard == topShard {
		return "masterchain"
	}
	if block.Workchain == 0 && block.Shard == topShard {
		return "basechain"
	}
	return fmt.Sprintf("%016x", uint64(block.Shard))
}

type StatusSnapshot struct {
	p2p.StatusSnapshot
	BlockSync               blocksync.StatusSnapshot
	AppliedMasterchain      *ton.BlockIDExt
	LocalMasterchain        *ton.BlockIDExt
	LocalBasechain          *ton.BlockIDExt
	LocalBasechainShards    []ShardStatusSnapshot
	LocalStateLoaded        bool
	LocalStateError         string
	AppliedMasterchainUtime int64
	LocalMasterchainUtime   int64
	LocalBasechainUtime     int64
	LocalMasterchainTx      uint32
	LocalBasechainTx        uint32
	LocalMasterchainHasTx   bool
	LocalBasechainHasTx     bool
	RecentTPS               StatusTPSSnapshot
	BackgroundTask          string
}

type ShardStatusSnapshot struct {
	Block           ton.BlockIDExt
	Utime           int64
	Transactions    uint32
	HasTransactions bool
}

type StatusTPSSnapshot struct {
	WindowMasters   int
	Transactions    uint64
	DurationSeconds int64
	TPS             float64
	Complete        bool
}

func New(logger zerolog.Logger, node *p2p.Node, blockSync *blocksync.Service, store storage.Storage, stateSync *state.Syncer, opts Options) *Service {
	if opts.ArchiveCatchUpCheckpointBlocks == 0 {
		opts.ArchiveCatchUpCheckpointBlocks = DefaultArchiveCatchUpCheckpointBlocks
	}
	if opts.ArchiveCatchUpCheckpointPeriod == 0 {
		opts.ArchiveCatchUpCheckpointPeriod = DefaultArchiveCatchUpCheckpointPeriod
	}
	if opts.ArchiveCatchUpPrefetchWindows == 0 {
		opts.ArchiveCatchUpPrefetchWindows = DefaultArchiveCatchUpPrefetchWindows
	}
	if opts.NextBlockCheckpointBlocks == 0 {
		opts.NextBlockCheckpointBlocks = DefaultNextBlockCheckpointBlocks
	}
	if opts.CheckpointBytes == 0 {
		opts.CheckpointBytes = DefaultCheckpointBytes
	}
	if opts.SyncBackpressureWindows == 0 {
		opts.SyncBackpressureWindows = DefaultSyncBackpressureWindows
	}
	if opts.ShutdownContext == nil {
		opts.ShutdownContext = context.Background()
	}
	if opts.MinSyncDiskFreeBytes == 0 {
		opts.MinSyncDiskFreeBytes = defaultMinSyncDiskFreeBytes
	}
	if opts.MinStateSerializationDiskFreeBytes == 0 {
		opts.MinStateSerializationDiskFreeBytes = stateSerializationMinDiskFreeBytes
	}

	svc := &Service{
		log:                                logger,
		node:                               node,
		blockSync:                          blockSync,
		storage:                            store,
		stateSync:                          stateSync,
		liveState:                          opts.CurrentStatePublisher,
		liveBlockCache:                     opts.LiveBlockCache,
		liveStateUsesBlockCache:            opts.CurrentStatePublisherUsesLiveBlockCache,
		sync:                               opts.SyncObserver,
		applyHooks:                         newBlockApplyHookRunner(logger, opts.Extension),
		externalMessageChecker:             opts.ExternalMessageChecker,
		externalMessageHooks:               newExternalMessageHookRunner(logger, opts.Extension),
		blockReceivedHooks:                 newBlockReceivedHookRunner(logger, opts.Extension),
		stateCellPrewrite:                  newStateCellPrewriter(logger, store, checkpointBackpressureBytes(opts.CheckpointBytes, opts.SyncBackpressureWindows)),
		artifactPrewrite:                   newArtifactPrewriter(logger, store, checkpointBackpressureBytes(opts.CheckpointBytes, opts.SyncBackpressureWindows)),
		archiveCatchUpCheckpointBlocks:     opts.ArchiveCatchUpCheckpointBlocks,
		archiveCatchUpCheckpointPeriod:     opts.ArchiveCatchUpCheckpointPeriod,
		archiveCatchUpPrefetchWindows:      opts.ArchiveCatchUpPrefetchWindows,
		nextBlockCheckpointBlocks:          opts.NextBlockCheckpointBlocks,
		checkpointBytes:                    opts.CheckpointBytes,
		syncBackpressureWindows:            opts.SyncBackpressureWindows,
		shutdownContext:                    opts.ShutdownContext,
		currentStateWake:                   make(chan struct{}, 1),
		shardDescriptionWake:               make(chan struct{}, 1),
		shardDescriptionHints:              map[storage.BlockRootHash]shardDescriptionHint{},
		preparedShardBlockSlots:            make(chan struct{}, preparedShardBlockWorkers),
		maintenanceWake:                    make(chan struct{}, 1),
		stateTTL:                           opts.StateTTL,
		archiveTTL:                         opts.ArchiveTTL,
		archiveFromZero:                    opts.ArchiveFromZero,
		syncUntil:                          opts.SyncUntil,
		syncDiskSpacePath:                  opts.StorageDir,
		minSyncDiskFreeBytes:               opts.MinSyncDiskFreeBytes,
		minStateSerializationDiskFreeBytes: opts.MinStateSerializationDiskFreeBytes,
	}
	svc.stateSerializer = newStateSerializer(logger, store, opts.StateFilesDir, opts.DisableStateSerialization, opts.PersistentStateLargeBOCBatchSize, opts.StateSerializeOnePass)
	if svc.liveState != nil {
		svc.liveState.SetNonfinalCellLoader(svc.stateCellLoader())
	}
	return svc
}

func (s *Service) nextCheckpointBlocks() uint32 {
	if s.nextBlockCheckpointBlocks != 0 {
		return s.nextBlockCheckpointBlocks
	}
	return DefaultNextBlockCheckpointBlocks
}

func (s *Service) checkpointBytesTarget() uint64 {
	if s.checkpointBytes != 0 {
		return s.checkpointBytes
	}
	return DefaultCheckpointBytes
}

func (s *Service) syncBackpressureWindowCount() uint32 {
	if s.syncBackpressureWindows != 0 {
		return s.syncBackpressureWindows
	}
	return DefaultSyncBackpressureWindows
}

func checkpointBackpressureBlocks(target uint32, windows uint32) uint32 {
	if windows == 0 {
		windows = DefaultSyncBackpressureWindows
	}
	if target > ^uint32(0)/windows {
		return ^uint32(0)
	}
	return target * windows
}

func checkpointBackpressureBytes(target uint64, windows uint32) uint64 {
	if windows == 0 {
		windows = DefaultSyncBackpressureWindows
	}
	windowCount := uint64(windows)
	if target > ^uint64(0)/windowCount {
		return ^uint64(0)
	}
	return target * windowCount
}

func (s *Service) Start(ctx context.Context) {
	s.startOnce.Do(func() {
		s.stateCellPrewrite.start(ctx, s.currentStatePersistContext(), s.runAsync)
		s.artifactPrewrite.start(ctx, s.runAsync)
		s.runAsync(func() {
			s.runInitialStateSync(ctx)
		})
		s.runAsync(func() {
			s.runBlockProcessor(ctx)
		})
		s.runAsync(func() {
			s.runShardDescriptionProcessor(ctx)
		})
		s.runAsync(func() {
			s.runDelayedServiceMaintenance(ctx)
		})
	})
}

func (s *Service) runAsync(fn func()) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		fn()
	}()
}

func (s *Service) Wait() {
	s.wg.Wait()
}

func (s *Service) StatusSnapshot() StatusSnapshot {
	snapshot := StatusSnapshot{
		StatusSnapshot: s.node.StatusSnapshot(),
		BlockSync:      s.blockSync.StatusSnapshot(),
		BackgroundTask: s.backgroundTaskStatus(),
	}

	ctx := context.Background()
	current, err := s.currentStatusSnapshot(ctx)
	if err == nil {
		snapshot.LocalStateLoaded = true
		master := current.Masterchain.Block
		masterMetrics := s.blockStatusMetrics(ctx, &current.Masterchain)
		snapshot.LocalMasterchain = &master
		snapshot.LocalMasterchainUtime = masterMetrics.Utime
		snapshot.LocalMasterchainTx = masterMetrics.Transactions
		snapshot.LocalMasterchainHasTx = masterMetrics.HasTransactions
		snapshot.RecentTPS = s.recentTPSSnapshot(ctx, current, statusTPSMasterWindow)
		for _, shard := range current.Shards {
			if shard.Block.Workchain != 0 {
				continue
			}

			block := shard.Block
			metrics := s.blockStatusMetrics(ctx, &shard)
			snapshot.LocalBasechainShards = append(snapshot.LocalBasechainShards, ShardStatusSnapshot{
				Block:           block,
				Utime:           metrics.Utime,
				Transactions:    metrics.Transactions,
				HasTransactions: metrics.HasTransactions,
			})
			if block.Shard == topShard {
				snapshot.LocalBasechain = &block
				snapshot.LocalBasechainUtime = metrics.Utime
				snapshot.LocalBasechainTx = metrics.Transactions
				snapshot.LocalBasechainHasTx = metrics.HasTransactions
			}
		}
		s.populateStatusLatestBasechain(ctx, &snapshot, current)
		sort.Slice(snapshot.LocalBasechainShards, func(i, j int) bool {
			left := snapshot.LocalBasechainShards[i].Block
			right := snapshot.LocalBasechainShards[j].Block
			if left.Workchain != right.Workchain {
				return left.Workchain < right.Workchain
			}
			return uint64(left.Shard) < uint64(right.Shard)
		})
	} else if !errors.Is(err, storage.ErrNotFound) {
		snapshot.LocalStateError = err.Error()
	}

	appliedMaster := s.appliedMasterchainStatusSnapshot()
	if current != nil && (appliedMaster == nil || appliedMaster.Block.SeqNo < current.Masterchain.Block.SeqNo) {
		appliedMaster = storage.CloneBlockState(&current.Masterchain)
	}
	if appliedMaster != nil {
		master := appliedMaster.Block
		metrics := s.blockStatusMetrics(ctx, appliedMaster)
		snapshot.AppliedMasterchain = &master
		snapshot.AppliedMasterchainUtime = metrics.Utime
	}

	return snapshot
}

func (s *Service) SyncLagSeconds() (int64, error) {
	nowUnix := time.Now().Unix()
	var maxLag int64
	hasLag := false

	s.currentStatusMu.RLock()
	defer s.currentStatusMu.RUnlock()

	current := s.currentStatus
	if current == nil {
		return 0, fmt.Errorf("current state is missing for sync lag: %w", storage.ErrNotFound)
	}
	if current.Masterchain.Parsed != nil && current.Masterchain.Parsed.GenUTime != 0 {
		maxLag = nowUnix - int64(current.Masterchain.Parsed.GenUTime)
		hasLag = true
	}
	for _, shard := range current.Shards {
		if shard.Block.Workchain != 0 || shard.Parsed == nil || shard.Parsed.GenUTime == 0 {
			continue
		}

		lag := nowUnix - int64(shard.Parsed.GenUTime)
		if !hasLag || lag > maxLag {
			maxLag = lag
			hasLag = true
		}
	}
	if !hasLag {
		return 0, fmt.Errorf("current state has no block utime for sync lag: %w", storage.ErrNotFound)
	}
	return maxLag, nil
}

func (s *Service) populateStatusLatestBasechain(ctx context.Context, snapshot *StatusSnapshot, current *storage.CurrentState) {
	if snapshot == nil || current == nil {
		return
	}

	latestMaster := current.Masterchain.Block
	if snapshot.LatestMasterchain != nil && snapshot.LatestMasterchain.SeqNo > latestMaster.SeqNo {
		latestMaster = *snapshot.LatestMasterchain
	}

	shards, err := s.masterShardBlocks(ctx, latestMaster)
	if err == nil {
		setStatusLatestBasechain(snapshot, shards)
		return
	}
	if !latestMaster.Equals(&current.Masterchain.Block) {
		s.log.Debug().
			Err(err).
			Str("latest_masterchain", storage.FormatBlockRef(latestMaster)).
			Str("current_masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
			Msg("skip latest basechain status because latest shard blocks are unavailable")
		return
	}
	currentShards := make([]ton.BlockIDExt, 0, len(current.Shards))
	for _, shard := range current.Shards {
		currentShards = append(currentShards, shard.Block)
	}
	setStatusLatestBasechain(snapshot, currentShards)
}

func setStatusLatestBasechain(snapshot *StatusSnapshot, shards []ton.BlockIDExt) {
	latestByShard := make(map[storage.ShardKey]ton.BlockIDExt, len(snapshot.LatestBasechainShards)+len(shards))
	for _, shard := range snapshot.LatestBasechainShards {
		if shard.Workchain == 0 {
			latestByShard[storage.ShardKeyFromBlock(shard)] = shard
		}
	}
	if len(latestByShard) == 0 && snapshot.LatestBasechain != nil && snapshot.LatestBasechain.Workchain == 0 {
		latestByShard[storage.ShardKeyFromBlock(*snapshot.LatestBasechain)] = *snapshot.LatestBasechain
	}
	for _, shard := range shards {
		if shard.Workchain != 0 {
			continue
		}
		key := storage.ShardKeyFromBlock(shard)
		if current, ok := latestByShard[key]; !ok || shard.SeqNo > current.SeqNo {
			latestByShard[key] = shard
		}
	}
	if len(latestByShard) == 0 {
		return
	}

	baseShards := make([]ton.BlockIDExt, 0, len(latestByShard))
	var latest ton.BlockIDExt
	hasLatest := false
	for _, shard := range latestByShard {
		baseShards = append(baseShards, shard)
		if !hasLatest || shard.SeqNo > latest.SeqNo {
			latest = shard
			hasLatest = true
		}
	}

	sort.Slice(baseShards, func(i, j int) bool {
		left := storage.ShardKeyFromBlock(baseShards[i])
		right := storage.ShardKeyFromBlock(baseShards[j])
		if left.Workchain != right.Workchain {
			return left.Workchain < right.Workchain
		}
		return uint64(left.Shard) < uint64(right.Shard)
	})
	snapshot.LatestBasechain = &latest
	snapshot.LatestBasechainShards = baseShards
}

func (s *Service) currentStatusSnapshot(ctx context.Context) (*storage.CurrentState, error) {
	s.currentStatusMu.RLock()
	current := storage.CloneCurrentState(s.currentStatus)
	s.currentStatusMu.RUnlock()
	if current != nil {
		return current, nil
	}
	return s.storage.CurrentState(ctx)
}

func blockStateUtime(ctx context.Context, store storage.Storage, state *storage.BlockState) int64 {
	if state == nil {
		return 0
	}
	if state.Parsed != nil && state.Parsed.GenUTime != 0 {
		return int64(state.Parsed.GenUTime)
	}
	return blockUtimeFromMeta(ctx, store, &state.Block)
}

func blockUtimeFromMeta(ctx context.Context, store storage.Storage, block *ton.BlockIDExt) int64 {
	if store == nil || block == nil {
		return 0
	}

	meta, err := store.BlockMeta(ctx, *block)
	if err != nil || meta == nil || meta.GenUTime == 0 {
		return 0
	}
	return int64(meta.GenUTime)
}

func (s *Service) runInitialStateSync(ctx context.Context) {
	s.node.SetRebroadcastQuiet(true)
	quiet := true
	defer func() {
		if quiet {
			s.node.SetRebroadcastQuiet(false)
		}
	}()

	for {
		err := s.ensureCurrentState(ctx)
		if errors.Is(err, context.Canceled) {
			return
		}
		if err == nil {
			if quiet {
				s.node.SetRebroadcastQuiet(false)
				quiet = false
			}
			if s.syncUntilFrozen() {
				s.log.Info().
					Uint32("sync_until", s.syncUntil).
					Msg("current state sync stopped after sync_until")
				return
			}
			if !s.waitCurrentStatePoll(ctx, currentStateLivePollDelay) {
				return
			}
			continue
		}

		event := s.log.Error()
		message := "failed to initialize current state, will retry"
		retryDelay := 5 * time.Second
		if errors.Is(err, p2p.ErrStateNotAvailable) {
			retryDelay = time.Second
			event = s.log.Debug()
			message = "state snapshot is not available from current peers, will retry selected state"
		} else if isExpectedRetryError(err) {
			event = s.log.Debug()
		}
		event.Err(err).Dur("retry_in", retryDelay).Msg(message)

		if waitRetry(ctx, retryDelay) != nil {
			return
		}
	}
}

func (s *Service) runBlockProcessor(ctx context.Context) {
	masterHotJobs := make(chan blocksync.SyncedBlock, syncedBlockMasterHotQueueLimit)
	priorityJobs := make(chan blocksync.SyncedBlock, syncedBlockPriorityQueueLimit)
	jobs := make(chan blocksync.SyncedBlock, syncedBlockProcessorWorkers*2)
	var wg sync.WaitGroup
	for worker := 0; worker < syncedBlockMasterHotWorkers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.runBlockProcessorWorker(ctx, masterHotJobs, nil)
		}()
	}
	for worker := 0; worker < syncedBlockProcessorWorkers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.runBlockProcessorWorker(ctx, priorityJobs, jobs)
		}()
	}
	defer func() {
		close(masterHotJobs)
		close(priorityJobs)
		close(jobs)
		wg.Wait()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case synced, ok := <-s.blockSync.Blocks():
			if !ok {
				return
			}
			if s.syncUntilFrozen() {
				synced.Reject()
				continue
			}
			targetJobs := jobs
			if isHotMasterchainSyncedBlock(synced) {
				targetJobs = masterHotJobs
			} else if synced.Priority {
				targetJobs = priorityJobs
			}

			select {
			case targetJobs <- synced:
			case <-ctx.Done():
				synced.Reject()
				return
			}
		}
	}
}

func isHotMasterchainSyncedBlock(synced blocksync.SyncedBlock) bool {
	if !synced.Priority || synced.CatchUp {
		return false
	}
	block := synced.Downloaded.ID
	return block.Workchain == -1 && block.Shard == topShard
}

func (s *Service) runBlockProcessorWorker(ctx context.Context, priorityJobs, jobs <-chan blocksync.SyncedBlock) {
	for {
		synced, ok := nextSyncedBlockProcessorJob(ctx, priorityJobs, jobs)
		if !ok {
			return
		}
		if s.syncUntilFrozen() {
			synced.Reject()
			continue
		}

		if err := s.processSyncedBlock(ctx, synced); err != nil {
			synced.Reject()
			if errors.Is(err, errSyncedBlockDeferred) {
				continue
			}
			if !errors.Is(err, context.Canceled) {
				s.logProcessError(err)
			}
			continue
		}
		synced.Accept()
	}
}

func nextSyncedBlockProcessorJob(ctx context.Context, priorityJobs, jobs <-chan blocksync.SyncedBlock) (blocksync.SyncedBlock, bool) {
	select {
	case synced, ok := <-priorityJobs:
		if ok {
			return synced, true
		}
		priorityJobs = nil
	default:
	}

	for priorityJobs != nil || jobs != nil {
		select {
		case synced, ok := <-priorityJobs:
			if !ok {
				priorityJobs = nil
				continue
			}
			return synced, true
		case synced, ok := <-jobs:
			if !ok {
				jobs = nil
				continue
			}
			return synced, true
		case <-ctx.Done():
			return blocksync.SyncedBlock{}, false
		}
	}

	return blocksync.SyncedBlock{}, false
}

func (s *Service) processSyncedBlock(ctx context.Context, synced blocksync.SyncedBlock) (err error) {
	source := SyncBlockSourceBroadcast
	if synced.CatchUp {
		source = SyncBlockSourcePeerCatchUp
	}
	observation := SyncBlockObservation{
		Pipeline:         "blocksync",
		Chain:            syncChainLabel(synced.Downloaded.ID),
		Shard:            syncShardLabel(synced.Downloaded.ID),
		Source:           source,
		Origin:           syncBlockOriginForSource(source),
		Result:           "success",
		CatchUp:          synced.CatchUp,
		DownloadDuration: synced.DownloadElapsed,
	}
	defer func() {
		if errors.Is(err, errSyncedBlockDeferred) {
			return
		}
		if err != nil {
			observation.Result = syncBlockResultForError(err)
		}
		s.observeSyncBlock(observation)
	}()

	prepareStarted := time.Now()
	downloaded := synced.Downloaded
	verified, err := verifyDownloadedBlock(downloaded)
	if err != nil {
		observation.PrepareDuration = time.Since(prepareStarted)
		return err
	}

	if s.log.GetLevel() <= zerolog.DebugLevel {
		stats, err := StatsFromDownloadedBlock(downloaded)
		if err != nil {
			s.log.Debug().
				Err(err).
				Str("block", downloaded.BlockRef()).
				Str("overlay", synced.Trigger.Overlay).
				Bool("catch_up", synced.CatchUp).
				Msg("failed to collect block stats")
		} else {
			s.log.Debug().
				Str("block", downloaded.BlockRef()).
				Uint32("seqno", downloaded.ID.SeqNo).
				Str("overlay", synced.Trigger.Overlay).
				Bool("catch_up", synced.CatchUp).
				Int("transactions", stats.Transactions).
				Msg("processed synced block")
		}
	}

	if downloaded.ID.Workchain == -1 && downloaded.ID.Shard == topShard {
		checked, err := s.validateSyncedMasterchainBlock(ctx, verified)
		if err != nil {
			observation.PrepareDuration = time.Since(prepareStarted)
			if errors.Is(err, storage.ErrNotFound) {
				s.queueMasterchainBroadcastCandidateFromSource(verified, synced.Trigger.SourcePeerID)
				s.wakeCurrentStateSync()
				return errSyncedBlockDeferred
			}
			return err
		}
		verified.consensusChecked = checked
		prepared, err := prepareVerifiedBlockForApply(verified)
		if err != nil {
			observation.PrepareDuration = time.Since(prepareStarted)
			return err
		}
		prepared.PrepareElapsed = time.Since(prepareStarted)
		observation.PrepareDuration = prepared.PrepareElapsed
		s.rememberSeenMasterchainBlock(prepared.ID)
		s.queuePreparedMasterchainBlockFromSource(prepared, synced.Trigger.SourcePeerID)
		s.wakeCurrentStateSync()
		return nil
	}

	// Shard states are advanced only by the main sync pipeline. Broadcast blocks
	// are kept as download/cache hints so they cannot compete with live tail
	// apply; preparing them ahead here turns the commit-stage resolve into a
	// take of ready state-update cells.
	s.prepareShardBlockAheadFromVerified(verified)
	observation.PrepareDuration = time.Since(prepareStarted)
	return nil
}

func (s *Service) validateSyncedMasterchainBlock(ctx context.Context, block VerifiedBlock) (*checkedMasterchainConsensus, error) {
	prev, checked, err := s.validateMasterchainBlockConsensus(ctx, block, "validate synced masterchain block")
	if errors.Is(err, storage.ErrNotFound) {
		s.log.Debug().
			Str("block", block.BlockRef()).
			Str("prev", storage.FormatBlockRef(prev)).
			Msg("skipping synced masterchain block until previous state is available for signature validation")
	}
	return checked, err
}

func (s *Service) validateVerifiedMasterchainBlock(ctx context.Context, block VerifiedBlock) (*checkedMasterchainConsensus, error) {
	_, checked, err := s.validateMasterchainBlockConsensus(ctx, block, "validate masterchain block")
	return checked, err
}

func (s *Service) validateMasterchainBlockConsensus(ctx context.Context, block VerifiedBlock, validationLabel string) (ton.BlockIDExt, *checkedMasterchainConsensus, error) {
	if block.Meta == nil || len(block.Meta.PrevRefs) != 1 {
		return ton.BlockIDExt{}, nil, fmt.Errorf("masterchain block %s has no single previous ref", block.BlockRef())
	}

	prev := block.Meta.PrevRefs[0]
	current, err := s.loadMasterStateForConsensus(ctx, prev)
	if errors.Is(err, storage.ErrNotFound) {
		return prev, nil, fmt.Errorf("previous masterchain state %s: %w", storage.FormatBlockRef(prev), storage.ErrNotFound)
	}
	if err != nil {
		return prev, nil, fmt.Errorf("load previous masterchain state %s: %w", storage.FormatBlockRef(prev), err)
	}

	if block.consensus == nil {
		return prev, nil, fmt.Errorf("masterchain block %s has no prepared consensus proof", block.BlockRef())
	}
	checked, err := s.checkMasterchainBlockConsensusWithProof(current, block.consensus)
	if err != nil {
		return prev, nil, fmt.Errorf("%s %s: %w", validationLabel, block.BlockRef(), err)
	}
	return prev, checked, nil
}

func waitRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (s *Service) waitCurrentStatePoll(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-s.currentStateWake:
		return true
	case <-timer.C:
		return true
	}
}

func (s *Service) wakeCurrentStateSync() {
	select {
	case s.currentStateWake <- struct{}{}:
	default:
	}
	s.wakeServiceMaintenance()
}

func (s *Service) wakeServiceMaintenance() {
	select {
	case s.maintenanceWake <- struct{}{}:
	default:
	}
}

func (s *Service) logProcessError(err error) {
	event := s.log.Error()
	if isExpectedRetryError(err) {
		event = s.log.Debug()
	}
	event.Err(err).Msg("failed to process synced block")
}

func isExpectedRetryError(err error) bool {
	if errors.Is(err, p2p.ErrStateNotAvailable) ||
		errors.Is(err, p2p.ErrBlockNotAvailable) ||
		errors.Is(err, errCellGenerationMigrationRunning) ||
		errors.Is(err, errCellGenerationMigrationStopping) ||
		errors.Is(err, errCellGenerationMigrationStopped) ||
		errors.Is(err, errPendingCellGenerationCompaction) ||
		errors.Is(err, errPersistentStateGCActive) ||
		errors.Is(err, errArchiveTTLGCActive) ||
		errors.Is(err, errExclusiveServiceTaskHighReadAmp) ||
		errors.Is(err, errExclusiveServiceTaskHighLag) ||
		errors.Is(err, errStateSerializationDelayed) ||
		errors.Is(err, errStateSerializationLowDiskSpace) ||
		errors.Is(err, errStateSerializationCanceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	return isTimeoutError(err)
}

func isTimeoutError(err error) bool {
	var timeout timeoutError
	return errors.As(err, &timeout) && timeout.Timeout()
}

func formatCatchUpProgress(done, total uint32) string {
	if total == 0 || done >= total {
		return "100.0%"
	}

	return formatCatchUpProgressPercent(float64(done) * 100 / float64(total))
}

func formatLagCatchUpProgress(startRemaining int64, remaining int64) string {
	if startRemaining <= 0 || remaining <= 0 {
		return "100.0%"
	}
	if remaining >= startRemaining {
		return "0.0%"
	}

	progress := float64(startRemaining-remaining) * 100 / float64(startRemaining)
	return formatCatchUpProgressPercent(progress)
}

func formatCatchUpProgressPercent(progress float64) string {
	if progress > 0 && progress < 0.1 {
		return fmt.Sprintf("%.3f%%", progress)
	}
	if progress > 100 {
		progress = 100
	}
	return fmt.Sprintf("%.1f%%", progress)
}

func formatBlockRate(done uint32, elapsed time.Duration) string {
	return formatBlockRate64(uint64(done), elapsed)
}

func formatBlockRate64(done uint64, elapsed time.Duration) string {
	if done == 0 || elapsed <= 0 {
		return "0 blocks/s"
	}

	rate := float64(done) / elapsed.Seconds()
	switch {
	case rate >= 100:
		return fmt.Sprintf("%.0f blocks/s", rate)
	case rate >= 10:
		return fmt.Sprintf("%.1f blocks/s", rate)
	default:
		return fmt.Sprintf("%.2f blocks/s", rate)
	}
}

func formatCatchUpETA(done, total uint32, elapsed time.Duration) string {
	if done == 0 || done >= total || elapsed <= 0 {
		return "unknown"
	}

	rate := float64(done) / elapsed.Seconds()
	if rate <= 0 {
		return "unknown"
	}

	remaining := total - done
	eta := time.Duration(float64(remaining) / rate * float64(time.Second)).Round(time.Second)
	return eta.String()
}

func formatLagCatchUpETA(startRemaining int64, remaining int64, elapsed time.Duration) string {
	if startRemaining <= 0 || remaining <= 0 || remaining >= startRemaining || elapsed <= 0 {
		return "unknown"
	}

	done := startRemaining - remaining
	rate := float64(done) / elapsed.Seconds()
	if rate <= 0 {
		return "unknown"
	}

	eta := time.Duration(float64(remaining) / rate * float64(time.Second)).Round(time.Second)
	return eta.String()
}
