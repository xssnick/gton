package service

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/xssnick/gton/service/blocksync"
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
	shardStateDownloadBuffer           = 32
	shardStateDownloadWorkers          = 4
	nextBlockDescriptionLookahead      = shardStateDownloadBuffer
	shardStateCatchUpRetryDelay        = time.Second
	currentStateLivePollDelay          = 300 * time.Millisecond
	syncDiskSpaceRetryDelay            = 5 * time.Second
	defaultMinSyncDiskFreeBytes        = 10 << 30
	stateSerializationMinDiskFreeBytes = 30 << 30
	nextBlockBootstrapBlocks           = 0
	nextBlockBootstrapProbeTimeout     = 2 * time.Second
	nextBlockBootstrapProbePeers       = 3
	nextBlockBootstrapUrgentPeers      = 8
	chainBlockDownloadRetries          = 3
	chainBlockDownloadRetryDelay       = 500 * time.Millisecond
	nextMasterchainQueueLimit          = 64
	masterStateCacheLimit              = 2048
	syncedBlockProcessorWorkers        = 8
	statusTPSMasterWindow              = 10
)

const (
	DefaultNextBlockCheckpointBlocks = 600
	DefaultCheckpointBytes           = uint64(1 << 30)
)

var (
	errMasterchainPrevMismatch = errors.New("masterchain previous state is not current")
	errSyncedBlockNotVerified  = errors.New("synced block was not verified")
	errShardDescriptionTooOld  = errors.New("shard block description is older than current state")
	errShardDescriptionTooNew  = errors.New("shard block description is too far ahead of current state")
)
var errShardCatchUpNeedsSnapshot = errors.New("shard catch-up requires state snapshot")

type shardBlockLoader func(context.Context, ton.BlockIDExt) (p2p.DownloadedBlock, error)

type shardStateDownload struct {
	prev            ton.BlockIDExt
	block           p2p.DownloadedBlock
	err             error
	source          string
	downloadElapsed time.Duration
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
	resolverWait time.Duration
	applied      int
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
	log       zerolog.Logger
	node      *p2p.Node
	blockSync *blocksync.Service
	storage   storage.Storage
	stateSync *state.Syncer
	liveState CurrentStatePublisher
	sync      SyncObserver

	appliedArtifacts *appliedBlockArtifactWriter

	archiveCatchUpCheckpointBlocks uint32
	archiveCatchUpCheckpointPeriod time.Duration
	archiveCatchUpPrefetchWindows  int
	nextBlockCheckpointBlocks      uint32
	checkpointBytes                uint64
	shutdownContext                context.Context

	stateMu sync.Mutex

	currentStateWake      chan struct{}
	shardDescriptionWake  chan struct{}
	shardDescriptionMu    sync.Mutex
	shardDescriptionHints map[string]shardDescriptionHint
	shardDescriptionOrder []string

	nextMasterchainMx    sync.Mutex
	nextMasterchainQueue map[string]p2p.DownloadedBlock
	validatorCache       masterchainValidatorCache
	masterStateCacheMu   sync.Mutex
	masterStateCache     map[string]*storage.BlockState
	masterStateCacheKeys []string

	currentStatePersistMu              sync.Mutex
	currentStatePersistErrMu           sync.Mutex
	currentStatePersistErr             error
	stateCellLoaderMu                  sync.RWMutex
	stateCellLoaders                   map[uint64]cell.LazyCellLoader
	nextStateCellLoaderID              uint64
	stateSerializer                    *stateSerializer
	maintenanceWake                    chan struct{}
	stateTTL                           time.Duration
	archiveTTL                         time.Duration
	syncDiskSpacePath                  string
	minSyncDiskFreeBytes               uint64
	minStateSerializationDiskFreeBytes uint64
	syncDiskSpaceProbe                 syncDiskSpaceProbe
	syncDiskSpaceRetryDelay            time.Duration
	exclusiveTaskMu                    sync.Mutex
	exclusiveTask                      exclusiveServiceTask
	cellMigrationMu                    sync.Mutex
	cellGenerationSwitching            bool
	currentStatusMu                    sync.RWMutex
	currentStatus                      *storage.CurrentState

	startOnce sync.Once
	wg        sync.WaitGroup
}

type Options struct {
	ArchiveCatchUpCheckpointBlocks     uint32
	ArchiveCatchUpCheckpointPeriod     time.Duration
	ArchiveCatchUpPrefetchWindows      int
	NextBlockCheckpointBlocks          uint32
	CheckpointBytes                    uint64
	CurrentStatePublisher              CurrentStatePublisher
	ShutdownContext                    context.Context
	StateFilesDir                      string
	StateTTL                           time.Duration
	ArchiveTTL                         time.Duration
	StorageDir                         string
	MinSyncDiskFreeBytes               uint64
	MinStateSerializationDiskFreeBytes uint64
	DisableStateSerialization          bool
	SyncObserver                       SyncObserver
}

type CurrentStatePublisher interface {
	SetLiveCurrentState(*storage.CurrentState)
}

type SyncObserver interface {
	ObserveSyncCurrentState(SyncCurrentStateObservation)
	ObserveSyncBlock(SyncBlockObservation)
	ObserveSyncPersist(SyncPersistObservation)
}

type SyncCurrentStateObservation struct {
	MasterchainSeqno uint32
	ShardClientSeqno uint32
}

type SyncBlockObservation struct {
	Pipeline         string
	Chain            string
	Source           string
	Result           string
	CatchUp          bool
	DownloadDuration time.Duration
	ApplyDuration    time.Duration
}

type SyncPersistObservation struct {
	Mode          string
	Result        string
	QueueDuration time.Duration
	Duration      time.Duration
	States        int
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
	if observation.Source == "" {
		observation.Source = "unknown"
	}
	if observation.Result == "" {
		observation.Result = "unknown"
	}
	s.sync.ObserveSyncBlock(observation)
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

type StatusSnapshot struct {
	p2p.StatusSnapshot
	BlockSync             blocksync.StatusSnapshot
	LocalMasterchain      *ton.BlockIDExt
	LocalBasechain        *ton.BlockIDExt
	LocalBasechainShards  []ShardStatusSnapshot
	LocalStateLoaded      bool
	LocalStateError       string
	LocalMasterchainUtime int64
	LocalBasechainUtime   int64
	LocalMasterchainTx    uint32
	LocalBasechainTx      uint32
	LocalMasterchainHasTx bool
	LocalBasechainHasTx   bool
	RecentTPS             StatusTPSSnapshot
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
	if opts.ShutdownContext == nil {
		opts.ShutdownContext = context.Background()
	}
	if opts.StateTTL <= 0 {
		opts.StateTTL = 3 * 24 * time.Hour
	}
	if opts.ArchiveTTL <= 0 {
		opts.ArchiveTTL = 7 * 24 * time.Hour
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
		sync:                               opts.SyncObserver,
		appliedArtifacts:                   newAppliedBlockArtifactWriter(logger, store, appliedBlockArtifactFlusher(opts.CurrentStatePublisher)),
		archiveCatchUpCheckpointBlocks:     opts.ArchiveCatchUpCheckpointBlocks,
		archiveCatchUpCheckpointPeriod:     opts.ArchiveCatchUpCheckpointPeriod,
		archiveCatchUpPrefetchWindows:      opts.ArchiveCatchUpPrefetchWindows,
		nextBlockCheckpointBlocks:          opts.NextBlockCheckpointBlocks,
		checkpointBytes:                    opts.CheckpointBytes,
		shutdownContext:                    opts.ShutdownContext,
		currentStateWake:                   make(chan struct{}, 1),
		shardDescriptionWake:               make(chan struct{}, 1),
		shardDescriptionHints:              map[string]shardDescriptionHint{},
		maintenanceWake:                    make(chan struct{}, 1),
		stateTTL:                           opts.StateTTL,
		archiveTTL:                         opts.ArchiveTTL,
		syncDiskSpacePath:                  opts.StorageDir,
		minSyncDiskFreeBytes:               opts.MinSyncDiskFreeBytes,
		minStateSerializationDiskFreeBytes: opts.MinStateSerializationDiskFreeBytes,
	}
	svc.stateSerializer = newStateSerializer(logger, store, opts.StateFilesDir, opts.DisableStateSerialization)
	svc.configureLiveBlockPublisher(opts.CurrentStatePublisher)
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

func checkpointBackpressureBlocks(target uint32) uint32 {
	if target > ^uint32(0)/2 {
		return ^uint32(0)
	}
	return target * 2
}

func checkpointBackpressureBytes(target uint64) uint64 {
	if target > ^uint64(0)/2 {
		return ^uint64(0)
	}
	return target * 2
}

func (s *Service) Start(ctx context.Context) error {
	s.startOnce.Do(func() {
		if s.appliedArtifacts != nil {
			s.runAsync(func() {
				s.appliedArtifacts.run(ctx)
			})
		}
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
			s.runServiceMaintenance(ctx)
		})
	})
	return nil
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
	}
	if s.blockSync != nil {
		snapshot.BlockSync = s.blockSync.StatusSnapshot()
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

	return snapshot
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
	jobs := make(chan blocksync.SyncedBlock, syncedBlockProcessorWorkers*2)
	var wg sync.WaitGroup
	for worker := 0; worker < syncedBlockProcessorWorkers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.runBlockProcessorWorker(ctx, jobs)
		}()
	}
	defer func() {
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
			select {
			case jobs <- synced:
			case <-ctx.Done():
				synced.Reject()
				return
			}
		}
	}
}

func (s *Service) runBlockProcessorWorker(ctx context.Context, jobs <-chan blocksync.SyncedBlock) {
	for synced := range jobs {
		if err := s.processSyncedBlock(ctx, synced); err != nil {
			synced.Reject()
			if !errors.Is(err, context.Canceled) {
				s.logProcessError(err)
			}
			continue
		}
		synced.Accept()
	}
}

func (s *Service) processSyncedBlock(ctx context.Context, synced blocksync.SyncedBlock) (err error) {
	started := time.Now()
	source := "broadcast"
	if synced.CatchUp {
		source = "catch_up"
	}
	observation := SyncBlockObservation{
		Pipeline:         "blocksync",
		Chain:            syncChainLabel(synced.Downloaded.ID),
		Source:           source,
		Result:           "success",
		CatchUp:          synced.CatchUp,
		DownloadDuration: synced.DownloadElapsed,
	}
	defer func() {
		if err != nil {
			observation.Result = "error"
		}
		observation.ApplyDuration = time.Since(started)
		s.observeSyncBlock(observation)
	}()

	downloaded, err := prepareDownloadedBlock(synced.Downloaded)
	if err != nil {
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
		if err := s.validateSyncedMasterchainBlock(ctx, downloaded); err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return fmt.Errorf("%w: %v", errSyncedBlockNotVerified, err)
			}
			return err
		}
		downloaded, err = prepareDownloadedBlockStateCells(downloaded)
		if err != nil {
			return err
		}
		s.rememberSeenMasterchainBlock(downloaded.ID)
		s.queueVerifiedMasterchainBlock(downloaded)
		s.wakeCurrentStateSync()
		return nil
	}

	// Shard states are advanced only by the main sync pipeline. Broadcast blocks
	// are kept as download/cache hints so they cannot compete with live tail apply.
	return nil
}

func (s *Service) validateSyncedMasterchainBlock(ctx context.Context, downloaded p2p.DownloadedBlock) error {
	if downloaded.Meta == nil || len(downloaded.Meta.PrevRefs) != 1 {
		return fmt.Errorf("masterchain block %s has no single previous ref", downloaded.BlockRef())
	}

	prev := downloaded.Meta.PrevRefs[0]
	current, err := s.loadMasterStateForConsensus(ctx, prev)
	if errors.Is(err, storage.ErrNotFound) {
		s.log.Debug().
			Str("block", downloaded.BlockRef()).
			Str("prev", storage.FormatBlockRef(prev)).
			Msg("skipping synced masterchain block until previous state is available for signature validation")
		return fmt.Errorf("previous masterchain state %s: %w", storage.FormatBlockRef(prev), storage.ErrNotFound)
	}
	if err != nil {
		return fmt.Errorf("load previous masterchain state %s: %w", storage.FormatBlockRef(prev), err)
	}

	if err = s.validateMasterchainBlockConsensusWithProof(current, tonBlockForConsensus{block: downloaded.ID, proofBOC: downloaded.ProofBOC, broadcastSignatures: downloaded.BroadcastSignatures}, nil); err != nil {
		return fmt.Errorf("validate synced masterchain block %s: %w", downloaded.BlockRef(), err)
	}
	return nil
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
	s.wakePersistentStateSerializer()
}

func (s *Service) wakePersistentStateSerializer() {
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
		errors.Is(err, errSyncedBlockNotVerified) ||
		errors.Is(err, errCellGenerationMigrationRunning) ||
		errors.Is(err, errPersistentStateGCActive) ||
		errors.Is(err, errArchiveTTLGCActive) ||
		errors.Is(err, errExclusiveServiceTaskHighReadAmp) ||
		errors.Is(err, errExclusiveServiceTaskHighLag) ||
		errors.Is(err, errStateSerializationLowDiskSpace) ||
		errors.Is(err, errStateSerializationCanceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var timeout interface {
		Timeout() bool
	}
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
