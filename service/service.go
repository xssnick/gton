package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"flexserver/service/blocksync"
	"flexserver/service/p2p"
	"flexserver/service/state"
	"flexserver/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
)

const (
	topShard                         = int64(-1 << 63)
	shardStateCatchUpParallelism     = 4
	shardStateDownloadBuffer         = 32
	shardStateDownloadWorkers        = 4
	nextBlockDescriptionLookahead    = shardStateDownloadBuffer
	shardStateCatchUpRetryDelay      = time.Second
	currentStateLivePollDelay        = 300 * time.Millisecond
	nextBlockCatchUpCheckpointBlocks = 300
	nextBlockIdleCheckpointDelay     = 2 * time.Second
	nextBlockBootstrapBlocks         = 0
	nextBlockBootstrapProbeTimeout   = 2 * time.Second
	nextBlockBootstrapProbePeers     = 3
	nextBlockBootstrapUrgentPeers    = 8
	chainBlockDownloadRetries        = 3
	chainBlockDownloadRetryDelay     = 500 * time.Millisecond
	nextMasterchainQueueLimit        = 64
	masterStateCacheLimit            = 2048
	syncedBlockProcessorWorkers      = 8
)

var (
	errMasterchainPrevMismatch = errors.New("masterchain previous state is not current")
	errSyncedBlockNotVerified  = errors.New("synced block was not verified")
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

type currentStateResult struct {
	state   *storage.CurrentState
	changed bool
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

	appliedArtifacts *appliedBlockArtifactWriter

	archiveCatchUpCheckpointBlocks uint32
	archiveCatchUpCheckpointPeriod time.Duration
	archiveCatchUpPrefetchWindows  int
	shutdownContext                context.Context

	stateMu sync.Mutex

	currentStateWake chan struct{}

	nextMasterchainMx    sync.Mutex
	nextMasterchainQueue map[string]p2p.DownloadedBlock
	validatorCache       masterchainValidatorCache
	masterStateCacheMu   sync.Mutex
	masterStateCache     map[string]*storage.BlockState
	masterStateCacheKeys []string

	currentStatePersistMu    sync.Mutex
	currentStatePersistErrMu sync.Mutex
	currentStatePersistErr   error
	currentStatusMu          sync.RWMutex
	currentStatus            *storage.CurrentState

	startOnce sync.Once
	wg        sync.WaitGroup
}

type Options struct {
	ArchiveCatchUpCheckpointBlocks uint32
	ArchiveCatchUpCheckpointPeriod time.Duration
	ArchiveCatchUpPrefetchWindows  int
	CurrentStatePublisher          CurrentStatePublisher
	ShutdownContext                context.Context
}

type CurrentStatePublisher interface {
	SetLiveCurrentState(*storage.CurrentState)
}

type StatusSnapshot struct {
	p2p.StatusSnapshot
	LocalMasterchain      *ton.BlockIDExt
	LocalBasechain        *ton.BlockIDExt
	LocalStateLoaded      bool
	LocalStateError       string
	LocalMasterchainUtime int64
	LocalBasechainUtime   int64
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
	if opts.ShutdownContext == nil {
		opts.ShutdownContext = context.Background()
	}

	svc := &Service{
		log:                            logger,
		node:                           node,
		blockSync:                      blockSync,
		storage:                        store,
		stateSync:                      stateSync,
		liveState:                      opts.CurrentStatePublisher,
		appliedArtifacts:               newAppliedBlockArtifactWriter(logger, store, appliedBlockArtifactFlusher(opts.CurrentStatePublisher)),
		archiveCatchUpCheckpointBlocks: opts.ArchiveCatchUpCheckpointBlocks,
		archiveCatchUpCheckpointPeriod: opts.ArchiveCatchUpCheckpointPeriod,
		archiveCatchUpPrefetchWindows:  opts.ArchiveCatchUpPrefetchWindows,
		shutdownContext:                opts.ShutdownContext,
		currentStateWake:               make(chan struct{}, 1),
	}
	svc.configureLiveBlockPublisher(opts.CurrentStatePublisher)
	return svc
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

	current, err := s.currentStatusSnapshot(context.Background())
	if err == nil {
		snapshot.LocalStateLoaded = true
		master := current.Masterchain.Block
		snapshot.LocalMasterchain = &master
		if base, ok := current.Shards[storage.ShardKey{Workchain: 0, Shard: topShard}]; ok {
			block := base.Block
			snapshot.LocalBasechain = &block
		}
		snapshot.LocalMasterchainUtime = blockStateUtime(context.Background(), s.storage, &current.Masterchain)
		if snapshot.LocalBasechain != nil {
			base := current.Shards[storage.ShardKey{Workchain: 0, Shard: topShard}]
			snapshot.LocalBasechainUtime = blockStateUtime(context.Background(), s.storage, &base)
		}
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

func (s *Service) processSyncedBlock(ctx context.Context, synced blocksync.SyncedBlock) error {
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
		verified, err := s.validateSyncedMasterchainBlock(ctx, downloaded)
		if err != nil {
			return err
		}
		if !verified {
			return errSyncedBlockNotVerified
		}
		downloaded, err = prepareDownloadedBlockStateCells(downloaded)
		if err != nil {
			return err
		}
		if err := s.rememberSeenMasterchainBlock(ctx, downloaded.ID); err != nil {
			return err
		}
		s.queueVerifiedMasterchainBlock(downloaded)
		s.wakeCurrentStateSync()
		return nil
	}

	// Shard states are advanced only by the main sync pipeline. Broadcast blocks
	// are kept as download/cache hints so they cannot compete with live tail apply.
	return nil
}

func (s *Service) validateSyncedMasterchainBlock(ctx context.Context, downloaded p2p.DownloadedBlock) (bool, error) {
	if downloaded.Meta == nil || len(downloaded.Meta.PrevRefs) != 1 {
		return false, fmt.Errorf("masterchain block %s has no single previous ref", downloaded.BlockRef())
	}

	prev := downloaded.Meta.PrevRefs[0]
	current, err := s.loadMasterStateForConsensus(ctx, prev)
	if errors.Is(err, storage.ErrNotFound) {
		s.log.Debug().
			Str("block", downloaded.BlockRef()).
			Str("prev", storage.FormatBlockRef(prev)).
			Msg("skipping synced masterchain block until previous state is available for signature validation")
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load previous masterchain state %s: %w", storage.FormatBlockRef(prev), err)
	}

	if err = s.validateMasterchainBlockConsensus(current, tonBlockForConsensus{block: downloaded.ID, proofBOC: downloaded.ProofBOC, broadcastSignatures: downloaded.BroadcastSignatures}); err != nil {
		return false, fmt.Errorf("validate synced masterchain block %s: %w", downloaded.BlockRef(), err)
	}
	return true, nil
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

	progress := float64(done) * 100 / float64(total)
	if progress > 0 && progress < 0.1 {
		return fmt.Sprintf("%.3f%%", progress)
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

func formatCellRate64(cells uint64, elapsed time.Duration) string {
	if cells == 0 || elapsed <= 0 {
		return "0 cells/s"
	}

	rate := float64(cells) / elapsed.Seconds()
	switch {
	case rate >= 1_000_000:
		return fmt.Sprintf("%.2f Mcells/s", rate/1_000_000)
	case rate >= 1_000:
		return fmt.Sprintf("%.2f Kcells/s", rate/1_000)
	default:
		return fmt.Sprintf("%.0f cells/s", rate)
	}
}

func formatByteRate(bytes int64, elapsed time.Duration) string {
	if bytes <= 0 || elapsed <= 0 {
		return "0 B/s"
	}

	rate := float64(bytes) / elapsed.Seconds()
	units := []string{"B/s", "KB/s", "MB/s", "GB/s"}
	unit := 0
	for rate >= 1024 && unit < len(units)-1 {
		rate /= 1024
		unit++
	}

	if unit == 0 {
		return fmt.Sprintf("%.0f %s", rate, units[unit])
	}
	return fmt.Sprintf("%.2f %s", rate, units[unit])
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
