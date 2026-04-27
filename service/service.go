package service

import (
	"context"
	"errors"
	"flexserver/service/blocksync"
	"flexserver/service/p2p"
	"flexserver/service/state"
	"flexserver/service/storage"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
)

const (
	topShard                     = int64(-1 << 63)
	shardStateCatchUpParallelism = 4
	shardStateDownloadBuffer     = 32
	shardStateDownloadWorkers    = 4
	shardStateCatchUpRetryDelay  = time.Second
)

var errMasterchainPrevMismatch = errors.New("masterchain previous state is not current")
var errShardCatchUpNeedsSnapshot = errors.New("shard catch-up requires state snapshot")

type shardStatePersistFunc func(context.Context, *storage.BlockState) error

type shardStateDownload struct {
	prev  ton.BlockIDExt
	block p2p.DownloadedBlock
	err   error
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

	stateMu sync.Mutex

	startOnce sync.Once
	startErr  error
	wg        sync.WaitGroup
}

type StatusSnapshot struct {
	p2p.StatusSnapshot
	LocalMasterchain *ton.BlockIDExt
	LocalBasechain   *ton.BlockIDExt
	LocalStateLoaded bool
	LocalStateError  string
}

func New(logger zerolog.Logger, node *p2p.Node, blockSync *blocksync.Service, store storage.Storage, stateSync *state.Syncer) *Service {
	return &Service{
		log:       logger,
		node:      node,
		blockSync: blockSync,
		storage:   store,
		stateSync: stateSync,
	}
}

func (s *Service) Start(ctx context.Context) error {
	s.startOnce.Do(func() {
		if err := s.node.Start(ctx); err != nil {
			s.startErr = err
			return
		}

		s.runAsync(func() {
			s.blockSync.Run(ctx)
		})
		s.runAsync(func() {
			s.runInitialStateSync(ctx)
		})
		s.runAsync(func() {
			s.runBlockProcessor(ctx)
		})
	})
	return s.startErr
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
	s.node.Wait()
}

func (s *Service) StatusSnapshot() StatusSnapshot {
	snapshot := StatusSnapshot{
		StatusSnapshot: s.node.StatusSnapshot(),
	}

	current, err := s.storage.CurrentState(context.Background())
	if err == nil {
		snapshot.LocalStateLoaded = true
		master := current.Masterchain.Block
		snapshot.LocalMasterchain = &master
		if base, ok := current.Shards[storage.ShardKey{Workchain: 0, Shard: topShard}]; ok {
			block := base.Block
			snapshot.LocalBasechain = &block
		}
		return snapshot
	}
	if !errors.Is(err, storage.ErrNotFound) {
		snapshot.LocalStateError = err.Error()
	}
	return snapshot
}

func (s *Service) runInitialStateSync(ctx context.Context) {
	s.node.SetRebroadcastQuiet(true)
	defer s.node.SetRebroadcastQuiet(false)

	for {
		err := s.ensureCurrentState(ctx)
		if err == nil || errors.Is(err, context.Canceled) {
			return
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
	for {
		select {
		case <-ctx.Done():
			return
		case synced, ok := <-s.blockSync.Blocks():
			if !ok {
				return
			}
			if err := s.processSyncedBlock(ctx, synced); err != nil && !errors.Is(err, context.Canceled) {
				s.logProcessError(err)
			}
		}
	}
}

func (s *Service) processSyncedBlock(ctx context.Context, synced blocksync.SyncedBlock) error {
	downloaded, err := prepareDownloadedBlock(synced.Downloaded)
	if err != nil {
		return err
	}

	stats, err := StatsFromDownloadedBlock(downloaded)
	if err != nil {
		s.log.Warn().
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

	if downloaded.ID.Workchain == -1 && downloaded.ID.Shard == topShard {
		return s.syncMasterchainState(ctx, downloaded)
	}

	return s.deriveBlockState(ctx, downloaded)
}

func (s *Service) ensureCurrentState(ctx context.Context) error {
	s.stateMu.Lock()
	current, err := s.storage.CurrentState(ctx)
	s.stateMu.Unlock()

	if err == nil {
		s.log.Info().
			Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
			Uint32("shard_client_seqno", current.ShardClientSeqno).
			Int("shards", len(current.Shards)).
			Msg("loaded current state from storage")
		return s.catchUpCurrentState(ctx)
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return fmt.Errorf("load current state: %w", err)
	}

	s.log.Info().Msg("current state is missing, starting state snapshot sync")
	snapshot, err := s.stateSync.SyncCurrent(ctx)
	if err != nil {
		return fmt.Errorf("sync current state: %w", err)
	}

	s.log.Info().
		Str("masterchain", storage.FormatBlockRef(snapshot.Masterchain.Block)).
		Uint32("shard_client_seqno", snapshot.ShardClientSeqno).
		Int("shards", len(snapshot.Shards)).
		Msg("synced current state")
	return s.catchUpCurrentState(ctx)
}

func (s *Service) syncMasterchainState(ctx context.Context, downloaded p2p.DownloadedBlock) error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	current, err := s.storage.CurrentState(ctx)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return fmt.Errorf("load current state: %w", err)
	}
	if err == nil {
		switch {
		case current.Masterchain.Block.Equals(&downloaded.ID):
			s.log.Debug().
				Str("block", downloaded.BlockRef()).
				Msg("masterchain state is already current")
			return nil
		case current.Masterchain.Block.SeqNo > downloaded.ID.SeqNo:
			s.log.Debug().
				Str("current", storage.FormatBlockRef(current.Masterchain.Block)).
				Str("block", downloaded.BlockRef()).
				Msg("skip older masterchain block")
			return nil
		}
	}
	if errors.Is(err, storage.ErrNotFound) {
		s.log.Debug().
			Str("block", downloaded.BlockRef()).
			Msg("skip masterchain state apply because current state is missing")
		return nil
	}

	next, err := s.applyMasterchainBlock(ctx, current, downloaded)
	if err != nil {
		if errors.Is(err, errMasterchainPrevMismatch) {
			s.log.Debug().
				Err(err).
				Str("block", downloaded.BlockRef()).
				Msg("skip masterchain state apply")
			return nil
		}
		return err
	}

	s.log.Debug().
		Str("masterchain", storage.FormatBlockRef(next.Masterchain.Block)).
		Msg("updated current state")
	return nil
}

func (s *Service) deriveBlockState(ctx context.Context, downloaded p2p.DownloadedBlock) error {
	if meta, err := s.storage.BlockMeta(ctx, downloaded.ID); err == nil {
		if meta.Has(storage.BlockMetaHasStateSnapshot) {
			return nil
		}
	} else if !errors.Is(err, storage.ErrNotFound) {
		return fmt.Errorf("load block meta %s: %w", downloaded.BlockRef(), err)
	}

	downloaded, err := prepareDownloadedBlock(downloaded)
	if err != nil {
		return err
	}
	meta := downloaded.Meta
	if len(meta.PrevRefs) != 1 {
		s.log.Debug().
			Str("block", downloaded.BlockRef()).
			Int("prev_refs", len(meta.PrevRefs)).
			Msg("skip state apply for non-linear block")
		return nil
	}

	prev := meta.PrevRefs[0]
	prevState, err := s.storage.BlockState(ctx, prev)
	if err == nil {
		next, err := ApplyBlock(prevState, downloaded)
		if err != nil {
			return fmt.Errorf("apply block %s: %w", downloaded.BlockRef(), err)
		}
		if err = s.persistShardBlockState(ctx, next); err != nil {
			return fmt.Errorf("persist derived block state %s: %w", storage.FormatBlockRef(next.Block), err)
		}

		s.log.Debug().
			Str("block", downloaded.BlockRef()).
			Str("prev", storage.FormatBlockRef(prev)).
			Msg("saved derived block state without advancing current shard pointer")
		return nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return fmt.Errorf("load previous block state %s: %w", storage.FormatBlockRef(prev), err)
	}

	s.log.Debug().
		Str("block", downloaded.BlockRef()).
		Str("prev", storage.FormatBlockRef(prev)).
		Msg("skip state apply because previous state is missing")
	return nil
}

func (s *Service) catchUpCurrentState(ctx context.Context) error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	current, err := s.storage.CurrentState(ctx)
	if err != nil {
		return fmt.Errorf("load current state: %w", err)
	}

	for {
		if current.ShardClientSeqno == 0 {
			current.ShardClientSeqno = current.Masterchain.Block.SeqNo
		}

		target, err := s.node.WaitMasterchainBlock(ctx)
		if err != nil {
			return err
		}

		if target.SeqNo > current.ShardClientSeqno && target.SeqNo-current.ShardClientSeqno >= nextBlockCatchUpMaxRemaining {
			current, err = s.catchUpShardClientFromArchives(ctx, current, target)
			if err != nil {
				return err
			}
			if current.Masterchain.Block.SeqNo >= target.SeqNo {
				s.log.Info().
					Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
					Uint32("shard_client_seqno", current.ShardClientSeqno).
					Int("shards", len(current.Shards)).
					Msg("current state caught up")
				return nil
			}
		}

		masterState, err := s.catchUpMasterchainBlocks(ctx, current.Masterchain, target)
		if err != nil {
			return err
		}

		next, changed, err := s.currentStateForMasterState(ctx, current, masterState)
		if err != nil {
			return err
		}

		if changed {
			if err = s.storage.SaveBlockStateAndCurrentState(ctx, masterState, next); err != nil {
				return fmt.Errorf("persist caught-up current state: %w", err)
			}
		}
		current = next

		if current.Masterchain.Block.SeqNo >= target.SeqNo {
			s.log.Info().
				Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
				Uint32("shard_client_seqno", current.ShardClientSeqno).
				Int("shards", len(current.Shards)).
				Msg("current state caught up")
			return nil
		}
	}
}

func (s *Service) catchUpMasterchainBlocks(ctx context.Context, current storage.BlockState, target ton.BlockIDExt) (*storage.BlockState, error) {
	master, err := s.loadBlockStateForApply(ctx, current)
	if err != nil {
		return nil, fmt.Errorf("load current masterchain state %s: %w", storage.FormatBlockRef(current.Block), err)
	}
	if master.Block.SeqNo >= target.SeqNo {
		return master, nil
	}

	started := time.Now()
	startSeqno := master.Block.SeqNo
	totalBlocks := target.SeqNo - startSeqno
	lastProgress := started
	timing := newCatchUpTiming(started)
	s.log.Info().
		Str("from", storage.FormatBlockRef(master.Block)).
		Str("target", storage.FormatBlockRef(target)).
		Uint32("total_blocks", totalBlocks).
		Uint32("remaining", target.SeqNo-master.Block.SeqNo).
		Int("download_buffer", shardStateDownloadBuffer).
		Msg("catching up masterchain blocks")

	downloadCtx, cancelDownloads := context.WithCancel(ctx)
	downloads := s.downloadShardStateBlocks(downloadCtx, master.Block, target)
	defer cancelDownloads()

	for master.Block.SeqNo < target.SeqNo {
		waitStarted := time.Now()
		item, ok := <-downloads
		timing.downloadWait += time.Since(waitStarted)
		if !ok {
			if err := downloadCtx.Err(); err != nil {
				return nil, err
			}
			return nil, fmt.Errorf("masterchain block download stream stopped at %s before target %s", storage.FormatBlockRef(master.Block), storage.FormatBlockRef(target))
		}
		if item.err != nil {
			return nil, item.err
		}
		if !item.prev.Equals(&master.Block) {
			return nil, fmt.Errorf("downloaded masterchain block %s follows %s while current state is %s", item.block.BlockRef(), storage.FormatBlockRef(item.prev), storage.FormatBlockRef(master.Block))
		}

		if item.block.ID.Workchain != -1 || item.block.ID.Shard != topShard {
			return nil, fmt.Errorf("download next masterchain block after %s returned %s", storage.FormatBlockRef(master.Block), item.block.BlockRef())
		}

		applyStarted := time.Now()
		nextMaster, err := ApplyBlock(master, item.block)
		timing.apply += time.Since(applyStarted)
		if err != nil {
			return nil, fmt.Errorf("apply masterchain block %s: %w", item.block.BlockRef(), err)
		}
		master = nextMaster
		timing.blocks++

		persistStarted := time.Now()
		if err := s.storage.SaveBlockState(ctx, master); err != nil {
			return nil, fmt.Errorf("persist masterchain checkpoint %s: %w", storage.FormatBlockRef(master.Block), err)
		}
		timing.persist += time.Since(persistStarted)
		timing.checkpoints++

		if time.Since(lastProgress) >= 5*time.Second || master.Block.SeqNo >= target.SeqNo {
			done := master.Block.SeqNo - startSeqno
			windowElapsed := time.Since(timing.windowStarted)

			s.log.Info().
				Str("current", storage.FormatBlockRef(master.Block)).
				Str("target", storage.FormatBlockRef(target)).
				Uint32("processed_blocks", done).
				Uint32("total_blocks", totalBlocks).
				Uint32("remaining", target.SeqNo-master.Block.SeqNo).
				Str("progress", formatCatchUpProgress(done, totalBlocks)).
				Str("speed", formatBlockRate(done, time.Since(started))).
				Str("eta", formatCatchUpETA(done, totalBlocks, time.Since(started))).
				Msg("masterchain block catch-up progress")
			s.log.Debug().
				Str("current", storage.FormatBlockRef(master.Block)).
				Str("target", storage.FormatBlockRef(target)).
				Uint32("window_blocks", timing.blocks).
				Dur("window_elapsed", windowElapsed).
				Str("window_speed", formatBlockRate(timing.blocks, windowElapsed)).
				Dur("download_wait_total", timing.downloadWait).
				Dur("apply_total", timing.apply).
				Dur("persist_total", timing.persist).
				Dur("download_wait_avg", avgDuration(timing.downloadWait, timing.blocks)).
				Dur("apply_avg", avgDuration(timing.apply, timing.blocks)).
				Dur("persist_avg", avgDuration(timing.persist, timing.blocks)).
				Uint32("checkpoints", timing.checkpoints).
				Msg("masterchain catch-up timing")
			lastProgress = time.Now()
			timing.reset(lastProgress)
		}
	}

	cancelDownloads()

	return master, nil
}

func (s *Service) downloadNextChainBlock(ctx context.Context, prev ton.BlockIDExt) (p2p.DownloadedBlock, error) {
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		downloaded, err := s.node.DownloadNextBlockFull(ctx, prev)
		if err == nil && downloaded != nil {
			return *downloaded, nil
		}
		if err == nil {
			err = fmt.Errorf("empty response")
		}
		lastErr = err

		if attempt == 3 {
			break
		}
		if err = waitRetry(ctx, 500*time.Millisecond); err != nil {
			return p2p.DownloadedBlock{}, err
		}
	}

	return p2p.DownloadedBlock{}, fmt.Errorf("download next block after %s: %w", storage.FormatBlockRef(prev), lastErr)
}

func (s *Service) downloadExactChainBlock(ctx context.Context, block ton.BlockIDExt) (p2p.DownloadedBlock, error) {
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		downloaded, err := s.node.DownloadBlockFull(ctx, block)
		if err == nil && downloaded != nil {
			return *downloaded, nil
		}
		if err == nil {
			err = fmt.Errorf("empty response")
		}
		lastErr = err

		if attempt == 3 {
			break
		}
		if err = waitRetry(ctx, 500*time.Millisecond); err != nil {
			return p2p.DownloadedBlock{}, err
		}
	}

	return p2p.DownloadedBlock{}, fmt.Errorf("download block %s: %w", storage.FormatBlockRef(block), lastErr)
}

func (s *Service) downloadShardStateBlocks(ctx context.Context, start ton.BlockIDExt, target ton.BlockIDExt) <-chan shardStateDownload {
	downloads := make(chan shardStateDownload, shardStateDownloadBuffer)

	go func() {
		defer close(downloads)

		prev := start

		for !prev.Equals(&target) {
			if prev.SeqNo >= target.SeqNo {
				s.sendShardStateDownload(ctx, downloads, shardStateDownload{
					err: fmt.Errorf("%w: downloaded chain block %s does not match target %s", errShardCatchUpNeedsSnapshot, storage.FormatBlockRef(prev), storage.FormatBlockRef(target)),
				})
				return
			}

			indexed, err := s.lookupIndexedChainBlocks(ctx, prev, target, shardStateDownloadBuffer)
			if err != nil {
				s.sendShardStateDownload(ctx, downloads, shardStateDownload{err: err})
				return
			}
			if len(indexed) > 0 {
				if !s.downloadIndexedChainBlocks(ctx, downloads, prev, indexed) {
					return
				}
				prev = indexed[len(indexed)-1]
				continue
			}

			downloaded, err := s.downloadNextChainBlock(ctx, prev)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				if isExpectedRetryError(err) {
					s.log.Debug().
						Err(err).
						Str("current", storage.FormatBlockRef(prev)).
						Str("target", storage.FormatBlockRef(target)).
						Dur("retry_in", shardStateCatchUpRetryDelay).
						Msg("retry chain block download")
					if err = waitRetry(ctx, shardStateCatchUpRetryDelay); err != nil {
						return
					}
					continue
				}

				s.sendShardStateDownload(ctx, downloads, shardStateDownload{err: err})
				return
			}
			if downloaded.ID.Workchain != target.Workchain || downloaded.ID.Shard != target.Shard || downloaded.ID.SeqNo > target.SeqNo || downloaded.ID.SeqNo <= prev.SeqNo {
				s.sendShardStateDownload(ctx, downloads, shardStateDownload{
					err: fmt.Errorf("%w: next chain block after %s returned %s for target %s", errShardCatchUpNeedsSnapshot, storage.FormatBlockRef(prev), downloaded.BlockRef(), storage.FormatBlockRef(target)),
				})
				return
			}

			downloaded, err = prepareDownloadedBlock(downloaded)
			if err != nil {
				s.sendShardStateDownload(ctx, downloads, shardStateDownload{err: err})
				return
			}

			item := shardStateDownload{
				prev:  prev,
				block: downloaded,
			}
			if !s.sendShardStateDownload(ctx, downloads, item) {
				return
			}

			prev = downloaded.ID
		}
	}()

	return downloads
}

func (s *Service) lookupIndexedChainBlocks(ctx context.Context, prev ton.BlockIDExt, target ton.BlockIDExt, limit int) ([]ton.BlockIDExt, error) {
	key := storage.BlockHistoryKey{
		Workchain: prev.Workchain,
		Shard:     prev.Shard,
	}
	blocks := make([]ton.BlockIDExt, 0, limit)
	for seqno := prev.SeqNo + 1; seqno <= target.SeqNo && len(blocks) < limit; seqno++ {
		block, err := s.storage.LookupBlockBySeqNo(ctx, key, seqno)
		if errors.Is(err, storage.ErrNotFound) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("lookup indexed block wc=%d shard=%016x seqno=%d: %w", key.Workchain, uint64(key.Shard), seqno, err)
		}
		if block.Workchain != target.Workchain || block.Shard != target.Shard || block.SeqNo != seqno {
			return nil, fmt.Errorf("%w: indexed block for wc=%d shard=%016x seqno=%d returned %s", errShardCatchUpNeedsSnapshot, key.Workchain, uint64(key.Shard), seqno, storage.FormatBlockRef(block))
		}

		blocks = append(blocks, block)
		if block.Equals(&target) {
			break
		}
	}
	return blocks, nil
}

func (s *Service) downloadIndexedChainBlocks(ctx context.Context, downloads chan<- shardStateDownload, prev ton.BlockIDExt, blocks []ton.BlockIDExt) bool {
	if len(blocks) == 0 {
		return true
	}

	workers := shardStateDownloadWorkers
	if len(blocks) < workers {
		workers = len(blocks)
	}

	s.log.Debug().
		Str("from", storage.FormatBlockRef(prev)).
		Str("to", storage.FormatBlockRef(blocks[len(blocks)-1])).
		Int("blocks", len(blocks)).
		Int("workers", workers).
		Msg("downloading indexed chain blocks in parallel")

	jobs := make(chan shardStateDownloadJob)
	results := make(chan shardStateDownload, len(blocks))
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				downloaded, err := s.downloadExactChainBlockWithRetry(ctx, job.block)
				if err == nil && downloaded.Parsed == nil {
					downloaded, err = prepareDownloadedBlock(downloaded)
				}
				item := shardStateDownload{
					prev:  job.prev,
					block: downloaded,
					err:   err,
				}
				select {
				case results <- item:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		jobPrev := prev
		for _, block := range blocks {
			job := shardStateDownloadJob{
				prev:  jobPrev,
				block: block,
			}
			select {
			case jobs <- job:
			case <-ctx.Done():
				return
			}
			jobPrev = block
		}
	}()

	pending := make(map[uint32]shardStateDownload, len(blocks))
	nextSeqno := blocks[0].SeqNo
	received := 0
	for received < len(blocks) {
		select {
		case <-ctx.Done():
			wg.Wait()
			return false
		case item := <-results:
			received++
			if item.err != nil {
				wg.Wait()
				return s.sendShardStateDownload(ctx, downloads, item)
			}

			pending[item.block.ID.SeqNo] = item
			for {
				item, ok := pending[nextSeqno]
				if !ok {
					break
				}
				delete(pending, nextSeqno)
				if !s.sendShardStateDownload(ctx, downloads, item) {
					wg.Wait()
					return false
				}
				nextSeqno++
			}
		}
	}

	wg.Wait()
	return true
}

func (s *Service) downloadExactChainBlockWithRetry(ctx context.Context, block ton.BlockIDExt) (p2p.DownloadedBlock, error) {
	for {
		downloaded, err := s.downloadExactChainBlock(ctx, block)
		if err == nil {
			return downloaded, nil
		}
		if errors.Is(err, context.Canceled) {
			return p2p.DownloadedBlock{}, err
		}
		if !isExpectedRetryError(err) {
			return p2p.DownloadedBlock{}, err
		}

		s.log.Debug().
			Err(err).
			Str("block", storage.FormatBlockRef(block)).
			Dur("retry_in", shardStateCatchUpRetryDelay).
			Msg("retry indexed block download")
		if err = waitRetry(ctx, shardStateCatchUpRetryDelay); err != nil {
			return p2p.DownloadedBlock{}, err
		}
	}
}

func (s *Service) sendShardStateDownload(ctx context.Context, downloads chan<- shardStateDownload, item shardStateDownload) bool {
	select {
	case downloads <- item:
		return true
	case <-ctx.Done():
		return false
	}
}

func (s *Service) applyMasterchainBlock(ctx context.Context, current *storage.CurrentState, downloaded p2p.DownloadedBlock) (*storage.CurrentState, error) {
	downloaded, err := prepareDownloadedBlock(downloaded)
	if err != nil {
		return nil, err
	}
	meta := downloaded.Meta
	if len(meta.PrevRefs) != 1 {
		return nil, fmt.Errorf("masterchain block %s has %d previous refs", downloaded.BlockRef(), len(meta.PrevRefs))
	}
	prev := meta.PrevRefs[0]
	if !prev.Equals(&current.Masterchain.Block) {
		return nil, fmt.Errorf("%w: block=%s prev=%s current=%s", errMasterchainPrevMismatch, downloaded.BlockRef(), storage.FormatBlockRef(prev), storage.FormatBlockRef(current.Masterchain.Block))
	}

	currentMaster, err := s.loadBlockStateForApply(ctx, current.Masterchain)
	if err != nil {
		return nil, fmt.Errorf("load current masterchain state %s: %w", storage.FormatBlockRef(current.Masterchain.Block), err)
	}

	nextMaster, err := ApplyBlock(currentMaster, downloaded)
	if err != nil {
		return nil, fmt.Errorf("apply masterchain block %s: %w", downloaded.BlockRef(), err)
	}

	nextCurrent, _, err := s.currentStateForMasterState(ctx, current, nextMaster)
	if err != nil {
		return nil, err
	}

	if err = s.storage.SaveBlockStateAndCurrentState(ctx, nextMaster, nextCurrent); err != nil {
		return nil, fmt.Errorf("persist masterchain and current state: %w", err)
	}

	return nextCurrent, nil
}

func (s *Service) currentStateForMasterState(ctx context.Context, current *storage.CurrentState, masterState *storage.BlockState) (*storage.CurrentState, bool, error) {
	targets, err := state.ShardBlocksFromMasterState(masterState)
	if err != nil {
		return nil, false, err
	}

	next := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: masterState.Block.SeqNo,
		Masterchain:      blockStateWithoutCells(masterState),
		Shards:           make(map[storage.ShardKey]storage.BlockState, len(targets)),
	}

	changed := !current.Masterchain.Block.Equals(&masterState.Block) || current.ShardClientSeqno != masterState.Block.SeqNo || len(current.Shards) != len(targets)
	if len(targets) == 0 {
		return next, changed, nil
	}

	type shardResult struct {
		key   storage.ShardKey
		state *storage.BlockState
		err   error
	}

	workers := shardStateCatchUpParallelism
	if len(targets) < workers {
		workers = len(targets)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan shardResult, len(targets))
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup

	for _, target := range targets {
		target := target
		key := storage.ShardKeyFromBlock(target)
		route, err := s.shardStateRoute(ctx, current, target)
		if err != nil {
			return nil, false, err
		}

		if route.done {
			next.Shards[key] = blockStateWithoutCells(&route.previous[0])
			continue
		}
		if route.ahead {
			existing := route.previous[0]
			changed = true
			next.Shards[key] = blockStateWithoutCells(&existing)
			s.log.Debug().
				Str("current", storage.FormatBlockRef(existing.Block)).
				Str("target", storage.FormatBlockRef(target)).
				Msg("keep current shard state ahead of master shard target")
			continue
		}
		changed = true

		wg.Add(1)
		go func() {
			defer wg.Done()

			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				results <- shardResult{key: key, err: ctx.Err()}
				return
			}
			defer func() { <-sem }()

			shardState, err := s.catchUpShardRoute(ctx, route, target)
			if err != nil {
				cancel()
			}
			results <- shardResult{key: key, state: shardState, err: err}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var firstErr error
	for res := range results {
		if res.err != nil {
			if errors.Is(res.err, context.Canceled) {
				if firstErr == nil && ctx.Err() != nil && !errors.Is(ctx.Err(), context.Canceled) {
					firstErr = ctx.Err()
				}
				continue
			}
			if firstErr == nil || errors.Is(firstErr, context.Canceled) {
				firstErr = res.err
			}
			continue
		}
		next.Shards[res.key] = blockStateWithoutCells(res.state)
	}
	if firstErr != nil {
		return nil, false, firstErr
	}
	if len(next.Shards) != len(targets) {
		if ctx.Err() != nil {
			return nil, false, ctx.Err()
		}
		return nil, false, fmt.Errorf("loaded %d shard states, expected %d", len(next.Shards), len(targets))
	}

	return next, changed, nil
}

type shardStateRoute struct {
	previous      []storage.BlockState
	downloaded    p2p.DownloadedBlock
	hasDownloaded bool
	done          bool
	ahead         bool
	kind          string
}

func (s *Service) shardStateRoute(ctx context.Context, current *storage.CurrentState, target ton.BlockIDExt) (shardStateRoute, error) {
	key := storage.ShardKeyFromBlock(target)
	if existing, ok := current.Shards[key]; ok {
		route := shardStateRoute{
			previous: []storage.BlockState{existing},
			kind:     "linear",
		}
		if existing.Block.Equals(&target) {
			route.done = true
		} else if existing.Block.SeqNo > target.SeqNo {
			route.ahead = true
		}
		return route, nil
	}

	downloaded, err := s.loadOrDownloadBlockForApply(ctx, target)
	if err != nil {
		return shardStateRoute{}, fmt.Errorf("load shard route target block %s: %w", storage.FormatBlockRef(target), err)
	}

	meta := downloaded.Meta
	switch len(meta.PrevRefs) {
	case 1:
		prev, ok := currentShardByBlock(current, meta.PrevRefs[0])
		if !ok {
			return shardStateRoute{}, fmt.Errorf("%w: current parent shard %s for target %s is missing", errShardCatchUpNeedsSnapshot, storage.FormatBlockRef(meta.PrevRefs[0]), storage.FormatBlockRef(target))
		}
		return shardStateRoute{
			previous:      []storage.BlockState{prev},
			downloaded:    downloaded,
			hasDownloaded: true,
			kind:          "split",
		}, nil
	case 2:
		left, ok := currentShardByBlock(current, meta.PrevRefs[0])
		if !ok {
			return shardStateRoute{}, fmt.Errorf("%w: current left merge shard %s for target %s is missing", errShardCatchUpNeedsSnapshot, storage.FormatBlockRef(meta.PrevRefs[0]), storage.FormatBlockRef(target))
		}
		right, ok := currentShardByBlock(current, meta.PrevRefs[1])
		if !ok {
			return shardStateRoute{}, fmt.Errorf("%w: current right merge shard %s for target %s is missing", errShardCatchUpNeedsSnapshot, storage.FormatBlockRef(meta.PrevRefs[1]), storage.FormatBlockRef(target))
		}
		return shardStateRoute{
			previous:      []storage.BlockState{left, right},
			downloaded:    downloaded,
			hasDownloaded: true,
			kind:          "merge",
		}, nil
	default:
		return shardStateRoute{}, fmt.Errorf("%w: shard block %s has %d previous refs", errShardCatchUpNeedsSnapshot, storage.FormatBlockRef(target), len(meta.PrevRefs))
	}
}

func currentShardByBlock(current *storage.CurrentState, block ton.BlockIDExt) (storage.BlockState, bool) {
	key := storage.ShardKeyFromBlock(block)
	state, ok := current.Shards[key]
	if !ok || !state.Block.Equals(&block) {
		return storage.BlockState{}, false
	}
	return state, true
}

func (s *Service) loadOrDownloadBlockForApply(ctx context.Context, block ton.BlockIDExt) (p2p.DownloadedBlock, error) {
	downloaded, err := loadStoredBlockForApply(ctx, s.storage, block, true)
	if err == nil {
		return downloaded, nil
	}
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return p2p.DownloadedBlock{}, err
	}

	downloaded, err = s.downloadExactChainBlockWithRetry(ctx, block)
	if err != nil {
		return p2p.DownloadedBlock{}, err
	}
	downloaded, err = prepareDownloadedBlock(downloaded)
	if err != nil {
		return p2p.DownloadedBlock{}, err
	}
	if err = s.storage.SaveBlockMeta(downloaded.Meta); err != nil {
		return p2p.DownloadedBlock{}, fmt.Errorf("persist downloaded block meta %s: %w", storage.FormatBlockRef(block), err)
	}
	return downloaded, nil
}

func (s *Service) catchUpShardRoute(ctx context.Context, route shardStateRoute, target ton.BlockIDExt) (*storage.BlockState, error) {
	if !route.hasDownloaded {
		current := route.previous[0]
		return s.catchUpShardState(ctx, &current, target)
	}

	downloaded, err := prepareDownloadedBlock(route.downloaded)
	if err != nil {
		return nil, err
	}
	if !downloaded.ID.Equals(&target) {
		return nil, fmt.Errorf("shard route downloaded %s instead of target %s", downloaded.BlockRef(), storage.FormatBlockRef(target))
	}
	if len(downloaded.Meta.PrevRefs) != len(route.previous) {
		return nil, fmt.Errorf("%w: shard %s route %s has %d previous refs, expected %d", errShardCatchUpNeedsSnapshot, downloaded.BlockRef(), route.kind, len(downloaded.Meta.PrevRefs), len(route.previous))
	}

	previous := make([]*storage.BlockState, len(route.previous))
	for i := range route.previous {
		if !downloaded.Meta.PrevRefs[i].Equals(&route.previous[i].Block) {
			return nil, fmt.Errorf("%w: shard %s route %s previous[%d] is %s, current is %s", errShardCatchUpNeedsSnapshot, downloaded.BlockRef(), route.kind, i, storage.FormatBlockRef(downloaded.Meta.PrevRefs[i]), storage.FormatBlockRef(route.previous[i].Block))
		}
		state, err := s.loadBlockStateForApply(ctx, route.previous[i])
		if err != nil {
			return nil, fmt.Errorf("load %s previous shard state %s: %w", route.kind, storage.FormatBlockRef(route.previous[i].Block), err)
		}
		previous[i] = state
	}

	event := s.log.Debug().
		Str("target", downloaded.BlockRef()).
		Str("route", route.kind)
	if len(route.previous) > 0 {
		event.Str("prev", storage.FormatBlockRef(route.previous[0].Block))
	}
	if len(route.previous) > 1 {
		event.Str("prev_right", storage.FormatBlockRef(route.previous[1].Block))
	}
	event.Msg("applying shard topology transition")

	next, err := ApplyBlockWithPreviousStates(previous, downloaded)
	if err != nil {
		return nil, fmt.Errorf("apply %s shard block %s: %w", route.kind, downloaded.BlockRef(), err)
	}
	if err = s.persistShardBlockState(ctx, next); err != nil {
		return nil, fmt.Errorf("persist %s shard block state %s: %w", route.kind, storage.FormatBlockRef(next.Block), err)
	}
	return next, nil
}

func (s *Service) catchUpShardState(ctx context.Context, current *storage.BlockState, target ton.BlockIDExt) (*storage.BlockState, error) {
	return s.catchUpShardStateWithPersist(ctx, current, target, s.persistShardBlockState)
}

func (s *Service) catchUpShardStateWithPersist(ctx context.Context, current *storage.BlockState, target ton.BlockIDExt, persist shardStatePersistFunc) (*storage.BlockState, error) {
	if current != nil && current.Block.Equals(&target) {
		return storage.CloneBlockState(current), nil
	}

	targetState, err := s.storage.BlockState(ctx, target)
	if err == nil {
		return targetState, nil
	}
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return nil, fmt.Errorf("load target shard state %s: %w", storage.FormatBlockRef(target), err)
	}
	if current == nil {
		return nil, fmt.Errorf("%w: current shard for target %s is missing", errShardCatchUpNeedsSnapshot, storage.FormatBlockRef(target))
	}
	if current.Block.SeqNo >= target.SeqNo {
		return nil, fmt.Errorf("%w: current shard %s cannot advance to target %s", errShardCatchUpNeedsSnapshot, storage.FormatBlockRef(current.Block), storage.FormatBlockRef(target))
	}

	shard, err := s.loadBlockStateForApply(ctx, *current)
	if err != nil {
		return nil, fmt.Errorf("load current shard state %s: %w", storage.FormatBlockRef(current.Block), err)
	}

	started := time.Now()
	startSeqno := shard.Block.SeqNo
	totalBlocks := target.SeqNo - startSeqno
	if totalBlocks >= nextBlockCatchUpMaxRemaining {
		return nil, fmt.Errorf("%w: shard gap from %s to %s is too large for next-block catch-up", errShardCatchUpNeedsSnapshot, storage.FormatBlockRef(shard.Block), storage.FormatBlockRef(target))
	}

	lastProgress := started
	timing := newCatchUpTiming(started)
	s.log.Info().
		Str("from", storage.FormatBlockRef(shard.Block)).
		Str("target", storage.FormatBlockRef(target)).
		Uint32("total_blocks", totalBlocks).
		Uint32("remaining", target.SeqNo-shard.Block.SeqNo).
		Int("download_buffer", shardStateDownloadBuffer).
		Msg("catching up shard state")

	downloadCtx, cancelDownloads := context.WithCancel(ctx)
	downloads := s.downloadShardStateBlocks(downloadCtx, shard.Block, target)
	defer cancelDownloads()

	for !shard.Block.Equals(&target) {
		if shard.Block.SeqNo >= target.SeqNo {
			return nil, fmt.Errorf("%w: downloaded shard %s does not match target %s", errShardCatchUpNeedsSnapshot, storage.FormatBlockRef(shard.Block), storage.FormatBlockRef(target))
		}

		waitStarted := time.Now()
		item, ok := <-downloads
		timing.downloadWait += time.Since(waitStarted)
		if !ok {
			if err := downloadCtx.Err(); err != nil {
				return nil, err
			}
			return nil, fmt.Errorf("shard block download stream stopped at %s before target %s", storage.FormatBlockRef(shard.Block), storage.FormatBlockRef(target))
		}
		if item.err != nil {
			return nil, item.err
		}
		if !item.prev.Equals(&shard.Block) {
			return nil, fmt.Errorf("%w: downloaded shard block %s follows %s while current state is %s", errShardCatchUpNeedsSnapshot, item.block.BlockRef(), storage.FormatBlockRef(item.prev), storage.FormatBlockRef(shard.Block))
		}

		downloaded := item.block
		meta := downloaded.Meta
		if len(meta.PrevRefs) != 1 || !meta.PrevRefs[0].Equals(&shard.Block) {
			return nil, fmt.Errorf("%w: shard block %s has non-linear previous refs", errShardCatchUpNeedsSnapshot, downloaded.BlockRef())
		}

		applyStarted := time.Now()
		nextShard, err := ApplyBlock(shard, downloaded)
		timing.apply += time.Since(applyStarted)
		if err != nil {
			return nil, fmt.Errorf("apply shard block %s: %w", downloaded.BlockRef(), err)
		}
		shard = nextShard
		timing.blocks++

		persistStarted := time.Now()
		if err := persist(ctx, shard); err != nil {
			return nil, fmt.Errorf("persist shard checkpoint %s: %w", storage.FormatBlockRef(shard.Block), err)
		}
		timing.persist += time.Since(persistStarted)
		timing.checkpoints++

		if time.Since(lastProgress) >= 5*time.Second || shard.Block.Equals(&target) {
			done := shard.Block.SeqNo - startSeqno
			windowElapsed := time.Since(timing.windowStarted)

			s.log.Info().
				Str("current", storage.FormatBlockRef(shard.Block)).
				Str("target", storage.FormatBlockRef(target)).
				Uint32("processed_blocks", done).
				Uint32("total_blocks", totalBlocks).
				Uint32("remaining", target.SeqNo-shard.Block.SeqNo).
				Str("progress", formatCatchUpProgress(done, totalBlocks)).
				Str("speed", formatBlockRate(done, time.Since(started))).
				Str("eta", formatCatchUpETA(done, totalBlocks, time.Since(started))).
				Msg("shard state catch-up progress")
			s.log.Debug().
				Str("current", storage.FormatBlockRef(shard.Block)).
				Str("target", storage.FormatBlockRef(target)).
				Uint32("window_blocks", timing.blocks).
				Dur("window_elapsed", windowElapsed).
				Str("window_speed", formatBlockRate(timing.blocks, windowElapsed)).
				Dur("download_wait_total", timing.downloadWait).
				Dur("apply_total", timing.apply).
				Dur("persist_total", timing.persist).
				Dur("download_wait_avg", avgDuration(timing.downloadWait, timing.blocks)).
				Dur("apply_avg", avgDuration(timing.apply, timing.blocks)).
				Dur("persist_avg", avgDuration(timing.persist, timing.blocks)).
				Uint32("checkpoints", timing.checkpoints).
				Msg("shard state catch-up timing")
			lastProgress = time.Now()
			timing.reset(lastProgress)
		}
	}

	cancelDownloads()

	return shard, nil
}

func (s *Service) loadBlockStateForApply(ctx context.Context, state storage.BlockState) (*storage.BlockState, error) {
	if state.Cell != nil && state.Parsed != nil {
		return storage.CloneBlockState(&state), nil
	}
	return s.storage.BlockState(ctx, state.Block)
}

func blockStateWithoutCells(state *storage.BlockState) storage.BlockState {
	return storage.BlockState{
		Block:         state.Block,
		StateRootHash: append([]byte(nil), state.StateRootHash...),
		StateCellHash: append([]byte(nil), state.StateCellHash...),
		StateFileHash: append([]byte(nil), state.StateFileHash...),
		CellsCount:    state.CellsCount,
		DownloadedAt:  state.DownloadedAt,
	}
}

func (s *Service) persistShardBlockState(ctx context.Context, next *storage.BlockState) error {
	return s.storage.SaveBlockState(ctx, next)
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
