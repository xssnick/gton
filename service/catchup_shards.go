package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

func (s *SyncCoordinator) downloadShardStateBlocks(ctx context.Context, start ton.BlockIDExt, target ton.BlockIDExt) <-chan shardStateDownload {
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

			cached, err := s.takeCachedMasterchainBlockForApply(ctx, prev, target)
			if err == nil {
				if !s.sendShardStateDownload(ctx, downloads, shardStateDownload{prev: prev, block: cached.block, source: cached.source, prepareElapsed: cached.prepareElapsed}) {
					return
				}
				prev = cached.block.ID
				continue
			}
			if !errors.Is(err, storage.ErrNotFound) {
				s.sendShardStateDownload(ctx, downloads, shardStateDownload{err: err})
				return
			}

			indexed, err := s.lookupIndexedChainBlocks(ctx, prev, target, shardStateDownloadBuffer)
			if err != nil {
				s.sendShardStateDownload(ctx, downloads, shardStateDownload{err: err, source: SyncBlockSourceIndexed})
				return
			}
			if len(indexed) > 0 {
				if !s.downloadKnownChainBlocks(ctx, downloads, prev, indexed, SyncBlockSourceIndexed) {
					return
				}
				prev = indexed[len(indexed)-1]
				continue
			}

			described, err := s.lookupNextChainBlockDescriptions(ctx, prev, target, nextBlockDescriptionLookahead)
			if err != nil {
				s.sendShardStateDownload(ctx, downloads, shardStateDownload{err: err, source: SyncBlockSourceNextDescription})
				return
			}
			if len(described) > 0 {
				if !s.downloadKnownChainBlocks(ctx, downloads, prev, described, SyncBlockSourceNextDescription) {
					return
				}
				prev = described[len(described)-1]
				continue
			}

			// Taken before the attempt so a block published while it runs is
			// not waited out by the retry below.
			stateWake := s.currentStateWakeChan()
			downloadStarted := time.Now()
			downloaded, err := s.downloadNextChainBlock(ctx, prev)
			downloadElapsed := time.Since(downloadStarted)
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
					if err = s.waitShardStateCatchUpRetry(ctx, stateWake, shardStateCatchUpRetryDelay); err != nil {
						return
					}
					continue
				}

				s.sendShardStateDownload(ctx, downloads, shardStateDownload{
					err:             err,
					source:          SyncBlockSourceNextBlock,
					downloadElapsed: downloadElapsed,
				})
				return
			}
			if downloaded.ID.Workchain != target.Workchain || downloaded.ID.Shard != target.Shard || downloaded.ID.SeqNo > target.SeqNo || downloaded.ID.SeqNo <= prev.SeqNo {
				s.sendShardStateDownload(ctx, downloads, shardStateDownload{
					err:             fmt.Errorf("%w: next chain block after %s returned %s for target %s", errShardCatchUpNeedsSnapshot, storage.FormatBlockRef(prev), downloaded.BlockRef(), storage.FormatBlockRef(target)),
					source:          syncBlockSourceForKind(SyncBlockSourceNextBlock, downloaded.Kind),
					downloadElapsed: downloadElapsed,
				})
				return
			}

			prepareStarted := time.Now()
			verified, err := s.verifyDownloadedBlock(downloaded)
			if err != nil {
				s.sendShardStateDownload(ctx, downloads, shardStateDownload{
					err:             err,
					source:          syncBlockSourceForKind(SyncBlockSourceNextBlock, downloaded.Kind),
					downloadElapsed: downloadElapsed,
					prepareElapsed:  time.Since(prepareStarted),
				})
				return
			}
			prepared, err := prepareVerifiedMasterchainBlockForNextSync(prev, verified)
			if err != nil {
				s.sendShardStateDownload(ctx, downloads, shardStateDownload{
					err:             err,
					source:          syncBlockSourceForKind(SyncBlockSourceNextBlock, verified.Kind),
					downloadElapsed: downloadElapsed,
					prepareElapsed:  time.Since(prepareStarted),
				})
				return
			}
			// Off the serial apply goroutine: pay for the signature batch here
			// so apply keeps only the hash checks.
			s.preverifyMasterchainConsensusSignatures(prepared.consensus)
			prepared.PrepareElapsed = time.Since(prepareStarted)

			item := shardStateDownload{
				prev:            prev,
				block:           prepared,
				source:          syncBlockSourceForKind(SyncBlockSourceNextBlock, verified.Kind),
				downloadElapsed: downloadElapsed,
				prepareElapsed:  prepared.PrepareElapsed,
			}
			if !s.sendShardStateDownload(ctx, downloads, item) {
				return
			}

			prev = prepared.ID
		}
	}()

	return downloads
}

func (s *SyncCoordinator) lookupIndexedChainBlocks(ctx context.Context, prev ton.BlockIDExt, target ton.BlockIDExt, limit int) ([]ton.BlockIDExt, error) {
	blocks := make([]ton.BlockIDExt, 0, limit)
	for seqno := prev.SeqNo + 1; seqno <= target.SeqNo && len(blocks) < limit; seqno++ {
		ref := storage.BlockSeqRef{Workchain: prev.Workchain, Shard: prev.Shard, SeqNo: seqno}
		block, err := s.storage.LookupBlockBySeqNo(ctx, ref)
		if errors.Is(err, storage.ErrNotFound) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("lookup indexed block wc=%d shard=%016x seqno=%d: %w", ref.Workchain, uint64(ref.Shard), seqno, err)
		}
		if block.Workchain != target.Workchain || block.Shard != target.Shard || block.SeqNo != seqno {
			return nil, fmt.Errorf("%w: indexed block for wc=%d shard=%016x seqno=%d returned %s", errShardCatchUpNeedsSnapshot, ref.Workchain, uint64(ref.Shard), seqno, storage.FormatBlockRef(block))
		}

		blocks = append(blocks, block)
		if block.Equals(&target) {
			break
		}
	}
	return blocks, nil
}

func (s *SyncCoordinator) lookupNextChainBlockDescriptions(ctx context.Context, prev ton.BlockIDExt, target ton.BlockIDExt, limit int) ([]ton.BlockIDExt, error) {
	if limit <= 0 {
		return nil, nil
	}

	blocks := make([]ton.BlockIDExt, 0, limit)
	for len(blocks) < limit && !prev.Equals(&target) {
		block, err := s.node.NextBlockDescription(ctx, prev)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil, err
			}
			if len(blocks) > 0 && isExpectedRetryError(err) {
				s.log.Debug().
					Err(err).
					Str("current", storage.FormatBlockRef(prev)).
					Str("target", storage.FormatBlockRef(target)).
					Int("blocks", len(blocks)).
					Msg("stopped next block description lookahead")
				return blocks, nil
			}
			if isExpectedRetryError(err) {
				return nil, nil
			}
			return nil, fmt.Errorf("lookup next block description after %s: %w", storage.FormatBlockRef(prev), err)
		}
		if block.Workchain != target.Workchain || block.Shard != target.Shard || block.SeqNo > target.SeqNo || block.SeqNo <= prev.SeqNo {
			return nil, fmt.Errorf("%w: next block description after %s returned %s for target %s", errShardCatchUpNeedsSnapshot, storage.FormatBlockRef(prev), storage.FormatBlockRef(block), storage.FormatBlockRef(target))
		}

		blocks = append(blocks, block)
		prev = block
	}
	return blocks, nil
}

func (s *SyncCoordinator) downloadKnownChainBlocks(ctx context.Context, downloads chan<- shardStateDownload, prev ton.BlockIDExt, blocks []ton.BlockIDExt, source SyncBlockSource) bool {
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
		Str("source", string(source)).
		Msg("downloading chain blocks in parallel")

	jobs := make(chan shardStateDownloadJob)
	results := make(chan shardStateDownload, len(blocks))
	downloadCtx, cancelDownload := context.WithCancel(ctx)
	defer cancelDownload()

	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				downloadStarted := time.Now()
				downloaded, err := s.downloadExactChainBlockWithRetry(downloadCtx, job.block)
				downloadElapsed := time.Since(downloadStarted)
				var verified VerifiedBlock
				var prepared PreparedBlock
				var prepareElapsed time.Duration
				if err == nil {
					prepareStarted := time.Now()
					verified, err = s.verifyDownloadedBlock(downloaded)
					if err == nil {
						prepared, err = prepareVerifiedMasterchainBlockForNextSync(job.prev, verified)
					}
					if err == nil {
						// Download workers run in parallel with apply: pay for
						// the signature batch here so apply keeps only the
						// hash checks.
						s.preverifyMasterchainConsensusSignatures(prepared.consensus)
					}
					prepareElapsed = time.Since(prepareStarted)
				}
				if err == nil {
					prepared.PrepareElapsed = prepareElapsed
				}
				item := shardStateDownload{
					prev:            job.prev,
					block:           prepared,
					err:             err,
					downloadElapsed: downloadElapsed,
					prepareElapsed:  prepareElapsed,
				}
				if err == nil {
					item.source = syncBlockSourceForKind(source, verified.Kind)
				} else {
					item.source = source
				}
				select {
				case results <- item:
				case <-downloadCtx.Done():
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
			case <-downloadCtx.Done():
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
			cancelDownload()
			wg.Wait()
			return false
		case item := <-results:
			received++
			if item.err != nil {
				cancelDownload()
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
					cancelDownload()
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

func (s *SyncCoordinator) downloadExactChainBlockWithRetry(ctx context.Context, block ton.BlockIDExt) (p2p.DownloadedBlock, error) {
	state := exactBlockDownloadProbeState{
		started: time.Now(),
	}
	for {
		// Taken before the attempt so a block published while it runs is not
		// waited out by the retry below.
		stateWake := s.currentStateWakeChan()
		attemptStarted := time.Now()
		downloaded, source, err := s.downloadExactChainBlockProbe(ctx, block, state)
		attemptElapsed := time.Since(attemptStarted)
		if err == nil {
			s.observeSyncBlock(SyncBlockObservation{
				Pipeline:         "exact_block_download",
				Chain:            syncChainLabel(block),
				Shard:            syncShardLabel(block),
				Source:           source,
				Origin:           syncBlockOriginForSource(source),
				Result:           "success",
				CatchUp:          true,
				DownloadDuration: attemptElapsed,
			})
			return downloaded, nil
		}
		s.observeSyncBlock(SyncBlockObservation{
			Pipeline:         "exact_block_download",
			Chain:            syncChainLabel(block),
			Shard:            syncShardLabel(block),
			Source:           SyncBlockSourcePeerProbe,
			Origin:           SyncBlockOriginDownload,
			Result:           syncBlockResultForError(err),
			CatchUp:          true,
			DownloadDuration: attemptElapsed,
		})
		if errors.Is(err, context.Canceled) {
			return p2p.DownloadedBlock{}, err
		}
		if !isExpectedRetryError(err) {
			return p2p.DownloadedBlock{}, err
		}
		state.consecutiveMisses++
		decision := s.exactBlockDownloadProbeDecision(state)

		s.log.Debug().
			Err(err).
			Str("block", storage.FormatBlockRef(block)).
			Int("consecutive_misses", state.consecutiveMisses).
			Int("next_probe_peers", decision.peerLimit).
			Int("next_staged_probe_peers", decision.stagedPeerLimit).
			Dur("waited", time.Since(state.started)).
			Dur("retry_in", shardStateCatchUpRetryDelay).
			Msg("retry indexed block download")
		if err = s.waitShardStateCatchUpRetry(ctx, stateWake, shardStateCatchUpRetryDelay); err != nil {
			return p2p.DownloadedBlock{}, err
		}
	}
}

// waitShardStateCatchUpRetry parks before the next download attempt. The caller
// passes the wake it took before the attempt that missed, so state published
// while that attempt ran is not waited out here.
func (s *SyncCoordinator) waitShardStateCatchUpRetry(ctx context.Context, wake <-chan struct{}, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-wake:
		return nil
	case <-timer.C:
		return nil
	}
}

func (s *SyncCoordinator) sendShardStateDownload(ctx context.Context, downloads chan<- shardStateDownload, item shardStateDownload) bool {
	select {
	case downloads <- item:
		return true
	case <-ctx.Done():
		return false
	}
}
