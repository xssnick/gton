package service

import (
	"context"
	"fmt"
	"time"

	"flexserver/service/p2p"
	"flexserver/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

func (s *Service) catchUpShardClientWithNextBlocks(ctx context.Context, current *storage.CurrentState, target ton.BlockIDExt) (*storage.CurrentState, error) {
	return s.runNextSyncToTarget(ctx, current, target)
}

func (s *Service) catchUpShardClientBootstrap(ctx context.Context, current *storage.CurrentState) (*storage.CurrentState, bool, error) {
	return s.runNextSyncBootstrap(ctx, current)
}

func (s *Service) publishLiveCurrentState(current *storage.CurrentState) {
	if current == nil {
		return
	}

	s.currentStatusMu.Lock()
	s.currentStatus = storage.CloneCurrentState(current)
	s.currentStatusMu.Unlock()
}

func (s *Service) publishCommittedCurrentState(current *storage.CurrentState) {
	s.publishLiveCurrentState(current)
	if s.liveState != nil {
		s.liveState.SetLiveCurrentState(current)
	}
}

func (s *Service) stageCurrentBlockStates(ctx context.Context, current *storage.CurrentState) error {
	for _, state := range currentBlockStates(current) {
		if err := s.storage.StageBlockState(ctx, state); err != nil {
			return err
		}
	}
	return nil
}

type masterchainApplyTiming struct {
	total       time.Duration
	prepare     time.Duration
	consensus   time.Duration
	stateUpdate time.Duration
}

func (s *Service) applyMasterchainTransition(current *storage.BlockState, downloaded p2p.DownloadedBlock) (*storage.BlockState, masterchainApplyTiming, error) {
	return s.applyMasterchainTransitionWithConsensusProof(current, downloaded, nil)
}

func (s *Service) applyMasterchainTransitionWithConsensusProof(current *storage.BlockState, downloaded p2p.DownloadedBlock, proof *masterchainConsensusProof) (*storage.BlockState, masterchainApplyTiming, error) {
	started := time.Now()
	var timing masterchainApplyTiming
	finish := func() masterchainApplyTiming {
		timing.total = time.Since(started)
		return timing
	}

	if current == nil {
		return nil, finish(), fmt.Errorf("current masterchain state is nil")
	}

	stageStarted := time.Now()
	downloaded, err := prepareDownloadedBlock(downloaded)
	timing.prepare += time.Since(stageStarted)
	if err != nil {
		return nil, finish(), err
	}
	if downloaded.ID.Workchain != -1 || downloaded.ID.Shard != topShard {
		return nil, finish(), fmt.Errorf("download next masterchain block after %s returned %s", storage.FormatBlockRef(current.Block), downloaded.BlockRef())
	}
	if len(downloaded.Meta.PrevRefs) != 1 {
		return nil, finish(), fmt.Errorf("masterchain block %s has %d previous refs", downloaded.BlockRef(), len(downloaded.Meta.PrevRefs))
	}
	prev := downloaded.Meta.PrevRefs[0]
	if !prev.Equals(&current.Block) {
		return nil, finish(), fmt.Errorf("%w: block=%s prev=%s current=%s", errMasterchainPrevMismatch, downloaded.BlockRef(), storage.FormatBlockRef(prev), storage.FormatBlockRef(current.Block))
	}

	stageStarted = time.Now()
	if err = s.validateMasterchainBlockConsensusWithProof(current, tonBlockForConsensus{block: downloaded.ID, proofBOC: downloaded.ProofBOC, broadcastSignatures: downloaded.BroadcastSignatures}, proof); err != nil {
		timing.consensus += time.Since(stageStarted)
		return nil, finish(), fmt.Errorf("validate masterchain consensus for %s: %w", downloaded.BlockRef(), err)
	}
	timing.consensus += time.Since(stageStarted)

	stageStarted = time.Now()
	next, err := ApplyBlock(current, downloaded)
	timing.stateUpdate += time.Since(stageStarted)
	if err != nil {
		return nil, finish(), fmt.Errorf("apply masterchain block %s: %w", downloaded.BlockRef(), err)
	}

	return next, finish(), nil
}

func (s *Service) persistNextBlockCurrentState(current *storage.CurrentState, timing *catchUpTiming) (*storage.CurrentState, error) {
	if current == nil {
		return nil, fmt.Errorf("current state is nil")
	}
	if err := s.takeCurrentStatePersistError(); err != nil {
		return nil, err
	}

	persisted := currentStateWithoutCells(current)
	live := storage.CloneCurrentState(current)
	states := currentBlockStates(current)
	master := current.Masterchain.Block
	shardClientSeqno := current.ShardClientSeqno
	shards := len(current.Shards)
	queuedAt := time.Now()

	lockStarted := time.Now()
	s.currentStatePersistMu.Lock()
	timing.persist += time.Since(lockStarted)
	if err := s.takeCurrentStatePersistError(); err != nil {
		s.currentStatePersistMu.Unlock()
		return nil, err
	}
	timing.checkpoints++

	s.log.Debug().
		Str("masterchain", storage.FormatBlockRef(master)).
		Uint32("shard_client_seqno", shardClientSeqno).
		Int("shards", shards).
		Msg("next-block shard-client checkpoint scheduled")

	persistCtx := s.currentStatePersistContext()
	s.runAsync(func() {
		defer s.currentStatePersistMu.Unlock()

		started := time.Now()
		err := s.storage.SaveBlockStatesAndCurrentState(persistCtx, states, persisted)
		elapsed := time.Since(started)
		if err != nil {
			wrapped := fmt.Errorf("persist next-block current state %s: %w", storage.FormatBlockRef(master), err)
			s.setCurrentStatePersistError(wrapped)
			s.log.Error().
				Err(wrapped).
				Str("masterchain", storage.FormatBlockRef(master)).
				Uint32("shard_client_seqno", shardClientSeqno).
				Dur("queued_for", time.Since(queuedAt)).
				Dur("elapsed", elapsed).
				Msg("next-block shard-client checkpoint failed")
			return
		}

		s.log.Debug().
			Str("masterchain", storage.FormatBlockRef(master)).
			Uint32("shard_client_seqno", shardClientSeqno).
			Int("states", len(states)).
			Int("shards", shards).
			Dur("queued_for", time.Since(queuedAt)).
			Dur("elapsed", elapsed).
			Msg("next-block shard-client checkpoint persisted")
		s.publishCommittedCurrentState(live)
	})

	return live, nil
}

func (s *Service) persistNextBlockCurrentStateSync(current *storage.CurrentState, timing *catchUpTiming, reason string) (*storage.CurrentState, error) {
	if current == nil {
		return nil, fmt.Errorf("current state is nil")
	}
	if err := s.takeCurrentStatePersistError(); err != nil {
		return nil, err
	}

	persisted := currentStateWithoutCells(current)
	live := storage.CloneCurrentState(current)
	states := currentBlockStates(current)
	master := current.Masterchain.Block
	shardClientSeqno := current.ShardClientSeqno
	shards := len(current.Shards)

	lockStarted := time.Now()
	s.currentStatePersistMu.Lock()
	timing.persist += time.Since(lockStarted)
	defer s.currentStatePersistMu.Unlock()
	if err := s.takeCurrentStatePersistError(); err != nil {
		return nil, err
	}
	timing.checkpoints++

	started := time.Now()
	persistCtx := s.currentStatePersistContext()
	if err := s.storage.SaveBlockStatesAndCurrentState(persistCtx, states, persisted); err != nil {
		return nil, fmt.Errorf("persist next-block current state %s: %w", storage.FormatBlockRef(master), err)
	}

	s.log.Info().
		Str("masterchain", storage.FormatBlockRef(master)).
		Uint32("shard_client_seqno", shardClientSeqno).
		Int("states", len(states)).
		Int("shards", shards).
		Str("reason", reason).
		Dur("elapsed", time.Since(started)).
		Msg("next-block shard-client checkpoint persisted")
	s.publishCommittedCurrentState(live)
	return live, nil
}

func (s *Service) currentStatePersistContext() context.Context {
	if s.shutdownContext != nil {
		return s.shutdownContext
	}
	return context.Background()
}

func (s *Service) setCurrentStatePersistError(err error) {
	if err == nil {
		return
	}
	s.currentStatePersistErrMu.Lock()
	if s.currentStatePersistErr == nil {
		s.currentStatePersistErr = err
	}
	s.currentStatePersistErrMu.Unlock()
}

func (s *Service) takeCurrentStatePersistError() error {
	s.currentStatePersistErrMu.Lock()
	defer s.currentStatePersistErrMu.Unlock()

	err := s.currentStatePersistErr
	s.currentStatePersistErr = nil
	return err
}

func (s *Service) waitCurrentStatePersist(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.currentStatePersistMu.Lock()
		s.currentStatePersistMu.Unlock()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return s.takeCurrentStatePersistError()
	}
}

func (s *Service) downloadNextChainBlockProbe(ctx context.Context, prev ton.BlockIDExt) (p2p.DownloadedBlock, string, error) {
	if s.node == nil {
		return p2p.DownloadedBlock{}, "", fmt.Errorf("p2p node is nil")
	}

	for {
		if downloaded, ok := s.takeQueuedMasterchainBlock(prev, masterchainSeqnoTarget(^uint32(0))); ok {
			return downloaded, "queue", nil
		}

		queryCtx, cancel := context.WithTimeout(ctx, nextBlockBootstrapProbeTimeout)
		probePeers := nextBlockBootstrapProbePeers
		broadcastWake, ready := s.node.MasterchainBroadcastAfter(prev.SeqNo)
		if ready {
			probePeers = nextBlockBootstrapUrgentPeers
		}
		result := make(chan nextBlockProbeResult, 1)
		go func() {
			downloaded, err := s.node.ProbeNextBlockFull(queryCtx, prev, probePeers)
			result <- nextBlockProbeResult{downloaded: downloaded, err: err}
		}()

		select {
		case res := <-result:
			cancel()
			if res.err != nil {
				return p2p.DownloadedBlock{}, "", fmt.Errorf("probe next block after %s: %w", storage.FormatBlockRef(prev), res.err)
			}
			if res.downloaded == nil {
				return p2p.DownloadedBlock{}, "", fmt.Errorf("probe next block after %s: empty response", storage.FormatBlockRef(prev))
			}
			prepared, err := prepareDownloadedBlock(*res.downloaded)
			if err != nil {
				return p2p.DownloadedBlock{}, "", fmt.Errorf("prepare probed next block after %s: %w", storage.FormatBlockRef(prev), err)
			}
			return prepared, "probe", nil
		case <-s.currentStateWake:
			cancel()
			continue
		case <-broadcastWake:
			cancel()
			continue
		case <-queryCtx.Done():
			cancel()
			return p2p.DownloadedBlock{}, "", fmt.Errorf("probe next block after %s: %w", storage.FormatBlockRef(prev), queryCtx.Err())
		case <-ctx.Done():
			cancel()
			return p2p.DownloadedBlock{}, "", ctx.Err()
		}
	}
}

type nextBlockProbeResult struct {
	downloaded *p2p.DownloadedBlock
	err        error
}

func downloadedBlockTimeLag(downloaded p2p.DownloadedBlock, now time.Time) (uint32, int64, bool) {
	if downloaded.Meta == nil || downloaded.Meta.GenUTime == 0 {
		return 0, 0, false
	}
	return downloaded.Meta.GenUTime, now.Unix() - int64(downloaded.Meta.GenUTime), true
}

func (s *Service) downloadNextChainBlock(ctx context.Context, prev ton.BlockIDExt) (p2p.DownloadedBlock, error) {
	return s.downloadChainBlockWithRetry(ctx, fmt.Sprintf("download next block after %s", storage.FormatBlockRef(prev)), func(ctx context.Context) (*p2p.DownloadedBlock, error) {
		return s.node.DownloadNextBlockFull(ctx, prev)
	})
}

func (s *Service) downloadExactChainBlock(ctx context.Context, block ton.BlockIDExt) (p2p.DownloadedBlock, error) {
	return s.downloadChainBlockWithRetry(ctx, fmt.Sprintf("download block %s", storage.FormatBlockRef(block)), func(ctx context.Context) (*p2p.DownloadedBlock, error) {
		return s.node.DownloadBlockFull(ctx, block)
	})
}

func (s *Service) downloadChainBlockWithRetry(ctx context.Context, label string, download func(context.Context) (*p2p.DownloadedBlock, error)) (p2p.DownloadedBlock, error) {
	var lastErr error
	for attempt := 1; attempt <= chainBlockDownloadRetries; attempt++ {
		downloaded, err := download(ctx)
		if err == nil && downloaded != nil {
			return *downloaded, nil
		}
		if err == nil {
			err = fmt.Errorf("empty response")
		}
		lastErr = err

		if attempt == chainBlockDownloadRetries {
			break
		}
		if err = waitRetry(ctx, chainBlockDownloadRetryDelay); err != nil {
			return p2p.DownloadedBlock{}, err
		}
	}

	return p2p.DownloadedBlock{}, fmt.Errorf("%s: %w", label, lastErr)
}
