package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xssnick/gton/service/storage"
)

type archiveCheckpointResult struct {
	persisted        *storage.CurrentState
	states           appliedStateCheckpoint
	checkpointBlocks uint32
	elapsed          time.Duration
	reason           string
	err              error
}

type archiveProgressGoalKind uint8

const (
	archiveProgressGoalNone archiveProgressGoalKind = iota
	archiveProgressGoalLiveTail
	archiveProgressGoalSyncUntil
)

type archiveProgressGoal struct {
	kind                     archiveProgressGoalKind
	remainingSeconds         int64
	hasKnownRemainingSeconds bool
}

func (g archiveProgressGoal) knownRemaining() bool {
	return g.hasKnownRemainingSeconds
}

type archiveDownloadBackpressureGate struct {
	mu     sync.Mutex
	done   chan struct{}
	paused bool
}

func (g *archiveDownloadBackpressureGate) pause() {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.paused {
		return
	}
	g.paused = true
	g.done = make(chan struct{})
}

func (g *archiveDownloadBackpressureGate) resume() {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.paused {
		return
	}
	g.paused = false
	close(g.done)
	g.done = nil
}

func (g *archiveDownloadBackpressureGate) wait(ctx context.Context) error {
	for {
		g.mu.Lock()
		if !g.paused {
			g.mu.Unlock()
			return nil
		}
		done := g.done
		g.mu.Unlock()

		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (r *archiveCatchUpRunner) pauseArchiveDownloadsForCheckpointBackpressure() func() {
	r.downloadGate.pause()
	return r.downloadGate.resume
}

func (s *Service) adaptArchiveCheckpointBlocks(current uint32, elapsed time.Duration) uint32 {
	base := s.archiveCatchUpCheckpointBlocks
	maxBlocks := uint32(archiveCatchUpMaxAdaptiveCheckpointBlocks)
	if maxBlocks < base {
		maxBlocks = base
	}

	next := current
	if elapsed >= archiveCatchUpCheckpointSlowThreshold && current < maxBlocks {
		next = current * 2
		if next > maxBlocks {
			next = maxBlocks
		}
	} else if elapsed <= archiveCatchUpCheckpointFastThreshold && current > base {
		next = current / 2
		if next < base {
			next = base
		}
	}
	return next
}

func (r *archiveCatchUpRunner) archiveProgressGoalAt(now time.Time) archiveProgressGoal {
	blockUTime := blockStateUtime(r.ctx, r.service.storage, &r.current.Masterchain)

	goal := archiveSyncUntilProgressGoal(r.service.syncUntil, blockUTime, now.Unix())
	if goal.kind != archiveProgressGoalNone {
		return goal
	}

	if blockUTime == 0 {
		return archiveProgressGoal{}
	}
	return archiveProgressGoal{
		kind:                     archiveProgressGoalLiveTail,
		remainingSeconds:         remainingLagSeconds(now.Unix() - blockUTime),
		hasKnownRemainingSeconds: true,
	}
}

func archiveSyncUntilProgressGoal(syncUntil uint32, blockUTime int64, nowUnix int64) archiveProgressGoal {
	if syncUntil == 0 {
		return archiveProgressGoal{}
	}
	// Future sync_until does not shorten the current archive download; live-tail is still the active target.
	if int64(syncUntil) >= nowUnix {
		return archiveProgressGoal{}
	}

	goal := archiveProgressGoal{kind: archiveProgressGoalSyncUntil}
	if blockUTime <= 0 {
		return goal
	}

	remaining := int64(syncUntil) - blockUTime
	if remaining < 0 {
		remaining = 0
	}
	goal.remainingSeconds = remaining
	goal.hasKnownRemainingSeconds = true
	return goal
}

func (r *archiveCatchUpRunner) shouldWaitArchiveCheckpointBackpressure() bool {
	if r.checkpointDone == nil {
		return false
	}

	pendingBlocks := r.current.ShardClientSeqno - r.lastCheckpointSeqno
	if pendingBlocks >= r.archiveCheckpointBackpressureBlocks() {
		return true
	}
	return r.pendingArchiveCheckpointBytes() >= r.archiveCheckpointBackpressureBytes()
}

func (r *archiveCatchUpRunner) archiveCheckpointBackpressureBlocks() uint32 {
	target := r.checkpointBlocksTarget
	return checkpointBackpressureBlocks(target, r.service.syncBackpressureWindows)
}

func (r *archiveCatchUpRunner) archiveCheckpointBackpressureBytes() uint64 {
	return checkpointBackpressureBytes(r.service.checkpointBytes, r.service.syncBackpressureWindows)
}

func (r *archiveCatchUpRunner) pendingArchiveCheckpointBytes() uint64 {
	total := r.stateCells.byteSize()
	artifactBytes := r.checkpointStates.byteSize()
	if total > ^uint64(0)-artifactBytes {
		return ^uint64(0)
	}
	return total + artifactBytes
}

func (r *archiveCatchUpRunner) persistCheckpoint(reason string) (uint32, error) {
	var checkpointBlocks uint32
	for {
		completedBlocks, err := r.finishCheckpoint(true)
		checkpointBlocks += completedBlocks
		if err != nil {
			return checkpointBlocks, err
		}
		if r.current.ShardClientSeqno <= r.lastCheckpointSeqno {
			return checkpointBlocks, nil
		}
		if err = r.startCheckpoint(reason); err != nil {
			return checkpointBlocks, err
		}
	}
}

func (r *archiveCatchUpRunner) startCheckpoint(reason string) error {
	if r.checkpointDone != nil || r.current.ShardClientSeqno <= r.lastCheckpointSeqno {
		return nil
	}
	if err := r.service.checkCurrentStatePersistAllowed(); err != nil {
		return err
	}
	lockStarted := time.Now()
	r.service.currentStatePersistMu.Lock()
	lockElapsed := time.Since(lockStarted)
	if err := r.service.checkCurrentStatePersistAllowed(); err != nil {
		r.service.currentStatePersistMu.Unlock()
		return err
	}

	current := storage.CloneCurrentState(r.current)
	checkpointStates := r.checkpointStates.checkpoint()
	checkpointCells := r.stateCells.beginCheckpoint()
	releaseCheckpointCells := func() {}
	if loader := checkpointCells.retainedLoader(r.service.stateCellLoader()); loader != nil {
		releaseCheckpointCells = r.service.retainStateCellLoader(loader)
	}
	checkpointBlocks := r.current.ShardClientSeqno - r.lastCheckpointSeqno
	done := make(chan archiveCheckpointResult, 1)
	r.checkpointDone = done

	r.service.log.Debug().
		Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
		Uint32("shard_client_seqno", current.ShardClientSeqno).
		Uint32("checkpoint_blocks", checkpointBlocks).
		Str("reason", reason).
		Msg("archive shard-client checkpoint scheduled")

	r.service.runAsync(func() {
		var releaseOnce sync.Once
		release := func() {
			releaseOnce.Do(releaseCheckpointCells)
		}
		defer release()

		startedCheckpoint := time.Now()
		persisted, err := r.persistArchiveCurrentState(current, checkpointBlocks, lockElapsed, checkpointStates.entries, checkpointCells)
		r.service.currentStatePersistMu.Unlock()
		if err != nil {
			r.service.setCurrentStatePersistError(err)
			r.service.log.Error().
				Err(err).
				Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
				Uint32("shard_client_seqno", current.ShardClientSeqno).
				Uint32("checkpoint_blocks", checkpointBlocks).
				Str("reason", reason).
				Msg("archive shard-client checkpoint failed")
		}
		if persisted != nil {
			r.service.publishLiveCurrentBlockMarkers(persisted)
			r.service.publishCommittedCurrentState(persisted)
		}
		release()
		done <- archiveCheckpointResult{
			persisted:        persisted,
			states:           checkpointStates,
			checkpointBlocks: checkpointBlocks,
			elapsed:          time.Since(startedCheckpoint),
			reason:           reason,
			err:              err,
		}
	})
	return nil
}

func (r *archiveCatchUpRunner) finishCheckpoint(wait bool) (uint32, error) {
	if r.checkpointDone == nil {
		return 0, nil
	}

	done := r.checkpointDone
	var result archiveCheckpointResult
	waitStarted := time.Now()
	if wait {
		result = archiveCheckpointResult{}
		ticker := time.NewTicker(archiveCheckpointWaitLogInterval)
		defer ticker.Stop()
		for {
			select {
			case result = <-done:
				goto checkpointDone
			case <-ticker.C:
				r.logCheckpointWait(waitStarted)
			case <-r.ctx.Done():
				return 0, r.ctx.Err()
			}
		}
	} else {
		select {
		case result = <-done:
		default:
			return 0, nil
		}
	}
checkpointDone:
	r.checkpointDone = nil
	if result.err != nil {
		return result.checkpointBlocks, result.err
	}
	if result.persisted == nil {
		return result.checkpointBlocks, fmt.Errorf("archive checkpoint completed without persisted state")
	}
	if result.persisted.ShardClientSeqno <= r.lastCheckpointSeqno {
		return 0, nil
	}
	if result.persisted.ShardClientSeqno > r.current.ShardClientSeqno {
		return result.checkpointBlocks, fmt.Errorf("archive checkpoint persisted future seqno %d after current seqno %d", result.persisted.ShardClientSeqno, r.current.ShardClientSeqno)
	}

	previousTarget := r.checkpointBlocksTarget
	r.checkpointBlocksTarget = r.service.adaptArchiveCheckpointBlocks(r.checkpointBlocksTarget, result.elapsed)
	if r.checkpointBlocksTarget != previousTarget {
		r.service.log.Debug().
			Uint32("previous_checkpoint_blocks", previousTarget).
			Uint32("checkpoint_blocks", r.checkpointBlocksTarget).
			Dur("checkpoint_elapsed", result.elapsed).
			Msg("adjusted archive checkpoint interval")
	}

	if result.persisted.ShardClientSeqno == r.current.ShardClientSeqno {
		r.current = result.persisted
	}
	r.checkpointStates.completeCheckpoint(result.states)
	r.lastCheckpoint = time.Now()
	r.lastCheckpointSeqno = result.persisted.ShardClientSeqno
	r.importCache.dropBefore(r.lastCheckpointSeqno + 1)
	r.recordArchiveCheckpointProgress(result.checkpointBlocks, result.elapsed)

	waitElapsed := time.Since(waitStarted)
	event := r.service.log.Debug()
	if wait && waitElapsed >= time.Second {
		event = r.service.log.Info()
	}
	event.
		Str("masterchain", storage.FormatBlockRef(result.persisted.Masterchain.Block)).
		Uint32("shard_client_seqno", result.persisted.ShardClientSeqno).
		Uint32("checkpoint_blocks", result.checkpointBlocks).
		Uint32("checkpoint_target_blocks", r.checkpointBlocksTarget).
		Dur("elapsed", result.elapsed).
		Dur("wait_elapsed", waitElapsed).
		Str("reason", result.reason).
		Msg("archive shard-client checkpoint completed")
	return result.checkpointBlocks, nil
}

func (r *archiveCatchUpRunner) logCheckpointWait(started time.Time) {
	r.service.log.Info().
		Str("masterchain", storage.FormatBlockRef(r.current.Masterchain.Block)).
		Uint32("shard_client_seqno", r.current.ShardClientSeqno).
		Uint32("persisted_masterchain_seqno", r.lastCheckpointSeqno).
		Uint32("pending_checkpoint_blocks", r.current.ShardClientSeqno-r.lastCheckpointSeqno).
		Uint64("pending_checkpoint_bytes", r.pendingArchiveCheckpointBytes()).
		Uint32("checkpoint_target_blocks", r.checkpointBlocksTarget).
		Uint64("checkpoint_target_bytes", r.service.checkpointBytes).
		Uint32("checkpoint_backpressure_blocks", r.archiveCheckpointBackpressureBlocks()).
		Uint64("checkpoint_backpressure_bytes", r.archiveCheckpointBackpressureBytes()).
		Dur("wait_elapsed", time.Since(started)).
		Msg("waiting for archive shard-client checkpoint")
}

func (r *archiveCatchUpRunner) persistProgressBeforeRetry(err error) error {
	if err == nil || r.current.ShardClientSeqno <= r.lastCheckpointSeqno || errors.Is(err, context.Canceled) {
		return nil
	}

	checkpointBlocks, persistErr := r.persistCheckpoint("retry")
	if persistErr != nil {
		return fmt.Errorf("persist archive retry checkpoint: %w", persistErr)
	}

	r.service.log.Info().
		Err(err).
		Str("masterchain", storage.FormatBlockRef(r.current.Masterchain.Block)).
		Uint32("shard_client_seqno", r.current.ShardClientSeqno).
		Uint32("checkpoint_blocks", checkpointBlocks).
		Msg("persisted archive shard-client progress before retry")

	r.checkpointBlocksTarget = r.service.archiveCatchUpCheckpointBlocks
	r.importCache.dropBefore(r.current.ShardClientSeqno + 1)
	return nil
}

func (r *archiveCatchUpRunner) returnWithProgress(err error) error {
	if persistErr := r.persistProgressBeforeRetry(err); persistErr != nil {
		return errors.Join(err, persistErr)
	}
	return err
}

func (r *archiveCatchUpRunner) restartPipeline(err error) error {
	if persistErr := r.persistProgressBeforeRetry(err); persistErr != nil {
		return errors.Join(err, persistErr)
	}

	r.pipeline.cancel()
	entries, hits := r.importCache.stats()
	r.service.log.Debug().
		Err(err).
		Uint32("current_seqno", r.current.ShardClientSeqno).
		Int("preloaded_archives", entries).
		Uint64("preload_cache_hits", hits).
		Msg("retrying archive shard-client preload from current state")

	if err = waitRetry(r.ctx, time.Second); err != nil {
		return err
	}
	r.pipeline = r.startShardClientArchiveWindowPipeline()
	return nil
}
