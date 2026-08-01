package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

type nextBlockBootstrapProbeState struct {
	consecutiveMisses int
	liveTail          bool
	// broadcastGraceSeqno is the raw-broadcast seqno the probe already granted
	// a decode-grace hold for, so a broadcast whose local decode never
	// completes delays at most one probe round.
	broadcastGraceSeqno uint32
	// lastObtainAt/obtainInterval pace the first probe of every height: at the
	// live tail the next block is not due before roughly one block interval,
	// so probing earlier only produces guaranteed misses and steals the win
	// from the local broadcast pipeline.
	lastObtainAt   time.Time
	obtainInterval time.Duration
	// lastObtainFromBroadcast gates the pace: parking the probe is a bet that
	// the next block arrives by broadcast, and the bet is only placed while
	// the previous block actually came that way. When broadcasts stop, the
	// very next height reverts to immediate probing, so the download-only lag
	// stays flat instead of drifting by pace headroom every block.
	lastObtainFromBroadcast bool
}

func (s *nextBlockBootstrapProbeState) noteObtained(now time.Time, fromBroadcast bool) {
	s.lastObtainFromBroadcast = fromBroadcast
	last := s.lastObtainAt
	s.lastObtainAt = now
	if last.IsZero() {
		return
	}
	gap := now.Sub(last)
	if gap <= 0 || gap > nextBlockBootstrapPaceMaxGap {
		return
	}
	if s.obtainInterval <= 0 {
		s.obtainInterval = gap
		return
	}
	// slew-limit the sample so a single stalled block cannot inflate the pace
	if limit := s.obtainInterval * 2; gap > limit {
		gap = limit
	}
	s.obtainInterval = (s.obtainInterval*7 + gap*3) / 10
}

// probeDelay returns how long the next block is still expected to take based
// on the observed obtain cadence; zero when pacing does not apply.
func (s nextBlockBootstrapProbeState) probeDelay(now time.Time) time.Duration {
	if !s.liveTail || !s.lastObtainFromBroadcast || s.consecutiveMisses > 0 || s.obtainInterval <= 0 || s.lastObtainAt.IsZero() {
		return 0
	}
	target := s.obtainInterval + nextBlockBootstrapPaceHeadroom
	if target > nextBlockBootstrapPaceMaxDelay {
		target = nextBlockBootstrapPaceMaxDelay
	}
	delay := target - now.Sub(s.lastObtainAt)
	if delay < nextBlockBootstrapPaceMinDelay {
		return 0
	}
	return delay
}

type nextBlockBootstrapProbeDecision struct {
	peerLimit             int
	consecutiveMisses     int
	liveTail              bool
	prevSeqno             uint32
	rawBroadcastAhead     bool
	rawBroadcastSeqno     uint32
	observedAhead         bool
	seenAhead             bool
	queuedFutureAhead     bool
	preferredSourcePeerID p2p.PeerID
	aheadBlocks           uint32
	lowestMissingSeqno    uint32
	lagSeconds            int64
	hasLag                bool
}

// rawBroadcastNextPending reports that the raw broadcast signal points exactly
// at the next needed block: its payload was received and its decode is still
// in flight locally, so the probe should yield a grace window instead of
// fanning out against its own pipeline.
func (d nextBlockBootstrapProbeDecision) rawBroadcastNextPending() bool {
	return d.rawBroadcastAhead && d.rawBroadcastSeqno == d.prevSeqno+1
}

// rawBroadcastBeyondNext reports a raw broadcast more than one block ahead:
// the broadcast for the next needed block was missed, so only a download can
// advance the chain and an urgent fanout is justified.
func (d nextBlockBootstrapProbeDecision) rawBroadcastBeyondNext() bool {
	return d.rawBroadcastAhead && d.rawBroadcastSeqno > d.prevSeqno+1
}

func (s *Service) nextBlockBootstrapProbeDecision(prev ton.BlockIDExt, prevUTime int64, state nextBlockBootstrapProbeState) (nextBlockBootstrapProbeDecision, <-chan struct{}, error) {
	decision := nextBlockBootstrapProbeDecision{
		peerLimit:         nextBlockBootstrapProbePeers,
		consecutiveMisses: state.consecutiveMisses,
		liveTail:          state.liveTail,
		prevSeqno:         prev.SeqNo,
	}

	rawBroadcastSeqno, broadcastWake := s.node.MasterchainBroadcastAfter(prev.SeqNo)
	decision.rawBroadcastSeqno = rawBroadcastSeqno
	decision.rawBroadcastAhead = decision.rawBroadcastSeqno > prev.SeqNo

	observed, err := s.node.ObservedMasterchainBlock()
	if err == nil {
		var seqno uint32
		seqno, err = masterchainSeqnoAhead(prev.SeqNo, observed)
		if err == nil {
			decision.observedAhead = true
			decision.noteAheadSeqno(prev.SeqNo, seqno)
		}
	}
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return nextBlockBootstrapProbeDecision{}, nil, fmt.Errorf("observe masterchain head: %w", err)
	}

	seen, err := s.node.SeenMasterchainBlock()
	if err == nil {
		var seqno uint32
		seqno, err = masterchainSeqnoAhead(prev.SeqNo, seen)
		if err == nil {
			decision.seenAhead = true
			decision.noteAheadSeqno(prev.SeqNo, seqno)
		}
	}
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return nextBlockBootstrapProbeDecision{}, nil, fmt.Errorf("read seen masterchain head: %w", err)
	}

	queued, err := s.queuedMasterchainFuture(prev)
	if err == nil {
		decision.queuedFutureAhead = true
		decision.preferredSourcePeerID = queued.sourcePeerID
		decision.lowestMissingSeqno = queued.lowestMissingSeqno
		decision.noteAheadSeqno(prev.SeqNo, queued.block.SeqNo)
	} else if !errors.Is(err, storage.ErrNotFound) {
		return nextBlockBootstrapProbeDecision{}, nil, fmt.Errorf("inspect queued masterchain future: %w", err)
	}

	if prevUTime != 0 {
		decision.lagSeconds = time.Now().Unix() - prevUTime
		decision.hasLag = true
	}

	if decision.shouldUseWideFanout() {
		decision.peerLimit = nextBlockBootstrapWidePeers
	} else if decision.shouldUseUrgentFanout() {
		decision.peerLimit = nextBlockBootstrapUrgentPeers
	}
	if !decision.liveTail {
		decision.preferredSourcePeerID = p2p.PeerID{}
	}
	return decision, broadcastWake, nil
}

func masterchainSeqnoAhead(currentSeqno uint32, block ton.BlockIDExt) (uint32, error) {
	if block.Workchain != -1 || block.Shard != topShard || block.SeqNo <= currentSeqno {
		return 0, storage.ErrNotFound
	}
	return block.SeqNo, nil
}

func (d *nextBlockBootstrapProbeDecision) noteAheadSeqno(currentSeqno uint32, seqno uint32) {
	if seqno <= currentSeqno {
		return
	}
	gap := seqno - currentSeqno
	if gap > d.aheadBlocks {
		d.aheadBlocks = gap
	}
}

func (d nextBlockBootstrapProbeDecision) shouldUseUrgentFanout() bool {
	// rawBroadcastNextPending intentionally does not widen the fanout: the
	// next block's broadcast is already received and decoding locally, so the
	// probe is only a fallback and hammering peers would race that decode.
	if d.liveTail && (d.rawBroadcastBeyondNext() || d.observedAhead || d.seenAhead || d.queuedFutureAhead) {
		return true
	}
	if d.consecutiveMisses >= nextBlockBootstrapUrgentMisses {
		return true
	}
	return d.hasLag && d.lagSeconds >= nextBlockBootstrapUrgentLagSeconds
}

func (d nextBlockBootstrapProbeDecision) shouldUseWideFanout() bool {
	if d.consecutiveMisses >= nextBlockBootstrapWideMisses {
		return true
	}
	if d.aheadBlocks >= nextBlockBootstrapWideGapBlocks {
		return true
	}
	return d.hasLag && d.lagSeconds >= nextBlockBootstrapWideLagSeconds
}

func (d nextBlockBootstrapProbeDecision) probeTimeout() time.Duration {
	if d.liveTail {
		return nextBlockBootstrapLiveProbeTimeout
	}
	return nextBlockBootstrapProbeTimeout
}

func (d nextBlockBootstrapProbeDecision) stagedPeerLimit() int {
	if !d.liveTail || d.peerLimit >= nextBlockBootstrapWidePeers {
		return d.peerLimit
	}
	return nextBlockBootstrapWidePeers
}

func nextBlockBootstrapLiveTail(blockUTime int64, nowUnix int64) bool {
	return blockUTime != 0 && nowUnix-blockUTime <= nextBlockBootstrapLiveLagSeconds
}

func (s *Service) publishCommittedCurrentState(current *storage.CurrentState) {
	s.observeBroadcastFlushedCurrentState(current)
	s.rememberAppliedMasterchainState(&current.Masterchain)
	s.rememberAppliedShardHeads(current.Shards)
	snapshot := s.publishLiveCurrentStateChanged(current)
	if snapshot == nil {
		return
	}
	if s.liveState != nil {
		s.liveState.SetLiveCurrentStateSnapshot(snapshot)
		s.liveState.MarkLiveCurrentStateFlushed(current)
	}
	s.wakeServiceMaintenance()
}

// publishLiveCurrentStateChanged clones current once, publishes the clone as
// the node-wide current status and returns it so callers can reuse it as an
// immutable snapshot (e.g. hand it to the live view without a second clone).
// Returns nil when current is behind the already-published status.
func (s *Service) publishLiveCurrentStateChanged(current *storage.CurrentState) *storage.CurrentState {
	next := storage.CloneCurrentState(current)

	s.currentStatusMu.Lock()
	if currentStateBehind(next, s.currentStatus) {
		s.currentStatusMu.Unlock()
		return nil
	}
	s.currentStatus = next
	s.currentStatusMu.Unlock()

	s.observeBroadcastLiveCurrentState(next)

	s.node.NotifyCompressedBlockStateReady(currentStateCompressedBlockStateRefs(next)...)
	return next
}

func currentStateCompressedBlockStateRefs(current *storage.CurrentState) []ton.BlockIDExt {
	refs := make([]ton.BlockIDExt, 0, len(current.Shards)+1)
	refs = append(refs, current.Masterchain.Block)
	for _, key := range storage.SortedShardKeys(current.Shards) {
		shard := current.Shards[key]
		refs = append(refs, shard.Block)
	}
	return refs
}

// rememberAppliedShardHeads publishes the applied top block of every followed
// shard to classify, which drops broadcasts at or below them before parsing a
// proof, checking signatures or decoding a payload.
func (s *Service) rememberAppliedShardHeads(shards map[storage.ShardKey]storage.BlockState) {
	if len(shards) == 0 {
		return
	}

	heads := make([]ton.BlockIDExt, 0, len(shards))
	for _, shard := range shards {
		heads = append(heads, shard.Block)
	}
	s.node.NoteAppliedShardHeads(heads)
}

func (s *Service) rememberAppliedMasterchainState(state *storage.BlockState) {
	if state.Block.Workchain != -1 || state.Block.Shard != topShard {
		return
	}
	s.node.NoteAppliedMasterchainSeqno(state.Block.SeqNo)

	next := storage.CloneBlockState(state)

	s.currentStatusMu.Lock()
	if s.appliedMasterchainStatus != nil && s.appliedMasterchainStatus.Block.SeqNo >= next.Block.SeqNo {
		s.currentStatusMu.Unlock()
		return
	}
	s.appliedMasterchainStatus = next
	s.currentStatusMu.Unlock()
}

func (s *Service) appliedMasterchainStatusSnapshot() *storage.BlockState {
	s.currentStatusMu.RLock()
	state := storage.CloneBlockState(s.appliedMasterchainStatus)
	s.currentStatusMu.RUnlock()
	return state
}

func currentStateBehind(next *storage.CurrentState, current *storage.CurrentState) bool {
	if current == nil {
		return false
	}

	nextShardClientSeqno := next.ShardClientSeqno
	if nextShardClientSeqno == 0 {
		nextShardClientSeqno = next.Masterchain.Block.SeqNo
	}
	currentShardClientSeqno := current.ShardClientSeqno
	if currentShardClientSeqno == 0 {
		currentShardClientSeqno = current.Masterchain.Block.SeqNo
	}
	if nextShardClientSeqno != currentShardClientSeqno {
		return nextShardClientSeqno < currentShardClientSeqno
	}
	return next.Masterchain.Block.SeqNo < current.Masterchain.Block.SeqNo
}

type masterchainApplyTiming struct {
	total       time.Duration
	prepare     time.Duration
	consensus   time.Duration
	stateUpdate time.Duration
}

func (s *Service) applyMasterchainTransition(ctx context.Context, current *storage.BlockState, block PreparedBlock, checked *checkedMasterchainConsensus, applier stateUpdateApplier, hook *blockApplyHookMeta) (*storage.BlockState, masterchainApplyTiming, error) {
	started := time.Now()
	var timing masterchainApplyTiming
	finish := func() masterchainApplyTiming {
		timing.total = time.Since(started)
		return timing
	}

	if block.ID.Workchain != -1 || block.ID.Shard != topShard {
		return nil, finish(), fmt.Errorf("download next masterchain block after %s returned %s", storage.FormatBlockRef(current.Block), block.BlockRef())
	}
	if len(block.Meta.PrevRefs) != 1 {
		return nil, finish(), fmt.Errorf("masterchain block %s has no single previous ref", block.BlockRef())
	}
	prev := block.Meta.PrevRefs[0]
	if !prev.Equals(&current.Block) {
		return nil, finish(), fmt.Errorf("%w: block=%s prev=%s current=%s", errMasterchainPrevMismatch, block.BlockRef(), storage.FormatBlockRef(prev), storage.FormatBlockRef(current.Block))
	}

	if checked != nil {
		stageStarted := time.Now()
		if err := checked.validateFor(current, block.ID); err != nil {
			timing.consensus += time.Since(stageStarted)
			return nil, finish(), fmt.Errorf("checked masterchain consensus for %s: %w", block.BlockRef(), err)
		}
		timing.consensus += time.Since(stageStarted)
	}

	stageStarted := time.Now()
	next, err := s.applyBlockWithHooks(ctx, []*storage.BlockState{current}, block, applier, hook)
	timing.stateUpdate += time.Since(stageStarted)
	if err != nil {
		return nil, finish(), fmt.Errorf("apply masterchain block %s: %w", block.BlockRef(), err)
	}

	return next, finish(), nil
}

// beginNextBlockCheckpointLocked validates persist availability and prepares
// the checkpoint. Called with currentStatePersistMu held; on error the mutex
// is released before returning.
func (s *Service) beginNextBlockCheckpointLocked(current *storage.CurrentState, timing *catchUpTiming, entries []storage.StateCheckpointBlock, cells *stateCellCheckpointCache, artifactPrewriteTarget uint64) (stateCheckpointData, error) {
	if err := s.checkCurrentStatePersistAllowed(); err != nil {
		s.currentStatePersistMu.Unlock()
		return stateCheckpointData{}, err
	}
	checkpoint := prepareStateCheckpoint(current, entries, cells)
	checkpoint.artifactPrewriteTarget = artifactPrewriteTarget
	timing.checkpoints++
	return checkpoint, nil
}

// saveNextBlockCheckpointLocked runs the checkpoint save and is the single
// owner of releasing currentStatePersistMu: it unlocks exactly once on every
// path. onError runs before the unlock on the failure path so persist errors
// become visible to the next flush before the mutex is released.
func (s *Service) saveNextBlockCheckpointLocked(ctx context.Context, checkpoint stateCheckpointData, cells *stateCellCheckpointCache, onCommitted func(), onError func(error)) (*storage.CurrentState, []SyncPersistStageObservation, time.Duration, error) {
	started := time.Now()
	committed, stages, err := s.saveStateCheckpoint(ctx, checkpoint.persisted, checkpoint.entries, checkpoint.cells, checkpoint.cellPrewriteTarget, checkpoint.artifactPrewriteTarget)
	elapsed := time.Since(started)
	if err != nil {
		if onError != nil {
			onError(err)
		}
		s.currentStatePersistMu.Unlock()
		return nil, stages, elapsed, err
	}

	if onCommitted != nil {
		onCommitted()
	}
	cells.complete()
	s.currentStatePersistMu.Unlock()
	return committed, stages, elapsed, nil
}

func (s *Service) persistNextBlockCurrentStateLocked(current *storage.CurrentState, timing *catchUpTiming, entries []storage.StateCheckpointBlock, cells *stateCellCheckpointCache, artifactPrewriteTarget uint64, onCommitted func(), onDone func(), lockElapsed time.Duration, queuedAt time.Time) (*storage.CurrentState, error) {
	checkpoint, err := s.beginNextBlockCheckpointLocked(current, timing, entries, cells, artifactPrewriteTarget)
	if err != nil {
		return nil, err
	}
	master := current.Masterchain.Block
	shardClientSeqno := current.ShardClientSeqno
	shards := len(current.Shards)

	s.log.Debug().
		Str("masterchain", storage.FormatBlockRef(master)).
		Uint32("shard_client_seqno", shardClientSeqno).
		Int("shards", shards).
		Dur("lock_wait", lockElapsed).
		Msg("next-block shard-client checkpoint scheduled")

	persistCtx := s.shutdownContext
	s.runAsync(func() {
		if onDone != nil {
			defer onDone()
		}

		var wrapped error
		committed, stages, elapsed, err := s.saveNextBlockCheckpointLocked(persistCtx, checkpoint, cells, onCommitted, func(saveErr error) {
			wrapped = fmt.Errorf("persist next-block current state %s: %w", storage.FormatBlockRef(master), saveErr)
			s.setCurrentStatePersistError(wrapped)
		})
		if err != nil {
			s.observeSyncPersist(SyncPersistObservation{
				Mode:          "next_block_async",
				Result:        "error",
				QueueDuration: time.Since(queuedAt) - elapsed,
				Duration:      elapsed,
				States:        len(checkpoint.entries),
				Stages:        stages,
			})
			s.log.Error().
				Err(wrapped).
				Str("masterchain", storage.FormatBlockRef(master)).
				Uint32("shard_client_seqno", shardClientSeqno).
				Dur("queued_for", time.Since(queuedAt)).
				Dur("elapsed", elapsed).
				Msg("next-block shard-client checkpoint failed")
			return
		}

		s.observeSyncPersist(SyncPersistObservation{
			Mode:          "next_block_async",
			Result:        "success",
			QueueDuration: time.Since(queuedAt) - elapsed,
			Duration:      elapsed,
			States:        len(checkpoint.entries),
			Stages:        stages,
		})
		s.log.Debug().
			Str("masterchain", storage.FormatBlockRef(master)).
			Uint32("shard_client_seqno", shardClientSeqno).
			Int("states", len(checkpoint.entries)).
			Int("shards", shards).
			Dur("queued_for", time.Since(queuedAt)).
			Dur("elapsed", elapsed).
			Msg("next-block shard-client checkpoint persisted")
		s.publishCommittedCurrentState(committed)
		s.markLiveCheckpointStatesFlushed(checkpoint.entries)
	})

	return checkpoint.live, nil
}

func (s *Service) persistNextBlockCurrentStateSyncLocked(current *storage.CurrentState, timing *catchUpTiming, reason string, entries []storage.StateCheckpointBlock, cells *stateCellCheckpointCache, artifactPrewriteTarget uint64, onCommitted func(), onDone func(), lockElapsed time.Duration) (*storage.CurrentState, error) {
	checkpoint, err := s.beginNextBlockCheckpointLocked(current, timing, entries, cells, artifactPrewriteTarget)
	if err != nil {
		return nil, err
	}
	master := current.Masterchain.Block
	shardClientSeqno := current.ShardClientSeqno
	shards := len(current.Shards)
	if onDone != nil {
		defer onDone()
	}

	persistCtx := s.shutdownContext
	committed, stages, elapsed, err := s.saveNextBlockCheckpointLocked(persistCtx, checkpoint, cells, onCommitted, nil)
	if err != nil {
		s.observeSyncPersist(SyncPersistObservation{
			Mode:          "next_block_sync",
			Result:        "error",
			QueueDuration: lockElapsed,
			Duration:      elapsed,
			States:        len(checkpoint.entries),
			Stages:        stages,
		})
		return nil, fmt.Errorf("persist next-block current state %s: %w", storage.FormatBlockRef(master), err)
	}

	s.observeSyncPersist(SyncPersistObservation{
		Mode:          "next_block_sync",
		Result:        "success",
		QueueDuration: lockElapsed,
		Duration:      elapsed,
		States:        len(checkpoint.entries),
		Stages:        stages,
	})
	s.log.Info().
		Str("masterchain", storage.FormatBlockRef(master)).
		Uint32("shard_client_seqno", shardClientSeqno).
		Int("shards", shards).
		Str("reason", reason).
		Dur("elapsed", elapsed).
		Int("states", len(checkpoint.entries)).
		Msg("next-block shard-client checkpoint persisted")
	s.publishCommittedCurrentState(committed)
	s.markLiveCheckpointStatesFlushed(checkpoint.entries)
	return checkpoint.live, nil
}

func (s *Service) markLiveCheckpointStatesFlushed(entries []storage.StateCheckpointBlock) {
	if len(entries) == 0 {
		return
	}

	var stateBlocks []ton.BlockIDExt
	var artifactBlocks []ton.BlockIDExt
	for _, entry := range entries {
		if entry.State != nil {
			stateBlocks = append(stateBlocks, entry.State.Block)
		}
		if entry.Artifact != nil {
			artifactBlocks = append(artifactBlocks, entry.Artifact.ID)
		}
	}
	if s.liveState != nil && len(stateBlocks) > 0 {
		s.liveState.MarkLiveBlockStatesFlushed(stateBlocks)
	}
	if s.liveState != nil {
		for _, block := range artifactBlocks {
			s.liveState.MarkLiveBlockFlushed(block)
		}
	}
	if s.liveBlockCache != nil {
		for _, block := range artifactBlocks {
			s.liveBlockCache.MarkBlockFlushed(block)
		}
	}
}

func (s *Service) setCurrentStatePersistError(err error) {
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
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		if s.currentStatePersistMu.TryLock() {
			s.currentStatePersistMu.Unlock()
			return s.takeCurrentStatePersistError()
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Service) waitCurrentStatePersistOrWake(ctx context.Context) (bool, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		// Taken before the probe: a wake landing between the probe and the
		// select would otherwise be lost, and here that does not merely delay
		// the wait — the wake is the only thing that reports "woken" rather
		// than "the persist finished".
		wake := s.currentStateWakeChan()
		if s.currentStatePersistMu.TryLock() {
			s.currentStatePersistMu.Unlock()
			return false, s.takeCurrentStatePersistError()
		}

		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-wake:
			if s.currentStatePersistMu.TryLock() {
				s.currentStatePersistMu.Unlock()
				if err := s.takeCurrentStatePersistError(); err != nil {
					return false, err
				}
			}
			return true, nil
		case <-ticker.C:
		}
	}
}

// nextMasterchainProbeHoldDelay returns how long the probe should stay parked
// before hitting peers: the pace delay while the next block is not yet due,
// raised to a one-shot decode grace when the raw broadcast for exactly the
// next block was already received and is decoding locally.
func nextMasterchainProbeHoldDelay(decision *nextBlockBootstrapProbeDecision, state *nextBlockBootstrapProbeState, now time.Time) time.Duration {
	if !decision.liveTail {
		return 0
	}
	// a queued future block, a peer-observed newer head or a raw broadcast
	// beyond the next block all mean the next block already exists somewhere
	// and only a download can advance; do not park the probe
	if decision.queuedFutureAhead || decision.observedAhead || decision.seenAhead || decision.rawBroadcastBeyondNext() {
		return 0
	}

	// pacing assumes the head is current; with a real lag the next blocks are
	// already produced somewhere and waiting out the cadence only adds delay
	hold := time.Duration(0)
	if !decision.hasLag || decision.lagSeconds < nextBlockBootstrapUrgentLagSeconds {
		hold = state.probeDelay(now)
	}
	if decision.rawBroadcastNextPending() && state.broadcastGraceSeqno != decision.rawBroadcastSeqno {
		state.broadcastGraceSeqno = decision.rawBroadcastSeqno
		if nextBlockBootstrapDecodeGrace > hold {
			hold = nextBlockBootstrapDecodeGrace
		}
	}
	return hold
}

// holdNextMasterchainProbe parks the probe for up to hold, returning early
// with the block when the local broadcast pipeline queues it, or with
// ErrNotFound as soon as the decoded next block lands in the node broadcast
// cache (the probe then consumes it locally without touching peers). A raw
// broadcast for the next block arriving mid-hold extends the park once by the
// decode grace; a raw broadcast beyond the next block ends the hold so the
// fallback probe can catch up by download.
func (s *Service) holdNextMasterchainProbe(ctx context.Context, prev, target ton.BlockIDExt, state *nextBlockBootstrapProbeState, hold time.Duration) (cachedMasterchainBlockForApply, error) {
	deadline := time.Now().Add(hold)
	timer := time.NewTimer(hold)
	defer timer.Stop()

	stateWake := s.currentStateWakeChan()
	_, rawWake := s.node.MasterchainBroadcastAfter(prev.SeqNo)
	nextBroadcastWake, unwatch := s.node.WatchMasterchainNextBroadcastBlock(prev)
	defer unwatch()
	// the decoded next block may already sit in the node broadcast cache
	// (it can arrive while the previous block is still applying); the
	// watch above only fires on future stores, so check before parking.
	// Same for the queue: the wake channel above only reports closes after
	// its capture, so a block queued between the caller's miss and this
	// point must be picked up by a check, not a wake.
	if s.node.HasMasterchainNextBroadcastBlock(prev) {
		return cachedMasterchainBlockForApply{}, storage.ErrNotFound
	}
	if cached, err := s.takeCachedMasterchainBlockForApply(ctx, prev, target); err == nil || !errors.Is(err, storage.ErrNotFound) {
		return cached, err
	}

	for {
		select {
		case <-nextBroadcastWake:
			// the decoded broadcast for the next block landed in the node
			// cache; end the hold so the probe picks it up without network
			return cachedMasterchainBlockForApply{}, storage.ErrNotFound
		case <-ctx.Done():
			return cachedMasterchainBlockForApply{}, ctx.Err()
		case <-stateWake:
			stateWake = s.currentStateWakeChan()
			cached, err := s.takeCachedMasterchainBlockForApply(ctx, prev, target)
			if err == nil || !errors.Is(err, storage.ErrNotFound) {
				return cached, err
			}
		case <-rawWake:
			cached, err := s.takeCachedMasterchainBlockForApply(ctx, prev, target)
			if err == nil || !errors.Is(err, storage.ErrNotFound) {
				return cached, err
			}
			rawSeqno, wake := s.node.MasterchainBroadcastAfter(prev.SeqNo)
			rawWake = wake
			if rawSeqno == 0 {
				continue
			}
			if rawSeqno > prev.SeqNo+1 {
				return cachedMasterchainBlockForApply{}, storage.ErrNotFound
			}
			if state.broadcastGraceSeqno == rawSeqno {
				continue
			}
			// the raw broadcast for exactly the next block landed while
			// parked: grant its decode the grace window before hitting peers
			state.broadcastGraceSeqno = rawSeqno
			graceDeadline := time.Now().Add(nextBlockBootstrapDecodeGrace)
			if !graceDeadline.After(deadline) {
				continue
			}
			deadline = graceDeadline
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(time.Until(deadline))
		case <-timer.C:
			return cachedMasterchainBlockForApply{}, storage.ErrNotFound
		}
	}
}

func (s *Service) downloadNextChainBlockProbe(ctx context.Context, prev ton.BlockIDExt, prevUTime int64, state *nextBlockBootstrapProbeState) (PreparedBlock, SyncBlockSource, time.Duration, error) {
	target := masterchainSeqnoTarget(^uint32(0))

	cached, err := s.takeCachedMasterchainBlockForApply(ctx, prev, target)
	if err == nil {
		return cached.block, cached.source, cached.prepareElapsed, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return PreparedBlock{}, "", 0, err
	}

	decision, broadcastWake, err := s.nextBlockBootstrapProbeDecision(prev, prevUTime, *state)
	if err != nil {
		return PreparedBlock{}, "", 0, err
	}
	if hold := nextMasterchainProbeHoldDelay(&decision, state, time.Now()); hold > 0 {
		held, err := s.holdNextMasterchainProbe(ctx, prev, target, state, hold)
		if err == nil {
			return held.block, held.source, held.prepareElapsed, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return PreparedBlock{}, "", 0, err
		}
		// the hold expired without the broadcast pipeline delivering the
		// block; refresh the ahead signals so the fallback probe fans out on
		// the current picture
		decision, broadcastWake, err = s.nextBlockBootstrapProbeDecision(prev, prevUTime, *state)
		if err != nil {
			return PreparedBlock{}, "", 0, err
		}
	}
	stagedPeerLimit := decision.stagedPeerLimit()
	queryCtx, cancel := context.WithTimeout(ctx, decision.probeTimeout())
	if decision.peerLimit > nextBlockBootstrapProbePeers || stagedPeerLimit > decision.peerLimit {
		event := s.log.Debug().
			Str("current", storage.FormatBlockRef(prev)).
			Int("probe_peers", decision.peerLimit).
			Int("staged_probe_peers", stagedPeerLimit).
			Int("consecutive_misses", decision.consecutiveMisses).
			Bool("live_tail", decision.liveTail).
			Bool("raw_broadcast_ahead", decision.rawBroadcastAhead).
			Bool("observed_masterchain_ahead", decision.observedAhead).
			Bool("seen_masterchain_ahead", decision.seenAhead).
			Bool("queued_future_ahead", decision.queuedFutureAhead).
			Uint32("ahead_blocks", decision.aheadBlocks)
		if decision.lowestMissingSeqno != 0 {
			event.Uint32("lowest_missing_seqno", decision.lowestMissingSeqno)
		}
		if !decision.preferredSourcePeerID.IsZero() {
			event.Str("preferred_source_peer_id", decision.preferredSourcePeerID.String())
		}
		if decision.hasLag {
			event.Int64("lag_seconds", decision.lagSeconds)
		}
		event.Msg("probing next masterchain block with urgent fanout")
	}
	result := make(chan nextBlockProbeResult, 1)
	probeReturned := make(chan struct{})
	go func() {
		downloaded, err := s.node.ProbeNextBlockFull(queryCtx, prev, p2p.ProbeNextBlockFullOptions{
			PeerLimit:       decision.peerLimit,
			StagedPeerLimit: stagedPeerLimit,
			StageDelay:      nextBlockBootstrapLiveStageDelay,
			PreferredPeerID: decision.preferredSourcePeerID,
			LiveTail:        decision.liveTail,
		})
		close(probeReturned)
		// ctx, not queryCtx: the callers below cancel queryCtx as soon as they
		// have a winner, and the shared preparation must not be abandoned
		// half-way for the consumer that is still waiting on it.
		result <- s.prepareNextBlockProbeResult(ctx, prev, downloaded, err)
	}()

	prepared, source, prepareElapsed, err := s.waitNextMasterchainApplyCandidate(
		ctx,
		queryCtx,
		prev,
		target,
		broadcastWake,
		probeReturned,
		result,
		cancel,
	)
	if err != nil {
		return PreparedBlock{}, source, prepareElapsed, err
	}
	return prepared, source, prepareElapsed, nil
}

func (s *Service) waitNextMasterchainApplyCandidate(
	ctx context.Context,
	queryCtx context.Context,
	prev, target ton.BlockIDExt,
	broadcastWake <-chan struct{},
	probeReturned <-chan struct{},
	result <-chan nextBlockProbeResult,
	cancel context.CancelFunc,
) (PreparedBlock, SyncBlockSource, time.Duration, error) {
	queryDone := queryCtx.Done()
	stateWake := s.currentStateWakeChan()
	// A block queued between the caller's miss and the capture above closed a
	// channel this wait never selects on; re-check before parking.
	cached, err := s.takeCachedMasterchainBlockForApply(ctx, prev, target)
	if err == nil {
		cancel()
		return cached.block, cached.source, cached.prepareElapsed, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		cancel()
		return PreparedBlock{}, "", 0, err
	}
	for {
		select {
		case res := <-result:
			cached, err := s.takeCachedMasterchainBlockForApply(ctx, prev, target)
			if err == nil {
				cancel()
				return cached.block, cached.source, cached.prepareElapsed, nil
			}
			if !errors.Is(err, storage.ErrNotFound) {
				cancel()
				return PreparedBlock{}, "", 0, err
			}

			cancel()
			if res.err != nil {
				return PreparedBlock{}, res.source, res.prepareElapsed, res.err
			}
			return res.block, res.source, res.prepareElapsed, nil
		case <-stateWake:
			stateWake = s.currentStateWakeChan()
			cached, err := s.takeCachedMasterchainBlockForApply(ctx, prev, target)
			if err == nil {
				cancel()
				return cached.block, cached.source, cached.prepareElapsed, nil
			}
			if !errors.Is(err, storage.ErrNotFound) {
				cancel()
				return PreparedBlock{}, "", 0, err
			}
		case <-broadcastWake:
			cached, err := s.takeCachedMasterchainBlockForApply(ctx, prev, target)
			if err == nil {
				cancel()
				return cached.block, cached.source, cached.prepareElapsed, nil
			}
			if !errors.Is(err, storage.ErrNotFound) {
				cancel()
				return PreparedBlock{}, "", 0, err
			}
			broadcastWake = nil
		case <-probeReturned:
			probeReturned = nil
			queryDone = nil
		case <-queryDone:
			select {
			case <-probeReturned:
				probeReturned = nil
				queryDone = nil
				continue
			default:
			}

			cancel()
			return PreparedBlock{}, "", 0, fmt.Errorf("probe next block after %s: %w", storage.FormatBlockRef(prev), queryCtx.Err())
		case <-ctx.Done():
			cancel()
			return PreparedBlock{}, "", 0, ctx.Err()
		}
	}
}

type nextBlockProbeResult struct {
	block          PreparedBlock
	source         SyncBlockSource
	prepareElapsed time.Duration
	err            error
}

func (s *Service) prepareNextBlockProbeResult(ctx context.Context, prev ton.BlockIDExt, downloaded *p2p.DownloadedBlock, err error) nextBlockProbeResult {
	if err != nil {
		return nextBlockProbeResult{err: fmt.Errorf("probe next block after %s: %w", storage.FormatBlockRef(prev), err)}
	}

	prepareStarted := time.Now()
	verified, err := s.verifyDownloadedBlock(*downloaded)
	prepareElapsed := time.Since(prepareStarted)
	if err != nil {
		return nextBlockProbeResult{
			source:         syncBlockSourceForKind(SyncBlockSourcePeerProbe, downloaded.Kind),
			prepareElapsed: prepareElapsed,
			err:            fmt.Errorf("verify probed next block after %s: %w", storage.FormatBlockRef(prev), err),
		}
	}

	// Same gate order as before: reject a block that does not follow prev
	// before paying for its state-update cells.
	if err = checkVerifiedMasterchainBlockFollows(prev, verified); err == nil {
		var prepared PreparedBlock
		// Shared with the block-sync worker, which usually prepares the very
		// same decoded broadcast concurrently.
		prepared, err = s.prepareVerifiedMasterchainBlockShared(ctx, verified)
		if err == nil {
			// The probe goroutine runs in parallel with apply: pay for the
			// signature batch of a downloaded block here so apply keeps only
			// the hash checks. Broadcast-origin blocks are already marked.
			s.preverifyMasterchainConsensusSignatures(prepared.consensus)
			prepared.PrepareElapsed = time.Since(prepareStarted)
			return nextBlockProbeResult{
				block:          prepared,
				source:         syncBlockSourceForKind(SyncBlockSourcePeerProbe, verified.Kind),
				prepareElapsed: prepared.PrepareElapsed,
			}
		}
	}

	return nextBlockProbeResult{
		source:         syncBlockSourceForKind(SyncBlockSourcePeerProbe, verified.Kind),
		prepareElapsed: time.Since(prepareStarted),
		err:            fmt.Errorf("prepare probed next block after %s: %w", storage.FormatBlockRef(prev), err),
	}
}

func downloadedBlockTimeLag(downloaded PreparedBlock, now time.Time) (uint32, int64) {
	if downloaded.Meta.GenUTime == 0 {
		return 0, 0
	}
	return downloaded.Meta.GenUTime, now.Unix() - int64(downloaded.Meta.GenUTime)
}

func (s *Service) downloadNextChainBlock(ctx context.Context, prev ton.BlockIDExt) (p2p.DownloadedBlock, error) {
	return s.downloadChainBlockWithRetry(ctx, func() string {
		return "download next block after " + storage.FormatBlockRef(prev)
	}, func(ctx context.Context) (*p2p.DownloadedBlock, error) {
		return s.node.DownloadNextBlockFull(ctx, prev)
	})
}

// downloadChainBlockWithRetry takes the label lazily: it is only read when
// every attempt failed.
func (s *Service) downloadChainBlockWithRetry(ctx context.Context, label func() string, download func(context.Context) (*p2p.DownloadedBlock, error)) (p2p.DownloadedBlock, error) {
	var lastErr error
	for attempt := 1; attempt <= chainBlockDownloadRetries; attempt++ {
		downloaded, err := download(ctx)
		if err == nil {
			return *downloaded, nil
		}
		lastErr = err

		if attempt == chainBlockDownloadRetries {
			break
		}
		if err = waitRetry(ctx, chainBlockDownloadRetryDelay); err != nil {
			return p2p.DownloadedBlock{}, err
		}
	}

	return p2p.DownloadedBlock{}, fmt.Errorf("%s: %w", label(), lastErr)
}
