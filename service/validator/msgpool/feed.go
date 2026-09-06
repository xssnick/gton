package msgpool

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	sharddomain "github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/storage"
)

// AppliedBlock is one block with a resident post-state as the pool reads it:
// the identity that names an out-queue position, the block the delta is
// derived from, and the post-state the recovery path reseeds from.
//
// It is deliberately not the node's applied-block event. The pool is reached
// from two deployments — the validator's own workflow and the standalone
// collator extension — and from two producers inside the validator — the
// node's apply pipeline and the consensus acceptance of a block this node
// validated (Feed.ObserveAccepted). The fields below are the whole of what the
// feed reads, so naming them here keeps the message pool free of the node's
// hook surface and makes the retention rule (see Feed.Observe) checkable.
type AppliedBlock struct {
	ID        ton.BlockIDExt
	BlockRoot *cell.Cell
	// StateRoot is the state after the block. The delta path does not need it;
	// the recovery path cannot run without it.
	StateRoot *cell.Cell
	StartLT   uint64
	GenUTime  uint32
}

// AccountPrewarmer schedules raw cell-record warming for destinations and
// envelopes a pooled message will be read through. Implementations are
// non-blocking and may discard a hint: warming never participates in
// correctness.
type AccountPrewarmer interface {
	EnqueueRoot(cell.Hash) bool
	EnqueueAccount(workchain int32, account [32]byte) bool
}

// FeedOptions assembles a Feed.
type FeedOptions struct {
	Pool   *Pool
	Logger zerolog.Logger
	// Prewarmer is optional; nil warms nothing.
	Prewarmer AccountPrewarmer
	// FreshnessWindow gates bookkeeping by generation time. Historical
	// catch-up blocks do not update the pool inline. 0 means 5m, negative
	// processes every block.
	FreshnessWindow time.Duration
	// HeadSettleDelay arms the pool from an old chain head: a stale block
	// unsuperseded for this long is processed anyway, because a halted chain
	// leaves an old block at the head and a collator still needs the view of
	// exactly that state. 0 means 3s.
	HeadSettleDelay time.Duration
	// DisableInternals keeps the externals half of the feed and turns off the
	// internal-message section.
	DisableInternals bool
}

const (
	defaultFeedFreshnessWindow = 5 * time.Minute
	defaultFeedHeadSettleDelay = 3 * time.Second

	maxRetainedFeedTargets = 1024
)

// Feed advances the message pool by blocks whose post-state is resident.
//
// It owns everything that decision needs and nothing else: the destination
// topology the applied source is admitted against, the per-source serialization
// that keeps the out-queue runs gap-free, and the deferral of blocks too old to
// arm the pool from. Both deployments drive it from their own applied-block
// hook, which is what makes "the collator reads the queue the feed left at the
// head" true for the standalone collator and not only for a validator.
//
// A validator additionally drives it from consensus acceptance (ObserveAccepted):
// the state of a block this node validated and finalized is resident the moment
// the block is accepted, while the node's apply pipeline reaches the same block
// about a second later under load (p50 on the stand). The C++ collator collects
// neighbour out-queues from the shard states of the latest known shard blocks as
// soon as those states exist (collator.cpp request_neighbor_msg_queues), not when
// a later pipeline finishes, so feeding from acceptance is the parity move. The
// two producers deliver the same block twice; the per-source bookkeeping keys on
// the block identity (seqno and root hash), so whichever arrives first advances
// the pool and the other is a no-op — see observeSource.
type Feed struct {
	pool      *Pool
	internals *Internals
	log       zerolog.Logger
	prewarmer AccountPrewarmer

	freshness        time.Duration
	settle           time.Duration
	disableInternals bool

	stats feedCounters

	// topologyMu holds one generation of the destination projection against
	// every concurrent feed. Feeds for different sources run concurrently under
	// the read lock; only Reconcile takes it exclusively.
	topologyMu sync.RWMutex
	// topology is nil until the first exact masterchain projection is published.
	// Immutable topology values are shared with concurrent feeds under
	// topologyMu.
	topology *Topology

	// sizeUnverified latches the one-time warning about states that do not
	// store the out-queue size.
	sizeUnverified atomic.Bool

	processingMu sync.Mutex
	processing   map[ShardIdent]*feedSource

	// pending holds the newest stale block per source; a settled entry (no
	// newer block superseded it) is the actual chain head and gets processed.
	pendingMu sync.Mutex
	pending   map[ShardIdent]pendingFeedHead
}

// NewFeed binds a feed to one pool.
func NewFeed(options FeedOptions) *Feed {
	freshness := options.FreshnessWindow
	if freshness == 0 {
		freshness = defaultFeedFreshnessWindow
	}
	settle := options.HeadSettleDelay
	if settle <= 0 {
		settle = defaultFeedHeadSettleDelay
	}

	return &Feed{
		pool:             options.Pool,
		internals:        options.Pool.Internals(),
		log:              options.Logger,
		prewarmer:        options.Prewarmer,
		freshness:        freshness,
		settle:           settle,
		disableInternals: options.DisableInternals,
		processing:       make(map[ShardIdent]*feedSource),
		pending:          make(map[ShardIdent]pendingFeedHead),
	}
}

// SettleDelay is how long a stale head waits before SweepSettled takes it. The
// caller owns the ticker that drives the sweep.
func (f *Feed) SettleDelay() time.Duration {
	return f.settle
}

// pendingFeedHead is one deferred stale-head candidate.
type pendingFeedHead struct {
	block AppliedBlock
	at    time.Time
}

// feedSource serializes block bookkeeping for one shard chain and remembers its
// high-water mark. A settled stale head can race a fresh apply, but it can never
// run after a newer block has committed its bookkeeping.
//
// The mark is the block identity, not the seqno alone. Two producers deliver
// the same block (acceptance first, the apply pipeline later), and the second
// delivery must be a no-op; a block at the same height with another root hash
// is not the same block and must run, because feedInternals is the only path
// that reseeds a destination away from a same-height fork.
type feedSource struct {
	mu        sync.Mutex
	processed bool
	seqno     uint32
	rootHash  [32]byte
	targets   []feedTarget
}

// feedOrigin names the producer that delivered a block to the feed. It changes
// nothing about the bookkeeping and exists so the two producers can be told
// apart in the counters.
type feedOrigin uint8

const (
	feedOriginApplied feedOrigin = iota
	feedOriginAccepted
)

type feedCounters struct {
	appliedFed, appliedSuperseded   atomic.Uint64
	acceptedFed, acceptedSuperseded atomic.Uint64
	deferred                        atomic.Uint64
}

// FeedStats is a snapshot of the feed counters.
type FeedStats struct {
	// AppliedFed and AcceptedFed count the blocks that advanced a source, by
	// producer. AppliedSuperseded and AcceptedSuperseded count the deliveries
	// that found their source already at or past the block: for a shard this
	// node validates, AppliedSuperseded is the apply pipeline confirming a block
	// acceptance already fed.
	AppliedFed         uint64
	AppliedSuperseded  uint64
	AcceptedFed        uint64
	AcceptedSuperseded uint64
	// Deferred counts the blocks the freshness window held back.
	Deferred uint64
}

// Stats returns a counter snapshot.
func (f *Feed) Stats() FeedStats {
	return FeedStats{
		AppliedFed:         f.stats.appliedFed.Load(),
		AppliedSuperseded:  f.stats.appliedSuperseded.Load(),
		AcceptedFed:        f.stats.acceptedFed.Load(),
		AcceptedSuperseded: f.stats.acceptedSuperseded.Load(),
		Deferred:           f.stats.deferred.Load(),
	}
}

func (c *feedCounters) observed(origin feedOrigin, fed bool) {
	switch {
	case origin == feedOriginAccepted && fed:
		c.acceptedFed.Add(1)
	case origin == feedOriginAccepted:
		c.acceptedSuperseded.Add(1)
	case fed:
		c.appliedFed.Add(1)
	default:
		c.appliedSuperseded.Add(1)
	}
}

type feedAction uint8

const (
	feedApply feedAction = iota + 1
	feedReseed
)

type feedTarget struct {
	destination ShardIdent
	action      feedAction
}

// Observe advances the pool by one applied block. A block older than the
// freshness window is deferred instead: it is remembered as its source's newest
// stale head and processed by SweepSettled only if nothing supersedes it. It
// reports whether the block advanced its source inline; a deferred block and a
// block the source is already at or past both report false.
//
// The block's identity is copied on the deferral path only. Everything else the
// feed reads is either consumed inline or an immutable cell root.
func (f *Feed) Observe(block AppliedBlock) bool {
	return f.observe(block, feedOriginApplied)
}

// ObserveAccepted advances the pool by one block this node accepted in
// consensus, with the state the acceptance computed. It is Observe with another
// producer: the same block reaches Observe from the apply pipeline later, and
// that delivery is then a no-op. The caller runs it synchronously on the
// acceptance path, the way the apply hook runs Observe — a 250-message block
// costs ~0.4 ms on the delta path (BenchmarkHookProcessBlock250); only the first
// block that overtakes the apply stream pays a reseed, because the run is then
// behind by more than one block and ApplyBlock refuses the gap.
func (f *Feed) ObserveAccepted(block AppliedBlock) bool {
	return f.observe(block, feedOriginAccepted)
}

func (f *Feed) observe(block AppliedBlock, origin feedOrigin) bool {
	source := ShardIdent{Workchain: block.ID.Workchain, Shard: uint64(block.ID.Shard)}
	if f.freshness >= 0 && time.Since(time.Unix(int64(block.GenUTime), 0)) > f.freshness {
		deferred := block
		deferred.ID = *block.ID.Copy()

		f.pendingMu.Lock()
		head, exists := f.pending[source]
		if !exists || head.block.ID.SeqNo <= block.ID.SeqNo {
			f.pending[source] = pendingFeedHead{block: deferred, at: time.Now()}
		}
		f.pendingMu.Unlock()
		f.stats.deferred.Add(1)

		return false
	}

	f.pendingMu.Lock()
	if head, exists := f.pending[source]; exists && head.block.ID.SeqNo <= block.ID.SeqNo {
		delete(f.pending, source)
	}
	f.pendingMu.Unlock()

	fed := f.observeSource(source, block)
	f.stats.observed(origin, fed)

	return fed
}

// SweepSettled processes stale blocks that stayed unsuperseded for the settle
// delay — the apply stream quiesced, so they are the actual chain heads. A fresh
// block racing in concurrently is serialized with the stale candidate, and the
// per-source high-water mark rejects the candidate if the fresh block wins.
func (f *Feed) SweepSettled() {
	now := time.Now()
	var settled []AppliedBlock
	f.pendingMu.Lock()
	for source, head := range f.pending {
		if now.Sub(head.at) >= f.settle {
			settled = append(settled, head.block)
			delete(f.pending, source)
		}
	}
	f.pendingMu.Unlock()

	for i := range settled {
		block := settled[i]
		source := ShardIdent{Workchain: block.ID.Workchain, Shard: uint64(block.ID.Shard)}
		if f.observeSource(source, block) {
			f.log.Info().Str("block", storage.FormatBlockRef(block.ID)).
				Msg("stale chain head settled, arming the pool from it")
		}
	}
}

// observeSource applies one block under its source-chain serializer. The
// high-water check is inside the same critical section as the bookkeeping, so a
// stale head selected before a fresh apply cannot later reseed or drop the
// source behind that fresh block, and the second producer of a block already fed
// cannot re-run its delta.
//
// Only a block below the mark, or the very block at the mark, is refused. A
// same-height block with another root hash runs: acceptance and apply cannot
// disagree on a finalized block, but a preloaded or restored view can name
// another candidate at that height, and feedInternals reseeding the destination
// from the delivered state is the only way back onto the chain.
func (f *Feed) observeSource(source ShardIdent, block AppliedBlock) bool {
	tracked := f.source(source)
	tracked.mu.Lock()
	defer tracked.mu.Unlock()

	seqno := block.ID.SeqNo
	if tracked.processed && (seqno < tracked.seqno ||
		seqno == tracked.seqno && bytes.Equal(block.ID.RootHash, tracked.rootHash[:])) {
		return false
	}

	f.eraseApplied(block)

	if !f.disableInternals {
		f.feedInternals(tracked, source, block)
	}

	tracked.processed = true
	tracked.seqno = seqno
	copy(tracked.rootHash[:], block.ID.RootHash)

	return true
}

// eraseApplied drops the externals the block imported from the pool. It is
// idempotent, so the deferral path and a later sweep of the same block may both
// run it.
func (f *Feed) eraseApplied(block AppliedBlock) {
	hashes, err := AppliedNormHashesFromBlockRoot(block.BlockRoot)
	if err != nil {
		f.log.Warn().Err(err).Str("block", storage.FormatBlockRef(block.ID)).
			Msg("cannot extract applied externals from block")

		return
	}
	if len(hashes) == 0 {
		return
	}

	f.pool.EraseApplied(hashes)
	f.log.Debug().Str("block", storage.FormatBlockRef(block.ID)).
		Int("normalized_hashes", len(hashes)).Msg("cleaned applied externals")
}

func (f *Feed) source(source ShardIdent) *feedSource {
	f.processingMu.Lock()
	defer f.processingMu.Unlock()

	tracked := f.processing[source]
	if tracked == nil {
		tracked = &feedSource{}
		f.processing[source] = tracked
	}

	return tracked
}

// Reconcile publishes one masterchain projection as the destination topology.
// It is exclusive against every concurrent feed, so a block feed and a topology
// transition observe one generation together.
func (f *Feed) Reconcile(topology Topology) error {
	if f.disableInternals {
		return nil
	}

	f.topologyMu.Lock()
	defer f.topologyMu.Unlock()

	// The projection is a pure function of the shard configuration, which only
	// moves on a split, a merge or a session rotation, while this runs
	// synchronously on every masterchain apply. Re-publishing an identical
	// destination set and re-running a pure-predicate prune over every source of
	// every destination costs the apply path a full sweep for nothing.
	//
	// A source seeded by a collation between two sweeps is not pruned until the
	// projection actually changes. That is memory-only and self-healing: a cut
	// serves only the sources its request names, so an unreferenced run can
	// never reach a candidate.
	if f.topology != nil && f.topology.Equal(topology) {
		return nil
	}
	if err := topology.Reconcile(f.internals); err != nil {
		return err
	}
	f.topology = &topology

	return nil
}

// feedInternals advances the internal-message section by the applied block.
// The synchronous per-chain apply order makes the feed gap-free by
// construction, so a tracked source always continues with exactly the next
// block: the fast path applies its out-queue delta and verifies the tracked
// queue size against the size the post-state stores. Everything else (first
// sight of the source, a malformed delta, or a size mismatch) reseeds the run
// from the post-state — the single recovery path.
func (f *Feed) feedInternals(tracked *feedSource, source ShardIdent, block AppliedBlock) {
	f.topologyMu.RLock()
	defer f.topologyMu.RUnlock()

	id := block.ID
	topology := f.topology
	if topology != nil && !topology.ContainsSource(source) {
		return
	}

	ref := SourceRef{Seqno: id.SeqNo}
	copy(ref.RootHash[:], id.RootHash)

	targets := tracked.targets[:0]
	defer func() {
		clear(targets)
		if cap(targets) > maxRetainedFeedTargets {
			tracked.targets = nil
		} else {
			tracked.targets = targets[:0]
		}
	}()

	applyCount := 0
	reseedCount := 0
	var prewarmed feedPrewarmSeen
	if f.prewarmer != nil {
		prewarmed.accounts = make(map[feedPrewarmAccount]struct{})
		prewarmed.envelopes = make(map[cell.Hash]struct{})
	}
	f.internals.VisitDestinations(func(destination ShardIdent) bool {
		if !sharddomain.IsNeighbor(
			destination.Workchain,
			int64(destination.Shard),
			source.Workchain,
			int64(source.Shard),
		) {
			return true
		}

		top, err := f.internals.SourceTop(destination, source)
		switch {
		case err == nil && id.SeqNo > top.Seqno:
			targets = append(targets, feedTarget{destination: destination, action: feedApply})
			applyCount++
		case err == nil && id.SeqNo == top.Seqno && ref.RootHash != top.RootHash:
			// A preloaded or restored view may name another same-height
			// candidate. The applied state is authoritative; never accept the
			// seqno alone as an idempotency key.
			targets = append(targets, feedTarget{destination: destination, action: feedReseed})
			reseedCount++
		case errors.Is(err, ErrNotFound):
			targets = append(targets, feedTarget{destination: destination, action: feedReseed})
			reseedCount++
		case err != nil:
			f.log.Error().Err(err).Str("block", storage.FormatBlockRef(id)).
				Int32("destination_workchain", destination.Workchain).
				Uint64("destination_shard", destination.Shard).
				Msg("internals source position unavailable")
		}

		return true
	})
	if applyCount == 0 && reseedCount == 0 {
		return
	}

	var expected *uint64
	var sizeErr error
	if block.StateRoot != nil {
		queueSize, err := QueueSizeFromStateRoot(block.StateRoot)
		switch {
		case err == nil:
			expected = &queueSize
		case errors.Is(err, ErrQueueSizeNotStored):
			if f.sizeUnverified.CompareAndSwap(false, true) {
				f.log.Warn().Err(err).Str("block", storage.FormatBlockRef(id)).
					Msg("state stores no out-queue size, internals size invariant is unverified")
			}
		default:
			sizeErr = fmt.Errorf("read state out-queue size: %w", err)
		}
	}

	// applyCount is not maintained past this gate: only reseedCount decides
	// the recovery path below.
	//
	// The merge below advances one cursor over targets while walking routed,
	// which is correct only because both are derived from the same immutable
	// internalsSnapshot.destinations and are therefore in the order
	// Internals.ReconcileDestinations imposes with CompareShardIdent.
	// The same holds for the seeds merge further down.
	var downgrade error
	var downgradeCause string
	if applyCount > 0 && sizeErr == nil {
		routed, err := f.internals.DeltasFromBlockRoot(source, ref, block.BlockRoot, block.StartLT)
		if err == nil {
			targetIndex := 0
			for index := range routed {
				destination := routed[index].Destination
				for targetIndex < len(targets) && CompareShardIdent(targets[targetIndex].destination, destination) < 0 {
					targetIndex++
				}
				if targetIndex == len(targets) || targets[targetIndex].destination != destination ||
					targets[targetIndex].action != feedApply {
					continue
				}
				routed[index].Delta.ExpectedQueueSize = expected
				if applyErr := f.internals.ApplyBlock(destination, source, ref, routed[index].Delta); applyErr != nil {
					targets[targetIndex].action = feedReseed
					reseedCount++
					f.log.Warn().Err(applyErr).Str("block", storage.FormatBlockRef(id)).
						Int32("destination_workchain", destination.Workchain).
						Uint64("destination_shard", destination.Shard).
						Msg("internals delta not applied, reseeding destination")
				} else {
					f.prewarmPooled(routed[index].Delta.Added, &prewarmed)
				}
			}
		} else {
			downgrade, downgradeCause = err, "internals delta parse failed, reseeding destinations"
		}
	} else if sizeErr != nil {
		downgrade, downgradeCause = sizeErr, "internals size verification failed, reseeding destinations"
	}
	if downgrade != nil {
		// A block-wide failure says nothing about individual destinations, so
		// every apply target falls back to the single recovery path.
		f.log.Warn().Err(downgrade).Str("block", storage.FormatBlockRef(id)).Msg(downgradeCause)
		for index := range targets {
			if targets[index].action == feedApply {
				targets[index].action = feedReseed
				reseedCount++
			}
		}
	}
	if reseedCount == 0 {
		return
	}

	if block.StateRoot == nil {
		f.dropReseedTargets(targets, source)
		f.log.Warn().Str("block", storage.FormatBlockRef(id)).
			Int("destinations", reseedCount).
			Msg("applied block carries no state, internals sources dropped")

		return
	}

	seeds, total, err := f.internals.SeedsFromStateRoot(source, ref, block.StateRoot)
	if err != nil {
		f.dropReseedTargets(targets, source)
		f.log.Warn().Err(err).Str("block", storage.FormatBlockRef(id)).
			Int("destinations", reseedCount).
			Msg("internals reseed failed, sources dropped")

		return
	}
	seeded := 0
	targetIndex := 0
	for index := range seeds {
		destination := seeds[index].Destination
		for targetIndex < len(targets) && CompareShardIdent(targets[targetIndex].destination, destination) < 0 {
			targetIndex++
		}
		if targetIndex == len(targets) || targets[targetIndex].destination != destination ||
			targets[targetIndex].action != feedReseed {
			continue
		}
		if seedErr := f.internals.Seed(destination, source, ref, seeds[index].Messages, total); seedErr != nil {
			_ = f.internals.DropSource(destination, source)
			f.log.Warn().Err(seedErr).Str("block", storage.FormatBlockRef(id)).
				Int32("destination_workchain", destination.Workchain).
				Uint64("destination_shard", destination.Shard).
				Msg("internals destination reseed failed")

			continue
		}
		f.prewarmPooled(seeds[index].Messages, &prewarmed)
		seeded++
	}
	f.log.Debug().Str("block", storage.FormatBlockRef(id)).
		Int("destinations", seeded).
		Msg("internals runs seeded")
}

// dropReseedTargets untracks the source at every destination that asked for a
// reseed. It is the terminal step of the recovery path: a run that cannot be
// reseeded must not stay behind at a position the applied state contradicts.
func (f *Feed) dropReseedTargets(targets []feedTarget, source ShardIdent) {
	for index := range targets {
		if targets[index].action == feedReseed {
			_ = f.internals.DropSource(targets[index].destination, source)
		}
	}
}

type feedPrewarmAccount struct {
	workchain int32
	account   [32]byte
}

type feedPrewarmSeen struct {
	accounts  map[feedPrewarmAccount]struct{}
	envelopes map[cell.Hash]struct{}
}

// prewarmPooled schedules full destination-account and exact message envelope
// record warming after messages enter a committed run. seen spans every routed
// destination of one source update, because overlapping shard views may share
// messages.
func (f *Feed) prewarmPooled(messages []*InternalMessage, seen *feedPrewarmSeen) {
	if f.prewarmer == nil {
		return
	}

	for _, message := range messages {
		if !message.DestinationPrewarmable {
			continue
		}
		key := feedPrewarmAccount{
			workchain: message.DestinationWorkchain,
			account:   message.DestinationAccount,
		}
		if _, exists := seen.accounts[key]; exists {
			continue
		}

		seen.accounts[key] = struct{}{}
		f.prewarmer.EnqueueAccount(key.workchain, key.account)
	}

	for _, message := range messages {
		root := cell.Hash(message.EnvHash)
		if _, exists := seen.envelopes[root]; exists {
			continue
		}

		seen.envelopes[root] = struct{}{}
		f.prewarmer.EnqueueRoot(root)
	}
}
