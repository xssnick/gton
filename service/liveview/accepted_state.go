package liveview

import (
	"errors"
	"fmt"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

// DefaultAcceptedStateCache bounds the blocks this node has finalized itself
// and published here before anything committed them.
//
// It is sized on the only thing that consumes the publication: the distance
// between a shard block this node finalized and the applied current state that
// supersedes it, which is the shard client's lag. Measured out of gap on the
// three-node network that motivated this, that lag is p50 3 s / p90 6 s — 15 to
// 30 blocks at a 200 ms slot — so 128 is roughly four times the p90 and still a
// fixed, small number of shard states.
//
// It is deliberately not "unlimited even if you ask". A publication here is by
// construction not evictable by the ordinary block cache (liveBlockEvictableLocked
// refuses an unflushed block, and protectedLiveBlocksLocked pins this one on
// top of that), so an unbounded accepted set is an unbounded pin — precisely the
// shape a standstill would grow without limit. Zero and negative both mean "use
// this default"; there is no way to ask for no bound at all.
const DefaultAcceptedStateCache = 128

var errAcceptedStateIncomplete = errors.New("liveview: accepted block state is incomplete")

func acceptedStateCacheSize(configured int) int {
	if configured <= 0 {
		return DefaultAcceptedStateCache
	}

	return configured
}

// PublishAcceptedBlockState makes a block this node has itself finalized, and
// the state it computed for that block, readable BEFORE either of them is
// committed to the node database.
//
// This is the "extend the live view" half of the read rule. Collation and
// validation read state only through this store, and the store could not answer
// for a locally finalized shard block at all: the applied states it holds are
// published by the shard client, which only gets there once a masterchain block
// carrying that shard top is applied. Until then backingSeqnoLookupAllowedLocked
// refuses even to ask the backing (the block is ahead of the applied shard top),
// so no amount of waiting for a commit could have helped — the state has to be
// published, and this is the entry point that publishes it.
//
// WHAT MAY BE PUBLISHED, and the restriction is the important part. Only a state
// this node produced by applying the block's own update to a FULL parent it
// holds — a ChainState root. A state that is merely sufficient to VALIDATE the
// block is not sufficient to be the next block's applied parent: on the
// full-collated shard path the verifier runs against predecessors rebuilt from
// the candidate's own collated proof, and that root is narrow (pruned where the
// block never read). Such a root has the same hash as the full one, so a hash
// check cannot tell them apart. The producer side is what keeps them out — the
// collator's successor is never released except over the very trees it was built
// on — and the check below is the cheap structural half of the same rule: a
// proof root, or a pruned branch, is refused here.
//
// LIFETIME. Until the applied current state reaches or passes the block
// (cleanupAcceptedStatesLocked), bounded by DefaultAcceptedStateCache
// publications (trimAcceptedStatesLocked). It is NOT gated on the block or the
// state having been flushed: waiting for the flush is the cost this exists to
// remove. Dropping it takes the live block with it, because nothing else is
// holding an uncommitted block.
//
// RESTART. Nothing here is durable, and nothing here needs to be — but the reason
// is the quorum certificate rather than any local write. A block reaching this
// entry point has been notarized, which required 2/3 of weight to vote for it, and
// each of those votes was gated on that validator having durably stored the
// candidate. So the block is held by at least 2/3 of the network and is
// recoverable from it whatever this process has managed to write, and a restart
// that loses this publication returns exactly to the pre-existing behaviour: the
// state reappears when the shard client applies the block. Records of this node's
// own consensus commitments are NOT published here and stay durable-before-use;
// this carries a block payload and a state, which are ordinary recoverable data.
//
// Masterchain blocks are refused. The masterchain never stalled in the measured
// incident, its applied state follows within a block or two, and its replay
// ordering is a deliberate parity with the reference; publishing an uncommitted
// masterchain state would also reach the masterchain state cache, which is a
// different blast radius for no measured gain.
func (s *Store) PublishAcceptedBlockState(artifacts storage.LiveBlockArtifacts) error {
	if err := validateAcceptedBlockState(artifacts); err != nil {
		return err
	}
	// The block view — state-root proof, accounts-dict prewarm — is deliberately
	// NOT built here. This runs on the consensus acceptance path; none of the
	// readers the publication exists for use fragments (a chain tip needs the
	// state, the block root and the BOC, and the collator's predecessor read needs
	// the first two); and BlockFragments rebuilds a view on demand out of exactly
	// the reads this publication makes answerable. So the publication is the cheap
	// half only, and nothing is lost.
	artifacts.AvailabilityOnly = true

	if err := s.publishLiveBlockArtifacts(artifacts, true); err != nil {
		return err
	}
	s.notifyAcceptedBlockState(artifacts)

	return nil
}

// acceptedStateObserver is one ObserveAcceptedBlockStates registration.
type acceptedStateObserver struct {
	id      uint64
	observe func(storage.LiveBlockArtifacts)
}

// ObserveAcceptedBlockStates registers observe to run for every publication
// PublishAcceptedBlockState installs, with the artifacts as published: the
// block root, the block BOC, the block metadata and the resident state cell.
// The returned function removes the registration.
//
// It exists for the validator's internal-message pool. The pool mirrors the
// out-queues of neighbour shards and used to advance only on the node's
// applied-block hook, which under load runs about a second after this node has
// finalized the block itself (apply lag p50 ~1 s on the stand). The state this
// call publishes is the same resident tree the pool needs to read that queue
// from, so the publication is the earliest point at which the pool can move,
// and it is the point at which the C++ collator sees the neighbour state too
// (a validated block's state, not a committed one).
//
// The observer runs synchronously on the publishing goroutine, strictly after the
// publication is installed and outside every store lock: an observer that reads
// the block or its state back from this store during the call is served the
// publication. A failed publication notifies nobody, so an observer never learns
// of a block the store itself cannot answer for.
func (s *Store) ObserveAcceptedBlockStates(observe func(storage.LiveBlockArtifacts)) func() {
	s.acceptedObserversMu.Lock()
	s.acceptedObserverNext++
	id := s.acceptedObserverNext
	s.acceptedObservers = append(s.acceptedObservers, acceptedStateObserver{id: id, observe: observe})
	s.acceptedObserversMu.Unlock()

	return func() {
		s.acceptedObserversMu.Lock()
		defer s.acceptedObserversMu.Unlock()

		for index := range s.acceptedObservers {
			if s.acceptedObservers[index].id != id {
				continue
			}
			s.acceptedObservers = append(s.acceptedObservers[:index], s.acceptedObservers[index+1:]...)

			return
		}
	}
}

// notifyAcceptedBlockState runs every registered observer over one installed
// publication. The registration list is copied under its lock and the observers
// run outside it, so an observer may unregister itself or another from inside
// the call.
func (s *Store) notifyAcceptedBlockState(artifacts storage.LiveBlockArtifacts) {
	s.acceptedObserversMu.Lock()
	if len(s.acceptedObservers) == 0 {
		s.acceptedObserversMu.Unlock()

		return
	}
	observers := append([]acceptedStateObserver(nil), s.acceptedObservers...)
	s.acceptedObserversMu.Unlock()

	for index := range observers {
		observers[index].observe(artifacts)
	}
}

func validateAcceptedBlockState(artifacts storage.LiveBlockArtifacts) error {
	block := artifacts.Block
	if !blockproof.IsFullBlockID(block) {
		return fmt.Errorf("%w: block id is incomplete", errAcceptedStateIncomplete)
	}
	if block.SeqNo == 0 {
		return fmt.Errorf("%w: a zerostate is not an accepted block", errAcceptedStateIncomplete)
	}
	if block.Workchain == masterchainID && block.Shard == masterchainShard {
		return fmt.Errorf("%w: masterchain blocks are not published ahead of their apply", errAcceptedStateIncomplete)
	}
	if artifacts.State == nil || artifacts.State.Cell == nil {
		return fmt.Errorf("%w: state cells are required", errAcceptedStateIncomplete)
	}
	if !blockIDEqual(artifacts.State.Block, block) {
		return fmt.Errorf("%w: state belongs to another block", errAcceptedStateIncomplete)
	}
	// The structural half of the full-state restriction documented on
	// PublishAcceptedBlockState. Both halves are needed and neither implies the
	// other: a pruned branch carries a level, while a Merkle proof over a level-0
	// tree is itself level 0 and is caught only by the special-cell check. It
	// still does not catch narrowness — nothing local can — which is why the
	// producer is the real guarantee.
	if artifacts.State.Cell.Level() != 0 || artifacts.State.Cell.IsSpecial() {
		return fmt.Errorf("%w: state root is not an ordinary level-0 cell", errAcceptedStateIncomplete)
	}
	if artifacts.Root == nil && len(artifacts.BlockData) == 0 {
		return fmt.Errorf("%w: block root or block data is required", errAcceptedStateIncomplete)
	}
	return nil
}

// BlockArtifactsSignal returns the channel closed by the next block-artifact
// publication of any kind, including a flush transition.
//
// It exists so a reader that needs a specific block can wait for an EDGE
// instead of polling. The contract is the one WaitBlockArtifacts already relies
// on: every signal is raised under s.mu by the same critical section that
// installs the artifacts, so a caller that takes the channel BEFORE its read and
// blocks on it only after the read failed cannot lose a wake-up. The signal
// carries no information about which block arrived, so the caller's own read
// remains the predicate — which is what makes it safe for readers whose
// requirement differs from WaitBlockArtifacts' (loadChainTip also needs the
// block BOC, for instance).
func (s *Store) BlockArtifactsSignal() <-chan struct{} {
	s.mu.RLock()
	notify := s.blockArtifacts
	s.mu.RUnlock()

	return notify
}

// rememberAcceptedStateLocked enrols one publication in the accepted-state
// bound. A block the applied state has already reached is not enrolled: it needs
// no protection and no release.
func (s *Store) rememberAcceptedStateLocked(block ton.BlockIDExt) {
	if s.coveredByCurrentStateLocked(block) {
		return
	}

	key := storage.BlockKey(block)
	if _, ok := s.acceptedStates[key]; !ok {
		s.acceptedStates[key] = cloneBlockID(block)
		s.acceptedOrder = append(s.acceptedOrder, key)
	}
	s.trimAcceptedStatesLocked()
}

// trimAcceptedStatesLocked releases the oldest publications over the bound.
// Oldest by publication order, which for one shard is oldest by seqno.
func (s *Store) trimAcceptedStatesLocked() {
	limit := s.acceptedCacheSize
	if limit <= 0 {
		limit = DefaultAcceptedStateCache
	}
	for len(s.acceptedOrder) > limit {
		before := len(s.acceptedOrder)
		s.dropAcceptedStateLocked(s.acceptedOrder[0])
		if len(s.acceptedOrder) == before {
			// Unreachable while the order and the map are maintained together, and
			// a guard rather than a comment because the alternative to noticing is
			// spinning under the store lock.
			return
		}
	}
}

// cleanupAcceptedStatesLocked releases every publication the applied current
// state has reached or passed. That is the ordinary end of an accepted state's
// life: the sync pipeline has applied the block and published its own copy, so
// this one is no longer the only one.
func (s *Store) cleanupAcceptedStatesLocked() {
	if len(s.acceptedStates) == 0 {
		return
	}

	for _, key := range append([]storage.BlockRootHash(nil), s.acceptedOrder...) {
		block, ok := s.acceptedStates[key]
		if !ok || !s.coveredByCurrentStateLocked(block) {
			continue
		}
		s.dropAcceptedStateLocked(key)
	}
}

// dropAcceptedStateLocked releases one accepted publication, and the live block
// with it — but ONLY the live block this publication is still the owner of.
//
// Taking the live block along is the point where this publication is the only
// producer that ever published the block: an uncommitted block is never
// evictable by the ordinary cache limit, so leaving it behind after its accepted
// entry is gone would leak it forever.
//
// OWNERSHIP is what bounds that, and it is not the same question as "does the
// current state refer to this block". One masterchain block routinely carries a
// shard top several seqnos ahead, so the applied current state moves from below
// this block to above it without ever naming it: coveredByCurrentStateLocked is
// then true while currentRefersToBlockLocked is false. If the sync pipeline had
// published its own copy of the block in the meantime — which is exactly what
// applying it does — the entry in s.blocks is the pipeline's, merged over ours,
// and unflushed until the next checkpoint. Deleting it there destroyed a
// publication this bookkeeping never made, and until the flush the store could
// not answer for the block either: BlockState and LoadStateCellTree returned
// not-found where they answered a moment earlier. That is a hole for the
// liteserver and httpapi readers, and for a validator needing a parent behind
// the applied top it turned a 1 Hz poll into a wait bounded by the 30 s chain-tip
// backstop. acceptedOwner is cleared by any other producer's publication, so what
// is dropped here is only ever what was published here.
func (s *Store) dropAcceptedStateLocked(key storage.BlockRootHash) {
	block, ok := s.acceptedStates[key]
	delete(s.acceptedStates, key)
	for i := range s.acceptedOrder {
		if s.acceptedOrder[i] != key {
			continue
		}
		s.acceptedOrder = append(s.acceptedOrder[:i], s.acceptedOrder[i+1:]...)
		break
	}
	if !ok || s.currentRefersToBlockLocked(block) {
		return
	}
	cached := s.blocks[key]
	if cached == nil || !blockIDEqual(cached.id, block) || !cached.acceptedOwner {
		return
	}
	// A block the store has since committed is left to the ordinary cache limit,
	// which is what governs every other committed block. Only the uncommitted one
	// — the entry nothing else would ever release — goes with its publication.
	if s.liveBlockEvictableLocked(cached) {
		return
	}
	s.deleteLiveBlockLocked(key, cached, liveBlockKind(cached.id))
}

// acceptedStateWinsLocked reports that the state already published for one block
// must survive the publication being installed.
//
// WHICH PUBLICATION WINS, and why it is the accepted one. Three producers can
// publish a state for the same shard block: this node's own acceptance, which
// carries the full tree the validator computed and signed; the non-final ingest,
// which REBUILDS the tree from cell records behind a lazy loader; and the apply
// pipeline, which carries the committed tree. All three have the same root hash —
// a block's state is fixed by its Merkle update — so nothing about content is at
// stake. What is at stake is IDENTITY: chain_state.go compares tip states BY
// POINTER for the live-successor carry-back, so a reader that gets a second
// materialization of a parent silently pays a full re-apply per candidate, with
// no error and no metric. The accepted publication is the one materialization the
// producer is holding anyway, it is already fully resident where a rebuild is
// lazily backed, and it is the only one that exists at all while the block is
// ahead of the applied shard top. So it wins for as long as it is enrolled.
//
// It wins only against OTHER producers: an accepted publication installs its own
// state, which is what makes a retry, or a second acceptance of the same block,
// idempotent rather than a no-op.
//
// Once the publication is released — the applied current state reached the block,
// or the bound trimmed it — this returns false and the ordinary producers install
// their own state exactly as before. Nothing here outlives the enrolment.
//
// The rule is on the PUBLICATION path only. rememberCurrentBlockStatesLocked, the
// other writer of s.states, installs the states of the blocks the applied current
// state names — and a block the current state names is a block it has reached, so
// its accepted entry is released in the same critical section. Guarding that
// writer too would leave the store holding one materialization in s.states and
// another in the current snapshot, which is a divergence with no reader that
// wants it.
func (s *Store) acceptedStateWinsLocked(
	key storage.BlockRootHash,
	stateKey liveBlockLookupKey,
	acceptedPublication bool,
) bool {
	if acceptedPublication {
		return false
	}
	if _, enrolled := s.acceptedStates[key]; !enrolled {
		return false
	}
	_, published := s.states[stateKey]

	return published
}

// AcceptedStateBlocks reports the blocks currently published ahead of their
// commit, newest last. It exists for tests and diagnostics; the read path never
// consults it.
func (s *Store) AcceptedStateBlocks() []ton.BlockIDExt {
	s.mu.RLock()
	defer s.mu.RUnlock()

	blocks := make([]ton.BlockIDExt, 0, len(s.acceptedOrder))
	for _, key := range s.acceptedOrder {
		if block, ok := s.acceptedStates[key]; ok {
			blocks = append(blocks, cloneBlockID(block))
		}
	}

	return blocks
}
