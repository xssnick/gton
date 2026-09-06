package collator

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// successorPayload is what a collation knows about the block it has just
// finished, at the instant its record goes inert and before anything is
// serialized. It is everything the next slot's build reads out of a committed
// predecessor, handed over by value rather than resolved back out of the
// session — the resolution path exists for a predecessor that is already
// installed, and this one is not.
//
// Every field is final at the handoff: the state and its update come out of
// buildStateAndBlockParts, the queue size was last moved by flushQueueDeletes,
// the storage stats and the external feedback by the execution phases, and the
// file hash is taken on the branch that produced the block bytes.
type successorPayload struct {
	// ID is complete, FileHash included: the block bytes exist by then.
	ID           ton.BlockIDExt
	Root         *cell.Cell
	State        *cell.Cell
	StateUpdate  *cell.Cell
	StartLT      uint64
	GenUTime     uint32
	OutQueueSize uint64
	StorageStats AccountStorageStats
	Externals    []msgpool.ExternalFeedback
}

// successorPort is the collation's one-way door to whoever wants the next slot
// started early. It is installed by the acquisition layer and left nil
// everywhere else, which is what keeps every deterministic entry point — the
// benchmarks, the golden fixtures, the verifier — on the sequential path.
type successorPort struct {
	offer func(SuccessorOffer)
	token successorToken
	// revoke tells the producer to stop the successor this port handed it.
	// Candidate nodes installed while building that successor deliberately stay
	// in the session branch until its next Retain boundary. Dropping one here by
	// block root would be ABA-unsafe: a deterministic retry can already have
	// reused the same root by the time this asynchronous revoke finishes.
	revoke func(*successorToken, [32]byte, PipelineHandoffOutcome)
	// chain is what the acquisition layer resolved for the block being built.
	// The successor's own chain is derived from it at the handoff rather than
	// re-resolved, because the state it would resolve against is not installed
	// yet.
	previous     []PreviousBlock
	queueBase    []PreviousBlock
	candidateTip *[32]byte
	policy       CandidateState
	exclude      [][32]byte
	slot         uint32

	// mu guards offered, which is written on the block-BOC branch and read by
	// whichever goroutine discovers that this block will not be the one
	// committed. Exactly one revoke takes effect.
	mu        sync.Mutex
	offered   *[32]byte
	offeredAt time.Time
	endedAt   time.Time
	// adopted is set by the producer when it actually starts a successor from
	// this offer. Without it the overlap below would count a handoff the
	// producer declined — the last slot of a window declines every time — as if
	// a successor had run beside the whole tail, and the series would report a
	// saving on slots that saved nothing.
	adopted atomic.Bool
}

// overlap is how long the successor ran beside the rest of this block: from the
// handoff to whichever came first, the end of this build or the withdrawal of
// the offer. It is the quantity the whole change exists to produce, and it is
// reported once per handoff rather than once per production — a production that
// handed nothing over has no overlap, and one whose successor was withdrawn
// early has a shorter one than the build it belonged to.
func (p *successorPort) overlap(buildEnd time.Time) (time.Duration, bool) {
	if p == nil {
		return 0, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.offeredAt.IsZero() || !p.adopted.Load() {
		return 0, false
	}
	end := buildEnd
	if !p.endedAt.IsZero() && p.endedAt.Before(end) {
		end = p.endedAt
	}
	if end.Before(p.offeredAt) {
		return 0, true
	}

	return end.Sub(p.offeredAt), true
}

// noteAdopted is the producer saying it started a successor from this offer.
// Only then is there an overlap to measure.
func (p *successorPort) noteAdopted() {
	if p == nil {
		return
	}
	p.adopted.Store(true)
}

// noteOffered records the predecessor root this port handed over, so a later
// revoke names the same block.
func (p *successorPort) noteOffered(root [32]byte) {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.offered = &root
	p.offeredAt = time.Now()
	p.mu.Unlock()
}

// revokeOffered withdraws whatever this port handed over, once. It is called
// from every path that learns the block it offered will not be the block
// committed for its slot: a size-limit rebuild, which changes the root, and any
// failure after the handoff.
//
// The producer's adoption check already refuses a successor built on a root it
// did not commit, so this is not what makes the pipeline safe. What it does is
// stop the doomed successor immediately instead of leaving it running until the
// producer reaches its slot, which on a retry is precisely when the CPU is worth
// most.
//
// The withdrawal runs on a goroutine of its own, because the caller is the
// predecessor's own collation on the first line of a size-limit rebuild — the
// slot's last chance — and the revoke joins a build that may be inside a TVM
// execution or a store read. The session's Retain transition later removes
// abandoned speculative nodes together with every fork that did not become the
// selected lineage.
//
// Nothing waits for it. A producer that reaches the slot before the withdrawal
// does is already covered by the adoption check, which refuses a successor whose
// predecessor root is not the one this build went on to commit — and a rebuild
// moves that root by definition.
func (p *successorPort) revokeOffered(outcome PipelineHandoffOutcome) {
	if p == nil {
		return
	}
	p.mu.Lock()
	root := p.offered
	p.offered = nil
	if root != nil {
		p.endedAt = time.Now()
	}
	p.mu.Unlock()
	if root == nil {
		return
	}
	revoke, token, revoked := p.revoke, &p.token, *root
	go func() {
		if revoke != nil {
			revoke(token, revoked, outcome)
		}
	}()
}

// successorToken identifies one concrete handoff attempt. The block root alone
// cannot do that: retries deliberately reuse their slot seed and can reproduce
// the same block byte-for-byte while an older asynchronous revoke is still
// unwinding. The pointed-to byte keeps distinct live tokens at distinct
// addresses (unlike pointers to zero-sized values).
type successorToken struct {
	_ byte
}

// SuccessorOffer is one predecessor handed to the producer before it exists as
// a committed candidate. The producer either adopts it — starting the next
// slot's build against it — or drops it, and the choice is made against Root:
// a size-limit retry rebuilds the block and moves that hash, so an offer whose
// root no longer matches the predecessor the producer went on to commit is not
// merely stale, it describes a state that never existed.
type SuccessorOffer struct {
	successorPayload
	token *successorToken

	// adopt is how the producer tells the port a successor really started, which
	// is the difference between an overlap and a declined offer.
	adopt func()

	// QueueBase, CandidateTip and Previous are the queue lineage the successor
	// builds on, derived from the predecessor's own resolved chain by the same
	// rule recordCandidateLocked applies when it installs a candidate.
	QueueBase    []PreviousBlock
	CandidateTip *[32]byte
	Previous     []PreviousBlock

	// Policy is the empty-block decision for the successor's slot, taken from
	// the state this block produced. It replaces the ResolveCandidateState the
	// producer would otherwise run, which would have to take the session lock
	// in front of the commit that is about to need it.
	Policy CandidateState

	// Exclude are the external messages this block consumed. The successor opens
	// its stream before that consumption is reported to the pool, so it has to
	// be told directly or it re-executes them.
	Exclude [][32]byte

	predecessorSlot uint32
	handoffAt       time.Time
}

// successorSlot is where a handed-over successor waits for the producer to
// reach the slot it belongs to. One mutex-guarded cell rather than a channel:
// the producer has several places it can arrive from — the ordinary wait, the
// unbounded wait, the restored-slot branch, the empty-slot branch — and a
// channel would need a receive case in every one of them, which is exactly the
// shape that lost offers in review.
type successorSlot struct {
	mu     sync.Mutex
	future *candidateBuildFuture
	root   [32]byte
	token  *successorToken
	// handoff is where the parked build's own successor offer waits, for a slot
	// whose build outlives the producer that started it. The in-window slot
	// leaves it nil: there the producer is still running and takes the offer
	// directly.
	handoff *speculativeHandoff
	closed  bool
}

// install parks a started future. It reports false when the slot is closed or
// already occupied, and the caller must then stop what it started.
func (s *successorSlot) install(
	future *candidateBuildFuture,
	root [32]byte,
	token *successorToken,
	handoff *speculativeHandoff,
) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.future != nil {
		return false
	}
	s.future = future
	s.root = root
	s.token = token
	s.handoff = handoff

	return true
}

// takeIf removes what is parked only if it was started by the named handoff
// attempt on the named predecessor. Both are required: a deterministic retry
// can reproduce the same root while the old attempt's revoke is still in
// flight, and that revoke must not take down the replacement.
func (s *successorSlot) takeIf(token *successorToken, root [32]byte) *candidateBuildFuture {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.future == nil || s.token != token || s.root != root {
		return nil
	}
	future := s.future
	s.future, s.root, s.token, s.handoff = nil, [32]byte{}, nil, nil

	return future
}

// take removes whatever is parked, with the predecessor root it was started on
// so the caller can decide whether it still describes reality.
func (s *successorSlot) take() (*candidateBuildFuture, [32]byte, *speculativeHandoff) {
	s.mu.Lock()
	defer s.mu.Unlock()
	future, root, handoff := s.future, s.root, s.handoff
	s.future, s.root, s.token, s.handoff = nil, [32]byte{}, nil, nil

	return future, root, handoff
}

// close stops any resident future and refuses every later install. The producer
// defers it, so a window that ends anywhere — a lost slot, a failed emission, a
// cancelled context — takes its speculative successor down with it.
func (s *successorSlot) close() {
	if future := s.closeCancelled(); future != nil {
		<-future.done
	}
}

// closeCancelled is close without the join: it refuses every later install and
// cancels whatever was parked, handing the future back so the caller can wait
// for it after cancelling everything else it owns. Closing has to be synchronous
// — a slot that refuses installs only once some goroutine gets around to it
// would let a successor be parked after its window ended — but the waiting does
// not.
func (s *successorSlot) closeCancelled() *candidateBuildFuture {
	s.mu.Lock()
	future := s.future
	s.future, s.token, s.handoff, s.closed = nil, nil, nil, true
	s.mu.Unlock()
	if future != nil {
		future.cancel()
	}

	return future
}
