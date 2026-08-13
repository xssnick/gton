package msgpool

import (
	"bytes"
	"cmp"
	"container/heap"
	"errors"
	"fmt"
	"sync"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// The internals section is a derived, pre-ordered view of the inbound
// internal message stream: for every source chain (a neighbor shard or the
// owner shard itself) it mirrors the subset of the source out-queue routed
// into the owner shard. The view advances by per-block deltas, so at
// collation time the input is already parsed and (lt, hash)-ordered and Cut
// only merges ready runs.
//
// The view is exact by construction: a seed reads the queue straight from a
// state, and every delta is the consensus-validated queue transformation of
// one finalized block, so the run equals the state walk at every position.
// The full-queue-size counter (checked against the size the state stores)
// guards the construction against parsing bugs, and rebuild-equivalence
// tests (seed(prev)+delta == seed(cur)) pin it down in CI. There is no
// second serving path: a violated invariant untracks the source and the
// feeder reseeds it from the block post-state.

// SourceRef pins one position of a source chain.
type SourceRef struct {
	Seqno    uint32
	RootHash [32]byte
}

// InternalMessage is one prepared inbound internal message of a source
// out-queue.
type InternalMessage struct {
	// Key is the source out-queue dictionary key: next-hop prefix and
	// message hash.
	Key QueueKey
	// EnqueuedLT orders the message inside the import stream — the
	// canonical AugOutMsgQueue leaf lt (emitted_lt for v2 envelopes,
	// otherwise created_lt).
	EnqueuedLT uint64
	// QueueLT is the EnqueuedMsg.enqueued_lt of the queue entry — when the
	// envelope entered this particular queue (the source block's start_lt
	// for transit re-enqueues). ProcessedUpto accounting and the
	// already-processed checks use this lt, never the canonical one.
	QueueLT uint64
	// EnvHash is the queued MsgEnvelope cell hash — the identity used by
	// msg_export_deq_short removals.
	EnvHash [32]byte

	// Envelope is the parsed queued envelope, EnvelopeCell its exact cell
	// (import descriptors must reference it bit-for-bit), Root the message
	// cell inside it.
	Envelope     tlb.MsgEnvelope
	EnvelopeCell *cell.Cell
	Root         *cell.Cell

	// Source identifies the queue the message comes from; SourceSeqno the
	// source block that enqueued it. Seeded entries carry the seed block
	// seqno as a floor.
	Source      ShardIdent
	SourceSeqno uint32
}

// InternalsDelta is the out-queue change of one source block. The
// materialized messages are filtered to one destination while the totals
// count the whole queue effect. Routed deltas may share immutable message
// objects when prepared parent/child destinations overlap.
type InternalsDelta struct {
	// Added is sorted by (EnqueuedLT, MsgHash).
	Added []*InternalMessage
	// RemovedKeys lists dequeued entries resolved to full queue keys
	// (msg_export_deq / msg_export_deq_imm).
	RemovedKeys []QueueKey
	// RemovedEnvHashes lists dequeued entries known only by envelope hash
	// (msg_export_deq_short).
	RemovedEnvHashes [][32]byte

	// AddedTotal and RemovedTotal count the queue effect across all
	// destinations — the owner filter applies only to the materialized
	// fields above. They drive the full-queue-size invariant.
	AddedTotal   int
	RemovedTotal int
	// ExpectedQueueSize, when set, pins the full queue size the post-state
	// stores: ApplyBlock verifies the counter against it under the lock and
	// untracks the source on mismatch — there is no window where a drifted
	// view could be served as exact.
	ExpectedQueueSize *uint64
}

// Typed failures.
var (
	// ErrNotFound reports that an internal-message source is not tracked.
	ErrNotFound = errors.New("msgpool: internals source not found")

	// ErrCutNotReady: a requested source position is ahead of the tracked
	// run — the barrier case, retry once more blocks were fed.
	ErrCutNotReady = errors.New("msgpool: internals source not ready")
	// ErrCutStale: the requested position conflicts with the tracked view —
	// a root-hash mismatch, a broken candidate chain, or a position behind
	// the seed. The context must be rebuilt against the tracked chain.
	ErrCutStale = errors.New("msgpool: internals cut stale")

	// ErrApplyGap: a block delta skipped ahead of the run — events were
	// lost, reseed from state.
	ErrApplyGap = errors.New("msgpool: internals apply gap")
	// ErrApplyDisorder: a delta or seed violated queue invariants (lt
	// order, key uniqueness, size accounting). The source is untracked;
	// reseed from state.
	ErrApplyDisorder = errors.New("msgpool: internals apply disorder")
)

// runEntry wraps a committed message with a versioned removal tombstone.
// Keeping the removal seqno lets a cut below the current top reconstruct the
// exact historical view until compaction advances the retained-history floor.
type runEntry struct {
	msg       *InternalMessage
	removedAt uint32
}

// matView is a memoized destination-level candidate overlay. Views are
// immutable once published; any source-run or candidate mutation invalidates
// the cache generations.
type matView struct {
	tip     [32]byte
	runGen  uint64
	candGen uint64
	entries []runEntry
}

// sourceRun is the committed queue view of one source chain.
type sourceRun struct {
	top       SourceRef
	seedSeqno uint32
	// refs is a bounded consecutive root-hash history used to authenticate
	// exact historical cuts against the already advanced feed tip.
	refs []SourceRef
	// queueSize tracks the full source out-queue size (every destination),
	// the cheap drift detector against the size stored in states.
	queueSize int64
	// entries is (EnqueuedLT, MsgHash)-sorted; tombstones are compacted
	// once they outnumber half of the slice. Compaction advances seedSeqno,
	// because cuts before that point can no longer reconstruct removals.
	entries      []runEntry
	byKey        map[QueueKey]int
	byEnv        map[[32]byte]int
	removedCount int
}

func (r *sourceRun) live() int { return len(r.entries) - r.removedCount }

func (r *sourceRun) hasRef(ref SourceRef) bool {
	if len(r.refs) == 0 || ref.Seqno < r.refs[0].Seqno {
		return false
	}
	index := int(ref.Seqno - r.refs[0].Seqno)
	return index < len(r.refs) && r.refs[index] == ref
}

// CandidateSource pins one committed predecessor queue used as the base of a
// first speculative candidate. Split has one parent source; merge has the
// ordered left and right child sources.
type CandidateSource struct {
	Source  ShardIdent
	Visible SourceRef
}

// CandidateRequest records one destination candidate. Base is required only
// when Parent is nil; later candidates inherit the exact base through Parent.
type CandidateRequest struct {
	ID     [32]byte
	Parent *[32]byte
	Seqno  uint32
	Base   []CandidateSource
	Delta  *InternalsDelta
}

// candidateDelta is a speculative destination-chain delta keyed by candidate
// root hash.
type candidateDelta struct {
	id        [32]byte
	parent    [32]byte
	hasParent bool
	seqno     uint32
	base      []CandidateSource
	delta     *InternalsDelta
}

// destinationState is one destination's internal-message section. Its lock is
// deliberately independent from every other destination: shard and
// masterchain collations must not serialize on an unrelated queue walk.
type destinationState struct {
	owner ShardIdent

	mu         sync.Mutex
	runs       map[ShardIdent]*sourceRun
	candidates map[[32]byte]*candidateDelta
	view       *matView
	runGen     uint64
	// candGen invalidates memoized overlay views on any candidate change.
	candGen uint64

	stats internalsCounters
}

type internalsCounters struct {
	seeds, appliedBlocks, appliedAdded, appliedRemoved uint64
	candidatesAdded, candidatesDropped                 uint64
	cuts, cutMessages, cutFailures                     uint64
	// compactions and cutStaleHistory pair the cause with its effect:
	// compaction resets a run's reconstructable history to its applied top,
	// and every later request for a position below that floor is rejected,
	// which sends the collator to the from-state seed walk. Both are counted
	// so the cost of the reset can be judged from a live node instead of
	// from the shape of the trigger.
	compactions, cutStaleHistory uint64
}

func newDestinationState(owner ShardIdent) *destinationState {
	return &destinationState{
		owner:      owner,
		runs:       map[ShardIdent]*sourceRun{},
		candidates: map[[32]byte]*candidateDelta{},
	}
}

// SourceTop reports the last applied position of a source run. It returns
// ErrNotFound when the source is not tracked.
func (n *destinationState) sourceTop(source ShardIdent) (SourceRef, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	run := n.runs[source]
	if run == nil {
		return SourceRef{}, ErrNotFound
	}
	return run.top, nil
}

func (n *destinationState) validateSourceRef(source ShardIdent, ref SourceRef) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	_, err := n.sourceRunAtLocked(source, ref)

	return err
}

// Seed replaces the run of a source with the full owner-filtered queue view
// of the block top. Entries must be (lt, hash)-sorted and unique by key;
// queueTotal is the full queue size of that state across all destinations.
func (n *destinationState) seed(source ShardIdent, top SourceRef, msgs []*InternalMessage, queueTotal uint64) error {
	if err := validateSorted(msgs, n.owner); err != nil {
		return err
	}
	if err := validateSource(msgs, source, top.Seqno); err != nil {
		return err
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	n.stats.seeds++

	run := &sourceRun{
		top:       top,
		seedSeqno: top.Seqno,
		refs:      []SourceRef{top},
		queueSize: int64(queueTotal),
		entries:   make([]runEntry, len(msgs)),
		byKey:     make(map[QueueKey]int, len(msgs)),
		byEnv:     make(map[[32]byte]int, len(msgs)),
	}
	for i, msg := range msgs {
		run.entries[i] = runEntry{msg: msg}
		if _, dup := run.byKey[msg.Key]; dup {
			return fmt.Errorf("%w: duplicate queue key %x in seed", ErrApplyDisorder, msg.Key)
		}
		if _, dup := run.byEnv[msg.EnvHash]; dup {
			return fmt.Errorf("%w: duplicate envelope hash %x in seed", ErrApplyDisorder, msg.EnvHash)
		}
		run.byKey[msg.Key] = i
		run.byEnv[msg.EnvHash] = i
	}
	n.runs[source] = run
	n.runGen++
	n.view = nil
	if source == n.owner {
		n.pruneCandidatesLocked(top)
	}
	return nil
}

// ApplyBlock advances a source run by one finalized block delta. A delta
// whose seqno is at or below the run top is an idempotent no-op; a delta
// past top+1 fails with ErrApplyGap. A violated queue invariant untracks
// the source — either way the feeder recovers by reseeding from state. A
// speculative candidate matching the new top is pruned together with every
// candidate at or below it.
func (n *destinationState) applyBlock(source ShardIdent, ref SourceRef, delta *InternalsDelta) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	run := n.runs[source]
	if run == nil {
		return fmt.Errorf("%w: source %d:%016x is not seeded", ErrApplyGap, source.Workchain, source.Shard)
	}
	if ref.Seqno <= run.top.Seqno {
		if run.hasRef(ref) {
			return nil
		}
		return fmt.Errorf("%w: conflicting or expired replay at source seqno %d", ErrApplyDisorder, ref.Seqno)
	}
	if ref.Seqno != run.top.Seqno+1 {
		return fmt.Errorf("%w: have %d, got %d", ErrApplyGap, run.top.Seqno, ref.Seqno)
	}

	if err := n.applyDeltaLocked(run, source, ref, delta); err != nil {
		// The cached view can no longer be trusted; untrack the source so
		// cuts fail closed until the reseed.
		delete(n.runs, source)
		n.runGen++
		n.view = nil
		return err
	}

	n.stats.appliedBlocks++
	run.top = ref
	run.refs = append(run.refs, ref)
	n.trimSourceHistoryLocked(run)
	n.runGen++
	n.view = nil
	if source == n.owner {
		n.pruneCandidatesLocked(ref)
	}
	n.compactLocked(run)
	return nil
}

func (n *destinationState) applyDeltaLocked(run *sourceRun, source ShardIdent, ref SourceRef, delta *InternalsDelta) error {
	if err := validateSorted(delta.Added, n.owner); err != nil {
		return err
	}
	if err := validateSource(delta.Added, source, ref.Seqno); err != nil {
		return err
	}

	// Removals first: a block may dequeue and re-enqueue under disjoint
	// keys, but never both for one key.
	for _, key := range delta.RemovedKeys {
		if err := removeFromRun(run, key, run.byKey, ref.Seqno); err != nil {
			return err
		}
		n.stats.appliedRemoved++
	}
	for _, env := range delta.RemovedEnvHashes {
		if err := removeFromRun(run, env, run.byEnv, ref.Seqno); err != nil {
			return err
		}
		n.stats.appliedRemoved++
	}

	if len(delta.Added) > 0 {
		if err := addValidatedToRun(run, delta.Added); err != nil {
			return fmt.Errorf("%w: %v", ErrApplyDisorder, err)
		}
		n.stats.appliedAdded += uint64(len(delta.Added))
	}

	run.queueSize += int64(delta.AddedTotal) - int64(delta.RemovedTotal)
	if run.queueSize < 0 {
		return fmt.Errorf("%w: block %d drives the queue size negative", ErrApplyDisorder, ref.Seqno)
	}
	if delta.ExpectedQueueSize != nil && run.queueSize != int64(*delta.ExpectedQueueSize) {
		return fmt.Errorf("%w: block %d queue size %d diverges from the state's %d",
			ErrApplyDisorder, ref.Seqno, run.queueSize, *delta.ExpectedQueueSize)
	}
	return nil
}

// addValidatedToRun rejects duplicates against the whole run before inserting
// any of them, then appends when the batch is strictly newer than everything
// live and merges otherwise. The name carries the precondition: the batch must
// already have passed validateSorted, which is the single owner of in-batch
// uniqueness — every path in here (applyDeltaLocked, addCandidate) runs it
// first, so a second in-batch detector here would be dead weight.
func addValidatedToRun(run *sourceRun, added []*InternalMessage) error {
	for _, message := range added {
		if _, inRun := run.byKey[message.Key]; inRun {
			return fmt.Errorf("duplicates queue key %x", message.Key)
		}
		if _, inRun := run.byEnv[message.EnvHash]; inRun {
			return fmt.Errorf("duplicates envelope %x", message.EnvHash)
		}
	}

	last := lastLive(run.entries)
	if last != nil && CompareLtHash(last, added[0]) >= 0 {
		return mergeRunAdditions(run, added)
	}
	for _, message := range added {
		run.entries = append(run.entries, runEntry{msg: message})
		run.byKey[message.Key] = len(run.entries) - 1
		run.byEnv[message.EnvHash] = len(run.entries) - 1
	}

	return nil
}

// mergeRunAdditions handles transit messages whose canonical emitted/created
// lt predates entries already waiting in this shard queue. New local messages
// normally hit the append fast path above; transit and requeue descriptors may
// require a real sorted merge.
func mergeRunAdditions(run *sourceRun, added []*InternalMessage) error {
	merged := make([]runEntry, 0, len(run.entries)+len(added))
	entryIndex := 0
	addedIndex := 0
	for entryIndex < len(run.entries) || addedIndex < len(added) {
		if entryIndex == len(run.entries) {
			for ; addedIndex < len(added); addedIndex++ {
				merged = append(merged, runEntry{msg: added[addedIndex]})
			}
			break
		}
		if addedIndex == len(added) {
			merged = append(merged, run.entries[entryIndex:]...)
			break
		}
		order := CompareLtHash(run.entries[entryIndex].msg, added[addedIndex])
		if order == 0 {
			if run.entries[entryIndex].removedAt == 0 {
				return fmt.Errorf("%w: duplicate message hash in merged queue", ErrApplyDisorder)
			}

			// A message may leave and later re-enter the queue with the same
			// canonical order key. Retain the old generation for historical
			// cuts; its tombstone prevents a duplicate in current views.
			merged = append(merged, run.entries[entryIndex])
			entryIndex++
			continue
		}
		if order < 0 {
			merged = append(merged, run.entries[entryIndex])
			entryIndex++
			continue
		}
		merged = append(merged, runEntry{msg: added[addedIndex]})
		addedIndex++
	}

	run.entries = merged
	clear(run.byKey)
	clear(run.byEnv)
	for i := range run.entries {
		if run.entries[i].removedAt != 0 {
			continue
		}
		run.byKey[run.entries[i].msg.Key] = i
		run.byEnv[run.entries[i].msg.EnvHash] = i
	}
	return nil
}

// removeFromRun tombstones the entry resolved through one of the run maps.
// A miss is a disorder: a seed is the exact queue snapshot and every later
// dequeue references an entry present in it, so an unresolved removal means
// the view diverged.
func removeFromRun[K comparable](run *sourceRun, key K, index map[K]int, seqno uint32) error {
	idx, ok := index[key]
	if !ok {
		return fmt.Errorf("%w: removal of an untracked queue entry", ErrApplyDisorder)
	}
	e := &run.entries[idx]
	e.removedAt = seqno
	run.removedCount++
	delete(run.byKey, e.msg.Key)
	delete(run.byEnv, e.msg.EnvHash)
	return nil
}

// compactLocked drops the tombstones once they outweigh the live entries. It
// deliberately collapses the whole reconstructable history in one step: the
// surviving entries carry no removal positions any more, so no cut before the
// applied top could be replayed exactly.
//
// A window-bounded variant (keep tombstones removed within the last K applied
// positions, raise seedSeqno only to top-K) would keep serving the historical
// cuts a lagging shard configuration asks for. It is not done here on purpose:
// the fallback it avoids — rebuilding the neighbour queue from its state — is
// the C++ collator's normal path, so this is baseline cost, not a regression,
// and retaining tombstones would have to be paid on every run. compactions and
// cutStaleHistory measure both halves of that trade first; the change also
// needs its own trigger (counting only compactable tombstones), because the
// current one would stay satisfied by the retained ones and re-fire a full
// byKey/byEnv rebuild on every applied block.
func (n *destinationState) compactLocked(run *sourceRun) {
	if run.removedCount <= 64 || run.removedCount*2 <= len(run.entries) {
		return
	}
	n.stats.compactions++
	compacted := run.entries[:0]
	for _, e := range run.entries {
		if e.removedAt == 0 {
			compacted = append(compacted, e)
		}
	}
	clear(run.entries[len(compacted):])
	run.entries = compacted
	run.removedCount = 0
	run.seedSeqno = run.top.Seqno
	run.refs = []SourceRef{run.top}
	clear(run.byKey)
	clear(run.byEnv)
	for i := range run.entries {
		run.byKey[run.entries[i].msg.Key] = i
		run.byEnv[run.entries[i].msg.EnvHash] = i
	}
}

// DropSource forgets a run and its pending candidates; subsequent cuts
// fail until a reseed.
func (n *destinationState) dropSource(source ShardIdent) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, exists := n.runs[source]; !exists {
		return
	}
	delete(n.runs, source)
	n.runGen++
	n.view = nil
	n.dropCandidatesUsingSourceLocked(source)
}

// PruneSources drops every source the keep predicate rejects — the shard
// rotation GC: sources that left the shard configuration (splits, merges)
// would otherwise stay tracked forever.
func (n *destinationState) pruneSources(keep func(ShardIdent) bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for source := range n.runs {
		if !keep(source) {
			delete(n.runs, source)
			n.runGen++
			n.view = nil
			n.dropCandidatesUsingSourceLocked(source)
		}
	}
}

// addCandidate records one speculative destination-chain queue delta.
func (n *destinationState) addCandidate(request CandidateRequest) error {
	if request.Delta == nil {
		return errors.New("msgpool: candidate delta is required")
	}
	if request.ID == ([32]byte{}) {
		return errors.New("msgpool: candidate id is zero")
	}
	if err := validateSorted(request.Delta.Added, n.owner); err != nil {
		return err
	}
	if err := validateSource(request.Delta.Added, n.owner, request.Seqno); err != nil {
		return err
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if existing := n.candidates[request.ID]; existing != nil {
		if candidateMatchesRequest(existing, request) {
			return nil
		}
		return fmt.Errorf("%w: candidate %x conflicts with its recorded content", ErrCutStale, request.ID[:8])
	}
	if len(n.candidates) >= maxTrackedCandidates {
		return fmt.Errorf("msgpool: internals tracks too many candidates (%d)", len(n.candidates))
	}

	candidate := &candidateDelta{
		id:    request.ID,
		seqno: request.Seqno,
		delta: request.Delta,
	}
	if request.Parent == nil {
		if err := n.validateCandidateBaseLocked(request.Base, request.Seqno); err != nil {
			return err
		}
		candidate.base = append([]CandidateSource(nil), request.Base...)
	} else {
		if len(request.Base) != 0 {
			return errors.New("msgpool: continued candidate must not redefine its base")
		}
		parent := n.candidates[*request.Parent]
		if parent == nil {
			// Applying a candidate promotes its delta into the committed owner
			// run and removes the speculative node. A child prepared concurrently
			// is still valid only when that exact parent is now the run tip.
			run := n.runs[n.owner]
			if run == nil || run.top.RootHash != *request.Parent || request.Seqno != run.top.Seqno+1 {
				return fmt.Errorf("%w: candidate parent %x is unknown", ErrCutStale, request.Parent[:8])
			}
			candidate.base = []CandidateSource{{Source: n.owner, Visible: run.top}}
		} else {
			if request.Seqno != parent.seqno+1 {
				return fmt.Errorf("%w: candidate seqno %d does not follow parent %d",
					ErrCutStale, request.Seqno, parent.seqno)
			}
			candidate.parent = *request.Parent
			candidate.hasParent = true
		}
	}

	n.candidates[request.ID] = candidate
	n.candGen++
	n.view = nil
	n.stats.candidatesAdded++
	return nil
}

func candidateMatchesRequest(candidate *candidateDelta, request CandidateRequest) bool {
	if candidate.seqno != request.Seqno || candidate.hasParent != (request.Parent != nil) {
		return false
	}
	if candidate.hasParent && candidate.parent != *request.Parent {
		return false
	}
	if len(candidate.base) != len(request.Base) {
		return false
	}
	for index := range candidate.base {
		if candidate.base[index] != request.Base[index] {
			return false
		}
	}

	return equalInternalsDelta(candidate.delta, request.Delta)
}

func equalInternalsDelta(left, right *InternalsDelta) bool {
	if left == nil || right == nil {
		return left == right
	}
	if left.AddedTotal != right.AddedTotal || left.RemovedTotal != right.RemovedTotal ||
		len(left.Added) != len(right.Added) || len(left.RemovedKeys) != len(right.RemovedKeys) ||
		len(left.RemovedEnvHashes) != len(right.RemovedEnvHashes) {
		return false
	}
	if (left.ExpectedQueueSize == nil) != (right.ExpectedQueueSize == nil) {
		return false
	}
	if left.ExpectedQueueSize != nil && *left.ExpectedQueueSize != *right.ExpectedQueueSize {
		return false
	}
	for index := range left.Added {
		leftMessage := left.Added[index]
		rightMessage := right.Added[index]
		if leftMessage == nil || rightMessage == nil {
			if leftMessage != rightMessage {
				return false
			}
			continue
		}
		if leftMessage.Key != rightMessage.Key || leftMessage.EnqueuedLT != rightMessage.EnqueuedLT ||
			leftMessage.QueueLT != rightMessage.QueueLT || leftMessage.EnvHash != rightMessage.EnvHash ||
			leftMessage.Source != rightMessage.Source || leftMessage.SourceSeqno != rightMessage.SourceSeqno ||
			leftMessage.EnvelopeCell.HashKey() != rightMessage.EnvelopeCell.HashKey() ||
			leftMessage.Root.HashKey() != rightMessage.Root.HashKey() {
			return false
		}
	}
	for index := range left.RemovedKeys {
		if left.RemovedKeys[index] != right.RemovedKeys[index] {
			return false
		}
	}
	for index := range left.RemovedEnvHashes {
		if left.RemovedEnvHashes[index] != right.RemovedEnvHashes[index] {
			return false
		}
	}

	return true
}

// DropCandidate forgets a speculative delta (the candidate lost).
func (n *destinationState) dropCandidate(id [32]byte) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropCandidateTreeLocked(id)
}

func (n *destinationState) dropCandidates(ids [][32]byte) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, id := range ids {
		n.dropCandidateTreeLocked(id)
	}
}

func (n *destinationState) pruneCandidatesLocked(top SourceRef) {
	for id, cand := range n.candidates {
		if cand.seqno <= top.Seqno {
			delete(n.candidates, id)
			n.candGen++
			n.stats.candidatesDropped++
		}
	}
	for _, candidate := range n.candidates {
		if candidate.hasParent && candidate.seqno == top.Seqno+1 && candidate.parent == top.RootHash {
			candidate.hasParent = false
			candidate.parent = [32]byte{}
			candidate.base = []CandidateSource{{Source: n.owner, Visible: top}}
			n.candGen++
		}
	}
	n.dropOrphanCandidatesLocked()
	n.view = nil
}

func (n *destinationState) validateCandidateBaseLocked(base []CandidateSource, seqno uint32) error {
	if err := validateCandidateBaseTopology(n.owner, base, seqno); err != nil {
		return err
	}

	for index := range base {
		if err := n.validateVisibleLocked(base[index].Source, base[index].Visible); err != nil {
			return err
		}
	}

	return nil
}

func validateCandidateBaseTopology(owner ShardIdent, base []CandidateSource, seqno uint32) error {
	if len(base) < 1 || len(base) > 2 {
		return errors.New("msgpool: first candidate requires one or two predecessor sources")
	}
	for index := range base {
		if index > 0 && CompareShardIdent(base[index-1].Source, base[index].Source) >= 0 {
			return errors.New("msgpool: candidate predecessor sources are not unique and ordered")
		}
	}
	if owner.Workchain == -1 {
		if len(base) != 1 || base[0].Source != owner {
			return fmt.Errorf("%w: masterchain candidate requires its exact linear predecessor", ErrCutStale)
		}
	} else if len(base) == 1 {
		source := base[0].Source
		if source != owner && !isDirectShardChild(source, owner) {
			return fmt.Errorf("%w: candidate predecessor is neither the destination nor its direct parent", ErrCutStale)
		}
	} else if !isDirectShardChild(owner, base[0].Source) ||
		!isDirectShardChild(owner, base[1].Source) {
		return fmt.Errorf("%w: merge predecessors are not the destination's ordered direct children", ErrCutStale)
	}

	var predecessorSeqno uint32
	for index := range base {
		predecessorSeqno = max(predecessorSeqno, base[index].Visible.Seqno)
	}
	if uint64(predecessorSeqno)+1 != uint64(seqno) {
		return fmt.Errorf("%w: candidate %d does not follow predecessor maximum %d",
			ErrCutStale, seqno, predecessorSeqno)
	}

	return nil
}

func (n *destinationState) dropCandidatesUsingSourceLocked(source ShardIdent) {
	for id, candidate := range n.candidates {
		for _, base := range candidate.base {
			if base.Source == source {
				n.dropCandidateTreeLocked(id)
				break
			}
		}
	}
}

func (n *destinationState) dropCandidateTreeLocked(root [32]byte) {
	pending := [][32]byte{root}
	for len(pending) > 0 {
		id := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if _, exists := n.candidates[id]; !exists {
			continue
		}
		delete(n.candidates, id)
		n.candGen++
		n.stats.candidatesDropped++
		for childID, candidate := range n.candidates {
			if candidate.hasParent && candidate.parent == id {
				pending = append(pending, childID)
			}
		}
	}
	n.view = nil
}

func (n *destinationState) dropOrphanCandidatesLocked() {
	for {
		dropped := false
		for id, candidate := range n.candidates {
			if candidate.hasParent && n.candidates[candidate.parent] == nil {
				delete(n.candidates, id)
				n.candGen++
				n.stats.candidatesDropped++
				dropped = true
			}
		}
		if !dropped {
			return
		}
	}
}

// CutSource selects the visible position of one source inside a cut.
type CutSource struct {
	// Visible is the top source block admissible for the collation context
	// (the masterchain shard configuration; for the own chain — the parent).
	Visible SourceRef
}

// CutRequest describes one collation input slice.
type CutRequest struct {
	// Sources lists every queue admitted into the cut; unlisted tracked
	// sources contribute nothing.
	Sources map[ShardIdent]CutSource
	// CandidateTip replaces its exact committed predecessor source(s) with a
	// speculative destination-level queue view.
	CandidateTip *[32]byte
	// CandidateSources admits the predecessor topology carried by CandidateTip.
	// The exact positions live in the recorded candidate chain and may advance
	// atomically when an applied block promotes its root into a committed run.
	CandidateSources []ShardIdent
	// Limit caps the returned messages; <= 0 means no cap.
	Limit int
}

// Cut is a ready collation input slice.
type Cut struct {
	// Messages is the canonical (EnqueuedLT, MsgHash) import order across
	// all requested sources. The messages are shared read-only views: the
	// pool retains them and never mutates a message after publication, and
	// callers must not either — no per-message copies are made.
	Messages []*InternalMessage
	// More reports that Limit cut the stream short.
	More bool
}

// Cut merges the requested source views into the canonical import order.
// It either serves the exact requested context or fails typed:
// ErrCutNotReady (feed the missing blocks and retry) or ErrCutStale (the
// context conflicts with the tracked chain and must be rebuilt).
func (n *destinationState) cut(req CutRequest) (*Cut, error) {
	n.mu.Lock()
	n.stats.cuts++

	cursors := make([]*cutCursor, 0, len(req.Sources)+1)
	var candidateSources map[ShardIdent]struct{}
	if req.CandidateTip != nil {
		cursor, sources, err := n.candidateCursorLocked(req, *req.CandidateTip, req.Limit)
		if err != nil {
			n.stats.cutFailures++
			n.mu.Unlock()
			return nil, err
		}
		candidateSources = sources
		cursors = append(cursors, cursor)
	}
	for source, sel := range req.Sources {
		if _, replaced := candidateSources[source]; replaced {
			continue
		}
		cursor, err := n.sourceCursorLocked(source, sel, req.Limit)
		if err != nil {
			n.stats.cutFailures++
			n.mu.Unlock()
			return nil, err
		}
		cursors = append(cursors, cursor)
	}
	n.mu.Unlock()

	ready := cursors[:0]
	hasTruncated := false
	for _, cursor := range cursors {
		hasTruncated = hasTruncated || cursor.truncated
		if cursor.advance() {
			ready = append(ready, cursor)
		}
	}

	limit := req.Limit
	if limit <= 0 {
		limit = int(^uint(0) >> 1)
	}
	out := &Cut{}
	merge := cursorHeap(ready)
	heap.Init(&merge)
	for merge.Len() > 0 {
		if len(out.Messages) >= limit {
			out.More = true
			break
		}
		top := merge[0]
		out.Messages = append(out.Messages, top.current)
		if top.advance() {
			heap.Fix(&merge, 0)
		} else {
			heap.Pop(&merge)
		}
	}
	if !out.More && len(out.Messages) >= limit && hasTruncated {
		out.More = true
	}
	n.mu.Lock()
	n.stats.cutMessages += uint64(len(out.Messages))
	n.mu.Unlock()
	return out, nil
}

// sourceCursorLocked validates the requested position of one source and
// snapshots its merge cursor. Filtering and merging run after the lock is
// released, so a Skip callback cannot block the feed or re-enter this section.
func (n *destinationState) sourceCursorLocked(source ShardIdent, sel CutSource, limit int) (*cutCursor, error) {
	run, err := n.sourceRunAtLocked(source, sel.Visible)
	if err != nil {
		return nil, err
	}

	// The cursor snapshots the admissible prefix under the lock (committed
	// entries mutate their tombstones there); every filter is a pure value
	// predicate, so nothing runs after the lock but the merge itself.
	cursor := &cutCursor{}
	cursor.snapshot(run.entries, sel.Visible.Seqno, limit)
	return cursor, nil
}

func (n *destinationState) sourceRunAtLocked(source ShardIdent, visible SourceRef) (*sourceRun, error) {
	run := n.runs[source]
	if run == nil {
		return nil, fmt.Errorf("%w: source %d:%016x is not tracked", ErrCutNotReady, source.Workchain, source.Shard)
	}
	switch {
	case visible.Seqno > run.top.Seqno:
		return nil, fmt.Errorf("%w: source %d:%016x has %d, cut wants %d",
			ErrCutNotReady, source.Workchain, source.Shard, run.top.Seqno, visible.Seqno)
	case visible.Seqno < run.seedSeqno:
		// The requested position is behind the retained history floor, which
		// only compaction and trimSourceHistoryLocked move. The caller is not
		// wrong: neighbour positions come from the masterchain shard
		// configuration and legitimately lag the applied tip. Count it, since
		// this is exactly the event that pushes a collation onto the
		// from-state seed walk.
		n.stats.cutStaleHistory++
		return nil, fmt.Errorf("%w: cut at %d predates the tracked view (seeded at %d)",
			ErrCutStale, visible.Seqno, run.seedSeqno)
	case !run.hasRef(visible):
		return nil, fmt.Errorf("%w: source %d:%016x root hash mismatch at position %d",
			ErrCutStale, source.Workchain, source.Shard, visible.Seqno)
	}

	return run, nil
}

func (n *destinationState) trimSourceHistoryLocked(run *sourceRun) {
	if len(run.refs) <= maxSourceRefHistory {
		return
	}
	drop := len(run.refs) - maxSourceRefHistory
	copy(run.refs, run.refs[drop:])
	run.refs = run.refs[:maxSourceRefHistory]
	run.seedSeqno = max(run.seedSeqno, run.refs[0].Seqno)
}

func (n *destinationState) validateVisibleLocked(source ShardIdent, visible SourceRef) error {
	_, err := n.sourceRunAtLocked(source, visible)
	return err
}

func (n *destinationState) candidateCursorLocked(
	request CutRequest,
	tip [32]byte,
	limit int,
) (*cutCursor, map[ShardIdent]struct{}, error) {
	if n.candidates[tip] == nil {
		return n.promotedCandidateCursorLocked(request, tip, limit)
	}

	chain, err := n.candidateChainLocked(tip)
	if err != nil {
		return nil, nil, err
	}
	base := chain[0].base
	replaced := make(map[ShardIdent]struct{}, len(base)+len(request.CandidateSources))
	for _, source := range request.CandidateSources {
		replaced[source] = struct{}{}
	}
	for _, predecessor := range base {
		_, admitted := request.Sources[predecessor.Source]
		for index := 0; !admitted && index < len(request.CandidateSources); index++ {
			admitted = request.CandidateSources[index] == predecessor.Source
		}
		if !admitted {
			return nil, nil, fmt.Errorf("%w: candidate base source %d:%016x is not admitted by the collation topology",
				ErrCutStale, predecessor.Source.Workchain, predecessor.Source.Shard)
		}
		replaced[predecessor.Source] = struct{}{}
	}

	view := n.view
	if view == nil || view.tip != tip || view.runGen != n.runGen || view.candGen != n.candGen {
		baseRun, buildErr := n.candidateBaseRunLocked(base)
		if buildErr != nil {
			return nil, nil, buildErr
		}
		entries, buildErr := materializeCandidateEntries(baseRun, chain)
		if buildErr != nil {
			return nil, nil, buildErr
		}
		view = &matView{
			tip:     tip,
			runGen:  n.runGen,
			candGen: n.candGen,
			entries: entries,
		}
		n.view = view
	}

	cursor := &cutCursor{}
	cursor.snapshot(view.entries, chain[len(chain)-1].seqno, limit)
	return cursor, replaced, nil
}

// promotedCandidateCursorLocked covers the only valid way a requested
// candidate may disappear: its exact block was applied while acquisition was
// preparing the cut. ApplyBlock promotes the delta and prunes the candidate
// under this same lock, so finding the root in the committed owner history is
// an atomic proof that the requested queue view is now the committed one.
// Every other missing root remains stale.
func (n *destinationState) promotedCandidateCursorLocked(
	request CutRequest,
	tip [32]byte,
	limit int,
) (*cutCursor, map[ShardIdent]struct{}, error) {
	run := n.runs[n.owner]
	if run == nil {
		return nil, nil, fmt.Errorf("%w: unknown candidate %x", ErrCutStale, tip[:8])
	}

	var visible SourceRef
	found := false
	for index := len(run.refs) - 1; index >= 0; index-- {
		if run.refs[index].RootHash == tip {
			visible = run.refs[index]
			found = true
			break
		}
	}
	if !found {
		return nil, nil, fmt.Errorf("%w: unknown candidate %x", ErrCutStale, tip[:8])
	}

	_, admitted := request.Sources[n.owner]
	for index := 0; !admitted && index < len(request.CandidateSources); index++ {
		admitted = request.CandidateSources[index] == n.owner
	}
	if !admitted {
		return nil, nil, fmt.Errorf("%w: promoted candidate source %d:%016x is not admitted by the collation topology",
			ErrCutStale, n.owner.Workchain, n.owner.Shard)
	}

	cursor, err := n.sourceCursorLocked(n.owner, CutSource{Visible: visible}, limit)
	if err != nil {
		return nil, nil, err
	}
	replaced := make(map[ShardIdent]struct{}, len(request.CandidateSources)+1)
	replaced[n.owner] = struct{}{}
	for _, source := range request.CandidateSources {
		replaced[source] = struct{}{}
	}

	return cursor, replaced, nil
}

func (n *destinationState) candidateBaseRunLocked(base []CandidateSource) (*sourceRun, error) {
	streams := make([][]runEntry, len(base))
	for index, predecessor := range base {
		run, err := n.sourceRunAtLocked(predecessor.Source, predecessor.Visible)
		if err != nil {
			return nil, err
		}
		// live() is the exact upper bound of the filter below, so the stream
		// never regrows while walking a whole queue.
		streams[index] = make([]runEntry, 0, run.live())
		for _, entry := range run.entries {
			// visibleAt already contains the source-seqno bound.
			if entry.visibleAt(predecessor.Visible.Seqno) {
				streams[index] = append(streams[index], runEntry{msg: entry.msg})
			}
		}
	}

	entries := streams[0]
	if len(streams) == 2 {
		entries = mergeCommittedEntries(streams[0], streams[1])
	}
	run := &sourceRun{
		entries: entries,
		byKey:   make(map[QueueKey]int, len(entries)),
		byEnv:   make(map[[32]byte]int, len(entries)),
	}
	for index, entry := range entries {
		if _, duplicate := run.byKey[entry.msg.Key]; duplicate {
			return nil, fmt.Errorf("%w: candidate base duplicates a queue key", ErrCutStale)
		}
		if _, duplicate := run.byEnv[entry.msg.EnvHash]; duplicate {
			return nil, fmt.Errorf("%w: candidate base duplicates an envelope", ErrCutStale)
		}
		run.byKey[entry.msg.Key] = index
		run.byEnv[entry.msg.EnvHash] = index
	}

	return run, nil
}

func mergeCommittedEntries(left, right []runEntry) []runEntry {
	merged := make([]runEntry, 0, len(left)+len(right))
	for len(left) > 0 && len(right) > 0 {
		if CompareLtHash(left[0].msg, right[0].msg) <= 0 {
			merged = append(merged, left[0])
			left = left[1:]
		} else {
			merged = append(merged, right[0])
			right = right[1:]
		}
	}
	merged = append(merged, left...)
	merged = append(merged, right...)
	return merged
}

// materializeCandidateEntries applies speculative deltas in chain order onto a
// private run, mutating it in place. The caller must hand over a freshly built,
// unshared run — candidateBaseRunLocked copies every entry as a new runEntry
// value, so the tombstones written below never reach the committed n.runs. Any
// future memoization of the base run has to reinstate a copy here.
func materializeCandidateEntries(private *sourceRun, overlay []*candidateDelta) ([]runEntry, error) {
	for _, candidate := range overlay {
		for _, key := range candidate.delta.RemovedKeys {
			if err := removeFromRun(private, key, private.byKey, candidate.seqno); err != nil {
				return nil, fmt.Errorf("%w: candidate %d removes an absent queue key: %v", ErrCutStale, candidate.seqno, err)
			}
		}
		for _, hash := range candidate.delta.RemovedEnvHashes {
			if err := removeFromRun(private, hash, private.byEnv, candidate.seqno); err != nil {
				return nil, fmt.Errorf("%w: candidate %d removes an absent envelope: %v", ErrCutStale, candidate.seqno, err)
			}
		}

		if len(candidate.delta.Added) == 0 {
			continue
		}
		// %v, not %w: mergeRunAdditions wraps ErrApplyDisorder, which must not
		// enter an ErrCutStale chain.
		if err := addValidatedToRun(private, candidate.delta.Added); err != nil {
			return nil, fmt.Errorf("%w: candidate %d additions: %v", ErrCutStale, candidate.seqno, err)
		}
	}

	return private.entries, nil
}

// candidateChainLocked walks tip to the explicit committed predecessor base
// and returns candidate deltas in application order.
func (n *destinationState) candidateChainLocked(tip [32]byte) ([]*candidateDelta, error) {
	var chain []*candidateDelta
	seen := make(map[[32]byte]struct{}, maxTrackedCandidates)
	for at := tip; ; {
		if _, duplicate := seen[at]; duplicate || len(chain) >= maxTrackedCandidates {
			return nil, fmt.Errorf("%w: candidate chain %x contains a cycle", ErrCutStale, tip[:8])
		}
		seen[at] = struct{}{}
		cand := n.candidates[at]
		if cand == nil {
			// Unknown candidates cannot become known by waiting: own-chain
			// candidates are registered before they are cut on, so a miss is
			// a dead fork or a caller bug — the context must be rebuilt.
			return nil, fmt.Errorf("%w: unknown candidate %x", ErrCutStale, at[:8])
		}
		chain = append(chain, cand)
		if !cand.hasParent {
			break
		}
		at = cand.parent
	}
	for left, right := 0, len(chain)-1; left < right; left, right = left+1, right-1 {
		chain[left], chain[right] = chain[right], chain[left]
	}
	if len(chain[0].base) == 0 {
		return nil, fmt.Errorf("%w: candidate chain %x has no committed base", ErrCutStale, tip[:8])
	}
	for index := 1; index < len(chain); index++ {
		if chain[index].seqno != chain[index-1].seqno+1 {
			return nil, fmt.Errorf("%w: candidate chain %x skips seqno %d",
				ErrCutStale, tip[:8], chain[index-1].seqno+1)
		}
	}
	return chain, nil
}

// cutCursor walks one pre-filtered source snapshot in order. It keeps message
// pointers rather than run entries: the snapshot is taken per source per
// collation over the whole visible run, and advance reads nothing else.
// Sharing run.entries instead is not an option — tombstones are written in
// place under the section lock while the merge runs unlocked.
type cutCursor struct {
	entries   []*InternalMessage
	truncated bool

	pos     int
	current *InternalMessage
}

// snapshot copies the admissible entries visible at the cut position up to
// limit (<= 0 means all). One extra admissible entry marks the snapshot as
// truncated.
func (c *cutCursor) snapshot(entries []runEntry, maxSeqno uint32, limit int) {
	capacity := len(entries)
	if limit > 0 {
		capacity = min(limit, capacity)
	}
	c.entries = make([]*InternalMessage, 0, capacity)
	for i := range entries {
		// visibleAt already contains the source-seqno bound.
		if !entries[i].visibleAt(maxSeqno) {
			continue
		}
		if limit > 0 && len(c.entries) == limit {
			c.truncated = true
			return
		}
		c.entries = append(c.entries, entries[i].msg)
	}
}

// advance moves to the next message; false means exhausted.
func (c *cutCursor) advance() bool {
	if c.pos >= len(c.entries) {
		c.current = nil
		return false
	}
	c.current = c.entries[c.pos]
	c.pos++
	return true
}

type cursorHeap []*cutCursor

func (h cursorHeap) Len() int { return len(h) }
func (h cursorHeap) Less(i, j int) bool {
	return CompareLtHash(h[i].current, h[j].current) < 0
}
func (h cursorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *cursorHeap) Push(x any)   { *h = append(*h, x.(*cutCursor)) }
func (h *cursorHeap) Pop() any {
	old := *h
	c := old[len(old)-1]
	*h = old[:len(old)-1]
	return c
}

// InternalsStats is a snapshot of the section counters.
type InternalsStats struct {
	Destinations int
	Sources      int
	Entries      int
	Removed      int
	Candidates   int

	Seeds          uint64
	AppliedBlocks  uint64
	AppliedAdded   uint64
	AppliedRemoved uint64

	CandidatesAdded   uint64
	CandidatesDropped uint64

	Cuts        uint64
	CutMessages uint64
	CutFailures uint64

	// Compactions counts run compactions, each of which resets the
	// reconstructable history to the applied top; CutStaleHistory counts the
	// requests refused because they landed below that floor.
	Compactions     uint64
	CutStaleHistory uint64
}

// Stats returns a counter snapshot.
func (n *destinationState) statsSnapshot() InternalsStats {
	n.mu.Lock()
	defer n.mu.Unlock()
	st := InternalsStats{
		Sources:           len(n.runs),
		Candidates:        len(n.candidates),
		Seeds:             n.stats.seeds,
		AppliedBlocks:     n.stats.appliedBlocks,
		AppliedAdded:      n.stats.appliedAdded,
		AppliedRemoved:    n.stats.appliedRemoved,
		CandidatesAdded:   n.stats.candidatesAdded,
		CandidatesDropped: n.stats.candidatesDropped,
		Cuts:              n.stats.cuts,
		CutMessages:       n.stats.cutMessages,
		CutFailures:       n.stats.cutFailures,
		Compactions:       n.stats.compactions,
		CutStaleHistory:   n.stats.cutStaleHistory,
	}
	for _, run := range n.runs {
		st.Entries += run.live()
		st.Removed += run.removedCount
	}
	return st
}

const (
	maxTrackedCandidates = 64
	maxSourceRefHistory  = 256
)

// lastLive returns the last non-removed message of a run, or nil.
func lastLive(entries []runEntry) *InternalMessage {
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].removedAt == 0 {
			return entries[i].msg
		}
	}
	return nil
}

func (e *runEntry) visibleAt(seqno uint32) bool {
	return e.msg.SourceSeqno <= seqno && (e.removedAt == 0 || e.removedAt > seqno)
}

// CompareLtHash orders messages by (EnqueuedLT, MsgHash) — the canonical
// import order shared by the pool, the collator input validation and the
// reference OutputQueueMerger.
func CompareLtHash(a, b *InternalMessage) int {
	if a.EnqueuedLT != b.EnqueuedLT {
		if a.EnqueuedLT < b.EnqueuedLT {
			return -1
		}
		return 1
	}
	return bytes.Compare(a.Key[12:], b.Key[12:])
}

// validateSorted checks the (lt, hash) order and owner routing of a
// prepared message batch. It is the single owner of in-batch uniqueness: every
// path into a run (seed, applyDeltaLocked, addCandidate) runs it before the
// batch reaches addValidatedToRun, which therefore only checks the run.
func validateSorted(msgs []*InternalMessage, owner ShardIdent) error {
	// The queue key carries no lt, so the strict (lt, hash) order below does not
	// rule out one key appearing twice in a batch. A batch that got past this
	// would put the same key in the view twice and feed it to collation.
	keys := make(map[QueueKey]struct{}, len(msgs))
	envelopes := make(map[[32]byte]struct{}, len(msgs))
	for i, msg := range msgs {
		if msg == nil || msg.EnvelopeCell == nil || msg.Root == nil {
			return fmt.Errorf("%w: incomplete internal message %d", ErrApplyDisorder, i)
		}
		if !owner.ContainsPrefix(msg.Key.NextHop()) {
			return fmt.Errorf("%w: message %d next hop is outside the owner shard", ErrApplyDisorder, i)
		}
		if i > 0 && CompareLtHash(msgs[i-1], msg) >= 0 {
			return fmt.Errorf("%w: message %d breaks the (lt, hash) order", ErrApplyDisorder, i)
		}
		if _, duplicate := keys[msg.Key]; duplicate {
			return fmt.Errorf("%w: message %d repeats queue key %x", ErrApplyDisorder, i, msg.Key)
		}
		if _, duplicate := envelopes[msg.EnvHash]; duplicate {
			return fmt.Errorf("%w: message %d repeats envelope %x", ErrApplyDisorder, i, msg.EnvHash)
		}
		keys[msg.Key] = struct{}{}
		envelopes[msg.EnvHash] = struct{}{}
	}

	return nil
}

func validateSource(msgs []*InternalMessage, source ShardIdent, seqno uint32) error {
	for index, message := range msgs {
		if message.Source != source {
			return fmt.Errorf("%w: message %d belongs to a different source", ErrApplyDisorder, index)
		}
		if message.SourceSeqno > seqno {
			return fmt.Errorf("%w: message %d is newer than source position %d", ErrApplyDisorder, index, seqno)
		}
	}

	return nil
}

// CompareShardIdent is the canonical (workchain, shard) order. Every sort and
// every two-pointer merge over shard identifiers has to agree on it, including
// the ones outside this package that walk pool output.
func CompareShardIdent(left, right ShardIdent) int {
	if left.Workchain != right.Workchain {
		return cmp.Compare(left.Workchain, right.Workchain)
	}

	return cmp.Compare(left.Shard, right.Shard)
}

func isDirectShardChild(parent, child ShardIdent) bool {
	if parent.Workchain != child.Workchain || parent.Shard == 0 || child.Shard == 0 {
		return false
	}

	// The lowest set bit is TON's shard-depth marker; a direct child moves
	// it exactly one position to the right while retaining the parent prefix.
	parentTag := parent.Shard & (^parent.Shard + 1)
	childTag := child.Shard & (^child.Shard + 1)
	return parentTag>>1 == childTag && parent.Contains(child.Workchain, child.Shard)
}
