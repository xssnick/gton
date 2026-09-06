package msgpool

import (
	"container/heap"
	"errors"
	"fmt"
	"sync"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

// Branch is one validator session's speculative internal-message lineage.
// Message payloads remain owned by msgpool and are shared read-only. Branch
// state contains only pointer snapshots, immutable candidate deltas and their
// indexes; feed updates never acquire its lock or mutate it.
type Branch struct {
	destination ShardIdent
	state       *destinationState
	routing     *internalsSnapshot

	mu         sync.Mutex
	closed     bool
	sources    map[branchSourceKey]*branchSource
	candidates map[[32]byte]*branchCandidate
	children   map[[32]byte]map[[32]byte]struct{}
}

type branchSourceKey struct {
	source  ShardIdent
	visible SourceRef
}

type branchSource struct {
	entries []*InternalMessage
}

type branchBase struct {
	sources []CandidateSource
	entries []*InternalMessage
	byKey   map[QueueKey]*InternalMessage
	byEnv   map[[32]byte]*InternalMessage
	byOrder map[branchOrderKey]*InternalMessage
}

type branchCandidate struct {
	id     [32]byte
	seqno  uint32
	parent *branchCandidate
	base   *branchBase
	delta  branchDelta
}

type branchDelta struct {
	raw        *InternalsDelta
	added      []*InternalMessage
	addedKey   map[QueueKey]*InternalMessage
	addedEnv   map[[32]byte]*InternalMessage
	addedOrder map[branchOrderKey]*InternalMessage
	removedKey map[QueueKey]*InternalMessage
	removedEnv map[[32]byte]*InternalMessage
	removedOrd map[branchOrderKey]*InternalMessage
}

type branchOrderKey struct {
	lt   uint64
	hash [32]byte
}

func newBranch(destination ShardIdent, state *destinationState, routing *internalsSnapshot) *Branch {
	return &Branch{
		destination: destination,
		state:       state,
		routing:     routing,
		sources:     make(map[branchSourceKey]*branchSource),
		candidates:  make(map[[32]byte]*branchCandidate),
		children:    make(map[[32]byte]map[[32]byte]struct{}),
	}
}

func (b *Branch) seedSource(source ShardIdent, visible SourceRef, messages []*InternalMessage) error {
	if err := validateSorted(messages, b.destination); err != nil {
		return err
	}
	if err := validateSource(messages, source, visible.Seqno); err != nil {
		return err
	}

	entries := append([]*InternalMessage(nil), messages...)
	key := branchSourceKey{source: source, visible: visible}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	if current := b.sources[key]; current != nil {
		if equalBranchMessages(current.entries, entries) {
			return nil
		}
		return fmt.Errorf("%w: source %d:%016x conflicts at position %d",
			ErrCutStale, source.Workchain, source.Shard, visible.Seqno)
	}
	b.sources[key] = &branchSource{entries: entries}

	return nil
}

// PinSource snapshots one exact committed run into the lease without parsing
// state. ErrCutNotReady and ErrCutStale tell the caller to wait or explicitly
// install the authenticated state view with SeedSourceFromStateRoot.
func (b *Branch) PinSource(source ShardIdent, visible SourceRef) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}

	_, err := b.pinSourceLocked(source, visible)
	return err
}

// SourcePinnable reports whether PinSource would succeed for this exact
// position, without pinning anything. It is the admission form of PinSource's
// own precondition: a run this branch already holds is always pinnable, and
// otherwise the committed state must hold that exact reference.
//
// Collation asks before it commits to a masterchain view. A view whose
// registered neighbour tops this node has not ingested yet would force the
// from-state seed walk at the slot, or be refused outright on a build that may
// not seed, so the pick prefers a view whose whole neighbour set is already
// there and only falls back to one that is not.
func (b *Branch) SourcePinnable(source ShardIdent, visible SourceRef) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return false
	}
	if b.sources[branchSourceKey{source: source, visible: visible}] != nil {
		return true
	}
	b.state.mu.Lock()
	defer b.state.mu.Unlock()
	_, err := b.state.sourceRunAtLocked(source, visible)

	return err == nil
}

// SeedSourceFromStateRoot derives and installs one exact source snapshot with
// the branch's pinned destination router and returns its immutable messages.
// Unlike Internals.SeedsFromStateRoot, it cannot be redirected by a newer
// global split/merge topology.
func (b *Branch) SeedSourceFromStateRoot(
	source ShardIdent,
	visible SourceRef,
	stateRoot *cell.Cell,
) ([]*InternalMessage, uint64, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, 0, ErrClosed
	}
	routing := b.routing
	b.mu.Unlock()

	seeds, total, err := routedSeedsFromStateRoot(stateRoot, source, visible, routing)
	if err != nil {
		return nil, 0, err
	}
	if len(seeds) != 1 || seeds[0].Destination != b.destination {
		return nil, 0, errors.New("msgpool: branch routing snapshot is invalid")
	}
	if err = b.seedSource(source, visible, seeds[0].Messages); err != nil {
		return nil, 0, err
	}

	return seeds[0].Messages, total, nil
}

// DeltaFromBlockRoot derives one candidate's queue delta using the routing
// rule pinned when the branch opened. A concurrent split, merge or rotation
// therefore cannot change the destination projection halfway through a
// session.
func (b *Branch) DeltaFromBlockRoot(
	source ShardIdent,
	ref SourceRef,
	root *cell.Cell,
	startLT uint64,
) (*InternalsDelta, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, ErrClosed
	}
	routing := b.routing
	b.mu.Unlock()

	routed, err := routedDeltasFromBlockRoot(root, startLT, source, ref, routing)
	if err != nil {
		return nil, err
	}
	if len(routed) != 1 || routed[0].Destination != b.destination {
		return nil, errors.New("msgpool: branch routing snapshot is invalid")
	}

	return routed[0].Delta, nil
}

// AddCandidate appends one immutable candidate node to this session lineage.
// Root candidates pin their committed predecessor queues once; children only
// allocate indexes proportional to their own delta, never to queue length.
func (b *Branch) AddCandidate(request CandidateRequest) error {
	_, err := b.addCandidate(request, false)

	return err
}

// AddOrReusePromotedCandidate is AddCandidate for a block-derived local
// install that may race its predecessor's promotion from candidate to applied
// state. It additionally accepts only the narrow linear Parent=P/Base=P
// representation change, with the same seqno and complete delta. The returned
// ownership tells a surrounding transaction whether it may roll this call back.
func (b *Branch) AddOrReusePromotedCandidate(request CandidateRequest) (CandidateInstall, error) {
	return b.addCandidate(request, true)
}

// ReusePromotedCandidate verifies an already installed candidate without
// materializing a new applied-base snapshot. It accepts the same exact or
// linear Parent=P/Base=P forms as AddOrReusePromotedCandidate and reports
// ErrCandidateNotFound when the caller must prepare the base and install it.
//
// It deliberately does not run the delta prologue AddCandidate runs. This
// installs nothing: it either recognizes the request as content-identical to a
// node already in the branch — which was validated when it was installed, and
// whose equality is decided over the whole delta — or it hands the request
// straight to AddOrReusePromotedCandidate, which validates it there. Running
// the prologue here too meant two passes building two maps over every message
// of the delta on the way to the same install.
func (b *Branch) ReusePromotedCandidate(request CandidateRequest) error {
	if request.Delta == nil {
		return errors.New("msgpool: candidate delta is required")
	}
	if request.ID == ([32]byte{}) {
		return errors.New("msgpool: candidate id is zero")
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	existing := b.candidates[request.ID]
	if existing == nil {
		return fmt.Errorf("%w: candidate %x", ErrCandidateNotFound, request.ID[:8])
	}
	if branchCandidateMatches(existing, request) ||
		b.branchCandidateMatchesPromotedBase(existing, request) {
		return nil
	}

	return fmt.Errorf("%w: candidate %x conflicts with its recorded content", ErrCutStale, request.ID[:8])
}

func (b *Branch) addCandidate(
	request CandidateRequest,
	allowPromotedBase bool,
) (CandidateInstall, error) {
	if request.Delta == nil {
		return 0, errors.New("msgpool: candidate delta is required")
	}
	if request.ID == ([32]byte{}) {
		return 0, errors.New("msgpool: candidate id is zero")
	}
	if err := validateSorted(request.Delta.Added, b.destination); err != nil {
		return 0, err
	}
	if err := validateSource(request.Delta.Added, b.destination, request.Seqno); err != nil {
		return 0, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0, ErrClosed
	}
	if existing := b.candidates[request.ID]; existing != nil {
		if branchCandidateMatches(existing, request) ||
			(allowPromotedBase && b.branchCandidateMatchesPromotedBase(existing, request)) {
			return CandidateInstallReused, nil
		}
		return 0, fmt.Errorf("%w: candidate %x conflicts with its recorded content", ErrCutStale, request.ID[:8])
	}
	candidate := &branchCandidate{id: request.ID, seqno: request.Seqno}
	if request.Parent == nil {
		if err := validateCandidateBaseTopology(b.destination, request.Base, request.Seqno); err != nil {
			return 0, err
		}
		base, err := b.snapshotBaseLocked(request.Base)
		if err != nil {
			return 0, err
		}
		candidate.base = base
	} else {
		if len(request.Base) != 0 {
			return 0, errors.New("msgpool: continued candidate must not redefine its base")
		}
		parent := b.candidates[*request.Parent]
		if parent == nil {
			return 0, fmt.Errorf("%w: candidate parent %x is unknown", ErrCutStale, request.Parent[:8])
		}
		if request.Seqno != parent.seqno+1 {
			return 0, fmt.Errorf("%w: candidate seqno %d does not follow parent %d",
				ErrCutStale, request.Seqno, parent.seqno)
		}
		candidate.parent = parent
		candidate.base = parent.base
	}

	delta, err := b.indexDeltaLocked(candidate.parent, candidate.base, request.Delta)
	if err != nil {
		return 0, fmt.Errorf("%w: candidate %d delta: %v", ErrCutStale, request.Seqno, err)
	}
	candidate.delta = delta
	b.candidates[candidate.id] = candidate
	if candidate.parent != nil {
		children := b.children[candidate.parent.id]
		if children == nil {
			children = make(map[[32]byte]struct{})
			b.children[candidate.parent.id] = children
		}
		children[candidate.id] = struct{}{}
	}

	return CandidateInstallAdded, nil
}

// HasCandidate reports whether this lineage already holds the candidate node.
// It exists for the speculative first slot, whose acquisition re-derives a
// foreign lineage on every attempt: a node that is already present must be
// skipped rather than re-added, because the fork that installed it first may
// have recorded a different parent shape and AddCandidate would then report a
// conflict for content that is in fact the same block.
func (b *Branch) HasCandidate(id [32]byte) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.candidates[id] != nil
}

// Cut merges a branch tip with exact committed neighbor snapshots. Every
// cursor walks immutable pointer slices lazily, so Limit bounds ordinary work
// without materializing the whole candidate queue.
func (b *Branch) Cut(request CutRequest) (*Cut, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, ErrClosed
	}

	cursors := make([]*branchCursor, 0, len(request.Sources)+1)
	replaced := map[ShardIdent]struct{}{}
	if request.CandidateTip != nil {
		tip := b.candidates[*request.CandidateTip]
		if tip == nil {
			b.mu.Unlock()
			return nil, fmt.Errorf("%w: unknown candidate %x", ErrCutStale, request.CandidateTip[:8])
		}
		for _, source := range request.CandidateSources {
			replaced[source] = struct{}{}
		}
		for _, source := range tip.base.sources {
			_, admitted := request.Sources[source.Source]
			for index := 0; !admitted && index < len(request.CandidateSources); index++ {
				admitted = request.CandidateSources[index] == source.Source
			}
			if !admitted {
				b.mu.Unlock()
				return nil, fmt.Errorf("%w: candidate base source %d:%016x is not admitted by the collation topology",
					ErrCutStale, source.Source.Workchain, source.Source.Shard)
			}
			replaced[source.Source] = struct{}{}
		}
		cursors = append(cursors, &branchCursor{
			entries: tip.base.entries,
			live: func(message *InternalMessage) bool {
				return b.lookupOrder(tip, tip.base, orderKey(message)) == message
			},
		})
		for at := tip; at != nil; at = at.parent {
			candidate := at
			if len(candidate.delta.added) == 0 {
				continue
			}
			cursors = append(cursors, &branchCursor{
				entries: candidate.delta.added,
				live: func(message *InternalMessage) bool {
					return b.lookupOrder(tip, tip.base, orderKey(message)) == message
				},
			})
		}
	}
	for source, selection := range request.Sources {
		if _, skip := replaced[source]; skip {
			continue
		}
		view, err := b.pinSourceLocked(source, selection.Visible)
		if err != nil {
			b.mu.Unlock()
			return nil, err
		}
		cursors = append(cursors, &branchCursor{entries: view.entries})
	}
	b.mu.Unlock()

	ready := cursors[:0]
	for _, cursor := range cursors {
		if cursor.advance() {
			ready = append(ready, cursor)
		}
	}
	limit := request.Limit
	if limit <= 0 {
		limit = int(^uint(0) >> 1)
	}
	result := &Cut{}
	merge := branchCursorHeap(ready)
	heap.Init(&merge)
	for merge.Len() > 0 && len(result.Messages) < limit {
		top := merge[0]
		result.Messages = append(result.Messages, top.current)
		if top.advance() {
			heap.Fix(&merge, 0)
		} else {
			heap.Pop(&merge)
		}
	}
	result.More = merge.Len() > 0

	return result, nil
}

// Retain keeps tip, its ancestors and every descendant built on tip. nil clears
// all speculative and pinned source state. It is the window-transition
// ownership boundary: forks which diverged before the selected tip are stale,
// while descendants of that exact tip may still belong to live pipelined
// builds.
func (b *Branch) Retain(tip *[32]byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	if tip == nil {
		clear(b.candidates)
		clear(b.children)
		clear(b.sources)
		return nil
	}
	candidate := b.candidates[*tip]
	if candidate == nil {
		return fmt.Errorf("%w: unknown candidate %x", ErrCutStale, tip[:8])
	}
	keep := make(map[[32]byte]struct{})
	for at := candidate; at != nil; at = at.parent {
		keep[at.id] = struct{}{}
	}
	pending := [][32]byte{candidate.id}
	for len(pending) > 0 {
		last := len(pending) - 1
		id := pending[last]
		pending = pending[:last]
		for child := range b.children[id] {
			if _, retained := keep[child]; retained {
				continue
			}
			keep[child] = struct{}{}
			pending = append(pending, child)
		}
	}
	for id := range b.candidates {
		if _, retained := keep[id]; !retained {
			delete(b.candidates, id)
		}
	}
	clear(b.children)
	for _, current := range b.candidates {
		if current.parent == nil {
			continue
		}
		children := b.children[current.parent.id]
		if children == nil {
			children = make(map[[32]byte]struct{})
			b.children[current.parent.id] = children
		}
		children[current.id] = struct{}{}
	}
	keepSources := make(map[branchSourceKey]struct{}, len(candidate.base.sources))
	for _, source := range candidate.base.sources {
		keepSources[branchSourceKey{source: source.Source, visible: source.Visible}] = struct{}{}
	}
	for key := range b.sources {
		if _, retained := keepSources[key]; !retained {
			delete(b.sources, key)
		}
	}

	return nil
}

// DropCandidate removes id and its descendants. Missing ids are idempotent.
func (b *Branch) DropCandidate(id [32]byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.dropTreeLocked(id)
}

// Close releases every pointer snapshot held by the lease. It is idempotent
// and deliberately independent of the current topology.
func (b *Branch) Close() {
	b.mu.Lock()
	b.closed = true
	b.sources = nil
	b.candidates = nil
	b.children = nil
	b.routing = nil
	b.state = nil
	b.mu.Unlock()
}

func (b *Branch) snapshotBaseLocked(sources []CandidateSource) (*branchBase, error) {
	streams := make([][]*InternalMessage, len(sources))
	for index, source := range sources {
		view, err := b.pinSourceLocked(source.Source, source.Visible)
		if err != nil {
			return nil, err
		}
		streams[index] = view.entries
	}
	entries := streams[0]
	if len(streams) == 2 {
		entries = mergeBranchEntries(streams[0], streams[1])
	}
	base := &branchBase{
		sources: append([]CandidateSource(nil), sources...),
		entries: entries,
		byKey:   make(map[QueueKey]*InternalMessage, len(entries)),
		byEnv:   make(map[[32]byte]*InternalMessage, len(entries)),
		byOrder: make(map[branchOrderKey]*InternalMessage, len(entries)),
	}
	for _, message := range entries {
		if base.byKey[message.Key] != nil || base.byEnv[message.EnvHash] != nil ||
			base.byOrder[orderKey(message)] != nil {
			return nil, fmt.Errorf("%w: candidate base contains duplicate queue identity", ErrCutStale)
		}
		base.byKey[message.Key] = message
		base.byEnv[message.EnvHash] = message
		base.byOrder[orderKey(message)] = message
	}

	return base, nil
}

func (b *Branch) pinSourceLocked(source ShardIdent, visible SourceRef) (*branchSource, error) {
	key := branchSourceKey{source: source, visible: visible}
	if view := b.sources[key]; view != nil {
		return view, nil
	}
	b.state.mu.Lock()
	run, err := b.state.sourceRunAtLocked(source, visible)
	if err != nil {
		b.state.mu.Unlock()
		return nil, err
	}
	entries := make([]*InternalMessage, 0, run.live())
	for index := range run.entries {
		if run.entries[index].visibleAt(visible.Seqno) {
			entries = append(entries, run.entries[index].msg)
		}
	}
	b.state.mu.Unlock()
	view := &branchSource{entries: entries}
	b.sources[key] = view

	return view, nil
}

func (b *Branch) indexDeltaLocked(
	parent *branchCandidate,
	base *branchBase,
	delta *InternalsDelta,
) (branchDelta, error) {
	indexed := branchDelta{
		raw:        delta,
		added:      delta.Added,
		addedKey:   make(map[QueueKey]*InternalMessage, len(delta.Added)),
		addedEnv:   make(map[[32]byte]*InternalMessage, len(delta.Added)),
		addedOrder: make(map[branchOrderKey]*InternalMessage, len(delta.Added)),
		removedKey: make(map[QueueKey]*InternalMessage, len(delta.RemovedKeys)+len(delta.RemovedEnvHashes)),
		removedEnv: make(map[[32]byte]*InternalMessage, len(delta.RemovedKeys)+len(delta.RemovedEnvHashes)),
		removedOrd: make(map[branchOrderKey]*InternalMessage, len(delta.RemovedKeys)+len(delta.RemovedEnvHashes)),
	}
	for _, key := range delta.RemovedKeys {
		message := b.lookupKey(parent, base, key)
		if message == nil || indexed.removedKey[message.Key] != nil {
			return branchDelta{}, errors.New("removal of an absent queue key")
		}
		indexed.recordRemoval(message)
	}
	for _, env := range delta.RemovedEnvHashes {
		message := b.lookupEnv(parent, base, env)
		if message == nil || indexed.removedKey[message.Key] != nil {
			return branchDelta{}, errors.New("removal of an absent envelope")
		}
		indexed.recordRemoval(message)
	}
	for _, message := range delta.Added {
		order := orderKey(message)
		if current := b.lookupKey(parent, base, message.Key); current != nil && indexed.removedKey[current.Key] == nil {
			return branchDelta{}, errors.New("addition repeats a live queue key")
		}
		if current := b.lookupEnv(parent, base, message.EnvHash); current != nil && indexed.removedEnv[current.EnvHash] == nil {
			return branchDelta{}, errors.New("addition repeats a live envelope")
		}
		if current := b.lookupOrder(parent, base, order); current != nil && indexed.removedOrd[order] == nil {
			return branchDelta{}, errors.New("addition repeats a live canonical order key")
		}
		indexed.addedKey[message.Key] = message
		indexed.addedEnv[message.EnvHash] = message
		indexed.addedOrder[order] = message
	}

	return indexed, nil
}

func (d *branchDelta) recordRemoval(message *InternalMessage) {
	d.removedKey[message.Key] = message
	d.removedEnv[message.EnvHash] = message
	d.removedOrd[orderKey(message)] = message
}

func (b *Branch) lookupKey(tip *branchCandidate, base *branchBase, key QueueKey) *InternalMessage {
	for at := tip; at != nil; at = at.parent {
		if message := at.delta.addedKey[key]; message != nil {
			return message
		}
		if at.delta.removedKey[key] != nil {
			return nil
		}
	}
	return base.byKey[key]
}

func (b *Branch) lookupEnv(tip *branchCandidate, base *branchBase, hash [32]byte) *InternalMessage {
	for at := tip; at != nil; at = at.parent {
		if message := at.delta.addedEnv[hash]; message != nil {
			return message
		}
		if at.delta.removedEnv[hash] != nil {
			return nil
		}
	}
	return base.byEnv[hash]
}

func (b *Branch) lookupOrder(tip *branchCandidate, base *branchBase, key branchOrderKey) *InternalMessage {
	for at := tip; at != nil; at = at.parent {
		if message := at.delta.addedOrder[key]; message != nil {
			return message
		}
		if at.delta.removedOrd[key] != nil {
			return nil
		}
	}
	return base.byOrder[key]
}

func (b *Branch) dropTreeLocked(root [32]byte) {
	pending := [][32]byte{root}
	for len(pending) > 0 {
		last := len(pending) - 1
		id := pending[last]
		pending = pending[:last]
		candidate := b.candidates[id]
		if candidate == nil {
			continue
		}
		for child := range b.children[id] {
			pending = append(pending, child)
		}
		delete(b.children, id)
		delete(b.candidates, id)
		if candidate.parent != nil {
			delete(b.children[candidate.parent.id], id)
		}
	}
}

func branchCandidateMatches(candidate *branchCandidate, request CandidateRequest) bool {
	if candidate.seqno != request.Seqno || (candidate.parent != nil) != (request.Parent != nil) {
		return false
	}
	if candidate.parent != nil && candidate.parent.id != *request.Parent {
		return false
	}
	if candidate.parent == nil {
		if len(candidate.base.sources) != len(request.Base) {
			return false
		}
		for index := range request.Base {
			if candidate.base.sources[index] != request.Base[index] {
				return false
			}
		}
	}
	return equalInternalsDelta(candidate.delta.raw, request.Delta)
}

func (b *Branch) branchCandidateMatchesPromotedBase(
	candidate *branchCandidate,
	request CandidateRequest,
) bool {
	if candidate.seqno != request.Seqno || !equalInternalsDelta(candidate.delta.raw, request.Delta) {
		return false
	}
	if candidate.parent != nil && request.Parent == nil {
		return len(request.Base) == 1 &&
			candidateSourceNamesParent(request.Base[0], b.destination, candidate.parent)
	}
	if candidate.parent == nil && request.Parent != nil && len(request.Base) == 0 {
		parent := b.candidates[*request.Parent]

		return parent != nil && len(candidate.base.sources) == 1 &&
			candidateSourceNamesParent(candidate.base.sources[0], b.destination, parent)
	}

	return false
}

func candidateSourceNamesParent(
	source CandidateSource,
	destination ShardIdent,
	parent *branchCandidate,
) bool {
	return source.Source == destination && source.Visible == (SourceRef{
		Seqno:    parent.seqno,
		RootHash: parent.id,
	})
}

func orderKey(message *InternalMessage) branchOrderKey {
	var hash [32]byte
	copy(hash[:], message.Key[12:])
	return branchOrderKey{lt: message.EnqueuedLT, hash: hash}
}

func mergeBranchEntries(left, right []*InternalMessage) []*InternalMessage {
	merged := make([]*InternalMessage, 0, len(left)+len(right))
	for len(left) > 0 && len(right) > 0 {
		if CompareLtHash(left[0], right[0]) <= 0 {
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

func equalBranchMessages(left, right []*InternalMessage) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		a, b := left[index], right[index]
		if a.Key != b.Key || a.EnqueuedLT != b.EnqueuedLT || a.QueueLT != b.QueueLT ||
			a.EnvHash != b.EnvHash || a.Source != b.Source || a.SourceSeqno != b.SourceSeqno ||
			a.EnvelopeCell.HashKey() != b.EnvelopeCell.HashKey() || a.Root.HashKey() != b.Root.HashKey() {
			return false
		}
	}
	return true
}

type branchCursor struct {
	entries []*InternalMessage
	live    func(*InternalMessage) bool
	index   int
	current *InternalMessage
}

func (c *branchCursor) advance() bool {
	for c.index < len(c.entries) {
		message := c.entries[c.index]
		c.index++
		if c.live == nil || c.live(message) {
			c.current = message
			return true
		}
	}
	c.current = nil
	return false
}

type branchCursorHeap []*branchCursor

func (h branchCursorHeap) Len() int           { return len(h) }
func (h branchCursorHeap) Less(i, j int) bool { return CompareLtHash(h[i].current, h[j].current) < 0 }
func (h branchCursorHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *branchCursorHeap) Push(value any)    { *h = append(*h, value.(*branchCursor)) }
func (h *branchCursorHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	old[last] = nil
	*h = old[:last]
	return value
}
