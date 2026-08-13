package msgpool

import (
	"fmt"
	"math/bits"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// RoutedDelta is one destination's projection of a source block queue delta.
// Message payloads are shared read-only between overlapping destinations.
type RoutedDelta struct {
	Destination ShardIdent
	Delta       *InternalsDelta
}

// RoutedSeed is one destination's projection of a source state out-queue.
// Message payloads are shared read-only between overlapping destinations.
type RoutedSeed struct {
	Destination ShardIdent
	Messages    []*InternalMessage
}

// Internals owns all active and prepared destination views. Reconciliation
// publishes an immutable topology snapshot atomically; each destination keeps
// an independent lock for queue mutation and cuts.
type Internals struct {
	log zerolog.Logger

	reconcileMu sync.Mutex
	snapshot    atomic.Pointer[internalsSnapshot]
}

type internalsSnapshot struct {
	destinations []destinationEntry
	byShard      map[ShardIdent]*destinationState
	router       destinationRouter
}

type destinationEntry struct {
	shard ShardIdent
	state *destinationState
}

func newInternals(log zerolog.Logger) *Internals {
	internals := &Internals{log: log}
	internals.snapshot.Store(&internalsSnapshot{
		byShard: map[ShardIdent]*destinationState{},
		router:  destinationRouter{workchains: map[int32]*routeNode{}},
	})

	return internals
}

// ReconcileDestinations atomically replaces the tracked topology. Exact
// duplicates are folded; surviving destinations retain their queue state.
func (i *Internals) ReconcileDestinations(destinations []ShardIdent) error {
	unique := make(map[ShardIdent]struct{}, len(destinations))
	ordered := make([]ShardIdent, 0, len(destinations))
	for _, destination := range destinations {
		if err := validateDestination(destination); err != nil {
			return err
		}
		if _, exists := unique[destination]; exists {
			continue
		}

		unique[destination] = struct{}{}
		ordered = append(ordered, destination)
	}
	slices.SortFunc(ordered, CompareShardIdent)

	i.reconcileMu.Lock()
	defer i.reconcileMu.Unlock()

	previous := i.snapshot.Load()
	next := &internalsSnapshot{
		destinations: make([]destinationEntry, len(ordered)),
		byShard:      make(map[ShardIdent]*destinationState, len(ordered)),
		router:       destinationRouter{workchains: map[int32]*routeNode{}},
	}
	for index, shard := range ordered {
		state := previous.byShard[shard]
		if state == nil {
			state = newDestinationState(shard)
		}
		next.destinations[index] = destinationEntry{shard: shard, state: state}
		next.byShard[shard] = state
		next.router.insert(shard, index)
	}
	i.snapshot.Store(next)
	i.log.Debug().Int("destinations", len(next.destinations)).Msg("internal-message topology reconciled")

	return nil
}

// VisitDestinations walks one immutable topology snapshot without exposing or
// copying its backing slice. Returning false stops the walk.
func (i *Internals) VisitDestinations(visit func(ShardIdent) bool) {
	snapshot := i.snapshot.Load()
	for index := range snapshot.destinations {
		if !visit(snapshot.destinations[index].shard) {
			return
		}
	}
}

// DeltasFromBlockRoot parses one source block once and routes every queue
// operation through the current immutable destination trie.
func (i *Internals) DeltasFromBlockRoot(
	source ShardIdent,
	ref SourceRef,
	root *cell.Cell,
	startLT uint64,
) ([]RoutedDelta, error) {
	return routedDeltasFromBlockRoot(root, startLT, source, ref, i.snapshot.Load())
}

// SeedsFromStateRoot walks one source out-queue once and routes every entry
// through the current immutable destination trie.
func (i *Internals) SeedsFromStateRoot(
	source ShardIdent,
	top SourceRef,
	root *cell.Cell,
) ([]RoutedSeed, uint64, error) {
	return routedSeedsFromStateRoot(root, source, top, i.snapshot.Load())
}

// SourceTop reports the applied source position inside one destination.
func (i *Internals) SourceTop(destination, source ShardIdent) (SourceRef, error) {
	state, err := i.destination(destination)
	if err != nil {
		return SourceRef{}, err
	}

	return state.sourceTop(source)
}

// ValidateSourceRef verifies that an exact committed source position remains
// reconstructable inside one destination. A run retains a bounded root-hash
// history, so callers need not regress its applied top to build on an older
// consensus predecessor.
func (i *Internals) ValidateSourceRef(destination, source ShardIdent, ref SourceRef) error {
	state, err := i.destination(destination)
	if err != nil {
		return err
	}

	return state.validateSourceRef(source, ref)
}

// Seed replaces one destination/source run with an exact routed state view.
func (i *Internals) Seed(
	destination, source ShardIdent,
	top SourceRef,
	messages []*InternalMessage,
	queueTotal uint64,
) error {
	state, err := i.destination(destination)
	if err != nil {
		return err
	}
	return state.seed(source, top, messages, queueTotal)
}

// ApplyBlock advances one destination/source run by one routed block delta.
func (i *Internals) ApplyBlock(
	destination, source ShardIdent,
	ref SourceRef,
	delta *InternalsDelta,
) error {
	state, err := i.destination(destination)
	if err != nil {
		return err
	}
	return state.applyBlock(source, ref, delta)
}

// DropSource forgets one destination/source run.
func (i *Internals) DropSource(destination, source ShardIdent) error {
	state, err := i.destination(destination)
	if err != nil {
		return err
	}
	state.dropSource(source)

	return nil
}

// PruneSources drops source runs rejected by keep in one destination.
func (i *Internals) PruneSources(destination ShardIdent, keep func(ShardIdent) bool) error {
	state, err := i.destination(destination)
	if err != nil {
		return err
	}
	state.pruneSources(keep)

	return nil
}

// OpenBranch creates an isolated speculative queue lease for one validator
// session. The lease pins the destination state and routing rule that existed
// at admission time, so a later topology publication cannot reroute candidate
// content or invalidate already authenticated source snapshots.
func (i *Internals) OpenBranch(destination ShardIdent) (*Branch, error) {
	state, err := i.destination(destination)
	if err != nil {
		return nil, err
	}

	routing := &internalsSnapshot{
		destinations: []destinationEntry{{shard: destination, state: state}},
		byShard:      map[ShardIdent]*destinationState{destination: state},
		router:       destinationRouter{workchains: map[int32]*routeNode{}},
	}
	routing.router.insert(destination, 0)

	return newBranch(destination, state, routing), nil
}

// AddCandidate records one destination-scoped speculative queue delta.
func (i *Internals) AddCandidate(destination ShardIdent, request CandidateRequest) error {
	state, err := i.destination(destination)
	if err != nil {
		return err
	}

	return state.addCandidate(request)
}

// DropCandidate forgets one destination-scoped speculative delta.
func (i *Internals) DropCandidate(destination ShardIdent, id [32]byte) error {
	state, err := i.destination(destination)
	if err != nil {
		return err
	}
	state.dropCandidate(id)

	return nil
}

// DropCandidates forgets candidate branches under one destination lock.
// Missing roots are already absent and therefore idempotent.
func (i *Internals) DropCandidates(destination ShardIdent, ids [][32]byte) error {
	state, err := i.destination(destination)
	if err != nil {
		return err
	}
	state.dropCandidates(ids)

	return nil
}

// Cut merges the exact source views of one destination.
func (i *Internals) Cut(destination ShardIdent, request CutRequest) (*Cut, error) {
	state, err := i.destination(destination)
	if err != nil {
		return nil, err
	}

	return state.cut(request)
}

// Stats returns aggregate counters across the atomically published topology.
func (i *Internals) Stats() InternalsStats {
	snapshot := i.snapshot.Load()
	stats := InternalsStats{Destinations: len(snapshot.destinations)}
	for index := range snapshot.destinations {
		section := snapshot.destinations[index].state.statsSnapshot()
		stats.Sources += section.Sources
		stats.Entries += section.Entries
		stats.Removed += section.Removed
		stats.Candidates += section.Candidates
		stats.Seeds += section.Seeds
		stats.AppliedBlocks += section.AppliedBlocks
		stats.AppliedAdded += section.AppliedAdded
		stats.AppliedRemoved += section.AppliedRemoved
		stats.CandidatesAdded += section.CandidatesAdded
		stats.CandidatesDropped += section.CandidatesDropped
		stats.Cuts += section.Cuts
		stats.CutMessages += section.CutMessages
		stats.CutFailures += section.CutFailures
		stats.Compactions += section.Compactions
		stats.CutStaleHistory += section.CutStaleHistory
	}

	return stats
}

func (i *Internals) destination(shard ShardIdent) (*destinationState, error) {
	state := i.snapshot.Load().byShard[shard]
	if state == nil {
		return nil, fmt.Errorf("%w: destination %d:%016x", ErrNotFound, shard.Workchain, shard.Shard)
	}

	return state, nil
}

func validateDestination(destination ShardIdent) error {
	if destination.Shard == 0 {
		return fmt.Errorf("msgpool: invalid zero destination shard")
	}
	depth := 63 - bits.TrailingZeros64(destination.Shard)
	if depth > 60 {
		return fmt.Errorf("msgpool: destination %d:%016x has unsupported depth %d",
			destination.Workchain, destination.Shard, depth)
	}
	if destination.Workchain == -1 && destination.Shard != ShardAll {
		return fmt.Errorf("msgpool: masterchain destination is not the root shard")
	}

	return nil
}

type destinationRouter struct {
	workchains map[int32]*routeNode
}

type routeNode struct {
	destinations []int
	children     [2]*routeNode
}

func (r *destinationRouter) insert(destination ShardIdent, index int) {
	node := r.workchains[destination.Workchain]
	if node == nil {
		node = &routeNode{}
		r.workchains[destination.Workchain] = node
	}
	depth := 63 - bits.TrailingZeros64(destination.Shard)
	for bitIndex := 0; bitIndex < depth; bitIndex++ {
		bit := destination.Shard >> (63 - bitIndex) & 1
		if node.children[bit] == nil {
			node.children[bit] = &routeNode{}
		}
		node = node.children[bit]
	}
	node.destinations = append(node.destinations, index)
}

// walk visits every destination covering the prefix. Overlapping prepared
// parent/children live on the same root-to-leaf path and are all returned.
func (r destinationRouter) walk(prefix AccountPrefix, visit func(int)) {
	node := r.workchains[prefix.Workchain]
	if node == nil {
		return
	}
	for depth := 0; node != nil; depth++ {
		for _, destination := range node.destinations {
			visit(destination)
		}
		if depth == 64 {
			return
		}
		bit := prefix.Prefix >> (63 - depth) & 1
		node = node.children[bit]
	}
}
