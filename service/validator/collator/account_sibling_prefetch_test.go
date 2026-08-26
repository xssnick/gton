package collator

import (
	"bytes"
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// lazyPredecessorStore re-serves a predecessor state the way the node's cell
// store does — every reference a lazy stub resolved through a loader — and
// counts the loads. Nothing else in the package can answer the question the
// prefetch exists for: how many cells did THIS phase ask storage for. A
// resident fixture answers zero to that question no matter what the code does.
type lazyPredecessorStore struct {
	records map[cell.Hash][]byte

	mu    sync.Mutex
	count int
}

func newLazyPredecessorStore(tb testing.TB, root *cell.Cell) *lazyPredecessorStore {
	tb.Helper()

	store := &lazyPredecessorStore{records: map[cell.Hash][]byte{}}
	store.index(tb, root)
	return store
}

func (s *lazyPredecessorStore) index(tb testing.TB, c *cell.Cell) {
	tb.Helper()
	if c == nil {
		return
	}
	hash := c.HashKey()
	if _, ok := s.records[hash]; ok {
		return
	}
	record, err := storage.CellRecordFromCell(c)
	if err != nil {
		tb.Fatalf("encode cell record: %v", err)
	}
	s.records[hash] = storage.EncodeCellRecord(record)
	for i := 0; i < int(c.RefsNum()); i++ {
		ref, err := c.PeekRef(i)
		if err != nil {
			tb.Fatalf("peek ref %d: %v", i, err)
		}
		s.index(tb, ref)
	}
}

func (s *lazyPredecessorStore) loads() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

func (s *lazyPredecessorStore) loader() cell.LazyCellLoader {
	var self cell.LazyCellLoader
	self = func(hash cell.Hash) (*cell.Cell, error) {
		s.mu.Lock()
		s.count++
		s.mu.Unlock()
		record, ok := s.records[hash]
		if !ok {
			return nil, cell.ErrLazyRefNotFound
		}
		return storage.DecodeLazyCellRecordTrusted(hash[:], record, self)
	}
	return self
}

func (s *lazyPredecessorStore) root(tb testing.TB, root *cell.Cell) *cell.Cell {
	tb.Helper()

	hash := root.HashKey()
	lazy, err := storage.DecodeLazyCellRecordTrusted(hash[:], s.records[hash], s.loader())
	if err != nil {
		tb.Fatalf("lazify predecessor root: %v", err)
	}
	return lazy
}

// storeShapedCollatedRequest is the mainnet collated fixture with its
// predecessor state served through a counting store instead of held resident.
func storeShapedCollatedRequest(tb testing.TB) (ShardRequest, *lazyPredecessorStore) {
	tb.Helper()

	req := benchMainnetCollatedRequest(tb, 3)
	store := newLazyPredecessorStore(tb, req.Previous.State)
	req.Previous.State = store.root(tb, req.Previous.State)
	return req, store
}

// The bulk ShardAccounts write re-hashes every fork on a touched account's path
// from both of its branches, so it has to read the branch the update did not
// descend into. On a store-backed predecessor those untouched siblings are the
// entire storage traffic of the stage. The prefetch resolves them while the
// machine is still executing, and this pins that the stage itself then asks
// storage for nothing at all.
func TestAccountSiblingPrefetchRemovesStageStoreReads(t *testing.T) {
	req, store := storeShapedCollatedRequest(t)
	c := collateBeforeAccounts(t, testBuilder(), req, newPhaseTimer())

	// The prefetch loads are storage reads too. Settling them before the
	// bracket is what makes the bracket mean "what the stage read", rather than
	// "what the stage read plus whatever the prefetch had not finished yet".
	resolved := c.accountSiblings.join()
	if len(resolved) == 0 {
		t.Fatal("the prefetch resolved nothing; the fixture is not store-shaped")
	}

	before := store.loads()
	if err := c.finishAccounts(); err != nil {
		t.Fatalf("finish accounts: %v", err)
	}
	if reads := store.loads() - before; reads != 0 {
		t.Fatalf("the final account stage asked storage for %d cells, want 0 — "+
			"the prefetch is meant to have resolved every one of them beforehand", reads)
	}
	if _, err := c.finish(); err != nil {
		t.Fatalf("finish: %v", err)
	}
}

// The prefetch is an optimization, never a precondition: a sibling that did not
// make it into the handoff is loaded by the bulk write exactly as it was before
// the prefetch existed, and the block does not notice either way. Emptying the
// whole handoff is the strongest form of "did not make it" — a failed loader, a
// cancelled context and an unfinished job all reduce to it.
func TestAccountSiblingPrefetchMissingHandoffFallsBackToLoading(t *testing.T) {
	withHandoff, prefetchedReads := prefetchArmCandidate(t, false)
	withoutHandoff, loadedReads := prefetchArmCandidate(t, true)

	if prefetchedReads != 0 {
		t.Fatalf("the stage read %d cells with the handoff intact, want 0", prefetchedReads)
	}
	if loadedReads == 0 {
		t.Fatal("the stage read nothing with an empty handoff; it is not loading the siblings itself")
	}
	if !bytes.Equal(withHandoff.BlockBOC, withoutHandoff.BlockBOC) {
		t.Fatalf("dropping the prefetch handoff moved the block: %d bytes against %d",
			len(withHandoff.BlockBOC), len(withoutHandoff.BlockBOC))
	}
	if !bytes.Equal(withHandoff.CollatedData, withoutHandoff.CollatedData) {
		t.Fatalf("dropping the prefetch handoff moved the collated data: %d bytes against %d",
			len(withHandoff.CollatedData), len(withoutHandoff.CollatedData))
	}
	t.Logf("stage store reads: %d with the handoff, %d without; candidate %d block / %d collated bytes either way",
		prefetchedReads, loadedReads, len(withHandoff.BlockBOC), len(withHandoff.CollatedData))
}

// prefetchArmCandidate builds the store-shaped fixture and reports the
// candidate together with what the final account stage read from storage.
// Dropping the handoff reproduces every way the prefetch can come up empty
// without having to make a loader fail at a moment nothing can pin down.
func prefetchArmCandidate(tb testing.TB, dropHandoff bool) (*Candidate, int) {
	tb.Helper()

	req, store := storeShapedCollatedRequest(tb)
	c := collateBeforeAccounts(tb, testBuilder(), req, newPhaseTimer())
	if resolved := c.accountSiblings.join(); len(resolved) == 0 {
		tb.Fatal("the prefetch resolved nothing; the fixture is not store-shaped")
	}
	if dropHandoff {
		c.accountSiblings.cells = nil
	}

	before := store.loads()
	if err := c.finishAccounts(); err != nil {
		tb.Fatalf("finish accounts: %v", err)
	}
	reads := store.loads() - before
	candidate, err := c.finish()
	if err != nil {
		tb.Fatalf("finish: %v", err)
	}
	return candidate, reads
}

// A nil or still-lazy cell in a loaded-path entry is refused by the bulk
// resolver, which ends the collation instead of slowing it down. The handoff is
// therefore a correctness surface, not only a performance one.
func TestAccountSiblingPrefetchHandoffIsResident(t *testing.T) {
	req, _ := storeShapedCollatedRequest(t)
	c := collateBeforeAccounts(t, testBuilder(), req, newPhaseTimer())

	resolved := c.accountSiblings.join()
	if len(resolved) == 0 {
		t.Fatal("the prefetch resolved nothing; the fixture is not store-shaped")
	}
	for i, sibling := range resolved {
		if sibling == nil {
			t.Fatalf("prefetched sibling %d is nil", i)
		}
		if sibling.IsLazy() {
			t.Fatalf("prefetched sibling %d is still lazy", i)
		}
	}
}

// An abandoned collation must leave nothing behind. prepareShardPhases can fail
// after the lanes have been opened, and retryUnderSizeLimit builds a fresh
// collation per attempt, so a prefetch nobody joins is a normal outcome rather
// than an exceptional one. Every worker must therefore run itself out without
// anything waiting on it — which is what makes the fire-and-forget shape safe
// and what a worker pool parked on a channel would not give.
func TestAccountSiblingPrefetchWorkersFinishWhenNobodyJoins(t *testing.T) {
	req, _ := storeShapedCollatedRequest(t)
	c := collateBeforeAccounts(t, testBuilder(), req, newPhaseTimer())
	if len(c.lanes) == 0 {
		t.Fatal("the fixture opened no account lanes")
	}

	// The collation is dropped here, exactly as a failed build drops it. join
	// is the test's own observation of the workers, not the production handoff:
	// a worker that could park forever would never let it return.
	finished := make(chan struct{})
	go func() {
		c.accountSiblings.join()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(30 * time.Second):
		t.Fatal("prefetch workers are still running long after the collation was abandoned")
	}
}

// A cancelled collation must not keep asking storage for cells it will never
// use. The workers check the context per job, so a build abandoned early stops
// paying for the prefetch it started.
func TestAccountSiblingPrefetchStopsOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	req, store := storeShapedCollatedRequest(t)
	c := collateBeforeAccounts(t, testBuilder(), req, newPhaseTimer())
	// The collation's own prefetch is settled first, so the reads counted below
	// are the cancelled prefetcher's and nothing else.
	c.accountSiblings.join()

	prefetch := newAccountSiblingPrefetch(ctx)
	before := store.loads()
	for _, lane := range c.lanes {
		prefetch.submit(lane.accountPath)
	}
	if resolved := prefetch.join(); len(resolved) != 0 {
		t.Fatalf("a cancelled prefetch resolved %d cells", len(resolved))
	}
	if reads := store.loads() - before; reads != 0 {
		t.Fatalf("a cancelled prefetch read %d cells from storage", reads)
	}
}

// The prefetch reads for its own sake, and those reads must stay outside the
// collation's read set: the set is the sole record the collated proof is built
// from, so a proof that grew by however far the prefetch happened to get would
// move the candidate's bytes with the state of the cell store. The workers are
// therefore handed a stub with no trace on it.
//
// Whether a path node carries a recorder trace is not this walk's property to
// rely on — the production spines happen not to today — so the guard is driven
// here with one that does, which is the shape a rewiring of either recorder
// would produce.
func TestAccountSiblingPrefetchLoadsCannotEnterTheReadSet(t *testing.T) {
	left := cell.BeginCell().MustStoreUInt(0xA1, 8).EndCell()
	right := cell.BeginCell().MustStoreUInt(0xB2, 8).EndCell()
	fork := cell.BeginCell().MustStoreUInt(1, 1).MustStoreRef(left).MustStoreRef(right).EndCell()

	// Store-backed, because a resident fork has no lazy reference to prefetch.
	store := newLazyPredecessorStore(t, fork)
	rs := cell.NewReadSet(store.root(t, fork))
	node, err := rs.Root().Prewarm()
	if err != nil {
		t.Fatalf("resolve the path node: %v", err)
	}
	if node.Trace() == nil {
		t.Fatal("the path node carries no recorder trace, so nothing here could observe the leak")
	}

	prefetch := newAccountSiblingPrefetch(t.Context())
	prefetch.submit([]*cell.Cell{node})
	resolved := prefetch.join()
	if len(resolved) != 2 {
		t.Fatalf("the prefetch resolved %d siblings, want the fork's 2", len(resolved))
	}

	// Resolving is not what records: the recorder is told on a parse, and
	// parsing a handed-over sibling is exactly what the bulk write does with it,
	// to recombine the augmentation of the fork above it.
	for i, sibling := range resolved {
		if _, err = sibling.BeginParse(); err != nil {
			t.Fatalf("parse prefetched sibling %d: %v", i, err)
		}
	}
	for _, hash := range []cell.Hash{left.HashKey(), right.HashKey()} {
		if _, recorded := rs.Contains(hash); recorded {
			t.Fatalf("prefetched sibling %x entered the read set; the collated proof would then "+
				"depend on how far the prefetch got", hash)
		}
	}
	if size := rs.Size(); size != 0 {
		t.Fatalf("the prefetch put %d cells into the read set, want none", size)
	}
}

// The per-lane build is spread over workers, so the two dictionary entries every
// lane contributes have to come out of the fan-out exactly as a sequential pass
// would have produced them. Cells are immutable and hashed at creation, and a
// worker touches only its own lane, but that is an argument; this is the check.
func TestBuildAccountLanesFanOutMatchesSequentialBuild(t *testing.T) {
	req := benchMainnetCollatedRequest(t, 3)
	c := collateBeforeAccounts(t, testBuilder(), req, newPhaseTimer())

	var lanes []*accountLane
	for _, lane := range c.lanes {
		if lane.touched {
			lanes = append(lanes, lane)
		}
	}
	if len(lanes) < 2 {
		t.Fatalf("the fixture touched %d accounts, too few to fan out", len(lanes))
	}
	// Map order is not an input to either arm, but the two arms have to be
	// compared element by element, so they are given the same order.
	slices.SortFunc(lanes, func(left, right *accountLane) int {
		return bytes.Compare(left.key[:], right.key[:])
	})

	parallel := make([]accountLaneBuild, len(lanes))
	sequential := make([]accountLaneBuild, len(lanes))
	for i, lane := range lanes {
		parallel[i] = accountLaneBuild{lane: lane}
		sequential[i] = accountLaneBuild{lane: lane}
	}
	buildAccountLanes(parallel)
	for i := range sequential {
		buildAccountLane(&sequential[i])
	}

	for i := range lanes {
		got, want := &parallel[i], &sequential[i]
		if got.err != nil || want.err != nil {
			t.Fatalf("lane %x: parallel %v, sequential %v", lanes[i].key, got.err, want.err)
		}
		if got.deleted != want.deleted {
			t.Fatalf("lane %x: deleted %v against %v", lanes[i].key, got.deleted, want.deleted)
		}
		assertSameCell(t, lanes[i].key, "account key", got.entry.Key, want.entry.Key)
		assertSameCell(t, lanes[i].key, "account value", got.entry.Value, want.entry.Value)
		if got.entry.Mode != want.entry.Mode {
			t.Fatalf("lane %x: account write mode %v against %v", lanes[i].key, got.entry.Mode, want.entry.Mode)
		}
		if !bytes.Equal(got.block.Key, want.block.Key) {
			t.Fatalf("lane %x: account block keys differ", lanes[i].key)
		}
		assertSameCell(t, lanes[i].key, "account block", got.block.Value, want.block.Value)
		assertSameCell(t, lanes[i].key, "storage stat", got.storageStat, want.storageStat)
	}
}

func assertSameCell(tb testing.TB, key [32]byte, what string, got, want *cell.Cell) {
	tb.Helper()

	switch {
	case got == nil && want == nil:
		return
	case got == nil || want == nil:
		tb.Fatalf("lane %x: %s is %v against %v", key, what, got, want)
	case got.HashKey() != want.HashKey():
		tb.Fatalf("lane %x: %s hashes to %x against %x", key, what, got.HashKey(), want.HashKey())
	}
}
