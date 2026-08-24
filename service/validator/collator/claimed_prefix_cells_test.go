package collator

import (
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/msgpool"
)

// collateToValidationClosure drives the real pipeline to the instant before
// traceValidationClosure runs — cleanup has walked the claimed prefix, the
// accounts are finished and the Merkle update exists — and hands the collation
// back so one closure task can be measured on its own. finish() cannot be used
// for this: it seals the read set on the way out, and the read set is what the
// assertions are about.
func collateToValidationClosure(tb testing.TB, req ShardRequest) *collation {
	tb.Helper()

	c := collateThroughAccounts(tb, testBuilder(), req, newPhaseTimer())
	if err := c.flushDescriptors(); err != nil {
		tb.Fatalf("flush descriptors: %v", err)
	}
	if err := c.flushQueueDeletes(); err != nil {
		tb.Fatalf("flush queue deletes: %v", err)
	}
	if err := c.limits.addProof(c.outQueue.RootCell()); err != nil {
		tb.Fatalf("final outbound queue proof: %v", err)
	}
	if err := c.limits.addProof(c.dispatchQueue.RootCell()); err != nil {
		tb.Fatalf("final dispatch queue proof: %v", err)
	}
	parts, err := c.buildStateAndBlockParts()
	if err != nil {
		tb.Fatalf("build state and block parts: %v", err)
	}
	if err = c.limits.addProof(parts.state); err != nil {
		tb.Fatalf("final state proof: %v", err)
	}

	return c
}

func recordedHashSet(tb testing.TB, c *collation) map[cell.Hash]struct{} {
	tb.Helper()

	hashes := c.usage.Hashes()
	if len(hashes) == 0 {
		tb.Fatal("the read set is empty, so nothing it is compared against means anything")
	}
	set := make(map[cell.Hash]struct{}, len(hashes))
	for _, hash := range hashes {
		set[hash] = struct{}{}
	}
	return set
}

// The set this pass records is the reason it exists. Omitting cells from it is
// what produced 65 self-rejected candidates and 65 "prunned branch" rejects
// from the reference implementation, so serving the cells out of cleanup's walk
// instead of walking again is only admissible if the two produce the same set,
// cell for cell, in the read set and in the collated-proof estimator behind it.
//
// Both arms are real collations of the same request, driven to the same point;
// one takes the replay and the other is forced through the walk by dropping the
// collection. The pre-closure read sets are compared first, so a difference
// afterwards cannot be blamed on the two builds having diverged earlier.
func TestClaimedPrefixReplayRecordsExactlyTheWalksCells(t *testing.T) {
	req := benchMainnetCollatedRequest(t, 3)

	replayed := collateToValidationClosure(t, req)
	walked := collateToValidationClosure(t, req)

	if !replayed.claimedPrefix.serves(replayed.oldOutQueue, replayed.shard, replayed.processedClaim) {
		t.Fatal("the fixture does not take the replay path, so this test measures nothing")
	}
	collected := len(replayed.claimedPrefix.cells)
	if collected == 0 {
		t.Fatal("the collection is empty, so the replay records nothing and the comparison is vacuous")
	}
	// The control arm: no collection, so the closure has to walk for itself.
	walked.claimedPrefix.release()
	if walked.claimedPrefix.serves(walked.oldOutQueue, walked.shard, walked.processedClaim) {
		t.Fatal("the control arm still takes the replay")
	}

	beforeReplay := recordedHashSet(t, replayed)
	beforeWalk := recordedHashSet(t, walked)
	if !maps.Equal(beforeReplay, beforeWalk) {
		t.Fatalf("the two collations read different cells before the closure: %d against %d",
			len(beforeReplay), len(beforeWalk))
	}

	if err := replayed.traceProcessedQueueValidationClosure(); err != nil {
		t.Fatalf("replay arm: %v", err)
	}
	if err := walked.traceProcessedQueueValidationClosure(); err != nil {
		t.Fatalf("walk arm: %v", err)
	}

	afterReplay := recordedHashSet(t, replayed)
	afterWalk := recordedHashSet(t, walked)
	if added := len(afterWalk) - len(beforeWalk); added == 0 {
		t.Fatal("the walk arm recorded nothing new, so the two sets agree only because neither moved")
	}
	for hash := range afterWalk {
		if _, ok := afterReplay[hash]; !ok {
			t.Fatalf("the replay is missing %x, which the walk recorded — this is the under-recording "+
				"class that makes validators reject the candidate on a pruned branch", hash)
		}
	}
	for hash := range afterReplay {
		if _, ok := afterWalk[hash]; !ok {
			t.Fatalf("the replay recorded %x, which the walk does not — the collated proof is wider "+
				"than the validator's own read set", hash)
		}
	}

	// The estimator is fed from the same record callback and is what actually
	// selects the previous-state proof, so its answer has to agree cell for cell
	// as well.
	replayLoaded := replayed.collatedProofEstimate.loadedHashes()
	walkLoaded := walked.collatedProofEstimate.loadedHashes()
	for hash := range afterWalk {
		if replayLoaded.loaded(hash) != walkLoaded.loaded(hash) {
			t.Fatalf("the collated-proof estimator disagrees about %x: replay %v, walk %v",
				hash, replayLoaded.loaded(hash), walkLoaded.loaded(hash))
		}
	}

	t.Logf("claimed prefix: %d cells replayed, %d cells recorded by the closure, read set %d -> %d",
		collected, len(afterWalk)-len(beforeWalk), len(beforeWalk), len(afterWalk))
}

// The other shape of the same question: a block whose claimed prefix actually
// contains work. Cleanup's walk then suspends the ignore scope around each
// dequeue, so a leaf's cells are recorded there as block content rather than
// observed as prefix cells, and the two halves have to add up to the same set
// the closure's own walk would record.
func TestClaimedPrefixReplayMatchesTheWalkWhenCleanupDequeues(t *testing.T) {
	const stale = 6

	drain := func(t *testing.T) *collation {
		t.Helper()

		c := claimedDrainFixture(t, stale)
		if err := c.cleanupClaimedLocalDequeues(); err != nil {
			t.Fatalf("claimed-prefix cleanup: %v", err)
		}
		if c.stats.QueueCleaned != stale {
			t.Fatalf("cleanup dequeued %d entries, want %d — the dequeue window is not exercised",
				c.stats.QueueCleaned, stale)
		}
		return c
	}

	replayed := drain(t)
	walked := drain(t)
	if !replayed.claimedPrefix.serves(replayed.oldOutQueue, replayed.shard, replayed.processedClaim) {
		t.Fatal("cleanup did not stamp a collection, so the replay is not under test")
	}
	walked.claimedPrefix.release()

	before := recordedHashSet(t, walked)
	if !maps.Equal(recordedHashSet(t, replayed), before) {
		t.Fatal("the two fixtures read different cells during cleanup")
	}

	if err := replayed.traceProcessedQueueValidationClosure(); err != nil {
		t.Fatalf("replay arm: %v", err)
	}
	if err := walked.traceProcessedQueueValidationClosure(); err != nil {
		t.Fatalf("walk arm: %v", err)
	}

	afterReplay := recordedHashSet(t, replayed)
	afterWalk := recordedHashSet(t, walked)
	if !maps.Equal(afterReplay, afterWalk) {
		t.Fatalf("the replay recorded %d cells and the walk %d over a prefix cleanup drained",
			len(afterReplay), len(afterWalk))
	}
	t.Logf("drained prefix: %d entries dequeued, read set %d -> %d", stale, len(before), len(afterWalk))
}

// The collection must hold the WALK's reads and nothing else, and today nothing
// cleanup's visit callback does happens to read a cell — alreadyProcessed
// answers from parsed records, and the dequeue runs with the ignore scope
// closed, where the observer is not consulted at all. So the bracket has no
// runtime consequence to assert against, and the first traced read added inside
// that callback would silently widen the collated proof by cells the validator
// never opens.
//
// It is pinned at the call site instead, the way this package pins the other
// invariants that only a source edit can break.
func TestClaimedPrefixCollectionIsSuspendedInsideCleanupsCallback(t *testing.T) {
	parsed, err := parser.ParseFile(token.NewFileSet(),
		packageSourceFile(t, "cleanup.go"), nil, 0)
	if err != nil {
		t.Fatalf("parse cleanup.go: %v", err)
	}

	var callback *ast.FuncLit
	for _, decl := range parsed.Decls {
		declared, ok := decl.(*ast.FuncDecl)
		if !ok || declared.Name.Name != "cleanupClaimedLocalDequeues" {
			continue
		}
		ast.Inspect(declared.Body, func(node ast.Node) bool {
			call, isCall := node.(*ast.CallExpr)
			if !isCall {
				return true
			}
			name, isIdent := call.Fun.(*ast.Ident)
			if !isIdent || name.Name != "walkSemanticQueuePrefix" || len(call.Args) == 0 {
				return true
			}
			if lit, isLit := call.Args[len(call.Args)-1].(*ast.FuncLit); isLit {
				callback = lit
			}
			return true
		})
	}
	if callback == nil {
		t.Fatal("cleanupClaimedLocalDequeues no longer passes a literal callback to walkSemanticQueuePrefix")
	}
	if len(callback.Body.List) < 2 {
		t.Fatal("the callback is too short to open with the suspend bracket")
	}

	first, ok := callback.Body.List[0].(*ast.ExprStmt)
	if !ok || callSelector(first.X) != "suspend" {
		t.Fatalf("the callback opens with %T instead of suspending the claimed-prefix collection",
			callback.Body.List[0])
	}
	second, ok := callback.Body.List[1].(*ast.DeferStmt)
	if !ok || callSelector(second.Call) != "resume" {
		t.Fatalf("the callback does not defer resuming the collection right after suspending it, "+
			"so an early return leaves it suspended for the rest of the walk: %T", callback.Body.List[1])
	}
}

// callSelector names the method a call expression invokes, or "" for anything
// that is not a method call.
func callSelector(node ast.Node) string {
	call, ok := node.(*ast.CallExpr)
	if !ok {
		return ""
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return ""
	}
	return selector.Sel.Name
}

// prefixLoadCounter is a store-shaped predecessor that attributes every load to
// whichever claimed-prefix walk is on the stack. It is the only way to see the
// property this change is for: the prefix is resolved once per build rather than
// once per pass over it, and a share of a profile cannot say that.
// The counters are locked because the loader is reached from the collation's
// parallel account lanes and its sibling prefetch, not only from the two walks
// this test is about. Indexing happens before the build and needs no lock.
type prefixLoadCounter struct {
	records map[cell.Hash][]byte

	mu      sync.Mutex
	total   int
	cleanup int
	closure int
}

func newPrefixLoadCounter() *prefixLoadCounter {
	return &prefixLoadCounter{records: map[cell.Hash][]byte{}}
}

func (l *prefixLoadCounter) index(tb testing.TB, c *cell.Cell) {
	tb.Helper()

	if c == nil {
		return
	}
	hash := c.HashKey()
	if _, known := l.records[hash]; known {
		return
	}
	record, err := storage.CellRecordFromCell(c)
	if err != nil {
		tb.Fatalf("cell record: %v", err)
	}
	l.records[hash] = storage.EncodeCellRecord(record)
	for i := range int(c.RefsNum()) {
		ref, refErr := c.PeekRef(i)
		if refErr != nil {
			tb.Fatalf("peek ref: %v", refErr)
		}
		l.index(tb, ref)
	}
}

// callerPass names the claimed-prefix pass this load was made under, by walking
// the stack rather than by counting frames: the walk sits several closures deep
// and its depth is not something a test may pin.
func callerPass() string {
	var pcs [96]uintptr
	frames := runtime.CallersFrames(pcs[:runtime.Callers(3, pcs[:])])
	for {
		frame, more := frames.Next()
		switch {
		case strings.Contains(frame.Function, "cleanupClaimedLocalDequeues"):
			return "cleanup"
		case strings.Contains(frame.Function, "traceProcessedQueueValidationClosure"):
			return "closure"
		}
		if !more {
			return ""
		}
	}
}

func (l *prefixLoadCounter) loader() cell.LazyCellLoader {
	var self cell.LazyCellLoader
	self = func(hash cell.Hash) (*cell.Cell, error) {
		pass := callerPass()
		l.mu.Lock()
		l.total++
		switch pass {
		case "cleanup":
			l.cleanup++
		case "closure":
			l.closure++
		}
		l.mu.Unlock()

		encoded, known := l.records[hash]
		if !known {
			return nil, cell.ErrLazyRefNotFound
		}
		return storage.DecodeLazyCellRecordTrusted(hash[:], encoded, self)
	}
	return self
}

func (l *prefixLoadCounter) root(tb testing.TB, c *cell.Cell) *cell.Cell {
	tb.Helper()

	l.index(tb, c)
	hash := c.HashKey()
	out, err := storage.DecodeLazyCellRecordTrusted(hash[:], l.records[hash], l.loader())
	if err != nil {
		tb.Fatalf("lazy root: %v", err)
	}
	return out
}

// The prize: on a predecessor that lives in the store rather than in memory,
// the claimed prefix is resolved once. Cleanup still pays for it, because
// cleanup is the pass that has to find the entries; the closure pays nothing,
// because it records what cleanup already parsed.
//
// Cleanup's own count is asserted non-zero in the same breath, so a fixture
// that stopped exercising the prefix at all cannot read as a win.
func TestClaimedPrefixReplayResolvesThePrefixOncePerBuild(t *testing.T) {
	req := benchMainnetCollatedRequest(t, 3)
	loads := newPrefixLoadCounter()
	req.Previous.State = loads.root(t, req.Previous.State)

	candidate, err := testBuilder().BuildShard(t.Context(), req)
	if err != nil {
		t.Fatalf("collate a store-shaped predecessor: %v", err)
	}
	assertBenchMainnetCollated(t, candidate)

	loads.mu.Lock()
	total, cleanup, closure := loads.total, loads.cleanup, loads.closure
	loads.mu.Unlock()

	if cleanup == 0 {
		t.Fatal("cleanup loaded nothing over the claimed prefix, so this fixture measures nothing")
	}
	if closure != 0 {
		t.Fatalf("the validation closure made %d loads over the claimed prefix; cleanup already parsed "+
			"those cells and the closure is meant to record them, not resolve them again "+
			"(cleanup %d, total %d)", closure, cleanup, total)
	}
	t.Logf("store-shaped predecessor: %d loads total, %d under cleanup's prefix walk, %d under the closure",
		total, cleanup, closure)
}

// The stamp is a decision, not an assumption. A collection that did not come
// from a walk over this queue, this shard and this bound must send the closure
// back to walking, and the set it then records must still be the whole one.
func TestClaimedPrefixStampRejectsAWalkItDidNotMake(t *testing.T) {
	req := benchMainnetCollatedRequest(t, 1)

	cases := []struct {
		name       string
		breakStamp func(*collation)
	}{
		{"a bound recomputed after cleanup", func(c *collation) {
			c.claimedPrefix.bound.lt++
		}},
		{"another target shard", func(c *collation) {
			c.claimedPrefix.shard = msgpool.ShardIdent{Workchain: c.shard.Workchain, Shard: c.shard.Shard ^ 1}
		}},
		{"another predecessor queue", func(c *collation) {
			c.claimedPrefix.root = cell.Hash{}
		}},
		{"a walk that stopped early", func(c *collation) {
			c.claimedPrefix.complete = false
		}},
	}

	reference := collateToValidationClosure(t, req)
	reference.claimedPrefix.release()
	if err := reference.traceProcessedQueueValidationClosure(); err != nil {
		t.Fatalf("reference walk: %v", err)
	}
	want := recordedHashSet(t, reference)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := collateToValidationClosure(t, req)
			if !c.claimedPrefix.serves(c.oldOutQueue, c.shard, c.processedClaim) {
				t.Fatal("the fixture does not take the replay before the stamp is broken")
			}
			tc.breakStamp(c)
			if c.claimedPrefix.serves(c.oldOutQueue, c.shard, c.processedClaim) {
				t.Fatal("the stamp still serves the closure after it was broken")
			}
			if err := c.traceProcessedQueueValidationClosure(); err != nil {
				t.Fatalf("fallback walk: %v", err)
			}
			if got := recordedHashSet(t, c); !maps.Equal(got, want) {
				t.Fatalf("the fallback recorded %d cells, the reference walk %d", len(got), len(want))
			}
		})
	}
}

// A stamp on a collection nothing filled is the worst outcome available here:
// the closure would take it, record an empty set and ship a proof missing the
// whole claimed prefix. Three ways to reach one, all of them ordinary — the two
// topologies where cleanup returns before its walk, and the configuration where
// the observer is never installed because there is no proof to widen.
func TestClaimedPrefixIsUnstampedWhenCleanupDidNotWalk(t *testing.T) {
	req := benchMainnetCollatedRequest(t, 1)

	for _, tc := range []struct {
		name string
		skip func(*collation)
	}{
		{"after a merge", func(c *collation) { c.topology.kind = topologyAfterMerge }},
		{"no local cleanup frontier", func(c *collation) { c.localCleanup = nil }},
		{"no collated proof to widen", func(c *collation) { c.fullCollated = false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := collateBeforeAccounts(t, testBuilder(), req, newPhaseTimer())
			// collateBeforeAccounts has already run cleanup once; undo its
			// stamp so the skipped rerun is what the assertion sees.
			c.claimedPrefix = claimedPrefixCells{}
			tc.skip(c)
			if err := c.cleanupClaimedLocalDequeues(); err != nil {
				t.Fatalf("cleanup: %v", err)
			}
			if c.claimedPrefix.serves(c.oldOutQueue, c.shard, c.processedClaim) {
				t.Fatal("cleanup returned without walking and still stamped the collection")
			}
		})
	}
}
