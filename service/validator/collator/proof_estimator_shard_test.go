package collator

import (
	"runtime"
	"sync"
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

// referenceProofSizeEstimator is the estimator with the sharding taken out: one
// mutex, one table, one counter, and otherwise the same rule. It is the
// reference the sharded one is held against, in the same spirit as
// TestProofSeenHashSetMatchesTheMapItReplaced holding the table against the map
// it replaced — the estimator's seen set selects the collated proof and its byte
// total feeds admission, so "one lock or sixteen, the same answers" is the whole
// specification and a single-locked implementation is the only honest statement
// of it.
//
// It states the rule as it is after selection and charging became separate
// bits: an execution read selects without charging, and the traversal that later
// reaches the same cell through the predecessor tree charges it and its
// children's boundaries. Before that split this reference and the estimator
// alike skipped that cell entirely, so the proof carried cells the estimate had
// never counted.
type referenceProofSizeEstimator struct {
	mu    sync.Mutex
	seen  proofSeenHashSet
	bytes uint64
}

func (e *referenceProofSizeEstimator) addLoadedCell(root *cell.Cell) {
	hash := root.HashKey()
	var refBuf [4]cell.Hash
	refs := refBuf[:root.RefsNum()]
	for i := range refs {
		refs[i] = root.MustRefHashAt(i)
	}
	loadedBytes := uint64(5+(root.BitsSize()+7)/8) + uint64(len(refs))*3

	e.mu.Lock()
	prior := e.seen.observe(hash, proofSeenLoaded|proofSeenSelected, proofSeenBoundary)
	if prior&proofSeenLoaded != 0 {
		e.mu.Unlock()
		return
	}
	if prior&proofSeenBoundary != 0 {
		e.bytes -= estimatedPrunedBranchBytes
	}
	e.bytes += loadedBytes
	for _, ref := range refs {
		if e.seen.observe(ref, proofSeenBoundary, 0) != 0 {
			continue
		}
		e.bytes += estimatedPrunedBranchBytes
	}
	e.mu.Unlock()
}

func (e *referenceProofSizeEstimator) addExecutionRead(loaded *cell.Cell) {
	if loaded == nil {
		return
	}
	hash := loaded.HashKey()

	e.mu.Lock()
	e.seen.observe(hash, proofSeenSelected, 0)
	e.mu.Unlock()
}

// entries lists every hash the table holds against whether it is emitted — the
// predicate the collated-proof selector walks with, which is the answer that
// matters for equality. The hash chunks are append-only and hold exactly the
// entries the slots name, so walking them is the whole set.
func (s *proofSeenHashSet) entries(into map[cell.Hash]bool) {
	for _, chunk := range s.hashes {
		for _, hash := range chunk {
			into[hash] = s.emitted(hash)
		}
	}
}

func (e *proofSizeEstimator) entries() map[cell.Hash]bool {
	out := map[cell.Hash]bool{}
	for i := range e.shards {
		e.shards[i].seen.entries(out)
	}
	return out
}

func (e *referenceProofSizeEstimator) entries() map[cell.Hash]bool {
	out := map[cell.Hash]bool{}
	e.seen.entries(out)
	return out
}

// proofEstimatorFixture is a forest with the shapes the estimator distinguishes:
// cells of every reference count, children shared between parents, and three
// levels so a cell is both a loaded cell and somebody's reference.
//
// It returns the cells to feed as loaded, in the order to feed them. Every fifth
// leaf is deliberately left out of that list: those are the cells that stay
// referenced-only, the ones charged as a fixed pruned branch and reported
// loaded=false — without them every entry in the set would end loaded and the
// comparison would never see a lost or invented bit.
func proofEstimatorFixture(t *testing.T) []*cell.Cell {
	t.Helper()

	var leaves []*cell.Cell
	for i := range 64 {
		// Sizes vary, because the loaded charge is derived from the bit count.
		padding := make([]byte, i%100)
		leaves = append(leaves, cell.BeginCell().
			MustStoreUInt(uint64(i), 32).
			MustStoreSlice(padding, uint(8*len(padding))).
			EndCell())
	}
	var parents []*cell.Cell
	for i := range 48 {
		b := cell.BeginCell().MustStoreUInt(uint64(i), 16)
		// Overlapping windows, so most children are referenced by more than one
		// parent and the "already charged as a pruned branch" path is reached.
		for r := range 1 + i%4 {
			b.MustStoreRef(leaves[(i+r*7)%len(leaves)])
		}
		parents = append(parents, b.EndCell())
	}
	var roots []*cell.Cell
	for i := range 12 {
		b := cell.BeginCell().MustStoreUInt(uint64(i), 24)
		for r := range 1 + i%4 {
			b.MustStoreRef(parents[(i*3+r)%len(parents)])
		}
		roots = append(roots, b.EndCell())
	}

	// Roots first, so their children are already standing in as pruned branches
	// when the loaded call for each child arrives and has to promote it.
	feed := append(append([]*cell.Cell{}, roots...), parents...)
	for i, leaf := range leaves {
		if i%5 == 0 {
			continue
		}
		feed = append(feed, leaf)
	}
	if len(feed) != 111 {
		t.Fatalf("fixture feeds %d cells, want 111", len(feed))
	}
	return feed
}

// The sharded estimator must be indistinguishable from the single-locked one it
// replaces, because its seen set IS the collated-proof selector (build.go feeds
// loadedHashes to buildCollatedRoots) and its byte total IS what admission
// compares against the collated-data limit. A shard that lost a hash would take
// that cell out of the emitted proof — which peers report back as a pruned
// branch while the block still verifies locally — and a byte total off by one
// term changes which messages the block admits.
func TestShardedProofEstimatorMatchesTheSingleLockedOneItReplaced(t *testing.T) {
	cells := proofEstimatorFixture(t)
	sharded := newProofSizeEstimator(0)
	reference := &referenceProofSizeEstimator{}

	// Every order the estimator sees in a collation, in one script: a first
	// load, the same cell again, an execution read arriving before its load and
	// another arriving after, and a repeat of both.
	for pass := range 3 {
		for i, c := range cells {
			switch (i + pass) % 5 {
			case 0:
				sharded.addExecutionRead(c)
				reference.addExecutionRead(c)
			case 1:
				sharded.addLoadedCell(c)
				reference.addLoadedCell(c)
				sharded.addExecutionRead(c)
				reference.addExecutionRead(c)
			default:
				sharded.addLoadedCell(c)
				reference.addLoadedCell(c)
			}
		}
	}

	assertProofEstimatorsAgree(t, sharded, reference)
}

// The same equality under the concurrency the sharding exists for. Only loaded
// cells are fed here, and that is the production shape rather than a
// convenience: addExecutionRead is called from the serial collation goroutine
// alone (execute.go recordExecutionRead), while addLoadedCell arrives from the
// dictionary batches and the five closure walks at once.
//
// The byte total is asserted exactly, not approximately, because it is
// order-independent for this feed: every term of it belongs to a single hash,
// and that hash's contribution is the same whichever observation arrives first —
// a cell reached as a reference first is charged a pruned branch and then
// promoted to its loaded size, a cell loaded first is charged its loaded size
// and the later reference charges nothing. That is the property that makes
// releasing the parent's shard before observing its children safe.
func TestShardedProofEstimatorIsUnchangedByParallelFeed(t *testing.T) {
	cells := proofEstimatorFixture(t)
	reference := &referenceProofSizeEstimator{}
	for _, c := range cells {
		reference.addLoadedCell(c)
	}

	for attempt := range 8 {
		sharded := newProofSizeEstimator(0)
		workers := max(runtime.GOMAXPROCS(0), 4)
		var wg sync.WaitGroup
		for w := range workers {
			wg.Go(func() {
				// Each worker starts at a different offset and wraps, so the
				// same cell is offered by several workers at once and every
				// worker meets a different mix of first sights and repeats.
				for i := range cells {
					sharded.addLoadedCell(cells[(i+w*attempt+w*13)%len(cells)])
				}
			})
		}
		wg.Wait()

		assertProofEstimatorsAgree(t, sharded, reference)
	}
}

func assertProofEstimatorsAgree(t *testing.T, sharded *proofSizeEstimator, reference *referenceProofSizeEstimator) {
	t.Helper()

	want := reference.entries()
	got := sharded.entries()
	// The fixture has to reach both bits, or the comparison below degenerates
	// into "every entry is loaded" and stops separating the two estimators.
	var referencedOnly int
	for _, loaded := range want {
		if !loaded {
			referencedOnly++
		}
	}
	if len(want) == 0 || referencedOnly == 0 {
		t.Fatalf("the reference estimator holds %d hashes of which %d are referenced-only; "+
			"the fixture is not exercising both bits", len(want), referencedOnly)
	}
	if len(got) != len(want) {
		t.Fatalf("sharded estimator holds %d hashes, want %d", len(got), len(want))
	}
	// Through the view, not the shards directly: loadedHashes is what selection
	// reads, so a view that routed a hash to the wrong shard would lose it from
	// the proof while the shards themselves still held it.
	view := sharded.loadedHashes()
	for hash, loaded := range want {
		gotLoaded, present := got[hash]
		if !present {
			t.Fatalf("hash %x is absent from the sharded estimator", hash[:6])
		}
		if gotLoaded != loaded {
			t.Fatalf("hash %x is loaded=%v in the sharded estimator, want %v", hash[:6], gotLoaded, loaded)
		}
		if view.loaded(hash) != loaded {
			t.Fatalf("loadedHashes reports hash %x as loaded=%v, want %v", hash[:6], view.loaded(hash), loaded)
		}
	}
	if sharded.size() != reference.bytes {
		t.Fatalf("sharded estimate = %d bytes, want %d", sharded.size(), reference.bytes)
	}
}

// The shard is picked from a byte the fingerprint does not read. If it were
// picked from one of the first four, every fingerprint inside a shard would
// share its low bits, and since the slot index is the fingerprint masked, the
// table would use one slot in proofEstimatorShards and its probe runs would grow
// with the population instead of staying flat.
func TestProofEstimatorShardKeyIsOutsideTheFingerprint(t *testing.T) {
	var hash cell.Hash
	for i := range hash {
		hash[i] = 0x11
	}
	base := proofCellFingerprint(hash)
	for shard := range proofEstimatorShards {
		hash[len(hash)-1] = byte(shard)
		if proofEstimatorShardOf(hash) != shard {
			t.Fatalf("shard of a hash ending in %#02x = %d, want %d", shard, proofEstimatorShardOf(hash), shard)
		}
		if proofCellFingerprint(hash) != base {
			t.Fatalf("changing the shard byte moved the fingerprint")
		}
	}
}

// The rule the split exists for, stated on its own rather than through the
// reference implementation.
//
// A cell the transaction executor reports is selected into the proof but cannot
// be charged when it arrives: execution also loads the inbound message and cells
// it built itself, which are not in the predecessor tree and are never emitted.
// The cells that ARE in that tree are reached again by the traversal, and that
// second arrival is what must charge them — the cell's own size, and the pruned
// boundary of every child it names. Folding selection into the loaded bit made
// the traversal find them already "loaded" and skip both, so the estimate ran
// under what the proof really wrote while the proof carried the cells all the
// same.
func TestExecutionReadSelectsWithoutChargingAndTheTraversalStillCharges(t *testing.T) {
	child := cell.BeginCell().MustStoreUInt(0xC1, 24).EndCell()
	parent := cell.BeginCell().MustStoreUInt(0xA1, 24).MustStoreRef(child).EndCell()
	loadedBytes := uint64(5+(parent.BitsSize()+7)/8) + 3

	// Execution reports it first: in the proof, on the books for nothing.
	// Selection is asked of its own estimator, because loadedHashes seals the
	// one it is asked of and nothing may record into it afterwards.
	selected := newProofSizeEstimator(0)
	selected.addExecutionRead(parent)
	if !selected.loadedHashes().loaded(parent.HashKey()) {
		t.Fatal("an execution read did not select the cell into the proof")
	}

	viaExecution := newProofSizeEstimator(0)
	viaExecution.addExecutionRead(parent)
	if got := viaExecution.size(); got != 0 {
		t.Fatalf("an execution read charged %d bytes; it must charge none", got)
	}

	// The traversal reaches the same cell. Both charges must land: the cell at
	// its loaded size, and its child as a pruned boundary.
	viaExecution.addLoadedCell(parent)
	want := loadedBytes + estimatedPrunedBranchBytes
	if got := viaExecution.size(); got != want {
		t.Fatalf("execution-read-then-traversed = %d bytes, want %d", got, want)
	}

	// And it comes out identical to the order where the traversal arrives first,
	// which is the only reason the number can be trusted: which route sees a
	// cell first is a scheduling accident.
	viaTraversal := newProofSizeEstimator(0)
	viaTraversal.addLoadedCell(parent)
	viaTraversal.addExecutionRead(parent)
	if got := viaTraversal.size(); got != want {
		t.Fatalf("traversed-then-execution-read = %d bytes, want %d", got, want)
	}

	// A repeat of either route changes nothing.
	viaExecution.addExecutionRead(parent)
	viaExecution.addLoadedCell(parent)
	if got := viaExecution.size(); got != want {
		t.Fatalf("repeats moved the estimate to %d bytes, want %d", got, want)
	}
}

// The other half of the split: the boundary subtraction must fire only where a
// boundary was actually charged. An execution read creates an entry that never
// paid one, so a promotion keyed on the entry merely existing would refund forty
// bytes the estimate never took — and an estimate that goes backwards admits
// messages a block cannot hold.
func TestPromotionRefundsOnlyABoundaryThatWasCharged(t *testing.T) {
	child := cell.BeginCell().MustStoreUInt(0xC2, 24).EndCell()
	parent := cell.BeginCell().MustStoreUInt(0xA2, 24).MustStoreRef(child).EndCell()
	childBytes := uint64(5 + (child.BitsSize()+7)/8)

	// The child is charged a real boundary by its parent, then loaded: the
	// boundary is refunded and the loaded size charged in its place.
	refunded := newProofSizeEstimator(0)
	refunded.addLoadedCell(parent)
	beforeLoad := refunded.size()
	refunded.addLoadedCell(child)
	if got, want := refunded.size(), beforeLoad-estimatedPrunedBranchBytes+childBytes; got != want {
		t.Fatalf("boundary promotion = %d bytes, want %d", got, want)
	}

	// The same cell selected by an execution read first was never charged a
	// boundary, so loading it only adds.
	notRefunded := newProofSizeEstimator(0)
	notRefunded.addExecutionRead(child)
	notRefunded.addLoadedCell(child)
	if got := notRefunded.size(); got != childBytes {
		t.Fatalf("execution-read promotion = %d bytes, want %d", got, childBytes)
	}
}
