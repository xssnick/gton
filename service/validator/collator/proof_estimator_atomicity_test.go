package collator

import (
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

// proofEstimatorSettle is how long a goroutine is given to reach the shard lock
// a test holds, and how long a call that must NOT return is watched for. The
// estimator calls under test are a hash-table probe and a lock acquisition
// apiece, so a goroutine that is going to get past one is past it in
// microseconds and this is four orders of magnitude of slack. The failures below
// are all "the value came back", never "it was slow".
const proofEstimatorSettle = 100 * time.Millisecond

// proofEstimatorMintedCells never repeats a nonce, so two cells asked for in the
// same shard are two cells. The tests here are sequential, and a shared counter
// is what makes "another cell in this shard" a call rather than a parameter.
var proofEstimatorMintedCells uint64

// cellInProofEstimatorShard mines a cell whose hash lands in the named shard, so
// a test can decide which lock a call will stop on. Cells are spread by a byte
// of a cryptographic hash, so the search is a handful of tries.
func cellInProofEstimatorShard(t *testing.T, shard int, refs ...*cell.Cell) *cell.Cell {
	t.Helper()

	for range 1 << 20 {
		proofEstimatorMintedCells++
		b := cell.BeginCell().MustStoreUInt(proofEstimatorMintedCells, 64)
		for _, ref := range refs {
			b.MustStoreRef(ref)
		}
		c := b.EndCell()
		if proofEstimatorShardOf(c.HashKey()) == shard {
			return c
		}
	}
	t.Fatalf("no cell hashes into shard %d", shard)
	return nil
}

// seedProofEstimatorShard gives a shard the populated table every shard has by
// the time the selector asks for its view: tens of thousands of entries in a
// table presized from the previous build. It matters to what a leak looks like.
// A view over an EMPTY shard cannot be written through — the first record
// allocates the slot array the view does not have, and the first hash chunk
// reallocates the one it copied — so a test that skipped this would watch a
// broken view answer correctly by accident and report the defect as fixed.
func seedProofEstimatorShard(t *testing.T, e *proofSizeEstimator, shard int) {
	t.Helper()

	e.addLoadedCell(cellInProofEstimatorShard(t, shard))
}

// The byte total is what admission compares against the collated-data limit
// after every message, and one charge covers a loaded cell AND the pruned
// boundaries of its children. Those halves land in different shards, so a total
// summed while a charge was between them is short by up to four pruned branches
// — a number the estimator never holds, arrived at by nothing but which
// goroutine ran when, and enough to admit a message the same block would refuse
// on the next run.
//
// Held here by stopping a charge on a child's shard and asking for the total
// while it is stopped. The answer must be the estimator before the charge or
// after it; if it cannot be either, size() must not answer at all.
func TestProofEstimatorSizeNeverSeesAChargeHalfDone(t *testing.T) {
	const blocked = 9
	children := []*cell.Cell{
		cellInProofEstimatorShard(t, blocked),
		cellInProofEstimatorShard(t, 2),
		cellInProofEstimatorShard(t, 5),
		cellInProofEstimatorShard(t, 14),
	}
	// The parent lands outside every child's shard, so its own bytes are charged
	// even while the first child's shard is held: that is what makes the
	// half-done total a value distinct from both endpoints.
	parent := cellInProofEstimatorShard(t, 7, children...)

	settled := newProofSizeEstimator(0)
	settled.addLoadedCell(parent)
	after := settled.size()
	if after == 0 {
		t.Fatal("the fixture charges nothing")
	}

	estimator := newProofSizeEstimator(0)
	estimator.shards[blocked].mu.Lock()

	charged := make(chan struct{})
	go func() {
		estimator.addLoadedCell(parent)
		close(charged)
	}()
	// The charge is now either stopped on the held shard with the parent already
	// counted (what this test exists to catch) or stopped before counting
	// anything.
	time.Sleep(proofEstimatorSettle)

	sized := make(chan uint64, 1)
	go func() {
		sized <- estimator.size()
	}()
	var (
		observed uint64
		answered bool
	)
	select {
	case observed = <-sized:
		answered = true
	case <-time.After(proofEstimatorSettle):
		// size() is waiting for the charge to finish, which is the property.
	}
	if answered && observed != 0 && observed != after {
		t.Fatalf("size() = %d while a charge was in flight, which is neither 0 before it nor %d after: "+
			"the total was summed across a charge", observed, after)
	}

	estimator.shards[blocked].mu.Unlock()
	<-charged
	if !answered {
		if total := <-sized; total != after {
			t.Fatalf("size() = %d once the charge completed, want %d", total, after)
		}
	}
	if total := estimator.size(); total != after {
		t.Fatalf("settled estimate = %d, want %d", total, after)
	}
}

// The seen set this returns IS the predicate that selects the previous-state
// proof (build.go, CreateHashUsageProofResolvedSized), so every cell it names
// decides collated bytes. A view assembled shard by shard is a mixture of
// sixteen instants, and a view that shares tables still being written keeps
// changing while the selection walk asks it — either one is a collated proof
// that depends on when a goroutine ran.
//
// Held here by stopping the view part-way and recording underneath it. What the
// selector was handed must be the estimator as it stood when it asked.
func TestProofEstimatorViewDoesNotMoveUnderTheSelector(t *testing.T) {
	const blocked = 8
	// Both in shards the view has already passed by the time it stops, which is
	// where a shard-at-a-time view is open to a record arriving behind it.
	early := cellInProofEstimatorShard(t, 0)
	late := cellInProofEstimatorShard(t, 3)
	// Recorded before anything is blocked, so the view is asserted to hold
	// something and cannot pass by coming back empty.
	before := cellInProofEstimatorShard(t, 12)

	estimator := newProofSizeEstimator(4096)
	for _, shard := range []int{0, 3, 12} {
		seedProofEstimatorShard(t, estimator, shard)
	}
	estimator.addLoadedCell(before)
	estimator.shards[blocked].mu.Lock()

	viewed := make(chan *proofLoadedHashes, 1)
	go func() {
		viewed <- estimator.loadedHashes()
	}()
	time.Sleep(proofEstimatorSettle)

	recorded := make(chan struct{})
	go func() {
		estimator.addLoadedCell(early)
		estimator.addLoadedCell(late)
		close(recorded)
	}()
	time.Sleep(proofEstimatorSettle)

	estimator.shards[blocked].mu.Unlock()
	view := <-viewed
	<-recorded

	if !view.loaded(before.HashKey()) {
		t.Fatal("the view lost a cell recorded before it was taken")
	}
	for i, c := range []*cell.Cell{early, late} {
		if view.loaded(c.HashKey()) {
			t.Fatalf("the view holds cell %d of the pair recorded after the view was asked for: "+
				"the selector's predicate moved under it", i)
		}
	}
}

// The same property one step further out: the view must still answer the same
// after the estimator has been offered more cells, because the selection walk
// runs for milliseconds after loadedHashes returns and asks the predicate tens
// of thousands of times. Sharing the tables is only sound because the seal stops
// anything reaching them.
func TestProofEstimatorViewIsFrozenOnceTaken(t *testing.T) {
	const shard = 6
	// Presized past what this test stores, so the table cannot grow: a grow
	// replaces the slot array and would hide a leaked write behind a
	// reallocation rather than showing it through the shared one.
	estimator := newProofSizeEstimator(4096)
	seedProofEstimatorShard(t, estimator, shard)
	recorded := cellInProofEstimatorShard(t, shard)
	estimator.addLoadedCell(recorded)

	view := estimator.loadedHashes()
	if !view.loaded(recorded.HashKey()) {
		t.Fatal("the view lost the cell it was taken over")
	}

	// Into the same shard, so the leak this asserts against is the shared table
	// being written in place rather than a shard the view never had.
	late := cellInProofEstimatorShard(t, shard)
	estimator.addLoadedCell(late)
	estimator.addExecutionRead(late)
	if view.loaded(late.HashKey()) {
		t.Fatal("a cell recorded after the view was taken reached it through the shared table")
	}
	if estimator.loadedHashes().loaded(late.HashKey()) {
		t.Fatal("the sealed estimator kept recording")
	}
	if !view.loaded(recorded.HashKey()) {
		t.Fatal("the view lost the cell it was taken over")
	}
}
