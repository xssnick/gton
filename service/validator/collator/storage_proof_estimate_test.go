package collator

import (
	"crypto/sha256"
	"encoding/binary"
	"runtime"
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

// The production estimate replaces a complete proof build and standalone BOC
// serialization, so being cheaper is not enough: at every growth step it must
// be no smaller than the exact operation it replaced. These trees exercise a
// leaf, a deep chain, four-reference cells, and a DAG whose child is shared by
// two parents. The dictionary-specific shared-account growth is covered by
// TestTrackAccountStorageProofChargesSharedDictionaryOnce.
func TestAccountStorageProofEstimateCoversExactBOC(t *testing.T) {
	maxLeaf := cell.BeginCell().MustStoreSlice(make([]byte, 128), 1023).EndCell()
	chainLeaf := cell.BeginCell().MustStoreUInt(1, 1).EndCell()
	chainMiddle := cell.BeginCell().MustStoreUInt(2, 2).MustStoreRef(chainLeaf).EndCell()
	chainRoot := cell.BeginCell().MustStoreUInt(3, 2).MustStoreRef(chainMiddle).EndCell()
	sharedLeft := cell.BeginCell().MustStoreUInt(4, 3).MustStoreRef(maxLeaf).EndCell()
	sharedRight := cell.BeginCell().MustStoreUInt(5, 3).MustStoreRef(maxLeaf).EndCell()
	wideRoot := cell.BeginCell().
		MustStoreUInt(6, 3).
		MustStoreRef(sharedLeft).
		MustStoreRef(sharedRight).
		MustStoreRef(chainRoot).
		MustStoreRef(maxLeaf).
		EndCell()

	tests := []struct {
		name  string
		root  *cell.Cell
		paths [][]int
	}{
		{name: "leaf", root: maxLeaf, paths: [][]int{{}}},
		{name: "chain", root: chainRoot, paths: [][]int{{}, {0}, {0, 0}}},
		{
			name: "shared wide DAG",
			root: wideRoot,
			paths: [][]int{
				{},
				{0},
				{0, 0},
				{1},
				{1, 0}, // the same maxLeaf must not be charged twice
				{2},
				{2, 0},
				{2, 0, 0},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := newAccountStorageProof(test.root)
			lane := &accountLane{initialStorageStat: test.root, storageProof: record.builder}
			collation := &collation{
				fullCollated:         true,
				accountStorageProofs: map[cell.Hash]*accountStorageProof{test.root.HashKey(): record},
			}
			var previous uint64
			for step, path := range test.paths {
				readStorageProofPath(t, record.builder.Root(), path)
				collation.trackAccountStorageProof(lane)

				exact := exactAccountStorageProofBOC(t, record.builder)
				if got := collation.collatedFixedEstimate; got < uint64(len(exact)) {
					t.Fatalf("step %d path %v: estimate = %d, exact wrapped BOC = %d",
						step, path, got, len(exact))
				}
				if collation.collatedFixedEstimate < previous {
					t.Fatalf("step %d path %v: estimate decreased from %d to %d",
						step, path, previous, collation.collatedFixedEstimate)
				}
				previous = collation.collatedFixedEstimate

				// Re-reading and re-tracking the same cells is a zero-delta fast
				// path: neither the shared proof nor its admission charge grows.
				readStorageProofPath(t, record.builder.Root(), path)
				collation.trackAccountStorageProof(lane)
				if collation.collatedFixedEstimate != previous {
					t.Fatalf("step %d path %v: duplicate read changed estimate from %d to %d",
						step, path, previous, collation.collatedFixedEstimate)
				}
			}
		})
	}
}

func TestAccountStorageProofBoundaryEstimateCoversLeavesAndPrunedLevels(t *testing.T) {
	maxLeaf := cell.BeginCell().MustStoreSlice(make([]byte, 128), 1023).EndCell()
	branch := cell.BeginCell().MustStoreUInt(0xa5, 8).MustStoreRef(maxLeaf).EndCell()
	levelOne, err := cell.CreatePrunedBranch(branch, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	maskTwo, err := cell.CreatePrunedBranch(branch, 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	sparseSource := cell.BeginCell().MustStoreRef(maskTwo).EndCell()
	sparseLevelThree, err := cell.CreatePrunedBranch(sparseSource, 3, 3)
	if err != nil {
		t.Fatal(err)
	}
	levelOneParent := cell.BeginCell().MustStoreRef(levelOne).EndCell()
	levelOneParentBoundary, err := cell.CreatePrunedBranch(levelOneParent, 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	levelTwoParent := cell.BeginCell().MustStoreRef(maskTwo).EndCell()
	levelTwoParentBoundary, err := cell.CreatePrunedBranch(levelTwoParent, 3, 3)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name     string
		boundary *cell.Cell
		emitted  *cell.Cell
	}{
		// CreatePrunedBranch returns a resident leaf unchanged, even when that
		// leaf is much larger than the ordinary 38-byte pruned boundary.
		{name: "maximum leaf", boundary: maxLeaf, emitted: maxLeaf},
		{name: "ordinary branch", boundary: branch, emitted: levelOne},
		// A non-terminal ordinary cell inherits its refs' level mask. Pruning it
		// serializes every significant source hash and depth, so a fixed level-zero
		// boundary price would undercount both of these cases.
		{name: "level one parent", boundary: levelOneParent, emitted: levelOneParentBoundary},
		{name: "level two parent", boundary: levelTwoParent, emitted: levelTwoParentBoundary},
		{name: "level one pruned", boundary: levelOne, emitted: levelOne},
		// A sparse level mask stores several hashes and depths in the pruned
		// payload; the fixed 41-byte level-zero allowance is not enough for it.
		{name: "sparse level three pruned", boundary: sparseLevelThree, emitted: sparseLevelThree},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Four bytes per reference is the BOC format maximum. emitted has no
			// wrapper/header here: those are covered separately by
			// accountStorageProofBOCOverhead.
			exactUpperWidth := uint64(2+test.emitted.SerializedBOCBodySize()) + uint64(test.emitted.RefsNum())*4
			if got := accountStorageProofBoundaryBytes(test.boundary); got < exactUpperWidth {
				t.Fatalf("boundary estimate = %d, emitted cell at max ref width = %d", got, exactUpperWidth)
			}
		})
	}
}

func readStorageProofPath(tb testing.TB, root *cell.Cell, path []int) {
	tb.Helper()

	current := root
	if _, err := current.BeginParse(); err != nil {
		tb.Fatalf("read storage proof root: %v", err)
	}
	for _, ref := range path {
		next, err := current.PeekRef(ref)
		if err != nil {
			tb.Fatalf("read storage proof ref %d: %v", ref, err)
		}
		current = next
		if _, err = current.BeginParse(); err != nil {
			tb.Fatalf("parse storage proof ref %d: %v", ref, err)
		}
	}
}

func exactAccountStorageProofBOC(tb testing.TB, builder *cell.MerkleProofBuilder) []byte {
	tb.Helper()

	proof, err := builder.CreateProof()
	if err != nil {
		tb.Fatalf("build exact account storage proof: %v", err)
	}
	boc, err := wrapAccountStorageProof(proof).ToBOCWithOptionsErr(cell.BOCSerializeOptions{WithCRC32C: true})
	if err != nil {
		tb.Fatalf("serialize exact account storage proof: %v", err)
	}

	return boc
}

func BenchmarkAccountStorageProofEstimate(b *testing.B) {
	root, keys := accountStorageProofBenchmarkDict(b, 128)

	b.Run("exact snapshot", func(b *testing.B) {
		b.ReportAllocs()
		var total uint64
		for b.Loop() {
			builder := cell.NewMerkleProofBuilder(root)
			dict := builder.Root().AsDict(256)
			for _, key := range keys {
				if _, err := dict.LoadValueByBytesKey(key[:]); err != nil {
					b.Fatal(err)
				}
				total += uint64(len(exactAccountStorageProofBOC(b, builder)))
			}
		}
		runtime.KeepAlive(total)
	})

	b.Run("incremental track", func(b *testing.B) {
		b.ReportAllocs()
		var total uint64
		for b.Loop() {
			record := newAccountStorageProof(root)
			lane := &accountLane{initialStorageStat: root, storageProof: record.builder}
			collation := &collation{
				fullCollated:         true,
				accountStorageProofs: map[cell.Hash]*accountStorageProof{root.HashKey(): record},
			}
			dict := record.builder.Root().AsDict(256)
			for _, key := range keys {
				if _, err := dict.LoadValueByBytesKey(key[:]); err != nil {
					b.Fatal(err)
				}
				collation.trackAccountStorageProof(lane)
			}
			total += collation.collatedFixedEstimate
		}
		runtime.KeepAlive(total)
	})
}

func accountStorageProofBenchmarkDict(tb testing.TB, count int) (*cell.Cell, [][32]byte) {
	tb.Helper()

	dict := cell.NewDict(256)
	keys := make([][32]byte, count)
	for i := range keys {
		var seed [8]byte
		binary.BigEndian.PutUint64(seed[:], uint64(i+1))
		keys[i] = sha256.Sum256(seed[:])
		if err := dict.SetBuilderByBytesKey(keys[i][:], cell.BeginCell().MustStoreUInt(uint64(i), 32)); err != nil {
			tb.Fatal(err)
		}
	}
	root, err := dict.ToCell()
	if err != nil {
		tb.Fatal(err)
	}

	return root, keys
}
