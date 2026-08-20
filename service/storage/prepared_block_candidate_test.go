package storage

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestPreparedBlockCandidateBindsCanonicalSerialization(t *testing.T) {
	root := cell.BeginCell().MustStoreUInt(0xb10c, 32).EndCell()
	prepared, err := PrepareBlockCandidate(-1, -1<<63, 42, root)
	if err != nil {
		t.Fatal(err)
	}

	id := prepared.ID()
	if !bytes.Equal(id.RootHash, root.Hash()) {
		t.Fatal("prepared candidate root hash differs from its root")
	}
	fileHash := sha256.Sum256(prepared.BlockBOC())
	if !bytes.Equal(id.FileHash, fileHash[:]) {
		t.Fatal("prepared candidate file hash differs from its canonical BOC")
	}
	if prepared.Root() == root {
		t.Fatal("prepared candidate retained the caller's root ownership")
	}
	if prepared.Root().HashKey() != root.HashKey() {
		t.Fatal("prepared candidate detached root changed the block hash")
	}

	// ID returns ownership-safe slices; changing a consumer's copy cannot alter
	// the sealed identity later handed to another package.
	id.RootHash[0] ^= 0xff
	id.FileHash[0] ^= 0xff
	again := prepared.ID()
	if !bytes.Equal(again.RootHash, root.Hash()) || !bytes.Equal(again.FileHash, fileHash[:]) {
		t.Fatal("mutating a returned block ID changed the prepared artifact")
	}

	// BOC ownership is isolated for the same reason: the trusted cache path no
	// longer rehashes it after accepting this capsule.
	exposedBOC := prepared.BlockBOC()
	exposedBOC[len(exposedBOC)-1] ^= 0xff
	if gotHash := sha256.Sum256(prepared.BlockBOC()); !bytes.Equal(gotHash[:], fileHash[:]) {
		t.Fatal("mutating a returned BOC changed the prepared artifact")
	}
}

func TestPreparedBlockCandidateDetachesCombinedBOCArena(t *testing.T) {
	shared := cell.BeginCell().MustStoreUInt(0x5a, 8).EndCell()
	blockRoot := cell.BeginCell().
		MustStoreRef(cell.BeginCell().MustStoreUInt(1, 1).MustStoreRef(shared).EndCell()).
		MustStoreRef(cell.BeginCell().MustStoreUInt(0, 1).MustStoreRef(shared).EndCell()).
		EndCell()
	collatedRoot := cell.BeginCell().MustStoreUInt(0xc011a7ed, 32).
		MustStoreRef(cell.BeginCell().MustStoreUInt(0xbeef, 16).EndCell()).
		EndCell()
	combined := cell.ToBOCWithOptions(
		[]*cell.Cell{blockRoot, collatedRoot},
		cell.BOCSerializeOptions{WithCRC32C: true},
	)
	roots, err := cell.FromBOCMultiRoot(combined)
	if err != nil {
		t.Fatal(err)
	}

	prepared, err := PrepareBlockCandidate(0, -1<<63, 7, roots[0])
	if err != nil {
		t.Fatal(err)
	}
	detachedCells := collectPreparedTestCells(prepared.Root())
	sourceCells := collectPreparedTestCells(roots...)
	for detached := range detachedCells {
		if _, aliases := sourceCells[detached]; aliases {
			t.Fatal("prepared block retained a cell from the combined parse arena")
		}
	}
	if len(detachedCells) != len(collectPreparedTestCells(roots[0])) {
		t.Fatal("prepared block copied cells outside the reachable block DAG")
	}

	wantBOC := roots[0].ToBOCWithOptions(cell.BOCSerializeOptions{
		WithCRC32C:    true,
		WithIndex:     true,
		WithCacheBits: true,
		WithIntHashes: true,
	})
	if got := prepared.BlockBOC(); !bytes.Equal(got, wantBOC) {
		t.Fatal("detaching the combined candidate changed the canonical block BOC")
	}
	left := prepared.Root().MustPeekRef(0).MustPeekRef(0)
	right := prepared.Root().MustPeekRef(1).MustPeekRef(0)
	if left != right {
		t.Fatal("detached prepared block lost shared-reference identity")
	}
}

func collectPreparedTestCells(roots ...*cell.Cell) map[*cell.Cell]struct{} {
	seen := make(map[*cell.Cell]struct{})
	queue := append([]*cell.Cell(nil), roots...)
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if _, exists := seen[current]; exists {
			continue
		}

		seen[current] = struct{}{}
		for i := 0; i < int(current.RefsNum()); i++ {
			queue = append(queue, current.MustPeekRef(i))
		}
	}

	return seen
}
