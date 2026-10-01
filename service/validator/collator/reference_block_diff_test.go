package collator

import (
	"context"
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func refStats(t *testing.T, label string, c *cell.Cell) (cells, boundaries, bits int) {
	t.Helper()
	if c == nil {
		return
	}
	seen := map[cell.Hash]struct{}{}
	var w func(x *cell.Cell, d int)
	w = func(x *cell.Cell, d int) {
		if x == nil || d > 600 {
			return
		}
		h := x.HashKey()
		if _, ok := seen[h]; ok {
			return
		}
		seen[h] = struct{}{}
		cells++
		bits += int(x.BitsSize())
		if x.GetType() == cell.PrunedCellType {
			boundaries++
			return
		}
		for i := 0; i < int(x.RefsNum()); i++ {
			if r, e := x.PeekRef(i); e == nil {
				w(r, d+1)
			}
		}
	}
	w(c, 0)
	t.Logf("%-22s cells=%6d boundaries=%5d bodies=%6d bits=%9d", label, cells, boundaries, cells-boundaries, bits)
	return
}

func refReport(t *testing.T, who string, blockBOC []byte) {
	t.Helper()
	root, err := cell.FromBOC(blockBOC)
	if err != nil {
		t.Fatalf("%s: parse block: %v", who, err)
	}
	var blk tlb.Block
	if err = tlb.LoadFromCell(&blk, root.MustBeginParse()); err != nil {
		t.Fatalf("%s: decode block: %v", who, err)
	}
	t.Logf("=== %s: %d bytes ===", who, len(blockBOC))
	refStats(t, who+" whole block", root)
	refStats(t, who+" state update", blk.StateUpdate)
	if f, e := blk.StateUpdate.PeekRef(0); e == nil {
		refStats(t, who+"   update.from", f)
	}
	if to, e := blk.StateUpdate.PeekRef(1); e == nil {
		refStats(t, who+"   update.to", to)
	}
	if x, e := root.PeekRef(3); e == nil {
		refStats(t, who+" extra", x)
	}
}

type refSide struct {
	boundaries map[cell.Hash]int // hash -> depth first seen
	bodies     map[cell.Hash]int
}

func refCollect(root *cell.Cell) refSide {
	out := refSide{boundaries: map[cell.Hash]int{}, bodies: map[cell.Hash]int{}}
	seen := map[cell.Hash]struct{}{}
	var w func(x *cell.Cell, d int)
	w = func(x *cell.Cell, d int) {
		if x == nil || d > 600 {
			return
		}
		h := x.HashKeyAt(0)
		if _, ok := seen[h]; ok {
			return
		}
		seen[h] = struct{}{}
		if x.GetType() == cell.PrunedCellType {
			out.boundaries[h] = d
			return
		}
		out.bodies[h] = d
		for i := 0; i < int(x.RefsNum()); i++ {
			if r, e := x.PeekRef(i); e == nil {
				w(r, d+1)
			}
		}
	}
	w(root, 0)
	return out
}

func refDiffSides(t *testing.T, label string, a, b refSide) {
	t.Helper()
	onlyA, onlyB := 0, 0
	depthsA := map[int]int{}
	for h, d := range a.bodies {
		_, inBodies := b.bodies[h]
		_, inBounds := b.boundaries[h]
		if !inBodies && !inBounds {
			onlyA++
			depthsA[d]++
		}
	}
	for h := range b.bodies {
		_, inBodies := a.bodies[h]
		_, inBounds := a.boundaries[h]
		if !inBodies && !inBounds {
			onlyB++
		}
	}
	bodyAsBoundary, boundaryAsBody := 0, 0
	for h := range a.bodies {
		if _, ok := b.boundaries[h]; ok {
			bodyAsBoundary++
		}
	}
	for h := range a.boundaries {
		if _, ok := b.bodies[h]; ok {
			boundaryAsBody++
		}
	}
	t.Logf("%s: cells only in GO=%d, only in CPP=%d | GO body where CPP prunes=%d | GO prunes where CPP body=%d",
		label, onlyA, onlyB, bodyAsBoundary, boundaryAsBody)
	if len(depthsA) > 0 {
		keys := []int{}
		for d := range depthsA {
			keys = append(keys, d)
		}
		sort.Ints(keys)
		line := ""
		for _, d := range keys {
			line += fmt.Sprintf(" d%d=%d", d, depthsA[d])
		}
		t.Logf("%s: GO-only cells by depth:%s", label, line)
	}
}

// TestReferenceBlockDiff compares the block this collator produces against one
// the reference node produced from the same fixture, section by section and then
// cell by cell. It is how the 22 KB the source half of the state update used to
// carry was found: every other number matched — same predecessor, same resulting
// state root, byte-identical extra — and the difference sat entirely in which
// source paths the update opened.
//
// Point CPP_BLOCK at a block dumped by the reference harness:
//
//	bench-collate --fixture fixture/mainnet --iterations 1 --dump-block /tmp/cpp_block.boc
//
// The fixture must be the one whose predecessor this request carries; the test
// prints both roots so a mismatch is visible rather than silently compared.
func TestReferenceBlockDiff(t *testing.T) {
	path := os.Getenv("CPP_BLOCK")
	if path == "" {
		t.Skip("set CPP_BLOCK")
	}
	cppBOC, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	req := benchMainnetCollatedRequest(t, 1)
	t.Logf("GO  predecessor state root %x", req.Previous.State.HashKeyAt(0))
	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatalf("collate: %v", err)
	}

	refReport(t, "GO ", candidate.BlockBOC)
	refReport(t, "CPP", cppBOC)

	goRoot, _ := cell.FromBOC(candidate.BlockBOC)
	cppRoot, _ := cell.FromBOC(cppBOC)
	var goBlk, cppBlk tlb.Block
	if err = tlb.LoadFromCell(&goBlk, goRoot.MustBeginParse()); err != nil {
		t.Fatal(err)
	}
	if err = tlb.LoadFromCell(&cppBlk, cppRoot.MustBeginParse()); err != nil {
		t.Fatal(err)
	}
	gf, _ := goBlk.StateUpdate.PeekRef(0)
	cf, _ := cppBlk.StateUpdate.PeekRef(0)
	gt, _ := goBlk.StateUpdate.PeekRef(1)
	ct, _ := cppBlk.StateUpdate.PeekRef(1)
	t.Logf("GO  update.from root %x", gf.HashKeyAt(0))
	t.Logf("CPP update.from root %x", cf.HashKeyAt(0))
	t.Logf("GO  update.to   root %x", gt.HashKeyAt(0))
	t.Logf("CPP update.to   root %x", ct.HashKeyAt(0))
	t.Logf("GO  block seqno %d root %x", candidate.ID.SeqNo, candidate.ID.RootHash)
	refDiffSides(t, "update.from", refCollect(gf), refCollect(cf))
	refDiffSides(t, "update.to  ", refCollect(gt), refCollect(ct))
}
