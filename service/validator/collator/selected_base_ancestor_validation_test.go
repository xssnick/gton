package collator

import (
	"crypto/sha256"
	"errors"
	"strings"
	"testing"

	"github.com/xssnick/gton/service/validator/simplex"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestSelectedBaseAncestorsRejectUnboundRoots(t *testing.T) {
	f := newSpeculativeLineageFixture(t)
	tooMany := make([]*cell.Cell, MaxSpeculativeLineageBlocks)
	for i := range tooMany {
		tooMany[i] = f.prevB0.Block
	}
	cases := map[string][]*cell.Cell{
		"missing root":      {nil},
		"unrelated root":    {cell.BeginCell().EndCell()},
		"tip as parent":     {f.prevB1.Block},
		"reversed lineage":  {f.prevB1.Block, f.prevB0.Block},
		"duplicate parent":  {f.prevB0.Block, f.prevB0.Block},
		"lineage above cap": tooMany,
	}
	for name, roots := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := NewSelectedBaseState(
				[32]byte{0x71}, simplex.CandidateID{Slot: 3, Hash: [32]byte{0x72}},
				f.b1.ID, f.b1.BlockBOC, f.prevB1.Block, f.b1.State, roots,
			)
			if !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("unbound lineage accepted: %v", err)
			}
		})
	}
}

func TestSelectedBaseAncestorsOwnPointerList(t *testing.T) {
	f := newSpeculativeLineageFixture(t)
	roots := []*cell.Cell{f.prevB0.Block}
	base, err := NewSelectedBaseState(
		[32]byte{0x71}, simplex.CandidateID{Slot: 3, Hash: [32]byte{0x72}},
		f.b1.ID, f.b1.BlockBOC, f.prevB1.Block, f.b1.State, roots,
	)
	if err != nil {
		t.Fatal(err)
	}
	roots[0] = f.prevB1.Block
	if len(base.ancestors) != 1 || base.ancestors[0].Block != f.prevB0.Block ||
		!base.ancestors[0].ID.Equals(&f.b0.ID) {
		t.Fatal("caller changed the bound ancestor through its input slice")
	}
	if base.ancestors[0].State != nil {
		t.Fatal("ancestor capability retained a full state")
	}
}

func TestSelectedBaseAncestorsStopAtTopologyBoundary(t *testing.T) {
	f := newSpeculativeLineageFixture(t)
	for _, boundary := range []string{"split", "merge"} {
		t.Run(boundary, func(t *testing.T) {
			// Rebind the tip to a parent whose header marks a topology boundary.
			// The constructor checks this exact edge; it does not replay the
			// consensus validation that authenticated the supplied block roots.
			var parent tlb.Block
			if err := parseExact(&parent, f.prevB0.Block); err != nil {
				t.Fatal(err)
			}
			if boundary == "split" {
				parent.BlockInfo.AfterSplit = true
			} else {
				parent.BlockInfo.AfterMerge = true
				second := parent.BlockInfo.PrevRef.Prev1
				parent.BlockInfo.PrevRef.Prev2 = &second
			}
			parentRoot, err := tlb.ToCell(&parent)
			if err != nil {
				t.Fatal(err)
			}
			parentFileHash := sha256.Sum256(parentRoot.ToBOC())
			var tip tlb.Block
			if err = parseExact(&tip, f.prevB1.Block); err != nil {
				t.Fatal(err)
			}
			tip.BlockInfo.PrevRef.Prev1.RootHash = parentRoot.Hash()
			tip.BlockInfo.PrevRef.Prev1.FileHash = parentFileHash[:]
			tipRoot, err := tlb.ToCell(&tip)
			if err != nil {
				t.Fatal(err)
			}
			wire := tipRoot.ToBOC()
			fileHash := sha256.Sum256(wire)
			id := cloneBlockID(f.b1.ID)
			id.RootHash = tipRoot.Hash()
			id.FileHash = fileHash[:]
			candidate := simplex.CandidateID{Slot: 3, Hash: [32]byte{0x72}}
			base, err := NewSelectedBaseState(
				[32]byte{0x71}, candidate, id, wire, tipRoot, f.b1.State, []*cell.Cell{parentRoot},
			)
			if err != nil || len(base.ancestors) != 1 {
				t.Fatalf("terminal boundary ancestor refused: %v", err)
			}
			_, err = NewSelectedBaseState(
				[32]byte{0x71}, candidate, id, wire, tipRoot, f.b1.State,
				[]*cell.Cell{parentRoot, f.prevB0.Block},
			)
			if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "split or merge boundary") {
				t.Fatalf("ancestor walk crossed a topology boundary: %v", err)
			}
		})
	}
}
