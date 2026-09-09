package collator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/msgpool"
	"github.com/xssnick/gton/service/validator/simplex"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestSpeculativeCarriedAncestorsBuildWithoutStoreReads(t *testing.T) {
	for _, depth := range []int{1, 2, 4, MaxSpeculativeLineageBlocks} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			f := newSpeculativeLineageFixture(t)
			previous := f.prevB0
			built := f.b0
			blocks := []PreviousBlock{previous}
			for i := 1; i < depth; i++ {
				request := emptyCandidateRequest(t)
				request.Previous = previous
				request.Masterchain.Groups.Active[0].Registered[0].Block = previous.ID
				var err error
				built, err = testBuilder().BuildShard(t.Context(), request)
				if err != nil {
					t.Fatal(err)
				}
				previous = PreviousBlock{
					ID: built.ID, Block: candidateBlock(t, built), State: built.State,
					OutQueueSize: uint64Pointer(built.Stats.OutQueueSize),
				}
				blocks = append(blocks, previous)
			}
			roots := make([]*cell.Cell, 0, depth-1)
			for i := depth - 2; i >= 0; i-- {
				roots = append(roots, blocks[i].Block)
			}
			candidate := simplex.CandidateID{Slot: uint32(depth + 3), Hash: [32]byte{0xc1}}
			base, err := NewSelectedBaseState([32]byte{0xc2}, candidate, built.ID, built.BlockBOC, previous.Block, built.State, roots)
			if err != nil {
				t.Fatal(err)
			}
			for _, ancestor := range base.ancestors {
				if ancestor.State != nil || ancestor.OutQueueSize != nil {
					t.Fatal("ancestor capability retained a full state")
				}
			}
			clear(f.managed.blocks)
			store := &finalizedAnchorNoReadStore{}
			f.acquisition.store = store
			request := BuildRequest{
				Session: ActivatedSession{Session: Session{Shard: f.shard}},
				Slot:    candidate.Slot + 1, Parent: simplex.Parent(candidate),
				speculative: &speculativeBase{state: base, at: time.Now()},
			}
			var acquired PreviousBlock
			for attempt := 0; attempt < 2; attempt++ {
				chain, err := f.acquisition.resolveChain(t.Context(), f.managed, request)
				if err != nil {
					t.Fatalf("resolve attempt %d: %v", attempt, err)
				}
				if chain.candidateTip == nil || len(chain.queueBase) != 1 || !sameBlockID(chain.queueBase[0].ID, f.anchor.ID) {
					t.Fatal("carried lineage lost its exact candidate tip or applied anchor")
				}
				if len(chain.previous) != 1 || !sameBlockID(chain.previous[0].ID, previous.ID) ||
					chain.previous[0].Block != previous.Block || chain.previous[0].State != previous.State {
					t.Fatal("acquisition did not return the selected predecessor roots")
				}
				acquired = chain.previous[0]
				if _, err = f.acquisition.cutCommittedViews(
					f.branch, f.destination,
					map[msgpool.ShardIdent]*localNeighborView{f.destination: {previous: previous}},
					chain.queueBase, chain.candidateTip, false, &prewarmHints{},
				); err != nil {
					t.Fatalf("cut without speculative seeding: %v", err)
				}
			}
			build := emptyCandidateRequest(t)
			build.Previous = acquired
			build.Masterchain.Groups.Active[0].Registered[0].Block = previous.ID
			if _, err = testBuilder().BuildShard(t.Context(), build); err != nil {
				t.Fatalf("build over carried lineage: %v", err)
			}
			if store.calls != 0 || len(f.managed.blocks) != 0 || len(f.managed.candidates) != 0 {
				t.Fatalf("lineage read/published states: store=%d blocks=%d candidates=%d", store.calls, len(f.managed.blocks), len(f.managed.candidates))
			}
		})
	}
}

func TestSpeculativeCarriedAncestorsDoNotBypassTheLineageCap(t *testing.T) {
	f := newSpeculativeLineageFixture(t)
	previous, built := f.prevB0, f.b0
	blocks := []PreviousBlock{previous}
	for i := 1; i <= MaxSpeculativeLineageBlocks; i++ {
		request := emptyCandidateRequest(t)
		request.Previous = previous
		request.Masterchain.Groups.Active[0].Registered[0].Block = previous.ID
		var err error
		built, err = testBuilder().BuildShard(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		previous = PreviousBlock{ID: built.ID, Block: candidateBlock(t, built), State: built.State, OutQueueSize: uint64Pointer(0)}
		blocks = append(blocks, previous)
	}
	// Tip plus fifteen carried ancestors is the maximum capability. The
	// seventeenth uncommitted block is in the old cache, and must still be
	// refused by both the initial walk and its existing-node shortcut.
	roots := make([]*cell.Cell, 0, MaxSpeculativeLineageBlocks-1)
	for i := len(blocks) - 2; i > 0; i-- {
		roots = append(roots, blocks[i].Block)
	}
	id := simplex.CandidateID{Slot: 20, Hash: [32]byte{0xc3}}
	base, err := NewSelectedBaseState([32]byte{0xc4}, id, built.ID, built.BlockBOC, previous.Block, built.State, roots)
	if err != nil {
		t.Fatal(err)
	}
	request := BuildRequest{
		Session: ActivatedSession{Session: Session{Shard: f.shard}},
		Slot:    21, Parent: simplex.Parent(id), speculative: &speculativeBase{state: base},
	}
	if _, err = f.acquisition.resolveChain(t.Context(), f.managed, request); !errors.Is(err, ErrAcquisitionNotReady) ||
		!strings.Contains(err.Error(), "more than 16 blocks") {
		t.Fatalf("initial over-cap lineage = %v", err)
	}
	var parentKey *[32]byte
	for _, block := range blocks {
		key, err := blockRootKey(block.ID)
		if err != nil {
			t.Fatal(err)
		}
		ref, err := localSourceRef(block.ID)
		if err != nil {
			t.Fatal(err)
		}
		delta, err := f.branch.DeltaFromBlockRoot(f.destination, ref, block.Block, 0)
		if err != nil {
			t.Fatal(err)
		}
		insert := msgpool.CandidateRequest{ID: key, Seqno: block.ID.SeqNo, Delta: delta, Parent: parentKey}
		if parentKey == nil {
			insert.Base = candidateSources([]PreviousBlock{f.anchor})
		}
		if err = f.branch.AddCandidate(insert); err != nil {
			t.Fatal(err)
		}
		parentKey = &key
	}
	if _, err = f.acquisition.resolveChain(context.Background(), f.managed, request); !errors.Is(err, ErrAcquisitionNotReady) ||
		!strings.Contains(err.Error(), "more than 16 blocks") {
		t.Fatalf("existing-node over-cap lineage = %v", err)
	}
}

func BenchmarkSelectedBaseAncestorBinding(b *testing.B) {
	for _, depth := range []int{1, 4, MaxSpeculativeLineageBlocks} {
		b.Run(fmt.Sprint(depth), func(b *testing.B) {
			request := emptyCandidateRequest(b)
			var built *Candidate
			var root *cell.Cell
			blocks := make([]*cell.Cell, 0, depth)
			for i := 0; i < depth; i++ {
				var err error
				built, err = testBuilder().BuildShard(b.Context(), request)
				if err != nil {
					b.Fatal(err)
				}
				root = candidateBlock(b, built)
				blocks = append(blocks, root)
				request.Previous = PreviousBlock{ID: built.ID, Block: root, State: built.State, OutQueueSize: uint64Pointer(0)}
				request.Masterchain.Groups.Active[0].Registered[0].Block = built.ID
			}
			var ancestors []*cell.Cell
			for i := len(blocks) - 2; i >= 0; i-- {
				ancestors = append(ancestors, blocks[i])
			}
			id := simplex.CandidateID{Slot: uint32(depth), Hash: [32]byte{0xc5}}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := NewSelectedBaseState([32]byte{0xc6}, id, built.ID, built.BlockBOC, root, built.State, ancestors); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
