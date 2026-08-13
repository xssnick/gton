package collator

import (
	"errors"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"

	"github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/validator/msgpool"
)

// The masterchain still lists our ancestor until our own first block is
// registered, and that ancestor covers both halves of the split. ValidateQuery
// derives msg_export_deq_short.import_block_lt from the neighbor whose
// processed frontier covers the message (check_out_msg), so reasoning from the
// raw set would stamp the ancestor's end_lt onto dequeues of our own messages
// and get the block rejected.
func TestNormalizeTrivialNeighborsReplacesRegisteredAncestor(t *testing.T) {
	rootID := int64(shard.Root)
	left, err := shard.Child(rootID, true)
	if err != nil {
		t.Fatal(err)
	}
	right, err := shard.Child(rootID, false)
	if err != nil {
		t.Fatal(err)
	}
	target := msgpool.ShardIdent{Workchain: 0, Shard: uint64(left)}

	ancestor := Neighbor{
		Block: testBlockID(0, rootID, 4, 0x11),
		Shard: msgpool.ShardIdent{Workchain: 0, Shard: uint64(rootID)},
		EndLT: 5_000,
		Processed: []tlb.ProcessedUptoRecord{
			{ShardPrefix: uint64(rootID), MCSeqno: 3, LastMsgLT: 4_500},
		},
	}
	foreign := Neighbor{
		Block: testBlockID(1, rootID, 6, 0x21),
		Shard: msgpool.ShardIdent{Workchain: 1, Shard: uint64(rootID)},
		EndLT: 6_000,
	}
	predecessor := Neighbor{
		Block: testBlockID(0, left, 9, 0x31),
		Shard: target,
		EndLT: 7_000,
		Processed: []tlb.ProcessedUptoRecord{
			{ShardPrefix: uint64(left), MCSeqno: 3, LastMsgLT: 6_800},
		},
	}

	entries, err := normalizeTrivialNeighbors([]Neighbor{ancestor, foreign}, target, predecessor, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("effective neighbors = %d, want 3", len(entries))
	}
	wantOrder := []msgpool.ShardIdent{
		{Workchain: 0, Shard: uint64(right)},
		foreign.Shard,
		target,
	}
	for i := range wantOrder {
		if entries[i].Shard != wantOrder[i] {
			t.Fatalf("effective neighbor %d = %+v, want %+v", i, entries[i].Shard, wantOrder[i])
		}
	}

	byShard := make(map[msgpool.ShardIdent]effectiveNeighbor, len(entries))
	for _, entry := range entries {
		byShard[entry.Shard] = entry
	}
	if _, listed := byShard[ancestor.Shard]; listed {
		t.Fatal("the registered ancestor still answers for our own half")
	}

	own, listed := byShard[target]
	if !listed {
		t.Fatal("the predecessor is not part of the effective set")
	}
	if own.origin != -1 || own.EndLT != 7_000 {
		t.Fatalf("predecessor entry = origin %d, end lt %d; want -1 and the predecessor end lt",
			own.origin, own.EndLT)
	}

	sibling, listed := byShard[msgpool.ShardIdent{Workchain: 0, Shard: uint64(right)}]
	if !listed {
		t.Fatal("the sibling half of the ancestor queue is not covered")
	}
	if sibling.origin != 0 || sibling.EndLT != 5_000 {
		t.Fatalf("sibling entry = origin %d, end lt %d; want 0 and the ancestor end lt",
			sibling.origin, sibling.EndLT)
	}
	if len(sibling.Processed) != 1 || sibling.Processed[0].ShardPrefix != uint64(right) {
		t.Fatalf("sibling processed frontier = %v, want the ancestor frontier narrowed to the sibling",
			sibling.Processed)
	}
	if len(ancestor.Processed) != 1 || ancestor.Processed[0].ShardPrefix != uint64(rootID) {
		t.Fatal("normalization mutated the registered ancestor frontier")
	}
	if foreignEntry := byShard[foreign.Shard]; foreignEntry.origin != 1 {
		t.Fatalf("foreign neighbor origin = %d, want 1", foreignEntry.origin)
	}
}

// The masterchain can retain both child descriptors for several blocks after
// the first merged block. C++ add_trivial_neighbor handles this as case 4: the
// merged predecessor replaces the first child and the second child is disabled.
func TestNormalizeTrivialNeighborsContinuedAfterMerge(t *testing.T) {
	rootID := int64(shard.Root)
	left, err := shard.Child(rootID, true)
	if err != nil {
		t.Fatal(err)
	}
	right, err := shard.Child(rootID, false)
	if err != nil {
		t.Fatal(err)
	}
	target := msgpool.ShardIdent{Workchain: 0, Shard: uint64(rootID)}
	predecessor := Neighbor{
		Block: testBlockID(0, rootID, 10, 0x31),
		Shard: target,
		EndLT: 7_000,
	}

	entries, err := normalizeTrivialNeighbors([]Neighbor{
		{Block: testBlockID(0, left, 9, 0x41), Shard: msgpool.ShardIdent{Workchain: 0, Shard: uint64(left)}},
		{Block: testBlockID(0, right, 9, 0x51), Shard: msgpool.ShardIdent{Workchain: 0, Shard: uint64(right)}},
	}, target, predecessor, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("effective neighbors = %d, want the merged predecessor only", len(entries))
	}
	if entries[0].origin != -1 || entries[0].Shard != target || entries[0].EndLT != predecessor.EndLT {
		t.Fatalf("merged predecessor entry = %+v", entries[0])
	}
}

func TestNormalizeTrivialNeighborsRejectsInconsistentCoverage(t *testing.T) {
	rootID := int64(shard.Root)
	left, err := shard.Child(rootID, true)
	if err != nil {
		t.Fatal(err)
	}
	root := msgpool.ShardIdent{Workchain: 0, Shard: uint64(rootID)}
	child := msgpool.ShardIdent{Workchain: 0, Shard: uint64(left)}
	leftNeighbor := Neighbor{Shard: child, Block: testBlockID(0, left, 1, 0x41)}

	tests := []struct {
		name       string
		neighbors  []Neighbor
		target     msgpool.ShardIdent
		afterMerge bool
		wantError  string
	}{
		{
			name: "same shard and child both cover target",
			neighbors: []Neighbor{
				{Shard: root, Block: testBlockID(0, rootID, 1, 0x31)},
				leftNeighbor,
			},
			target:    root,
			wantError: "cover this shard",
		},
		{
			name:      "continued merge lists only one child",
			neighbors: []Neighbor{leftNeighbor},
			target:    root,
			wantError: "without its sibling",
		},
		{
			name:       "after merge with a single predecessor",
			neighbors:  []Neighbor{leftNeighbor},
			target:     root,
			afterMerge: true,
			wantError:  "want the two predecessors",
		},
		{
			name:       "after merge listing an ancestor",
			neighbors:  []Neighbor{{Shard: root, Block: testBlockID(0, rootID, 1, 0x61)}},
			target:     child,
			afterMerge: true,
			wantError:  "want the two predecessors",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := normalizeTrivialNeighbors(
				test.neighbors,
				test.target,
				Neighbor{Shard: test.target},
				test.afterMerge,
			)
			if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error = %v, want ErrInvalidInput containing %q", err, test.wantError)
			}
		})
	}
}
