package collator

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

// The masterchain state update must carry the creator-statistics dictionary as
// a pruned branch, not as a copy. The dictionary is the largest structure in
// McStateExtra — on a live network it holds thousands of entries — and a block
// only ever touches a handful of its keys: its own creator, the creators of the
// shard tops it registers, and the aggregate. Every untouched subtree has to
// stay pinned by hash to the predecessor.
//
// It stopped doing that. The Merkle update prunes at the cells the read set
// recorded, and the creator dictionary is first read inside
// buildStateAndBlockParts — which the deferred-recording window used to cover,
// so those reads landed in a buffer, the source graph never descended into the
// dictionary, and the whole of it was serialized into the update. On the stand
// that made every masterchain block we produced 816 kB against the 12.7 kB of
// the reference nodes, all of it state update, at ~78% of the masterchain block
// byte limit — while the block itself carried three transactions.
//
// The bound below is deliberately coarse: the point is the order of magnitude
// between "a few touched paths" and "the whole dictionary", not a byte count
// that would have to be re-tuned whenever the fixture changes shape.
func TestBuildMasterPrunesUntouchedCreatorStats(t *testing.T) {
	const (
		entries      = 1_500
		maxUpdateBOC = 32 << 10
	)

	seeded := make(map[[32]byte]creatorStats, entries)
	for i := range entries {
		seeded[creatorStatsPruneKey(i)] = creatorStats{
			masterchain: discountedCounter{lastUpdated: 1_000, total: 3, count2048: 3 << 32, count65536: 3 << 32},
			shardchain:  discountedCounter{lastUpdated: 1_000, total: 7, count2048: 7 << 32, count65536: 7 << 32},
		}
	}
	statsRoot := blockCreateStatsTestCell(t, seeded)
	statsBOC := len(statsRoot.ToBOC())
	if statsBOC < 4*maxUpdateBOC {
		t.Fatalf("seeded creator dictionary is %d bytes, too small to tell pruning from copying", statsBOC)
	}

	fixture := newMasterBuildFixtureWith(t, masterBuildFixtureOptions{creatorStats: statsRoot})
	if fixture.request.Config.capabilities&capCreateStats == 0 {
		t.Skip("fixture configuration does not enable creator statistics")
	}

	candidate, err := testBuilder().BuildMaster(context.Background(), fixture.request)
	if err != nil {
		t.Fatalf("build masterchain candidate: %v", err)
	}

	// Correctness first: whatever the update contains, it must still turn the
	// predecessor state into exactly the state the candidate claims.
	if err = cell.ValidateMerkleUpdate(candidate.StateUpdate); err != nil {
		t.Fatalf("validate masterchain state update: %v", err)
	}
	applied, err := cell.ApplyMerkleUpdate(fixture.request.Previous.State, candidate.StateUpdate)
	if err != nil {
		t.Fatalf("apply masterchain state update: %v", err)
	}
	if applied.HashKey() != candidate.State.HashKey() {
		t.Fatal("masterchain state update does not produce the candidate state")
	}

	updateBOC := len(candidate.StateUpdate.ToBOC())
	t.Logf("creator dictionary %d bytes, state update %d bytes, block %d bytes",
		statsBOC, updateBOC, len(candidate.BlockBOC))
	if updateBOC > maxUpdateBOC {
		t.Fatalf(
			"state update is %d bytes over a %d-byte creator dictionary (limit %d): untouched creator subtrees are being copied instead of pruned",
			updateBOC, statsBOC, maxUpdateBOC,
		)
	}

	// A second build over the same request must be byte-identical: the pruning
	// decision is part of the block bytes, so a non-deterministic source graph
	// would show up here before it showed up as a rejected candidate.
	again, err := testBuilder().BuildMaster(context.Background(), fixture.request)
	if err != nil {
		t.Fatalf("rebuild masterchain candidate: %v", err)
	}
	if !bytes.Equal(candidate.BlockBOC, again.BlockBOC) {
		t.Fatal("masterchain candidate bytes changed between two builds of the same request")
	}
}

// creatorStatsPruneKey spreads keys across the dictionary's prefix space so the
// seeded entries form a deep tree rather than one long shared prefix; a tree
// with a shallow fan-out would prune well by accident.
func creatorStatsPruneKey(index int) [32]byte {
	var key [32]byte
	binary.BigEndian.PutUint64(key[:8], uint64(index)*0x9E3779B97F4A7C15)
	binary.BigEndian.PutUint64(key[8:16], uint64(index)+1)
	return key
}
