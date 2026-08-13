package collator

import (
	"testing"

	"github.com/xssnick/gton/service/validator/msgpool"
)

func partitionTestKey(nextWorkchain int32, nextPrefix uint64, hash byte) msgpool.QueueKey {
	var key msgpool.QueueKey
	binaryPutInt32(key[0:4], nextWorkchain)
	binaryPutUint64(key[4:12], nextPrefix)
	for i := 12; i < len(key); i++ {
		key[i] = hash
	}

	return key
}

func binaryPutInt32(dst []byte, v int32) {
	u := uint32(v)
	dst[0], dst[1], dst[2], dst[3] = byte(u>>24), byte(u>>16), byte(u>>8), byte(u)
}

func binaryPutUint64(dst []byte, v uint64) {
	for i := range 8 {
		dst[i] = byte(v >> (56 - 8*i))
	}
}

// One scan partitioned by next hop has to reproduce, per neighbor, exactly the
// order a per-neighbor scan produced: the canonical comparator is a total order
// on queue keys, so restricting it to a subset is the same as sorting that
// subset. This is what makes the single pass safe when a shard has more than
// one neighbor — the case cleanup_test.go never reaches.
func TestQueueCandidatePartitionPreservesPerNeighborOrder(t *testing.T) {
	master := msgpool.ShardIdent{Workchain: -1, Shard: msgpool.ShardAll}
	left := msgpool.ShardIdent{Workchain: 0, Shard: 0x4000000000000000}
	right := msgpool.ShardIdent{Workchain: 0, Shard: 0xc000000000000000}

	// Interleaved across neighbors and deliberately out of lt order, so a
	// partition that leaked the global position would show up immediately.
	candidates := []dequeueCandidate{
		{lt: 10, key: partitionTestKey(0, 0x2000000000000000, 0x01)},
		{lt: 11, key: partitionTestKey(-1, 0x8000000000000000, 0x02)},
		{lt: 12, key: partitionTestKey(0, 0xe000000000000000, 0x03)},
		{lt: 13, key: partitionTestKey(0, 0x6000000000000000, 0x04)},
		{lt: 14, key: partitionTestKey(-1, 0x8000000000000000, 0x05)},
		{lt: 15, key: partitionTestKey(0, 0xa000000000000000, 0x06)},
	}

	parts := []cleanupPart{
		{neighbor: Neighbor{Shard: master}},
		{neighbor: Neighbor{Shard: left}},
		{neighbor: Neighbor{Shard: right}},
	}
	partitionQueueCandidates(parts, candidates)

	want := map[msgpool.ShardIdent][]uint64{
		master: {11, 14},
		left:   {10, 13},
		right:  {12, 15},
	}
	total := 0
	for i := range parts {
		expected := want[parts[i].neighbor.Shard]
		if len(parts[i].candidates) != len(expected) {
			t.Fatalf("%+v got %d candidates, want %d", parts[i].neighbor.Shard, len(parts[i].candidates), len(expected))
		}
		for j := range expected {
			if parts[i].candidates[j].lt != expected[j] {
				t.Fatalf("%+v candidate %d lt = %d, want %d",
					parts[i].neighbor.Shard, j, parts[i].candidates[j].lt, expected[j])
			}
		}
		total += len(parts[i].candidates)
	}
	if total != len(candidates) {
		t.Fatalf("partition covered %d of %d candidates", total, len(candidates))
	}
}

// Every entry belongs to exactly one neighbor. A key whose next hop no neighbor
// covers must be dropped rather than attach itself to the first bucket.
func TestQueueCandidatePartitionDropsUncoveredNextHops(t *testing.T) {
	parts := []cleanupPart{
		{neighbor: Neighbor{Shard: msgpool.ShardIdent{Workchain: 0, Shard: 0x4000000000000000}}},
	}
	partitionQueueCandidates(parts, []dequeueCandidate{
		{lt: 1, key: partitionTestKey(0, 0xc000000000000000, 0x07)},
	})
	if len(parts[0].candidates) != 0 {
		t.Fatal("an entry outside every neighbor prefix was bucketed")
	}
}
