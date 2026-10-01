package pebblestore

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

func decodedCellLoadKey(i int) decodedCellCacheKey {
	var key decodedCellCacheKey
	key.generation = 1
	// The shard is taken from the last byte, so the counter is written there to
	// spread the keys the way real hashes do.
	key.hash[31] = byte(i)
	key.hash[0] = byte(i >> 8)
	key.hash[1] = byte(i >> 16)

	return key
}

// The split has to be real: a group whose keys all land in one shard is a group
// that was never sharded, and the benchmark below would then measure the old
// lock while claiming to measure the new one.
func TestDecodedCellLoadGroupSpreadsAcrossShards(t *testing.T) {
	var group decodedCellLoadGroup
	seen := map[*decodedCellLoadShard]int{}
	for i := range 4096 {
		seen[group.shardOf(decodedCellLoadKey(i))]++
	}
	if len(seen) != decodedCellLoadShards {
		t.Fatalf("4096 keys reached %d of %d shards", len(seen), decodedCellLoadShards)
	}
	// Even occupancy matters as much as coverage: a mask that folded most keys
	// into one shard would still "reach" all of them.
	want := 4096 / decodedCellLoadShards
	for shard, n := range seen {
		if n != want {
			t.Fatalf("shard %p took %d keys, want %d", shard, n, want)
		}
	}
}

// Coalescing is per key and must survive the split: two callers of the same key
// still share one load, and that is true whichever shard the key lands in.
func TestDecodedCellLoadGroupStillCoalescesPerKey(t *testing.T) {
	var group decodedCellLoadGroup
	for _, i := range []int{0, 1, 63, 64, 255} {
		key := decodedCellLoadKey(i)
		var loads atomic.Int64
		release := make(chan struct{})
		joined := make(chan struct{})
		done := make(chan *cell.Cell, 2)
		leader := cell.BeginCell().MustStoreUInt(uint64(i), 32).EndCell()

		go func() {
			c, err := group.do(context.Background(), key, func(context.Context) (*cell.Cell, error) {
				loads.Add(1)
				close(joined)
				<-release
				return leader, nil
			})
			if err != nil {
				t.Error(err)
			}
			done <- c
		}()
		<-joined
		// The follower must be parked on the flight before the leader is let go,
		// or the leader finishes and retires the flight first and the follower
		// simply runs its own load — which would pass a weaker assertion while
		// proving nothing. decodedCellFollowerContext reports the moment do()
		// selects on the flight, which is exactly that point.
		follower := &decodedCellFollowerContext{Context: context.Background(), joined: make(chan struct{})}
		go func() {
			c, err := group.do(follower, key, func(context.Context) (*cell.Cell, error) {
				loads.Add(1)
				return cell.BeginCell().EndCell(), nil
			})
			if err != nil {
				t.Error(err)
			}
			done <- c
		}()
		<-follower.joined
		close(release)
		for range 2 {
			if got := <-done; got != leader {
				t.Fatalf("key %d: follower did not receive the leader's cell", i)
			}
		}
		if n := loads.Load(); n != 1 {
			t.Fatalf("key %d: %d loads ran, want 1", i, n)
		}
	}
}

// What the sharding is for. The load itself is trivial on purpose: the cost
// being measured is the table's own lock, which every cold miss takes twice.
func BenchmarkDecodedCellLoadGroupContended(b *testing.B) {
	var group decodedCellLoadGroup
	loaded := cell.BeginCell().EndCell()
	var counter atomic.Int64

	b.RunParallel(func(pb *testing.PB) {
		i := int(counter.Add(1)) << 20
		for pb.Next() {
			i++
			_, err := group.do(context.Background(), decodedCellLoadKey(i), func(context.Context) (*cell.Cell, error) {
				return loaded, nil
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
