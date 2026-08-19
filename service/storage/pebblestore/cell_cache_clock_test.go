package pebblestore

import (
	"encoding/binary"
	"errors"
	"math/rand"
	"sync"
	"testing"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// oneBucketCache builds the smallest cache in which eviction is observable and
// deterministic: one shard whose table is a single bucket of
// decodedCellCacheWays ways, so every key collides and the CLOCK scan is the
// only thing deciding who stays.
func oneBucketCache(t *testing.T) *decodedCellCache {
	t.Helper()

	cache := newDecodedCellCache(decodedCellCacheConfig{
		enabled: true,
		shards:  1,
		entries: decodedCellCacheWays,
	})
	if cache == nil {
		t.Fatal("cache is nil")
	}
	shard := &cache.shards[0]
	if len(shard.hands) != 1 || shard.ways != decodedCellCacheWays {
		t.Fatalf("fixture wants one bucket of %d ways, got %d buckets of %d ways",
			decodedCellCacheWays, len(shard.hands), shard.ways)
	}
	return cache
}

func numberedTestCell(t *testing.T, n uint64) (*cell.Cell, cell.Hash) {
	t.Helper()

	c := cell.BeginCell().MustStoreUInt(n, 64).EndCell()
	return c, c.HashKey()
}

// Cache hits must allocate NOTHING: the hit path is probed on every lazy cell
// resolve, and the old LRU already achieved zero allocations per hit — the
// CLOCK rebuild may not regress that while buying its lock freedom.
func TestDecodedCellCacheHitsAllocateZero(t *testing.T) {
	cache := newDecodedCellCache(decodedCellCacheConfig{
		enabled: true,
		shards:  4,
		entries: 64,
	})
	loaded, hash := numberedTestCell(t, 42)
	cache.set(activeCellCacheNamespace, hash[:], loaded)

	allocs := testing.AllocsPerRun(1000, func() {
		got, err := cache.getHash(activeCellCacheNamespace, hash)
		if err != nil || got != loaded {
			t.Fatal("warm lookup missed")
		}
	})
	if allocs != 0 {
		t.Fatalf("getHash hit allocates %.1f objects per call, want 0", allocs)
	}

	allocs = testing.AllocsPerRun(1000, func() {
		got, err := cache.get(activeCellCacheNamespace, hash[:])
		if err != nil || got != loaded {
			t.Fatal("warm slice lookup missed")
		}
	})
	if allocs != 0 {
		t.Fatalf("get hit allocates %.1f objects per call, want 0", allocs)
	}
}

// The CLOCK recency contract, pinned at both ends: an entry that was READ
// survives a full eviction cycle over its bucket, and an entry that was only
// written does not. This is the property the ref bit exists for; a cache that
// ignored it would still pass every routing test while evicting the dictionary
// top as readily as a one-touch cell.
func TestDecodedCellCacheHotEntrySurvivesEvictionCycleOneTouchDoesNot(t *testing.T) {
	cache := oneBucketCache(t)

	hot, hotHash := numberedTestCell(t, 0)
	cache.set(activeCellCacheNamespace, hotHash[:], hot)

	oneTouchHashes := make([]cell.Hash, 0, decodedCellCacheWays-1)
	for i := uint64(1); i < decodedCellCacheWays; i++ {
		filler, fillerHash := numberedTestCell(t, i)
		cache.set(activeCellCacheNamespace, fillerHash[:], filler)
		oneTouchHashes = append(oneTouchHashes, fillerHash)
	}

	// The one read that separates hot from one-touch.
	if _, err := cache.getHash(activeCellCacheNamespace, hotHash); err != nil {
		t.Fatalf("hot entry vanished before the cycle: %v", err)
	}

	// A full eviction cycle: as many inserts as the bucket has ways. Every
	// one-touch entry is a victim candidate with a clear ref bit; the hot entry
	// spends its ref bit to survive the pass.
	for i := uint64(100); i < 100+decodedCellCacheWays-1; i++ {
		churn, churnHash := numberedTestCell(t, i)
		cache.set(activeCellCacheNamespace, churnHash[:], churn)
	}

	if _, err := cache.getHash(activeCellCacheNamespace, hotHash); err != nil {
		t.Fatal("the read entry did not survive a full eviction cycle; the ref bit is not buying a second pass")
	}
	for _, fillerHash := range oneTouchHashes {
		if _, err := cache.getHash(activeCellCacheNamespace, fillerHash); err == nil {
			t.Fatalf("one-touch entry %x survived a full eviction cycle ahead of new inserts", fillerHash[:4])
		}
	}
}

// Capacity is a hard cap by construction — a set-associative table cannot hold
// more entries than it has slots — and the requested entry count is never
// exceeded by the bucket-geometry rounding.
func TestDecodedCellCacheCapacityIsHardCap(t *testing.T) {
	for _, entries := range []int{1, 2, 7, 8, 100, 1 << 10} {
		cache := newDecodedCellCache(decodedCellCacheConfig{
			enabled: true,
			shards:  4,
			entries: entries,
		})
		capacity := cache.capacity()
		if capacity < 1 || capacity > entries {
			t.Fatalf("entries=%d: capacity %d outside (0, %d]", entries, capacity, entries)
		}

		for i := uint64(0); i < uint64(entries)*10; i++ {
			loaded, hash := numberedTestCell(t, i)
			cache.set(activeCellCacheNamespace, hash[:], loaded)
		}
		if got := cache.len(); got > capacity {
			t.Fatalf("entries=%d: %d resident entries over a capacity of %d", entries, got, capacity)
		}
	}
}

// Concurrent readers, writers, and a generation sweeper, for the -race run.
// Correctness bar: a lookup either misses or returns exactly the cell that was
// set under that key — never a cell filed under another key.
func TestDecodedCellCacheConcurrentStress(t *testing.T) {
	cache := newDecodedCellCache(decodedCellCacheConfig{
		enabled: true,
		shards:  4,
		entries: 128,
	})

	const keys = 512
	cells := make([]*cell.Cell, keys)
	hashes := make([]cell.Hash, keys)
	for i := range cells {
		cells[i], hashes[i] = numberedTestCell(t, uint64(i))
	}
	generationOf := func(i int) uint64 { return uint64(i % 3) }

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for {
				select {
				case <-stop:
					return
				default:
				}
				i := rng.Intn(keys)
				got, err := cache.getHash(generationOf(i), hashes[i])
				if err == nil {
					if got != cells[i] {
						t.Error("lookup returned a cell filed under another key")
						return
					}
				} else if !errors.Is(err, storage.ErrNotFound) {
					t.Errorf("lookup error: %v", err)
					return
				}
			}
		}(int64(worker))
	}
	for worker := 0; worker < 2; worker++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for {
				select {
				case <-stop:
					return
				default:
				}
				i := rng.Intn(keys)
				cache.set(generationOf(i), hashes[i][:], cells[i])
			}
		}(int64(100 + worker))
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			cache.deleteGeneration(uint64(i % 3))
		}
	}()

	for i := uint64(0); i < 50_000; i++ {
		k := i % keys
		cache.set(generationOf(int(k)), hashes[k][:], cells[k])
		if got, err := cache.getHash(generationOf(int(k)), hashes[k]); err == nil && got != cells[k] {
			t.Fatal("main goroutine lookup returned a cell filed under another key")
		}
	}
	close(stop)
	wg.Wait()

	if got := cache.len(); got > cache.capacity() {
		t.Fatalf("%d resident entries over a capacity of %d after stress", got, cache.capacity())
	}
}

// Distinct generations of one hash are distinct keys even though they land in
// the same bucket, and a generation sweep removes exactly its own.
func TestDecodedCellCacheGenerationsShareBucketsNotEntries(t *testing.T) {
	cache := oneBucketCache(t)
	loaded, hash := numberedTestCell(t, 7)

	cache.set(1, hash[:], loaded)
	cache.set(2, hash[:], loaded)

	if _, err := cache.getHash(1, hash); err != nil {
		t.Fatalf("generation 1 lookup: %v", err)
	}
	if _, err := cache.getHash(2, hash); err != nil {
		t.Fatalf("generation 2 lookup: %v", err)
	}

	cache.deleteGeneration(1)
	if _, err := cache.getHash(1, hash); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("generation 1 survived its sweep: %v", err)
	}
	if _, err := cache.getHash(2, hash); err != nil {
		t.Fatalf("the sweep took generation 2 with it: %v", err)
	}
}

// Benchmarks for the three shapes that drove the rebuild. Baselines measured on
// the LRU this cache replaced: 55 ns sequential, 48-60 ns parallel-10 uniform,
// 155 ns parallel-10 on one hot key (the exclusive shard mutex plus
// MoveToFront serialized exactly the most-served entries).

func benchWarmDecodedCellCache(b *testing.B, keys int) (*decodedCellCache, []cell.Hash) {
	b.Helper()

	cache := newDecodedCellCache(decodedCellCacheConfig{
		enabled: true,
		shards:  DefaultDecodedCellCacheShards,
		entries: DefaultDecodedCellCacheEntries,
	})
	hashes := make([]cell.Hash, keys)
	for i := 0; i < keys; i++ {
		loaded := cell.BeginCell().MustStoreUInt(uint64(i), 64).EndCell()
		hashes[i] = loaded.HashKey()
		cache.set(activeCellCacheNamespace, hashes[i][:], loaded)
	}
	return cache, hashes
}

func BenchmarkDecodedCellCacheHitSequential(b *testing.B) {
	cache, hashes := benchWarmDecodedCellCache(b, 4096)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		if _, err := cache.getHash(activeCellCacheNamespace, hashes[i&4095]); err != nil {
			b.Fatal("miss on a warm key")
		}
	}
}

func BenchmarkDecodedCellCacheHitParallelUniform(b *testing.B) {
	cache, hashes := benchWarmDecodedCellCache(b, 4096)
	b.ReportAllocs()
	b.SetParallelism(10)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i uint32
		var seed [32]byte
		binary.BigEndian.PutUint32(seed[:4], rand.Uint32())
		i = binary.BigEndian.Uint32(seed[:4])
		for pb.Next() {
			i++
			if _, err := cache.getHash(activeCellCacheNamespace, hashes[i&4095]); err != nil {
				b.Fatal("miss on a warm key")
			}
		}
	})
}

func BenchmarkDecodedCellCacheHitParallelHot(b *testing.B) {
	cache, hashes := benchWarmDecodedCellCache(b, 4096)
	hot := hashes[0]
	b.ReportAllocs()
	b.SetParallelism(10)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := cache.getHash(activeCellCacheNamespace, hot); err != nil {
				b.Fatal("miss on the hot key")
			}
		}
	})
}

func BenchmarkDecodedCellCacheInsert(b *testing.B) {
	cache, _ := benchWarmDecodedCellCache(b, 4096)
	loaded := cell.BeginCell().MustStoreUInt(1<<40, 64).EndCell()
	hashes := make([]cell.Hash, 1<<16)
	for i := range hashes {
		binary.BigEndian.PutUint64(hashes[i][:8], uint64(i)*0x9e3779b97f4a7c15)
		binary.BigEndian.PutUint64(hashes[i][8:16], uint64(i)*0xbf58476d1ce4e5b9)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		cache.set(activeCellCacheNamespace, hashes[i&(1<<16-1)][:], loaded)
	}
}
