package pebblestore

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"sync"
	"testing"

	"github.com/xssnick/gton/internal/manualmem"
)

// testRecordCache builds a small deterministic cache: one shard, two 4 KiB
// regions, index sized by the caller. Every hash lands in shard 0.
func testRecordCache(t *testing.T, regions, regionBytes, slots int) *cellRecordCache {
	t.Helper()

	cache := newCellRecordCache(cellRecordCacheConfig{
		shards:             1,
		regionsPerShard:    regions,
		regionBytes:        regionBytes,
		indexSlotsPerShard: slots,
	})
	if cache == nil {
		t.Fatal("record cache config rejected")
	}
	t.Cleanup(cache.free)
	return cache
}

// selfDescribingRecord builds a hash and a record whose bytes embed that hash,
// so any lookup result can be verified against the key it was requested under
// — the property the concurrent stress leans on.
func selfDescribingRecord(n uint64, size int) (hash []byte, record []byte) {
	hash = make([]byte, 32)
	binary.BigEndian.PutUint64(hash[0:8], n*0x9e3779b97f4a7c15+1)
	binary.BigEndian.PutUint64(hash[8:16], n*0xbf58476d1ce4e5b9+1)
	binary.BigEndian.PutUint64(hash[16:24], n)
	if size < 40 {
		size = 40
	}
	record = make([]byte, size)
	copy(record, hash)
	binary.BigEndian.PutUint64(record[32:40], n)
	return hash, record
}

func TestCellRecordCacheRoundtrip(t *testing.T) {
	cache := testRecordCache(t, 2, 4096, 1024)

	hash, record := selfDescribingRecord(1, 104)
	if got := cache.get(hash, nil); got != nil {
		t.Fatal("empty cache answered a lookup")
	}
	cache.put(hash, record)

	got := cache.get(hash, nil)
	if !bytes.Equal(got, record) {
		t.Fatalf("roundtrip mismatch: got %d bytes, want %d identical ones", len(got), len(record))
	}

	// A second put of the same hash is a no-op, not a second copy.
	cache.put(hash, record)
	stats := cache.stats()
	if stats.Entries != 1 {
		t.Fatalf("duplicate put filed %d entries, want 1", stats.Entries)
	}
	if stats.Bytes != int64(recordCacheHeaderSize+len(record)) {
		t.Fatalf("resident bytes = %d, want %d", stats.Bytes, recordCacheHeaderSize+len(record))
	}
}

// Two different hashes sharing the SAME first 16 bytes share both the 8-byte
// index prefix and the probe origin. The in-arena full hash is what keeps them
// exact; a cache comparing prefixes alone would answer one's record for the
// other.
func TestCellRecordCachePrefixCollisionsAreExact(t *testing.T) {
	cache := testRecordCache(t, 2, 4096, 1024)

	hashA, recordA := selfDescribingRecord(7, 104)
	hashB := bytes.Clone(hashA)
	hashB[31] ^= 0xff // same prefix, same probe origin, different hash
	recordB := bytes.Clone(recordA)
	copy(recordB, hashB)
	recordB[100] ^= 0xaa

	cache.put(hashA, recordA)
	cache.put(hashB, recordB)

	if got := cache.get(hashA, nil); !bytes.Equal(got, recordA) {
		t.Fatal("prefix twin A did not round-trip its own record")
	}
	if got := cache.get(hashB, nil); !bytes.Equal(got, recordB) {
		t.Fatal("prefix twin B did not round-trip its own record")
	}
}

// Region rotation is the eviction: once the ring recycles the region an entry
// lives in, its slot is a ghost and the lookup is a miss — while entries in
// still-live regions keep answering.
func TestCellRecordCacheRegionRotationEvicts(t *testing.T) {
	cache := testRecordCache(t, 2, 4096, 1024)

	// ~136 B per entry, 4096 B regions: 30 entries per region. Insert three
	// regions' worth without ever reading, so nothing is salvaged.
	const total = 90
	for i := uint64(0); i < total; i++ {
		hash, record := selfDescribingRecord(i, 100)
		cache.put(hash, record)
	}

	earliestHash, _ := selfDescribingRecord(0, 100)
	if got := cache.get(earliestHash, nil); got != nil {
		t.Fatal("an entry from a recycled region is still answered; generation bump did not invalidate it")
	}
	// The sharper half: a recycled region's entry whose BYTES are still intact
	// (the ring has not reached them yet) must be a ghost purely by generation.
	// 90 entries = exactly three 30-entry regions over a 2-region ring, so the
	// final region holds keys 60..89 and key 31's bytes in the re-recycled
	// region... keep it simple with a dedicated cache below instead.
	fresh := testRecordCache(t, 2, 4096, 1024)
	for i := uint64(0); i < 61; i++ {
		hash, record := selfDescribingRecord(i, 100)
		fresh.put(hash, record)
	}
	// 61 inserts: region 0 holds 0..29, region 1 holds 30..59, insert 60
	// recycles region 0 and overwrites only key 0's bytes. Key 1's bytes are
	// intact in the arena; ONLY the generation says it is gone.
	intactHash, _ := selfDescribingRecord(1, 100)
	if got := fresh.get(intactHash, nil); got != nil {
		t.Fatal("a recycled region's entry with intact bytes is still answered; the lookup is not checking the region generation")
	}
	latestHash, latestRecord := selfDescribingRecord(total-1, 100)
	if got := cache.get(latestHash, nil); !bytes.Equal(got, latestRecord) {
		t.Fatal("the freshest entry is gone; rotation evicted the write region itself")
	}

	stats := cache.stats()
	if stats.Entries <= 0 || stats.Bytes <= 0 {
		t.Fatalf("liveness accounting broke: entries=%d bytes=%d", stats.Entries, stats.Bytes)
	}
	if stats.Bytes > cache.capacityBytes() {
		t.Fatalf("resident bytes %d exceed the arena %d", stats.Bytes, cache.capacityBytes())
	}
}

// The CLOCK salvage contract at rotation: an entry that was READ survives its
// region's death by re-append, an entry that was only written does not.
func TestCellRecordCacheSalvageKeepsHotDropsCold(t *testing.T) {
	cache := testRecordCache(t, 2, 4096, 1024)

	hotHash, hotRecord := selfDescribingRecord(1000, 100)
	cache.put(hotHash, hotRecord)
	coldHashes := make([][]byte, 0, 10)
	for i := uint64(0); i < 10; i++ {
		hash, record := selfDescribingRecord(i, 100)
		cache.put(hash, record)
		coldHashes = append(coldHashes, hash)
	}

	// The read that sets the ref bit.
	if got := cache.get(hotHash, nil); !bytes.Equal(got, hotRecord) {
		t.Fatal("hot entry vanished before rotation")
	}

	// Drive the ring all the way around so the hot entry's region is recycled.
	for i := uint64(2000); i < 2070; i++ {
		hash, record := selfDescribingRecord(i, 100)
		cache.put(hash, record)
	}

	if got := cache.get(hotHash, nil); !bytes.Equal(got, hotRecord) {
		t.Fatal("the READ entry did not survive its region's rotation; salvage is not honouring the ref bit")
	}
	for _, hash := range coldHashes {
		if got := cache.get(hash, nil); got != nil {
			t.Fatalf("a never-read entry %x survived its region's rotation", hash[:4])
		}
	}
}

// The salvage budget is the live-lock guard: when more than half a region is
// hot, salvage stops at the budget and counts the truncation instead of
// carrying the whole region forward forever.
func TestCellRecordCacheSalvageBudgetTruncates(t *testing.T) {
	cache := testRecordCache(t, 2, 4096, 1024)

	// Fill region 0 with entries and read EVERY one, making them all hot:
	// ~30 entries x 136 B = ~4 KB hot bytes against a 2 KB salvage budget.
	hot := make([][]byte, 0, 30)
	for i := uint64(0); i < 30; i++ {
		hash, record := selfDescribingRecord(i, 100)
		cache.put(hash, record)
		if got := cache.get(hash, nil); got == nil {
			break // region rotated mid-fill; the ones already read are enough
		}
		hot = append(hot, hash)
	}

	// Rotate the ring twice over.
	for i := uint64(5000); i < 5090; i++ {
		hash, record := selfDescribingRecord(i, 100)
		cache.put(hash, record)
	}

	if got := cache.stats().SalvageTruncated; got == 0 {
		t.Fatal("an all-hot region rotated without ever hitting the 50% salvage budget")
	}

	survivors := 0
	for _, hash := range hot {
		if cache.get(hash, nil) != nil {
			survivors++
		}
	}
	if survivors == len(hot) {
		t.Fatal("every hot entry survived; the salvage budget is not being enforced")
	}
}

// An insert that cannot place a slot within the probe budget is declined and
// counted — the index never grows and never displaces a live chain for a
// newcomer.
func TestCellRecordCacheDeclinesOnTablePressure(t *testing.T) {
	// 32 slots, arena big enough that rotation never frees any: pressure is
	// purely on the index.
	cache := testRecordCache(t, 2, 64<<10, 32)

	for i := uint64(0); i < 200; i++ {
		hash, record := selfDescribingRecord(i, 100)
		cache.put(hash, record)
	}

	stats := cache.stats()
	if stats.Declined == 0 {
		t.Fatalf("200 inserts into a 32-slot index declined nothing (inserts=%d)", stats.Inserts)
	}
	if stats.Entries > 32 {
		t.Fatalf("%d live entries in a 32-slot index", stats.Entries)
	}
	if stats.Inserts+stats.Declined != 200 {
		t.Fatalf("inserts=%d + declined=%d should account for all 200 puts", stats.Inserts, stats.Declined)
	}
}

// Close returns every byte to the allocator. Under cgo this is C memory: a
// leak here is invisible to the Go runtime forever, which is why the balance
// is asserted rather than trusted.
func TestCellRecordCacheFreeReturnsManualMemory(t *testing.T) {
	before := manualmem.Allocated()

	cache := newCellRecordCache(cellRecordCacheConfig{
		shards:          4,
		regionsPerShard: 2,
		regionBytes:     64 << 10,
	})
	if cache == nil {
		t.Fatal("record cache config rejected")
	}
	held := manualmem.Allocated() - before
	want := cache.capacityBytes() + cache.indexBytes()
	if held != want {
		t.Fatalf("allocator balance grew by %d, want arena+index = %d", held, want)
	}

	cache.free()
	if got := manualmem.Allocated() - before; got != 0 {
		t.Fatalf("free left %d bytes outstanding in the manual allocator", got)
	}
	cache.free() // double free must be a no-op
	if got := manualmem.Allocated() - before; got != 0 {
		t.Fatalf("second free moved the balance to %d", got)
	}

	if got := cache.get([]byte("0123456789abcdef0123456789abcdef"), nil); got != nil {
		t.Fatal("a freed cache answered a lookup")
	}
}

func TestCellRecordCacheConfigGeometry(t *testing.T) {
	cfg := cellRecordCacheConfigFromBytes(4 << 30)
	if cfg.shards != cellRecordCacheShards {
		t.Fatalf("shards = %d, want %d", cfg.shards, cellRecordCacheShards)
	}
	if cfg.regionBytes != recordCacheRegionTargetBytes {
		t.Fatalf("region bytes = %d, want %d", cfg.regionBytes, recordCacheRegionTargetBytes)
	}
	if got := int64(cfg.shards) * int64(cfg.regionsPerShard) * int64(cfg.regionBytes); got != 4<<30 {
		t.Fatalf("effective arena = %d, want the full 4 GiB", got)
	}

	// A dust-sized budget clamps UP to a workable ring; zero is the off switch
	// and is handled before this derivation.
	tiny := cellRecordCacheConfigFromBytes(1 << 20)
	if tiny.regionsPerShard < 2 || tiny.regionBytes < recordCacheRegionMinBytes {
		t.Fatalf("tiny budget produced an unworkable ring: %d regions of %d bytes", tiny.regionsPerShard, tiny.regionBytes)
	}
}

// Concurrent readers and writers over regions small enough to rotate
// constantly, for the -race run. Correctness bar: a lookup either misses or
// returns the exact record filed under that hash — the self-describing
// payload proves which.
func TestCellRecordCacheConcurrentStress(t *testing.T) {
	cache := testRecordCache(t, 3, 4096, 1024)

	const keys = 256
	hashes := make([][]byte, keys)
	records := make([][]byte, keys)
	for i := range hashes {
		hashes[i], records[i] = selfDescribingRecord(uint64(i), 60+i%120)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			var buf []byte
			for {
				select {
				case <-stop:
					return
				default:
				}
				i := rng.Intn(keys)
				buf = cache.get(hashes[i], buf)
				if buf != nil && !bytes.Equal(buf, records[i]) {
					t.Errorf("lookup under key %d returned another record", i)
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
				cache.put(hashes[i], records[i])
			}
		}(int64(100 + worker))
	}

	for i := 0; i < 30_000; i++ {
		k := i % keys
		cache.put(hashes[k], records[k])
		if got := cache.get(hashes[k], nil); got != nil && !bytes.Equal(got, records[k]) {
			t.Fatal("main goroutine lookup returned another record")
		}
	}
	close(stop)
	wg.Wait()

	stats := cache.stats()
	if stats.Bytes > cache.capacityBytes() {
		t.Fatalf("resident bytes %d exceed the arena %d after stress", stats.Bytes, cache.capacityBytes())
	}
	if stats.Entries < 0 || stats.Bytes < 0 {
		t.Fatalf("liveness accounting went negative: entries=%d bytes=%d", stats.Entries, stats.Bytes)
	}
}

// Nil-cache calls are the disabled configuration and must all be no-ops.
func TestCellRecordCacheNilIsDisabled(t *testing.T) {
	var cache *cellRecordCache
	hash, record := selfDescribingRecord(1, 100)
	cache.put(hash, record)
	if got := cache.get(hash, nil); got != nil {
		t.Fatal("nil cache answered")
	}
	cache.free()
	if stats := cache.stats(); stats != (cellRecordCacheStats{}) {
		t.Fatalf("nil cache reports stats %+v", stats)
	}
	if cache.capacityBytes() != 0 || cache.indexBytes() != 0 {
		t.Fatal("nil cache reports capacity")
	}
}

func TestCellRecordCacheStressAccountingClosesOnQuiescence(t *testing.T) {
	cache := testRecordCache(t, 2, 4096, 1024)

	// Deterministic single-threaded churn: after any amount of it, the live
	// counters must equal what a fresh scan of the cache can still answer.
	live := map[string][]byte{}
	for i := uint64(0); i < 500; i++ {
		hash, record := selfDescribingRecord(i%80, 90+int(i%40))
		cache.put(hash, record)
		if i%3 == 0 {
			cache.get(hash, nil)
		}
		live[string(hash)] = record
	}

	answered := int64(0)
	for hash := range live {
		if cache.get([]byte(hash), nil) != nil {
			answered++
		}
	}
	stats := cache.stats()
	if stats.Entries != answered {
		t.Fatalf("liveEntries=%d but %d keys are answerable", stats.Entries, answered)
	}
}
