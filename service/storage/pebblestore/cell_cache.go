package pebblestore

import (
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	DefaultDecodedCellCacheShards = 64

	// DefaultDecodedCellCacheEntries sizes THE decoded cell cache. There is one:
	// every consumer that decodes a cell out of celldb — the lightserver, proof
	// building, the archive importer, sync and download, collation and
	// validation — shares it.
	//
	// It is small on purpose. Each entry is a live Go object graph that every GC
	// mark cycle has to scan, so the cost of this cache is paid per collection,
	// not once. With slab-based lazy cells (one heap object for a cell with
	// refs, two for a leaf) and the one-object CLOCK entry below, an entry is
	// ~3-4 live objects, which is what let this default double from 64Ki to
	// 128Ki without raising the mark bill of the old LRU (whose entry carried a
	// list.Element and an interface box besides the cell). Bulk capacity is
	// still bought off-heap: in the encoded record cache (malloc'd under cgo),
	// pebble's block cache and the OS page cache, where a byte costs nothing to
	// mark.
	//
	// This was briefly two caches, one per consumer class. That is not
	// re-openable as a sizing question alone: the collator and the validator
	// must hold the SAME *cell.Cell for a parent, because
	// ChainState.validatedCandidateState compares tip states by pointer, and two
	// caches cannot both supply one object. See
	// TestOneDecodedCellCacheGivesPointerIdentity.
	DefaultDecodedCellCacheEntries = 128 << 10

	// decodedCellCacheWays is the bucket associativity: how many entries share
	// one CLOCK ring. Eight 8-byte slot pointers are two cache lines, and eight
	// ways keeps the conflict-miss rate of hash-distributed keys negligible
	// while bounding both the lookup probe and the victim scan to one bucket.
	decodedCellCacheWays = 8
)

// decodedCellCache is a sharded, set-associative CLOCK cache with LOCK-FREE
// reads.
//
// Why not the LRU it replaced: an LRU hit is a WRITE — list.MoveToFront under
// an exclusive shard mutex — so the hottest entries (the dictionary top that
// every consumer resolves) serialized exactly the most-served lookups. Measured
// on the old cache: 55 ns sequential, 48-60 ns at ten workers on uniform keys,
// but 155 ns at ten workers on one hot key. Putting the same LRU behind an
// RWMutex does not fix it (124 ns: RLock's readerCount atomic itself
// ping-pongs), and copy-on-write per insert would copy ~1024 entries per shard
// on every miss. The design that composes read scaling with eviction is this
// one: a flat table of buckets x ways of atomic.Pointer slots. A hit is a
// handful of atomic loads plus a key compare on an immutable entry, and its
// only write is setting the entry's CLOCK ref bit WHEN IT IS CLEAR — a
// steady-state hit on a hot entry writes nothing at all and scales flat.
//
// Writers (insert, generation sweep) serialize on a per-shard mutex and evict
// with a per-bucket CLOCK scan: entries whose ref bit is set get it cleared and
// survive the pass, entries without it are victims. Entries are immutable once
// published; replacement publishes a NEW entry object into the slot, and
// readers that loaded the old pointer keep a valid cell.
type decodedCellCache struct {
	shards []decodedCellCacheShard
}

// decodedCellCacheConfig sizes the cache in ENTRIES, not bytes. Each entry is a
// live Go object graph (a *cell.Cell plus its meta, body slice and ref
// placeholders) that every GC mark cycle has to scan, so the cost that matters
// is the object count, which an entry count bounds directly and a byte budget
// does not.
type decodedCellCacheConfig struct {
	enabled bool
	shards  int
	entries int
}

type decodedCellCacheShard struct {
	// mu serializes writers only: set and deleteGeneration. Lookups never take
	// it.
	mu sync.Mutex
	// slots is the flat bucket table: bucketCount buckets of ways slots each,
	// bucket b occupying slots[b*ways : (b+1)*ways].
	slots []atomic.Pointer[decodedCellCacheEntry]
	// hands holds each bucket's CLOCK hand (a way index). Guarded by mu.
	hands []uint8
	// bucketMask is bucketCount-1; bucketCount is a power of two.
	bucketMask uint32
	ways       int
}

// decodedCellCacheEntry is immutable once published except for its ref bit.
// ONE small struct per cached cell — the old LRU entry carried a list.Element
// and an interface box on top, which the GC then had to mark per entry per
// cycle.
type decodedCellCacheEntry struct {
	key  decodedCellCacheKey
	cell *cell.Cell
	// ref is the CLOCK recency bit. Hits set it ONLY when it is clear, so a hot
	// entry's steady state is a pure read; the eviction scan clears it to give
	// the entry one more pass.
	ref atomic.Bool
}

type decodedCellCacheKey struct {
	generation uint64
	hash       [32]byte
}

func newDecodedCellCache(cfg decodedCellCacheConfig) *decodedCellCache {
	if !cfg.enabled || cfg.entries <= 0 {
		return nil
	}

	entries := cfg.entries
	shards := cfg.shards
	if shards < 1 {
		shards = 1
	}
	if shards > entries {
		shards = entries
	}

	cache := &decodedCellCache{
		shards: make([]decodedCellCacheShard, shards),
	}
	perShard := entries / shards
	if perShard < 1 {
		perShard = 1
	}

	// Bucket geometry: the bucket count must be a power of two so lookups can
	// mask instead of divide, so the per-shard capacity rounds DOWN to
	// ways * 2^k <= perShard. Entries stay a hard cap — a set-associative table
	// cannot exceed its slot count by construction — and the effective capacity
	// is what capacity() reports and open.go logs.
	ways := perShard
	if ways > decodedCellCacheWays {
		ways = decodedCellCacheWays
	}
	buckets := 1
	for buckets*2*ways <= perShard {
		buckets *= 2
	}

	for i := range cache.shards {
		cache.shards[i] = decodedCellCacheShard{
			slots:      make([]atomic.Pointer[decodedCellCacheEntry], buckets*ways),
			hands:      make([]uint8, buckets),
			bucketMask: uint32(buckets - 1),
			ways:       ways,
		}
	}
	return cache
}

// capacity reports the total number of entries the cache can hold across all
// of its shards. A nil cache holds nothing.
func (c *decodedCellCache) capacity() int {
	if c == nil {
		return 0
	}

	total := 0
	for i := range c.shards {
		total += len(c.shards[i].slots)
	}
	return total
}

// shardCount reports how many shards the cache actually has, which is not
// always what was requested: newDecodedCellCache clamps shards down to entries
// when entries < shards, so a request for 64 shards over 32 entries yields 32.
// Startup logging reports this rather than the request.
func (c *decodedCellCache) shardCount() int {
	if c == nil {
		return 0
	}
	return len(c.shards)
}

// len reports how many entries are resident right now. It exists for tests and
// diagnostics, and it reads every slot, so it is not for the hot path.
func (c *decodedCellCache) len() int {
	if c == nil {
		return 0
	}

	total := 0
	for i := range c.shards {
		shard := &c.shards[i]
		for j := range shard.slots {
			if shard.slots[j].Load() != nil {
				total++
			}
		}
	}
	return total
}

func (c *decodedCellCache) get(generation uint64, hash []byte) (*cell.Cell, error) {
	if c == nil {
		return nil, storage.ErrNotFound
	}

	return c.getKey(newDecodedCellCacheKey(generation, hash))
}

func (c *decodedCellCache) getHash(generation uint64, hash cell.Hash) (*cell.Cell, error) {
	if c == nil {
		return nil, storage.ErrNotFound
	}

	return c.getKey(decodedCellCacheKey{
		generation: generation,
		hash:       [32]byte(hash),
	})
}

// getKey is the LOCK-FREE hit path: probe one bucket's ways with atomic
// pointer loads and compare the immutable key inside the entry. The only write
// a hit can make is setting the CLOCK ref bit, and only when it is clear —
// repeated hits on a hot entry are pure reads, which is what keeps the hot-key
// parallel cost flat instead of the old LRU's serialized MoveToFront.
func (c *decodedCellCache) getKey(key decodedCellCacheKey) (*cell.Cell, error) {
	shard := &c.shards[c.shardIndex(key.hash)]
	base := shard.bucketBase(key.hash)
	for way := 0; way < shard.ways; way++ {
		entry := shard.slots[base+way].Load()
		if entry == nil || entry.key != key {
			continue
		}
		if !entry.ref.Load() {
			entry.ref.Store(true)
		}
		return entry.cell, nil
	}
	return nil, storage.ErrNotFound
}

func (c *decodedCellCache) set(generation uint64, hash []byte, loaded *cell.Cell) {
	if c == nil {
		return
	}

	key := newDecodedCellCacheKey(generation, hash)
	shard := &c.shards[c.shardIndex(key.hash)]
	entry := &decodedCellCacheEntry{key: key, cell: loaded}

	shard.mu.Lock()
	defer shard.mu.Unlock()

	base := shard.bucketBase(key.hash)
	bucket := base / shard.ways

	// Same key already resident: replace in place. The new entry keeps the ref
	// bit set — it was just written, which counts as use.
	freeWay := -1
	for way := 0; way < shard.ways; way++ {
		resident := shard.slots[base+way].Load()
		if resident == nil {
			if freeWay < 0 {
				freeWay = way
			}
			continue
		}
		if resident.key == key {
			entry.ref.Store(true)
			shard.slots[base+way].Store(entry)
			return
		}
	}
	if freeWay >= 0 {
		shard.slots[base+freeWay].Store(entry)
		return
	}

	// CLOCK victim scan over the full bucket: a set ref bit buys exactly one
	// more pass, so the scan terminates within two sweeps. New entries publish
	// with the ref bit CLEAR: an entry nobody reads again is the next pass's
	// victim, an entry that got hit survives it.
	hand := int(shard.hands[bucket])
	for sweep := 0; sweep < 2*shard.ways; sweep++ {
		resident := shard.slots[base+hand].Load()
		if resident != nil && resident.ref.Load() {
			resident.ref.Store(false)
			hand = (hand + 1) % shard.ways
			continue
		}
		shard.slots[base+hand].Store(entry)
		shard.hands[bucket] = uint8((hand + 1) % shard.ways)
		return
	}
	// Every ref bit was set twice over by concurrent readers while we scanned.
	// Evict at the hand anyway: this is a cache, not a registry.
	shard.slots[base+hand].Store(entry)
	shard.hands[bucket] = uint8((hand + 1) % shard.ways)
}

func (c *decodedCellCache) deleteGeneration(generation uint64) {
	if c == nil {
		return
	}

	for i := range c.shards {
		shard := &c.shards[i]
		shard.mu.Lock()
		for j := range shard.slots {
			entry := shard.slots[j].Load()
			if entry != nil && entry.key.generation == generation {
				shard.slots[j].Store(nil)
			}
		}
		shard.mu.Unlock()
	}
}

func (c *decodedCellCache) shardIndex(hash [32]byte) int {
	return int(binary.BigEndian.Uint32(hash[:4]) % uint32(len(c.shards)))
}

// bucketBase maps a hash to the first slot of its bucket. It reads a hash word
// disjoint from the one shardIndex uses, so the bucket spread within a shard is
// independent of the shard split.
func (s *decodedCellCacheShard) bucketBase(hash [32]byte) int {
	return int(binary.BigEndian.Uint32(hash[4:8])&s.bucketMask) * s.ways
}

func newDecodedCellCacheKey(generation uint64, hash []byte) decodedCellCacheKey {
	var key decodedCellCacheKey
	key.generation = generation
	copy(key.hash[:], hash)
	return key
}
