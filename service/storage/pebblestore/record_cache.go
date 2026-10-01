package pebblestore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/xssnick/gton/internal/manualmem"
)

// The encoded cell RECORD cache: the tier between the decoded cell cache and
// pebble. It holds celldb records — the raw bytes a pebble Get would return,
// BEFORE decode — so a hit costs a probe, a copy and a 211-300 ns decode
// (~0.3-0.5 us end to end) against 26 us for pebble's own block cache, 90 us
// for the OS page cache and 1.6 ms for the device, measured on the
// deployment's celldb.
//
// Its memory is pebble-style, not Go-style: each shard owns ONE region ring
// allocated through internal/manualmem — malloc'd outside the GC under cgo
// (the deployed binary builds CGO_ENABLED=1), Go-heap noscan bytes under
// cgo=0 — so multi-GiB capacity here costs the collector nothing to mark.
// That is why this cache is byte-denominated where the decoded cache is
// entry-denominated: its cost model is resident bytes, not live objects.
//
// Layout per shard:
//
//   - ARENA: a ring of fixed-size regions in one allocation. Writers bump-
//     pointer append self-describing entries [32B full hash][4B len][record].
//     When the tail region fills, the ring advances: the next (oldest) region
//     is recycled by bumping its GENERATION — one atomic store that instantly
//     invalidates every index entry pointing into it.
//   - INDEX: a flat open-addressed table of 16-byte slots, two words each:
//     [8B hash prefix][1b ref | 23b generation | 40b arena offset], linear
//     probing, never deleting. A slot whose generation no longer matches its
//     region's is a GHOST: lookups skip it, inserts reuse it. An insert that
//     cannot place within the probe budget is DECLINED and counted — this is
//     a cache, refusal is cheap and bounded probes are what keep lookups flat.
//
// Readers are LOCK-FREE via a region-generation seqlock: load the slot, copy
// the record out of the arena, re-check the region generation; a rotation in
// between turns the read into a miss. The full 32-byte hash stored in the
// arena makes 8-byte prefix collisions exact and doubles as a corruption
// guard; tonutils' validateLoadedLazyRef remains the outer safety net on every
// decoded cell.
//
// Recency is CLOCK, like the decoded cache: a hit sets the slot's ref bit only
// when it is clear, and on rotation the dying region's log is walked and
// entries whose slot is live AND ref-set are re-appended with the bit cleared
// — under a MANDATORY salvage budget of half a region, which is the live-lock
// guard: without it a working set of exactly one region's worth of hot
// entries could rotate forever without freeing a byte. Budget exhaustion is
// counted (salvage_truncated).
//
// Keyed by hash ALONE, no generation namespace: cells are content-addressed,
// so one hash is one byte string everywhere, the in-arena hash compare proves
// the mapping exact, and a record surviving a celldb generation swap is a
// correct answer, not a stale one.
type cellRecordCache struct {
	shards      []cellRecordShard
	regionBytes int

	inserts          atomic.Uint64
	declined         atomic.Uint64
	salvageTruncated atomic.Uint64

	closed atomic.Bool
}

type cellRecordCacheConfig struct {
	shards          int
	regionsPerShard int
	regionBytes     int
	// indexSlotsPerShard must be a power of two. Zero derives it from the
	// arena size; tests set it directly to force table pressure.
	indexSlotsPerShard int
}

type cellRecordShard struct {
	// mu serializes writers: insert, rotation and free. Readers never take it.
	mu sync.Mutex
	// readers gates free(): lookups register here so Close cannot release the
	// arena under a reader mid-copy. Uniformly distributed keys keep this
	// counter uncontended; it is the record-cache analogue of the sharded
	// metrics counters.
	readers atomic.Int64

	// arena and indexRaw come from manualmem; slots is an atomic.Uint64
	// overlay over indexRaw (8-byte aligned by the allocator's contract).
	arena    []byte
	indexRaw []byte
	slots    []atomic.Uint64
	slotMask uint64

	// regionGen is each region's current generation, always non-zero in its
	// low 23 bits so a packed slot word for a live entry can never be zero
	// (zero means "empty slot"). Bumped by rotation, read by the seqlock.
	regionGen  []atomic.Uint64
	regionFill []int
	// writeRegion is the ring position appends go to. Guarded by mu.
	writeRegion int

	// liveEntries/liveBytes track entries whose index slot still points at
	// them. Written under mu, read atomically by stats().
	liveEntries atomic.Int64
	liveBytes   atomic.Int64
}

const (
	// cellRecordCacheShards is fixed: the cache is sharded by the hash's first
	// byte, the same uniformly distributed key the lazy-load counters shard on.
	cellRecordCacheShards = 64

	recordCacheHeaderSize  = 36 // 32B full hash + 4B little-endian length
	recordCacheProbeBudget = 16

	recordCacheRefBit     = uint64(1) << 63
	recordCacheGenShift   = 40
	recordCacheGenMask    = uint64(1)<<23 - 1
	recordCacheOffsetMask = uint64(1)<<recordCacheGenShift - 1

	// recordCacheRegionTargetBytes is the region size at production scale; a
	// small total budget shrinks it so every shard still gets a ring of at
	// least two regions.
	recordCacheRegionTargetBytes = 8 << 20
	recordCacheRegionMinBytes    = 64 << 10

	// recordCacheIndexArenaBytesPerSlot sizes the index from the arena: one
	// 16-byte slot per this many arena bytes. Derived from the MEASURED record
	// distribution of a mainnet-fixture store (307,786 records: mean 104.5 B,
	// p50 114, p90 124, max 266), so a full arena of mean-sized entries
	// (104.5 + 36 header = 140.5 B each) fills the table to ~0.51 before the
	// power-of-two round-up — comfortably inside the 16-slot probe budget, at
	// an index cost of ~22% of the arena budget.
	recordCacheIndexArenaBytesPerSlot = 72
)

// cellRecordCacheConfigFromBytes turns the byte budget into geometry. The
// budget is ARENA bytes; the derived index adds ~22-25% on top (reported by
// open.go's log line). Budgets too small for 64 shards of two minimum regions
// are clamped up — the knob's off switch is zero, not "one page".
func cellRecordCacheConfigFromBytes(totalBytes int64) cellRecordCacheConfig {
	shards := cellRecordCacheShards
	minTotal := int64(shards * 2 * recordCacheRegionMinBytes)
	if totalBytes < minTotal {
		totalBytes = minTotal
	}

	perShard := totalBytes / int64(shards)
	regionBytes := int64(recordCacheRegionTargetBytes)
	if regionBytes > perShard/2 {
		regionBytes = perShard / 2
	}
	if regionBytes < recordCacheRegionMinBytes {
		regionBytes = recordCacheRegionMinBytes
	}
	regions := int(perShard / regionBytes)

	return cellRecordCacheConfig{
		shards:          shards,
		regionsPerShard: regions,
		regionBytes:     int(regionBytes),
	}
}

func newCellRecordCache(cfg cellRecordCacheConfig) *cellRecordCache {
	if cfg.shards <= 0 || cfg.regionsPerShard < 2 || cfg.regionBytes < 4096 {
		return nil
	}

	slotCount := cfg.indexSlotsPerShard
	if slotCount == 0 {
		target := cfg.regionsPerShard * cfg.regionBytes / recordCacheIndexArenaBytesPerSlot
		slotCount = 1024
		for slotCount < target {
			slotCount *= 2
		}
	}
	if slotCount&(slotCount-1) != 0 {
		panic(fmt.Sprintf("record cache index slots must be a power of two, got %d", slotCount))
	}

	cache := &cellRecordCache{
		shards:      make([]cellRecordShard, cfg.shards),
		regionBytes: cfg.regionBytes,
	}
	for i := range cache.shards {
		shard := &cache.shards[i]
		shard.arena = manualmem.Alloc(cfg.regionsPerShard * cfg.regionBytes)
		shard.indexRaw = manualmem.Alloc(slotCount * 16)
		shard.slots = unsafe.Slice((*atomic.Uint64)(unsafe.Pointer(unsafe.SliceData(shard.indexRaw))), slotCount*2)
		shard.slotMask = uint64(slotCount - 1)
		shard.regionGen = make([]atomic.Uint64, cfg.regionsPerShard)
		for r := range shard.regionGen {
			shard.regionGen[r].Store(1)
		}
		shard.regionFill = make([]int, cfg.regionsPerShard)
	}
	return cache
}

// free releases every arena back to the allocator. It waits out in-flight
// readers per shard: a lookup registers in readers before touching the arena
// and free() flips closed before waiting, so no reader can copy from a region
// after it is returned to malloc.
func (c *cellRecordCache) free() {
	if c == nil || !c.closed.CompareAndSwap(false, true) {
		return
	}

	for i := range c.shards {
		shard := &c.shards[i]
		shard.mu.Lock()
		for shard.readers.Load() != 0 {
			runtime.Gosched()
		}
		shard.slots = nil
		manualmem.Free(shard.indexRaw)
		shard.indexRaw = nil
		manualmem.Free(shard.arena)
		shard.arena = nil
		shard.mu.Unlock()
	}
}

// capacityBytes reports the total arena budget. A nil cache holds nothing.
func (c *cellRecordCache) capacityBytes() int64 {
	if c == nil {
		return 0
	}
	total := int64(0)
	for i := range c.shards {
		total += int64(len(c.shards[i].arena))
	}
	return total
}

// indexBytes reports the index overhead on top of capacityBytes.
func (c *cellRecordCache) indexBytes() int64 {
	if c == nil {
		return 0
	}
	total := int64(0)
	for i := range c.shards {
		total += int64(len(c.shards[i].indexRaw))
	}
	return total
}

type cellRecordCacheStats struct {
	Entries          int64
	Bytes            int64
	CapacityBytes    int64
	IndexBytes       int64
	Inserts          uint64
	Declined         uint64
	SalvageTruncated uint64
}

func (c *cellRecordCache) stats() cellRecordCacheStats {
	if c == nil {
		return cellRecordCacheStats{}
	}

	stats := cellRecordCacheStats{
		CapacityBytes:    c.capacityBytes(),
		IndexBytes:       c.indexBytes(),
		Inserts:          c.inserts.Load(),
		Declined:         c.declined.Load(),
		SalvageTruncated: c.salvageTruncated.Load(),
	}
	for i := range c.shards {
		stats.Entries += c.shards[i].liveEntries.Load()
		stats.Bytes += c.shards[i].liveBytes.Load()
	}
	return stats
}

func (c *cellRecordCache) shardFor(hash []byte) *cellRecordShard {
	return &c.shards[int(hash[0])%len(c.shards)]
}

func recordCacheProbeOrigin(hash []byte) (prefix uint64, origin uint64) {
	// The prefix word and the probe origin come from disjoint hash bytes, so a
	// probe chain never clusters by prefix.
	return binary.BigEndian.Uint64(hash[:8]), binary.BigEndian.Uint64(hash[8:16])
}

// get copies the cached record for hash into dst (reusing its capacity) and
// returns it, or returns nil on a miss. LOCK-FREE: slot words are atomic
// loads, the copy is guarded by the region-generation seqlock, and the only
// possible write is the CLOCK ref bit when it is clear.
func (c *cellRecordCache) get(hash []byte, dst []byte) []byte {
	if c == nil || len(hash) != 32 {
		return nil
	}

	shard := c.shardFor(hash)
	shard.readers.Add(1)
	defer shard.readers.Add(-1)
	if c.closed.Load() {
		return nil
	}

	prefix, origin := recordCacheProbeOrigin(hash)
	for i := uint64(0); i < recordCacheProbeBudget; i++ {
		slot := (origin + i) & shard.slotMask
		packed := shard.slots[2*slot+1].Load()
		if packed == 0 {
			// Never-delete linear probing: the first truly empty slot ends
			// every probe chain (ghosts stay non-zero).
			return nil
		}
		if shard.slots[2*slot].Load() != prefix {
			continue
		}

		gen := packed >> recordCacheGenShift & recordCacheGenMask
		offset := int(packed & recordCacheOffsetMask)
		region := offset / c.regionBytes
		if region >= len(shard.regionGen) || offset+recordCacheHeaderSize > (region+1)*c.regionBytes {
			continue
		}
		if shard.regionGen[region].Load()&recordCacheGenMask != gen {
			continue // ghost
		}

		entry := shard.arena[offset:]
		if !bytes.Equal(entry[:32], hash) {
			continue // 8-byte prefix collision, made exact by the full hash
		}
		length := int(binary.LittleEndian.Uint32(entry[32:recordCacheHeaderSize]))
		if offset+recordCacheHeaderSize+length > (region+1)*c.regionBytes {
			continue
		}
		dst = append(dst[:0], entry[recordCacheHeaderSize:recordCacheHeaderSize+length]...)

		// Seqlock re-check: a rotation between the slot load and the copy may
		// have overwritten the bytes we copied. Genuinely gone, so a miss.
		if shard.regionGen[region].Load()&recordCacheGenMask != gen {
			return nil
		}

		if packed&recordCacheRefBit == 0 {
			// Set-if-clear keeps the steady-state hit a pure read. A failed
			// CAS means a writer moved the slot; recency is best-effort.
			shard.slots[2*slot+1].CompareAndSwap(packed, packed|recordCacheRefBit)
		}
		return dst
	}
	return nil
}

// put files one record under its hash. Present-and-live hashes are skipped
// (content-addressed: same hash, same bytes); an insert that cannot place a
// slot within the probe budget, or a record that cannot fit a region even
// after rotation, is declined and counted.
func (c *cellRecordCache) put(hash []byte, record []byte) {
	if c == nil || len(hash) != 32 || len(record) == 0 {
		return
	}
	entrySize := recordCacheHeaderSize + len(record)
	if entrySize > c.regionBytes {
		c.declined.Add(1)
		return
	}

	shard := c.shardFor(hash)
	prefix, origin := recordCacheProbeOrigin(hash)

	shard.mu.Lock()
	defer shard.mu.Unlock()
	if c.closed.Load() {
		return
	}

	place := -1
	for i := uint64(0); i < recordCacheProbeBudget; i++ {
		slot := (origin + i) & shard.slotMask
		packed := shard.slots[2*slot+1].Load()
		if packed == 0 {
			if place < 0 {
				place = int(slot)
			}
			break // first empty ends the chain; nothing lives past it
		}

		gen := packed >> recordCacheGenShift & recordCacheGenMask
		offset := int(packed & recordCacheOffsetMask)
		region := offset / c.regionBytes
		live := region < len(shard.regionGen) && shard.regionGen[region].Load()&recordCacheGenMask == gen
		if !live {
			if place < 0 {
				place = int(slot) // ghost: reusable, but keep probing for a live copy
			}
			continue
		}
		if shard.slots[2*slot].Load() == prefix && bytes.Equal(shard.arena[offset:offset+32], hash) {
			return // already cached and live
		}
	}
	if place < 0 {
		c.declined.Add(1)
		return
	}

	if !shard.reserve(c, entrySize) {
		c.declined.Add(1)
		return
	}

	region := shard.writeRegion
	offset := region*c.regionBytes + shard.regionFill[region]
	entry := shard.arena[offset:]
	copy(entry[:32], hash)
	binary.LittleEndian.PutUint32(entry[32:recordCacheHeaderSize], uint32(len(record)))
	copy(entry[recordCacheHeaderSize:], record)
	shard.regionFill[region] += entrySize

	gen := shard.regionGen[region].Load() & recordCacheGenMask
	// Publish order: arena bytes are written above, the prefix word next, the
	// packed word LAST — a slot is not a slot until packed is non-zero, and
	// the in-arena hash compare makes any interleaving a clean miss rather
	// than a wrong answer.
	shard.slots[2*uint64(place)].Store(prefix)
	shard.slots[2*uint64(place)+1].Store(gen<<recordCacheGenShift | uint64(offset))

	shard.liveEntries.Add(1)
	shard.liveBytes.Add(int64(entrySize))
	c.inserts.Add(1)
}

// reserve makes room for entrySize in the write region, rotating the ring at
// most twice. Two rotations suffice for any entry no larger than half a
// region (salvage keeps at most half of a recycled region); anything larger
// is the caller's decline.
func (s *cellRecordShard) reserve(c *cellRecordCache, entrySize int) bool {
	for attempt := 0; attempt < 2; attempt++ {
		if s.regionFill[s.writeRegion]+entrySize <= c.regionBytes {
			return true
		}
		s.rotate(c)
	}
	return s.regionFill[s.writeRegion]+entrySize <= c.regionBytes
}

// rotate advances the ring: the oldest region's generation is bumped — one
// store that turns every index entry pointing there into a ghost and fails
// any in-flight reader's seqlock — and its self-describing log is walked to
// salvage CLOCK-hot entries (slot live AND ref bit set) by re-appending them
// at the region's start with the bit cleared. The salvage budget of half a
// region is MANDATORY: it is the live-lock guard that keeps a fully-hot ring
// making forward progress, and hitting it is counted, not hidden.
func (s *cellRecordShard) rotate(c *cellRecordCache) {
	next := (s.writeRegion + 1) % len(s.regionGen)
	genOld := s.regionGen[next].Load()
	genNew := genOld + 1
	if genNew&recordCacheGenMask == 0 {
		genNew++ // zero is "empty slot", never a generation
	}
	s.regionGen[next].Store(genNew)

	base := next * c.regionBytes
	fill := s.regionFill[next]
	budget := c.regionBytes / 2
	writePos, readPos := 0, 0
	truncated := false
	for readPos+recordCacheHeaderSize <= fill {
		entryHash := s.arena[base+readPos : base+readPos+32]
		length := int(binary.LittleEndian.Uint32(s.arena[base+readPos+32 : base+readPos+recordCacheHeaderSize]))
		entrySize := recordCacheHeaderSize + length
		if readPos+entrySize > fill {
			break // truncated tail cannot happen without a writer bug; stop rather than read past the log
		}

		slot, packed := s.findLiveSlot(c, entryHash, genOld&recordCacheGenMask, uint64(base+readPos))
		if slot >= 0 {
			hot := packed&recordCacheRefBit != 0
			if hot && !truncated && writePos+entrySize > budget {
				truncated = true
				c.salvageTruncated.Add(1)
			}
			if hot && !truncated {
				// Forward in-place copy: writePos <= readPos always, and
				// copy() handles the overlap.
				copy(s.arena[base+writePos:base+writePos+entrySize], s.arena[base+readPos:base+readPos+entrySize])
				s.slots[2*uint64(slot)+1].Store((genNew&recordCacheGenMask)<<recordCacheGenShift | uint64(base+writePos))
				writePos += entrySize
			} else {
				s.liveEntries.Add(-1)
				s.liveBytes.Add(-int64(entrySize))
			}
		}
		readPos += entrySize
	}
	s.regionFill[next] = writePos
	s.writeRegion = next
}

// findLiveSlot locates the index slot that points exactly at the entry at
// arena offset target with generation gen. A slot pointing anywhere else means
// this arena copy is dead (superseded or already ghosted) and must not be
// salvaged or double-discounted.
func (s *cellRecordShard) findLiveSlot(c *cellRecordCache, hash []byte, gen uint64, target uint64) (int, uint64) {
	prefix, origin := recordCacheProbeOrigin(hash)
	for i := uint64(0); i < recordCacheProbeBudget; i++ {
		slot := (origin + i) & s.slotMask
		packed := s.slots[2*slot+1].Load()
		if packed == 0 {
			return -1, 0
		}
		if s.slots[2*slot].Load() != prefix {
			continue
		}
		if packed>>recordCacheGenShift&recordCacheGenMask == gen && packed&recordCacheOffsetMask == target {
			return int(slot), packed
		}
	}
	return -1, 0
}
