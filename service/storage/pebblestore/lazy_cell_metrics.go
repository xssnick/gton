package pebblestore

import (
	"sync/atomic"
	"time"

	"github.com/xssnick/gton/service/storage"
)

// Where the store-read layers are cut apart.
//
// Neither pebble nor the kernel reports which of them served a particular Get,
// so the split is inferred from the read's own duration. The cut points are the
// geometric midpoints between tiers measured on the deployment's disk (a 760 KB
// payload on /dev/md2 under a load average of 8-10, the same box the node runs
// on): pebble block cache 26.3 us, OS page cache 90.3 us, device 1.585 ms.
// sqrt(26.3*90.3) is 48.7 us and sqrt(90.3*1585) is 378 us, rounded out to 50
// and 400. A midpoint is chosen rather than a tier boundary so that ordinary
// jitter around either tier stays inside its own band.
//
// These are variables rather than constants so a deployment on different
// hardware can re-cut them once it has measured its own tiers; nothing derives
// them automatically, because a self-tuning threshold would silently reclassify
// history and make two days of graph incomparable.
var (
	lazyCellBlockCacheThreshold = 50 * time.Microsecond
	lazyCellDiskThreshold       = 400 * time.Microsecond
)

// lazyCellLoadCounters counts lazy cell loads by the layer that answered them.
// Every load increments exactly one counter, so the layers sum to the total.
//
// Sharded, because the decoded-cache counter sits on the hot path: a cache hit
// is otherwise sub-microsecond, and one shared atomic under real lightserver
// concurrency is not cheap. Measured on a 10-core machine with a RANDOM key per
// operation, which is what the hash gives us:
//
//	workers                1      4     10     20
//	one shared counter   7.1   34.6   31.8   51.5 ns/op
//	64 shards (this)     3.2   10.5   11.7   13.0 ns/op
//
// So about 2.7x at ten workers, not the order of magnitude a per-worker key
// would suggest. Two things follow, and both are the reason this stops at 64
// shards rather than growing them:
//
// Raising the shard count barely helps — 256 shards measured 10.4 ns at ten
// workers against 64's 11.7. A random key cannot produce core locality at any
// shard count, so the line migrates between cores regardless and only the
// collision probability moves.
//
// What WOULD help is mixing the core into the key: the same benchmark with a
// per-worker component reached 2.4-3.3 ns. Go exposes no cheap per-P index, so
// that needs a sync.Pool holding pointers into a fixed shard array (pool
// clearing then costs a reassignment, not a count). Deliberately not done: it
// buys about 9 ns on a path whose cache hit already costs on the order of a
// hundred, and at the lightserver's ~60k loads/s the whole counter is under a
// millisecond of CPU per second.
//
// Padding alone does not help either (34.9 ns at four workers): the contention
// is over one counter, not between adjacent fields, so it is not false sharing.
// The per-shard padding below is for a different reason — see there.
//
// The shard key is the cell hash's first byte, which every call site already
// holds and which is uniformly distributed by construction.
const lazyCellLoadShards = 64

type lazyCellLoadShard struct {
	decodedCache atomic.Uint64
	recordCache  atomic.Uint64
	blockCache   atomic.Uint64
	pageCache    atomic.Uint64
	disk         atomic.Uint64
	// Pads the shard out to a 64-byte cache line so neighbouring shards, which
	// different cores do touch concurrently, cannot share one.
	_ [24]byte
}

type lazyCellLoadCounters struct {
	shards [lazyCellLoadShards]lazyCellLoadShard
}

func (c *lazyCellLoadCounters) shard(key byte) *lazyCellLoadShard {
	return &c.shards[key&(lazyCellLoadShards-1)]
}

func (c *lazyCellLoadCounters) observeDecodedCache(key byte) {
	c.shard(key).decodedCache.Add(1)
}

func (c *lazyCellLoadCounters) observeRecordCache(key byte) {
	c.shard(key).recordCache.Add(1)
}

// observeStoreRead records one load that fell through to the cell store, banded
// by how long the read took. The caller times only the store read itself, not
// the decode that follows it, because the decode is the same work at every
// layer and folding it in would blur the bands it is meant to separate.
//
// The timer costs about 57 ns for the pair, against a store read that costs
// 26 us at its cheapest — 0.2 per cent, and less on every slower layer. It is
// deliberately absent from the decoded-cache path, which has no such floor to
// hide it in.
func (c *lazyCellLoadCounters) observeStoreRead(key byte, elapsed time.Duration) {
	shard := c.shard(key)
	switch {
	case elapsed < lazyCellBlockCacheThreshold:
		shard.blockCache.Add(1)
	case elapsed < lazyCellDiskThreshold:
		shard.pageCache.Add(1)
	default:
		shard.disk.Add(1)
	}
}

func (c *lazyCellLoadCounters) snapshot() []storage.LazyCellLoadMetric {
	var decodedCache, recordCache, blockCache, pageCache, disk uint64
	for i := range c.shards {
		shard := &c.shards[i]
		decodedCache += shard.decodedCache.Load()
		recordCache += shard.recordCache.Load()
		blockCache += shard.blockCache.Load()
		pageCache += shard.pageCache.Load()
		disk += shard.disk.Load()
	}

	return []storage.LazyCellLoadMetric{
		{Layer: storage.LazyCellLoadLayerDecodedCache, Count: decodedCache},
		{Layer: storage.LazyCellLoadLayerRecordCache, Count: recordCache},
		{Layer: storage.LazyCellLoadLayerBlockCache, Count: blockCache},
		{Layer: storage.LazyCellLoadLayerPageCache, Count: pageCache},
		{Layer: storage.LazyCellLoadLayerDisk, Count: disk},
	}
}
