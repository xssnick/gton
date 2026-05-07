package pebblestore

import (
	"container/list"
	"sync"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	decodedCellCacheShards     = 64
	decodedCellCacheBytesPerEl = 16 << 10
	decodedCellCacheMinEntries = 64 << 10
	decodedCellCacheMaxEntries = 1 << 20
)

type decodedCellCache struct {
	shards [decodedCellCacheShards]decodedCellCacheShard
}

type decodedCellCacheShard struct {
	mu       sync.Mutex
	capacity int
	items    map[[32]byte]*list.Element
	order    *list.List
}

type decodedCellCacheEntry struct {
	key  [32]byte
	cell *cell.Cell
}

func newDecodedCellCache(cellCacheBytes int64) *decodedCellCache {
	entries := int(cellCacheBytes / decodedCellCacheBytesPerEl)
	if entries < decodedCellCacheMinEntries {
		entries = decodedCellCacheMinEntries
	}
	if entries > decodedCellCacheMaxEntries {
		entries = decodedCellCacheMaxEntries
	}

	cache := &decodedCellCache{}
	perShard := entries / decodedCellCacheShards
	if perShard < 1 {
		perShard = 1
	}
	for i := range cache.shards {
		cache.shards[i] = decodedCellCacheShard{
			capacity: perShard,
			items:    make(map[[32]byte]*list.Element, perShard),
			order:    list.New(),
		}
	}
	return cache
}

func (c *decodedCellCache) get(hash []byte) (*cell.Cell, bool) {
	if c == nil || len(hash) != 32 {
		return nil, false
	}

	key := decodedCellCacheKey(hash)
	shard := &c.shards[key[0]&byte(decodedCellCacheShards-1)]
	shard.mu.Lock()
	defer shard.mu.Unlock()

	elem := shard.items[key]
	if elem == nil {
		return nil, false
	}
	shard.order.MoveToFront(elem)
	entry := elem.Value.(decodedCellCacheEntry)
	return entry.cell, true
}

func (c *decodedCellCache) set(hash []byte, loaded *cell.Cell) {
	if c == nil || loaded == nil || len(hash) != 32 {
		return
	}

	key := decodedCellCacheKey(hash)
	shard := &c.shards[key[0]&byte(decodedCellCacheShards-1)]
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if elem := shard.items[key]; elem != nil {
		elem.Value = decodedCellCacheEntry{key: key, cell: loaded}
		shard.order.MoveToFront(elem)
		return
	}

	elem := shard.order.PushFront(decodedCellCacheEntry{key: key, cell: loaded})
	shard.items[key] = elem
	for shard.order.Len() > shard.capacity {
		evicted := shard.order.Back()
		if evicted == nil {
			return
		}
		shard.order.Remove(evicted)
		entry := evicted.Value.(decodedCellCacheEntry)
		delete(shard.items, entry.key)
	}
}

func decodedCellCacheKey(hash []byte) [32]byte {
	var key [32]byte
	copy(key[:], hash)
	return key
}
