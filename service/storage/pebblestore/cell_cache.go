package pebblestore

import (
	"container/list"
	"encoding/binary"
	"sync"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	DefaultDecodedCellCacheShards        = 64
	DefaultDecodedCellCacheBytesPerEntry = int64(16 << 10)
	DefaultDecodedCellCacheMinEntries    = 64 << 10
	DefaultDecodedCellCacheMaxEntries    = 1 << 20
)

type decodedCellCache struct {
	shards []decodedCellCacheShard
}

type decodedCellCacheConfig struct {
	enabled       bool
	shards        int
	cacheBytes    int64
	bytesPerEntry int64
	minEntries    int
	maxEntries    int
}

type decodedCellCacheShard struct {
	mu       sync.Mutex
	capacity int
	items    map[decodedCellCacheKey]*list.Element
	order    *list.List
}

type decodedCellCacheEntry struct {
	key  decodedCellCacheKey
	cell *cell.Cell
}

type decodedCellCacheKey struct {
	generation uint64
	hash       [32]byte
}

func newDecodedCellCache(cfg decodedCellCacheConfig) *decodedCellCache {
	if !cfg.enabled {
		return nil
	}

	entries64 := cfg.cacheBytes / cfg.bytesPerEntry
	if entries64 > int64(cfg.maxEntries) {
		entries64 = int64(cfg.maxEntries)
	}
	entries := int(entries64)
	if entries < cfg.minEntries {
		entries = cfg.minEntries
	}
	shards := cfg.shards
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
	for i := range cache.shards {
		cache.shards[i] = decodedCellCacheShard{
			capacity: perShard,
			items:    make(map[decodedCellCacheKey]*list.Element, perShard),
			order:    list.New(),
		}
	}
	return cache
}

func (c *decodedCellCache) get(generation uint64, hash []byte) (*cell.Cell, error) {
	if c == nil {
		return nil, storage.ErrNotFound
	}

	key := newDecodedCellCacheKey(generation, hash)
	shard := &c.shards[c.shardIndex(key.hash)]
	shard.mu.Lock()
	defer shard.mu.Unlock()

	elem := shard.items[key]
	if elem == nil {
		return nil, storage.ErrNotFound
	}
	shard.order.MoveToFront(elem)
	entry := elem.Value.(decodedCellCacheEntry)
	return entry.cell, nil
}

func (c *decodedCellCache) set(generation uint64, hash []byte, loaded *cell.Cell) {
	if c == nil {
		return
	}

	key := newDecodedCellCacheKey(generation, hash)
	shard := &c.shards[c.shardIndex(key.hash)]
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

func (c *decodedCellCache) shardIndex(hash [32]byte) int {
	return int(binary.BigEndian.Uint32(hash[:4]) % uint32(len(c.shards)))
}

func newDecodedCellCacheKey(generation uint64, hash []byte) decodedCellCacheKey {
	var key decodedCellCacheKey
	key.generation = generation
	copy(key.hash[:], hash)
	return key
}
