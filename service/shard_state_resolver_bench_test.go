package service

import (
	"encoding/binary"
	"testing"

	"github.com/xssnick/gton/service/storage"
)

func BenchmarkShardStateResolverCacheEviction(b *testing.B) {
	resolver := &shardStateResolver{
		cache: make(map[storage.BlockRootHash]*storage.BlockState, shardStateResolverCacheLimit+1),
	}
	for i := uint64(0); i < shardStateResolverCacheLimit; i++ {
		resolver.cache[benchmarkShardStateCacheKey(i)] = &storage.BlockState{
			Block: testBlockID(0, topShard, uint32(i)),
		}
	}

	next := uint64(shardStateResolverCacheLimit)
	b.ReportAllocs()
	for b.Loop() {
		resolver.cache[benchmarkShardStateCacheKey(next)] = &storage.BlockState{
			Block: testBlockID(0, topShard, uint32(next)),
		}
		resolver.evictCacheLocked()
		next++
	}
}

func benchmarkShardStateCacheKey(value uint64) storage.BlockRootHash {
	var key storage.BlockRootHash
	binary.LittleEndian.PutUint64(key[:], value)
	return key
}
