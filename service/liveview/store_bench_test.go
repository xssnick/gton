package liveview

import (
	"context"
	"testing"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

func BenchmarkStoreCachedHistoryLookup(b *testing.B) {
	const historyBlocks = 10_000

	live, _, _, _, key := benchmarkStoreIndexedHistory(b, historyBlocks)
	ctx := context.Background()

	b.Run("lt", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := live.LookupBlockByLT(ctx, storage.BlockHistoryKey{Workchain: key.workchain, Shard: key.shard}, 15_000); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("unix-time", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := live.LookupBlockByUnixTime(ctx, storage.BlockHistoryKey{Workchain: key.workchain, Shard: key.shard}, 7_500); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("lt-prefix", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := live.LookupBlockByLTForPrefix(ctx, storage.BlockHistoryKey{Workchain: key.workchain, Shard: key.shard}, 15_000); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("unix-time-prefix", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := live.LookupBlockByUnixTimeForPrefix(ctx, storage.BlockHistoryKey{Workchain: key.workchain, Shard: key.shard}, 7_500); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkStorePublishBlockWithStateIndexedHistory(b *testing.B) {
	const historyBlocks = 10_000

	benchmarks := []struct {
		name    string
		publish func(*Store, storage.BlockRootHash, *liveBlock, *storage.BlockState)
	}{
		{
			name: "legacy-two-refreshes",
			publish: func(live *Store, key storage.BlockRootHash, block *liveBlock, state *storage.BlockState) {
				live.putBlockLocked(key, block, nil)
				live.rememberBlockStateLocked(*state)
			},
		},
		{
			name: "combined-one-refresh",
			publish: func(live *Store, key storage.BlockRootHash, block *liveBlock, state *storage.BlockState) {
				live.putBlockLocked(key, block, state)
			},
		},
	}

	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			live, block, state, meta, historyKey := benchmarkStoreIndexedHistory(b, historyBlocks)
			blockKey := storage.BlockKey(block)
			stateKey, ok := liveBlockLookupKeyFromBlock(block)
			if !ok {
				b.Fatal("benchmark block has an incomplete id")
			}
			seqKey := liveSeqKey{workchain: block.Workchain, shard: block.Shard, seqno: block.SeqNo}

			b.ReportAllocs()
			for b.Loop() {
				benchmark.publish(live, blockKey, &liveBlock{id: block, meta: meta}, state)

				if got := len(live.ltIndex[historyKey]); got != historyBlocks+1 {
					b.Fatalf("LT index length = %d, want %d", got, historyBlocks+1)
				}
				if got := len(live.unixIndex[historyKey]); got != historyBlocks+1 {
					b.Fatalf("unix index length = %d, want %d", got, historyBlocks+1)
				}
				live.ltIndex[historyKey] = live.ltIndex[historyKey][:historyBlocks]
				live.unixIndex[historyKey] = live.unixIndex[historyKey][:historyBlocks]
				delete(live.blocks, blockKey)
				delete(live.states, stateKey)
				delete(live.metas, blockKey)
				delete(live.seqIndex, seqKey)
				live.removeBlockOrderLocked(blockKey, liveBlockKind(block))
			}
		})
	}
}

func benchmarkStoreIndexedHistory(b *testing.B, historyBlocks uint32) (*Store, ton.BlockIDExt, *storage.BlockState, *storage.BlockMeta, liveHistoryKey) {
	b.Helper()

	live := New(noopBacking{}, Options{
		MasterBlockCache: -1,
		ShardBlockCache:  -1,
	})
	shard := int64(1) << 62
	historyKey := liveHistoryKey{workchain: 0, shard: shard}
	for seqno := uint32(1); seqno <= historyBlocks; seqno++ {
		block := testLiveBlockID(0, shard, seqno, byte(seqno))
		live.addMetaHistoryIndexLocked(&storage.BlockMeta{
			ID:       block,
			GenUTime: seqno,
			StartLT:  uint64(seqno)*2 - 1,
			EndLT:    uint64(seqno) * 2,
		})
	}

	block := testLiveBlockID(0, shard, historyBlocks+1, 0xfe)
	state := &storage.BlockState{Block: block}
	meta := &storage.BlockMeta{
		ID:       block,
		GenUTime: historyBlocks + 1,
		StartLT:  uint64(historyBlocks)*2 + 1,
		EndLT:    uint64(historyBlocks)*2 + 2,
	}
	return live, block, state, meta, historyKey
}
