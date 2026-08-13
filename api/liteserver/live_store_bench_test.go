package liteserver

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/xssnick/gton/service/liveview"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func BenchmarkLiveStoreOverlay(b *testing.B) {
	for _, shards := range []int{16, 128, 512} {
		b.Run(fmt.Sprintf("current-state/shards-%d", shards), func(b *testing.B) {
			live, _, _ := benchmarkLiveStoreWithCurrent(b, shards)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				current, err := live.CurrentState(context.Background())
				if err != nil {
					b.Fatalf("load current state: %v", err)
				}
				if len(current.Shards) != shards {
					b.Fatalf("current shards = %d, want %d", len(current.Shards), shards)
				}
			}
		})

		b.Run(fmt.Sprintf("masterchain-info/shards-%d", shards), func(b *testing.B) {
			live, current, _ := benchmarkLiveStoreWithCurrent(b, shards)
			srv := testServer(live)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				info, _, err := srv.masterchainInfoWithUTime(context.Background())
				if err != nil {
					b.Fatalf("load masterchain info: %v", err)
				}
				if info.Last == nil || !info.Last.Equals(&current.Masterchain.Block) {
					b.Fatalf("masterchain block mismatch")
				}
			}
		})

		b.Run(fmt.Sprintf("block-state-current-shard/shards-%d", shards), func(b *testing.B) {
			live, _, target := benchmarkLiveStoreWithCurrent(b, shards)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				state, err := live.BlockState(context.Background(), target)
				if err != nil {
					b.Fatalf("load block state: %v", err)
				}
				if !state.Block.Equals(&target) {
					b.Fatalf("loaded block mismatch")
				}
			}
		})

		b.Run(fmt.Sprintf("current-account-blocks/shards-%d", shards), func(b *testing.B) {
			live, current, _ := benchmarkLiveStoreWithCurrent(b, shards)
			account := bytes.Repeat([]byte{0x44}, 32)
			candidates, err := storage.AccountShardCandidates(0, account)
			if err != nil {
				b.Fatal(err)
			}
			targetShard := candidates[len(candidates)-1]
			targetState, targetRoot := benchmarkBlockState(b, 0, targetShard, 3000)
			current.Shards[storage.ShardKey{Workchain: 0, Shard: targetShard}] = targetState
			if err := live.publishLiveBlockData(targetState.Block, targetRoot, testBlockBOC(targetRoot), true); err != nil {
				b.Fatalf("set live account shard block: %v", err)
			}
			live.SetLiveCurrentState(current)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				blocks, err := live.CurrentAccountBlocks(context.Background(), 0, account)
				if err != nil {
					b.Fatalf("load current account blocks: %v", err)
				}
				if !blocks.Account.Equals(&targetState.Block) {
					b.Fatalf("account block mismatch")
				}
			}
		})
	}

	b.Run("lookup-lt/live-blocks-4096", func(b *testing.B) {
		live := benchmarkLiveStoreWithIndexes(4096)
		key := storage.BlockHistoryKey{Workchain: 0, Shard: 1 << 3}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := live.LookupBlockByLT(context.Background(), key, 50); err != nil {
				b.Fatalf("lookup block by lt: %v", err)
			}
		}
	})

	b.Run("lookup-seqno/live-blocks-4096", func(b *testing.B) {
		live := benchmarkLiveStoreWithIndexes(4096)
		key := storage.BlockHistoryKey{Workchain: 0, Shard: 1 << 3}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := live.LookupBlockBySeqNo(context.Background(), storage.BlockSeqRef{Workchain: key.Workchain, Shard: key.Shard, SeqNo: 1}); err != nil {
				b.Fatalf("lookup block by seqno: %v", err)
			}
		}
	})
}

func benchmarkLiveStoreWithIndexes(blocks int) *LiveStore {
	live := NewLiveStore(&fakeStore{}, liveview.Options{MasterBlockCache: blocks, ShardBlockCache: blocks})

	for i := 0; i < blocks; i++ {
		// Root hashes must be unique per block: the live store keys blocks by
		// root hash, so colliding hashes overwrite earlier blocks and drop
		// their history index entries.
		rootHash := bytes.Repeat([]byte{0x01}, 32)
		fileHash := bytes.Repeat([]byte{0x02}, 32)
		binary.BigEndian.PutUint32(rootHash, uint32(i+1))
		binary.BigEndian.PutUint32(fileHash, uint32(i+1))
		block := ton.BlockIDExt{
			Workchain: 0,
			Shard:     int64(i+1) << 3,
			SeqNo:     uint32(i + 1),
			RootHash:  rootHash,
			FileHash:  fileHash,
		}
		meta := &storage.BlockMeta{
			ID:       block,
			StartLT:  uint64(i*100 + 1),
			EndLT:    uint64(i*100 + 100),
			GenUTime: uint32(1000 + i),
		}
		if err := live.PublishLiveBlockArtifacts(storage.LiveBlockArtifacts{
			Block:           block,
			Meta:            meta,
			ArtifactFlushed: true,
		}); err != nil {
			panic(err)
		}
	}
	return live
}

func benchmarkLiveStoreWithCurrent(tb testing.TB, shards int) (*LiveStore, *storage.CurrentState, ton.BlockIDExt) {
	tb.Helper()

	live := NewLiveStore(&fakeStore{}, liveview.Options{MasterBlockCache: 8, ShardBlockCache: shards + 8})
	masterState, masterRoot := benchmarkBlockState(tb, masterchainID, masterchainShard, 1000)
	current := &storage.CurrentState{
		Masterchain: masterState,
		Shards:      make(map[storage.ShardKey]storage.BlockState, shards),
	}
	if err := live.publishLiveBlockData(current.Masterchain.Block, masterRoot, testBlockBOC(masterRoot), true); err != nil {
		tb.Fatalf("set live master block: %v", err)
	}

	var target ton.BlockIDExt
	for i := 0; i < shards; i++ {
		shard := int64(i+1) << 3
		state, root := benchmarkBlockState(tb, 0, shard, uint32(2000+i))
		current.Shards[storage.ShardKey{Workchain: 0, Shard: shard}] = state
		target = state.Block
		if err := live.publishLiveBlockData(state.Block, root, testBlockBOC(root), true); err != nil {
			tb.Fatalf("set live shard block: %v", err)
		}
	}
	live.SetLiveCurrentState(current)
	return live, current, target
}

func benchmarkBlockState(tb testing.TB, workchain int32, shard int64, seqno uint32) (storage.BlockState, *cell.Cell) {
	tb.Helper()

	stateRoot := cell.BeginCell().MustStoreUInt(uint64(seqno), 32).EndCell()
	block, root := benchmarkBlockWithState(tb, workchain, shard, seqno, stateRoot)
	return storage.BlockState{
		Block:         block,
		StateRootHash: stateRoot.Hash(0),
		Cell:          stateRoot,
		Parsed:        &tlb.ShardStateUnsplit{GenUTime: seqno},
	}, root
}

func benchmarkBlockWithState(tb testing.TB, workchain int32, shard int64, seqno uint32, stateRoot *cell.Cell) (ton.BlockIDExt, *cell.Cell) {
	tb.Helper()

	var header tlb.BlockHeader
	header.Version = 1
	header.Shard = tlb.ShardIdent{PrefixBits: 0, WorkchainID: workchain, ShardPrefix: uint64(shard)}
	header.SeqNo = seqno
	header.StartLt = 1
	header.EndLt = 100
	header.GenUtime = 1000 + seqno
	header.MinRefMcSeqno = seqno
	header.PrevRef = tlb.BlkPrevInfo{Prev1: tlb.ExtBlkRef{
		EndLt:    1,
		SeqNo:    seqno - 1,
		RootHash: bytes.Repeat([]byte{0x03}, 32),
		FileHash: bytes.Repeat([]byte{0x04}, 32),
	}}
	if workchain != masterchainID {
		header.NotMaster = true
		header.MasterRef = &tlb.ExtBlkRef{
			EndLt:    1,
			SeqNo:    seqno,
			RootHash: bytes.Repeat([]byte{0x05}, 32),
			FileHash: bytes.Repeat([]byte{0x06}, 32),
		}
	}

	root, err := tlb.ToCell(&tlb.Block{
		GlobalID:    -239,
		BlockInfo:   header,
		ValueFlow:   cell.BeginCell().EndCell(),
		StateUpdate: benchmarkMerkleUpdateCell(tb, cell.BeginCell().EndCell(), stateRoot),
		Extra: &tlb.BlockExtra{
			InMsgDesc:          cell.BeginCell().EndCell(),
			OutMsgDesc:         cell.BeginCell().EndCell(),
			ShardAccountBlocks: cell.BeginCell().EndCell(),
			RandSeed:           bytes.Repeat([]byte{0x01}, 32),
			CreatedBy:          bytes.Repeat([]byte{0x02}, 32),
		},
	})
	if err != nil {
		tb.Fatalf("build benchmark block: %v", err)
	}
	return testBlockIDForData(workchain, shard, seqno, root, testBlockBOC(root)), root
}

func benchmarkMerkleUpdateCell(tb testing.TB, oldRoot *cell.Cell, newRoot *cell.Cell) *cell.Cell {
	tb.Helper()

	update, err := cell.BeginCell().
		MustStoreUInt(uint64(cell.MerkleUpdateCellType), 8).
		MustStoreSlice(oldRoot.Hash(0), 256).
		MustStoreSlice(newRoot.Hash(0), 256).
		MustStoreUInt(uint64(oldRoot.Depth(0)), 16).
		MustStoreUInt(uint64(newRoot.Depth(0)), 16).
		MustStoreRef(oldRoot).
		MustStoreRef(newRoot).
		EndCellSpecial(true)
	if err != nil {
		tb.Fatalf("build benchmark merkle update: %v", err)
	}
	return update
}
