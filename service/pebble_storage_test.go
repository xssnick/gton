package service

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/xssnick/gton/service/archive/packfile"
	sharddomain "github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/storage/pebblestore"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func mustStorageShardPrefixCandidates(tb testing.TB, prefix uint64) []int64 {
	tb.Helper()

	candidates, err := storage.ShardPrefixCandidates(0, prefix)
	if err != nil {
		tb.Fatal(err)
	}
	return candidates
}

func mustStorageAccountShardCandidates(tb testing.TB, workchain int32, account []byte) []int64 {
	tb.Helper()

	candidates, err := storage.AccountShardCandidates(workchain, account)
	if err != nil {
		tb.Fatal(err)
	}
	return candidates
}

func TestPebbleStoragePeerServingRoundTrip(t *testing.T) {
	store := openTestPebbleStorage(t)
	ctx := context.Background()

	prev := testPebbleBlockID(-1, topShard, 1)
	block := testPebbleBlockID(-1, topShard, 2)

	blockData := []byte{0xAA, 0xBB, 0xCC}
	proofData := []byte{0x10, 0x20, 0x30}
	zeroStateData := []byte{0x01, 0x00, 0x02}

	if _, err := store.SaveStateCheckpointEntries(ctx, []storage.StateCheckpointBlock{{
		Artifact: &storage.ServedBlockFull{
			ID:    prev,
			Block: []byte{0x70},
			Proof: []byte{0x71},
			Meta:  &storage.BlockMeta{ID: prev, GenUTime: prev.SeqNo},
		},
		Links: []storage.ServedBlockLink{{Prev: prev, Next: block}},
	}, {
		Artifact: &storage.ServedBlockFull{
			ID:    block,
			Block: blockData,
			Proof: proofData,
			Meta:  &storage.BlockMeta{ID: block, GenUTime: block.SeqNo},
		},
	}}, storage.StateCellRecords{}, nil); err != nil {
		t.Fatalf("save block full: %v", err)
	}
	if err := store.SaveZeroState(prev, zeroStateData, nil); err != nil {
		t.Fatalf("save zero state: %v", err)
	}

	archiveSeqno := int32(101)
	archiveBlock := testPebbleBlockID(-1, topShard, uint32(archiveSeqno))
	archiveData := []byte{0x55, 0x44}
	if _, err := store.SaveStateCheckpointEntries(ctx, []storage.StateCheckpointBlock{{
		Artifact: &storage.ServedBlockFull{
			ID:    archiveBlock,
			Block: archiveData,
			Meta:  &storage.BlockMeta{ID: archiveBlock, GenUTime: 1010, StartLT: 101, EndLT: 102},
		},
	}}, storage.StateCellRecords{}, nil); err != nil {
		t.Fatalf("save archive import: %v", err)
	}

	got, err := store.BlockFull(ctx, block)
	if err != nil {
		t.Fatalf("block full: %v", err)
	}
	if !bytes.Equal(got.Block, blockData) || !bytes.Equal(got.Proof, proofData) || got.IsLink {
		t.Fatalf("unexpected block full: %#v", got)
	}

	next, err := store.NextBlockFull(ctx, prev)
	if err != nil {
		t.Fatalf("next block full: %v", err)
	}
	if !next.ID.Equals(&block) {
		t.Fatalf("unexpected next block: got=%v want=%v", next.ID, block)
	}

	zeroState, err := store.ZeroState(ctx, prev)
	if err != nil {
		t.Fatalf("zero state: %v", err)
	}
	if !bytes.Equal(zeroState, zeroStateData) {
		t.Fatalf("unexpected zero state: %x", zeroState)
	}

	archiveID, err := store.ArchiveInfo(ctx, archiveSeqno, -1, topShard)
	if err != nil {
		t.Fatalf("archive info mismatch: err=%v id=%d", err, archiveID)
	}

	archiveHeader, err := store.ArchiveSlice(ctx, archiveID, 0, packfile.HeaderSize)
	var wantMagic [packfile.HeaderSize]byte
	binary.LittleEndian.PutUint32(wantMagic[:], packfile.PackageMagic)
	if err != nil || !bytes.Equal(archiveHeader, wantMagic[:]) {
		t.Fatalf("archive header mismatch: err=%v data=%x", err, archiveHeader)
	}

	archiveOffset := int64(packfile.HeaderSize + packfile.EntryHeaderSize + len(packfile.EntryName(packfile.KindBlock, archiveBlock)))
	archive, err := store.ArchiveSlice(ctx, archiveID, archiveOffset, int32(len(archiveData)))
	if err != nil || !bytes.Equal(archive, archiveData) {
		t.Fatalf("archive slice mismatch: err=%v data=%x", err, archive)
	}
}

func TestPebbleStorageKeepsMetadataOnlyBlocksOutOfHistoryIndexes(t *testing.T) {
	store := openTestPebbleStorage(t)
	ctx := context.Background()

	blockA := testPebbleBlockID(0, topShard, 10)
	blockB := testPebbleBlockID(0, topShard, 11)

	if err := store.SaveBlockMeta(&storage.BlockMeta{
		ID:       blockA,
		GenUTime: 100,
		StartLT:  1000,
		EndLT:    1999,
	}); err != nil {
		t.Fatalf("save block meta A: %v", err)
	}
	if err := store.SaveBlockMeta(&storage.BlockMeta{
		ID:       blockB,
		GenUTime: 120,
		StartLT:  2000,
		EndLT:    2999,
	}); err != nil {
		t.Fatalf("save block meta B: %v", err)
	}

	meta, err := store.BlockMeta(ctx, blockB)
	if err != nil {
		t.Fatalf("load metadata-only block meta: %v", err)
	}
	if meta.GenUTime != 120 || meta.EndLT != 2999 {
		t.Fatalf("metadata-only block meta = %+v", meta)
	}
	if _, err := store.LookupBlockBySeqNo(ctx, storage.BlockSeqRef{Workchain: 0, Shard: topShard, SeqNo: 11}); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("metadata-only lookup by seqno error = %v, want ErrNotFound", err)
	}
	if _, err := store.LookupBlockByLT(ctx, storage.BlockHistoryKey{Workchain: 0, Shard: topShard}, 500); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("metadata-only lookup by early lt error = %v, want ErrNotFound", err)
	}
	if _, err := store.LookupBlockByLT(ctx, storage.BlockHistoryKey{Workchain: 0, Shard: topShard}, 2500); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("metadata-only lookup by lt error = %v, want ErrNotFound", err)
	}
	if _, err := store.LookupBlockByLT(ctx, storage.BlockHistoryKey{Workchain: 0, Shard: topShard}, 3000); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("lookup future lt error = %v, want ErrNotFound", err)
	}
	if _, err := store.LookupBlockByUnixTime(ctx, storage.BlockHistoryKey{Workchain: 0, Shard: topShard}, 90); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("metadata-only lookup by early utime error = %v, want ErrNotFound", err)
	}
	if _, err := store.LookupBlockByUnixTime(ctx, storage.BlockHistoryKey{Workchain: 0, Shard: topShard}, 110); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("metadata-only lookup by utime error = %v, want ErrNotFound", err)
	}
	if _, err := store.LookupBlockByUnixTime(ctx, storage.BlockHistoryKey{Workchain: 0, Shard: topShard}, 121); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("lookup future utime error = %v, want ErrNotFound", err)
	}

	leaf := cell.BeginCell().MustStoreUInt(0xAA, 8).EndCell()
	root := cell.BeginCell().MustStoreUInt(0xBB, 8).MustStoreRef(leaf).MustStoreRef(leaf).EndCell()
	records, err := collectCellRecordsForTest(root)
	if err != nil {
		t.Fatalf("collect cell records: %v", err)
	}
	if err = saveActiveTestCells(store, records); err != nil {
		t.Fatalf("save cells: %v", err)
	}
	rootHash := root.HashKey()
	loaded, err := store.LoadCell(ctx, rootHash[:])
	if err != nil {
		t.Fatalf("load cell: %v", err)
	}
	requireLazyCellBOC(t, loaded, root)

	masterState := storage.BlockState{Block: testPebbleBlockID(-1, topShard, 100), Cell: cell.BeginCell().EndCell()}
	shardState := storage.BlockState{Block: blockB, Cell: cell.BeginCell().EndCell()}
	current := &storage.CurrentState{
		SyncedAt:    time.Now().Round(0),
		Masterchain: masterState,
		Shards: map[storage.ShardKey]storage.BlockState{
			{Workchain: 0, Shard: topShard}: shardState,
		},
	}
	if err = saveTestStateCheckpoint(ctx, store, []*storage.BlockState{&masterState, &shardState}, current); err != nil {
		t.Fatalf("save state checkpoint: %v", err)
	}
	gotCurrent, err := store.CurrentState(ctx)
	if err != nil {
		t.Fatalf("current state: %v", err)
	}
	if !gotCurrent.Masterchain.Block.Equals(&current.Masterchain.Block) {
		t.Fatalf("masterchain block mismatch")
	}
	shard := gotCurrent.Shards[storage.ShardKey{Workchain: 0, Shard: topShard}]
	if !shard.Block.Equals(&blockB) {
		t.Fatalf("shard block mismatch")
	}

}

type pebbleHistoryLookup func(
	context.Context,
	*pebblestore.Store,
	storage.BlockHistoryKey,
	uint64,
) (ton.BlockIDExt, error)

type pebbleHistoryLookupTest struct {
	name         string
	metadataOnly storage.BlockMeta
	served       storage.BlockMeta
	missingValue uint64
	servedValue  uint64
	lookup       pebbleHistoryLookup
}

func TestPebbleStorageHistoryLookupDoesNotSkipMetadataOnlyLowerBound(t *testing.T) {
	tests := []pebbleHistoryLookupTest{
		{
			name: "logical time",
			metadataOnly: storage.BlockMeta{
				ID:       testPebbleBlockID(0, topShard, 12),
				GenUTime: 130,
				StartLT:  3000,
				EndLT:    3999,
			},
			served: storage.BlockMeta{
				ID:       testPebbleBlockID(0, topShard, 13),
				GenUTime: 140,
				StartLT:  4000,
				EndLT:    4999,
			},
			missingValue: 3500,
			servedValue:  4500,
			lookup: func(
				ctx context.Context,
				store *pebblestore.Store,
				key storage.BlockHistoryKey,
				value uint64,
			) (ton.BlockIDExt, error) {
				return store.LookupBlockByLT(ctx, key, value)
			},
		},
		{
			name: "unix time",
			metadataOnly: storage.BlockMeta{
				ID:       testPebbleBlockID(0, topShard, 14),
				GenUTime: 130,
				StartLT:  5000,
				EndLT:    5999,
			},
			served: storage.BlockMeta{
				ID:       testPebbleBlockID(0, topShard, 15),
				GenUTime: 140,
				StartLT:  6000,
				EndLT:    6999,
			},
			missingValue: 130,
			servedValue:  140,
			lookup: func(
				ctx context.Context,
				store *pebblestore.Store,
				key storage.BlockHistoryKey,
				value uint64,
			) (ton.BlockIDExt, error) {
				return store.LookupBlockByUnixTime(ctx, key, uint32(value))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := openTestPebbleStorage(t)
			ctx := context.Background()
			key := storage.BlockHistoryKey{Workchain: 0, Shard: topShard}

			if err := store.SaveBlockMeta(&test.metadataOnly); err != nil {
				t.Fatalf("save metadata-only block meta: %v", err)
			}
			saveTestPebbleFullBlockMeta(t, store, &test.served)

			if _, err := test.lookup(ctx, store, key, test.missingValue); !errors.Is(err, storage.ErrNotFound) {
				t.Fatalf("lookup metadata-only lower-bound error = %v, want ErrNotFound", err)
			}
			got, err := test.lookup(ctx, store, key, test.servedValue)
			if err != nil {
				t.Fatalf("lookup served block: %v", err)
			}
			if !got.Equals(&test.served.ID) {
				t.Fatalf("lookup served block = %s, want %s", storage.FormatBlockRef(got), storage.FormatBlockRef(test.served.ID))
			}
		})
	}
}

type pebbleAccountPathLookupTest struct {
	name            string
	topSeqno        uint32
	intermediateSeq uint32
	pathSeqno       uint32
	siblingSeqno    uint32
	topEndLT        uint64
	pathStartLT     uint64
	siblingStartLT  uint64
	lookupLT        uint64
}

func TestPebbleStorageLookupBlockByAccountLTUsesShardPrefixPath(t *testing.T) {
	tests := []pebbleAccountPathLookupTest{
		{
			name:            "exact interval on shard path",
			topSeqno:        10,
			intermediateSeq: 15,
			pathSeqno:       20,
			siblingSeqno:    30,
			topEndLT:        100,
			pathStartLT:     101,
			siblingStartLT:  101,
			lookupLT:        150,
		},
		{
			name:            "lower bound on shard path",
			topSeqno:        50,
			intermediateSeq: 40,
			pathSeqno:       20,
			siblingSeqno:    10,
			topEndLT:        500,
			pathStartLT:     150,
			siblingStartLT:  1,
			lookupLT:        125,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := openTestPebbleStorage(t)
			ctx := context.Background()
			account := bytes.Repeat([]byte{0x40}, 32)
			shards := mustStorageAccountShardCandidates(t, 0, account)
			if len(shards) < 3 {
				t.Fatalf("account shard candidates = %d, want at least 3", len(shards))
			}

			topBlock := testPebbleBlockID(0, topShard, test.topSeqno)
			intermediateBlock := testPebbleBlockID(0, shards[1], test.intermediateSeq)
			pathBlock := testPebbleBlockID(0, shards[2], test.pathSeqno)
			siblingBlock := testPebbleBlockID(0, int64(0x2000000000000000), test.siblingSeqno)
			for _, meta := range []*storage.BlockMeta{
				{ID: topBlock, StartLT: 1, EndLT: test.topEndLT},
				{ID: intermediateBlock, StartLT: 1, EndLT: 100},
				{ID: pathBlock, StartLT: test.pathStartLT, EndLT: 200},
				{ID: siblingBlock, StartLT: test.siblingStartLT, EndLT: 200},
			} {
				saveTestPebbleFullBlockMeta(t, store, meta)
			}

			got, err := store.LookupBlockByAccountLT(ctx, 0, account, test.lookupLT)
			if err != nil {
				t.Fatalf("lookup account lt: %v", err)
			}
			if !got.Equals(&pathBlock) {
				t.Fatalf("lookup account block = %s, want %s", storage.FormatBlockRef(got), storage.FormatBlockRef(pathBlock))
			}
		})
	}
}

func TestPebbleStorageLookupBlockByAccountLTDoesNotSkipMetadataOnlyBestCandidate(t *testing.T) {
	store := openTestPebbleStorage(t)
	ctx := context.Background()
	account := bytes.Repeat([]byte{0x40}, 32)
	shards := mustStorageAccountShardCandidates(t, 0, account)
	if len(shards) < 3 {
		t.Fatalf("account shard candidates = %d, want at least 3", len(shards))
	}

	metadataOnly := testPebbleBlockID(0, shards[2], 20)
	served := testPebbleBlockID(0, shards[2], 21)

	if err := store.SaveBlockMeta(&storage.BlockMeta{
		ID:      metadataOnly,
		StartLT: 101,
		EndLT:   200,
	}); err != nil {
		t.Fatalf("save metadata-only account block meta: %v", err)
	}
	saveTestPebbleFullBlockMeta(t, store, &storage.BlockMeta{
		ID:      served,
		StartLT: 201,
		EndLT:   300,
	})

	if _, err := store.LookupBlockByAccountLT(ctx, 0, account, 150); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("lookup metadata-only account lower-bound error = %v, want ErrNotFound", err)
	}
	got, err := store.LookupBlockByAccountLT(ctx, 0, account, 250)
	if err != nil {
		t.Fatalf("lookup served account lt: %v", err)
	}
	if !got.Equals(&served) {
		t.Fatalf("lookup served account block = %s, want %s", storage.FormatBlockRef(got), storage.FormatBlockRef(served))
	}
}

func TestPebbleStorageLookupBlockByAccountLTDoesNotReturnFloorBlock(t *testing.T) {
	store := openTestPebbleStorage(t)
	ctx := context.Background()
	account := bytes.Repeat([]byte{0x40}, 32)
	block := testPebbleBlockID(0, topShard, 10)

	saveTestPebbleFullBlockMeta(t, store, &storage.BlockMeta{
		ID:      block,
		StartLT: 1,
		EndLT:   100,
	})

	_, err := store.LookupBlockByAccountLT(ctx, 0, account, 150)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("lookup floor block error = %v, want ErrNotFound", err)
	}
}

func TestPebbleStorageLookupBlockForPrefixMatchesCppShardPath(t *testing.T) {
	store := openTestPebbleStorage(t)
	ctx := context.Background()
	prefix := int64(0x4000000000000008)
	shards := mustStorageShardPrefixCandidates(t, uint64(prefix))
	if len(shards) < 3 {
		t.Fatalf("shard prefix candidates = %d, want at least 3", len(shards))
	}

	rootBlock := testPebbleBlockID(0, shards[0], 50)
	intermediateBlock := testPebbleBlockID(0, shards[1], 40)
	pathBlock := testPebbleBlockID(0, shards[2], 20)
	pathLaterBlock := testPebbleBlockID(0, shards[2], 21)
	siblingBlock := testPebbleBlockID(0, int64(0x2000000000000000), 10)
	for _, meta := range []*storage.BlockMeta{
		{ID: rootBlock, StartLT: 1, EndLT: 500, GenUTime: 500},
		{ID: intermediateBlock, StartLT: 1, EndLT: 100, GenUTime: 100},
		{ID: pathBlock, StartLT: 150, EndLT: 200, GenUTime: 200},
		{ID: pathLaterBlock, StartLT: 201, EndLT: 600, GenUTime: 600},
		{ID: siblingBlock, StartLT: 1, EndLT: 200, GenUTime: 150},
	} {
		saveTestPebbleFullBlockMeta(t, store, meta)
	}

	got, err := store.LookupBlockBySeqNoForPrefix(ctx, storage.BlockSeqRef{Workchain: 0, Shard: prefix, SeqNo: rootBlock.SeqNo})
	if err != nil || !got.Equals(&rootBlock) {
		t.Fatalf("lookup root seqno = %s, err %v, want %s", storage.FormatBlockRef(got), err, storage.FormatBlockRef(rootBlock))
	}

	got, err = store.LookupBlockBySeqNoForPrefix(ctx, storage.BlockSeqRef{Workchain: 0, Shard: prefix, SeqNo: pathBlock.SeqNo})
	if err != nil || !got.Equals(&pathBlock) {
		t.Fatalf("lookup path seqno = %s, err %v, want %s", storage.FormatBlockRef(got), err, storage.FormatBlockRef(pathBlock))
	}

	if _, err = store.LookupBlockBySeqNoForPrefix(ctx, storage.BlockSeqRef{Workchain: 0, Shard: prefix, SeqNo: siblingBlock.SeqNo}); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("lookup sibling seqno error = %v, want ErrNotFound", err)
	}

	key := storage.BlockHistoryKey{Workchain: 0, Shard: prefix}
	got, err = store.LookupBlockByLTForPrefix(ctx, key, 125)
	if err != nil || !got.Equals(&pathBlock) {
		t.Fatalf("lookup prefix lt = %s, err %v, want %s", storage.FormatBlockRef(got), err, storage.FormatBlockRef(pathBlock))
	}

	got, err = store.LookupBlockByUnixTimeForPrefix(ctx, key, 125)
	if err != nil || !got.Equals(&pathBlock) {
		t.Fatalf("lookup prefix utime = %s, err %v, want %s", storage.FormatBlockRef(got), err, storage.FormatBlockRef(pathBlock))
	}

	got, err = store.LookupBlockByLTForPrefix(ctx, key, 500)
	if err != nil || !got.Equals(&rootBlock) {
		t.Fatalf("lookup exact root lt = %s, err %v, want %s", storage.FormatBlockRef(got), err, storage.FormatBlockRef(rootBlock))
	}

	got, err = store.LookupBlockByUnixTimeForPrefix(ctx, key, 500)
	if err != nil || !got.Equals(&rootBlock) {
		t.Fatalf("lookup exact root utime = %s, err %v, want %s", storage.FormatBlockRef(got), err, storage.FormatBlockRef(rootBlock))
	}
}

func TestPebbleStorageLookupBlockForPrefixDoesNotSkipMetadataOnlyCandidate(t *testing.T) {
	store := openTestPebbleStorage(t)
	ctx := context.Background()
	prefix := int64(0x4000000000000008)
	shards := mustStorageShardPrefixCandidates(t, uint64(prefix))

	metadataOnly := testPebbleBlockID(0, shards[2], 20)
	intermediate := testPebbleBlockID(0, shards[1], 40)
	served := testPebbleBlockID(0, shards[0], 50)
	if err := store.SaveBlockMeta(&storage.BlockMeta{
		ID:       metadataOnly,
		StartLT:  150,
		EndLT:    200,
		GenUTime: 200,
	}); err != nil {
		t.Fatalf("save metadata-only path block: %v", err)
	}
	saveTestPebbleFullBlockMeta(t, store, &storage.BlockMeta{
		ID:       served,
		StartLT:  1,
		EndLT:    500,
		GenUTime: 500,
	})
	saveTestPebbleFullBlockMeta(t, store, &storage.BlockMeta{
		ID:       intermediate,
		StartLT:  1,
		EndLT:    100,
		GenUTime: 100,
	})

	key := storage.BlockHistoryKey{Workchain: 0, Shard: prefix}
	if _, err := store.LookupBlockByLTForPrefix(ctx, key, 125); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("lookup metadata-only prefix lt error = %v, want ErrNotFound", err)
	}
	if _, err := store.LookupBlockByUnixTimeForPrefix(ctx, key, 125); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("lookup metadata-only prefix utime error = %v, want ErrNotFound", err)
	}
}

func TestPebbleStorageLookupBlockForPrefixStopsAfterHistoryGap(t *testing.T) {
	store := openTestPebbleStorage(t)
	ctx := context.Background()
	prefix := int64(0x4000000000000008)
	shards := mustStorageShardPrefixCandidates(t, uint64(prefix))

	rootBlock := testPebbleBlockID(0, shards[0], 50)
	deeperBlock := testPebbleBlockID(0, shards[2], 20)
	for _, meta := range []*storage.BlockMeta{
		{ID: rootBlock, StartLT: 1, EndLT: 500, GenUTime: 500},
		{ID: deeperBlock, StartLT: 150, EndLT: 200, GenUTime: 200},
	} {
		saveTestPebbleFullBlockMeta(t, store, meta)
	}

	key := storage.BlockHistoryKey{Workchain: 0, Shard: prefix}
	got, err := store.LookupBlockByLTForPrefix(ctx, key, 125)
	if err != nil || !got.Equals(&rootBlock) {
		t.Fatalf("lookup lt across history gap = %s, err %v, want %s", storage.FormatBlockRef(got), err, storage.FormatBlockRef(rootBlock))
	}

	got, err = store.LookupBlockByUnixTimeForPrefix(ctx, key, 125)
	if err != nil || !got.Equals(&rootBlock) {
		t.Fatalf("lookup utime across history gap = %s, err %v, want %s", storage.FormatBlockRef(got), err, storage.FormatBlockRef(rootBlock))
	}
}

func TestPebbleStorageLookupBlockBySeqNoForPrefixStopsAfterHistoryGap(t *testing.T) {
	store := openTestPebbleStorage(t)
	ctx := context.Background()
	prefix := int64(0x4000000000000008)
	shards := mustStorageShardPrefixCandidates(t, uint64(prefix))

	rootBlock := testPebbleBlockID(0, shards[0], 10)
	blockPastGap := testPebbleBlockID(0, shards[2], 20)
	saveTestPebbleFullBlockMeta(t, store, &storage.BlockMeta{ID: rootBlock, StartLT: 1, EndLT: 100, GenUTime: 100})
	saveTestPebbleFullBlockMeta(t, store, &storage.BlockMeta{ID: blockPastGap, StartLT: 101, EndLT: 200, GenUTime: 200})

	_, err := store.LookupBlockBySeqNoForPrefix(ctx, storage.BlockSeqRef{
		Workchain: 0,
		Shard:     prefix,
		SeqNo:     blockPastGap.SeqNo,
	})
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("lookup seqno across history gap error = %v, want ErrNotFound", err)
	}
}

func TestPebbleStorageLookupBlockForPrefixDirectPathPreservesSplitBoundary(t *testing.T) {
	store := openTestPebbleStorage(t)
	ctx := context.Background()
	childShard := int64(0x4000000000000000)
	rootBlock := testPebbleBlockID(0, topShard, 10)
	childFirst := testPebbleBlockID(0, childShard, 11)
	childLater := testPebbleBlockID(0, childShard, 12)
	for _, meta := range []*storage.BlockMeta{
		{ID: rootBlock, StartLT: 1, EndLT: 100, GenUTime: 100},
		{ID: childFirst, StartLT: 100, EndLT: 200, GenUTime: 100},
		{ID: childLater, StartLT: 201, EndLT: 300, GenUTime: 200},
	} {
		saveTestPebbleFullBlockMeta(t, store, meta)
	}

	key := storage.BlockHistoryKey{Workchain: 0, Shard: childShard}
	got, err := store.LookupBlockBySeqNoForPrefix(ctx, storage.BlockSeqRef{Workchain: 0, Shard: childShard, SeqNo: childFirst.SeqNo})
	if err != nil || !got.Equals(&childFirst) {
		t.Fatalf("lookup direct seqno = %s, err %v, want %s", storage.FormatBlockRef(got), err, storage.FormatBlockRef(childFirst))
	}

	got, err = store.LookupBlockByLTForPrefix(ctx, key, 100)
	if err != nil || !got.Equals(&rootBlock) {
		t.Fatalf("lookup split-boundary lt = %s, err %v, want %s", storage.FormatBlockRef(got), err, storage.FormatBlockRef(rootBlock))
	}
	got, err = store.LookupBlockByUnixTimeForPrefix(ctx, key, 100)
	if err != nil || !got.Equals(&rootBlock) {
		t.Fatalf("lookup split-boundary utime = %s, err %v, want %s", storage.FormatBlockRef(got), err, storage.FormatBlockRef(rootBlock))
	}

	got, err = store.LookupBlockByLTForPrefix(ctx, key, 250)
	if err != nil || !got.Equals(&childLater) {
		t.Fatalf("lookup direct covered lt = %s, err %v, want %s", storage.FormatBlockRef(got), err, storage.FormatBlockRef(childLater))
	}
	got, err = store.LookupBlockByUnixTimeForPrefix(ctx, key, 150)
	if err != nil || !got.Equals(&childLater) {
		t.Fatalf("lookup direct established utime = %s, err %v, want %s", storage.FormatBlockRef(got), err, storage.FormatBlockRef(childLater))
	}
}

var pebbleLookupBenchSink ton.BlockIDExt

type pebbleLookupBenchmarkTest struct {
	name  string
	depth uint32
}

func BenchmarkPebbleStorageLookupBlockForPrefix(b *testing.B) {
	tests := []pebbleLookupBenchmarkTest{
		{name: "depth_6", depth: 6},
		{name: "depth_60", depth: 60},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			store := openTestPebbleStorage(b)
			ctx := context.Background()
			prefix := int64(0x4000000000000008)
			for depth := uint32(0); depth < tt.depth; depth++ {
				ancestorShard, err := sharddomain.FromAccountPrefix(uint64(prefix), depth)
				if err != nil {
					b.Fatal(err)
				}
				ancestor := testPebbleBlockID(0, ancestorShard, depth+1)
				saveTestPebbleFullBlockMeta(b, store, &storage.BlockMeta{
					ID:       ancestor,
					StartLT:  uint64(depth) + 1,
					EndLT:    uint64(depth) + 100,
					GenUTime: depth + 100,
				})
			}

			shard, err := sharddomain.FromAccountPrefix(uint64(prefix), tt.depth)
			if err != nil {
				b.Fatal(err)
			}
			previous := testPebbleBlockID(0, shard, 69)
			block := testPebbleBlockID(0, shard, 70)
			saveTestPebbleFullBlockMeta(b, store, &storage.BlockMeta{
				ID:       previous,
				StartLT:  501,
				EndLT:    600,
				GenUTime: 600,
			})
			saveTestPebbleFullBlockMeta(b, store, &storage.BlockMeta{
				ID:       block,
				StartLT:  601,
				EndLT:    700,
				GenUTime: 700,
			})

			exactKey := storage.BlockHistoryKey{Workchain: 0, Shard: shard}
			prefixKey := storage.BlockHistoryKey{Workchain: 0, Shard: prefix}
			exactRef := storage.BlockSeqRef{Workchain: 0, Shard: shard, SeqNo: block.SeqNo}
			prefixRef := storage.BlockSeqRef{Workchain: 0, Shard: prefix, SeqNo: block.SeqNo}

			b.Run("seq_exact", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					got, err := store.LookupBlockBySeqNo(ctx, exactRef)
					if err != nil {
						b.Fatal(err)
					}
					pebbleLookupBenchSink = got
				}
			})
			b.Run("seq_prefix", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					got, err := store.LookupBlockBySeqNoForPrefix(ctx, prefixRef)
					if err != nil {
						b.Fatal(err)
					}
					pebbleLookupBenchSink = got
				}
			})
			b.Run("seq_direct_prefix", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					got, err := store.LookupBlockBySeqNoForPrefix(ctx, exactRef)
					if err != nil {
						b.Fatal(err)
					}
					pebbleLookupBenchSink = got
				}
			})
			b.Run("seq_future_prefix", func(b *testing.B) {
				future := exactRef
				future.SeqNo = 1000
				b.ReportAllocs()
				for b.Loop() {
					if _, err := store.LookupBlockBySeqNoForPrefix(ctx, future); !errors.Is(err, storage.ErrNotFound) {
						b.Fatalf("future seqno error = %v, want ErrNotFound", err)
					}
				}
			})
			b.Run("lt_exact", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					got, err := store.LookupBlockByLT(ctx, exactKey, 650)
					if err != nil {
						b.Fatal(err)
					}
					pebbleLookupBenchSink = got
				}
			})
			b.Run("lt_prefix", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					got, err := store.LookupBlockByLTForPrefix(ctx, prefixKey, 650)
					if err != nil {
						b.Fatal(err)
					}
					pebbleLookupBenchSink = got
				}
			})
			b.Run("lt_direct_prefix", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					got, err := store.LookupBlockByLTForPrefix(ctx, exactKey, 650)
					if err != nil {
						b.Fatal(err)
					}
					pebbleLookupBenchSink = got
				}
			})
			b.Run("utime_exact", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					got, err := store.LookupBlockByUnixTime(ctx, exactKey, 650)
					if err != nil {
						b.Fatal(err)
					}
					pebbleLookupBenchSink = got
				}
			})
			b.Run("utime_prefix", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					got, err := store.LookupBlockByUnixTimeForPrefix(ctx, prefixKey, 650)
					if err != nil {
						b.Fatal(err)
					}
					pebbleLookupBenchSink = got
				}
			})
			b.Run("utime_direct_prefix", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					got, err := store.LookupBlockByUnixTimeForPrefix(ctx, exactKey, 650)
					if err != nil {
						b.Fatal(err)
					}
					pebbleLookupBenchSink = got
				}
			})
		})
	}
}

func TestPebbleStoragePersistentStateSerializerMetadata(t *testing.T) {
	store := openTestPebbleStorage(t)
	ctx := context.Background()

	last := testPebbleBlockID(-1, topShard, 100)
	written := testPebbleBlockID(-1, topShard, 50)
	cursor := &storage.PersistentStateSerializerState{
		LastBlock:             last,
		LastWrittenBlock:      written,
		LastWrittenBlockUTime: 123456,
	}
	if err := store.SavePersistentStateSerializerState(ctx, cursor); err != nil {
		t.Fatalf("save serializer state: %v", err)
	}

	gotCursor, err := store.PersistentStateSerializerState(ctx)
	if err != nil {
		t.Fatalf("load serializer state: %v", err)
	}
	if !gotCursor.LastBlock.Equals(&last) || !gotCursor.LastWrittenBlock.Equals(&written) || gotCursor.LastWrittenBlockUTime != cursor.LastWrittenBlockUTime {
		t.Fatalf("unexpected serializer cursor: %#v", gotCursor)
	}

	active := &storage.PersistentStateSerializerActive{
		Block:         last,
		StartedAtUnix: 42,
	}
	if err = store.SaveActivePersistentStateSerialization(ctx, active); err != nil {
		t.Fatalf("save active serializer state: %v", err)
	}
	gotActive, err := store.ActivePersistentStateSerialization(ctx)
	if err != nil {
		t.Fatalf("load active serializer state: %v", err)
	}
	if !gotActive.Block.Equals(&active.Block) || gotActive.StartedAtUnix != active.StartedAtUnix {
		t.Fatalf("unexpected active serializer state: %#v", gotActive)
	}
	if err = store.DeleteActivePersistentStateSerialization(ctx); err != nil {
		t.Fatalf("delete active serializer state: %v", err)
	}
	if _, err = store.ActivePersistentStateSerialization(ctx); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("active serializer state after delete error = %v, want not found", err)
	}

	expired := &storage.PersistentStateDescription{
		MasterchainBlock: testPebbleBlockID(-1, topShard, 60),
		StartTime:        10,
		EndTime:          20,
	}
	if err = store.SavePersistentStateDescription(ctx, expired); err != nil {
		t.Fatalf("save expired description: %v", err)
	}
	descriptions, err := store.PersistentStateDescriptions(ctx)
	if err != nil {
		t.Fatalf("load descriptions after expired save: %v", err)
	}
	if len(descriptions) != 0 {
		t.Fatalf("expired description was stored: %#v", descriptions)
	}

	desc := &storage.PersistentStateDescription{
		MasterchainBlock: testPebbleBlockID(-1, topShard, 70),
		StartTime:        uint32(time.Now().Unix()),
		EndTime:          uint64(time.Now().Add(time.Hour).Unix()),
		ShardBlocks: []storage.PersistentStateDescriptionShard{{
			Block:      testPebbleBlockID(0, topShard, 71),
			SplitDepth: 4,
		}},
	}
	if err = store.SavePersistentStateDescription(ctx, desc); err != nil {
		t.Fatalf("save description: %v", err)
	}
	descriptions, err = store.PersistentStateDescriptions(ctx)
	if err != nil {
		t.Fatalf("load descriptions: %v", err)
	}
	if len(descriptions) != 1 {
		t.Fatalf("descriptions count = %d, want 1", len(descriptions))
	}
	if !descriptions[0].MasterchainBlock.Equals(&desc.MasterchainBlock) || descriptions[0].ShardBlocks[0].SplitDepth != 4 {
		t.Fatalf("unexpected description: %#v", descriptions[0])
	}
}

func TestPebbleStorageLoadsCellsByRepresentationHash(t *testing.T) {
	store := openTestPebbleStorage(t)
	ctx := context.Background()

	hidden := cell.BeginCell().MustStoreUInt(0xAA, 8).EndCell()
	var depth [2]byte
	binary.BigEndian.PutUint16(depth[:], hidden.Depth(0))
	hiddenLevelZeroHash := hidden.HashKey(0)
	prunedData := append([]byte{byte(cell.PrunedCellType), 0x01}, hiddenLevelZeroHash[:]...)
	prunedData = append(prunedData, depth[:]...)
	pruned, err := cell.BeginCell().MustStoreSlice(prunedData, uint(len(prunedData))*8).EndCellSpecial(true)
	if err != nil {
		t.Fatalf("create pruned cell: %v", err)
	}
	root := cell.BeginCell().MustStoreRef(pruned).EndCell()
	rootHash := root.HashKey()
	rootLevelZeroHash := root.HashKey(0)
	if rootHash == rootLevelZeroHash {
		t.Fatalf("test fixture must have different level-zero and max-level hashes")
	}

	records, err := collectCellRecordsForTest(root)
	if err != nil {
		t.Fatalf("collect cell records: %v", err)
	}
	if err = saveActiveTestCells(store, records); err != nil {
		t.Fatalf("save cells: %v", err)
	}

	loaded, err := store.LoadCell(ctx, rootHash[:])
	if err != nil {
		t.Fatalf("load cell by level-zero hash: %v", err)
	}
	requireLazyCellBOC(t, loaded, root)

	fullRoot := cell.BeginCell().MustStoreRef(hidden).EndCell()
	prunedLevelZeroHash := pruned.HashKey(0)
	if prunedLevelZeroHash != hiddenLevelZeroHash {
		t.Fatalf("test fixture must share level-zero hash")
	}
	prunedHash := pruned.HashKey()
	hiddenHash := hidden.HashKey()
	if prunedHash == hiddenHash {
		t.Fatalf("test fixture must have different representation hashes")
	}

	records, err = collectCellRecordsForTest(fullRoot)
	if err != nil {
		t.Fatalf("collect full cell records: %v", err)
	}
	if err = saveActiveTestCells(store, records); err != nil {
		t.Fatalf("save full cells: %v", err)
	}

	fullRootHash := fullRoot.HashKey()
	loadedFull, err := store.LoadCell(ctx, fullRootHash[:])
	if err != nil {
		t.Fatalf("load full root: %v", err)
	}
	requireLazyCellBOC(t, loadedFull, fullRoot)

	loadedPruned, err := store.LoadCell(ctx, rootHash[:])
	if err != nil {
		t.Fatalf("reload pruned root: %v", err)
	}
	requireLazyCellBOC(t, loadedPruned, root)
}

func TestPebbleStorageLoadsLazyCells(t *testing.T) {
	store := openTestPebbleStorage(t)
	ctx := context.Background()

	leaf := cell.BeginCell().MustStoreUInt(0xAA, 8).EndCell()
	child := cell.BeginCell().MustStoreUInt(0xBB, 8).MustStoreRef(leaf).EndCell()
	root := cell.BeginCell().MustStoreUInt(0xCC, 8).MustStoreRef(child).EndCell()
	records, err := collectCellRecordsForTest(root)
	if err != nil {
		t.Fatalf("collect cell records: %v", err)
	}
	if err = saveActiveTestCells(store, records); err != nil {
		t.Fatalf("save cells: %v", err)
	}

	rootHash := root.HashKey()
	loaded, err := store.LoadCell(ctx, rootHash[:])
	if err != nil {
		t.Fatalf("load lazy root: %v", err)
	}
	if loaded.IsLazy() {
		t.Fatalf("loaded root should have body")
	}
	if loaded.HashKey() != root.HashKey() || loaded.Depth() != root.Depth() {
		t.Fatalf("loaded root metadata mismatch")
	}

	loadedChild, err := loaded.PeekRef(0)
	if err != nil {
		t.Fatalf("load lazy child: %v", err)
	}
	loadedChildSlice, err := loadedChild.BeginParse()
	if err != nil {
		t.Fatalf("materialize lazy child: %v", err)
	}
	loadedChild = loadedChildSlice.BaseCell()
	if loadedChild.HashKey() != child.HashKey() || loadedChild.Depth() != child.Depth() {
		t.Fatalf("loaded child metadata mismatch")
	}
	if loadedChild.RefsNum() != child.RefsNum() {
		t.Fatalf("loaded child refs mismatch: got=%d want=%d lazy=%v dump=%s", loadedChild.RefsNum(), child.RefsNum(), loadedChild.IsLazy(), loadedChild.Dump(512))
	}

	loadedLeaf, err := loadedChild.PeekRef(0)
	if err != nil {
		t.Fatalf("load lazy leaf: %v", err)
	}
	loadedLeafSlice, err := loadedLeaf.BeginParse()
	if err != nil {
		t.Fatalf("materialize lazy leaf: %v", err)
	}
	loadedLeaf = loadedLeafSlice.BaseCell()
	if loadedLeaf.HashKey() != leaf.HashKey() || loadedLeaf.Depth() != leaf.Depth() {
		t.Fatalf("loaded leaf metadata mismatch")
	}

	hidden := cell.BeginCell().MustStoreUInt(0xDD, 8).EndCell()
	var depth [2]byte
	binary.BigEndian.PutUint16(depth[:], hidden.Depth(0))
	hiddenHash := hidden.HashKey(0)
	prunedData := append([]byte{byte(cell.PrunedCellType), 0x01}, hiddenHash[:]...)
	prunedData = append(prunedData, depth[:]...)
	pruned, err := cell.BeginCell().MustStoreSlice(prunedData, uint(len(prunedData))*8).EndCellSpecial(true)
	if err != nil {
		t.Fatalf("create pruned cell: %v", err)
	}
	prunedRoot := cell.BeginCell().MustStoreRef(pruned).EndCell()
	records, err = collectCellRecordsForTest(prunedRoot)
	if err != nil {
		t.Fatalf("collect pruned cell records: %v", err)
	}
	if err = saveActiveTestCells(store, records); err != nil {
		t.Fatalf("save pruned cells: %v", err)
	}

	prunedRootHash := prunedRoot.HashKey()
	loadedPrunedRoot, err := store.LoadCell(ctx, prunedRootHash[:])
	if err != nil {
		t.Fatalf("load lazy pruned root: %v", err)
	}
	loadedPruned, err := loadedPrunedRoot.PeekRef(0)
	if err != nil {
		t.Fatalf("load lazy pruned ref: %v", err)
	}
	loadedPrunedSlice, err := loadedPruned.BeginParse()
	if err != nil {
		t.Fatalf("materialize lazy pruned ref: %v", err)
	}
	loadedPruned = loadedPrunedSlice.BaseCell()
	if loadedPruned.HashKey() != pruned.HashKey() || loadedPruned.Depth() != pruned.Depth() {
		t.Fatalf("loaded pruned metadata mismatch")
	}
}

func openTestPebbleStorage(t testing.TB) *pebblestore.Store {
	t.Helper()

	store, err := pebblestore.Open(pebblestore.Options{
		Dir: filepath.Join(t.TempDir(), "storage"),
	})
	if err != nil {
		t.Fatalf("open pebble storage: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close pebble storage: %v", err)
		}
	})
	return store
}

func saveTestPebbleFullBlockMeta(t testing.TB, store *pebblestore.Store, meta *storage.BlockMeta) {
	t.Helper()

	fullMeta := meta.Clone()
	entries := []storage.StateCheckpointBlock{}
	if fullMeta.ID.Workchain != -1 && !fullMeta.MasterchainRefKnown() {
		master := testPebbleBlockID(-1, topShard, 1)
		fullMeta.MasterchainRefSeqno = master.SeqNo
		entries = append(entries, storage.StateCheckpointBlock{
			State: &storage.BlockState{
				Block:         master,
				StateRootHash: bytes.Repeat([]byte{0x03}, 32),
			},
			Artifact: &storage.ServedBlockFull{
				ID:    master,
				Block: []byte{0x03},
				Proof: []byte{0x04},
				Meta:  &storage.BlockMeta{ID: master, GenUTime: 1},
			},
		})
	}
	entries = append(entries, storage.StateCheckpointBlock{
		State: &storage.BlockState{
			Block:         fullMeta.ID,
			StateRootHash: bytes.Repeat([]byte{0x01}, 32),
		},
		Artifact: &storage.ServedBlockFull{
			ID:    fullMeta.ID,
			Block: []byte{0x01},
			Proof: []byte{0x02},
			Meta:  fullMeta,
		},
	})
	if _, err := store.SaveStateCheckpointEntries(context.Background(), entries, storage.StateCellRecords{}, nil); err != nil {
		t.Fatalf("save full block meta %s: %v", storage.FormatBlockRef(meta.ID), err)
	}
}

func requireLazyCellBOC(t testing.TB, loaded *cell.Cell, want *cell.Cell) {
	t.Helper()

	if loaded.IsLazy() {
		t.Fatalf("expected loaded cell body")
	}

	materialized := materializeLazyCell(t, loaded)
	if !bytes.Equal(materialized.ToBOC(), want.ToBOC()) {
		t.Fatalf("loaded cell mismatch")
	}
}

func materializeLazyCell(t testing.TB, cl *cell.Cell) *cell.Cell {
	t.Helper()

	loader := cl.MustBeginParse()
	cl = loader.BaseCell()
	bits, data, err := loader.RestBits()
	if err != nil {
		t.Fatalf("load cell bits: %v", err)
	}

	builder := cell.BeginCell().MustStoreSlice(data, bits)
	for i := 0; i < int(cl.RefsNum()); i++ {
		ref, err := cl.PeekRef(i)
		if err != nil {
			t.Fatalf("load lazy ref %d: %v", i, err)
		}
		builder.MustStoreRef(materializeLazyCell(t, ref))
	}

	if cl.IsSpecial() {
		special, err := builder.EndCellSpecial(true)
		if err != nil {
			t.Fatalf("end special cell: %v", err)
		}
		return special
	}
	return builder.EndCell()
}

func testPebbleBlockID(workchain int32, shard int64, seqno uint32) ton.BlockIDExt {
	root := bytes.Repeat([]byte{byte(seqno)}, 32)
	file := bytes.Repeat([]byte{byte(seqno + 1)}, 32)
	return ton.BlockIDExt{
		Workchain: workchain,
		Shard:     shard,
		SeqNo:     seqno,
		RootHash:  root,
		FileHash:  file,
	}
}
