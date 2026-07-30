package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

// This file pins the prefix lookups to the reference node. cppBlockCommon below
// is a transcription of ArchiveSlice::get_block_common (cppnode/ton/validator/
// db/archive-slice.cpp), and the tests run it and the real store over the same
// shard histories and compare block ids.
//
// The two indexes line up with the reference by construction: C++ stores one
// db.lt.el.value per block carrying handle->logical_time() and
// handle->unix_time(), and logical_time() is the block's END lt (accept-block.cpp
// sets lt_ = info.end_lt), which is exactly what hotKeyBlockLTIndex keys on. So
// "first entry at or after the requested point" means the same thing on both
// sides, and only the early-return rule had to be ported.

// cppHistoryEntry is one db.lt.el.value: the per-shard index entry C++ writes
// for every block.
type cppHistoryEntry struct {
	block ton.BlockIDExt
	endLT uint64
	utime uint32
}

// cppBlockCommon is get_block_common with `exact=false`. compare returns the
// sign of (requested - entry), matching the lambdas C++ passes in.
func cppBlockCommon(
	histories map[int64][]cppHistoryEntry,
	workchain int32,
	shard int64,
	compare func(cppHistoryEntry) int,
) (ton.BlockIDExt, bool) {
	var (
		blockID    ton.BlockIDExt
		blockValid bool
		ls         uint32
		seenAny    bool
	)

	maxDepth := uint32(60)
	if workchain == masterchainWorkchain {
		// shard_prefix of a masterchain account is the masterchain shard at
		// every depth, so the loop only ever visits this one history.
		maxDepth = 0
	}
	for depth := uint32(0); depth <= maxDepth; depth++ {
		prefix := storage.AccountShardPrefix(uint64(shard), depth)
		entries, ok := histories[prefix]
		if !ok || len(entries) == 0 {
			if !seenAny {
				continue
			}
			break
		}
		seenAny = true

		// `if (compare_desc(*g) > 0) continue;` - the request is past the last
		// entry of this history, so it holds nothing at or after it.
		if compare(entries[len(entries)-1]) > 0 {
			continue
		}

		var (
			leftSeqno   uint32
			leftValid   bool
			rightEntry  cppHistoryEntry
			rightValid  bool
			exactEntry  cppHistoryEntry
			exactIsHere bool
		)
		for _, entry := range entries {
			switch cmp := compare(entry); {
			case cmp == 0:
				exactEntry = entry
				exactIsHere = true
			case cmp > 0:
				leftSeqno = entry.block.SeqNo
				leftValid = true
			default:
				if !rightValid {
					rightEntry = entry
					rightValid = true
				}
			}
			if exactIsHere {
				break
			}
		}
		if exactIsHere {
			return exactEntry.block, true
		}

		if rightValid && (!blockValid || blockID.SeqNo > rightEntry.block.SeqNo) {
			blockID = rightEntry.block
			blockValid = true
		}
		if leftValid && ls < leftSeqno {
			ls = leftSeqno
		}
		if blockValid && ls+1 == blockID.SeqNo {
			return blockID, true
		}
	}
	return blockID, blockValid
}

const masterchainWorkchain int32 = -1

func cppLookupByLT(histories map[int64][]cppHistoryEntry, workchain int32, shard int64, lt uint64) (ton.BlockIDExt, bool) {
	return cppBlockCommon(histories, workchain, shard, func(entry cppHistoryEntry) int {
		switch {
		case lt > entry.endLT:
			return 1
		case lt == entry.endLT:
			return 0
		default:
			return -1
		}
	})
}

func cppLookupByUnixTime(histories map[int64][]cppHistoryEntry, workchain int32, shard int64, utime uint32) (ton.BlockIDExt, bool) {
	return cppBlockCommon(histories, workchain, shard, func(entry cppHistoryEntry) int {
		switch {
		case utime > entry.utime:
			return 1
		case utime == entry.utime:
			return 0
		default:
			return -1
		}
	})
}

// shardTimeline builds a chain of blocks whose shard changes over time, the way
// a split and a later merge move an account between histories. seqnos are
// global to the chain, so they stay adjacent across a shard change exactly as
// they do on a real network.
type shardTimeline struct {
	workchain int32
	blocks    []cppHistoryEntry
	starts    []uint64
}

func newShardTimeline(workchain int32) *shardTimeline {
	return &shardTimeline{workchain: workchain}
}

func (tl *shardTimeline) add(shard int64, seqno uint32, startLT, endLT uint64, utime uint32) {
	tl.blocks = append(tl.blocks, cppHistoryEntry{
		block: ton.BlockIDExt{
			Workchain: tl.workchain,
			Shard:     shard,
			SeqNo:     seqno,
			RootHash:  bytes.Repeat([]byte{byte(seqno), byte(seqno >> 8)}, 16),
			FileHash:  bytes.Repeat([]byte{byte(seqno + 1), byte(seqno >> 8)}, 16),
		},
		endLT: endLT,
		utime: utime,
	})
	tl.starts = append(tl.starts, startLT)
}

func (tl *shardTimeline) histories() map[int64][]cppHistoryEntry {
	histories := map[int64][]cppHistoryEntry{}
	for _, entry := range tl.blocks {
		histories[entry.block.Shard] = append(histories[entry.block.Shard], entry)
	}
	for shard := range histories {
		entries := histories[shard]
		sort.Slice(entries, func(i, j int) bool { return entries[i].block.SeqNo < entries[j].block.SeqNo })
		histories[shard] = entries
	}
	return histories
}

func (tl *shardTimeline) save(t *testing.T, store *Store) {
	t.Helper()

	// Go through the served-artifact path rather than SaveBlockMeta: the index
	// gates on BlockMetaHasServedFull, which only that path may set.
	blocks := make([]*storage.ServedBlockFull, 0, len(tl.blocks))
	for i, entry := range tl.blocks {
		meta := &storage.BlockMeta{
			ID:       entry.block,
			StartLT:  tl.starts[i],
			EndLT:    entry.endLT,
			GenUTime: entry.utime,
		}
		if entry.block.Workchain != masterchainWorkchain {
			meta.MasterchainRefSeqno = entry.block.SeqNo
		}
		blocks = append(blocks, &storage.ServedBlockFull{
			ID:    entry.block,
			Block: []byte{0x01},
			Proof: []byte{0x02},
			Meta:  meta,
		})
	}
	if err := saveTestArchiveArtifacts(store, testArchiveImport{FullBlocks: blocks}); err != nil {
		t.Fatalf("save served blocks: %v", err)
	}
}

func openLookupTestStore(t *testing.T) *Store {
	t.Helper()

	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	})
	return store
}

func describeBlock(block ton.BlockIDExt, ok bool) string {
	if !ok {
		return "<none>"
	}
	return fmt.Sprintf("shard %016x seqno %d", uint64(block.Shard), block.SeqNo)
}

// splitMergeTimeline reproduces the shape that broke the unix-time lookup: a
// parent shard splits, the child produces blocks, and the shards merge back, so
// the parent history holds two blocks whose seqnos are far apart with the
// child's blocks in between.
func splitMergeTimeline(workchain int32) *shardTimeline {
	const (
		parent = int64(0x4000000000000000)
		child  = int64(0x6000000000000000)
	)

	tl := newShardTimeline(workchain)
	tl.add(parent, 10, 1000, 1009, 1000)
	tl.add(child, 15, 1500, 1509, 1500)
	tl.add(child, 16, 1600, 1609, 1600)
	tl.add(parent, 21, 2000, 2009, 2000)
	return tl
}

func rootSplitMergeTimeline(workchain int32) *shardTimeline {
	const (
		root  = int64(-1 << 63) // 0x8000000000000000, prefix length 0
		child = int64(-1<<63) | int64(0x4000000000000000)
	)

	tl := newShardTimeline(workchain)
	tl.add(root, 110, 3000, 3009, 3000)
	tl.add(child, 115, 3500, 3509, 3500)
	tl.add(child, 116, 3600, 3609, 3600)
	tl.add(root, 121, 4000, 4009, 4000)
	return tl
}

func assertPrefixLookupsMatchCPP(t *testing.T, tl *shardTimeline, key storage.BlockHistoryKey) {
	t.Helper()

	store := openLookupTestStore(t)
	tl.save(t, store)
	histories := tl.histories()

	ctx := context.Background()

	var utimes []uint32
	var lts []uint64
	for _, entry := range tl.blocks {
		for _, delta := range []int{-5, -1, 0, 1, 5} {
			utimes = append(utimes, uint32(int(entry.utime)+delta))
			lts = append(lts, uint64(int(entry.endLT)+delta))
		}
	}

	for _, utime := range utimes {
		want, wantOK := cppLookupByUnixTime(histories, key.Workchain, key.Shard, utime)
		got, err := store.LookupBlockByUnixTimeForPrefix(ctx, key, utime)
		gotOK := err == nil
		if !gotOK && !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("utime %d: unexpected error %v", utime, err)
		}
		if gotOK != wantOK || (wantOK && !got.Equals(&want)) {
			t.Fatalf(
				"utime %d: store returned %s, reference node returns %s",
				utime,
				describeBlock(got, gotOK),
				describeBlock(want, wantOK),
			)
		}
	}

	for _, lt := range lts {
		want, wantOK := cppLookupByLT(histories, key.Workchain, key.Shard, lt)
		got, err := store.LookupBlockByLTForPrefix(ctx, key, lt)
		gotOK := err == nil
		if !gotOK && !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("lt %d: unexpected error %v", lt, err)
		}
		if gotOK != wantOK || (wantOK && !got.Equals(&want)) {
			t.Fatalf(
				"lt %d: store returned %s, reference node returns %s",
				lt,
				describeBlock(got, gotOK),
				describeBlock(want, wantOK),
			)
		}
	}
}

// The direct hit may only be returned without walking the account's shard
// prefixes when its predecessor in the same history is its seqno predecessor -
// the single early return get_block_common takes. Judging the history's first
// entry instead let a lookup stop on a post-merge parent block while the child
// shard held the correct, earlier one.
func TestPrefixLookupsMatchReferenceAcrossSplitAndMerge(t *testing.T) {
	tl := splitMergeTimeline(0)
	assertPrefixLookupsMatchCPP(t, tl, storage.BlockHistoryKey{
		Workchain: 0,
		Shard:     0x4000000000000000,
	})
}

// A workchain root shard splits like any other, so it must not skip the walk.
// Only the masterchain may, and only because its prefix never splits.
func TestPrefixLookupsMatchReferenceForWorkchainRootShard(t *testing.T) {
	tl := rootSplitMergeTimeline(0)
	assertPrefixLookupsMatchCPP(t, tl, storage.BlockHistoryKey{
		Workchain: 0,
		Shard:     -1 << 63,
	})
}

// The masterchain never splits, so every lookup has to keep answering straight
// from its one history.
func TestPrefixLookupsMatchReferenceForMasterchain(t *testing.T) {
	tl := newShardTimeline(-1)
	shard := int64(-1 << 63)
	for i := uint32(0); i < 8; i++ {
		tl.add(shard, 100+i, uint64(5000+i*10), uint64(5009+i*10), 5000+i*10)
	}
	assertPrefixLookupsMatchCPP(t, tl, storage.BlockHistoryKey{
		Workchain: -1,
		Shard:     shard,
	})
}

// The exact numbers from the report, asserted directly so a regression names
// the wrong block rather than only disagreeing with the model.
func TestUnixTimeLookupDescendsIntoChildShard(t *testing.T) {
	store := openLookupTestStore(t)
	tl := splitMergeTimeline(0)
	tl.save(t, store)

	key := storage.BlockHistoryKey{Workchain: 0, Shard: 0x4000000000000000}
	got, err := store.LookupBlockByUnixTimeForPrefix(context.Background(), key, 1495)
	if err != nil {
		t.Fatalf("lookup by utime: %v", err)
	}
	if got.SeqNo != 15 || uint64(got.Shard) != 0x6000000000000000 {
		t.Fatalf("utime 1495 returned %s, want the child shard's seqno 15", describeBlock(got, true))
	}
}
