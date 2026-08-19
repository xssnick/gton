package pebblestore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xssnick/gton/service/archive/packfile"
	"github.com/xssnick/gton/service/storage"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type testArchiveImport struct {
	FullBlocks []*storage.ServedBlockFull
	Links      []storage.ServedBlockLink
}

type testArtifactMetricsObserver struct {
	archivePackageBytes  int64
	persistentStateBytes int64
}

func (o *testArtifactMetricsObserver) AddArchivePackageBytes(delta int64) {
	o.archivePackageBytes += delta
}

func (o *testArtifactMetricsObserver) AddPersistentStateBytes(delta int64) {
	o.persistentStateBytes += delta
}

func saveTestArchiveArtifacts(store *Store, imported testArchiveImport) error {
	entries := make([]storage.StateCheckpointBlock, 0, len(imported.FullBlocks))
	if len(imported.FullBlocks) == 0 {
		entries = append(entries, storage.StateCheckpointBlock{Links: imported.Links})
	} else {
		for idx, full := range imported.FullBlocks {
			entry := storage.StateCheckpointBlock{Artifact: full}
			if idx == 0 {
				entry.Links = imported.Links
			}
			entries = append(entries, entry)
		}
	}

	_, err := store.SaveStateCheckpointEntries(context.Background(), entries, storage.StateCellRecords{}, nil)
	return err
}

func TestSetServedBlockFullArtifactRefsRejectsMissingProofRef(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	block := &storage.ServedBlockFull{
		ID: testServedBlockID(-1, topShard, 42, 0x42),
	}
	kind := storage.ServedProofBlock
	err = store.withHotBatch(func(batch *pebble.Batch) error {
		return store.setServedBlockFullArtifactRefs(
			batch,
			block,
			[]storage.ServedProofKind{kind},
			nil,
			map[storage.ServedProofKind]*storage.ArtifactRef{},
		)
	})
	if err == nil {
		t.Fatal("missing proof ref was accepted")
	}
	if !strings.Contains(err.Error(), "proof ref invariant violated") {
		t.Fatalf("missing proof ref error = %v, want invariant violation", err)
	}
	if !strings.Contains(err.Error(), string(kind)) {
		t.Fatalf("missing proof ref error = %v, want proof kind %q", err, kind)
	}
	if !strings.Contains(err.Error(), storage.FormatBlockRef(block.ID)) {
		t.Fatalf("missing proof ref error = %v, want block context", err)
	}
}

func TestCheckpointArchiveArtifactsAllowsShardSplitNextChildrenAcrossBatches(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	prev := testServedBlockID(0, int64(-1<<63), 100, 1)
	left := testServedBlockID(0, int64(0x4000000000000000), 101, 2)
	right := testServedBlockID(0, int64(-0x4000000000000000), 101, 3)
	master := testServedBlockID(-1, int64(-1<<63), 90, 4)

	if err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{
			testLinkPreviousBlock(master, 0),
			testLinkPreviousBlock(prev, master.SeqNo),
			testLinkPreviousBlock(right, master.SeqNo),
		},
		Links: []storage.ServedBlockLink{{Prev: prev, Next: right}},
	}); err != nil {
		t.Fatalf("link right split child: %v", err)
	}
	if err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{
			testLinkPreviousBlock(left, master.SeqNo),
		},
		Links: []storage.ServedBlockLink{{Prev: prev, Next: left}},
	}); err != nil {
		t.Fatalf("link left split child: %v", err)
	}

	got, err := readNextBlockLink(context.Background(), store, prev)
	if err != nil {
		t.Fatalf("load next block link: %v", err)
	}
	if !got.Equals(&left) {
		t.Fatalf("next block link = %s, want left split child %s", storage.FormatBlockRef(got), storage.FormatBlockRef(left))
	}
}

func TestCheckpointArchiveArtifactsAllowsBothShardSplitChildrenAfterDurableParent(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	prev := testServedBlockID(0, int64(-1<<63), 100, 1)
	left := testServedBlockID(0, int64(0x4000000000000000), 101, 2)
	right := testServedBlockID(0, int64(-0x4000000000000000), 101, 3)
	master := testServedBlockID(-1, int64(-1<<63), 90, 4)

	if err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{
			testLinkPreviousBlock(master, 0),
			testLinkPreviousBlock(prev, master.SeqNo),
		},
	}); err != nil {
		t.Fatalf("save durable split parent: %v", err)
	}
	if err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{
			testLinkPreviousBlock(right, master.SeqNo),
			testLinkPreviousBlock(left, master.SeqNo),
		},
		Links: []storage.ServedBlockLink{
			{Prev: prev, Next: right},
			{Prev: prev, Next: left},
		},
	}); err != nil {
		t.Fatalf("save split children: %v", err)
	}

	got, err := readNextBlockLink(context.Background(), store, prev)
	if err != nil {
		t.Fatalf("load next block link: %v", err)
	}
	if !got.Equals(&left) {
		t.Fatalf("next block link = %s, want left split child %s", storage.FormatBlockRef(got), storage.FormatBlockRef(left))
	}
}

func TestCheckpointArchiveArtifactsRejectsSameShardNextForkAcrossBatches(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	prev := testServedBlockID(0, int64(-1<<63), 100, 1)
	next := testServedBlockID(0, int64(-1<<63), 101, 2)
	fork := testServedBlockID(0, int64(-1<<63), 101, 3)
	master := testServedBlockID(-1, int64(-1<<63), 90, 4)

	if err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{
			testLinkPreviousBlock(master, 0),
			testLinkPreviousBlock(prev, master.SeqNo),
			testLinkPreviousBlock(next, master.SeqNo),
		},
		Links: []storage.ServedBlockLink{{Prev: prev, Next: next}},
	}); err != nil {
		t.Fatalf("link next block: %v", err)
	}
	if err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{
			testLinkPreviousBlock(fork, master.SeqNo),
		},
		Links: []storage.ServedBlockLink{{Prev: prev, Next: fork}},
	}); err == nil {
		t.Fatal("same-shard next block fork was accepted")
	}
}

func TestCheckpointArchiveArtifactsSelectsLeftShardSplitNextLinkInBatch(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	prev := testServedBlockID(0, int64(-1<<63), 100, 1)
	left := testServedBlockID(0, int64(0x4000000000000000), 101, 2)
	right := testServedBlockID(0, int64(-0x4000000000000000), 101, 3)
	master := testServedBlockID(-1, int64(-1<<63), 90, 4)
	if err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{
			testLinkPreviousBlock(master, 0),
			testLinkPreviousBlock(prev, master.SeqNo),
			testLinkPreviousBlock(right, master.SeqNo),
			testLinkPreviousBlock(left, master.SeqNo),
		},
		Links: []storage.ServedBlockLink{
			{Prev: prev, Next: right},
			{Prev: prev, Next: left},
		},
	}); err != nil {
		t.Fatalf("save archive import split links: %v", err)
	}

	got, err := readNextBlockLink(context.Background(), store, prev)
	if err != nil {
		t.Fatalf("load next block link: %v", err)
	}
	if !got.Equals(&left) {
		t.Fatalf("archive import next block link = %s, want left split child %s", storage.FormatBlockRef(got), storage.FormatBlockRef(left))
	}
}

func TestCheckpointArchiveArtifactsRejectsNextLinkWithoutNextBlock(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	prev := testServedBlockID(-1, int64(-1<<63), 100, 1)
	next := testServedBlockID(-1, int64(-1<<63), 101, 2)

	err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{testLinkPreviousBlock(prev, 0)},
		Links:      []storage.ServedBlockLink{{Prev: prev, Next: next}},
	})
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("missing next block link error = %v, want ErrNotFound", err)
	}
	if _, err = store.BlockMeta(context.Background(), prev); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("partial import previous block meta error = %v, want ErrNotFound", err)
	}
	if _, err = store.BlockMeta(context.Background(), next); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("partial import next block meta error = %v, want ErrNotFound", err)
	}
}

func TestCheckpointArchiveArtifactsKeepsSameBatchMetaAndNextLink(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	prev := testServedBlockID(-1, int64(-1<<63), 100, 1)
	next := testServedBlockID(-1, int64(-1<<63), 101, 2)
	if err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{{
			ID:    prev,
			Block: []byte{0x01},
			Proof: []byte{0x02},
			Meta: &storage.BlockMeta{
				ID:       prev,
				GenUTime: 123,
				StartLT:  10,
				EndLT:    20,
			},
		}, {
			ID:    next,
			Block: []byte{0x03},
			Proof: []byte{0x04},
			Meta: &storage.BlockMeta{
				ID:       next,
				GenUTime: 124,
				StartLT:  20,
				EndLT:    30,
			},
		}},
		Links: []storage.ServedBlockLink{{Prev: prev, Next: next}},
	}); err != nil {
		t.Fatalf("save archive import: %v", err)
	}

	meta, err := store.BlockMeta(context.Background(), prev)
	if err != nil {
		t.Fatalf("load prev meta: %v", err)
	}
	if !meta.Has(storage.BlockMetaHasServedFull) || meta.GenUTime != 123 || meta.EndLT != 20 {
		t.Fatalf("prev meta = flags=%v utime=%d end_lt=%d, want served meta", meta.Flags, meta.GenUTime, meta.EndLT)
	}
	if len(meta.NextRefs) != 1 || !meta.NextRefs[0].Equals(&next) {
		t.Fatalf("prev next refs = %+v, want %s", meta.NextRefs, storage.FormatBlockRef(next))
	}
}

func testLinkPreviousBlock(block ton.BlockIDExt, masterSeqno uint32) *storage.ServedBlockFull {
	meta := &storage.BlockMeta{
		ID:       block,
		GenUTime: block.SeqNo,
	}
	if block.Workchain != -1 || block.Shard != int64(-1<<63) {
		meta.MasterchainRefSeqno = masterSeqno
	}
	return &storage.ServedBlockFull{
		ID:    block,
		Block: []byte{0x01},
		Proof: []byte{0x02},
		Meta:  meta,
	}
}

func TestCheckpointArchiveArtifactsRejectsEmptyFullBlock(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	block := testServedBlockID(-1, int64(-1<<63), 102, 4)
	err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{{ID: block}},
	})
	if err == nil || !strings.Contains(err.Error(), "has no block data or proof") {
		t.Fatalf("save empty full block err = %v, want empty block error", err)
	}
	if _, err = store.BlockMeta(context.Background(), block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("empty full block meta err = %v, want ErrNotFound", err)
	}
}

func TestCheckpointArchiveArtifactsRejectsMetadataOnlyFullBlock(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	block := testServedBlockID(-1, int64(-1<<63), 102, 4)
	err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{{
			ID:   block,
			Meta: &storage.BlockMeta{ID: block},
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "has no block data or proof") {
		t.Fatalf("save metadata-only full block err = %v, want missing payload error", err)
	}
	if _, err = store.BlockMeta(context.Background(), block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("metadata-only full block meta err = %v, want ErrNotFound", err)
	}
}

func TestCheckpointArchiveArtifactsRejectsForgedServedFullFlag(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	block := testServedBlockID(-1, int64(-1<<63), 102, 4)
	meta := &storage.BlockMeta{ID: block}
	meta.Mark(storage.BlockMetaHasServedFull)
	err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{{
			ID:   block,
			Meta: meta,
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "has no block data or proof") {
		t.Fatalf("save forged served-full flag err = %v, want missing payload error", err)
	}
	if _, err = store.BlockMeta(context.Background(), block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("forged served-full block meta err = %v, want ErrNotFound", err)
	}
}

func TestCheckpointArchiveArtifactsRejectsFullBlockMetaNextRefs(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	block := testServedBlockID(-1, int64(-1<<63), 102, 4)
	next := testServedBlockID(-1, int64(-1<<63), 103, 5)
	err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{{
			ID:    block,
			Block: []byte{0x01},
			Proof: []byte{0x02},
			Meta: &storage.BlockMeta{
				ID:       block,
				NextRefs: []ton.BlockIDExt{next},
			},
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "cannot set next refs directly") {
		t.Fatalf("save full block meta next refs err = %v, want direct next refs error", err)
	}
	if _, err = store.BlockMeta(context.Background(), block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("direct next refs full block meta err = %v, want ErrNotFound", err)
	}
}

func TestCheckpointArchiveArtifactsDoesNotMarkPartialBlockAsServedFull(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	block := testServedBlockID(-1, int64(-1<<63), 102, 4)
	if err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{{
			ID:    block,
			Block: []byte{0x01},
			Meta:  &storage.BlockMeta{ID: block, GenUTime: 123},
		}},
	}); err != nil {
		t.Fatalf("save partial archive import: %v", err)
	}

	meta, err := store.BlockMeta(context.Background(), block)
	if err != nil {
		t.Fatalf("load block meta: %v", err)
	}
	if meta.Has(storage.BlockMetaHasServedFull) {
		t.Fatalf("partial block was marked served full: flags=%v", meta.Flags)
	}
	if !meta.Has(storage.BlockMetaHasBlockData) {
		t.Fatalf("partial block data flag is missing: flags=%v", meta.Flags)
	}
	if _, err = store.BlockFull(context.Background(), block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("partial block full error = %v, want ErrNotFound", err)
	}
}

func TestCheckpointArchiveArtifactsRejectsInvalidBlockDataWithoutMeta(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	block := testServedBlockID(-1, int64(-1<<63), 103, 5)
	err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{{
			ID:    block,
			Block: []byte{0x01},
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "parse block boc") {
		t.Fatalf("save invalid block data err = %v, want parse block boc", err)
	}
	if _, err = store.BlockMeta(context.Background(), block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("invalid block meta err = %v, want ErrNotFound", err)
	}
}

func readNextBlockLink(ctx context.Context, store *Store, prev ton.BlockIDExt) (ton.BlockIDExt, error) {
	meta, err := store.BlockMeta(ctx, prev)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if len(meta.NextRefs) == 0 {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	return meta.NextRefs[0], nil
}

func TestStoredZeroStateBlocks(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if _, err = store.StoredZeroStateBlocks(ctx); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("empty zerostate blocks error = %v, want ErrNotFound", err)
	}

	blockA := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     0,
		RootHash:  bytes.Repeat([]byte{0x11}, 32),
		FileHash:  bytes.Repeat([]byte{0x12}, 32),
	}
	blockB := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     0,
		RootHash:  bytes.Repeat([]byte{0x21}, 32),
		FileHash:  bytes.Repeat([]byte{0x22}, 32),
	}
	if err = store.SaveZeroState(blockA, []byte{0x01}, nil); err != nil {
		t.Fatalf("save zerostate A: %v", err)
	}
	if err = store.SaveZeroState(blockB, []byte{0x02}, nil); err != nil {
		t.Fatalf("save zerostate B: %v", err)
	}

	blocks, err := store.StoredZeroStateBlocks(ctx)
	if err != nil {
		t.Fatalf("stored zerostate blocks: %v", err)
	}
	if !containsBlock(blocks, blockA) || !containsBlock(blocks, blockB) {
		t.Fatalf("stored zerostate blocks = %#v, want both test blocks", blocks)
	}
}

func TestSaveZeroStateRejectsEmptyData(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		RootHash:  bytes.Repeat([]byte{0x11}, 32),
		FileHash:  bytes.Repeat([]byte{0x12}, 32),
	}
	if err = store.SaveZeroState(block, nil, nil); err == nil {
		t.Fatal("empty zerostate data was accepted")
	}
	if _, err = store.ZeroState(context.Background(), block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("zerostate after rejected save error = %v, want ErrNotFound", err)
	}
}

func containsBlock(blocks []ton.BlockIDExt, want ton.BlockIDExt) bool {
	for _, block := range blocks {
		if block.Equals(&want) {
			return true
		}
	}
	return false
}

func TestCheckpointArchiveArtifactsStoresOriginalBlockBOC(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	root := cell.BeginCell().
		MustStoreUInt(0xab, 8).
		MustStoreRef(cell.BeginCell().MustStoreUInt(0xcd, 8).EndCell()).
		EndCell()
	blockData := root.ToBOCWithOptions(cell.BOCSerializeOptions{
		WithCRC32C:    true,
		WithIndex:     true,
		WithCacheBits: true,
		WithTopHash:   true,
		WithIntHashes: true,
	})
	rootHash := root.HashKey()
	fileHash := sha256.Sum256(blockData)
	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     77,
		RootHash:  append([]byte(nil), rootHash[:]...),
		FileHash:  append([]byte(nil), fileHash[:]...),
	}

	err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{{
			ID:    block,
			Block: append([]byte(nil), blockData...),
			Meta:  &storage.BlockMeta{ID: block, GenUTime: 123, StartLT: 10, EndLT: 20},
		}},
	})
	if err != nil {
		t.Fatalf("save block full: %v", err)
	}

	got, err := store.BlockData(context.Background(), block)
	if err != nil {
		t.Fatalf("load block data: %v", err)
	}
	if !bytes.Equal(got, blockData) {
		t.Fatalf("block data mismatch: got %x want %x", got, blockData)
	}

	rawRef, err := store.getHotCopy(context.Background(), hotKeyBlockDataRef(block))
	if err != nil {
		t.Fatalf("load block artifact ref: %v", err)
	}
	ref, err := decodeArtifactRef(rawRef)
	if err != nil {
		t.Fatalf("decode block artifact ref: %v", err)
	}
	packed, err := store.readArtifactRef(context.Background(), ref)
	if err != nil {
		t.Fatalf("read packed block data: %v", err)
	}
	if !bytes.Equal(packed, blockData) {
		t.Fatalf("packed block data mismatch: got %x want %x", packed, blockData)
	}
	refPath, err := store.artifactRefPath(context.Background(), ref)
	if err != nil {
		t.Fatalf("resolve block artifact ref path: %v", err)
	}

	store.artifactMu.Lock()
	_, pending := store.pendingArchiveSync[store.artifactPath(refPath)]
	pendingCount := len(store.pendingArchiveSync)
	store.artifactMu.Unlock()
	if pending {
		t.Fatal("archive block pack is still pending sync after ref commit")
	}
	if pendingCount != 0 {
		t.Fatalf("pending archive sync files = %d, want 0", pendingCount)
	}
	stat, err := os.Stat(store.artifactPath(refPath))
	if err != nil {
		t.Fatalf("stat archive block pack: %v", err)
	}
	if stat.Size() < ref.Offset+ref.Size {
		t.Fatalf("archive block pack size = %d, want at least ref end %d", stat.Size(), ref.Offset+ref.Size)
	}
	if _, err = store.getHotCopy(context.Background(), hotKeyPackAppendDirty(refPath)); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("dirty append marker error = %v, want ErrNotFound", err)
	}
}

func TestArchivePackageMetadataTracksFirstMasterBlock(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()
	metricsObserver := &testArtifactMetricsObserver{}
	store.SetArtifactMetricsObserver(metricsObserver)

	firstSaved := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     150,
		RootHash:  bytes.Repeat([]byte{0x15}, 32),
		FileHash:  bytes.Repeat([]byte{0x16}, 32),
	}
	earlier := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     120,
		RootHash:  bytes.Repeat([]byte{0x12}, 32),
		FileHash:  bytes.Repeat([]byte{0x13}, 32),
	}
	if err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{
			{
				ID:    firstSaved,
				Block: []byte{0x01},
				Meta:  &storage.BlockMeta{ID: firstSaved, GenUTime: 1500, StartLT: 15, EndLT: 16},
			},
			{
				ID:    earlier,
				Block: []byte{0x02},
				Meta:  &storage.BlockMeta{ID: earlier, GenUTime: 1200, StartLT: 12, EndLT: 13},
			},
		},
	}); err != nil {
		t.Fatalf("save blocks: %v", err)
	}

	archiveID, err := store.ArchiveInfo(context.Background(), 120, -1, int64(-1<<63))
	if err != nil {
		t.Fatalf("archive info: %v", err)
	}
	raw, err := store.getHotCopy(context.Background(), hotKeyArchivePackage(archiveID))
	if err != nil {
		t.Fatalf("load archive package meta: %v", err)
	}
	meta, err := decodeArchivePackageMeta(raw)
	if err != nil {
		t.Fatalf("decode archive package meta: %v", err)
	}
	if meta.firstMasterSeq != 120 || meta.firstMasterUTime != 1200 || meta.firstMasterLT != 12 {
		t.Fatalf("unexpected first master metadata seq=%d utime=%d lt=%d", meta.firstMasterSeq, meta.firstMasterUTime, meta.firstMasterLT)
	}
	if meta.startSeq != 100 {
		t.Fatalf("unexpected archive package start seqno %d", meta.startSeq)
	}
	if meta.path == "" || meta.size == 0 {
		t.Fatalf("archive package meta did not record path/size: %+v", meta)
	}
	if metricsObserver.archivePackageBytes != meta.size {
		t.Fatalf("archive package metric bytes = %d, want %d", metricsObserver.archivePackageBytes, meta.size)
	}
}

func TestCheckpointArchiveArtifactsRegistersInlineMasterPackageMetadata(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     150,
		RootHash:  bytes.Repeat([]byte{0x31}, 32),
		FileHash:  bytes.Repeat([]byte{0x32}, 32),
	}

	err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{{
			ID:    block,
			Block: []byte{0x01},
			Proof: []byte{0x02},
			Meta:  &storage.BlockMeta{ID: block, GenUTime: 1500, StartLT: 15, EndLT: 16},
		}},
	})
	if err != nil {
		t.Fatalf("save archive import: %v", err)
	}

	archiveID, err := store.ArchiveInfo(context.Background(), int32(block.SeqNo), -1, int64(-1<<63))
	if err != nil {
		t.Fatalf("archive info: %v", err)
	}
	raw, err := store.getHotCopy(context.Background(), hotKeyArchivePackage(archiveID))
	if err != nil {
		t.Fatalf("reload archive package meta: %v", err)
	}
	meta, err := decodeArchivePackageMeta(raw)
	if err != nil {
		t.Fatalf("decode updated archive package meta: %v", err)
	}
	if meta.firstMasterSeq != block.SeqNo || meta.firstMasterUTime != 1500 || meta.firstMasterLT != 15 {
		t.Fatalf("unexpected archive metadata seq=%d utime=%d lt=%d", meta.firstMasterSeq, meta.firstMasterUTime, meta.firstMasterLT)
	}
}

func TestArchiveSliceSeesArchiveImportAppendAfterMetaCacheWarm(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	first := testServedBlockID(-1, topShard, 150, 0x31)
	second := testServedBlockID(-1, topShard, 151, 0x33)
	if err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{testArchivePackageCacheBlock(first, []byte{0x01}, []byte{0x02})},
	}); err != nil {
		t.Fatalf("save first archive import: %v", err)
	}

	archiveID, oldSize := warmArchivePackageMetaCache(t, store, ctx, first)
	if err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{testArchivePackageCacheBlock(second, []byte{0x03}, []byte{0x04})},
	}); err != nil {
		t.Fatalf("save second archive import: %v", err)
	}
	secondArchiveID, err := store.ArchiveInfo(ctx, int32(second.SeqNo), second.Workchain, second.Shard)
	if err != nil {
		t.Fatalf("second archive info: %v", err)
	}
	if secondArchiveID != archiveID {
		t.Fatalf("second archive id = %d, want %d", secondArchiveID, archiveID)
	}

	tail, err := store.ArchiveSlice(ctx, archiveID, oldSize, 4096)
	if err != nil {
		t.Fatalf("archive slice after append: %v", err)
	}
	if len(tail) == 0 {
		t.Fatal("archive slice returned EOF at old cached package size after append")
	}
}

func TestArchiveSliceSeesCheckpointAppendAfterMetaCacheWarm(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	first := testServedBlockID(-1, topShard, 250, 0x41)
	second := testServedBlockID(-1, topShard, 251, 0x43)
	if _, err = store.SaveStateCheckpointEntries(ctx, []storage.StateCheckpointBlock{{
		Artifact: testArchivePackageCacheBlock(first, []byte{0x11}, []byte{0x12}),
	}}, storage.StateCellRecords{}, nil); err != nil {
		t.Fatalf("save first checkpoint artifact: %v", err)
	}

	archiveID, oldSize := warmArchivePackageMetaCache(t, store, ctx, first)
	if _, err = store.SaveStateCheckpointEntries(ctx, []storage.StateCheckpointBlock{{
		Artifact: testArchivePackageCacheBlock(second, []byte{0x13}, []byte{0x14}),
	}}, storage.StateCellRecords{}, nil); err != nil {
		t.Fatalf("save second checkpoint artifact: %v", err)
	}
	secondArchiveID, err := store.ArchiveInfo(ctx, int32(second.SeqNo), second.Workchain, second.Shard)
	if err != nil {
		t.Fatalf("second checkpoint archive info: %v", err)
	}
	if secondArchiveID != archiveID {
		t.Fatalf("second checkpoint archive id = %d, want %d", secondArchiveID, archiveID)
	}

	tail, err := store.ArchiveSlice(ctx, archiveID, oldSize, 4096)
	if err != nil {
		t.Fatalf("checkpoint archive slice after append: %v", err)
	}
	if len(tail) == 0 {
		t.Fatal("checkpoint archive slice returned EOF at old cached package size after append")
	}
}

func TestCheckpointArchiveArtifactsRegistersInlineShardPackage(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	master := testServedBlockID(-1, topShard, 180, 0x50)
	shard := testServedBlockID(0, int64(0x4000000000000000), 910, 0x51)
	blockData := []byte{0x10, 0x11}
	proofData := []byte{0x20, 0x21}

	if _, err = store.ArchiveInfo(context.Background(), int32(master.SeqNo), shard.Workchain, shard.Shard); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("archive info before import error = %v, want ErrNotFound", err)
	}

	err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{
			{
				ID:    master,
				Block: []byte{0x01},
				Proof: []byte{0x02},
				Meta:  &storage.BlockMeta{ID: master, GenUTime: 1800, StartLT: 17, EndLT: 18},
			},
			{
				ID:    shard,
				Block: blockData,
				Proof: proofData,
				Meta: &storage.BlockMeta{
					ID:                  shard,
					MasterchainRefSeqno: master.SeqNo,
					GenUTime:            1800,
					StartLT:             18,
					EndLT:               19,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("save archive import: %v", err)
	}

	archiveID, err := store.ArchiveInfo(context.Background(), int32(master.SeqNo), shard.Workchain, shard.Shard)
	if err != nil {
		t.Fatalf("archive info after import: %v", err)
	}
	raw, err := store.getHotCopy(context.Background(), hotKeyArchivePackage(archiveID))
	if err != nil {
		t.Fatalf("load archive package meta: %v", err)
	}
	meta, err := decodeArchivePackageMeta(raw)
	if err != nil {
		t.Fatalf("decode archive package meta: %v", err)
	}
	if meta.firstMasterSeq != master.SeqNo || meta.firstMasterUTime != 1800 || meta.firstMasterLT != 18 {
		t.Fatalf("unexpected archive package first master seq=%d utime=%d lt=%d", meta.firstMasterSeq, meta.firstMasterUTime, meta.firstMasterLT)
	}

	store.artifactMu.Lock()
	pendingCount := len(store.pendingArchiveSync)
	store.artifactMu.Unlock()
	if pendingCount != 0 {
		t.Fatalf("pending archive sync files = %d, want 0", pendingCount)
	}
}

func TestArchivePackIDsFollowCppPackageIndexOrder(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	baseMaster := testServedBlockID(-1, topShard, 20000, 0x60)
	if err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{{
			ID:    baseMaster,
			Block: []byte{0x01},
			Proof: []byte{0x02},
			Meta:  &storage.BlockMeta{ID: baseMaster, GenUTime: 20000, StartLT: 20, EndLT: 21},
		}},
	}); err != nil {
		t.Fatalf("save base master package: %v", err)
	}

	baseID, err := store.ArchiveInfo(ctx, int32(baseMaster.SeqNo), -1, topShard)
	if err != nil {
		t.Fatalf("base master archive info: %v", err)
	}
	if archivePackageIndex(baseID) != 0 || uint32(uint64(baseID)) != baseMaster.SeqNo {
		t.Fatalf("base master archive id index/base = %d/%d, want 0/%d", archivePackageIndex(baseID), uint32(uint64(baseID)), baseMaster.SeqNo)
	}

	nextMaster := testServedBlockID(-1, topShard, 20150, 0x61)
	if err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{{
			ID:    nextMaster,
			Block: []byte{0x03},
			Proof: []byte{0x04},
			Meta:  &storage.BlockMeta{ID: nextMaster, GenUTime: 20150, StartLT: 22, EndLT: 23},
		}},
	}); err != nil {
		t.Fatalf("save next master package: %v", err)
	}

	nextID, err := store.ArchiveInfo(ctx, int32(nextMaster.SeqNo), -1, topShard)
	if err != nil {
		t.Fatalf("next master archive info: %v", err)
	}
	if archivePackageIndex(nextID) != 1 || uint32(uint64(nextID)) != baseMaster.SeqNo {
		t.Fatalf("next master archive id index/base = %d/%d, want 1/%d", archivePackageIndex(nextID), uint32(uint64(nextID)), baseMaster.SeqNo)
	}

	shard := testServedBlockID(0, topShard, 1000, 0x62)
	if err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{{
			ID:    shard,
			Block: []byte{0x05},
			Proof: []byte{0x06},
			Meta: &storage.BlockMeta{
				ID:                  shard,
				MasterchainRefSeqno: nextMaster.SeqNo,
				GenUTime:            20150,
				StartLT:             24,
				EndLT:               25,
			},
		}},
	}); err != nil {
		t.Fatalf("save shard package: %v", err)
	}

	shardID, err := store.ArchiveInfo(ctx, int32(nextMaster.SeqNo), shard.Workchain, shard.Shard)
	if err != nil {
		t.Fatalf("shard archive info: %v", err)
	}
	if archivePackageIndex(shardID) != 2 || uint32(uint64(shardID)) != baseMaster.SeqNo {
		t.Fatalf("shard archive id index/base = %d/%d, want 2/%d", archivePackageIndex(shardID), uint32(uint64(shardID)), baseMaster.SeqNo)
	}

	next, err := store.nextArchivePackageIndex(baseMaster.SeqNo)
	if err != nil {
		t.Fatalf("next archive package index: %v", err)
	}
	if next != 3 {
		t.Fatalf("next archive package index = %d, want 3", next)
	}
}

func TestAppendArchiveEntriesAllocatesDynamicPackageIDsWithinCheckpoint(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	master := testServedBlockID(-1, topShard, 20150, 0x70)
	shardA := testServedBlockID(0, int64(0x4000000000000000), 1000, 0x71)
	shardB := testServedBlockID(0, int64(-0x4000000000000000), 1001, 0x72)
	masterMeta := &storage.BlockMeta{
		ID:       master,
		GenUTime: 20150,
		StartLT:  201,
		EndLT:    202,
	}
	shardMeta := func(block ton.BlockIDExt) *storage.BlockMeta {
		return &storage.BlockMeta{
			ID:                  block,
			MasterchainRefSeqno: master.SeqNo,
			GenUTime:            20150,
			StartLT:             uint64(block.SeqNo),
			EndLT:               uint64(block.SeqNo + 1),
		}
	}

	refs, registrations, err := store.appendArchiveEntries([]archiveAppendRequest{
		{kind: packfile.KindBlock, block: master, meta: masterMeta, data: []byte{0x01}},
		{kind: packfile.KindBlock, block: shardA, meta: shardMeta(shardA), data: []byte{0x02}},
		{kind: packfile.KindProof, block: shardA, meta: shardMeta(shardA), data: []byte{0x03}},
		{kind: packfile.KindBlock, block: shardB, meta: shardMeta(shardB), data: []byte{0x04}},
	})
	if err != nil {
		t.Fatalf("append archive entries: %v", err)
	}
	if len(refs) != 4 {
		t.Fatalf("refs = %d, want 4", len(refs))
	}

	wantIndexes := []uint32{1, 2, 2, 3}
	for idx, ref := range refs {
		if ref == nil {
			t.Fatalf("ref %d is nil", idx)
		}
		if got := archivePackageIndex(ref.ArchivePackageID); got != wantIndexes[idx] {
			t.Fatalf("ref %d archive package index = %d, want %d", idx, got, wantIndexes[idx])
		}
		if got := uint32(uint64(ref.ArchivePackageID)); got != 20000 {
			t.Fatalf("ref %d archive package base = %d, want 20000", idx, got)
		}
	}
	if len(registrations) != 3 {
		t.Fatalf("archive package registrations = %d, want 3", len(registrations))
	}
}

func TestAppendArchiveEntriesReusesLoadedPackageIDs(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	master := testServedBlockID(-1, topShard, 20150, 0x73)
	shardA := testServedBlockID(0, int64(0x4000000000000000), 1000, 0x74)
	shardB := testServedBlockID(0, int64(-0x4000000000000000), 1001, 0x75)
	masterMeta := &storage.BlockMeta{
		ID:       master,
		GenUTime: 20150,
		StartLT:  201,
		EndLT:    202,
	}
	shardMeta := func(block ton.BlockIDExt) *storage.BlockMeta {
		return &storage.BlockMeta{
			ID:                  block,
			MasterchainRefSeqno: master.SeqNo,
			GenUTime:            20150,
			StartLT:             uint64(block.SeqNo),
			EndLT:               uint64(block.SeqNo + 1),
		}
	}

	if err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{{
			ID:    master,
			Block: []byte{0x01},
			Proof: []byte{0x02},
			Meta:  masterMeta,
		}},
	}); err != nil {
		t.Fatalf("save master archive: %v", err)
	}
	if err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{{
			ID:    shardA,
			Block: []byte{0x03},
			Proof: []byte{0x04},
			Meta:  shardMeta(shardA),
		}},
	}); err != nil {
		t.Fatalf("save shard archive: %v", err)
	}

	masterID, err := store.ArchiveInfo(ctx, int32(master.SeqNo), master.Workchain, master.Shard)
	if err != nil {
		t.Fatalf("master archive info: %v", err)
	}
	shardAID, err := store.ArchiveInfo(ctx, int32(master.SeqNo), shardA.Workchain, shardA.Shard)
	if err != nil {
		t.Fatalf("shard archive info: %v", err)
	}

	refs, registrations, err := store.appendArchiveEntries([]archiveAppendRequest{
		{kind: packfile.KindBlock, block: master, meta: masterMeta, data: []byte{0x05}},
		{kind: packfile.KindBlock, block: shardA, meta: shardMeta(shardA), data: []byte{0x06}},
		{kind: packfile.KindProof, block: shardA, meta: shardMeta(shardA), data: []byte{0x07}},
		{kind: packfile.KindBlock, block: shardB, meta: shardMeta(shardB), data: []byte{0x08}},
	})
	if err != nil {
		t.Fatalf("append archive entries: %v", err)
	}
	if len(refs) != 4 {
		t.Fatalf("refs = %d, want 4", len(refs))
	}
	if refs[0].ArchivePackageID != masterID {
		t.Fatalf("master archive id = %d, want existing %d", refs[0].ArchivePackageID, masterID)
	}
	if refs[1].ArchivePackageID != shardAID || refs[2].ArchivePackageID != shardAID {
		t.Fatalf("shard archive ids = %d/%d, want existing %d", refs[1].ArchivePackageID, refs[2].ArchivePackageID, shardAID)
	}
	if got := archivePackageIndex(refs[3].ArchivePackageID); got != 3 {
		t.Fatalf("new shard archive package index = %d, want 3", got)
	}
	if got := uint32(uint64(refs[3].ArchivePackageID)); got != 20000 {
		t.Fatalf("new shard archive package base = %d, want 20000", got)
	}
	if len(registrations) != 3 {
		t.Fatalf("archive package registrations = %d, want 3", len(registrations))
	}
}

func TestArchiveInfoUsesKeyBlockPackageBaseInsidePreviousSliceRange(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	oldMaster := testServedBlockID(-1, topShard, 521000, 0x10)
	oldShard := testServedBlockID(0, topShard, 1000, 0x11)
	if err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{
			{
				ID:    oldMaster,
				Block: []byte{0x00},
				Proof: []byte{0x09},
				Meta: &storage.BlockMeta{
					ID:       oldMaster,
					GenUTime: oldMaster.SeqNo,
					StartLT:  0,
					EndLT:    1,
				},
			},
			{
				ID:    oldShard,
				Block: []byte{0x01},
				Proof: []byte{0x02},
				Meta: &storage.BlockMeta{
					ID:                  oldShard,
					MasterchainRefSeqno: oldMaster.SeqNo,
					GenUTime:            oldMaster.SeqNo,
					StartLT:             1,
					EndLT:               2,
				},
			},
		},
	}); err != nil {
		t.Fatalf("save old shard archive: %v", err)
	}

	oldID, err := store.ArchiveInfo(context.Background(), int32(oldMaster.SeqNo), oldShard.Workchain, oldShard.Shard)
	if err != nil {
		t.Fatalf("old archive info: %v", err)
	}

	keyMaster := testServedBlockID(-1, topShard, 521044, 0x20)
	newShard := testServedBlockID(0, topShard, 1001, 0x21)
	if err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{
			{
				ID:    keyMaster,
				Block: []byte{0x03},
				Proof: []byte{0x04},
				Meta: &storage.BlockMeta{
					ID:       keyMaster,
					Flags:    storage.BlockMetaIsKeyBlock,
					GenUTime: keyMaster.SeqNo,
					StartLT:  3,
					EndLT:    4,
				},
			},
			{
				ID:    newShard,
				Block: []byte{0x05},
				Proof: []byte{0x06},
				Meta: &storage.BlockMeta{
					ID:                  newShard,
					MasterchainRefSeqno: keyMaster.SeqNo,
					GenUTime:            keyMaster.SeqNo,
					StartLT:             5,
					EndLT:               6,
				},
			},
		},
	}); err != nil {
		t.Fatalf("save key-base archive: %v", err)
	}

	stillOldID, err := store.ArchiveInfo(context.Background(), int32(keyMaster.SeqNo-1), oldShard.Workchain, oldShard.Shard)
	if err != nil {
		t.Fatalf("old range archive info: %v", err)
	}
	if stillOldID != oldID {
		t.Fatalf("pre-key archive id = %d, want old id %d", stillOldID, oldID)
	}

	newID, err := store.ArchiveInfo(context.Background(), int32(keyMaster.SeqNo), newShard.Workchain, newShard.Shard)
	if err != nil {
		t.Fatalf("new archive info: %v", err)
	}
	if newID == oldID {
		t.Fatalf("key-base archive reused previous range id %d", newID)
	}
	if uint32(uint64(newID)) != keyMaster.SeqNo {
		t.Fatalf("new archive id low bits = %d, want key base %d", uint32(uint64(newID)), keyMaster.SeqNo)
	}

	raw, err := store.getHotCopy(context.Background(), hotKeyArchivePackage(newID))
	if err != nil {
		t.Fatalf("load new archive package meta: %v", err)
	}
	meta, err := decodeArchivePackageMeta(raw)
	if err != nil {
		t.Fatalf("decode new archive package meta: %v", err)
	}
	wantPath := "archive/packages/arch0005/archive.521044.0_8000000000000000.pack"
	if meta.path != wantPath {
		t.Fatalf("new archive package path = %s, want %s", meta.path, wantPath)
	}
}

func TestCheckpointArchiveArtifactsRejectsInlineShardPackageWithoutMasterRef(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	shard := testServedBlockID(0, int64(0x6000000000000000), 920, 0x52)

	err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{{
			ID:    shard,
			Block: []byte{0x30},
			Proof: []byte{0x40},
			Meta: &storage.BlockMeta{
				ID:       shard,
				GenUTime: 1810,
				StartLT:  181,
				EndLT:    182,
			},
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "masterchain reference is required") {
		t.Fatalf("save archive import error = %v, want missing masterchain ref", err)
	}
}

func TestCheckpointArchiveArtifactsKeyBlockProofUsesKeyArchivePack(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     345678,
		RootHash:  bytes.Repeat([]byte{0x03}, 32),
		FileHash:  bytes.Repeat([]byte{0x04}, 32),
	}
	blockData := []byte{0x11, 0x22, 0x33}
	proofData := []byte{0x44, 0x55, 0x66}

	if err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{{
			ID:    block,
			Block: blockData,
			Proof: proofData,
			Meta: &storage.BlockMeta{
				ID:       block,
				Flags:    storage.BlockMetaIsKeyBlock,
				GenUTime: 3456780,
				StartLT:  345678,
				EndLT:    345679,
			},
		}},
	}); err != nil {
		t.Fatalf("save key proof: %v", err)
	}

	gotProof, err := store.BlockProof(context.Background(), storage.ServedProofKeyBlock, block)
	if err != nil {
		t.Fatalf("load key proof: %v", err)
	}
	if !bytes.Equal(gotProof, proofData) {
		t.Fatalf("key proof data mismatch: got %x want %x", gotProof, proofData)
	}

	keyPack := filepath.Join(dir, "archive", "packages", "key000", "key.archive.200000.pack")
	if _, err = os.Stat(keyPack); err != nil {
		t.Fatalf("key pack was not created: %v", err)
	}
	if _, err = store.getHotCopy(context.Background(), hotKeyProofRef(storage.ServedProofKeyBlock, block)); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("legacy proof ref error = %v, want ErrNotFound", err)
	}
	if _, err = store.getHotCopy(context.Background(), hotKeyKeyProofRef(storage.ServedProofKeyBlock, block)); err != nil {
		t.Fatalf("key proof ref missing: %v", err)
	}
	meta, err := store.BlockMeta(context.Background(), block)
	if err != nil {
		t.Fatalf("load block meta: %v", err)
	}
	if !meta.Has(storage.BlockMetaIsKeyBlock) {
		t.Fatal("key proof did not mark block meta as key block")
	}
}

func TestPendingKeyProofPackKeepsFirstCleanSize(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	committed := testServedBlockID(-1, topShard, 345678, 0x34)
	if err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{{
			ID:    committed,
			Block: []byte{0x11},
			Proof: []byte{0x21},
			Meta: &storage.BlockMeta{
				ID:       committed,
				Flags:    storage.BlockMetaIsKeyBlock,
				GenUTime: 3456780,
				StartLT:  345678,
				EndLT:    345679,
			},
		}},
	}); err != nil {
		t.Fatalf("save committed key proof: %v", err)
	}

	path := store.keyBlockProofPackPath(committed.SeqNo)
	stat, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat committed key proof pack: %v", err)
	}
	publishedSize := stat.Size()

	pendingA := testServedBlockID(-1, topShard, 345679, 0x35)
	if _, err = store.appendKeyBlockProofEntry(storage.ServedProofKeyBlock, pendingA, []byte{0x31}); err != nil {
		t.Fatalf("append first pending key proof: %v", err)
	}
	pendingB := testServedBlockID(-1, topShard, 345680, 0x36)
	if _, err = store.appendKeyBlockProofEntry(storage.ServedProofKeyBlock, pendingB, []byte{0x41}); err != nil {
		t.Fatalf("append second pending key proof: %v", err)
	}

	pending, ok := store.pendingKeyProofSync[path]
	if !ok {
		t.Fatalf("missing pending key proof sync for %s", path)
	}
	if pending.cleanSize != publishedSize {
		t.Fatalf("pending clean size = %d, want published size %d", pending.cleanSize, publishedSize)
	}
	relPath, err := store.packJournalPath(path)
	if err != nil {
		t.Fatalf("pack journal path: %v", err)
	}
	raw, err := store.getHotCopy(context.Background(), hotKeyPackAppendDirty(relPath))
	if err != nil {
		t.Fatalf("load dirty append marker: %v", err)
	}
	markerCleanSize, err := decodePackAppendDirty(raw)
	if err != nil {
		t.Fatalf("decode dirty append marker: %v", err)
	}
	if markerCleanSize != publishedSize {
		t.Fatalf("dirty marker clean size = %d, want published size %d", markerCleanSize, publishedSize)
	}

	store.abandonPendingArtifactPacks()

	stat, err = os.Stat(path)
	if err != nil {
		t.Fatalf("stat abandoned key proof pack: %v", err)
	}
	if stat.Size() != publishedSize {
		t.Fatalf("key proof pack size after abandon = %d, want %d", stat.Size(), publishedSize)
	}
	if _, err = store.getHotCopy(context.Background(), hotKeyPackAppendDirty(relPath)); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("dirty append marker after abandon = %v, want ErrNotFound", err)
	}
}

func TestCheckpointArchiveRegistrationsUsePendingKeyBlockPackageStart(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	key := testServedBlockID(-1, topShard, 123, 0x41)
	next := testServedBlockID(-1, topShard, 124, 0x42)
	entries := []storage.StateCheckpointBlock{
		{
			Artifact: &storage.ServedBlockFull{
				ID:    key,
				Block: []byte{0x01},
				Proof: []byte{0x02},
				Meta: &storage.BlockMeta{
					ID:       key,
					Flags:    storage.BlockMetaIsKeyBlock,
					GenUTime: 1230,
					StartLT:  123,
					EndLT:    124,
				},
			},
		},
		{
			Artifact: &storage.ServedBlockFull{
				ID:    next,
				Block: []byte{0x03},
				Proof: []byte{0x04},
				Meta: &storage.BlockMeta{
					ID:       next,
					GenUTime: 1240,
					StartLT:  124,
					EndLT:    125,
				},
			},
		},
	}

	_, registrations, _, err := store.prepareCheckpointArtifactWrites(entries)
	if err != nil {
		t.Fatalf("prepare checkpoint artifacts: %v", err)
	}

	var masterRegs []archivePackRegistration
	for _, reg := range registrations {
		if reg.workchain == -1 && reg.shard == topShard {
			masterRegs = append(masterRegs, reg)
		}
	}
	if len(masterRegs) != 1 {
		t.Fatalf("master archive registrations = %+v, want one key package registration", masterRegs)
	}
	reg := masterRegs[0]
	if reg.archiveID != int64(key.SeqNo) || reg.baseSeq != key.SeqNo || reg.startSeq != key.SeqNo {
		t.Fatalf("master archive registration = %+v, want key package base/start %d", reg, key.SeqNo)
	}
}

func TestCheckpointArchiveAppendUsesStateMasterchainRefForShardArtifact(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	master := testServedBlockID(-1, topShard, 20150, 0x4a)
	shard := testServedBlockID(0, int64(0x4000000000000000), 1000, 0x4b)
	entries := []storage.StateCheckpointBlock{{
		State: &storage.BlockState{
			Block:          shard,
			MasterchainRef: &master,
			StateRootHash:  bytes.Repeat([]byte{0x4c}, 32),
		},
		Artifact: &storage.ServedBlockFull{
			ID:    shard,
			Block: []byte{0x01},
			Proof: []byte{0x02},
			Meta: &storage.BlockMeta{
				ID:       shard,
				GenUTime: 20150,
				StartLT:  uint64(shard.SeqNo),
				EndLT:    uint64(shard.SeqNo + 1),
			},
		},
	}}

	_, registrations, _, err := store.prepareCheckpointArtifactWrites(entries)
	if err != nil {
		t.Fatalf("prepare checkpoint artifacts: %v", err)
	}
	if len(registrations) != 1 {
		t.Fatalf("archive registrations = %d, want 1", len(registrations))
	}
	reg := registrations[0]
	if reg.workchain != shard.Workchain || reg.shard != shard.Shard {
		t.Fatalf("archive registration shard = %d/%016x, want %d/%016x", reg.workchain, uint64(reg.shard), shard.Workchain, uint64(shard.Shard))
	}
	if reg.baseSeq != 20000 || reg.startSeq != 20100 {
		t.Fatalf("archive registration base/start = %d/%d, want 20000/20100", reg.baseSeq, reg.startSeq)
	}
}

func TestCheckpointArchiveArtifactsReplacesMissingArtifactRefs(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     99,
		RootHash:  bytes.Repeat([]byte{0x01}, 32),
		FileHash:  bytes.Repeat([]byte{0x02}, 32),
	}
	missing := encodeArtifactRef(&storage.ArtifactRef{
		ArchivePackage:   true,
		ArchivePackageID: 1234,
		Offset:           10,
		Size:             4,
	})
	if err = store.withHotBatch(func(batch *pebble.Batch) error {
		if err := batch.Set(hotKeyBlockDataRef(block), missing, pebble.NoSync); err != nil {
			return err
		}
		return batch.Set(hotKeyStoredProofRef(storage.ServedProofBlock, block), missing, pebble.NoSync)
	}); err != nil {
		t.Fatalf("save stale refs: %v", err)
	}

	if _, err = store.BlockData(context.Background(), block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("block data with missing ref error = %v, want ErrNotFound", err)
	}
	if _, err = store.BlockProof(context.Background(), storage.ServedProofBlock, block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("block proof with missing ref error = %v, want ErrNotFound", err)
	}

	blockData := []byte{0x11, 0x22, 0x33}
	proofData := []byte{0x44, 0x55, 0x66}
	if err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{{
			ID:    block,
			Block: blockData,
			Proof: proofData,
			Meta:  &storage.BlockMeta{ID: block, GenUTime: 123, StartLT: 10, EndLT: 20},
		}},
	}); err != nil {
		t.Fatalf("save block full: %v", err)
	}

	gotBlock, err := store.BlockData(context.Background(), block)
	if err != nil {
		t.Fatalf("load replaced block data: %v", err)
	}
	if !bytes.Equal(gotBlock, blockData) {
		t.Fatalf("replaced block data = %x, want %x", gotBlock, blockData)
	}
	gotProof, err := store.BlockProof(context.Background(), storage.ServedProofBlock, block)
	if err != nil {
		t.Fatalf("load replaced proof: %v", err)
	}
	if !bytes.Equal(gotProof, proofData) {
		t.Fatalf("replaced proof = %x, want %x", gotProof, proofData)
	}
}

func TestArchiveInfoNormalizesSplitShardPrefix(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	splitShard := int64(-8646911284551352320) // 0x8800000000000000
	master := testServedBlockID(-1, topShard, 42, 0x10)
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     splitShard,
		SeqNo:     42,
		RootHash:  bytes.Repeat([]byte{0x01}, 32),
		FileHash:  bytes.Repeat([]byte{0x02}, 32),
	}
	if err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{
			{
				ID:    master,
				Block: []byte{0},
				Proof: []byte{3},
				Meta:  &storage.BlockMeta{ID: master, GenUTime: 420, StartLT: 41, EndLT: 42},
			},
			{
				ID:    block,
				Block: []byte{1},
				Proof: []byte{2},
				Meta:  &storage.BlockMeta{ID: block, MasterchainRefSeqno: master.SeqNo, GenUTime: 420, StartLT: 42, EndLT: 43},
			},
		},
	}); err != nil {
		t.Fatalf("save archive import: %v", err)
	}

	exact, err := store.ArchiveInfo(context.Background(), 42, 0, splitShard)
	if err != nil {
		t.Fatalf("exact archive info: %v", err)
	}
	normalized, err := store.ArchiveInfo(context.Background(), 42, 0, int64(-1<<63))
	if err != nil {
		t.Fatalf("normalized archive info: %v", err)
	}
	if normalized != exact {
		t.Fatalf("normalized archive id = %d, want %d", normalized, exact)
	}
}

func TestArchivePackIDSeparatesBasechainFullAndSplitShard(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	master := testServedBlockID(-1, topShard, 156, 0x10)
	full := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(-1 << 63),
		SeqNo:     156,
		RootHash:  bytes.Repeat([]byte{0x11}, 32),
		FileHash:  bytes.Repeat([]byte{0x12}, 32),
	}
	if err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{{
			ID:    master,
			Block: []byte{0},
			Proof: []byte{5},
			Meta:  &storage.BlockMeta{ID: master, GenUTime: 1560, StartLT: 155, EndLT: 156},
		}},
	}); err != nil {
		t.Fatalf("save master archive: %v", err)
	}
	if err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{{
			ID:    full,
			Block: []byte{1},
			Proof: []byte{2},
			Meta:  &storage.BlockMeta{ID: full, MasterchainRefSeqno: master.SeqNo, GenUTime: 1560, StartLT: 156, EndLT: 157},
		}},
	}); err != nil {
		t.Fatalf("save full basechain archive: %v", err)
	}

	splitShard := int64(0x4000000000000000)
	split := ton.BlockIDExt{
		Workchain: 0,
		Shard:     splitShard,
		SeqNo:     156,
		RootHash:  bytes.Repeat([]byte{0x21}, 32),
		FileHash:  bytes.Repeat([]byte{0x22}, 32),
	}
	if err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{{
			ID:    split,
			Block: []byte{3},
			Proof: []byte{4},
			Meta:  &storage.BlockMeta{ID: split, MasterchainRefSeqno: master.SeqNo, GenUTime: 1560, StartLT: 158, EndLT: 159},
		}},
	}); err != nil {
		t.Fatalf("save split basechain archive: %v", err)
	}

	fullID, err := store.ArchiveInfo(context.Background(), 156, 0, int64(-1<<63))
	if err != nil {
		t.Fatalf("full archive info: %v", err)
	}
	splitID, err := store.ArchiveInfo(context.Background(), 156, 0, splitShard)
	if err != nil {
		t.Fatalf("split archive info: %v", err)
	}
	if fullID == splitID {
		t.Fatalf("archive ids collided: full=%d split=%d", fullID, splitID)
	}
}

func TestArchivePackIDSeparatesDeepShardPrefixes(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	master := testServedBlockID(-1, topShard, 3085024, 0x30)
	shardA := testShardID(0xb684000000000000)
	shardB := testShardID(0xb68c000000000000)
	if err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{{
			ID:    master,
			Block: []byte{0x01},
			Proof: []byte{0x02},
			Meta:  &storage.BlockMeta{ID: master, GenUTime: 3085024, StartLT: 99, EndLT: 100},
		}},
	}); err != nil {
		t.Fatalf("save master archive: %v", err)
	}

	for idx, shard := range []int64{shardA, shardB} {
		block := ton.BlockIDExt{
			Workchain: 0,
			Shard:     shard,
			SeqNo:     uint32(100 + idx),
			RootHash:  bytes.Repeat([]byte{byte(0x40 + idx)}, 32),
			FileHash:  bytes.Repeat([]byte{byte(0x50 + idx)}, 32),
		}
		if err = saveTestArchiveArtifacts(store, testArchiveImport{
			FullBlocks: []*storage.ServedBlockFull{{
				ID:    block,
				Block: []byte{byte(0x60 + idx)},
				Proof: []byte{byte(0x70 + idx)},
				Meta: &storage.BlockMeta{
					ID:                  block,
					MasterchainRefSeqno: master.SeqNo,
					GenUTime:            3085024,
					StartLT:             uint64(100 + idx),
					EndLT:               uint64(101 + idx),
				},
			}},
		}); err != nil {
			t.Fatalf("save shard %016x archive: %v", uint64(shard), err)
		}
	}

	idA, err := store.ArchiveInfo(context.Background(), int32(master.SeqNo), 0, shardA)
	if err != nil {
		t.Fatalf("shard A archive info: %v", err)
	}
	idB, err := store.ArchiveInfo(context.Background(), int32(master.SeqNo), 0, shardB)
	if err != nil {
		t.Fatalf("shard B archive info: %v", err)
	}
	if idA == idB {
		t.Fatalf("deep shard archive ids collided: shard_a=%016x shard_b=%016x id=%d", uint64(shardA), uint64(shardB), idA)
	}
}

func TestPruneArchivePackagesDeletesOldPackagesAfterBoundary(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	oldA := testArchivePruneBlock(10, 0x10)
	oldB := testArchivePruneBlock(150, 0x20)
	boundary := testArchivePruneBlock(220, 0x30)
	newer := testArchivePruneBlock(320, 0x40)

	savePruneBlock := func(block ton.BlockIDExt, utime uint32) string {
		t.Helper()
		if err := saveTestArchiveArtifacts(store, testArchiveImport{
			FullBlocks: []*storage.ServedBlockFull{{
				ID:    block,
				Block: []byte{byte(block.SeqNo)},
				Meta:  &storage.BlockMeta{ID: block, GenUTime: utime, StartLT: uint64(block.SeqNo), EndLT: uint64(block.SeqNo + 1)},
			}},
		}); err != nil {
			t.Fatalf("save block %d: %v", block.SeqNo, err)
		}
		raw, err := store.getHotCopy(context.Background(), hotKeyBlockDataRef(block))
		if err != nil {
			t.Fatalf("load block ref %d: %v", block.SeqNo, err)
		}
		ref, err := decodeArtifactRef(raw)
		if err != nil {
			t.Fatalf("decode block ref %d: %v", block.SeqNo, err)
		}
		path, err := store.artifactRefPath(context.Background(), ref)
		if err != nil {
			t.Fatalf("resolve block ref path %d: %v", block.SeqNo, err)
		}
		return store.artifactPath(path)
	}

	oldAPath := savePruneBlock(oldA, 1000)
	oldBPath := savePruneBlock(oldB, 1500)
	boundaryPath := savePruneBlock(boundary, 2000)
	newerPath := savePruneBlock(newer, 3000)
	oldShard := testServedBlockID(0, int64(-1<<63), 20, 0x55)
	if err = store.SaveBlockMeta(&storage.BlockMeta{ID: oldShard, GenUTime: 1000, StartLT: 20, EndLT: 21}); err != nil {
		t.Fatalf("save old shard meta: %v", err)
	}
	wantDeletedBytes := testFileSize(t, oldAPath) + testFileSize(t, oldBPath)

	stats, err := store.PruneArchivePackages(context.Background(), 2500, 0)
	if err != nil {
		t.Fatalf("prune archive packages: %v", err)
	}
	if stats.DeletedBeforeSeqno != 200 {
		t.Fatalf("deleted before seqno = %d, want 200", stats.DeletedBeforeSeqno)
	}
	if stats.RetainedBoundarySeqno != 200 {
		t.Fatalf("retained boundary seqno = %d, want 200", stats.RetainedBoundarySeqno)
	}
	if stats.DeletedPackages != 2 || stats.DeletedPackageFiles != 2 {
		t.Fatalf("deleted packages/files = %d/%d, want 2/2", stats.DeletedPackages, stats.DeletedPackageFiles)
	}
	if stats.DeletedPackageBytes != wantDeletedBytes {
		t.Fatalf("deleted package bytes = %d, want %d", stats.DeletedPackageBytes, wantDeletedBytes)
	}
	if stats.DeletedBlockMeta != 3 {
		t.Fatalf("deleted block meta = %d, want 3", stats.DeletedBlockMeta)
	}

	for _, block := range []ton.BlockIDExt{oldA, oldB} {
		if _, err = store.BlockMeta(context.Background(), block); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("old block meta %d error = %v, want ErrNotFound", block.SeqNo, err)
		}
		if _, err = store.BlockData(context.Background(), block); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("old block data %d error = %v, want ErrNotFound", block.SeqNo, err)
		}
		if _, err = store.ArchiveInfo(context.Background(), int32(block.SeqNo), -1, int64(-1<<63)); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("old archive info %d error = %v, want ErrNotFound", block.SeqNo, err)
		}
	}
	if _, err = store.BlockMeta(context.Background(), oldShard); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("old shard meta error = %v, want ErrNotFound", err)
	}

	for _, block := range []ton.BlockIDExt{boundary, newer} {
		if _, err = store.BlockMeta(context.Background(), block); err != nil {
			t.Fatalf("kept block meta %d: %v", block.SeqNo, err)
		}
		if _, err = store.BlockData(context.Background(), block); err != nil {
			t.Fatalf("kept block data %d: %v", block.SeqNo, err)
		}
		if _, err = store.ArchiveInfo(context.Background(), int32(block.SeqNo), -1, int64(-1<<63)); err != nil {
			t.Fatalf("kept archive info %d: %v", block.SeqNo, err)
		}
	}

	for _, path := range []string{oldAPath, oldBPath} {
		if _, err = os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("old archive pack %s stat error = %v, want missing", path, err)
		}
	}
	for _, path := range []string{boundaryPath, newerPath} {
		if _, err = os.Stat(path); err != nil {
			t.Fatalf("kept archive pack %s: %v", path, err)
		}
	}
}

func TestPruneArchivePackagesRetainsPersistentStateTTLMetadata(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	const secondsPerDay = uint64(24 * 60 * 60)

	ctx := context.Background()
	snapshotUTime := uint32(1 << 19)
	nowUnix := uint64(snapshotUTime) + 8*secondsPerDay
	cutoffUnix := uint32(nowUnix - 7*secondsPerDay)
	if ttl := persistentStateFileTTL(snapshotUTime); ttl <= nowUnix {
		t.Fatalf("persistent state ttl = %d, want after now %d", ttl, nowUnix)
	}

	snapshot := testArchivePruneBlock(10, 0x10)
	old := testArchivePruneBlock(150, 0x20)
	boundary := testArchivePruneBlock(220, 0x30)
	newer := testArchivePruneBlock(320, 0x40)
	stateBlock := testServedBlockID(0, int64(-1<<63), 77, 0x50)
	staleStateBlock := testServedBlockID(0, int64(0x4000000000000000), 78, 0x60)

	saveArchivePruneBlock(t, store, snapshot, snapshotUTime, storage.BlockMetaIsKeyBlock, []byte{0x91})
	saveArchivePruneBlock(t, store, old, snapshotUTime+uint32(secondsPerDay/2), 0, nil)
	saveArchivePruneBlock(t, store, boundary, cutoffUnix-1, 0, nil)
	saveArchivePruneBlock(t, store, newer, cutoffUnix+1, 0, nil)
	if err = store.SaveBlockMeta(&storage.BlockMeta{
		ID:                  stateBlock,
		GenUTime:            snapshotUTime,
		StartLT:             uint64(stateBlock.SeqNo),
		EndLT:               uint64(stateBlock.SeqNo + 1),
		MasterchainRefSeqno: snapshot.SeqNo,
	}); err != nil {
		t.Fatalf("save basechain state block meta: %v", err)
	}
	if err = store.SaveBlockMeta(&storage.BlockMeta{
		ID:                  staleStateBlock,
		Flags:               storage.BlockMetaHasStateSnapshot | storage.BlockMetaHasStateCells,
		GenUTime:            snapshotUTime,
		StartLT:             uint64(staleStateBlock.SeqNo),
		EndLT:               uint64(staleStateBlock.SeqNo + 1),
		StateRootHash:       bytes.Repeat([]byte{0x61}, 32),
		MasterchainRefSeqno: snapshot.SeqNo,
	}); err != nil {
		t.Fatalf("save stale checkpoint state meta: %v", err)
	}

	snapshotPath := saveTestPersistentStatePruneFileForBlock(t, store, stateBlock, snapshot)
	saveTestPersistentStatePruneFile(t, store, boundary)
	saveTestPersistentStatePruneFile(t, store, newer)
	masterMeta, err := store.BlockMeta(ctx, snapshot)
	if err != nil {
		t.Fatalf("load master meta before archive prune: %v", err)
	}
	if masterMeta.Has(storage.BlockMetaHasStateSnapshot) {
		t.Fatal("basechain-only persistent state unexpectedly marked master meta")
	}

	stats, err := store.PruneArchivePackages(ctx, cutoffUnix, 0)
	if err != nil {
		t.Fatalf("prune archive packages: %v", err)
	}
	if stats.DeletedBeforeSeqno != 210 {
		t.Fatalf("deleted before seqno = %d, want 210", stats.DeletedBeforeSeqno)
	}

	masterMeta, err = store.BlockMeta(ctx, snapshot)
	if err != nil {
		t.Fatalf("load retained master ttl meta: %v", err)
	}
	if !isArchivePrunedSnapshotMeta(masterMeta) || len(masterMeta.StateRootHash) != 0 {
		t.Fatalf("retained master ttl meta = %+v, want compact group marker", masterMeta)
	}
	if masterMeta.GenUTime != snapshotUTime {
		t.Fatalf("retained master gen_utime = %d, want %d", masterMeta.GenUTime, snapshotUTime)
	}
	stateMeta, err := store.BlockMeta(ctx, stateBlock)
	if err != nil {
		t.Fatalf("load retained basechain state meta: %v", err)
	}
	wantStateRootHash := bytes.Repeat([]byte{byte(stateBlock.SeqNo + 1)}, 32)
	if !isArchivePrunedSnapshotMeta(stateMeta) || !bytes.Equal(stateMeta.StateRootHash, wantStateRootHash) {
		t.Fatalf("retained basechain state meta = %+v, want compact state-root marker", stateMeta)
	}
	if _, err = store.BlockMeta(ctx, staleStateBlock); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("stale checkpoint state meta error = %v, want ErrNotFound", err)
	}
	if _, err = store.BlockData(ctx, snapshot); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("pruned snapshot block data error = %v, want ErrNotFound", err)
	}
	if _, err = store.BlockProof(ctx, storage.ServedProofBlock, snapshot); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("pruned snapshot block proof error = %v, want ErrNotFound", err)
	}
	assertArchiveIndexesMissing := func() {
		t.Helper()

		for label, key := range map[string][]byte{
			"master seqno": hotKeyBlockSeqIndex(storage.BlockSeqRefFromBlock(snapshot)),
			"master lt":    hotKeyBlockLTIndex(storage.BlockSeqRefFromBlock(snapshot), uint64(snapshot.SeqNo+1)),
			"master utime": hotKeyBlockUTimeIndex(storage.BlockSeqRefFromBlock(snapshot), snapshotUTime),
			"key block":    hotKeyKeyBlockSeqIndex(snapshot.SeqNo),
			"state seqno":  hotKeyBlockSeqIndex(storage.BlockSeqRefFromBlock(stateBlock)),
			"state lt":     hotKeyBlockLTIndex(storage.BlockSeqRefFromBlock(stateBlock), uint64(stateBlock.SeqNo+1)),
			"state utime":  hotKeyBlockUTimeIndex(storage.BlockSeqRefFromBlock(stateBlock), snapshotUTime),
		} {
			if _, indexErr := store.getHotCopy(ctx, key); !errors.Is(indexErr, storage.ErrNotFound) {
				t.Fatalf("pruned snapshot %s index error = %v, want ErrNotFound", label, indexErr)
			}
		}
	}
	assertArchiveIndexesMissing()

	resavedPath := saveTestPersistentStatePruneFileForBlock(t, store, stateBlock, snapshot)
	if resavedPath != snapshotPath {
		t.Fatalf("re-saved persistent state path = %s, want %s", resavedPath, snapshotPath)
	}
	stateMeta, err = store.BlockMeta(ctx, stateBlock)
	if err != nil {
		t.Fatalf("load re-saved snapshot meta: %v", err)
	}
	if !isArchivePrunedSnapshotMeta(stateMeta) || !bytes.Equal(stateMeta.StateRootHash, wantStateRootHash) {
		t.Fatalf("re-saved snapshot meta = %+v, want compact state-root marker", stateMeta)
	}
	if stateMeta.GenUTime != snapshotUTime {
		t.Fatalf("re-saved snapshot gen_utime = %d, want %d", stateMeta.GenUTime, snapshotUTime)
	}
	file, err := store.PersistentStateFile(ctx, stateBlock, snapshot, 0)
	if err != nil {
		t.Fatalf("load re-saved persistent state file: %v", err)
	}
	if !bytes.Equal(file.FileHash, bytes.Repeat([]byte{byte(stateBlock.SeqNo)}, 32)) ||
		!bytes.Equal(file.StateRootHash, wantStateRootHash) {
		t.Fatalf("re-saved persistent state hashes = file:%x root:%x", file.FileHash, file.StateRootHash)
	}
	assertArchiveIndexesMissing()

	stateStats, err := store.PruneExpiredPersistentStateFiles(ctx, nowUnix, 2, 0)
	if err != nil {
		t.Fatalf("prune unexpired persistent state: %v", err)
	}
	if stateStats.DeletedFileRecords != 0 || stateStats.DeletedDiskFiles != 0 {
		t.Fatalf("unexpired persistent state deleted records/files = %d/%d, want 0/0", stateStats.DeletedFileRecords, stateStats.DeletedDiskFiles)
	}
	assertTestPersistentStatePresentForBlock(t, store, stateBlock, snapshot, snapshotPath)

	expiredNowUnix := persistentStateFileTTL(snapshotUTime) + 1
	stateStats, err = store.PruneExpiredPersistentStateFiles(ctx, expiredNowUnix, 2, 0)
	if err != nil {
		t.Fatalf("prune expired persistent state: %v", err)
	}
	if stateStats.DeletedFileRecords != 1 || stateStats.DeletedDiskFiles != 1 {
		t.Fatalf("expired persistent state deleted records/files = %d/%d, want 1/1", stateStats.DeletedFileRecords, stateStats.DeletedDiskFiles)
	}
	assertTestPersistentStatePrunedForBlock(t, store, stateBlock, snapshot, snapshotPath)
	if _, err = store.BlockMeta(ctx, stateBlock); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expired basechain state compact meta error = %v, want ErrNotFound", err)
	}
	assertArchiveIndexesMissing()
	if _, err = store.BlockMeta(ctx, snapshot); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expired master ttl marker error = %v, want ErrNotFound", err)
	}
}

func TestPruneArchivePackagesKeepsKeyProofPackForPrunedKeyBlock(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	oldKey := testArchivePruneBlock(10, 0x10)
	oldB := testArchivePruneBlock(150, 0x20)
	boundary := testArchivePruneBlock(220, 0x30)
	newer := testArchivePruneBlock(320, 0x40)

	oldKeyPath := saveArchivePruneBlock(t, store, oldKey, 1000, storage.BlockMetaIsKeyBlock, []byte{0x91})
	oldBPath := saveArchivePruneBlock(t, store, oldB, 1500, 0, nil)
	saveArchivePruneBlock(t, store, boundary, 2000, 0, nil)
	saveArchivePruneBlock(t, store, newer, 3000, 0, nil)
	keyPackPath := store.keyBlockProofPackPath(oldKey.SeqNo)
	wantDeletedBytes := testFileSize(t, oldKeyPath) + testFileSize(t, oldBPath)

	stats, err := store.PruneArchivePackages(context.Background(), 2500, 0)
	if err != nil {
		t.Fatalf("prune archive packages: %v", err)
	}
	if stats.DeletedPackageFiles != 2 {
		t.Fatalf("deleted package files = %d, want 2", stats.DeletedPackageFiles)
	}
	if stats.DeletedPackageBytes != wantDeletedBytes {
		t.Fatalf("deleted package bytes = %d, want %d", stats.DeletedPackageBytes, wantDeletedBytes)
	}
	if _, err = os.Stat(keyPackPath); err != nil {
		t.Fatalf("key proof pack missing after prune: %v", err)
	}
	gotProof, err := store.BlockProof(context.Background(), storage.ServedProofKeyBlock, oldKey)
	if err != nil {
		t.Fatalf("load pruned key block proof: %v", err)
	}
	if !bytes.Equal(gotProof, []byte{0x91}) {
		t.Fatalf("pruned key block proof data = %x, want 91", gotProof)
	}
}

func TestPruneArchivePackagesKeepsKeyProofPackWithRetainedRef(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	oldKey := testArchivePruneBlock(10, 0x10)
	oldB := testArchivePruneBlock(150, 0x20)
	retainedKey := testArchivePruneBlock(220, 0x30)
	newer := testArchivePruneBlock(320, 0x40)

	oldKeyPath := saveArchivePruneBlock(t, store, oldKey, 1000, storage.BlockMetaIsKeyBlock, []byte{0x91})
	oldBPath := saveArchivePruneBlock(t, store, oldB, 1500, 0, nil)
	saveArchivePruneBlock(t, store, retainedKey, 2000, storage.BlockMetaIsKeyBlock, []byte{0x92})
	saveArchivePruneBlock(t, store, newer, 3000, 0, nil)
	keyPackPath := store.keyBlockProofPackPath(oldKey.SeqNo)
	wantDeletedBytes := testFileSize(t, oldKeyPath) + testFileSize(t, oldBPath)

	stats, err := store.PruneArchivePackages(context.Background(), 2500, 0)
	if err != nil {
		t.Fatalf("prune archive packages: %v", err)
	}
	if stats.DeletedPackageFiles != 2 {
		t.Fatalf("deleted package files = %d, want 2", stats.DeletedPackageFiles)
	}
	if stats.DeletedPackageBytes != wantDeletedBytes {
		t.Fatalf("deleted package bytes = %d, want %d", stats.DeletedPackageBytes, wantDeletedBytes)
	}
	if _, err = os.Stat(keyPackPath); err != nil {
		t.Fatalf("retained key proof pack missing: %v", err)
	}
	gotOldProof, err := store.BlockProof(context.Background(), storage.ServedProofKeyBlock, oldKey)
	if err != nil {
		t.Fatalf("load pruned key proof: %v", err)
	}
	if !bytes.Equal(gotOldProof, []byte{0x91}) {
		t.Fatalf("pruned key proof data = %x, want 91", gotOldProof)
	}
	gotProof, err := store.BlockProof(context.Background(), storage.ServedProofKeyBlock, retainedKey)
	if err != nil {
		t.Fatalf("load retained key proof: %v", err)
	}
	if !bytes.Equal(gotProof, []byte{0x92}) {
		t.Fatalf("retained key proof data = %x, want 92", gotProof)
	}
}

func TestOpenRemovesDeletePendingArchivePack(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	strayPath := filepath.Join(dir, "archive", "packages", "arch0000", "stray.pack")
	if err := os.MkdirAll(filepath.Dir(strayPath), 0o755); err != nil {
		t.Fatalf("create stray pack dir: %v", err)
	}
	if err := os.WriteFile(strayPath, []byte{0x01, 0x02, 0x03}, 0o644); err != nil {
		t.Fatalf("write stray pack: %v", err)
	}
	relPath, err := store.packJournalPath(strayPath)
	if err != nil {
		t.Fatalf("pack journal path: %v", err)
	}
	if err = store.withHotBatchOptions(pebble.Sync, func(batch *pebble.Batch) error {
		return store.setPackDeletePending(batch, strayPath)
	}); err != nil {
		t.Fatalf("set delete pending: %v", err)
	}
	if err = store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	store, err = Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer func() { _ = store.Close() }()

	if _, err = os.Stat(strayPath); !os.IsNotExist(err) {
		t.Fatalf("stray archive pack stat error = %v, want missing", err)
	}
	if _, err = store.getHotCopy(context.Background(), hotKeyPackDeletePending(relPath)); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("delete pending marker after recovery = %v, want ErrNotFound", err)
	}
}

func TestDeleteArchivePackageRecordsSkipsDirtyPack(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	block := testArchivePruneBlock(62, 0x62)
	path := saveArchivePruneBlock(t, store, block, 6200, 0, nil)
	stat, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat archive pack: %v", err)
	}
	if _, err = store.markPackAppendDirty(path, stat.Size()); err != nil {
		t.Fatalf("mark dirty append: %v", err)
	}
	store.artifactMu.Lock()
	store.markPendingPackSync(store.pendingArchiveSync, path, stat.Size(), stat.Size(), stat.Size())

	archiveID, err := store.ArchiveInfo(ctx, int32(block.SeqNo), block.Workchain, block.Shard)
	if err != nil {
		store.artifactMu.Unlock()
		t.Fatalf("archive info: %v", err)
	}
	raw, err := store.getHotCopy(ctx, hotKeyArchivePackage(archiveID))
	if err != nil {
		store.artifactMu.Unlock()
		t.Fatalf("load archive package meta: %v", err)
	}
	meta, err := decodeArchivePackageMeta(raw)
	if err != nil {
		store.artifactMu.Unlock()
		t.Fatalf("decode archive package meta: %v", err)
	}

	paths, deletedPackages, deletedKeys, err := store.deleteArchivePackageRecords(ctx, []archivePackageMeta{meta}, block.SeqNo+archiveSliceMasterchainBlocks)
	delete(store.pendingArchiveSync, path)
	store.artifactMu.Unlock()
	if err != nil {
		t.Fatalf("delete archive package records: %v", err)
	}
	if len(paths) != 0 {
		t.Fatalf("delete paths = %v, want none", paths)
	}
	if deletedPackages != 0 {
		t.Fatalf("deleted packages = %d, want 0", deletedPackages)
	}
	if deletedKeys != 0 {
		t.Fatalf("deleted keys = %d, want 0", deletedKeys)
	}
	if _, err = store.ArchiveInfo(ctx, int32(block.SeqNo), block.Workchain, block.Shard); err != nil {
		t.Fatalf("archive info after dirty skip: %v", err)
	}
	if _, err = store.getHotCopy(ctx, hotKeyPackDeletePending(meta.path)); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("delete pending marker after dirty skip = %v, want ErrNotFound", err)
	}
	if err = store.clearPackAppendDirty(path); err != nil {
		t.Fatalf("clear dirty append marker: %v", err)
	}
}

func TestOpenTruncatesArchivePackToPublishedRefs(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	block := testArchivePruneBlock(42, 0x42)
	if err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{{
			ID:    block,
			Block: []byte{0x42},
			Meta:  &storage.BlockMeta{ID: block, GenUTime: 4200, StartLT: 42, EndLT: 43},
		}},
	}); err != nil {
		t.Fatalf("save block: %v", err)
	}

	raw, err := store.getHotCopy(context.Background(), hotKeyBlockDataRef(block))
	if err != nil {
		t.Fatalf("load block ref: %v", err)
	}
	ref, err := decodeArtifactRef(raw)
	if err != nil {
		t.Fatalf("decode block ref: %v", err)
	}
	refPath, err := store.artifactRefPath(context.Background(), ref)
	if err != nil {
		t.Fatalf("resolve block ref path: %v", err)
	}
	path := store.artifactPath(refPath)
	stat, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat archive pack before tail append: %v", err)
	}
	publishedSize := stat.Size()
	if _, err = store.markPackAppendDirty(path, publishedSize); err != nil {
		t.Fatalf("mark dirty append: %v", err)
	}
	tail, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open archive pack for tail append: %v", err)
	}
	if _, err = tail.Write([]byte{0xaa, 0xbb, 0xcc}); err != nil {
		_ = tail.Close()
		t.Fatalf("append uncommitted tail: %v", err)
	}
	if err = tail.Close(); err != nil {
		t.Fatalf("close appended archive pack: %v", err)
	}
	if err = store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	store, err = Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer func() { _ = store.Close() }()

	stat, err = os.Stat(path)
	if err != nil {
		t.Fatalf("stat archive pack: %v", err)
	}
	if stat.Size() != publishedSize {
		t.Fatalf("archive pack size = %d, want %d", stat.Size(), publishedSize)
	}
	if _, err = store.getHotCopy(context.Background(), hotKeyPackAppendDirty(refPath)); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("dirty append marker after recovery = %v, want ErrNotFound", err)
	}
	if _, err = store.BlockData(context.Background(), block); err != nil {
		t.Fatalf("block data after tail truncation: %v", err)
	}
}

func TestAbandonPendingArchivePackTruncatesTail(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	block := testArchivePruneBlock(42, 0x42)
	refs, _, err := store.appendArchiveEntries([]archiveAppendRequest{{
		kind:  packfile.KindBlock,
		block: block,
		meta: &storage.BlockMeta{
			ID:       block,
			GenUTime: 4200,
			StartLT:  42,
			EndLT:    43,
		},
		data: []byte{0x42},
	}})
	if err != nil {
		t.Fatalf("append archive entry: %v", err)
	}
	if len(refs) != 1 || refs[0] == nil {
		t.Fatalf("archive refs = %#v, want one ref", refs)
	}

	path := store.artifactPath(refs[0].Path)
	pending, ok := store.pendingArchiveSync[path]
	if !ok {
		t.Fatalf("missing pending archive sync for %s", path)
	}
	stat, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat appended archive pack: %v", err)
	}
	if stat.Size() <= pending.cleanSize {
		t.Fatalf("appended archive pack size = %d, clean size = %d", stat.Size(), pending.cleanSize)
	}

	store.abandonPendingArtifactPacks()

	stat, err = os.Stat(path)
	if pending.cleanSize == 0 && errors.Is(err, os.ErrNotExist) {
		stat = nil
	} else if err != nil {
		t.Fatalf("stat truncated archive pack: %v", err)
	}
	if stat != nil && stat.Size() != pending.cleanSize {
		t.Fatalf("archive pack size = %d, want %d", stat.Size(), pending.cleanSize)
	}
	if len(store.pendingArchiveSync) != 0 {
		t.Fatalf("pending archive sync entries = %d, want 0", len(store.pendingArchiveSync))
	}
	if _, err = store.getHotCopy(context.Background(), hotKeyPackAppendDirty(refs[0].Path)); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("dirty append marker after abandon = %v, want ErrNotFound", err)
	}
}

func testArchivePruneBlock(seqno uint32, seed byte) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     seqno,
		RootHash:  bytes.Repeat([]byte{seed}, 32),
		FileHash:  bytes.Repeat([]byte{seed + 1}, 32),
	}
}

func saveArchivePruneBlock(t *testing.T, store *Store, block ton.BlockIDExt, utime uint32, flags storage.BlockMetaFlags, proof []byte) string {
	t.Helper()

	full := &storage.ServedBlockFull{
		ID:    block,
		Block: []byte{byte(block.SeqNo)},
		Meta: &storage.BlockMeta{
			ID:       block,
			Flags:    flags,
			GenUTime: utime,
			StartLT:  uint64(block.SeqNo),
			EndLT:    uint64(block.SeqNo + 1),
		},
	}
	if len(proof) > 0 {
		full.Proof = proof
	}

	if err := saveTestArchiveArtifacts(store, testArchiveImport{FullBlocks: []*storage.ServedBlockFull{full}}); err != nil {
		t.Fatalf("save block %d: %v", block.SeqNo, err)
	}
	raw, err := store.getHotCopy(context.Background(), hotKeyBlockDataRef(block))
	if err != nil {
		t.Fatalf("load block ref %d: %v", block.SeqNo, err)
	}
	ref, err := decodeArtifactRef(raw)
	if err != nil {
		t.Fatalf("decode block ref %d: %v", block.SeqNo, err)
	}
	refPath, err := store.artifactRefPath(context.Background(), ref)
	if err != nil {
		t.Fatalf("resolve block ref path %d: %v", block.SeqNo, err)
	}
	return store.artifactPath(refPath)
}

func testShardID(value uint64) int64 {
	return int64(value)
}

func testServedBlockID(workchain int32, shard int64, seqno uint32, seed byte) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: workchain,
		Shard:     shard,
		SeqNo:     seqno,
		RootHash:  bytes.Repeat([]byte{seed}, 32),
		FileHash:  bytes.Repeat([]byte{seed + 1}, 32),
	}
}

func testArchivePackageCacheBlock(block ton.BlockIDExt, blockData []byte, proofData []byte) *storage.ServedBlockFull {
	return &storage.ServedBlockFull{
		ID:    block,
		Block: blockData,
		Proof: proofData,
		Meta: &storage.BlockMeta{
			ID:       block,
			GenUTime: block.SeqNo * 10,
			StartLT:  uint64(block.SeqNo),
			EndLT:    uint64(block.SeqNo + 1),
		},
	}
}

func warmArchivePackageMetaCache(t *testing.T, store *Store, ctx context.Context, block ton.BlockIDExt) (int64, int64) {
	t.Helper()

	archiveID, err := store.ArchiveInfo(ctx, int32(block.SeqNo), block.Workchain, block.Shard)
	if err != nil {
		t.Fatalf("archive info: %v", err)
	}
	raw, err := store.getHotCopy(ctx, hotKeyArchivePackage(archiveID))
	if err != nil {
		t.Fatalf("load archive package meta: %v", err)
	}
	meta, err := decodeArchivePackageMeta(raw)
	if err != nil {
		t.Fatalf("decode archive package meta: %v", err)
	}
	if meta.size <= 0 {
		t.Fatalf("archive package size = %d, want positive", meta.size)
	}
	if meta.size > int64(^uint32(0)>>1) {
		t.Fatalf("archive package size = %d, too large for test", meta.size)
	}
	data, err := store.ArchiveSlice(ctx, archiveID, 0, int32(meta.size))
	if err != nil {
		t.Fatalf("warm archive package meta cache: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("archive package warm read returned empty data")
	}
	return archiveID, meta.size
}

func TestArtifactSlicesReturnNotFoundOnTruncatedFiles(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(0x4000000000000000),
		SeqNo:     77,
		RootHash:  bytes.Repeat([]byte{0x01}, 32),
		FileHash:  bytes.Repeat([]byte{0x02}, 32),
	}
	master := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     78,
		RootHash:  bytes.Repeat([]byte{0x03}, 32),
		FileHash:  bytes.Repeat([]byte{0x04}, 32),
	}

	stateName, err := storage.PersistentStateFileName(block, master, 0)
	if err != nil {
		t.Fatalf("state file name: %v", err)
	}
	statePath := filepath.Join(store.StateFilesDir(), stateName)
	if err = os.WriteFile(statePath, []byte{1, 2, 3, 4, 5}, 0o644); err != nil {
		t.Fatalf("write state file: %v", err)
	}
	if err = store.SavePersistentStateFile(&storage.PersistentStateFile{
		Block:            block,
		MasterchainBlock: master,
		EffectiveShard:   0,
		Ref:              &storage.ArtifactRef{Path: statePath, Size: 5},
		FileHash:         bytes.Repeat([]byte{0x55}, 32),
		StateRootHash:    bytes.Repeat([]byte{0x66}, 32),
	}); err != nil {
		t.Fatalf("save persistent state file: %v", err)
	}
	if err = os.Truncate(statePath, 4); err != nil {
		t.Fatalf("truncate state file: %v", err)
	}
	if _, err = store.PersistentStateSlice(context.Background(), block, master, 0, 0, 5); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("truncated state slice error = %v, want ErrNotFound", err)
	}

	if err = saveTestArchiveArtifacts(store, testArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{{
			ID:    master,
			Block: []byte{9, 8, 7},
			Proof: []byte{6, 5, 4},
			Meta:  &storage.BlockMeta{ID: master, GenUTime: 780, StartLT: 78, EndLT: 79},
		}},
	}); err != nil {
		t.Fatalf("save archive import: %v", err)
	}
	archiveID, err := store.ArchiveInfo(context.Background(), 78, -1, int64(-1<<63))
	if err != nil {
		t.Fatalf("archive info: %v", err)
	}
	rawArchiveRef, err := store.getHotCopy(context.Background(), hotKeyArchivePackage(archiveID))
	if err != nil {
		t.Fatalf("load archive package meta: %v", err)
	}
	archiveRef, err := decodeArchivePackageMeta(rawArchiveRef)
	if err != nil {
		t.Fatalf("decode archive package meta: %v", err)
	}
	archivePath := store.artifactPath(archiveRef.path)
	stat, err := os.Stat(archivePath)
	if err != nil {
		t.Fatalf("stat archive file: %v", err)
	}
	if err = os.Truncate(archivePath, stat.Size()-1); err != nil {
		t.Fatalf("truncate archive file: %v", err)
	}
	if _, err = store.ArchiveSlice(context.Background(), archiveID, 0, int32(stat.Size())); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("truncated archive slice error = %v, want ErrNotFound", err)
	}
}

func TestSavePersistentStateFileServesSizeSliceAndMeta(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()
	metricsObserver := &testArtifactMetricsObserver{}
	store.SetArtifactMetricsObserver(metricsObserver)

	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(0x4000000000000000),
		SeqNo:     77,
		RootHash:  bytes.Repeat([]byte{0x01}, 32),
		FileHash:  bytes.Repeat([]byte{0x02}, 32),
	}
	master := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     78,
		RootHash:  bytes.Repeat([]byte{0x03}, 32),
		FileHash:  bytes.Repeat([]byte{0x04}, 32),
	}
	data := []byte{1, 2, 3, 4, 5, 6}
	name, err := storage.PersistentStateFileName(block, master, 0)
	if err != nil {
		t.Fatalf("persistent state file name: %v", err)
	}
	path := filepath.Join(store.StateFilesDir(), name)
	if err = os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write state file: %v", err)
	}
	fileHash := bytes.Repeat([]byte{0x55}, 32)
	stateRootHash := bytes.Repeat([]byte{0x66}, 32)

	if err = store.SavePersistentStateFile(&storage.PersistentStateFile{
		Block:            block,
		MasterchainBlock: master,
		EffectiveShard:   0,
		Ref:              &storage.ArtifactRef{Path: path, Size: int64(len(data))},
		FileHash:         fileHash,
		StateRootHash:    stateRootHash,
	}); err != nil {
		t.Fatalf("save persistent state file: %v", err)
	}
	if metricsObserver.persistentStateBytes != int64(len(data)) {
		t.Fatalf("persistent state metric bytes = %d, want %d", metricsObserver.persistentStateBytes, len(data))
	}

	size, err := store.PersistentStateSize(context.Background(), block, master, 0)
	if err != nil {
		t.Fatalf("persistent state size: %v", err)
	}
	if size != int64(len(data)) {
		t.Fatalf("persistent state size = %d, want %d", size, len(data))
	}

	chunk, err := store.PersistentStateSlice(context.Background(), block, master, 0, 2, 3)
	if err != nil {
		t.Fatalf("persistent state slice: %v", err)
	}
	if !bytes.Equal(chunk, data[2:5]) {
		t.Fatalf("persistent state slice = %x, want %x", chunk, data[2:5])
	}

	meta, err := store.BlockMeta(context.Background(), block)
	if err != nil {
		t.Fatalf("load block meta: %v", err)
	}
	if !meta.Has(storage.BlockMetaHasStateSnapshot) {
		t.Fatal("persistent state file did not mark block meta")
	}
	if !bytes.Equal(meta.StateFileHash, fileHash) {
		t.Fatalf("state file hash = %x, want %x", meta.StateFileHash, fileHash)
	}
	if !bytes.Equal(meta.StateRootHash, stateRootHash) {
		t.Fatalf("state root hash = %x, want %x", meta.StateRootHash, stateRootHash)
	}
	if _, err = store.LookupBlockBySeqNo(context.Background(), storage.BlockSeqRefFromBlock(block)); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("persistent state metadata lookup by seqno error = %v, want ErrNotFound", err)
	}

	if err = store.DeletePersistentStateFile(context.Background(), block, master, 0); err != nil {
		t.Fatalf("delete persistent state file: %v", err)
	}
	if metricsObserver.persistentStateBytes != 0 {
		t.Fatalf("persistent state metric bytes after delete = %d, want 0", metricsObserver.persistentStateBytes)
	}
	if _, err = store.PersistentStateSize(context.Background(), block, master, 0); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("persistent state size after delete error = %v, want not found", err)
	}
	if _, err = os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("persistent state file after delete error = %v, want not exist", err)
	}
	meta, err = store.BlockMeta(context.Background(), block)
	if err != nil {
		t.Fatalf("load block meta after delete: %v", err)
	}
	if meta.Has(storage.BlockMetaHasStateSnapshot) {
		t.Fatal("persistent state delete left snapshot flag in block meta")
	}
	if len(meta.StateFileHash) != 0 {
		t.Fatalf("state file hash after delete = %x, want empty", meta.StateFileHash)
	}
	if _, err = store.LookupBlockBySeqNo(context.Background(), storage.BlockSeqRefFromBlock(block)); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("persistent state metadata lookup by seqno after delete error = %v, want ErrNotFound", err)
	}
}

func TestDeletePersistentStateFileClearsSnapshotOnlyBlockMeta(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(0x4000000000000000),
		SeqNo:     79,
		RootHash:  bytes.Repeat([]byte{0x11}, 32),
		FileHash:  bytes.Repeat([]byte{0x12}, 32),
	}
	master := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     80,
		RootHash:  bytes.Repeat([]byte{0x13}, 32),
		FileHash:  bytes.Repeat([]byte{0x14}, 32),
	}
	data := []byte{1, 2, 3}
	name, err := storage.PersistentStateFileName(block, master, 0)
	if err != nil {
		t.Fatalf("persistent state file name: %v", err)
	}
	path := filepath.Join(store.StateFilesDir(), name)
	if err = os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write state file: %v", err)
	}

	if err = store.SavePersistentStateFile(&storage.PersistentStateFile{
		Block:            block,
		MasterchainBlock: master,
		EffectiveShard:   0,
		Ref:              &storage.ArtifactRef{Path: path, Size: int64(len(data))},
		FileHash:         bytes.Repeat([]byte{0x15}, 32),
		StateRootHash:    bytes.Repeat([]byte{0x16}, 32),
	}); err != nil {
		t.Fatalf("save persistent state file: %v", err)
	}
	if err = store.DeletePersistentStateFile(context.Background(), block, master, 0); err != nil {
		t.Fatalf("delete persistent state file: %v", err)
	}
	meta, err := store.BlockMeta(context.Background(), block)
	if err != nil {
		t.Fatalf("load persistent state block meta after delete: %v", err)
	}
	if meta.Has(storage.BlockMetaHasStateSnapshot) {
		t.Fatal("persistent state delete left snapshot flag in block meta")
	}
	if len(meta.StateFileHash) != 0 {
		t.Fatalf("persistent state delete left state file hash %x", meta.StateFileHash)
	}
}

func TestPruneExpiredPersistentStateFilesKeepsTwoNewestGroups(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	nowUnix := uint64(10_000_000)

	masters := []ton.BlockIDExt{
		testArchivePruneBlock(10, 0x10),
		testArchivePruneBlock(20, 0x20),
		testArchivePruneBlock(30, 0x30),
		testArchivePruneBlock(40, 0x40),
	}
	paths := make(map[uint32]string, len(masters))
	for _, master := range masters {
		saveTestPersistentStatePruneMasterMeta(t, store, master)
		paths[master.SeqNo] = saveTestPersistentStatePruneFile(t, store, master)
	}

	stats, err := store.PruneExpiredPersistentStateFiles(ctx, nowUnix, 2, 1)
	if err != nil {
		t.Fatalf("prune persistent states: %v", err)
	}
	if stats.ScannedFiles != 4 {
		t.Fatalf("scanned files = %d, want 4", stats.ScannedFiles)
	}
	if stats.DeletedFileRecords != 1 || stats.DeletedDiskFiles != 1 {
		t.Fatalf("deleted = records:%d disk:%d, want records:1 disk:1", stats.DeletedFileRecords, stats.DeletedDiskFiles)
	}
	if stats.DeletedDiskBytes != 1 {
		t.Fatalf("deleted disk bytes = %d, want 1", stats.DeletedDiskBytes)
	}
	assertTestPersistentStatePruned(t, store, masters[0], paths[10])
	assertTestPersistentStatePresent(t, store, masters[1], paths[20])
	assertTestPersistentStatePresent(t, store, masters[2], paths[30])
	assertTestPersistentStatePresent(t, store, masters[3], paths[40])

	stats, err = store.PruneExpiredPersistentStateFiles(ctx, nowUnix, 2, 0)
	if err != nil {
		t.Fatalf("prune persistent states second pass: %v", err)
	}
	if stats.DeletedFileRecords != 1 || stats.DeletedDiskFiles != 1 {
		t.Fatalf("second pass deleted = records:%d disk:%d, want records:1 disk:1", stats.DeletedFileRecords, stats.DeletedDiskFiles)
	}
	if stats.DeletedDiskBytes != 1 {
		t.Fatalf("second pass deleted disk bytes = %d, want 1", stats.DeletedDiskBytes)
	}
	if stats.RetainedRecentGroups != 2 || stats.OldestRetainedMasterSeqno != 30 {
		t.Fatalf("retained groups = %d oldest = %d, want 2 and 30", stats.RetainedRecentGroups, stats.OldestRetainedMasterSeqno)
	}
	assertTestPersistentStatePruned(t, store, masters[1], paths[20])
	assertTestPersistentStatePresent(t, store, masters[2], paths[30])
	assertTestPersistentStatePresent(t, store, masters[3], paths[40])
}

func TestPruneExpiredPersistentStateFilesKeepsPendingCellGenerationOrigin(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	nowUnix := uint64(10_000_000)

	masters := []ton.BlockIDExt{
		testArchivePruneBlock(10, 0x10),
		testArchivePruneBlock(20, 0x20),
		testArchivePruneBlock(30, 0x30),
	}
	paths := make(map[uint32]string, len(masters))
	for _, master := range masters {
		saveTestPersistentStatePruneMasterMeta(t, store, master)
		paths[master.SeqNo] = saveTestPersistentStatePruneFile(t, store, master)
	}
	if _, err = store.BeginCellGeneration(ctx, masters[0]); err != nil {
		t.Fatalf("begin pending cell generation: %v", err)
	}

	stats, err := store.PruneExpiredPersistentStateFiles(ctx, nowUnix, 1, 0)
	if err != nil {
		t.Fatalf("prune persistent states: %v", err)
	}
	if stats.DeletedFileRecords != 1 || stats.DeletedDiskFiles != 1 {
		t.Fatalf("deleted = records:%d disk:%d, want records:1 disk:1", stats.DeletedFileRecords, stats.DeletedDiskFiles)
	}
	assertTestPersistentStatePresent(t, store, masters[0], paths[10])
	assertTestPersistentStatePruned(t, store, masters[1], paths[20])
	assertTestPersistentStatePresent(t, store, masters[2], paths[30])
}

func TestPrunePersistentStateFilesToLimitDeletesOlderUnexpiredGroups(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	masters := []ton.BlockIDExt{
		testArchivePruneBlock(10, 0x10),
		testArchivePruneBlock(20, 0x20),
		testArchivePruneBlock(30, 0x30),
		testArchivePruneBlock(40, 0x40),
	}
	paths := make(map[uint32]string, len(masters))
	for _, master := range masters {
		saveTestPersistentStatePruneMasterMeta(t, store, master)
		paths[master.SeqNo] = saveTestPersistentStatePruneFile(t, store, master)
	}

	stats, err := store.PrunePersistentStateFilesToLimit(ctx, masters[3].SeqNo, 2)
	if err != nil {
		t.Fatalf("prune persistent states to limit: %v", err)
	}
	if stats.DeletedFileRecords != 2 || stats.DeletedDiskFiles != 2 || stats.DeletedDiskBytes != 2 {
		t.Fatalf("deleted = records:%d files:%d bytes:%d, want 2/2/2", stats.DeletedFileRecords, stats.DeletedDiskFiles, stats.DeletedDiskBytes)
	}
	if stats.RetainedRecentGroups != 2 || stats.OldestRetainedMasterSeqno != masters[2].SeqNo {
		t.Fatalf("retained groups = %d oldest = %d, want 2 and %d", stats.RetainedRecentGroups, stats.OldestRetainedMasterSeqno, masters[2].SeqNo)
	}
	assertTestPersistentStatePruned(t, store, masters[0], paths[10])
	assertTestPersistentStatePruned(t, store, masters[1], paths[20])
	assertTestPersistentStatePresent(t, store, masters[2], paths[30])
	assertTestPersistentStatePresent(t, store, masters[3], paths[40])
}

func TestPrunePersistentStateFilesToLimitKeepsPendingMigrationOrigin(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	masters := []ton.BlockIDExt{
		testArchivePruneBlock(10, 0x10),
		testArchivePruneBlock(20, 0x20),
		testArchivePruneBlock(30, 0x30),
		testArchivePruneBlock(40, 0x40),
	}
	paths := make(map[uint32]string, len(masters))
	for _, master := range masters {
		saveTestPersistentStatePruneMasterMeta(t, store, master)
		paths[master.SeqNo] = saveTestPersistentStatePruneFile(t, store, master)
	}
	if _, err = store.BeginCellGeneration(ctx, masters[1]); err != nil {
		t.Fatalf("begin pending cell generation: %v", err)
	}

	stats, err := store.PrunePersistentStateFilesToLimit(ctx, masters[3].SeqNo, 1)
	if err != nil {
		t.Fatalf("prune persistent states to limit: %v", err)
	}
	if stats.DeletedFileRecords != 2 || stats.DeletedDiskFiles != 2 {
		t.Fatalf("deleted = records:%d files:%d, want 2/2", stats.DeletedFileRecords, stats.DeletedDiskFiles)
	}
	assertTestPersistentStatePruned(t, store, masters[0], paths[10])
	assertTestPersistentStatePresent(t, store, masters[1], paths[20])
	assertTestPersistentStatePruned(t, store, masters[2], paths[30])
	assertTestPersistentStatePresent(t, store, masters[3], paths[40])
}

func TestPrunePersistentStateFilesToLimitLeavesGroupsAfterTarget(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	masters := []ton.BlockIDExt{
		testArchivePruneBlock(10, 0x10),
		testArchivePruneBlock(20, 0x20),
		testArchivePruneBlock(30, 0x30),
		testArchivePruneBlock(40, 0x40),
	}
	paths := make(map[uint32]string, len(masters))
	for _, master := range masters {
		saveTestPersistentStatePruneMasterMeta(t, store, master)
		paths[master.SeqNo] = saveTestPersistentStatePruneFile(t, store, master)
	}

	if _, err = store.PrunePersistentStateFilesToLimit(ctx, masters[2].SeqNo, 1); err != nil {
		t.Fatalf("prune persistent states to limit: %v", err)
	}
	assertTestPersistentStatePruned(t, store, masters[0], paths[10])
	assertTestPersistentStatePruned(t, store, masters[1], paths[20])
	assertTestPersistentStatePresent(t, store, masters[2], paths[30])
	assertTestPersistentStatePresent(t, store, masters[3], paths[40])
}

func TestPrunePreviousPersistentStateFilesDeletesLatestOlderGroup(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	masters := []ton.BlockIDExt{
		testArchivePruneBlock(10, 0x10),
		testArchivePruneBlock(20, 0x20),
		testArchivePruneBlock(30, 0x30),
		testArchivePruneBlock(40, 0x40),
	}
	paths := make(map[uint32]string, len(masters))
	for _, master := range masters {
		saveTestPersistentStatePruneMasterMeta(t, store, master)
		paths[master.SeqNo] = saveTestPersistentStatePruneFile(t, store, master)
	}

	stats, err := store.PrunePreviousPersistentStateFiles(ctx, 35)
	if err != nil {
		t.Fatalf("prune previous persistent state: %v", err)
	}
	if stats.DeletedMasterSeqno != 30 {
		t.Fatalf("deleted master seqno = %d, want 30", stats.DeletedMasterSeqno)
	}
	if stats.DeletedFileRecords != 1 || stats.DeletedDiskFiles != 1 || stats.DeletedDiskBytes != 1 {
		t.Fatalf("deleted = records:%d files:%d bytes:%d, want 1/1/1", stats.DeletedFileRecords, stats.DeletedDiskFiles, stats.DeletedDiskBytes)
	}
	assertTestPersistentStatePresent(t, store, masters[0], paths[10])
	assertTestPersistentStatePresent(t, store, masters[1], paths[20])
	assertTestPersistentStatePruned(t, store, masters[2], paths[30])
	assertTestPersistentStatePresent(t, store, masters[3], paths[40])
}

func TestPrunePreviousPersistentStateFilesKeepsPendingCellGenerationOrigin(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	masters := []ton.BlockIDExt{
		testArchivePruneBlock(10, 0x10),
		testArchivePruneBlock(20, 0x20),
		testArchivePruneBlock(30, 0x30),
		testArchivePruneBlock(40, 0x40),
	}
	paths := make(map[uint32]string, len(masters))
	for _, master := range masters {
		saveTestPersistentStatePruneMasterMeta(t, store, master)
		paths[master.SeqNo] = saveTestPersistentStatePruneFile(t, store, master)
	}
	if _, err = store.BeginCellGeneration(ctx, masters[2]); err != nil {
		t.Fatalf("begin pending cell generation: %v", err)
	}

	stats, err := store.PrunePreviousPersistentStateFiles(ctx, 35)
	if err != nil {
		t.Fatalf("prune previous persistent state: %v", err)
	}
	if stats.DeletedMasterSeqno != 20 {
		t.Fatalf("deleted master seqno = %d, want 20", stats.DeletedMasterSeqno)
	}
	assertTestPersistentStatePresent(t, store, masters[0], paths[10])
	assertTestPersistentStatePruned(t, store, masters[1], paths[20])
	assertTestPersistentStatePresent(t, store, masters[2], paths[30])
	assertTestPersistentStatePresent(t, store, masters[3], paths[40])
}

func saveTestPersistentStatePruneMasterMeta(t *testing.T, store *Store, master ton.BlockIDExt) {
	t.Helper()

	err := store.withHotBatch(func(batch *pebble.Batch) error {
		return store.setMergedBlockMeta(batch, &storage.BlockMeta{
			ID:       master,
			GenUTime: 1 << 17,
		})
	})
	if err != nil {
		t.Fatalf("save master meta %s: %v", storage.FormatBlockRef(master), err)
	}
}

func saveTestPersistentStatePruneFile(t *testing.T, store *Store, master ton.BlockIDExt) string {
	t.Helper()

	return saveTestPersistentStatePruneFileForBlock(t, store, master, master)
}

func saveTestPersistentStatePruneFileForBlock(t *testing.T, store *Store, block ton.BlockIDExt, master ton.BlockIDExt) string {
	t.Helper()

	data := []byte{byte(block.SeqNo)}
	name, err := storage.PersistentStateFileName(block, master, 0)
	if err != nil {
		t.Fatalf("persistent state file name: %v", err)
	}
	path := filepath.Join(store.StateFilesDir(), name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir state dir: %v", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write persistent state file: %v", err)
	}

	if err := store.SavePersistentStateFile(&storage.PersistentStateFile{
		Block:            block,
		MasterchainBlock: master,
		EffectiveShard:   0,
		Ref:              &storage.ArtifactRef{Path: path, Size: int64(len(data))},
		FileHash:         bytes.Repeat([]byte{byte(block.SeqNo)}, 32),
		StateRootHash:    bytes.Repeat([]byte{byte(block.SeqNo + 1)}, 32),
	}); err != nil {
		t.Fatalf("save persistent state file %s: %v", storage.FormatBlockRef(block), err)
	}
	return path
}

func assertTestPersistentStatePruned(t *testing.T, store *Store, master ton.BlockIDExt, path string) {
	t.Helper()

	assertTestPersistentStatePrunedForBlock(t, store, master, master, path)
}

func assertTestPersistentStatePrunedForBlock(t *testing.T, store *Store, block ton.BlockIDExt, master ton.BlockIDExt, path string) {
	t.Helper()

	if _, err := store.PersistentStateSize(context.Background(), block, master, 0); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("persistent state %s size error = %v, want ErrNotFound", storage.FormatBlockRef(block), err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("persistent state file %s stat error = %v, want not exist", path, err)
	}
}

func assertTestPersistentStatePresent(t *testing.T, store *Store, master ton.BlockIDExt, path string) {
	t.Helper()

	assertTestPersistentStatePresentForBlock(t, store, master, master, path)
}

func assertTestPersistentStatePresentForBlock(t *testing.T, store *Store, block ton.BlockIDExt, master ton.BlockIDExt, path string) {
	t.Helper()

	if _, err := store.PersistentStateSize(context.Background(), block, master, 0); err != nil {
		t.Fatalf("persistent state %s size: %v", storage.FormatBlockRef(block), err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("persistent state file %s stat: %v", path, err)
	}
}

func testFileSize(t *testing.T, path string) uint64 {
	t.Helper()

	stat, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if stat.Size() <= 0 {
		return 0
	}
	return uint64(stat.Size())
}
