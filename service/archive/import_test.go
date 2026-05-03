package archive

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"

	"flexserver/service/archive/packfile"
	"flexserver/service/storage"
	"flexserver/service/storage/pebblestore"

	"github.com/xssnick/tonutils-go/ton"
)

func TestImportFileStoresFullBlocksAndNextLinks(t *testing.T) {
	store := openTestPebbleStore(t)

	block10 := testBlockID(0, topShard, 10)
	block11 := testBlockID(0, topShard, 11)
	path := writeTestPackage(t, []testEntry{
		{name: testEntryName("block", block10), data: []byte{0x10, 0x01}},
		{name: testEntryName("prooflink", block10), data: []byte{0x10, 0x02}},
		{name: testEntryName("proof", block11), data: []byte{0x11, 0x01}},
		{name: "ignored_file", data: []byte{0x99}},
		{name: testEntryName("block", block11), data: []byte{0x11, 0x02}},
	})

	var imported []ton.BlockIDExt
	stats, err := ImportFile(context.Background(), &Downloaded{
		MasterchainSeqno: 100,
		ArchiveID:        777,
		Peer:             "peer",
		Path:             path,
		Bytes:            1234,
	}, ImportSink{
		Writer: store,
		FullBlock: func(block *storage.ServedBlockFull) error {
			imported = append(imported, block.ID)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("import archive: %v", err)
	}

	if stats.Entries != 4 || stats.IgnoredEntries != 1 || stats.Blocks != 2 || stats.Proofs != 1 || stats.ProofLinks != 1 || stats.FullBlocks != 2 || stats.Links != 1 {
		t.Fatalf("unexpected stats: %#v", stats)
	}
	if len(imported) != 2 || !imported[0].Equals(&block10) || !imported[1].Equals(&block11) {
		t.Fatalf("unexpected direct imports: %#v", imported)
	}

	full10, err := store.BlockFull(context.Background(), block10)
	if err != nil {
		t.Fatalf("load block 10: %v", err)
	}
	if !full10.IsLink || string(full10.Proof) != string([]byte{0x10, 0x02}) {
		t.Fatalf("unexpected block 10 full: %#v", full10)
	}

	full11, err := store.BlockFull(context.Background(), block11)
	if err != nil {
		t.Fatalf("load block 11: %v", err)
	}
	if full11.IsLink || string(full11.Proof) != string([]byte{0x11, 0x01}) {
		t.Fatalf("unexpected block 11 full: %#v", full11)
	}

	next, err := store.NextBlockFull(context.Background(), block10)
	if err != nil {
		t.Fatalf("load next block: %v", err)
	}
	if !next.ID.Equals(&block11) {
		t.Fatalf("unexpected next block: got=%s want=%s", storage.FormatBlockRef(next.ID), storage.FormatBlockRef(block11))
	}
}

func TestImportStreamStoresAfterArtifactPathIsAssigned(t *testing.T) {
	store := openTestPebbleStore(t)

	block := testBlockID(0, topShard, 12)
	path := writeTestPackage(t, []testEntry{
		{name: testEntryName("block", block), data: []byte{0x12, 0x01}},
		{name: testEntryName("proof", block), data: []byte{0x12, 0x02}},
	})
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read test package: %v", err)
	}

	imported, err := ImportStream(context.Background(), &Downloaded{
		MasterchainSeqno: 100,
		ArchiveID:        778,
		Shard:            ShardID{Workchain: 0, Shard: topShard},
	}, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("stream import archive: %v", err)
	}
	if len(imported.FullBlocks) != 1 {
		t.Fatalf("expected one full block, got %d", len(imported.FullBlocks))
	}

	imported.SetArtifactPath(path)
	if err = imported.Store(ImportSink{Writer: store}); err != nil {
		t.Fatalf("store streamed import: %v", err)
	}

	full, err := store.BlockFull(context.Background(), block)
	if err != nil {
		t.Fatalf("load streamed block: %v", err)
	}
	if string(full.Block) != string([]byte{0x12, 0x01}) || string(full.Proof) != string([]byte{0x12, 0x02}) {
		t.Fatalf("unexpected streamed full block: %#v", full)
	}
}

func TestParseEntryName(t *testing.T) {
	block := testBlockID(-1, topShard, 42)
	ref, err := parseEntryName(testEntryName("proof", block))
	if err != nil {
		t.Fatalf("parse archive entry name: %v", err)
	}
	if ref.kind != "proof" || !ref.id.Equals(&block) {
		t.Fatalf("unexpected ref: %#v", ref)
	}

	if _, err = parseEntryName("unrelated"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("parse unrelated entry error = %v, want ErrNotFound", err)
	}
}

func TestImportFileSeqRangeTracksRequestedShard(t *testing.T) {
	store := openTestPebbleStore(t)

	master := testBlockID(-1, topShard, 42)
	base := testBlockID(0, topShard, 1000)
	path := writeTestPackage(t, []testEntry{
		{name: testEntryName("block", base), data: []byte{0x10}},
		{name: testEntryName("proof", base), data: []byte{0x11}},
		{name: testEntryName("block", master), data: []byte{0x20}},
		{name: testEntryName("proof", master), data: []byte{0x21}},
	})

	stats, err := ImportFile(context.Background(), &Downloaded{
		MasterchainSeqno: 42,
		Shard:            ShardID{Workchain: -1, Shard: topShard},
		Path:             path,
	}, ImportSink{
		Writer: store,
	})
	if err != nil {
		t.Fatalf("import archive: %v", err)
	}
	if stats.FirstSeqno != master.SeqNo || stats.LastSeqno != master.SeqNo {
		t.Fatalf("unexpected masterchain seq range: first=%d last=%d", stats.FirstSeqno, stats.LastSeqno)
	}
	if stats.MasterchainFirstSeqno != master.SeqNo || stats.MasterchainLastSeqno != master.SeqNo {
		t.Fatalf("unexpected masterchain stats range: first=%d last=%d", stats.MasterchainFirstSeqno, stats.MasterchainLastSeqno)
	}
}

func openTestPebbleStore(tb testing.TB) *pebblestore.Store {
	tb.Helper()

	store, err := pebblestore.Open(pebblestore.Options{Dir: tb.TempDir()})
	if err != nil {
		tb.Fatalf("open pebble store: %v", err)
	}
	tb.Cleanup(func() { _ = store.Close() })
	return store
}

func TestObserveMasterchainBlockShardsFromFixture(t *testing.T) {
	rawFixture, err := os.ReadFile("../testdata/masterchain_block_fixture.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}

	var fixture struct {
		Block struct {
			Workchain   int32  `json:"workchain"`
			Shard       string `json:"shard"`
			SeqNo       uint32 `json:"seqno"`
			RootHashHex string `json:"root_hash_hex"`
			FileHashHex string `json:"file_hash_hex"`
		} `json:"block"`
		RawBOCBase64 string `json:"raw_boc_base64"`
	}
	if err = json.Unmarshal(rawFixture, &fixture); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}

	blockData, err := base64.StdEncoding.DecodeString(fixture.RawBOCBase64)
	if err != nil {
		t.Fatalf("decode block boc base64: %v", err)
	}
	rootHash, err := hex.DecodeString(fixture.Block.RootHashHex)
	if err != nil {
		t.Fatalf("decode root hash: %v", err)
	}
	fileHash, err := hex.DecodeString(fixture.Block.FileHashHex)
	if err != nil {
		t.Fatalf("decode file hash: %v", err)
	}
	shard, err := strconv.ParseUint(fixture.Block.Shard, 16, 64)
	if err != nil {
		t.Fatalf("parse shard: %v", err)
	}

	stats := &ImportStats{}
	err = observeMasterchainBlockShards(stats, ton.BlockIDExt{
		Workchain: fixture.Block.Workchain,
		Shard:     int64(shard),
		SeqNo:     fixture.Block.SeqNo,
		RootHash:  rootHash,
		FileHash:  fileHash,
	}, blockData)
	if err != nil {
		t.Fatalf("observe masterchain block shards: %v", err)
	}
	if len(stats.MasterchainShardBlocks) == 0 {
		t.Fatal("expected shard blocks from masterchain block fixture")
	}
}

func TestShardIDContainsBlock(t *testing.T) {
	left := ShardID{Workchain: 0, Shard: int64(0x4000000000000000)}
	right := testBlockID(0, int64(-0x4000000000000000), 1)
	child := testBlockID(0, int64(0x6000000000000000), 1)
	otherWorkchain := testBlockID(1, int64(0x6000000000000000), 1)

	if !left.ContainsBlock(child) {
		t.Fatal("left shard prefix should contain child shard")
	}
	if left.ContainsBlock(right) {
		t.Fatal("left shard prefix should not contain right shard")
	}
	if left.ContainsBlock(otherWorkchain) {
		t.Fatal("shard prefix should not contain another workchain")
	}
}

type testEntry struct {
	name string
	data []byte
}

func writeTestPackage(t *testing.T, entries []testEntry) string {
	t.Helper()

	file, err := os.CreateTemp(t.TempDir(), "archive-*.pack")
	if err != nil {
		t.Fatalf("create archive: %v", err)
	}
	defer func() { _ = file.Close() }()

	if err = binary.Write(file, binary.LittleEndian, uint32(packfile.PackageMagic)); err != nil {
		t.Fatalf("write archive magic: %v", err)
	}
	for _, entry := range entries {
		header := uint32(packfile.EntryMagic) | uint32(len(entry.name))<<16
		if err = binary.Write(file, binary.LittleEndian, header); err != nil {
			t.Fatalf("write entry header: %v", err)
		}
		if err = binary.Write(file, binary.LittleEndian, uint32(len(entry.data))); err != nil {
			t.Fatalf("write entry size: %v", err)
		}
		if _, err = file.Write([]byte(entry.name)); err != nil {
			t.Fatalf("write entry name: %v", err)
		}
		if _, err = file.Write(entry.data); err != nil {
			t.Fatalf("write entry data: %v", err)
		}
	}
	return file.Name()
}

func testBlockID(workchain int32, shard int64, seqno uint32) ton.BlockIDExt {
	root := make([]byte, 32)
	file := make([]byte, 32)
	root[0] = byte(seqno)
	root[31] = 0x01
	file[0] = byte(seqno)
	file[31] = 0x02
	return ton.BlockIDExt{
		Workchain: workchain,
		Shard:     shard,
		SeqNo:     seqno,
		RootHash:  root,
		FileHash:  file,
	}
}

func testEntryName(kind string, block ton.BlockIDExt) string {
	return fmt.Sprintf("%s_(%d,%016x,%d):%x:%x", kind, block.Workchain, uint64(block.Shard), block.SeqNo, block.RootHash, block.FileHash)
}
