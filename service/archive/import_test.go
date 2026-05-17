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

	"github.com/xssnick/gton/service/archive/packfile"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/storage/pebblestore"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestImportFileStoresFullBlocksAndNextLinks(t *testing.T) {
	store := openTestPebbleStore(t)

	block10, block10Data := readMasterchainBlockFixture(t)
	path := writeTestPackage(t, []testEntry{
		{name: testEntryName("block", block10), data: block10Data},
		{name: testEntryName("prooflink", block10), data: []byte{0x10, 0x02}},
		{name: "ignored_file", data: []byte{0x99}},
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

	if stats.Entries != 2 || stats.IgnoredEntries != 1 || stats.Blocks != 1 || stats.Proofs != 0 || stats.ProofLinks != 1 || stats.FullBlocks != 1 || stats.Links != 0 {
		t.Fatalf("unexpected stats: %#v", stats)
	}
	if len(imported) != 1 || !imported[0].Equals(&block10) {
		t.Fatalf("unexpected direct imports: %#v", imported)
	}

	full10, err := store.BlockFull(context.Background(), block10)
	if err != nil {
		t.Fatalf("load block 10: %v", err)
	}
	if !full10.IsLink || string(full10.Proof) != string([]byte{0x10, 0x02}) {
		t.Fatalf("unexpected block 10 full: %#v", full10)
	}

}

func TestImportStreamStoresAfterArtifactPathIsAssigned(t *testing.T) {
	store := openTestPebbleStore(t)

	block, blockData := readMasterchainBlockFixture(t)
	path := writeTestPackage(t, []testEntry{
		{name: testEntryName("block", block), data: blockData},
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
	if len(imported.PreparedBlocks) != 1 {
		t.Fatalf("expected one prepared block, got %d", len(imported.PreparedBlocks))
	}
	if imported.Stats.StateUpdateCells == 0 {
		t.Fatal("expected prepared state update cells")
	}
	if imported.Stats.BlockPrepareElapsed <= 0 {
		t.Fatal("expected block prepare timing")
	}

	imported.SetArtifactPath(path)
	if err = imported.Store(ImportSink{Writer: store}); err != nil {
		t.Fatalf("store streamed import: %v", err)
	}

	full, err := store.BlockFull(context.Background(), block)
	if err != nil {
		t.Fatalf("load streamed block: %v", err)
	}
	if string(full.Block) != string(blockData) || string(full.Proof) != string([]byte{0x12, 0x02}) {
		t.Fatalf("unexpected streamed full block: %#v", full)
	}
}

func TestPrepareImportedBlockBuildsStateMetaAndPreparedCells(t *testing.T) {
	block, blockData := readMasterchainBlockFixture(t)

	prepared, err := prepareImportedBlock(block, blockData)
	if err != nil {
		t.Fatalf("prepare imported block: %v", err)
	}
	if prepared.State == nil {
		t.Fatal("state meta was not prepared")
	}
	if prepared.State.Cell != nil {
		t.Fatal("archive-imported state meta should not retain materialized cell root")
	}
	if !bytes.Equal(prepared.State.StateRootHash, prepared.Meta.StateRootHash) {
		t.Fatalf("state root hash mismatch: got=%x want=%x", prepared.State.StateRootHash, prepared.Meta.StateRootHash)
	}
	if got := len(prepared.State.StateCellHash); got != 32 {
		t.Fatalf("state cell hash len = %d, want 32", got)
	}
	var stateCellHash cell.Hash
	copy(stateCellHash[:], prepared.State.StateCellHash)
	if len(prepared.StateUpdateToCells[stateCellHash]) == 0 {
		t.Fatalf("prepared update_to cells do not contain state root %x", prepared.State.StateCellHash)
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

	master, masterData := readMasterchainBlockFixture(t)
	path := writeTestPackage(t, []testEntry{
		{name: testEntryName("block", master), data: masterData},
		{name: testEntryName("proof", master), data: []byte{0x21}},
	})

	stats, err := ImportFile(context.Background(), &Downloaded{
		MasterchainSeqno: master.SeqNo,
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

func readMasterchainBlockFixture(t *testing.T) (ton.BlockIDExt, []byte) {
	t.Helper()

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

	return ton.BlockIDExt{
		Workchain: fixture.Block.Workchain,
		Shard:     int64(shard),
		SeqNo:     fixture.Block.SeqNo,
		RootHash:  rootHash,
		FileHash:  fileHash,
	}, blockData
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
