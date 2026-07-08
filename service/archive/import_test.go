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

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestImportBytesBuildsFullBlocksAndNextLinks(t *testing.T) {
	block10, block10Data := readMasterchainBlockFixture(t)
	path := writeTestPackage(t, []testEntry{
		{name: testEntryName("block", block10), data: block10Data},
		{name: testEntryName("proof", block10), data: []byte{0x10, 0x02}},
		{name: "ignored_file", data: []byte{0x99}},
	})
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read test package: %v", err)
	}

	imported, err := ImportBytes(context.Background(), &Downloaded{
		MasterchainSeqno: 100,
		ArchiveID:        777,
		Peer:             "peer",
		Bytes:            1234,
	}, data)
	if err != nil {
		t.Fatalf("import archive: %v", err)
	}
	stats := imported.Stats

	if stats.Entries != 2 || stats.IgnoredEntries != 1 || stats.Blocks != 1 || stats.Proofs != 1 || stats.ProofLinks != 0 || stats.FullBlocks != 1 || stats.Links != 0 {
		t.Fatalf("unexpected stats: %#v", stats)
	}
	if len(imported.FullBlocks) != 1 || !imported.FullBlocks[0].ID.Equals(&block10) {
		t.Fatalf("unexpected imported blocks: %#v", imported.FullBlocks)
	}
	full10 := imported.FullBlocks[0]
	if full10.IsLink || string(full10.Proof) != string([]byte{0x10, 0x02}) || !bytes.Equal(full10.Block, block10Data) {
		t.Fatalf("unexpected block 10 full: %#v", full10)
	}
}

func TestImportBytesStoresInlineBlocks(t *testing.T) {
	block, blockData := readMasterchainBlockFixture(t)
	path := writeTestPackage(t, []testEntry{
		{name: testEntryName("block", block), data: blockData},
		{name: testEntryName("proof", block), data: []byte{0x12, 0x02}},
	})
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read test package: %v", err)
	}

	imported, err := ImportBytes(context.Background(), &Downloaded{
		MasterchainSeqno: 100,
		ArchiveID:        778,
		Shard:            ShardID{Workchain: 0, Shard: topShard},
	}, data)
	if err != nil {
		t.Fatalf("stream import archive: %v", err)
	}
	if len(imported.FullBlocks) != 1 {
		t.Fatalf("expected one full block, got %d", len(imported.FullBlocks))
	}
	if len(imported.PreparedBlocks) != 1 {
		t.Fatalf("expected one prepared block, got %d", len(imported.PreparedBlocks))
	}
	for _, prepared := range imported.PreparedBlocks {
		if prepared.MessageEntries == nil {
			t.Fatal("expected prepared block to carry extracted message entries")
		}
	}
	if imported.Stats.StateUpdateCells == 0 {
		t.Fatal("expected prepared state update cells")
	}
	if imported.Stats.BlockPrepareElapsed <= 0 {
		t.Fatal("expected block prepare timing")
	}
	full := imported.FullBlocks[0]
	if string(full.Block) != string(blockData) || string(full.Proof) != string([]byte{0x12, 0x02}) {
		t.Fatalf("unexpected streamed full block: %#v", full)
	}
}

func TestCanonicalArchiveProofKeepsCanonicalKind(t *testing.T) {
	master := testBlockID(-1, topShard, 71)
	shard := testBlockID(0, topShard, 72)

	tests := []struct {
		name     string
		part     *blockParts
		wantLink bool
		wantData []byte
		wantNone bool
	}{
		{
			name: "master uses proof",
			part: &blockParts{
				id:        master,
				proof:     []byte{0x02},
				proofLink: []byte{0x01},
			},
			wantData: []byte{0x02},
		},
		{
			name: "shard uses prooflink",
			part: &blockParts{
				id:        shard,
				proof:     []byte{0x03},
				proofLink: []byte{0x04},
			},
			wantLink: true,
			wantData: []byte{0x04},
		},
		{
			name: "shard ignores proof without link",
			part: &blockParts{
				id:    shard,
				proof: []byte{0x05},
			},
			wantNone: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proof, isLink, ok := canonicalArchiveProof(tt.part)
			if tt.wantNone {
				if ok {
					t.Fatalf("canonical proof = %x is_link=%v, want none", proof, isLink)
				}
				return
			}
			if !ok || isLink != tt.wantLink || !bytes.Equal(proof, tt.wantData) {
				t.Fatalf("canonical proof = %x is_link=%v ok=%v, want data=%x is_link=%v", proof, isLink, ok, tt.wantData, tt.wantLink)
			}
		})
	}
}

func TestImportStreamDoesNotExposePartialArtifacts(t *testing.T) {
	block, blockData := readMasterchainBlockFixture(t)
	path := writeTestPackage(t, []testEntry{
		{name: testEntryName("block", block), data: blockData},
		{name: testEntryName("proof", testBlockID(-1, topShard, block.SeqNo+1)), data: []byte{0x02}},
	})
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read test package: %v", err)
	}

	imported, err := ImportStream(context.Background(), &Downloaded{
		MasterchainSeqno: 100,
		Shard:            ShardID{Workchain: -1, Shard: topShard},
	}, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("import archive stream: %v", err)
	}
	if len(imported.FullBlocks) != 0 {
		t.Fatalf("full block count = %d, want none", len(imported.FullBlocks))
	}
}

func TestImportStreamDoesNotLinkPartialBlockEntries(t *testing.T) {
	block, blockData := readMasterchainBlockFixture(t)
	partial := testBlockID(block.Workchain, block.Shard, block.SeqNo+1)
	path := writeTestPackage(t, []testEntry{
		{name: testEntryName("block", block), data: blockData},
		{name: testEntryName("proof", block), data: []byte{0x02}},
		{name: testEntryName("block", partial), data: []byte{0x42}},
	})
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read test package: %v", err)
	}

	imported, err := ImportStream(context.Background(), &Downloaded{
		MasterchainSeqno: 100,
		Shard:            ShardID{Workchain: -1, Shard: topShard},
	}, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("import archive stream: %v", err)
	}
	if len(imported.FullBlocks) != 1 {
		t.Fatalf("full block count = %d, want 1", len(imported.FullBlocks))
	}
	if len(imported.Links) != 0 {
		t.Fatalf("links = %#v, want none", imported.Links)
	}
	if imported.Stats.Blocks != 2 || imported.Stats.FullBlocks != 1 || imported.Stats.Links != 0 {
		t.Fatalf("unexpected stats: %#v", imported.Stats)
	}
}

func TestImportStreamMasterFullBlockIgnoresProofLinkOrder(t *testing.T) {
	block, blockData := readMasterchainBlockFixture(t)

	tests := []struct {
		name    string
		entries []testEntry
	}{
		{
			name: "prooflink before proof",
			entries: []testEntry{
				{name: testEntryName("prooflink", block), data: []byte{0x01}},
				{name: testEntryName("block", block), data: blockData},
				{name: testEntryName("proof", block), data: []byte{0x02}},
			},
		},
		{
			name: "proof before prooflink",
			entries: []testEntry{
				{name: testEntryName("proof", block), data: []byte{0x02}},
				{name: testEntryName("block", block), data: blockData},
				{name: testEntryName("prooflink", block), data: []byte{0x01}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeTestPackage(t, tt.entries)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read test package: %v", err)
			}

			imported, err := ImportBytes(context.Background(), &Downloaded{
				MasterchainSeqno: 100,
				ArchiveID:        779,
				Shard:            ShardID{Workchain: -1, Shard: topShard},
			}, data)
			if err != nil {
				t.Fatalf("import archive bytes: %v", err)
			}
			if len(imported.FullBlocks) != 1 {
				t.Fatalf("full block count = %d, want 1", len(imported.FullBlocks))
			}
			full := imported.FullBlocks[0]
			if full.IsLink || !bytes.Equal(full.Proof, []byte{0x02}) {
				t.Fatalf("full proof = %x is_link=%v, want canonical proof", full.Proof, full.IsLink)
			}
		})
	}
}

func TestCanonicalArchiveProofPrefersShardProofLink(t *testing.T) {
	shard := testBlockID(0, topShard, 73)
	proof, isLink, ok := canonicalArchiveProof(&blockParts{
		id:        shard,
		proof:     []byte{0x01},
		proofLink: []byte{0x02},
	})
	if !ok || !isLink || !bytes.Equal(proof, []byte{0x02}) {
		t.Fatalf("canonical proof = %x is_link=%v ok=%v, want shard proof link", proof, isLink, ok)
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
	var stateRootHash cell.Hash
	copy(stateRootHash[:], prepared.State.StateRootHash)
	if !prepared.StateUpdateToCells.Has(stateRootHash) {
		t.Fatalf("prepared update_to cells do not contain state root %x", prepared.State.StateRootHash)
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

func TestImportBytesSeqRangeTracksRequestedShard(t *testing.T) {
	master, masterData := readMasterchainBlockFixture(t)
	path := writeTestPackage(t, []testEntry{
		{name: testEntryName("block", master), data: masterData},
		{name: testEntryName("proof", master), data: []byte{0x21}},
	})
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read test package: %v", err)
	}

	imported, err := ImportBytes(context.Background(), &Downloaded{
		MasterchainSeqno: master.SeqNo,
		Shard:            ShardID{Workchain: -1, Shard: topShard},
	}, data)
	if err != nil {
		t.Fatalf("import archive: %v", err)
	}
	stats := imported.Stats
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
