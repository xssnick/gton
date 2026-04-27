package service

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flexserver/service/p2p"
	tnstore "flexserver/service/storage"
	"os"
	"strconv"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestApplyBlockFromFixture(t *testing.T) {
	downloaded := mustLoadFixtureDownloadedBlock(t)

	var block tlb.Block
	if err := tlb.LoadFromCell(&block, downloaded.Block.BeginParse()); err != nil {
		t.Fatalf("load fixture tlb block: %v", err)
	}

	oldStateCell := block.StateUpdate.MustPeekRef(0)
	var oldParsed tlb.ShardStateUnsplit
	if err := tlb.LoadFromCell(&oldParsed, oldStateCell.BeginParse()); err != nil {
		t.Fatalf("parse old state from update: %v", err)
	}

	oldWC, oldShard := tlb.ConvertShardIdentToShard(oldParsed.ShardIdent)
	oldStateHash := oldStateCell.HashKey(0)
	current, err := tnstore.ParseStateCell(&ton.BlockIDExt{
		Workchain: oldWC,
		Shard:     int64(oldShard),
		SeqNo:     oldParsed.Seqno,
	}, oldStateCell, oldStateCell.ToBOCWithOptions(cell.BOCOptions{}), oldStateHash[:], nil)
	if err != nil {
		t.Fatalf("build current state from old update branch: %v", err)
	}

	next, err := ApplyBlock(current, *downloaded)
	if err != nil {
		t.Fatalf("apply block: %v", err)
	}

	if !next.Block.Equals(&downloaded.ID) {
		t.Fatalf("unexpected next block id %s", tnstore.FormatBlockRef(next.Block))
	}
	if next.Parsed == nil {
		t.Fatal("expected parsed next state")
	}
	if next.Parsed.Seqno != downloaded.ID.SeqNo {
		t.Fatalf("unexpected next state seqno %d", next.Parsed.Seqno)
	}

	newStateCell := block.StateUpdate.MustPeekRef(1)
	newStateHash := newStateCell.HashKey(0)
	if got, want := hex.EncodeToString(next.StateRootHash), hex.EncodeToString(newStateHash[:]); got != want {
		t.Fatalf("unexpected next state root hash %s want %s", got, want)
	}
}

func TestApplyBlockRejectsWrongCurrentState(t *testing.T) {
	downloaded := mustLoadFixtureDownloadedBlock(t)

	current := &tnstore.BlockState{
		Block: ton.BlockIDExt{
			Workchain: -1,
			Shard:     topShard,
			SeqNo:     downloaded.ID.SeqNo - 1,
		},
		StateRootHash: bytes32(0x42),
	}

	if _, err := ApplyBlock(current, *downloaded); err == nil {
		t.Fatal("expected apply block to reject wrong current state")
	}
}

func mustLoadFixtureDownloadedBlock(tb testing.TB) *p2p.DownloadedBlock {
	tb.Helper()

	rawFixture, err := os.ReadFile("testdata/masterchain_block_fixture.json")
	if err != nil {
		tb.Fatalf("read fixture: %v", err)
	}

	var fixture blockFixture
	if err = json.Unmarshal(rawFixture, &fixture); err != nil {
		tb.Fatalf("decode fixture: %v", err)
	}

	blockData, err := base64.StdEncoding.DecodeString(fixture.RawBOCBase64)
	if err != nil {
		tb.Fatalf("decode block boc base64: %v", err)
	}
	blockCell, err := cell.FromBOC(blockData)
	if err != nil {
		tb.Fatalf("parse block boc: %v", err)
	}

	rootHash, err := hex.DecodeString(fixture.Block.RootHashHex)
	if err != nil {
		tb.Fatalf("decode root hash: %v", err)
	}

	fileHash, err := hex.DecodeString(fixture.Block.FileHashHex)
	if err != nil {
		tb.Fatalf("decode file hash: %v", err)
	}

	shard, err := strconv.ParseUint(fixture.Block.Shard, 16, 64)
	if err != nil {
		tb.Fatalf("parse shard: %v", err)
	}

	return &p2p.DownloadedBlock{
		ID: ton.BlockIDExt{
			Workchain: fixture.Block.Workchain,
			Shard:     int64(shard),
			SeqNo:     fixture.Block.SeqNo,
			RootHash:  rootHash,
			FileHash:  fileHash,
		},
		Block: blockCell,
	}
}

func bytes32(fill byte) []byte {
	out := make([]byte, 32)
	for i := range out {
		out[i] = fill
	}
	return out
}
