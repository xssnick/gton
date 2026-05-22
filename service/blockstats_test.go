package service

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"strconv"
	"testing"

	"github.com/xssnick/gton/service/p2p"
	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type blockFixture struct {
	Block struct {
		Workchain   int32  `json:"workchain"`
		Shard       string `json:"shard"`
		SeqNo       uint32 `json:"seqno"`
		RootHashHex string `json:"root_hash_hex"`
		FileHashHex string `json:"file_hash_hex"`
	} `json:"block"`
	RawBOCBase64 string `json:"raw_boc_base64"`
}

func TestStatsFromDownloadedBlockFixture(t *testing.T) {
	rawFixture, err := os.ReadFile("testdata/masterchain_block_fixture.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}

	var fixture blockFixture
	if err = json.Unmarshal(rawFixture, &fixture); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}

	blockData, err := base64.StdEncoding.DecodeString(fixture.RawBOCBase64)
	if err != nil {
		t.Fatalf("decode block boc base64: %v", err)
	}
	blockCell, err := cell.FromBOC(blockData)
	if err != nil {
		t.Fatalf("parse block boc: %v", err)
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

	blockID := ton.BlockIDExt{
		Workchain: fixture.Block.Workchain,
		Shard:     int64(shard),
		SeqNo:     fixture.Block.SeqNo,
		RootHash:  rootHash,
		FileHash:  fileHash,
	}

	stats, err := StatsFromDownloadedBlock(p2p.DownloadedBlock{
		ID:    blockID,
		Block: blockCell,
	})
	if err != nil {
		t.Fatalf("collect block stats: %v", err)
	}

	if stats.ID.Workchain != fixture.Block.Workchain {
		t.Fatalf("unexpected workchain %d", stats.ID.Workchain)
	}
	if uint64(stats.ID.Shard) != shard {
		t.Fatalf("unexpected shard %016x", uint64(stats.ID.Shard))
	}
	if stats.ID.SeqNo != fixture.Block.SeqNo {
		t.Fatalf("unexpected seqno %d", stats.ID.SeqNo)
	}
	if stats.Transactions <= 0 {
		t.Fatalf("expected positive transaction count, got %d", stats.Transactions)
	}

	txCount, err := tnstore.BlockTransactionCountFromBlockData(blockID, blockData)
	if err != nil {
		t.Fatalf("count block transactions: %v", err)
	}
	if txCount != uint32(stats.Transactions) {
		t.Fatalf("transaction count = %d, want %d", txCount, stats.Transactions)
	}
}
