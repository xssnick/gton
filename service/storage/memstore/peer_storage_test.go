package memstore

import (
	"context"
	"errors"
	"testing"

	"flexserver/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

const topShard = int64(-1 << 63)

func TestPeerStoreSaveBlockFullAlsoServesDataAndProof(t *testing.T) {
	store := NewPeerStore()
	block := ton.BlockIDExt{Workchain: -1, Shard: topShard, SeqNo: 7}

	err := store.SaveBlockFull(&storage.ServedBlockFull{
		ID:     block,
		Block:  []byte{0x01, 0x02, 0x03},
		Proof:  []byte{0xAA, 0xBB},
		IsLink: false,
	})
	if err != nil {
		t.Fatalf("save block full: %v", err)
	}

	data, err := store.BlockData(context.Background(), block)
	if err != nil {
		t.Fatalf("load block data: %v", err)
	}
	if string(data) != string([]byte{0x01, 0x02, 0x03}) {
		t.Fatalf("unexpected block data: %x", data)
	}

	proof, err := store.BlockProof(context.Background(), storage.ServedProofBlock, block)
	if err != nil {
		t.Fatalf("load block proof: %v", err)
	}
	if string(proof) != string([]byte{0xAA, 0xBB}) {
		t.Fatalf("unexpected block proof: %x", proof)
	}
}

func TestPeerStoreReturnsErrNotFound(t *testing.T) {
	store := NewPeerStore()
	block := ton.BlockIDExt{Workchain: -1, Shard: topShard, SeqNo: 77}

	_, err := store.BlockFull(context.Background(), block)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
