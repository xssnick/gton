package state

import (
	"bytes"
	"errors"
	"testing"

	"flexserver/service/archive"
	"flexserver/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

func TestTrustedInitProofFromArchiveImportUsesProofLink(t *testing.T) {
	block := ton.BlockIDExt{Workchain: -1, Shard: masterchainShard, SeqNo: 42}
	proof := []byte{1, 2, 3}

	got, err := trustedInitProofFromArchiveImport(block, &archive.Imported{
		ServedArchiveImport: storage.ServedArchiveImport{
			Proofs: []storage.ServedBlockProof{
				{Kind: storage.ServedProofBlock, ID: block, Data: []byte{9}},
				{Kind: storage.ServedProofBlockLink, ID: block, Data: proof},
			},
		},
	})
	if err != nil {
		t.Fatalf("trusted init proof: %v", err)
	}
	if !bytes.Equal(got, proof) {
		t.Fatalf("unexpected proof %x, want %x", got, proof)
	}
}

func TestTrustedInitProofFromArchiveImportUsesFullBlockProof(t *testing.T) {
	block := ton.BlockIDExt{Workchain: -1, Shard: masterchainShard, SeqNo: 42}
	proof := []byte{4, 5, 6}

	got, err := trustedInitProofFromArchiveImport(block, &archive.Imported{
		ServedArchiveImport: storage.ServedArchiveImport{
			FullBlocks: []*storage.ServedBlockFull{{ID: block, Proof: proof}},
		},
	})
	if err != nil {
		t.Fatalf("trusted init proof: %v", err)
	}
	if !bytes.Equal(got, proof) {
		t.Fatalf("unexpected proof %x, want %x", got, proof)
	}
}

func TestTrustedInitProofFromArchiveImportMissing(t *testing.T) {
	block := ton.BlockIDExt{Workchain: -1, Shard: masterchainShard, SeqNo: 42}

	if _, err := trustedInitProofFromArchiveImport(block, &archive.Imported{}); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("trusted init proof error = %v, want ErrNotFound", err)
	}
}
