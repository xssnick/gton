package p2p

import (
	"bytes"
	"testing"

	"github.com/xssnick/tonutils-go/ton"
)

func TestValidateDownloadedProofAcceptsKeyProofLink(t *testing.T) {
	block, _, link := testPeerMasterBlockProof(t, 170)

	if err := validateDownloadedProof(block, link, true, true); err != nil {
		t.Fatalf("validate key proof link: %v", err)
	}
}

func TestValidateDownloadedProofRejectsMalformedProof(t *testing.T) {
	block := testStoredMasterBlockID(171)

	if err := validateDownloadedProof(block, []byte{0x01, 0x02}, false, false); err == nil {
		t.Fatal("malformed proof was accepted")
	}
}

func TestValidateDownloadedProofRejectsProofForMismatch(t *testing.T) {
	block, proof, _ := testPeerMasterBlockProof(t, 173)
	requested := block
	requested.SeqNo++

	if err := validateDownloadedProof(requested, proof, false, false); err == nil {
		t.Fatal("proof for another block was accepted")
	}
}

func TestValidateDownloadedProofRejectsShardFullProof(t *testing.T) {
	root := testPeerBlockRoot(t, 0, topShard, 172)
	rootHash := root.HashKey()
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     topShard,
		SeqNo:     172,
		RootHash:  bytes.Clone(rootHash[:]),
		FileHash:  bytes.Repeat([]byte{0x72}, 32),
	}
	proof := testPeerBlockProofEnvelopeBOC(t, block, root, nil)

	if err := validateDownloadedProof(block, proof, false, false); err == nil {
		t.Fatal("non-masterchain full proof was accepted")
	}
}
