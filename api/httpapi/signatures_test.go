package httpapi

import (
	"bytes"
	"encoding/base64"
	"testing"

	"github.com/xssnick/tonutils-go/ton"
)

func TestHTTPBlockSignaturesFormatsOrdinaryAndSimplex(t *testing.T) {
	id := ton.BlockIDExt{
		Workchain: masterchainWorkchain,
		Shard:     masterchainShard,
		SeqNo:     7,
		RootHash:  bytes.Repeat([]byte{0x11}, 32),
		FileHash:  bytes.Repeat([]byte{0x22}, 32),
	}
	sig := ton.Signature{
		NodeIDShort: bytes.Repeat([]byte{0x33}, 32),
		Signature:   bytes.Repeat([]byte{0x44}, 64),
	}

	ordinary := httpBlockSignatures(id, ton.SignatureSetOrdinary{Signatures: []ton.Signature{sig}}).(blockSignatures)
	if ordinary.Type != blockSignaturesType {
		t.Fatalf("ordinary type = %s", ordinary.Type)
	}
	if ordinary.ID.Seqno != 7 || ordinary.ID.RootHash != tonHash(id.RootHash) {
		t.Fatalf("unexpected block id: %+v", ordinary.ID)
	}
	if ordinary.Signatures[0].Type != blockSignatureType {
		t.Fatalf("signature type = %s", ordinary.Signatures[0].Type)
	}
	if ordinary.Signatures[0].NodeIDShort != tonHash(sig.NodeIDShort) {
		t.Fatalf("node_id_short = %s", ordinary.Signatures[0].NodeIDShort)
	}
	if ordinary.Signatures[0].Signature != base64.StdEncoding.EncodeToString(sig.Signature) {
		t.Fatalf("signature = %s", ordinary.Signatures[0].Signature)
	}

	simplex := httpBlockSignatures(id, ton.SignatureSetSimplex{
		Signatures: []ton.Signature{sig},
		SessionID:  bytes.Repeat([]byte{0x55}, 32),
		Slot:       3,
		Candidate:  []byte{1, 2, 3},
	}).(blockSignaturesSimplex)
	if simplex.Type != blockSignaturesSimplexType {
		t.Fatalf("simplex type = %s", simplex.Type)
	}
	if simplex.SessionID != tonHash(bytes.Repeat([]byte{0x55}, 32)) {
		t.Fatalf("session id = %s", simplex.SessionID)
	}
	if simplex.Slot != 3 {
		t.Fatalf("slot = %d", simplex.Slot)
	}
	if simplex.Candidate != base64.StdEncoding.EncodeToString([]byte{1, 2, 3}) {
		t.Fatalf("candidate = %s", simplex.Candidate)
	}
}
