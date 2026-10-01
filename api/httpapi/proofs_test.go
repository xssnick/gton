package httpapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

func TestShardBlockProofMasterchainShape(t *testing.T) {
	master := proofTestBlock(masterchainWorkchain, 11, 0x11)
	store := proofTestStore{
		current: master,
		seqLookup: map[storage.BlockSeqRef]ton.BlockIDExt{
			{Workchain: masterchainWorkchain, Shard: masterchainShard, SeqNo: master.SeqNo}: master,
		},
	}
	server := newTestServer()
	server.store = store

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v2/getShardBlockProof?workchain=-1&shard=-9223372036854775808&seqno=11", nil)

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	result := resultMap(t, decodeResponse(t, rec.Body.Bytes()))
	if result["@type"] != shardBlockProofType {
		t.Fatalf("unexpected @type %v", result["@type"])
	}
	from := nestedMap(t, result, "from")
	if from["seqno"] != float64(11) {
		t.Fatalf("from seqno = %v, want 11", from["seqno"])
	}
	if from["root_hash"] != base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x11}, 32)) {
		t.Fatalf("unexpected from root hash %v", from["root_hash"])
	}
	mcID := nestedMap(t, result, "mc_id")
	if mcID["seqno"] != float64(11) {
		t.Fatalf("mc_id seqno = %v, want 11", mcID["seqno"])
	}
	if links, ok := result["links"].([]any); !ok || len(links) != 0 {
		t.Fatalf("links = %#v, want empty array", result["links"])
	}
	if proof, ok := result["mc_proof"].([]any); !ok || len(proof) != 0 {
		t.Fatalf("mc_proof = %#v, want empty array", result["mc_proof"])
	}
}

func TestShardBlockProofRejectsOldFromMasterchain(t *testing.T) {
	from := proofTestBlock(masterchainWorkchain, 9, 0x09)
	master := proofTestBlock(masterchainWorkchain, 10, 0x10)
	shard := proofTestBlock(0, 20, 0x20)
	store := proofTestStore{
		current: from,
		seqLookup: map[storage.BlockSeqRef]ton.BlockIDExt{
			{Workchain: shard.Workchain, Shard: shard.Shard, SeqNo: shard.SeqNo}:            shard,
			{Workchain: masterchainWorkchain, Shard: masterchainShard, SeqNo: master.SeqNo}: master,
		},
		metas: map[storage.BlockRootHash]*storage.BlockMeta{
			storage.BlockKey(shard): {ID: shard, MasterchainRefSeqno: master.SeqNo},
		},
	}
	server := newTestServer()
	server.store = store

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v2/getShardBlockProof?workchain=0&shard=-9223372036854775808&seqno=20", nil)

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}
	body := decodeResponse(t, rec.Body.Bytes())
	if body.OK {
		t.Fatal("ok = true, want false")
	}
	if body.Error != "from mc block is too old" {
		t.Fatalf("error = %q, want from mc block is too old", body.Error)
	}
}

type proofTestStore struct {
	Store
	current   ton.BlockIDExt
	seqLookup map[storage.BlockSeqRef]ton.BlockIDExt
	metas     map[storage.BlockRootHash]*storage.BlockMeta
}

func (s proofTestStore) CurrentMasterchainInfo(context.Context) (ton.BlockIDExt, []byte, uint32, error) {
	return s.current, nil, 0, nil
}

func (s proofTestStore) LookupBlockBySeqNo(_ context.Context, ref storage.BlockSeqRef) (ton.BlockIDExt, error) {
	block, ok := s.seqLookup[ref]
	if !ok {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	return block, nil
}

func (s proofTestStore) BlockMeta(_ context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error) {
	meta := s.metas[storage.BlockKey(block)]
	if meta == nil {
		return nil, storage.ErrNotFound
	}
	return meta, nil
}

func proofTestBlock(workchain int32, seqno uint32, fill byte) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: workchain,
		Shard:     masterchainShard,
		SeqNo:     seqno,
		RootHash:  bytes.Repeat([]byte{fill}, 32),
		FileHash:  bytes.Repeat([]byte{fill + 1}, 32),
	}
}
