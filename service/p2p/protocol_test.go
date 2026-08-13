package p2p

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xssnick/gton/service/storage"

	"github.com/pierrec/lz4/v4"
	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/keys"
	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestCustomBroadcastTypesRoundTrip(t *testing.T) {
	msg := tonnodeapi.BlockBroadcastCompressed{
		ID: ton.BlockIDExt{
			Workchain: 0,
			Shard:     topShard,
			SeqNo:     123,
			RootHash:  make([]byte, 32),
			FileHash:  make([]byte, 32),
		},
		CatchainSeqno:    10,
		ValidatorSetHash: 20,
		Flags:            0,
		Compressed:       []byte{1, 2, 3},
	}

	serialized, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize compressed broadcast: %v", err)
	}

	var parsed any
	if _, err = tl.Parse(&parsed, serialized, true); err != nil {
		t.Fatalf("parse compressed broadcast: %v", err)
	}

	if _, ok := parsed.(tonnodeapi.BlockBroadcastCompressed); !ok {
		t.Fatalf("unexpected type after parse: %T", parsed)
	}
}

func TestCompressedV2BroadcastSignatureSetDecodesAsValueType(t *testing.T) {
	msg := tonnodeapi.BlockBroadcastCompressedV2{
		ID: ton.BlockIDExt{
			Workchain: -1,
			Shard:     topShard,
			SeqNo:     124,
			RootHash:  make([]byte, 32),
			FileHash:  make([]byte, 32),
		},
		SignatureSet: tonnodeapi.SignatureSetOrdinary{
			CatchainSeqno:    10,
			ValidatorSetHash: 20,
			Signatures:       []tonnodeapi.BlockSignature{},
		},
		Flags:          0,
		Proof:          []byte{1, 2, 3},
		DataCompressed: []byte{4, 5, 6},
	}

	serialized, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize compressed v2 broadcast: %v", err)
	}

	var parsed any
	if _, err = tl.Parse(&parsed, serialized, true); err != nil {
		t.Fatalf("parse compressed v2 broadcast: %v", err)
	}

	broadcast, ok := parsed.(tonnodeapi.BlockBroadcastCompressedV2)
	if !ok {
		t.Fatalf("unexpected type after parse: %T", parsed)
	}
	if _, ok = broadcast.SignatureSet.(tonnodeapi.SignatureSetOrdinary); !ok {
		t.Fatalf("unexpected signature set type after parse: %T", broadcast.SignatureSet)
	}
}

func TestBlockFinalityBroadcastRoundTrip(t *testing.T) {
	msg := BlockFinalityBroadcast{
		ID: ton.BlockIDExt{
			Workchain: -1,
			Shard:     topShard,
			SeqNo:     125,
			RootHash:  bytes.Repeat([]byte{0x11}, 32),
			FileHash:  bytes.Repeat([]byte{0x22}, 32),
		},
		SignatureSet: tonnodeapi.SignatureSetSimplex{
			Final:            true,
			CatchainSeqno:    10,
			ValidatorSetHash: 20,
			Signatures: []tonnodeapi.BlockSignature{{
				Who:       bytes.Repeat([]byte{0x33}, 32),
				Signature: bytes.Repeat([]byte{0x44}, ed25519.SignatureSize),
			}},
			SessionID: bytes.Repeat([]byte{0x55}, 32),
			Slot:      3,
			Candidate: ton.ConsensusCandidateHashDataOrdinary{
				Block:            ton.BlockIDExt{RootHash: make([]byte, 32), FileHash: make([]byte, 32)},
				CollatedFileHash: bytes.Repeat([]byte{0x66}, 32),
				Parent:           ton.ConsensusCandidateWithoutParents{},
			},
		},
	}

	serialized, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize block finality broadcast: %v", err)
	}

	var parsed any
	if _, err = tl.Parse(&parsed, serialized, true); err != nil {
		t.Fatalf("parse block finality broadcast: %v", err)
	}

	broadcast, ok := parsed.(BlockFinalityBroadcast)
	if !ok {
		t.Fatalf("unexpected type after parse: %T", parsed)
	}
	if !broadcast.ID.Equals(&msg.ID) {
		t.Fatalf("unexpected block after parse: %s", storage.FormatBlockRef(broadcast.ID))
	}
	if _, ok = broadcast.SignatureSet.(tonnodeapi.SignatureSetSimplex); !ok {
		t.Fatalf("unexpected signature set type after parse: %T", broadcast.SignatureSet)
	}
}

func TestDownloadTypesRoundTrip(t *testing.T) {
	msg := DataFullCompressed{
		ID: ton.BlockIDExt{
			Workchain: -1,
			Shard:     topShard,
			SeqNo:     777,
			RootHash:  bytes.Repeat([]byte{0x11}, 32),
			FileHash:  bytes.Repeat([]byte{0x22}, 32),
		},
		Flags:      0,
		Compressed: []byte{1, 2, 3, 4},
		IsLink:     false,
	}

	serialized, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize compressed data full: %v", err)
	}

	var parsed any
	if _, err = tl.Parse(&parsed, serialized, true); err != nil {
		t.Fatalf("parse compressed data full: %v", err)
	}

	if _, ok := parsed.(DataFullCompressed); !ok {
		t.Fatalf("unexpected type after parse: %T", parsed)
	}
}

func TestBlockDescriptionTypesRoundTrip(t *testing.T) {
	msg := GetNextBlockDescription{
		PrevBlock: ton.BlockIDExt{
			Workchain: -1,
			Shard:     topShard,
			SeqNo:     42,
			RootHash:  bytes.Repeat([]byte{0x11}, 32),
			FileHash:  bytes.Repeat([]byte{0x22}, 32),
		},
	}

	serialized, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize getNextBlockDescription: %v", err)
	}

	var parsed any
	if _, err = tl.Parse(&parsed, serialized, true); err != nil {
		t.Fatalf("parse getNextBlockDescription: %v", err)
	}
	if _, ok := parsed.(GetNextBlockDescription); !ok {
		t.Fatalf("unexpected type after parse: %T", parsed)
	}
}

func TestSlaveAndOutMsgQueueTypesRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		msg  any
		want any
	}{
		{
			name: "sendExtMessage",
			msg: SendExtMessage{
				Message: tonnodeapi.ExternalMessage{Data: []byte{1, 2, 3}},
			},
			want: SendExtMessage{},
		},
		{
			name: "success",
			msg:  Success{},
			want: Success{},
		},
		{
			name: "getOutMsgQueueProof",
			msg: GetOutMsgQueueProof{
				DstShard: tonnodeapi.ShardID{Workchain: 0, Shard: topShard},
				Blocks: []ton.BlockIDExt{{
					Workchain: 0,
					Shard:     topShard,
					SeqNo:     10,
					RootHash:  bytes.Repeat([]byte{0x10}, 32),
					FileHash:  bytes.Repeat([]byte{0x11}, 32),
				}},
				Limits: ImportedMsgQueueLimits{MaxBytes: 4096, MaxMsgs: 32},
			},
			want: GetOutMsgQueueProof{},
		},
		{
			name: "outMsgQueueProof",
			msg: OutMsgQueueProof{
				QueueProofs:      []byte{1},
				BlockStateProofs: []byte{2},
				MessageCounts:    []int32{3},
			},
			want: OutMsgQueueProof{},
		},
		{
			name: "outMsgQueueProofEmpty",
			msg:  OutMsgQueueProofEmpty{},
			want: OutMsgQueueProofEmpty{},
		},
		{
			name: "outMsgQueueProofBroadcast",
			msg: OutMsgQueueProofBroadcast{
				DstShard: tonnodeapi.ShardID{
					Workchain: 0,
					Shard:     topShard,
				},
				Block: ton.BlockIDExt{
					Workchain: 0,
					Shard:     topShard,
					SeqNo:     11,
					RootHash:  bytes.Repeat([]byte{0x12}, 32),
					FileHash:  bytes.Repeat([]byte{0x13}, 32),
				},
				Limits: ImportedMsgQueueLimits{
					MaxBytes: 4096,
					MaxMsgs:  32,
				},
				Proof: OutMsgQueueProofEmpty{},
			},
			want: OutMsgQueueProofBroadcast{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			serialized, err := tl.Serialize(tc.msg, true)
			if err != nil {
				t.Fatalf("serialize: %v", err)
			}

			var parsed any
			if _, err = tl.Parse(&parsed, serialized, true); err != nil {
				t.Fatalf("parse: %v", err)
			}
			if fmt.Sprintf("%T", parsed) != fmt.Sprintf("%T", tc.want) {
				t.Fatalf("unexpected type after parse: %T", parsed)
			}
		})
	}
}

func TestSimpleBroadcastSignatureMatchesReferenceShape(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	inner := tonnodeapi.NewShardBlockBroadcast{
		Block: tonnodeapi.NewShardBlock{
			ID: ton.BlockIDExt{
				Workchain: 0,
				Shard:     topShard,
				SeqNo:     7,
				RootHash:  make([]byte, 32),
				FileHash:  make([]byte, 32),
			},
			CCSeqno: 1,
			Data:    []byte{0xAA},
		},
	}

	payload, err := tl.Serialize(inner, true)
	if err != nil {
		t.Fatalf("serialize payload: %v", err)
	}

	sourceID, err := tl.Hash(keys.PublicKeyED25519{Key: pub})
	if err != nil {
		t.Fatalf("hash source id: %v", err)
	}
	hash, err := tl.Hash(OverlayBroadcastID{
		Source:   sourceID,
		DataHash: hashSimpleBroadcastPayload(payload),
		Flags:    0,
	})
	if err != nil {
		t.Fatalf("hash broadcast id: %v", err)
	}
	now := uint32(time.Now().Unix())

	toSign, err := tl.Serialize(overlay.BroadcastToSign{Hash: hash, Date: now}, true)
	if err != nil {
		t.Fatalf("serialize toSign: %v", err)
	}

	msg := overlay.Broadcast{
		Source:      keys.PublicKeyED25519{Key: pub},
		Certificate: overlay.CertificateEmpty{},
		Flags:       0,
		Data:        payload,
		Date:        int32(now),
		Signature:   ed25519.Sign(priv, toSign),
	}

	overlayID := testPeerID("reference-shape-overlay")
	receiver, err := overlay.NewBroadcastReceiver(overlayID[:], maxOverlayPayloadSize, true, false)
	if err != nil {
		t.Fatalf("create broadcast receiver: %v", err)
	}
	t.Cleanup(receiver.Close)
	received := false
	receiver.SetBroadcastHandlerWithInfo(func(got tl.Serializable, info overlay.BroadcastInfo) overlay.BroadcastDisposition {
		parsed, ok := got.(tonnodeapi.NewShardBlockBroadcast)
		if !ok || parsed.Block.ID.SeqNo != inner.Block.ID.SeqNo {
			t.Fatalf("unexpected decoded broadcast: %#v", got)
		}
		if !bytes.Equal(info.SourceID, sourceID) || info.Delivery != overlay.BroadcastDeliverySimple {
			t.Fatalf("unexpected broadcast info: %#v", info)
		}
		received = true
		return overlay.BroadcastDispositionAcceptAndRelay
	})

	base := newTestOverlayADNL()
	transport := overlay.CreateExtendedADNL(base)
	if _, err = transport.AttachOverlay(receiver); err != nil {
		t.Fatalf("attach broadcast receiver: %v", err)
	}
	if err = base.customHandler(&adnl.MessageCustom{Data: []tl.Serializable{
		overlay.Message{Overlay: overlayID[:]},
		msg,
	}}); err != nil {
		t.Fatalf("handle simple broadcast: %v", err)
	}
	if !received {
		t.Fatal("signed broadcast was not delivered")
	}
}

func TestDecodeCompressedBlock(t *testing.T) {
	blockCell := testPeerBlockRoot(t, 0, 1)
	blockData := serializeCompressedBlockRoot(blockCell)
	fileHash := sha256.Sum256(blockData)
	blockHash := blockCell.HashKey()
	id := ton.BlockIDExt{
		Workchain: 0,
		Shard:     topShard,
		SeqNo:     1,
		RootHash:  blockHash[:],
		FileHash:  fileHash[:],
	}
	proofCell := testBlockProofCell(t, id, nil)

	multiRoot := cell.ToBOCWithOptions([]*cell.Cell{proofCell, blockCell}, cell.BOCSerializeOptions{WithCRC32C: true})
	compressed := make([]byte, lz4.CompressBlockBound(len(multiRoot)))
	n, err := lz4.CompressBlock(multiRoot, compressed, nil)
	if err != nil {
		t.Fatalf("compress multi-root boc: %v", err)
	}
	compressed = compressed[:n]

	res, err := decodeCompressedBlock(DataFullCompressed{
		ID:         id,
		Compressed: compressed,
		IsLink:     true,
	})
	if err != nil {
		t.Fatalf("decode compressed block: %v", err)
	}

	if res.Kind != "tonNode.dataFullCompressed" {
		t.Fatalf("unexpected kind %q", res.Kind)
	}
	if !res.VerifiedRootHash {
		t.Fatalf("expected root hash verification")
	}

	if res.Proof == nil {
		t.Fatalf("missing parsed proof")
	}
	if res.Proof.HashKey() != proofCell.HashKey() {
		t.Fatalf("proof hash mismatch")
	}

	if res.Block == nil {
		t.Fatalf("missing parsed block")
	}
	if res.Block.HashKey() != blockCell.HashKey() {
		t.Fatalf("block hash mismatch")
	}
}

func TestDecodeCompressedBlockV2(t *testing.T) {
	blockCell := testPeerBlockRoot(t, 0, 2)
	blockData := serializeCompressedBlockRoot(blockCell)
	fileHash := sha256.Sum256(blockData)
	blockHash := blockCell.HashKey()
	id := ton.BlockIDExt{
		Workchain: 0,
		Shard:     topShard,
		SeqNo:     2,
		RootHash:  blockHash[:],
		FileHash:  fileHash[:],
	}
	proofCell := testBlockProofCell(t, id, nil)

	compressed, err := cell.CompressBOC([]*cell.Cell{blockCell}, cell.CompressionImprovedStructureLZ4, nil)
	if err != nil {
		t.Fatalf("compress block boc v2: %v", err)
	}

	res, err := decodeCompressedBlockV2(DataFullCompressedV2{
		ID:              id,
		Proof:           proofCell.ToBOC(),
		BlockCompressed: compressed,
		IsLink:          true,
	}, nil)
	if err != nil {
		t.Fatalf("decode compressed block v2: %v", err)
	}

	if res.Kind != "tonNode.dataFullCompressedV2" {
		t.Fatalf("unexpected kind %q", res.Kind)
	}
	if !res.VerifiedRootHash {
		t.Fatalf("expected root hash verification")
	}
	if res.Block == nil || res.Block.HashKey() != blockCell.HashKey() {
		t.Fatalf("block hash mismatch")
	}
}

func TestDecodeShardBlockBroadcastCompressedV2AllowsNonFinalSimplex(t *testing.T) {
	blockCell := testPeerBlockRoot(t, 0, 3)
	blockData := serializeCompressedBlockRoot(blockCell)
	fileHash := sha256.Sum256(blockData)
	blockHash := blockCell.HashKey()
	id := ton.BlockIDExt{
		Workchain: 0,
		Shard:     topShard,
		SeqNo:     3,
		RootHash:  blockHash[:],
		FileHash:  fileHash[:],
	}
	proofCell := testBlockProofCell(t, id, nil)

	compressed, err := cell.CompressBOC([]*cell.Cell{blockCell}, cell.CompressionImprovedStructureLZ4, nil)
	if err != nil {
		t.Fatalf("compress shard block broadcast v2: %v", err)
	}

	res, err := decodeBlockBroadcastCompressedV2(tonnodeapi.BlockBroadcastCompressedV2{
		ID: id,
		SignatureSet: tonnodeapi.SignatureSetSimplex{
			Final:     false,
			SessionID: bytes.Repeat([]byte{0x44}, 32),
			Candidate: ton.ConsensusCandidateHashDataOrdinary{
				Block:            id,
				CollatedFileHash: bytes.Repeat([]byte{0x46}, 32),
				Parent:           ton.ConsensusCandidateWithoutParents{},
			},
		},
		Proof:          proofCell.ToBOC(),
		DataCompressed: compressed,
	}, nil)
	if err != nil {
		t.Fatalf("decode shard block broadcast v2: %v", err)
	}
	if res.Kind != "tonNode.blockBroadcastCompressedV2" {
		t.Fatalf("unexpected kind %q", res.Kind)
	}
	if !res.VerifiedRootHash {
		t.Fatalf("expected root hash verification")
	}
}

func TestDecodeMasterchainBlockBroadcastCompressedV2RejectsNonFinalSimplex(t *testing.T) {
	blockCell := testPeerBlockRoot(t, -1, 3)
	blockData := serializeCompressedBlockRoot(blockCell)
	fileHash := sha256.Sum256(blockData)
	blockHash := blockCell.HashKey()
	id := ton.BlockIDExt{
		Workchain: -1,
		Shard:     topShard,
		SeqNo:     3,
		RootHash:  blockHash[:],
		FileHash:  fileHash[:],
	}
	proofCell := testBlockProofCell(t, id, nil)

	compressed, err := cell.CompressBOC([]*cell.Cell{blockCell}, cell.CompressionImprovedStructureLZ4, nil)
	if err != nil {
		t.Fatalf("compress masterchain block broadcast v2: %v", err)
	}

	_, err = decodeBlockBroadcastCompressedV2(tonnodeapi.BlockBroadcastCompressedV2{
		ID: id,
		SignatureSet: tonnodeapi.SignatureSetSimplex{
			Final:     false,
			SessionID: bytes.Repeat([]byte{0x45}, 32),
			Candidate: ton.ConsensusCandidateHashDataOrdinary{
				Block:            id,
				CollatedFileHash: bytes.Repeat([]byte{0x47}, 32),
				Parent:           ton.ConsensusCandidateWithoutParents{},
			},
		},
		Proof:          proofCell.ToBOC(),
		DataCompressed: compressed,
	}, nil)
	if !errors.Is(err, errBroadcastSignatureSetNonFinal) {
		t.Fatalf("decode masterchain block broadcast v2 err = %v, want %v", err, errBroadcastSignatureSetNonFinal)
	}
}

func testBlockProofCell(t *testing.T, id ton.BlockIDExt, signatures *cell.Cell) *cell.Cell {
	t.Helper()
	if id.Shard != topShard {
		t.Fatalf("test proof helper supports only top shard, got %016x", uint64(id.Shard))
	}

	return cell.BeginCell().
		MustStoreUInt(0xc3, 8).
		MustStoreUInt(0, 2).
		MustStoreUInt(0, 6).
		MustStoreUInt(uint64(uint32(id.Workchain)), 32).
		MustStoreUInt(0, 64).
		MustStoreUInt(uint64(id.SeqNo), 32).
		MustStoreSlice(id.RootHash, 256).
		MustStoreSlice(id.FileHash, 256).
		MustStoreRef(cell.BeginCell().EndCell()).
		MustStoreMaybeRef(signatures).
		EndCell()
}

func testProofSignatureSet() *cell.Cell {
	return cell.BeginCell().
		MustStoreUInt(0x11, 8).
		MustStoreUInt(0, 32).
		MustStoreUInt(0, 32).
		MustStoreUInt(0, 32).
		MustStoreUInt(0, 64).
		MustStoreDict(nil).
		EndCell()
}
