package p2p

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"flexserver/service/archive"
	"fmt"
	"testing"
	"time"

	"github.com/pierrec/lz4/v4"
	"github.com/xssnick/tonutils-go/adnl/keys"
	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestCustomBroadcastTypesRoundTrip(t *testing.T) {
	msg := BlockBroadcastCompressed{
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

	if _, ok := parsed.(BlockBroadcastCompressed); !ok {
		t.Fatalf("unexpected type after parse: %T", parsed)
	}
}

func TestCompressedV2BroadcastSignatureSetDecodesAsValueType(t *testing.T) {
	msg := BlockBroadcastCompressedV2{
		ID: ton.BlockIDExt{
			Workchain: -1,
			Shard:     topShard,
			SeqNo:     124,
			RootHash:  make([]byte, 32),
			FileHash:  make([]byte, 32),
		},
		SignatureSet: SignatureSetOrdinary{
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

	broadcast, ok := parsed.(BlockBroadcastCompressedV2)
	if !ok {
		t.Fatalf("unexpected type after parse: %T", parsed)
	}
	if _, ok = broadcast.SignatureSet.(SignatureSetOrdinary); !ok {
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
				DstShard: archive.ShardID{Workchain: 0, Shard: topShard},
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

	sub := &overlaySubscription{
		node: newTestNode(t),
		spec: overlaySpec{Name: "test"},
	}
	if !checkSimpleBroadcastDate(int32(msg.Date)) {
		t.Fatalf("test timestamp unexpectedly rejected")
	}
	if err := sub.handleSimpleBroadcast(nil, msg); err != nil {
		t.Fatalf("handle simple broadcast: %v", err)
	}
}

func TestDecodeCompressedBlock(t *testing.T) {
	proofCell := cell.BeginCell().MustStoreUInt(0xAA, 8).EndCell()
	blockCell := cell.BeginCell().MustStoreUInt(0xBB, 8).EndCell()

	multiRoot := cell.ToBOCWithOptions([]*cell.Cell{proofCell, blockCell}, cell.BOCSerializeOptions{WithCRC32C: true})
	compressed := make([]byte, lz4.CompressBlockBound(len(multiRoot)))
	n, err := lz4.CompressBlock(multiRoot, compressed, nil)
	if err != nil {
		t.Fatalf("compress multi-root boc: %v", err)
	}
	compressed = compressed[:n]

	blockData := blockCell.ToBOCWithOptions(cell.BOCSerializeOptions{})
	fileHash := sha256.Sum256(blockData)
	blockHash := blockCell.HashKey()

	res, err := decodeCompressedBlock(DataFullCompressed{
		ID: ton.BlockIDExt{
			Workchain: 0,
			Shard:     topShard,
			SeqNo:     1,
			RootHash:  blockHash[:],
			FileHash:  fileHash[:],
		},
		Compressed: compressed,
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
	proofCell := cell.BeginCell().MustStoreUInt(0xCC, 8).EndCell()
	blockCell := cell.BeginCell().MustStoreUInt(0xDD, 8).EndCell()

	compressed, err := cell.CompressBOC([]*cell.Cell{blockCell}, cell.CompressionImprovedStructureLZ4, nil)
	if err != nil {
		t.Fatalf("compress block boc v2: %v", err)
	}

	blockData := blockCell.ToBOCWithOptions(cell.BOCSerializeOptions{})
	fileHash := sha256.Sum256(blockData)
	blockHash := blockCell.HashKey()

	res, err := decodeCompressedBlockV2(DataFullCompressedV2{
		ID: ton.BlockIDExt{
			Workchain: 0,
			Shard:     topShard,
			SeqNo:     2,
			RootHash:  blockHash[:],
			FileHash:  fileHash[:],
		},
		Proof:           proofCell.ToBOC(),
		BlockCompressed: compressed,
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
