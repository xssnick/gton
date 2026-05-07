package blockproof

import (
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const testTopShard = int64(-1 << 63)

func TestCheckProofShapeMatchesCXXProofAndProofLinkRules(t *testing.T) {
	master := testBlockID(-1, 10)
	shard := testBlockID(0, 10)
	signatures := testSignatureSet(0x12345678, 33, 100)

	tests := []struct {
		name       string
		id         ton.BlockIDExt
		isLink     bool
		signatures *cell.Cell
		wantErr    bool
	}{
		{name: "master proof with signatures", id: master, signatures: signatures},
		{name: "master proof without signatures", id: master, wantErr: true},
		{name: "master proof link without signatures", id: master, isLink: true},
		{name: "master proof link with signatures", id: master, isLink: true, signatures: signatures, wantErr: true},
		{name: "shard proof link without signatures", id: shard, isLink: true},
		{name: "shard proof link with signatures", id: shard, isLink: true, signatures: signatures, wantErr: true},
		{name: "shard full proof", id: shard, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := testBlockProof(t, tt.id, tt.signatures)
			err := CheckProofShape(tt.id, root, tt.isLink)
			if (err != nil) != tt.wantErr {
				t.Fatalf("CheckProofShape err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func testBlockID(workchain int32, seqno uint32) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: workchain,
		Shard:     testTopShard,
		SeqNo:     seqno,
		RootHash:  make([]byte, 32),
		FileHash:  make([]byte, 32),
	}
}

func testBlockProof(t *testing.T, id ton.BlockIDExt, signatures *cell.Cell) *cell.Cell {
	t.Helper()

	root, err := tlb.ToCell(&BlockProof{
		ProofFor: blockIDExtTLB{
			ShardID: tlb.ShardIdent{
				PrefixBits:  0,
				WorkchainID: id.Workchain,
				ShardPrefix: 0,
			},
			SeqNo:    id.SeqNo,
			RootHash: id.RootHash,
			FileHash: id.FileHash,
		},
		Root:       cell.BeginCell().EndCell(),
		Signatures: signatures,
	})
	if err != nil {
		t.Fatalf("serialize block proof: %v", err)
	}
	return root
}

func testSignatureSet(validatorSetHash uint32, catchainSeqno uint32, sigWeight uint64) *cell.Cell {
	return cell.BeginCell().
		MustStoreUInt(0x11, 8).
		MustStoreUInt(uint64(validatorSetHash), 32).
		MustStoreUInt(uint64(catchainSeqno), 32).
		MustStoreUInt(0, 32).
		MustStoreUInt(sigWeight, 64).
		MustStoreDict(nil).
		EndCell()
}
