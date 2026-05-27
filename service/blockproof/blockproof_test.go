package blockproof

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/tl"
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
		{name: "master proof link with signatures", id: master, isLink: true, signatures: signatures},
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

func TestCheckPreparedSignaturesAcceptsOrdinaryAndSimplexVotes(t *testing.T) {
	block := testBlockID(0, 77)
	validators, privateKeys := testValidators(t, 4)
	catchainSeqno := uint32(9)
	setHash, err := ValidatorSetHash(catchainSeqno, validators)
	if err != nil {
		t.Fatalf("validator set hash: %v", err)
	}

	candidate := testSimplexCandidate(t, block)
	sessionID := make([]byte, 32)
	sessionID[0] = 0x99

	tests := []struct {
		name string
		set  *ValidatorSignatureSet
	}{
		{
			name: "ordinary final",
			set:  NewOrdinaryValidatorSignatureSet(catchainSeqno, setHash, nil),
		},
		{
			name: "simplex final",
			set:  NewSimplexValidatorSignatureSet(catchainSeqno, setHash, nil, true, sessionID, 11, candidate),
		},
		{
			name: "simplex approve",
			set:  NewSimplexValidatorSignatureSet(catchainSeqno, setHash, nil, false, sessionID, 11, candidate),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload, err := signaturePayload(block, tt.set)
			if err != nil {
				t.Fatalf("payload: %v", err)
			}
			signatures := testSignatures(t, validators[:3], privateKeys[:3], payload)

			var signed *ValidatorSignatureSet
			if tt.set.IsSimplex() {
				signed = NewSimplexValidatorSignatureSet(catchainSeqno, setHash, signatures, tt.set.IsFinal(), sessionID, 11, candidate)
			} else {
				signed = NewOrdinaryValidatorSignatureSet(catchainSeqno, setHash, signatures)
			}

			if err = CheckPreparedSignaturesWithValidators(block, signed, validators); err != nil {
				t.Fatalf("check signatures: %v", err)
			}
		})
	}
}

func TestCheckPreparedMasterchainSignaturesRejectsNonFinalSimplex(t *testing.T) {
	block := testBlockID(-1, 78)
	set := NewSimplexValidatorSignatureSet(9, 0x12345678, nil, false, make([]byte, 32), 11, []byte{0x01})
	if err := CheckPreparedMasterchainSignaturesWithValidators(block, set, nil); err == nil {
		t.Fatal("expected non-final simplex masterchain signatures to be rejected")
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

func testValidators(t *testing.T, count int) ([]*tlb.ValidatorAddr, []ed25519.PrivateKey) {
	t.Helper()

	validators := make([]*tlb.ValidatorAddr, count)
	privateKeys := make([]ed25519.PrivateKey, count)
	for i := 0; i < count; i++ {
		pub, priv, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatalf("generate validator key: %v", err)
		}
		adnl := make([]byte, 32)
		adnl[0] = byte(i + 1)
		validators[i] = &tlb.ValidatorAddr{
			PublicKey: tlb.SigPubKeyED25519{Key: pub},
			Weight:    1,
			ADNLAddr:  adnl,
		}
		privateKeys[i] = priv
	}
	return validators, privateKeys
}

func testSignatures(t *testing.T, validators []*tlb.ValidatorAddr, privateKeys []ed25519.PrivateKey, payload []byte) []ton.Signature {
	t.Helper()

	signatures := make([]ton.Signature, len(validators))
	for i, validator := range validators {
		nodeID, err := tl.Hash(keys.PublicKeyED25519{Key: validator.PublicKey.Key})
		if err != nil {
			t.Fatalf("hash validator key: %v", err)
		}
		signatures[i] = ton.Signature{
			NodeIDShort: nodeID,
			Signature:   ed25519.Sign(privateKeys[i], payload),
		}
	}
	return signatures
}

func testSimplexCandidate(t *testing.T, block ton.BlockIDExt) []byte {
	t.Helper()

	data, err := tl.Serialize(ton.ConsensusCandidateHashDataOrdinary{
		Block:            block,
		CollatedFileHash: make([]byte, 32),
		Parent:           ton.ConsensusCandidateWithoutParents{},
	}, true)
	if err != nil {
		t.Fatalf("serialize simplex candidate: %v", err)
	}
	return data
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
