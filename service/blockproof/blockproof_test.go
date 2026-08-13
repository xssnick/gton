package blockproof

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const testTopShard = int64(-1 << 63)

func TestBlockIDExtTLBFromBlockRejectsZeroShard(t *testing.T) {
	_, err := blockIDExtTLBFromBlock(ton.BlockIDExt{Workchain: 0, Shard: 0, SeqNo: 1})
	if !errors.Is(err, shard.ErrInvalidID) {
		t.Fatalf("error = %v, want ErrInvalidID", err)
	}
}

func TestBlockIDExtTLBFromBlockRejectsNonTLBShardIdent(t *testing.T) {
	tests := []struct {
		name      string
		workchain int32
		shard     int64
		want      string
	}{
		{
			name:      "invalid workchain",
			workchain: math.MinInt32,
			shard:     shard.Root,
			want:      "workchain is invalid",
		},
		{
			name:      "prefix depth 61",
			workchain: 0,
			shard:     1 << 2,
			want:      "prefix length 61",
		},
		{
			name:      "prefix depth 62",
			workchain: 0,
			shard:     1 << 1,
			want:      "prefix length 62",
		},
		{
			name:      "prefix depth 63",
			workchain: 0,
			shard:     1,
			want:      "prefix length 63",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			id := testBlockID(test.workchain, 1)
			id.Shard = test.shard

			_, err := blockIDExtTLBFromBlock(id)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestBlockIDExtTLBFromBlockAcceptsMaximumTLBShardDepth(t *testing.T) {
	id := testBlockID(0, 1)
	id.Shard = 1 << 3

	encoded, err := blockIDExtTLBFromBlock(id)
	if err != nil {
		t.Fatalf("encode maximum-depth shard: %v", err)
	}
	if encoded.ShardID.PrefixBits != 60 {
		t.Fatalf("prefix length = %d, want 60", encoded.ShardID.PrefixBits)
	}
	if got := encoded.blockID(); got.Workchain != id.Workchain || got.Shard != id.Shard {
		t.Fatalf("decoded shard = (%d,%016x), want (%d,%016x)", got.Workchain, uint64(got.Shard), id.Workchain, uint64(id.Shard))
	}
}

func TestBlockIDExtTLBFromBlockRejectsMalformedHashes(t *testing.T) {
	tests := []struct {
		name     string
		rootHash []byte
		fileHash []byte
	}{
		{name: "short root hash", rootHash: make([]byte, 31), fileHash: make([]byte, 32)},
		{name: "long root hash", rootHash: make([]byte, 33), fileHash: make([]byte, 32)},
		{name: "short file hash", rootHash: make([]byte, 32), fileHash: make([]byte, 31)},
		{name: "long file hash", rootHash: make([]byte, 32), fileHash: make([]byte, 33)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			id := testBlockID(0, 1)
			id.RootHash = test.rootHash
			id.FileHash = test.fileHash

			_, err := blockIDExtTLBFromBlock(id)
			if !errors.Is(err, storage.ErrInvalidBlockIDHashes) {
				t.Fatalf("error = %v, want ErrInvalidBlockIDHashes", err)
			}
		})
	}
}

func TestShardProofLookupShardRejectsZero(t *testing.T) {
	_, err := shardProofLookupShard(0)
	if !errors.Is(err, shard.ErrInvalidID) {
		t.Fatalf("error = %v, want ErrInvalidID", err)
	}
}

func TestFindShardLeafRejectsZeroShard(t *testing.T) {
	_, _, err := findShardLeaf(nil, 0, false)
	if !errors.Is(err, shard.ErrInvalidID) {
		t.Fatalf("error = %v, want ErrInvalidID", err)
	}
}

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

func TestCheckProofShapeRejectsProofForMismatch(t *testing.T) {
	signatures := testSignatureSet(0x12345678, 33, 100)

	tests := []struct {
		name   string
		proof  ton.BlockIDExt
		expect ton.BlockIDExt
		isLink bool
		sig    *cell.Cell
	}{
		{
			name:   "master proof",
			proof:  testBlockID(-1, 20),
			expect: testBlockID(-1, 21),
			sig:    signatures,
		},
		{
			name:   "shard proof link",
			proof:  testBlockID(0, 20),
			expect: testBlockID(0, 21),
			isLink: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := testBlockProof(t, tt.proof, tt.sig)
			if err := CheckProofShape(tt.expect, root, tt.isLink); err == nil {
				t.Fatal("proof with mismatched ProofFor was accepted")
			}
		})
	}
}

func TestParseBlockSignatureSetRejectsTrailingData(t *testing.T) {
	ordinary := testSignatureSet(0x12345678, 33, 0)
	simplex, err := tlb.ToCell(&blockSignaturesSimplex{
		ValidatorSetHash: 0x12345678,
		CatchainSeqno:    33,
		SessionID:        make([]byte, 32),
		CandidateData:    cell.BeginCell().MustStoreBinarySnake([]byte{1}).EndCell(),
	})
	if err != nil {
		t.Fatalf("serialize simplex signatures: %v", err)
	}

	trailingRef := cell.BeginCell().EndCell()
	tests := []struct {
		name string
		root *cell.Cell
	}{
		{name: "ordinary bit", root: ordinary.ToBuilder().MustStoreBoolBit(true).EndCell()},
		{name: "ordinary ref", root: ordinary.ToBuilder().MustStoreRef(trailingRef).EndCell()},
		{name: "simplex bit", root: simplex.ToBuilder().MustStoreBoolBit(true).EndCell()},
		{name: "simplex ref", root: simplex.ToBuilder().MustStoreRef(trailingRef).EndCell()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ParseBlockSignatureSetCell(tt.root); err == nil {
				t.Fatal("block signatures with trailing data were accepted")
			}
		})
	}
}

func TestParseBlockSignatureSetRejectsTooManySignatures(t *testing.T) {
	root := cell.BeginCell().
		MustStoreUInt(0x11, 8).
		MustStoreUInt(0x12345678, 32).
		MustStoreUInt(33, 32).
		MustStoreUInt(maxBlockSignatures+1, 32).
		MustStoreUInt(0, 64).
		MustStoreDict(nil).
		EndCell()

	if _, err := ParseBlockSignatureSetCell(root); err == nil {
		t.Fatal("block signatures above the C++ limit were accepted")
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
	prepared, err := PrepareValidatorSet(catchainSeqno, validators)
	if err != nil {
		t.Fatalf("prepare validator set: %v", err)
	}
	if prepared.Hash() != setHash || prepared.CatchainSeqno() != catchainSeqno {
		t.Fatalf("prepared validator set mismatch")
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
				signed = NewSimplexValidatorSignatureSet(catchainSeqno, setHash, signatures, tt.set.Final(), sessionID, 11, candidate)
			} else {
				signed = NewOrdinaryValidatorSignatureSet(catchainSeqno, setHash, signatures)
			}

			if err = CheckPreparedSignatures(block, signed, prepared); err != nil {
				t.Fatalf("check signatures: %v", err)
			}
		})
	}
}

func TestCheckPreparedSignaturesRejectsValidatorSetMutations(t *testing.T) {
	block := testBlockID(-1, 79)
	validators, privateKeys := testValidators(t, 4)
	catchainSeqno := uint32(9)
	setHash, err := ValidatorSetHash(catchainSeqno, validators)
	if err != nil {
		t.Fatalf("validator set hash: %v", err)
	}
	prepared, err := PrepareValidatorSet(catchainSeqno, validators)
	if err != nil {
		t.Fatalf("prepare validator set: %v", err)
	}

	payload, err := signaturePayload(block, NewOrdinaryValidatorSignatureSet(catchainSeqno, setHash, nil))
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	validSignatures := testSignatures(t, validators[:3], privateKeys[:3], payload)

	unknownPub, unknownPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate unknown validator key: %v", err)
	}
	unknownID, err := tl.Hash(keys.PublicKeyED25519{Key: unknownPub})
	if err != nil {
		t.Fatalf("hash unknown validator key: %v", err)
	}
	unknownSignature := ton.Signature{
		NodeIDShort: unknownID,
		Signature:   ed25519.Sign(unknownPriv, payload),
	}

	badSignature := append([]ton.Signature(nil), validSignatures...)
	badSignature[0].Signature = append([]byte(nil), badSignature[0].Signature...)
	badSignature[0].Signature[0] ^= 0x01

	tests := []struct {
		name    string
		sigs    []ton.Signature
		setHash uint32
	}{
		{
			name:    "duplicate validator signature",
			sigs:    []ton.Signature{validSignatures[0], validSignatures[1], validSignatures[0]},
			setHash: setHash,
		},
		{
			name:    "unknown validator signature",
			sigs:    []ton.Signature{validSignatures[0], validSignatures[1], unknownSignature},
			setHash: setHash,
		},
		{
			name:    "insufficient signed weight",
			sigs:    validSignatures[:2],
			setHash: setHash,
		},
		{
			name:    "incorrect validator signature",
			sigs:    badSignature,
			setHash: setHash,
		},
		{
			name:    "incorrect validator set hash",
			sigs:    validSignatures,
			setHash: setHash + 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			set := NewOrdinaryValidatorSignatureSet(catchainSeqno, tt.setHash, tt.sigs)
			if err := CheckPreparedSignatures(block, set, prepared); err == nil {
				t.Fatal("mutated validator signature set was accepted")
			}
		})
	}
}

func TestPreparedValidatorSetCopiesValidatorKeys(t *testing.T) {
	block := testBlockID(-1, 82)
	validators, privateKeys := testValidators(t, 4)
	catchainSeqno := uint32(9)
	setHash, err := ValidatorSetHash(catchainSeqno, validators)
	if err != nil {
		t.Fatalf("validator set hash: %v", err)
	}
	prepared, err := PrepareValidatorSet(catchainSeqno, validators)
	if err != nil {
		t.Fatalf("prepare validator set: %v", err)
	}

	sigSet := NewOrdinaryValidatorSignatureSet(catchainSeqno, setHash, nil)
	payload, err := signaturePayload(block, sigSet)
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	signatures := testSignatures(t, validators[:3], privateKeys[:3], payload)
	signed := NewOrdinaryValidatorSignatureSet(catchainSeqno, setHash, signatures)

	validators[0].PublicKey.Key[0] ^= 0x01

	if err := CheckPreparedSignatures(block, signed, prepared); err != nil {
		t.Fatalf("prepared validator set should not share validator keys: %v", err)
	}
}

func TestValidatorSignatureSetFinalitySignaturesCellRoundTrip(t *testing.T) {
	block := testBlockID(0, 83)
	validators, privateKeys := testValidators(t, 4)
	catchainSeqno := uint32(9)
	setHash, err := ValidatorSetHash(catchainSeqno, validators)
	if err != nil {
		t.Fatalf("validator set hash: %v", err)
	}
	prepared, err := PrepareValidatorSet(catchainSeqno, validators)
	if err != nil {
		t.Fatalf("prepare validator set: %v", err)
	}

	candidate := testSimplexCandidate(t, block)
	sessionID := bytes.Repeat([]byte{0x42}, 32)

	base := NewSimplexValidatorSignatureSet(catchainSeqno, setHash, nil, true, sessionID, 11, candidate)
	payload, err := signaturePayload(block, base)
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	signatures := testSignatures(t, validators[:3], privateKeys[:3], payload)
	signatures[0], signatures[2] = signatures[2], signatures[0]
	signed := NewSimplexValidatorSignatureSet(catchainSeqno, setHash, signatures, true, sessionID, 11, candidate)

	signaturesCell, err := signed.FinalitySignaturesCell(prepared)
	if err != nil {
		t.Fatalf("signatures cell: %v", err)
	}
	parsed, err := ParseBlockSignatureSetCell(signaturesCell)
	if err != nil {
		t.Fatalf("parse signatures cell: %v", err)
	}
	if !bytes.Equal(parsed.ValidatorSignatures.ContentKey(block), signed.ContentKey(block)) {
		t.Fatalf("content key mismatch after round trip")
	}
	if err = CheckPreparedBlockSignatures(block, parsed, prepared); err != nil {
		t.Fatalf("check parsed signatures: %v", err)
	}
	wire, err := LiteSignatureSet(signaturesCell)
	if err != nil {
		t.Fatalf("decode signatures cell for wire: %v", err)
	}
	serialized := wire.(ton.SignatureSetSimplex).Signatures
	for i := range signatures {
		if !bytes.Equal(serialized[i].NodeIDShort, signatures[i].NodeIDShort) {
			t.Fatalf("serialized signature %d changed certificate order", i)
		}
	}

	approve := NewSimplexValidatorSignatureSet(catchainSeqno, setHash, nil, false, sessionID, 11, candidate)
	if _, err = approve.FinalitySignaturesCell(prepared); err == nil {
		t.Fatal("non-final simplex signatures cell was accepted")
	}

	ordinary := NewOrdinaryValidatorSignatureSet(catchainSeqno, setHash, signatures)
	if _, err = ordinary.FinalitySignaturesCell(prepared); err == nil {
		t.Fatal("ordinary signatures cell was accepted as finality")
	}
}

func TestPrepareValidatorSignatureSetRejectsHeaderMismatches(t *testing.T) {
	blockID := testBlockID(-1, 80)
	validators, _ := testValidators(t, 4)
	catchainSeqno := uint32(9)
	setHash, err := ValidatorSetHash(catchainSeqno, validators)
	if err != nil {
		t.Fatalf("validator set hash: %v", err)
	}

	block := &tlb.Block{}
	block.BlockInfo.GenValidatorListHashShort = setHash
	block.BlockInfo.GenCatchainSeqno = catchainSeqno

	tests := []struct {
		name string
		set  *ValidatorSignatureSet
	}{
		{
			name: "validator set hash mismatch",
			set:  NewOrdinaryValidatorSignatureSet(catchainSeqno, setHash+1, nil),
		},
		{
			name: "catchain seqno mismatch",
			set:  NewOrdinaryValidatorSignatureSet(catchainSeqno+1, setHash, nil),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := PrepareValidatorSignatureSet(blockID, block, tt.set); err == nil {
				t.Fatal("signature set with header mismatch was accepted")
			}
		})
	}
}

func TestCheckPreparedSignaturesRejectsSimplexCandidateBlockMismatch(t *testing.T) {
	block := testBlockID(0, 81)
	otherBlock := testBlockID(0, 82)
	validators, _ := testValidators(t, 4)
	catchainSeqno := uint32(9)
	setHash, err := ValidatorSetHash(catchainSeqno, validators)
	if err != nil {
		t.Fatalf("validator set hash: %v", err)
	}
	prepared, err := PrepareValidatorSet(catchainSeqno, validators)
	if err != nil {
		t.Fatalf("prepare validator set: %v", err)
	}

	sessionID := make([]byte, 32)
	sessionID[0] = 0x99
	set := NewSimplexValidatorSignatureSet(catchainSeqno, setHash, nil, true, sessionID, 11, testSimplexCandidate(t, otherBlock))

	if err := CheckPreparedSignatures(block, set, prepared); err == nil {
		t.Fatal("simplex candidate for another block was accepted")
	}
}

func TestCheckPreparedMasterchainSignaturesRejectsNonFinalSimplex(t *testing.T) {
	block := testBlockID(-1, 78)
	set := &BlockSignatureSet{
		ValidatorSignatures: NewSimplexValidatorSignatureSet(9, 0x12345678, nil, false, make([]byte, 32), 11, []byte{0x01}),
	}
	if err := CheckPreparedMasterchainSignatures(block, set, nil); err == nil {
		t.Fatal("expected non-final simplex masterchain signatures to be rejected")
	}
}

func TestCheckPreparedMasterchainSignaturesRejectsForgedCellWeight(t *testing.T) {
	block := testBlockID(-1, 84)
	validators, privateKeys := testValidators(t, 1)
	catchainSeqno := uint32(12)
	prepared, err := PrepareValidatorSet(catchainSeqno, validators)
	if err != nil {
		t.Fatalf("prepare validator set: %v", err)
	}

	base := NewOrdinaryValidatorSignatureSet(catchainSeqno, prepared.Hash(), nil)
	payload, err := signaturePayload(block, base)
	if err != nil {
		t.Fatalf("signature payload: %v", err)
	}
	signed := NewOrdinaryValidatorSignatureSet(
		catchainSeqno,
		prepared.Hash(),
		testSignatures(t, validators, privateKeys, payload),
	)
	dict, actualWeight, err := signed.signaturesDict(prepared)
	if err != nil {
		t.Fatalf("signature dictionary: %v", err)
	}
	cellSet, err := tlb.ToCell(&blockSignaturesOrdinary{
		ValidatorSetHash: prepared.Hash(),
		CatchainSeqno:    catchainSeqno,
		SigCount:         uint32(signed.SignatureCount()),
		SigWeight:        actualWeight + 1,
		Signatures:       dict,
	})
	if err != nil {
		t.Fatalf("serialize forged block signatures: %v", err)
	}
	parsed, err := ParseBlockSignatureSetCell(cellSet)
	if err != nil {
		t.Fatalf("parse forged block signatures: %v", err)
	}
	if parsed.DeclaredWeight != actualWeight+1 {
		t.Fatalf("declared weight = %d, want %d", parsed.DeclaredWeight, actualWeight+1)
	}
	if err = CheckPreparedMasterchainSignatures(block, parsed, prepared); err == nil {
		t.Fatal("masterchain proof with forged signature weight was accepted")
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

func testValidators(t testing.TB, count int) ([]*tlb.ValidatorAddr, []ed25519.PrivateKey) {
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

func testSignatures(t testing.TB, validators []*tlb.ValidatorAddr, privateKeys []ed25519.PrivateKey, payload []byte) []ton.Signature {
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

func BenchmarkPrepareValidatorSet(b *testing.B) {
	validators, _ := testValidators(b, 100)

	b.ReportAllocs()
	for b.Loop() {
		prepared, err := PrepareValidatorSet(9, validators)
		if err != nil {
			b.Fatal(err)
		}
		_ = prepared
	}
}

func BenchmarkCheckPreparedSignatures(b *testing.B) {
	block := testBlockID(-1, 90)
	validators, privateKeys := testValidators(b, 100)
	prepared, err := PrepareValidatorSet(9, validators)
	if err != nil {
		b.Fatal(err)
	}

	base := NewOrdinaryValidatorSignatureSet(9, prepared.Hash(), nil)
	payload, err := signaturePayload(block, base)
	if err != nil {
		b.Fatal(err)
	}
	signatures := testSignatures(b, validators, privateKeys, payload)
	signed := NewOrdinaryValidatorSignatureSet(9, prepared.Hash(), signatures)

	b.ReportAllocs()
	for b.Loop() {
		if err := CheckPreparedSignatures(block, signed, prepared); err != nil {
			b.Fatal(err)
		}
	}
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
