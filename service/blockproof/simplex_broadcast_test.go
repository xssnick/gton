package blockproof

import (
	"bytes"
	"crypto/ed25519"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

func TestValidatorSignatureSet_SimplexBroadcastSignatureSet(t *testing.T) {
	t.Parallel()

	catchainSeqno := uint32(0x80000001)
	validatorSetHash := uint32(0xfedcba98)
	const slot = int32(17)

	block := simplexBroadcastTestBlock()
	candidateData := serializeSimplexBroadcastCandidate(t, ton.ConsensusCandidateHashDataOrdinary{
		Block:            block,
		CollatedFileHash: bytes.Repeat([]byte{0x31}, 32),
		Parent: ton.ConsensusCandidateParent{
			ID: ton.ConsensusCandidateID{
				Slot: 4,
				Hash: bytes.Repeat([]byte{0x32}, 32),
			},
		},
	})
	sessionID := bytes.Repeat([]byte{0x41}, 32)
	signatures := []ton.Signature{{
		NodeIDShort: bytes.Repeat([]byte{0x51}, 32),
		Signature:   bytes.Repeat([]byte{0x52}, ed25519.SignatureSize),
	}}
	set := NewSimplexValidatorSignatureSet(
		catchainSeqno,
		validatorSetHash,
		signatures,
		true,
		sessionID,
		slot,
		candidateData,
	)

	got, err := set.SimplexBroadcastSignatureSet()
	if err != nil {
		t.Fatalf("build simplex broadcast signature set: %v", err)
	}
	hasUnexpectedScalar := !got.Final ||
		got.CatchainSeqno != int32(catchainSeqno) ||
		got.ValidatorSetHash != int32(validatorSetHash) ||
		got.Slot != slot
	if hasUnexpectedScalar {
		t.Fatalf("unexpected scalar fields: %+v", got)
	}
	if len(got.Signatures) != 1 {
		t.Fatalf("unexpected signature count: %d", len(got.Signatures))
	}
	if !bytes.Equal(got.Signatures[0].Who, signatures[0].NodeIDShort) {
		t.Fatalf("unexpected validator node id: %x", got.Signatures[0].Who)
	}
	if !bytes.Equal(got.Signatures[0].Signature, signatures[0].Signature) {
		t.Fatalf("unexpected validator signature: %x", got.Signatures[0].Signature)
	}
	if !bytes.Equal(got.SessionID, sessionID) {
		t.Fatalf("unexpected session id: %x", got.SessionID)
	}

	candidate, ok := got.Candidate.(ton.ConsensusCandidateHashDataOrdinary)
	if !ok {
		t.Fatalf("unexpected candidate type: %T", got.Candidate)
	}
	if !candidate.Block.Equals(&block) {
		t.Fatalf("unexpected candidate block: %+v", candidate.Block)
	}
	parent, ok := candidate.Parent.(ton.ConsensusCandidateParent)
	if !ok {
		t.Fatalf("unexpected candidate parent type: %T", candidate.Parent)
	}
	parentID, ok := parent.ID.(ton.ConsensusCandidateID)
	if !ok {
		t.Fatalf("unexpected candidate parent id type: %T", parent.ID)
	}

	got.SessionID[0] = 0
	got.Signatures[0].Who[0] = 0
	got.Signatures[0].Signature[0] = 0
	candidate.Block.RootHash[0] = 0
	candidate.CollatedFileHash[0] = 0
	parentID.Hash[0] = 0

	again, err := set.SimplexBroadcastSignatureSet()
	if err != nil {
		t.Fatalf("build simplex broadcast signature set again: %v", err)
	}
	againCandidate := again.Candidate.(ton.ConsensusCandidateHashDataOrdinary)
	againParent := againCandidate.Parent.(ton.ConsensusCandidateParent)
	againParentID := againParent.ID.(ton.ConsensusCandidateID)
	if again.SessionID[0] != 0x41 || again.Signatures[0].Who[0] != 0x51 || again.Signatures[0].Signature[0] != 0x52 {
		t.Fatal("returned signature set aliases validator signature storage")
	}
	candidateAliasesStorage := againCandidate.Block.RootHash[0] != block.RootHash[0] ||
		againCandidate.CollatedFileHash[0] != 0x31 ||
		againParentID.Hash[0] != 0x32
	if candidateAliasesStorage {
		t.Fatal("returned candidate aliases validator signature storage")
	}
	if _, err = tl.Serialize(again, true); err != nil {
		t.Fatalf("serialize returned simplex signature set: %v", err)
	}
}

func TestValidatorSignatureSet_SimplexBroadcastSignatureSetCandidateKinds(t *testing.T) {
	t.Parallel()

	block := simplexBroadcastTestBlock()
	tests := []struct {
		name      string
		candidate any
	}{
		{
			name: "ordinary without parents",
			candidate: ton.ConsensusCandidateHashDataOrdinary{
				Block:            block,
				CollatedFileHash: bytes.Repeat([]byte{0x11}, 32),
				Parent:           ton.ConsensusCandidateWithoutParents{},
			},
		},
		{
			name: "empty",
			candidate: ton.ConsensusCandidateHashDataEmpty{
				Block: block,
				Parent: ton.ConsensusCandidateID{
					Slot: 3,
					Hash: bytes.Repeat([]byte{0x12}, 32),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			candidateData := serializeSimplexBroadcastCandidate(t, tt.candidate)
			set := NewSimplexValidatorSignatureSet(7, 8, nil, false, make([]byte, 32), 9, candidateData)

			got, err := set.SimplexBroadcastSignatureSet()
			if err != nil {
				t.Fatalf("build simplex broadcast signature set: %v", err)
			}
			serialized, err := tl.Serialize(got.Candidate, true)
			if err != nil {
				t.Fatalf("serialize decoded candidate: %v", err)
			}
			if !bytes.Equal(serialized, candidateData) {
				t.Fatalf("candidate changed during conversion: got %x, want %x", serialized, candidateData)
			}
		})
	}
}

func TestValidatorSignatureSet_SimplexBroadcastSignatureSetRejectsInvalidShape(t *testing.T) {
	t.Parallel()

	validCandidate := serializeSimplexBroadcastCandidate(t, ton.ConsensusCandidateHashDataOrdinary{
		Block:            simplexBroadcastTestBlock(),
		CollatedFileHash: bytes.Repeat([]byte{0x61}, 32),
		Parent:           ton.ConsensusCandidateWithoutParents{},
	})
	unsupportedCandidate := serializeSimplexBroadcastCandidate(t, simplexBroadcastTestBlock())

	tests := []struct {
		name string
		set  func() *ValidatorSignatureSet
		want string
	}{
		{
			name: "ordinary set",
			set: func() *ValidatorSignatureSet {
				return NewOrdinaryValidatorSignatureSet(1, 2, nil)
			},
			want: "not simplex",
		},
		{
			name: "short session id",
			set: func() *ValidatorSignatureSet {
				return NewSimplexValidatorSignatureSet(1, 2, nil, true, make([]byte, 31), 3, validCandidate)
			},
			want: "invalid simplex session id len 31",
		},
		{
			name: "too many signatures",
			set: func() *ValidatorSignatureSet {
				signatures := make([]ton.Signature, maxBlockSignatures+1)
				for i := range signatures {
					signatures[i] = ton.Signature{
						NodeIDShort: make([]byte, 32),
						Signature:   make([]byte, ed25519.SignatureSize),
					}
				}
				return NewSimplexValidatorSignatureSet(1, 2, signatures, true, make([]byte, 32), 3, validCandidate)
			},
			want: "too many validator signatures",
		},
		{
			name: "short validator id",
			set: func() *ValidatorSignatureSet {
				return NewSimplexValidatorSignatureSet(1, 2, []ton.Signature{{
					NodeIDShort: make([]byte, 31),
					Signature:   make([]byte, ed25519.SignatureSize),
				}}, true, make([]byte, 32), 3, validCandidate)
			},
			want: "invalid validator node id len 31",
		},
		{
			name: "short validator signature",
			set: func() *ValidatorSignatureSet {
				return NewSimplexValidatorSignatureSet(1, 2, []ton.Signature{{
					NodeIDShort: make([]byte, 32),
					Signature:   make([]byte, ed25519.SignatureSize-1),
				}}, true, make([]byte, 32), 3, validCandidate)
			},
			want: "invalid validator signature len 63",
		},
		{
			name: "empty candidate",
			set: func() *ValidatorSignatureSet {
				return NewSimplexValidatorSignatureSet(1, 2, nil, true, make([]byte, 32), 3, nil)
			},
			want: "candidate data is empty",
		},
		{
			name: "malformed boxed candidate",
			set: func() *ValidatorSignatureSet {
				return NewSimplexValidatorSignatureSet(1, 2, nil, true, make([]byte, 32), 3, []byte{1, 2, 3})
			},
			want: "not enough bytes to parse struct interface",
		},
		{
			name: "candidate with trailing data",
			set: func() *ValidatorSignatureSet {
				candidate := append(bytes.Clone(validCandidate), 0)
				return NewSimplexValidatorSignatureSet(1, 2, nil, true, make([]byte, 32), 3, candidate)
			},
			want: "candidate data has 1 trailing bytes",
		},
		{
			name: "unsupported boxed candidate",
			set: func() *ValidatorSignatureSet {
				return NewSimplexValidatorSignatureSet(1, 2, nil, true, make([]byte, 32), 3, unsupportedCandidate)
			},
			want: "unsupported simplex candidate type ton.BlockIDExt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if _, err := tt.set().SimplexBroadcastSignatureSet(); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("unexpected error: got %v, want substring %q", err, tt.want)
			}
		})
	}
}

func simplexBroadcastTestBlock() ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(-1 << 63),
		SeqNo:     10,
		RootHash:  bytes.Repeat([]byte{0x21}, 32),
		FileHash:  bytes.Repeat([]byte{0x22}, 32),
	}
}

func serializeSimplexBroadcastCandidate(t *testing.T, candidate any) []byte {
	t.Helper()

	data, err := tl.Serialize(candidate, true)
	if err != nil {
		t.Fatalf("serialize simplex broadcast candidate: %v", err)
	}

	return data
}
