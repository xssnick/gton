package validator

import (
	"bytes"
	"crypto/ed25519"
	"testing"

	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/simplex"
)

// The bare broadcast frame is now parsed without copying, on the strength of
// the transport contract that its buffer is owned only for the receive
// callback. That makes one thing load-bearing which used to be free: every
// byte field the artifact keeps past the call — the signature, an empty
// candidate's block hashes — has to be copied where the artifact is built.
// This test scribbles over the input the instant the decode returns and holds
// that nothing the artifact or its lazy wire reports moved with it.
func TestDecodeBroadcastRetainsNothingFromTheTransportBuffer(t *testing.T) {
	config, leaderKey := runtimeTestConfig(0x64, &runtimeTestJournal{})
	codec, err := newCandidateCodec(config, CandidateLimits{
		MaxBlockBytes:        1 << 20,
		MaxCollatedDataBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}

	ordinary := runtimeOrdinaryArtifact(t, config, leaderKey, 0, simplex.Genesis())
	empty := nocopyEmptyArtifact(t, config, leaderKey, ordinary.Candidate.ID)

	for name, want := range map[string]*CandidateArtifact{"block": ordinary, "empty": empty} {
		t.Run(name, func(t *testing.T) {
			broadcast, err := simplex.SerializeCandidateForBroadcast(
				want.Candidate,
				want.BlockBOC,
				want.CollatedData,
			)
			if err != nil {
				t.Fatal(err)
			}
			payload := bytes.Clone(broadcast.Data)
			got, lazy, err := codec.decodeBroadcastDeferred(payload, nil, want.Candidate.ID.Slot)
			if err != nil {
				t.Fatal(err)
			}
			wireBefore, hashBefore, err := lazy.materialize()
			if err != nil {
				t.Fatal(err)
			}
			wireBefore = bytes.Clone(wireBefore)

			// The transport takes its buffer back: every byte flips.
			for i := range payload {
				payload[i] ^= 0xff
			}

			if !bytes.Equal(got.Candidate.Signature, want.Candidate.Signature) {
				t.Fatal("candidate signature followed the transport buffer")
			}
			if !sameBlockID(got.Candidate.Block, want.Candidate.Block) {
				t.Fatal("candidate block id followed the transport buffer")
			}
			if got.Candidate.ID != want.Candidate.ID || got.Candidate.Parent != want.Candidate.Parent {
				t.Fatal("candidate ids followed the transport buffer")
			}
			if !bytes.Equal(got.BlockBOC, want.BlockBOC) || !bytes.Equal(got.CollatedData, want.CollatedData) {
				t.Fatal("candidate payload followed the transport buffer")
			}
			if err := codec.verifyCandidate(&got.Candidate); err != nil {
				t.Fatalf("decoded candidate no longer verifies after the buffer was reused: %v", err)
			}
			wireAfter, hashAfter, err := lazy.materialize()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(wireAfter, wireBefore) || hashAfter != hashBefore {
				t.Fatal("lazy canonical wire followed the transport buffer")
			}
		})
	}
}

// The wrapped wire a candidate query answers with, or a storage read returns,
// is parsed without copying too. The delegation is what makes that
// load-bearing on this path: the wrapper carries it inline, where the broadcast
// path is handed one already parsed out of the extra, so decodeVerified has to
// copy it itself. This test scribbles over the wire the instant the decode
// returns — for a validator candidate, a delegated one, a delegated empty one
// and a delegated one in the structural compression form, whose cells come out
// of the decompressor's own buffer — and holds that nothing the artifact or its
// lazy wire reports moved with it.
func TestDecodeWrappedRetainsNothingFromTheWire(t *testing.T) {
	config, leaderKey := runtimeTestConfig(0x67, &runtimeTestJournal{})
	codec, err := newCandidateCodec(config, CandidateLimits{
		MaxBlockBytes:        1 << 20,
		MaxCollatedDataBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}

	ordinary := runtimeOrdinaryArtifact(t, config, leaderKey, 0, simplex.Genesis())
	delegated := runtimeDelegatedArtifact(t, config, leaderKey, ordinary)
	delegatedEmpty := runtimeDelegatedArtifact(
		t,
		config,
		leaderKey,
		nocopyEmptyArtifact(t, config, leaderKey, ordinary.Candidate.ID),
	)

	canonicalWire := func(artifact *CandidateArtifact) []byte {
		wire, err := simplex.SerializeCandidate(artifact.Candidate, artifact.BlockBOC, artifact.CollatedData)
		if err != nil {
			t.Fatal(err)
		}

		return wire
	}

	broadcast, err := simplex.SerializeCandidateForBroadcast(
		delegated.Candidate,
		delegated.BlockBOC,
		delegated.CollatedData,
	)
	if err != nil {
		t.Fatal(err)
	}
	structuralData := candidateBroadcastBlockData(t, broadcast.Data)
	blockRoot, err := cell.FromBOC(delegated.BlockBOC)
	if err != nil {
		t.Fatal(err)
	}
	collatedRoots, err := cell.FromBOCMultiRoot(delegated.CollatedData)
	if err != nil {
		t.Fatal(err)
	}
	compressed, err := cell.CompressBOC(
		append([]*cell.Cell{blockRoot}, collatedRoots...),
		cell.CompressionImprovedStructureLZ4,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	structuralData.Candidate, err = tl.Serialize(simplex.ValidatorSessionCompressedCandidateV2{
		Source:   make([]byte, 32),
		Round:    int32(delegated.Candidate.Block.SeqNo),
		RootHash: delegated.Candidate.Block.RootHash,
		Data:     compressed,
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	structuralWire, err := (&simplex.ConsensusCandidateWrapped{
		Data:       structuralData,
		Delegation: delegated.Candidate.Delegation,
	}).Serialize()
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name string
		want *CandidateArtifact
		wire []byte
	}{
		{name: "validator", want: ordinary, wire: canonicalWire(ordinary)},
		{name: "delegated", want: delegated, wire: canonicalWire(delegated)},
		{name: "delegated empty", want: delegatedEmpty, wire: canonicalWire(delegatedEmpty)},
		{name: "delegated structural", want: delegated, wire: structuralWire},
	} {
		t.Run(test.name, func(t *testing.T) {
			wire := bytes.Clone(test.wire)
			got, lazy, err := codec.decodeDeferred(wire, &test.want.Candidate.ID)
			if err != nil {
				t.Fatal(err)
			}

			// The answer or the storage read is gone: every byte flips.
			for i := range wire {
				wire[i] ^= 0xff
			}

			if !sameDelegation(got.Candidate.Delegation, test.want.Candidate.Delegation) {
				t.Fatal("candidate delegation followed the wire")
			}
			if !bytes.Equal(got.Candidate.Signature, test.want.Candidate.Signature) {
				t.Fatal("candidate signature followed the wire")
			}
			if !sameBlockID(got.Candidate.Block, test.want.Candidate.Block) {
				t.Fatal("candidate block id followed the wire")
			}
			if got.Candidate.ID != test.want.Candidate.ID || got.Candidate.Parent != test.want.Candidate.Parent {
				t.Fatal("candidate ids followed the wire")
			}
			if !bytes.Equal(got.BlockBOC, test.want.BlockBOC) ||
				!bytes.Equal(got.CollatedData, test.want.CollatedData) {
				t.Fatal("candidate payload followed the wire")
			}
			if err := codec.verifyCandidate(&got.Candidate); err != nil {
				t.Fatalf("decoded candidate no longer verifies after the wire was reused: %v", err)
			}
			materialized, _, err := lazy.materialize()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(materialized, canonicalWire(test.want)) {
				t.Fatal("lazy canonical wire followed the wire")
			}
		})
	}
}

func nocopyEmptyArtifact(
	t *testing.T,
	config SessionConfig,
	leaderKey ed25519.PrivateKey,
	parent simplex.CandidateID,
) *CandidateArtifact {
	t.Helper()

	candidate := simplex.Candidate{
		Parent: simplex.Parent(parent),
		Leader: 0,
		Empty:  true,
		Block: ton.BlockIDExt{
			Workchain: config.Shard.Workchain,
			Shard:     config.Shard.Shard,
			SeqNo:     1,
			RootHash:  bytes.Repeat([]byte{0x22}, 32),
			FileHash:  bytes.Repeat([]byte{0x33}, 32),
		},
	}
	candidate.ID = candidate.ComputeID(parent.Slot + 1)
	var err error
	candidate.Signature, err = simplex.SignCandidate(
		runtimeTestSigner{key: leaderKey},
		config.SessionID,
		candidate.ID,
	)
	if err != nil {
		t.Fatal(err)
	}

	return &CandidateArtifact{Candidate: candidate}
}
