package validator

import (
	"bytes"
	"testing"

	"github.com/xssnick/tonutils-go/ton"

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
	emptyCandidate := simplex.Candidate{
		Parent: simplex.Parent(ordinary.Candidate.ID),
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
	emptyCandidate.ID = emptyCandidate.ComputeID(1)
	emptyCandidate.Signature, err = simplex.SignCandidate(
		runtimeTestSigner{key: leaderKey},
		config.SessionID,
		emptyCandidate.ID,
	)
	if err != nil {
		t.Fatal(err)
	}
	empty := &CandidateArtifact{Candidate: emptyCandidate}

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
