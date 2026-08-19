package validator

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"math"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/ton"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

// TestBlockAccepterRejectsForeignSessionCertificate is the whole justification
// for the binding check in BlockAccepter.validate, and it must fail loudly if
// that check is ever removed.
//
// The accepter no longer re-verifies the quorum: it reconstructs the signed
// payload from its own session id and stamps the result as verified. A
// certificate whose signatures cover another session's payload is a perfectly
// valid quorum — for that other session — and without the binding check the
// accepter would publish it under a SignaturesVerifiedKey nobody ever earned.
func TestBlockAccepterRejectsForeignSessionCertificate(t *testing.T) {
	shard := groups.ShardID{Workchain: 0, Shard: math.MinInt64}

	for _, test := range []struct {
		name    string
		mutate  func(*SessionConfig)
		wantErr string
	}{
		{
			name:    "another session id",
			mutate:  func(config *SessionConfig) { config.SessionID = [32]byte{0x56} },
			wantErr: "verified for another consensus session",
		},
		{
			name: "another validator roster",
			mutate: func(config *SessionConfig) {
				validators := append([]groups.Validator(nil), config.Validators...)
				validators[0].Weight++
				config.Validators = validators
			},
			wantErr: "verified against another validator set",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newAcceptanceTestFixture(t, shard)
			acceptance := fixture.acceptance(simplex.VoteNotarize, false)

			// Same certificate bytes, verified against a foreign (session,
			// roster) pair. Its signatures are genuine for that pair.
			foreign := fixture.config
			test.mutate(&foreign)
			vote := acceptance.Certificate.Vote()
			certificate := &simplex.Certificate{
				Vote: vote,
				Signatures: []simplex.VoteSignature{{
					ValidatorIndex: 0,
					Signature: ed25519.Sign(
						fixture.privateKey,
						simplex.DataToSign(foreign.SessionID, simplex.VoteBytes(vote)),
					),
				}},
			}
			sealed, err := simplex.VerifyCertificate(
				foreign.SessionID,
				runtimeValidators(foreign.Validators),
				certificate,
			)
			if err != nil {
				t.Fatalf("verify foreign certificate: %v", err)
			}
			acceptance.Certificate = sealed

			node := &acceptanceTestNode{}
			accepter, err := newAcceptanceTestAccepter(fixture, node)
			if err != nil {
				t.Fatalf("construct block accepter: %v", err)
			}
			err = accepter.Accept(context.Background(), acceptance, acceptanceTestViewResolver(BlockAcceptanceView{}))
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("accept foreign-session certificate error = %v, want %q", err, test.wantErr)
			}
			if len(node.blocks) != 0 {
				t.Fatalf("submitted %d blocks under a foreign certificate", len(node.blocks))
			}
		})
	}
}

// TestBlockAccepterRejectsUnverifiedCertificate: the zero VerifiedCertificate
// is the only one a caller outside package simplex can produce, and it must not
// get a block accepted.
func TestBlockAccepterRejectsUnverifiedCertificate(t *testing.T) {
	fixture := newAcceptanceTestFixture(t, groups.ShardID{Workchain: 0, Shard: math.MinInt64})
	acceptance := fixture.acceptance(simplex.VoteNotarize, false)
	acceptance.Certificate = simplex.VerifiedCertificate{}

	node := &acceptanceTestNode{}
	accepter, err := newAcceptanceTestAccepter(fixture, node)
	if err != nil {
		t.Fatalf("construct block accepter: %v", err)
	}
	err = accepter.Accept(context.Background(), acceptance, acceptanceTestViewResolver(BlockAcceptanceView{}))
	if err == nil || !strings.Contains(err.Error(), "was not verified") {
		t.Fatalf("accept unverified certificate error = %v, want a not-verified rejection", err)
	}
	if len(node.blocks) != 0 {
		t.Fatalf("submitted %d blocks under an unverified certificate", len(node.blocks))
	}
}

// TestBlockAccepterStillBindsCertificateToBlock: the seal proves a quorum over
// a simplex.CandidateID and says nothing about which block that id names. The
// checks that turn it into a statement about *this* block stay, and removing any
// of them makes the seal meaningless.
func TestBlockAccepterStillBindsCertificateToBlock(t *testing.T) {
	shard := groups.ShardID{Workchain: 0, Shard: math.MinInt64}

	for _, test := range []struct {
		name    string
		mutate  func(t *testing.T, fixture acceptanceTestFixture, acceptance *BlockAcceptance)
		wantErr string
	}{
		{
			name: "certificate certifies another candidate",
			mutate: func(t *testing.T, fixture acceptanceTestFixture, acceptance *BlockAcceptance) {
				other := simplex.CandidateID{Slot: acceptance.Certificate.Vote().ID.Slot, Hash: [32]byte{0x6f}}
				acceptance.Certificate = fixture.seal(t, simplex.NotarizeVote(other))
			},
			wantErr: "certificate vote does not match certified candidate",
		},
		{
			name: "certified candidate references another block",
			mutate: func(t *testing.T, fixture acceptanceTestFixture, acceptance *BlockAcceptance) {
				other := fixture.candidate.Candidate
				block := *other.Block.Copy()
				block.SeqNo++
				other.Block = block
				other.ID = other.ComputeID(other.ID.Slot)

				acceptance.CertifiedCandidate = &CandidateArtifact{Candidate: other}
				acceptance.Certificate = fixture.seal(t, simplex.NotarizeVote(other.ID))
			},
			wantErr: "certified candidate references another block",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newAcceptanceTestFixture(t, shard)
			acceptance := fixture.acceptance(simplex.VoteNotarize, false)
			test.mutate(t, fixture, &acceptance)

			node := &acceptanceTestNode{}
			accepter, err := newAcceptanceTestAccepter(fixture, node)
			if err != nil {
				t.Fatalf("construct block accepter: %v", err)
			}
			err = accepter.Accept(context.Background(), acceptance, acceptanceTestViewResolver(BlockAcceptanceView{}))
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("accept error = %v, want %q", err, test.wantErr)
			}
			if len(node.blocks) != 0 {
				t.Fatalf("submitted %d blocks under a mis-bound certificate", len(node.blocks))
			}
		})
	}
}

// seal produces a genuinely verified certificate over vote for the fixture's own
// session and roster.
func (f acceptanceTestFixture) seal(t *testing.T, vote simplex.Vote) simplex.VerifiedCertificate {
	t.Helper()

	verified, err := simplex.VerifyCertificate(
		f.config.SessionID,
		runtimeValidators(f.config.Validators),
		&simplex.Certificate{
			Vote: vote,
			Signatures: []simplex.VoteSignature{{
				ValidatorIndex: 0,
				Signature: ed25519.Sign(
					f.privateKey,
					simplex.DataToSign(f.config.SessionID, simplex.VoteBytes(vote)),
				),
			}},
		},
	)
	if err != nil {
		t.Fatalf("verify fixture certificate: %v", err)
	}

	return verified
}

// TestSimplexSignaturePayloadMatchesConsensusEngine is T6, and it is load
// bearing.
//
// The consensus engine signs simplex.DataToSign(sessionID, VoteBytes(vote));
// blockproof verifies blockproof.SimplexSignaturePayload over the same logical
// vote. The two constructions live in different packages and share no code.
// Their byte equality used to be enforced implicitly — block acceptance
// re-verified on the blockproof side every quorum the engine had verified on the
// simplex side, so a divergence failed instantly and locally. That double
// verification is gone. Nothing else enforces the equality now, and a divergence
// would publish blocks whose signatures no peer accepts.
func TestSimplexSignaturePayloadMatchesConsensusEngine(t *testing.T) {
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     math.MinInt64,
		SeqNo:     42,
		RootHash:  bytes.Repeat([]byte{0x31}, 32),
		FileHash:  bytes.Repeat([]byte{0x32}, 32),
	}
	sessionID := [32]byte{0x77, 0x21}

	candidate := simplex.Candidate{
		Parent:           simplex.Genesis(),
		Leader:           0,
		Block:            block,
		CollatedFileHash: [32]byte{0x51},
	}
	for _, slot := range []uint32{0, 1, 3, 0x7fffffff} {
		candidate.ID = candidate.ComputeID(slot)
		hashData := candidate.HashDataBytes()

		for _, test := range []struct {
			name  string
			final bool
			vote  simplex.Vote
		}{
			{name: "notarize", vote: simplex.NotarizeVote(candidate.ID)},
			{name: "finalize", final: true, vote: simplex.FinalizeVote(candidate.ID)},
		} {
			t.Run(test.name, func(t *testing.T) {
				engine := simplex.DataToSign(sessionID, simplex.VoteBytes(test.vote))
				proof, err := blockproof.SimplexSignaturePayload(
					block,
					test.final,
					sessionID[:],
					int32(slot),
					hashData,
				)
				if err != nil {
					t.Fatalf("build blockproof payload: %v", err)
				}
				if !bytes.Equal(engine, proof) {
					t.Fatalf(
						"signed payload diverged\n simplex:   %x\n blockproof: %x",
						engine,
						proof,
					)
				}
			})
		}
	}
}

// TestAcceptedSignaturesVerifyAgainstTheNetworkPath closes the same loop from
// the other end: whatever the accepter publishes must still pass the full
// Ed25519 check the receiving side runs. If the two payload constructions ever
// drift, this is what the network would see.
func TestAcceptedSignaturesVerifyAgainstTheNetworkPath(t *testing.T) {
	for _, shard := range []groups.ShardID{
		{Workchain: 0, Shard: math.MinInt64},
		{Workchain: -1, Shard: math.MinInt64},
	} {
		fixture := newAcceptanceTestFixture(t, shard)
		kind := simplex.VoteNotarize
		// Acceptance returns before it ever consults the view for a masterchain
		// block, and the fixture only builds one for a shard.
		view := BlockAcceptanceView{}
		if shard.IsMasterchain() {
			kind = simplex.VoteFinalize
		} else {
			view = fixture.view(t)
		}
		acceptance := fixture.acceptance(kind, false)

		node := &acceptanceTestNode{}
		accepter, err := newAcceptanceTestAccepter(fixture, node)
		if err != nil {
			t.Fatalf("construct block accepter: %v", err)
		}
		if err = accepter.Accept(
			context.Background(),
			acceptance,
			acceptanceTestViewResolver(view),
		); err != nil {
			t.Fatalf("accept block: %v", err)
		}

		signatures := fixture.signatureSet(t, acceptance)
		if err = blockproof.CheckPreparedSignatures(
			fixture.candidate.Candidate.Block,
			signatures,
			fixture.validators,
		); err != nil {
			t.Fatalf("published signatures fail the full network-side check: %v", err)
		}
	}
}
