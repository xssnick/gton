package validator

import (
	"bytes"
	"crypto/sha256"
	"reflect"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

type acceptanceRootFixtureCase struct {
	name  string
	shard groups.ShardID
	kind  simplex.VoteKind
}

type acceptanceRootMismatchCase struct {
	name   string
	mutate func(*ChainState)
}

func acceptanceResidentState(t testing.TB, artifact *CandidateArtifact) *ChainState {
	t.Helper()
	block, err := cell.FromBOC(artifact.BlockBOC)
	if err != nil {
		t.Fatal(err)
	}
	state := cell.BeginCell().MustStoreUInt(0x1256, 16).EndCell()

	return &ChainState{
		shard: groups.ShardID{Workchain: artifact.Candidate.Block.Workchain, Shard: artifact.Candidate.Block.Shard},
		tips: []ChainTip{{
			ID:       *artifact.Candidate.Block.Copy(),
			BlockBOC: artifact.BlockBOC,
			Block:    block,
			State:    state,
		}},
		root: state,
	}
}

func TestBlockAccepterReusesExactResidentBlock(t *testing.T) {
	for _, test := range []acceptanceRootFixtureCase{
		{name: "shard notarization", shard: groups.ShardID{Workchain: 0, Shard: -1 << 63}, kind: simplex.VoteNotarize},
		{name: "shard finalization", shard: groups.ShardID{Workchain: 0, Shard: -1 << 63}, kind: simplex.VoteFinalize},
		{name: "masterchain finalization", shard: groups.ShardID{Workchain: -1, Shard: -1 << 63}, kind: simplex.VoteFinalize},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newAcceptanceTestFixture(t, test.shard)
			accepter, err := newAcceptanceTestAccepter(fixture, &acceptanceTestNode{})
			if err != nil {
				t.Fatal(err)
			}
			acceptance := fixture.acceptance(test.kind, false)
			decoded, err := accepter.Prepare(t.Context(), acceptance, nil)
			if err != nil {
				t.Fatal(err)
			}
			state := acceptanceResidentState(t, acceptance.Candidate)
			acceptance.state = state

			for _, copyWire := range []bool{false, true} {
				name := "shared wire"
				if copyWire {
					name = "equal copied wire"
					acceptance.Candidate.BlockBOC = bytes.Clone(acceptance.Candidate.BlockBOC)
				}
				t.Run(name, func(t *testing.T) {
					resident, err := accepter.Prepare(t.Context(), acceptance, nil)
					if err != nil {
						t.Fatal(err)
					}
					if resident.block.Block != state.tips[0].Block {
						t.Fatal("acceptance decoded a root already held by its exact tip")
					}
					if !bytes.Equal(resident.block.ProofBOC, decoded.block.ProofBOC) ||
						!bytes.Equal(resident.block.SignaturesVerifiedKey, decoded.block.SignaturesVerifiedKey) ||
						!reflect.DeepEqual(resident.block.Meta, decoded.block.Meta) {
						t.Fatal("resident and decoded acceptance produced different proof evidence or metadata")
					}
				})
			}
		})
	}
}

func TestBlockAccepterDecodesWithoutAnExactResidentTip(t *testing.T) {
	fixture := newAcceptanceTestFixture(t, groups.ShardID{Workchain: 0, Shard: -1 << 63})
	accepter, err := newAcceptanceTestAccepter(fixture, &acceptanceTestNode{})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []acceptanceRootMismatchCase{
		{name: "other block", mutate: func(state *ChainState) { state.tips[0].ID.SeqNo++ }},
		{name: "other file identity", mutate: func(state *ChainState) { state.tips[0].ID.FileHash[0] ^= 1 }},
		{name: "other block bytes", mutate: func(state *ChainState) { state.tips[0].BlockBOC = []byte{0xff} }},
		{name: "multiple tips", mutate: func(state *ChainState) { state.tips = append(state.tips, state.tips[0]) }},
		{name: "missing parsed root", mutate: func(state *ChainState) { state.tips[0].Block = nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			acceptance := fixture.acceptance(simplex.VoteFinalize, false)
			acceptance.state = acceptanceResidentState(t, acceptance.Candidate)
			original := acceptance.state.tips[0].Block
			test.mutate(acceptance.state)
			prepared, err := accepter.Prepare(t.Context(), acceptance, nil)
			if err != nil {
				t.Fatal(err)
			}
			if prepared.block.Block == original {
				t.Fatal("acceptance reused an unrelated or unavailable resident tip")
			}
			if prepared.block.Block.HashKey() != original.HashKey() {
				t.Fatal("ordinary wire decoding changed the accepted block")
			}
		})
	}
}

func TestBlockAccepterResidentRootCannotHideMalformedWire(t *testing.T) {
	fixture := newAcceptanceTestFixture(t, groups.ShardID{Workchain: 0, Shard: -1 << 63})
	state := acceptanceResidentState(t, fixture.candidate)
	fixture.candidate.BlockBOC = []byte{0xde, 0xad, 0xbe, 0xef}
	fileHash := sha256.Sum256(fixture.candidate.BlockBOC)
	fixture.candidate.Candidate.Block.FileHash = fileHash[:]
	fixture.candidate.Candidate.ID = fixture.candidate.Candidate.ComputeID(3)
	// Even a tip carrying the certified ID must not substitute its old root for
	// different bytes: hashing the new bytes alone would accept this bad wire.
	state.tips[0].ID = *fixture.candidate.Candidate.Block.Copy()
	acceptance := fixture.acceptance(simplex.VoteFinalize, false)
	acceptance.state = state
	accepter, err := newAcceptanceTestAccepter(fixture, &acceptanceTestNode{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = accepter.Prepare(t.Context(), acceptance, nil); err == nil || !strings.Contains(err.Error(), "parse block boc") {
		t.Fatalf("resident root with malformed candidate wire error = %v, want a BOC decode error", err)
	}
}

func TestBlockAccepterResidentRootStillChecksFileHash(t *testing.T) {
	fixture := newAcceptanceTestFixture(t, groups.ShardID{Workchain: 0, Shard: -1 << 63})
	fixture.candidate.Candidate.Block.FileHash = bytes.Repeat([]byte{0xf1}, 32)
	fixture.candidate.Candidate.ID = fixture.candidate.Candidate.ComputeID(3)
	acceptance := fixture.acceptance(simplex.VoteFinalize, false)
	acceptance.state = acceptanceResidentState(t, acceptance.Candidate)
	accepter, err := newAcceptanceTestAccepter(fixture, &acceptanceTestNode{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = accepter.Prepare(t.Context(), acceptance, nil); err == nil || !strings.Contains(err.Error(), "file hash mismatch") {
		t.Fatalf("resident root with wrong file hash error = %v, want file hash mismatch", err)
	}
}

func TestBlockAccepterResidentRootStillChecksRootHash(t *testing.T) {
	fixture := newAcceptanceTestFixture(t, groups.ShardID{Workchain: 0, Shard: -1 << 63})
	acceptance := fixture.acceptance(simplex.VoteFinalize, false)
	acceptance.state = acceptanceResidentState(t, acceptance.Candidate)
	acceptance.state.tips[0].Block = cell.BeginCell().MustStoreUInt(0xff, 8).EndCell()
	accepter, err := newAcceptanceTestAccepter(fixture, &acceptanceTestNode{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = accepter.Prepare(t.Context(), acceptance, nil); err == nil || !strings.Contains(err.Error(), "root hash mismatch") {
		t.Fatalf("wrong resident root error = %v, want root hash mismatch", err)
	}
}
