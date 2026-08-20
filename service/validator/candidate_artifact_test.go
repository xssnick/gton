package validator

import (
	"bytes"
	"crypto/ed25519"
	"fmt"
	"testing"

	"github.com/xssnick/gton/service/validator/simplex"
)

func TestCandidateCodecRoundTripValidatorAndDelegatedSupportedProtocols(t *testing.T) {
	for protocolVersion := uint8(0); protocolVersion <= simplex.MaxProtocolVersion; protocolVersion++ {
		t.Run(fmt.Sprintf("protocol_%d", protocolVersion), func(t *testing.T) {
			config, leaderKey := runtimeTestConfig(0x61, &runtimeTestJournal{})
			config.Protocol.ProtocolVersion = protocolVersion
			codec, err := newCandidateCodec(config, CandidateLimits{
				MaxBlockBytes:        1 << 20,
				MaxCollatedDataBytes: 1 << 20,
			})
			if err != nil {
				t.Fatal(err)
			}
			ordinary := runtimeOrdinaryArtifact(t, config, leaderKey, 0, simplex.Genesis())

			wire, err := simplex.SerializeCandidate(ordinary.Candidate, ordinary.BlockBOC, ordinary.CollatedData)
			if err != nil {
				t.Fatal(err)
			}
			broadcastWire, broadcast, err := codec.encodeForBroadcast(ordinary)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(broadcastWire, wire) || !bytes.Equal(broadcast.Data, wire) {
				t.Fatal("plain candidate resolver wire differs from FEC candidate data")
			}
			extra, err := simplex.ParseBroadcastExtra(broadcast.Extra)
			if err != nil {
				t.Fatal(err)
			}
			if extra.Slot != ordinary.Candidate.ID.Slot || extra.Delegation != nil {
				t.Fatalf("plain candidate broadcast extra = %+v", extra)
			}
			decoded, err := codec.decode(wire, &ordinary.Candidate.ID)
			if err != nil {
				t.Fatal(err)
			}
			assertCandidateArtifactEqual(t, decoded, ordinary)
			assertDecodedValidationRoots(t, decoded)
			assertPreparedBlockRoute(t, protocolVersion, decoded)
			if decoded.Candidate.Delegation != nil {
				t.Fatal("validator candidate gained delegation")
			}

			collatorSeed := bytes.Repeat([]byte{0xc7}, ed25519.SeedSize)
			collatorKey := ed25519.NewKeyFromSeed(collatorSeed)
			collatorPublic := collatorKey.Public().(ed25519.PublicKey)
			delegated := *ordinary
			delegated.Candidate = ordinary.Candidate
			delegated.Candidate.Delegation = &simplex.Delegation{CollatorKey: collatorPublic}
			delegated.Candidate.Delegation.Signature, err = simplex.SignDelegation(
				runtimeTestSigner{key: leaderKey},
				config.SessionID,
				0,
				simplex.KeyNodeIDShort(collatorPublic),
			)
			if err != nil {
				t.Fatal(err)
			}
			delegated.Candidate.Signature, err = simplex.SignCandidate(
				runtimeTestSigner{key: collatorKey},
				config.SessionID,
				delegated.Candidate.ID,
			)
			if err != nil {
				t.Fatal(err)
			}

			wire, err = simplex.SerializeCandidate(delegated.Candidate, delegated.BlockBOC, delegated.CollatedData)
			if err != nil {
				t.Fatal(err)
			}
			broadcastWire, broadcast, err = codec.encodeForBroadcast(&delegated)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(broadcastWire, wire) || bytes.Equal(broadcast.Data, wire) {
				t.Fatal("delegated resolver wire was not derived from the bare FEC payload")
			}
			extra, err = simplex.ParseBroadcastExtra(broadcast.Extra)
			if err != nil {
				t.Fatal(err)
			}
			if extra.Slot != delegated.Candidate.ID.Slot || extra.Delegation == nil ||
				!bytes.Equal(extra.Delegation.CollatorKey, collatorPublic) {
				t.Fatalf("delegated candidate broadcast extra = %+v", extra)
			}
			decoded, err = codec.decode(wire, &delegated.Candidate.ID)
			if err != nil {
				t.Fatal(err)
			}
			assertCandidateArtifactEqual(t, decoded, &delegated)
			assertDecodedValidationRoots(t, decoded)
			assertPreparedBlockRoute(t, protocolVersion, decoded)
			if decoded.Candidate.Delegation == nil ||
				!bytes.Equal(decoded.Candidate.Delegation.CollatorKey, collatorPublic) ||
				!bytes.Equal(decoded.Candidate.Delegation.Signature, delegated.Candidate.Delegation.Signature) {
				t.Fatal("delegation changed during candidate round trip")
			}

			// A delegated candidate is produced locally by a standalone collator,
			// so it now arrives with its payload already compressed from the roots
			// it was built from. The capsule must be a pure shortcut: identical
			// resolver wire, identical FEC payload, identical extra — and consumed
			// on use, so a retained artifact does not hold a third copy of a
			// payload the resolver already keeps as wire.
			prepared := delegated
			prepared.prepared = preparedFromArtifactBOCs(t, &delegated)
			preparedWire, preparedBroadcast, err := codec.encodeForBroadcast(&prepared)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(preparedWire, broadcastWire) ||
				!bytes.Equal(preparedBroadcast.Data, broadcast.Data) ||
				!bytes.Equal(preparedBroadcast.Extra, broadcast.Extra) {
				t.Fatal("prepared delegated encoding differs from the full path")
			}
			if prepared.prepared != nil {
				t.Fatal("prepared payload survived encodeForBroadcast")
			}
		})
	}
}

func assertDecodedValidationRoots(t *testing.T, artifact *CandidateArtifact) {
	t.Helper()

	if artifact.validationRoots == nil || artifact.validationRoots.block == nil {
		t.Fatal("decoded candidate carries no validation roots")
	}
	if !bytes.Equal(artifact.validationRoots.block.Hash(), artifact.Candidate.Block.RootHash) {
		t.Fatal("decoded validation block root differs from the candidate")
	}
	if len(artifact.validationRoots.collated) == 0 {
		t.Fatal("decoded candidate carries no collated roots")
	}
}

func assertPreparedBlockRoute(t *testing.T, protocolVersion uint8, artifact *CandidateArtifact) {
	t.Helper()

	prepared := artifact.PreparedBlockCandidate()
	if protocolVersion != 1 {
		if prepared != nil {
			t.Fatalf("protocol %d retained a legacy cache artifact", protocolVersion)
		}
		return
	}
	if prepared == nil {
		t.Fatal("protocol 1 candidate has no prepared cache artifact")
	}
	if id := prepared.ID(); !sameBlockID(id, artifact.Candidate.Block) {
		t.Fatalf("prepared block id differs from candidate: got %+v want %+v", id, artifact.Candidate.Block)
	}
	if !bytes.Equal(prepared.BlockBOC(), artifact.BlockBOC) {
		t.Fatal("prepared block BOC differs from candidate artifact")
	}
}

func TestCandidateCodecRejectsUnsupportedSimplexConfig(t *testing.T) {
	for _, protocol := range []SessionProtocol{
		{Version: 1, ProtocolVersion: 3, SlotsPerLeaderWindow: 4},
		{Version: 2, ProtocolVersion: simplex.MaxProtocolVersion + 1, SlotsPerLeaderWindow: 4},
	} {
		config, _ := runtimeTestConfig(0x60+protocol.ProtocolVersion, &runtimeTestJournal{})
		config.Protocol = protocol
		if _, err := newCandidateCodec(config, CandidateLimits{
			MaxBlockBytes:        1 << 20,
			MaxCollatedDataBytes: 1 << 20,
		}); err == nil {
			t.Fatalf("unsupported protocol %+v was accepted", protocol)
		}
	}
}

func TestCandidateCodecEnforcesDecodeLimitsBeforeAdmission(t *testing.T) {
	config, leaderKey := runtimeTestConfig(0x62, &runtimeTestJournal{})
	wide, err := newCandidateCodec(config, CandidateLimits{
		MaxBlockBytes:        1 << 20,
		MaxCollatedDataBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	artifact := runtimeOrdinaryArtifact(t, config, leaderKey, 0, simplex.Genesis())
	wire, err := simplex.SerializeCandidate(artifact.Candidate, artifact.BlockBOC, artifact.CollatedData)
	if err != nil {
		t.Fatal(err)
	}

	narrow, err := newCandidateCodec(config, CandidateLimits{MaxBlockBytes: 1, MaxCollatedDataBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = narrow.decode(wire, nil); err == nil {
		t.Fatal("oversized candidate passed narrow decode limits")
	}

	wrongID := artifact.Candidate.ID
	wrongID.Hash[0] ^= 0xff
	if _, err = wide.decode(wire, &wrongID); err == nil {
		t.Fatal("candidate decoded for a different requested id")
	}
}

func BenchmarkCandidateCodecEncode(b *testing.B) {
	config, leaderKey := runtimeTestConfig(0x63, &runtimeTestJournal{})
	codec, err := newCandidateCodec(config, CandidateLimits{
		MaxBlockBytes:        1 << 20,
		MaxCollatedDataBytes: 1 << 20,
	})
	if err != nil {
		b.Fatal(err)
	}
	artifact := runtimeOrdinaryArtifact(b, config, leaderKey, 0, simplex.Genesis())
	b.ReportAllocs()
	b.SetBytes(int64(len(artifact.BlockBOC) + len(artifact.CollatedData)))
	b.ResetTimer()

	for range b.N {
		if _, _, err = codec.encodeForBroadcast(artifact); err != nil {
			b.Fatal(err)
		}
	}
}

func assertCandidateArtifactEqual(t *testing.T, got, want *CandidateArtifact) {
	t.Helper()

	if got.Candidate.ID != want.Candidate.ID || got.Candidate.Parent != want.Candidate.Parent ||
		got.Candidate.Leader != want.Candidate.Leader || got.Candidate.Empty != want.Candidate.Empty ||
		!sameBlockID(got.Candidate.Block, want.Candidate.Block) ||
		got.Candidate.CollatedFileHash != want.Candidate.CollatedFileHash ||
		!bytes.Equal(got.Candidate.Signature, want.Candidate.Signature) ||
		!bytes.Equal(got.BlockBOC, want.BlockBOC) || !bytes.Equal(got.CollatedData, want.CollatedData) {
		t.Fatalf("candidate artifact changed during round trip\ngot:  %+v\nwant: %+v", got.Candidate, want.Candidate)
	}
}
