package validator

import (
	"bytes"
	"crypto/ed25519"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

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

func TestCandidateCodecBroadcastSplitMatchesWrappedAndOwnsInputs(t *testing.T) {
	config, leaderKey := runtimeTestConfig(0x65, &runtimeTestJournal{})
	codec, err := newCandidateCodec(config, CandidateLimits{
		MaxBlockBytes:        1 << 20,
		MaxCollatedDataBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	ordinary := runtimeOrdinaryArtifact(t, config, leaderKey, 0, simplex.Genesis())
	delegated := runtimeDelegatedArtifact(t, config, leaderKey, ordinary)

	for _, test := range []struct {
		name     string
		artifact *CandidateArtifact
	}{
		{name: "validator", artifact: ordinary},
		{name: "delegated", artifact: delegated},
	} {
		t.Run(test.name, func(t *testing.T) {
			canonical, err := simplex.SerializeCandidate(
				test.artifact.Candidate,
				test.artifact.BlockBOC,
				test.artifact.CollatedData,
			)
			if err != nil {
				t.Fatal(err)
			}
			wrapped, err := codec.decode(canonical, &test.artifact.Candidate.ID)
			if err != nil {
				t.Fatal(err)
			}
			broadcast, err := simplex.SerializeCandidateForBroadcast(
				test.artifact.Candidate,
				test.artifact.BlockBOC,
				test.artifact.CollatedData,
			)
			if err != nil {
				t.Fatal(err)
			}
			extra, err := simplex.ParseBroadcastExtra(broadcast.Extra)
			if err != nil {
				t.Fatal(err)
			}

			split, lazy, err := codec.decodeBroadcastDeferred(
				broadcast.Data,
				extra.Delegation,
				test.artifact.Candidate.ID.Slot,
			)
			if err != nil {
				t.Fatal(err)
			}
			assertCandidateArtifactEqual(t, split, wrapped)
			if !sameDelegation(split.Candidate.Delegation, wrapped.Candidate.Delegation) {
				t.Fatal("split broadcast decode changed the delegation")
			}
			if lazy.wire != nil || lazy.blockRoot == nil {
				t.Fatal("split broadcast decode eagerly materialized the canonical wire")
			}

			// Transport buffers end with the callback. Destroy both inputs before
			// the first lazy-wire consumer to prove the artifact owns everything
			// canonical materialization retains.
			clear(broadcast.Data)
			if extra.Delegation != nil {
				clear(extra.Delegation.CollatorKey)
				clear(extra.Delegation.Signature)
			}
			wire, _, err := lazy.materialize()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(wire, canonical) {
				t.Fatal("lazy split decode did not reproduce the byte-canonical wrapped wire")
			}
		})
	}
}

func TestCandidateCodecBroadcastRechecksSlotAndSignatures(t *testing.T) {
	config, leaderKey := runtimeTestConfig(0x66, &runtimeTestJournal{})
	codec, err := newCandidateCodec(config, CandidateLimits{
		MaxBlockBytes:        1 << 20,
		MaxCollatedDataBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	ordinary := runtimeOrdinaryArtifact(t, config, leaderKey, 0, simplex.Genesis())
	delegated := runtimeDelegatedArtifact(t, config, leaderKey, ordinary)
	broadcast, err := simplex.SerializeCandidateForBroadcast(
		delegated.Candidate,
		delegated.BlockBOC,
		delegated.CollatedData,
	)
	if err != nil {
		t.Fatal(err)
	}
	extra, err := simplex.ParseBroadcastExtra(broadcast.Extra)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name       string
		payload    []byte
		delegation *simplex.Delegation
		slot       uint32
	}{
		{
			name:       "wrong broadcast slot",
			payload:    broadcast.Data,
			delegation: extra.Delegation,
			slot:       delegated.Candidate.ID.Slot + 1,
		},
		{
			name:       "missing delegation",
			payload:    broadcast.Data,
			delegation: nil,
			slot:       delegated.Candidate.ID.Slot,
		},
		{
			name:       "bad delegation signature",
			payload:    broadcast.Data,
			delegation: corruptDelegationSignature(extra.Delegation),
			slot:       delegated.Candidate.ID.Slot,
		},
		{
			name:       "bad candidate signature",
			payload:    forgedCandidateBroadcast(t, delegated),
			delegation: extra.Delegation,
			slot:       delegated.Candidate.ID.Slot,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := codec.decodeBroadcast(test.payload, test.delegation, test.slot); err == nil {
				t.Fatal("invalid split candidate broadcast was accepted")
			}
		})
	}
}

func TestCandidateCodecBroadcastChecksSlotBeforePayloadDecode(t *testing.T) {
	config, leaderKey := runtimeTestConfig(0x68, &runtimeTestJournal{})
	codec, err := newCandidateCodec(config, CandidateLimits{
		MaxBlockBytes:        1 << 20,
		MaxCollatedDataBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	artifact := runtimeOrdinaryArtifact(t, config, leaderKey, 0, simplex.Genesis())
	broadcast, err := simplex.SerializeCandidateForBroadcast(
		artifact.Candidate,
		artifact.BlockBOC,
		artifact.CollatedData,
	)
	if err != nil {
		t.Fatal(err)
	}
	data := candidateBroadcastBlockData(t, broadcast.Data)
	data.Slot++
	data.Candidate = []byte{0xff}
	payload, err := tl.Serialize(data, true)
	if err != nil {
		t.Fatal(err)
	}

	if _, err = codec.decodeBroadcast(payload, nil, artifact.Candidate.ID.Slot); err == nil ||
		!strings.Contains(err.Error(), "broadcast slot mismatch") {
		t.Fatalf("wrong-slot corrupt payload error = %v, want slot mismatch before payload decode", err)
	}
}

func TestCandidateCodecRejectsNegativeWireSlotAndInvalidShape(t *testing.T) {
	config, leaderKey := runtimeTestConfig(0x69, &runtimeTestJournal{})
	codec, err := newCandidateCodec(config, CandidateLimits{
		MaxBlockBytes:        1 << 20,
		MaxCollatedDataBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	artifact := runtimeOrdinaryArtifact(t, config, leaderKey, 0, simplex.Genesis())
	broadcast, err := simplex.SerializeCandidateForBroadcast(
		artifact.Candidate,
		artifact.BlockBOC,
		artifact.CollatedData,
	)
	if err != nil {
		t.Fatal(err)
	}
	negative := candidateBroadcastBlockData(t, broadcast.Data)
	negative.Slot = -1
	negativeID := artifact.Candidate.ComputeID(math.MaxUint32)
	negative.Signature, err = simplex.SignCandidate(
		runtimeTestSigner{key: leaderKey},
		config.SessionID,
		negativeID,
	)
	if err != nil {
		t.Fatal(err)
	}
	negativePayload, err := tl.Serialize(negative, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = codec.decodeBroadcast(negativePayload, nil, math.MaxUint32); err == nil ||
		!strings.Contains(err.Error(), "slot is negative") {
		t.Fatalf("negative candidate slot error = %v", err)
	}

	parent := simplex.CandidateID{Slot: 1, Hash: [32]byte{0x91}}
	malformed := runtimeOrdinaryArtifact(t, config, leaderKey, 1, simplex.Parent(parent))
	validShape := malformed.Candidate
	validShape.Parent = simplex.Genesis()
	validShape.ID = validShape.ComputeID(validShape.ID.Slot)
	validShape.Signature, err = simplex.SignCandidate(
		runtimeTestSigner{key: leaderKey},
		config.SessionID,
		validShape.ID,
	)
	if err != nil {
		t.Fatal(err)
	}
	validBroadcast, err := simplex.SerializeCandidateForBroadcast(
		validShape,
		malformed.BlockBOC,
		malformed.CollatedData,
	)
	if err != nil {
		t.Fatal(err)
	}
	invalidShape := candidateBroadcastBlockData(t, validBroadcast.Data)
	invalidShape.Parent = ton.ConsensusCandidateParent{ID: ton.ConsensusCandidateID{
		Slot: int32(parent.Slot),
		Hash: parent.Hash[:],
	}}
	invalidShape.Signature = malformed.Candidate.Signature
	invalidPayload, err := tl.Serialize(invalidShape, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = codec.decodeBroadcast(invalidPayload, nil, malformed.Candidate.ID.Slot); err == nil ||
		!strings.Contains(err.Error(), "candidate shape") {
		t.Fatalf("invalid candidate shape error = %v", err)
	}
}

func TestCandidateCodecBroadcastEmptyAndV2CanonicalWire(t *testing.T) {
	config, leaderKey := runtimeTestConfig(0x6a, &runtimeTestJournal{})
	codec, err := newCandidateCodec(config, CandidateLimits{
		MaxBlockBytes:        1 << 20,
		MaxCollatedDataBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("empty", func(t *testing.T) {
		candidate := simplex.Candidate{
			Parent: simplex.Parent(simplex.CandidateID{Slot: 0, Hash: [32]byte{0x11}}),
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
		candidate.ID = candidate.ComputeID(1)
		candidate.Signature, err = simplex.SignCandidate(
			runtimeTestSigner{key: leaderKey},
			config.SessionID,
			candidate.ID,
		)
		if err != nil {
			t.Fatal(err)
		}
		broadcast, err := simplex.SerializeCandidateForBroadcast(candidate, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		artifact, lazy, err := codec.decodeBroadcastDeferred(broadcast.Data, nil, candidate.ID.Slot)
		if err != nil {
			t.Fatal(err)
		}
		if !artifact.Candidate.Empty || lazy.wire == nil || lazy.blockRoot != nil {
			t.Fatal("empty broadcast did not take the eager cheap-wire path")
		}
		canonical, err := simplex.SerializeCandidate(candidate, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		wire, _, err := lazy.materialize()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(wire, canonical) {
			t.Fatal("empty broadcast lazy wire differs from canonical wire")
		}
	})

	t.Run("compressed v2", func(t *testing.T) {
		v1Config := config
		v1Config.Protocol.ProtocolVersion = 1
		v1Codec, err := newCandidateCodec(v1Config, CandidateLimits{
			MaxBlockBytes:        1 << 20,
			MaxCollatedDataBytes: 1 << 20,
		})
		if err != nil {
			t.Fatal(err)
		}
		artifact := runtimeOrdinaryArtifact(t, config, leaderKey, 0, simplex.Genesis())
		broadcast, err := simplex.SerializeCandidateForBroadcast(
			artifact.Candidate,
			artifact.BlockBOC,
			artifact.CollatedData,
		)
		if err != nil {
			t.Fatal(err)
		}
		data := candidateBroadcastBlockData(t, broadcast.Data)
		blockRoot, err := cell.FromBOC(artifact.BlockBOC)
		if err != nil {
			t.Fatal(err)
		}
		collatedRoots, err := cell.FromBOCMultiRoot(artifact.CollatedData)
		if err != nil {
			t.Fatal(err)
		}
		roots := append([]*cell.Cell{blockRoot}, collatedRoots...)
		compressed, err := cell.CompressBOC(roots, cell.CompressionImprovedStructureLZ4, nil)
		if err != nil {
			t.Fatal(err)
		}
		data.Candidate, err = tl.Serialize(simplex.ValidatorSessionCompressedCandidateV2{
			Source:   make([]byte, 32),
			Round:    int32(artifact.Candidate.Block.SeqNo),
			RootHash: artifact.Candidate.Block.RootHash,
			Data:     compressed,
		}, true)
		if err != nil {
			t.Fatal(err)
		}
		v2Payload, err := tl.Serialize(data, true)
		if err != nil {
			t.Fatal(err)
		}
		decoded, lazy, err := v1Codec.decodeBroadcastDeferred(
			v2Payload,
			nil,
			artifact.Candidate.ID.Slot,
		)
		if err != nil {
			t.Fatal(err)
		}
		assertCandidateArtifactEqual(t, decoded, artifact)
		assertPreparedBlockRoute(t, 1, decoded)
		canonical, err := simplex.SerializeCandidate(
			artifact.Candidate,
			artifact.BlockBOC,
			artifact.CollatedData,
		)
		if err != nil {
			t.Fatal(err)
		}
		wire, _, err := lazy.materialize()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(wire, canonical) {
			t.Fatal("v2 broadcast did not materialize baseline byte-canonical wire")
		}
	})
}

func candidateBroadcastBlockData(t testing.TB, payload []byte) simplex.ConsensusBlockData {
	t.Helper()

	data, err := simplex.ParseCandidateData(payload)
	if err != nil {
		t.Fatal(err)
	}
	block, ok := data.(simplex.ConsensusBlockData)
	if !ok {
		t.Fatalf("candidate broadcast data type = %T", data)
	}

	return block
}

func runtimeDelegatedArtifact(
	t testing.TB,
	config SessionConfig,
	leaderKey ed25519.PrivateKey,
	ordinary *CandidateArtifact,
) *CandidateArtifact {
	t.Helper()

	collatorKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0xc9}, ed25519.SeedSize))
	collatorPublic := collatorKey.Public().(ed25519.PublicKey)
	delegated := *ordinary
	delegated.Candidate = ordinary.Candidate
	delegated.Candidate.Delegation = &simplex.Delegation{CollatorKey: collatorPublic}
	var err error
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

	return &delegated
}

func sameDelegation(left, right *simplex.Delegation) bool {
	if left == nil || right == nil {
		return left == right
	}

	return bytes.Equal(left.CollatorKey, right.CollatorKey) &&
		bytes.Equal(left.Signature, right.Signature)
}

func corruptDelegationSignature(delegation *simplex.Delegation) *simplex.Delegation {
	corrupt := cloneDelegation(delegation)
	corrupt.Signature[0] ^= 0xff

	return corrupt
}

func forgedCandidateBroadcast(t testing.TB, artifact *CandidateArtifact) []byte {
	t.Helper()

	forged := *artifact
	forged.Candidate = artifact.Candidate
	forged.Candidate.Signature = bytes.Clone(artifact.Candidate.Signature)
	forged.Candidate.Signature[0] ^= 0xff
	broadcast, err := simplex.SerializeCandidateForBroadcast(
		forged.Candidate,
		forged.BlockBOC,
		forged.CollatedData,
	)
	if err != nil {
		t.Fatal(err)
	}

	return broadcast.Data
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
	if !artifact.generationTimeKnown {
		t.Fatal("decoded candidate did not retain generation time from its collated roots")
	}
	want, err := candidateGenUtime(artifact.CollatedData)
	if err != nil {
		t.Fatal(err)
	}
	if artifact.generationTimeMS != uint64(want.UnixMilli()) {
		t.Fatalf("decoded generation time = %d, want %d", artifact.generationTimeMS, want.UnixMilli())
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

var (
	candidateBroadcastPayloadSink    []byte
	candidateBroadcastDelegationSink *simplex.Delegation
)

// BenchmarkCandidateBroadcastReceiveBoundary isolates the allocation removed
// from delegated receive. The old boundary built consensus.candidate around
// the entire bare FEC payload; the split boundary retains that payload only for
// the synchronous decode and copies the two small delegation fields it keeps.
func BenchmarkCandidateBroadcastReceiveBoundary(b *testing.B) {
	config, leaderKey := runtimeTestConfig(0x67, &runtimeTestJournal{})
	artifact := benchmarkPreparedArtifact(b, config, leaderKey, 6850, 4330)
	artifact = runtimeDelegatedArtifact(b, config, leaderKey, artifact)
	broadcast, err := simplex.SerializeCandidateForBroadcast(
		artifact.Candidate,
		artifact.BlockBOC,
		artifact.CollatedData,
	)
	if err != nil {
		b.Fatal(err)
	}
	extra, err := simplex.ParseBroadcastExtra(broadcast.Extra)
	if err != nil {
		b.Fatal(err)
	}
	received := simplex.CandidateBroadcast{Data: broadcast.Data, Extra: broadcast.Extra}

	b.Run("wrapped_input", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(received.Data)))
		for b.Loop() {
			candidateBroadcastPayloadSink, err = received.CandidateWire(extra.Delegation)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("split_input", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(received.Data)))
		for b.Loop() {
			candidateBroadcastPayloadSink = received.Data
			candidateBroadcastDelegationSink = cloneDelegation(extra.Delegation)
		}
	})
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
