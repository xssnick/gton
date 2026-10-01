package collator

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/simplex"
)

// preparedRouteCandidate builds a candidate whose BOCs and capsule are all
// serializations of the same two roots, the way collation.finish produces them.
// The fake candidates elsewhere in this file carry string payloads, which are
// enough for signature and marker checks but cannot be compressed.
func preparedRouteCandidate(t *testing.T, session Session, slot uint32, createdBy [32]byte) *Candidate {
	t.Helper()

	blockRoot := cell.BeginCell().MustStoreUInt(uint64(slot)+0xb10c, 64).EndCell()
	blockBOC, err := blockRoot.ToBOCWithOptionsErr(cell.BOCSerializeOptions{
		WithCRC32C:    true,
		WithIndex:     true,
		WithCacheBits: true,
		WithIntHashes: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	collatedRoots := []*cell.Cell{
		cell.BeginCell().MustStoreUInt(uint64(slot)+0xc011a, 64).EndCell(),
		cell.BeginCell().MustStoreUInt(uint64(slot)+0xdada, 64).EndCell(),
	}
	collatedData, err := cell.ToBOCWithOptionsErr(collatedRoots, cell.BOCSerializeOptions{WithCRC32C: true})
	if err != nil {
		t.Fatal(err)
	}

	seqNo := slot + 1
	fileHash := sha256.Sum256(blockBOC)
	collatedFileHash := sha256.Sum256(collatedData)
	rootHash := blockRoot.Hash()
	prepared, err := simplex.PrepareCandidate(seqNo, blockRoot, collatedRoots, fileHash, collatedFileHash,
		simplex.PayloadCellHint(blockBOC, collatedData))
	if err != nil {
		t.Fatal(err)
	}

	built := runtimeBuiltCandidate(BuildRequest{Session: ActivatedSession{Session: session}, Slot: slot})
	built.ID.SeqNo = seqNo
	built.ID.RootHash = append([]byte(nil), rootHash...)
	built.ID.FileHash = append([]byte(nil), fileHash[:]...)
	built.CreatedBy = createdBy
	built.BlockBOC = blockBOC
	built.CollatedData = collatedData
	built.CollatedFileHash = collatedFileHash
	built.State = cell.BeginCell().MustStoreUInt(uint64(slot)+0x57a7e, 64).EndCell()
	built.StateUpdate = cell.BeginCell().MustStoreUInt(uint64(slot)+0xadd, 64).EndCell()
	built.prepared = prepared
	built.provenance = newCandidateProvenance(built, blockRoot)

	return built
}

// TestSignArtifactCarriesPreparedForBothAuthorities pins the producer side that
// both emit routes now read. The delegated authority is the one that matters:
// its artifact used to lose the capsule one conversion later, in
// ConsensusObserver.BroadcastCandidate, so nothing downstream could have
// noticed had signArtifact dropped it too.
func TestSignArtifactCarriesPreparedForBothAuthorities(t *testing.T) {
	fixture := newRuntimeFixture(t, 1, 1, nil, nil, nil)
	defer fixture.close(t)

	session, _ := fixture.session(0x71, 1, 0, time.Now())
	authorities := map[string]CandidateAuthority{
		"self":      CandidateAuthoritySelf,
		"delegated": CandidateAuthorityDelegated,
	}
	for name, authority := range authorities {
		t.Run(name, func(t *testing.T) {
			built := preparedRouteCandidate(t, session, 0, session.Validators[0].PublicKey)
			artifact, err := fixture.service.signArtifact(
				session,
				productionWindow{
					Leader:     0,
					Authority:  authority,
					SelfSigner: &runtimeCountingSigner{private: fixture.leaderPriv},
				},
				0,
				simplex.Genesis(),
				built,
			)
			if err != nil {
				t.Fatal(err)
			}
			if artifact.Prepared() != built.prepared {
				t.Fatalf("signed %s artifact prepared = %p, want the built capsule %p",
					name, artifact.Prepared(), built.prepared)
			}

			// The capsule must serialize to exactly what the BOC path produces,
			// or the two routes would put different bytes on the wire.
			viaCapsule, err := simplex.SerializeCandidateForBroadcastPrepared(
				artifact.Candidate,
				artifact.Prepared(),
			)
			if err != nil {
				t.Fatal(err)
			}
			viaBOCs, err := simplex.SerializeCandidateForBroadcast(
				artifact.Candidate,
				artifact.BlockBOC,
				artifact.CollatedData,
			)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(viaCapsule.Data, viaBOCs.Data) || !bytes.Equal(viaCapsule.Extra, viaBOCs.Extra) {
				t.Fatal("prepared broadcast differs from the full path")
			}
		})
	}
}

// TestArtifactFromRecordKeepsRememberedPreparedPayload covers the window-replay
// path, on both routes. It is off the hot path, but keeping the capsule is what
// makes rememberEmitted's retention of it worth its memory: the alternative is
// holding a compressed payload for the life of the window and then rebuilding
// it from the BOCs anyway.
//
// The two hash checks artifactFromRecord runs against the persisted marker are
// exactly the binding a serializer would otherwise re-derive, so this is the
// one replay site where reusing the capsule is proven right where it is used.
func TestArtifactFromRecordKeepsRememberedPreparedPayload(t *testing.T) {
	fixture := newRuntimeFixture(t, 1, 1, nil, nil, nil)
	defer fixture.close(t)

	session, _ := fixture.session(0x73, 1, 0, time.Now())
	window := productionWindow{
		ID:         WindowID{SessionID: session.ID, StartSlot: 0},
		Leader:     0,
		Authority:  CandidateAuthoritySelf,
		SelfSigner: &runtimeCountingSigner{private: fixture.leaderPriv},
	}
	built := preparedRouteCandidate(t, session, 0, session.Validators[0].PublicKey)
	emitted, err := fixture.service.signArtifact(session, window, 0, simplex.Genesis(), built)
	if err != nil {
		t.Fatal(err)
	}
	record := CandidateRecord{
		WindowID:         window.ID,
		Authority:        window.Authority,
		ID:               emitted.Candidate.ID,
		Parent:           emitted.Candidate.Parent,
		Leader:           emitted.Candidate.Leader,
		Block:            emitted.Candidate.Block,
		CollatedFileHash: emitted.Candidate.CollatedFileHash,
		Signature:        emitted.Candidate.Signature,
	}

	replayed, err := fixture.service.artifactFromRecord(session, window, record, emitted, true)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.Prepared() != emitted.Prepared() {
		t.Fatalf("replayed artifact prepared = %p, want the remembered capsule %p",
			replayed.Prepared(), emitted.Prepared())
	}

	// A remembered artifact that does not answer to the signed marker is a
	// different block; the capsule must not smuggle it past the hash checks.
	corrupted := emitted
	corrupted.CollatedData = append([]byte(nil), emitted.CollatedData...)
	corrupted.CollatedData[len(corrupted.CollatedData)-1] ^= 0xff
	if _, err = fixture.service.artifactFromRecord(session, window, record, corrupted, true); !errors.Is(
		err,
		ErrCandidateConflict,
	) {
		t.Fatalf("replay over a corrupted artifact error = %v, want ErrCandidateConflict", err)
	}
}

// TestSignEmptyArtifactCarriesNoPreparedPayload keeps the negative half: an
// empty candidate has no payload, and PreparedCandidate.bind rejects one that
// claims otherwise.
func TestSignEmptyArtifactCarriesNoPreparedPayload(t *testing.T) {
	fixture := newRuntimeFixture(t, 1, 1, nil, nil, nil)
	defer fixture.close(t)

	session, _ := fixture.session(0x72, 1, 0, time.Now())
	parent := simplex.Parent(simplex.CandidateID{Slot: 0, Hash: [32]byte{0x01}})
	artifact, err := fixture.service.signEmptyArtifact(
		session,
		productionWindow{
			Leader:     0,
			Authority:  CandidateAuthoritySelf,
			SelfSigner: &runtimeCountingSigner{private: fixture.leaderPriv},
		},
		1,
		parent,
		runtimeTestBlockID(session.Shard.Workchain, session.Shard.Shard, 1),
	)
	if err != nil {
		t.Fatal(err)
	}
	if artifact.Prepared() != nil {
		t.Fatal("empty candidate artifact carries a prepared payload")
	}
}
