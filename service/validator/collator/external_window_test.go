package collator

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/simplex"
)

func TestExternalWindowDeduplicatesNormalizedMessagesAcrossBlocks(t *testing.T) {
	for _, workers := range []int{-1, 1, 4} {
		t.Run(fmt.Sprintf("workers_%d", workers), func(t *testing.T) {
			req := newExternalWaveFixture(t, 1, 1, 2).request
			req.internalWaveWorkers = workers
			inputs := req.Externals
			invalidRoot, err := tlb.ToCell(&tlb.ExternalMessage{
				DstAddr: address.NewAddress(0, 1, make([]byte, 32)),
				Body:    cell.BeginCell().EndCell(),
			})
			if err != nil {
				t.Fatal(err)
			}
			invalid := externalInput(t, invalidRoot)
			variant := func(input ExternalInput, fee uint64) ExternalInput {
				message := *input.message.Message().AsExternalIn()
				message.ImportFee = tlb.FromNanoTONU(fee)
				root, err := tlb.ToCell(&message)
				if err != nil {
					t.Fatal(err)
				}
				result := externalInput(t, root)
				if result.Ref.Hash == input.Ref.Hash || result.normalizedHash != input.normalizedHash {
					t.Fatal("fixture must change the raw hash while preserving the normalized hash")
				}
				return result
			}

			// Each block receives fresh wire encodings of all prior messages. The
			// first block also has a budget skip which must remain eligible later.
			batches := [][]ExternalInput{
				{invalid, variant(invalid, 1), inputs[0], variant(inputs[0], 1), inputs[1], inputs[2]},
				{variant(invalid, 2), variant(inputs[0], 2), variant(inputs[1], 2), inputs[2]},
				{variant(invalid, 3), variant(inputs[0], 3), variant(inputs[1], 3), variant(inputs[2], 3)},
			}
			for block, batch := range batches {
				req.Externals = batch
				req.MaxExternalAttempts = 2
				candidate, err := testBuilder().BuildShard(context.Background(), req)
				if err != nil {
					t.Fatalf("block %d: %v", block, err)
				}
				stats := candidate.Stats
				switch block {
				case 0:
					if stats.ExternalIncluded != 1 || stats.ExternalNotAccepted != 1 || stats.ExternalInvalid != 1 ||
						stats.ExternalSkippedLimit != 1 || stats.ExternalSkippedDuplicate != 2 {
						t.Fatalf("first block stats: %+v", stats)
					}
				case 1:
					if stats.ExternalIncluded != 1 || stats.ExternalAttempts != 1 || stats.ExternalSkippedDuplicate != 3 {
						t.Fatalf("second block stats: %+v", stats)
					}
				case 2:
					if stats.ExternalAttempts != 0 || stats.ExternalSkippedDuplicate != 4 || len(candidate.Externals) != 0 {
						t.Fatalf("third block retried a processed message: %+v", stats)
					}
				}
				want := [][32]byte{invalid.normalizedHash, inputs[0].normalizedHash, inputs[1].normalizedHash}
				if block > 0 {
					want = append(want, inputs[2].normalizedHash)
				}
				if !slices.Equal(candidate.processedExternals, want) {
					t.Fatalf("block %d processed hashes = %x, want %x", block, candidate.processedExternals, want)
				}
				root, err := cell.FromBOC(candidate.BlockBOC)
				if err != nil {
					t.Fatal(err)
				}
				queueSize := candidate.Stats.OutQueueSize
				req.Previous = PreviousBlock{ID: candidate.ID, Block: root, State: candidate.State, OutQueueSize: &queueSize}
				req.processedExternals = candidate.processedExternals
				req.Header.GenUtime++
				req.Header.GenUtimeMS = uint64(req.Header.GenUtime) * 1000
			}
		})
	}
}

func TestExternalWindowHistorySurvivesArtifactReplayAndResetsAtNextWindow(t *testing.T) {
	fixture := newRuntimeFixture(t, 1, 1, nil, nil, nil)
	defer fixture.close(t)
	session, _ := fixture.session(0x75, 3, 0, time.Now())
	window := productionWindow{
		ID:         WindowID{SessionID: session.ID, StartSlot: 0},
		Leader:     0,
		Authority:  CandidateAuthoritySelf,
		SelfSigner: &runtimeCountingSigner{private: fixture.leaderPriv},
	}
	built := preparedRouteCandidate(t, session, 0, session.Validators[0].PublicKey)
	want := [][32]byte{{1}, {2}, {3}}
	built.processedExternals = want
	emitted, err := fixture.service.signArtifact(session, window, 0, simplex.Genesis(), built)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := fixture.service.artifactFromRecord(session, window, recordFromArtifact(emitted), emitted.retained(), true)
	if err != nil {
		t.Fatal(err)
	}
	parent, previous := successorLineage(replayed)
	request := slotBuildRequest(ActivatedSession{Session: session}, SessionRecord{}, window, 1, parent, previous, 1, time.Time{})
	if !slices.Equal(request.excludeExternals, want) {
		t.Fatalf("replayed successor history = %x, want %x", request.excludeExternals, want)
	}
	empty, err := fixture.service.signEmptyArtifact(session, window, 1, parent, replayed.Candidate.Block)
	if err != nil {
		t.Fatal(err)
	}
	empty.processedExternals = request.excludeExternals
	empty, err = fixture.service.artifactFromRecord(session, window, recordFromArtifact(empty), empty.retained(), true)
	if err != nil {
		t.Fatal(err)
	}
	parent, previous = successorLineage(empty)
	request = slotBuildRequest(ActivatedSession{Session: session}, SessionRecord{}, window, 2, parent, previous, 1, time.Time{})
	if !slices.Equal(request.excludeExternals, want) {
		t.Fatalf("empty replay lost window history: %x", request.excludeExternals)
	}
	window.ID.StartSlot = session.SlotsPerLeaderWindow
	request = slotBuildRequest(ActivatedSession{Session: session}, SessionRecord{}, window, window.ID.StartSlot, parent, previous, 1, time.Time{})
	if len(request.excludeExternals) != 0 {
		t.Fatalf("new leader window inherited exclusions: %x", request.excludeExternals)
	}
}
