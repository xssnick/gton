package validator

import (
	"context"
	"errors"
	"testing"

	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/simplex"
)

func TestLocalCandidateRouterRoutesAndReleasesExactSession(t *testing.T) {
	router := NewLocalCandidateRouter()
	sessionID := [32]byte{0x41}
	blockBOC := []byte{1, 2, 3}
	collatedData := []byte{4, 5, 6}
	want := collator.CandidateArtifact{
		SessionID:    sessionID,
		WindowID:     collator.WindowID{SessionID: sessionID, StartSlot: 8},
		Candidate:    simplex.Candidate{ID: simplex.CandidateID{Slot: 8}},
		BlockBOC:     blockBOC,
		CollatedData: collatedData,
	}
	delivered := make(chan collator.CandidateArtifact, 1)
	release, err := router.Register(sessionID, func(_ context.Context, artifact collator.CandidateArtifact) error {
		delivered <- artifact
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = router.Register(sessionID, func(context.Context, collator.CandidateArtifact) error {
		return nil
	}); !errors.Is(err, ErrCandidateRouteConflict) {
		t.Fatalf("duplicate registration error = %v, want ErrCandidateRouteConflict", err)
	}

	if err = router.EmitCandidate(context.Background(), want); err != nil {
		t.Fatal(err)
	}
	got := <-delivered
	if got.WindowID != want.WindowID || got.Candidate.ID != want.Candidate.ID {
		t.Fatalf("routed artifact = %+v, want %+v", got, want)
	}
	if &got.BlockBOC[0] != &blockBOC[0] || &got.CollatedData[0] != &collatedData[0] {
		t.Fatal("candidate router copied immutable payloads")
	}

	release()
	release()
	if err = router.EmitCandidate(context.Background(), want); !errors.Is(err, ErrCandidateRouteNotFound) {
		t.Fatalf("released route error = %v, want ErrCandidateRouteNotFound", err)
	}
}

func TestLocalCandidateRouterRejectsArtifactWindowFromAnotherSession(t *testing.T) {
	router := NewLocalCandidateRouter()
	err := router.EmitCandidate(context.Background(), collator.CandidateArtifact{
		SessionID: [32]byte{1},
		WindowID:  collator.WindowID{SessionID: [32]byte{2}},
	})
	if err == nil {
		t.Fatal("artifact with a foreign window session was accepted")
	}
}

func TestLocalCandidateRouterClassifiesClosedLeaderWindowAsStale(t *testing.T) {
	router := NewLocalCandidateRouter()
	sessionID := [32]byte{0x43}
	_, err := router.Register(sessionID, func(context.Context, collator.CandidateArtifact) error {
		return ErrLeaderWindowClosed
	})
	if err != nil {
		t.Fatal(err)
	}

	err = router.EmitCandidate(context.Background(), collator.CandidateArtifact{
		SessionID: sessionID,
		WindowID:  collator.WindowID{SessionID: sessionID},
	})
	if !errors.Is(err, collator.ErrStaleWindow) {
		t.Fatalf("closed window delivery error = %v, want ErrStaleWindow", err)
	}
}
