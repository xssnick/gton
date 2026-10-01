package collator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
	"github.com/xssnick/gton/service/validator/simplex"
)

func TestMasterAcquisitionRejectsCandidateAcrossSessionRotation(t *testing.T) {
	fixture, shardState := localValidationMasterFixture(t)
	session := localMasterTestSession(t, fixture.request.Groups)
	fixture.request.CreatedBy = session.Validators[0].PublicKey
	lifetime := fixture.request.Groups.Config.Catchain.MasterchainLifetime
	fixture.request.Header.GenUtime = (fixture.oldState.GenUTime/lifetime + 1) * lifetime
	fixture.request.Header.GenUtimeMS = uint64(fixture.request.Header.GenUtime) * 1000
	first, err := testBuilder().BuildMaster(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	tracker, err := groups.NewTracker(groups.TrackerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	previous := candidatePrevious(t, first)
	rotated, err := tracker.Project(fixture.request.Groups, groups.ApplyInput{
		Block: first.ID,
		Root:  first.State,
		AsOf:  time.Unix(int64(fixture.request.Header.GenUtime), 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	next, err := activeMasterSession(rotated)
	if err != nil {
		t.Fatal(err)
	}
	if next.ID == session.ID || next.ValidatorSetHash == session.ValidatorSetHash {
		t.Fatal("fixture did not rotate the masterchain validator session")
	}
	secondRequest := fixture.request
	secondRequest.Previous = previous
	secondRequest.Groups = rotated
	secondRequest.Header.GenUtime++
	secondRequest.Header.GenUtimeMS += 1000
	second, err := testBuilder().BuildMaster(context.Background(), secondRequest)
	if err != nil {
		t.Fatal(err)
	}
	pool := msgpool.New(msgpool.Config{})
	defer pool.Close()
	if err = pool.Internals().ReconcileDestinations([]msgpool.ShardIdent{targetShardIdent(session.Shard)}); err != nil {
		t.Fatal(err)
	}
	acquisition, err := NewLocalAcquisition(LocalAcquisitionOptions{
		Builder: testBuilder(),
		Store: &localValidationStore{states: []localValidationState{
			{block: fixture.request.Previous.ID, root: fixture.request.Previous.State},
			{block: first.ID, root: first.State},
			{block: fixture.oldShard, root: shardState},
		}},
		Groups:    &localAcquisitionTestGroups{snapshot: fixture.request.Groups, projected: rotated},
		Messages:  pool,
		Semantics: testCandidateTransitionVerifier,
	})
	if err != nil {
		t.Fatal(err)
	}
	update := localValidationSessionUpdate(t, session.Session, fixture.request.Groups, fixture.request.Previous.ID)
	_, err = acquisition.ValidateCandidate(context.Background(), ValidationRequest{
		Session:  session,
		Update:   update,
		Previous: []PreviousBlock{previous},
		Candidate: simplex.Candidate{
			Leader:           0,
			Block:            second.ID,
			CollatedFileHash: second.CollatedFileHash,
		},
		BlockBOC:     second.BlockBOC,
		CollatedData: second.CollatedData,
	})
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("candidate from the next session validated in the old session: %v", err)
	}

	update.HasCurrentWindow = true
	update.CurrentWindowStartAt = time.Unix(int64(fixture.request.Header.GenUtime), 0)
	if err = acquisition.PrepareSession(context.Background(), session.Session, update); err != nil {
		t.Fatal(err)
	}
	if err = acquisition.ActivateSession(context.Background(), SessionActivation{
		SessionID:      session.ID,
		Genesis:        session.Genesis,
		MinMasterchain: session.MinMasterchain,
	}, update); err != nil {
		t.Fatal(err)
	}
	parent := simplex.Candidate{Block: first.ID, CollatedFileHash: first.CollatedFileHash}
	parent.ID = parent.ComputeID(0)
	artifact := CandidateArtifact{SessionID: session.ID, Candidate: parent}
	managed, err := acquisition.session(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	view, err := acquisition.masterViewForPredecessor(context.Background(), managed.master, previous, update.CurrentWindowStartAt)
	if err != nil {
		t.Fatal(err)
	}
	for _, cached := range []bool{false, true} {
		state := localCandidateState{block: previous}
		if cached {
			state.master = view
		}
		managed.candidates[parent.ID] = state
		_, err = acquisition.AcquireMaster(context.Background(), BuildRequest{
			Session:  session,
			Update:   update,
			Slot:     1,
			Parent:   simplex.Parent(parent.ID),
			Previous: &artifact,
		})
		if !errors.Is(err, ErrAcquisitionNotReady) {
			t.Fatalf("old session built across rotation (cached=%t): %v", cached, err)
		}
	}
}
