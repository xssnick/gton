package collator

import (
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/simplex"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestStandaloneEmissionCarriesBuiltSuccessor(t *testing.T) {
	fixture := newRuntimeFixture(t, 1, 1, nil, nil, nil)
	defer fixture.close(t)
	session, _ := fixture.session(0x95, 1, 0, time.Now())
	request := emptyCandidateRequest(t)
	request.CreatedBy = session.Validators[0].PublicKey
	built, err := testBuilder().BuildShard(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	window := productionWindow{
		ID:     WindowID{SessionID: session.ID, StartSlot: 0},
		Leader: 0, Authority: CandidateAuthoritySelf,
		SelfSigner: &runtimeCountingSigner{private: fixture.leaderPriv},
	}
	artifact, err := fixture.service.signArtifact(session, window, 0, simplex.Genesis(), built)
	if err != nil {
		t.Fatal(err)
	}
	parent := request.Previous.State
	root, opens := artifact.BuiltSuccessor().Over(parent, parent)
	if !opens || root != built.State {
		t.Fatal("emission lost the builder's exact successor")
	}
	if _, opens = artifact.retained().BuiltSuccessor().Over(parent, parent); opens {
		t.Fatal("retained emission pinned the successor past its handoff")
	}
	other, err := cell.FromBOC(parent.ToBOC())
	if err != nil {
		t.Fatal(err)
	}
	if _, opens = artifact.BuiltSuccessor().Over(other, other); opens {
		t.Fatal("successor opened against another materialization of its parent")
	}
	wrong := cell.BeginCell().MustStoreUInt(0xdeaf, 16).EndCell()
	if _, opens = artifact.BuiltSuccessor().Over(wrong, parent); opens {
		t.Fatal("successor opened against a different combined parent")
	}
}

func TestControllerForwardsNotarizationToCommitteePace(t *testing.T) {
	_, backend, observer := newControllerTestFixture(t)
	service := &Service{}
	backend.notarized = service.ObserveConsensusNotarized
	sessionID := [32]byte{0x51}
	pace := service.pace(Session{ID: sessionID}, 400*time.Millisecond)
	start := time.Unix(1_700_000_000, 0)
	budget := pace.budget(400 * time.Millisecond)
	for slot := uint32(0); slot < 5; slot++ {
		parent := simplex.Genesis()
		if slot != 0 {
			parent = simplex.Parent(paceCandidate(slot - 1))
		}
		pace.noteEmitted(paceCandidate(slot), paceEmission{
			at:           start.Add(time.Duration(slot) * 250 * time.Millisecond),
			targetRate:   400 * time.Millisecond,
			budget:       budget,
			window:       WindowID{SessionID: sessionID},
			parent:       parent,
			transactions: 400,
			limited:      true,
			demand:       true,
			elapsed:      budget.duration,
		})
	}
	observer.mu.Lock()
	notarized := observer.events.Notarized
	observer.mu.Unlock()
	if notarized == nil {
		t.Fatal("controller installed no notarization callback")
	}
	for slot := uint32(0); slot < 5; slot++ {
		notarized(sessionID, paceCandidate(slot), start.Add(500*time.Millisecond+time.Duration(slot)*466*time.Millisecond))
	}
	samples := pace.snapshot().samples
	if samples != 1 {
		t.Fatalf("standalone committee samples = %d, want 1 complete slow span", samples)
	}
	if after := pace.budget(400 * time.Millisecond); after.duration >= budget.duration {
		t.Fatal("standalone work budget did not respond to sustained slow certification")
	}
}
