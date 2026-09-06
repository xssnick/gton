package collator

import (
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/groups"
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
	shard := groups.ShardID{Workchain: 0, Shard: -1 << 63}
	pace := service.pace(shard)
	start := time.Unix(1_700_000_000, 0)
	for slot := uint32(0); slot < 3; slot++ {
		pace.noteEmitted(paceCandidate(slot), start.Add(time.Duration(slot)*250*time.Millisecond), 400)
	}
	observer.mu.Lock()
	notarized := observer.events.Notarized
	observer.mu.Unlock()
	if notarized == nil {
		t.Fatal("controller installed no notarization callback")
	}
	for slot := uint32(0); slot < 3; slot++ {
		notarized(shard, paceCandidate(slot), start.Add(500*time.Millisecond+time.Duration(slot)*466*time.Millisecond))
	}
	_, samples := pace.estimate()
	if samples != 2 {
		t.Fatalf("standalone committee samples = %d, want 2", samples)
	}
	if cap := pace.transactionCap(400 * time.Millisecond); cap == adaptiveTransactionStart {
		t.Fatal("standalone transaction cap did not adapt to certification feedback")
	}
}
