package collator

import (
	"reflect"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

// Every deadline a slot's build runs to comes out of one constructor, so a build
// started by the producer loop and a build started anywhere else cannot end up on
// different schedules. This asserts the constructor leaves nothing behind: a
// field added to BuildRequest and filled at one call site but not here would give
// the two builds different budgets, and nothing downstream compares them.
//
// FinalizedAnchor is the single deliberate exception — it belongs to the restore
// path, which does not go through a leader window at all.
func TestSlotBuildRequestFillsEveryScheduledField(t *testing.T) {
	record := SessionRecord{
		Update: SessionUpdate{
			CurrentWindowStart:   10,
			CurrentWindowStartAt: time.Unix(1_700_000_000, 0),
			TargetRate:           480 * time.Millisecond,
		},
	}
	record.Session.Shard.Workchain = 0
	window := productionWindow{Leader: 3}
	window.ID.StartSlot = 10
	previous := &CandidateArtifact{}
	session := ActivatedSession{}
	session.Session.ID = [32]byte{1}
	parent := simplex.ParentID{Exists: true}
	parent.ID.Slot = 11

	request := slotBuildRequest(session, record, window, 12, parent, previous, adaptiveTransactionCeiling, time.Time{})

	if got, want := request.PaceStartedAt, buildStartTime(record, 12); !got.Equal(want) {
		t.Errorf("PaceStartedAt = %v, want the scheduled build start %v", got, want)
	}
	if got, want := request.ExternalWaitUntil, externalWaitUntil(record, 12); !got.Equal(want) {
		t.Errorf("ExternalWaitUntil = %v, want %v", got, want)
	}
	if got, want := request.ExternalProcessUntil, externalProcessUntil(record, 12); !got.Equal(want) {
		t.Errorf("ExternalProcessUntil = %v, want %v", got, want)
	}
	if got, want := request.BuildSoftDeadline, softBuildDeadline(record, 12); !got.Equal(want) {
		t.Errorf("BuildSoftDeadline = %v, want %v", got, want)
	}
	// On a shard the wait and the process instants are the same slot start, so
	// swapping the two helpers is invisible there. On masterchain the wait is zero
	// and the process instant is three quarters of a target rate past the slot, so
	// the swap deletes the external phase's deadline outright.
	masterRecord := record
	masterRecord.Session.Shard = groups.ShardID{Workchain: -1, Shard: -1 << 63}
	master := slotBuildRequest(session, masterRecord, window, 12, parent, previous, adaptiveTransactionCeiling, time.Time{})
	if got, want := master.ExternalWaitUntil, externalWaitUntil(masterRecord, 12); !got.Equal(want) {
		t.Errorf("masterchain ExternalWaitUntil = %v, want %v", got, want)
	}
	if got, want := master.ExternalProcessUntil, externalProcessUntil(masterRecord, 12); !got.Equal(want) {
		t.Errorf("masterchain ExternalProcessUntil = %v, want %v", got, want)
	}
	if master.ExternalWaitUntil.Equal(master.ExternalProcessUntil) {
		t.Error("the masterchain fixture cannot tell the two external instants apart")
	}
	if got, want := master.PaceStartedAt, buildStartTime(masterRecord, 12); !got.Equal(want) {
		t.Errorf("masterchain PaceStartedAt = %v, want %v", got, want)
	}

	value := reflect.ValueOf(request)
	for i := 0; i < value.NumField(); i++ {
		name := value.Type().Field(i).Name
		switch name {
		case "FinalizedAnchor":
			// The restore path, which does not go through a leader window.
			continue
		case "PreviousPending", "excludeExternals", "onSuccessor", "revokeSuccessor":
			// The handoff path sets these after the fact: the callback on every
			// request the producer starts, the other two only on a request for a
			// slot whose predecessor is not committed yet.
			continue
		case "speculative":
			// A build started before its window was observed, which by definition
			// has no scheduled slot to derive anything from: SpeculateWindow
			// builds its own request and every instant in it comes from the
			// estimate the bet was placed with.
			continue
		case "crossWindowBet":
			// The other bet, for the first slot of the next window: also not a
			// scheduled slot of this one. offerNextWindow builds its own request
			// and extrapolates the schedule a window further.
			continue
		case "sessionStartAt":
			// The third bet, window zero of a fresh session: placed at
			// activation by SpeculateSessionStart, which builds its own request
			// and stamps it with the instant the bet was placed.
			continue
		}
		if value.Field(i).IsZero() {
			t.Errorf("slotBuildRequest left %s at its zero value", name)
		}
	}
}

// successorLineage must hand on the retained form. The pointer outlives the slot
// that produced it — a build future carries it across a slot boundary — and the
// unretained artifact keeps the whole previous candidate's cell DAG alive behind
// it, which is a leak of megabytes per slot rather than a wrong answer.
func TestSuccessorLineageHandsOnTheRetainedArtifact(t *testing.T) {
	artifact := CandidateArtifact{}
	artifact.Candidate.ID.Slot = 7
	// The roots are the point. An artifact without them is its own retained form,
	// so a lineage that forgot to retain would be indistinguishable from one that
	// did — and what it would be holding is the whole block cell DAG and the
	// entire collated proof set, for the rest of the window.
	artifact.blockRoot = cell.BeginCell().MustStoreUInt(7, 8).EndCell()
	artifact.collatedRoots = []*cell.Cell{artifact.blockRoot}

	parent, previous := successorLineage(artifact)

	if parent != simplex.Parent(artifact.Candidate.ID) {
		t.Errorf("parent = %v, want the artifact's own candidate id", parent)
	}
	if previous == nil {
		t.Fatal("successorLineage returned no lineage pointer")
	}
	if previous.blockRoot != nil || previous.collatedRoots != nil {
		t.Error("the lineage pointer still holds the borrowed roots; it pins a whole slot's cells " +
			"for the rest of the window")
	}
	if !reflect.DeepEqual(*previous, artifact.retained()) {
		t.Error("the lineage pointer does not hold the retained form")
	}
}

// Which builds may seed is derived from the request alone, and it has to be:
// the acquisition cannot see whether a producer is waiting on the mutex it is
// about to hold for a queue walk, only whether this build is one of the two
// kinds that run beside an unfinished block.
func TestSeedingIsRefusedExactlyForBuildsBesideAnUnfinishedBlock(t *testing.T) {
	for _, test := range []struct {
		name    string
		request BuildRequest
		allowed bool
	}{
		{
			name:    "an ordinary slot owns its slot outright",
			request: BuildRequest{Slot: 3},
			allowed: true,
		},
		{
			name: "a pipelined successor runs in front of its predecessor's commit",
			request: BuildRequest{
				Slot:            4,
				PreviousPending: &SuccessorOffer{},
			},
		},
		{
			name: "a speculative first slot runs in front of AdvanceConsensusBase",
			request: BuildRequest{
				Slot:        8,
				speculative: &speculativeBase{},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := seedingAllowedFor(test.request); got != test.allowed {
				t.Fatalf("seedingAllowedFor = %v, want %v", got, test.allowed)
			}
		})
	}
}

// A slot whose schedule is already in the past — the window opened late — must
// not ship an underloaded block sooner than one target rate after the
// producer's previous emission: the floor extends the wait, the process bound
// and the soft deadline together, and only ever forwards. A floor before the
// schedule changes nothing, and the masterchain, which never waits for
// externals, keeps its zero wait.
func TestSlotBuildRequestSpacesAnUnderloadedSlotFromThePreviousEmission(t *testing.T) {
	record := SessionRecord{
		Update: SessionUpdate{
			CurrentWindowStart:   10,
			CurrentWindowStartAt: time.Unix(1_700_000_000, 0),
			TargetRate:           400 * time.Millisecond,
		},
	}
	window := productionWindow{Leader: 3}
	window.ID.StartSlot = 10
	session := ActivatedSession{}
	parent := simplex.ParentID{Exists: true}

	scheduled := slotBuildRequest(session, record, window, 12, parent, &CandidateArtifact{}, 300, time.Time{})
	// The previous candidate left two seconds after the slot's own start: the
	// schedule is stale, the floor is that emission plus a rate.
	late := scheduled.ExternalWaitUntil.Add(2 * time.Second)
	spaced := slotBuildRequest(session, record, window, 12, parent, &CandidateArtifact{}, 300, late)
	if !spaced.ExternalWaitUntil.Equal(late) || !spaced.ExternalProcessUntil.Equal(late) {
		t.Fatalf("wait/process = %v/%v, want the floor %v", spaced.ExternalWaitUntil, spaced.ExternalProcessUntil, late)
	}
	if want := late.Add(record.Update.TargetRate); !spaced.BuildSoftDeadline.Equal(want) {
		t.Fatalf("soft deadline = %v, want one rate past the floor %v", spaced.BuildSoftDeadline, want)
	}
	if !spaced.PaceStartedAt.Equal(scheduled.PaceStartedAt) {
		t.Fatal("the pace start is the schedule's and must not move with the floor")
	}

	// A floor before the schedule leaves the schedule alone.
	early := slotBuildRequest(session, record, window, 12, parent, &CandidateArtifact{}, 300, scheduled.ExternalWaitUntil.Add(-time.Second))
	if !early.ExternalWaitUntil.Equal(scheduled.ExternalWaitUntil) || !early.BuildSoftDeadline.Equal(scheduled.BuildSoftDeadline) {
		t.Fatal("a floor before the schedule must not change the schedule")
	}

	masterRecord := record
	masterRecord.Session.Shard = groups.ShardID{Workchain: -1, Shard: -1 << 63}
	master := slotBuildRequest(session, masterRecord, window, 12, parent, &CandidateArtifact{}, 300, late)
	if !master.ExternalWaitUntil.IsZero() {
		t.Fatalf("masterchain ExternalWaitUntil = %v, want zero: the masterchain never waits for externals", master.ExternalWaitUntil)
	}

	// The producer's floor is its previous emission plus a rate, and nothing
	// before the first emission.
	producer := &windowProducer{record: record}
	if !producer.underloadedNotBefore().IsZero() {
		t.Fatal("a producer that has emitted nothing has no floor")
	}
	producer.lastEmittedAt = time.Unix(1_700_000_100, 0)
	if got, want := producer.underloadedNotBefore(), producer.lastEmittedAt.Add(400*time.Millisecond); !got.Equal(want) {
		t.Fatalf("floor = %v, want one rate after the previous emission %v", got, want)
	}
}
