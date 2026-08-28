package validator

import (
	"context"
	"errors"
	"github.com/xssnick/gton/service/validator/collator"
	"strings"
	"testing"
	"time"

	"github.com/xssnick/gton/console"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

func TestFactoryRegistersValidatorStatusCommand(t *testing.T) {
	commands := &console.Registry{}
	store := newValidatorTestStorage()
	observedAt := time.Date(2026, time.August, 5, 9, 10, 11, 0, time.UTC)
	storedID := SessionStorageID{
		SessionID:      supervisorTestID(0x61),
		Shard:          supervisorTestShard(),
		CatchainSeqno:  7,
		IsValidator:    true,
		ValidatorKeyID: supervisorTestID(0x62),
		LocalADNLID:    supervisorTestID(0x63),
		ValidatorIndex: 4,
	}
	store.status = StorageStatus{
		PendingWrites: 3,
		DB: collator.DBMetrics{
			DiskSize:              2 << 30,
			LiveSize:              1 << 30,
			ReadAmp:               5,
			L0Files:               6,
			L0Sublevels:           2,
			CompactionDebt:        512 << 20,
			CompactionsInProgress: 1,
			MemTableSize:          8 << 20,
			WALSize:               4 << 20,
		},
		Sessions: []StoredSession{{
			ID:                      storedID,
			Candidates:              8,
			Finalized:               7,
			Votes:                   6,
			Certificates:            5,
			LeaderWindows:           4,
			FirstNonAnnouncedWindow: 12,
			LastFinalized:           &simplex.CandidateID{Slot: 19},
			LastLeaderWindow: &LeaderWindowRecord{
				StartSlot:  20,
				EndSlot:    29,
				ObservedAt: observedAt,
			},
		}},
	}

	node := validatorTestNodeWithGroups(t)
	node.Commands = commands
	extension, err := New(validatorTestOptions(Options{
		Storage:       store,
		StatsInterval: -1,
		EnableGroups:  true,
	}))(node)
	if err != nil {
		t.Fatal(err)
	}
	service := extension.(*Service)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = service.Close(ctx)
	})

	out, err := commands.Execute(context.Background(), "StAtUs VaLiDaToR")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"Validator Status\n",
		"started=false closed=false",
		"Internal Message Pool\n  destinations=0",
		"Validator Groups\n  not initialized",
		"pending_writes=3 sessions=1 disk=2.0GiB live=1.0GiB read_amp=5 l0=6/2",
		"local_index=4 role=validator",
		"first_non_announced_window=12 last_finalized_slot=19",
		"last_leader_slots=[20,29) last_leader_at=2026-08-05T09:10:11Z",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("validator status missing %q:\n%s", want, out)
		}
	}

	if _, err = commands.Execute(context.Background(), "status validator extra"); !errors.Is(err, console.ErrNotFound) {
		t.Fatalf("status validator argument error = %v, want ErrNotFound", err)
	}

	if err = service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	out, err = commands.Execute(context.Background(), "status validator")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "started=true closed=false") {
		t.Fatalf("started lifecycle is absent:\n%s", out)
	}
}

func TestFactoryFailsOnValidatorStatusCommandConflict(t *testing.T) {
	commands := &console.Registry{}
	if err := commands.Register("STATUS   VALIDATOR", func(context.Context, []string) (string, error) {
		return "existing", nil
	}); err != nil {
		t.Fatal(err)
	}

	node := validatorTestNode()
	node.Commands = commands
	_, err := New(validatorTestOptions(Options{StatsInterval: -1}))(node)
	if err == nil || !strings.Contains(err.Error(), "register validator status command") {
		t.Fatalf("factory command conflict error = %v", err)
	}
}

func TestValidatorStatusPropagatesStorageError(t *testing.T) {
	want := errors.New("status unavailable")
	store := newValidatorTestStorage()
	store.statusErr = want
	commands := &console.Registry{}
	node := validatorTestNode()
	node.Commands = commands

	extension, err := New(validatorTestOptions(Options{Storage: store, StatsInterval: -1}))(node)
	if err != nil {
		t.Fatal(err)
	}
	service := extension.(*Service)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = service.Close(ctx)
	})

	if _, err = commands.Execute(context.Background(), "status validator"); !errors.Is(err, want) {
		t.Fatalf("status command error = %v, want %v", err, want)
	}
}

func TestValidatorStatusMergesRuntimeAndDurableSession(t *testing.T) {
	localKey := supervisorTestID(0x71)
	keys := &supervisorTestKeys{ids: [][32]byte{localKey}}
	preparer := newSupervisorTestPreparer()
	store := newValidatorTestStorage()
	commands := &console.Registry{}
	node := validatorTestNodeWithGroups(t)
	node.Commands = commands

	extension, err := New(validatorTestOptions(Options{
		Keys:          keys,
		Storage:       store,
		StatsInterval: -1,
		EnableGroups:  true,
		// The replayed masterchain head is historical, so the startup catch-up
		// gate would keep the supervisor stopped until the settled-head escape
		// hatch; this test reconciles the supervisor directly and needs it
		// started at Start.
		ConsensusCatchupThreshold: -1,
		PrepareSession:            preparer.prepare,
	}))(node)
	if err != nil {
		t.Fatal(err)
	}
	service := extension.(*Service)
	if err = service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = service.Close(ctx)
	})

	group := supervisorTestGroup(0x72, supervisorTestShard(), localKey)
	service.sessions.Reconcile(supervisorTestSnapshot(190, []groups.Session{group}, nil))
	waitFor(t, func() bool {
		status := service.sessions.Status()
		return status.Running == 1 && len(status.Sessions) == 1
	}, "session status did not reach running")

	runtimeStatus := service.sessions.Status().Sessions[0]
	observedAt := time.Date(2026, time.August, 5, 10, 11, 12, 0, time.UTC)
	store.mu.Lock()
	store.status.Sessions = []StoredSession{{
		ID:                      runtimeStatus.StorageID,
		Candidates:              11,
		Finalized:               9,
		Votes:                   13,
		Certificates:            4,
		LeaderWindows:           2,
		FirstNonAnnouncedWindow: 17,
		LastFinalized:           &simplex.CandidateID{Slot: 44},
		LastLeaderWindow: &LeaderWindowRecord{
			StartSlot:  30,
			EndSlot:    39,
			ObservedAt: observedAt,
		},
	}}
	store.mu.Unlock()

	status, err := service.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.Supervisor == nil || status.Supervisor.Running != 1 || len(status.Supervisor.Sessions) != 1 {
		t.Fatalf("supervisor status = %+v", status.Supervisor)
	}
	session := status.Supervisor.Sessions[0]
	if session.LocalIndex != 0 || session.Phase != SessionPhaseRunning {
		t.Fatalf("merged runtime status = %+v", session)
	}

	out, err := commands.Execute(context.Background(), "status validator")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"masterchain_seqno=190 desired=1 preparing=0 prepared=0 running=1",
		"local_index=0 role=validator",
		"active=true phase=running",
		"candidates=11 finalized=9 votes=13 certificates=4 leader_windows=2",
		"first_non_announced_window=17 last_finalized_slot=44",
		"last_leader_slots=[30,39) last_leader_at=2026-08-05T10:11:12Z",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("merged validator status missing %q:\n%s", want, out)
		}
	}
}

func TestValidatorStatusAllowsNilCommandRegistry(t *testing.T) {
	extension, err := New(validatorTestOptions(Options{StatsInterval: -1}))(validatorTestNode())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err = extension.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestLocalGroupStatusesReportValidatorIndexesWithoutRuntime(t *testing.T) {
	localKey := supervisorTestID(0x81)
	foreignKey := supervisorTestID(0x82)
	active := supervisorTestGroup(0x83, supervisorTestShard(), foreignKey, localKey)
	future := supervisorTestGroup(0x84, groups.ShardID{Workchain: -1, Shard: -1 << 63}, localKey)
	foreign := supervisorTestGroup(0x85, supervisorTestShard(), foreignKey)
	snapshot := supervisorTestSnapshot(100, []groups.Session{active, foreign}, []groups.Session{future})

	local, err := localGroupStatuses(snapshot, [][32]byte{localKey})
	if err != nil {
		t.Fatal(err)
	}
	if len(local) != 2 {
		t.Fatalf("local groups = %+v, want active and future memberships", local)
	}
	if local[0].SessionID != active.ID || local[0].LocalIndex != 1 || !local[0].Active {
		t.Fatalf("active local membership = %+v", local[0])
	}
	if local[1].SessionID != future.ID || local[1].LocalIndex != 0 || local[1].Active {
		t.Fatalf("future local membership = %+v", local[1])
	}

	var out strings.Builder
	formatGroupStatus(&out, GroupTrackerStatus{
		Enabled:     true,
		Initialized: true,
		Local:       local,
	})
	for _, want := range []string{
		"local_index=1 shard=0:8000000000000000 catchain=7 active=true",
		"local_index=0 shard=-1:8000000000000000 catchain=7 active=false",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("group status missing %q:\n%s", want, out.String())
		}
	}
}

func TestLocalGroupStatusesRejectAmbiguousLocalRoster(t *testing.T) {
	first := supervisorTestID(0x91)
	second := supervisorTestID(0x92)
	group := supervisorTestGroup(0x93, supervisorTestShard(), first, second)
	snapshot := supervisorTestSnapshot(101, []groups.Session{group}, nil)

	if _, err := localGroupStatuses(snapshot, [][32]byte{first, second}); err == nil {
		t.Fatal("ambiguous local roster status error = nil")
	}
}
