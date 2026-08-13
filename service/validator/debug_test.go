package validator

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/xssnick/gton/console"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/simplex"
)

type validatorDebugTestStorage struct {
	*validatorTestStorage
	sessions      []SessionStorageID
	sessionsErr   error
	candidate     CandidateRecord
	candidateErr  error
	loadedID      simplex.CandidateID
	loadedSession SessionStorageID
}

type validatorFailingStatusCollator struct {
	validatorTestCollator
	status    collator.Status
	statusErr error
}

func (c *validatorFailingStatusCollator) Status(context.Context) (collator.Status, error) {
	return c.status, c.statusErr
}

func (s *validatorDebugTestStorage) Sessions(context.Context) ([]SessionStorageID, error) {
	return append([]SessionStorageID(nil), s.sessions...), s.sessionsErr
}

func (s *validatorDebugTestStorage) Candidate(
	_ context.Context,
	session SessionStorageID,
	id simplex.CandidateID,
) (CandidateRecord, error) {
	s.loadedSession = session
	s.loadedID = id
	if s.candidateErr != nil {
		return CandidateRecord{}, s.candidateErr
	}

	return CandidateRecord{
		ID:   s.candidate.ID,
		Wire: append([]byte(nil), s.candidate.Wire...),
	}, nil
}

func TestValidatorDebugStatusJSON(t *testing.T) {
	commands := &console.Registry{}
	store := &validatorDebugTestStorage{validatorTestStorage: newValidatorTestStorage()}
	session := debugTestSessionStorageID()
	lastFinalized := simplex.CandidateID{Slot: 27, Hash: supervisorTestID(0x41)}
	store.status = StorageStatus{
		PendingWrites: 2,
		Sessions: []StoredSession{{
			ID:            session,
			Candidates:    9,
			Finalized:     8,
			LastFinalized: &lastFinalized,
		}},
	}

	node := validatorTestNode()
	node.Commands = commands
	extension, err := New(validatorTestOptions(Options{
		Storage:       store,
		StatsInterval: -1,
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

	output, err := commands.Execute(context.Background(), "DeBuG VaLiDaToR StAtUs --json")
	if err != nil {
		t.Fatal(err)
	}
	var status validatorDebugStatus
	if err = json.Unmarshal([]byte(output), &status); err != nil {
		t.Fatalf("decode debug status: %v\n%s", err, output)
	}
	if status.Schema != validatorDebugStatusSchema {
		t.Fatalf("schema = %q, want %q", status.Schema, validatorDebugStatusSchema)
	}
	if status.Lifecycle.Started || status.Lifecycle.Closed {
		t.Fatalf("lifecycle = %+v", status.Lifecycle)
	}
	if status.Consensus != nil || status.Collator.Enabled {
		t.Fatalf("disabled runtime sections = consensus %+v collator %+v", status.Consensus, status.Collator)
	}
	if status.Storage.PendingWrites != 2 || len(status.Storage.Sessions) != 1 {
		t.Fatalf("storage status = %+v", status.Storage)
	}
	if status.Storage.SessionsTotal != 1 || status.Storage.SessionsTruncated {
		t.Fatalf("storage session summary = %+v", status.Storage)
	}
	wantNamespace, err := session.Namespace()
	if err != nil {
		t.Fatal(err)
	}
	stored := status.Storage.Sessions[0]
	if stored.Namespace != hex.EncodeToString(wantNamespace[:]) ||
		stored.LastFinalized == nil || stored.LastFinalized.Slot != 27 {
		t.Fatalf("stored session = %+v", stored)
	}
	if status.Groups.Local == nil || status.Storage.Sessions == nil {
		t.Fatal("bounded JSON collections must encode as arrays, not null")
	}

	if _, err = commands.Execute(context.Background(), "debug validator status"); !errors.Is(err, console.ErrNotFound) {
		t.Fatalf("status without JSON flag error = %v, want ErrNotFound", err)
	}
}

func TestValidatorDebugStatusBoundsHistoricalSessions(t *testing.T) {
	commands := &console.Registry{}
	store := &validatorDebugTestStorage{validatorTestStorage: newValidatorTestStorage()}
	active := debugStatusSessionStorageID(2_000)
	const historicalSessionCount = 1_000
	historical := make([]StoredSession, 0, historicalSessionCount)
	for i := historicalSessionCount; i >= 1; i-- {
		historical = append(historical, StoredSession{
			ID:         debugStatusSessionStorageID(uint16(i)),
			Candidates: uint64(i),
		})
	}
	store.status = StorageStatus{Sessions: append(historical, StoredSession{ID: active})}

	node := validatorTestNode()
	node.Commands = commands
	extension, err := New(validatorTestOptions(Options{
		Storage:       store,
		StatsInterval: -1,
		PrepareSession: func(context.Context, SessionConfig, SessionState, SessionStart) (SessionRuntime, error) {
			return nil, errors.New("test session preparation must not run")
		},
	}))(node)
	if err != nil {
		t.Fatal(err)
	}
	service := extension.(*Service)
	service.sessions.statusMu.Lock()
	service.sessions.status = SessionSupervisorStatus{
		Sessions: []SessionRuntimeStatus{{
			StorageID:     active,
			SessionID:     active.SessionID,
			ADNLID:        active.LocalADNLID,
			LocalIndex:    active.ValidatorIndex,
			Shard:         active.Shard,
			CatchainSeqno: active.CatchainSeqno,
			Active:        true,
			Phase:         SessionPhaseRunning,
		}},
	}
	service.sessions.statusMu.Unlock()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = service.Close(ctx)
	})

	output, err := commands.Execute(context.Background(), "debug validator status --json")
	if err != nil {
		t.Fatal(err)
	}
	if len(output) > maxValidatorDebugStatusBytes {
		t.Fatalf("debug output is %d bytes, limit is %d", len(output), maxValidatorDebugStatusBytes)
	}
	var status validatorDebugStatus
	if err = json.Unmarshal([]byte(output), &status); err != nil {
		t.Fatalf("decode debug status: %v", err)
	}
	if status.Storage.SessionsTotal != uint64(len(store.status.Sessions)) || !status.Storage.SessionsTruncated {
		t.Fatalf("storage summary = %+v", status.Storage)
	}
	if len(status.Storage.Sessions) != maxValidatorDebugStoredSessions {
		t.Fatalf("storage sample length = %d, want %d", len(status.Storage.Sessions), maxValidatorDebugStoredSessions)
	}
	activeIdentity, err := makeValidatorDebugSessionIdentity(active)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(status.Storage.Sessions, func(session validatorDebugStoredSession) bool {
		return session.Namespace == activeIdentity.Namespace
	}) {
		t.Fatalf("runtime storage session %s is absent from %+v", activeIdentity.Namespace, status.Storage.Sessions)
	}
	if status.Consensus == nil || !slices.ContainsFunc(status.Consensus.Sessions, func(session validatorDebugRuntimeSession) bool {
		return session.Namespace == activeIdentity.Namespace && session.Active
	}) {
		t.Fatalf("runtime consensus session %s is absent from %+v", activeIdentity.Namespace, status.Consensus)
	}
}

func TestSelectValidatorDebugStoredSessionsIsDeterministicAndPrioritizesRuntime(t *testing.T) {
	active := debugStatusSessionStorageID(250)
	stored := make([]StoredSession, 0, maxValidatorDebugStoredSessions+2)
	for i := maxValidatorDebugStoredSessions + 1; i >= 1; i-- {
		stored = append(stored, StoredSession{ID: debugStatusSessionStorageID(uint16(i))})
	}
	stored = append(stored, StoredSession{ID: active})
	supervisor := &SessionSupervisorStatus{Sessions: []SessionRuntimeStatus{{StorageID: active}}}

	first := selectValidatorDebugStoredSessions(stored, supervisor)
	reversed := append([]StoredSession(nil), stored...)
	slices.Reverse(reversed)
	second := selectValidatorDebugStoredSessions(reversed, supervisor)
	if len(first) != maxValidatorDebugStoredSessions || len(second) != len(first) {
		t.Fatalf("sample lengths = %d and %d", len(first), len(second))
	}
	for i := range first {
		if first[i].ID != second[i].ID {
			t.Fatalf("sample differs at %d: %+v != %+v", i, first[i].ID, second[i].ID)
		}
		if i > 0 && !sessionStorageIDLess(first[i-1].ID, first[i].ID) {
			t.Fatalf("sample is not canonically ordered at %d", i)
		}
	}
	if !slices.ContainsFunc(first, func(session StoredSession) bool { return session.ID == active }) {
		t.Fatal("runtime session was not selected")
	}
	if slices.ContainsFunc(first, func(session StoredSession) bool {
		return session.ID == debugStatusSessionStorageID(uint16(maxValidatorDebugStoredSessions+1))
	}) {
		t.Fatal("non-runtime history displaced by runtime priority was selected")
	}
}

func TestValidatorDebugStatusDegradesOnCollatorStatusError(t *testing.T) {
	commands := &console.Registry{}
	store := &validatorDebugTestStorage{validatorTestStorage: newValidatorTestStorage()}
	statusErr := errors.New("invalid collator candidate record version: " + strings.Repeat("x", 700))
	localCollator := &validatorFailingStatusCollator{statusErr: statusErr}
	node := validatorTestNode()
	node.Commands = commands
	extension, err := New(validatorTestOptions(Options{
		Storage:       store,
		LocalCollator: localCollator,
		StatsInterval: -1,
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

	output, err := commands.Execute(context.Background(), "debug validator status --json")
	if err != nil {
		t.Fatal(err)
	}
	var status validatorDebugStatus
	if err = json.Unmarshal([]byte(output), &status); err != nil {
		t.Fatalf("decode degraded validator status: %v\n%s", err, output)
	}
	if !status.Collator.Enabled || !strings.Contains(status.Collator.StatusError, "invalid collator candidate") {
		t.Fatalf("degraded collator section = %+v", status.Collator)
	}
	if len([]rune(status.Collator.StatusError)) != maxDebugStatusErrorRunes+1 {
		t.Fatalf("bounded status error length = %d", len([]rune(status.Collator.StatusError)))
	}
	if status.Storage.Sessions == nil {
		t.Fatal("validator storage was not reported after collator status failure")
	}

	if _, err = commands.Execute(context.Background(), "status validator"); !errors.Is(err, statusErr) {
		t.Fatalf("strict validator status error = %v, want %v", err, statusErr)
	}

	output, err = commands.Execute(context.Background(), "debug collator status --json")
	if err != nil {
		t.Fatal(err)
	}
	var integrated integratedCollatorDebugStatus
	if err = json.Unmarshal([]byte(output), &integrated); err != nil {
		t.Fatalf("decode integrated collator status: %v\n%s", err, output)
	}
	if integrated.Schema != collatorDebugStatusSchema || integrated.Mode != "integrated" ||
		!strings.Contains(integrated.StatusError, "invalid collator candidate") {
		t.Fatalf("integrated collator status = %+v", integrated)
	}
}

func TestIntegratedCollatorDebugDoesNotReplaceExplicitOwner(t *testing.T) {
	commands := &console.Registry{}
	if err := commands.Register("debug collator", func(context.Context, []string) (string, error) {
		return "standalone", nil
	}); err != nil {
		t.Fatal(err)
	}
	node := validatorTestNode()
	node.Commands = commands
	extension, err := New(validatorTestOptions(Options{
		LocalCollator: &validatorTestCollator{},
		StatsInterval: -1,
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

	output, err := commands.Execute(context.Background(), "debug collator status --json")
	if err != nil {
		t.Fatal(err)
	}
	if output != "standalone" {
		t.Fatalf("explicit collator owner output = %q, want standalone", output)
	}
}

func TestValidatorDebugExportCandidate(t *testing.T) {
	commands := &console.Registry{}
	session := debugTestSessionStorageID()
	namespace, err := session.Namespace()
	if err != nil {
		t.Fatal(err)
	}
	id := simplex.CandidateID{Slot: 417, Hash: supervisorTestID(0x51)}
	wire := []byte("canonical candidate wire")
	store := &validatorDebugTestStorage{
		validatorTestStorage: newValidatorTestStorage(),
		sessions:             []SessionStorageID{session},
		candidate:            CandidateRecord{ID: id, Wire: wire},
	}
	node := validatorTestNode()
	node.Commands = commands
	extension, err := New(validatorTestOptions(Options{
		Storage:       store,
		StatsInterval: -1,
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

	path := filepath.Join(t.TempDir(), "candidate.wire")
	command := fmt.Sprintf(
		"debug validator export-candidate %x %d %x %s",
		namespace,
		id.Slot,
		id.Hash,
		path,
	)
	output, err := commands.Execute(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	var result candidateExportResult
	if err = json.Unmarshal([]byte(output), &result); err != nil {
		t.Fatalf("decode export result: %v\n%s", err, output)
	}
	if result.Schema != candidateExportSchema || result.Namespace != hex.EncodeToString(namespace[:]) ||
		result.Slot != id.Slot || result.CandidateHash != hex.EncodeToString(id.Hash[:]) ||
		result.Bytes != len(wire) || result.Path != path {
		t.Fatalf("export result = %+v", result)
	}
	exported, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(exported, wire) {
		t.Fatalf("exported wire = %q, want %q", exported, wire)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != candidateExportFileMode {
		t.Fatalf("export mode = %o, want %o", info.Mode().Perm(), candidateExportFileMode)
	}
	if store.loadedSession != session || store.loadedID != id {
		t.Fatalf("loaded candidate = (%+v, %+v), want (%+v, %+v)", store.loadedSession, store.loadedID, session, id)
	}

	if _, err = commands.Execute(context.Background(), command); err == nil ||
		!strings.Contains(err.Error(), "already exists") {
		t.Fatalf("second export error = %v, want no-overwrite error", err)
	}
	exported, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(exported, wire) {
		t.Fatalf("wire changed after rejected overwrite: %q", exported)
	}
}

func TestValidatorDebugExportCandidateRejectsInvalidSelector(t *testing.T) {
	commands := &console.Registry{}
	store := &validatorDebugTestStorage{validatorTestStorage: newValidatorTestStorage()}
	node := validatorTestNode()
	node.Commands = commands
	extension, err := New(validatorTestOptions(Options{
		Storage:       store,
		StatsInterval: -1,
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

	path := filepath.Join(t.TempDir(), "candidate.wire")
	_, err = commands.Execute(
		context.Background(),
		fmt.Sprintf("debug validator export-candidate short 1 %064x %s", 1, path),
	)
	if err == nil || !strings.Contains(err.Error(), "session namespace") {
		t.Fatalf("short namespace error = %v", err)
	}

	_, err = commands.Execute(
		context.Background(),
		fmt.Sprintf("debug validator export-candidate %064x 1 %064x %s", 2, 3, path),
	)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("unknown namespace error = %v, want storage ErrNotFound", err)
	}
}

func TestFactoryFailsOnValidatorDebugCommandConflict(t *testing.T) {
	commands := &console.Registry{}
	if err := commands.Register("debug validator", func(context.Context, []string) (string, error) {
		return "existing", nil
	}); err != nil {
		t.Fatal(err)
	}

	node := validatorTestNode()
	node.Commands = commands
	_, err := New(validatorTestOptions(Options{StatsInterval: -1}))(node)
	if err == nil || !strings.Contains(err.Error(), "register validator debug command") {
		t.Fatalf("factory command conflict error = %v", err)
	}
}

func debugTestSessionStorageID() SessionStorageID {
	return SessionStorageID{
		SessionID:      supervisorTestID(0x11),
		Shard:          supervisorTestShard(),
		CatchainSeqno:  19,
		IsValidator:    true,
		ValidatorKeyID: supervisorTestID(0x12),
		LocalADNLID:    supervisorTestID(0x13),
		ValidatorIndex: 2,
		Protocol: SessionProtocol{
			Version:              3,
			ProtocolVersion:      3,
			SlotsPerLeaderWindow: 10,
		},
	}
}

func debugStatusSessionStorageID(suffix uint16) SessionStorageID {
	id := debugTestSessionStorageID()
	id.SessionID[len(id.SessionID)-2] = byte(suffix >> 8)
	id.SessionID[len(id.SessionID)-1] = byte(suffix)

	return id
}
