package collator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/ton"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

type remoteCollatorTransportTest struct {
	id [32]byte

	starts      int
	closes      int
	record      SessionRecord
	update      SessionUpdate
	retired     [32]byte
	probeQuery  AuthenticatedQuery
	probe       simplex.ConsensusPleaseCollatePrepare
	commitQuery AuthenticatedQuery
	commit      simplex.ConsensusPleaseCollate
	session     SessionRecord
	err         error
}

func remoteCollatorTestSession(id [32]byte) (Session, SessionUpdate) {
	return Session{
			ID:                   id,
			Shard:                groups.ShardID{Workchain: 0, Shard: -1 << 63},
			ConsensusVersion:     2,
			ProtocolVersion:      3,
			SlotsPerLeaderWindow: 4,
			Validators: []SessionValidator{{
				PublicKey: [32]byte{3},
				ADNLID:    [32]byte{4},
				Weight:    1,
			}},
		}, SessionUpdate{
			SessionID:        id,
			TargetRate:       time.Second,
			MasterchainBlock: runtimeTestBlockID(-1, -1<<63, 10),
		}
}

func (t *remoteCollatorTransportTest) CollatorID() [32]byte { return t.id }

func (t *remoteCollatorTransportTest) Start(context.Context) error {
	t.starts++

	return t.err
}

func (t *remoteCollatorTransportTest) Close(context.Context) error {
	t.closes++

	return t.err
}

func (t *remoteCollatorTransportTest) Session(context.Context, [32]byte) (SessionRecord, error) {
	if t.err != nil {
		return SessionRecord{}, t.err
	}
	if t.session.Session.ID == ([32]byte{}) {
		return SessionRecord{}, ErrNotFound
	}

	return t.session, nil
}

func (t *remoteCollatorTransportTest) PrepareSession(_ context.Context, record SessionRecord) error {
	t.record = record

	return t.err
}

func (t *remoteCollatorTransportTest) UpdateSession(_ context.Context, update SessionUpdate) error {
	t.update = update

	return t.err
}

func (t *remoteCollatorTransportTest) RetireSession(_ context.Context, sessionID [32]byte) error {
	t.retired = sessionID

	return t.err
}

func (t *remoteCollatorTransportTest) Probe(
	_ context.Context,
	query AuthenticatedQuery,
	request simplex.ConsensusPleaseCollatePrepare,
) error {
	t.probeQuery = query
	t.probe = request

	return t.err
}

func (t *remoteCollatorTransportTest) Commit(
	_ context.Context,
	query AuthenticatedQuery,
	request simplex.ConsensusPleaseCollate,
) error {
	t.commitQuery = query
	t.commit = request

	return t.err
}

func TestRemoteCollatorMapsDelegatedV3Requests(t *testing.T) {
	transport := &remoteCollatorTransportTest{id: [32]byte{1}}
	client, err := NewRemoteCollator(transport)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err = client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err = client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if transport.starts != 1 {
		t.Fatalf("transport starts = %d, want 1", transport.starts)
	}

	session, update := remoteCollatorTestSession([32]byte{2})
	if _, err = client.Session(ctx, session.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown session error = %v, want ErrNotFound", err)
	}
	update.HasCurrentWindow = true
	update.CurrentWindowStart = 12
	update.CurrentWindowObservedSlot = 12
	update.CurrentWindowStartAt = time.Unix(100, 0)
	if err = client.PrepareSession(ctx, session, update); err != nil {
		t.Fatal(err)
	}
	if transport.record.Session.ID != ([32]byte{}) {
		t.Fatal("remote lifecycle unexpectedly reached delegated-collation transport")
	}
	loaded, err := client.Session(ctx, session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Session.ID != session.ID || loaded.Update.CurrentWindowStart != 12 {
		t.Fatalf("loaded record = %+v", loaded)
	}

	update.CurrentWindowStart = 16
	update.CurrentWindowObservedSlot = 16
	update.CurrentWindowStartAt = time.Unix(104, 0)
	if err = client.UpdateSession(ctx, update); err != nil {
		t.Fatal(err)
	}
	loaded, err = client.Session(ctx, session.ID)
	if err != nil || loaded.Update.CurrentWindowStart != 16 {
		t.Fatalf("updated local record = %+v, err %v", loaded, err)
	}

	source := [32]byte{3}
	if err = client.Probe(ctx, WindowPreparation{
		SessionID:  session.ID,
		SourceADNL: source,
		StartSlot:  20,
	}); err != nil {
		t.Fatal(err)
	}
	if transport.probeQuery != (AuthenticatedQuery{SessionID: session.ID, SourceADNL: source}) ||
		transport.probe.WindowStartSlot != 20 {
		t.Fatalf("probe = (%+v, %+v)", transport.probeQuery, transport.probe)
	}

	request := WindowRequest{
		SessionID:  session.ID,
		SourceADNL: source,
		PleaseCollate: simplex.ConsensusPleaseCollate{
			WindowStartSlot: 20,
			Signature:       []byte{4, 5},
		},
	}
	if err = client.CommitDelegation(ctx, request); err != nil {
		t.Fatal(err)
	}
	if transport.commitQuery != (AuthenticatedQuery{SessionID: session.ID, SourceADNL: source}) ||
		transport.commit.WindowStartSlot != 20 || len(transport.commit.Signature) != 2 {
		t.Fatalf("commit = (%+v, %+v)", transport.commitQuery, transport.commit)
	}

	if err = client.RetireSession(ctx, session.ID); err != nil {
		t.Fatal(err)
	}
	if transport.retired != ([32]byte{}) {
		t.Fatal("remote retirement unexpectedly reached delegated-collation transport")
	}
	if err = client.UpdateSession(ctx, update); !errors.Is(err, ErrNotFound) {
		t.Fatalf("update retired session = %v, want %v", err, ErrNotFound)
	}
	if err = client.RetireSession(ctx, session.ID); err != nil {
		t.Fatalf("idempotent local retirement: %v", err)
	}

	if err = client.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err = client.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if transport.closes != 1 {
		t.Fatalf("transport closes = %d, want 1", transport.closes)
	}
	if err = client.PrepareSession(ctx, session, update); !errors.Is(err, ErrClosed) {
		t.Fatalf("prepare after close = %v, want %v", err, ErrClosed)
	}
}

func TestNewRemoteCollatorRejectsMissingIdentity(t *testing.T) {
	if _, err := NewRemoteCollator(nil); err == nil {
		t.Fatal("nil transport was accepted")
	}
	if _, err := NewRemoteCollator(&remoteCollatorTransportTest{}); err == nil {
		t.Fatal("zero collator identity was accepted")
	}
}

func TestRemoteCollatorClassifiesWindowTransportFailures(t *testing.T) {
	transport := &remoteCollatorTransportTest{id: [32]byte{1}}
	client, err := NewRemoteCollator(transport)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err = client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	session, update := remoteCollatorTestSession([32]byte{2})
	if err = client.PrepareSession(ctx, session, update); err != nil {
		t.Fatal(err)
	}

	transportErr := errors.New("transport disconnected")
	transport.err = errors.Join(ErrUnavailable, transportErr)
	err = client.Probe(ctx, WindowPreparation{SessionID: session.ID})
	if !errors.Is(err, ErrUnavailable) || !errors.Is(err, transportErr) {
		t.Fatalf("probe error = %v, want unavailable transport error", err)
	}
	err = client.CommitDelegation(ctx, WindowRequest{SessionID: session.ID})
	if !errors.Is(err, ErrUnavailable) || !errors.Is(err, transportErr) {
		t.Fatalf("commit error = %v, want unavailable transport error", err)
	}

	serverErr := errors.New("remote server rejected request")
	transport.err = serverErr
	err = client.Probe(ctx, WindowPreparation{SessionID: session.ID})
	if !errors.Is(err, serverErr) || errors.Is(err, ErrUnavailable) {
		t.Fatalf("probe server error = %v, want fatal server error", err)
	}
	err = client.CommitDelegation(ctx, WindowRequest{SessionID: session.ID})
	if !errors.Is(err, serverErr) || errors.Is(err, ErrUnavailable) {
		t.Fatalf("commit server error = %v, want fatal server error", err)
	}

	for _, domainErr := range []error{ErrUnauthorized, ErrWindowConflict, ErrAlreadyDelegated} {
		transport.err = domainErr
		err = client.Probe(ctx, WindowPreparation{SessionID: session.ID})
		if !errors.Is(err, domainErr) || errors.Is(err, ErrUnavailable) {
			t.Fatalf("probe domain error = %v, want only %v", err, domainErr)
		}

		err = client.CommitDelegation(ctx, WindowRequest{SessionID: session.ID})
		if !errors.Is(err, domainErr) || errors.Is(err, ErrUnavailable) {
			t.Fatalf("commit domain error = %v, want only %v", err, domainErr)
		}
	}
}

func TestRemoteCollatorOwnsTransportBoundarySlices(t *testing.T) {
	transport := &remoteCollatorTransportTest{id: [32]byte{1}}
	client, err := NewRemoteCollator(transport)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err = client.Start(ctx); err != nil {
		t.Fatal(err)
	}

	session, baseUpdate := remoteCollatorTestSession([32]byte{2})
	genesis := runtimeTestBlockID(0, -1<<63, 7)
	activation := SessionActivation{
		SessionID:      session.ID,
		Genesis:        []ton.BlockIDExt{genesis},
		MinMasterchain: baseUpdate.MasterchainBlock,
	}
	update := baseUpdate
	update.Registered = []groups.ShardDescription{{
		Shard: groups.ShardID{Workchain: 0, Shard: -1 << 63},
		Block: runtimeTestBlockID(0, -1<<63, 8),
	}}
	if err = client.PrepareSession(ctx, session, update); err != nil {
		t.Fatal(err)
	}
	if err = client.ActivateSession(ctx, activation); err != nil {
		t.Fatal(err)
	}
	if err = client.PrepareSession(ctx, session, update); err != nil {
		t.Fatalf("idempotent prepare after activation: %v", err)
	}
	wantGenesisRoot := genesis.RootHash[0]
	session.Validators[0].PublicKey[0] = 0xff
	activation.Genesis[0].RootHash[0] = 0xff
	wantRegisteredRoot := update.Registered[0].Block.RootHash[0]
	update.Registered[0].Block.RootHash[0] ^= 0xff
	loaded, err := client.Session(ctx, session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Session.Validators[0].PublicKey[0] != 3 || loaded.Activation == nil ||
		loaded.Activation.Genesis[0].RootHash[0] != wantGenesisRoot ||
		loaded.Update.Registered[0].Block.RootHash[0] != wantRegisteredRoot {
		t.Fatal("remote lifecycle cache retained caller-owned slices")
	}
	loaded.Activation.Genesis[0].RootHash[0] = 0xee
	reloaded, err := client.Session(ctx, session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.Activation.Genesis[0].RootHash[0] != wantGenesisRoot {
		t.Fatal("remote session result shares cache-owned slices")
	}

	signature := []byte{8, 9}
	if err = client.CommitDelegation(ctx, WindowRequest{
		SessionID: session.ID,
		PleaseCollate: simplex.ConsensusPleaseCollate{
			Signature: signature,
		},
	}); err != nil {
		t.Fatal(err)
	}
	signature[0] = 0xff
	if transport.commit.Signature[0] != 8 {
		t.Fatal("remote commit transport retained caller-owned signature")
	}
}

type remoteCollatorConcurrentTransport struct {
	*remoteCollatorTransportTest
	startCall func(context.Context) error
	probeCall func(context.Context, AuthenticatedQuery, simplex.ConsensusPleaseCollatePrepare) error
	closeCall func(context.Context) error
}

func (t *remoteCollatorConcurrentTransport) Start(ctx context.Context) error {
	if t.startCall != nil {
		return t.startCall(ctx)
	}

	return t.remoteCollatorTransportTest.Start(ctx)
}

func (t *remoteCollatorConcurrentTransport) Probe(
	ctx context.Context,
	query AuthenticatedQuery,
	request simplex.ConsensusPleaseCollatePrepare,
) error {
	if t.probeCall != nil {
		return t.probeCall(ctx, query, request)
	}

	return t.remoteCollatorTransportTest.Probe(ctx, query, request)
}

func (t *remoteCollatorConcurrentTransport) Close(ctx context.Context) error {
	if t.closeCall != nil {
		return t.closeCall(ctx)
	}

	return t.remoteCollatorTransportTest.Close(ctx)
}

func TestRemoteCollatorStartLifecycleLockHonorsContext(t *testing.T) {
	startEntered := make(chan struct{})
	releaseStart := make(chan struct{})
	var startOnce sync.Once
	transport := &remoteCollatorConcurrentTransport{
		remoteCollatorTransportTest: &remoteCollatorTransportTest{id: [32]byte{1}},
		startCall: func(ctx context.Context) error {
			startOnce.Do(func() { close(startEntered) })

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-releaseStart:
				return nil
			}
		},
	}
	client, err := NewRemoteCollator(transport)
	if err != nil {
		t.Fatal(err)
	}

	firstStart := make(chan error, 1)
	go func() { firstStart <- client.Start(context.Background()) }()
	select {
	case <-startEntered:
	case <-time.After(time.Second):
		t.Fatal("first remote start did not enter transport")
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	secondStart := make(chan error, 1)
	go func() { secondStart <- client.Start(waitCtx) }()

	var secondErr error
	lockBlockedPastDeadline := false
	select {
	case secondErr = <-secondStart:
	case <-time.After(time.Second):
		lockBlockedPastDeadline = true
	}

	close(releaseStart)
	select {
	case err = <-firstStart:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("first remote start did not finish")
	}
	if lockBlockedPastDeadline {
		select {
		case <-secondStart:
		case <-time.After(time.Second):
			t.Fatal("second remote start remained blocked after lifecycle release")
		}

		t.Fatal("second remote start ignored its context while waiting for lifecycle lock")
	}
	if !errors.Is(secondErr, context.DeadlineExceeded) {
		t.Fatalf("second remote start = %v, want deadline", secondErr)
	}
	if err = client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRemoteCollatorCloseDrainsAdmittedOperations(t *testing.T) {
	probeStarted := make(chan struct{})
	releaseProbe := make(chan struct{})
	closeStarted := make(chan struct{})
	var probeOnce sync.Once
	var closeOnce sync.Once
	transport := &remoteCollatorConcurrentTransport{
		remoteCollatorTransportTest: &remoteCollatorTransportTest{id: [32]byte{1}},
		probeCall: func(
			ctx context.Context,
			_ AuthenticatedQuery,
			_ simplex.ConsensusPleaseCollatePrepare,
		) error {
			probeOnce.Do(func() { close(probeStarted) })
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-releaseProbe:
				return nil
			}
		},
		closeCall: func(context.Context) error {
			closeOnce.Do(func() { close(closeStarted) })

			return nil
		},
	}
	client, err := NewRemoteCollator(transport)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err = client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	sessionID := [32]byte{2}
	session, update := remoteCollatorTestSession(sessionID)
	if err = client.PrepareSession(ctx, session, update); err != nil {
		t.Fatal(err)
	}

	probeResult := make(chan error, 1)
	go func() {
		probeResult <- client.Probe(ctx, WindowPreparation{SessionID: sessionID})
	}()
	select {
	case <-probeStarted:
	case <-time.After(time.Second):
		t.Fatal("remote probe did not start")
	}

	closeResult := make(chan error, 1)
	go func() { closeResult <- client.Close(ctx) }()
	deadline := time.Now().Add(time.Second)
	for {
		status, statusErr := client.Status(ctx)
		if statusErr != nil {
			t.Fatal(statusErr)
		}
		if status.Closing {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("remote collator did not enter closing state")
		}
		time.Sleep(time.Millisecond)
	}
	if err = client.Probe(ctx, WindowPreparation{SessionID: sessionID}); !errors.Is(err, ErrClosed) {
		t.Fatalf("probe admitted during close = %v, want ErrClosed", err)
	}
	select {
	case <-closeStarted:
		t.Fatal("transport closed before the admitted update drained")
	default:
	}

	close(releaseProbe)
	if err = <-probeResult; err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-closeResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("remote close did not finish after update drain")
	}
	select {
	case <-closeStarted:
	default:
		t.Fatal("transport close was not called")
	}
}

func TestRemoteCollatorLifecycleLockHonorsContext(t *testing.T) {
	closeStarted := make(chan struct{})
	releaseClose := make(chan struct{})
	var closeOnce sync.Once
	transport := &remoteCollatorConcurrentTransport{
		remoteCollatorTransportTest: &remoteCollatorTransportTest{id: [32]byte{1}},
		closeCall: func(ctx context.Context) error {
			closeOnce.Do(func() { close(closeStarted) })

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-releaseClose:
				return nil
			}
		},
	}
	client, err := NewRemoteCollator(transport)
	if err != nil {
		t.Fatal(err)
	}
	if err = client.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	firstClose := make(chan error, 1)
	go func() { firstClose <- client.Close(context.Background()) }()
	select {
	case <-closeStarted:
	case <-time.After(time.Second):
		t.Fatal("first remote close did not enter transport")
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	secondClose := make(chan error, 1)
	go func() { secondClose <- client.Close(waitCtx) }()

	var secondErr error
	lockBlockedPastDeadline := false
	select {
	case secondErr = <-secondClose:
	case <-time.After(time.Second):
		lockBlockedPastDeadline = true
	}

	close(releaseClose)
	select {
	case err = <-firstClose:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("first remote close did not finish")
	}
	if lockBlockedPastDeadline {
		select {
		case <-secondClose:
		case <-time.After(time.Second):
			t.Fatal("second remote close remained blocked after lifecycle release")
		}

		t.Fatal("second remote close ignored its context while waiting for lifecycle lock")
	}
	if !errors.Is(secondErr, context.DeadlineExceeded) {
		t.Fatalf("second remote close = %v, want deadline", secondErr)
	}
}

func TestRemoteCollatorSerializesOneSessionWithoutBlockingOthers(t *testing.T) {
	entered := make(chan [32]byte, 4)
	releaseA := make(chan struct{})
	releaseB := make(chan struct{})
	sessionA := [32]byte{2}
	sessionB := [32]byte{3}
	transport := &remoteCollatorConcurrentTransport{
		remoteCollatorTransportTest: &remoteCollatorTransportTest{id: [32]byte{1}},
		probeCall: func(
			ctx context.Context,
			query AuthenticatedQuery,
			_ simplex.ConsensusPleaseCollatePrepare,
		) error {
			entered <- query.SessionID
			release := releaseA
			if query.SessionID == sessionB {
				release = releaseB
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-release:
				return nil
			}
		},
	}
	client, err := NewRemoteCollator(transport)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err = client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	for _, sessionID := range [][32]byte{sessionA, sessionB} {
		session, update := remoteCollatorTestSession(sessionID)
		if err = client.PrepareSession(
			ctx,
			session,
			update,
		); err != nil {
			t.Fatal(err)
		}
	}

	firstA := make(chan error, 1)
	go func() { firstA <- client.Probe(ctx, WindowPreparation{SessionID: sessionA}) }()
	select {
	case got := <-entered:
		if got != sessionA {
			t.Fatalf("first entered session = %x, want %x", got, sessionA)
		}
	case <-time.After(time.Second):
		t.Fatal("first session A update did not enter transport")
	}

	waitCtx, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
	err = client.Probe(waitCtx, WindowPreparation{SessionID: sessionA})
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("concurrent same-session update = %v, want deadline", err)
	}
	select {
	case got := <-entered:
		t.Fatalf("second same-session update entered transport for %x", got)
	default:
	}

	resultB := make(chan error, 1)
	go func() { resultB <- client.Probe(ctx, WindowPreparation{SessionID: sessionB}) }()
	select {
	case got := <-entered:
		if got != sessionB {
			t.Fatalf("parallel entered session = %x, want %x", got, sessionB)
		}
	case <-time.After(time.Second):
		t.Fatal("independent session update was blocked")
	}

	close(releaseB)
	if err = <-resultB; err != nil {
		t.Fatal(err)
	}
	close(releaseA)
	if err = <-firstA; err != nil {
		t.Fatal(err)
	}
	if err = client.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
