package collator

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPreparedDelegationSurvivesAdmissionPolicyChanges(t *testing.T) {
	fixture := newRuntimeFixture(t, 1, 1, nil, nil, nil)
	defer fixture.close(t)
	session, update := fixture.session(85, 2, 0, time.Time{})
	update.HasCurrentWindow = false
	fixture.prepare(t, session, update)
	lag := 10 * time.Second
	fixture.service.opts.SyncLag = func() (time.Duration, error) { return lag, nil }
	probe := WindowPreparation{SessionID: session.ID, SourceADNL: fixture.sourceADNL, StartSlot: 2}
	if err := fixture.service.Probe(context.Background(), probe); err != nil {
		t.Fatal(err)
	}

	lag = time.Hour
	delete(fixture.service.allowed, fixture.sourceADNL)
	update.CollatorMaxFutureWindow = 0
	if err := fixture.service.UpdateSession(context.Background(), update); err != nil {
		t.Fatal(err)
	}
	if err := fixture.service.Probe(context.Background(), probe); err != nil {
		t.Fatalf("repeat prepare: %v", err)
	}
	if err := fixture.service.CommitDelegation(context.Background(), fixture.request(t, session, 2)); err != nil {
		t.Fatalf("commit after prepare: %v", err)
	}
	probe.StartSlot = 4
	if err := fixture.service.Probe(context.Background(), probe); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("unprepared window bypassed whitelist: %v", err)
	}

	fixture.service.allowed[fixture.sourceADNL] = struct{}{}
	if err := fixture.service.Probe(context.Background(), probe); !errors.Is(err, ErrWindowTooFar) {
		t.Fatalf("zero future limit ignored: %v", err)
	}
	update.CollatorMaxFutureWindow = 4
	if err := fixture.service.UpdateSession(context.Background(), update); err != nil {
		t.Fatal(err)
	}
	if err := fixture.service.Probe(context.Background(), probe); !errors.Is(err, ErrAcquisitionNotReady) {
		t.Fatalf("new window ignored sync lag: %v", err)
	}
	lag = 0
	update.CollatorMaxSyncDelay = 0
	if err := fixture.service.UpdateSession(context.Background(), update); err != nil {
		t.Fatal(err)
	}
	if err := fixture.service.Probe(context.Background(), probe); err != nil {
		t.Fatalf("zero sync delay boundary: %v", err)
	}
}

func TestDelegationSyncLagErrorDoesNotPrepareWindow(t *testing.T) {
	fixture := newRuntimeFixture(t, 1, 1, nil, nil, nil)
	defer fixture.close(t)
	session, update := fixture.session(86, 1, 0, time.Time{})
	update.HasCurrentWindow = false
	fixture.prepare(t, session, update)
	fixture.service.opts.SyncLag = func() (time.Duration, error) { return 0, errors.New("no applied state") }
	probe := WindowPreparation{SessionID: session.ID, SourceADNL: fixture.sourceADNL, StartSlot: 1}
	if err := fixture.service.Probe(context.Background(), probe); !errors.Is(err, ErrAcquisitionNotReady) {
		t.Fatal(err)
	}
	fixture.service.opts.SyncLag = func() (time.Duration, error) { return time.Hour, nil }
	if err := fixture.service.Probe(context.Background(), probe); !errors.Is(err, ErrAcquisitionNotReady) {
		t.Fatalf("failed prepare was cached: %v", err)
	}
}
