package collator

import (
	"context"
	"sync"
	"testing"
	"time"
)

type runtimeCandidateSessionAdmissionStorage struct {
	*runtimeMemoryStorage
	entered chan struct{}
	release chan struct{}
	markers chan CandidateRecord
	once    sync.Once
}

func (s *runtimeCandidateSessionAdmissionStorage) SaveSession(ctx context.Context, record SessionRecord, done func(error)) {
	if record.Update.CurrentWindowStart == 1 {
		s.once.Do(func() { close(s.entered) })
		<-s.release
	}

	s.runtimeMemoryStorage.SaveSession(ctx, record, done)
}

func (s *runtimeCandidateSessionAdmissionStorage) SaveCandidate(record CandidateRecord, done func(error)) {
	s.markers <- record
	session, err := s.runtimeMemoryStorage.Session(context.Background(), record.WindowID.SessionID)
	if err == nil && session.Update.CurrentWindowStart != record.WindowID.StartSlot {
		err = ErrCandidateConflict
	}
	if err != nil {
		done(err)

		return
	}

	s.runtimeMemoryStorage.SaveCandidate(record, done)
}

func TestRuntimeCandidateWaitsForSessionAdmissionWhileBuildRuns(t *testing.T) {
	base := newRuntimeMemoryStorage()
	storage := &runtimeCandidateSessionAdmissionStorage{
		runtimeMemoryStorage: base,
		entered:              make(chan struct{}),
		release:              make(chan struct{}),
		markers:              make(chan CandidateRecord, 2),
	}
	built := make(chan BuildRequest, 1)
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(_ context.Context, request BuildRequest) (*Candidate, error) {
		built <- request

		return runtimeBuiltCandidate(request), nil
	}
	emitted := make(chan CandidateArtifact, 1)
	fixture := newRuntimeFixture(t, 1, 1, pipeline, base, func(_ context.Context, artifact CandidateArtifact) error {
		emitted <- artifact

		return nil
	})
	fixture.service.opts.Storage = storage
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(storage.release) }) }
	defer func() {
		release()
		fixture.close(t)
	}()

	session, initial := fixture.session(201, 1, 0, time.Now())
	fixture.prepare(t, session, initial)
	observed := initial
	observed.CurrentWindowStart = 1
	observed.CurrentWindowObservedSlot = 1
	observed.CurrentWindowStartAt = time.Now()
	if err := fixture.service.ApplyConsensusProgress(context.Background(), runtimeConsensusProgress(session, observed)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-storage.entered:
	case <-time.After(time.Second):
		t.Fatal("session write did not reach admission")
	}
	if err := fixture.service.CommitDelegation(context.Background(), fixture.request(t, session, 1)); err != nil {
		t.Fatal(err)
	}
	if request := runtimeAwaitBuild(t, built); request.Slot != 1 {
		t.Fatalf("built slot %d, want 1", request.Slot)
	}
	select {
	case record := <-storage.markers:
		t.Fatalf("candidate slot %d overtook its session admission", record.ID.Slot)
	case <-time.After(50 * time.Millisecond):
	}

	release()
	if artifact := runtimeAwaitArtifact(t, emitted); artifact.Candidate.ID.Slot != 1 {
		t.Fatalf("emitted slot %d, want 1", artifact.Candidate.ID.Slot)
	}
}
