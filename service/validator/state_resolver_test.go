package validator

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
	"github.com/xssnick/tonutils-go/ton"
)

type resolverEventLog struct {
	mu     sync.Mutex
	events []string
}

func TestStateResolverAcceptanceRetryDelayAndFlag(t *testing.T) {
	backend := newRuntimeTestBackend()
	var mu sync.Mutex
	var calls []struct {
		at    time.Time
		retry bool
	}
	backend.acceptance = func(_ context.Context, acceptance BlockAcceptance) error {
		mu.Lock()
		calls = append(calls, struct {
			at    time.Time
			retry bool
		}{at: time.Now(), retry: acceptance.Retry})
		count := len(calls)
		mu.Unlock()
		if count == 1 {
			return ErrBlockNotReady
		}

		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resolver := &stateResolver{backend: backend, ctx: ctx}
	if err := resolver.acceptBlock(BlockAcceptance{}); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 2 {
		t.Fatalf("accept calls = %d, want 2", len(calls))
	}
	if calls[0].retry || !calls[1].retry {
		t.Fatalf("retry flags = [%v %v], want [false true]", calls[0].retry, calls[1].retry)
	}
	if delay := calls[1].at.Sub(calls[0].at); delay < 950*time.Millisecond {
		t.Fatalf("accept retry delay = %v, want approximately one second", delay)
	}
}

func TestStateResolverAcceptanceRetryCancels(t *testing.T) {
	backend := newRuntimeTestBackend()
	backend.acceptance = func(context.Context, BlockAcceptance) error { return ErrBlockNotReady }
	ctx, cancel := context.WithCancel(context.Background())
	resolver := &stateResolver{backend: backend, ctx: ctx}
	result := make(chan error, 1)
	go func() { result <- resolver.acceptBlock(BlockAcceptance{}) }()
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, ErrResolverClosed) {
			t.Fatalf("accept cancellation error = %v, want ErrResolverClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("accept retry did not stop on cancellation")
	}
}

func TestStateResolverReplaysPersistedFinalizationOnce(t *testing.T) {
	storage := newRuntimeTestStorage()
	config, privateKey := runtimeTestConfig(0x92, &runtimeTestJournal{})
	artifact := runtimeOrdinaryArtifact(t, config, privateKey, 1, simplex.Genesis())
	candidates := newResolverForTest(
		storage,
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		simplex.DefaultParams(),
	)
	if err := candidates.stage(artifact, []byte{0x01}); err != nil {
		t.Fatal(err)
	}
	candidates.observeNotarization(
		artifact.Candidate.ID,
		&simplex.Certificate{Vote: simplex.NotarizeVote(artifact.Candidate.ID)},
	)
	defer candidates.close()

	backend := newRuntimeTestBackend()
	var acceptances []BlockAcceptance
	backend.acceptance = func(_ context.Context, acceptance BlockAcceptance) error {
		acceptances = append(acceptances, acceptance)

		return nil
	}
	resolver := newStateResolver(
		config.Shard,
		config.StorageID,
		storage,
		backend,
		candidates,
		StoredSessionState{Finalized: []simplex.CandidateID{artifact.Candidate.ID}},
		nil,
	)
	defer resolver.close()
	if err := resolver.start(context.Background(), runtimeTestStart()); err != nil {
		t.Fatal(err)
	}

	certificate := &simplex.Certificate{Vote: simplex.FinalizeVote(artifact.Candidate.ID)}
	if err := resolver.finalize(context.Background(), artifact.Candidate.ID, certificate); err != nil {
		t.Fatal(err)
	}
	if len(acceptances) != 1 || !acceptances[0].Replay || acceptances[0].Retry {
		t.Fatalf("replayed acceptances = %+v, want one replay without retry", acceptances)
	}
	if err := resolver.finalize(context.Background(), artifact.Candidate.ID, certificate); err != nil {
		t.Fatal(err)
	}
	if len(acceptances) != 1 {
		t.Fatalf("acceptance was replayed %d times, want exactly once", len(acceptances))
	}
}

func TestStateResolverRejectsUnavailableGenesis(t *testing.T) {
	resolver := &stateResolver{}
	err := resolver.start(context.Background(), SessionStart{})
	if err == nil || err.Error() != "validator runtime: session genesis is unavailable" {
		t.Fatalf("start error = %v", err)
	}
}

func TestStateResolverReplaysMasterchainFinalizationsBeforeStart(t *testing.T) {
	storage := newRuntimeTestStorage()
	candidates := newResolverForTest(
		storage,
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		simplex.DefaultParams(),
	)
	defer candidates.close()

	shard := groups.ShardID{Workchain: -1, Shard: -1 << 63}
	firstID := simplex.CandidateID{Slot: 3, Hash: [32]byte{0xa1}}
	secondID := simplex.CandidateID{Slot: 7, Hash: [32]byte{0xb2}}
	first := &CandidateArtifact{Candidate: simplex.Candidate{
		ID:     firstID,
		Parent: simplex.Genesis(),
		Block: ton.BlockIDExt{
			Workchain: -1,
			Shard:     -1 << 63,
			SeqNo:     1,
			RootHash:  bytes.Repeat([]byte{0x31}, 32),
			FileHash:  bytes.Repeat([]byte{0x32}, 32),
		},
	}}
	second := &CandidateArtifact{Candidate: simplex.Candidate{
		ID:     secondID,
		Parent: simplex.Parent(firstID),
		Block: ton.BlockIDExt{
			Workchain: -1,
			Shard:     -1 << 63,
			SeqNo:     2,
			RootHash:  bytes.Repeat([]byte{0x41}, 32),
			FileHash:  bytes.Repeat([]byte{0x42}, 32),
		},
	}}
	for i, artifact := range []*CandidateArtifact{first, second} {
		if err := candidates.stage(artifact, []byte{byte(i + 1)}); err != nil {
			t.Fatal(err)
		}
		candidates.observeNotarization(
			artifact.Candidate.ID,
			&simplex.Certificate{Vote: simplex.NotarizeVote(artifact.Candidate.ID)},
		)
	}

	var accepted []simplex.CandidateID
	var sawNonReplay bool
	backend := newRuntimeTestBackend()
	backend.acceptance = func(_ context.Context, acceptance BlockAcceptance) error {
		if !acceptance.Replay {
			sawNonReplay = true
		}
		accepted = append(accepted, acceptance.Candidate.Candidate.ID)

		return nil
	}
	recovery := []*simplex.Certificate{
		{Vote: simplex.FinalizeVote(firstID)},
		{Vote: simplex.FinalizeVote(secondID)},
	}
	resolver := newStateResolver(
		shard,
		SessionStorageID{},
		storage,
		backend,
		candidates,
		StoredSessionState{},
		recovery,
	)
	defer resolver.close()

	start := runtimeTestStart()
	start.Genesis[0].Workchain = -1
	start.Genesis[0].Shard = -1 << 63
	if err := resolver.start(context.Background(), start); err != nil {
		t.Fatal(err)
	}
	if err := resolver.start(context.Background(), start); err != nil {
		t.Fatalf("idempotent recovery start: %v", err)
	}
	if len(accepted) != 2 || accepted[0] != firstID || accepted[1] != secondID {
		t.Fatalf("replayed finalizations = %v, want [%s %s]", accepted, firstID, secondID)
	}
	if sawNonReplay {
		t.Fatal("bootstrap finalization was not marked as replay")
	}
	if err := resolver.finalize(context.Background(), secondID, recovery[1]); err != nil {
		t.Fatal(err)
	}
	if len(accepted) != 2 {
		t.Fatalf("Simplex replay duplicated %d accepted blocks", len(accepted)-2)
	}
}

func TestStateResolverSkipsAppliedPersistedRecoveryPrefix(t *testing.T) {
	storage := newRuntimeTestStorage()
	candidates := newResolverForTest(
		storage,
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		simplex.DefaultParams(),
	)
	defer candidates.close()

	shard := groups.ShardID{Workchain: -1, Shard: -1 << 63}
	firstID := simplex.CandidateID{Slot: 3, Hash: [32]byte{0xa1}}
	secondID := simplex.CandidateID{Slot: 7, Hash: [32]byte{0xb2}}
	first := &CandidateArtifact{Candidate: simplex.Candidate{
		ID:     firstID,
		Parent: simplex.Genesis(),
		Block: ton.BlockIDExt{
			Workchain: -1,
			Shard:     -1 << 63,
			SeqNo:     1,
			RootHash:  bytes.Repeat([]byte{0x31}, 32),
			FileHash:  bytes.Repeat([]byte{0x32}, 32),
		},
	}}
	second := &CandidateArtifact{Candidate: simplex.Candidate{
		ID:     secondID,
		Parent: simplex.Parent(firstID),
		Block: ton.BlockIDExt{
			Workchain: -1,
			Shard:     -1 << 63,
			SeqNo:     2,
			RootHash:  bytes.Repeat([]byte{0x41}, 32),
			FileHash:  bytes.Repeat([]byte{0x42}, 32),
		},
	}}
	for i, artifact := range []*CandidateArtifact{first, second} {
		if err := candidates.stage(artifact, []byte{byte(i + 1)}); err != nil {
			t.Fatal(err)
		}
		candidates.observeNotarization(
			artifact.Candidate.ID,
			&simplex.Certificate{Vote: simplex.NotarizeVote(artifact.Candidate.ID)},
		)
	}

	backend := newRuntimeTestBackend()
	acceptances := 0
	backend.acceptance = func(context.Context, BlockAcceptance) error {
		acceptances++
		return nil
	}
	recovery := []*simplex.Certificate{
		{Vote: simplex.FinalizeVote(firstID)},
		{Vote: simplex.FinalizeVote(secondID)},
	}
	resolver := newStateResolver(
		shard,
		SessionStorageID{},
		storage,
		backend,
		candidates,
		StoredSessionState{Finalized: []simplex.CandidateID{firstID, secondID}},
		recovery,
	)
	defer resolver.close()

	start := runtimeTestStart()
	start.Genesis[0].Workchain = -1
	start.Genesis[0].Shard = -1 << 63
	if err := resolver.start(context.Background(), start); err != nil {
		t.Fatal(err)
	}
	if acceptances != 0 {
		t.Fatalf("already applied recovery acceptances = %d, want 0", acceptances)
	}
	if err := resolver.finalize(context.Background(), secondID, recovery[1]); err != nil {
		t.Fatal(err)
	}
	if acceptances != 0 {
		t.Fatalf("Simplex replay duplicated applied block acceptance")
	}
}

func TestStateResolverReplaysOnlyRecoveryCrashGap(t *testing.T) {
	storage := newRuntimeTestStorage()
	candidates := newResolverForTest(
		storage,
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		simplex.DefaultParams(),
	)
	defer candidates.close()

	shard := groups.ShardID{Workchain: -1, Shard: -1 << 63}
	ids := []simplex.CandidateID{
		{Slot: 3, Hash: [32]byte{0xa1}},
		{Slot: 7, Hash: [32]byte{0xb2}},
		{Slot: 11, Hash: [32]byte{0xc3}},
	}
	parent := simplex.Genesis()
	for i, id := range ids {
		artifact := &CandidateArtifact{Candidate: simplex.Candidate{
			ID:     id,
			Parent: parent,
			Block: ton.BlockIDExt{
				Workchain: -1,
				Shard:     -1 << 63,
				SeqNo:     uint32(i + 1),
				RootHash:  bytes.Repeat([]byte{byte(0x31 + i)}, 32),
				FileHash:  bytes.Repeat([]byte{byte(0x41 + i)}, 32),
			},
		}}
		if err := candidates.stage(artifact, []byte{byte(i + 1)}); err != nil {
			t.Fatal(err)
		}
		candidates.observeNotarization(
			id,
			&simplex.Certificate{Vote: simplex.NotarizeVote(id)},
		)
		parent = simplex.Parent(id)
	}

	backend := newRuntimeTestBackend()
	thirdReady := false
	backend.load = func(_ context.Context, request ChainStateRequest) (ChainStateData, error) {
		block := request.Blocks[0]
		if block.SeqNo == 3 && !thirdReady {
			return ChainStateData{}, ErrBlockNotReady
		}
		tip := ChainTip{ID: block, State: backend.stateRoot}
		if block.SeqNo != 0 {
			tip.BlockBOC = []byte{0x01}
		}
		return ChainStateData{Tips: []ChainTip{tip}}, nil
	}
	var accepted []simplex.CandidateID
	backend.acceptance = func(_ context.Context, acceptance BlockAcceptance) error {
		accepted = append(accepted, acceptance.Candidate.Candidate.ID)
		if acceptance.Candidate.Candidate.ID == ids[2] {
			thirdReady = true
		}
		return nil
	}
	recovery := make([]*simplex.Certificate, len(ids))
	for i, id := range ids {
		recovery[i] = &simplex.Certificate{Vote: simplex.FinalizeVote(id)}
	}
	resolver := newStateResolver(
		shard,
		SessionStorageID{},
		storage,
		backend,
		candidates,
		StoredSessionState{Finalized: ids},
		recovery,
	)
	defer resolver.close()

	start := runtimeTestStart()
	start.Genesis[0].Workchain = -1
	start.Genesis[0].Shard = -1 << 63
	if err := resolver.start(context.Background(), start); err != nil {
		t.Fatal(err)
	}
	if len(accepted) != 1 || accepted[0] != ids[2] {
		t.Fatalf("replayed crash gap = %v, want [%s]", accepted, ids[2])
	}
}

func TestStateResolverWaitsForEachMasterchainReplayBeforeSubmittingNext(t *testing.T) {
	storage := newRuntimeTestStorage()
	candidates := newResolverForTest(
		storage,
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		simplex.DefaultParams(),
	)
	defer candidates.close()

	shard := groups.ShardID{Workchain: -1, Shard: -1 << 63}
	firstID := simplex.CandidateID{Slot: 3, Hash: [32]byte{0xa1}}
	secondID := simplex.CandidateID{Slot: 7, Hash: [32]byte{0xb2}}
	first := &CandidateArtifact{Candidate: simplex.Candidate{
		ID:     firstID,
		Parent: simplex.Genesis(),
		Block: ton.BlockIDExt{
			Workchain: -1,
			Shard:     -1 << 63,
			SeqNo:     1,
			RootHash:  bytes.Repeat([]byte{0x31}, 32),
			FileHash:  bytes.Repeat([]byte{0x32}, 32),
		},
	}}
	second := &CandidateArtifact{Candidate: simplex.Candidate{
		ID:     secondID,
		Parent: simplex.Parent(firstID),
		Block: ton.BlockIDExt{
			Workchain: -1,
			Shard:     -1 << 63,
			SeqNo:     2,
			RootHash:  bytes.Repeat([]byte{0x41}, 32),
			FileHash:  bytes.Repeat([]byte{0x42}, 32),
		},
	}}
	for i, artifact := range []*CandidateArtifact{first, second} {
		if err := candidates.stage(artifact, []byte{byte(i + 1)}); err != nil {
			t.Fatal(err)
		}
		candidates.observeNotarization(
			artifact.Candidate.ID,
			&simplex.Certificate{Vote: simplex.NotarizeVote(artifact.Candidate.ID)},
		)
	}

	firstSubmitted := make(chan struct{})
	secondSubmitted := make(chan struct{})
	firstApplied := make(chan struct{})
	backend := newRuntimeTestBackend()
	backend.acceptance = func(_ context.Context, acceptance BlockAcceptance) error {
		switch acceptance.Candidate.Candidate.ID {
		case firstID:
			close(firstSubmitted)
		case secondID:
			close(secondSubmitted)
		}

		return nil
	}
	backend.load = func(ctx context.Context, request ChainStateRequest) (ChainStateData, error) {
		block := request.Blocks[0]
		if block.SeqNo == 1 {
			select {
			case <-firstApplied:
			default:
				return ChainStateData{}, ErrBlockNotReady
			}
		}

		return ChainStateData{Tips: []ChainTip{{
			ID:       block,
			State:    backend.stateRoot,
			BlockBOC: []byte{0x01},
		}}}, nil
	}
	recovery := []*simplex.Certificate{
		{Vote: simplex.FinalizeVote(firstID)},
		{Vote: simplex.FinalizeVote(secondID)},
	}
	resolver := newStateResolver(
		shard,
		SessionStorageID{},
		storage,
		backend,
		candidates,
		StoredSessionState{},
		recovery,
	)
	defer resolver.close()

	start := runtimeTestStart()
	start.Genesis[0].Workchain = -1
	start.Genesis[0].Shard = -1 << 63
	result := make(chan error, 1)
	go func() { result <- resolver.start(context.Background(), start) }()

	select {
	case <-firstSubmitted:
	case <-time.After(time.Second):
		t.Fatal("first recovery block was not submitted")
	}
	select {
	case <-secondSubmitted:
		t.Fatal("second recovery block was submitted before the first became readable")
	case <-time.After(100 * time.Millisecond):
	}
	close(firstApplied)

	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("recovery did not continue after the first block became readable")
	}
	select {
	case <-secondSubmitted:
	default:
		t.Fatal("second recovery block was not submitted")
	}
}

func TestStateResolverWaitsForFinalizedBlockApply(t *testing.T) {
	storage := newRuntimeTestStorage()
	config, privateKey := runtimeTestConfig(0x91, &runtimeTestJournal{})
	artifact := runtimeOrdinaryArtifact(t, config, privateKey, 1, simplex.Genesis())
	candidates := newResolverForTest(
		storage,
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		simplex.DefaultParams(),
	)
	if err := candidates.stage(artifact, []byte{0x01}); err != nil {
		t.Fatal(err)
	}
	candidates.observeNotarization(
		artifact.Candidate.ID,
		&simplex.Certificate{Vote: simplex.NotarizeVote(artifact.Candidate.ID)},
	)
	defer candidates.close()

	backend := newRuntimeTestBackend()
	var loads int
	backend.load = func(_ context.Context, request ChainStateRequest) (ChainStateData, error) {
		if request.Blocks[0].SeqNo == 0 {
			return ChainStateData{Tips: []ChainTip{{
				ID:    request.Blocks[0],
				State: backend.stateRoot,
			}}}, nil
		}
		loads++
		if loads == 1 {
			return ChainStateData{}, ErrBlockNotReady
		}
		return ChainStateData{Tips: []ChainTip{{
			ID:       request.Blocks[0],
			State:    backend.stateRoot,
			BlockBOC: []byte{0x01},
		}}}, nil
	}

	resolver := newStateResolver(
		config.Shard,
		config.StorageID,
		storage,
		backend,
		candidates,
		StoredSessionState{Finalized: []simplex.CandidateID{artifact.Candidate.ID}},
		nil,
	)
	defer resolver.close()
	if err := resolver.start(context.Background(), runtimeTestStart()); err != nil {
		t.Fatal(err)
	}

	started := time.Now()
	resolved, err := resolver.resolve(context.Background(), simplex.Parent(artifact.Candidate.ID))
	if err != nil {
		t.Fatal(err)
	}
	block, err := resolved.State.NormalBlock()
	if err != nil {
		t.Fatal(err)
	}
	if !sameBlockID(block, artifact.Candidate.Block) {
		t.Fatalf("resolved block = %+v, want %+v", block, artifact.Candidate.Block)
	}
	if loads != 2 {
		t.Fatalf("finalized block loads = %d, want 2", loads)
	}
	if delay := time.Since(started); delay < 950*time.Millisecond {
		t.Fatalf("finalized-state retry delay = %v, want approximately one second", delay)
	}
}

func (l *resolverEventLog) add(event string) {
	l.mu.Lock()
	l.events = append(l.events, event)
	l.mu.Unlock()
}

func (l *resolverEventLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]string(nil), l.events...)
}

type orderedFinalizeStorage struct {
	*runtimeTestStorage
	events *resolverEventLog
}

func (s *orderedFinalizeStorage) MarkFinalized(
	_ SessionStorageID,
	id simplex.CandidateID,
	done func(error),
) {
	s.events.add("mark:" + id.String())
	done(nil)
}

func TestStateResolverPropagatesFinalCertificateAcrossEmptyCandidate(t *testing.T) {
	events := &resolverEventLog{}
	storage := &orderedFinalizeStorage{runtimeTestStorage: newRuntimeTestStorage(), events: events}
	backend := newRuntimeTestBackend()
	var accepted BlockAcceptance
	backend.acceptance = func(_ context.Context, acceptance BlockAcceptance) error {
		accepted = acceptance
		events.add("accept:" + acceptance.Candidate.Candidate.ID.String())

		return nil
	}
	params := simplex.DefaultParams()
	candidates := newResolverForTest(storage, &retryCandidateProvider{called: make(chan struct{}, 1)}, 1, params)

	ordinaryID := simplex.CandidateID{Slot: 0, Hash: [32]byte{0xa1}}
	ordinary := &CandidateArtifact{Candidate: simplex.Candidate{
		ID:     ordinaryID,
		Parent: simplex.Genesis(),
	}}
	emptyID := simplex.CandidateID{Slot: 1, Hash: [32]byte{0xb2}}
	empty := &CandidateArtifact{Candidate: simplex.Candidate{
		ID:     emptyID,
		Parent: simplex.Parent(ordinaryID),
		Empty:  true,
	}}
	if err := candidates.stage(ordinary, []byte{0x01}); err != nil {
		t.Fatal(err)
	}
	if err := candidates.stage(empty, []byte{0x02}); err != nil {
		t.Fatal(err)
	}
	candidates.observeNotarization(ordinaryID, &simplex.Certificate{Vote: simplex.NotarizeVote(ordinaryID)})
	candidates.observeNotarization(emptyID, &simplex.Certificate{Vote: simplex.NotarizeVote(emptyID)})

	resolver := newStateResolver(
		groups.ShardID{Workchain: 0, Shard: -1 << 63},
		SessionStorageID{},
		storage,
		backend,
		candidates,
		StoredSessionState{},
		nil,
	)
	if err := resolver.start(context.Background(), runtimeTestStart()); err != nil {
		t.Fatal(err)
	}
	finalCertificate := &simplex.Certificate{Vote: simplex.FinalizeVote(emptyID)}
	if err := resolver.finalize(context.Background(), emptyID, finalCertificate); err != nil {
		t.Fatal(err)
	}
	resolver.close()
	candidates.close()

	if accepted.Candidate != ordinary {
		t.Fatal("empty candidate, rather than its ordinary ancestor, was accepted")
	}
	if accepted.Certificate != finalCertificate || accepted.CertifiedCandidate != empty {
		t.Fatal("final certificate/certified candidate pair was not propagated to the ordinary ancestor")
	}
	want := []string{
		"accept:" + ordinaryID.String(),
		"mark:" + ordinaryID.String(),
		"mark:" + emptyID.String(),
	}
	got := events.snapshot()
	if len(got) != len(want) {
		t.Fatalf("finalization events = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("finalization events = %v, want %v", got, want)
		}
	}
}

func TestStateResolverLineageStopsAtNewestExactFinalizedBlock(t *testing.T) {
	t.Run("ordinary anchor", func(t *testing.T) {
		resolver, candidates, artifacts := lineageResolverForTest(t, false)
		defer resolver.close()
		defer candidates.close()

		anchor := artifacts[0].Candidate.Block
		got, err := resolver.lineage(
			context.Background(),
			simplex.Parent(artifacts[2].Candidate.ID),
			&anchor,
		)
		if err != nil {
			t.Fatal(err)
		}
		assertArtifactIdentity(t, got.Candidates, artifacts)
		if got.AppliedAnchor == nil || !sameBlockID(*got.AppliedAnchor, anchor) {
			t.Fatalf("applied anchor = %v, want %v", got.AppliedAnchor, anchor)
		}
	})

	t.Run("empty anchor", func(t *testing.T) {
		resolver, candidates, artifacts := lineageResolverForTest(t, true)
		defer resolver.close()
		defer candidates.close()

		anchor := artifacts[0].Candidate.Block
		got, err := resolver.lineage(
			context.Background(),
			simplex.Parent(artifacts[2].Candidate.ID),
			&anchor,
		)
		if err != nil {
			t.Fatal(err)
		}
		assertArtifactIdentity(t, got.Candidates, artifacts[1:])
		if got.AppliedAnchor == nil || !sameBlockID(*got.AppliedAnchor, anchor) {
			t.Fatalf("applied anchor = %v, want %v", got.AppliedAnchor, anchor)
		}
	})
}

func TestStateResolverLineageStartsAtNewestAppliedCandidate(t *testing.T) {
	resolver, candidates, artifacts := lineageResolverForTest(t, false)
	defer resolver.close()
	defer candidates.close()

	resolver.mu.Lock()
	resolver.finalized[artifacts[1].Candidate.ID] = &finalizedState{
		isDone:     true,
		reconciled: true,
		applied:    true,
	}
	resolver.mu.Unlock()

	masterchainAnchor := artifacts[0].Candidate.Block
	got, err := resolver.lineage(
		context.Background(),
		simplex.Parent(artifacts[2].Candidate.ID),
		&masterchainAnchor,
	)
	if err != nil {
		t.Fatal(err)
	}
	assertArtifactIdentity(t, got.Candidates, artifacts[1:])
	if got.AppliedAnchor == nil || !sameBlockID(*got.AppliedAnchor, artifacts[1].Candidate.Block) {
		t.Fatalf("applied anchor = %v, want newest finalized candidate %v", got.AppliedAnchor, artifacts[1].Candidate.Block)
	}
}

func TestStateResolverLineageWaitsForExactFinalizedState(t *testing.T) {
	resolver, candidates, artifacts := lineageResolverForTest(t, false)
	defer resolver.close()
	defer candidates.close()

	resolver.mu.Lock()
	resolver.finalized[artifacts[0].Candidate.ID] = &finalizedState{isDone: true, reconciled: true}
	resolver.mu.Unlock()
	ready := false
	calls := 0
	backend := newRuntimeTestBackend()
	backend.load = func(
		_ context.Context,
		request ChainStateRequest,
	) (ChainStateData, error) {
		calls++
		if !ready {
			return ChainStateData{}, ErrBlockNotReady
		}

		tips := make([]ChainTip, len(request.Blocks))
		for i := range request.Blocks {
			tips[i] = ChainTip{ID: request.Blocks[i], State: backend.stateRoot, BlockBOC: []byte{0x01}}
		}

		return ChainStateData{Tips: tips}, nil
	}
	resolver.backend = backend

	masterchainAnchor := artifacts[0].Candidate.Block
	if _, err := resolver.lineage(
		context.Background(),
		simplex.Parent(artifacts[2].Candidate.ID),
		&masterchainAnchor,
	); !errors.Is(err, ErrBlockNotReady) {
		t.Fatalf("unavailable finalized state error = %v, want %v", err, ErrBlockNotReady)
	}

	ready = true
	got, err := resolver.lineage(
		context.Background(),
		simplex.Parent(artifacts[2].Candidate.ID),
		&masterchainAnchor,
	)
	if err != nil {
		t.Fatal(err)
	}
	assertArtifactIdentity(t, got.Candidates, artifacts)
	if got.AppliedAnchor == nil || !sameBlockID(*got.AppliedAnchor, masterchainAnchor) {
		t.Fatalf("applied anchor = %v, want masterchain anchor %v", got.AppliedAnchor, masterchainAnchor)
	}
	if got.AppliedAnchorState == nil || len(got.AppliedAnchorState.tips) != 1 ||
		!sameBlockID(got.AppliedAnchorState.tips[0].ID, masterchainAnchor) ||
		got.AppliedAnchorState.tips[0].State != backend.stateRoot {
		t.Fatal("lineage did not retain the exact applied anchor state")
	}
	if _, err = resolver.lineage(
		context.Background(),
		simplex.Parent(artifacts[2].Candidate.ID),
		&masterchainAnchor,
	); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("cached applied anchor loaded %d times, want 2 total attempts", calls)
	}
}

func TestStateResolverLineageAllowsGenesisAnchorOnly(t *testing.T) {
	resolver, candidates, artifacts := lineageResolverForTest(t, false)
	defer resolver.close()
	defer candidates.close()

	genesis := runtimeTestStart().Genesis[0]
	got, err := resolver.lineage(
		context.Background(),
		simplex.Parent(artifacts[2].Candidate.ID),
		&genesis,
	)
	if err != nil {
		t.Fatal(err)
	}
	assertArtifactIdentity(t, got.Candidates, artifacts)
	if got.AppliedAnchor != nil {
		t.Fatalf("genesis lineage has candidate anchor %v", got.AppliedAnchor)
	}

	unrelated := ton.BlockIDExt{
		Workchain: genesis.Workchain,
		Shard:     genesis.Shard,
		SeqNo:     genesis.SeqNo + 10,
		RootHash:  bytes.Repeat([]byte{0xe1}, 32),
		FileHash:  bytes.Repeat([]byte{0xe2}, 32),
	}
	if _, err = resolver.lineage(
		context.Background(),
		simplex.Parent(artifacts[2].Candidate.ID),
		&unrelated,
	); !errors.Is(err, errFinalizedLineageAhead) {
		t.Fatalf("unrelated finalized anchor error = %v, want %v", err, errFinalizedLineageAhead)
	}
}

func TestStateResolverFinalizationReleasesOnlyCompletedStatesBelowWatermark(t *testing.T) {
	resolver, candidates, _ := lineageResolverForTest(t, false)
	defer resolver.close()
	defer candidates.close()

	parentAt := func(slot uint32, tag byte) simplex.ParentID {
		return simplex.Parent(simplex.CandidateID{Slot: slot, Hash: [32]byte{tag}})
	}
	completed := func() *stateFlight {
		done := make(chan struct{})
		close(done)

		return &stateFlight{
			done: done,
			result: ResolvedState{
				State: &ChainState{tips: []ChainTip{{BlockBOC: make([]byte, 128)}}},
			},
		}
	}
	// The session has been running long enough to hold one resolved parent per
	// slot, plus the genesis flight and one parent still being applied.
	inFlight := &stateFlight{done: make(chan struct{})}
	genesis := &stateFlight{done: make(chan struct{})}
	close(genesis.done)

	resolver.mu.Lock()
	resolver.states = map[simplex.ParentID]*stateFlight{
		simplex.Genesis():  genesis,
		parentAt(50, 0xff): inFlight,
	}
	for slot := uint32(0); slot < 100; slot++ {
		resolver.states[parentAt(slot, 0xc0)] = completed()
	}
	resolver.mu.Unlock()

	if stats := resolver.cacheStats(); stats.States != 102 || stats.Resolved != 101 ||
		stats.BlockBOCBytes != 100*128 {
		t.Fatalf("cache stats before finalization = %+v", stats)
	}

	resolver.notifyFinalized(100)

	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	// Genesis, the retained margin below the watermark, and the flight that is
	// still resolving. Dropping the latter would let the next resolve start a
	// second ApplyMerkleUpdate over the same parent.
	if len(resolver.states) != 2+stateCacheRetainedSlots {
		t.Fatalf("retained states = %d, want %d", len(resolver.states), 2+stateCacheRetainedSlots)
	}
	if resolver.states[simplex.Genesis()] != genesis {
		t.Fatal("genesis state was released")
	}
	if resolver.states[parentAt(50, 0xff)] != inFlight {
		t.Fatal("in-flight state below the watermark was released")
	}
	for slot := uint32(100 - stateCacheRetainedSlots); slot < 100; slot++ {
		if resolver.states[parentAt(slot, 0xc0)] == nil {
			t.Fatalf("state at slot %d was released inside the retained margin", slot)
		}
	}
	for slot := uint32(0); slot < 100-stateCacheRetainedSlots; slot++ {
		if resolver.states[parentAt(slot, 0xc0)] != nil {
			t.Fatalf("state at slot %d was retained below the watermark", slot)
		}
	}
}

func TestStateResolverFinalizationKeepsEarlySlotsWithinRetainedMargin(t *testing.T) {
	resolver, candidates, _ := lineageResolverForTest(t, false)
	defer resolver.close()
	defer candidates.close()

	id := simplex.Parent(simplex.CandidateID{Slot: 0, Hash: [32]byte{0xc0}})
	done := make(chan struct{})
	close(done)

	resolver.mu.Lock()
	resolver.states = map[simplex.ParentID]*stateFlight{id: {done: done}}
	resolver.mu.Unlock()

	// A session younger than the retained margin has no watermark to apply.
	resolver.notifyFinalized(stateCacheRetainedSlots - 1)

	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	if len(resolver.states) != 1 {
		t.Fatalf("retained states = %d, want the first slots kept", len(resolver.states))
	}
}

func lineageResolverForTest(
	t *testing.T,
	emptyAnchor bool,
) (*stateResolver, *candidateResolver, []*CandidateArtifact) {
	t.Helper()

	storage := newRuntimeTestStorage()
	candidates := newResolverForTest(
		storage,
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		simplex.DefaultParams(),
	)
	anchor := ton.BlockIDExt{
		Workchain: 0,
		Shard:     -1 << 63,
		SeqNo:     1,
		RootHash:  bytes.Repeat([]byte{0xa1}, 32),
		FileHash:  bytes.Repeat([]byte{0xa2}, 32),
	}
	blocks := []ton.BlockIDExt{
		anchor,
		anchor,
		{
			Workchain: 0,
			Shard:     -1 << 63,
			SeqNo:     2,
			RootHash:  bytes.Repeat([]byte{0xb1}, 32),
			FileHash:  bytes.Repeat([]byte{0xb2}, 32),
		},
	}
	if !emptyAnchor {
		blocks[1] = ton.BlockIDExt{
			Workchain: 0,
			Shard:     -1 << 63,
			SeqNo:     2,
			RootHash:  bytes.Repeat([]byte{0xaa}, 32),
			FileHash:  bytes.Repeat([]byte{0xab}, 32),
		}
	}
	artifacts := make([]*CandidateArtifact, 3)
	parent := simplex.Genesis()
	for i := range artifacts {
		id := simplex.CandidateID{Slot: uint32(i), Hash: [32]byte{byte(0xc0 + i)}}
		artifacts[i] = &CandidateArtifact{
			Candidate: simplex.Candidate{
				ID:     id,
				Parent: parent,
				Empty:  emptyAnchor && i == 1,
				Block:  blocks[i],
			},
			BlockBOC:     []byte{byte(0xd0 + i)},
			CollatedData: []byte{byte(0xe0 + i)},
		}
		if err := candidates.stage(artifacts[i], []byte{byte(i + 1)}); err != nil {
			t.Fatal(err)
		}
		candidates.observeNotarization(id, &simplex.Certificate{Vote: simplex.NotarizeVote(id)})
		parent = simplex.Parent(id)
	}
	resolver := newStateResolver(
		groups.ShardID{Workchain: 0, Shard: -1 << 63},
		SessionStorageID{},
		storage,
		newRuntimeTestBackend(),
		candidates,
		StoredSessionState{},
		nil,
	)
	if err := resolver.start(context.Background(), runtimeTestStart()); err != nil {
		t.Fatal(err)
	}

	return resolver, candidates, artifacts
}

func assertArtifactIdentity(
	t *testing.T,
	got []*CandidateArtifact,
	want []*CandidateArtifact,
) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("lineage length = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("lineage[%d] = %p, want original %p", i, got[i], want[i])
		}
		if &got[i].BlockBOC[0] != &want[i].BlockBOC[0] ||
			&got[i].CollatedData[0] != &want[i].CollatedData[0] {
			t.Fatalf("lineage[%d] copied immutable payloads", i)
		}
	}
}
