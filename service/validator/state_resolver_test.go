package validator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
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

func TestStateResolverAcceptanceReusesPreparationOnRetry(t *testing.T) {
	// A full retry cycle of the production constants is what this asserts, so it
	// spends its second asleep rather than computing. The constants must not
	// shrink — the assertions below are the pin on them — but such seconds need
	// not be spent one after another.
	//
	// Parallel is safe in this test specifically because every timing assertion
	// it makes is a lower bound — the retry must not arrive sooner than the
	// production delay — and it sets no deadline of its own. Contention can only
	// make a lower bound easier to satisfy.
	t.Parallel()
	backend := newRuntimeTestBackend()
	var mu sync.Mutex
	var calls []time.Time
	preparations := 0
	backend.prepareAcceptance = func(_ context.Context, acceptance BlockAcceptance) (PreparedBlockAcceptance, error) {
		preparations++
		return &runtimeTestBlockAcceptance{backend: backend, acceptance: acceptance}, nil
	}
	backend.acceptance = func(context.Context, BlockAcceptance) error {
		mu.Lock()
		calls = append(calls, time.Now())
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
	if preparations != 1 {
		t.Fatalf("preparation count = %d, want 1", preparations)
	}
	if delay := calls[1].Sub(calls[0]); delay < 950*time.Millisecond {
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

func TestStateResolverDropsFlightAfterLastWaiterCancels(t *testing.T) {
	storage := newRuntimeTestStorage()
	provider := &retryCandidateProvider{
		called:   make(chan struct{}, 1),
		finished: make(chan struct{}, 1),
	}
	params := simplex.DefaultParams()
	params.CandidateResolveTimeout = time.Minute
	params.CandidateResolveTimeoutCap = time.Minute
	candidates := newResolverForTest(storage, provider, 2, params)
	defer candidates.close()

	config, _ := runtimeTestConfig(0x90, &runtimeTestJournal{})
	resolver := newStateResolver(
		config.Shard,
		config.StorageID,
		storage,
		newRuntimeTestBackend(),
		candidates,
		StoredSessionState{},
		nil,
		params,
		config.Protocol.SlotsPerLeaderWindow,
	)
	defer resolver.close()
	if err := resolver.start(context.Background(), runtimeTestStart()); err != nil {
		t.Fatal(err)
	}

	parent := simplex.Parent(simplex.CandidateID{Slot: 31, Hash: [32]byte{0x31}})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := resolver.resolve(ctx, parent)
		result <- err
	}()
	select {
	case <-provider.called:
	case <-time.After(time.Second):
		t.Fatal("state flight did not enter candidate resolution")
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("state resolve error = %v, want context cancellation", err)
	}
	select {
	case <-provider.finished:
	case <-time.After(time.Second):
		t.Fatal("candidate request outlived the last state waiter")
	}

	deadline := time.Now().Add(time.Second)
	for {
		resolver.mu.Lock()
		flight := resolver.states[parent]
		resolver.mu.Unlock()
		if flight == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("cancelled state flight remains cached: %p", flight)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestStateResolverFinalizationCandidateEpochExpires(t *testing.T) {
	storage := newRuntimeTestStorage()
	provider := &retryCandidateProvider{
		called:   make(chan struct{}, 1),
		finished: make(chan struct{}, 1),
	}
	params := simplex.DefaultParams()
	params.TargetRate = time.Millisecond
	params.MaxLeaderWindowDesync = 1
	params.CandidateResolveTimeout = time.Second
	params.CandidateResolveTimeoutCap = 30 * time.Millisecond
	candidates := newResolverForTest(storage, provider, 2, params)
	defer candidates.close()

	config, _ := runtimeTestConfig(0x8f, &runtimeTestJournal{})
	resolver := newStateResolver(
		config.Shard,
		config.StorageID,
		storage,
		newRuntimeTestBackend(),
		candidates,
		StoredSessionState{},
		nil,
		params,
		config.Protocol.SlotsPerLeaderWindow,
	)
	defer resolver.close()
	if err := resolver.start(context.Background(), runtimeTestStart()); err != nil {
		t.Fatal(err)
	}

	id := simplex.CandidateID{Slot: 41, Hash: [32]byte{0x41}}
	err := resolver.finalize(context.Background(), id, simplex.VerifiedCertificate{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("finalization candidate epoch error = %v, want context deadline", err)
	}
	select {
	case <-provider.finished:
	case <-time.After(time.Second):
		t.Fatal("expired finalization left its candidate request running")
	}
	waitForCandidateResolveFlight(t, candidates, id, nil)

	resolver.mu.Lock()
	marker := resolver.finalized[id]
	resolver.mu.Unlock()
	if marker != nil {
		t.Fatalf("failed finalization retained marker %+v", marker)
	}
}

func TestStateResolverWaitsForCancelledFlightCleanupBeforeRestart(t *testing.T) {
	storage := newRuntimeTestStorage()
	params := simplex.DefaultParams()
	candidates := newResolverForTest(
		storage,
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		params,
	)
	defer candidates.close()

	config, _ := runtimeTestConfig(0x91, &runtimeTestJournal{})
	resolver := newStateResolver(
		config.Shard,
		config.StorageID,
		storage,
		newRuntimeTestBackend(),
		candidates,
		StoredSessionState{},
		nil,
		params,
		config.Protocol.SlotsPerLeaderWindow,
	)
	defer resolver.close()
	if err := resolver.start(context.Background(), runtimeTestStart()); err != nil {
		t.Fatal(err)
	}

	parent := simplex.Genesis()
	old := &stateFlight{
		done:      make(chan struct{}),
		cancel:    func() {},
		cancelErr: context.Canceled,
	}
	resolver.mu.Lock()
	resolver.states[parent] = old
	resolver.mu.Unlock()

	result := make(chan error, 1)
	go func() {
		resolved, err := resolver.resolve(context.Background(), parent)
		if err == nil && resolved.State == nil {
			err = errors.New("restarted genesis resolve returned no state")
		}
		result <- err
	}()
	select {
	case err := <-result:
		t.Fatalf("new caller joined the cancelled flight: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	resolver.mu.Lock()
	delete(resolver.states, parent)
	old.finished = true
	old.err = context.Canceled
	close(old.done)
	resolver.mu.Unlock()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("resolve after cancelled-flight cleanup: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("caller did not restart after cancelled-flight cleanup")
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
		resolverTestSeal(t, simplex.NotarizeVote(artifact.Candidate.ID)),
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
		simplex.DefaultParams(),
		4,
	)
	defer resolver.close()
	if err := resolver.start(context.Background(), runtimeTestStart()); err != nil {
		t.Fatal(err)
	}

	certificate := resolverTestSeal(t, simplex.FinalizeVote(artifact.Candidate.ID))
	if err := resolver.finalize(context.Background(), artifact.Candidate.ID, certificate); err != nil {
		t.Fatal(err)
	}
	if len(acceptances) != 1 || !acceptances[0].Replay {
		t.Fatalf("replayed acceptances = %+v, want one replay", acceptances)
	}
	if err := resolver.finalize(context.Background(), artifact.Candidate.ID, certificate); err != nil {
		t.Fatal(err)
	}
	if len(acceptances) != 1 {
		t.Fatalf("acceptance was replayed %d times, want exactly once", len(acceptances))
	}
}

func TestStateResolverPersistsFinalizedCandidateForPeerlessRestart(t *testing.T) {
	storage := newRuntimeTestStorage()
	config, privateKey := runtimeTestConfig(resolverTestSessionTag, &runtimeTestJournal{})
	artifact := runtimeOrdinaryArtifact(t, config, privateKey, 1, simplex.Genesis())
	params := simplex.DefaultParams()
	provider := &retryCandidateProvider{called: make(chan struct{}, 1)}

	candidates := newResolverForTest(
		storage,
		provider,
		1,
		params,
	)
	candidates.session = config.StorageID
	wire, _, err := candidates.codec.encodeForBroadcast(artifact)
	if err != nil {
		t.Fatal(err)
	}
	if err = candidates.stage(artifact, wire); err != nil {
		t.Fatal(err)
	}
	notarization := resolverTestSeal(t, simplex.NotarizeVote(artifact.Candidate.ID))
	candidates.observeNotarization(artifact.Candidate.ID, notarization)

	backend := newRuntimeTestBackend()
	resolver := newStateResolver(
		config.Shard,
		config.StorageID,
		storage,
		backend,
		candidates,
		StoredSessionState{},
		nil,
		params,
		config.Protocol.SlotsPerLeaderWindow,
	)
	if err = resolver.start(context.Background(), runtimeTestStart()); err != nil {
		t.Fatal(err)
	}
	finalization := resolverTestSeal(t, simplex.FinalizeVote(artifact.Candidate.ID))
	if err = resolver.finalize(context.Background(), artifact.Candidate.ID, finalization); err != nil {
		t.Fatal(err)
	}
	resolver.close()
	candidates.close()

	if storage.saveCount() != 1 {
		t.Fatalf("candidate saves = %d, want one finalization-owned write", storage.saveCount())
	}

	restartedProvider := &retryCandidateProvider{called: make(chan struct{}, 1)}
	restartedCandidates := newRestoredResolverForTest(
		storage,
		restartedProvider,
		1,
		params,
		StoredSessionState{CandidateIDs: []simplex.CandidateID{artifact.Candidate.ID}},
	)
	restartedCandidates.session = config.StorageID
	restartedCandidates.observeNotarization(artifact.Candidate.ID, notarization)
	restartedBackend := newRuntimeTestBackend()
	restarted := newStateResolver(
		config.Shard,
		config.StorageID,
		storage,
		restartedBackend,
		restartedCandidates,
		StoredSessionState{Finalized: []simplex.CandidateID{artifact.Candidate.ID}},
		[]simplex.VerifiedCertificate{finalization},
		params,
		config.Protocol.SlotsPerLeaderWindow,
	)
	t.Cleanup(func() {
		restarted.close()
		restartedCandidates.close()
	})

	if err = restarted.start(context.Background(), runtimeTestStart()); err != nil {
		t.Fatalf("restart recovery without peers: %v", err)
	}
	if storage.saveCount() != 1 {
		t.Fatalf("candidate saves after restart = %d, want idempotent one", storage.saveCount())
	}
	if _, requests, _ := restartedProvider.snapshot(); len(requests) != 0 {
		t.Fatalf("peerless restart made candidate requests: %+v", requests)
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
			RootHash:  testTipRootHash(0x31),
			FileHash:  testTipFileHash(0x31),
		},
	}}
	second := &CandidateArtifact{Candidate: simplex.Candidate{
		ID:     secondID,
		Parent: simplex.Parent(firstID),
		Block: ton.BlockIDExt{
			Workchain: -1,
			Shard:     -1 << 63,
			SeqNo:     2,
			RootHash:  testTipRootHash(0x41),
			FileHash:  testTipFileHash(0x41),
		},
	}}
	for i, artifact := range []*CandidateArtifact{first, second} {
		if err := candidates.stage(artifact, []byte{byte(i + 1)}); err != nil {
			t.Fatal(err)
		}
		candidates.observeNotarization(
			artifact.Candidate.ID,
			resolverTestSeal(t, simplex.NotarizeVote(artifact.Candidate.ID)),
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
	recovery := []simplex.VerifiedCertificate{
		resolverTestSeal(t, simplex.FinalizeVote(firstID)),
		resolverTestSeal(t, simplex.FinalizeVote(secondID)),
	}
	resolver := newStateResolver(
		shard,
		SessionStorageID{},
		storage,
		backend,
		candidates,
		StoredSessionState{},
		recovery,
		simplex.DefaultParams(),
		4,
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
			RootHash:  testTipRootHash(0x31),
			FileHash:  testTipFileHash(0x31),
		},
	}}
	second := &CandidateArtifact{Candidate: simplex.Candidate{
		ID:     secondID,
		Parent: simplex.Parent(firstID),
		Block: ton.BlockIDExt{
			Workchain: -1,
			Shard:     -1 << 63,
			SeqNo:     2,
			RootHash:  testTipRootHash(0x41),
			FileHash:  testTipFileHash(0x41),
		},
	}}
	for i, artifact := range []*CandidateArtifact{first, second} {
		if err := candidates.stage(artifact, []byte{byte(i + 1)}); err != nil {
			t.Fatal(err)
		}
		candidates.observeNotarization(
			artifact.Candidate.ID,
			resolverTestSeal(t, simplex.NotarizeVote(artifact.Candidate.ID)),
		)
	}

	backend := newRuntimeTestBackend()
	acceptances := 0
	backend.acceptance = func(context.Context, BlockAcceptance) error {
		acceptances++
		return nil
	}
	recovery := []simplex.VerifiedCertificate{
		resolverTestSeal(t, simplex.FinalizeVote(firstID)),
		resolverTestSeal(t, simplex.FinalizeVote(secondID)),
	}
	resolver := newStateResolver(
		shard,
		SessionStorageID{},
		storage,
		backend,
		candidates,
		StoredSessionState{Finalized: []simplex.CandidateID{firstID, secondID}},
		recovery,
		simplex.DefaultParams(),
		4,
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
				RootHash:  testTipRootHash(uint64(0x31 + i)),
				FileHash:  testTipFileHash(uint64(0x31 + i)),
			},
		}}
		if err := candidates.stage(artifact, []byte{byte(i + 1)}); err != nil {
			t.Fatal(err)
		}
		candidates.observeNotarization(
			id,
			resolverTestSeal(t, simplex.NotarizeVote(id)),
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
			tip.BlockBOC = testTipBOCFor(block)
			tip.Block = testTipBlockFor(block)
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
	recovery := make([]simplex.VerifiedCertificate, len(ids))
	for i, id := range ids {
		recovery[i] = resolverTestSeal(t, simplex.FinalizeVote(id))
	}
	resolver := newStateResolver(
		shard,
		SessionStorageID{},
		storage,
		backend,
		candidates,
		StoredSessionState{Finalized: ids},
		recovery,
		simplex.DefaultParams(),
		4,
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
	// A full retry cycle of the production constants is what this asserts, so it
	// spends its second asleep rather than computing.
	//
	// Deliberately not parallel. This one asserts that the second replay has not
	// been submitted within 100 ms, against a production retry delay of one
	// second, and then gives the whole recovery two seconds to finish. Both are
	// satisfied by the test goroutine being scheduled promptly, not by the code
	// being right, so contention here would read as a product bug.
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
			RootHash:  testTipRootHash(0x31),
			FileHash:  testTipFileHash(0x31),
		},
	}}
	second := &CandidateArtifact{Candidate: simplex.Candidate{
		ID:     secondID,
		Parent: simplex.Parent(firstID),
		Block: ton.BlockIDExt{
			Workchain: -1,
			Shard:     -1 << 63,
			SeqNo:     2,
			RootHash:  testTipRootHash(0x41),
			FileHash:  testTipFileHash(0x41),
		},
	}}
	for i, artifact := range []*CandidateArtifact{first, second} {
		if err := candidates.stage(artifact, []byte{byte(i + 1)}); err != nil {
			t.Fatal(err)
		}
		candidates.observeNotarization(
			artifact.Candidate.ID,
			resolverTestSeal(t, simplex.NotarizeVote(artifact.Candidate.ID)),
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
			BlockBOC: testTipBOCFor(block),
			Block:    testTipBlockFor(block),
		}}}, nil
	}
	recovery := []simplex.VerifiedCertificate{
		resolverTestSeal(t, simplex.FinalizeVote(firstID)),
		resolverTestSeal(t, simplex.FinalizeVote(secondID)),
	}
	resolver := newStateResolver(
		shard,
		SessionStorageID{},
		storage,
		backend,
		candidates,
		StoredSessionState{},
		recovery,
		simplex.DefaultParams(),
		4,
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
	// A full retry cycle of the production constants is what this asserts, so it
	// spends its second asleep rather than computing. The constants must not
	// shrink — the assertions below are the pin on them — but such seconds need
	// not be spent one after another.
	//
	// Parallel is safe in this test specifically because every timing assertion
	// it makes is a lower bound — the retry must not arrive sooner than the
	// production delay — and it sets no deadline of its own. Contention can only
	// make a lower bound easier to satisfy.
	t.Parallel()
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
		resolverTestSeal(t, simplex.NotarizeVote(artifact.Candidate.ID)),
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
			BlockBOC: testTipBOCFor(request.Blocks[0]),
			Block:    testTipBlockFor(request.Blocks[0]),
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
		simplex.DefaultParams(),
		4,
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
	candidates.observeNotarization(ordinaryID, resolverTestSeal(t, simplex.NotarizeVote(ordinaryID)))
	candidates.observeNotarization(emptyID, resolverTestSeal(t, simplex.NotarizeVote(emptyID)))

	resolver := newStateResolver(
		groups.ShardID{Workchain: 0, Shard: -1 << 63},
		SessionStorageID{},
		storage,
		backend,
		candidates,
		StoredSessionState{},
		nil,
		simplex.DefaultParams(),
		4,
	)
	if err := resolver.start(context.Background(), runtimeTestStart()); err != nil {
		t.Fatal(err)
	}
	finalCertificate := resolverTestSeal(t, simplex.FinalizeVote(emptyID))
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

func TestStateResolverAncestryStopsAtExactFinalizedBlockWithoutLoadingState(t *testing.T) {
	t.Run("ordinary anchor", func(t *testing.T) {
		resolver, candidates, artifacts := lineageResolverForTest(t, false)
		defer resolver.close()
		defer candidates.close()

		stateLoads := 0
		backend := newRuntimeTestBackend()
		backend.load = func(context.Context, ChainStateRequest) (ChainStateData, error) {
			stateLoads++

			return ChainStateData{}, errors.New("unexpected chain-state load")
		}
		resolver.backend = backend

		anchor := artifacts[0].Candidate.Block
		got, err := resolver.ancestry(
			context.Background(),
			simplex.Parent(artifacts[2].Candidate.ID),
			&anchor,
		)
		if err != nil {
			t.Fatal(err)
		}
		assertAncestryStats(t, got, 3, 3, 0, 0)
		if stateLoads != 0 {
			t.Fatalf("ancestry loaded %d chain states, want none", stateLoads)
		}
	})

	t.Run("empty anchor", func(t *testing.T) {
		resolver, candidates, artifacts := lineageResolverForTest(t, true)
		defer resolver.close()
		defer candidates.close()

		stateLoads := 0
		backend := newRuntimeTestBackend()
		backend.load = func(context.Context, ChainStateRequest) (ChainStateData, error) {
			stateLoads++

			return ChainStateData{}, errors.New("unexpected chain-state load")
		}
		resolver.backend = backend

		anchor := artifacts[0].Candidate.Block
		got, err := resolver.ancestry(
			context.Background(),
			simplex.Parent(artifacts[2].Candidate.ID),
			&anchor,
		)
		if err != nil {
			t.Fatal(err)
		}
		assertAncestryStats(t, got, 2, 2, 0, 0)
		if stateLoads != 0 {
			t.Fatalf("ancestry loaded %d chain states, want none", stateLoads)
		}
	})
}

func TestStateResolverAncestryUsesTheExactSuppliedFinalizedBlock(t *testing.T) {
	resolver, candidates, artifacts := lineageResolverForTest(t, false)
	defer resolver.close()
	defer candidates.close()

	resolver.mu.Lock()
	resolver.finalized[artifacts[1].Candidate.ID] = &finalizedState{
		isDone:     true,
		reconciled: true,
	}
	resolver.mu.Unlock()

	masterchainAnchor := artifacts[0].Candidate.Block
	got, err := resolver.ancestry(
		context.Background(),
		simplex.Parent(artifacts[2].Candidate.ID),
		&masterchainAnchor,
	)
	if err != nil {
		t.Fatal(err)
	}
	// The newer local finalization marker is not the caller's ancestry
	// constraint. The walk must follow exact parent links until it reaches the
	// block supplied by the selected-base contract.
	assertAncestryStats(t, got, 3, 3, 0, 0)
}

func TestStateResolverAncestryWithoutFinalizedBlockDoesNotWalk(t *testing.T) {
	resolver, candidates, artifacts := lineageResolverForTest(t, false)
	defer resolver.close()
	defer candidates.close()

	stateLoads := 0
	backend := newRuntimeTestBackend()
	backend.load = func(
		context.Context,
		ChainStateRequest,
	) (ChainStateData, error) {
		stateLoads++

		return ChainStateData{}, errors.New("unexpected chain-state load")
	}
	resolver.backend = backend

	base := simplex.Parent(artifacts[len(artifacts)-1].Candidate.ID)
	before := candidates.cacheStats()
	got, err := resolver.ancestry(
		context.Background(),
		base,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	assertAncestryStats(t, got, 0, 0, 0, 0)
	if stateLoads != 0 {
		t.Fatalf("nil-finalized ancestry loaded %d chain states, want none", stateLoads)
	}
	if after := candidates.cacheStats(); after != before {
		t.Fatalf("nil-finalized ancestry changed candidate cache from %+v to %+v", before, after)
	}
}

func TestStateResolverAncestryAllowsGenesisAndRejectsForeignBlock(t *testing.T) {
	resolver, candidates, artifacts := lineageResolverForTest(t, false)
	defer resolver.close()
	defer candidates.close()

	genesis := runtimeTestStart().Genesis[0]
	got, err := resolver.ancestry(
		context.Background(),
		simplex.Parent(artifacts[2].Candidate.ID),
		&genesis,
	)
	if err != nil {
		t.Fatal(err)
	}
	assertAncestryStats(t, got, 3, 3, 0, 0)

	// Another valid block already present in this fixture is still foreign to
	// the exact parent chain below the chosen base.
	unrelated := artifacts[2].Candidate.Block
	foreignStats, err := resolver.ancestry(
		context.Background(),
		simplex.Parent(artifacts[1].Candidate.ID),
		&unrelated,
	)
	if !errors.Is(err, errFinalizedLineageAhead) {
		t.Fatalf("unrelated finalized anchor error = %v, want %v", err, errFinalizedLineageAhead)
	}
	assertAncestryStats(t, foreignStats, 2, 2, 0, 0)
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

	resolver.notifyFinalized(100, retentionFloorNone)

	margin := resolver.stateRetainedSlots()
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	// Genesis, the retained margin below the watermark, and the flight that is
	// still resolving. Dropping the latter would let the next resolve start a
	// second ApplyMerkleUpdate over the same parent.
	if len(resolver.states) != 2+int(margin) {
		t.Fatalf("retained states = %d, want %d", len(resolver.states), 2+int(margin))
	}
	if resolver.states[simplex.Genesis()] != genesis {
		t.Fatal("genesis state was released")
	}
	if resolver.states[parentAt(50, 0xff)] != inFlight {
		t.Fatal("in-flight state below the watermark was released")
	}
	for slot := 100 - margin; slot < 100; slot++ {
		if resolver.states[parentAt(slot, 0xc0)] == nil {
			t.Fatalf("state at slot %d was released inside the retained margin", slot)
		}
	}
	for slot := uint32(0); slot < 100-margin; slot++ {
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
	resolver.notifyFinalized(resolver.stateRetainedSlots()-1, retentionFloorNone)

	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	if len(resolver.states) != 1 {
		t.Fatalf("retained states = %d, want the first slots kept", len(resolver.states))
	}
}

// Every finalization whose state was ever resolved carries a whole separately
// loaded state tree. Nothing but this sweep drops one, so a session that runs
// for an hour would otherwise hold every state version it has finalized.
func TestStateResolverFinalizationReleasesAppliedStatesBelowWatermark(t *testing.T) {
	resolver, candidates, _ := lineageResolverForTest(t, false)
	defer resolver.close()
	defer candidates.close()

	const finalized = uint32(100)
	margin := resolver.finalizedStateRetainedSlots()
	watermark := finalized - margin
	block := func(slot uint32) ton.BlockIDExt {
		return ton.BlockIDExt{
			Workchain: 0,
			Shard:     -1 << 63,
			SeqNo:     slot + 1,
			RootHash:  testTipRootHash(uint64(slot)<<8 | 0x11),
			FileHash:  testTipFileHash(uint64(slot)<<8 | 0x11),
		}
	}
	id := func(slot uint32) simplex.CandidateID {
		return simplex.CandidateID{Slot: slot, Hash: [32]byte{byte(slot), 0xf0}}
	}

	resolver.mu.Lock()
	for slot := uint32(0); slot < finalized; slot++ {
		state := &finalizedState{isDone: true, reconciled: true}
		resolver.finalized[id(slot)] = state
		resolver.rememberAppliedStateLocked(id(slot), state, &ChainState{
			shard: resolver.shard,
			tips:  []ChainTip{{ID: block(slot), BlockBOC: make([]byte, 128)}},
		})
	}
	// A finalization still being applied keeps its state until it completes.
	applying := resolver.finalized[id(1)]
	applying.inFlight = &resolverFlight{done: make(chan struct{})}
	// An ancestry guard reaching the oldest candidate only constrains candidate
	// payload retention. The selected base carries its resolved state directly,
	// so this floor must not pin historical applied-state trees.
	resolver.noteLineageFloorLocked(0)
	resolver.mu.Unlock()

	resolver.notifyFinalized(finalized, retentionFloorNone)

	resolver.mu.Lock()
	retained := len(resolver.applied)
	resolver.mu.Unlock()
	if retained != int(margin)+1 {
		t.Fatalf("retained applied states = %d, want the margin plus the one in flight", retained)
	}

	resolver.mu.Lock()
	for slot := uint32(0); slot < finalized; slot++ {
		state := resolver.finalized[id(slot)]
		if state == nil || !state.isDone || !state.reconciled {
			t.Fatalf("finalization marker at slot %d was dropped with its state: %+v", slot, state)
		}
		switch {
		case slot == 1:
			if state.appliedState == nil {
				t.Fatal("a finalization still in flight lost its applied state")
			}
		case slot < watermark:
			if state.appliedState != nil {
				t.Fatalf("applied state at slot %d below watermark %d was retained", slot, watermark)
			}
		default:
			if state.appliedState == nil {
				t.Fatalf("applied state at slot %d was released inside the retained margin", slot)
			}
		}
	}
	resolver.mu.Unlock()
}

// The ancestry guard reaches back to the masterchain-visible finalized block,
// which lags this session's own finalized slot, so it routinely asks for
// candidates the finalization sweep has already released.
func TestStateResolverAncestryRetainsReleasedAncestorsWithoutPayloadReads(t *testing.T) {
	storage := newRuntimeTestStorage()
	runtime, privateKey := prepareRuntimeTest(
		t,
		0x5b,
		storage,
		newRuntimeTestNetwork(),
		newRuntimeTestBackend(),
	)
	defer runtime.Close()
	if err := runtime.states.start(context.Background(), runtimeTestStart()); err != nil {
		t.Fatal(err)
	}

	artifacts := make([]*CandidateArtifact, 3)
	parent := simplex.Genesis()
	for i := range artifacts {
		artifact := runtimeBlockArtifact(
			t,
			runtime.config,
			privateKey,
			uint32(i),
			parent,
			uint32(i)+1,
			uint64(0xa1+i),
		)
		wire, _, err := runtime.codec.encodeForBroadcast(artifact)
		if err != nil {
			t.Fatal(err)
		}
		if err = runtime.candidates.stage(artifact, wire); err != nil {
			t.Fatal(err)
		}
		if err = runtime.candidates.store(context.Background(), artifact.Candidate.ID); err != nil {
			t.Fatal(err)
		}
		runtime.candidates.observeNotarization(
			artifact.Candidate.ID,
			resolverTestSeal(t, simplex.NotarizeVote(artifact.Candidate.ID)),
		)
		artifacts[i] = artifact
		parent = simplex.Parent(artifact.Candidate.ID)
	}
	runtime.candidates.notifyFinalized(uint32(len(artifacts))+candidateCacheRetainedSlots, retentionFloorNone)

	anchor := artifacts[0].Candidate.Block
	walk, err := runtime.states.ancestry(context.Background(), parent, &anchor)
	if err != nil {
		t.Fatal(err)
	}
	assertAncestryStats(t, walk, len(artifacts), len(artifacts), 0, 0)
	if storage.loadCount() != 0 {
		t.Fatalf("candidate reloads = %d, want no payload reads for known ancestry", storage.loadCount())
	}
	if stats := runtime.candidates.cacheStats(); stats.Candidates != 0 || stats.Bytes != 0 {
		t.Fatalf("ancestry repinned released payloads: %+v", stats)
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
		RootHash:  testTipRootHash(0xa1),
		FileHash:  testTipFileHash(0xa1),
	}
	blocks := []ton.BlockIDExt{
		anchor,
		anchor,
		{
			Workchain: 0,
			Shard:     -1 << 63,
			SeqNo:     2,
			RootHash:  testTipRootHash(0xb1),
			FileHash:  testTipFileHash(0xb1),
		},
	}
	if !emptyAnchor {
		blocks[1] = ton.BlockIDExt{
			Workchain: 0,
			Shard:     -1 << 63,
			SeqNo:     2,
			RootHash:  testTipRootHash(0xaa),
			FileHash:  testTipFileHash(0xaa),
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
		candidates.observeNotarization(id, resolverTestSeal(t, simplex.NotarizeVote(id)))
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
		simplex.DefaultParams(),
		4,
	)
	if err := resolver.start(context.Background(), runtimeTestStart()); err != nil {
		t.Fatal(err)
	}

	return resolver, candidates, artifacts
}

func assertAncestryStats(
	t *testing.T,
	got lineageWalkStats,
	visited int,
	memory int,
	storage int,
	peer int,
) {
	t.Helper()

	if got.Visited != visited {
		t.Fatalf("ancestry visited %d candidates, want %d", got.Visited, visited)
	}
	want := [lineageStepSourceCount]int{
		LineageStepMemory:  memory,
		LineageStepStorage: storage,
		LineageStepPeer:    peer,
	}
	if got.Steps != want {
		t.Fatalf("ancestry step sources = %v, want %v", got.Steps, want)
	}
}

// laggingSession is a consensus session whose selected base reaches farther
// back than the fixed candidate-retention margin. Every candidate is durable
// and finalized, so it exercises whether a completed ancestry guard moves the
// payload floor without pinning historical state trees.
type laggingSession struct {
	storage    *runtimeTestStorage
	backend    *runtimeTestBackend
	candidates *candidateResolver
	states     *stateResolver
	artifacts  []*CandidateArtifact
	// stride is how many consensus slots separate two consecutive candidates.
	// Everything between them is a slot that settled without a block.
	stride int

	mu sync.Mutex
	// State loads stay observable here solely as a regression guard: ancestry
	// follows candidate parent links and must never reach this backend.
	loads    int
	notReady int
	capped   bool
}

func newLaggingSessionForTest(t *testing.T, length int, appliedThrough int) *laggingSession {
	t.Helper()

	return newLaggingSessionWithStride(t, length, appliedThrough, 1)
}

// newLaggingSessionWithStride places consecutive candidates stride slots apart.
// A stride above one is a session whose slots advanced without producing
// anything — skip certificates, which Simplex settles without a candidate and
// without a finalization — so the slot distance between two candidates says
// nothing about how much this session is holding.
func newLaggingSessionWithStride(
	t *testing.T,
	length int,
	appliedThrough int,
	stride int,
) *laggingSession {
	t.Helper()

	config, privateKey := runtimeTestConfig(0x71, &runtimeTestJournal{})
	storage := newRuntimeTestStorage()
	backend := newRuntimeTestBackend()
	candidates := newResolverForTest(
		storage,
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		simplex.DefaultParams(),
	)
	states := newStateResolver(
		config.Shard,
		SessionStorageID{},
		storage,
		backend,
		candidates,
		StoredSessionState{},
		nil,
		simplex.DefaultParams(),
		4,
	)
	session := &laggingSession{
		storage:    storage,
		backend:    backend,
		candidates: candidates,
		states:     states,
		artifacts:  make([]*CandidateArtifact, length),
	}
	if err := states.start(context.Background(), runtimeTestStart()); err != nil {
		t.Fatal(err)
	}

	// The node has applied everything up to appliedThrough and nothing after it,
	// which is exactly what a state load reports while the block is still in the
	// apply queue.
	backend.load = func(_ context.Context, request ChainStateRequest) (ChainStateData, error) {
		tips := make([]ChainTip, len(request.Blocks))
		for i := range request.Blocks {
			if int(request.Blocks[i].SeqNo) > appliedThrough+1 {
				session.mu.Lock()
				session.notReady++
				session.mu.Unlock()

				return ChainStateData{}, ErrBlockNotReady
			}
			tips[i] = ChainTip{ID: request.Blocks[i], State: backend.stateRoot}
			if request.Blocks[i].SeqNo != 0 {
				tips[i].BlockBOC = testTipBOCFor(request.Blocks[i])
				tips[i].Block = testTipBlockFor(request.Blocks[i])
			}
		}
		session.mu.Lock()
		session.loads++
		session.mu.Unlock()

		return ChainStateData{Tips: tips}, nil
	}

	session.stride = stride
	parent := simplex.Genesis()
	for index := range length {
		slot := index * stride
		// The block payload tag is one byte wide, and nothing here depends on it
		// being distinct: a block identity carries its sequence number too.
		artifact := runtimeBlockArtifact(
			t, config, privateKey, uint32(slot), parent, uint32(slot+1), uint64(slot%256),
		)
		wire, _, err := candidates.codec.encodeForBroadcast(artifact)
		if err != nil {
			t.Fatal(err)
		}
		if err = candidates.stage(artifact, wire); err != nil {
			t.Fatal(err)
		}
		if err = candidates.store(context.Background(), artifact.Candidate.ID); err != nil {
			t.Fatal(err)
		}
		id := artifact.Candidate.ID
		candidates.observeNotarization(id, resolverTestSeal(t, simplex.NotarizeVote(id)))
		states.mu.Lock()
		states.finalized[id] = &finalizedState{isDone: true, reconciled: true}
		states.mu.Unlock()
		session.artifacts[index] = artifact
		parent = simplex.Parent(id)
	}

	return session
}

// sweep runs the pair of finalization sweeps in the order and with the wiring
// the session runtime uses.
func (s *laggingSession) sweep(slot uint32) {
	budgetFloor := s.candidates.retentionCapFloor(slot)
	floor := s.states.notifyFinalized(slot, budgetFloor)
	s.candidates.notifyFinalized(slot, floor.Slot)
	s.mu.Lock()
	s.capped = floor.Capped
	s.mu.Unlock()
}

// slotOf is the consensus slot the i-th candidate occupies.
func (s *laggingSession) slotOf(index int) uint32 {
	return uint32(index * s.stride)
}

// walk is one leader window's ancestry guard, from the tip back to the
// masterchain-visible finalized anchor.
func (s *laggingSession) walk(t *testing.T, tip int, anchorIndex int) lineageWalkStats {
	t.Helper()

	anchor := s.artifacts[anchorIndex].Candidate.Block
	walk, err := s.states.ancestry(
		context.Background(),
		simplex.Parent(s.artifacts[tip].Candidate.ID),
		&anchor,
	)
	if err != nil {
		t.Fatalf("ancestry from slot %d back to slot %d: %v", tip, anchorIndex, err)
	}

	return walk
}

func (s *laggingSession) counts() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.storage.loadCount(), s.loads
}

func (s *laggingSession) close() {
	s.states.close()
	s.candidates.close()
}

// A node whose apply pipeline trails consensus by more than the fixed retention
// margin still leads windows, and every one of them walks back to the anchor
// apply stopped at. Releasing on the margin alone drops exactly that lineage
// and reloads it — a mainnet-scale candidate per step, plus the anchor state —
// once per window, forever. What the sweeps keep is what the walk needs.
func TestRetentionFloorLeavesALaggingLeaderWindowNothingToReload(t *testing.T) {
	for _, lag := range []int{int(candidateCacheRetainedSlots) + 1, 32, 64} {
		t.Run(fmt.Sprintf("lag-%d", lag), func(t *testing.T) {
			const length = 96
			tip := length - 1
			session := newLaggingSessionForTest(t, length, tip-lag)
			defer session.close()

			for slot := range tip + 1 {
				session.sweep(uint32(slot))
			}
			// The first window to walk this far back is the one that tells the
			// sweeps how far back that is.
			session.walk(t, tip, tip-lag)

			// Steady state: consensus finalizes again, both sweeps run again, and
			// the next window opens.
			session.sweep(uint32(tip))
			candidateReads, stateLoads := session.counts()
			walk := session.walk(t, tip, tip-lag)
			reads, loads := session.counts()
			if got := reads - candidateReads; got != 0 {
				t.Fatalf("a leader window %d slots behind read %d candidates back from storage", lag, got)
			}
			if got := loads - stateLoads; got != 0 {
				t.Fatalf("a leader window %d slots behind reloaded %d chain states from the node", lag, got)
			}
			if walk.Visited != lag+1 {
				t.Fatalf("the walk covered %d candidates, want the %d of this lag: "+
					"the test is not exercising what it claims", walk.Visited, lag+1)
			}
		})
	}
}

// THE REGRESSION THIS FILE EXISTS FOR.
//
// A session that skips settles slot after slot without producing a candidate:
// Simplex advances its slot counter over skip certificates and emits no
// finalization for any of them. The retention floor used to be bounded by
// slot - 64, so 64 skipped slots — 12.8 s on a 200 ms network — made this node
// give up on the lineage its own producer had asked for, while it was holding
// nothing at all. Parent resolution then went to storage on every candidate,
// which is how a stalled session became a session that also could not vote.
//
// The floor may only be given up for memory that is actually retained, so slots
// that never carried a block must cost nothing.
func TestRetentionFloorIgnoresSkippedSlots(t *testing.T) {
	// Eight candidates 16 slots apart: 112 consensus slots, of which 105 settled
	// without a block. The span is far past the 64 slots the old bound allowed
	// and far inside the ten-minute backstop that now replaces it.
	const (
		length = 8
		stride = 16
	)
	tip := length - 1
	session := newLaggingSessionWithStride(t, length, 0, stride)
	defer session.close()

	// The producer states its requirement before the sweep: compact ancestry
	// does not reload payloads that an earlier unconstrained sweep released.
	session.walk(t, tip, 0)

	// Consensus finalizes again at the tip, having skipped everything between.
	session.sweep(session.slotOf(tip))

	session.mu.Lock()
	capped := session.capped
	session.mu.Unlock()
	if capped {
		t.Fatalf("retention gave up after %d slots holding only %d candidates: "+
			"skipped slots are being charged as production lag",
			session.slotOf(tip), length)
	}

	retained := session.candidates.cacheStats()
	if retained.Candidates != length {
		t.Fatalf("retained payloads = %d, want the %d candidates that exist", retained.Candidates, length)
	}

	// And the walk the floor exists for still costs nothing.
	candidateReads, stateLoads := session.counts()
	walk := session.walk(t, tip, 0)
	reads, loads := session.counts()
	if reads != candidateReads || loads != stateLoads {
		t.Fatalf("a window over %d skipped slots read %d candidates and %d states back from storage",
			session.slotOf(tip), reads-candidateReads, loads-stateLoads)
	}
	if walk.Visited != length {
		t.Fatalf("the walk covered %d candidates, want %d", walk.Visited, length)
	}
}

// The floor follows the producer, so a node that falls arbitrarily far behind
// would pin arbitrarily much. What stops it is the session's memory budget, and
// nothing else: once the retained payloads no longer fit, the sweeps prune
// again. Compact ancestry must continue to work without restoring the payloads
// that the budget just released.
func TestRetentionFloorResumesPruningPastTheBudget(t *testing.T) {
	for _, budget := range []struct {
		name   string
		budget retentionBudget
	}{
		{
			name:   "payloads",
			budget: retentionBudget{Bytes: 1 << 30, Payloads: 8, Duration: retentionFloorCapDuration},
		},
		{
			// Each test candidate carries a few hundred bytes of wire, block and
			// collated data, so a 2 KiB budget is a handful of them.
			name:   "bytes",
			budget: retentionBudget{Bytes: 2 << 10, Payloads: 1 << 20, Duration: retentionFloorCapDuration},
		},
	} {
		t.Run(budget.name, func(t *testing.T) {
			const length = 160
			tip := length - 1
			lag := 100
			session := newLaggingSessionForTest(t, length, tip-lag)
			defer session.close()

			session.candidates.mu.Lock()
			session.candidates.budget = budget.budget
			session.candidates.mu.Unlock()

			// State the producer's floor while its payloads are still resident,
			// then force the budget to choose which of them the sweep releases.
			session.walk(t, tip, tip-lag)
			session.sweep(uint32(tip))

			session.mu.Lock()
			capped := session.capped
			session.mu.Unlock()
			if !capped {
				t.Fatalf("a producer asking for %d candidates did not exhaust the budget", lag+1)
			}

			retained := session.candidates.cacheStats()
			if retained.Candidates >= lag+1 {
				t.Fatalf("retained payloads = %d, so pruning did not resume for a lineage of %d",
					retained.Candidates, lag+1)
			}

			// The compact identity survives pruning; opening the next window must
			// neither read nor repin the payloads that exceeded the budget.
			before, _ := session.counts()
			walk := session.walk(t, tip, tip-lag)
			after, _ := session.counts()
			if after != before {
				t.Fatalf("capped ancestry reloaded %d payloads", after-before)
			}
			if got := session.candidates.cacheStats(); got != retained {
				t.Fatalf("capped ancestry changed payload retention: before %+v, after %+v", retained, got)
			}
			if walk.Visited != lag+1 {
				t.Fatalf("the walk past the budget covered %d candidates, want %d",
					walk.Visited, lag+1)
			}
		})
	}
}

// lineageWalkRecorder captures the lineage-walk observations a session runtime
// emits. Everything else is inherited from the no-op observer, because
// ValidationObserver is a bounded-enum interface and a partial implementation
// would not compile.
type lineageWalkRecorder struct {
	selfRejectionObserver
	observations []LineageWalkObservation
}

func (o *lineageWalkRecorder) ObserveLineageWalk(observation LineageWalkObservation) {
	o.observations = append(o.observations, observation)
}

// A lineage walk that fails is the one worth observing: it is what an abstain
// is made of, and the three questions the field investigation had to answer by
// hand — how deep did it get, how many steps left memory, how long did it take
// — are exactly the ones the observation carries.
//
// The walk therefore has to report its stats on the way out of an error, and
// the runtime has to be able to observe them. Discarding them on the error
// return left the observation reachable only on success, so the instrumentation
// added for this failure mode never fired on it.
func TestLineageWalkFailureIsObservedWithItsStats(t *testing.T) {
	const length = 5

	session := newLaggingSessionForTest(t, length, length-1)
	defer session.close()

	// A finalized anchor no candidate in the walk can match. The walk descends
	// the whole chain to the genesis parent and then fails, which is the shape of
	// every "finalized lineage is ahead of this session" failure.
	foreign := ton.BlockIDExt{
		Workchain: 0,
		Shard:     math.MinInt64,
		SeqNo:     999999,
		RootHash:  bytes.Repeat([]byte{0x5e}, 32),
		FileHash:  bytes.Repeat([]byte{0x5f}, 32),
	}
	base := simplex.Parent(session.artifacts[length-1].Candidate.ID)

	got, err := session.states.ancestry(context.Background(), base, &foreign)
	if !errors.Is(err, errFinalizedLineageAhead) {
		t.Fatalf("ancestry against a foreign anchor: err = %v, want %v", err, errFinalizedLineageAhead)
	}
	if got.Visited != length {
		t.Fatalf("failed walk reported %d visited candidates, want %d", got.Visited, length)
	}
	steps := 0
	for _, count := range got.Steps {
		steps += count
	}
	if steps != got.Visited {
		t.Fatalf("failed walk step sources sum to %d, want %d", steps, got.Visited)
	}

	// And the runtime observation those stats exist for actually fires. The
	// resolvers are swapped in whole so the walk is the one measured above.
	recorder := &lineageWalkRecorder{}
	runtime, _ := prepareRuntimeTest(t, 0x72, newRuntimeTestStorage(), newRuntimeTestNetwork(), newRuntimeTestBackend())
	defer runtime.Close()
	runtime.states = session.states
	runtime.candidates = session.candidates
	runtime.metrics = recorder

	if err = runtime.verifyWindowAncestry(
		context.Background(),
		simplex.Window{StartSlot: length},
		&foreign,
	); !errors.Is(err, errFinalizedLineageAhead) {
		t.Fatalf("window lineage against a foreign anchor: err = %v, want %v", err, errFinalizedLineageAhead)
	}
	// A window with no base descends nothing and fails on the same anchor
	// mismatch, so it is the DEGENERATE case of the shape this instrumentation
	// exists for, not a different event: zero visits, failure, in whatever time
	// the attempt took. It is observed. Suppressing samples with zero visits —
	// which is what this call site used to assert — hid exactly the walks that
	// find the session finalized ahead of the base they were handed, because
	// those are the ones that never descend.
	if len(recorder.observations) != 1 {
		t.Fatalf("a window with no base observed %d walks, want 1", len(recorder.observations))
	}
	if empty := recorder.observations[0]; empty.Result != LineageWalkFailure || empty.Candidates != 0 {
		t.Fatalf("no-base walk observed as %+v, want a failure at depth 0", empty)
	}

	if err = runtime.verifyWindowAncestry(
		context.Background(),
		simplex.Window{StartSlot: length, Base: base},
		&foreign,
	); !errors.Is(err, errFinalizedLineageAhead) {
		t.Fatalf("window lineage against a foreign anchor: err = %v, want %v", err, errFinalizedLineageAhead)
	}
	if len(recorder.observations) != 2 {
		t.Fatalf("failed window lineage produced %d observations, want 2", len(recorder.observations))
	}
	observation := recorder.observations[1]
	if observation.Result != LineageWalkFailure {
		t.Fatalf("observed result = %v, want LineageWalkFailure", observation.Result)
	}
	if observation.Candidates != length {
		t.Fatalf("observed depth = %d, want %d", observation.Candidates, length)
	}
}
