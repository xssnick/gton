package validator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
	"time"

	corestorage "github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/simplex"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type retryCandidateProvider struct {
	mu sync.Mutex

	deadlines []time.Duration
	requests  []CandidateRequest
	active    int
	maxActive int
	called    chan struct{}
	finished  chan struct{}
}

func (p *retryCandidateProvider) RequestCandidate(
	ctx context.Context,
	request CandidateRequest,
) (CandidateResponse, error) {
	deadline, _ := ctx.Deadline()
	p.mu.Lock()
	p.deadlines = append(p.deadlines, time.Until(deadline))
	p.requests = append(p.requests, request)
	p.active++
	p.maxActive = max(p.maxActive, p.active)
	p.mu.Unlock()
	p.called <- struct{}{}

	<-ctx.Done()

	p.mu.Lock()
	p.active--
	p.mu.Unlock()
	if p.finished != nil {
		p.finished <- struct{}{}
	}

	return CandidateResponse{}, ctx.Err()
}

func (p *retryCandidateProvider) snapshot() ([]time.Duration, []CandidateRequest, int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]time.Duration(nil), p.deadlines...), append([]CandidateRequest(nil), p.requests...), p.maxActive
}

// resolverTestSessionTag seeds the session id, the roster and the signing key
// every resolver these tests build shares; runtimeTestConfig derives all three
// from it.
const resolverTestSessionTag = byte(0x71)

// resolverTestSeal builds a verified certificate for that roster.
func resolverTestSeal(t testing.TB, vote simplex.Vote) simplex.VerifiedCertificate {
	t.Helper()

	config, privateKey := runtimeTestConfig(resolverTestSessionTag, &runtimeTestJournal{})

	return runtimeTestSeal(t, config, privateKey, vote)
}

func mustValidateBootstrap(state *simplex.BootstrapState) simplex.ValidatedBootstrap {
	bootstrap, err := simplex.ValidateBootstrap([32]byte{resolverTestSessionTag}, nil, state)
	if err != nil {
		panic(err)
	}

	return bootstrap
}

func newResolverForTest(
	storage ValidatorStorage,
	provider CandidateProvider,
	peerCount int,
	params simplex.Params,
) *candidateResolver {
	return newRestoredResolverForTest(storage, provider, peerCount, params, StoredSessionState{})
}

// newRestoredResolverForTest builds the resolver a restarted session builds:
// the durable candidate index is all it knows about the candidates it already
// wrote before the crash.
func newRestoredResolverForTest(
	storage ValidatorStorage,
	provider CandidateProvider,
	peerCount int,
	params simplex.Params,
	stored StoredSessionState,
) *candidateResolver {
	limits := CandidateLimits{MaxBlockBytes: 1 << 20, MaxCollatedDataBytes: 1 << 20}
	// The session id below is the one runtimeTestConfig derives from this tag, so
	// the codec verifies exactly the candidates these tests encode.
	config, _ := runtimeTestConfig(resolverTestSessionTag, &runtimeTestJournal{})
	codec, err := newCandidateCodec(config, limits)
	if err != nil {
		panic(err)
	}
	resolver, err := newCandidateResolver(candidateResolverOptions{
		Session:    SessionStorageID{},
		SessionID:  [32]byte{resolverTestSessionTag},
		Storage:    storage,
		Provider:   provider,
		Codec:      codec,
		Validators: runtimeValidators(config.Validators),
		PeerCount:  peerCount,
		Limits:     limits,
		Params:     params,
		Stored:     stored,
		Bootstrap:  mustValidateBootstrap(&simplex.BootstrapState{}),
	})
	if err != nil {
		panic(err)
	}

	return resolver
}

func TestCandidateResolverSingleflightBackoffAndCallerCancellation(t *testing.T) {
	storage := newRuntimeTestStorage()
	provider := &retryCandidateProvider{
		called:   make(chan struct{}, 8),
		finished: make(chan struct{}, 8),
	}
	params := simplex.DefaultParams()
	params.CandidateResolveTimeout = 20 * time.Millisecond
	params.CandidateResolveTimeoutMultiplier = 2
	params.CandidateResolveTimeoutCap = 60 * time.Millisecond
	params.CandidateResolveCooldown = time.Millisecond
	resolver := newResolverForTest(storage, provider, 2, params)

	ctx, cancel := context.WithCancel(context.Background())
	id := simplex.CandidateID{Slot: 9, Hash: [32]byte{0x19}}
	results := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := resolver.resolve(ctx, id)
			results <- err
		}()
	}
	for range 3 {
		select {
		case <-provider.called:
		case <-time.After(time.Second):
			t.Fatal("candidate request did not advance")
		}
	}
	cancel()
	for range 2 {
		if err := <-results; !errors.Is(err, context.Canceled) {
			t.Fatalf("resolve error = %v, want caller cancellation", err)
		}
	}
	select {
	case <-provider.finished:
	case <-time.After(time.Second):
		t.Fatal("the shared peer request outlived every caller")
	}
	waitForCandidateResolveFlight(t, resolver, id, nil)

	deadlines, requests, maxActive := provider.snapshot()
	if maxActive != 1 {
		t.Fatalf("concurrent provider calls = %d, want one shared resolve flight", maxActive)
	}
	if len(deadlines) < 3 {
		t.Fatalf("provider calls = %d, want at least 3", len(deadlines))
	}
	wants := []time.Duration{20 * time.Millisecond, 40 * time.Millisecond, 60 * time.Millisecond}
	for i, want := range wants {
		if difference := deadlines[i] - want; difference < -8*time.Millisecond || difference > 8*time.Millisecond {
			t.Fatalf("request %d timeout = %v, want %v (±8ms)", i, deadlines[i], want)
		}
		if !requests[i].WantCandidate || !requests[i].WantNotarization {
			t.Fatalf("request %d did not ask for both missing parts: %+v", i, requests[i])
		}
		if requests[i].MaximumReplyBytes != 3<<20 {
			t.Fatalf("request %d maximum reply = %d, want %d", i, requests[i].MaximumReplyBytes, 3<<20)
		}
	}
	resolver.close()
}

func TestCandidateResolverExpiresUnownedFlight(t *testing.T) {
	provider := &retryCandidateProvider{
		called:   make(chan struct{}, 1),
		finished: make(chan struct{}, 1),
	}
	params := simplex.DefaultParams()
	params.TargetRate = time.Millisecond
	params.MaxLeaderWindowDesync = 1
	params.CandidateResolveTimeout = time.Second
	params.CandidateResolveTimeoutCap = 15 * time.Millisecond
	resolver := newResolverForTest(newRuntimeTestStorage(), provider, 2, params)
	defer resolver.close()

	id := simplex.CandidateID{Slot: 17, Hash: [32]byte{0x17}}
	started := time.Now()
	_, err := resolver.resolve(context.Background(), id)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired resolve error = %v, want context deadline", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("unowned flight lived for %v", elapsed)
	}
	select {
	case <-provider.finished:
	case <-time.After(time.Second):
		t.Fatal("expired flight left its peer request running")
	}
	waitForCandidateResolveFlight(t, resolver, id, nil)
}

func TestCandidateResolverFinalizationSharesFiniteFlight(t *testing.T) {
	provider := &retryCandidateProvider{
		called:   make(chan struct{}, 2),
		finished: make(chan struct{}, 2),
	}
	params := simplex.DefaultParams()
	params.TargetRate = time.Millisecond
	params.MaxLeaderWindowDesync = 1
	params.CandidateResolveTimeout = time.Second
	params.CandidateResolveTimeoutCap = 40 * time.Millisecond
	resolver := newResolverForTest(newRuntimeTestStorage(), provider, 2, params)
	defer resolver.close()

	id := simplex.CandidateID{Slot: 23, Hash: [32]byte{0x23}}
	ownedResult := make(chan error, 1)
	go func() {
		_, err := resolver.resolveFinalization(context.Background(), id)
		ownedResult <- err
	}()
	select {
	case <-provider.called:
	case <-time.After(time.Second):
		resolver.close()
		t.Fatal("owned resolve did not start")
	}

	callerCtx, callerCancel := context.WithCancel(context.Background())
	callerResult := make(chan error, 1)
	go func() {
		_, err := resolver.resolve(callerCtx, id)
		callerResult <- err
	}()
	waitForCandidateResolveOwnership(t, resolver, id, 2, 1)
	callerCancel()
	if err := <-callerResult; !errors.Is(err, context.Canceled) {
		resolver.close()
		t.Fatalf("ordinary waiter error = %v, want context cancellation", err)
	}
	select {
	case <-provider.finished:
		t.Fatal("ordinary waiter cancellation stopped finalization-owned work")
	case <-time.After(10 * time.Millisecond):
	}

	select {
	case err := <-ownedResult:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("owned resolve epoch error = %v, want context deadline", err)
		}
	case <-time.After(time.Second):
		t.Fatal("finalization-owned flight outlived its finite epoch")
	}
	select {
	case <-provider.finished:
	case <-time.After(time.Second):
		t.Fatal("expired finalization flight left its peer request running")
	}
	waitForCandidateResolveFlight(t, resolver, id, nil)
}

func waitForCandidateResolveFlight(
	t testing.TB,
	resolver *candidateResolver,
	id simplex.CandidateID,
	want *resolverFlight,
) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for {
		resolver.mu.Lock()
		entry := resolver.entries[id]
		var got *resolverFlight
		if entry != nil {
			got = entry.resolve
		}
		resolver.mu.Unlock()
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("resolve flight = %p, want %p", got, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitForCandidateResolveOwnership(
	t testing.TB,
	resolver *candidateResolver,
	id simplex.CandidateID,
	waiters int,
	owners int,
) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for {
		resolver.mu.Lock()
		entry := resolver.entries[id]
		if entry != nil && entry.resolve != nil &&
			entry.resolve.waiters == waiters && entry.resolve.owners == owners {
			resolver.mu.Unlock()

			return
		}
		resolver.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatalf("resolve ownership did not reach waiters=%d owners=%d", waiters, owners)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCandidateResolverOneNodeRequiresLocalData(t *testing.T) {
	storage := newRuntimeTestStorage()
	provider := &retryCandidateProvider{called: make(chan struct{}, 1)}
	resolver := newResolverForTest(storage, provider, 1, simplex.DefaultParams())

	_, err := resolver.resolve(context.Background(), simplex.CandidateID{Slot: 1})
	if !errors.Is(err, ErrCandidateUnavailable) {
		t.Fatalf("resolve error = %v, want ErrCandidateUnavailable", err)
	}
	resolver.close()
	deadlines, _, _ := provider.snapshot()
	if len(deadlines) != 0 {
		t.Fatalf("one-node resolver made %d network requests", len(deadlines))
	}
}

func TestCandidateResolverHandsParsedDAGToOneValidation(t *testing.T) {
	resolver := newResolverForTest(
		newRuntimeTestStorage(),
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		simplex.DefaultParams(),
	)
	t.Cleanup(resolver.close)
	resolver.validateCandidates = true

	root := cell.BeginCell().MustStoreUInt(0xb10c, 32).EndCell()
	collated := cell.BeginCell().MustStoreUInt(0xc011, 16).EndCell()
	prepared, err := corestorage.PrepareBlockCandidate(0, -1<<63, 7, root)
	if err != nil {
		t.Fatal(err)
	}
	id := simplex.CandidateID{Slot: 7, Hash: [32]byte{0x77}}
	artifact := &CandidateArtifact{
		Candidate:     simplex.Candidate{ID: id, Block: prepared.ID()},
		BlockBOC:      prepared.BlockBOC(),
		preparedBlock: prepared,
		validationRoots: &candidateValidationRoots{
			block:    root,
			collated: []*cell.Cell{collated},
		},
	}
	if err = resolver.stage(artifact, []byte{0x77}); err != nil {
		t.Fatal(err)
	}

	resolver.mu.Lock()
	entry := resolver.entries[id]
	retained := entry.candidate
	handoff := entry.validationRoots
	resolver.mu.Unlock()
	if artifact.preparedBlock == nil {
		t.Fatal("staging consumed the caller's prepared cache artifact")
	}
	if retained == artifact || retained.preparedBlock != nil {
		t.Fatal("resolver retained the ephemeral prepared block DAG")
	}
	if retained.validationRoots != nil || handoff != artifact.validationRoots {
		t.Fatal("resolver did not separate the one-shot validation DAG from retained bytes")
	}

	const readers = 8
	results := make(chan *CandidateArtifact, readers)
	start := make(chan struct{})
	var workers sync.WaitGroup
	for range readers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			candidate, loadErr := resolver.candidate(t.Context(), id)
			if loadErr != nil {
				t.Errorf("load candidate: %v", loadErr)
				return
			}
			results <- candidate
		}()
	}
	close(start)
	workers.Wait()
	close(results)

	claimed := 0
	for candidate := range results {
		if candidate.validationRoots == nil {
			continue
		}
		claimed++
		if candidate.validationRoots.block != root || candidate.validationRoots.collated[0] != collated {
			t.Fatal("validation received cells from another parse")
		}
	}
	if claimed != 1 {
		t.Fatalf("parsed DAG was claimed %d times, want exactly once", claimed)
	}

	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	if resolver.entries[id].validationRoots != nil || resolver.entries[id].candidate.validationRoots != nil {
		t.Fatal("resolver retained parsed DAG after the first validation claimed it")
	}
}

func TestCandidateResolverObserverDropsParsedDAG(t *testing.T) {
	resolver := newResolverForTest(
		newRuntimeTestStorage(),
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		simplex.DefaultParams(),
	)
	t.Cleanup(resolver.close)

	id := simplex.CandidateID{Slot: 8, Hash: [32]byte{0x78}}
	artifact := &CandidateArtifact{
		Candidate: simplex.Candidate{ID: id},
		validationRoots: &candidateValidationRoots{
			block: cell.BeginCell().EndCell(),
		},
	}
	if err := resolver.stage(artifact, []byte{0x78}); err != nil {
		t.Fatal(err)
	}

	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	entry := resolver.entries[id]
	if entry.validationRoots != nil || entry.candidate.validationRoots != nil {
		t.Fatal("non-voting resolver retained a validation DAG")
	}
}

func TestCandidateResolverStoreSingleflight(t *testing.T) {
	storage := newRuntimeTestStorage()
	callback := make(chan func(error), 1)
	storage.saveHook = func(_ SessionStorageID, _ CandidateRecord, done func(error)) {
		callback <- done
	}
	resolver := newResolverForTest(storage, &retryCandidateProvider{called: make(chan struct{}, 1)}, 1, simplex.DefaultParams())
	id := simplex.CandidateID{Slot: 4, Hash: [32]byte{0x44}}
	if err := resolver.stage(&CandidateArtifact{Candidate: simplex.Candidate{ID: id}}, []byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}

	results := make(chan error, 2)
	for range 2 {
		go func() { results <- resolver.store(context.Background(), id) }()
	}
	var done func(error)
	select {
	case done = <-callback:
	case <-time.After(time.Second):
		t.Fatal("candidate save did not start")
	}
	if storage.saveCount() != 1 {
		t.Fatalf("candidate save calls = %d, want one", storage.saveCount())
	}
	done(nil)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	resolver.close()
}

func TestCandidateResolverCompletesFlightFromLocalParts(t *testing.T) {
	for _, candidateFirst := range []bool{true, false} {
		name := "notarization-first"
		if candidateFirst {
			name = "candidate-first"
		}
		t.Run(name, func(t *testing.T) {
			provider := &retryCandidateProvider{
				called:   make(chan struct{}, 1),
				finished: make(chan struct{}, 1),
			}
			params := simplex.DefaultParams()
			params.CandidateResolveTimeout = 5 * time.Second
			resolver := newResolverForTest(newRuntimeTestStorage(), provider, 2, params)
			defer resolver.close()

			id := simplex.CandidateID{Slot: 12, Hash: [32]byte{0x91}}
			artifact := &CandidateArtifact{Candidate: simplex.Candidate{ID: id}}
			certificate := resolverTestSeal(t, simplex.NotarizeVote(id))
			result := make(chan CandidateResolution, 1)
			errs := make(chan error, 1)
			go func() {
				resolution, err := resolver.resolve(context.Background(), id)
				result <- resolution
				errs <- err
			}()

			select {
			case <-provider.called:
			case <-time.After(time.Second):
				t.Fatal("candidate request did not start")
			}
			if candidateFirst {
				if err := resolver.stage(artifact, []byte{1}); err != nil {
					t.Fatal(err)
				}
				resolver.observeNotarization(id, certificate)
			} else {
				resolver.observeNotarization(id, certificate)
				if err := resolver.stage(artifact, []byte{1}); err != nil {
					t.Fatal(err)
				}
			}

			select {
			case resolution := <-result:
				if resolution.Candidate != artifact || resolution.Notarization != certificate {
					t.Fatal("resolver returned different locally completed parts")
				}
				if err := <-errs; err != nil {
					t.Fatal(err)
				}
			case <-time.After(200 * time.Millisecond):
				t.Fatal("resolve waiter remained blocked on provider timeout")
			}
			select {
			case <-provider.finished:
			case <-time.After(200 * time.Millisecond):
				t.Fatal("completed resolution did not cancel the active provider request")
			}
		})
	}
}

func TestCandidateResolverServeRateLimitAndUnknownIDMemory(t *testing.T) {
	storage := newRuntimeTestStorage()
	params := simplex.DefaultParams()
	params.CandidateResolveRateLimit = 2
	resolver := newResolverForTest(
		storage,
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		2,
		params,
	)
	defer resolver.close()

	source := simplex.PeerID{0x31}
	request := CandidateRequest{
		SessionID:     resolver.sessionID,
		ID:            simplex.CandidateID{Slot: 99, Hash: [32]byte{0x77}},
		WantCandidate: true,
	}
	for range 2 {
		response, err := resolver.response(context.Background(), source, request)
		if err != nil {
			t.Fatal(err)
		}
		if len(response.CandidateWire) != 0 || response.Notarization != nil {
			t.Fatal("unknown candidate returned data")
		}
	}
	if _, err := resolver.response(context.Background(), source, request); !errors.Is(err, ErrCandidateRequestRateLimited) {
		t.Fatalf("third request error = %v, want rate limit", err)
	}
	if len(resolver.entries) != 0 {
		t.Fatalf("unknown request interned %d candidate entries", len(resolver.entries))
	}
	if storage.loadCount() != 0 {
		t.Fatalf("unknown request made %d storage lookups", storage.loadCount())
	}

	otherSource := simplex.PeerID{0x32}
	if _, err := resolver.response(context.Background(), otherSource, request); err != nil {
		t.Fatalf("independent source inherited rate limit: %v", err)
	}
	params.CandidateResolveRateLimit = 1
	resolver.updateParams(params)
	if _, err := resolver.response(context.Background(), source, request); err != nil {
		t.Fatalf("rate change did not clear source windows: %v", err)
	}
	if _, err := resolver.response(context.Background(), source, request); !errors.Is(err, ErrCandidateRequestRateLimited) {
		t.Fatalf("updated rate limit error = %v", err)
	}
}

// stageStoredRealCandidateForTest stages a candidate whose wire really decodes,
// which is what any path that goes back to the durable copy needs.
func stageStoredRealCandidateForTest(
	t *testing.T,
	resolver *candidateResolver,
	slot uint32,
) *CandidateArtifact {
	t.Helper()

	config, privateKey := runtimeTestConfig(0x71, &runtimeTestJournal{})
	artifact := runtimeOrdinaryArtifact(t, config, privateKey, slot, simplex.Genesis())
	wire, _, err := resolver.codec.encodeForBroadcast(artifact)
	if err != nil {
		t.Fatal(err)
	}
	if err = resolver.stage(artifact, wire); err != nil {
		t.Fatal(err)
	}
	if err = resolver.store(context.Background(), artifact.Candidate.ID); err != nil {
		t.Fatal(err)
	}

	return artifact
}

func stageStoredCandidateForTest(
	t *testing.T,
	resolver *candidateResolver,
	slot uint32,
	empty bool,
) *CandidateArtifact {
	t.Helper()

	id := simplex.CandidateID{Slot: slot, Hash: [32]byte{byte(slot), 0xc0}}
	artifact := &CandidateArtifact{Candidate: simplex.Candidate{ID: id, Empty: empty}}
	if !empty {
		artifact.BlockBOC = make([]byte, 8)
		artifact.CollatedData = make([]byte, 4)
	}
	if err := resolver.stage(artifact, []byte{byte(slot), byte(slot >> 8), 0x01}); err != nil {
		t.Fatal(err)
	}
	if err := resolver.store(context.Background(), id); err != nil {
		t.Fatal(err)
	}

	return artifact
}

func TestCandidateResolverFinalizationReleasesStoredPayloadsBelowWatermark(t *testing.T) {
	storage := newRuntimeTestStorage()
	resolver := newResolverForTest(
		storage,
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		simplex.DefaultParams(),
	)
	defer resolver.close()

	const finalized = uint32(100)
	watermark := finalized - candidateCacheRetainedSlots
	for slot := uint32(0); slot < 50; slot++ {
		stageStoredCandidateForTest(t, resolver, slot, false)
	}
	loading := stageStoredCandidateForTest(t, resolver, 50, false)
	unstored := &CandidateArtifact{
		Candidate:    simplex.Candidate{ID: simplex.CandidateID{Slot: 51, Hash: [32]byte{51, 0xc0}}},
		BlockBOC:     make([]byte, 8),
		CollatedData: make([]byte, 4),
	}
	if err := resolver.stage(unstored, []byte{51, 0, 0x01}); err != nil {
		t.Fatal(err)
	}
	empty := stageStoredCandidateForTest(t, resolver, 52, true)
	notarized := stageStoredCandidateForTest(t, resolver, 53, false)
	certificate := resolverTestSeal(t, simplex.NotarizeVote(notarized.Candidate.ID))
	resolver.observeNotarization(notarized.Candidate.ID, certificate)
	// Inside the fixed margin by exactly one slot, derived from the margin rather
	// than written as a literal: the margin is a wall-clock bound converted at the
	// session rate, so a literal here silently stops testing the margin the day
	// the conversion changes.
	margin := stageStoredCandidateForTest(t, resolver, watermark+1, false)

	// A storage flight installs or writes out the very bytes the sweep would
	// drop, so it keeps them.
	resolver.mu.Lock()
	resolver.entries[loading.Candidate.ID].load = &resolverFlight{done: make(chan struct{})}
	resolver.mu.Unlock()

	stats := resolver.notifyFinalized(finalized, retentionFloorNone)
	if stats != (candidateCacheStats{Entries: 55, Candidates: 3, Stored: 54, Bytes: 33}) {
		t.Fatalf("cache stats after finalization = %+v", stats)
	}
	if stats != resolver.cacheStats() {
		t.Fatalf("swept projection %+v differs from a plain snapshot %+v", stats, resolver.cacheStats())
	}

	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	for slot := uint32(0); slot < 50; slot++ {
		entry := resolver.entries[simplex.CandidateID{Slot: slot, Hash: [32]byte{byte(slot), 0xc0}}]
		if entry.candidate != nil || entry.wire != nil {
			t.Fatalf("payload at slot %d below watermark %d was retained", slot, watermark)
		}
		if !entry.durable || !entry.hasWireHash {
			t.Fatalf("released entry at slot %d lost its durable identity: %+v", slot, entry)
		}
	}
	released := resolver.entries[notarized.Candidate.ID]
	if released.candidate != nil || released.notarization != certificate {
		t.Fatal("release dropped the notarization certificate of a finalized candidate")
	}
	// Nothing ever stored this one, so consensus never accepted it: below the
	// watermark it is garbage, and it goes without leaving a durable claim
	// behind that would send a later need to a file that does not exist.
	garbage := resolver.entries[unstored.Candidate.ID]
	if garbage.candidate != nil || garbage.wire != nil {
		t.Fatal("a payload with no durable copy was pinned for the life of the session")
	}
	if garbage.durable || !garbage.hasWireHash {
		t.Fatalf("releasing an unstored payload left a wrong claim behind: %+v", garbage)
	}
	for _, kept := range []*CandidateArtifact{loading, empty, margin} {
		if resolver.entries[kept.Candidate.ID].candidate != kept {
			t.Fatalf("candidate at slot %d was released", kept.Candidate.ID.Slot)
		}
	}
}

// The finalization sweep runs on the Simplex engine goroutine, under the
// resolver lock, on every finalization. Its cost has to follow what the cache
// retains and not what the session remembers, because entries are kept for the
// life of the session and their map only grows.
func TestCandidateResolverFinalizationSweepVisitsOnlyRetainedPayloads(t *testing.T) {
	resolver := newResolverForTest(
		newRuntimeTestStorage(),
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		simplex.DefaultParams(),
	)
	defer resolver.close()

	const entries = 5000
	for slot := uint32(0); slot < entries; slot++ {
		stageStoredCandidateForTest(t, resolver, slot, false)
		// Empty candidates hold no payload at all, so they must never enter the
		// queue the sweep walks.
		stageStoredCandidateForTest(t, resolver, entries+slot, true)
	}

	resolver.mu.Lock()
	queued := len(resolver.retained)
	resolver.mu.Unlock()
	if queued != entries {
		t.Fatalf("release queue holds %d entries, want the %d with a payload", queued, entries)
	}

	// Only the empty candidates are left: they carry no BOCs, so releasing one
	// would trade nothing for a storage read on every lineage step through it.
	stats := resolver.notifyFinalized(2*entries, retentionFloorNone)
	if stats.Candidates != entries {
		t.Fatalf("cache after finalization = %+v, want every payload released", stats)
	}
	if stats != resolver.cacheStats() {
		t.Fatalf("swept projection %+v differs from a plain snapshot %+v", stats, resolver.cacheStats())
	}

	// Whatever the session has accumulated, the next sweep walks the payloads
	// that arrived since the last one.
	resolver.mu.Lock()
	swept := len(resolver.retained)
	remembered := len(resolver.entries)
	resolver.mu.Unlock()
	if swept != 0 {
		t.Fatalf("release queue kept %d released entries", swept)
	}
	if remembered != 2*entries {
		t.Fatalf("resolver remembers %d entries, want %d identities kept", remembered, 2*entries)
	}

	for slot := uint32(0); slot < 3; slot++ {
		stageStoredCandidateForTest(t, resolver, 2*entries+slot, false)
	}
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	if len(resolver.retained) != 3 {
		t.Fatalf("next sweep would walk %d entries, want the 3 that hold a payload", len(resolver.retained))
	}
}

// A released candidate is stored, not lost. Reporting it unavailable would tell
// consensus a durable write failed when it provably succeeded.
func TestCandidateResolverStoreIsIdempotentAfterRelease(t *testing.T) {
	storage := newRuntimeTestStorage()
	resolver := newResolverForTest(
		storage,
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		simplex.DefaultParams(),
	)
	defer resolver.close()

	artifact := stageStoredCandidateForTest(t, resolver, 4, false)
	resolver.notifyFinalized(4+candidateCacheRetainedSlots+1, retentionFloorNone)

	if err := resolver.store(context.Background(), artifact.Candidate.ID); err != nil {
		t.Fatalf("store of a released but durable candidate: %v", err)
	}
	if storage.saveCount() != 1 {
		t.Fatalf("candidate saves = %d, want the first one only", storage.saveCount())
	}

	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	if resolver.entries[artifact.Candidate.ID].candidate != nil {
		t.Fatal("a repeated store pinned the payload the finalization sweep released")
	}
}

func TestCandidateResolverKeepsEarlySlotsWithinRetainedMargin(t *testing.T) {
	resolver := newResolverForTest(
		newRuntimeTestStorage(),
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		simplex.DefaultParams(),
	)
	defer resolver.close()

	artifact := stageStoredCandidateForTest(t, resolver, 0, false)
	// A session younger than the retained margin has no watermark to apply.
	stats := resolver.notifyFinalized(candidateCacheRetainedSlots-1, retentionFloorNone)
	if stats.Candidates != 1 {
		t.Fatalf("retained candidates = %d, want the first slots kept", stats.Candidates)
	}

	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	if resolver.entries[artifact.Candidate.ID].candidate != artifact {
		t.Fatal("the first slot of the session was released")
	}
}

func TestCandidateResolverRejectsConflictingDuplicateAfterRelease(t *testing.T) {
	resolver := newResolverForTest(
		newRuntimeTestStorage(),
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		simplex.DefaultParams(),
	)
	defer resolver.close()

	artifact := stageStoredCandidateForTest(t, resolver, 1, false)
	resolver.notifyFinalized(1+candidateCacheRetainedSlots+1, retentionFloorNone)

	if err := resolver.stage(artifact, []byte{0x9f}); !errors.Is(err, ErrCandidateConflict) {
		t.Fatalf("conflicting duplicate after release: err = %v, want %v", err, ErrCandidateConflict)
	}
	if err := resolver.stage(artifact, []byte{1, 0, 0x01}); err != nil {
		t.Fatalf("identical rebroadcast after release: %v", err)
	}

	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	if resolver.entries[artifact.Candidate.ID].candidate != nil {
		t.Fatal("a rebroadcast pinned the payload the finalization sweep released")
	}
}

func TestCandidateResolverServesReleasedCandidateWithoutRetaining(t *testing.T) {
	storage := newRuntimeTestStorage()
	resolver := newResolverForTest(
		storage,
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		simplex.DefaultParams(),
	)
	defer resolver.close()

	artifact := stageStoredCandidateForTest(t, resolver, 2, false)
	wire := []byte{2, 0, 0x01}
	resolver.notifyFinalized(2+candidateCacheRetainedSlots+1, retentionFloorNone)

	request := CandidateRequest{
		SessionID:     resolver.sessionID,
		ID:            artifact.Candidate.ID,
		WantCandidate: true,
	}
	source := simplex.PeerID{0x41}
	for served := 1; served <= 2; served++ {
		response, err := resolver.response(context.Background(), source, request)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(response.CandidateWire, wire) {
			t.Fatalf("served wire = %x, want the stored %x", response.CandidateWire, wire)
		}
		if storage.loadCount() != served {
			t.Fatalf("storage reads after %d serves = %d", served, storage.loadCount())
		}
		resolver.mu.Lock()
		retained := resolver.entries[artifact.Candidate.ID].candidate
		resolver.mu.Unlock()
		if retained != nil {
			t.Fatal("serving a released candidate rebuilt the cache the sweep dropped")
		}
	}

	// The durable copy for this id is replaced the only way a content-addressed
	// store can produce one: a different wire under a different identity. The
	// bytes are never re-hashed here, so this identity is what has to catch it.
	storage.mu.Lock()
	replacement := []byte{0x77}
	storage.candidates[validatorTestNamespaceOf(resolver.session)][artifact.Candidate.ID] = CandidateRecord{
		ID:          artifact.Candidate.ID,
		Wire:        replacement,
		ContentHash: sha256.Sum256(replacement),
	}
	storage.mu.Unlock()
	if _, err := resolver.response(
		context.Background(),
		source,
		request,
	); !errors.Is(err, ErrCandidateConflict) {
		t.Fatalf("serving a rewritten durable candidate: err = %v, want %v", err, ErrCandidateConflict)
	}
}

// The identity a loaded candidate is checked against is the one the store
// indexes it under, not one the resolver re-derives from the megabytes it just
// read. A durable copy the store no longer indexes under the identity this
// session staged is a conflict on both the serving and the loading path.
func TestCandidateResolverChecksTheStoredCandidateIdentity(t *testing.T) {
	storage := newRuntimeTestStorage()
	resolver := newResolverForTest(
		storage,
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		simplex.DefaultParams(),
	)
	defer resolver.close()

	artifact := stageStoredRealCandidateForTest(t, resolver, 5)
	id := artifact.Candidate.ID
	resolver.notifyFinalized(5+candidateCacheRetainedSlots+1, retentionFloorNone)

	storage.mu.Lock()
	record := storage.candidates[validatorTestNamespaceOf(resolver.session)][id]
	record.ContentHash[0] ^= 0xff
	storage.candidates[validatorTestNamespaceOf(resolver.session)][id] = record
	storage.mu.Unlock()

	if _, err := resolver.response(context.Background(), simplex.PeerID{0x51}, CandidateRequest{
		SessionID:     resolver.sessionID,
		ID:            id,
		WantCandidate: true,
	}); !errors.Is(err, ErrCandidateConflict) {
		t.Fatalf("serving a candidate stored under another identity: err = %v, want %v", err, ErrCandidateConflict)
	}
	if _, err := resolver.loadCandidate(context.Background(), id); !errors.Is(err, ErrCandidateConflict) {
		t.Fatalf("loading a candidate stored under another identity: err = %v, want %v", err, ErrCandidateConflict)
	}
}

// A peer sweeping old slots is not one peer: the rate limit bounds a source,
// while the overlay behind it is unbounded. Every request for the same released
// candidate must therefore share one storage read, and none of them may put the
// payload back into the cache the finalization sweep dropped.
func TestCandidateResolverCoalescesConcurrentServesOfOneReleasedCandidate(t *testing.T) {
	const peers = 256

	storage := newRuntimeTestStorage()
	release := make(chan struct{})
	var hookMu sync.Mutex
	reading, concurrent := 0, 0
	storage.loadHook = func() {
		hookMu.Lock()
		reading++
		concurrent = max(concurrent, reading)
		hookMu.Unlock()

		<-release

		hookMu.Lock()
		reading--
		hookMu.Unlock()
	}
	params := simplex.DefaultParams()
	params.CandidateResolveRateLimit = 1
	resolver := newResolverForTest(
		storage,
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		params,
	)
	defer resolver.close()

	artifact := stageStoredCandidateForTest(t, resolver, 6, false)
	wire := []byte{6, 0, 0x01}
	resolver.notifyFinalized(6+candidateCacheRetainedSlots+1, retentionFloorNone)

	responses := make(chan CandidateResponse, peers)
	errs := make(chan error, peers)
	for peer := range peers {
		go func() {
			response, err := resolver.response(
				context.Background(),
				simplex.PeerID{byte(peer), byte(peer >> 8)},
				CandidateRequest{
					SessionID:     resolver.sessionID,
					ID:            artifact.Candidate.ID,
					WantCandidate: true,
				},
			)
			responses <- response
			errs <- err
		}()
	}
	// Every request has passed the per-source rate window, so all of them are
	// inside the resolver while its only storage read is held open below.
	deadline := time.Now().Add(10 * time.Second)
	for {
		resolver.mu.Lock()
		admitted := len(resolver.requests)
		resolver.mu.Unlock()
		if admitted == peers {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d requests reached the resolver", admitted, peers)
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	close(release)

	for range peers {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
		response := <-responses
		if !bytes.Equal(response.CandidateWire, wire) {
			t.Fatalf("served wire = %x, want the stored %x", response.CandidateWire, wire)
		}
	}
	hookMu.Lock()
	overlapping := concurrent
	hookMu.Unlock()
	if overlapping != 1 {
		t.Fatalf("%d storage reads of one candidate overlapped, want one shared read", overlapping)
	}
	if storage.loadCount() != 1 {
		t.Fatalf("%d concurrent serves made %d storage reads, want one", peers, storage.loadCount())
	}

	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	entry := resolver.entries[artifact.Candidate.ID]
	if entry.candidate != nil || entry.wire != nil || entry.serve != nil {
		t.Fatal("a coalesced serve retained the payload the finalization sweep dropped")
	}
}

// A resolve waiter must survive the finalization sweep landing between its
// flight completing and its own return. The entry below is exactly what such a
// waiter observes when the sweep wins that race.
func TestCandidateResolverResolveSurvivesReleaseAfterFlight(t *testing.T) {
	resolver := newResolverForTest(
		newRuntimeTestStorage(),
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		simplex.DefaultParams(),
	)
	defer resolver.close()

	artifact := stageStoredCandidateForTest(t, resolver, 3, false)
	id := artifact.Candidate.ID
	certificate := resolverTestSeal(t, simplex.NotarizeVote(id))
	flight := &resolverFlight{done: make(chan struct{}), cancel: func() {}}

	resolver.mu.Lock()
	entry := resolver.entries[id]
	entry.notarization = certificate
	entry.resolve = flight
	resolver.completeResolveLocked(id, entry)
	entry.resolve = flight
	resolver.releasePayloadLocked(id, entry)
	resolver.mu.Unlock()

	resolution, err := resolver.resolve(context.Background(), id)
	if err != nil {
		t.Fatalf("resolve after the sweep released the payload: %v", err)
	}
	if resolution.Candidate != artifact || resolution.Notarization != certificate {
		t.Fatal("resolve returned a resolution the completed flight did not produce")
	}
}

// A resolve that has the candidate and only lacks its notarization certificate
// does not claim the payload. The certificate is a separate field, so the
// finalization sweep must still be able to release the bytes, and the flight
// must still complete from the durable copy afterwards.
func TestCandidateResolverReleasesPayloadUnderAResolveWaitingForItsCertificate(t *testing.T) {
	storage := newRuntimeTestStorage()
	provider := &retryCandidateProvider{
		called:   make(chan struct{}, 8),
		finished: make(chan struct{}, 8),
	}
	params := simplex.DefaultParams()
	// Long enough that completing within the test can only come from the local
	// certificate, never from this request expiring.
	params.CandidateResolveTimeout = time.Minute
	params.CandidateResolveTimeoutCap = time.Minute
	params.CandidateResolveCooldown = time.Millisecond
	resolver := newResolverForTest(storage, provider, 2, params)
	defer resolver.close()

	// A real wire: completing this resolve after the release goes through the
	// stored copy, which is decoded exactly as any other loaded candidate is.
	artifact := stageStoredRealCandidateForTest(t, resolver, 7)
	id := artifact.Candidate.ID
	resolved := make(chan CandidateResolution, 1)
	errs := make(chan error, 1)
	go func() {
		resolution, err := resolver.resolve(context.Background(), id)
		resolved <- resolution
		errs <- err
	}()
	select {
	case <-provider.called:
	case <-time.After(time.Second):
		t.Fatal("resolve did not start asking peers for the certificate")
	}
	_, requests, _ := provider.snapshot()
	if requests[0].WantCandidate {
		t.Fatal("resolve asked a peer for a candidate it holds locally")
	}

	stats := resolver.notifyFinalized(7+candidateCacheRetainedSlots+1, retentionFloorNone)
	if stats.Candidates != 0 || stats.Bytes != 0 {
		t.Fatalf("cache after finalization = %+v, want the payload released", stats)
	}
	if stats != resolver.cacheStats() {
		t.Fatalf("swept projection %+v differs from a plain snapshot %+v", stats, resolver.cacheStats())
	}
	resolver.mu.Lock()
	entry := resolver.entries[id]
	released := entry.candidate == nil && entry.wire == nil
	durable := entry.durable && entry.hasWireHash
	resolver.mu.Unlock()
	if !released {
		t.Fatal("a resolve waiting for a certificate kept a finalized payload pinned")
	}
	if !durable {
		t.Fatal("the released entry lost the durable identity it is recoverable by")
	}

	certificate := resolverTestSeal(t, simplex.NotarizeVote(id))
	resolver.observeNotarization(id, certificate)
	select {
	case <-provider.finished:
	case <-time.After(time.Second):
		t.Fatal("the local certificate did not end the outstanding peer request")
	}
	select {
	case resolution := <-resolved:
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
		if resolution.Notarization != certificate {
			t.Fatal("resolve returned another certificate")
		}
		if resolution.Candidate == nil ||
			resolution.Candidate.Candidate.ID != id {
			t.Fatal("resolve did not complete from the durable copy of the released payload")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("resolve did not complete after its certificate arrived")
	}
}

func TestCandidateRequestWindowIsSliding(t *testing.T) {
	var window candidateRequestWindow
	now := time.Unix(100, 0)
	if !window.allow(now, 1) {
		t.Fatal("first request was rejected")
	}
	if window.allow(now.Add(time.Second), 1) {
		t.Fatal("sample at the exact one-second boundary expired early")
	}
	if !window.allow(now.Add(time.Second+time.Nanosecond), 1) {
		t.Fatal("sample older than the one-second window did not expire")
	}
}

// wireCandidateProvider answers every request from one fixed wire, which is
// what a peer that still holds the candidate does.
type wireCandidateProvider struct {
	mu sync.Mutex

	wire     []byte
	requests []CandidateRequest
}

func (p *wireCandidateProvider) RequestCandidate(
	_ context.Context,
	request CandidateRequest,
) (CandidateResponse, error) {
	p.mu.Lock()
	p.requests = append(p.requests, request)
	p.mu.Unlock()
	if !request.WantCandidate {
		return CandidateResponse{}, nil
	}

	return CandidateResponse{CandidateWire: bytes.Clone(p.wire)}, nil
}

func (p *wireCandidateProvider) snapshot() []CandidateRequest {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]CandidateRequest(nil), p.requests...)
}

// A restarted session knows the ids of the candidates it wrote before the crash
// and nothing else about them. Every path that asks whether a candidate is
// durable has to read the claim that restore installs: Simplex turns a failed
// store into a fatal session abort, so reporting a candidate unavailable
// because this process has not written it itself kills the session.
func TestCandidateResolverStoresCandidateRestoredFromTheDurableIndex(t *testing.T) {
	storage := newRuntimeTestStorage()
	seed := newResolverForTest(
		storage,
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		simplex.DefaultParams(),
	)
	artifact := stageStoredRealCandidateForTest(t, seed, 3)
	wire, _, err := seed.codec.encodeForBroadcast(artifact)
	if err != nil {
		t.Fatal(err)
	}
	seed.close()

	id := artifact.Candidate.ID
	resolver := newRestoredResolverForTest(
		storage,
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		simplex.DefaultParams(),
		StoredSessionState{CandidateIDs: []simplex.CandidateID{id}},
	)
	defer resolver.close()

	// Answering one peer is all it takes for the restart stub to learn the wire
	// identity of a candidate whose payload this process never held.
	if _, err = resolver.response(context.Background(), simplex.PeerID{0x61}, CandidateRequest{
		SessionID:     resolver.sessionID,
		ID:            id,
		WantCandidate: true,
	}); err != nil {
		t.Fatalf("serving a restored candidate: %v", err)
	}

	if err = resolver.stage(artifact, wire); err != nil {
		t.Fatalf("rebroadcast of a restored candidate: %v", err)
	}
	if err = resolver.store(context.Background(), id); err != nil {
		t.Fatalf("store of a candidate restored from the durable index: %v", err)
	}
	if storage.saveCount() != 1 {
		t.Fatalf("candidate saves = %d, want the one the seeding session made", storage.saveCount())
	}
}

// A candidate fetched from a peer for a consensus that never accepted it is
// garbage: nothing stores it, so nothing can ever release it either, and its
// payload stays pinned for the life of the session. Below the watermark it must
// go — and because it leaves no durable copy behind, a later need for it has to
// go back to the peers rather than read a file that does not exist.
func TestCandidateResolverReleasesPeerFetchedPayloadWithNoDurableCopy(t *testing.T) {
	config, privateKey := runtimeTestConfig(0x71, &runtimeTestJournal{})
	storage := newRuntimeTestStorage()
	params := simplex.DefaultParams()
	params.CandidateResolveCooldown = time.Millisecond
	seed := newResolverForTest(storage, &retryCandidateProvider{called: make(chan struct{}, 1)}, 1, params)
	artifact := runtimeOrdinaryArtifact(t, config, privateKey, 8, simplex.Genesis())
	wire, _, err := seed.codec.encodeForBroadcast(artifact)
	if err != nil {
		t.Fatal(err)
	}
	seed.close()

	provider := &wireCandidateProvider{wire: wire}
	resolver := newResolverForTest(storage, provider, 2, params)
	defer resolver.close()

	id := artifact.Candidate.ID
	certificate := resolverTestSeal(t, simplex.NotarizeVote(id))
	resolver.observeNotarization(id, certificate)
	if err = resolver.mergeResponse(CandidateRequest{
		SessionID:         resolver.sessionID,
		ID:                id,
		WantCandidate:     true,
		MaximumReplyBytes: resolver.maxReply,
	}, CandidateResponse{CandidateWire: bytes.Clone(wire)}); err != nil {
		t.Fatal(err)
	}

	stats := resolver.notifyFinalized(8+candidateCacheRetainedSlots+1, retentionFloorNone)
	if stats.Candidates != 0 || stats.Bytes != 0 {
		t.Fatalf("cache after finalization = %+v, want the unstored payload released", stats)
	}
	if stats != resolver.cacheStats() {
		t.Fatalf("swept projection %+v differs from a plain snapshot %+v", stats, resolver.cacheStats())
	}

	resolver.mu.Lock()
	entry := resolver.entries[id]
	released := entry.candidate == nil && entry.wire == nil && entry.lazyWire == nil
	deferredIdentity := !entry.hasWireHash && entry.notarization == certificate
	queued := len(resolver.retained)
	resolver.mu.Unlock()
	if !released {
		t.Fatal("a payload consensus never accepted stayed pinned below the watermark")
	}
	if !deferredIdentity {
		t.Fatal("an unconsumed peer response materialized an exact wire identity before release")
	}
	if queued != 0 {
		t.Fatalf("release queue holds %d entries after releasing everything in it", queued)
	}

	// Nothing durable was left behind, so the bytes come back from a peer.
	resolution, err := resolver.resolve(context.Background(), id)
	if err != nil {
		t.Fatalf("resolve after releasing an unstored payload: %v", err)
	}
	if resolution.Candidate == nil || resolution.Candidate.Candidate.ID != id {
		t.Fatal("resolve did not recover the released payload")
	}
	if storage.loadCount() != 0 {
		t.Fatalf("%d storage reads for a candidate that was never stored", storage.loadCount())
	}
	requests := provider.snapshot()
	if len(requests) == 0 || !requests[0].WantCandidate {
		t.Fatalf("peer requests = %+v, want the candidate asked of a peer", requests)
	}
}

// A durable copy that cannot be read is not a durable copy. The claim has to
// fall away with the failed read so the peer path — whose own comment says it
// is there for exactly this — is reachable, instead of turning one unreadable
// file into a permanent resolve failure while peers serve the right bytes.
func TestCandidateResolverFallsBackToPeersWhenTheDurableCopyIsCorrupt(t *testing.T) {
	config, privateKey := runtimeTestConfig(0x71, &runtimeTestJournal{})
	storage := newRuntimeTestStorage()
	params := simplex.DefaultParams()
	params.CandidateResolveCooldown = time.Millisecond
	seed := newResolverForTest(storage, &retryCandidateProvider{called: make(chan struct{}, 1)}, 1, params)
	artifact := runtimeOrdinaryArtifact(t, config, privateKey, 9, simplex.Genesis())
	wire, _, err := seed.codec.encodeForBroadcast(artifact)
	if err != nil {
		t.Fatal(err)
	}
	seed.close()

	id := artifact.Candidate.ID
	corrupt := []byte{0x00, 0x01, 0x02, 0x03}
	storage.mu.Lock()
	storage.candidates[validatorTestNamespaceOf(SessionStorageID{})] = map[simplex.CandidateID]CandidateRecord{
		id: {ID: id, Wire: corrupt, ContentHash: sha256.Sum256(corrupt)},
	}
	storage.mu.Unlock()

	provider := &wireCandidateProvider{wire: wire}
	resolver := newRestoredResolverForTest(
		storage,
		provider,
		2,
		params,
		StoredSessionState{CandidateIDs: []simplex.CandidateID{id}},
	)
	defer resolver.close()
	resolver.observeNotarization(id, resolverTestSeal(t, simplex.NotarizeVote(id)))

	resolution, err := resolver.resolve(context.Background(), id)
	if err != nil {
		t.Fatalf("resolve over an unreadable durable copy: %v", err)
	}
	if resolution.Candidate == nil || resolution.Candidate.Candidate.ID != id {
		t.Fatal("resolve did not recover the candidate from a peer")
	}
	requests := provider.snapshot()
	if len(requests) == 0 || !requests[0].WantCandidate {
		t.Fatalf("peer requests = %+v, want the candidate asked of a peer", requests)
	}
	if storage.loadCount() != 1 {
		t.Fatalf("storage reads = %d, want the one failed read of the corrupt copy", storage.loadCount())
	}
}

// TestCandidateResolverStoreAsyncSharesOneWrite pins the split the producer
// depends on: submitting the write and joining it are two separate acts, one
// record reaches storage, and every party learns the same outcome — the joining
// waiter through the flight, the detached submitter through its notifier.
func TestCandidateResolverStoreAsyncSharesOneWrite(t *testing.T) {
	storage := newRuntimeTestStorage()
	callback := make(chan func(error), 1)
	storage.saveHook = func(_ SessionStorageID, _ CandidateRecord, done func(error)) {
		callback <- done
	}
	resolver := newResolverForTest(storage, &retryCandidateProvider{called: make(chan struct{}, 1)}, 1, simplex.DefaultParams())
	defer resolver.close()

	id := simplex.CandidateID{Slot: 6, Hash: [32]byte{0x66}}
	if err := resolver.stage(&CandidateArtifact{Candidate: simplex.Candidate{ID: id}}, []byte{7, 8, 9}); err != nil {
		t.Fatal(err)
	}

	notified := make(chan error, 1)
	flight, err := resolver.storeAsync(id, func(storeErr error) { notified <- storeErr })
	if err != nil {
		t.Fatalf("storeAsync: %v", err)
	}
	if flight == nil {
		t.Fatal("storeAsync returned no flight for a candidate that is not durable")
	}
	// The submission is synchronous: the record is already with storage before
	// storeAsync returns, on the caller's goroutine.
	var done func(error)
	select {
	case done = <-callback:
	default:
		t.Fatal("storeAsync did not submit the write before returning")
	}
	select {
	case <-flight.done:
		t.Fatal("storeAsync waited for the write to complete")
	default:
	}

	// The voter's own StoreCandidate hook arrives while the producer's write is
	// still outstanding. It must join that write, not start a second one.
	joinedNotify := make(chan error, 1)
	joined, err := resolver.storeAsync(id, func(storeErr error) { joinedNotify <- storeErr })
	if err != nil {
		t.Fatalf("second storeAsync: %v", err)
	}
	if joined != flight {
		t.Fatal("the second submitter started a separate write")
	}
	if storage.saveCount() != 1 {
		t.Fatalf("candidate save calls = %d, want one", storage.saveCount())
	}

	failure := errors.New("disk is gone")
	done(failure)
	if storage.saveCount() != 1 {
		t.Fatalf("candidate save calls after completion = %d, want one", storage.saveCount())
	}
	for _, notifications := range []chan error{notified, joinedNotify} {
		select {
		case got := <-notifications:
			if !errors.Is(got, failure) {
				t.Fatalf("notified error = %v, want %v", got, failure)
			}
		case <-time.After(time.Second):
			t.Fatal("a submitter of the shared write was never notified")
		}
	}
	if flight.err == nil || !errors.Is(flight.err, failure) {
		t.Fatalf("flight error = %v, want %v", flight.err, failure)
	}
}

// TestCandidateResolverStoreAsyncSkipsDurableCandidate keeps the durable
// short-circuit reporting success rather than a missing flight the caller would
// have to interpret.
func TestCandidateResolverStoreAsyncSkipsDurableCandidate(t *testing.T) {
	storage := newRuntimeTestStorage()
	id := simplex.CandidateID{Slot: 3, Hash: [32]byte{0x33}}
	resolver := newRestoredResolverForTest(
		storage,
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		simplex.DefaultParams(),
		StoredSessionState{CandidateIDs: []simplex.CandidateID{id}},
	)
	defer resolver.close()

	notified := make(chan error, 1)
	flight, err := resolver.storeAsync(id, func(storeErr error) { notified <- storeErr })
	if err != nil {
		t.Fatalf("storeAsync on a durable candidate: %v", err)
	}
	if flight != nil {
		t.Fatal("a durable candidate started a second write")
	}
	select {
	case got := <-notified:
		if got != nil {
			t.Fatalf("durable notification = %v, want nil", got)
		}
	default:
		t.Fatal("the durable short-circuit did not notify")
	}
	if storage.saveCount() != 0 {
		t.Fatalf("candidate save calls = %d, want none", storage.saveCount())
	}
}
