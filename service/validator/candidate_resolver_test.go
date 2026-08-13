package validator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/simplex"
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

func mustValidateBootstrap(state *simplex.BootstrapState) simplex.ValidatedBootstrap {
	bootstrap, err := simplex.ValidateBootstrap([32]byte{0x71}, nil, state)
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
	resolver, err := newCandidateResolver(candidateResolverOptions{
		Session:   SessionStorageID{},
		SessionID: [32]byte{0x71},
		Storage:   storage,
		Provider:  provider,
		PeerCount: peerCount,
		Limits:    CandidateLimits{MaxBlockBytes: 1 << 20, MaxCollatedDataBytes: 1 << 20},
		Params:    params,
		Stored:    StoredSessionState{},
		Bootstrap: mustValidateBootstrap(&simplex.BootstrapState{}),
	})
	if err != nil {
		panic(err)
	}

	return resolver
}

func TestCandidateResolverSingleflightBackoffAndCallerCancellation(t *testing.T) {
	storage := newRuntimeTestStorage()
	provider := &retryCandidateProvider{called: make(chan struct{}, 8)}
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
	resolver.close()

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
			certificate := &simplex.Certificate{Vote: simplex.NotarizeVote(id)}
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
