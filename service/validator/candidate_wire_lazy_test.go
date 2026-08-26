package validator

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/simplex"
)

// decodeCanonical is the eager control for lazy-wire tests. Production never
// needs both a decoded artifact and its canonical wire at the same boundary.
func (c *candidateCodec) decodeCanonical(
	wire []byte,
	expected *simplex.CandidateID,
) (*CandidateArtifact, []byte, error) {
	artifact, err := c.decodeVerified(wire, expected)
	if err != nil {
		return nil, nil, err
	}

	var prepared *simplex.PreparedCandidate
	if artifact.validationRoots != nil && artifact.validationRoots.block != nil {
		var fileHash [32]byte
		copy(fileHash[:], artifact.Candidate.Block.FileHash)
		prepared, err = simplex.PrepareCandidate(
			artifact.Candidate.Block.SeqNo,
			artifact.validationRoots.block,
			artifact.validationRoots.collated,
			fileHash,
			artifact.Candidate.CollatedFileHash,
			simplex.PayloadCellHint(artifact.BlockBOC, artifact.CollatedData),
		)
		if err != nil {
			return nil, nil, err
		}
	}

	var canonical []byte
	if prepared != nil {
		canonical, err = simplex.SerializeCandidatePrepared(artifact.Candidate, prepared)
	} else {
		canonical, err = simplex.SerializeCandidate(
			artifact.Candidate,
			artifact.BlockBOC,
			artifact.CollatedData,
		)
	}
	if err != nil {
		return nil, nil, err
	}

	return artifact, canonical, nil
}

// A received candidate must not pay for its canonical wire on receipt. The
// measured cost — a combined BOC serialization plus an LZ4 pass, 3.27 s of CPU
// per minute on the testnet validator — was being paid for every candidate on
// the receive path, and nothing there read the result. The build belongs to
// the first consumer that needs the bytes.
//
// Each assertion below is paired with something that would fail if the lazy
// path were bypassed: the deferred decode must leave the wire unbuilt, and the
// bytes it later builds must be the canonical wire to the byte, under the
// digest the store is keyed by.
func TestReceivedCandidateWireIsBuiltOnFirstUse(t *testing.T) {
	config, leaderKey := runtimeTestConfig(0x71, &runtimeTestJournal{})
	codec, err := newCandidateCodec(config, CandidateLimits{
		MaxBlockBytes:        1 << 20,
		MaxCollatedDataBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	ordinary := runtimeOrdinaryArtifact(t, config, leaderKey, 0, simplex.Genesis())
	wire, err := simplex.SerializeCandidate(ordinary.Candidate, ordinary.BlockBOC, ordinary.CollatedData)
	if err != nil {
		t.Fatal(err)
	}
	// The control: what the eager path builds for the same input. The lazy
	// wire must reproduce it exactly, or the store would be keyed under a
	// digest the eager path never produced and a restart would read its own
	// index as a conflict.
	_, canonical, err := codec.decodeCanonical(wire, &ordinary.Candidate.ID)
	if err != nil {
		t.Fatal(err)
	}

	artifact, lazy, err := codec.decodeDeferred(wire, &ordinary.Candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if lazy.wire != nil || lazy.blockRoot == nil {
		t.Fatal("the deferred decode built the wire on receipt; the deferral is not happening")
	}

	storage := newRuntimeTestStorage()
	records := make(chan CandidateRecord, 1)
	storage.saveHook = func(_ SessionStorageID, record CandidateRecord, done func(error)) {
		records <- record
		done(nil)
	}
	resolver := newResolverForTest(
		storage,
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		simplex.DefaultParams(),
	)
	defer resolver.close()

	id := artifact.Candidate.ID
	if err = resolver.stageDeferred(artifact, lazy); err != nil {
		t.Fatal(err)
	}
	resolver.mu.Lock()
	entry := resolver.entries[id]
	staged := entry != nil && entry.lazyWire == lazy && entry.wire == nil && !entry.hasWireHash
	resolver.mu.Unlock()
	if !staged {
		t.Fatal("staging a deferred candidate built or hashed its wire")
	}

	// The store is the first consumer. It must hand over canonical bytes under
	// their own digest — the same pair the eager path hands over.
	if _, err = resolver.storeAsync(id, nil); err != nil {
		t.Fatal(err)
	}
	record := <-records
	if !bytes.Equal(record.Wire, canonical) {
		t.Fatal("the lazily built wire differs from the canonical wire the eager decode builds")
	}
	if record.ContentHash != sha256.Sum256(canonical) {
		t.Fatal("the store was handed a digest that is not the sha256 of the canonical wire")
	}
	resolver.mu.Lock()
	installed := entry.lazyWire == nil && bytes.Equal(entry.wire, canonical) && entry.hasWireHash &&
		entry.wireHash == sha256.Sum256(canonical)
	resolver.mu.Unlock()
	if !installed {
		t.Fatal("materializing for the store did not install the wire and its digest on the entry")
	}

	// And once built, the bytes are built exactly once: a second consumer reads
	// what the first installed.
	response, err := resolver.response(context.Background(), simplex.PeerID{}, CandidateRequest{SessionID: resolver.sessionID, ID: id, WantCandidate: true})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(response.CandidateWire, canonical) {
		t.Fatal("a peer was served bytes that are not the canonical wire")
	}
}

// A peer's request may be the first consumer, ahead of any store. The wire it
// is served must still be canonical, and serving must install it so the store
// that follows does not build it again.
func TestReceivedCandidateWireIsBuiltByTheFirstRequest(t *testing.T) {
	config, leaderKey := runtimeTestConfig(0x72, &runtimeTestJournal{})
	codec, err := newCandidateCodec(config, CandidateLimits{
		MaxBlockBytes:        1 << 20,
		MaxCollatedDataBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	ordinary := runtimeOrdinaryArtifact(t, config, leaderKey, 0, simplex.Genesis())
	wire, err := simplex.SerializeCandidate(ordinary.Candidate, ordinary.BlockBOC, ordinary.CollatedData)
	if err != nil {
		t.Fatal(err)
	}
	_, canonical, err := codec.decodeCanonical(wire, &ordinary.Candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	artifact, lazy, err := codec.decodeDeferred(wire, &ordinary.Candidate.ID)
	if err != nil {
		t.Fatal(err)
	}

	resolver := newResolverForTest(
		newRuntimeTestStorage(),
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		simplex.DefaultParams(),
	)
	defer resolver.close()
	id := artifact.Candidate.ID
	if err = resolver.stageDeferred(artifact, lazy); err != nil {
		t.Fatal(err)
	}

	response, err := resolver.response(context.Background(), simplex.PeerID{}, CandidateRequest{SessionID: resolver.sessionID, ID: id, WantCandidate: true})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(response.CandidateWire, canonical) {
		t.Fatal("the first request was served bytes that are not the canonical wire")
	}
	resolver.mu.Lock()
	entry := resolver.entries[id]
	installed := entry.lazyWire == nil && entry.hasWireHash && entry.wireHash == sha256.Sum256(canonical)
	resolver.mu.Unlock()
	if !installed {
		t.Fatal("serving a request did not install the wire it built")
	}
}

func TestDeferredStageAccountsAlreadyBuiltWire(t *testing.T) {
	resolver := newResolverForTest(
		newRuntimeTestStorage(),
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		simplex.DefaultParams(),
	)
	t.Cleanup(resolver.close)
	id := simplex.CandidateID{Slot: 2, Hash: [32]byte{0x82}}
	artifact := &CandidateArtifact{Candidate: simplex.Candidate{ID: id, Empty: true}}
	wire := []byte("prebuilt empty candidate wire")
	lazy := materializedLazyWireForTest(wire)

	if err := resolver.stageDeferred(artifact, lazy); err != nil {
		t.Fatal(err)
	}

	resolver.mu.Lock()
	entry := resolver.entries[id]
	installed := entry.candidate == artifact && entry.lazyWire == nil &&
		bytes.Equal(entry.wire, wire) && entry.hasWireHash && entry.wireHash == sha256.Sum256(wire)
	resolver.mu.Unlock()
	if !installed {
		t.Fatal("an already-built deferred wire was not installed eagerly")
	}
	projection := resolver.cacheProjection()
	if recomputed := resolver.cacheStats(); projection != recomputed {
		t.Fatalf("running cache projection %+v differs from recomputed %+v", projection, recomputed)
	}
	if projection.Candidates != 1 || projection.Bytes != int64(len(wire)) {
		t.Fatalf("cache projection = %+v, want one candidate and %d bytes", projection, len(wire))
	}
}

func TestLazyStoreClaimSurvivesFinalization(t *testing.T) {
	storage := newRuntimeTestStorage()
	resolver := newResolverForTest(
		storage,
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		simplex.DefaultParams(),
	)
	t.Cleanup(resolver.close)
	id := simplex.CandidateID{Slot: 3, Hash: [32]byte{0x83}}
	artifact := &CandidateArtifact{
		Candidate:    simplex.Candidate{ID: id},
		BlockBOC:     []byte("block"),
		CollatedData: []byte("collated"),
	}
	wire := []byte("materialized candidate wire")
	lazy, releaseMaterialize, materialized := blockingLazyWireForTest(wire)
	if err := resolver.stageDeferred(artifact, lazy); err != nil {
		t.Fatal(err)
	}

	type storeResult struct {
		flight *resolverFlight
		err    error
	}
	stored := make(chan storeResult, 1)
	go func() {
		flight, err := resolver.storeAsync(id, nil)
		stored <- storeResult{flight: flight, err: err}
	}()
	waitForStoreBuildForTest(t, resolver, id)

	resolver.notifyFinalized(id.Slot+candidateCacheRetainedSlots+1, retentionFloorNone)
	resolver.mu.Lock()
	retained := resolver.entries[id].candidate != nil
	resolver.mu.Unlock()
	if !retained {
		t.Fatal("finalization released a lazy payload already claimed by StoreCandidate")
	}

	close(releaseMaterialize)
	<-materialized
	result := <-stored
	if result.err != nil {
		t.Fatal(result.err)
	}
	if result.flight == nil {
		t.Fatal("lazy store did not create a storage flight")
	}
	<-result.flight.done
	if result.flight.err != nil {
		t.Fatal(result.flight.err)
	}
	if storage.saveCount() != 1 {
		t.Fatalf("candidate saves = %d, want one", storage.saveCount())
	}
	if projection, recomputed := resolver.cacheProjection(), resolver.cacheStats(); projection != recomputed {
		t.Fatalf("running cache projection %+v differs from recomputed %+v", projection, recomputed)
	}
}

func TestLazyStoreDoesNotSubmitAfterClose(t *testing.T) {
	storage := newRuntimeTestStorage()
	resolver := newResolverForTest(
		storage,
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		simplex.DefaultParams(),
	)
	id := simplex.CandidateID{Slot: 4, Hash: [32]byte{0x84}}
	artifact := &CandidateArtifact{
		Candidate:    simplex.Candidate{ID: id},
		BlockBOC:     []byte("block"),
		CollatedData: []byte("collated"),
	}
	lazy, releaseMaterialize, materialized := blockingLazyWireForTest([]byte("candidate wire"))
	if err := resolver.stageDeferred(artifact, lazy); err != nil {
		t.Fatal(err)
	}

	errResult := make(chan error, 1)
	go func() {
		_, err := resolver.storeAsync(id, nil)
		errResult <- err
	}()
	waitForStoreBuildForTest(t, resolver, id)
	resolver.close()
	close(releaseMaterialize)
	<-materialized
	if err := <-errResult; !errors.Is(err, ErrResolverClosed) {
		t.Fatalf("store after close error = %v, want %v", err, ErrResolverClosed)
	}
	if storage.saveCount() != 0 {
		t.Fatalf("candidate saves after close = %d, want none", storage.saveCount())
	}
	resolver.mu.Lock()
	entry := resolver.entries[id]
	unchanged := entry.storeBuilds == 0 && entry.lazyWire == lazy && entry.wire == nil && !entry.hasWireHash
	resolver.mu.Unlock()
	if !unchanged {
		t.Fatal("store materialization changed resolver state after close")
	}
}

// A duplicate received after the payload or its durable copy already won does
// not need a canonical wire at all. This is different from a released,
// non-durable entry: there the remembered hash must still be compared before a
// replacement is installed.
func TestDeferredStageDiscardsKnownCandidateWithoutBuilding(t *testing.T) {
	config, leaderKey := runtimeTestConfig(0x73, &runtimeTestJournal{})
	codec, err := newCandidateCodec(config, CandidateLimits{
		MaxBlockBytes:        1 << 20,
		MaxCollatedDataBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	ordinary := runtimeOrdinaryArtifact(t, config, leaderKey, 0, simplex.Genesis())
	wire, err := simplex.SerializeCandidate(ordinary.Candidate, ordinary.BlockBOC, ordinary.CollatedData)
	if err != nil {
		t.Fatal(err)
	}
	eager, canonical, err := codec.decodeCanonical(wire, &ordinary.Candidate.ID)
	if err != nil {
		t.Fatal(err)
	}

	resolver := newResolverForTest(
		newRuntimeTestStorage(),
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		simplex.DefaultParams(),
	)
	defer resolver.close()
	// The entry already holds the canonical bytes and their hash, as it would
	// after a store or an eager decode.
	if err = resolver.stage(eager, canonical); err != nil {
		t.Fatal(err)
	}

	artifact, lazy, err := codec.decodeDeferred(wire, &ordinary.Candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err = resolver.stageDeferred(artifact, lazy); err != nil {
		t.Fatalf("a rebroadcast of a known candidate was refused: %v", err)
	}
	if lazy.wire != nil {
		t.Fatal("admitting a rebroadcast under a known hash built its wire")
	}

	// And an entry that is durable and released admits the rebroadcast without
	// re-pinning the payload, exactly as the eager stage does.
	resolver.mu.Lock()
	entry := resolver.entries[artifact.Candidate.ID]
	resolver.releasePayloadLocked(artifact.Candidate.ID, entry)
	entry.durable = true
	resolver.mu.Unlock()
	if err = resolver.stageDeferred(artifact, lazy); err != nil {
		t.Fatal(err)
	}
	resolver.mu.Lock()
	repinned := entry.candidate != nil || entry.lazyWire != nil
	resolver.mu.Unlock()
	if repinned {
		t.Fatal("a rebroadcast re-pinned the payload of a durable, released candidate")
	}
}

func TestMergeResponseDefersMissingCandidateWire(t *testing.T) {
	config, leaderKey := runtimeTestConfig(resolverTestSessionTag, &runtimeTestJournal{})
	ordinary := runtimeOrdinaryArtifact(t, config, leaderKey, 0, simplex.Genesis())
	wire, err := simplex.SerializeCandidate(ordinary.Candidate, ordinary.BlockBOC, ordinary.CollatedData)
	if err != nil {
		t.Fatal(err)
	}
	resolver := newResolverForTest(
		newRuntimeTestStorage(),
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		2,
		simplex.DefaultParams(),
	)
	t.Cleanup(resolver.close)

	err = resolver.mergeResponse(CandidateRequest{
		SessionID:         resolver.sessionID,
		ID:                ordinary.Candidate.ID,
		WantCandidate:     true,
		MaximumReplyBytes: resolver.maxReply,
	}, CandidateResponse{CandidateWire: wire})
	if err != nil {
		t.Fatal(err)
	}

	resolver.mu.Lock()
	entry := resolver.entries[ordinary.Candidate.ID]
	deferred := entry != nil && entry.candidate != nil && entry.lazyWire != nil &&
		entry.lazyWire.wire == nil && entry.wire == nil && !entry.hasWireHash
	resolver.mu.Unlock()
	if !deferred {
		t.Fatal("mergeResponse built or hashed a missing candidate wire before a consumer asked for it")
	}
}

func TestMergeVerifiedResponseCompletesWithNotarization(t *testing.T) {
	resolver := newResolverForTest(
		newRuntimeTestStorage(),
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		2,
		simplex.DefaultParams(),
	)
	t.Cleanup(resolver.close)
	id := simplex.CandidateID{Slot: 10, Hash: [32]byte{0x90}}
	flightCtx, cancel := context.WithCancel(context.Background())
	flight := &resolverFlight{done: make(chan struct{}), cancel: cancel}
	resolver.mu.Lock()
	resolver.entry(id).resolve = flight
	resolver.mu.Unlock()
	artifact := &CandidateArtifact{Candidate: simplex.Candidate{ID: id}}
	certificate := resolverTestSeal(t, simplex.NotarizeVote(id))

	if err := resolver.mergeVerifiedResponse(
		id,
		artifact,
		failedLazyWireForTest(errors.New("completed merge materialized its wire")),
		certificate,
	); err != nil {
		t.Fatal(err)
	}
	select {
	case <-flight.done:
	default:
		t.Fatal("candidate and notarization did not complete the resolve flight")
	}
	if flight.err != nil || flight.result.Candidate != artifact || flight.result.Notarization != certificate {
		t.Fatalf("resolve flight result = %+v, error = %v", flight.result, flight.err)
	}
	select {
	case <-flightCtx.Done():
	default:
		t.Fatal("completed resolve flight context was not cancelled")
	}
}

func TestMergeVerifiedResponseRejectsClosedResolver(t *testing.T) {
	resolver := newResolverForTest(
		newRuntimeTestStorage(),
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		2,
		simplex.DefaultParams(),
	)
	resolver.close()
	id := simplex.CandidateID{Slot: 10, Hash: [32]byte{0x9f}}
	err := resolver.mergeVerifiedResponse(
		id,
		&CandidateArtifact{Candidate: simplex.Candidate{ID: id}},
		failedLazyWireForTest(errors.New("closed merge materialized its wire")),
		simplex.VerifiedCertificate{},
	)
	if !errors.Is(err, ErrResolverClosed) {
		t.Fatalf("closed merge error = %v, want %v", err, ErrResolverClosed)
	}
	resolver.mu.Lock()
	_, inserted := resolver.entries[id]
	resolver.mu.Unlock()
	if inserted {
		t.Fatal("a response arriving after close inserted a resolver entry")
	}
}

func TestLoadStoredWireRechecksHashAfterDecode(t *testing.T) {
	storage := newRuntimeTestStorage()
	seed := newResolverForTest(
		storage,
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		simplex.DefaultParams(),
	)
	artifact := stageStoredRealCandidateForTest(t, seed, 11)
	seed.close()

	id := artifact.Candidate.ID
	resolver := newRestoredResolverForTest(
		storage,
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		simplex.DefaultParams(),
		StoredSessionState{CandidateIDs: []simplex.CandidateID{id}},
	)
	t.Cleanup(resolver.close)
	decodeReachedSignature := make(chan struct{})
	resumeDecode := make(chan struct{})
	resolver.codec.schedule = &blockingLeaderSchedule{
		LeaderSchedule: resolver.codec.schedule,
		entered:        decodeReachedSignature,
		release:        resumeDecode,
	}

	loaded := make(chan error, 1)
	go func() {
		_, err := resolver.loadStoredWire(context.Background(), id)
		loaded <- err
	}()
	<-decodeReachedSignature
	remembered := sha256.Sum256([]byte("another exact candidate wire"))
	resolver.mu.Lock()
	entry := resolver.entries[id]
	entry.wireHash = remembered
	entry.hasWireHash = true
	resolver.mu.Unlock()
	close(resumeDecode)

	if err := <-loaded; !errors.Is(err, ErrCandidateConflict) {
		t.Fatalf("stored-wire race error = %v, want %v", err, ErrCandidateConflict)
	}
	resolver.mu.Lock()
	unchanged := entry.hasWireHash && entry.wireHash == remembered
	resolver.mu.Unlock()
	if !unchanged {
		t.Fatal("stored-wire decode overwrote the exact identity learned concurrently")
	}
}

func TestMergeVerifiedResponseDiscardsCandidateAndDurableRaces(t *testing.T) {
	errMaterialized := errors.New("lazy wire was materialized")
	for _, test := range []struct {
		name        string
		wantDurable bool
		setup       func(*candidateResolver, simplex.CandidateID) *CandidateArtifact
	}{
		{
			name: "candidate",
			setup: func(resolver *candidateResolver, id simplex.CandidateID) *CandidateArtifact {
				existing := &CandidateArtifact{Candidate: simplex.Candidate{ID: id}}
				wire := []byte("existing")
				resolver.mu.Lock()
				resolver.attachPayloadLocked(id, resolver.entry(id), existing, wire, sha256.Sum256(wire))
				resolver.mu.Unlock()

				return existing
			},
		},
		{
			name:        "durable",
			wantDurable: true,
			setup: func(resolver *candidateResolver, id simplex.CandidateID) *CandidateArtifact {
				resolver.mu.Lock()
				resolver.markDurableLocked(resolver.entry(id))
				resolver.mu.Unlock()

				return nil
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			resolver := newResolverForTest(
				newRuntimeTestStorage(),
				&retryCandidateProvider{called: make(chan struct{}, 1)},
				2,
				simplex.DefaultParams(),
			)
			t.Cleanup(resolver.close)
			id := simplex.CandidateID{Slot: 11, Hash: [32]byte{0x91}}
			existing := test.setup(resolver, id)
			incoming := &CandidateArtifact{Candidate: simplex.Candidate{ID: id}}
			lazy := failedLazyWireForTest(errMaterialized)

			if err := resolver.mergeVerifiedResponse(id, incoming, lazy, simplex.VerifiedCertificate{}); err != nil {
				t.Fatalf("merge over %s race: %v", test.name, err)
			}

			resolver.mu.Lock()
			entry := resolver.entries[id]
			unchanged := entry.candidate == existing && entry.lazyWire == nil && entry.durable == test.wantDurable
			resolver.mu.Unlock()
			if !unchanged {
				t.Fatalf("%s race was replaced by the peer response", test.name)
			}
		})
	}
}

func TestMergeVerifiedResponseComparesRememberedWireHash(t *testing.T) {
	for _, test := range []struct {
		name      string
		knownWire []byte
		wantErr   error
	}{
		{name: "match", knownWire: []byte("incoming")},
		{name: "conflict", knownWire: []byte("different"), wantErr: ErrCandidateConflict},
	} {
		t.Run(test.name, func(t *testing.T) {
			resolver := newResolverForTest(
				newRuntimeTestStorage(),
				&retryCandidateProvider{called: make(chan struct{}, 1)},
				2,
				simplex.DefaultParams(),
			)
			t.Cleanup(resolver.close)
			id := simplex.CandidateID{Slot: 12, Hash: [32]byte{0x92}}
			resolver.mu.Lock()
			entry := resolver.entry(id)
			entry.wireHash = sha256.Sum256(test.knownWire)
			entry.hasWireHash = true
			resolver.mu.Unlock()

			incoming := &CandidateArtifact{Candidate: simplex.Candidate{ID: id}}
			lazy := materializedLazyWireForTest([]byte("incoming"))
			err := resolver.mergeVerifiedResponse(id, incoming, lazy, simplex.VerifiedCertificate{})
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("merge error = %v, want %v", err, test.wantErr)
			}

			resolver.mu.Lock()
			entry = resolver.entries[id]
			if test.wantErr == nil {
				installed := entry.candidate == incoming && bytes.Equal(entry.wire, lazy.wire) &&
					entry.lazyWire == nil && entry.hasWireHash && entry.wireHash == lazy.hash
				resolver.mu.Unlock()
				if !installed {
					t.Fatal("matching remembered wire was not installed eagerly")
				}

				return
			}
			unchanged := entry.candidate == nil && entry.lazyWire == nil && entry.wire == nil &&
				entry.hasWireHash && entry.wireHash == sha256.Sum256(test.knownWire)
			resolver.mu.Unlock()
			if !unchanged {
				t.Fatal("conflicting wire changed the remembered entry")
			}
		})
	}
}

func TestMergeResponseRejectsDifferentValidWireForSameCandidateID(t *testing.T) {
	config, leaderKey := runtimeTestConfig(resolverTestSessionTag, &runtimeTestJournal{})
	plain := runtimeOrdinaryArtifact(t, config, leaderKey, 12, simplex.Genesis())
	plainWire, err := simplex.SerializeCandidate(plain.Candidate, plain.BlockBOC, plain.CollatedData)
	if err != nil {
		t.Fatal(err)
	}

	collatorKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0xc8}, ed25519.SeedSize))
	collatorPublic := collatorKey.Public().(ed25519.PublicKey)
	delegated := *plain
	delegated.Candidate = plain.Candidate
	delegated.Candidate.Delegation = &simplex.Delegation{CollatorKey: collatorPublic}
	delegated.Candidate.Delegation.Signature, err = simplex.SignDelegation(
		runtimeTestSigner{key: leaderKey},
		config.SessionID,
		12,
		simplex.KeyNodeIDShort(collatorPublic),
	)
	if err != nil {
		t.Fatal(err)
	}
	delegated.Candidate.Signature, err = simplex.SignCandidate(
		runtimeTestSigner{key: collatorKey},
		config.SessionID,
		delegated.Candidate.ID,
	)
	if err != nil {
		t.Fatal(err)
	}
	delegatedWire, err := simplex.SerializeCandidate(
		delegated.Candidate,
		delegated.BlockBOC,
		delegated.CollatedData,
	)
	if err != nil {
		t.Fatal(err)
	}
	if delegated.Candidate.ID != plain.Candidate.ID || bytes.Equal(delegatedWire, plainWire) {
		t.Fatal("fixture does not carry two distinct valid wires under one candidate id")
	}

	resolver := newResolverForTest(
		newRuntimeTestStorage(),
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		2,
		simplex.DefaultParams(),
	)
	t.Cleanup(resolver.close)
	id := plain.Candidate.ID
	if err = resolver.stage(plain, plainWire); err != nil {
		t.Fatal(err)
	}
	resolver.notifyFinalized(id.Slot+candidateCacheRetainedSlots+1, retentionFloorNone)

	err = resolver.mergeResponse(CandidateRequest{
		SessionID:         resolver.sessionID,
		ID:                id,
		WantCandidate:     true,
		MaximumReplyBytes: resolver.maxReply,
	}, CandidateResponse{CandidateWire: delegatedWire})
	if !errors.Is(err, ErrCandidateConflict) {
		t.Fatalf("different valid wire error = %v, want %v", err, ErrCandidateConflict)
	}
	resolver.mu.Lock()
	entry := resolver.entries[id]
	unchanged := entry.candidate == nil && entry.wire == nil && entry.lazyWire == nil &&
		entry.hasWireHash && entry.wireHash == sha256.Sum256(plainWire)
	resolver.mu.Unlock()
	if !unchanged {
		t.Fatal("different valid wire replaced the exact identity retained after release")
	}
}

func TestDeferredStageComparesRememberedWireHash(t *testing.T) {
	for _, test := range []struct {
		name      string
		knownWire []byte
		wantErr   error
	}{
		{name: "match", knownWire: []byte("incoming")},
		{name: "conflict", knownWire: []byte("different"), wantErr: ErrCandidateConflict},
	} {
		t.Run(test.name, func(t *testing.T) {
			resolver := newResolverForTest(
				newRuntimeTestStorage(),
				&retryCandidateProvider{called: make(chan struct{}, 1)},
				2,
				simplex.DefaultParams(),
			)
			t.Cleanup(resolver.close)
			id := simplex.CandidateID{Slot: 13, Hash: [32]byte{0x93}}
			resolver.mu.Lock()
			entry := resolver.entry(id)
			entry.wireHash = sha256.Sum256(test.knownWire)
			entry.hasWireHash = true
			resolver.mu.Unlock()

			incoming := &CandidateArtifact{Candidate: simplex.Candidate{ID: id}}
			lazy := materializedLazyWireForTest([]byte("incoming"))
			err := resolver.stageDeferred(incoming, lazy)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("stage error = %v, want %v", err, test.wantErr)
			}

			resolver.mu.Lock()
			entry = resolver.entries[id]
			installed := entry.candidate == incoming && bytes.Equal(entry.wire, lazy.wire) &&
				entry.lazyWire == nil && entry.hasWireHash && entry.wireHash == lazy.hash
			resolver.mu.Unlock()
			if (test.wantErr == nil) != installed {
				t.Fatalf("installed = %t with error %v", installed, err)
			}
		})
	}
}

func TestMergeVerifiedResponseConcurrentMissingCandidate(t *testing.T) {
	resolver := newResolverForTest(
		newRuntimeTestStorage(),
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		2,
		simplex.DefaultParams(),
	)
	t.Cleanup(resolver.close)
	id := simplex.CandidateID{Slot: 14, Hash: [32]byte{0x94}}
	errMaterialized := errors.New("losing lazy wire was materialized")

	const attempts = 32
	start := make(chan struct{})
	errs := make(chan error, attempts)
	var workers sync.WaitGroup
	for range attempts {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			err := resolver.mergeVerifiedResponse(
				id,
				&CandidateArtifact{Candidate: simplex.Candidate{ID: id}},
				failedLazyWireForTest(errMaterialized),
				simplex.VerifiedCertificate{},
			)
			errs <- err
		}()
	}
	close(start)
	workers.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	resolver.mu.Lock()
	entry := resolver.entries[id]
	oneCandidate := entry != nil && entry.candidate != nil && entry.lazyWire != nil &&
		entry.wire == nil && !entry.hasWireHash && resolver.cache.Candidates == 1
	resolver.mu.Unlock()
	if !oneCandidate {
		t.Fatal("concurrent responses installed or accounted more than one candidate")
	}
}

func failedLazyWireForTest(err error) *lazyCandidateWire {
	lazy := &lazyCandidateWire{}
	lazy.once.Do(func() {
		lazy.err = err
	})

	return lazy
}

func materializedLazyWireForTest(wire []byte) *lazyCandidateWire {
	lazy := &lazyCandidateWire{
		wire: bytes.Clone(wire),
		hash: sha256.Sum256(wire),
	}
	lazy.once.Do(func() {})

	return lazy
}

func blockingLazyWireForTest(wire []byte) (*lazyCandidateWire, chan<- struct{}, <-chan struct{}) {
	lazy := &lazyCandidateWire{}
	release := make(chan struct{})
	done := make(chan struct{})
	started := make(chan struct{})
	go func() {
		lazy.once.Do(func() {
			close(started)
			<-release
			lazy.wire = bytes.Clone(wire)
			lazy.hash = sha256.Sum256(wire)
		})
		close(done)
	}()
	<-started

	return lazy, release, done
}

func waitForStoreBuildForTest(t testing.TB, resolver *candidateResolver, id simplex.CandidateID) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for {
		resolver.mu.Lock()
		entry := resolver.entries[id]
		building := entry != nil && entry.storeBuilds != 0
		resolver.mu.Unlock()
		if building {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("store did not claim the lazy wire for materialization")
		}
		time.Sleep(time.Millisecond)
	}
}

type blockingLeaderSchedule struct {
	simplex.LeaderSchedule
	once    sync.Once
	entered chan<- struct{}
	release <-chan struct{}
}

func (s *blockingLeaderSchedule) ExpectedLeader(slot uint32) uint32 {
	s.once.Do(func() {
		close(s.entered)
		<-s.release
	})

	return s.LeaderSchedule.ExpectedLeader(slot)
}
