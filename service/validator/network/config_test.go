package network

import (
	"context"
	"crypto/ed25519"
	"testing"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/validator"
	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

func TestValidatorSessionSpecBindsV3TransportAndCandidateSources(t *testing.T) {
	privateKey := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	var publicKey [ed25519.PublicKeySize]byte
	copy(publicKey[:], privateKey.Public().(ed25519.PublicKey))
	keyID, err := groups.PublicKeyHash(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	localADNL := [32]byte{0x51}
	observerADNL := [32]byte{0x52}
	collatorADNL := [32]byte{0x53}
	manager := &Manager{localADNLID: localADNL}
	spec, err := manager.validatorSessionSpec(validator.SessionConfig{
		SessionID:        [32]byte{1},
		CatchainSeqno:    2,
		ValidatorSetHash: 3,
		Validators: []groups.Validator{{
			PublicKey: publicKey, PublicKeyHash: keyID, ADNL: localADNL, Weight: 1,
		}},
		OverlayMembers: [][32]byte{localADNL, observerADNL},
		CollatorsByValidator: []groups.CollatorRegistryEntry{{
			ValidatorKeyID: keyID, CollatorADNLIDs: [][32]byte{collatorADNL},
		}},
		Protocol: validator.SessionProtocol{
			ProtocolVersion: 3,
			UseQUIC:         false,
		},
		CandidateLimits: validator.CandidateLimits{
			MaxBlockBytes: 100, MaxCollatedDataBytes: 200,
		},
		Identity: validator.SessionIdentity{
			ADNLID: localADNL,
			Validator: &validator.ValidatorIdentity{
				Index: 0, KeyID: keyID, Signer: testOverlaySigner{key: privateKey},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	validatorSource := p2p.PeerID(keyID)
	collatorSource := p2p.PeerID(collatorADNL)
	if len(spec.authorized) != 2 || spec.authorized[validatorSource] == 0 ||
		spec.authorized[collatorSource] == 0 {
		t.Fatalf("authorized candidate sources = %#v", spec.authorized)
	}
	if _, observerAuthorized := spec.authorized[p2p.PeerID(observerADNL)]; observerAuthorized {
		t.Fatal("wider overlay observer was authorized to originate candidates")
	}
	if spec.candidateADNL[validatorSource] != p2p.PeerID(localADNL) ||
		spec.candidateADNL[collatorSource] != collatorSource {
		t.Fatalf("candidate source ADNL binding = %#v", spec.candidateADNL)
	}
	overlayConfig := spec.overlayConfig()
	if !overlayConfig.UseQUIC || !overlayConfig.EnableTwoStep || overlayConfig.AllowLegacyBroadcasts {
		t.Fatalf("private overlay v3 policy = %#v", overlayConfig)
	}
}

func TestSessionSpecsRejectUnknownAndLegacyProtocols(t *testing.T) {
	manager := &Manager{}
	for _, version := range []uint8{1, 2, 4} {
		_, err := manager.validatorSessionSpec(validator.SessionConfig{
			Protocol: validator.SessionProtocol{ProtocolVersion: version},
		})
		if err == nil {
			t.Fatalf("validator protocol version %d was accepted", version)
		}
	}
	// The observer half needs a manager whose ADNL is actually in the overlay,
	// otherwise every descriptor is rejected by a later check and the version
	// gate is never reached.
	observerManager := &Manager{localADNLID: observerFixtureLocalADNL}
	for _, version := range []uint8{1, 2, 4} {
		if _, err := observerManager.observerSessionSpec(observerSessionFixture(version)); err == nil {
			t.Fatalf("observer protocol version %d was accepted", version)
		}
	}
	// The control: the same descriptor with the one supported version must be
	// accepted. Without it the observer half proves nothing — a descriptor that
	// trips a later check is rejected for every version, including the good one,
	// and the version gate could be deleted outright without failing anything.
	if _, err := observerManager.observerSessionSpec(observerSessionFixture(3)); err != nil {
		t.Fatalf("observer protocol version 3 was rejected: %v", err)
	}
}

var observerFixtureLocalADNL = [32]byte{0x72}

// observerSessionFixture is a descriptor that is valid in every respect except
// the protocol version under test, so a rejection is attributable to the
// version and nothing else.
func observerSessionFixture(version uint8) collator.OverlaySession {
	remoteKey, remoteKeyID := observerFixtureKey(0x71)
	return collator.OverlaySession{
		Session: collator.Session{
			ID:               [32]byte{1},
			CatchainSeqno:    2,
			ValidatorSetHash: 3,
			ProtocolVersion:  version,
			Validators: []collator.SessionValidator{{
				PublicKey: remoteKey,
				Weight:    1,
			}},
		},
		Role:                      collator.OverlayRoleObserver,
		AllOverlayNodes:           [][32]byte{observerFixtureLocalADNL, remoteKeyID},
		MaxBlockSize:              100,
		MaxCollatedDataSize:       200,
		ObserversInPrivateOverlay: true,
	}
}

func TestValidatorSessionSpecUsesSigningKeyADNLFallback(t *testing.T) {
	localKey, localKeyID := testValidatorKey(t, 0x61)
	remoteKey, remoteKeyID := testValidatorKey(t, 0x62)
	manager := &Manager{localADNLID: localKeyID}

	spec, err := manager.validatorSessionSpec(validator.SessionConfig{
		SessionID:        [32]byte{1},
		CatchainSeqno:    2,
		ValidatorSetHash: 3,
		Validators: []groups.Validator{
			{PublicKey: localKey, PublicKeyHash: localKeyID, Weight: 1},
			{PublicKey: remoteKey, PublicKeyHash: remoteKeyID, Weight: 1},
		},
		OverlayMembers: [][32]byte{localKeyID, remoteKeyID},
		Protocol:       validator.SessionProtocol{ProtocolVersion: 3},
		CandidateLimits: validator.CandidateLimits{
			MaxBlockBytes: 100, MaxCollatedDataBytes: 200,
		},
		Identity: validator.SessionIdentity{
			ADNLID: localKeyID,
			Validator: &validator.ValidatorIdentity{
				Index: 0, KeyID: localKeyID, Signer: testOverlaySigner{
					key: ed25519.NewKeyFromSeed(bytesOf(0x61, ed25519.SeedSize)),
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if spec.validatorByADNL[p2p.PeerID(localKeyID)] != 0 ||
		spec.validatorByADNL[p2p.PeerID(remoteKeyID)] != 1 {
		t.Fatalf("validator ADNL fallback map = %#v", spec.validatorByADNL)
	}
	if len(spec.peers) != 1 || spec.peers[0] != p2p.PeerID(remoteKeyID) {
		t.Fatalf("remote fallback peers = %x, want %x", spec.peers, remoteKeyID)
	}
	if spec.candidateADNL[p2p.PeerID(localKeyID)] != p2p.PeerID(localKeyID) ||
		spec.candidateADNL[p2p.PeerID(remoteKeyID)] != p2p.PeerID(remoteKeyID) {
		t.Fatalf("candidate ADNL fallback map = %#v", spec.candidateADNL)
	}
}

func TestObserverSessionSpecUsesRemoteSigningKeyADNLFallback(t *testing.T) {
	remoteKey, remoteKeyID := testValidatorKey(t, 0x71)
	localObserver := [32]byte{0x72}
	manager := &Manager{localADNLID: localObserver}

	spec, err := manager.observerSessionSpec(collator.OverlaySession{
		Session: collator.Session{
			ID:               [32]byte{1},
			CatchainSeqno:    2,
			ValidatorSetHash: 3,
			ProtocolVersion:  3,
			Validators: []collator.SessionValidator{{
				PublicKey: remoteKey,
				Weight:    1,
			}},
		},
		Role:                      collator.OverlayRoleObserver,
		AllOverlayNodes:           [][32]byte{localObserver, remoteKeyID},
		MaxBlockSize:              100,
		MaxCollatedDataSize:       200,
		BroadcastMode:             collator.CandidateBroadcastPrivateOverlay,
		ObserversInPrivateOverlay: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if spec.validatorByADNL[p2p.PeerID(remoteKeyID)] != 0 {
		t.Fatalf("observer validator ADNL fallback map = %#v", spec.validatorByADNL)
	}
	if spec.candidateADNL[p2p.PeerID(remoteKeyID)] != p2p.PeerID(remoteKeyID) {
		t.Fatalf("observer candidate ADNL fallback map = %#v", spec.candidateADNL)
	}
}

func TestPersistentObserverSharesPhysicalHubWithStandaloneObserver(t *testing.T) {
	validatorKey, validatorKeyID := testValidatorKey(t, 0x73)
	localObserver := [32]byte{0x74}
	sessionID := [32]byte{0x75}
	opener := &testOverlayOpener{}
	manager := &Manager{
		openOverlay: opener.open,
		broadcasts:  &testBlockPublisher{},
		localADNLID: localObserver,
		sessions:    make(map[[32]byte]*session),
	}
	if err := manager.Start(context.Background(), collator.RemoteHandlers{
		Probe: func(
			context.Context,
			collator.AuthenticatedQuery,
			simplex.ConsensusPleaseCollatePrepare,
		) error {
			return nil
		},
		Commit: func(
			context.Context,
			collator.AuthenticatedQuery,
			simplex.ConsensusPleaseCollate,
		) error {
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := manager.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})

	config := validator.SessionConfig{
		SessionID:        sessionID,
		CatchainSeqno:    2,
		ValidatorSetHash: 3,
		Validators: []groups.Validator{{
			PublicKey: validatorKey, PublicKeyHash: validatorKeyID, Weight: 1,
		}},
		OverlayMembers: [][32]byte{localObserver, validatorKeyID},
		Protocol:       validator.SessionProtocol{ProtocolVersion: 3},
		CandidateLimits: validator.CandidateLimits{
			MaxBlockBytes: 100, MaxCollatedDataBytes: 200,
		},
		Identity: validator.SessionIdentity{ADNLID: localObserver},
	}
	runtimeNetwork, err := manager.PreparePersistentObserverSession(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	runtimeEndpoint := runtimeNetwork.(*sessionEndpoint)
	runtimeSpec, err := runtimeEndpoint.hub.contribution(sessionKindValidator)
	if err != nil {
		t.Fatal(err)
	}
	if runtimeSpec.kind != sessionKindValidator || runtimeSpec.role != collator.OverlayRoleObserver ||
		runtimeSpec.signer != nil {
		t.Fatalf(
			"persistent observer transport = kind %d role %d signer %T",
			runtimeSpec.kind,
			runtimeSpec.role,
			runtimeSpec.signer,
		)
	}
	startTestEndpoint(t, runtimeEndpoint, &testSessionReceiver{})
	extra, err := (&simplex.BroadcastExtra{Slot: 1}).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	if err = runtimeEndpoint.BroadcastCandidate(
		context.Background(),
		simplex.CandidateBroadcast{Data: []byte{1}, Extra: extra},
		validator.CandidateArtifact{Candidate: simplex.Candidate{Empty: true}},
	); err == nil {
		t.Fatal("persistent observer originated a candidate")
	}

	standaloneNetwork, err := manager.PrepareSession(context.Background(), collator.OverlaySession{
		Session: collator.Session{
			ID:               sessionID,
			CatchainSeqno:    2,
			ValidatorSetHash: 3,
			ProtocolVersion:  3,
			Validators: []collator.SessionValidator{{
				PublicKey: validatorKey,
				Weight:    1,
			}},
		},
		Role:                      collator.OverlayRoleObserver,
		AllOverlayNodes:           [][32]byte{localObserver, validatorKeyID},
		MaxBlockSize:              100,
		MaxCollatedDataSize:       200,
		BroadcastMode:             collator.CandidateBroadcastPrivateOverlay,
		ObserversInPrivateOverlay: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	standaloneEndpoint := standaloneNetwork.(*sessionEndpoint)
	if standaloneEndpoint == runtimeEndpoint {
		t.Fatal("validator-owned and standalone observers shared one lifecycle endpoint")
	}
	hub := manager.sessions[sessionID]
	if len(manager.sessions) != 1 || hub.endpoint(sessionKindValidator) != runtimeEndpoint ||
		hub.endpoint(sessionKindObserver) != standaloneEndpoint {
		t.Fatal("observer roles did not share one session hub")
	}
	if len(opener.overlays) != 2 || opener.overlays[0].closed != 1 || opener.latest().closed != 0 {
		t.Fatalf(
			"physical overlay lifecycle = opens %d first closes %d active closes %d",
			len(opener.overlays),
			opener.overlays[0].closed,
			opener.latest().closed,
		)
	}
}

func TestAuthorizedCandidateSourcesPreservesValidatorOnCollatorCollision(t *testing.T) {
	validatorKeyID := [32]byte{0x81}
	validatorADNL := [32]byte{0x82}
	authorized, candidateADNL, err := authorizedCandidateSources(
		[][32]byte{validatorKeyID},
		[]groups.Validator{{PublicKeyHash: validatorKeyID, ADNL: validatorADNL}},
		[]groups.CollatorRegistryEntry{{
			ValidatorKeyID:  validatorKeyID,
			CollatorADNLIDs: [][32]byte{validatorKeyID},
		}},
		1024,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(authorized) != 1 {
		t.Fatalf("authorized source count = %d, want 1", len(authorized))
	}
	if got := candidateADNL[p2p.PeerID(validatorKeyID)]; got != p2p.PeerID(validatorADNL) {
		t.Fatalf("colliding source ADNL = %x, want validator ADNL %x", got, validatorADNL)
	}
}

func TestValidatorRosterRejectsDuplicateEffectiveADNL(t *testing.T) {
	firstKey, firstKeyID := testValidatorKey(t, 0x91)
	secondKey, secondKeyID := testValidatorKey(t, 0x92)
	_, _, err := validatorRoster([]groups.Validator{
		{PublicKey: firstKey, PublicKeyHash: firstKeyID, Weight: 1},
		{PublicKey: secondKey, PublicKeyHash: secondKeyID, ADNL: firstKeyID, Weight: 1},
	})
	if err == nil {
		t.Fatal("duplicate effective validator ADNL was accepted")
	}
}

// observerFixtureKey mirrors testValidatorKey without a *testing.T, so the
// fixture above can be a plain function.
func observerFixtureKey(seedByte byte) ([ed25519.PublicKeySize]byte, [32]byte) {
	privateKey := ed25519.NewKeyFromSeed(bytesOf(seedByte, ed25519.SeedSize))
	var publicKey [ed25519.PublicKeySize]byte
	copy(publicKey[:], privateKey.Public().(ed25519.PublicKey))
	keyID, _ := groups.PublicKeyHash(publicKey)
	return publicKey, keyID
}

func testValidatorKey(t *testing.T, seedByte byte) ([ed25519.PublicKeySize]byte, [32]byte) {
	t.Helper()
	privateKey := ed25519.NewKeyFromSeed(bytesOf(seedByte, ed25519.SeedSize))
	var publicKey [ed25519.PublicKeySize]byte
	copy(publicKey[:], privateKey.Public().(ed25519.PublicKey))
	keyID, err := groups.PublicKeyHash(publicKey)
	if err != nil {
		t.Fatal(err)
	}

	return publicKey, keyID
}

func bytesOf(value byte, size int) []byte {
	result := make([]byte, size)
	for i := range result {
		result[i] = value
	}

	return result
}
