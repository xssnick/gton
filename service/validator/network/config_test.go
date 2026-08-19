package network

import (
	"context"
	"crypto/ed25519"
	"errors"
	"math"
	"slices"
	"testing"
	"time"

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
		OverlayMembers:       [][32]byte{localADNL, observerADNL, collatorADNL},
		AllCurrentValidators: [][32]byte{localADNL},
		CollatorsByValidator: []groups.CollatorRegistryEntry{{
			ValidatorKeyID: keyID, CollatorADNLIDs: [][32]byte{collatorADNL},
		}},
		AllCollators: [][32]byte{collatorADNL},
		Protocol: validator.SessionProtocol{
			Version:              2,
			ProtocolVersion:      3,
			UseQUIC:              false,
			SlotsPerLeaderWindow: 1,
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
	overlayConfig := spec.consensusOverlayConfig()
	if overlayConfig.UseQUIC || !overlayConfig.EnableTwoStep || overlayConfig.AllowLegacyBroadcasts {
		t.Fatalf("private overlay v3 policy = %#v", overlayConfig)
	}
	if !slices.Equal(overlayConfig.TwoStepIntermediateMembers, []p2p.PeerID{p2p.PeerID(localADNL)}) {
		t.Fatalf("private overlay two-step intermediates = %x", overlayConfig.TwoStepIntermediateMembers)
	}
}

func TestProtocolZeroAndOneUseReferenceOverlayMembershipAndSources(t *testing.T) {
	localKey, localKeyID := testValidatorKey(t, 0xa1)
	remoteKey, remoteKeyID := testValidatorKey(t, 0xa2)
	localADNL := [32]byte{0x11}
	remoteADNL := [32]byte{0x12}
	observerADNL := [32]byte{0x13}
	sessionCollator := [32]byte{0x14}
	globalCollator := [32]byte{0x15}
	otherCurrentADNL := [32]byte{0x16}
	allCollators := [][32]byte{sessionCollator, globalCollator}
	wideMembers := [][32]byte{
		localADNL, remoteADNL, observerADNL, sessionCollator, globalCollator, otherCurrentADNL,
	}
	allCurrentValidators := [][32]byte{localADNL, remoteADNL, otherCurrentADNL}
	manager := &Manager{localADNLID: localADNL}

	for _, protocolVersion := range []uint8{0, 1} {
		t.Run(string(rune('0'+protocolVersion)), func(t *testing.T) {
			spec, err := manager.validatorSessionSpec(validator.SessionConfig{
				SessionID:        [32]byte{0x20},
				CatchainSeqno:    2,
				ValidatorSetHash: 3,
				Validators: []groups.Validator{
					{PublicKey: localKey, PublicKeyHash: localKeyID, ADNL: localADNL, Weight: 1},
					{PublicKey: remoteKey, PublicKeyHash: remoteKeyID, ADNL: remoteADNL, Weight: 1},
				},
				OverlayMembers:       wideMembers,
				AllCurrentValidators: allCurrentValidators,
				CollatorsByValidator: []groups.CollatorRegistryEntry{{
					ValidatorKeyID: localKeyID, CollatorADNLIDs: [][32]byte{sessionCollator},
				}},
				AllCollators: allCollators,
				Protocol: validator.SessionProtocol{
					Version: 2, ProtocolVersion: protocolVersion, UseQUIC: true, SlotsPerLeaderWindow: 2,
				},
				CandidateLimits: validator.CandidateLimits{
					MaxBlockBytes: 100, MaxCollatedDataBytes: 200,
				},
				Identity: validator.SessionIdentity{
					ADNLID: localADNL,
					Validator: &validator.ValidatorIdentity{
						Index: 0, KeyID: localKeyID,
						Signer: testOverlaySigner{key: ed25519.NewKeyFromSeed(bytesOf(0xa1, ed25519.SeedSize))},
					},
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			wantConsensusMembers := []p2p.PeerID{
				p2p.PeerID(localADNL), p2p.PeerID(remoteADNL),
				p2p.PeerID(sessionCollator), p2p.PeerID(globalCollator),
			}
			if !slices.Equal(spec.members, wantConsensusMembers) {
				t.Fatalf("consensus members = %x, want %x", spec.members, wantConsensusMembers)
			}
			if _, ok := spec.consensusAuthorized[p2p.PeerID(localKeyID)]; !ok {
				t.Fatal("consensus overlay did not authorize the validator signing-key hash")
			}
			if _, ok := spec.consensusAuthorized[p2p.PeerID(globalCollator)]; !ok {
				t.Fatal("consensus overlay omitted a global collator source")
			}

			candidateSource := p2p.PeerID(localKeyID)
			if protocolVersion == 1 {
				candidateSource = p2p.PeerID(localADNL)
			}
			if spec.candidateADNL[candidateSource] != p2p.PeerID(localADNL) ||
				spec.validatorSource[candidateSource] != 0 {
				t.Fatalf("candidate source binding = %#v / %#v", spec.candidateADNL, spec.validatorSource)
			}
			if _, ok := spec.authorized[p2p.PeerID(globalCollator)]; !ok {
				t.Fatal("candidate overlay omitted a global collator source")
			}
			if spec.consensusOverlayConfig().UseQUIC != true {
				t.Fatal("consensus overlay ignored the session QUIC policy")
			}
			wantIntermediates := peerIDs(allCurrentValidators)
			if !slices.Equal(spec.consensusOverlayConfig().TwoStepIntermediateMembers, wantIntermediates) {
				t.Fatalf("consensus two-step intermediates = %x", spec.consensusOverlayConfig().TwoStepIntermediateMembers)
			}

			if protocolVersion == 0 {
				if spec.hasBlockSync() || len(spec.blockSyncFullID) != 0 {
					t.Fatal("protocol 0 created a block-sync overlay")
				}
				return
			}
			if !spec.hasBlockSync() || !slices.Equal(spec.blockSyncMembers, peerIDs(wideMembers)) {
				t.Fatalf("protocol 1 block-sync members = %x", spec.blockSyncMembers)
			}
			if _, ok := spec.authorized[p2p.PeerID(localADNL)]; !ok {
				t.Fatal("protocol 1 block-sync overlay did not authorize validator ADNL")
			}
			if _, ok := spec.authorized[p2p.PeerID(localKeyID)]; ok {
				t.Fatal("protocol 1 block-sync overlay authorized validator signing-key hash")
			}
			if !spec.blockSyncOverlayConfig().UseQUIC {
				t.Fatal("block-sync overlay ignored the session QUIC policy")
			}
			if !slices.Equal(spec.blockSyncOverlayConfig().TwoStepIntermediateMembers, wantIntermediates) {
				t.Fatalf("block-sync two-step intermediates = %x", spec.blockSyncOverlayConfig().TwoStepIntermediateMembers)
			}
		})
	}
}

func TestProtocolZeroAndOneMasterchainKeepCollatorsOutOfConsensus(t *testing.T) {
	localKey, localKeyID := testValidatorKey(t, 0xc1)
	remoteKey, remoteKeyID := testValidatorKey(t, 0xc2)
	localADNL := [32]byte{0x41}
	remoteADNL := [32]byte{0x42}
	globalCollator := [32]byte{0x43}
	otherCurrentADNL := [32]byte{0x44}
	manager := &Manager{localADNLID: localADNL}

	for _, protocolVersion := range []uint8{0, 1} {
		t.Run(string(rune('0'+protocolVersion)), func(t *testing.T) {
			spec, err := manager.validatorSessionSpec(validator.SessionConfig{
				SessionID: [32]byte{0x44},
				Shard: groups.ShardID{
					Workchain: -1,
					Shard:     math.MinInt64,
				},
				Validators: []groups.Validator{
					{PublicKey: localKey, PublicKeyHash: localKeyID, ADNL: localADNL, Weight: 1},
					{PublicKey: remoteKey, PublicKeyHash: remoteKeyID, ADNL: remoteADNL, Weight: 1},
				},
				OverlayMembers:       [][32]byte{localADNL, remoteADNL, globalCollator, otherCurrentADNL},
				AllCurrentValidators: [][32]byte{localADNL, remoteADNL, otherCurrentADNL},
				AllCollators:         [][32]byte{globalCollator},
				Protocol: validator.SessionProtocol{
					Version: 2, ProtocolVersion: protocolVersion, SlotsPerLeaderWindow: 1,
				},
				CandidateLimits: validator.CandidateLimits{
					MaxBlockBytes: 100, MaxCollatedDataBytes: 200,
				},
				Identity: validator.SessionIdentity{
					ADNLID: localADNL,
					Validator: &validator.ValidatorIdentity{
						Index: 0, KeyID: localKeyID,
						Signer: testOverlaySigner{key: ed25519.NewKeyFromSeed(bytesOf(0xc1, ed25519.SeedSize))},
					},
				},
			})
			if err != nil {
				t.Fatal(err)
			}

			wantConsensusMembers := []p2p.PeerID{p2p.PeerID(localADNL), p2p.PeerID(remoteADNL)}
			if !slices.Equal(spec.members, wantConsensusMembers) {
				t.Fatalf("masterchain consensus members = %x, want %x", spec.members, wantConsensusMembers)
			}
			if _, ok := spec.consensusAuthorized[p2p.PeerID(globalCollator)]; ok {
				t.Fatal("masterchain consensus authorized a collator source")
			}
			if protocolVersion == 0 {
				if _, ok := spec.authorized[p2p.PeerID(globalCollator)]; ok {
					t.Fatal("protocol 0 masterchain candidate path authorized a collator source")
				}
				return
			}
			if _, ok := spec.authorized[p2p.PeerID(globalCollator)]; !ok {
				t.Fatal("protocol 1 block-sync path omitted a global collator source")
			}
			if !slices.Contains(spec.blockSyncMembers, p2p.PeerID(globalCollator)) {
				t.Fatal("protocol 1 block-sync membership omitted a global collator")
			}
		})
	}
}

func peerIDs(ids [][32]byte) []p2p.PeerID {
	result := make([]p2p.PeerID, len(ids))
	for i := range ids {
		result[i] = p2p.PeerID(ids[i])
	}
	return result
}

func TestProtocolOneStandaloneRolesSelectConsensusExplicitly(t *testing.T) {
	validatorKey, _ := testValidatorKey(t, 0xb1)
	validatorADNL := [32]byte{0x31}
	localADNL := [32]byte{0x32}
	globalCollator := [32]byte{0x34}
	descriptor := collator.OverlaySession{
		Session: collator.Session{
			ID: [32]byte{0x33}, ConsensusVersion: 2, ProtocolVersion: 1,
			SlotsPerLeaderWindow: 1, UseQUIC: true,
			Validators: []collator.SessionValidator{{
				PublicKey: validatorKey, ADNLID: validatorADNL, Weight: 1,
			}},
		},
		Role:                      collator.OverlayRoleObserver,
		AllCollators:              [][32]byte{globalCollator},
		AllCurrentValidators:      [][32]byte{validatorADNL},
		AllOverlayNodes:           [][32]byte{validatorADNL, localADNL, globalCollator},
		MaxBlockSize:              100,
		MaxCollatedDataSize:       200,
		BroadcastMode:             collator.CandidateBroadcastBlockSyncOverlay,
		ObserversInPrivateOverlay: false,
	}
	opener := &testOverlayOpener{}
	manager := &Manager{
		openOverlay: opener.open,
		broadcasts:  &testBlockPublisher{},
		localADNLID: localADNL,
		sessions:    make(map[[32]byte]*session),
	}
	registeredObserver := descriptor
	registeredObserver.AllCollators = append(
		append([][32]byte(nil), descriptor.AllCollators...),
		localADNL,
	)
	if _, err := manager.observerSessionSpec(registeredObserver); err == nil {
		t.Fatal("globally registered collator was accepted as a plain observer")
	}

	observerSpec, err := manager.observerSessionSpec(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if observerSpec.openConsensus {
		t.Fatal("protocol 1 plain observer opened the consensus overlay")
	}
	if observerSpec.canOriginateCandidate() {
		t.Fatal("protocol 1 observer enabled candidate sender workers")
	}
	endpoint, err := manager.prepare(context.Background(), observerSpec)
	if err != nil {
		t.Fatal(err)
	}
	if len(opener.overlays) != 1 || opener.configs[0].Name != "block-sync.330000000000" {
		t.Fatalf("observer physical overlays = %#v", opener.configs)
	}
	if opener.overlays[0].callbacks.Message != nil || opener.overlays[0].callbacks.Query != nil ||
		opener.overlays[0].callbacks.BroadcastPrecheck == nil {
		t.Fatal("block-sync-only observer installed consensus callbacks")
	}
	startTestEndpoint(t, endpoint, &testSessionReceiver{})
	endpoint.BroadcastToAll([]byte{1, 2, 3, 4})
	time.Sleep(10 * time.Millisecond)
	opener.overlays[0].mu.Lock()
	queries := len(opener.overlays[0].queryPeers)
	opener.overlays[0].mu.Unlock()
	if queries != 0 {
		t.Fatal("block-sync-only observer sent consensus traffic")
	}

	descriptor.Role = collator.OverlayRoleCollator
	descriptor.AllCollators = [][32]byte{localADNL, globalCollator}
	collatorSpec, err := manager.observerSessionSpec(descriptor)
	if err != nil {
		t.Fatalf("global collator was rejected without a session-filtered delegation entry: %v", err)
	}
	if !collatorSpec.openConsensus {
		t.Fatal("protocol 1 global collator did not open consensus")
	}
	if !collatorSpec.canOriginateCandidate() {
		t.Fatal("protocol 1 global collator disabled candidate sender workers")
	}

	descriptor.Role = collator.OverlayRoleCollator
	descriptor.AllCollators = append(descriptor.AllCollators, [32]byte{0x35})
	if _, err = manager.observerSessionSpec(descriptor); err == nil {
		t.Fatal("block-sync membership omitted an authorized global collator")
	}
}

func TestSessionSpecsAcceptProtocolsThroughV3(t *testing.T) {
	manager := &Manager{}
	for _, version := range []uint8{4, 255} {
		_, err := manager.validatorSessionSpec(validator.SessionConfig{
			Protocol: validator.SessionProtocol{
				Version: 2, ProtocolVersion: version, SlotsPerLeaderWindow: 1,
			},
		})
		if err == nil {
			t.Fatalf("validator protocol version %d was accepted", version)
		}
	}
	// The observer half needs a manager whose ADNL is actually in the overlay,
	// otherwise every descriptor is rejected by a later check and the version
	// gate is never reached.
	observerManager := &Manager{localADNLID: observerFixtureLocalADNL}
	for _, version := range []uint8{4, 255} {
		if _, err := observerManager.observerSessionSpec(observerSessionFixture(version)); err == nil {
			t.Fatalf("observer protocol version %d was accepted", version)
		}
	}
	// The controls: complete descriptors for every supported transport version
	// accepted. Without it the observer half proves nothing — a descriptor that
	// trips a later check is rejected for every version, including the good one,
	// and the version gate could be deleted outright without failing anything.
	if _, err := observerManager.observerSessionSpec(observerSessionFixture(0)); err == nil {
		t.Fatal("standalone protocol version 0 overlay was accepted")
	}
	for version := uint8(1); version <= simplex.MaxProtocolVersion; version++ {
		if _, err := observerManager.observerSessionSpec(observerSessionFixture(version)); err != nil {
			t.Fatalf("observer protocol version %d was rejected: %v", version, err)
		}
	}
}

func TestPersistentObserverSessionSpecRejectsProtocolZero(t *testing.T) {
	manager := &Manager{localADNLID: [32]byte{1}}
	_, err := manager.persistentObserverSessionSpec(validator.SessionConfig{
		Protocol: validator.SessionProtocol{Version: 2, ProtocolVersion: 0},
	})
	if err == nil {
		t.Fatal("persistent observer protocol version 0 was accepted")
	}
}

var observerFixtureLocalADNL = [32]byte{0x72}

// observerSessionFixture is a descriptor that is valid in every respect except
// the protocol version under test, so a rejection is attributable to the
// version and nothing else.
func observerSessionFixture(version uint8) collator.OverlaySession {
	remoteKey, remoteKeyID := observerFixtureKey(0x71)
	globalCollator := [32]byte{0x73}
	broadcastMode := collator.CandidateBroadcastPrivateOverlay
	if version == 1 {
		broadcastMode = collator.CandidateBroadcastBlockSyncOverlay
	}
	return collator.OverlaySession{
		Session: collator.Session{
			ID:                   [32]byte{1},
			CatchainSeqno:        2,
			ValidatorSetHash:     3,
			ConsensusVersion:     2,
			ProtocolVersion:      version,
			SlotsPerLeaderWindow: 1,
			Validators: []collator.SessionValidator{{
				PublicKey: remoteKey,
				Weight:    1,
			}},
		},
		Role:                      collator.OverlayRoleObserver,
		AllCurrentValidators:      [][32]byte{remoteKeyID},
		AllOverlayNodes:           [][32]byte{observerFixtureLocalADNL, remoteKeyID, globalCollator},
		AllCollators:              [][32]byte{globalCollator},
		MaxBlockSize:              100,
		MaxCollatedDataSize:       200,
		BroadcastMode:             broadcastMode,
		ObserversInPrivateOverlay: version >= 2,
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
		OverlayMembers:       [][32]byte{localKeyID, remoteKeyID},
		AllCurrentValidators: [][32]byte{localKeyID, remoteKeyID},
		Protocol: validator.SessionProtocol{
			Version: 2, ProtocolVersion: 3, SlotsPerLeaderWindow: 1,
		},
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
			ID:                   [32]byte{1},
			CatchainSeqno:        2,
			ValidatorSetHash:     3,
			ConsensusVersion:     2,
			ProtocolVersion:      3,
			SlotsPerLeaderWindow: 1,
			Validators: []collator.SessionValidator{{
				PublicKey: remoteKey,
				Weight:    1,
			}},
		},
		Role:                      collator.OverlayRoleObserver,
		AllCurrentValidators:      [][32]byte{remoteKeyID},
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

func TestPersistentObserverRejectsStandaloneObserverOnSameHub(t *testing.T) {
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
		OverlayMembers:       [][32]byte{localObserver, validatorKeyID},
		AllCurrentValidators: [][32]byte{validatorKeyID},
		Protocol: validator.SessionProtocol{
			Version: 2, ProtocolVersion: 3, SlotsPerLeaderWindow: 1,
		},
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
			ID:                   sessionID,
			CatchainSeqno:        2,
			ValidatorSetHash:     3,
			ConsensusVersion:     2,
			ProtocolVersion:      3,
			SlotsPerLeaderWindow: 1,
			Validators: []collator.SessionValidator{{
				PublicKey: validatorKey,
				Weight:    1,
			}},
		},
		Role:                      collator.OverlayRoleObserver,
		AllCurrentValidators:      [][32]byte{validatorKeyID},
		AllOverlayNodes:           [][32]byte{localObserver, validatorKeyID},
		MaxBlockSize:              100,
		MaxCollatedDataSize:       200,
		BroadcastMode:             collator.CandidateBroadcastPrivateOverlay,
		ObserversInPrivateOverlay: true,
	})
	if !errors.Is(err, ErrSessionConflict) {
		t.Fatalf("standalone observer attach error = %v, want conflict", err)
	}
	if standaloneNetwork != nil {
		t.Fatal("conflicting standalone observer returned a session endpoint")
	}
	hub := manager.sessions[sessionID]
	if len(manager.sessions) != 1 || hub.endpoint(sessionKindValidator) != runtimeEndpoint ||
		hub.endpoint(sessionKindObserver) != nil {
		t.Fatal("conflicting observer changed the existing session hub")
	}
	if len(opener.overlays) != 1 || opener.overlays[0].closed != 0 {
		t.Fatalf(
			"physical overlay lifecycle = opens %d closes %d",
			len(opener.overlays),
			opener.overlays[0].closed,
		)
	}
}

func TestAuthorizedCandidateSourcesPreservesValidatorOnCollatorCollision(t *testing.T) {
	validatorKeyID := [32]byte{0x81}
	validatorADNL := [32]byte{0x82}
	authorized, candidateADNL, err := authorizedCandidateSources(
		3,
		[][32]byte{validatorKeyID},
		[]groups.Validator{{PublicKeyHash: validatorKeyID, ADNL: validatorADNL}},
		[][32]byte{validatorKeyID},
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
