package node

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/liveview"
	"github.com/xssnick/gton/service/validator"
	core "github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/keyring"
	validatorpebble "github.com/xssnick/gton/service/validator/pebblestore"
	"github.com/xssnick/gton/service/validator/simplex"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm"
)

// compositionStore is a store that satisfies everything the consensus seam asks
// for WITHOUT being the live view. It is the shape consensusLiveView refuses, and
// TestConsensusCompositionRefusesAStoreThatIsNotTheLiveView is what it is for.
type compositionStore struct {
	hooks.Store
}

func (*compositionStore) WaitBlockArtifacts(context.Context, ton.BlockIDExt) error {
	return nil
}

// compositionLiveView is the store a real node hands the compositions: the live
// view itself, over a backing that holds nothing.
func compositionLiveView() *liveview.Store {
	return liveview.New(nothingBacked{})
}

type compositionNetwork struct {
	hooks.Network
	id                [32]byte
	validatorPrepares int
	observerPrepares  int
	prepared          validator.SessionNetwork
	prepareErr        error
	retired           int
}

func (n *compositionNetwork) LocalADNLID() [32]byte { return n.id }

func (n *compositionNetwork) PrepareValidatorSession(
	context.Context,
	validator.SessionConfig,
) (validator.SessionNetwork, error) {
	n.validatorPrepares++

	return n.prepared, n.prepareErr
}

func (n *compositionNetwork) PreparePersistentObserverSession(
	context.Context,
	validator.SessionConfig,
) (validator.SessionNetwork, error) {
	n.observerPrepares++

	return n.prepared, n.prepareErr
}

func (*compositionNetwork) PublishAcceptedBlock(validator.AcceptedBlockPublication) {}
func (n *compositionNetwork) RetireValidatorSession(context.Context, [32]byte) error {
	n.retired++

	return nil
}
func (*compositionNetwork) Start(context.Context, core.RemoteHandlers) error { return nil }
func (*compositionNetwork) PrepareSession(
	context.Context,
	core.OverlaySession,
) (validator.SessionNetwork, error) {
	return nil, errors.New("session transport is not implemented")
}
func (*compositionNetwork) UpdateSession(context.Context, core.OverlaySession) error { return nil }
func (*compositionNetwork) RetireSession(context.Context, [32]byte) error            { return nil }
func (*compositionNetwork) Close(context.Context) error                              { return nil }

type compositionSessionRuntime struct {
	closed bool
}

type compositionPreparedSessionNetwork struct{}

func (*compositionPreparedSessionNetwork) BroadcastToAll([]byte)            {}
func (*compositionPreparedSessionNetwork) BroadcastToValidators([]byte)     {}
func (*compositionPreparedSessionNetwork) BroadcastToRandom(uint32, []byte) {}
func (*compositionPreparedSessionNetwork) BroadcastCandidate(
	context.Context,
	simplex.CandidateBroadcast,
	validator.CandidateArtifact,
) error {
	return nil
}
func (*compositionPreparedSessionNetwork) RequestCandidate(
	context.Context,
	validator.CandidateRequest,
) (validator.CandidateResponse, error) {
	return validator.CandidateResponse{}, validator.ErrCandidateUnavailable
}
func (*compositionPreparedSessionNetwork) Start(context.Context, validator.SessionReceiver) error {
	return nil
}
func (*compositionPreparedSessionNetwork) Run(ctx context.Context) error {
	<-ctx.Done()

	return nil
}

type compositionRemoteTransport struct {
	id     [32]byte
	starts int
	closes int
}

func (t *compositionRemoteTransport) CollatorID() [32]byte { return t.id }
func (t *compositionRemoteTransport) Start(context.Context) error {
	t.starts++

	return nil
}
func (t *compositionRemoteTransport) Close(context.Context) error {
	t.closes++

	return nil
}
func (*compositionRemoteTransport) Probe(
	context.Context,
	core.AuthenticatedQuery,
	simplex.ConsensusPleaseCollatePrepare,
) error {
	return nil
}
func (*compositionRemoteTransport) Commit(
	context.Context,
	core.AuthenticatedQuery,
	simplex.ConsensusPleaseCollate,
) error {
	return nil
}

func (*compositionSessionRuntime) Recover(context.Context, validator.SessionStart) error {
	return nil
}
func (*compositionSessionRuntime) Run(context.Context, validator.SessionStart) error { return nil }
func (*compositionSessionRuntime) Update(context.Context, validator.SessionState) error {
	return nil
}
func (r *compositionSessionRuntime) Close() error {
	r.closed = true

	return nil
}
func (r *compositionSessionRuntime) Retire() error { return r.Close() }

func TestValidatorSessionRuntimeRetiresTransport(t *testing.T) {
	network := &compositionNetwork{}
	inner := &compositionSessionRuntime{}
	remoteTransport := &compositionRemoteTransport{id: [32]byte{2}}
	remoteCollator, err := core.NewRemoteCollator(remoteTransport)
	if err != nil {
		t.Fatal(err)
	}
	if err = remoteCollator.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	runtime := &validatorSessionRuntime{
		SessionRuntime: inner,
		network:        network,
		sessionID:      [32]byte{1},
		ownedCollator:  remoteCollator,
	}
	if err = runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if !inner.closed || remoteTransport.closes != 1 || network.retired != 1 {
		t.Fatalf(
			"runtime close: inner=%t remote closes=%d retired=%d",
			inner.closed,
			remoteTransport.closes,
			network.retired,
		)
	}
}

func TestSelectSessionProduction(t *testing.T) {
	validatorID := [32]byte{0x41}
	lowestCollatorID := [32]byte{0x11}
	highestCollatorID := [32]byte{0x31}
	tests := []struct {
		name       string
		config     validator.SessionConfig
		expected   core.ProductionMode
		collatorID [32]byte
	}{
		{
			name: "observer stays self",
			config: validator.SessionConfig{CollatorsByValidator: []groups.CollatorRegistryEntry{{
				ValidatorKeyID:  validatorID,
				CollatorADNLIDs: [][32]byte{lowestCollatorID},
			}}},
			expected: core.ProductionModeSelf,
		},
		{
			name: "validator without registry entry stays self",
			config: validator.SessionConfig{
				Identity: validator.SessionIdentity{Validator: &validator.ValidatorIdentity{KeyID: validatorID}},
				CollatorsByValidator: []groups.CollatorRegistryEntry{{
					ValidatorKeyID:  [32]byte{0x42},
					CollatorADNLIDs: [][32]byte{lowestCollatorID},
				}},
			},
			expected: core.ProductionModeSelf,
		},
		{
			name: "validator with empty registry entry stays self",
			config: validator.SessionConfig{
				Identity: validator.SessionIdentity{Validator: &validator.ValidatorIdentity{KeyID: validatorID}},
				CollatorsByValidator: []groups.CollatorRegistryEntry{{
					ValidatorKeyID: validatorID,
				}},
			},
			expected: core.ProductionModeSelf,
		},
		{
			name: "validator selects lowest collator id",
			config: validator.SessionConfig{
				Identity: validator.SessionIdentity{Validator: &validator.ValidatorIdentity{KeyID: validatorID}},
				CollatorsByValidator: []groups.CollatorRegistryEntry{{
					ValidatorKeyID:  validatorID,
					CollatorADNLIDs: [][32]byte{highestCollatorID, lowestCollatorID},
				}},
			},
			expected:   core.ProductionModeDelegated,
			collatorID: lowestCollatorID,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			selection := selectSessionProduction(test.config)
			if selection.mode != test.expected || selection.collatorID != test.collatorID {
				t.Fatalf(
					"selection = mode %d collator %x, want mode %d collator %x",
					selection.mode,
					selection.collatorID,
					test.expected,
					test.collatorID,
				)
			}
		})
	}
}

func TestPrepareSessionProductionStartsSelectedRemoteWithoutSelfFallback(t *testing.T) {
	validatorID := [32]byte{0x41}
	selectedCollatorID := [32]byte{0x21}
	config := validator.SessionConfig{
		Identity: validator.SessionIdentity{Validator: &validator.ValidatorIdentity{KeyID: validatorID}},
		CollatorsByValidator: []groups.CollatorRegistryEntry{{
			ValidatorKeyID:  validatorID,
			CollatorADNLIDs: [][32]byte{{0x31}, selectedCollatorID},
		}},
	}
	remoteTransport := &compositionRemoteTransport{}
	production, err := prepareSessionProduction(
		context.Background(),
		config,
		nil,
		validator.NewLocalCandidateRouter(),
		func(collatorID [32]byte) (core.Collator, error) {
			remoteTransport.id = collatorID

			return core.NewRemoteCollator(remoteTransport)
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if production.mode != core.ProductionModeDelegated ||
		production.producer == nil || production.ownedCollator != production.producer ||
		production.candidateRouter != nil {
		t.Fatalf("delegated production wiring = %+v", production)
	}
	if remoteTransport.id != selectedCollatorID || remoteTransport.starts != 1 {
		t.Fatalf(
			"remote transport = id %x starts %d, want id %x starts 1",
			remoteTransport.id,
			remoteTransport.starts,
			selectedCollatorID,
		)
	}
	if err = closeValidatorSessionCollator(production.ownedCollator); err != nil {
		t.Fatal(err)
	}
}

func TestPrepareSessionProductionKeepsSelfWhenRegistryHasNoLocalCollator(t *testing.T) {
	validatorID := [32]byte{0x41}
	config := validator.SessionConfig{
		Identity: validator.SessionIdentity{Validator: &validator.ValidatorIdentity{KeyID: validatorID}},
		CollatorsByValidator: []groups.CollatorRegistryEntry{{
			ValidatorKeyID:  [32]byte{0x42},
			CollatorADNLIDs: [][32]byte{{0x21}},
		}},
	}
	localTransport := &compositionRemoteTransport{id: [32]byte{0x51}}
	localCollator, err := core.NewRemoteCollator(localTransport)
	if err != nil {
		t.Fatal(err)
	}
	router := validator.NewLocalCandidateRouter()
	factoryCalls := 0
	production, err := prepareSessionProduction(
		context.Background(),
		config,
		localCollator,
		router,
		func([32]byte) (core.Collator, error) {
			factoryCalls++

			return nil, errors.New("unexpected remote factory call")
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if production.mode != core.ProductionModeSelf || production.producer != localCollator ||
		production.candidateRouter != router || production.ownedCollator != nil {
		t.Fatalf("self production wiring = %+v", production)
	}
	if factoryCalls != 0 || localTransport.starts != 0 {
		t.Fatalf("self production started remote path: factory=%d starts=%d", factoryCalls, localTransport.starts)
	}
}

func TestPrepareSessionProductionDoesNotFallbackAfterRemoteFailure(t *testing.T) {
	validatorID := [32]byte{0x41}
	config := validator.SessionConfig{
		Identity: validator.SessionIdentity{Validator: &validator.ValidatorIdentity{KeyID: validatorID}},
		CollatorsByValidator: []groups.CollatorRegistryEntry{{
			ValidatorKeyID:  validatorID,
			CollatorADNLIDs: [][32]byte{{0x21}},
		}},
	}
	expectedErr := errors.New("remote unavailable")
	production, err := prepareSessionProduction(
		context.Background(),
		config,
		nil,
		validator.NewLocalCandidateRouter(),
		func([32]byte) (core.Collator, error) {
			return nil, expectedErr
		},
	)
	if !errors.Is(err, expectedErr) {
		t.Fatalf("prepare error = %v, want %v", err, expectedErr)
	}
	if production != (sessionProduction{}) {
		t.Fatalf("failed delegated preparation returned self wiring: %+v", production)
	}
}

func TestPrepareValidatorSessionNetworkSelectsIdentityRole(t *testing.T) {
	network := &compositionNetwork{}
	validatorConfig := validator.SessionConfig{
		Identity: validator.SessionIdentity{Validator: &validator.ValidatorIdentity{}},
	}
	if _, err := prepareValidatorSessionNetwork(context.Background(), network, validatorConfig); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareValidatorSessionNetwork(
		context.Background(),
		network,
		validator.SessionConfig{},
	); err != nil {
		t.Fatal(err)
	}
	if network.validatorPrepares != 1 || network.observerPrepares != 1 {
		t.Fatalf(
			"session network prepares = validator %d observer %d",
			network.validatorPrepares,
			network.observerPrepares,
		)
	}
}

func TestPrepareBlockSyncObserverSessionOwnsOnlyPreparedTransport(t *testing.T) {
	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x71}, ed25519.SeedSize))
	publicKey := privateKey.Public().(ed25519.PublicKey)
	var public [32]byte
	copy(public[:], publicKey)
	config := validator.SessionConfig{
		SessionID: [32]byte{0x81},
		Shard: groups.ShardID{
			Workchain: 0,
			Shard:     math.MinInt64,
		},
		Validators: []groups.Validator{{
			PublicKey:     public,
			PublicKeyHash: simplex.KeyNodeIDShort(publicKey),
			Weight:        1,
		}},
		Protocol: validator.SessionProtocol{
			Version:              2,
			ProtocolVersion:      1,
			SlotsPerLeaderWindow: 4,
		},
		CandidateLimits: validator.CandidateLimits{
			MaxBlockBytes:        1 << 20,
			MaxCollatedDataBytes: 1 << 20,
		},
		Identity: validator.SessionIdentity{ADNLID: [32]byte{0x91}},
	}
	network := &compositionNetwork{id: config.Identity.ADNLID}
	runtime, err := prepareBlockSyncObserverSession(
		context.Background(),
		network,
		config,
		&compositionPreparedSessionNetwork{},
	)
	if err != nil {
		t.Fatal(err)
	}
	owned, ok := runtime.(*validatorSessionRuntime)
	if !ok || owned.ownedCollator != nil {
		t.Fatalf("block-sync observer runtime wiring = %#v", runtime)
	}
	if err = runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if network.retired != 1 {
		t.Fatalf("retired transports = %d, want 1", network.retired)
	}

	invalid := config
	invalid.Identity.Validator = &validator.ValidatorIdentity{}
	if _, err = prepareBlockSyncObserverSession(
		context.Background(),
		network,
		invalid,
		&compositionPreparedSessionNetwork{},
	); err == nil {
		t.Fatal("validator identity was accepted by block-sync observer composition")
	}
	if network.retired != 2 {
		t.Fatalf("retired transports after failed preparation = %d, want 2", network.retired)
	}
}

func TestValidatorStackCompositionRequiresP2PCapabilities(t *testing.T) {
	node := hooks.Node{
		Store:   &compositionStore{},
		Network: &compositionNetwork{},
		TVM:     tvm.NewTVM(),
	}
	_, err := newValidatorStackFactory(validatorStackComposition{
		localValidator: &localValidatorComposition{},
	})(node)
	if err == nil {
		t.Fatal("validator stack accepted a node without private-overlay capabilities")
	}
}

func TestLocalValidatorCompositionExposesShardTopInbox(t *testing.T) {
	identity := compositionCollatorIdentity(t, 0x31)
	store, err := validatorpebble.Open(validatorpebble.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := store.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	})
	runtime, err := validator.NewRuntime(validator.SharedRuntimeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.Close)
	validatorKeys := compositionValidatorKeys(t, 0x41)
	network := &compositionNetwork{id: identity.keyID}

	extension, err := newLocalValidatorFactory(localValidatorComposition{
		options: validator.Options{
			Keys:         validatorKeys,
			Storage:      store.Validator(),
			Runtime:      runtime,
			EnableGroups: true,
		},
		runtime:         runtime,
		delegations:     store.Validator(),
		collatorStorage: store.Collator(),
		collatorKeys:    identity.keys,
		collatorKeyID:   identity.keyID,
	}, network, nil, nil, nil)(hooks.Node{
		Store:   compositionLiveView(),
		Network: network,
		TVM:     tvm.NewTVM(),
		Logger:  zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := extension.(hooks.ShardTopBlockDescriptionObserver); !ok {
		t.Fatal("composed validator did not expose the local collator shard-top inbox")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err = extension.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestStandaloneCollatorCompositionBuildsCompleteExtension(t *testing.T) {
	identity := compositionCollatorIdentity(t, 0x51)
	store, err := validatorpebble.Open(validatorpebble.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := store.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	})
	runtime, err := validator.NewRuntime(validator.SharedRuntimeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.Close)
	network := &compositionNetwork{id: identity.keyID}

	extension, err := newStandaloneCollatorFactory(standaloneCollatorComposition{
		runtime:            runtime,
		validatorStorage:   store.Validator(),
		collatorStorage:    store.Collator(),
		keys:               identity.keys,
		keyID:              identity.keyID,
		allowAllValidators: true,
	}, network, nil)(hooks.Node{
		Store:   compositionLiveView(),
		Network: network,
		TVM:     tvm.NewTVM(),
		Logger:  zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := extension.(hooks.ShardTopBlockDescriptionObserver); !ok {
		t.Fatal("standalone collator did not expose the shard-top inbox")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err = extension.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func compositionCollatorIdentity(t *testing.T, fill byte) collatorIdentity {
	t.Helper()

	identity, err := configureCollatorIdentity(bytes.Repeat([]byte{fill}, ed25519.SeedSize))
	if err != nil {
		t.Fatal(err)
	}

	return identity
}

func compositionValidatorKeys(t *testing.T, fill byte) *keyring.Keyring {
	t.Helper()

	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{fill}, ed25519.SeedSize))
	keys, err := keyring.New(privateKey)
	clear(privateKey)
	if err != nil {
		t.Fatal(err)
	}

	return keys
}
