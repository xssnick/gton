package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	standalone "github.com/xssnick/gton/extensions/collator"
	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/liveview"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator"
	"github.com/xssnick/gton/service/validator/blockstats"
	core "github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/keyring"
	"github.com/xssnick/gton/service/validator/msgpool"
	validatornet "github.com/xssnick/gton/service/validator/network"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// validatorNetwork is the cohesive process transport shared by validator and
// standalone-collator roles. The concrete implementation owns one physical
// private overlay per session and multiplexes role-specific endpoints on it.
type validatorNetwork interface {
	validator.BlockPublisher
	validator.ConsensusObserverNetwork
	PrepareValidatorSession(context.Context, validator.SessionConfig) (validator.SessionNetwork, error)
	PreparePersistentObserverSession(context.Context, validator.SessionConfig) (validator.SessionNetwork, error)
	RetireValidatorSession(context.Context, [32]byte) error
}

// localSessionNode is the consensus seam onto the node.
//
// The field stays an interface so the seam's own pass-through property can be
// tested against a store that counts and returns fixed cells; what the seam is
// given in production is decided one level up, by consensusLiveView, which is
// where the identity of the store is asserted.
type localSessionNode struct {
	store   hooks.Store
	network hooks.Network
}

// localCollatorStateStore is the collator's read surface, built on the same seam
// the validator uses.
//
// It exists to make "collation and validation read state through one seam" true
// by construction rather than by coincidence. The collator was once handed
// node.Store directly while the validator went through localSessionNode; the two
// happened to agree, and would have kept agreeing right up until anything was
// added to the seam. What they must agree on is not a cache but a POINTER:
// ChainState.validatedCandidateState compares tip states by identity, so two
// materializations of one parent cost a silent full re-apply per candidate. One
// seam is the only structural guarantee of that.
//
// The collator additionally needs two reads hooks.Store does not carry, which is
// why it takes core.LocalStateStore separately rather than being handed a plain
// hooks.Store. Both are straight pass-throughs and deliberately untouched:
// blocks are stored as whole BOCs and never enter the decoded cell cache, so
// there is nothing to decide for them.
type localCollatorStateStore struct {
	localSessionNode
	artifacts core.LocalStateStore
}

// consensusLiveView is the composition-level half of the read rule: the store
// behind the consensus seam IS the live view, named as the concrete type.
//
// The package graph test in service/validator proves neither consensus package can
// REACH the node's meta DB and celldb, and the compile-time block beside it proves
// the live view satisfies everything the seam asks for. Neither closes this hole: a
// *pebblestore.Store satisfies the same method set, so it could be handed in here
// through the hooks.Store interface with both of those still green — and every read
// the consensus path makes would then go straight to the store, waiting for commits
// the live view exists to make unnecessary, and returning a SECOND materialization
// of each parent because a store read decodes fresh cells.
//
// So the assertion is here, where the store is chosen, and it is the identity of
// the type rather than its shape. Both consensus compositions go through it, so
// there is one answer to "which store does consensus read".
func consensusLiveView(role string, store hooks.Store) (*liveview.Store, error) {
	live, ok := store.(*liveview.Store)
	if !ok {
		return nil, fmt.Errorf(
			"%s composition: node store is %T and not the live view: collation and validation "+
				"read state only through the live view, and a store that merely satisfies the same "+
				"methods would read committed state and hand out a second materialization of every parent",
			role,
			store,
		)
	}

	return live, nil
}

func (s localCollatorStateStore) BlockRoot(ctx context.Context, block ton.BlockIDExt) (*cell.Cell, error) {
	return s.artifacts.BlockRoot(ctx, block)
}

func (s localCollatorStateStore) WaitBlockArtifacts(ctx context.Context, block ton.BlockIDExt) error {
	return s.artifacts.WaitBlockArtifacts(ctx, block)
}

const validatorSessionRetireTimeout = 10 * time.Second

type validatorSessionRuntime struct {
	validator.SessionRuntime
	network       validatorNetwork
	sessionID     [32]byte
	ownedCollator core.Collator
}

func (r *validatorSessionRuntime) Close() error {
	runtimeErr := r.SessionRuntime.Close()
	collatorErr := closeValidatorSessionCollator(r.ownedCollator)
	networkErr := retireValidatorSession(r.network, r.sessionID)

	return errors.Join(runtimeErr, collatorErr, networkErr)
}

func (r *validatorSessionRuntime) Retire() error {
	runtimeErr := r.SessionRuntime.Retire()
	collatorErr := closeValidatorSessionCollator(r.ownedCollator)
	networkErr := retireValidatorSession(r.network, r.sessionID)

	return errors.Join(runtimeErr, collatorErr, networkErr)
}

func closeValidatorSessionCollator(producer core.Collator) error {
	if producer == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), validatorSessionRetireTimeout)
	err := producer.Close(ctx)
	cancel()

	return err
}

func retireValidatorSession(network validatorNetwork, sessionID [32]byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), validatorSessionRetireTimeout)
	err := network.RetireValidatorSession(ctx, sessionID)
	cancel()

	return err
}

func prepareValidatorSessionNetwork(
	ctx context.Context,
	network validatorNetwork,
	config validator.SessionConfig,
) (validator.SessionNetwork, error) {
	if config.Identity.Validator == nil {
		return network.PreparePersistentObserverSession(ctx, config)
	}

	return network.PrepareValidatorSession(ctx, config)
}

func prepareBlockSyncObserverSession(
	ctx context.Context,
	network validatorNetwork,
	config validator.SessionConfig,
	sessionNetwork validator.SessionNetwork,
) (validator.SessionRuntime, error) {
	session, err := validator.PrepareBlockSyncObserverRuntime(
		ctx,
		config,
		sessionNetwork,
		config.CandidateLimits,
	)
	if err != nil {
		retireErr := retireValidatorSession(network, config.SessionID)

		return nil, errors.Join(
			fmt.Errorf("validator composition: prepare block-sync observer runtime: %w", err),
			retireErr,
		)
	}

	return &validatorSessionRuntime{
		SessionRuntime: session,
		network:        network,
		sessionID:      config.SessionID,
	}, nil
}

func (n localSessionNode) BlockData(ctx context.Context, block ton.BlockIDExt) ([]byte, error) {
	return n.store.BlockData(ctx, block)
}

func (n localSessionNode) BlockProof(
	ctx context.Context,
	kind storage.ServedProofKind,
	block ton.BlockIDExt,
) ([]byte, error) {
	return n.store.BlockProof(ctx, kind, block)
}

func (n localSessionNode) BlockState(
	ctx context.Context,
	block ton.BlockIDExt,
) (*storage.BlockState, error) {
	return n.store.BlockState(ctx, block)
}

// LoadStateCellTree is the single seam through which BOTH collation and
// validation read state: the collator reaches it through its LocalStateStore
// (localCollatorStateStore embeds this type) and the validator through its local
// session backend, and neither has any other route to the state tree.
//
// Routing them together is not an optimization, it is required.
// ChainState.validatedCandidateState compares tip states by POINTER, so if the
// two ever received different materializations of the same parent, the
// live-successor carry-back would stop matching and every candidate would cost a
// full re-apply — silently, with no error and no metric. That is why this is a
// pass-through and must stay one: anything that transforms, copies or rebuilds
// the returned tree here breaks per-call stability and therefore the identity.
// TestCollatorAndValidatorShareOneParentMaterialization pins it.
//
// There is nothing to route. There is one decoded cell cache behind this call
// for every consumer, and even when there were two, this seam did not choose
// between them: both callers only reach the storage decode when the live view
// has no resident state, and it essentially always does.
func (n localSessionNode) LoadStateCellTree(
	ctx context.Context,
	block ton.BlockIDExt,
	rootHash []byte,
) (*cell.Cell, error) {
	return n.store.LoadStateCellTree(ctx, block, rootHash)
}

func (n localSessionNode) LookupBlockBySeqNo(
	ctx context.Context,
	ref storage.BlockSeqRef,
) (ton.BlockIDExt, error) {
	return n.store.LookupBlockBySeqNo(ctx, ref)
}

func (n localSessionNode) SubmitBlockLocally(block p2p.DownloadedBlock) {
	n.network.SubmitBlockLocally(block)
}

// PublishAcceptedBlockState is the write counterpart of the read seam above, and
// a pass-through for the same reason: the state it publishes is installed BY
// REFERENCE, so the block's own producer, the collator and the validator all read
// back the one materialization that chain_state.go's pointer comparison needs.
// Anything that copied or rebuilt the state here would turn every subsequent
// candidate into a full re-apply, silently.
func (n localSessionNode) PublishAcceptedBlockState(artifacts storage.LiveBlockArtifacts) error {
	return n.store.PublishAcceptedBlockState(artifacts)
}

func (n localSessionNode) BlockArtifactsSignal() <-chan struct{} {
	return n.store.BlockArtifactsSignal()
}

type localValidatorComposition struct {
	options         validator.Options
	runtime         *validator.Runtime
	keys            *keyring.Keyring
	control         validatorControlOptions
	delegations     validator.DelegationAuthorizationStorage
	collatorStorage core.CollatorStorage
	collatorKeys    core.SigningKeys
	collatorKeyID   [32]byte
}

type sessionCollatorFactory func([32]byte) (core.Collator, error)

type sessionProductionSelection struct {
	mode       core.ProductionMode
	collatorID [32]byte
}

type sessionProduction struct {
	mode            core.ProductionMode
	producer        core.Collator
	candidateRouter *validator.LocalCandidateRouter
	ownedCollator   core.Collator
}

func selectSessionProduction(config validator.SessionConfig) sessionProductionSelection {
	selection := sessionProductionSelection{mode: core.ProductionModeSelf}
	if config.Identity.Validator == nil {
		return selection
	}

	validatorID := config.Identity.Validator.KeyID
	for i := range config.CollatorsByValidator {
		entry := config.CollatorsByValidator[i]
		if entry.ValidatorKeyID != validatorID {
			continue
		}
		for _, collatorID := range entry.CollatorADNLIDs {
			if selection.mode == core.ProductionModeSelf ||
				bytes.Compare(collatorID[:], selection.collatorID[:]) < 0 {
				selection.mode = core.ProductionModeDelegated
				selection.collatorID = collatorID
			}
		}
	}

	return selection
}

func prepareSessionProduction(
	ctx context.Context,
	config validator.SessionConfig,
	localCollator core.Collator,
	router *validator.LocalCandidateRouter,
	newRemoteCollator sessionCollatorFactory,
) (sessionProduction, error) {
	selection := selectSessionProduction(config)
	if selection.mode == core.ProductionModeSelf {
		return sessionProduction{
			mode:            core.ProductionModeSelf,
			producer:        localCollator,
			candidateRouter: router,
		}, nil
	}
	if newRemoteCollator == nil {
		return sessionProduction{}, errors.New("validator composition: remote collator factory is required")
	}

	remoteCollator, err := newRemoteCollator(selection.collatorID)
	if err != nil {
		return sessionProduction{}, fmt.Errorf(
			"validator composition: create remote collator %x: %w",
			selection.collatorID,
			err,
		)
	}
	if err = remoteCollator.Start(ctx); err != nil {
		closeErr := closeValidatorSessionCollator(remoteCollator)

		return sessionProduction{}, errors.Join(
			fmt.Errorf(
				"validator composition: start remote collator %x: %w",
				selection.collatorID,
				err,
			),
			closeErr,
		)
	}

	return sessionProduction{
		mode:          core.ProductionModeDelegated,
		producer:      remoteCollator,
		ownedCollator: remoteCollator,
	}, nil
}

type validatorStackComposition struct {
	localValidator     *localValidatorComposition
	standaloneCollator *standaloneCollatorComposition
	localADNLID        [32]byte
}

// newValidatorStackFactory creates the one process-wide transport manager and
// then builds role extensions around it. Start is forward and Close is reverse:
// the standalone observer installs delegated-collation handlers before validator
// sessions attach, while validator endpoints retire before the shared manager.
func newValidatorStackFactory(composition validatorStackComposition) hooks.ExtensionFactory {
	return func(node hooks.Node) (hooks.Extension, error) {
		if composition.localValidator == nil && composition.standaloneCollator == nil {
			return nil, errors.New("validator composition: no validator or collator role is configured")
		}

		var registry core.MetricsRegistry
		if node.Metrics != nil {
			var ok bool
			registry, ok = node.Metrics.(core.MetricsRegistry)
			if !ok {
				return nil, errors.New("validator composition: node metrics registry has an incompatible type")
			}
		}
		candidateTransportMetrics, err := validatornet.NewPrometheusCandidateTransportMetrics(registry)
		if err != nil {
			return nil, fmt.Errorf("validator composition: register candidate transport metrics: %w", err)
		}

		log := node.Logger.With().Str("component", "validator_network").Logger()
		hooks.RaiseGCPercent(log)
		manager, err := validatornet.NewManager(validatornet.ManagerOptions{
			PrivateOverlays:  node.PrivateOverlays,
			BlockBroadcasts:  node.BlockBroadcasts,
			CandidateMetrics: candidateTransportMetrics.Observer(),
			Logger:           log,
		})
		if err != nil {
			return nil, fmt.Errorf("validator composition: create network manager: %w", err)
		}
		if manager.LocalADNLID() != composition.localADNLID {
			closeErr := manager.Close(context.Background())

			return nil, errors.Join(
				fmt.Errorf(
					"validator composition: node ADNL identity %x differs from configured collator identity %x",
					manager.LocalADNLID(),
					composition.localADNLID,
				),
				closeErr,
			)
		}

		collatorMetrics, err := core.NewPrometheusMetrics(registry)
		if err != nil {
			closeErr := manager.Close(context.Background())

			return nil, errors.Join(
				fmt.Errorf("validator composition: register collator metrics: %w", err),
				closeErr,
			)
		}
		blockStats := blockstats.New()
		var validationObserver validator.ValidationObserver
		if composition.localValidator != nil {
			validationMetrics, metricsErr := validator.NewPrometheusValidationMetrics(registry)
			if metricsErr != nil {
				closeErr := manager.Close(context.Background())

				return nil, errors.Join(
					fmt.Errorf("validator composition: register validator metrics: %w", metricsErr),
					closeErr,
				)
			}
			validationObserver = validator.NewBlockStatsObserver(blockStats, validationMetrics.Observer())
		}

		factories := hooks.ExtensionComposer{
			newValidatorNetworkOwnerFactory(manager),
		}
		if composition.standaloneCollator != nil {
			factories = append(factories, newStandaloneCollatorFactory(
				*composition.standaloneCollator,
				manager,
				core.NewBlockStatsObserver(blockStats, collatorMetrics.Observer(core.MetricsModeStandalone)),
			))
		}
		if composition.localValidator != nil {
			newRemoteCollator := func(collatorID [32]byte) (core.Collator, error) {
				transport, transportErr := validatornet.NewRemoteCollatorClient(manager, collatorID)
				if transportErr != nil {
					return nil, transportErr
				}

				return core.NewRemoteCollator(transport)
			}
			factories = append(factories, newValidatorControlFactory(
				composition.localValidator.control,
				composition.localValidator.keys,
				composition.localADNLID,
				blockStats,
			))
			factories = append(factories, newLocalValidatorFactory(
				*composition.localValidator,
				manager,
				validationObserver,
				core.NewBlockStatsObserver(blockStats, collatorMetrics.Observer(core.MetricsModeValidator)),
				newRemoteCollator,
			))
		}

		return factories.New(node)
	}
}

type validatorNetworkOwner struct {
	manager *validatornet.Manager
}

func newValidatorNetworkOwnerFactory(manager *validatornet.Manager) hooks.ExtensionFactory {
	return func(hooks.Node) (hooks.Extension, error) {
		return &validatorNetworkOwner{manager: manager}, nil
	}
}

func (*validatorNetworkOwner) Start(ctx context.Context) error {
	return ctx.Err()
}

func (o *validatorNetworkOwner) Close(ctx context.Context) error {
	return o.manager.Close(ctx)
}

func (*validatorNetworkOwner) OnBlockApplied(context.Context, hooks.BlockAppliedEvent) error {
	return nil
}

func (*validatorNetworkOwner) OnExternalMessage(context.Context, hooks.ExternalMessageEvent) error {
	return nil
}

func (*validatorNetworkOwner) OnBlockReceived(context.Context, hooks.BlockReceivedEvent) error {
	return nil
}

func newLocalValidatorFactory(
	composition localValidatorComposition,
	network validatorNetwork,
	validationObserver validator.ValidationObserver,
	collationObserver core.CollationObserver,
	newRemoteCollator sessionCollatorFactory,
) hooks.ExtensionFactory {
	return func(node hooks.Node) (hooks.Extension, error) {
		if node.Store == nil || node.Network == nil || node.TVM == nil {
			return nil, errors.New("validator composition: node store, network, and TVM are required")
		}
		store, err := consensusLiveView("validator", node.Store)
		if err != nil {
			return nil, err
		}
		localNode := localSessionNode{store: store, network: node.Network}
		// Collation reads state through the same seam as validation. Sharing the
		// seam is what guarantees the two receive the SAME materialization of a
		// parent, which chain_state.go's live-successor carry-back requires.
		collatorStore := localCollatorStateStore{localSessionNode: localNode, artifacts: store}

		shardTops, err := core.NewShardTopInbox(core.ShardTopInboxOptions{})
		if err != nil {
			return nil, fmt.Errorf("validator composition: create shard top inbox: %w", err)
		}
		semantics := core.NewSemanticVerifier(node.TVM)
		{
			recomputeLog := node.Logger.With().Str("component", "validator").Logger()
			semantics.SetStorageStatRecomputeObserver(func(chain core.MetricChain, account [32]byte) {
				if validationObserver != nil {
					validationObserver.AddStorageStatRecompute(chain)
				}
				recomputeLog.Warn().
					Hex("account", account[:]).
					Msg("candidate storage-stat proof fell short of the replay walk; stat recomputed from state")
			})
		}
		acquisition, err := core.NewLocalAcquisition(core.LocalAcquisitionOptions{
			Builder:                core.NewBuilder(node.TVM, core.SupportedSoftware()),
			Logger:                 node.Logger.With().Str("component", "validator").Logger(),
			CollationObserver:      collationObserver,
			ValidationObserver:     validationObserver,
			Store:                  collatorStore,
			Groups:                 composition.runtime.Groups,
			Messages:               composition.runtime.Messages,
			ShardTops:              shardTops,
			Semantics:              semantics,
			AccountPrewarmer:       node.AccountPrewarmer,
			AccountPrewarmCapacity: node.AccountPrewarmCapacity,
		})
		if err != nil {
			return nil, fmt.Errorf("validator composition: create local acquisition: %w", err)
		}

		router := validator.NewLocalCandidateRouter()
		localCollator, err := core.NewService(core.ServiceOptions{
			ProductionMode:     core.ProductionModeSelf,
			Storage:            composition.collatorStorage,
			Pipeline:           acquisition,
			Keys:               composition.collatorKeys,
			CollatorKeyID:      composition.collatorKeyID,
			Observer:           collationObserver,
			Logger:             node.Logger.With().Str("component", "validator").Logger(),
			AllowAllValidators: true,
			Emit:               router.EmitCandidate,
		})
		if err != nil {
			return nil, fmt.Errorf("validator composition: create local collator: %w", err)
		}

		log := node.Logger.With().Str("component", "validator").Logger()
		prepare := func(
			ctx context.Context,
			config validator.SessionConfig,
			initial validator.SessionState,
			_ validator.SessionStart,
		) (validator.SessionRuntime, error) {
			if config.Identity.ADNLID != network.LocalADNLID() {
				return nil, fmt.Errorf(
					"validator composition: session %x requires unavailable ADNL identity %x",
					config.SessionID,
					config.Identity.ADNLID,
				)
			}

			sessionNetwork, prepareErr := prepareValidatorSessionNetwork(ctx, network, config)
			if prepareErr != nil {
				return nil, fmt.Errorf("validator composition: prepare session network: %w", prepareErr)
			}
			if sessionNetwork == nil {
				retireErr := retireValidatorSession(network, config.SessionID)

				return nil, errors.Join(
					errors.New("validator composition: session network is absent"),
					retireErr,
				)
			}
			if config.Identity.Validator == nil && config.Protocol.Version == 2 &&
				config.Protocol.ProtocolVersion == 1 {
				return prepareBlockSyncObserverSession(ctx, network, config, sessionNetwork)
			}
			production, prepareErr := prepareSessionProduction(
				ctx,
				config,
				localCollator,
				router,
				newRemoteCollator,
			)
			if prepareErr != nil {
				retireErr := retireValidatorSession(network, config.SessionID)

				return nil, errors.Join(prepareErr, retireErr)
			}
			backendPreparation, prepareErr := validator.PrepareLocalSessionBackend(ctx, validator.LocalSessionBackendOptions{
				Config:          config,
				Initial:         initial,
				Node:            localNode,
				Groups:          composition.runtime.Groups,
				Storage:         composition.options.Storage,
				Delegations:     composition.delegations,
				Acquisition:     acquisition,
				Collator:        production.producer,
				ProductionMode:  production.mode,
				CandidateRouter: production.candidateRouter,
				Publisher:       network,
				ShardTops:       shardTops,
				Metrics:         validationObserver,
				Logger:          log,
			})
			if prepareErr != nil {
				collatorErr := closeValidatorSessionCollator(production.ownedCollator)
				retireErr := retireValidatorSession(network, config.SessionID)

				return nil, errors.Join(
					fmt.Errorf("validator composition: prepare local session backend: %w", prepareErr),
					collatorErr,
					retireErr,
				)
			}
			backend := backendPreparation.Backend

			session, prepareErr := validator.PrepareSessionRuntime(
				ctx,
				config,
				backendPreparation.RuntimeState,
				validator.RuntimeOptions{
					Storage:    composition.options.Storage,
					Network:    sessionNetwork,
					Backend:    backend,
					Limits:     config.CandidateLimits,
					Observer:   validationObserver,
					Logger:     &log,
					CaptureDir: composition.options.CandidateCaptureDir,
				},
			)
			if prepareErr != nil {
				collatorErr := closeValidatorSessionCollator(production.ownedCollator)
				retireErr := retireValidatorSession(network, config.SessionID)

				return nil, errors.Join(prepareErr, backend.Close(), collatorErr, retireErr)
			}

			return &validatorSessionRuntime{
				SessionRuntime: session,
				network:        network,
				sessionID:      config.SessionID,
				ownedCollator:  production.ownedCollator,
			}, nil
		}

		options := composition.options
		options.LocalCollator = localCollator
		options.MasterchainViews = acquisition
		options.ShardTops = shardTops
		options.PrepareSession = prepare
		options.ObserverADNLIDs = [][32]byte{network.LocalADNLID()}
		options.Metrics = validationObserver

		return validator.New(options)(node)
	}
}

type standaloneCollatorComposition struct {
	runtime             *validator.Runtime
	validatorStorage    validator.ValidatorStorage
	collatorStorage     core.CollatorStorage
	keys                core.SigningKeys
	keyID               [32]byte
	allowedValidators   map[[32]byte]struct{}
	allowAllValidators  bool
	candidateCaptureDir string
}

func newStandaloneCollatorFactory(
	composition standaloneCollatorComposition,
	network validatorNetwork,
	collationObserver core.CollationObserver,
) hooks.ExtensionFactory {
	return func(node hooks.Node) (hooks.Extension, error) {
		if node.Store == nil || node.Network == nil || node.TVM == nil {
			return nil, errors.New("collator composition: node store, network, and TVM are required")
		}
		store, err := consensusLiveView("collator", node.Store)
		if err != nil {
			return nil, err
		}
		localNode := localSessionNode{store: store, network: node.Network}
		collatorStore := localCollatorStateStore{localSessionNode: localNode, artifacts: store}

		shardTops, err := core.NewShardTopInbox(core.ShardTopInboxOptions{})
		if err != nil {
			return nil, fmt.Errorf("collator composition: create shard top inbox: %w", err)
		}
		semantics := core.NewSemanticVerifier(node.TVM)
		{
			recomputeLog := node.Logger.With().Str("component", "collator").Logger()
			semantics.SetStorageStatRecomputeObserver(func(_ core.MetricChain, account [32]byte) {
				recomputeLog.Warn().
					Hex("account", account[:]).
					Msg("candidate storage-stat proof fell short of the replay walk; stat recomputed from state")
			})
		}
		acquisition, err := core.NewLocalAcquisition(core.LocalAcquisitionOptions{
			Builder:                core.NewBuilder(node.TVM, core.SupportedSoftware()),
			Logger:                 node.Logger.With().Str("component", "collator").Logger(),
			CollationObserver:      collationObserver,
			Store:                  collatorStore,
			Groups:                 composition.runtime.Groups,
			Messages:               composition.runtime.Messages,
			ShardTops:              shardTops,
			Semantics:              semantics,
			AccountPrewarmer:       node.AccountPrewarmer,
			AccountPrewarmCapacity: node.AccountPrewarmCapacity,
		})
		if err != nil {
			return nil, fmt.Errorf("collator composition: create local acquisition: %w", err)
		}

		log := node.Logger.With().Str("component", "collator").Logger()
		observer, err := validator.NewConsensusObserver(validator.ConsensusObserverOptions{
			Network:    network,
			Storage:    composition.validatorStorage,
			Node:       localNode,
			Groups:     composition.runtime.Groups,
			Publisher:  network,
			ShardTops:  shardTops,
			Logger:     log,
			CaptureDir: composition.candidateCaptureDir,
		})
		if err != nil {
			return nil, fmt.Errorf("collator composition: create consensus observer: %w", err)
		}
		backend, err := core.NewService(core.ServiceOptions{
			ProductionMode:     core.ProductionModeDelegated,
			Storage:            composition.collatorStorage,
			Pipeline:           acquisition,
			Keys:               composition.keys,
			CollatorKeyID:      composition.keyID,
			Observer:           collationObserver,
			Logger:             log,
			AllowedValidators:  composition.allowedValidators,
			AllowAllValidators: composition.allowAllValidators,
			Emit:               observer.BroadcastCandidate,
		})
		if err != nil {
			return nil, fmt.Errorf("collator composition: create backend: %w", err)
		}
		// One feed per pool: the controller owns its destination projection and
		// the extension drives it per applied block. A standalone collator that
		// skipped this reads its neighbours' out-queues out of state on every
		// window entry instead of off runs kept at the head.
		feed := msgpool.NewFeed(msgpool.FeedOptions{
			Pool:      composition.runtime.Messages,
			Logger:    log,
			Prewarmer: node.AccountPrewarmer,
		})
		controller, err := core.NewController(core.ControllerOptions{
			Backend:     backend,
			Storage:     composition.collatorStorage,
			Observer:    observer,
			Tracker:     composition.runtime.Groups,
			Acquisition: acquisition,
			Feed:        feed,
			Logger:      log,
		})
		if err != nil {
			return nil, fmt.Errorf("collator composition: create controller: %w", err)
		}

		return standalone.New(standalone.Options{
			Controller: controller,
			ShardTops:  shardTops,
			Messages:   composition.runtime.Messages,
			Feed:       feed,
		})(node)
	}
}
