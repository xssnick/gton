package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	standalone "github.com/xssnick/gton/extensions/collator"
	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator"
	core "github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/keyring"
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

type localSessionNode struct {
	store   hooks.Store
	network hooks.Network
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
// the standalone observer installs Delegated-v3 handlers before validator
// sessions attach, while validator endpoints retire before the shared manager.
func newValidatorStackFactory(composition validatorStackComposition) hooks.ExtensionFactory {
	return func(node hooks.Node) (hooks.Extension, error) {
		if composition.localValidator == nil && composition.standaloneCollator == nil {
			return nil, errors.New("validator composition: no validator or collator role is configured")
		}

		log := node.Logger.With().Str("component", "validator_network").Logger()
		manager, err := validatornet.NewManager(validatornet.ManagerOptions{
			PrivateOverlays: node.PrivateOverlays,
			BlockBroadcasts: node.BlockBroadcasts,
			Logger:          log,
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

		var registry core.MetricsRegistry
		if node.Metrics != nil {
			var ok bool
			registry, ok = node.Metrics.(core.MetricsRegistry)
			if !ok {
				closeErr := manager.Close(context.Background())

				return nil, errors.Join(
					errors.New("validator composition: node metrics registry has an incompatible type"),
					closeErr,
				)
			}
		}
		collatorMetrics, err := core.NewPrometheusMetrics(registry)
		if err != nil {
			closeErr := manager.Close(context.Background())

			return nil, errors.Join(
				fmt.Errorf("validator composition: register collator metrics: %w", err),
				closeErr,
			)
		}
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
			validationObserver = validationMetrics.Observer()
		}

		factories := hooks.ExtensionComposer{
			newValidatorNetworkOwnerFactory(manager),
		}
		if composition.standaloneCollator != nil {
			factories = append(factories, newStandaloneCollatorFactory(
				*composition.standaloneCollator,
				manager,
				collatorMetrics.Observer(core.MetricsModeStandalone),
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
			))
			factories = append(factories, newLocalValidatorFactory(
				*composition.localValidator,
				manager,
				validationObserver,
				collatorMetrics.Observer(core.MetricsModeValidator),
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

		shardTops, err := core.NewShardTopInbox(core.ShardTopInboxOptions{})
		if err != nil {
			return nil, fmt.Errorf("validator composition: create shard top inbox: %w", err)
		}
		acquisition, err := core.NewLocalAcquisition(core.LocalAcquisitionOptions{
			Builder:            core.NewBuilder(node.TVM, core.SupportedSoftware()),
			Logger:             node.Logger.With().Str("component", "validator").Logger(),
			CollationObserver:  collationObserver,
			ValidationObserver: validationObserver,
			Store:              node.Store,
			Groups:             composition.runtime.Groups,
			Messages:           composition.runtime.Messages,
			ShardTops:          shardTops,
			Semantics:          core.NewSemanticVerifier(node.TVM),
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

		localNode := localSessionNode{store: node.Store, network: node.Network}
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
			backend, prepareErr := validator.NewLocalSessionBackend(ctx, validator.LocalSessionBackendOptions{
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

			session, prepareErr := validator.PrepareSessionRuntime(ctx, config, initial, validator.RuntimeOptions{
				Storage:  composition.options.Storage,
				Network:  sessionNetwork,
				Backend:  backend,
				Limits:   config.CandidateLimits,
				Observer: validationObserver,
				Logger:   &log,
			})
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

		return validator.New(options)(node)
	}
}

type standaloneCollatorComposition struct {
	runtime            *validator.Runtime
	validatorStorage   validator.ValidatorStorage
	collatorStorage    core.CollatorStorage
	keys               core.SigningKeys
	keyID              [32]byte
	allowedValidators  map[[32]byte]struct{}
	allowAllValidators bool
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

		shardTops, err := core.NewShardTopInbox(core.ShardTopInboxOptions{})
		if err != nil {
			return nil, fmt.Errorf("collator composition: create shard top inbox: %w", err)
		}
		acquisition, err := core.NewLocalAcquisition(core.LocalAcquisitionOptions{
			Builder:           core.NewBuilder(node.TVM, core.SupportedSoftware()),
			Logger:            node.Logger.With().Str("component", "collator").Logger(),
			CollationObserver: collationObserver,
			Store:             node.Store,
			Groups:            composition.runtime.Groups,
			Messages:          composition.runtime.Messages,
			ShardTops:         shardTops,
			Semantics:         core.NewSemanticVerifier(node.TVM),
		})
		if err != nil {
			return nil, fmt.Errorf("collator composition: create local acquisition: %w", err)
		}

		localNode := localSessionNode{store: node.Store, network: node.Network}
		log := node.Logger.With().Str("component", "collator").Logger()
		observer, err := validator.NewConsensusObserver(validator.ConsensusObserverOptions{
			Network:   network,
			Storage:   composition.validatorStorage,
			Node:      localNode,
			Groups:    composition.runtime.Groups,
			Publisher: network,
			ShardTops: shardTops,
			Logger:    log,
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
		controller, err := core.NewController(core.ControllerOptions{
			Backend:     backend,
			Storage:     composition.collatorStorage,
			Observer:    observer,
			Tracker:     composition.runtime.Groups,
			Acquisition: acquisition,
			Messages:    composition.runtime.Messages,
			Logger:      log,
		})
		if err != nil {
			return nil, fmt.Errorf("collator composition: create controller: %w", err)
		}

		return standalone.New(standalone.Options{
			Controller: controller,
			ShardTops:  shardTops,
			Messages:   composition.runtime.Messages,
		})(node)
	}
}
