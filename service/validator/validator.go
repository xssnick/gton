package validator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/p2p"
	sharddomain "github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// SigningKeys is the validator-owned signing capability. Consensus sessions
// receive a signer bound to one of these IDs; private key material never
// crosses the keyring boundary.
type SigningKeys interface {
	KeyIDs() [][32]byte
	Sign(keyID [32]byte, payload []byte) ([]byte, error)
}

// Options configures the validator extension with dependencies prepared by the
// composition root.
type Options struct {
	// Keys signs validator payloads by TON short key ID. The composition root
	// owns the concrete keyring and injects only this capability.
	Keys SigningKeys
	// Storage owns durable consensus journals, candidates, and session
	// telemetry. Its lifecycle remains with the composition root.
	Storage ValidatorStorage
	// Runtime owns the group tracker and message pool shared with an in-process
	// local collator. The composition root closes it after all extensions stop.
	Runtime *Runtime
	// FreshnessWindow gates block bookkeeping by generation time. Historical
	// catch-up blocks do not update the message pool inline. 0 means 5m,
	// negative processes every block.
	FreshnessWindow time.Duration
	// ConsensusCatchupThreshold is the maximum masterchain age that admits
	// consensus immediately. An older head is admitted only after it settles and
	// the node knows no newer signed masterchain block. The gate is startup-only:
	// transient lag never stops an already admitted session. 0 means 80s,
	// negative disables the gate.
	ConsensusCatchupThreshold time.Duration
	// HeadSettleDelay arms the pool from an old chain head: a stale block
	// unsuperseded for this long is processed anyway — a halted chain
	// leaves an old block at the head, and the restarted collator still
	// needs the view of exactly that state. 0 means 3s.
	HeadSettleDelay time.Duration
	// StatsInterval drives periodic pool stats logging; 0 means 5m,
	// negative disables.
	StatsInterval time.Duration
	// DisableInternals turns off the internal-message section feed.
	DisableInternals bool
	// EnableGroups turns on the validator-group tracker when internals are
	// disabled. An enabled internal-message feed always enables it because the
	// tracker is the source of active and prepared destination topology.
	EnableGroups bool
	// PrepareSession creates inactive consensus runtimes for local group
	// validators and persistent-validator observers. Nil keeps the tracker
	// enabled without a consensus runtime, which is useful until transport and
	// candidate/finality services are wired by the composition root.
	PrepareSession SessionPreparer
	// ObserverADNLIDs is the complete set of persistent-overlay identities for
	// which the composition root can provide a local transport. Signing-key
	// ownership alone does not imply that this process owns every associated
	// ADNL identity.
	ObserverADNLIDs [][32]byte
	// Metrics receives bounded validator metrics, including session
	// specification rejection transitions. Nil disables those metrics.
	Metrics ValidationObserver
	// LocalCollator is the in-process self-production backend used by voting
	// sessions which do not select a configured remote collator. The validator
	// lifecycle starts it before preparing sessions and closes it after every
	// session runtime has stopped.
	LocalCollator collator.Collator
	// MasterchainViews installs the coherent block/state pair delivered by the
	// applied-block hook into local acquisition before session reconciliation.
	// It is optional for validators backed by an out-of-process collator.
	MasterchainViews collator.MasterchainViewPublisher
	// ShardTops retains verified shard descriptors for local masterchain
	// collation. Supplying it also enables the optional node observer hook.
	ShardTops *collator.ShardTopInbox
	// CandidateCaptureDir is the directory under the node data dir where a
	// candidate refused with a TVM/semantic replay error is dumped for offline
	// replay. Empty disables capture.
	CandidateCaptureDir string
}

// New returns an extension factory with explicit options.
func New(opts Options) hooks.ExtensionFactory {
	return func(n hooks.Node) (hooks.Extension, error) {
		resolved := opts
		if resolved.FreshnessWindow == 0 {
			resolved.FreshnessWindow = 5 * time.Minute
		}
		if resolved.ConsensusCatchupThreshold == 0 {
			resolved.ConsensusCatchupThreshold = 80 * time.Second
		}
		if resolved.HeadSettleDelay < 0 {
			return nil, fmt.Errorf("validator: head settle delay must not be negative")
		}
		if resolved.HeadSettleDelay == 0 {
			resolved.HeadSettleDelay = 3 * time.Second
		}
		if resolved.StatsInterval == 0 {
			resolved.StatsInterval = 5 * time.Minute
		}
		// Internal-message destinations are consensus topology, so their feed
		// cannot run without the masterchain group tracker that owns the active
		// and prepared shard set.
		if !resolved.DisableInternals {
			resolved.EnableGroups = true
		}

		if resolved.Keys == nil {
			return nil, errors.New("validator: signing keys are required")
		}
		if resolved.Storage == nil {
			return nil, errors.New("validator: storage is required")
		}
		if resolved.Runtime == nil || resolved.Runtime.Groups == nil || resolved.Runtime.Messages == nil {
			return nil, errors.New("validator: shared runtime is required")
		}
		if resolved.PrepareSession != nil && !resolved.EnableGroups {
			return nil, errors.New("validator: session preparer requires validator groups")
		}
		for i, id := range resolved.ObserverADNLIDs {
			if id == ([32]byte{}) {
				return nil, fmt.Errorf("validator: observer ADNL ID %d is zero", i)
			}
		}

		log := n.Logger.With().Str("component", "validator").Logger()
		s := &Service{
			log:              log,
			opts:             resolved,
			store:            n.Store,
			validatorStore:   resolved.Storage,
			pool:             resolved.Runtime.Messages,
			accountPrewarmer: n.AccountPrewarmer,
			tracker:          resolved.Runtime.Groups,
			localCollator:    resolved.LocalCollator,
			masterchainViews: resolved.MasterchainViews,
			masterchainHead:  n.MasterchainHead,
			pending:          map[msgpool.ShardIdent]pendingHead{},
			processing:       map[msgpool.ShardIdent]*sourceProcessing{},
			hooksIdle:        make(chan struct{}),
			closeDone:        make(chan struct{}),
			closeGate:        make(chan struct{}, 1),
		}
		s.closeGate <- struct{}{}
		if resolved.PrepareSession != nil {
			s.sessions = newSessionSupervisor(
				log,
				resolved.Keys,
				resolved.Storage,
				resolved.PrepareSession,
				resolved.ObserverADNLIDs...,
			)
			s.sessions.metrics = resolved.Metrics
		}
		close(s.hooksIdle)
		s.runCtx, s.cancel = context.WithCancel(context.Background())
		if n.Commands != nil {
			if err := n.Commands.Register("status validator", s.handleStatus); err != nil {
				s.cancel()

				return nil, fmt.Errorf("register validator status command: %w", err)
			}
			if err := n.Commands.Register("debug validator", s.handleDebug); err != nil {
				s.cancel()

				return nil, fmt.Errorf("register validator debug command: %w", err)
			}
			if s.localCollator != nil {
				if err := n.Commands.RegisterDefault("debug collator", s.handleCollatorDebug); err != nil {
					s.cancel()

					return nil, fmt.Errorf("register integrated collator debug command: %w", err)
				}
			}
		}
		if resolved.ShardTops != nil {
			return &serviceWithShardTops{Service: s, shardTops: resolved.ShardTops}, nil
		}

		return s, nil
	}
}

type serviceWithShardTops struct {
	*Service
	shardTops *collator.ShardTopInbox
}

var _ hooks.ShardTopBlockDescriptionObserver = (*serviceWithShardTops)(nil)

func (s *serviceWithShardTops) OnShardTopBlockDescription(
	ctx context.Context,
	description *p2p.ShardBlockDescription,
	root *cell.Cell,
) error {
	return s.shardTops.StoreShardTopDescription(ctx, description, root)
}

// Service is the validator extension. See the package comment for the
// current scope.
type Service struct {
	log              zerolog.Logger
	opts             Options
	store            groups.MasterchainHistory
	validatorStore   ValidatorStorage
	pool             *msgpool.Pool
	accountPrewarmer hooks.AccountPrewarmer

	internalsTopologyMu sync.RWMutex
	// internalsTopology is nil until the first exact masterchain projection is
	// published. Immutable topology values are shared with concurrent feeds
	// under internalsTopologyMu.
	internalsTopology *msgpool.Topology

	tracker          *GroupTracker
	sessions         *sessionSupervisor
	localCollator    collator.Collator
	masterchainViews collator.MasterchainViewPublisher
	masterchainHead  hooks.MasterchainHead

	runCtx context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	lifecycleMu    sync.Mutex
	started        bool
	starting       bool
	closed         bool
	startCancel    context.CancelFunc
	startDone      chan struct{}
	activeHooks    int
	hooksIdle      chan struct{}
	closeDone      chan struct{}
	closeGate      chan struct{}
	collatorClosed bool
	closeLogged    bool

	// pending holds the newest stale block per source; a settled entry (no
	// newer block superseded it) is the actual chain head and gets
	// processed despite its age.
	pendingMu sync.Mutex
	pending   map[msgpool.ShardIdent]pendingHead

	processingMu sync.Mutex
	processing   map[msgpool.ShardIdent]*sourceProcessing

	// sizeUnverified latches the one-time warning about states that do not
	// store the out-queue size.
	sizeUnverified atomic.Bool

	// consensusAdmitted is a one-way startup gate. C++ starts validator groups
	// from the synchronized head rather than every historical state seen during
	// catch-up; after admission it keeps them alive through transient lag.
	consensusAdmitted atomic.Bool
	consensusDeferred atomic.Bool

	consensusMu      sync.Mutex
	consensusPending *pendingConsensusSnapshot
	consensusStartMu sync.Mutex
	consensusStarted bool
}

type pendingConsensusSnapshot struct {
	snapshot *groups.Snapshot
	at       time.Time
}

// pendingHead is one deferred stale-head candidate.
type pendingHead struct {
	ev hooks.BlockAppliedEvent
	at time.Time
}

// sourceProcessing serializes block bookkeeping for one shard chain and
// remembers its high-water mark. A settled stale head can race a fresh apply,
// but it can never run after a newer block has committed its bookkeeping.
type sourceProcessing struct {
	mu               sync.Mutex
	processed        bool
	seqno            uint32
	internalsTargets []internalsFeedTarget
}

type internalsFeedAction uint8

const (
	feedApply internalsFeedAction = iota + 1
	feedReseed

	maxRetainedInternalsFeedTargets = 1024
	groupBootstrapRetryDelay        = 100 * time.Millisecond
)

type internalsFeedTarget struct {
	destination msgpool.ShardIdent
	action      internalsFeedAction
}

// Start initializes the group view and launches the event loop and pool janitor.
func (s *Service) Start(ctx context.Context) error {
	s.lifecycleMu.Lock()
	if s.started || s.starting {
		s.lifecycleMu.Unlock()

		return fmt.Errorf("validator: already started")
	}
	if s.closed {
		s.lifecycleMu.Unlock()

		return fmt.Errorf("validator: already closed")
	}

	startCtx, cancelStart := context.WithCancel(ctx)
	startDone := make(chan struct{})
	s.starting = true
	s.startCancel = cancelStart
	s.startDone = startDone
	hooksIdle := s.hooksIdle
	s.lifecycleMu.Unlock()

	var startErr error
	select {
	case <-hooksIdle:
	case <-startCtx.Done():
		startErr = startCtx.Err()
	}
	deferredGroups := false
	if startErr == nil && s.opts.EnableGroups {
		startErr = s.initializeGroupTracker(startCtx)
		if errors.Is(startErr, groups.ErrNoSnapshot) {
			deferredGroups = true
			startErr = nil
		}
	}
	// A recovered local collator activates durable sessions against the exact
	// validator-group snapshot. Bootstrap and admit that snapshot first; during
	// catch-up, starting the collator here would recover an obsolete window.
	if startErr == nil && !deferredGroups && s.consensusReadyToStart() {
		startErr = s.startConsensusServices()
	}
	if startErr == nil {
		startErr = startCtx.Err()
	}

	s.lifecycleMu.Lock()
	s.starting = false
	s.startCancel = nil
	s.startDone = nil
	close(startDone)
	if startErr != nil {
		s.lifecycleMu.Unlock()
		cancelStart()

		return startErr
	}
	if s.closed {
		s.lifecycleMu.Unlock()
		cancelStart()
		if s.sessions != nil {
			s.sessions.Close()
		}

		return context.Canceled
	}

	s.started = true
	workers := 2
	if deferredGroups {
		workers++
	}
	s.wg.Add(workers)
	go func() {
		defer s.wg.Done()
		s.loop()
	}()
	if deferredGroups {
		go func() {
			defer s.wg.Done()
			s.awaitGroupTracker()
		}()
	}
	// Stop consensus as soon as the extension lifetime ends. The composition
	// root keeps P2P alive until Close retires every session overlay.
	go func() {
		defer s.wg.Done()

		select {
		case <-ctx.Done():
			s.cancel()
		case <-s.runCtx.Done():
		}
	}()
	s.lifecycleMu.Unlock()
	cancelStart()

	s.log.Info().
		Bool("consensus_sessions", s.sessions != nil).
		Bool("waiting_for_current_state", deferredGroups).
		Msg("validator extension started")
	return nil
}

func (s *Service) startConsensusServices() error {
	s.consensusStartMu.Lock()
	defer s.consensusStartMu.Unlock()
	if s.consensusStarted {
		return nil
	}
	if err := s.runCtx.Err(); err != nil {
		return err
	}
	if s.localCollator != nil {
		if err := s.localCollator.Start(s.runCtx); err != nil {
			return err
		}
	}
	if err := s.runCtx.Err(); err != nil {
		return err
	}
	if s.sessions != nil {
		s.sessions.Start(s.runCtx)
	}
	s.consensusStarted = true

	return nil
}

func (s *Service) consensusReadyToStart() bool {
	return s.sessions == nil || s.consensusAdmitted.Load()
}

func (s *Service) awaitGroupTracker() {
	timer := time.NewTimer(groupBootstrapRetryDelay)
	defer timer.Stop()

	for {
		select {
		case <-s.runCtx.Done():
			return
		case <-timer.C:
		}

		err := s.initializeGroupTracker(s.runCtx)
		if err == nil {
			if s.consensusReadyToStart() {
				if err = s.startConsensusServices(); err != nil {
					if !errors.Is(err, context.Canceled) {
						s.log.Error().Err(err).Msg("validator consensus services did not start")
					}
					s.cancel()

					return
				}

				s.log.Info().Bool("consensus_sessions", s.sessions != nil).
					Msg("validator current state became available, consensus services started")
			} else {
				s.log.Info().Msg("validator group snapshot became available, consensus admission pending")
			}

			return
		}
		if !errors.Is(err, groups.ErrNoSnapshot) {
			s.log.Warn().Err(err).Dur("retry_in", groupBootstrapRetryDelay).
				Msg("validator group bootstrap failed, will retry")
		}

		timer.Reset(groupBootstrapRetryDelay)
	}
}

// Close stops hook processing and the event loop. The composition root closes
// the shared Runtime after every validator/collator consumer has stopped.
func (s *Service) Close(ctx context.Context) error {
	s.lifecycleMu.Lock()
	done := s.closeDone
	if !s.closed {
		s.closed = true
		startCancel := s.startCancel
		var startDone <-chan struct{}
		if s.starting {
			startDone = s.startDone
		}
		started := s.started
		hooksIdle := s.hooksIdle

		if startCancel != nil {
			startCancel()
		}
		go func() {
			if startDone != nil {
				<-startDone
			}
			<-hooksIdle
			s.cancel()
			if started {
				s.wg.Wait()
			}
			if s.sessions != nil {
				s.sessions.Close()
			}
			close(done)
		}()
	}
	s.lifecycleMu.Unlock()

	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	if s.localCollator != nil {
		select {
		case <-s.closeGate:
		case <-ctx.Done():
			return ctx.Err()
		}
		defer func() { s.closeGate <- struct{}{} }()

		s.lifecycleMu.Lock()
		collatorClosed := s.collatorClosed
		s.lifecycleMu.Unlock()
		if !collatorClosed {
			if err := s.localCollator.Close(ctx); err != nil {
				return err
			}
			s.lifecycleMu.Lock()
			s.collatorClosed = true
			s.lifecycleMu.Unlock()
		}
	}

	s.lifecycleMu.Lock()
	if !s.closeLogged {
		s.closeLogged = true
		s.lifecycleMu.Unlock()
		s.log.Info().Msg("validator extension stopped")

		return nil
	}
	s.lifecycleMu.Unlock()

	return nil
}

// OnExternalMessage pools an admitted external message synchronously — the
// ingress layer already ran the TVM admission, the pool only indexes the
// message (~1µs) and its caps are the admission limit: an overfull pool
// drops externals and nothing else. Never returns an error: erroring here
// would drop the message from the node relay path too.
func (s *Service) OnExternalMessage(_ context.Context, ev hooks.ExternalMessageEvent) error {
	priority := msgpool.ExternalPriorityBroadcast
	if ev.IsLocal {
		priority = msgpool.ExternalPriorityLocal
	}
	result, err := s.pool.AddExternal(
		ev.SerializedSize,
		ev.MessageRoot,
		ev.MessageParsed,
		priority,
	)
	if err != nil {
		s.log.Debug().Err(err).Msg("external message not pooled")
	} else if result.Outcome == msgpool.ExternalAddInserted && s.accountPrewarmer != nil {
		s.accountPrewarmer.EnqueueAccount(result.Destination.Workchain, result.Destination.Account)
	}
	return nil
}

// OnBlockApplied advances masterchain validator groups (a malformed
// masterchain state fails closed back to the apply pipeline; retrying the
// same valid event is idempotent), then runs the pool bookkeeping inline —
// that part never errors the node.
func (s *Service) OnBlockApplied(ctx context.Context, ev hooks.BlockAppliedEvent) error {
	if !s.beginBlockApply() {
		return nil
	}
	defer s.finishBlockApply()

	if s.opts.EnableGroups && isMasterchainEvent(ev) {
		result, buffered, err := s.tracker.applyOrBuffer(ev)
		if err != nil {
			// Applied-block producers may complete out of order: a locally
			// accepted newer masterchain block can publish its hook while an
			// older sync event is still queued. The tracker already represents
			// the newer canonical state, and replaying pool/session bookkeeping
			// for the older event would regress those views. Same-height fork
			// conflicts remain errors in Tracker.Apply.
			if errors.Is(err, groups.ErrStaleMasterchainState) {
				if _, parseErr := groups.ParseState(groups.StateInput{
					Block: ev.Meta.ID,
					Root:  ev.CurrentState,
				}); parseErr != nil {
					return fmt.Errorf("track validator groups for %s: %w", storage.FormatBlockRef(ev.Meta.ID), parseErr)
				}

				s.log.Debug().
					Str("block", storage.FormatBlockRef(ev.Meta.ID)).
					Msg("ignoring superseded masterchain applied event")

				return nil
			}

			return fmt.Errorf("track validator groups for %s: %w", storage.FormatBlockRef(ev.Meta.ID), err)
		}
		if buffered {
			s.processAppliedBlock(ev)

			return nil
		}
		s.logGroupTransitions(result.Transitions)
		if s.masterchainViews != nil {
			if err = s.masterchainViews.PublishMasterchainView(
				ctx,
				result.Snapshot,
				ev.BlockRoot,
				ev.CurrentState,
			); err != nil {
				return fmt.Errorf("publish validator masterchain view: %w", err)
			}
		}
		if !s.opts.DisableInternals {
			if err = s.reconcileInternals(result.Snapshot); err != nil {
				return fmt.Errorf("reconcile internal-message destinations: %w", err)
			}
		}
		if s.reconcileConsensus(result.Snapshot) && s.isStarted() {
			if err = s.startConsensusServices(); err != nil {
				return fmt.Errorf("start validator consensus services: %w", err)
			}
		}
	}

	s.processAppliedBlock(ev)
	return nil
}

func (s *Service) isStarted() bool {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()

	return s.started && !s.closed
}

// processAppliedBlock runs the pool bookkeeping synchronously on the apply
// path. Catch-up blocks outside the freshness window are not processed
// inline — during a sync they are superseded immediately and cost nothing —
// but the newest one is remembered per source: a halted chain leaves an old
// block at the head, and once nothing supersedes it for HeadSettleDelay the
// loop processes it anyway (see processSettledHeads).
func (s *Service) processAppliedBlock(ev hooks.BlockAppliedEvent) {
	source := msgpool.ShardIdent{Workchain: ev.Meta.ID.Workchain, Shard: uint64(ev.Meta.ID.Shard)}
	if s.opts.FreshnessWindow >= 0 &&
		time.Since(time.Unix(int64(ev.Meta.GenUTime), 0)) > s.opts.FreshnessWindow {
		s.pendingMu.Lock()
		pending, exists := s.pending[source]
		if !exists || pending.ev.Meta.ID.SeqNo <= ev.Meta.ID.SeqNo {
			s.pending[source] = pendingHead{ev: ev, at: time.Now()}
		}
		s.pendingMu.Unlock()
		return
	}

	s.pendingMu.Lock()
	if pending, exists := s.pending[source]; exists && pending.ev.Meta.ID.SeqNo <= ev.Meta.ID.SeqNo {
		delete(s.pending, source)
	}
	s.pendingMu.Unlock()
	s.processSourceBlock(source, ev)
}

// processSourceBlock applies one block under its source-chain serializer. The
// seqno check is inside the same critical section as the bookkeeping, so a
// stale head selected before a fresh apply cannot later reseed or drop the
// source behind that fresh block.
func (s *Service) processSourceBlock(source msgpool.ShardIdent, ev hooks.BlockAppliedEvent) bool {
	processing := s.sourceProcessing(source)
	processing.mu.Lock()
	defer processing.mu.Unlock()

	seqno := ev.Meta.ID.SeqNo
	if processing.processed && seqno <= processing.seqno {
		return false
	}

	s.processBlock(processing, ev)
	processing.processed = true
	processing.seqno = seqno
	return true
}

func (s *Service) sourceProcessing(source msgpool.ShardIdent) *sourceProcessing {
	s.processingMu.Lock()
	defer s.processingMu.Unlock()

	processing := s.processing[source]
	if processing == nil {
		processing = &sourceProcessing{}
		s.processing[source] = processing
	}
	return processing
}

// processBlock is the ungated bookkeeping body.
func (s *Service) processBlock(processing *sourceProcessing, ev hooks.BlockAppliedEvent) {
	// Externals imported by the applied block leave the pool.
	hashes, err := msgpool.AppliedNormHashesFromBlockRoot(ev.BlockRoot)
	if err != nil {
		s.log.Warn().Err(err).Str("block", storage.FormatBlockRef(ev.Meta.ID)).Msg("cannot extract applied externals from block")
	} else if len(hashes) > 0 {
		s.pool.EraseApplied(hashes)
		s.log.Debug().Str("block", storage.FormatBlockRef(ev.Meta.ID)).Int("normalized_hashes", len(hashes)).Msg("cleaned applied externals")
	}

	if !s.opts.DisableInternals {
		s.feedInternals(processing, ev)
	}

	// Future attachment points fed by the same event: Simplex session
	// lifecycle (stages 3/6).
}

// processSettledHeads processes stale blocks that stayed unsuperseded for
// the settle delay — the apply stream quiesced, so they are the actual
// chain heads. A fresh block racing in concurrently is serialized with the
// stale candidate, and the per-source high-water mark rejects the candidate
// if the fresh block wins.
func (s *Service) processSettledHeads() {
	now := time.Now()
	var settled []hooks.BlockAppliedEvent
	s.pendingMu.Lock()
	for source, p := range s.pending {
		if now.Sub(p.at) >= s.opts.HeadSettleDelay {
			settled = append(settled, p.ev)
			delete(s.pending, source)
		}
	}
	s.pendingMu.Unlock()

	for _, ev := range settled {
		source := msgpool.ShardIdent{Workchain: ev.Meta.ID.Workchain, Shard: uint64(ev.Meta.ID.Shard)}
		if s.processSourceBlock(source, ev) {
			s.log.Info().Str("block", storage.FormatBlockRef(ev.Meta.ID)).
				Msg("stale chain head settled, arming the pool from it")
		}
	}
}

func (s *Service) beginBlockApply() bool {
	for {
		s.lifecycleMu.Lock()
		if s.closed {
			s.lifecycleMu.Unlock()

			return false
		}
		if !s.starting {
			if s.activeHooks == 0 {
				s.hooksIdle = make(chan struct{})
			}
			s.activeHooks++
			s.lifecycleMu.Unlock()

			return true
		}
		startDone := s.startDone
		s.lifecycleMu.Unlock()

		select {
		case <-startDone:
		case <-s.runCtx.Done():
			return false
		}
	}
}

func (s *Service) finishBlockApply() {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()

	s.activeHooks--
	if s.activeHooks == 0 {
		close(s.hooksIdle)
	}
}

func (s *Service) initializeGroupTracker(ctx context.Context) error {
	if snapshot, err := s.tracker.Snapshot(); err == nil {
		return s.reconcileInitialGroupSnapshot(snapshot)
	} else if !errors.Is(err, groups.ErrNoSnapshot) {
		return fmt.Errorf("read initialized validator groups: %w", err)
	}

	transitions, err := s.tracker.Bootstrap(ctx, s.store, nil, time.Now())
	if err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	snapshot, snapshotErr := s.tracker.Snapshot()
	if snapshotErr == nil {
		if err = s.reconcileInitialGroupSnapshot(snapshot); err != nil {
			return err
		}
	} else if !errors.Is(snapshotErr, groups.ErrNoSnapshot) {
		return fmt.Errorf("read bootstrapped validator groups: %w", snapshotErr)
	}
	s.logGroupTransitions(transitions)

	return nil
}

func (s *Service) reconcileInitialGroupSnapshot(snapshot *groups.Snapshot) error {
	if !s.opts.DisableInternals {
		if err := s.reconcileInternals(snapshot); err != nil {
			return fmt.Errorf("reconcile internal-message destinations: %w", err)
		}
	}
	s.reconcileConsensus(snapshot)

	return nil
}

func (s *Service) reconcileConsensus(snapshot *groups.Snapshot) bool {
	if s.sessions == nil {
		return false
	}
	if s.consensusAdmitted.Load() {
		s.sessions.Reconcile(snapshot)

		return false
	}

	now := time.Now()
	generatedAt := time.Unix(int64(snapshot.GenUTime), 0)
	lag := now.Sub(generatedAt)
	if s.opts.ConsensusCatchupThreshold >= 0 &&
		(snapshot.GenUTime == 0 || lag > s.opts.ConsensusCatchupThreshold) {
		s.consensusMu.Lock()
		if !s.consensusAdmitted.Load() && (s.consensusPending == nil ||
			s.consensusPending.snapshot.MasterchainBlock.SeqNo <= snapshot.MasterchainBlock.SeqNo) {
			s.consensusPending = &pendingConsensusSnapshot{snapshot: snapshot, at: now}
		}
		s.consensusMu.Unlock()

		if s.consensusDeferred.CompareAndSwap(false, true) {
			s.log.Warn().
				Uint32("masterchain_seqno", snapshot.MasterchainBlock.SeqNo).
				Dur("masterchain_lag", lag).
				Msg("deferring consensus services while node catches up")
		}

		return false
	}

	return s.admitConsensus(snapshot)
}

func (s *Service) admitConsensus(snapshot *groups.Snapshot) bool {
	if !s.consensusAdmitted.CompareAndSwap(false, true) {
		s.sessions.Reconcile(snapshot)

		return false
	}

	s.consensusMu.Lock()
	s.consensusPending = nil
	s.consensusMu.Unlock()
	if s.consensusDeferred.Load() {
		s.log.Info().
			Uint32("masterchain_seqno", snapshot.MasterchainBlock.SeqNo).
			Msg("node caught up, admitting consensus sessions")
	}

	s.sessions.Reconcile(snapshot)

	return true
}

// processSettledConsensus admits an old head only when it stayed unchanged and
// the node knows no newer signed masterchain block. This is the halted-network
// escape hatch: wall-clock age alone never proves that the local node is behind.
func (s *Service) processSettledConsensus() error {
	if s.sessions == nil || s.consensusAdmitted.Load() {
		return nil
	}

	now := time.Now()
	s.consensusMu.Lock()
	pending := s.consensusPending
	if pending == nil || now.Sub(pending.at) < s.opts.HeadSettleDelay {
		s.consensusMu.Unlock()

		return nil
	}
	s.consensusMu.Unlock()

	if s.masterchainHead != nil {
		known, err := s.masterchainHead.SeenMasterchainBlock()
		if err == nil {
			if known.SeqNo > pending.snapshot.MasterchainBlock.SeqNo {
				return nil
			}
		} else if !errors.Is(err, storage.ErrNotFound) {
			return fmt.Errorf("read signed masterchain head: %w", err)
		}
	}

	s.consensusMu.Lock()
	unchanged := s.consensusPending == pending && !s.consensusAdmitted.Load()
	s.consensusMu.Unlock()
	if !unchanged || !s.admitConsensus(pending.snapshot) {
		return nil
	}

	s.log.Warn().
		Uint32("masterchain_seqno", pending.snapshot.MasterchainBlock.SeqNo).
		Msg("old masterchain head settled with no newer signed head; starting consensus for a halted network")

	return s.startConsensusServices()
}

func (s *Service) reconcileInternals(snapshot *groups.Snapshot) error {
	topology := msgpool.NewTopology(snapshot)

	s.internalsTopologyMu.Lock()
	defer s.internalsTopologyMu.Unlock()

	// The projection is a pure function of the shard configuration, which only
	// moves on a split, a merge or a session rotation, while this runs
	// synchronously on every masterchain apply. Re-publishing an identical
	// destination set and re-running a pure-predicate prune over every source of
	// every destination costs the apply path a full sweep for nothing.
	//
	// A source seeded by a collation between two sweeps is not pruned until the
	// projection actually changes. That is memory-only and self-healing: a cut
	// serves only the sources its request names, so an unreferenced run can
	// never reach a candidate.
	if s.internalsTopology != nil && s.internalsTopology.Equal(topology) {
		return nil
	}
	if err := topology.Reconcile(s.pool.Internals()); err != nil {
		return err
	}
	s.internalsTopology = &topology

	return nil
}

func isMasterchainEvent(ev hooks.BlockAppliedEvent) bool {
	return ev.Meta.ID.Workchain == -1 && ev.Meta.ID.Shard == -1<<63
}

func (s *Service) logGroupTransitions(transitions []groups.Transition) {
	for i := range transitions {
		transition := &transitions[i]
		s.log.Info().
			Str("transition", transition.Kind.String()).
			Int32("workchain", transition.Session.Shard.Workchain).
			Int64("shard", transition.Session.Shard.Shard).
			Uint32("catchain_seqno", transition.Session.CatchainSeqno).
			Hex("session_id", transition.Session.ID[:]).
			Msg("validator group lifecycle changed")
	}
}

// OnBlockReceived is not used by consensus or local collation; applied-block
// hooks provide the authenticated state transition both workflows need.
func (s *Service) OnBlockReceived(context.Context, hooks.BlockReceivedEvent) error {
	return nil
}

// loop drives the periodic work: settled stale heads and stats logging.
func (s *Service) loop() {
	var statsC <-chan time.Time
	if s.opts.StatsInterval > 0 {
		t := time.NewTicker(s.opts.StatsInterval)
		defer t.Stop()
		statsC = t.C
	}
	settle := time.NewTicker(max(s.opts.HeadSettleDelay/3, 10*time.Millisecond))
	defer settle.Stop()
	for {
		select {
		case <-s.runCtx.Done():
			return
		case <-settle.C:
			s.processSettledHeads()
			if err := s.processSettledConsensus(); err != nil {
				if !errors.Is(err, context.Canceled) {
					s.log.Error().Err(err).Msg("validator consensus services did not start")
				}
				s.cancel()

				return
			}
		case <-statsC:
			st := s.pool.Stats()
			s.log.Debug().
				Int("pooled", st.Pooled).
				Int64("pooled_bytes", st.PooledBytes).
				Uint64("added", st.Added).
				Uint64("dedup", st.DedupSkipped).
				Uint64("applied_deleted", st.AppliedDeleted).
				Uint64("included_quarantined", st.IncludedQuarantined).
				Uint64("included_released", st.IncludedReleased).
				Uint64("expired", st.Expired).
				Msg("validator message pool stats")
			if !s.opts.DisableInternals {
				ist := s.pool.Internals().Stats()
				s.log.Debug().
					Int("destinations", ist.Destinations).
					Int("sources", ist.Sources).
					Int("entries", ist.Entries).
					Uint64("applied_blocks", ist.AppliedBlocks).
					Uint64("added", ist.AppliedAdded).
					Uint64("removed", ist.AppliedRemoved).
					Uint64("seeds", ist.Seeds).
					Uint64("cuts", ist.Cuts).
					Uint64("cut_failures", ist.CutFailures).
					Msg("validator internals section stats")
			}
		}
	}
}

// feedInternals advances the internal-message section by the applied block.
// The synchronous per-chain apply order makes the feed gap-free by
// construction, so a tracked source always continues with exactly the next
// block: the fast path applies its out-queue delta and verifies the tracked
// queue size against the size the post-state stores. Everything else (first
// sight of the source, a malformed delta, or a size mismatch)
// reseeds the run from the post-state — the single recovery path.
func (s *Service) feedInternals(processing *sourceProcessing, ev hooks.BlockAppliedEvent) {
	// A block feed and a masterchain topology transition must observe one
	// generation together. Feeds for different sources still run concurrently;
	// the exclusive path is taken only on masterchain reconciliation.
	s.internalsTopologyMu.RLock()
	defer s.internalsTopologyMu.RUnlock()

	internals := s.pool.Internals()
	id := ev.Meta.ID
	source := msgpool.ShardIdent{Workchain: id.Workchain, Shard: uint64(id.Shard)}
	topology := s.internalsTopology
	if topology != nil && !topology.ContainsSource(source) {
		return
	}

	ref := msgpool.SourceRef{Seqno: id.SeqNo}
	copy(ref.RootHash[:], id.RootHash)

	targets := processing.internalsTargets[:0]
	defer func() {
		clear(targets)
		if cap(targets) > maxRetainedInternalsFeedTargets {
			processing.internalsTargets = nil
		} else {
			processing.internalsTargets = targets[:0]
		}
	}()

	applyCount := 0
	reseedCount := 0
	var prewarmed pooledInternalsPrewarmSeen
	if s.accountPrewarmer != nil {
		prewarmed.accounts = make(map[pooledAccountPrewarmKey]struct{})
		prewarmed.envelopes = make(map[cell.Hash]struct{})
	}
	internals.VisitDestinations(func(destination msgpool.ShardIdent) bool {
		if !sharddomain.IsNeighbor(
			destination.Workchain,
			int64(destination.Shard),
			source.Workchain,
			int64(source.Shard),
		) {
			return true
		}

		top, err := internals.SourceTop(destination, source)
		switch {
		case err == nil && id.SeqNo > top.Seqno:
			targets = append(targets, internalsFeedTarget{destination: destination, action: feedApply})
			applyCount++
		case err == nil && id.SeqNo == top.Seqno && ref.RootHash != top.RootHash:
			// A preloaded or restored view may name another same-height
			// candidate. The applied state is authoritative; never accept the
			// seqno alone as an idempotency key.
			targets = append(targets, internalsFeedTarget{destination: destination, action: feedReseed})
			reseedCount++
		case errors.Is(err, msgpool.ErrNotFound):
			targets = append(targets, internalsFeedTarget{destination: destination, action: feedReseed})
			reseedCount++
		case err != nil:
			s.log.Error().Err(err).Str("block", storage.FormatBlockRef(id)).
				Int32("destination_workchain", destination.Workchain).
				Uint64("destination_shard", destination.Shard).
				Msg("internals source position unavailable")
		}
		return true
	})
	if applyCount == 0 && reseedCount == 0 {
		return
	}

	var expected *uint64
	var sizeErr error
	if ev.CurrentState != nil {
		queueSize, err := msgpool.QueueSizeFromStateRoot(ev.CurrentState)
		switch {
		case err == nil:
			expected = &queueSize
		case errors.Is(err, msgpool.ErrQueueSizeNotStored):
			if s.sizeUnverified.CompareAndSwap(false, true) {
				s.log.Warn().Err(err).Str("block", storage.FormatBlockRef(id)).
					Msg("state stores no out-queue size, internals size invariant is unverified")
			}
		default:
			sizeErr = fmt.Errorf("read state out-queue size: %w", err)
		}
	}

	// applyCount is not maintained past this gate: only reseedCount decides
	// the recovery path below.
	//
	// The merge below advances one cursor over targets while walking routed,
	// which is correct only because both are derived from the same immutable
	// internalsSnapshot.destinations and are therefore in the order
	// Internals.ReconcileDestinations imposes with msgpool.CompareShardIdent.
	// The same holds for the seeds merge further down.
	var downgrade error
	var downgradeCause string
	if applyCount > 0 && sizeErr == nil {
		routed, err := internals.DeltasFromBlockRoot(source, ref, ev.BlockRoot, ev.Meta.StartLT)
		if err == nil {
			targetIndex := 0
			for index := range routed {
				destination := routed[index].Destination
				for targetIndex < len(targets) && msgpool.CompareShardIdent(targets[targetIndex].destination, destination) < 0 {
					targetIndex++
				}
				if targetIndex == len(targets) || targets[targetIndex].destination != destination ||
					targets[targetIndex].action != feedApply {
					continue
				}
				routed[index].Delta.ExpectedQueueSize = expected
				if applyErr := internals.ApplyBlock(destination, source, ref, routed[index].Delta); applyErr != nil {
					targets[targetIndex].action = feedReseed
					reseedCount++
					s.log.Warn().Err(applyErr).Str("block", storage.FormatBlockRef(id)).
						Int32("destination_workchain", destination.Workchain).
						Uint64("destination_shard", destination.Shard).
						Msg("internals delta not applied, reseeding destination")
				} else {
					s.prewarmPooledInternals(routed[index].Delta.Added, &prewarmed)
				}
			}
		} else {
			downgrade, downgradeCause = err, "internals delta parse failed, reseeding destinations"
		}
	} else if sizeErr != nil {
		downgrade, downgradeCause = sizeErr, "internals size verification failed, reseeding destinations"
	}
	if downgrade != nil {
		// A block-wide failure says nothing about individual destinations, so
		// every apply target falls back to the single recovery path.
		s.log.Warn().Err(downgrade).Str("block", storage.FormatBlockRef(id)).Msg(downgradeCause)
		for index := range targets {
			if targets[index].action == feedApply {
				targets[index].action = feedReseed
				reseedCount++
			}
		}
	}
	if reseedCount == 0 {
		return
	}

	if ev.CurrentState == nil {
		dropReseedTargets(internals, targets, source)
		s.log.Warn().Str("block", storage.FormatBlockRef(id)).
			Int("destinations", reseedCount).
			Msg("applied block carries no state, internals sources dropped")
		return
	}

	seeds, total, err := internals.SeedsFromStateRoot(source, ref, ev.CurrentState)
	if err != nil {
		dropReseedTargets(internals, targets, source)
		s.log.Warn().Err(err).Str("block", storage.FormatBlockRef(id)).
			Int("destinations", reseedCount).
			Msg("internals reseed failed, sources dropped")
		return
	}
	seeded := 0
	targetIndex := 0
	for index := range seeds {
		destination := seeds[index].Destination
		for targetIndex < len(targets) && msgpool.CompareShardIdent(targets[targetIndex].destination, destination) < 0 {
			targetIndex++
		}
		if targetIndex == len(targets) || targets[targetIndex].destination != destination ||
			targets[targetIndex].action != feedReseed {
			continue
		}
		if seedErr := internals.Seed(destination, source, ref, seeds[index].Messages, total); seedErr != nil {
			_ = internals.DropSource(destination, source)
			s.log.Warn().Err(seedErr).Str("block", storage.FormatBlockRef(id)).
				Int32("destination_workchain", destination.Workchain).
				Uint64("destination_shard", destination.Shard).
				Msg("internals destination reseed failed")
			continue
		}
		s.prewarmPooledInternals(seeds[index].Messages, &prewarmed)
		seeded++
	}
	s.log.Debug().Str("block", storage.FormatBlockRef(id)).
		Int("destinations", seeded).
		Msg("internals runs seeded")
}

// dropReseedTargets untracks the source at every destination that asked for a
// reseed. It is the terminal step of the recovery path: a run that cannot be
// reseeded must not stay behind at a position the applied state contradicts.
func dropReseedTargets(internals *msgpool.Internals, targets []internalsFeedTarget, source msgpool.ShardIdent) {
	for index := range targets {
		if targets[index].action == feedReseed {
			_ = internals.DropSource(targets[index].destination, source)
		}
	}
}
