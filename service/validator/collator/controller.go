package collator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
	"github.com/xssnick/gton/service/validator/simplex"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// ConsensusObserverEvents is the private event boundary installed once at
// observer startup.
type ConsensusObserverEvents struct {
	Progressed func(context.Context, ConsensusProgress) error
	Finalized  func(context.Context, ConsensusFinalization) error
}

// ConsensusFinalization is the ordinary block named by a final certificate.
// It is distinct from masterchain inclusion and advances only the producer's
// consensus-finalized watermark.
type ConsensusFinalization struct {
	SessionID [32]byte
	Block     ton.BlockIDExt
}

// ConsensusObserverSession is the complete tentative private-overlay and
// consensus projection.
type ConsensusObserverSession struct {
	Overlay OverlaySession
	Update  SessionUpdate
	Params  simplex.Params
}

// ConsensusObserver is the sole owner of the standalone private-overlay and
// local non-voting Simplex runtimes. A concrete implementation owns
// authenticated consensus and candidate ingress, simplex.Transport, durable
// journals, candidate/state resolution, and FEC query/broadcast handling.
// LeaderWindowObserved is one authenticated output of that progression, not a
// substitute for it. PrepareSession creates the overlay and tentative runtime
// once. ActivateSession starts that same runtime and must accept an exact retry
// after returning an error. Progressed delivery is asynchronous: activation
// must never wait for the callback to complete.
type ConsensusObserver interface {
	LocalADNLID() [32]byte
	Start(context.Context, RemoteHandlers, ConsensusObserverEvents) error
	// RecoverSessions loads durable observer namespaces left by an earlier
	// process and returns their unique consensus session IDs. Desired sessions
	// are prepared from those namespaces; stale ones are retired by Controller.
	RecoverSessions(context.Context) ([][32]byte, error)
	PrepareSession(context.Context, ConsensusObserverSession) error
	ActivateSession(context.Context, SessionActivation) error
	UpdateSession(context.Context, ConsensusObserverSession) error
	RetireSession(context.Context, [32]byte) error
	Close(context.Context) error
}

// ControllerBackend extends the shared local/remote Collator contract only at
// the standalone controller boundary. Service implements the cohesive local
// progression operation; RemoteCollator remains a validator-facing client and
// deliberately does not.
type ControllerBackend interface {
	Collator
	ApplyConsensusProgress(context.Context, ConsensusProgress) error
	ObserveConsensusFinalized(context.Context, [32]byte, ton.BlockIDExt) error
}

// MasterchainViewPublisher installs one coherent resident masterchain view
// before controller reconciliation can expose its sessions. LocalAcquisition
// implements this boundary; it is separate from the validator-facing Collator
// contract because remote collators never consume the node's borrowed cells.
type MasterchainViewPublisher interface {
	PublishMasterchainView(context.Context, *groups.Snapshot, *cell.Cell, *cell.Cell) error
}

// GroupTracker is the shared validator/collator masterchain projection. A
// concrete tracker owns replay and idempotent applied-state progression.
type GroupTracker interface {
	Bootstrap(
		context.Context,
		groups.MasterchainHistory,
		[]groups.BufferedMasterchainState,
		time.Time,
	) ([]groups.Transition, error)
	Apply(groups.ApplyInput) (groups.ApplyResult, error)
	Snapshot() (*groups.Snapshot, error)
}

// AppliedMasterchainState is the complete immutable artifact set delivered by
// the node hook before the ordinary live store publishes it.
type AppliedMasterchainState struct {
	Block     ton.BlockIDExt
	BlockRoot *cell.Cell
	StateRoot *cell.Cell
	AsOf      time.Time
}

// ControllerOptions assembles the standalone collator control plane. Backend
// and Storage refer to the same local collator database. Messages is the pool
// shared with local acquisition. Observer is the injected protocol boundary;
// this package implements no transport.
type ControllerOptions struct {
	Backend     ControllerBackend
	Storage     CollatorStorage
	Observer    ConsensusObserver
	Tracker     GroupTracker
	Acquisition MasterchainViewPublisher
	Messages    *msgpool.Pool
	Logger      zerolog.Logger
}

// ControllerStatus combines controller lifecycle/session counts with the
// backend production and storage view.
type ControllerStatus struct {
	Started          bool
	Closing          bool
	Closed           bool
	ActiveSessions   int
	FutureSessions   int
	BackendSessions  int
	ObserverSessions int
	Backend          Status
}

type controllerState uint8

const (
	controllerNew controllerState = iota
	controllerRunning
	controllerClosing
	controllerClosed
)

// Controller derives standalone collator and observer sessions from applied
// masterchain states. Apply/Close are serialized, while independent shard
// reconciliation runs concurrently and observer callbacks serialize only with
// their own session.
type Controller struct {
	backend     ControllerBackend
	storage     CollatorStorage
	observer    ConsensusObserver
	tracker     GroupTracker
	acquisition MasterchainViewPublisher
	messages    *msgpool.Internals
	policy      sessionProjectionPolicy
	topology    *msgpool.Topology
	log         zerolog.Logger

	applyMu ctxMutex
	mu      sync.RWMutex
	state   controllerState
	managed map[[32]byte]*controlledSession
}

type controlledSession struct {
	mu ctxMutex

	projection    projectedCollatorSession
	hasProjection bool
	reconciled    bool
	hasBackend    bool
	hasObserver   bool
}

// newControlledSession is the only way to build a managed controller session.
// The session lock is a channel semaphore whose zero value blocks forever
// instead of behaving as an unlocked mutex, so a bare struct literal would
// deadlock this session silently rather than fail to compile.
func newControlledSession() *controlledSession {
	return &controlledSession{mu: newCtxMutex()}
}

type controllerReconcileTask struct {
	id      [32]byte
	current *controlledSession
	next    *projectedCollatorSession
}

type controllerReconcileResult struct {
	retired bool
	err     error
}

// NewController validates fixed operator policy and binds the group tracker
// shared with local acquisition. Start restores durable backend inventory
// before opening ingress.
func NewController(options ControllerOptions) (*Controller, error) {
	if options.Backend == nil {
		return nil, errors.New("collator controller: backend is required")
	}
	if options.Storage == nil {
		return nil, errors.New("collator controller: storage is required")
	}
	if options.Observer == nil {
		return nil, errors.New("collator controller: consensus observer is required")
	}
	if options.Tracker == nil {
		return nil, errors.New("collator controller: group tracker is required")
	}
	if options.Acquisition == nil {
		return nil, errors.New("collator controller: local acquisition is required")
	}
	if options.Messages == nil {
		return nil, errors.New("collator controller: message pool is required")
	}
	if options.Backend.CollatorID() == ([32]byte{}) {
		return nil, errors.New("collator controller: backend collator id is zero")
	}
	if options.Observer.LocalADNLID() != options.Backend.CollatorID() {
		return nil, errors.New("collator controller: observer adnl id differs from backend collator id")
	}
	policy, err := newSessionProjectionPolicy(options)
	if err != nil {
		return nil, err
	}

	return &Controller{
		applyMu:     newCtxMutex(),
		backend:     options.Backend,
		storage:     options.Storage,
		observer:    options.Observer,
		tracker:     options.Tracker,
		acquisition: options.Acquisition,
		messages:    options.Messages.Internals(),
		policy:      policy,
		log:         options.Logger,
		managed:     make(map[[32]byte]*controlledSession),
	}, nil
}

// BootstrapMasterchain reconstructs the shared tracker before Start opens
// observer ingress. The tracker owns replay validation and idempotency; the
// controller only enforces lifecycle ordering with Apply and Close.
func (c *Controller) BootstrapMasterchain(
	ctx context.Context,
	history groups.MasterchainHistory,
	buffered []groups.BufferedMasterchainState,
	asOf time.Time,
) error {
	if err := c.applyMu.LockCtx(ctx); err != nil {
		return err
	}
	defer c.applyMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}

	c.mu.RLock()
	state := c.state
	c.mu.RUnlock()
	switch state {
	case controllerNew:
	case controllerClosing, controllerClosed:
		return ErrClosed
	default:
		return errors.New("collator controller: masterchain bootstrap must precede start")
	}

	if _, err := c.tracker.Bootstrap(ctx, history, buffered, asOf); err != nil {
		return fmt.Errorf("collator controller: bootstrap masterchain groups: %w", err)
	}

	return nil
}

// Start restores backend inventory, then starts the consensus observer which
// owns delegation ingress and private overlays. If the shared tracker was
// bootstrapped before composition, its current snapshot is reconciled before
// Start opens the controller; otherwise recovered sessions remain
// query-inaccessible until the first applied masterchain state.
func (c *Controller) Start(ctx context.Context) error {
	if err := c.applyMu.LockCtx(ctx); err != nil {
		return err
	}
	defer c.applyMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}

	c.mu.Lock()
	switch c.state {
	case controllerRunning:
		c.mu.Unlock()
		return nil
	case controllerClosing, controllerClosed:
		c.mu.Unlock()
		return ErrClosed
	}
	c.mu.Unlock()

	snapshot, snapshotErr := c.tracker.Snapshot()
	if snapshotErr != nil && !errors.Is(snapshotErr, groups.ErrNoSnapshot) {
		return fmt.Errorf("collator controller: load bootstrapped group snapshot: %w", snapshotErr)
	}
	if snapshotErr == nil {
		// Recovered sessions prepare their acquisition pipeline during backend
		// startup. Publish current destinations first so that preparation never
		// observes an uninitialized internal-message topology.
		if err := c.reconcileMessageTopology(snapshot); err != nil {
			return err
		}
	}

	if err := c.backend.Start(ctx); err != nil {
		return c.failStart(ctx, fmt.Errorf("collator controller: start backend: %w", err), false)
	}
	records, err := c.storage.Sessions(ctx)
	if err != nil {
		return c.failStart(ctx, fmt.Errorf("collator controller: recover sessions: %w", err), false)
	}
	recovered := make(map[[32]byte]*controlledSession, len(records))
	for i := range records {
		record := records[i]
		if _, duplicate := recovered[record.Session.ID]; duplicate {
			return c.failStart(ctx, fmt.Errorf(
				"collator controller: duplicate recovered session %x",
				record.Session.ID,
			), false)
		}
		managed := newControlledSession()
		managed.hasBackend = true
		recovered[record.Session.ID] = managed
	}
	if err = c.observer.Start(ctx, c.remoteHandlers(), ConsensusObserverEvents{
		Progressed: c.handleConsensusProgress,
		Finalized:  c.handleConsensusFinalization,
	}); err != nil {
		return c.failStart(ctx, fmt.Errorf("collator controller: start consensus observer: %w", err), true)
	}
	observerSessions, err := c.observer.RecoverSessions(ctx)
	if err != nil {
		return c.failStart(ctx, fmt.Errorf("collator controller: recover observer sessions: %w", err), true)
	}
	seenObservers := make(map[[32]byte]struct{}, len(observerSessions))
	for _, id := range observerSessions {
		if id == ([32]byte{}) {
			return c.failStart(ctx, errors.New("collator controller: recovered zero observer session id"), true)
		}
		if _, duplicate := seenObservers[id]; duplicate {
			return c.failStart(ctx, fmt.Errorf(
				"collator controller: duplicate recovered observer session %x",
				id,
			), true)
		}
		seenObservers[id] = struct{}{}
		managed := recovered[id]
		if managed == nil {
			managed = newControlledSession()
			recovered[id] = managed
		}
		managed.hasObserver = true
	}

	c.mu.Lock()
	c.managed = recovered
	c.mu.Unlock()

	if snapshotErr == nil {
		if err = c.reconcileSnapshot(ctx, snapshot); err != nil {
			return c.failStart(ctx, fmt.Errorf(
				"collator controller: reconcile bootstrapped group snapshot: %w",
				err,
			), true)
		}
	}

	c.mu.Lock()
	c.state = controllerRunning
	c.mu.Unlock()

	return nil
}

func (c *Controller) failStart(ctx context.Context, cause error, observerStarted bool) error {
	var cleanupErr error
	if observerStarted {
		cleanupErr = c.observer.Close(ctx)
	}
	if cleanupErr == nil {
		cleanupErr = c.backend.Close(ctx)
	}

	c.mu.Lock()
	c.state = controllerClosed
	if cleanupErr != nil {
		// The lifecycle owner calls Close after a failed Start. Keep cleanup
		// retryable when a dependency could not finish under the startup context.
		c.state = controllerClosing
	}
	clear(c.managed)
	c.mu.Unlock()

	return errors.Join(cause, cleanupErr)
}

// Close stops ingress, observers, and the backend in that order. Durable
// backend sessions remain available for the next process start.
func (c *Controller) Close(ctx context.Context) error {
	if err := c.applyMu.LockCtx(ctx); err != nil {
		return err
	}
	defer c.applyMu.Unlock()

	c.mu.Lock()
	if c.state == controllerClosed {
		c.mu.Unlock()
		return nil
	}
	c.state = controllerClosing
	c.mu.Unlock()

	if err := c.observer.Close(ctx); err != nil {
		return fmt.Errorf("collator controller: close consensus observer: %w", err)
	}
	if err := c.backend.Close(ctx); err != nil {
		return fmt.Errorf("collator controller: close backend: %w", err)
	}

	c.mu.Lock()
	c.state = controllerClosed
	clear(c.managed)
	c.mu.Unlock()

	return nil
}

// ApplyMasterchainState advances groups.Tracker and reconciles the complete
// active/future session set. Replaying the same block retries partial local
// lifecycle work after a transient backend or observer failure.
func (c *Controller) ApplyMasterchainState(ctx context.Context, input AppliedMasterchainState) error {
	if err := c.applyMu.LockCtx(ctx); err != nil {
		return err
	}
	defer c.applyMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.requireRunning(); err != nil {
		return err
	}

	result, err := c.tracker.Apply(groups.ApplyInput{
		Block: input.Block,
		Root:  input.StateRoot,
		AsOf:  input.AsOf,
	})
	if err != nil {
		return fmt.Errorf("collator controller: track masterchain state: %w", err)
	}
	if result.CollatorRegistryIssue != nil {
		// Delegation is now off for every validator until a later rotation reads
		// the registry. The projection this produces is an empty one, which is
		// exactly what a network without a registry produces, so nothing else
		// downstream can tell the two apart.
		c.log.Warn().
			Err(result.CollatorRegistryIssue).
			Str("block", storage.FormatBlockRef(input.Block)).
			Msg("delegated collator registry is unavailable; collator delegation is disabled for this rotation")
	}
	if err = c.acquisition.PublishMasterchainView(
		ctx,
		result.Snapshot,
		input.BlockRoot,
		input.StateRoot,
	); err != nil {
		return fmt.Errorf("collator controller: publish resident masterchain view: %w", err)
	}

	return c.reconcileSnapshot(ctx, result.Snapshot)
}

// Status returns a coherent bounded controller projection and backend status.
func (c *Controller) Status(ctx context.Context) (ControllerStatus, error) {
	if err := ctx.Err(); err != nil {
		return ControllerStatus{}, err
	}
	backend, err := c.backend.Status(ctx)
	if err != nil {
		return ControllerStatus{}, err
	}

	c.mu.RLock()
	state := c.state
	sessions := make([]*controlledSession, 0, len(c.managed))
	for _, managed := range c.managed {
		sessions = append(sessions, managed)
	}
	c.mu.RUnlock()

	status := ControllerStatus{
		Started: state != controllerNew,
		Closing: state == controllerClosing,
		Closed:  state == controllerClosed,
		Backend: backend,
	}
	for _, managed := range sessions {
		if err = managed.mu.LockCtx(ctx); err != nil {
			return ControllerStatus{}, err
		}
		if managed.hasProjection {
			if managed.projection.active() {
				status.ActiveSessions++
			} else {
				status.FutureSessions++
			}
		}
		if managed.hasBackend {
			status.BackendSessions++
		}
		if managed.hasObserver {
			status.ObserverSessions++
		}
		managed.mu.Unlock()
	}

	return status, nil
}

func (c *Controller) requireRunning() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	switch c.state {
	case controllerRunning:
		return nil
	case controllerClosing, controllerClosed:
		return ErrClosed
	default:
		return ErrNotStarted
	}
}

func (c *Controller) remoteHandlers() RemoteHandlers {
	return RemoteHandlers{
		Probe: func(
			ctx context.Context,
			query AuthenticatedQuery,
			wire simplex.ConsensusPleaseCollatePrepare,
		) error {
			if wire.WindowStartSlot < 0 {
				return errors.New("collator controller: negative delegation window")
			}
			if err := c.requireDelegationSession(ctx, query.SessionID); err != nil {
				return err
			}

			return c.backend.Probe(ctx, WindowPreparation{
				SessionID:  query.SessionID,
				SourceADNL: query.SourceADNL,
				StartSlot:  uint32(wire.WindowStartSlot),
			})
		},
		Commit: func(
			ctx context.Context,
			query AuthenticatedQuery,
			wire simplex.ConsensusPleaseCollate,
		) error {
			if wire.WindowStartSlot < 0 {
				return errors.New("collator controller: negative delegation window")
			}
			if err := c.requireDelegationSession(ctx, query.SessionID); err != nil {
				return err
			}

			return c.backend.CommitDelegation(ctx, WindowRequest{
				SessionID:     query.SessionID,
				SourceADNL:    query.SourceADNL,
				PleaseCollate: wire,
			})
		},
	}
}

func (c *Controller) requireDelegationSession(ctx context.Context, id [32]byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.requireRunning(); err != nil {
		// Delegated-collation queries do not expose whether a session is still being
		// restored or the controller is stopping. Ingress opens atomically only
		// after every session in the startup snapshot has reconciled.
		return ErrNotFound
	}

	c.mu.RLock()
	managed := c.managed[id]
	c.mu.RUnlock()
	if managed == nil {
		return ErrNotFound
	}

	if err := managed.mu.LockCtx(ctx); err != nil {
		return err
	}
	defer managed.mu.Unlock()
	if !managed.reconciled || !managed.hasProjection || !managed.hasBackend || !managed.hasObserver ||
		managed.projection.overlay.Role != OverlayRoleCollator {
		return ErrNotFound
	}

	return nil
}

func (c *Controller) handleConsensusProgress(ctx context.Context, progress ConsensusProgress) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.RLock()
	managed := c.managed[progress.SessionID]
	c.mu.RUnlock()
	if managed == nil {
		return ErrNotFound
	}

	if err := managed.mu.LockCtx(ctx); err != nil {
		return err
	}
	defer managed.mu.Unlock()
	if !managed.reconciled || !managed.hasProjection || !managed.hasObserver {
		return ErrNotFound
	}
	projection := managed.projection
	if !projection.active() {
		return ErrSessionUnavailable
	}
	if err := validateConsensusProgress(projection.session, progress); err != nil {
		return err
	}
	if !projection.hasBackend() || !managed.hasBackend {
		return nil
	}

	if err := c.backend.ApplyConsensusProgress(ctx, progress); err != nil {
		return fmt.Errorf("collator controller: apply consensus progress: %w", err)
	}

	return nil
}

func (c *Controller) handleConsensusFinalization(
	ctx context.Context,
	finalization ConsensusFinalization,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.RLock()
	managed := c.managed[finalization.SessionID]
	c.mu.RUnlock()
	if managed == nil {
		return ErrNotFound
	}

	if err := managed.mu.LockCtx(ctx); err != nil {
		return err
	}
	defer managed.mu.Unlock()
	if !managed.reconciled || !managed.hasProjection || !managed.hasObserver {
		return ErrNotFound
	}
	projection := managed.projection
	if !projection.active() {
		return ErrSessionUnavailable
	}
	if !projection.hasBackend() || !managed.hasBackend {
		return nil
	}

	if err := c.backend.ObserveConsensusFinalized(
		ctx,
		finalization.SessionID,
		finalization.Block,
	); err != nil {
		return fmt.Errorf("collator controller: observe consensus finalization: %w", err)
	}

	return nil
}

func validateConsensusProgress(session Session, progress ConsensusProgress) error {
	window := progress.Window
	windowSize := session.SlotsPerLeaderWindow
	if windowSize == 0 || window.StartSlot%windowSize != 0 ||
		window.StartSlot > ^uint32(0)-windowSize || window.EndSlot != window.StartSlot+windowSize {
		return ErrWindowConflict
	}
	if uint64(window.Leader) >= uint64(len(session.Validators)) ||
		window.Leader != window.StartSlot/windowSize%uint32(len(session.Validators)) {
		return ErrWindowConflict
	}
	if window.ObservedSlot < window.StartSlot || window.ObservedSlot >= window.EndSlot ||
		window.ObservedAt.IsZero() ||
		(window.ObservedSlot == window.StartSlot && progress.StartAt.IsZero()) {
		return ErrWindowConflict
	}
	if !window.Base.Exists {
		if progress.Base != nil {
			return ErrCandidateConflict
		}

		return nil
	}
	if progress.Base == nil || !progress.Base.matches(session.ID, window.Base.ID) ||
		window.Base.ID.Slot >= window.ObservedSlot ||
		progress.Base.block.ID.Workchain != session.Shard.Workchain ||
		progress.Base.block.ID.Shard != session.Shard.Shard {
		return ErrCandidateConflict
	}

	return nil
}

func (c *Controller) reconcileSnapshot(ctx context.Context, snapshot *groups.Snapshot) error {
	if err := c.reconcileMessageTopology(snapshot); err != nil {
		return err
	}
	if !snapshot.Ready || !snapshot.LifecycleEnabled {
		return nil
	}
	desired, err := projectCollatorSessions(snapshot, c.policy)
	if err != nil {
		return err
	}

	c.mu.Lock()
	ids := make([][32]byte, 0, len(c.managed)+len(desired))
	seen := make(map[[32]byte]struct{}, len(c.managed)+len(desired))
	for id := range c.managed {
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	for id := range desired {
		if _, exists := seen[id]; !exists {
			ids = append(ids, id)
			c.managed[id] = newControlledSession()
		}
	}
	sort.Slice(ids, func(i, j int) bool { return bytes.Compare(ids[i][:], ids[j][:]) < 0 })

	tasks := make([]controllerReconcileTask, len(ids))
	for i, id := range ids {
		tasks[i] = controllerReconcileTask{id: id, current: c.managed[id]}
		if next, exists := desired[id]; exists {
			nextCopy := next
			tasks[i].next = &nextCopy
		}
	}
	c.mu.Unlock()

	results := c.runReconcileTasks(ctx, tasks)
	var reconcileErr error
	c.mu.Lock()
	for i := range tasks {
		task := &tasks[i]
		result := results[i]
		if result.retired && c.managed[task.id] == task.current {
			delete(c.managed, task.id)
		}
		if result.err != nil {
			reconcileErr = errors.Join(reconcileErr, fmt.Errorf("session %x: %w", task.id, result.err))
		}
	}
	c.mu.Unlock()

	return reconcileErr
}

func (c *Controller) reconcileMessageTopology(snapshot *groups.Snapshot) error {
	topology := msgpool.NewTopology(snapshot)
	if c.topology != nil && c.topology.Equal(topology) {
		return nil
	}
	if err := topology.Reconcile(c.messages); err != nil {
		return fmt.Errorf("collator controller: reconcile message topology: %w", err)
	}
	c.topology = &topology

	return nil
}

func (c *Controller) runReconcileTasks(
	ctx context.Context,
	tasks []controllerReconcileTask,
) []controllerReconcileResult {
	results := make([]controllerReconcileResult, len(tasks))
	if len(tasks) == 0 {
		return results
	}

	var reconciles sync.WaitGroup
	reconciles.Add(len(tasks))
	for i := range tasks {
		go func() {
			defer reconciles.Done()
			results[i] = c.reconcileSession(ctx, &tasks[i])
		}()
	}
	reconciles.Wait()

	return results
}

func (c *Controller) reconcileSession(
	ctx context.Context,
	task *controllerReconcileTask,
) controllerReconcileResult {
	current := task.current
	if err := current.mu.LockCtx(ctx); err != nil {
		return controllerReconcileResult{err: err}
	}
	defer current.mu.Unlock()
	current.reconciled = false
	if task.next == nil {
		err := c.retireSession(ctx, task.id, current)
		return controllerReconcileResult{
			retired: err == nil && !current.hasObserver && !current.hasBackend,
			err:     err,
		}
	}
	next := *task.next
	if current.hasProjection && !current.projection.session.Equal(next.session) {
		return controllerReconcileResult{err: ErrSessionConflict}
	}
	wasActive := current.hasProjection && current.projection.active()

	if next.hasBackend() {
		if err := c.reconcileBackendDescriptor(ctx, current, next); err != nil {
			return controllerReconcileResult{err: err}
		}
	} else if current.hasBackend {
		if err := c.backend.RetireSession(ctx, task.id); err != nil && !isRetiredSessionError(err) {
			return controllerReconcileResult{err: fmt.Errorf("retire observer-only backend: %w", err)}
		}
		current.hasBackend = false
	}

	observerSession := next.observerSession()
	if !current.hasObserver || !current.hasProjection {
		if err := c.observer.PrepareSession(ctx, observerSession); err != nil {
			return controllerReconcileResult{err: fmt.Errorf("prepare consensus observer: %w", err)}
		}
		current.hasObserver = true
	} else if !current.hasProjection || !current.projection.observerSession().Equal(observerSession) {
		if err := c.observer.UpdateSession(ctx, observerSession); err != nil {
			return controllerReconcileResult{err: fmt.Errorf("update consensus observer: %w", err)}
		}
	}
	// Retain the exact successfully prepared projection across a transient
	// activation failure. Delegation and progress remain closed by reconciled,
	// while an exact replay can retry activation without rebuilding the overlay.
	current.projection = next
	current.hasProjection = true

	if next.active() {
		if next.hasBackend() {
			if err := c.backend.ActivateSession(ctx, *next.activation); err != nil {
				return controllerReconcileResult{err: fmt.Errorf("activate backend session: %w", err)}
			}
		}
		if err := c.observer.ActivateSession(ctx, *next.activation); err != nil {
			return controllerReconcileResult{err: fmt.Errorf("activate consensus observer: %w", err)}
		}
	} else if wasActive {
		return controllerReconcileResult{err: ErrSessionConflict}
	}

	current.reconciled = true

	return controllerReconcileResult{}
}

func (c *Controller) reconcileBackendDescriptor(
	ctx context.Context,
	current *controlledSession,
	next projectedCollatorSession,
) error {
	record, err := c.backend.Session(ctx, next.session.ID)
	if errors.Is(err, ErrNotFound) || errors.Is(err, ErrSessionRetired) {
		if err = c.backend.PrepareSession(ctx, next.session, next.update); err != nil {
			return fmt.Errorf("prepare backend session: %w", err)
		}
		current.hasBackend = true
		return nil
	}
	if err != nil {
		return fmt.Errorf("load backend session: %w", err)
	}
	current.hasBackend = true
	if !record.Session.Equal(next.session) {
		return fmt.Errorf("recovered backend session differs from group projection: %w", ErrSessionConflict)
	}
	if !next.active() && record.Activation != nil {
		return ErrSessionConflict
	}

	update, shouldUpdate, err := reconcileSessionUpdate(record.Update, next.update)
	if err != nil {
		return err
	}
	if shouldUpdate {
		if err = c.backend.UpdateSession(ctx, update); err != nil {
			return fmt.Errorf("update backend session: %w", err)
		}
	}

	return nil
}

func reconcileSessionUpdate(current, next SessionUpdate) (SessionUpdate, bool, error) {
	if next.MasterchainBlock.SeqNo < current.MasterchainBlock.SeqNo {
		return current, false, nil
	}
	if !blockAdvances(current.MasterchainBlock, next.MasterchainBlock) {
		return SessionUpdate{}, false, fmt.Errorf(
			"backend masterchain view conflicts with projection: %w",
			ErrSessionConflict,
		)
	}

	next.PreserveProgress(current)
	if current.Equal(next) {
		return current, false, nil
	}
	if err := ValidateSessionUpdateAdvance(current, next); err != nil {
		return SessionUpdate{}, false, fmt.Errorf("advance backend session view: %w", err)
	}

	return next, true, nil
}

func (c *Controller) retireSession(ctx context.Context, id [32]byte, current *controlledSession) error {
	if current.hasObserver {
		if err := c.observer.RetireSession(ctx, id); err != nil && !isRetiredSessionError(err) {
			return fmt.Errorf("retire consensus observer: %w", err)
		}
		current.hasObserver = false
	}
	if current.hasBackend {
		if err := c.backend.RetireSession(ctx, id); err != nil && !isRetiredSessionError(err) {
			return fmt.Errorf("retire backend session: %w", err)
		}
		current.hasBackend = false
	}
	current.hasProjection = false

	return nil
}

func isRetiredSessionError(err error) bool {
	return errors.Is(err, ErrNotFound) || errors.Is(err, ErrSessionRetired)
}

// Equal reports whether two observer descriptors project the same session.
func (s ConsensusObserverSession) Equal(other ConsensusObserverSession) bool {
	return s.Overlay.Equal(other.Overlay) &&
		s.Update.Equal(other.Update) && s.Params == other.Params
}
