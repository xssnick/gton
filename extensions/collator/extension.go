package collator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/console"
	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"
	core "github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	masterchainWorkchain = int32(-1)
	masterchainShard     = int64(-1 << 63)
	bootstrapRetryDelay  = 100 * time.Millisecond
)

// ShardTopDescriptionSink owns verified shard TopBlockDescr retention. A local
// collator uses ShardTopInbox; a remote deployment may implement the same
// boundary with its transport without changing the node hook.
type ShardTopDescriptionSink interface {
	StoreShardTopDescription(context.Context, *p2p.ShardBlockDescription, *cell.Cell) error
}

// Controller is the cohesive standalone-collator lifecycle consumed by this
// extension. BootstrapMasterchain reconstructs the shared group tracker before
// Start opens delegation ingress; later masterchain states advance that same
// tracker and reconcile local observer/collator sessions.
type Controller interface {
	BootstrapMasterchain(
		context.Context,
		groups.MasterchainHistory,
		[]groups.BufferedMasterchainState,
		time.Time,
	) error
	ApplyMasterchainState(context.Context, core.AppliedMasterchainState) error
	Start(context.Context) error
	Close(context.Context) error
	Status(context.Context) (core.ControllerStatus, error)
}

// Options contains the collator dependencies exposed through node hooks.
type Options struct {
	Controller Controller
	ShardTops  ShardTopDescriptionSink
	Messages   *msgpool.Pool
}

// New returns the lifecycle and console adapter for an already composed
// standalone controller. The composition root retains ownership of its shared
// validator database, local acquisition pipeline, and network provider.
func New(options Options) hooks.ExtensionFactory {
	return func(node hooks.Node) (hooks.Extension, error) {
		if options.Controller == nil {
			return nil, errors.New("collator extension: controller is required")
		}
		if options.ShardTops == nil {
			return nil, errors.New("collator extension: shard top description sink is required")
		}
		if options.Messages == nil {
			return nil, errors.New("collator extension: message pool is required")
		}
		if node.Store == nil {
			return nil, errors.New("collator extension: node store is required")
		}

		extension := &Extension{
			controller: options.Controller,
			history:    node.Store,
			shardTops:  options.ShardTops,
			messages:   options.Messages,
			log:        node.Logger.With().Str("component", "collator").Logger(),
			state:      extensionNew,
			buffered:   nil,
		}
		if err := registerStatusCollector(node.Metrics, options.Controller); err != nil {
			return nil, fmt.Errorf("register collator status metrics: %w", err)
		}
		if node.Commands != nil {
			if err := node.Commands.Register("status collator", extension.handleStatus); err != nil {
				return nil, fmt.Errorf("register collator status command: %w", err)
			}
			if err := node.Commands.Register("debug collator", extension.handleDebug); err != nil {
				return nil, fmt.Errorf("register collator debug command: %w", err)
			}
		}
		return extension, nil
	}
}

type extensionState uint8

const (
	extensionNew extensionState = iota
	extensionStarting
	extensionRunning
	extensionClosing
	extensionClosed
)

// Extension adapts a standalone controller to the node lifecycle. It retains
// only masterchain states observed before Start, then bootstraps exact history
// before the controller opens its delegated-collation endpoint.
type Extension struct {
	controller Controller
	history    groups.MasterchainHistory
	shardTops  ShardTopDescriptionSink
	messages   *msgpool.Pool
	log        zerolog.Logger

	mu       sync.Mutex
	state    extensionState
	cancel   context.CancelFunc
	done     chan struct{}
	startErr error
	buffered []groups.BufferedMasterchainState
	waiting  bool
}

var _ hooks.Extension = (*Extension)(nil)
var _ hooks.ShardTopBlockDescriptionObserver = (*Extension)(nil)
var _ Controller = (*core.Controller)(nil)

func (e *Extension) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	e.mu.Lock()
	switch e.state {
	case extensionRunning:
		e.mu.Unlock()
		return nil
	case extensionStarting:
		if e.waiting {
			e.mu.Unlock()

			return nil
		}
		done := e.done
		e.mu.Unlock()

		select {
		case <-done:
			e.mu.Lock()
			err := e.startErr
			e.mu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	case extensionClosing, extensionClosed:
		e.mu.Unlock()
		return core.ErrClosed
	}

	lifetime, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	buffered := e.buffered
	e.state = extensionStarting
	e.cancel = cancel
	e.done = done
	e.startErr = nil
	e.mu.Unlock()

	err := e.controller.BootstrapMasterchain(lifetime, e.history, buffered, time.Now())
	if errors.Is(err, groups.ErrNoSnapshot) {
		e.mu.Lock()
		if e.state == extensionStarting {
			e.waiting = true
			e.mu.Unlock()
			go e.awaitBootstrap(lifetime, cancel, done)

			return nil
		}
		e.mu.Unlock()
		err = core.ErrClosed
	}
	if err == nil {
		err = e.controller.Start(lifetime)
	}
	if err == nil {
		err = lifetime.Err()
	}

	return e.finishStart(cancel, done, err)
}

func (e *Extension) awaitBootstrap(
	lifetime context.Context,
	cancel context.CancelFunc,
	done chan struct{},
) {
	timer := time.NewTimer(bootstrapRetryDelay)
	defer timer.Stop()

	for {
		select {
		case <-lifetime.Done():
			e.finishStart(cancel, done, lifetime.Err())

			return
		case <-timer.C:
		}

		e.mu.Lock()
		if e.state != extensionStarting {
			e.mu.Unlock()
			e.finishStart(cancel, done, core.ErrClosed)

			return
		}
		buffered := append([]groups.BufferedMasterchainState(nil), e.buffered...)
		e.waiting = false
		e.mu.Unlock()

		err := e.controller.BootstrapMasterchain(lifetime, e.history, buffered, time.Now())
		if errors.Is(err, groups.ErrNoSnapshot) {
			e.mu.Lock()
			if e.state == extensionStarting {
				e.waiting = true
				e.mu.Unlock()
				timer.Reset(bootstrapRetryDelay)

				continue
			}
			e.mu.Unlock()
			err = core.ErrClosed
		}
		if err == nil {
			err = e.controller.Start(lifetime)
		}
		if err == nil {
			err = lifetime.Err()
		}
		e.finishStart(cancel, done, err)

		return
	}
}

func (e *Extension) finishStart(cancel context.CancelFunc, done chan struct{}, err error) error {
	if err != nil {
		cancel()
	}

	e.mu.Lock()
	if err == nil && e.state == extensionStarting {
		e.state = extensionRunning
	} else {
		if err == nil {
			err = core.ErrClosed
		}
		e.state = extensionClosing
	}
	e.waiting = false
	e.buffered = nil
	e.startErr = err
	close(done)
	e.mu.Unlock()

	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, core.ErrClosed) {
		e.log.Error().Err(err).Msg("collator extension startup failed")
	}

	return err
}

func (e *Extension) Close(ctx context.Context) error {
	// Every field below is already under e.mu, and Controller.Close is
	// idempotent under its own lock. A second mutex here would only serialize
	// Close against Close, which the hooks contract forbids anyway, and
	// Mutex.Lock is not context-aware so it could not honour ctx.
	var wait <-chan struct{}
	var cancel context.CancelFunc
	e.mu.Lock()
	switch e.state {
	case extensionClosed:
		e.mu.Unlock()
		return nil
	case extensionNew, extensionStarting, extensionRunning, extensionClosing:
		e.state = extensionClosing
		cancel = e.cancel
		// A previous Close may have timed out while Start was still unwinding.
		// Waiting on an already closed channel is cheap and keeps retries from
		// running controller cleanup concurrently with startup.
		wait = e.done
	}
	e.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if wait != nil {
		select {
		case <-wait:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if err := e.controller.Close(ctx); err != nil {
		return err
	}

	e.mu.Lock()
	e.state = extensionClosed
	e.buffered = nil
	e.mu.Unlock()

	return nil
}

func (e *Extension) OnBlockApplied(ctx context.Context, event hooks.BlockAppliedEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if event.Meta == nil {
		return errors.New("collator extension: applied block metadata is absent")
	}
	if event.Meta.ID.Workchain != masterchainWorkchain || event.Meta.ID.Shard != masterchainShard {
		e.cleanAppliedExternals(event)

		return nil
	}
	if event.CurrentState == nil {
		return errors.New("collator extension: applied masterchain state is absent")
	}
	if event.BlockRoot == nil {
		return errors.New("collator extension: applied masterchain block is absent")
	}
	if err := storage.ValidateBlockIDHashes(event.Meta.ID); err != nil {
		return fmt.Errorf("collator extension: invalid masterchain block id: %w", err)
	}

	for {
		e.mu.Lock()
		switch e.state {
		case extensionNew:
			e.bufferStateLocked(event)
			e.mu.Unlock()
			e.cleanAppliedExternals(event)

			return nil
		case extensionStarting:
			if e.waiting {
				e.bufferStateLocked(event)
				e.mu.Unlock()
				e.cleanAppliedExternals(event)

				return nil
			}
			done := e.done
			e.mu.Unlock()

			select {
			case <-done:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		case extensionRunning:
			e.mu.Unlock()
			err := e.controller.ApplyMasterchainState(ctx, core.AppliedMasterchainState{
				Block:     event.Meta.ID,
				BlockRoot: event.BlockRoot,
				StateRoot: event.CurrentState,
				AsOf:      time.Now(),
			})
			if err == nil {
				e.cleanAppliedExternals(event)
			}

			return err
		case extensionClosing, extensionClosed:
			err := e.startErr
			e.mu.Unlock()
			if err != nil {
				return err
			}

			return nil
		}
	}
}

func (e *Extension) OnExternalMessage(_ context.Context, event hooks.ExternalMessageEvent) error {
	priority := msgpool.ExternalPriorityBroadcast
	if event.IsLocal {
		priority = msgpool.ExternalPriorityLocal
	}
	if err := e.messages.AddExternal(nil, event.MessageRoot, event.MessageParsed, priority); err != nil {
		e.log.Debug().Err(err).Msg("external message not pooled")
	}

	return nil
}

func (*Extension) OnBlockReceived(context.Context, hooks.BlockReceivedEvent) error {
	return nil
}

func (e *Extension) OnShardTopBlockDescription(
	ctx context.Context,
	description *p2p.ShardBlockDescription,
	root *cell.Cell,
) error {
	return e.shardTops.StoreShardTopDescription(ctx, description, root)
}

func (e *Extension) cleanAppliedExternals(event hooks.BlockAppliedEvent) {
	hashes, err := msgpool.AppliedNormHashesFromBlockRoot(event.BlockRoot)
	if err != nil {
		e.log.Warn().Err(err).Str("block", storage.FormatBlockRef(event.Meta.ID)).
			Msg("cannot extract applied external messages")

		return
	}
	if len(hashes) == 0 {
		return
	}

	e.messages.EraseApplied(hashes)
}

func (e *Extension) handleStatus(ctx context.Context, args []string) (string, error) {
	if len(args) != 0 {
		return "", console.ErrNotFound
	}

	status, err := e.controller.Status(ctx)
	if err != nil {
		return "", err
	}

	return formatStatus(status), nil
}

// bufferStateLocked keeps applied masterchain states until the group tracker
// bootstraps. The consumer, groups.indexBufferedMasterchainStates, already
// keys and de-duplicates them — and already tolerates duplicates across
// sources — so this side only has to retain them in order.
func (e *Extension) bufferStateLocked(event hooks.BlockAppliedEvent) {
	// Meta is borrowed and may be reused after the callback returns, so the
	// block ids are copied element by element rather than sharing hash bytes.
	e.buffered = append(e.buffered, groups.BufferedMasterchainState{
		Block:    *event.Meta.ID.Copy(),
		Root:     event.CurrentState,
		PrevRefs: storage.CloneBlockIDs(event.Meta.PrevRefs),
	})
}
