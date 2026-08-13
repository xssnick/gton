package gton

import (
	"context"
	"fmt"
	"sync"

	"github.com/xssnick/gton/service"
	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/p2p"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

func extensionFromFactory(factory hooks.ExtensionFactory, node hooks.Node) (hooks.Extension, error) {
	if factory == nil {
		return nil, nil
	}

	extension, err := factory(node)
	if err != nil {
		return nil, fmt.Errorf("initialize static extension: %w", err)
	}
	if extension == nil {
		return nil, fmt.Errorf("static extension returned nil extension")
	}
	return extension, nil
}

type extensionHandlers struct {
	extension        hooks.Extension
	shardTopObserver hooks.ShardTopBlockDescriptionObserver

	mu     sync.Mutex
	closed bool
	active sync.WaitGroup
}

var _ service.ShardTopBlockDescriptionObserver = (*extensionHandlers)(nil)

type extensionEvents struct {
	service.EventHandlers
	handlers *extensionHandlers
}

func eventHandlersFromExtension(extension hooks.Extension) extensionEvents {
	if extension == nil {
		return extensionEvents{}
	}

	handlers := &extensionHandlers{extension: extension}
	events := service.EventHandlers{
		BlockApplied:    handlers,
		ExternalMessage: handlers,
		BlockReceived:   handlers,
	}
	if observer, ok := extension.(hooks.ShardTopBlockDescriptionObserver); ok {
		handlers.shardTopObserver = observer
		events.ShardTopBlockDescription = handlers
	}
	return extensionEvents{EventHandlers: events, handlers: handlers}
}

// stop closes hook admission and waits for every already admitted call. P2P
// may remain alive after this point so extension-owned private overlays can be
// retired before the network itself stops, but it can no longer enter the
// extension's ordinary event hooks.
func (e extensionEvents) stop() {
	if e.handlers == nil {
		return
	}

	e.handlers.mu.Lock()
	e.handlers.closed = true
	e.handlers.mu.Unlock()
	e.handlers.active.Wait()
}

func (h *extensionHandlers) begin() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return false
	}

	h.active.Add(1)
	return true
}

func (h *extensionHandlers) end() {
	h.active.Done()
}

func (h *extensionHandlers) ProcessBlockApplied(ctx context.Context, event service.BlockAppliedEvent) error {
	if !h.begin() {
		return nil
	}
	defer h.end()

	return h.extension.OnBlockApplied(ctx, hooks.BlockAppliedEvent{
		BlockBOC:             event.BlockBOC,
		ProofBOC:             event.ProofBOC,
		BlockRoot:            event.BlockRoot,
		Meta:                 event.Meta,
		PreviousState:        event.PreviousState,
		CurrentState:         event.CurrentState,
		InclusionMasterRef:   event.InclusionMasterRef,
		InclusionMasterState: event.InclusionMasterState,
	})
}

func (h *extensionHandlers) AcceptExternalMessage(ctx context.Context, event service.ExternalMessageEvent) error {
	if !h.begin() {
		return nil
	}
	defer h.end()

	return h.extension.OnExternalMessage(ctx, hooks.ExternalMessageEvent{
		IsLocal:       event.IsLocal,
		MessageRoot:   event.MessageRoot,
		MessageParsed: event.MessageParsed,
	})
}

func (h *extensionHandlers) ObserveBlockReceived(ctx context.Context, event service.BlockReceivedEvent) error {
	if !h.begin() {
		return nil
	}
	defer h.end()

	return h.extension.OnBlockReceived(ctx, hooks.BlockReceivedEvent{
		IsSigned:  event.IsSigned,
		BlockBOC:  event.BlockBOC,
		ProofBOC:  event.ProofBOC,
		BlockRoot: event.BlockRoot,
		Meta:      event.Meta,
	})
}

func (h *extensionHandlers) ObserveShardTopBlockDescription(
	ctx context.Context,
	description *p2p.ShardBlockDescription,
	root *cell.Cell,
) error {
	if !h.begin() {
		return nil
	}
	defer h.end()

	return h.shardTopObserver.OnShardTopBlockDescription(ctx, description, root)
}
