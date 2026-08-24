package gton

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xssnick/gton/service"
	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type recordingExtensionHandlers struct {
	applied  hooks.BlockAppliedEvent
	external hooks.ExternalMessageEvent
	received hooks.BlockReceivedEvent
	err      error
}

type recordingShardTopExtensionHandlers struct {
	*recordingExtensionHandlers
	description *p2p.ShardBlockDescription
	root        *cell.Cell
}

func (e *recordingShardTopExtensionHandlers) OnShardTopBlockDescription(
	_ context.Context,
	description *p2p.ShardBlockDescription,
	root *cell.Cell,
) error {
	e.description = description
	e.root = root

	return e.err
}

func (e *recordingExtensionHandlers) Start(context.Context) error {
	return nil
}

func (e *recordingExtensionHandlers) Close(context.Context) error {
	return nil
}

func (e *recordingExtensionHandlers) OnBlockApplied(_ context.Context, event hooks.BlockAppliedEvent) error {
	e.applied = event

	return e.err
}

func (e *recordingExtensionHandlers) OnExternalMessage(_ context.Context, event hooks.ExternalMessageEvent) error {
	e.external = event

	return e.err
}

func (e *recordingExtensionHandlers) OnBlockReceived(_ context.Context, event hooks.BlockReceivedEvent) error {
	e.received = event

	return e.err
}

func TestEventHandlersFromNilExtension(t *testing.T) {
	handlers := eventHandlersFromExtension(nil)
	if handlers.BlockApplied != nil || handlers.ExternalMessage != nil || handlers.BlockReceived != nil ||
		handlers.ShardTopBlockDescription != nil {
		t.Fatal("nil extension enabled core event handlers")
	}
}

func TestEventHandlersKeepShardTopCapabilityOptional(t *testing.T) {
	plain := eventHandlersFromExtension(&recordingExtensionHandlers{})
	if plain.ShardTopBlockDescription != nil {
		t.Fatal("mandatory extension enabled optional shard-top observer")
	}

	extension := &recordingShardTopExtensionHandlers{
		recordingExtensionHandlers: &recordingExtensionHandlers{},
	}
	handlers := eventHandlersFromExtension(extension)
	if handlers.ShardTopBlockDescription == nil {
		t.Fatal("optional shard-top observer was not collected")
	}
	description := &p2p.ShardBlockDescription{}
	root := cell.BeginCell().MustStoreUInt(1, 1).EndCell()
	if err := handlers.ShardTopBlockDescription.ObserveShardTopBlockDescription(
		context.Background(), description, root,
	); err != nil {
		t.Fatal(err)
	}
	if extension.description != description || extension.root != root {
		t.Fatal("shard-top event adapter changed borrowed pointers")
	}
}

func TestEventHandlersPreserveBorrowedValues(t *testing.T) {
	extension := &recordingExtensionHandlers{}
	handlers := eventHandlersFromExtension(extension)

	blockBOC := []byte{1, 2, 3}
	proofBOC := []byte{4, 5, 6}
	blockRoot := cell.BeginCell().MustStoreUInt(7, 3).EndCell()
	previousState := cell.BeginCell().MustStoreUInt(1, 1).EndCell()
	currentState := cell.BeginCell().MustStoreUInt(0, 1).EndCell()
	inclusionState := cell.BeginCell().MustStoreUInt(3, 2).EndCell()
	meta := &storage.BlockMeta{ID: ton.BlockIDExt{SeqNo: 42}}
	inclusionRef := &ton.BlockIDExt{Workchain: -1, Shard: -1 << 63, SeqNo: 43}
	externalRoot := cell.BeginCell().MustStoreUInt(9, 4).EndCell()
	externalMessage := &tlb.ExternalMessage{}

	if err := handlers.BlockApplied.ProcessBlockApplied(context.Background(), service.BlockAppliedEvent{
		BlockBOC:             blockBOC,
		ProofBOC:             proofBOC,
		BlockRoot:            blockRoot,
		Meta:                 meta,
		PreviousState:        previousState,
		CurrentState:         currentState,
		InclusionMasterRef:   inclusionRef,
		InclusionMasterState: inclusionState,
	}); err != nil {
		t.Fatalf("process block applied: %v", err)
	}
	if err := handlers.ExternalMessage.AcceptExternalMessage(context.Background(), service.ExternalMessageEvent{
		IsLocal:        true,
		SerializedSize: 123,
		MessageRoot:    externalRoot,
		MessageParsed:  externalMessage,
	}); err != nil {
		t.Fatalf("accept external message: %v", err)
	}
	if err := handlers.BlockReceived.ObserveBlockReceived(context.Background(), service.BlockReceivedEvent{
		IsSigned:  true,
		BlockBOC:  blockBOC,
		ProofBOC:  proofBOC,
		BlockRoot: blockRoot,
		Meta:      meta,
	}); err != nil {
		t.Fatalf("observe block received: %v", err)
	}

	if &extension.applied.BlockBOC[0] != &blockBOC[0] || &extension.applied.ProofBOC[0] != &proofBOC[0] {
		t.Fatal("block event adapter copied borrowed payloads")
	}
	if extension.applied.BlockRoot != blockRoot || extension.applied.Meta != meta ||
		extension.applied.PreviousState != previousState || extension.applied.CurrentState != currentState ||
		extension.applied.InclusionMasterRef != inclusionRef || extension.applied.InclusionMasterState != inclusionState {
		t.Fatal("block event adapter changed borrowed pointers")
	}
	if !extension.external.IsLocal || extension.external.SerializedSize != 123 ||
		extension.external.MessageRoot != externalRoot || extension.external.MessageParsed != externalMessage {
		t.Fatal("external-message event adapter changed borrowed values")
	}
	if !extension.received.IsSigned || &extension.received.BlockBOC[0] != &blockBOC[0] ||
		&extension.received.ProofBOC[0] != &proofBOC[0] || extension.received.BlockRoot != blockRoot || extension.received.Meta != meta {
		t.Fatal("block-received event adapter changed borrowed values")
	}
}

func TestEventHandlersReturnExtensionErrors(t *testing.T) {
	wantErr := errors.New("extension failed")
	handlers := eventHandlersFromExtension(&recordingExtensionHandlers{err: wantErr})

	if err := handlers.BlockApplied.ProcessBlockApplied(context.Background(), service.BlockAppliedEvent{}); !errors.Is(err, wantErr) {
		t.Fatalf("block-applied error = %v, want %v", err, wantErr)
	}
	if err := handlers.ExternalMessage.AcceptExternalMessage(context.Background(), service.ExternalMessageEvent{}); !errors.Is(err, wantErr) {
		t.Fatalf("external-message error = %v, want %v", err, wantErr)
	}
	if err := handlers.BlockReceived.ObserveBlockReceived(context.Background(), service.BlockReceivedEvent{}); !errors.Is(err, wantErr) {
		t.Fatalf("block-received error = %v, want %v", err, wantErr)
	}
}

type blockingExtensionHandlers struct {
	recordingExtensionHandlers
	started chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (e *blockingExtensionHandlers) OnBlockReceived(
	context.Context,
	hooks.BlockReceivedEvent,
) error {
	e.calls.Add(1)
	close(e.started)
	<-e.release

	return nil
}

func TestExtensionEventsStopDrainsAndClosesAdmission(t *testing.T) {
	extension := &blockingExtensionHandlers{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	events := eventHandlersFromExtension(extension)
	callDone := make(chan struct{})
	go func() {
		defer close(callDone)
		_ = events.BlockReceived.ObserveBlockReceived(context.Background(), service.BlockReceivedEvent{})
	}()

	select {
	case <-extension.started:
	case <-time.After(time.Second):
		t.Fatal("extension hook did not start")
	}

	stopDone := make(chan struct{})
	go func() {
		defer close(stopDone)
		events.stop()
	}()
	select {
	case <-stopDone:
		t.Fatal("event stop returned while an extension hook was active")
	case <-time.After(10 * time.Millisecond):
	}

	close(extension.release)
	select {
	case <-callDone:
	case <-time.After(time.Second):
		t.Fatal("extension hook did not return")
	}
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("event stop did not finish after the active hook returned")
	}

	if err := events.BlockReceived.ObserveBlockReceived(
		context.Background(),
		service.BlockReceivedEvent{},
	); err != nil {
		t.Fatalf("closed hook admission returned an error: %v", err)
	}
	if extension.calls.Load() != 1 {
		t.Fatalf("extension calls after stop = %d, want 1", extension.calls.Load())
	}
}
