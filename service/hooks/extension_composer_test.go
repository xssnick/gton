package hooks

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/xssnick/gton/service/p2p"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

var errComposerTest = errors.New("composer test error")

type recordingExtension struct {
	name     string
	calls    *[]string
	failHook string
}

func (e *recordingExtension) Start(context.Context) error {
	return nil
}

func (e *recordingExtension) Close(context.Context) error {
	return nil
}

func (e *recordingExtension) OnBlockApplied(context.Context, BlockAppliedEvent) error {
	e.record("applied")
	return e.hookErr("applied")
}

func (e *recordingExtension) OnExternalMessage(context.Context, ExternalMessageEvent) error {
	e.record("external")
	return e.hookErr("external")
}

func (e *recordingExtension) OnBlockReceived(context.Context, BlockReceivedEvent) error {
	e.record("received")
	return e.hookErr("received")
}

func (e *recordingExtension) record(call string) {
	*e.calls = append(*e.calls, e.name+"."+call)
}

func (e *recordingExtension) hookErr(call string) error {
	if e.failHook == call {
		return errComposerTest
	}
	return nil
}

type recordingLifecycleExtension struct {
	*recordingExtension
	startErr  error
	closeErr  error
	closeErrs []error
	closeCall int
}

type recordingShardTopExtension struct {
	*recordingExtension
	description *p2p.ShardBlockDescription
	root        *cell.Cell
}

func (e *recordingShardTopExtension) OnShardTopBlockDescription(
	_ context.Context,
	description *p2p.ShardBlockDescription,
	root *cell.Cell,
) error {
	e.description = description
	e.root = root
	e.record("shard_top")
	return e.hookErr("shard_top")
}

func (e *recordingLifecycleExtension) Start(context.Context) error {
	e.record("start")
	return e.startErr
}

func (e *recordingLifecycleExtension) Close(context.Context) error {
	e.record("close")
	e.closeCall++
	if len(e.closeErrs) != 0 {
		err := e.closeErrs[0]
		e.closeErrs = e.closeErrs[1:]

		return err
	}
	return e.closeErr
}

func recordingFactory(name string, calls *[]string, extension Extension, err error) ExtensionFactory {
	return func(Node) (Extension, error) {
		*calls = append(*calls, name+".new")
		if err != nil {
			return nil, err
		}
		return extension, nil
	}
}

func TestExtensionComposerNewInitializesFactoriesInOrder(t *testing.T) {
	var calls []string
	composer := ExtensionComposer{
		recordingFactory("a", &calls, &recordingExtension{name: "a", calls: &calls}, nil),
		recordingFactory("b", &calls, &recordingExtension{name: "b", calls: &calls}, nil),
	}

	extension, err := composer.New(Node{})
	if err != nil {
		t.Fatalf("new composer: %v", err)
	}
	if err = extension.OnBlockApplied(context.Background(), BlockAppliedEvent{}); err != nil {
		t.Fatalf("block applied: %v", err)
	}

	want := []string{"a.new", "b.new", "a.applied", "b.applied"}
	if !slices.Equal(calls, want) {
		t.Fatalf("unexpected calls %v", calls)
	}
}

func TestExtensionComposerNewStopsOnFactoryError(t *testing.T) {
	var calls []string
	composer := ExtensionComposer{
		recordingFactory("a", &calls, &recordingLifecycleExtension{
			recordingExtension: &recordingExtension{name: "a", calls: &calls},
		}, nil),
		recordingFactory("b", &calls, nil, errComposerTest),
		recordingFactory("c", &calls, &recordingExtension{name: "c", calls: &calls}, nil),
	}

	_, err := composer.New(Node{})
	if !errors.Is(err, errComposerTest) {
		t.Fatalf("expected factory error, got %v", err)
	}

	want := []string{"a.new", "b.new", "a.close"}
	if !slices.Equal(calls, want) {
		t.Fatalf("unexpected calls %v", calls)
	}
}

func TestExtensionComposerCallsHooksInOrder(t *testing.T) {
	var calls []string
	extension := &composedExtension{extensions: []Extension{
		&recordingExtension{name: "a", calls: &calls},
		&recordingExtension{name: "b", calls: &calls},
	}}

	if err := extension.OnBlockApplied(context.Background(), BlockAppliedEvent{}); err != nil {
		t.Fatalf("block applied: %v", err)
	}
	if err := extension.OnExternalMessage(context.Background(), ExternalMessageEvent{}); err != nil {
		t.Fatalf("external message: %v", err)
	}
	if err := extension.OnBlockReceived(context.Background(), BlockReceivedEvent{}); err != nil {
		t.Fatalf("block received: %v", err)
	}

	want := []string{
		"a.applied", "b.applied",
		"a.external", "b.external",
		"a.received", "b.received",
	}
	if !slices.Equal(calls, want) {
		t.Fatalf("unexpected calls %v", calls)
	}
}

func TestExtensionComposerStopsHookOnError(t *testing.T) {
	var calls []string
	extension := &composedExtension{extensions: []Extension{
		&recordingExtension{name: "a", calls: &calls},
		&recordingExtension{name: "b", calls: &calls, failHook: "external"},
		&recordingExtension{name: "c", calls: &calls},
	}}

	err := extension.OnExternalMessage(context.Background(), ExternalMessageEvent{})
	if !errors.Is(err, errComposerTest) {
		t.Fatalf("expected composer error, got %v", err)
	}

	want := []string{"a.external", "b.external"}
	if !slices.Equal(calls, want) {
		t.Fatalf("unexpected calls %v", calls)
	}
}

func TestExtensionComposerLifecycle(t *testing.T) {
	var calls []string
	extension := &composedExtension{extensions: []Extension{
		&recordingLifecycleExtension{recordingExtension: &recordingExtension{name: "a", calls: &calls}},
		&recordingExtension{name: "b", calls: &calls},
		&recordingLifecycleExtension{recordingExtension: &recordingExtension{name: "c", calls: &calls}},
	}}

	if err := extension.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := extension.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	want := []string{"a.start", "c.start", "c.close", "a.close"}
	if !slices.Equal(calls, want) {
		t.Fatalf("unexpected calls %v", calls)
	}
}

func TestExtensionComposerRetriesCloseBeforeLowerDependencies(t *testing.T) {
	var calls []string
	lower := &recordingLifecycleExtension{recordingExtension: &recordingExtension{name: "a", calls: &calls}}
	retrying := &recordingLifecycleExtension{
		recordingExtension: &recordingExtension{name: "b", calls: &calls},
		closeErrs:          []error{errComposerTest, nil},
	}
	upper := &recordingLifecycleExtension{recordingExtension: &recordingExtension{name: "c", calls: &calls}}
	extension := &composedExtension{extensions: []Extension{lower, retrying, upper}}

	err := extension.Close(context.Background())
	if !errors.Is(err, errComposerTest) {
		t.Fatalf("first close error = %v, want %v", err, errComposerTest)
	}
	if !slices.Equal(calls, []string{"c.close", "b.close"}) {
		t.Fatalf("first close calls = %v", calls)
	}

	if err = extension.Close(context.Background()); err != nil {
		t.Fatalf("retry close: %v", err)
	}
	want := []string{"c.close", "b.close", "b.close", "a.close"}
	if !slices.Equal(calls, want) {
		t.Fatalf("close calls = %v, want %v", calls, want)
	}
	if upper.closeCall != 1 || retrying.closeCall != 2 || lower.closeCall != 1 {
		t.Fatalf("close counts = upper %d, retrying %d, lower %d", upper.closeCall, retrying.closeCall, lower.closeCall)
	}
}

func TestExtensionComposerLeavesStartFailureCleanupToOwner(t *testing.T) {
	var calls []string
	extension := &composedExtension{extensions: []Extension{
		&recordingLifecycleExtension{recordingExtension: &recordingExtension{name: "a", calls: &calls}},
		&recordingLifecycleExtension{
			recordingExtension: &recordingExtension{name: "b", calls: &calls},
			startErr:           errComposerTest,
		},
		&recordingLifecycleExtension{recordingExtension: &recordingExtension{name: "c", calls: &calls}},
	}}

	err := extension.Start(context.Background())
	if !errors.Is(err, errComposerTest) {
		t.Fatalf("expected start error, got %v", err)
	}
	if err = extension.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	want := []string{"a.start", "b.start", "c.close", "b.close", "a.close"}
	if !slices.Equal(calls, want) {
		t.Fatalf("unexpected calls %v", calls)
	}
}

func TestExtensionComposerOmitsUnusedShardTopCapability(t *testing.T) {
	var calls []string
	extension, err := (ExtensionComposer{
		recordingFactory("a", &calls, &recordingExtension{name: "a", calls: &calls}, nil),
	}).New(Node{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := extension.(ShardTopBlockDescriptionObserver); ok {
		t.Fatal("composer exposed shard-top capability without a consumer")
	}
}

func TestExtensionComposerKeepsShardTopObserverOptional(t *testing.T) {
	var calls []string

	observerOnly, err := (ExtensionComposer{
		recordingFactory("observer", &calls, &recordingShardTopExtension{
			recordingExtension: &recordingExtension{name: "observer", calls: &calls},
		}, nil),
	}).New(Node{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := observerOnly.(ShardTopBlockDescriptionObserver); !ok {
		t.Fatal("observer composer omitted shard-top observation")
	}
}

func TestExtensionComposerPrecollectsAndCallsAllShardTopObservers(t *testing.T) {
	var calls []string
	first := &recordingShardTopExtension{recordingExtension: &recordingExtension{
		name: "a", calls: &calls, failHook: "shard_top",
	}}
	second := &recordingShardTopExtension{recordingExtension: &recordingExtension{name: "c", calls: &calls}}
	extension, err := (ExtensionComposer{
		recordingFactory("a", &calls, first, nil),
		recordingFactory("b", &calls, &recordingExtension{name: "b", calls: &calls}, nil),
		recordingFactory("c", &calls, second, nil),
	}).New(Node{})
	if err != nil {
		t.Fatal(err)
	}
	observer, ok := extension.(ShardTopBlockDescriptionObserver)
	if !ok {
		t.Fatal("composer omitted collected shard-top capability")
	}

	description := &p2p.ShardBlockDescription{}
	root := cell.BeginCell().MustStoreUInt(1, 1).EndCell()
	err = observer.OnShardTopBlockDescription(context.Background(), description, root)
	if !errors.Is(err, errComposerTest) {
		t.Fatalf("observer error = %v, want %v", err, errComposerTest)
	}
	if first.description != description || second.description != description || first.root != root || second.root != root {
		t.Fatal("composer changed borrowed shard-top values")
	}
	want := []string{"a.new", "b.new", "c.new", "a.shard_top", "c.shard_top"}
	if !slices.Equal(calls, want) {
		t.Fatalf("unexpected calls %v", calls)
	}
}
