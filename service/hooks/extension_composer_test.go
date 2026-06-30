package hooks

import (
	"context"
	"errors"
	"slices"
	"testing"
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
	startErr error
	closeErr error
}

func (e *recordingLifecycleExtension) Start(context.Context) error {
	e.record("start")
	return e.startErr
}

func (e *recordingLifecycleExtension) Close(context.Context) error {
	e.record("close")
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
		recordingFactory("a", &calls, &recordingExtension{name: "a", calls: &calls}, nil),
		recordingFactory("b", &calls, nil, errComposerTest),
		recordingFactory("c", &calls, &recordingExtension{name: "c", calls: &calls}, nil),
	}

	_, err := composer.New(Node{})
	if !errors.Is(err, errComposerTest) {
		t.Fatalf("expected factory error, got %v", err)
	}

	want := []string{"a.new", "b.new"}
	if !slices.Equal(calls, want) {
		t.Fatalf("unexpected calls %v", calls)
	}
}

func TestExtensionComposerCallsHooksInOrder(t *testing.T) {
	var calls []string
	extension := composedExtension{
		&recordingExtension{name: "a", calls: &calls},
		&recordingExtension{name: "b", calls: &calls},
	}

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
	extension := composedExtension{
		&recordingExtension{name: "a", calls: &calls},
		&recordingExtension{name: "b", calls: &calls, failHook: "external"},
		&recordingExtension{name: "c", calls: &calls},
	}

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
	extension := composedExtension{
		&recordingLifecycleExtension{recordingExtension: &recordingExtension{name: "a", calls: &calls}},
		&recordingExtension{name: "b", calls: &calls},
		&recordingLifecycleExtension{recordingExtension: &recordingExtension{name: "c", calls: &calls}},
	}

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

func TestExtensionComposerStartClosesStartedOnError(t *testing.T) {
	var calls []string
	extension := composedExtension{
		&recordingLifecycleExtension{recordingExtension: &recordingExtension{name: "a", calls: &calls}},
		&recordingLifecycleExtension{
			recordingExtension: &recordingExtension{name: "b", calls: &calls},
			startErr:           errComposerTest,
		},
		&recordingLifecycleExtension{recordingExtension: &recordingExtension{name: "c", calls: &calls}},
	}

	err := extension.Start(context.Background())
	if !errors.Is(err, errComposerTest) {
		t.Fatalf("expected start error, got %v", err)
	}

	want := []string{"a.start", "b.start", "a.close"}
	if !slices.Equal(calls, want) {
		t.Fatalf("unexpected calls %v", calls)
	}
}
