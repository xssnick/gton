package network

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/validator"
	"github.com/xssnick/gton/service/validator/collator"
)

// blockingQueryOverlay parks QueryRaw until released, ignoring its context the
// way a QUIC write waiting on peer flow control does.
type blockingQueryOverlay struct {
	*testPrivateOverlay
	entered chan context.Context
	release chan struct{}
}

func (o *blockingQueryOverlay) QueryRaw(
	ctx context.Context,
	_ p2p.PeerID,
	_ uint64,
	_ []byte,
) ([]byte, error) {
	o.entered <- ctx
	<-o.release
	return nil, nil
}

type warnCounter struct {
	mu    sync.Mutex
	warns int
}

func (c *warnCounter) Run(_ *zerolog.Event, level zerolog.Level, _ string) {
	if level != zerolog.WarnLevel {
		return
	}
	c.mu.Lock()
	c.warns++
	c.mu.Unlock()
}

func (c *warnCounter) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.warns
}

// receiveRequestContext fails the test when the request never reaches the
// overlay instead of leaving it to the package timeout.
func receiveRequestContext(t *testing.T, entered <-chan context.Context) context.Context {
	t.Helper()
	select {
	case ctx := <-entered:
		return ctx
	case <-time.After(time.Second):
	}

	t.Fatal("request did not reach the overlay")
	return nil
}

// closeHandlesWithin fails the test when closing the handles waits for an
// in-flight request, then checks that the close still reached that request.
func closeHandlesWithin(t *testing.T, hub *session, requestCtx context.Context) {
	t.Helper()
	closed := make(chan error, 1)
	go func() {
		closed <- hub.closeHandles()
	}()

	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("closeHandles waited for an in-flight request holding handleMu")
	}

	waitForCondition(t, func() bool {
		return requestCtx.Err() != nil
	})
}

func TestSendMessageRawDoesNotHoldHandleLockDuringSend(t *testing.T) {
	manager, _, validatorSpec, _ := testSessionManager(t)
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	overlay := &testPrivateOverlay{send: func(ctx context.Context, _ p2p.PeerID, _ []byte) error {
		entered <- ctx
		<-release
		return nil
	}}
	hub := newSession(manager, validatorSpec)
	hub.installInitialHandles(overlay, nil, validatorSpec)

	sent := make(chan error, 1)
	go func() {
		sent <- hub.sendMessageRaw(context.Background(), validatorSpec.peers[0], []byte{1})
	}()
	requestCtx := receiveRequestContext(t, entered)

	closeHandlesWithin(t, hub, requestCtx)
	close(release)
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
	if err := hub.sendMessageRaw(context.Background(), validatorSpec.peers[0], []byte{1}); !errors.Is(err, ErrSessionInactive) {
		t.Fatalf("send after close = %v, want %v", err, ErrSessionInactive)
	}
}

func TestQueryRawDoesNotHoldHandleLockDuringQuery(t *testing.T) {
	manager, _, validatorSpec, _ := testSessionManager(t)
	overlay := &blockingQueryOverlay{
		testPrivateOverlay: &testPrivateOverlay{},
		entered:            make(chan context.Context, 1),
		release:            make(chan struct{}),
	}
	hub := newSession(manager, validatorSpec)
	hub.installInitialHandles(overlay, nil, validatorSpec)

	queried := make(chan error, 1)
	go func() {
		_, err := hub.queryRaw(context.Background(), validatorSpec.peers[0], 1<<10, []byte{1})
		queried <- err
	}()
	requestCtx := receiveRequestContext(t, overlay.entered)

	closeHandlesWithin(t, hub, requestCtx)
	close(overlay.release)
	if err := <-queried; err != nil {
		t.Fatal(err)
	}
	if _, err := hub.queryRaw(context.Background(), validatorSpec.peers[0], 1<<10, []byte{1}); !errors.Is(err, ErrSessionInactive) {
		t.Fatalf("query after close = %v, want %v", err, ErrSessionInactive)
	}
}

func TestAcceptedBlockPublicationSkipsPersistentObserverQuietly(t *testing.T) {
	manager, _, validatorSpec, _ := testSessionManager(t)
	warnings := &warnCounter{}
	manager.log = zerolog.New(io.Discard).Hook(warnings)

	// persistentObserverSessionSpec: a validator-owned session with the
	// observer role and no signer.
	observerSpec := validatorSpec
	observerSpec.role = collator.OverlayRoleObserver
	observerSpec.signer = nil
	if _, err := manager.prepare(context.Background(), observerSpec); err != nil {
		t.Fatal(err)
	}

	for range 3 {
		manager.PublishAcceptedBlock(validator.AcceptedBlockPublication{
			SessionID: observerSpec.id,
		})
	}

	publisher := manager.broadcasts.(*testBlockPublisher)
	publisher.mu.Lock()
	accepted := len(publisher.accepted)
	publisher.mu.Unlock()
	if accepted != 0 {
		t.Fatalf("persistent observer accepted publications = %d, want 0", accepted)
	}
	if warns := warnings.count(); warns != 0 {
		t.Fatalf("persistent observer publication warnings = %d, want 0", warns)
	}
}
