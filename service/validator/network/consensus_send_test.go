package network

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xssnick/gton/service/p2p"
)

func TestConsensusSendsToSamePeerAreIndependent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		manager, opener, spec, _ := testSessionManager(t)
		endpoint, err := manager.prepare(t.Context(), spec)
		if err != nil {
			t.Fatal(err)
		}
		startTestEndpoint(t, endpoint, &testSessionReceiver{})
		target := spec.peers[0]
		started := make(chan context.Context, 1)
		delivered := make(chan byte, 1)
		handle := opener.latest()
		handle.mu.Lock()
		handle.send = func(ctx context.Context, peer p2p.PeerID, wire []byte) error {
			if peer != target {
				return nil
			}
			if wire[0] == 1 {
				started <- ctx
				<-ctx.Done()
				return ctx.Err()
			}
			delivered <- wire[0]
			return nil
		}
		handle.mu.Unlock()

		endpoint.BroadcastToAll([]byte{1})
		synctest.Wait()
		firstCtx := <-started
		endpoint.BroadcastToAll([]byte{2})
		synctest.Wait()
		select {
		case got := <-delivered:
			if got != 2 {
				t.Fatalf("newer message = %d, want 2", got)
			}
		default:
			t.Fatal("one stalled send blocked a newer message to the same peer")
		}
		if firstCtx.Err() != nil {
			t.Fatal("first send expired before the independent delivery")
		}
	})
}

func TestConsensusSendDeadlineIncludesDispatcherDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		manager, _, spec, _ := testSessionManager(t)
		hub := newSession(manager, spec)
		endpoint := hub.consumer
		// Park the dispatcher while enqueueing, then start it halfway through
		// the budget. The send must get only the remaining half, not a new second.
		endpoint.running = true
		queuedAt := time.Now()
		endpoint.BroadcastToAll([]byte{1})
		message := <-endpoint.outbound
		if want := queuedAt.Add(time.Second); !message.deadline.Equal(want) {
			t.Fatalf("deadline = %v, want %v", message.deadline, want)
		}
		time.Sleep(500 * time.Millisecond)

		started := make(chan context.Context, len(spec.peers))
		finished := make(chan error, len(spec.peers))
		overlay := &testPrivateOverlay{send: func(ctx context.Context, _ p2p.PeerID, _ []byte) error {
			started <- ctx
			<-ctx.Done()
			finished <- ctx.Err()
			return ctx.Err()
		}}
		hub.installInitialHandles(overlay, nil, spec)
		t.Cleanup(func() { _ = hub.closeHandles() })
		senders := &consensusPeerSenders{
			endpoint: endpoint,
			ctx:      t.Context(),
			peers:    make(map[p2p.PeerID]*consensusPeerSender),
		}
		defer senders.stop()
		senders.dispatch(message)
		synctest.Wait()
		if len(started) != len(spec.peers) {
			t.Fatalf("started %d sends, want %d", len(started), len(spec.peers))
		}
		for range spec.peers {
			ctx := <-started
			deadline, ok := ctx.Deadline()
			if !ok || !deadline.Equal(message.deadline) {
				t.Fatalf("send deadline = %v, want %v", deadline, message.deadline)
			}
		}
		time.Sleep(500 * time.Millisecond)
		synctest.Wait()
		if len(finished) != len(spec.peers) {
			t.Fatal("sends outlived the original message budget")
		}
		for range spec.peers {
			if err := <-finished; !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("send result = %v, want deadline exceeded", err)
			}
		}
		endpoint.sendWG.Wait()

		senders.dispatch(message)
		synctest.Wait()
		if len(started) != 0 {
			t.Fatal("expired queued message reached the transport")
		}
	})
}

func TestConsensusConcurrentSendsStopOnRetire(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		manager, opener, spec, _ := testSessionManager(t)
		endpoint, err := manager.prepare(t.Context(), spec)
		if err != nil {
			t.Fatal(err)
		}
		startTestEndpoint(t, endpoint, &testSessionReceiver{})
		const perPeer = 4
		started := make(chan context.Context, perPeer*len(spec.peers))
		var sends sync.WaitGroup
		handle := opener.latest()
		handle.mu.Lock()
		handle.send = func(ctx context.Context, _ p2p.PeerID, _ []byte) error {
			sends.Add(1)
			defer sends.Done()
			started <- ctx
			<-ctx.Done()
			return ctx.Err()
		}
		handle.mu.Unlock()

		for i := range perPeer {
			endpoint.BroadcastToAll([]byte{byte(i)})
		}
		synctest.Wait()
		if len(started) != perPeer*len(spec.peers) {
			t.Fatalf("concurrent sends = %d, want %d", len(started), perPeer*len(spec.peers))
		}
		if err := manager.RetireValidatorSession(t.Context(), spec.id); err != nil {
			t.Fatal(err)
		}
		sends.Wait()
		for range perPeer * len(spec.peers) {
			if ctx := <-started; !errors.Is(ctx.Err(), context.Canceled) {
				t.Fatalf("retired send context = %v, want canceled", ctx.Err())
			}
		}
		endpoint.BroadcastToAll([]byte{255})
		synctest.Wait()
		if len(started) != 0 {
			t.Fatal("retired endpoint started another send")
		}
	})
}
