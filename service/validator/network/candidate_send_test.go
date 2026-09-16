package network

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xssnick/gton/service/p2p"
)

func TestCandidateSendDeadlineIncludesQueueDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		manager, _, spec, _ := testSessionManager(t)
		hub := newSession(manager, spec)
		endpoint := hub.consumer
		endpoint.running = true
		queuedAt := time.Now()
		if err := endpoint.enqueueCandidate(outboundCandidateMessage{data: []byte{1}}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(candidateTransportSendBudget / 2)

		started := make(chan context.Context, 1)
		finished := make(chan error, 1)
		overlay := &testPrivateOverlay{broadcast: func(ctx context.Context) (p2p.TwoStepSendOutcome, error) {
			started <- ctx
			<-ctx.Done()
			finished <- ctx.Err()
			return p2p.TwoStepSendOutcome{Attempted: 1}, ctx.Err()
		}}
		hub.installInitialHandles(overlay, nil, spec)
		defer hub.closeHandles()
		ctx, cancel := context.WithCancel(t.Context())
		defer func() {
			cancel()
			endpoint.sendWG.Wait()
		}()
		endpoint.sendWG.Add(1)
		go endpoint.runCandidateSender(ctx)
		synctest.Wait()

		sendCtx := <-started
		deadline, ok := sendCtx.Deadline()
		if want := queuedAt.Add(candidateTransportSendBudget); !ok || !deadline.Equal(want) {
			t.Fatalf("candidate deadline = %v, want %v from enqueue", deadline, want)
		}
		time.Sleep(candidateTransportSendBudget / 2)
		synctest.Wait()
		select {
		case err := <-finished:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("candidate send error = %v, want deadline exceeded", err)
			}
		default:
			t.Fatal("candidate received a fresh send budget after waiting in the queue")
		}
	})
}

func TestCandidateSenderSkipsExpiredQueueAndSendsFreshCandidate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		manager, _, spec, _ := testSessionManager(t)
		observer := &testCandidateTransportObserver{}
		manager.candidateMetrics = observer
		hub := newSession(manager, spec)
		endpoint := hub.consumer
		endpoint.running = true
		if err := endpoint.enqueueCandidate(outboundCandidateMessage{data: []byte{1}}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(candidateTransportSendBudget)
		if err := endpoint.enqueueCandidate(outboundCandidateMessage{data: []byte{2}}); err != nil {
			t.Fatal(err)
		}

		overlay := &testPrivateOverlay{broadcastOut: p2p.TwoStepSendOutcome{Attempted: 1, Sent: 1}}
		hub.installInitialHandles(overlay, nil, spec)
		defer hub.closeHandles()
		ctx, cancel := context.WithCancel(t.Context())
		defer func() {
			cancel()
			endpoint.sendWG.Wait()
		}()
		endpoint.sendWG.Add(1)
		go endpoint.runCandidateSender(ctx)
		synctest.Wait()

		overlay.mu.Lock()
		broadcasts := overlay.broadcasts
		overlay.mu.Unlock()
		if broadcasts != 1 {
			t.Fatalf("broadcasts = %d, want only the fresh candidate", broadcasts)
		}
		queueItems, queueAges, drops, sends := observer.snapshot()
		if queueItems != 0 || len(queueAges) != 2 {
			t.Fatalf("queue items = %d, age samples = %d, want 0 and 2", queueItems, len(queueAges))
		}
		if len(sends) != 1 || sends[0].Result != CandidateTransportSendSuccess {
			t.Fatalf("candidate sends = %v, want one fresh successful send", sends)
		}
		if drops[CandidateOutboundDropExpired] != 1 {
			t.Fatalf("expired queue drops = %d, want 1", drops[CandidateOutboundDropExpired])
		}
	})
}

func TestCandidateSenderCancellationDoesNotSendQueuedCandidates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		manager, _, spec, _ := testSessionManager(t)
		observer := &testCandidateTransportObserver{}
		manager.candidateMetrics = observer
		hub := newSession(manager, spec)
		endpoint := hub.consumer
		endpoint.running = true
		if err := endpoint.enqueueCandidate(outboundCandidateMessage{data: []byte{1}}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(candidateTransportSendBudget)
		if err := endpoint.enqueueCandidate(outboundCandidateMessage{data: []byte{2}}); err != nil {
			t.Fatal(err)
		}

		overlay := &testPrivateOverlay{}
		hub.installInitialHandles(overlay, nil, spec)
		defer hub.closeHandles()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		endpoint.sendWG.Add(1)
		endpoint.runCandidateSender(ctx)
		endpoint.drainOutbound()

		if overlay.broadcasts != 0 {
			t.Fatalf("broadcasts after cancellation = %d, want 0", overlay.broadcasts)
		}
		queueItems, _, drops, sends := observer.snapshot()
		if queueItems != 0 || len(sends) != 0 {
			t.Fatalf("queue items = %d, send samples = %d, want 0 and 0", queueItems, len(sends))
		}
		if drops[CandidateOutboundDropExpired] != 0 {
			t.Fatalf("shutdown recorded %d deadline drops, want 0", drops[CandidateOutboundDropExpired])
		}
	})
}
