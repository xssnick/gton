package p2p

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"sync"
	"testing"
	"time"

	adnladdr "github.com/xssnick/tonutils-go/adnl/address"
)

type blockingPlumtreeLifecycleDHT struct {
	dhtBackend

	started      chan struct{}
	canceled     chan struct{}
	release      chan struct{}
	startedOnce  sync.Once
	canceledOnce sync.Once
}

func (d *blockingPlumtreeLifecycleDHT) FindAddresses(
	ctx context.Context,
	_ []byte,
) (*adnladdr.List, ed25519.PublicKey, error) {
	d.startedOnce.Do(func() {
		close(d.started)
	})

	<-ctx.Done()
	d.canceledOnce.Do(func() {
		close(d.canceled)
	})
	<-d.release
	return nil, nil, ctx.Err()
}

func TestPlumtreeRuntimeStartIsIdempotentAndWaitIsIsolated(t *testing.T) {
	node, _, runtime := newPlumtreeLifecycleTestRuntime(t, "start")

	unrelatedStarted := make(chan struct{})
	unrelatedRelease := make(chan struct{})
	unrelatedDone := make(chan struct{})
	var unrelatedReleaseOnce sync.Once
	releaseUnrelated := func() {
		unrelatedReleaseOnce.Do(func() {
			close(unrelatedRelease)
		})
	}
	defer releaseUnrelated()
	if !node.runAsync(func() {
		close(unrelatedStarted)
		<-unrelatedRelease
		close(unrelatedDone)
	}) {
		t.Fatal("node rejected unrelated work")
	}
	<-unrelatedStarted

	firstCtx, cancelFirst := context.WithCancel(t.Context())
	secondCtx, cancelSecond := context.WithCancel(t.Context())
	defer cancelSecond()
	runtime.Start(firstCtx)
	runtime.Start(secondCtx)
	cancelFirst()

	waitDone := make(chan struct{})
	go func() {
		runtime.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
	case <-time.After(time.Second):
		t.Fatal("runtime Wait joined a second Start or node-owned work")
	}

	select {
	case <-unrelatedDone:
		t.Fatal("runtime Wait joined or stopped unrelated node work")
	default:
	}
	releaseUnrelated()
	node.wg.Wait()
	runtime.Close()
}

func TestPlumtreeRuntimeCloseJoinsRepairsBeforeEngineClose(t *testing.T) {
	node, sub, runtime := newPlumtreeLifecycleTestRuntime(t, "close")
	dht := &blockingPlumtreeLifecycleDHT{
		started:  make(chan struct{}),
		canceled: make(chan struct{}),
		release:  make(chan struct{}),
	}
	var releaseOnce sync.Once
	releaseDHT := func() {
		releaseOnce.Do(func() {
			close(dht.release)
		})
	}
	defer releaseDHT()
	node.dht = dht

	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate repair peer key: %v", err)
	}
	peerID, err := peerIDFromED25519PublicKey(publicKey)
	if err != nil {
		t.Fatalf("derive repair peer id: %v", err)
	}

	sub.mx.Lock()
	sub.peers[peerID] = &overlayPeer{
		node:    node,
		id:      peerID,
		pub:     publicKey,
		route:   newTestPeerRoute(""),
		release: func() {},
	}
	sub.mx.Unlock()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runtime.Start(ctx)

	const firstRepairID = 1
	runtime.engine.mu.Lock()
	runtime.engine.repairs[firstRepairID] = plumtreeRepairAttempt{}
	runtime.engine.mu.Unlock()
	runtime.startRepair(plumtreeRepairAction{
		ID:            firstRepairID,
		To:            peerID,
		Timeout:       time.Minute,
		MaxAnswerSize: 1,
	})

	select {
	case <-dht.started:
	case <-time.After(time.Second):
		t.Fatal("repair did not enter the blocking transport")
	}

	closeDone := make(chan struct{}, 2)
	go func() {
		sub.close()
		closeDone <- struct{}{}
	}()

	select {
	case <-dht.canceled:
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel the repair context")
	}
	if err = runtime.HandleMessage(t.Context(), peerID, nil, nil); !errors.Is(err, errOverlayInactive) {
		t.Fatalf("message admission during Plumtree shutdown = %v, want %v", err, errOverlayInactive)
	}

	go func() {
		runtime.Close()
		closeDone <- struct{}{}
	}()

	select {
	case <-closeDone:
		t.Fatal("Close returned before the owned repair exited")
	default:
	}

	runtime.engine.mu.Lock()
	engineClosed := runtime.engine.closed
	runtime.engine.mu.Unlock()
	if engineClosed {
		t.Fatal("engine closed while an owned repair was still running")
	}

	if _, admitted := runtime.beginRepair(); admitted {
		runtime.workWG.Done()
		t.Fatal("runtime admitted repair work after Close began")
	}

	const rejectedRepairID = 2
	runtime.engine.mu.Lock()
	runtime.engine.repairs[rejectedRepairID] = plumtreeRepairAttempt{}
	runtime.engine.mu.Unlock()
	runtime.startRepair(plumtreeRepairAction{ID: rejectedRepairID, To: peerID})
	runtime.engine.mu.Lock()
	_, retained := runtime.engine.repairs[rejectedRepairID]
	runtime.engine.mu.Unlock()
	if retained {
		t.Fatal("rejected repair remained active in the engine")
	}

	releaseDHT()
	for range 2 {
		select {
		case <-closeDone:
		case <-time.After(time.Second):
			t.Fatal("Close did not join the released repair")
		}
	}

	runtime.engine.mu.Lock()
	engineClosed = runtime.engine.closed
	runtime.engine.mu.Unlock()
	if !engineClosed {
		t.Fatal("engine remained open after Close joined owned work")
	}
}

func newPlumtreeLifecycleTestRuntime(
	t *testing.T,
	label string,
) (*Node, *overlaySubscription, *plumtreeRuntime) {
	t.Helper()

	node := newTestNode(t)
	overlayID := testPeerID("plumtree-lifecycle-" + label)
	sub, err := node.newOverlaySubscription(overlaySpec{
		Name:      "plumtree-lifecycle-" + label,
		Kind:      overlayKindPublicShard,
		Workchain: 0,
		ShortID:   overlayID[:],
	})
	if err != nil {
		t.Fatalf("create Plumtree lifecycle subscription: %v", err)
	}
	t.Cleanup(sub.close)
	return node, sub, sub.plumtree
}
