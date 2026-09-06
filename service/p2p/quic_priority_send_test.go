package p2p

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl/overlay"
	adnlquic "github.com/xssnick/tonutils-go/adnl/quic"
)

// prioritySendWaitObserver records the priority_send_wait observations the
// relay writes of a test emit; sends run on their own goroutines, so reads
// are serialized with the writes.
type prioritySendWaitObserver struct {
	mu           sync.Mutex
	observations []BroadcastPipelineStageObservation
}

func (o *prioritySendWaitObserver) ObserveBroadcastPipelineStage(observation BroadcastPipelineStageObservation) {
	o.mu.Lock()
	o.observations = append(o.observations, observation)
	o.mu.Unlock()
}

func (o *prioritySendWaitObserver) waits() []BroadcastPipelineStageObservation {
	o.mu.Lock()
	defer o.mu.Unlock()

	waits := make([]BroadcastPipelineStageObservation, 0, len(o.observations))
	for _, observation := range o.observations {
		if observation.Stage == prioritySendWaitStage {
			waits = append(waits, observation)
		}
	}
	return waits
}

// prioritySendTestPeer is one live outbound QUIC path from a test node to a
// remote gateway that hands every received message payload to messages.
type prioritySendTestPeer struct {
	node     *Node
	peer     *overlayPeer
	envelope *quicOverlayEnvelope
	messages chan []byte
	observer *prioritySendWaitObserver
}

// prioritySendTestRemote shapes the remote gateway. hold, when non-nil, keeps
// every inbound message inside its handler until the channel is closed. With
// limits.MaxIncomingStreams = 1 that pins this node's next OpenStreamSync:
// quic-go hands out MAX_STREAMS credit only once the accepted stream is
// complete, and after our STOP_SENDING the remote's send side completes only
// when its handler has returned and the stream is closed (quic-go
// send_stream.go isNewlyCompleted), so the credit stays withheld exactly as
// long as the handler is held.
type prioritySendTestRemote struct {
	limits adnlquic.Limits
	hold   <-chan struct{}
}

func newPrioritySendTestPeer(t *testing.T) *prioritySendTestPeer {
	t.Helper()

	return newPrioritySendTestPeerWithRemote(t, prioritySendTestRemote{limits: adnlquic.DefaultLimits()})
}

func newPrioritySendTestPeerWithRemote(t *testing.T, remoteSpec prioritySendTestRemote) *prioritySendTestPeer {
	t.Helper()

	remote, err := adnlquic.NewGatewayWithLimits(remoteSpec.limits, quicOutboundTestKey(t))
	if err != nil {
		t.Fatalf("create remote gateway: %v", err)
	}
	messages := make(chan []byte, 64)
	remote.SetConnectionHandler(func(peer *adnlquic.Peer) error {
		peer.SetMessageHandler(func(_ context.Context, payload []byte) {
			if remoteSpec.hold != nil {
				<-remoteSpec.hold
			}
			messages <- bytes.Clone(payload)
		})
		return nil
	})
	remoteAddr := startQUICOutboundTestGateway(t, remote)
	remoteID, err := NewPeerID(remote.ID())
	if err != nil {
		t.Fatalf("parse remote peer id: %v", err)
	}

	node := newTestNode(t)
	startQUICOutboundTestGateway(t, node.quicGateway)
	runCtx, cancelRun := context.WithCancel(context.Background())
	node.runCtx = runCtx
	t.Cleanup(func() {
		cancelRun()
		node.wg.Wait()
	})
	observer := &prioritySendWaitObserver{}
	node.SetBroadcastPipelineObserver(observer)

	peer := &overlayPeer{
		node:  node,
		id:    remoteID,
		pub:   remote.PublicKey(),
		route: newTestPeerRoute(remoteAddr),
	}
	overlayID := testPeerID("priority-send-quic")
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "custom.priority-send-quic",
			Kind:    overlayKindCustomFixed,
			ShortID: overlayID[:],
			UseQUIC: true,
		},
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{remoteID: peer},
	})
	t.Cleanup(sub.broadcastReceiver.Close)

	warmCtx, cancelWarm := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelWarm()
	if _, err = peer.dialQUIC(warmCtx); err != nil {
		t.Fatalf("open the outbound QUIC path: %v", err)
	}

	return &prioritySendTestPeer{
		node:     node,
		peer:     peer,
		envelope: sub.quicEnvelope,
		messages: messages,
		observer: observer,
	}
}

func (fx *prioritySendTestPeer) relay() quicRouteBroadcastPeer {
	return quicRouteBroadcastPeer{peer: fx.peer, envelope: fx.envelope}
}

func (fx *prioritySendTestPeer) priority() quicPriorityBroadcastPeer {
	return quicPriorityBroadcastPeer{route: fx.relay()}
}

func (fx *prioritySendTestPeer) latchIdle() bool {
	return !fx.peer.prioritySend.raised.Load() && fx.peer.prioritySend.clearedChan() == nil
}

// expectMessage waits for the next payload the remote received and checks it
// carries body behind the overlay envelope.
func (fx *prioritySendTestPeer) expectMessage(t *testing.T, body []byte, what string) {
	t.Helper()

	select {
	case payload := <-fx.messages:
		if !bytes.HasSuffix(payload, body) {
			t.Fatalf("%s: remote received %x, want a message carrying %x", what, payload, body)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("%s: remote received nothing", what)
	}
}

func (fx *prioritySendTestPeer) expectSilence(t *testing.T, during time.Duration, what string) {
	t.Helper()

	select {
	case payload := <-fx.messages:
		t.Fatalf("%s: remote received %x", what, payload)
	case <-time.After(during):
	}
}

// awaitLatchRaised waits for a candidate write started on another goroutine to
// raise the peer's latch.
func (fx *prioritySendTestPeer) awaitLatchRaised(t *testing.T, what string) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for !fx.peer.prioritySend.raised.Load() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: latch not raised", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// awaitWaits waits until relay writes on other goroutines have reported at
// least n priority_send_wait observations and returns them.
func (fx *prioritySendTestPeer) awaitWaits(t *testing.T, n int) []BroadcastPipelineStageObservation {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for {
		waits := fx.observer.waits()
		if len(waits) >= n {
			return waits
		}
		if time.Now().After(deadline) {
			t.Fatalf("priority_send_wait observations = %d, want at least %d: %+v", len(waits), n, waits)
		}
		time.Sleep(time.Millisecond)
	}
}

// A relay write to a peer whose latch is raised does not open its stream until
// the latch is lowered, and then goes out.
func TestRelayWriteDefersToPriorityWriteInFlight(t *testing.T) {
	fx := newPrioritySendTestPeer(t)
	relay := fx.relay()
	body := []byte("relayed symbol of another leader")

	fx.peer.prioritySend.raise()
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		done <- relay.SendPreparedCustomMessage(ctx, body)
	}()

	const inFlight = 50 * time.Millisecond
	select {
	case err := <-done:
		t.Fatalf("relay write returned (%v) while the candidate write was in flight", err)
	default:
	}
	fx.expectSilence(t, inFlight, "relay write went out while the candidate write was in flight")

	fx.peer.prioritySend.lower()
	fx.expectMessage(t, body, "relay write after the candidate write returned")
	if err := <-done; err != nil {
		t.Fatalf("deferred relay write: %v", err)
	}

	waits := fx.observer.waits()
	if len(waits) != 1 {
		t.Fatalf("priority_send_wait observations = %d, want exactly one: %+v", len(waits), waits)
	}
	wait := waits[0]
	if wait.Kind != prioritySendWaitKind || wait.Delivery != prioritySendWaitDelivery {
		t.Fatalf("wait labels = %q/%q, want %q/%q", wait.Kind, wait.Delivery, prioritySendWaitKind, prioritySendWaitDelivery)
	}
	if wait.Result != prioritySendWaitCleared {
		t.Fatalf("wait result = %q, want %q", wait.Result, prioritySendWaitCleared)
	}
	if wait.Duration < inFlight/2 || wait.Duration >= quicPrioritySendWaitBound {
		t.Fatalf("wait duration = %v, want about %v and under the bound %v", wait.Duration, inFlight, quicPrioritySendWaitBound)
	}
	if !fx.latchIdle() {
		t.Fatal("relay write left the latch raised")
	}
}

// A candidate write that never returns holds relays for the bound and no
// longer: the relay write then goes out anyway.
func TestRelayWriteWaitIsBoundedWhenPriorityWriteNeverReturns(t *testing.T) {
	fx := newPrioritySendTestPeer(t)
	relay := fx.relay()
	body := []byte("relayed symbol behind a stuck candidate")

	fx.peer.prioritySend.raise()
	t.Cleanup(fx.peer.prioritySend.lower)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := time.Now()
	err := relay.SendPreparedCustomMessage(ctx, body)
	elapsed := time.Since(started)
	if err != nil {
		t.Fatalf("relay write behind a stuck candidate write: %v", err)
	}
	if elapsed < quicPrioritySendWaitBound {
		t.Fatalf("relay write went out after %v, before the %v bound", elapsed, quicPrioritySendWaitBound)
	}
	if elapsed > quicPrioritySendWaitBound+time.Second {
		t.Fatalf("relay write took %v behind a stuck candidate write, want about the %v bound", elapsed, quicPrioritySendWaitBound)
	}
	fx.expectMessage(t, body, "relay write after the bound")

	waits := fx.observer.waits()
	if len(waits) != 1 {
		t.Fatalf("priority_send_wait observations = %d, want exactly one: %+v", len(waits), waits)
	}
	if waits[0].Result != prioritySendWaitBounded {
		t.Fatalf("wait result = %q, want %q", waits[0].Result, prioritySendWaitBounded)
	}
	if waits[0].Duration < quicPrioritySendWaitBound {
		t.Fatalf("bounded wait duration = %v, want at least %v", waits[0].Duration, quicPrioritySendWaitBound)
	}
}

// The relay write's own context ends the wait early; the write then fails on
// that context as it would have without the latch.
func TestRelayWriteWaitEndsWithItsContext(t *testing.T) {
	fx := newPrioritySendTestPeer(t)
	relay := fx.relay()

	fx.peer.prioritySend.raise()
	t.Cleanup(fx.peer.prioritySend.lower)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := relay.SendPreparedCustomMessage(ctx, []byte("relayed symbol with a short budget"))
	elapsed := time.Since(started)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("relay write error = %v, want its own deadline", err)
	}
	if elapsed >= quicPrioritySendWaitBound {
		t.Fatalf("relay write waited %v past its own %v deadline", elapsed, 40*time.Millisecond)
	}

	waits := fx.observer.waits()
	if len(waits) != 1 || waits[0].Result != prioritySendWaitCanceled {
		t.Fatalf("priority_send_wait observations = %+v, want one %q", waits, prioritySendWaitCanceled)
	}

	// A relay write whose context is already over never waits and is not
	// charged to the latch: it fails the way it would have without one.
	expired, cancelExpired := context.WithCancel(context.Background())
	cancelExpired()
	if err = relay.SendPreparedCustomMessage(expired, []byte("relayed symbol on a spent budget")); !errors.Is(err, context.Canceled) {
		t.Fatalf("relay write on a spent context error = %v, want cancellation", err)
	}
	if waits = fx.observer.waits(); len(waits) != 1 {
		t.Fatalf("priority_send_wait observations = %+v after a relay write on a spent context, want the earlier one only", waits)
	}
}

// The latch is lowered whichever way the candidate write ends: delivered,
// refused for an offline peer, or failed on its context.
func TestPriorityWriteLowersLatchOnSuccessAndFailure(t *testing.T) {
	fx := newPrioritySendTestPeer(t)
	priority := fx.priority()

	body := []byte("own candidate symbol")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := priority.SendPreparedCustomMessage(ctx, body); err != nil {
		t.Fatalf("priority write: %v", err)
	}
	fx.expectMessage(t, body, "priority write")
	if !fx.latchIdle() {
		t.Fatal("delivered candidate write left the latch raised")
	}

	failedCtx, cancelFailed := context.WithCancel(context.Background())
	cancelFailed()
	if err := priority.SendPreparedCustomMessage(failedCtx, body); !errors.Is(err, context.Canceled) {
		t.Fatalf("priority write on a cancelled context error = %v, want cancellation", err)
	}
	if !fx.latchIdle() {
		t.Fatal("failed candidate write left the latch raised")
	}
	if err := priority.SendCustomMessage(failedCtx, overlay.Ping{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("priority message on a cancelled context error = %v, want cancellation", err)
	}
	if !fx.latchIdle() {
		t.Fatal("failed candidate message left the latch raised")
	}

	offlinePub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate offline peer key: %v", err)
	}
	offline := &overlayPeer{
		node:  fx.node,
		id:    peerIDForQUICOutboundTest(t, offlinePub),
		pub:   offlinePub,
		route: newTestPeerRoute(""),
	}
	offlinePriority := quicPriorityBroadcastPeer{route: quicRouteBroadcastPeer{peer: offline, envelope: fx.envelope}}
	if err = offlinePriority.SendPreparedCustomMessage(ctx, body); !errors.Is(err, errQUICPeerOffline) {
		t.Fatalf("priority write to an offline peer error = %v, want %v", err, errQUICPeerOffline)
	}
	if offline.prioritySend.raised.Load() || offline.prioritySend.clearedChan() != nil {
		t.Fatal("refused candidate write left the latch raised")
	}

	if waits := fx.observer.waits(); len(waits) != 0 {
		t.Fatalf("candidate writes waited on the latch: %+v", waits)
	}
}

// The candidate write never consults the latch: with another candidate still
// in flight to the peer it goes out at once while a relay write to the same
// peer keeps waiting, and relay writes never raise the latch themselves.
func TestPriorityWriteIsNotDelayedByLatchOrRelays(t *testing.T) {
	fx := newPrioritySendTestPeer(t)
	relay := fx.relay()
	priority := fx.priority()
	relayBody := []byte("relayed symbol")
	candidateBody := []byte("own candidate symbol")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 3; i++ {
		if err := relay.SendPreparedCustomMessage(ctx, relayBody); err != nil {
			t.Fatalf("relay write %d: %v", i, err)
		}
		fx.expectMessage(t, relayBody, "relay write")
	}
	if !fx.latchIdle() {
		t.Fatal("relay writes raised the latch")
	}

	// Another candidate to the same peer is still being written.
	fx.peer.prioritySend.raise()
	relayDone := make(chan error, 1)
	go func() {
		relayDone <- relay.SendPreparedCustomMessage(ctx, relayBody)
	}()

	started := time.Now()
	if err := priority.SendPreparedCustomMessage(ctx, candidateBody); err != nil {
		t.Fatalf("priority write with the latch raised: %v", err)
	}
	if elapsed := time.Since(started); elapsed >= quicPrioritySendWaitBound/2 {
		t.Fatalf("priority write took %v with the latch raised, want no wait", elapsed)
	}
	fx.expectMessage(t, candidateBody, "priority write with the latch raised")
	select {
	case err := <-relayDone:
		t.Fatalf("relay write returned (%v) while a candidate write was still in flight", err)
	default:
	}
	if !fx.peer.prioritySend.raised.Load() {
		t.Fatal("the completed candidate write lowered a latch another candidate still holds")
	}

	fx.peer.prioritySend.lower()
	fx.expectMessage(t, relayBody, "relay write after the last candidate write returned")
	if err := <-relayDone; err != nil {
		t.Fatalf("deferred relay write: %v", err)
	}
	if !fx.latchIdle() {
		t.Fatal("latch stayed raised after the last candidate write returned")
	}

	waits := fx.observer.waits()
	if len(waits) != 1 || waits[0].Result != prioritySendWaitCleared {
		t.Fatalf("priority_send_wait observations = %+v, want one %q from the deferred relay", waits, prioritySendWaitCleared)
	}
}

// The latch is raised for as long as a real candidate write sits inside
// OpenStreamSync. The remote grants one stream of credit and holds the message
// occupying it inside its handler, so the candidate write waits for credit:
// the latch reads raised the whole time, a relay write arriving meanwhile
// defers and reports the bound, the latch is idle again both when the
// candidate's own deadline ends the wait and when the credit finally arrives,
// and the candidate stream is opened ahead of the deferred relay.
func TestPriorityWriteRaisesLatchWhileItsStreamOpenWaits(t *testing.T) {
	limits := adnlquic.DefaultLimits()
	limits.MaxIncomingStreams = 1
	hold := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(hold) }) }
	t.Cleanup(release)
	fx := newPrioritySendTestPeerWithRemote(t, prioritySendTestRemote{limits: limits, hold: hold})
	relay := fx.relay()
	priority := fx.priority()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	occupier := []byte("message the remote is still handling")
	if err := relay.SendPreparedCustomMessage(ctx, occupier); err != nil {
		t.Fatalf("write occupying the remote's stream credit: %v", err)
	}
	fx.expectSilence(t, 20*time.Millisecond, "the remote let the held message through")

	// On a short budget the candidate write ends on its deadline, still inside
	// OpenStreamSync, and leaves the latch idle.
	candidateBody := []byte("own candidate symbol")
	shortCtx, cancelShort := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancelShort()
	shortDone := make(chan error, 1)
	go func() { shortDone <- priority.SendPreparedCustomMessage(shortCtx, candidateBody) }()
	fx.awaitLatchRaised(t, "candidate write waiting for stream credit on a short budget")
	select {
	case err := <-shortDone:
		t.Fatalf("candidate write returned (%v) while the remote held the stream credit", err)
	default:
	}
	select {
	case err := <-shortDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("candidate write behind withheld stream credit error = %v, want its own deadline", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("candidate write did not end on its deadline")
	}
	if !fx.latchIdle() {
		t.Fatal("candidate write that ended on its deadline left the latch raised")
	}

	// On a long budget it waits for the credit with the latch raised.
	longDone := make(chan error, 1)
	go func() { longDone <- priority.SendPreparedCustomMessage(ctx, candidateBody) }()
	fx.awaitLatchRaised(t, "candidate write waiting for stream credit")

	// A relay write arriving now defers, and since the credit cannot return
	// before the remote is released it runs into the bound.
	relayBody := []byte("relayed symbol of another leader")
	relayDone := make(chan error, 1)
	go func() { relayDone <- relay.SendPreparedCustomMessage(ctx, relayBody) }()
	waits := fx.awaitWaits(t, 1)
	if waits[0].Result != prioritySendWaitBounded || waits[0].Duration < quicPrioritySendWaitBound {
		t.Fatalf("relay wait behind a candidate write waiting for credit = %+v, want %q of at least %v", waits[0], prioritySendWaitBounded, quicPrioritySendWaitBound)
	}
	select {
	case err := <-longDone:
		t.Fatalf("candidate write returned (%v) while the remote held the stream credit", err)
	case payload := <-fx.messages:
		t.Fatalf("remote received %x while holding the stream credit", payload)
	default:
	}
	if !fx.peer.prioritySend.raised.Load() {
		t.Fatal("latch lowered while the candidate write was still waiting for stream credit")
	}

	// The remote finishes the held message: the credit returns, the candidate
	// stream that has been waiting for it opens first, the relay's after it.
	release()
	fx.expectMessage(t, occupier, "held message after the remote was released")
	fx.expectMessage(t, candidateBody, "candidate write after the stream credit returned")
	select {
	case err := <-longDone:
		if err != nil {
			t.Fatalf("candidate write after the stream credit returned: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("candidate write did not return after the stream credit came back")
	}
	if !fx.latchIdle() {
		t.Fatal("delivered candidate write left the latch raised")
	}
	fx.expectMessage(t, relayBody, "deferred relay write")
	if err := <-relayDone; err != nil {
		t.Fatalf("deferred relay write: %v", err)
	}
	if !fx.latchIdle() {
		t.Fatal("relay write left the latch raised")
	}
	if waits = fx.observer.waits(); len(waits) != 1 {
		t.Fatalf("priority_send_wait observations = %+v, want the deferred relay's only", waits)
	}
}

func TestPrioritySendLatchNestsOverlappingWrites(t *testing.T) {
	var latch quicPrioritySendLatch
	ctx := context.Background()

	if latch.raised.Load() || latch.clearedChan() != nil {
		t.Fatal("zero latch is not idle")
	}
	if result, waited := latch.await(ctx, time.Second); result != prioritySendNotWaited || waited != 0 {
		t.Fatalf("idle await = %v/%v, want no wait", result, waited)
	}

	latch.raise()
	latch.raise()
	first := latch.clearedChan()
	if first == nil {
		t.Fatal("raised latch has no cleared channel")
	}
	latch.lower()
	if !latch.raised.Load() || latch.clearedChan() != first {
		t.Fatal("latch cleared while a second write was still in flight")
	}
	if result, _ := latch.await(ctx, 5*time.Millisecond); result != prioritySendBounded {
		t.Fatalf("await with one write in flight = %v, want the bound", result)
	}

	// A context that is over before the wait starts is not the latch's doing.
	spent, cancelSpent := context.WithCancel(ctx)
	cancelSpent()
	if result, waited := latch.await(spent, time.Second); result != prioritySendNotWaited || waited != 0 {
		t.Fatalf("await on a spent context = %v/%v, want no wait", result, waited)
	}

	type awaited struct {
		result prioritySendWaitResult
		waited time.Duration
	}
	// A context that ends during the wait does end it.
	canceled, cancel := context.WithCancel(ctx)
	done := make(chan awaited, 1)
	go func() {
		result, waited := latch.await(canceled, time.Second)
		done <- awaited{result: result, waited: waited}
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case got := <-done:
		if got.result != prioritySendCanceled {
			t.Fatalf("await across cancel = %v, want canceled", got.result)
		}
		if got.waited < 10*time.Millisecond || got.waited >= time.Second {
			t.Fatalf("await across cancel waited %v", got.waited)
		}
	case <-time.After(time.Second):
		t.Fatal("await did not wake when its context ended")
	}

	go func() {
		result, waited := latch.await(ctx, time.Second)
		done <- awaited{result: result, waited: waited}
	}()
	time.Sleep(20 * time.Millisecond)
	latch.lower()
	select {
	case got := <-done:
		if got.result != prioritySendCleared {
			t.Fatalf("await across lower = %v, want cleared", got.result)
		}
		if got.waited < 10*time.Millisecond || got.waited >= time.Second {
			t.Fatalf("await across lower waited %v", got.waited)
		}
	case <-time.After(time.Second):
		t.Fatal("await did not wake when the latch cleared")
	}
	if latch.raised.Load() || latch.clearedChan() != nil {
		t.Fatal("latch is not idle after the last lower")
	}
}

// The path every relay write takes when no candidate is in flight must stay
// free: one atomic load, no timer, no closure.
func TestPrioritySendFastPathAllocatesNothing(t *testing.T) {
	peer := &overlayPeer{node: &Node{}}
	ctx := context.Background()

	if allocs := testing.AllocsPerRun(1000, func() {
		peer.awaitPrioritySend(ctx)
	}); allocs != 0 {
		t.Fatalf("idle awaitPrioritySend allocates %.1f objects per call, want 0", allocs)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		peer.prioritySend.await(ctx, quicPrioritySendWaitBound)
	}); allocs != 0 {
		t.Fatalf("idle latch await allocates %.1f objects per call, want 0", allocs)
	}
}

// Only the private overlay's own fan-out is priority-marked; the receive-side
// two-step forwarding set and the queued two-step rebroadcast set stay plain
// and therefore defer. RLDP peers are never marked.
func TestPrivateOverlayTwoStepPeerSetMarksOnlyOwnFanOut(t *testing.T) {
	validator := testPeerID("priority-two-step-validator")
	observer := testPeerID("priority-two-step-observer")
	overlayID := testPeerID("private.priority-two-step")
	newSub := func(useQUIC bool) *overlaySubscription {
		validatorPeer := &overlayPeer{id: validator, route: newTestPeerRoute("127.0.0.1:25041")}
		observerPeer := &overlayPeer{id: observer, route: newTestPeerRoute("127.0.0.1:25042")}
		sub := testOverlaySubscription(&overlaySubscription{
			spec: overlaySpec{
				Kind:                          overlayKindPrivate,
				ShortID:                       overlayID[:],
				UseQUIC:                       useQUIC,
				PrivateTwoStep:                true,
				PrivateTwoStepIntermediateIDs: map[PeerID]struct{}{validator: {}},
			},
			peers: map[PeerID]*overlayPeer{validator: validatorPeer, observer: observerPeer},
		})
		t.Cleanup(sub.broadcastReceiver.Close)
		sub.broadcastTargets.Store(&broadcastTargetsSnapshot{
			generation: sub.broadcastTargetsGen.Load(),
			builtAt:    time.Now(),
			peers:      []*overlayPeer{validatorPeer, observerPeer},
		})
		return sub
	}

	sub := newSub(true)
	own := (&PrivateOverlay{sub: sub}).twoStepPeerSet()
	if len(own) != 1 {
		t.Fatalf("own fan-out peers = %d, want the one intermediate", len(own))
	}
	marked, ok := own[0].(quicPriorityBroadcastPeer)
	if !ok {
		t.Fatalf("own fan-out peer = %T, want the priority-marked QUIC peer", own[0])
	}
	if marked.route.peer != sub.peers[validator] || !bytes.Equal(marked.ID(), validator[:]) {
		t.Fatal("priority-marked peer does not wrap the intermediate's route")
	}
	if marked.route.envelope != sub.quicEnvelope {
		t.Fatal("priority-marked peer lost the overlay envelope")
	}

	forwarding := (twoStepPeerSet{sub: sub}).Peers()
	if len(forwarding) != 2 {
		t.Fatalf("forwarding peers = %d, want both members", len(forwarding))
	}
	for _, peer := range forwarding {
		if _, ok = peer.(quicRouteBroadcastPeer); !ok {
			t.Fatalf("forwarding peer = %T, want the plain QUIC peer", peer)
		}
	}
	queued := sub.resolveTwoStepPeerSet(observer)
	if len(queued) != 1 {
		t.Fatalf("queued rebroadcast peers = %d, want the one intermediate", len(queued))
	}
	if _, ok = queued[0].(quicRouteBroadcastPeer); !ok {
		t.Fatalf("queued rebroadcast peer = %T, want the plain QUIC peer", queued[0])
	}

	rldp := newSub(false)
	for _, peer := range (&PrivateOverlay{sub: rldp}).twoStepPeerSet() {
		if _, ok = peer.(customRLDPBroadcastPeer); !ok {
			t.Fatalf("RLDP fan-out peer = %T, want it untouched", peer)
		}
	}
}
