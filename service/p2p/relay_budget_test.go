package p2p

import (
	"context"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

// A budget of 8 Mbit/s is a megabyte a second, with a second's burst: a
// megabyte goes at once, the next byte waits for the refill, and half a
// second later half a megabyte fits again. Nothing waits — a part that does
// not fit now is refused now.
func TestRelayEgressBudgetMetersBytesWithABurst(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	budget := newRelayEgressBudget(8_000_000, start)

	if !budget.allow(start, 1_000_000) {
		t.Fatal("the burst must admit a full second of the budget at once")
	}
	if budget.allow(start, 1) {
		t.Fatal("nothing must be admitted beyond the burst without a refill")
	}
	if budget.allow(start.Add(100*time.Millisecond), 200_000) {
		t.Fatal("100 ms refills 100 kB, not 200")
	}
	if !budget.allow(start.Add(600*time.Millisecond), 500_000) {
		t.Fatal("600 ms of refill must admit 500 kB")
	}
	// Idle for a minute: the burst is capped at one second's worth.
	if !budget.allow(start.Add(time.Minute), 1_000_000) {
		t.Fatal("a second's burst must be available after an idle spell")
	}
	if budget.allow(start.Add(time.Minute), 1) {
		t.Fatal("the burst must not accumulate beyond one second's worth")
	}
	budget.mu.Lock()
	dropped, forwarded := budget.droppedParts, budget.forwardedByte
	budget.mu.Unlock()
	if dropped != 3 || forwarded != 2_500_000 {
		t.Fatalf("dropped = %d forwarded = %d, want 3 refusals and 2.5 MB admitted", dropped, forwarded)
	}
}

func TestRelayEgressBudgetAllowIsAllocationFree(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	budget := newRelayEgressBudget(defaultRelayEgressBitsPerSecond, start)
	now := start
	allocs := testing.AllocsPerRun(1000, func() {
		now = now.Add(time.Millisecond)
		budget.allow(now, 1024)
	})
	if allocs != 0 {
		t.Fatalf("allow allocated %.1f objects per call on the relay hot path", allocs)
	}
}

// A disabled budget admits everything; the sampled relay peers are then
// handed out unwrapped.
func TestRelayEgressBudgetDisabledAdmitsEverything(t *testing.T) {
	var disabled *relayEgressBudget
	if !disabled.allow(time.Now(), 1<<30) {
		t.Fatal("a nil budget must admit any size")
	}
	sub := testOverlaySubscription(&overlaySubscription{spec: overlaySpec{Name: "test"}})
	peers := []overlay.BroadcastPeer{&recordingRelayPeer{}}
	if got := sub.budgetRelayPeers(peers); &got[0] != &peers[0] && got[0] != peers[0] {
		t.Fatal("with no budget the relay peers must be returned as they are")
	}
}

type recordingRelayPeer struct {
	framed   int
	bodied   int
	messages int
}

func (p *recordingRelayPeer) ID() []byte { return []byte{1} }

func (p *recordingRelayPeer) SendCustomMessage(context.Context, tl.Serializable) error {
	p.messages++
	return nil
}

func (p *recordingRelayPeer) SendPreparedCustomMessage(context.Context, []byte) error {
	p.bodied++
	return nil
}

func (p *recordingRelayPeer) SendPreparedBroadcastMessage(context.Context, *overlay.PreparedBroadcastMessage) error {
	p.framed++
	return nil
}

// Within the budget a forwarded part reaches the peer over its fastest path,
// the shared ADNL frame; beyond it the part is dropped without a send and
// counted, and the peer never hears of it.
func TestBudgetedRelayPeerDropsPartsBeyondTheBudget(t *testing.T) {
	node := newTestNode(t)
	start := time.Now()
	// 8 Mbit/s: one megabyte of burst, then a megabyte a second.
	node.relayEgress = newRelayEgressBudget(8_000_000, start)
	sub := testOverlaySubscription(&overlaySubscription{node: node, spec: overlaySpec{Name: "test"}})
	inner := &recordingRelayPeer{}
	peers := sub.budgetRelayPeers([]overlay.BroadcastPeer{inner})
	if len(peers) != 1 {
		t.Fatalf("budgeted peers = %d, want 1", len(peers))
	}
	peer, ok := peers[0].(overlay.PreparedBroadcastMessagePeer)
	if !ok {
		t.Fatal("a budgeted relay peer must keep the prepared-frame fast path")
	}
	part := overlay.NewPreparedBroadcastMessage(make([]byte, 100_000))

	sent := 0
	for i := 0; i < 20; i++ {
		if err := peer.SendPreparedBroadcastMessage(context.Background(), part); err != nil {
			t.Fatal(err)
		}
		sent++
	}
	// 100 kB + overhead per part against a megabyte burst: nine fit, the
	// tenth and everything after it is refused. The refill within this loop
	// is microseconds, far below one part.
	if inner.framed < 9 || inner.framed > 10 {
		t.Fatalf("parts forwarded = %d of %d, want the burst's worth (9-10)", inner.framed, sent)
	}
	if inner.bodied != 0 || inner.messages != 0 {
		t.Fatalf("the framed fast path must be used: bodied %d messages %d", inner.bodied, inner.messages)
	}
	node.relayEgress.mu.Lock()
	dropped := node.relayEgress.droppedParts
	node.relayEgress.mu.Unlock()
	if int(dropped) != sent-inner.framed {
		t.Fatalf("dropped = %d, want %d refused parts", dropped, sent-inner.framed)
	}
}
