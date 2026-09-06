package p2p

import (
	"context"
	"sync"
	"time"

	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

// The classic FEC broadcast of the public overlays is a flood: every part a
// node receives it forwards to broadcastFECRelayFanout peers, the reference
// does the same to propagate_broadcast_to of its neighbours, and every node
// therefore sees each part about five times. That redundancy is what lets a
// node drop some of its own forwarding without anyone missing a broadcast —
// and what makes forwarding all of it fatal when the link is the bound.
//
// Measured on the stand (2026-09-04, sixteen shards under load): this node
// forwarded 2.2 million FEC parts a minute — 37 thousand packets a second, the
// uplink at 952 Mbit/s of its gigabit — and everything that shares the link
// starved: its own candidate took 2 s at the median and 9 s at p90 to send
// instead of 60 ms, the peers it pinged fell from 64 alive to 48, and the
// external-message broadcasts, which are simple ones of a few hundred bytes,
// stopped arriving altogether for five minutes. The relay flood did not even
// deliver: with the link saturated more of its FEC streams died of idleness
// than completed.
//
// So forwarding is metered. Parts beyond the budget are not sent; the other
// copies in flight carry the broadcast. What this node originates — its own
// blocks, its candidates, its votes, the two-step forwarding for the
// committee — is not metered, because nothing else carries those.
const (
	// defaultRelayEgressBitsPerSecond is the budget for forwarded FEC parts on
	// a gigabit link: 150 Mbit/s, under half of what the flood took, which
	// leaves the rest of the link to the traffic only this node can send.
	defaultRelayEgressBitsPerSecond = 150_000_000
	// relayEgressBurst is how much of the budget may be spent at once after an
	// idle spell: one second's worth, so a burst of parts from a single block
	// is not throttled part by part when the link was free a moment ago.
	relayEgressBurst = time.Second
	// relayEgressPartOverheadBytes is what a forwarded part costs on the wire
	// beyond its body: the ADNL envelope, the channel packet framing and the
	// UDP/IP headers.
	relayEgressPartOverheadBytes = 96
	relayEgressDropReason        = "relay_egress_budget"
	relayEgressDropKind          = "overlay.broadcastFec"
)

// relayEgressBudget is a token bucket over bytes of forwarded FEC parts.
type relayEgressBudget struct {
	mu            sync.Mutex
	bytesPerSec   float64
	burstBytes    float64
	tokens        float64
	last          time.Time
	droppedParts  uint64
	droppedBytes  uint64
	forwardedByte uint64
}

func newRelayEgressBudget(bitsPerSecond int64, now time.Time) *relayEgressBudget {
	if bitsPerSecond <= 0 {
		return nil
	}
	bytesPerSec := float64(bitsPerSecond) / 8
	burst := bytesPerSec * relayEgressBurst.Seconds()
	return &relayEgressBudget{
		bytesPerSec: bytesPerSec,
		burstBytes:  burst,
		tokens:      burst,
		last:        now,
	}
}

// allow takes size bytes from the budget if they are available and reports
// whether the part may be sent. It never waits: a forwarded part that cannot
// go now is worth nothing later.
func (b *relayEgressBudget) allow(now time.Time, size int) bool {
	if b == nil {
		return true
	}
	cost := float64(size)
	b.mu.Lock()
	defer b.mu.Unlock()
	if elapsed := now.Sub(b.last); elapsed > 0 {
		b.tokens += elapsed.Seconds() * b.bytesPerSec
		if b.tokens > b.burstBytes {
			b.tokens = b.burstBytes
		}
		b.last = now
	}
	if b.tokens < cost {
		b.droppedParts++
		b.droppedBytes += uint64(size)
		return false
	}
	b.tokens -= cost
	b.forwardedByte += uint64(size)
	return true
}

// budgetedRelayPeer is a relay target whose forwarded parts are metered by the
// node's relay budget. It keeps the peer's prepared-message fast paths so the
// ADNL frame built once per part is still shared across the fan-out.
type budgetedRelayPeer struct {
	peer overlay.BroadcastPeer
	sub  *overlaySubscription
}

func (p budgetedRelayPeer) ID() []byte { return p.peer.ID() }

func (p budgetedRelayPeer) admit(size int) bool {
	if p.sub.node.relayEgress.allow(time.Now(), size+relayEgressPartOverheadBytes) {
		return true
	}
	p.sub.node.noteBroadcastDrop(p.sub.spec.Name, relayEgressDropKind, relayEgressDropReason)
	return false
}

func (p budgetedRelayPeer) SendCustomMessage(ctx context.Context, req tl.Serializable) error {
	// The unprepared path has no body to measure without serializing; the
	// prepared paths, which every fan-out below takes, do.
	if !p.admit(0) {
		return nil
	}
	return p.peer.SendCustomMessage(ctx, req)
}

func (p budgetedRelayPeer) SendPreparedCustomMessage(ctx context.Context, body []byte) error {
	if !p.admit(len(body)) {
		return nil
	}
	if bodied, ok := p.peer.(overlay.PreparedBroadcastPeer); ok {
		return bodied.SendPreparedCustomMessage(ctx, body)
	}
	return p.peer.SendCustomMessage(ctx, tl.Raw(body))
}

func (p budgetedRelayPeer) SendPreparedBroadcastMessage(ctx context.Context, msg *overlay.PreparedBroadcastMessage) error {
	if !p.admit(len(msg.Body())) {
		return nil
	}
	if framed, ok := p.peer.(overlay.PreparedBroadcastMessagePeer); ok {
		return framed.SendPreparedBroadcastMessage(ctx, msg)
	}
	if bodied, ok := p.peer.(overlay.PreparedBroadcastPeer); ok {
		return bodied.SendPreparedCustomMessage(ctx, msg.Body())
	}
	return p.peer.SendCustomMessage(ctx, tl.Raw(msg.Body()))
}

// budgetRelayPeers wraps the sampled relay targets of a public overlay so the
// FEC parts forwarded to them draw on the node's relay budget.
func (s *overlaySubscription) budgetRelayPeers(relay []overlay.BroadcastPeer) []overlay.BroadcastPeer {
	if s.node.relayEgress == nil || len(relay) == 0 {
		return relay
	}
	budgeted := make([]overlay.BroadcastPeer, len(relay))
	for i, peer := range relay {
		budgeted[i] = budgetedRelayPeer{peer: peer, sub: s}
	}
	return budgeted
}
