package p2p

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/tonutils-go/tl"
)

const (
	// A Plumtree forwarding decision contains at most this many distinct
	// non-eager destinations. Keeping one round buffered avoids retaining an
	// unbounded number of immutable payloads behind a flow-controlled peer.
	plumtreeOutboundBatchLimit = plumtreeActiveNeighbourLimit

	// C++ stops treating an eager peer as active after this many unacknowledged
	// full sends. Apply the same bound to queued wire objects for one peer.
	plumtreeOutboundPerPeerLimit = plumtreeMaxSentWithoutActivity

	// C++ submits the whole active-neighbour fanout asynchronously. This is the
	// corresponding per-overlay concurrency bound in Go.
	plumtreeOutboundParallelism = plumtreeActiveNeighbourLimit

	// Stalled peers can reject every FEC part in a burst. Aggregate those
	// expected resource-policy drops instead of writing one debug line per part.
	plumtreeOutboundDropLogInterval = 30 * time.Second

	// This is an allocation hint for Plumtree control bodies, not a wire limit.
	// tl.Append grows the buffer when a valid body is larger.
	plumtreeOutboundControlCapacity = 384
)

type plumtreeWireSend struct {
	to   PeerID
	wire []byte
}

type plumtreeWireBatch struct {
	sends []plumtreeWireSend
}

type plumtreePeerOutbound struct {
	pending [][]byte
	active  bool
	ready   bool
}

// plumtreeOutboundQueue is owned by one runtime loop. It preserves order for
// each peer while allowing different peers to make progress independently.
type plumtreeOutboundQueue struct {
	peers     map[PeerID]plumtreePeerOutbound
	ready     []PeerID
	readyHead int
}

func newPlumtreeOutboundQueue() plumtreeOutboundQueue {
	return plumtreeOutboundQueue{
		peers: make(map[PeerID]plumtreePeerOutbound),
	}
}

func (q *plumtreeOutboundQueue) add(batch plumtreeWireBatch) int {
	dropped := 0
	for _, send := range batch.sends {
		state := q.peers[send.to]
		if len(state.pending) >= plumtreeOutboundPerPeerLimit {
			dropped++
			continue
		}

		state.pending = append(state.pending, send.wire)
		if !state.active && !state.ready {
			state.ready = true
			q.ready = append(q.ready, send.to)
		}
		q.peers[send.to] = state
	}
	return dropped
}

func (q *plumtreeOutboundQueue) take() (PeerID, [][]byte, bool) {
	for q.readyHead < len(q.ready) {
		peer := q.ready[q.readyHead]
		q.readyHead++

		state, exists := q.peers[peer]
		if !exists || !state.ready {
			continue
		}
		state.ready = false
		if state.active || len(state.pending) == 0 {
			q.peers[peer] = state
			continue
		}

		sends := state.pending
		state.pending = nil
		state.active = true
		q.peers[peer] = state
		q.compactReady()
		return peer, sends, true
	}

	q.compactReady()
	return PeerID{}, nil, false
}

func (q *plumtreeOutboundQueue) finish(peer PeerID) {
	state, exists := q.peers[peer]
	if !exists || !state.active {
		panic("finished inactive Plumtree peer sender")
	}

	state.active = false
	if len(state.pending) == 0 {
		delete(q.peers, peer)
		return
	}
	if !state.ready {
		state.ready = true
		q.ready = append(q.ready, peer)
	}
	q.peers[peer] = state
}

func (q *plumtreeOutboundQueue) compactReady() {
	if q.readyHead == len(q.ready) {
		q.ready = q.ready[:0]
		q.readyHead = 0
		return
	}
	if q.readyHead < plumtreeOutboundBatchLimit {
		return
	}

	copy(q.ready, q.ready[q.readyHead:])
	q.ready = q.ready[:len(q.ready)-q.readyHead]
	q.readyHead = 0
}

func (r *plumtreeRuntime) prepareOutboundBatch(
	actions []plumtreeOutboundAction,
) plumtreeWireBatch {
	batch := plumtreeWireBatch{
		sends: make([]plumtreeWireSend, 0, len(actions)),
	}

	var previous *plumtreeOutboundAction
	var previousWire []byte
	for index := range actions {
		action := &actions[index]
		wire := action.Wire
		if action.Kind != plumtreeOutboundPayload {
			if previous != nil &&
				previous.Kind == action.Kind &&
				previous.Control == action.Control {
				wire = previousWire
			} else {
				wire, _ = r.sub.quicEnvelope.messageBuffer(
					plumtreeOutboundControlCapacity,
				)
				var err error
				wire, err = tl.Append(
					wire,
					plumtreeOutboundMessage(action),
					true,
				)
				if err != nil {
					r.sub.log.Error().
						Err(err).
						Msg("failed to serialize Plumtree control")
					previous = nil
					continue
				}
			}

			previous = action
			previousWire = wire
		} else {
			previous = nil
		}

		batch.sends = append(batch.sends, plumtreeWireSend{
			to:   action.To,
			wire: wire,
		})
	}
	return batch
}

func (r *plumtreeRuntime) enqueueOutbounds(batch plumtreeWireBatch) {
	if len(batch.sends) == 0 {
		return
	}

	select {
	case r.outbound <- batch:
	default:
		r.sub.log.Debug().
			Int("messages", len(batch.sends)).
			Msg("dropping Plumtree fanout because the outbound queue is full")
	}
}

func (r *plumtreeRuntime) startOutboundSend(
	ctx context.Context,
	done chan<- PeerID,
	peerID PeerID,
	wires [][]byte,
) {
	r.sub.node.runAsync(func() {
		r.sendOutboundBatch(ctx, peerID, wires)

		select {
		case done <- peerID:
		case <-ctx.Done():
		}
	})
}

func (r *plumtreeRuntime) sendOutboundBatch(
	parent context.Context,
	peerID PeerID,
	wires [][]byte,
) {
	path, err := r.sub.quicPeerPath(peerID)
	if err != nil {
		return
	}
	if !path.route.quicReady(time.Now()) {
		return
	}

	ctx, cancel := context.WithTimeout(parent, plumtreeOutboundTimeout)
	defer cancel()

	peer, err := path.dialGated(ctx)
	if err == nil {
		for _, wire := range wires {
			err = peer.SendOutboundMessage(ctx, wire)
			if err != nil {
				break
			}
		}
	}
	if err == nil || parent.Err() != nil {
		return
	}
	if errors.Is(err, errQUICDialDeferred) {
		return
	}
	r.sub.log.Debug().
		Err(fmt.Errorf("send Plumtree message: %w", err)).
		Str("peer_id", peerID.String()).
		Msg("failed to send Plumtree messages")
}
