package p2p

import (
	"context"
	"fmt"
	"time"

	sharddomain "github.com/xssnick/gton/service/shard"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

const (
	twoStepBroadcastKind        = "overlay.broadcastTwostep"
	twoStepRebroadcastQueueName = "two_step_rebroadcast"
)

type twoStepPeerSet struct {
	sub *overlaySubscription
}

func (set twoStepPeerSet) Peers() []overlay.BroadcastPeer {
	candidates := set.sub.twoStepCandidates(PeerID{})
	peers := make([]overlay.BroadcastPeer, 0, len(candidates))
	for _, peer := range candidates {
		if set.sub.spec.UseQUIC {
			if peer.route.QUICAddress() != "" {
				peers = append(peers, quicRouteBroadcastPeer{
					peer:     peer,
					envelope: set.sub.quicEnvelope,
				})
			}
			continue
		}
		peers = append(peers, customRLDPBroadcastPeer{
			id:        peer.id,
			transport: peer.rldpOverlay,
		})
	}
	return peers
}

type customRLDPBroadcastPeer struct {
	id        PeerID
	transport *overlay.RLDPOverlayWrapper
}

var _ overlay.PreparedBroadcastPeer = customRLDPBroadcastPeer{}

func (p customRLDPBroadcastPeer) ID() []byte {
	return p.id[:]
}

func (p customRLDPBroadcastPeer) SendCustomMessage(ctx context.Context, req tl.Serializable) error {
	return p.transport.SendCustomMessage(ctx, req)
}

func (p customRLDPBroadcastPeer) SendPreparedCustomMessage(ctx context.Context, body []byte) error {
	return p.transport.SendPreparedCustomMessage(ctx, body)
}

func (s *overlaySubscription) twoStepCandidates(sourcePeerID PeerID) []*overlayPeer {
	candidates := s.broadcastTargetsSnapshot().peers
	if sourcePeerID.IsZero() {
		return candidates
	}

	peers := make([]*overlayPeer, 0, len(candidates))
	for _, peer := range candidates {
		if peer.id == sourcePeerID {
			continue
		}
		peers = append(peers, peer)
	}
	return peers
}

func (s *overlaySubscription) twoStepIntermediateCandidates() []*overlayPeer {
	candidates := s.broadcastTargetsSnapshot().peers
	if !s.spec.isPrivateOverlay() {
		return candidates
	}

	peers := make([]*overlayPeer, 0, len(candidates))
	for _, peer := range candidates {
		if _, intermediate := s.spec.PrivateTwoStepIntermediateIDs[peer.id]; intermediate {
			peers = append(peers, peer)
		}
	}

	return peers
}

func (s *overlaySubscription) resolveTwoStepPeerSet(
	ctx context.Context,
	sourcePeerID PeerID,
) (overlay.StaticBroadcastPeerSet, []overlay.BroadcastTwoStepPeerError) {
	candidates := s.twoStepIntermediateCandidates()
	peers := make(overlay.StaticBroadcastPeerSet, 0, len(candidates))
	var failed []overlay.BroadcastTwoStepPeerError

	for _, peer := range candidates {
		if peer.id == sourcePeerID {
			continue
		}
		if s.spec.UseQUIC {
			_, err := peer.dialQUIC(ctx)
			if err != nil {
				failed = append(failed, overlay.BroadcastTwoStepPeerError{
					PeerID: peer.id[:],
					Err:    err,
				})
				continue
			}
			peers = append(peers, quicRouteBroadcastPeer{
				peer:     peer,
				envelope: s.quicEnvelope,
			})
			continue
		}

		peers = append(peers, customRLDPBroadcastPeer{
			id:        peer.id,
			transport: peer.rldpOverlay,
		})
	}
	return peers, failed
}

func planCustomRebroadcast(kind string, payloadLen int) rebroadcastPlan {
	switch kind {
	case "tonNode.newShardBlockBroadcast":
		if payloadLen <= ordinarySimpleBroadcastMaxSize {
			return rebroadcastPlan{mode: rebroadcastModeSimple}
		}
		return rebroadcastPlan{mode: rebroadcastModeFEC, flags: overlay.BroadcastFlagAnySender}
	case "tonNode.externalMessageBroadcast":
		return rebroadcastPlan{mode: rebroadcastModeFEC}
	default:
		return planRebroadcast(kind, payloadLen)
	}
}

func (s *overlaySubscription) checkCustomTwoStepBroadcastSource(info overlay.BroadcastPrecheckInfo) error {
	sourceID, err := NewPeerID(info.SourceID)
	if err != nil {
		s.node.noteBroadcastDrop(s.spec.Name, twoStepBroadcastKind, "invalid_source")
		return err
	}

	if _, ok := s.spec.MsgSenders[sourceID]; ok {
		return nil
	}
	if _, ok := s.spec.BlockSenders[sourceID]; ok {
		return nil
	}

	s.node.noteBroadcastDrop(s.spec.Name, twoStepBroadcastKind, "unauthorized_sender")
	return fmt.Errorf("custom overlay broadcast source %s is not configured", sourceID.String())
}

func (s *overlaySubscription) startTwoStepRebroadcastWorker(ctx context.Context) {
	if !s.spec.runsTwoStepRebroadcastWorker() {
		return
	}
	queue, ok := s.initTwoStepQueue()
	if !ok {
		return
	}

	s.node.runAsync(func() {
		s.runTwoStepRebroadcastLoop(ctx, queue)
	})
}

func (s *overlaySubscription) initTwoStepQueue() (*boundedQueue[rebroadcastRequest], bool) {
	s.mx.Lock()
	defer s.mx.Unlock()

	if s.twoStepQueueClosed {
		return nil, false
	}
	if s.twoStepQueue == nil {
		s.twoStepQueue = newBoundedQueue(peerRebroadcastQueueItems, peerRebroadcastQueueBytes, rebroadcastRequestBytes)
	}
	return s.twoStepQueue, true
}

func (s *overlaySubscription) twoStepQueueStatusSnapshot() (QueueStatusSnapshot, bool) {
	s.mx.Lock()
	defer s.mx.Unlock()

	if s.twoStepQueue == nil {
		return QueueStatusSnapshot{}, false
	}
	return s.twoStepQueue.StatusSnapshot(twoStepRebroadcastQueueName), true
}

func (s *overlaySubscription) runTwoStepRebroadcastLoop(ctx context.Context, queue *boundedQueue[rebroadcastRequest]) {
	for {
		req, ok := queue.Pop(ctx)
		if !ok {
			return
		}
		if req.expiredInQueue(time.Now()) {
			s.node.noteRebroadcastDropped(req)
			s.log.Debug().
				Str("kind", req.kind).
				Str("queue", req.queueName()).
				Msg("dropping stale custom two-step rebroadcast request")
			continue
		}

		if s.sendTwoStepRebroadcast(ctx, req) {
			s.node.noteRebroadcastSent(req)
		} else {
			s.node.noteRebroadcastDropped(req)
		}
	}
}

func (s *overlaySubscription) enqueueTwoStepRebroadcast(req rebroadcastRequest) bool {
	if len(s.twoStepCandidates(req.sourcePeerID)) == 0 {
		s.node.noteRebroadcastDropped(req)
		s.log.Debug().
			Str("kind", req.kind).
			Str("queue", req.queueName()).
			Msg("dropping custom two-step rebroadcast request because there are no peers")
		return false
	}

	queue, ok := s.initTwoStepQueue()
	if !ok {
		s.node.noteRebroadcastDropped(req)
		return false
	}

	req.queuedAt = time.Now()
	if queue.Push(req) {
		return true
	}

	s.node.noteRebroadcastDropped(req)
	s.log.Debug().
		Str("kind", req.kind).
		Str("queue", req.queueName()).
		Msg("dropping custom two-step rebroadcast request because queue is full")
	return false
}

func (s *overlaySubscription) sendTwoStepRebroadcast(ctx context.Context, req rebroadcastRequest) bool {
	payloadLen := req.payloadLen()
	if payloadLen == 0 || payloadLen > maxOverlayPayloadSize {
		return false
	}

	plan := s.rebroadcastPlan(req.kind, payloadLen)
	if plan.mode != rebroadcastModeFEC {
		return false
	}

	sendCtx, cancel := context.WithTimeout(ctx, peerRebroadcastTimeout)
	defer cancel()

	peerSet, resolveFailed := s.resolveTwoStepPeerSet(sendCtx, req.sourcePeerID)
	s.markTwoStepPeerFailures(resolveFailed)

	res, err := overlay.SendBroadcastTwoStep(sendCtx, overlay.BroadcastTwoStepSendRequest{
		Key:         s.node.privKey,
		Certificate: overlay.CertificateEmpty{},
		LocalADNLID: s.node.localID.Bytes(),
		Payload:     req.payload,
		Flags:       plan.flags,
		PeerSet:     peerSet,
	})
	s.markTwoStepPeerFailures(res.Failed)

	if err != nil && res.Sent == 0 {
		s.log.Debug().
			Err(err).
			Str("kind", req.kind).
			Int("attempted", res.Attempted+len(resolveFailed)).
			Int("failed", len(res.Failed)+len(resolveFailed)).
			Msg("failed to send custom two-step broadcast")
		return false
	}
	if err != nil {
		s.log.Debug().
			Err(err).
			Str("kind", req.kind).
			Int("sent", res.Sent).
			Int("failed", len(res.Failed)+len(resolveFailed)).
			Msg("partially sent custom two-step broadcast")
	}
	return res.Sent > 0
}

func (s *overlaySubscription) markTwoStepPeerFailures(failed []overlay.BroadcastTwoStepPeerError) {
	for _, peerErr := range failed {
		id, err := NewPeerID(peerErr.PeerID)
		if err != nil {
			continue
		}
		peer := s.peerByID(id)
		if peer == nil {
			continue
		}
		s.handlePeerQueryFailure(peer, peerErr.Err)
	}
}

func (s *overlaySubscription) peerByID(id PeerID) *overlayPeer {
	s.mx.Lock()
	defer s.mx.Unlock()

	return s.peers[id]
}

func (spec *overlaySpec) sendsShard(workchain int32, shard int64) bool {
	if len(spec.SenderShards) == 0 {
		return true
	}

	target := CustomOverlayShard{
		Workchain: workchain,
		Shard:     shard,
	}
	for _, configured := range spec.SenderShards {
		if shardsIntersect(configured, target) {
			return true
		}
	}
	return false
}

func shardsIntersect(a CustomOverlayShard, b CustomOverlayShard) bool {
	if a.Workchain != b.Workchain {
		return false
	}
	return sharddomain.Intersects(a.Shard, b.Shard)
}
