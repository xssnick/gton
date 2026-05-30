package p2p

import (
	"context"
	"fmt"
	"time"

	"github.com/xssnick/tonutils-go/adnl/overlay"
)

const (
	customTwoStepBroadcastKind        = "overlay.broadcastTwostep"
	customTwoStepRebroadcastQueueName = "custom_two_step_rebroadcast"
)

type customTwoStepPeerSet struct {
	sub          *overlaySubscription
	sourcePeerID PeerID
}

func (set customTwoStepPeerSet) Peers() []overlay.BroadcastPeer {
	if set.sub == nil {
		return nil
	}

	candidates := set.sub.rebroadcastCandidates()
	peers := make([]overlay.BroadcastPeer, 0, len(candidates))
	for _, peer := range candidates {
		if peer == nil || peer.overlay == nil {
			continue
		}
		if !set.sourcePeerID.IsZero() && peer.id == set.sourcePeerID {
			continue
		}
		peers = append(peers, peer.overlay)
	}
	return peers
}

func planCustomRebroadcast(kind string, payloadLen int) rebroadcastPlan {
	switch kind {
	case "tonNode.newShardBlockBroadcast":
		if payloadLen <= ordinarySimpleBroadcastMaxSize {
			return rebroadcastPlan{mode: rebroadcastModeSimple}
		}
		return rebroadcastPlan{mode: rebroadcastModeFEC, flags: overlay.BroadcastFlagAnySender}
	case "tonNode.externalMessageBroadcast", "tonNode.ihrMessageBroadcast":
		return rebroadcastPlan{mode: rebroadcastModeFEC}
	default:
		return planRebroadcast(kind, payloadLen)
	}
}

func (s *overlaySubscription) configureCustomTwoStepBroadcast(peer *overlayPeer) {
	if s.spec.Kind != overlayKindCustomFixed {
		return
	}

	peer.overlay.SetBroadcastPrecheckHandler(s.checkCustomTwoStepBroadcastSource)
	peer.overlay.EnableBroadcastTwoStep(s.node.localID.Bytes(), customTwoStepPeerSet{sub: s}, s.customTwoStepState())
}

func (s *overlaySubscription) checkCustomTwoStepBroadcastSource(info overlay.BroadcastPrecheckInfo) error {
	sourceID, err := NewPeerID(info.SourceID)
	if err != nil {
		s.node.noteBroadcastDrop(s.spec.Name, customTwoStepBroadcastKind, "invalid_source")
		return err
	}

	if _, ok := s.spec.MsgSenders[sourceID]; ok {
		return nil
	}
	if _, ok := s.spec.BlockSenders[sourceID]; ok {
		return nil
	}

	s.node.noteBroadcastDrop(s.spec.Name, customTwoStepBroadcastKind, "unauthorized_sender")
	return fmt.Errorf("custom overlay broadcast source %s is not configured", sourceID.String())
}

func (s *overlaySubscription) customTwoStepState() *overlay.BroadcastTwoStepState {
	s.mx.Lock()
	defer s.mx.Unlock()

	if s.twoStepState == nil {
		s.twoStepState = overlay.NewBroadcastTwoStepState()
	}
	return s.twoStepState
}

func (s *overlaySubscription) startCustomTwoStepRebroadcastWorker(ctx context.Context) {
	if s.spec.Kind != overlayKindCustomFixed {
		return
	}
	queue, ok := s.initCustomTwoStepQueue()
	if !ok {
		return
	}

	s.node.runAsync(func() {
		s.runCustomTwoStepRebroadcastLoop(ctx, queue)
	})
}

func (s *overlaySubscription) initCustomTwoStepQueue() (*boundedQueue[rebroadcastRequest], bool) {
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

func (s *overlaySubscription) customTwoStepQueueStatusSnapshot() (QueueStatusSnapshot, bool) {
	s.mx.Lock()
	defer s.mx.Unlock()

	if s.twoStepQueue == nil {
		return QueueStatusSnapshot{}, false
	}
	return s.twoStepQueue.StatusSnapshot(customTwoStepRebroadcastQueueName), true
}

func (s *overlaySubscription) runCustomTwoStepRebroadcastLoop(ctx context.Context, queue *boundedQueue[rebroadcastRequest]) {
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

		if s.sendCustomTwoStepRebroadcast(ctx, req) {
			s.node.noteRebroadcastSent(req)
		} else {
			s.node.noteRebroadcastDropped(req)
		}
	}
}

func (s *overlaySubscription) enqueueCustomTwoStepRebroadcast(req rebroadcastRequest) bool {
	peerSet := customTwoStepPeerSet{sub: s, sourcePeerID: req.sourcePeerID}
	if len(peerSet.Peers()) == 0 {
		s.node.noteRebroadcastDropped(req)
		s.log.Debug().
			Str("kind", req.kind).
			Str("queue", req.queueName()).
			Msg("dropping custom two-step rebroadcast request because there are no peers")
		return false
	}

	queue, ok := s.initCustomTwoStepQueue()
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

func (s *overlaySubscription) sendCustomTwoStepRebroadcast(ctx context.Context, req rebroadcastRequest) bool {
	payloadLen := req.payloadLen()
	if payloadLen == 0 || payloadLen > maxOverlayPayloadSize {
		return false
	}

	plan := planCustomRebroadcast(req.kind, payloadLen)
	if plan.mode != rebroadcastModeFEC {
		return false
	}

	sendCtx, cancel := context.WithTimeout(ctx, peerRebroadcastTimeout)
	defer cancel()

	res, err := overlay.SendBroadcastTwoStep(sendCtx, overlay.BroadcastTwoStepSendRequest{
		Key:         s.node.privKey,
		Certificate: overlay.CertificateEmpty{},
		LocalADNLID: s.node.localID.Bytes(),
		Payload:     req.payload,
		Flags:       plan.flags,
		PeerSet:     customTwoStepPeerSet{sub: s, sourcePeerID: req.sourcePeerID},
	})
	s.markCustomTwoStepPeerFailures(res.Failed)

	if err != nil && res.Sent == 0 {
		s.log.Debug().
			Err(err).
			Str("kind", req.kind).
			Int("attempted", res.Attempted).
			Int("failed", len(res.Failed)).
			Msg("failed to send custom two-step broadcast")
		return false
	}
	if err != nil {
		s.log.Debug().
			Err(err).
			Str("kind", req.kind).
			Int("sent", res.Sent).
			Int("failed", len(res.Failed)).
			Msg("partially sent custom two-step broadcast")
	}
	return res.Sent > 0
}

func (s *overlaySubscription) markCustomTwoStepPeerFailures(failed []overlay.BroadcastTwoStepPeerError) {
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

func (spec overlaySpec) sendsShard(workchain int32, shard int64) bool {
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
	return shardIsAncestor(a.Shard, b.Shard) || shardIsAncestor(b.Shard, a.Shard)
}
