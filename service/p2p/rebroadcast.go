package p2p

import (
	"context"
	"crypto/ed25519"
	"math/rand/v2"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

const (
	rebroadcastFECSymbolSize = ordinarySimpleBroadcastMaxSize
	rebroadcastFECBurstSize  = 4
	rebroadcastFECPace       = 3 * time.Millisecond
	rebroadcastFanout        = 5
	quietRebroadcastFanout   = 2
)

var quietRebroadcastIntervals = map[string]time.Duration{
	"tonNode.blockBroadcast":             250 * time.Millisecond,
	"tonNode.blockBroadcastCompressed":   250 * time.Millisecond,
	"tonNode.blockBroadcastCompressedV2": 250 * time.Millisecond,
	"tonNode.newShardBlockBroadcast":     500 * time.Millisecond,
	"tonNode.externalMessageBroadcast":   100 * time.Millisecond,
	"tonNode.ihrMessageBroadcast":        100 * time.Millisecond,
}

type rebroadcastMode uint8

const (
	rebroadcastModeSimple rebroadcastMode = iota
	rebroadcastModeFEC
)

type rebroadcastPlan struct {
	mode  rebroadcastMode
	flags int32
}

func (n *Node) SetRebroadcastQuiet(quiet bool) {
	if n.rebroadcastQuiet.Swap(quiet) == quiet {
		return
	}

	n.rebroadcastThrottleMu.Lock()
	clear(n.rebroadcastThrottleLast)
	n.rebroadcastThrottleMu.Unlock()

	n.log.Info().
		Bool("quiet", quiet).
		Msg("updated rebroadcast throttle")
}

func (n *Node) allowRebroadcast(req *rebroadcastRequest) bool {
	if req == nil {
		return false
	}
	if !n.rebroadcastQuiet.Load() {
		return true
	}

	interval := quietRebroadcastIntervals[req.kind]
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}

	key := req.kind
	if req.subscription != nil {
		key = req.subscription.spec.Name + ":" + req.kind
	}

	now := time.Now()
	n.rebroadcastThrottleMu.Lock()
	defer n.rebroadcastThrottleMu.Unlock()

	last := n.rebroadcastThrottleLast[key]
	if !last.IsZero() && now.Sub(last) < interval {
		return false
	}
	n.rebroadcastThrottleLast[key] = now
	return true
}

func planRebroadcast(kind string, payloadLen int) rebroadcastPlan {
	switch kind {
	case "tonNode.blockBroadcast", "tonNode.blockBroadcastCompressed", "tonNode.blockBroadcastCompressedV2":
		return rebroadcastPlan{mode: rebroadcastModeFEC, flags: overlay.BroadcastFlagAnySender}
	case "tonNode.newShardBlockBroadcast":
		if payloadLen <= ordinarySimpleBroadcastMaxSize {
			return rebroadcastPlan{mode: rebroadcastModeSimple}
		}
		return rebroadcastPlan{mode: rebroadcastModeFEC, flags: overlay.BroadcastFlagAnySender}
	case "tonNode.externalMessageBroadcast", "tonNode.ihrMessageBroadcast":
		if payloadLen <= ordinarySimpleBroadcastMaxSize {
			return rebroadcastPlan{mode: rebroadcastModeSimple}
		}
		return rebroadcastPlan{mode: rebroadcastModeFEC}
	default:
		if payloadLen <= ordinarySimpleBroadcastMaxSize {
			return rebroadcastPlan{mode: rebroadcastModeSimple}
		}
		return rebroadcastPlan{mode: rebroadcastModeFEC}
	}
}

func calcFECRebroadcastParts(payloadLen int, symbolSize uint32) uint32 {
	if payloadLen <= 0 || symbolSize == 0 {
		return 0
	}
	return uint32(payloadLen/int(symbolSize)+1) * 2
}

func (p *overlayPeer) initRebroadcastQueues() bool {
	if p == nil {
		return false
	}

	p.rebroadcastMx.Lock()
	defer p.rebroadcastMx.Unlock()

	if p.rebroadcastClosed {
		return false
	}
	if p.localRebroadcastQueue == nil {
		p.localRebroadcastQueue = newBoundedQueue(peerRebroadcastQueueItems, peerRebroadcastQueueBytes, rebroadcastRequestBytes)
	}
	if p.rebroadcastQueue == nil {
		p.rebroadcastQueue = newBoundedQueue(peerRebroadcastQueueItems, peerRebroadcastQueueBytes, rebroadcastRequestBytes)
	}
	return true
}

func (p *overlayPeer) closeRebroadcastQueues() {
	if p == nil {
		return
	}

	p.rebroadcastMx.Lock()
	local := p.localRebroadcastQueue
	regular := p.rebroadcastQueue
	p.rebroadcastClosed = true
	p.rebroadcastMx.Unlock()

	if local != nil {
		local.Close()
	}
	if regular != nil {
		regular.Close()
	}
}

func (p *overlayPeer) pushRebroadcast(req rebroadcastRequest) bool {
	if !p.initRebroadcastQueues() {
		return false
	}

	p.rebroadcastMx.Lock()
	local := p.localRebroadcastQueue
	regular := p.rebroadcastQueue
	p.rebroadcastMx.Unlock()

	if req.local {
		return local.Push(req)
	}
	return regular.Push(req)
}

func (p *overlayPeer) rebroadcastQueueSnapshots() (QueueStatusSnapshot, QueueStatusSnapshot, bool) {
	if p == nil {
		return QueueStatusSnapshot{}, QueueStatusSnapshot{}, false
	}

	p.rebroadcastMx.Lock()
	local := p.localRebroadcastQueue
	regular := p.rebroadcastQueue
	p.rebroadcastMx.Unlock()

	if local == nil && regular == nil {
		return QueueStatusSnapshot{}, QueueStatusSnapshot{}, false
	}

	var localSnapshot QueueStatusSnapshot
	if local != nil {
		localSnapshot = local.StatusSnapshot("local_rebroadcast")
	}

	var regularSnapshot QueueStatusSnapshot
	if regular != nil {
		regularSnapshot = regular.StatusSnapshot("rebroadcast")
	}

	return localSnapshot, regularSnapshot, true
}

func (req rebroadcastRequest) queueName() string {
	if req.local {
		return "local_rebroadcast"
	}
	return "rebroadcast"
}

func (n *Node) closePeerRebroadcastQueues() {
	for _, sub := range n.subscriptionsSnapshot() {
		for _, peer := range sub.peersSnapshot() {
			peer.closeRebroadcastQueues()
		}
	}
}

func (n *Node) noteRebroadcastSent(req rebroadcastRequest) {
	if req.local {
		n.localRebroadcastSent.Add(1)
	} else {
		n.peerRebroadcastSent.Add(1)
	}
	n.noteBroadcast("rebroadcasted", req.overlayName(), req.kind)
}

func (n *Node) noteRebroadcastDropped(req rebroadcastRequest) {
	if req.local {
		n.localRebroadcastDropped.Add(1)
		return
	}
	n.peerRebroadcastDropped.Add(1)
}

func (s *overlaySubscription) startPeerRebroadcastWorker(peer *overlayPeer) {
	if s == nil || s.node == nil || s.node.runCtx == nil || peer == nil {
		return
	}
	if !peer.initRebroadcastQueues() {
		return
	}

	s.node.runAsync(func() {
		s.runPeerRebroadcastLoop(s.node.runCtx, peer)
	})
}

func (s *overlaySubscription) runPeerRebroadcastLoop(ctx context.Context, peer *overlayPeer) {
	for {
		peer.rebroadcastMx.Lock()
		local := peer.localRebroadcastQueue
		regular := peer.rebroadcastQueue
		peer.rebroadcastMx.Unlock()
		if local == nil || regular == nil {
			return
		}

		req, ok := popPriority(ctx, local, regular)
		if !ok {
			return
		}
		if req.sourcePeerID != "" && req.sourcePeerID == peer.id {
			continue
		}

		if s.rebroadcastToPeer(ctx, peer, req) {
			s.node.noteRebroadcastSent(req)
		} else {
			s.node.noteRebroadcastDropped(req)
		}
	}
}

func (s *overlaySubscription) enqueueRebroadcast(req rebroadcastRequest) bool {
	if s == nil || s.node == nil {
		return false
	}
	if len(req.payload) == 0 || len(req.payload) > maxOverlayPayloadSize {
		s.node.noteRebroadcastDropped(req)
		s.log.Debug().
			Str("kind", req.kind).
			Str("queue", req.queueName()).
			Int("size", len(req.payload)).
			Msg("dropping rebroadcast request because payload size is invalid")
		return false
	}

	candidates := s.rebroadcastCandidatesForRequest(req)
	fanout := s.rebroadcastFanoutForRequest(req)
	attempts := 1
	shuffle := req.kind == "tonNode.externalMessageBroadcast" || req.kind == "tonNode.ihrMessageBroadcast"
	if req.local {
		attempts = localRebroadcastAttempts
		shuffle = true
	}

	queued := 0
	tried := make(map[string]struct{}, fanout*attempts)
	for attempt := 0; attempt < attempts && queued == 0; attempt++ {
		targets := selectRebroadcastQueueTargets(candidates, tried, fanout, shuffle)
		if len(targets) == 0 {
			break
		}

		for _, peer := range targets {
			tried[peer.id] = struct{}{}
			if peer.pushRebroadcast(req) {
				queued++
				if req.local {
					return true
				}
			}
		}
	}

	if queued > 0 {
		return true
	}

	s.node.noteRebroadcastDropped(req)
	s.log.Debug().
		Str("kind", req.kind).
		Str("queue", req.queueName()).
		Int("candidates", len(candidates)).
		Int("attempts", attempts).
		Msg("dropping rebroadcast request because peer queues are full")
	return false
}

func (s *overlaySubscription) rebroadcastCandidatesForRequest(req rebroadcastRequest) []*overlayPeer {
	candidates := s.rebroadcastCandidates()
	if req.sourcePeerID == "" {
		return candidates
	}

	filtered := candidates[:0]
	for _, peer := range candidates {
		if peer.id == req.sourcePeerID {
			continue
		}
		filtered = append(filtered, peer)
	}
	return filtered
}

func (s *overlaySubscription) rebroadcastFanoutForRequest(req rebroadcastRequest) int {
	if req.kind == "tonNode.externalMessageBroadcast" || req.kind == "tonNode.ihrMessageBroadcast" {
		return externalRebroadcastFanout
	}
	if s.node != nil && s.node.rebroadcastQuiet.Load() {
		return quietRebroadcastFanout
	}
	return rebroadcastFanout
}

func selectRebroadcastQueueTargets(candidates []*overlayPeer, tried map[string]struct{}, limit int, shuffle bool) []*overlayPeer {
	if limit <= 0 || len(candidates) == 0 {
		return nil
	}

	pool := make([]*overlayPeer, 0, len(candidates))
	for _, peer := range candidates {
		if peer == nil || peer.id == "" {
			continue
		}
		if _, ok := tried[peer.id]; ok {
			continue
		}
		pool = append(pool, peer)
	}
	if len(pool) == 0 {
		return nil
	}
	if shuffle {
		rand.Shuffle(len(pool), func(i, j int) {
			pool[i], pool[j] = pool[j], pool[i]
		})
	}
	if len(pool) > limit {
		pool = pool[:limit]
	}
	return pool
}

func (s *overlaySubscription) rebroadcastToPeer(ctx context.Context, peer *overlayPeer, req rebroadcastRequest) bool {
	if peer == nil || peer.overlay == nil || len(req.payload) == 0 || len(req.payload) > maxOverlayPayloadSize {
		return false
	}

	plan := planRebroadcast(req.kind, len(req.payload))
	switch plan.mode {
	case rebroadcastModeSimple:
		return s.rebroadcastSimpleToPeer(ctx, peer, req, plan)
	case rebroadcastModeFEC:
		return s.rebroadcastFECToPeer(ctx, peer, req, plan)
	default:
		s.log.Debug().Str("kind", req.kind).Msg("skipping rebroadcast because the delivery mode is unknown")
		return false
	}
}

func (s *overlaySubscription) rebroadcastSimpleToPeer(ctx context.Context, peer *overlayPeer, req rebroadcastRequest, plan rebroadcastPlan) bool {
	msg, err := s.node.buildSimpleBroadcast(req.payload, plan.flags)
	if err != nil {
		s.log.Debug().Err(err).Str("kind", req.kind).Msg("failed to build rebroadcast envelope")
		return false
	}

	sendCtx, cancel := context.WithTimeout(ctx, peerRebroadcastTimeout)
	err = peer.overlay.SendCustomMessage(sendCtx, msg)
	cancel()
	if err != nil {
		s.handlePeerQueryFailure(peer, err)
		s.log.Debug().
			Err(err).
			Str("kind", req.kind).
			Str("peer", peer.addr).
			Str("delivery", string(DeliverySimple)).
			Msg("failed to rebroadcast ordinary-node message")
		return false
	}

	return true
}

func (s *overlaySubscription) rebroadcastFECToPeer(ctx context.Context, peer *overlayPeer, req rebroadcastRequest, plan rebroadcastPlan) bool {
	totalParts := calcFECRebroadcastParts(len(req.payload), rebroadcastFECSymbolSize)
	sentParts, _, failedBatches := rebroadcastFECToPeersWithTimeout(
		ctx,
		s.log,
		s.node.privKey,
		[]overlay.BroadcastPeer{peer.overlay},
		req,
		plan,
		peerRebroadcastTimeout,
	)
	if sentParts < totalParts || failedBatches > 0 {
		if sentParts == 0 {
			s.markPeerQueryFailed(peer)
		}
		s.log.Debug().
			Str("kind", req.kind).
			Str("peer", peer.addr).
			Str("delivery", string(DeliveryFEC)).
			Uint32("parts", totalParts).
			Uint32("parts_sent", sentParts).
			Int("failed_batches", failedBatches).
			Msg("failed to rebroadcast ordinary-node message completely")
		return false
	}
	return true
}

func rebroadcastFECToPeers(ctx context.Context, log zerolog.Logger, key ed25519.PrivateKey, peers []overlay.BroadcastPeer, req rebroadcastRequest, plan rebroadcastPlan) (uint32, int, int) {
	return rebroadcastFECToPeersWithTimeout(ctx, log, key, peers, req, plan, peerQueryTimeout)
}

func rebroadcastFECToPeersWithTimeout(ctx context.Context, log zerolog.Logger, key ed25519.PrivateKey, peers []overlay.BroadcastPeer, req rebroadcastRequest, plan rebroadcastPlan, timeout time.Duration) (uint32, int, int) {
	if len(peers) == 0 {
		return 0, 0, 0
	}

	sender, err := overlay.NewBroadcastFECSender(
		key,
		overlay.CertificateEmpty{},
		req.payload,
		plan.flags,
		overlay.WithBroadcastFECSymbolSize(rebroadcastFECSymbolSize),
	)
	if err != nil {
		log.Debug().Err(err).Str("kind", req.kind).Msg("failed to initialize FEC rebroadcast sender")
		return 0, 0, 1
	}

	peerSet := overlay.StaticBroadcastPeerSet(peers)
	remaining := calcFECRebroadcastParts(len(req.payload), rebroadcastFECSymbolSize)
	var (
		sentParts     uint32
		batches       int
		failedBatches int
	)
	for remaining > 0 {
		batch := uint32(rebroadcastFECBurstSize)
		if remaining < batch {
			batch = remaining
		}
		batches++

		sendCtx, cancel := context.WithTimeout(ctx, timeout)
		sent, err := sender.SendNow(sendCtx, peerSet, batch)
		cancel()

		if err != nil {
			failedBatches++
			log.Debug().
				Err(err).
				Str("kind", req.kind).
				Str("delivery", string(DeliveryFEC)).
				Uint32("batch", batch).
				Msg("failed to rebroadcast ordinary-node message")
		}
		if sent == 0 {
			return sentParts, batches, failedBatches
		}
		sentParts += sent
		if sent >= remaining {
			return sentParts, batches, failedBatches
		}
		remaining -= sent

		select {
		case <-ctx.Done():
			return sentParts, batches, failedBatches
		case <-time.After(rebroadcastFECPace):
		}
	}
	return sentParts, batches, failedBatches
}

func (n *Node) buildSimpleBroadcast(payload []byte, flags int32) (overlay.Broadcast, error) {
	pub := n.privKey.Public().(ed25519.PublicKey)
	msg := overlay.Broadcast{
		Source:      keys.PublicKeyED25519{Key: pub},
		Certificate: overlay.CertificateEmpty{},
		Flags:       flags,
		Data:        append([]byte(nil), payload...),
		Date:        int32(time.Now().Unix()),
	}

	sourceID := make([]byte, 32)
	if msg.Flags&overlay.BroadcastFlagAnySender == 0 {
		var err error
		sourceID, err = tl.Hash(msg.Source)
		if err != nil {
			return overlay.Broadcast{}, err
		}
	}

	broadcastHash, err := tl.Hash(OverlayBroadcastID{
		Source:   sourceID,
		DataHash: hashSimpleBroadcastPayload(msg.Data),
		Flags:    msg.Flags,
	})
	if err != nil {
		return overlay.Broadcast{}, err
	}

	toSign, err := tl.Serialize(overlay.BroadcastToSign{
		Hash: broadcastHash,
		Date: uint32(msg.Date),
	}, true)
	if err != nil {
		return overlay.Broadcast{}, err
	}

	msg.Signature = ed25519.Sign(n.privKey, toSign)
	return msg, nil
}
