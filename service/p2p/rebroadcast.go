package p2p

import (
	"context"
	"crypto/ed25519"
	"math/rand/v2"
	"time"

	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

const (
	rebroadcastFECSymbolSize     = ordinarySimpleBroadcastMaxSize
	rebroadcastQueueMaxAge       = 7 * time.Second
	rebroadcastFECLagThreshold   = int64(5)
	rebroadcastFECWaitPoll       = 10 * time.Millisecond
	blockFECBackpressureSlots    = 2
	externalFECBackpressureSlots = 4
	masterPeerRebroadcastWorkers = 1
	basePeerRebroadcastWorkers   = 2
	rebroadcastFanout            = 5
	quietRebroadcastFanout       = 2
)

var quietRebroadcastIntervals = map[string]time.Duration{
	"tonNode.blockBroadcast":                         250 * time.Millisecond,
	"tonNode.blockBroadcastCompressed":               250 * time.Millisecond,
	"tonNode.blockBroadcastCompressedV2":             250 * time.Millisecond,
	"tonNode.newBlockCandidateBroadcast":             250 * time.Millisecond,
	"tonNode.newBlockCandidateBroadcastCompressed":   250 * time.Millisecond,
	"tonNode.newBlockCandidateBroadcastCompressedV2": 250 * time.Millisecond,
	"tonNode.newShardBlockBroadcast":                 500 * time.Millisecond,
	"tonNode.externalMessageBroadcast":               100 * time.Millisecond,
	"tonNode.ihrMessageBroadcast":                    100 * time.Millisecond,
}

type rebroadcastMode uint8

const (
	rebroadcastModeSimple rebroadcastMode = iota
	rebroadcastModeFEC
)

type rebroadcastFECLimiterClass uint8

const (
	rebroadcastFECLimiterBlock rebroadcastFECLimiterClass = iota
	rebroadcastFECLimiterExternal
)

type rebroadcastPlan struct {
	mode  rebroadcastMode
	flags int32
}

func newRebroadcastFECSlotLimits() map[rebroadcastFECLimiterClass]chan struct{} {
	return map[rebroadcastFECLimiterClass]chan struct{}{
		rebroadcastFECLimiterBlock:    make(chan struct{}, blockFECBackpressureSlots),
		rebroadcastFECLimiterExternal: make(chan struct{}, externalFECBackpressureSlots),
	}
}

func newRebroadcastFECPeerLimits() map[rebroadcastFECLimiterClass]map[PeerID]struct{} {
	return map[rebroadcastFECLimiterClass]map[PeerID]struct{}{
		rebroadcastFECLimiterBlock:    {},
		rebroadcastFECLimiterExternal: {},
	}
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
	if req.local {
		return true
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
	case "tonNode.blockBroadcast", "tonNode.blockBroadcastCompressed", "tonNode.blockBroadcastCompressedV2",
		"tonNode.newBlockCandidateBroadcast", "tonNode.newBlockCandidateBroadcastCompressed",
		"tonNode.newBlockCandidateBroadcastCompressedV2":
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

func (s *overlaySubscription) rebroadcastPlan(kind string, payloadLen int) rebroadcastPlan {
	if s != nil && s.spec.Kind == overlayKindCustomFixed {
		return planCustomRebroadcast(kind, payloadLen)
	}
	return planRebroadcast(kind, payloadLen)
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
	if req.subscription != nil && req.subscription.spec.Kind == overlayKindCustomFixed {
		payloadLen := req.payloadLen()
		if payloadLen > 0 && planCustomRebroadcast(req.kind, payloadLen).mode == rebroadcastModeFEC {
			return customTwoStepRebroadcastQueueName
		}
	}
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
	n.noteBroadcast("queue_rebroadcasted", req.overlayName(), req.kind)
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

	for i := 0; i < s.peerRebroadcastWorkerCount(); i++ {
		s.node.runAsync(func() {
			s.runPeerRebroadcastLoop(s.node.runCtx, peer)
		})
	}
}

func (s *overlaySubscription) peerRebroadcastWorkerCount() int {
	if s != nil && s.spec.Workchain == -1 && s.spec.Shard == topShard {
		return masterPeerRebroadcastWorkers
	}
	return basePeerRebroadcastWorkers
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
		if req.expiredInQueue(time.Now()) {
			s.node.noteRebroadcastDropped(req)
			s.log.Debug().
				Str("kind", req.kind).
				Str("queue", req.queueName()).
				Msg("dropping stale rebroadcast request")
			continue
		}
		if !req.sourcePeerID.IsZero() && req.sourcePeerID == peer.id {
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
	payloadLen := req.payloadLen()
	if payloadLen == 0 || payloadLen > maxOverlayPayloadSize {
		s.node.noteRebroadcastDropped(req)
		s.log.Debug().
			Str("kind", req.kind).
			Str("queue", req.queueName()).
			Int("size", payloadLen).
			Msg("dropping rebroadcast request because payload size is invalid")
		return false
	}

	plan := s.rebroadcastPlan(req.kind, payloadLen)
	if s.spec.Kind == overlayKindCustomFixed && plan.mode == rebroadcastModeFEC {
		return s.enqueueCustomTwoStepRebroadcast(req)
	}
	if plan.mode == rebroadcastModeFEC && req.fec == nil {
		fec, err := overlay.NewBroadcastFECSender(
			s.node.privKey,
			overlay.CertificateEmpty{},
			req.payload,
			plan.flags,
			overlay.WithBroadcastFECSymbolSize(rebroadcastFECSymbolSize),
		)
		if err != nil {
			s.node.noteRebroadcastDropped(req)
			s.log.Debug().
				Err(err).
				Str("kind", req.kind).
				Str("queue", req.queueName()).
				Int("size", payloadLen).
				Msg("dropping rebroadcast request because FEC sender cannot be created")
			return false
		}
		req.fec = fec
		req.payload = nil
		req.payloadSize = payloadLen
	}
	req.queuedAt = time.Now()

	candidates := s.rebroadcastCandidatesForRequest(req)
	fanout := s.rebroadcastFanoutForRequest(req)
	attempts := 1
	if req.local {
		attempts = localRebroadcastAttempts
	}

	queued := 0
	tried := make(map[PeerID]struct{}, fanout*attempts)
	for attempt := 0; attempt < attempts && queued < fanout; attempt++ {
		targets := selectRebroadcastQueueTargets(candidates, tried, fanout-queued)
		if len(targets) == 0 {
			break
		}

		for _, peer := range targets {
			tried[peer.id] = struct{}{}
			if peer.pushRebroadcast(req) {
				queued++
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
	if req.sourcePeerID.IsZero() {
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
	if s != nil && s.spec.Kind == overlayKindCustomFixed {
		return s.peerLimit()
	}
	if req.kind == "tonNode.externalMessageBroadcast" || req.kind == "tonNode.ihrMessageBroadcast" {
		return externalRebroadcastFanout
	}
	if s.node != nil && s.node.rebroadcastQuiet.Load() {
		return quietRebroadcastFanout
	}
	return rebroadcastFanout
}

func selectRebroadcastQueueTargets(candidates []*overlayPeer, tried map[PeerID]struct{}, limit int) []*overlayPeer {
	if limit <= 0 || len(candidates) == 0 {
		return nil
	}

	pool := make([]*overlayPeer, 0, len(candidates))
	for _, peer := range candidates {
		if peer == nil || peer.id.IsZero() {
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

	if len(pool) <= limit {
		return pool
	}

	// Match cppnode get_neighbours: sample unique peers from the full pool.
	targets := make([]*overlayPeer, 0, limit)
	for remaining := len(pool); len(targets) < limit; remaining-- {
		idx := rand.IntN(remaining)
		targets = append(targets, pool[idx])
		pool[idx] = pool[remaining-1]
	}

	return targets
}

func (s *overlaySubscription) rebroadcastToPeer(ctx context.Context, peer *overlayPeer, req rebroadcastRequest) bool {
	payloadLen := req.payloadLen()
	if peer == nil || peer.overlay == nil || payloadLen == 0 || payloadLen > maxOverlayPayloadSize {
		return false
	}

	plan := s.rebroadcastPlan(req.kind, payloadLen)
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
	releaseBackpressure, ok := s.node.waitRebroadcastFECBackpressure(ctx, peer, req)
	if !ok {
		return false
	}
	defer releaseBackpressure()
	if req.expiredInQueue(time.Now()) {
		return false
	}

	sender := req.fec
	if sender == nil {
		var err error
		sender, err = overlay.NewBroadcastFECSender(
			s.node.privKey,
			overlay.CertificateEmpty{},
			req.payload,
			plan.flags,
			overlay.WithBroadcastFECSymbolSize(rebroadcastFECSymbolSize),
		)
		if err != nil {
			s.log.Debug().Err(err).Str("kind", req.kind).Msg("failed to initialize FEC rebroadcast sender")
			return false
		}
	}

	if err := runFECBroadcasterToPeer(ctx, sender, peer.overlay, peerRebroadcastTimeout); err != nil {
		s.markPeerQueryFailed(peer)
		s.log.Debug().
			Err(err).
			Str("kind", req.kind).
			Str("peer", peer.addr).
			Str("delivery", string(DeliveryFEC)).
			Uint32("parts", sender.TotalParts()).
			Msg("failed to rebroadcast ordinary-node message completely")
		return false
	}
	return true
}

func (n *Node) waitRebroadcastFECBackpressure(ctx context.Context, peer *overlayPeer, req rebroadcastRequest) (func(), bool) {
	class := rebroadcastFECBackpressureClass(req.kind)
	for {
		if !n.rebroadcastFECBackpressureActive() {
			return func() {}, true
		}
		if req.expiredInQueue(time.Now()) {
			return nil, false
		}
		if release, ok := n.tryAcquireRebroadcastFECBackpressure(class, peer); ok {
			return release, true
		}

		timer := time.NewTimer(rebroadcastFECWaitPoll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, false
		case <-timer.C:
		}
	}
}

func (n *Node) rebroadcastFECBackpressureActive() bool {
	if n == nil || n.syncLag == nil {
		return false
	}

	lagSeconds, ok := n.syncLag.SyncLagSeconds()
	return ok && lagSeconds > rebroadcastFECLagThreshold
}

func rebroadcastFECBackpressureClass(kind string) rebroadcastFECLimiterClass {
	switch kind {
	case "tonNode.externalMessageBroadcast", "tonNode.ihrMessageBroadcast":
		return rebroadcastFECLimiterExternal
	default:
		return rebroadcastFECLimiterBlock
	}
}

func (n *Node) tryAcquireRebroadcastFECBackpressure(class rebroadcastFECLimiterClass, peer *overlayPeer) (func(), bool) {
	slots := n.rebroadcastFECSlotLimit(class)
	if slots == nil {
		return func() {}, true
	}

	peerID := rebroadcastBackpressurePeerID(peer)
	peerAcquired := false
	if !peerID.IsZero() {
		if !n.tryAcquireRebroadcastFECPeer(class, peerID) {
			return nil, false
		}
		peerAcquired = true
	}

	select {
	case slots <- struct{}{}:
		return func() {
			<-slots
			if peerAcquired {
				n.releaseRebroadcastFECPeer(class, peerID)
			}
		}, true
	default:
		if peerAcquired {
			n.releaseRebroadcastFECPeer(class, peerID)
		}
		return nil, false
	}
}

func (n *Node) rebroadcastFECSlotLimit(class rebroadcastFECLimiterClass) chan struct{} {
	if n == nil {
		return nil
	}
	if n.rebroadcastFECSlots == nil {
		n.rebroadcastFECSlots = newRebroadcastFECSlotLimits()
	}
	return n.rebroadcastFECSlots[class]
}

func (n *Node) tryAcquireRebroadcastFECPeer(class rebroadcastFECLimiterClass, peerID PeerID) bool {
	n.rebroadcastFECMu.Lock()
	defer n.rebroadcastFECMu.Unlock()

	if n.rebroadcastFECPeers == nil {
		n.rebroadcastFECPeers = newRebroadcastFECPeerLimits()
	}
	peers := n.rebroadcastFECPeers[class]
	if peers == nil {
		peers = map[PeerID]struct{}{}
		n.rebroadcastFECPeers[class] = peers
	}
	if _, ok := peers[peerID]; ok {
		return false
	}
	peers[peerID] = struct{}{}
	return true
}

func (n *Node) releaseRebroadcastFECPeer(class rebroadcastFECLimiterClass, peerID PeerID) {
	n.rebroadcastFECMu.Lock()
	defer n.rebroadcastFECMu.Unlock()

	if n.rebroadcastFECPeers == nil {
		return
	}
	delete(n.rebroadcastFECPeers[class], peerID)
}

func rebroadcastBackpressurePeerID(peer *overlayPeer) PeerID {
	if peer == nil {
		return PeerID{}
	}
	return peer.id
}

func runFECBroadcasterToPeer(ctx context.Context, sender *overlay.BroadcastFECSender, peer overlay.BroadcastPeer, timeout time.Duration) error {
	if peer == nil {
		return nil
	}
	broadcaster, err := overlay.NewBroadcastFECBroadcaster(sender, overlay.StaticBroadcastPeerSet{peer})
	if err != nil {
		return err
	}

	sendCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return broadcaster.Run(sendCtx)
}

func (req rebroadcastRequest) expiredInQueue(now time.Time) bool {
	return !req.queuedAt.IsZero() && now.Sub(req.queuedAt) > rebroadcastQueueMaxAge
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
