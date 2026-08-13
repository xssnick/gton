package p2p

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
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
	// fastSyncBroadcastSpeedMultiplier divides the base delay between FastSync
	// FEC parts. Kept internal so config does not expose a traffic knob.
	// Matches the shipped validator-engine default (3.33), not the 1.0 struct
	// default in full-node.h that no deployment runs with: 10ms per four
	// symbols divided by the multiplier, i.e. ~3ms.
	fastSyncBroadcastSpeedMultiplier    = 3.33
	minFastSyncBroadcastSpeedMultiplier = 1e-9
)

var quietRebroadcastIntervals = map[string]time.Duration{
	"tonNode.blockBroadcast":                         250 * time.Millisecond,
	"tonNode.blockBroadcastCompressed":               250 * time.Millisecond,
	"tonNode.blockBroadcastCompressedV2":             250 * time.Millisecond,
	"tonNode.newBlockCandidateBroadcast":             250 * time.Millisecond,
	"tonNode.newBlockCandidateBroadcastCompressed":   250 * time.Millisecond,
	"tonNode.newBlockCandidateBroadcastCompressedV2": 250 * time.Millisecond,
	blockFinalityBroadcastKind:                       250 * time.Millisecond,
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

	key := req.subscription.spec.Name + ":" + req.kind

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
		"tonNode.newBlockCandidateBroadcastCompressedV2",
		blockFinalityBroadcastKind:
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

func planFastSyncRebroadcast(kind string, payloadLen int) rebroadcastPlan {
	if kind == "tonNode.newShardBlockBroadcast" {
		if payloadLen <= ordinarySimpleBroadcastMaxSize {
			return rebroadcastPlan{
				mode:  rebroadcastModeSimple,
				flags: overlay.BroadcastFlagNoTwoStep,
			}
		}
		return rebroadcastPlan{
			mode: rebroadcastModeFEC,
			flags: overlay.BroadcastFlagAnySender |
				overlay.BroadcastFlagNoTwoStep,
		}
	}

	return rebroadcastPlan{
		mode:  rebroadcastModeFEC,
		flags: overlay.BroadcastFlagAnySender,
	}
}

func (p *overlayPeer) initRebroadcastQueues() bool {
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
	payloadLen := req.payloadLen()
	if payloadLen > 0 {
		plan := req.subscription.rebroadcastPlan(req.kind, payloadLen)
		if req.subscription.usesTwoStepForRequest(req, plan) {
			return twoStepRebroadcastQueueName
		}
	}
	if req.local {
		return "local_rebroadcast"
	}
	return "rebroadcast"
}

func (n *Node) noteRebroadcastSent(req rebroadcastRequest) {
	if req.local {
		n.localRebroadcastSent.Add(1)
	} else {
		n.peerRebroadcastSent.Add(1)
	}
	n.noteBroadcast(
		"queue_rebroadcasted",
		req.subscription.spec.Name,
		req.kind,
		req.subscription.rebroadcastDelivery(req),
	)
}

func (n *Node) noteRebroadcastDropped(req rebroadcastRequest) {
	if req.local {
		n.localRebroadcastDropped.Add(1)
		return
	}
	n.peerRebroadcastDropped.Add(1)
}

func (s *overlaySubscription) startPeerRebroadcastWorker(peer *overlayPeer) {
	if s.node.runCtx.Err() != nil {
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
	if s.spec.Workchain == -1 && s.spec.Shard == topShard {
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
	if s.customRebroadcastUnsupported(req.kind) {
		s.node.noteRebroadcastDropped(req)
		s.log.Debug().
			Str("kind", req.kind).
			Str("queue", req.queueName()).
			Msg("dropping unsupported custom rebroadcast")
		return false
	}
	if s.customBlockRebroadcastBlocked(req.kind) {
		s.node.noteRebroadcastDropped(req)
		s.log.Debug().
			Str("kind", req.kind).
			Str("queue", req.queueName()).
			Msg("dropping custom block rebroadcast because local node is not a block sender")
		return false
	}
	if !s.node.canAcceptBroadcast(req.kind, req.local) {
		s.node.noteRebroadcastDropped(req)
		s.log.Debug().
			Str("kind", req.kind).
			Str("queue", req.queueName()).
			Msg("dropping rebroadcast request because broadcast admission is closed")
		return false
	}
	if !s.node.allowRebroadcast(&req) {
		return false
	}
	candidates := s.rebroadcastCandidatesForRequest(req)
	if len(candidates) == 0 {
		s.node.noteRebroadcastDropped(req)
		return false
	}
	if err := req.materializePayload(); err != nil {
		s.node.noteRebroadcastDropped(req)
		s.log.Debug().
			Err(err).
			Str("kind", req.kind).
			Str("queue", req.queueName()).
			Msg("dropping rebroadcast request because payload cannot be serialized")
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
	if s.usesTwoStepForRequest(req, plan) {
		return s.enqueueTwoStepRebroadcast(req)
	}
	if plan.mode == rebroadcastModeSimple && req.simple == nil {
		msg, err := s.node.buildSimpleBroadcast(req.payload, plan.flags)
		if err != nil {
			s.node.noteRebroadcastDropped(req)
			s.log.Debug().
				Err(err).
				Str("kind", req.kind).
				Str("queue", req.queueName()).
				Int("size", payloadLen).
				Msg("dropping rebroadcast request because simple envelope cannot be created")
			return false
		}
		req.simple = &msg
	}
	if plan.mode == rebroadcastModeFEC && req.fec == nil {
		fec, err := overlay.NewBroadcastFECSender(
			s.node.privKey,
			req.certificate,
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

	preferred := s.rebroadcastPreferredCandidateIDs(req)
	fanout := s.rebroadcastFanoutForRequest(req)
	attempts := 1
	if req.local {
		attempts = localRebroadcastAttempts
	}

	queued := 0
	tried := make(map[PeerID]struct{}, fanout*attempts)
	for attempt := 0; attempt < attempts && queued < fanout; attempt++ {
		targets := selectRebroadcastQueueTargetsWithPreferred(candidates, tried, preferred, fanout-queued)
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

func (s *overlaySubscription) usesTwoStepRebroadcast(
	plan rebroadcastPlan,
) bool {
	if plan.mode != rebroadcastModeFEC {
		return false
	}
	if s.spec.alwaysTwoStepFEC() {
		return true
	}
	return s.spec.twoStepFECUnlessPlanOptsOut() &&
		plan.flags&overlay.BroadcastFlagNoTwoStep == 0
}

func (s *overlaySubscription) usesTwoStepForRequest(
	req rebroadcastRequest,
	plan rebroadcastPlan,
) bool {
	return !req.forceLegacyFEC && s.usesTwoStepRebroadcast(plan)
}

func (s *overlaySubscription) rebroadcastDelivery(
	req rebroadcastRequest,
) Delivery {
	plan := s.rebroadcastPlan(req.kind, req.payloadLen())
	if s.usesTwoStepForRequest(req, plan) {
		return DeliveryTwoStep
	}
	if plan.mode == rebroadcastModeSimple {
		return DeliverySimple
	}
	return DeliveryFEC
}

func (s *overlaySubscription) customRebroadcastUnsupported(kind string) bool {
	return s.spec.dropsIHRBroadcasts() && kind == "tonNode.ihrMessageBroadcast"
}

func (s *overlaySubscription) customBlockRebroadcastBlocked(kind string) bool {
	if !s.spec.authorizesBroadcastSenders() {
		return false
	}
	if _, ok := blockOverlayFanoutClass(kind); !ok {
		return false
	}

	_, ok := s.spec.BlockSenders[s.node.localID]
	return !ok
}

func (s *overlaySubscription) rebroadcastCandidatesForRequest(req rebroadcastRequest) []*overlayPeer {
	candidates := s.broadcastTargetsSnapshot().peers
	if req.sourcePeerID.IsZero() {
		return candidates
	}

	// The candidates slice is shared with the snapshot cache, filter into a
	// fresh slice instead of truncating it in place.
	filtered := make([]*overlayPeer, 0, len(candidates))
	for _, peer := range candidates {
		if peer.id == req.sourcePeerID {
			continue
		}
		filtered = append(filtered, peer)
	}
	return filtered
}

func (s *overlaySubscription) rebroadcastPreferredCandidateIDs(req rebroadcastRequest) map[PeerID]struct{} {
	s.mx.Lock()
	neighbours := append([]PeerID(nil), s.neighbours...)
	s.mx.Unlock()

	if len(neighbours) == 0 {
		return nil
	}

	ids := make(map[PeerID]struct{}, len(neighbours))
	for _, id := range neighbours {
		if id.IsZero() || id == req.sourcePeerID {
			continue
		}
		ids[id] = struct{}{}
	}
	if len(ids) == 0 {
		return nil
	}
	return ids
}

func (s *overlaySubscription) rebroadcastFanoutForRequest(req rebroadcastRequest) int {
	if req.kind == "tonNode.externalMessageBroadcast" || req.kind == "tonNode.ihrMessageBroadcast" {
		if s.spec.floodsWholeRoster() {
			if s.node.rebroadcastLagged() {
				return laggedExternalFanout
			}
			return s.peerLimit()
		}
		if req.local {
			fanout := s.node.effectiveLocalExternalFanout()
			if s.node.rebroadcastLagged() {
				return laggedLocalExternalFanout(fanout)
			}
			return fanout
		}
		if s.node.rebroadcastLagged() {
			return laggedExternalFanout
		}
		return externalRebroadcastFanout
	}
	if s.spec.floodsWholeRoster() {
		return s.peerLimit()
	}
	if s.node.rebroadcastQuiet.Load() {
		return quietRebroadcastFanout
	}
	return rebroadcastFanout
}

func normalizeLocalExternalFanout(fanout int) (int, error) {
	if fanout == 0 {
		return externalRebroadcastFanout, nil
	}
	if fanout < minLocalExternalFanout {
		return 0, fmt.Errorf("local external broadcast fanout cannot be less than %d", minLocalExternalFanout)
	}
	if fanout > maxLocalExternalFanout {
		return 0, fmt.Errorf("local external broadcast fanout cannot exceed %d", maxLocalExternalFanout)
	}
	return fanout, nil
}

func laggedLocalExternalFanout(fanout int) int {
	fanout /= 2
	if fanout < 1 {
		return 1
	}
	return fanout
}

func (n *Node) effectiveLocalExternalFanout() int {
	if n.localExternalFanout == 0 {
		return externalRebroadcastFanout
	}
	return n.localExternalFanout
}

func selectRebroadcastQueueTargetsWithPreferred(candidates []*overlayPeer, tried map[PeerID]struct{}, preferred map[PeerID]struct{}, limit int) []*overlayPeer {
	if len(preferred) == 0 {
		return selectRebroadcastQueueTargets(candidates, tried, limit)
	}

	preferredCandidates := make([]*overlayPeer, 0, len(preferred))
	others := make([]*overlayPeer, 0, len(candidates))
	for _, peer := range candidates {
		if peer.id.IsZero() {
			continue
		}
		if _, ok := tried[peer.id]; ok {
			continue
		}
		if _, ok := preferred[peer.id]; ok {
			preferredCandidates = append(preferredCandidates, peer)
			continue
		}
		others = append(others, peer)
	}

	targets := selectRebroadcastQueueTargets(preferredCandidates, nil, limit)
	if len(targets) >= limit {
		return targets
	}
	targets = append(targets, selectRebroadcastQueueTargets(others, nil, limit-len(targets))...)
	return targets
}

func selectRebroadcastQueueTargets(candidates []*overlayPeer, tried map[PeerID]struct{}, limit int) []*overlayPeer {
	if limit <= 0 || len(candidates) == 0 {
		return nil
	}

	pool := make([]*overlayPeer, 0, len(candidates))
	for _, peer := range candidates {
		if peer.id.IsZero() {
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
	if payloadLen == 0 || payloadLen > maxOverlayPayloadSize {
		return false
	}

	plan := s.rebroadcastPlan(req.kind, payloadLen)
	if plan.mode == rebroadcastModeSimple {
		return s.rebroadcastSimpleToPeer(ctx, peer, req, plan)
	}
	return s.rebroadcastFECToPeer(ctx, peer, req, plan)
}

func (s *overlaySubscription) rebroadcastSimpleToPeer(ctx context.Context, peer *overlayPeer, req rebroadcastRequest, plan rebroadcastPlan) bool {
	msg := req.simple
	if msg == nil {
		built, err := s.node.buildSimpleBroadcast(req.payload, plan.flags)
		if err != nil {
			s.log.Debug().Err(err).Str("kind", req.kind).Msg("failed to build rebroadcast envelope")
			return false
		}
		msg = &built
	}

	sendCtx, cancel := context.WithTimeout(ctx, peerRebroadcastTimeout)
	err := peer.broadcastPeer.SendCustomMessage(sendCtx, *msg)
	cancel()
	if errors.Is(err, errQUICPeerOffline) {
		// Offline peer: the send was a no-op, not a failure. Count it as a drop
		// (false) but do not fault the peer — the ping path owns eviction.
		return false
	}
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
			req.certificate,
			req.payload,
			plan.flags,
			overlay.WithBroadcastFECSymbolSize(rebroadcastFECSymbolSize),
		)
		if err != nil {
			s.log.Debug().Err(err).Str("kind", req.kind).Msg("failed to initialize FEC rebroadcast sender")
			return false
		}
	}

	pace := time.Duration(0)
	if s.spec.pacesFECBursts() {
		pace = s.node.fastSyncBroadcastFECPace
	}
	err := sendFastFECToPeer(
		ctx,
		sender,
		peer.broadcastPeer,
		peerRebroadcastTimeout,
		pace,
	)
	if errors.Is(err, errQUICPeerOffline) {
		// Offline peer: sendFastFECToPeer stopped at the first part instead of
		// pacing through the whole burst. Count the drop without faulting the
		// peer — the ping path owns eviction.
		return false
	}
	if err != nil {
		s.markPeerQueryFailed(peer)
		s.log.Debug().
			Err(err).
			Str("kind", req.kind).
			Str("peer", peer.addr).
			Str("delivery", string(DeliveryFEC)).
			Uint32("parts", sender.TotalParts()).
			Msg("failed to rebroadcast ordinary-node FEC parts")
		return false
	}
	return true
}

func (n *Node) waitRebroadcastFECBackpressure(ctx context.Context, peer *overlayPeer, req rebroadcastRequest) (func(), bool) {
	class := rebroadcastFECBackpressureClass(req.kind)
	for {
		if !n.rebroadcastLagged() {
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

func (n *Node) rebroadcastLagged() bool {
	if n.syncLag == nil {
		return false
	}

	lagSeconds, err := n.syncLag.SyncLagSeconds()
	return err == nil && lagSeconds > rebroadcastFECLagThreshold
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

	peerID := peer.id
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
	return n.rebroadcastFECSlots[class]
}

func (n *Node) tryAcquireRebroadcastFECPeer(class rebroadcastFECLimiterClass, peerID PeerID) bool {
	n.rebroadcastFECMu.Lock()
	defer n.rebroadcastFECMu.Unlock()

	peers := n.rebroadcastFECPeers[class]
	if _, ok := peers[peerID]; ok {
		return false
	}
	peers[peerID] = struct{}{}
	return true
}

func (n *Node) releaseRebroadcastFECPeer(class rebroadcastFECLimiterClass, peerID PeerID) {
	n.rebroadcastFECMu.Lock()
	defer n.rebroadcastFECMu.Unlock()

	delete(n.rebroadcastFECPeers[class], peerID)
}

func sendFastFECToPeer(
	ctx context.Context,
	sender *overlay.BroadcastFECSender,
	peer overlay.BroadcastPeer,
	timeout,
	pace time.Duration,
) error {
	sendCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	totalParts := sender.TotalParts()
	var paceTimer *time.Timer
	defer func() {
		if paceTimer != nil {
			paceTimer.Stop()
		}
	}()

	for seqno := uint32(0); seqno < totalParts; seqno++ {
		if err := sendCtx.Err(); err != nil {
			return err
		}
		// TON ordinary FEC sends four symbols per 10ms/multiplier burst.
		if pace > 0 &&
			seqno > 0 &&
			seqno%overlay.DefaultBroadcastFECBurstSize == 0 {
			if paceTimer == nil {
				paceTimer = time.NewTimer(pace)
			} else {
				paceTimer.Reset(pace)
			}
			select {
			case <-sendCtx.Done():
				return sendCtx.Err()
			case <-paceTimer.C:
			}
		}

		part, err := sender.Part(seqno)
		if err != nil {
			return fmt.Errorf("build FEC part %d: %w", seqno, err)
		}
		if err = peer.SendCustomMessage(sendCtx, part.Full); err != nil {
			return fmt.Errorf("send FEC part %d to peer %x: %w", seqno, peer.ID(), err)
		}
	}
	return nil
}

func normalizeFastSyncBroadcastFECPace(multiplier float64) (time.Duration, error) {
	if multiplier == 0 {
		multiplier = 1
	}
	if multiplier < 0 || math.IsNaN(multiplier) || math.IsInf(multiplier, 0) {
		return 0, fmt.Errorf("fast sync broadcast speed multiplier must be finite and positive")
	}
	if multiplier < minFastSyncBroadcastSpeedMultiplier {
		multiplier = minFastSyncBroadcastSpeedMultiplier
	}

	return time.Duration(float64(overlay.DefaultBroadcastFECPace) / multiplier), nil
}

func (req rebroadcastRequest) expiredInQueue(now time.Time) bool {
	return !req.queuedAt.IsZero() && now.Sub(req.queuedAt) > rebroadcastQueueMaxAge
}

func (n *Node) buildSimpleBroadcast(payload []byte, flags int32) (overlay.Broadcast, error) {
	msg := overlay.Broadcast{
		Source:      keys.PublicKeyED25519{Key: n.privKey.Public().(ed25519.PublicKey)},
		Certificate: overlay.CertificateEmpty{},
		Flags:       flags,
		Data:        payload,
		Date:        int32(time.Now().Unix()),
	}
	if err := msg.Sign(n.privKey); err != nil {
		return overlay.Broadcast{}, err
	}
	return msg, nil
}
