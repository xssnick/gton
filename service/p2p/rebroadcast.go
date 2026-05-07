package p2p

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
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

type rebroadcastTarget struct {
	peer overlay.BroadcastPeer
	id   string
	addr string
}

func (s *overlaySubscription) rebroadcastTargets() []rebroadcastTarget {
	peers := s.rebroadcastCandidates()
	fanout := rebroadcastFanout
	if s.node.rebroadcastQuiet.Load() {
		fanout = quietRebroadcastFanout
	}
	if len(peers) > fanout {
		peers = peers[:fanout]
	}
	res := make([]rebroadcastTarget, 0, len(peers))
	for _, peer := range peers {
		res = append(res, rebroadcastTarget{
			peer: peer.overlay,
			id:   peer.id,
			addr: peer.addr,
		})
	}
	return res
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

func (n *Node) runRebroadcastLoop(ctx context.Context) {
	for {
		req, ok := popPriority(ctx, n.localRebroadcastQueue, n.rebroadcastQueue)
		if !ok {
			return
		}

		req.subscription.rebroadcast(ctx, req)
	}
}

func (s *overlaySubscription) rebroadcast(ctx context.Context, req rebroadcastRequest) {
	if len(req.payload) == 0 {
		return
	}
	if len(req.payload) > maxOverlayPayloadSize {
		s.log.Debug().
			Str("kind", req.kind).
			Int("size", len(req.payload)).
			Msg("skipping rebroadcast because payload is too large")
		return
	}

	plan := planRebroadcast(req.kind, len(req.payload))
	switch plan.mode {
	case rebroadcastModeSimple:
		s.rebroadcastSimple(ctx, req, plan)
	case rebroadcastModeFEC:
		s.rebroadcastFEC(ctx, req, plan)
	default:
		s.log.Debug().Str("kind", req.kind).Msg("skipping rebroadcast because the delivery mode is unknown")
	}
}

func (s *overlaySubscription) rebroadcastSimple(ctx context.Context, req rebroadcastRequest, plan rebroadcastPlan) {
	targets := s.rebroadcastTargets()
	if len(targets) == 0 {
		s.log.Debug().
			Str("kind", req.kind).
			Str("delivery", string(DeliverySimple)).
			Msg("skipping rebroadcast because there are no target peers")
		return
	}

	targetList := formatRebroadcastTargets(targets)
	s.log.Debug().
		Str("kind", req.kind).
		Str("delivery", string(DeliverySimple)).
		Int("size", len(req.payload)).
		Int32("flags", plan.flags).
		Strs("targets", targetList).
		Msg("rebroadcasting ordinary-node message")

	sentTo, failedTo := rebroadcastSimpleToPeers(ctx, s.log, func(payload []byte, flags int32) (overlay.Broadcast, error) {
		return s.node.buildSimpleBroadcast(payload, flags)
	}, rebroadcastTargetPeers(targets), req, plan)

	s.log.Debug().
		Str("kind", req.kind).
		Str("delivery", string(DeliverySimple)).
		Int("size", len(req.payload)).
		Strs("targets", targetList).
		Int("sent_to", sentTo).
		Int("failed_to", failedTo).
		Msg("rebroadcasted ordinary-node message")
}

func (s *overlaySubscription) rebroadcastFEC(ctx context.Context, req rebroadcastRequest, plan rebroadcastPlan) {
	// TODO: Match cppnode's legacy overlay FEC relay more closely by forwarding
	// validated inbound FEC parts before full payload decode. Today we only start
	// rebroadcast after the broadcast payload is decoded by tonutils-go, because
	// the public Go overlay API does not yet expose an inbound-part relay hook.
	targets := s.rebroadcastTargets()
	if len(targets) == 0 {
		s.log.Debug().
			Str("kind", req.kind).
			Str("delivery", string(DeliveryFEC)).
			Msg("skipping rebroadcast because there are no target peers")
		return
	}

	targetList := formatRebroadcastTargets(targets)
	totalParts := calcFECRebroadcastParts(len(req.payload), rebroadcastFECSymbolSize)
	s.log.Debug().
		Str("kind", req.kind).
		Str("delivery", string(DeliveryFEC)).
		Int("size", len(req.payload)).
		Int32("flags", plan.flags).
		Uint32("parts", totalParts).
		Strs("targets", targetList).
		Msg("rebroadcasting ordinary-node message")

	sentParts, batches, failedBatches := rebroadcastFECToPeers(ctx, s.log, s.node.privKey, rebroadcastTargetPeers(targets), req, plan)

	s.log.Debug().
		Str("kind", req.kind).
		Str("delivery", string(DeliveryFEC)).
		Int("size", len(req.payload)).
		Uint32("parts", totalParts).
		Uint32("parts_sent", sentParts).
		Int("batches", batches).
		Int("failed_batches", failedBatches).
		Strs("targets", targetList).
		Msg("rebroadcasted ordinary-node message")
}

func rebroadcastSimpleToPeers(ctx context.Context, log zerolog.Logger, buildSimple func([]byte, int32) (overlay.Broadcast, error), peers []overlay.BroadcastPeer, req rebroadcastRequest, plan rebroadcastPlan) (int, int) {
	if len(peers) == 0 {
		return 0, 0
	}

	msg, err := buildSimple(req.payload, plan.flags)
	if err != nil {
		log.Debug().Err(err).Str("kind", req.kind).Msg("failed to build rebroadcast envelope")
		return 0, len(peers)
	}

	sentTo := 0
	failedTo := 0
	for _, peer := range peers {
		sendCtx, cancel := context.WithTimeout(ctx, peerQueryTimeout)
		err := peer.SendCustomMessage(sendCtx, msg)
		cancel()
		if err != nil {
			failedTo++
			log.Debug().
				Err(err).
				Str("kind", req.kind).
				Str("peer", peerIDHex(peer.ID())).
				Str("delivery", string(DeliverySimple)).
				Msg("failed to rebroadcast ordinary-node message")
			continue
		}
		sentTo++
	}
	return sentTo, failedTo
}

func rebroadcastFECToPeers(ctx context.Context, log zerolog.Logger, key ed25519.PrivateKey, peers []overlay.BroadcastPeer, req rebroadcastRequest, plan rebroadcastPlan) (uint32, int, int) {
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

		sendCtx, cancel := context.WithTimeout(ctx, peerQueryTimeout)
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

func peerIDHex(id []byte) string {
	if len(id) == 0 {
		return ""
	}
	return hex.EncodeToString(id)
}

func rebroadcastTargetPeers(targets []rebroadcastTarget) []overlay.BroadcastPeer {
	peers := make([]overlay.BroadcastPeer, 0, len(targets))
	for _, target := range targets {
		peers = append(peers, target.peer)
	}
	return peers
}

func formatRebroadcastTargets(targets []rebroadcastTarget) []string {
	res := make([]string, 0, len(targets))
	for _, target := range targets {
		id := target.id
		if len(id) > 12 {
			id = id[:12]
		}
		if target.addr != "" {
			res = append(res, target.addr+"("+id+")")
			continue
		}
		res = append(res, id)
	}
	return res
}
