package p2p

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

type acceptedBroadcast struct {
	fingerprint        string
	deduped            bool
	event              *BroadcastEvent
	masterchainWake    *ton.BlockIDExt
	rebroadcast        *rebroadcastRequest
	skipAcceptedMetric bool
}

type rebroadcastRequest struct {
	subscription *overlaySubscription
	kind         string
	payload      []byte
	sourcePeerID string
	local        bool
}

func (req rebroadcastRequest) overlayName() string {
	if req.subscription == nil {
		return ""
	}
	return req.subscription.spec.Name
}

func broadcastEventBytes(event BroadcastEvent) int64 {
	bytes := int64(256)
	if event.Downloaded != nil {
		bytes += int64(len(event.Downloaded.BlockBOC) + len(event.Downloaded.ProofBOC))
	}
	if event.ShardDescription != nil {
		bytes += int64(len(event.ShardDescription.Data))
	}
	return bytes
}

func rebroadcastRequestBytes(req rebroadcastRequest) int64 {
	return int64(len(req.payload)) + 256
}

func (s *overlaySubscription) handleOverlayBroadcast(peer *overlayPeer, msg any, delivery Delivery, trusted bool, sourceKey string) error {
	if !s.isActive() {
		return nil
	}

	if peer != nil {
		peer.noteReceive()
	}
	payload, err := tl.Serialize(msg, true)
	if err != nil {
		s.log.Debug().Err(err).Msg("failed to serialize inbound broadcast payload")
		return nil
	}

	accepted := s.classifyBroadcast(peer, msg, payload, delivery, trusted, sourceKey)
	if accepted == nil {
		return nil
	}

	s.node.acceptBroadcast(*accepted)
	return nil
}

func (s *overlaySubscription) classifyBroadcast(peer *overlayPeer, msg any, payload []byte, delivery Delivery, trusted bool, sourceKey string) *acceptedBroadcast {
	if len(payload) == 0 {
		return nil
	}
	if sourceKey == "" && peer != nil {
		sourceKey = downloadPeerKey(peer)
	}

	fingerprint := broadcastFingerprint(s.spec.ShortID, payload)

	switch data := msg.(type) {
	case tonnodeapi.BlockBroadcast:
		if !validBlockBroadcast(data.ID, data.Proof, data.Data) {
			return nil
		}
		kind := "tonNode.blockBroadcast"
		return s.acceptedFullBlockBroadcast(fingerprint, delivery, trusted, kind, data.ID, sourceKey, msg, s.inboundRebroadcast(kind, payload, peer))
	case tonnodeapi.BlockBroadcastCompressed:
		if !validCompressedBroadcast(data.ID, data.Compressed) {
			return nil
		}
		kind := "tonNode.blockBroadcastCompressed"
		return s.acceptedFullBlockBroadcast(fingerprint, delivery, trusted, kind, data.ID, sourceKey, msg, s.inboundRebroadcast(kind, payload, peer))
	case tonnodeapi.BlockBroadcastCompressedV2:
		if !validCompressedBroadcast(data.ID, data.DataCompressed) || len(data.Proof) == 0 {
			return nil
		}
		kind := "tonNode.blockBroadcastCompressedV2"
		return s.acceptedFullBlockBroadcast(fingerprint, delivery, trusted, kind, data.ID, sourceKey, msg, s.inboundRebroadcast(kind, payload, peer))
	case tonnodeapi.NewShardBlockBroadcast:
		if !validCompressedBroadcast(data.Block.ID, data.Block.Data) {
			return nil
		}
		kind := "tonNode.newShardBlockBroadcast"
		accepted := s.acceptedShardBlockBroadcast(fingerprint, delivery, trusted, data.Block.ID, sourceKey, data.Block.CCSeqno, data.Block.Data)
		accepted.rebroadcast = s.inboundRebroadcast(kind, payload, peer)
		return accepted
	case tonnodeapi.NewExternalMessageBroadcast:
		if len(data.Message.Data) == 0 {
			return nil
		}
		kind := "tonNode.externalMessageBroadcast"
		return &acceptedBroadcast{
			fingerprint: fingerprint,
			rebroadcast: s.inboundRebroadcast(kind, payload, peer),
		}
	case IhrMessageBroadcast:
		if len(data.Message.Data) == 0 {
			return nil
		}
		kind := "tonNode.ihrMessageBroadcast"
		return &acceptedBroadcast{
			fingerprint: fingerprint,
			rebroadcast: s.inboundRebroadcast(kind, payload, peer),
		}
	default:
		return nil
	}
}

func (s *overlaySubscription) inboundRebroadcast(kind string, payload []byte, peer *overlayPeer) *rebroadcastRequest {
	sourcePeerID := ""
	if peer != nil {
		sourcePeerID = peer.id
	}

	return &rebroadcastRequest{
		subscription: s,
		kind:         kind,
		payload:      payload,
		sourcePeerID: sourcePeerID,
	}
}

func (s *overlaySubscription) acceptedBlockBroadcast(fingerprint string, delivery Delivery, trusted bool, kind string, block ton.BlockIDExt, sourceKey string) *acceptedBroadcast {
	return &acceptedBroadcast{
		fingerprint: fingerprint,
		event: &BroadcastEvent{
			Overlay:    s.spec.Name,
			Kind:       kind,
			Delivery:   delivery,
			Trusted:    trusted,
			Block:      block,
			SourceKey:  sourceKey,
			ReceivedAt: time.Now(),
		},
	}
}

func (s *overlaySubscription) acceptedShardBlockBroadcast(fingerprint string, delivery Delivery, trusted bool, block ton.BlockIDExt, sourceKey string, catchainSeqno int32, data []byte) *acceptedBroadcast {
	accepted := s.acceptedBlockBroadcast(fingerprint, delivery, trusted, "tonNode.newShardBlockBroadcast", block, sourceKey)
	accepted.event.ShardDescription = &ShardBlockDescription{
		CatchainSeqno: catchainSeqno,
		Data:          append([]byte(nil), data...),
	}
	return accepted
}

func (s *overlaySubscription) acceptedFullBlockBroadcast(fingerprint string, delivery Delivery, trusted bool, kind string, block ton.BlockIDExt, sourceKey string, msg any, rebroadcast *rebroadcastRequest) *acceptedBroadcast {
	if !s.node.deduper.Mark(fingerprint, time.Now()) {
		return nil
	}
	if block.Workchain == -1 && block.Shard == topShard {
		s.node.trackRawMasterchainBroadcast(block)
	}

	downloaded, err := s.node.decodeBroadcastBlock(s.node.runCtx, msg)
	if err != nil {
		stateNotReady := isBroadcastDecompressionStateNotReady(err)
		if stateNotReady {
			s.node.schedulePendingBlockBroadcastDecode(pendingBlockBroadcastDecode{
				fingerprint: fingerprint,
				overlay:     s.spec.Name,
				delivery:    delivery,
				trusted:     trusted,
				kind:        kind,
				block:       block,
				sourceKey:   sourceKey,
				receivedAt:  time.Now(),
				msg:         msg,
			})
			s.log.Debug().
				Err(err).
				Str("block", formatBlockRef(block)).
				Str("kind", kind).
				Msg("queued block broadcast until previous state is available")
		} else {
			s.log.Debug().
				Err(err).
				Str("block", formatBlockRef(block)).
				Str("kind", kind).
				Msg("dropping block broadcast because payload decode failed")
		}

		if stateNotReady || (block.Workchain == -1 && block.Shard == topShard) {
			accepted := &acceptedBroadcast{
				fingerprint: fingerprint,
				deduped:     true,
			}
			if stateNotReady {
				accepted.rebroadcast = rebroadcast
			}
			if block.Workchain == -1 && block.Shard == topShard {
				wake := block
				accepted.masterchainWake = &wake
			}
			return accepted
		}
		return nil
	}

	accepted := s.acceptedBlockBroadcast(fingerprint, delivery, trusted, kind, block, sourceKey)
	accepted.deduped = true
	accepted.event.Downloaded = downloaded
	accepted.rebroadcast = rebroadcast
	return accepted
}

func (n *Node) acceptBroadcast(accepted acceptedBroadcast) {
	if accepted.fingerprint == "" {
		return
	}
	if !accepted.deduped && !n.deduper.Mark(accepted.fingerprint, time.Now()) {
		return
	}

	if accepted.masterchainWake != nil {
		n.trackRawMasterchainBroadcast(*accepted.masterchainWake)
	}

	acceptedNoted := false
	if accepted.event != nil {
		n.trackUnverifiedBroadcastBlock(*accepted.event)
		if accepted.event.Downloaded != nil && accepted.event.Downloaded.ID.Equals(&accepted.event.Block) {
			n.rememberDownloadedBlockAsync(nil, accepted.event.Downloaded)
			n.rememberShardBroadcastBlock(accepted.event.Downloaded)
		}
		if n.eventQueue.Push(*accepted.event) {
			acceptedNoted = true
			if !accepted.skipAcceptedMetric {
				n.noteBroadcast("accepted", accepted.event.Overlay, accepted.event.Kind)
			}
		}
	}

	if accepted.rebroadcast != nil {
		if !acceptedNoted && !accepted.skipAcceptedMetric {
			n.noteBroadcast("accepted", accepted.rebroadcast.overlayName(), accepted.rebroadcast.kind)
		}
		if n.allowRebroadcast(accepted.rebroadcast) {
			accepted.rebroadcast.subscription.enqueueRebroadcast(*accepted.rebroadcast)
		}
	}
}

func broadcastFingerprint(overlayID []byte, payload []byte) string {
	hasher := sha256.New()
	hasher.Write(overlayID)
	hasher.Write(payload)
	return hex.EncodeToString(hasher.Sum(nil))
}

func validBlockBroadcast(block ton.BlockIDExt, proof []byte, data []byte) bool {
	return validBlockID(block) && len(proof) > 0 && len(data) > 0
}

func validCompressedBroadcast(block ton.BlockIDExt, data []byte) bool {
	return validBlockID(block) && len(data) > 0
}

func validBlockID(block ton.BlockIDExt) bool {
	if len(block.RootHash) != 32 || len(block.FileHash) != 32 {
		return false
	}
	return true
}
