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
	fingerprint     string
	event           *BroadcastEvent
	masterchainWake *ton.BlockIDExt
	rebroadcast     *rebroadcastRequest
}

type rebroadcastRequest struct {
	subscription *overlaySubscription
	kind         string
	payload      []byte
}

func (s *overlaySubscription) handleOverlayBroadcast(peer *overlayPeer, msg any, delivery Delivery, trusted bool, sourceKey string) error {
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

	fingerprint := broadcastFingerprint(s.spec.ShortID, payload)

	switch data := msg.(type) {
	case tonnodeapi.BlockBroadcast:
		if !validBlockBroadcast(data.ID, data.Proof, data.Data) {
			return nil
		}
		return s.acceptedFullBlockBroadcast(fingerprint, delivery, trusted, "tonNode.blockBroadcast", data.ID, sourceKey, msg)
	case BlockBroadcastCompressed:
		if !validCompressedBroadcast(data.ID, data.Compressed) {
			return nil
		}
		return s.acceptedFullBlockBroadcast(fingerprint, delivery, trusted, "tonNode.blockBroadcastCompressed", data.ID, sourceKey, msg)
	case BlockBroadcastCompressedV2:
		if !validCompressedBroadcast(data.ID, data.DataCompressed) || len(data.Proof) == 0 {
			return nil
		}
		return s.acceptedFullBlockBroadcast(fingerprint, delivery, trusted, "tonNode.blockBroadcastCompressedV2", data.ID, sourceKey, msg)
	case tonnodeapi.NewShardBlockBroadcast:
		if !validCompressedBroadcast(data.Block.ID, data.Block.Data) {
			return nil
		}
		return s.acceptedBlockBroadcast(fingerprint, delivery, trusted, "tonNode.newShardBlockBroadcast", data.Block.ID, sourceKey)
	case tonnodeapi.NewExternalMessageBroadcast:
		if len(data.Message.Data) == 0 {
			return nil
		}
		return &acceptedBroadcast{
			fingerprint: fingerprint,
			rebroadcast: &rebroadcastRequest{
				subscription: s,
				kind:         "tonNode.externalMessageBroadcast",
				payload:      payload,
			},
		}
	case IhrMessageBroadcast:
		if len(data.Message.Data) == 0 {
			return nil
		}
		return &acceptedBroadcast{
			fingerprint: fingerprint,
			rebroadcast: &rebroadcastRequest{
				subscription: s,
				kind:         "tonNode.ihrMessageBroadcast",
				payload:      payload,
			},
		}
	default:
		return nil
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

func (s *overlaySubscription) acceptedFullBlockBroadcast(fingerprint string, delivery Delivery, trusted bool, kind string, block ton.BlockIDExt, sourceKey string, msg any) *acceptedBroadcast {
	downloaded, err := s.node.decodeBroadcastBlock(s.node.runCtx, msg)
	if err != nil {
		s.log.Debug().
			Err(err).
			Str("block", formatBlockRef(block)).
			Str("kind", kind).
			Msg("dropping block broadcast because payload decode failed")
		if block.Workchain == -1 && block.Shard == topShard {
			wake := block
			return &acceptedBroadcast{
				fingerprint:     fingerprint,
				masterchainWake: &wake,
			}
		}
		return nil
	}

	accepted := s.acceptedBlockBroadcast(fingerprint, delivery, trusted, kind, block, sourceKey)
	accepted.event.Downloaded = downloaded
	return accepted
}

func (n *Node) acceptBroadcast(accepted acceptedBroadcast) {
	if accepted.fingerprint == "" {
		return
	}
	if !n.deduper.Mark(accepted.fingerprint, time.Now()) {
		return
	}

	if accepted.masterchainWake != nil {
		n.trackRawMasterchainBroadcast(*accepted.masterchainWake)
	}

	if accepted.event != nil {
		n.trackUnverifiedBroadcastBlock(*accepted.event)
		_ = n.eventQueue.Push(*accepted.event)
	}

	if accepted.rebroadcast != nil && n.allowRebroadcast(accepted.rebroadcast) {
		_ = n.rebroadcastQueue.Push(*accepted.rebroadcast)
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
