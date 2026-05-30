package p2p

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc64"
	"time"

	"github.com/xssnick/gton/service/storage"

	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

var errBroadcastRejected = errors.New("broadcast rejected")

var externalMessageCRC64Table = crc64.MakeTable(crc64.ECMA)

type customBroadcastRole uint8

const (
	customBroadcastRoleMessage customBroadcastRole = iota
	customBroadcastRoleBlock
)

type acceptedBroadcast struct {
	fingerprint        string
	deduped            bool
	block              *ton.BlockIDExt
	event              *BroadcastEvent
	masterchainWake    *ton.BlockIDExt
	rebroadcast        *rebroadcastRequest
	skipAcceptedMetric bool
}

type rebroadcastRequest struct {
	subscription           *overlaySubscription
	kind                   string
	payload                []byte
	payloadSize            int
	fec                    *overlay.BroadcastFECSender
	sourcePeerID           PeerID
	local                  bool
	skipOverlayRebroadcast bool
	queuedAt               time.Time
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
	return int64(req.payloadLen()) + 256
}

func (req rebroadcastRequest) payloadLen() int {
	if len(req.payload) > 0 {
		return len(req.payload)
	}
	if req.payloadSize > 0 {
		return req.payloadSize
	}
	return 0
}

func (s *overlaySubscription) handleOverlayBroadcast(peer *overlayPeer, msg any, delivery Delivery, trusted bool, sourcePeerID PeerID) error {
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

	accepted := s.classifyBroadcast(peer, msg, payload, delivery, trusted, sourcePeerID)
	if accepted == nil {
		if delivery == DeliveryFEC && !trusted && s.broadcastFECRelayEnabled() {
			return errBroadcastRejected
		}
		return nil
	}

	s.node.acceptBroadcast(*accepted)
	return nil
}

func (s *overlaySubscription) classifyBroadcast(peer *overlayPeer, msg any, payload []byte, delivery Delivery, trusted bool, sourcePeerID PeerID) *acceptedBroadcast {
	if len(payload) == 0 {
		return nil
	}
	if sourcePeerID.IsZero() && peer != nil {
		sourcePeerID = peer.id
	}

	fingerprint := broadcastFingerprint(s.spec.ShortID, payload)

	switch data := msg.(type) {
	case tonnodeapi.BlockBroadcast:
		kind := "tonNode.blockBroadcast"
		if !s.allowCustomBroadcastSource(kind, sourcePeerID, customBroadcastRoleBlock) {
			return nil
		}
		if !validBlockBroadcast(data.ID, data.Proof, data.Data) {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "invalid_payload")
			return nil
		}
		return s.acceptedFullBlockBroadcast(fingerprint, delivery, trusted, kind, data.ID, sourcePeerID, msg, s.inboundRebroadcast(kind, payload, peer, delivery))
	case tonnodeapi.BlockBroadcastCompressed:
		kind := "tonNode.blockBroadcastCompressed"
		if !s.allowCustomBroadcastSource(kind, sourcePeerID, customBroadcastRoleBlock) {
			return nil
		}
		if !validCompressedBroadcast(data.ID, data.Compressed) {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "invalid_payload")
			return nil
		}
		return s.acceptedFullBlockBroadcast(fingerprint, delivery, trusted, kind, data.ID, sourcePeerID, msg, s.inboundRebroadcast(kind, payload, peer, delivery))
	case tonnodeapi.BlockBroadcastCompressedV2:
		kind := "tonNode.blockBroadcastCompressedV2"
		if !s.allowCustomBroadcastSource(kind, sourcePeerID, customBroadcastRoleBlock) {
			return nil
		}
		if !validCompressedBroadcast(data.ID, data.DataCompressed) || len(data.Proof) == 0 {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "invalid_payload")
			return nil
		}
		return s.acceptedFullBlockBroadcast(fingerprint, delivery, trusted, kind, data.ID, sourcePeerID, msg, s.inboundRebroadcast(kind, payload, peer, delivery))
	case tonnodeapi.NewShardBlockBroadcast:
		kind := "tonNode.newShardBlockBroadcast"
		if !s.allowCustomBroadcastSource(kind, sourcePeerID, customBroadcastRoleBlock) {
			return nil
		}
		if !validCompressedBroadcast(data.Block.ID, data.Block.Data) {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "invalid_payload")
			return nil
		}
		if err := s.node.checkShardDescriptionSignatures(data.Block.ID, data.Block.CCSeqno, data.Block.Data); err != nil {
			s.log.Debug().
				Err(err).
				Str("block", formatBlockRef(data.Block.ID)).
				Str("kind", kind).
				Msg("dropping shard block broadcast because validator signatures are not verified")
			s.node.noteBroadcastDrop(s.spec.Name, kind, "signature_check_failed")
			return nil
		}
		accepted := s.acceptedShardBlockBroadcast(fingerprint, delivery, trusted, data.Block.ID, sourcePeerID, data.Block.CCSeqno, data.Block.Data)
		accepted.rebroadcast = s.inboundRebroadcast(kind, payload, peer, delivery)
		return accepted
	case tonnodeapi.NewExternalMessageBroadcast:
		kind := "tonNode.externalMessageBroadcast"
		if !s.allowCustomBroadcastSource(kind, sourcePeerID, customBroadcastRoleMessage) {
			return nil
		}
		if len(data.Message.Data) == 0 {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "invalid_payload")
			return nil
		}
		if len(data.Message.Data) > maxOverlayPayloadSize {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "oversize_payload")
			return nil
		}

		hash := externalMessageFingerprint(s.spec.ShortID, data.Message.Data)
		now := time.Now()
		if !s.node.processedExternalMessages.Mark(hash, now) {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "seen")
			return nil
		}
		if s.node.myExternalMessages.Seen(hash, now) {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "seen")
			return nil
		}
		addrKey, err := externalMessageDestinationAddress(data.Message.Data)
		if err != nil {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "invalid_payload")
			return nil
		}
		if err = s.node.addExternalMessageAddressLimit(addrKey, now); err != nil {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "address_rate_limited")
			return nil
		}

		return &acceptedBroadcast{
			fingerprint: fingerprint,
			rebroadcast: s.inboundRebroadcast(kind, payload, peer, delivery),
		}
	case tonnodeapi.NewBlockCandidateBroadcast:
		kind := "tonNode.newBlockCandidateBroadcast"
		if !s.allowCustomBroadcastSource(kind, sourcePeerID, customBroadcastRoleBlock) {
			return nil
		}
		if !validBlockCandidateBroadcast(data.ID, data.Data) {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "invalid_payload")
			return nil
		}
		return s.acceptedBlockCandidateBroadcast(fingerprint, delivery, kind, data.ID, msg, payload, peer, sourcePeerID)
	case tonnodeapi.NewBlockCandidateBroadcastCompressed:
		kind := "tonNode.newBlockCandidateBroadcastCompressed"
		if !s.allowCustomBroadcastSource(kind, sourcePeerID, customBroadcastRoleBlock) {
			return nil
		}
		if !validCompressedBroadcast(data.ID, data.Compressed) {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "invalid_payload")
			return nil
		}
		return s.acceptedBlockCandidateBroadcast(fingerprint, delivery, kind, data.ID, msg, payload, peer, sourcePeerID)
	case tonnodeapi.NewBlockCandidateBroadcastCompressedV2:
		kind := "tonNode.newBlockCandidateBroadcastCompressedV2"
		if !s.allowCustomBroadcastSource(kind, sourcePeerID, customBroadcastRoleBlock) {
			return nil
		}
		if !validCompressedBroadcast(data.ID, data.Compressed) {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "invalid_payload")
			return nil
		}
		return s.acceptedBlockCandidateBroadcast(fingerprint, delivery, kind, data.ID, msg, payload, peer, sourcePeerID)
	case IhrMessageBroadcast:
		kind := "tonNode.ihrMessageBroadcast"
		if len(data.Message.Data) == 0 {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "invalid_payload")
			return nil
		}
		return &acceptedBroadcast{
			fingerprint: fingerprint,
			rebroadcast: s.inboundRebroadcast(kind, payload, peer, delivery),
		}
	default:
		return nil
	}
}

func (s *overlaySubscription) allowCustomBroadcastSource(kind string, sourcePeerID PeerID, role customBroadcastRole) bool {
	if s.spec.Kind != overlayKindCustomFixed {
		return true
	}
	if sourcePeerID.IsZero() {
		s.node.noteBroadcastDrop(s.spec.Name, kind, "missing_source")
		return false
	}

	switch role {
	case customBroadcastRoleMessage:
		if _, ok := s.spec.MsgSenders[sourcePeerID]; ok {
			return true
		}
	case customBroadcastRoleBlock:
		if _, ok := s.spec.BlockSenders[sourcePeerID]; ok {
			return true
		}
	}
	s.node.noteBroadcastDrop(s.spec.Name, kind, "unauthorized_sender")
	return false
}

func (s *overlaySubscription) inboundRebroadcast(kind string, payload []byte, peer *overlayPeer, delivery Delivery) *rebroadcastRequest {
	var sourcePeerID PeerID
	if peer != nil {
		sourcePeerID = peer.id
	}

	return &rebroadcastRequest{
		subscription:           s,
		kind:                   kind,
		payload:                payload,
		sourcePeerID:           sourcePeerID,
		skipOverlayRebroadcast: delivery == DeliveryTwoStep || (delivery == DeliveryFEC && s.broadcastFECRelayEnabled()),
	}
}

func (s *overlaySubscription) acceptedBlockBroadcast(fingerprint string, delivery Delivery, trusted bool, kind string, block ton.BlockIDExt, sourcePeerID PeerID) *acceptedBroadcast {
	return &acceptedBroadcast{
		fingerprint: fingerprint,
		block:       block.Copy(),
		event: &BroadcastEvent{
			Overlay:      s.spec.Name,
			Kind:         kind,
			Delivery:     delivery,
			Trusted:      trusted,
			Block:        block,
			SourcePeerID: sourcePeerID,
			ReceivedAt:   time.Now(),
		},
	}
}

func (s *overlaySubscription) acceptedShardBlockBroadcast(fingerprint string, delivery Delivery, trusted bool, block ton.BlockIDExt, sourcePeerID PeerID, catchainSeqno int32, data []byte) *acceptedBroadcast {
	accepted := s.acceptedBlockBroadcast(fingerprint, delivery, trusted, "tonNode.newShardBlockBroadcast", block, sourcePeerID)
	accepted.event.ShardDescription = &ShardBlockDescription{
		CatchainSeqno: catchainSeqno,
		Data:          append([]byte(nil), data...),
	}
	return accepted
}

func (s *overlaySubscription) acceptedFullBlockBroadcast(fingerprint string, delivery Delivery, trusted bool, kind string, block ton.BlockIDExt, sourcePeerID PeerID, msg any, rebroadcast *rebroadcastRequest) *acceptedBroadcast {
	if !s.node.deduper.Mark(fingerprint, time.Now()) {
		s.node.noteBroadcastDrop(s.spec.Name, kind, "seen")
		return nil
	}

	downloaded, err := s.node.decodeBroadcastBlock(s.node.runCtx, msg)
	if err != nil {
		stateNotReady := isBroadcastDecompressionStateNotReady(err)
		if stateNotReady {
			pendingState, ok := broadcastDecompressionStateNotReady(err)
			if !ok {
				s.log.Debug().
					Err(err).
					Str("block", formatBlockRef(block)).
					Str("kind", kind).
					Msg("dropping pending block broadcast because state-ready artifact is missing")
				s.node.noteBroadcastDrop(s.spec.Name, kind, "state_artifact_missing")
				return nil
			}

			v2, ok := msg.(tonnodeapi.BlockBroadcastCompressedV2)
			if !ok {
				s.log.Debug().
					Err(err).
					Str("block", formatBlockRef(block)).
					Str("kind", kind).
					Msg("dropping pending block broadcast because payload type is not compressed-v2")
				s.node.noteBroadcastDrop(s.spec.Name, kind, "invalid_payload")
				return nil
			}
			sigSet, sigErr := broadcastSignatureSetFromTL(v2.SignatureSet)
			if sigErr != nil {
				s.log.Debug().
					Err(sigErr).
					Str("block", formatBlockRef(block)).
					Str("kind", kind).
					Msg("dropping pending block broadcast because validator signatures cannot be parsed")
				s.node.noteBroadcastDrop(s.spec.Name, kind, "signature_parse_failed")
				return nil
			}
			if sigErr = s.node.checkBlockBroadcastSignatures(kind, block, pendingState.proofRoot, sigSet); sigErr != nil {
				s.log.Debug().
					Err(sigErr).
					Str("block", formatBlockRef(block)).
					Str("kind", kind).
					Msg("dropping pending block broadcast because validator signatures are not verified")
				s.node.noteBroadcastDrop(s.spec.Name, kind, "signature_check_failed")
				return nil
			}

			s.node.schedulePendingBlockBroadcastDecode(pendingBlockBroadcastDecode{
				fingerprint:  fingerprint,
				overlay:      s.spec.Name,
				delivery:     delivery,
				trusted:      trusted,
				kind:         kind,
				block:        block,
				sourcePeerID: sourcePeerID,
				receivedAt:   time.Now(),
				msg:          msg,
				prev:         pendingState.prev,
				proofRoot:    pendingState.proofRoot,
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
			s.node.noteBroadcastDrop(s.spec.Name, kind, "decode_failed")
		}

		if !stateNotReady {
			return nil
		}
		accepted := &acceptedBroadcast{
			fingerprint: fingerprint,
			deduped:     true,
			rebroadcast: rebroadcast,
		}
		if block.Workchain == -1 && block.Shard == topShard {
			wake := block
			accepted.masterchainWake = &wake
		}
		return accepted
	}

	sigSet, err := broadcastSignatureSetFromDecoded(msg, downloaded)
	if err != nil {
		s.log.Debug().
			Err(err).
			Str("block", formatBlockRef(block)).
			Str("kind", kind).
			Msg("dropping block broadcast because validator signatures cannot be parsed")
		s.node.noteBroadcastDrop(s.spec.Name, kind, "signature_parse_failed")
		return nil
	}
	if err = s.node.checkBlockBroadcastSignatures(kind, block, downloaded.Proof, sigSet); err != nil {
		s.log.Debug().
			Err(err).
			Str("block", formatBlockRef(block)).
			Str("kind", kind).
			Msg("dropping block broadcast because validator signatures are not verified")
		s.node.noteBroadcastDrop(s.spec.Name, kind, "signature_check_failed")
		return nil
	}

	accepted := s.acceptedBlockBroadcast(fingerprint, delivery, trusted, kind, block, sourcePeerID)
	accepted.deduped = true
	downloaded.SourcePeerID = sourcePeerID
	accepted.event.Downloaded = downloaded
	accepted.rebroadcast = rebroadcast
	return accepted
}

func (s *overlaySubscription) acceptedBlockCandidateBroadcast(fingerprint string, delivery Delivery, kind string, block ton.BlockIDExt, msg any, payload []byte, peer *overlayPeer, sourcePeerID PeerID) *acceptedBroadcast {
	if !s.node.deduper.Mark(fingerprint, time.Now()) {
		s.node.noteBroadcastDrop(s.spec.Name, kind, "seen")
		return nil
	}

	if s.node.nonfinalBlockCacheEnabled() {
		downloaded, err := decodeBlockCandidateBroadcast(msg)
		if err != nil {
			s.log.Debug().
				Err(err).
				Str("block", formatBlockRef(block)).
				Str("kind", kind).
				Msg("skip non-final live block cache update because block candidate payload decode failed")
		} else {
			downloaded.SourcePeerID = sourcePeerID
			s.node.publishNonfinalDownloadedBlock(downloaded, storage.LiveBlockNonfinalCandidate)
		}
	}

	return &acceptedBroadcast{
		fingerprint: fingerprint,
		deduped:     true,
		block:       block.Copy(),
		rebroadcast: s.inboundRebroadcast(kind, payload, peer, delivery),
	}
}

func (n *Node) acceptBroadcast(accepted acceptedBroadcast) {
	if accepted.fingerprint == "" {
		return
	}
	if !accepted.deduped && !n.deduper.Mark(accepted.fingerprint, time.Now()) {
		n.noteSeenAcceptedBroadcastDrop(accepted)
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
			n.publishNonfinalDownloadedBlock(accepted.event.Downloaded, storage.LiveBlockNonfinalSigned)
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
		if !accepted.rebroadcast.skipOverlayRebroadcast && n.allowRebroadcast(accepted.rebroadcast) {
			accepted.rebroadcast.subscription.enqueueRebroadcast(*accepted.rebroadcast)
		}
	}

	n.enqueueCustomOverlayFanout(accepted)
}

func (n *Node) enqueueCustomOverlayFanout(accepted acceptedBroadcast) {
	if n.customFanoutDeduper == nil || accepted.rebroadcast == nil || accepted.block == nil || len(accepted.rebroadcast.payload) == 0 {
		return
	}

	class, ok := customFanoutClass(accepted.rebroadcast.kind)
	if !ok {
		return
	}

	targets := n.customOverlayFanoutTargets(accepted)
	if len(targets) == 0 {
		return
	}

	key := customFanoutKey(class, *accepted.block)
	if !n.customFanoutDeduper.Mark(key, time.Now()) {
		return
	}

	for _, sub := range targets {
		req := rebroadcastRequest{
			subscription: sub,
			kind:         accepted.rebroadcast.kind,
			payload:      append([]byte(nil), accepted.rebroadcast.payload...),
			local:        true,
		}
		if req.payloadLen() == 0 {
			continue
		}
		sub.enqueueRebroadcast(req)
	}
}

func (n *Node) customOverlayFanoutTargets(accepted acceptedBroadcast) []*overlaySubscription {
	targets := make([]*overlaySubscription, 0)
	for _, sub := range n.subscriptionsSnapshot() {
		if sub == nil || sub == accepted.rebroadcast.subscription {
			continue
		}
		if sub.spec.Kind != overlayKindCustomFixed || !sub.isActive() {
			continue
		}
		if _, ok := sub.spec.BlockSenders[n.localID]; !ok {
			continue
		}
		if !sub.spec.sendsShard(accepted.block.Workchain, accepted.block.Shard) {
			continue
		}
		targets = append(targets, sub)
	}
	return targets
}

func customFanoutClass(kind string) (string, bool) {
	switch kind {
	case "tonNode.blockBroadcast", "tonNode.blockBroadcastCompressed", "tonNode.blockBroadcastCompressedV2",
		"tonNode.newBlockCandidateBroadcast", "tonNode.newBlockCandidateBroadcastCompressed",
		"tonNode.newBlockCandidateBroadcastCompressedV2":
		return "block", true
	case "tonNode.newShardBlockBroadcast":
		return "shard-desc", true
	default:
		return "", false
	}
}

func customFanoutKey(class string, block ton.BlockIDExt) string {
	return class + ":" + formatBlockRef(block)
}

func (n *Node) noteSeenAcceptedBroadcastDrop(accepted acceptedBroadcast) {
	if accepted.event != nil {
		n.noteBroadcastDrop(accepted.event.Overlay, accepted.event.Kind, "seen")
		return
	}
	if accepted.rebroadcast != nil {
		n.noteBroadcastDrop(accepted.rebroadcast.overlayName(), accepted.rebroadcast.kind, "seen")
	}
}

func broadcastFingerprint(overlayID []byte, payload []byte) string {
	hasher := sha256.New()
	hasher.Write(overlayID)
	hasher.Write(payload)
	return hex.EncodeToString(hasher.Sum(nil))
}

func externalMessageFingerprint(overlayID []byte, data []byte) string {
	crc := crc64.Update(0, externalMessageCRC64Table, overlayID)
	crc = crc64.Update(crc, externalMessageCRC64Table, data)

	var key [8]byte
	binary.BigEndian.PutUint64(key[:], crc)
	return string(key[:])
}

func validBlockBroadcast(block ton.BlockIDExt, proof []byte, data []byte) bool {
	return validBlockID(block) && len(proof) > 0 && len(data) > 0
}

func validCompressedBroadcast(block ton.BlockIDExt, data []byte) bool {
	return validBlockID(block) && len(data) > 0
}

func validBlockCandidateBroadcast(block ton.BlockIDExt, data []byte) bool {
	if !validBlockID(block) || len(data) == 0 || len(data) > maxOverlayPayloadSize {
		return false
	}
	return bytes.Equal(hashSimpleBroadcastPayload(data), block.FileHash)
}

func validBlockID(block ton.BlockIDExt) bool {
	if len(block.RootHash) != 32 || len(block.FileHash) != 32 {
		return false
	}
	return true
}
