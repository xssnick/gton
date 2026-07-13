package p2p

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/crc64"
	"sync"
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
	extraEvents        []BroadcastEvent
	masterchainWake    *ton.BlockIDExt
	rebroadcast        *rebroadcastRequest
	skipAcceptedMetric bool
}

type rebroadcastRequest struct {
	subscription           *overlaySubscription
	kind                   string
	payload                []byte
	payloadSize            int
	payloadSource          *broadcastPayload
	simple                 *overlay.Broadcast
	fec                    *overlay.BroadcastFECSender
	sourcePeerID           PeerID
	local                  bool
	skipOverlayRebroadcast bool
	queuedAt               time.Time
}

type broadcastPayload struct {
	mu sync.Mutex

	msg        any
	payload    []byte
	identity   []byte
	serialized bool
	err        error
}

func newKnownBroadcastPayload(payload []byte) *broadcastPayload {
	return &broadcastPayload{
		payload:    payload,
		identity:   payload,
		serialized: true,
	}
}

func newIdentifiedBroadcastPayload(msg any, identity []byte) *broadcastPayload {
	return &broadcastPayload{
		msg:      msg,
		identity: identity,
	}
}

func (p *broadcastPayload) bytes() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.serialized {
		return p.payload, p.err
	}

	p.payload, p.err = tl.Serialize(p.msg, true)
	p.msg = nil
	p.serialized = true
	return p.payload, p.err
}

func (p *broadcastPayload) fingerprint(overlayID []byte) (string, error) {
	identity := p.identity
	if len(identity) == 0 {
		payload, err := p.bytes()
		if err != nil {
			return "", err
		}
		identity = payload
	}
	if len(identity) == 0 {
		return "", nil
	}
	return broadcastFingerprint(overlayID, identity), nil
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
		bytes += int64(256)
		for _, link := range event.ShardDescription.Chain {
			bytes += int64(256 + len(link.PrevRefs)*128 + len(link.ProofBOC))
		}
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

func (req *rebroadcastRequest) materializePayload() error {
	if len(req.payload) > 0 || req.payloadSource == nil {
		return nil
	}

	payload, err := req.payloadSource.bytes()
	if err != nil {
		return err
	}
	req.payload = payload
	req.payloadSize = len(payload)
	req.payloadSource = nil
	return nil
}

func (s *overlaySubscription) handleOverlayBroadcastPayload(peer *overlayPeer, msg any, payload *broadcastPayload, delivery Delivery, trusted bool, sourcePeerID PeerID) error {
	if !s.isActive() {
		return nil
	}

	kind := broadcastKindLabel(msg)
	if peer != nil && peer.noteReceive() {
		s.peerPromoted(peer)
	}
	if !s.node.canAcceptBroadcast(kind, false) {
		s.node.noteBroadcastDrop(s.spec.Name, kind, "broadcast_admission_closed")
		if delivery == DeliveryFEC || delivery == DeliveryTwoStep {
			return errBroadcastRejected
		}
		return nil
	}

	classifyStarted := s.node.startBroadcastPipelineStage()
	accepted, err := s.classifyBroadcastPayload(peer, msg, payload, delivery, trusted, sourcePeerID)
	if err != nil {
		s.node.observeBroadcastPipelineStageSince(classifyStarted, broadcastPipelineStageClassify, kind, delivery, broadcastPipelineResultError)
		s.log.Debug().Err(err).Msg("failed to classify inbound broadcast payload")
		return nil
	}
	classifyResult := broadcastPipelineResultDrop
	if accepted != nil {
		classifyResult = broadcastPipelineResultSuccess
	}
	s.node.observeBroadcastPipelineStageSince(classifyStarted, broadcastPipelineStageClassify, kind, delivery, classifyResult)
	if accepted == nil {
		if delivery == DeliveryFEC && !trusted && s.broadcastFECRelayEnabled() {
			return errBroadcastRejected
		}
		return nil
	}

	s.node.acceptBroadcast(*accepted)
	return nil
}

func (s *overlaySubscription) classifyBroadcastPayload(peer *overlayPeer, msg any, payload *broadcastPayload, delivery Delivery, trusted bool, sourcePeerID PeerID) (*acceptedBroadcast, error) {
	if sourcePeerID.IsZero() && peer != nil {
		sourcePeerID = peer.id
	}
	if s.spec.Kind == overlayKindCustomFixed && sourcePeerID == s.node.localID {
		s.node.noteBroadcastDrop(s.spec.Name, broadcastKindLabel(msg), "self")
		return nil, nil
	}

	switch data := msg.(type) {
	case tonnodeapi.BlockBroadcast:
		return s.classifyFullBlockBroadcast(
			"tonNode.blockBroadcast",
			data.ID,
			validBlockBroadcast(data.ID, data.Proof, data.Data),
			peer, msg, payload, delivery, trusted, sourcePeerID,
		)
	case tonnodeapi.BlockBroadcastCompressed:
		return s.classifyFullBlockBroadcast(
			"tonNode.blockBroadcastCompressed",
			data.ID,
			validCompressedBroadcast(data.ID, data.Compressed),
			peer, msg, payload, delivery, trusted, sourcePeerID,
		)
	case tonnodeapi.BlockBroadcastCompressedV2:
		return s.classifyFullBlockBroadcast(
			"tonNode.blockBroadcastCompressedV2",
			data.ID,
			validCompressedBroadcast(data.ID, data.DataCompressed) && len(data.Proof) > 0,
			peer, msg, payload, delivery, trusted, sourcePeerID,
		)
	case tonnodeapi.NewShardBlockBroadcast:
		kind := "tonNode.newShardBlockBroadcast"
		if !s.allowCustomBroadcastSource(kind, sourcePeerID, customBroadcastRoleBlock) {
			return nil, nil
		}
		if !validCompressedBroadcast(data.Block.ID, data.Block.Data) {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "invalid_payload")
			return nil, nil
		}
		fingerprint, err := payload.fingerprint(s.spec.ShortID)
		if err != nil {
			return nil, err
		}
		if !s.node.deduper.Mark(fingerprint, time.Now()) {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "seen")
			return nil, nil
		}

		validateStarted := s.node.startBroadcastPipelineStage()
		desc, err := s.node.validateShardDescriptionBroadcast(data.Block.ID, data.Block.CCSeqno, data.Block.Data)
		if err == nil && desc == nil {
			err = errors.New("validated shard block description is empty")
		}
		validateResult := broadcastPipelineResultSuccess
		if err != nil {
			validateResult = broadcastPipelineResultError
		}
		s.node.observeBroadcastPipelineStageSince(validateStarted, broadcastPipelineStageShardDescValidate, kind, delivery, validateResult)
		if err != nil {
			s.log.Debug().
				Err(err).
				Str("block", formatBlockRef(data.Block.ID)).
				Str("kind", kind).
				Msg("dropping shard block broadcast because validator signatures are not verified")
			s.node.noteBroadcastDrop(s.spec.Name, kind, "signature_check_failed")
			return nil, nil
		}
		rebroadcast, err := s.inboundRebroadcastPayload(kind, payload, peer, delivery)
		if err != nil {
			s.node.deduper.Forget(fingerprint)
			return nil, err
		}
		accepted := s.acceptedShardBlockBroadcast(fingerprint, delivery, trusted, data.Block.ID, sourcePeerID, desc)
		accepted.deduped = true
		accepted.rebroadcast = rebroadcast
		return accepted, nil
	case tonnodeapi.NewExternalMessageBroadcast:
		kind := "tonNode.externalMessageBroadcast"
		if !s.allowCustomBroadcastSource(kind, sourcePeerID, customBroadcastRoleMessage) {
			return nil, nil
		}
		if len(data.Message.Data) == 0 {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "invalid_payload")
			return nil, nil
		}
		if len(data.Message.Data) > maxOverlayPayloadSize {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "oversize_payload")
			return nil, nil
		}

		hash := externalMessageFingerprint(s.spec.ShortID, data.Message.Data)
		now := time.Now()
		if !s.node.processedExternalMessages.Mark(hash, now) {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "seen")
			return nil, nil
		}
		if s.node.myExternalMessages.Seen(hash, now) {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "seen")
			return nil, nil
		}
		parsed, err := parseExternalMessageData(data.Message.Data)
		if err != nil {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "invalid_payload")
			return nil, nil
		}
		addrKey := parsed.address
		if err = s.node.addExternalMessageAddressLimit(addrKey, now); err != nil {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "address_rate_limited")
			return nil, nil
		}
		if err = s.node.acceptExternalMessage(s.node.runtimeContext(), ExternalMessageEvent{
			Body:    data.Message.Data,
			Root:    parsed.root,
			Message: parsed.message,
		}); err != nil {
			s.node.dropExternalMessageAddressLimit(addrKey, now)
			s.node.noteBroadcastDrop(s.spec.Name, kind, "external_message_rejected")
			return nil, nil
		}

		rebroadcast, err := s.inboundRebroadcastPayload(kind, payload, peer, delivery)
		if err != nil {
			s.node.dropExternalMessageAddressLimit(addrKey, now)
			return nil, err
		}

		// The crc64 key was already marked in processedExternalMessages above;
		// reuse it (deduped) instead of hashing the payload again and filling
		// the shared block deduper with high-rate externals.
		return &acceptedBroadcast{
			fingerprint: hash,
			deduped:     true,
			rebroadcast: rebroadcast,
		}, nil
	case tonnodeapi.NewBlockCandidateBroadcast:
		return s.classifyBlockCandidateBroadcast(
			"tonNode.newBlockCandidateBroadcast",
			data.ID,
			validBlockCandidateBroadcast(data.ID, data.Data),
			peer, msg, payload, delivery, trusted, sourcePeerID,
		)
	case tonnodeapi.NewBlockCandidateBroadcastCompressed:
		return s.classifyBlockCandidateBroadcast(
			"tonNode.newBlockCandidateBroadcastCompressed",
			data.ID,
			validCompressedBroadcast(data.ID, data.Compressed),
			peer, msg, payload, delivery, trusted, sourcePeerID,
		)
	case tonnodeapi.NewBlockCandidateBroadcastCompressedV2:
		return s.classifyBlockCandidateBroadcast(
			"tonNode.newBlockCandidateBroadcastCompressedV2",
			data.ID,
			validCompressedBroadcast(data.ID, data.Compressed),
			peer, msg, payload, delivery, trusted, sourcePeerID,
		)
	case BlockFinalityBroadcast:
		return s.classifyBlockFinalityBroadcast(
			blockFinalityBroadcastKind,
			data.ID,
			data.SignatureSet,
			peer, payload, delivery, trusted, sourcePeerID,
		)
	case IhrMessageBroadcast:
		kind := "tonNode.ihrMessageBroadcast"
		if s.spec.Kind == overlayKindCustomFixed {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "unsupported_custom_broadcast")
			return nil, nil
		}
		if len(data.Message.Data) == 0 {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "invalid_payload")
			return nil, nil
		}
		fingerprint, err := payload.fingerprint(s.spec.ShortID)
		if err != nil {
			return nil, err
		}
		rebroadcast, err := s.inboundRebroadcastPayload(kind, payload, peer, delivery)
		if err != nil {
			return nil, err
		}
		return &acceptedBroadcast{
			fingerprint: fingerprint,
			rebroadcast: rebroadcast,
		}, nil
	default:
		return nil, nil
	}
}

func (s *overlaySubscription) classifyFullBlockBroadcast(
	kind string,
	block ton.BlockIDExt,
	valid bool,
	peer *overlayPeer,
	msg any,
	payload *broadcastPayload,
	delivery Delivery,
	trusted bool,
	sourcePeerID PeerID,
) (*acceptedBroadcast, error) {
	if !s.allowCustomBroadcastSource(kind, sourcePeerID, customBroadcastRoleBlock) {
		return nil, nil
	}
	if !valid {
		s.node.noteBroadcastDrop(s.spec.Name, kind, "invalid_payload")
		return nil, nil
	}
	if s.node.alreadyAppliedMasterchainBroadcast(block) {
		s.node.noteBroadcastDrop(s.spec.Name, kind, "already_applied")
		return nil, nil
	}

	fingerprint, err := payload.fingerprint(s.spec.ShortID)
	if err != nil {
		return nil, err
	}
	return s.acceptedFullBlockBroadcast(fingerprint, delivery, trusted, kind, block, sourcePeerID, msg, payload, peer)
}

func (s *overlaySubscription) classifyBlockCandidateBroadcast(
	kind string,
	block ton.BlockIDExt,
	valid bool,
	peer *overlayPeer,
	msg any,
	payload *broadcastPayload,
	delivery Delivery,
	trusted bool,
	sourcePeerID PeerID,
) (*acceptedBroadcast, error) {
	if !s.allowCustomBroadcastSource(kind, sourcePeerID, customBroadcastRoleBlock) {
		return nil, nil
	}
	if !valid {
		s.node.noteBroadcastDrop(s.spec.Name, kind, "invalid_payload")
		return nil, nil
	}
	if s.node.alreadyAppliedMasterchainBroadcast(block) {
		s.node.noteBroadcastDrop(s.spec.Name, kind, "already_applied")
		return nil, nil
	}

	fingerprint, err := payload.fingerprint(s.spec.ShortID)
	if err != nil {
		return nil, err
	}
	return s.acceptedBlockCandidateBroadcast(fingerprint, delivery, trusted, kind, block, msg, payload, peer, sourcePeerID)
}

func (s *overlaySubscription) classifyBlockFinalityBroadcast(
	kind string,
	block ton.BlockIDExt,
	signatureSet any,
	peer *overlayPeer,
	payload *broadcastPayload,
	delivery Delivery,
	trusted bool,
	sourcePeerID PeerID,
) (*acceptedBroadcast, error) {
	if !s.allowCustomBroadcastSource(kind, sourcePeerID, customBroadcastRoleBlock) {
		return nil, nil
	}
	if !validBlockID(block) {
		s.node.noteBroadcastDrop(s.spec.Name, kind, "invalid_payload")
		return nil, nil
	}
	if s.node.alreadyAppliedMasterchainBroadcast(block) {
		s.node.noteBroadcastDrop(s.spec.Name, kind, "already_applied")
		return nil, nil
	}

	fingerprint, err := payload.fingerprint(s.spec.ShortID)
	if err != nil {
		return nil, err
	}
	if !s.node.deduper.Mark(fingerprint, time.Now()) {
		s.node.noteBroadcastDrop(s.spec.Name, kind, "seen")
		return nil, nil
	}

	signatures, err := broadcastSignatureSetFromTL(signatureSet)
	if err != nil {
		s.log.Debug().
			Err(err).
			Str("block", formatBlockRef(block)).
			Str("kind", kind).
			Msg("dropping block finality broadcast because validator signatures cannot be parsed")
		s.node.noteBroadcastDrop(s.spec.Name, kind, "signature_parse_failed")
		return nil, nil
	}
	if !signatures.IsSimplex() {
		s.node.noteBroadcastDrop(s.spec.Name, kind, "signature_parse_failed")
		return nil, nil
	}
	if isMasterchainBlock(block) && !signatures.Final() {
		s.node.noteBroadcastDrop(s.spec.Name, kind, "signature_parse_failed")
		return nil, nil
	}

	checkStarted := s.node.startBroadcastPipelineStage()
	signatureCheck, err := s.node.checkBlockFinalitySignatures(kind, block, signatures)
	checkResult := broadcastPipelineResultSuccess
	if err != nil {
		checkResult = broadcastPipelineResultError
	}
	s.node.observeBroadcastPipelineStageSince(checkStarted, broadcastPipelineStageFinalitySigCheck, kind, delivery, checkResult)
	if err != nil {
		s.node.forgetBroadcastFingerprintIfRetryable(fingerprint, err)
		s.log.Debug().
			Err(err).
			Str("block", formatBlockRef(block)).
			Str("kind", kind).
			Msg("dropping block finality broadcast because validator signatures are not verified")
		s.node.noteBroadcastDrop(s.spec.Name, kind, "signature_check_failed")
		return nil, nil
	}

	rebroadcast, err := s.inboundRebroadcastPayload(kind, payload, peer, delivery)
	if err != nil {
		s.node.deduper.Forget(fingerprint)
		return nil, err
	}
	if err = rebroadcast.materializePayload(); err != nil {
		s.node.deduper.Forget(fingerprint)
		return nil, err
	}

	payloadLen := rebroadcast.payloadLen()
	assembleStarted := s.node.startBroadcastPipelineStage()
	assembled, assembleErr := s.node.rememberBlockFinality(checkedBlockFinality{
		block:                 block,
		signatures:            signatures,
		signaturesCell:        signatureCheck.SignaturesCell,
		signaturesVerifiedKey: signatureCheck.SignaturesVerifiedKey,
		sourcePeerID:          sourcePeerID,
		payloadBytes:          payloadLen,
	})
	assembleResult := broadcastPipelineResultMiss
	if assembleErr != nil {
		assembleResult = broadcastPipelineResultError
	} else if len(assembled) > 0 {
		assembleResult = broadcastPipelineResultSuccess
	}
	s.node.observeBroadcastPipelineStageSince(assembleStarted, broadcastPipelineStageFinalityAssemble, kind, delivery, assembleResult)

	accepted := s.acceptedBlockBroadcast(fingerprint, delivery, trusted, kind, block, sourcePeerID)
	accepted.deduped = true
	accepted.rebroadcast = rebroadcast
	if len(assembled) > 0 {
		accepted.event.Downloaded = &assembled[0]
	}
	return accepted, nil
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

func (s *overlaySubscription) inboundRebroadcastPayload(kind string, payload *broadcastPayload, peer *overlayPeer, delivery Delivery) (*rebroadcastRequest, error) {
	payload.mu.Lock()
	if payload.serialized {
		data, err := payload.payload, payload.err
		payload.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return s.inboundRebroadcast(kind, data, peer, delivery), nil
	}
	payload.mu.Unlock()

	rebroadcast := s.inboundRebroadcast(kind, nil, peer, delivery)
	rebroadcast.payloadSource = payload
	return rebroadcast, nil
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

func (s *overlaySubscription) acceptedShardBlockBroadcast(fingerprint string, delivery Delivery, trusted bool, block ton.BlockIDExt, sourcePeerID PeerID, desc *ShardBlockDescription) *acceptedBroadcast {
	accepted := s.acceptedBlockBroadcast(fingerprint, delivery, trusted, "tonNode.newShardBlockBroadcast", block, sourcePeerID)
	accepted.event.ShardDescription = desc
	return accepted
}

func (s *overlaySubscription) blockFinalityBroadcastEvents(delivery Delivery, trusted bool, blocks []DownloadedBlock) []BroadcastEvent {
	if len(blocks) == 0 {
		return nil
	}

	events := make([]BroadcastEvent, 0, len(blocks))
	for i := range blocks {
		downloaded := blocks[i]
		events = append(events, BroadcastEvent{
			Overlay:      s.spec.Name,
			Kind:         blockFinalityBroadcastKind,
			Delivery:     delivery,
			Trusted:      trusted,
			Block:        downloaded.ID,
			Downloaded:   &downloaded,
			SourcePeerID: downloaded.SourcePeerID,
			ReceivedAt:   time.Now(),
		})
	}
	return events
}

func (s *overlaySubscription) acceptedFullBlockBroadcast(fingerprint string, delivery Delivery, trusted bool, kind string, block ton.BlockIDExt, sourcePeerID PeerID, msg any, payload *broadcastPayload, peer *overlayPeer) (*acceptedBroadcast, error) {
	if !s.node.deduper.Mark(fingerprint, time.Now()) {
		s.node.noteBroadcastDrop(s.spec.Name, kind, "seen")
		return nil, nil
	}

	proofRoot, preSigSet, signaturesChecked, err := predecodeBlockBroadcastSignatureCheck(block, msg)
	if err != nil {
		s.log.Debug().
			Err(err).
			Str("block", formatBlockRef(block)).
			Str("kind", kind).
			Msg("dropping block broadcast because validator signatures cannot be prepared before decode")
		s.node.noteBroadcastDrop(s.spec.Name, kind, "signature_parse_failed")
		return nil, nil
	}

	// the validator-signature pass only needs the parsed proof root and the
	// signature set, so it runs concurrently with the expensive block decode
	var sigCheck chan error
	if signaturesChecked {
		sigCheck = make(chan error, 1)
		go func() {
			sigCheck <- s.node.checkBlockBroadcastSignatures(kind, block, proofRoot, preSigSet)
		}()
	}

	// the same block arrives once per delivery path (public FEC + custom
	// two-step) under different broadcast fingerprints; the decode result is
	// pinned by the block id, so reuse it instead of decoding again
	downloaded, sigSet, cached := s.node.cachedDecodedBroadcast(kind, block)
	if cached && !signaturesChecked {
		// v1 tonNode.blockBroadcastCompressed carries its validator
		// signatures inside the payload, so preSigSet is nil and a cached
		// sigSet (the first copy's, possibly unverified) would gate every
		// later copy: a peer could pair valid block bytes with invalid
		// signatures to suppress honest copies for the cache TTL. Re-decode
		// and verify this payload's own signatures instead.
		downloaded, sigSet, cached = nil, nil, false
	}
	if cached {
		s.node.noteBroadcast("decode_reused", s.spec.Name, kind)
		if preSigSet != nil {
			sigSet = preSigSet
		}
	}

	// kinds whose signatures are already verified can leave the expensive
	// decode to the bounded pool: joining the signature check first keeps the
	// trust gate on this thread (a failure still suppresses FEC relay), while
	// a nil return right after lets the overlay relay the hash-verified,
	// signature-verified payload without waiting out a multi-MB decode
	if !cached && signaturesChecked {
		if sigErr := <-sigCheck; sigErr != nil {
			s.node.forgetBroadcastFingerprintIfRetryable(fingerprint, sigErr)
			s.log.Debug().
				Err(sigErr).
				Str("block", formatBlockRef(block)).
				Str("kind", kind).
				Str("source_peer_id", sourcePeerID.String()).
				Msg("dropping block broadcast because validator signatures are not verified")
			s.node.noteBroadcastDrop(s.spec.Name, kind, "signature_check_failed")
			return nil, nil
		}
		sigCheck = nil

		rebroadcast, rbErr := s.inboundRebroadcastPayload(kind, payload, peer, delivery)
		if rbErr != nil {
			s.node.deduper.Forget(fingerprint)
			return nil, rbErr
		}
		if s.node.enqueueBroadcastDecode(offloadedBroadcastDecode{
			fingerprint:  fingerprint,
			overlay:      s.spec.Name,
			delivery:     delivery,
			trusted:      trusted,
			kind:         kind,
			block:        block,
			sourcePeerID: sourcePeerID,
			receivedAt:   time.Now(),
			msg:          msg,
			proofRoot:    proofRoot,
			preSigSet:    preSigSet,
			rebroadcast:  rebroadcast,
		}) {
			// the ack counts as "accepted" for relay purposes even though the
			// decode may still fail on the pool (mirrors the reference node,
			// which relays after signature validation, not after decode);
			// block is set so custom-overlay fanout of the signature-verified
			// payload does not wait out the decode — the fanout deduper keeps
			// the worker's completion from fanning out a second time
			blockCopy := cloneBlockID(block)
			accepted := &acceptedBroadcast{
				fingerprint: fingerprint,
				deduped:     true,
				block:       &blockCopy,
				rebroadcast: rebroadcast,
			}
			if block.Workchain == -1 && block.Shard == topShard {
				wake := block
				accepted.masterchainWake = &wake
			}
			return accepted, nil
		}
	}

	if !cached {
		downloaded, sigSet, err = s.node.decodeBroadcastBlock(s.node.runtimeContext(), msg, proofRoot, preSigSet)
		// v1 payloads carry unverified signatures at this point, and their
		// deliveries never read the cache anyway — cache only verified kinds
		if err == nil && downloaded != nil && signaturesChecked {
			s.node.rememberDecodedBroadcast(kind, block, downloaded, sigSet)
		}
	}
	if sigCheck != nil {
		if sigErr := <-sigCheck; sigErr != nil {
			s.node.forgetBroadcastFingerprintIfRetryable(fingerprint, sigErr)
			s.log.Debug().
				Err(sigErr).
				Str("block", formatBlockRef(block)).
				Str("kind", kind).
				Str("source_peer_id", sourcePeerID.String()).
				Msg("dropping block broadcast because validator signatures are not verified")
			s.node.noteBroadcastDrop(s.spec.Name, kind, "signature_check_failed")
			return nil, nil
		}
	}
	if err != nil {
		stateNotReady := isBroadcastDecompressionStateNotReady(err)
		var rebroadcast *rebroadcastRequest
		if stateNotReady {
			pendingState, ok := broadcastDecompressionStateNotReady(err)
			if !ok {
				s.log.Debug().
					Err(err).
					Str("block", formatBlockRef(block)).
					Str("kind", kind).
					Msg("dropping pending block broadcast because state-ready artifact is missing")
				s.node.noteBroadcastDrop(s.spec.Name, kind, "state_artifact_missing")
				return nil, nil
			}

			_, isV2 := msg.(tonnodeapi.BlockBroadcastCompressedV2)
			if !isV2 {
				s.log.Debug().
					Err(err).
					Str("block", formatBlockRef(block)).
					Str("kind", kind).
					Msg("dropping pending block broadcast because payload type is not compressed-v2")
				s.node.noteBroadcastDrop(s.spec.Name, kind, "invalid_payload")
				return nil, nil
			}
			var payloadErr error
			rebroadcast, payloadErr = s.inboundRebroadcastPayload(kind, payload, peer, delivery)
			if payloadErr != nil {
				s.node.deduper.Forget(fingerprint)
				return nil, payloadErr
			}

			var verifiedSignaturesKey []byte
			if preSigSet != nil {
				verifiedSignaturesKey = preSigSet.ContentKey(block)
			}
			s.node.schedulePendingBlockBroadcastDecode(pendingBlockBroadcastDecode{
				fingerprint:           fingerprint,
				overlay:               s.spec.Name,
				delivery:              delivery,
				trusted:               trusted,
				kind:                  kind,
				block:                 block,
				sourcePeerID:          sourcePeerID,
				receivedAt:            time.Now(),
				msg:                   msg,
				prev:                  pendingState.prev,
				proofRoot:             pendingState.proofRoot,
				rebroadcast:           rebroadcast,
				verifiedSignaturesKey: verifiedSignaturesKey,
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
			return nil, nil
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
		return accepted, nil
	}

	if !signaturesChecked && (sigSet == nil || downloaded == nil) {
		s.node.noteBroadcastDrop(s.spec.Name, kind, "signature_parse_failed")
		return nil, nil
	}
	if !signaturesChecked {
		err = s.node.checkBlockBroadcastSignatures(kind, block, downloaded.Proof, sigSet)
	}
	if err != nil {
		s.node.forgetBroadcastFingerprintIfRetryable(fingerprint, err)
		s.log.Debug().
			Err(err).
			Str("block", formatBlockRef(block)).
			Str("kind", kind).
			Msg("dropping block broadcast because validator signatures are not verified")
		s.node.noteBroadcastDrop(s.spec.Name, kind, "signature_check_failed")
		return nil, nil
	}

	rebroadcast, err := s.inboundRebroadcastPayload(kind, payload, peer, delivery)
	if err != nil {
		s.node.deduper.Forget(fingerprint)
		return nil, err
	}

	accepted := s.acceptedBlockBroadcast(fingerprint, delivery, trusted, kind, block, sourcePeerID)
	accepted.deduped = true
	downloaded.SourcePeerID = sourcePeerID
	// Both branches above end with sigSet verified for this block; remember
	// the proven content so the apply path can skip an equal proof-signature
	// re-check.
	if sigSet != nil {
		downloaded.SignaturesVerifiedKey = sigSet.ContentKey(block)
	}
	accepted.event.Downloaded = downloaded
	accepted.rebroadcast = rebroadcast
	return accepted, nil
}

// NoteAppliedMasterchainSeqno records the highest applied masterchain seqno.
// Classify drops masterchain block broadcasts at or below it before any proof
// parsing or signature verification, mirroring the reference node's
// validate-broadcast early exit for already-applied blocks.
func (n *Node) NoteAppliedMasterchainSeqno(seqno uint32) {
	for {
		current := n.appliedMasterchainSeqno.Load()
		if seqno <= current {
			return
		}
		if n.appliedMasterchainSeqno.CompareAndSwap(current, seqno) {
			return
		}
	}
}

func (n *Node) alreadyAppliedMasterchainBroadcast(block ton.BlockIDExt) bool {
	if !isMasterchainBlock(block) {
		return false
	}
	applied := n.appliedMasterchainSeqno.Load()
	return applied > 0 && block.SeqNo <= applied
}

func (n *Node) forgetBroadcastFingerprintIfRetryable(fingerprint string, err error) {
	if fingerprint == "" || !errors.Is(err, ErrBroadcastSignatureRetryable) {
		return
	}

	n.deduper.Forget(fingerprint)
}

func (s *overlaySubscription) acceptedBlockCandidateBroadcast(fingerprint string, delivery Delivery, trusted bool, kind string, block ton.BlockIDExt, msg any, payload *broadcastPayload, peer *overlayPeer, sourcePeerID PeerID) (*acceptedBroadcast, error) {
	if !s.node.deduper.Mark(fingerprint, time.Now()) {
		s.node.noteBroadcastDrop(s.spec.Name, kind, "seen")
		return nil, nil
	}

	downloaded, _, cached := s.node.cachedDecodedBroadcast(kind, block)
	if cached {
		s.node.noteBroadcast("decode_reused", s.spec.Name, kind)
	} else {
		decodeStarted := s.node.startBroadcastPipelineStage()
		var err error
		downloaded, err = decodeBlockCandidateBroadcast(msg)
		decodeResult := broadcastPipelineResultSuccess
		if err != nil {
			decodeResult = broadcastPipelineResultError
		}
		s.node.observeBroadcastPipelineStageSince(decodeStarted, broadcastPipelineStageCandidateDecode, kind, delivery, decodeResult)
		if err != nil {
			s.log.Debug().
				Err(err).
				Str("block", formatBlockRef(block)).
				Str("kind", kind).
				Msg("dropping block candidate broadcast because payload decode failed")
			s.node.noteBroadcastDrop(s.spec.Name, kind, "decode_failed")
			return nil, nil
		}
		s.node.rememberDecodedBroadcast(kind, block, downloaded, nil)
	}

	rebroadcast, err := s.inboundRebroadcastPayload(kind, payload, peer, delivery)
	if err != nil {
		s.node.deduper.Forget(fingerprint)
		return nil, err
	}

	downloaded.SourcePeerID = sourcePeerID
	assembleStarted := s.node.startBroadcastPipelineStage()
	assembled, assembleErr := s.node.rememberBlockFinalityCandidate(downloaded)
	assembleResult := broadcastPipelineResultMiss
	if assembleErr != nil {
		assembleResult = broadcastPipelineResultError
	} else if len(assembled) > 0 {
		assembleResult = broadcastPipelineResultSuccess
	}
	s.node.observeBroadcastPipelineStageSince(assembleStarted, broadcastPipelineStageFinalityAssemble, kind, delivery, assembleResult)
	s.node.rememberShardBlockCandidate(downloaded)
	s.node.publishNonfinalDownloadedBlock(downloaded, storage.LiveBlockNonfinalCandidate)
	if s.node.blockReceivedHooks && !isMasterchainBlock(downloaded.ID) {
		s.node.observeBlockReceived(s.node.runtimeContext(), downloaded, false)
	}

	accepted := &acceptedBroadcast{
		fingerprint: fingerprint,
		deduped:     true,
		block:       block.Copy(),
		rebroadcast: rebroadcast,
	}
	accepted.extraEvents = s.blockFinalityBroadcastEvents(delivery, trusted, assembled)

	return accepted, nil
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
		acceptedNoted = n.acceptBroadcastEvent(*accepted.event, accepted.skipAcceptedMetric)
	}
	for _, event := range accepted.extraEvents {
		n.acceptBroadcastEvent(event, true)
	}

	if accepted.rebroadcast != nil {
		if !acceptedNoted && !accepted.skipAcceptedMetric {
			n.noteBroadcast("accepted", accepted.rebroadcast.overlayName(), accepted.rebroadcast.kind)
		}
		if !accepted.rebroadcast.skipOverlayRebroadcast {
			accepted.rebroadcast.subscription.enqueueRebroadcast(*accepted.rebroadcast)
		}
	}

	n.enqueueCustomOverlayFanout(accepted)
}

func (n *Node) acceptBroadcastEvent(event BroadcastEvent, skipAcceptedMetric bool) bool {
	n.trackUnverifiedBroadcastBlock(event)
	if event.Downloaded != nil && event.Downloaded.ID.Equals(&event.Block) {
		cacheStarted := n.startBroadcastPipelineStage()
		cacheResult := broadcastPipelineResultMiss
		if isMasterchainBlock(event.Downloaded.ID) {
			if n.rememberMasterchainNextBroadcastBlock(event.Downloaded) {
				cacheResult = broadcastPipelineResultSuccess
			}
		} else if n.rememberShardBroadcastBlock(event.Downloaded) {
			cacheResult = broadcastPipelineResultSuccess
		}
		n.observeBroadcastPipelineStageSince(cacheStarted, broadcastPipelineStageHotCacheNotify, event.Kind, event.Delivery, cacheResult)
		n.publishNonfinalDownloadedBlock(event.Downloaded, storage.LiveBlockNonfinalSigned)
		if n.blockReceivedHooks {
			n.observeBlockReceived(n.runtimeContext(), event.Downloaded, true)
		}
	}
	if !n.eventQueue.Push(event) {
		return false
	}
	if !skipAcceptedMetric {
		n.noteBroadcast("accepted", event.Overlay, event.Kind)
	}
	return true
}

func (n *Node) enqueueCustomOverlayFanout(accepted acceptedBroadcast) {
	if n.customFanoutDeduper == nil || accepted.rebroadcast == nil || accepted.block == nil {
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

	source := *accepted.rebroadcast
	if err := source.materializePayload(); err != nil {
		n.log.Debug().
			Err(err).
			Str("kind", source.kind).
			Msg("dropping custom fanout because payload cannot be serialized")
		return
	}
	if len(source.payload) == 0 {
		return
	}

	for _, sub := range targets {
		req := rebroadcastRequest{
			subscription: sub,
			kind:         source.kind,
			payload:      source.payload,
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
		"tonNode.newBlockCandidateBroadcastCompressedV2",
		blockFinalityBroadcastKind:
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

func broadcastKindLabel(msg any) string {
	switch msg.(type) {
	case tonnodeapi.BlockBroadcast:
		return "tonNode.blockBroadcast"
	case tonnodeapi.BlockBroadcastCompressed:
		return "tonNode.blockBroadcastCompressed"
	case tonnodeapi.BlockBroadcastCompressedV2:
		return "tonNode.blockBroadcastCompressedV2"
	case tonnodeapi.NewShardBlockBroadcast:
		return "tonNode.newShardBlockBroadcast"
	case tonnodeapi.NewExternalMessageBroadcast:
		return "tonNode.externalMessageBroadcast"
	case tonnodeapi.NewBlockCandidateBroadcast:
		return "tonNode.newBlockCandidateBroadcast"
	case tonnodeapi.NewBlockCandidateBroadcastCompressed:
		return "tonNode.newBlockCandidateBroadcastCompressed"
	case tonnodeapi.NewBlockCandidateBroadcastCompressedV2:
		return "tonNode.newBlockCandidateBroadcastCompressedV2"
	case BlockFinalityBroadcast:
		return blockFinalityBroadcastKind
	case IhrMessageBroadcast:
		return "tonNode.ihrMessageBroadcast"
	default:
		return "unknown"
	}
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

// broadcastFingerprint returns the raw sha256 bytes as a string key; dedupe
// maps never surface the key to humans, so it is not hex-encoded.
func broadcastFingerprint(overlayID []byte, payload []byte) string {
	hasher := sha256.New()
	hasher.Write(overlayID)
	hasher.Write(payload)
	var sum [sha256.Size]byte
	return string(hasher.Sum(sum[:0]))
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
