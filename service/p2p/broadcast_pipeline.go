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

var externalMessageCRC64Table = crc64.MakeTable(crc64.ECMA)

type customBroadcastRole uint8

const (
	customBroadcastRoleMessage customBroadcastRole = iota
	customBroadcastRoleBlock
)

type acceptedBroadcast struct {
	fingerprint        string
	deduped            bool
	delivery           Delivery
	block              *ton.BlockIDExt
	event              *BroadcastEvent
	extraEvents        []BroadcastEvent
	masterchainWake    *ton.BlockIDExt
	rebroadcast        *rebroadcastRequest
	skipAcceptedMetric bool
}

type broadcastDisposition uint8

const (
	broadcastDispositionIgnore broadcastDisposition = iota
	broadcastDispositionAccept
)

type broadcastResult struct {
	disposition broadcastDisposition
	accepted    acceptedBroadcast
}

func ignoredBroadcastResult() broadcastResult {
	return broadcastResult{disposition: broadcastDispositionIgnore}
}

func acceptedBroadcastResult(accepted acceptedBroadcast) broadcastResult {
	return broadcastResult{
		disposition: broadcastDispositionAccept,
		accepted:    accepted,
	}
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

func newKnownIdentifiedBroadcastPayload(payload, identity []byte) *broadcastPayload {
	return &broadcastPayload{
		payload:    payload,
		identity:   identity,
		serialized: true,
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

func (s *overlaySubscription) handleOverlayBroadcastPayload(peer *overlayPeer, msg any, payload *broadcastPayload, delivery Delivery, trusted bool, sourcePeerID PeerID) overlay.BroadcastDisposition {
	kind := broadcastKindLabel(msg)
	if peer != nil && peer.noteReceive() {
		s.peerPromoted(peer)
	}
	if !s.node.canAcceptBroadcast(kind, false) {
		s.node.noteBroadcastDrop(s.spec.Name, kind, "broadcast_admission_closed")
		return overlay.BroadcastDispositionRetry
	}

	classifyStarted := s.node.startBroadcastPipelineStage()
	result, err := s.classifyBroadcastPayload(peer, msg, payload, delivery, trusted, sourcePeerID)
	if err != nil {
		s.node.observeBroadcastPipelineStageSince(classifyStarted, broadcastPipelineStageClassify, kind, delivery, broadcastPipelineResultError)
		s.log.Debug().Err(err).Msg("failed to classify inbound broadcast payload")
		return overlay.BroadcastDispositionRetry
	}
	classifyResult := broadcastPipelineResultDrop
	if result.disposition == broadcastDispositionAccept {
		classifyResult = broadcastPipelineResultSuccess
	}
	s.node.observeBroadcastPipelineStageSince(classifyStarted, broadcastPipelineStageClassify, kind, delivery, classifyResult)
	if result.disposition == broadcastDispositionIgnore {
		return overlay.BroadcastDispositionIgnore
	}

	s.node.acceptBroadcast(result.accepted)
	return overlay.BroadcastDispositionAcceptAndRelay
}

func (s *overlaySubscription) classifyBroadcastPayload(peer *overlayPeer, msg any, payload *broadcastPayload, delivery Delivery, trusted bool, sourcePeerID PeerID) (broadcastResult, error) {
	if s.spec.restrictsBroadcastKinds() {
		// We publish DoNotReceiveBroadcasts in our member flags when this
		// overlay is send-only for us (a validator under plumtree), and drop
		// what a peer sends anyway. Upstream gates the same six kinds inside
		// OverlayImpl::process_broadcast — but not the two-step pair, which it
		// processes regardless, so neither do we. Note this only suppresses
		// application processing: the transport has already decoded and
		// relayed by the time classify runs, so it does not save that work.
		if delivery != DeliveryTwoStep &&
			s.fastSync != nil && s.fastSync.declinesBroadcasts() {
			s.node.noteBroadcastDrop(
				s.spec.Name,
				broadcastKindLabel(msg),
				"fast_sync_broadcasts_declined",
			)
			return ignoredBroadcastResult(), nil
		}
		if !fastSyncBroadcastSupported(msg) {
			s.node.noteBroadcastDrop(
				s.spec.Name,
				broadcastKindLabel(msg),
				"unsupported_fast_sync_broadcast",
			)
			return ignoredBroadcastResult(), nil
		}
	}

	if sourcePeerID.IsZero() && peer != nil {
		sourcePeerID = peer.id
	}
	if s.spec.dropsSelfSourcedBroadcasts() && sourcePeerID == s.node.localID {
		s.node.noteBroadcastDrop(s.spec.Name, broadcastKindLabel(msg), "self")
		return ignoredBroadcastResult(), nil
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
			return ignoredBroadcastResult(), nil
		}
		if !validCompressedBroadcast(data.Block.ID, data.Block.Data) {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "invalid_payload")
			return ignoredBroadcastResult(), nil
		}
		fingerprint, err := payload.fingerprint(s.spec.ShortID)
		if err != nil {
			return broadcastResult{}, err
		}
		if !s.node.deduper.Mark(fingerprint, time.Now()) {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "seen")
			return ignoredBroadcastResult(), nil
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
			return s.dropUnverifiedBroadcast(fingerprint, kind, data.Block.ID, sourcePeerID, err)
		}
		rebroadcast, err := s.inboundRebroadcastPayload(kind, payload, peer, delivery)
		if err != nil {
			s.node.deduper.Forget(fingerprint)
			return broadcastResult{}, err
		}
		accepted := s.acceptedShardBlockBroadcast(fingerprint, delivery, trusted, data.Block.ID, sourcePeerID, desc)
		accepted.deduped = true
		accepted.rebroadcast = rebroadcast
		return acceptedBroadcastResult(accepted), nil
	case tonnodeapi.NewExternalMessageBroadcast:
		kind := "tonNode.externalMessageBroadcast"
		if !s.allowCustomBroadcastSource(kind, sourcePeerID, customBroadcastRoleMessage) {
			return ignoredBroadcastResult(), nil
		}
		if len(data.Message.Data) == 0 {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "invalid_payload")
			return ignoredBroadcastResult(), nil
		}
		if len(data.Message.Data) > maxOverlayPayloadSize {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "oversize_payload")
			return ignoredBroadcastResult(), nil
		}

		hash := externalMessageFingerprint(s.spec.ShortID, data.Message.Data)
		now := time.Now()
		if !s.node.processedExternalMessages.Mark(hash, now) {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "seen")
			return ignoredBroadcastResult(), nil
		}
		if s.node.myExternalMessages.Seen(hash, now) {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "seen")
			return ignoredBroadcastResult(), nil
		}
		parsed, err := parseExternalMessageData(data.Message.Data)
		if err != nil {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "invalid_payload")
			return ignoredBroadcastResult(), nil
		}
		addrKey := parsed.address
		if err = s.node.addExternalMessageAddressLimit(addrKey, now); err != nil {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "address_rate_limited")
			return ignoredBroadcastResult(), nil
		}
		if err = s.node.acceptExternalMessage(s.node.runCtx, ExternalMessageEvent{
			Body:    data.Message.Data,
			Root:    parsed.root,
			Message: parsed.message,
		}); err != nil {
			s.node.externalMessageLimiter.Remove(addrKey, now)
			s.node.noteBroadcastDrop(s.spec.Name, kind, "external_message_rejected")
			return ignoredBroadcastResult(), nil
		}

		rebroadcast, err := s.inboundRebroadcastPayload(kind, payload, peer, delivery)
		if err != nil {
			s.node.externalMessageLimiter.Remove(addrKey, now)
			return broadcastResult{}, err
		}

		// The crc64 key was already marked in processedExternalMessages above;
		// reuse it (deduped) instead of hashing the payload again and filling
		// the shared block deduper with high-rate externals.
		return acceptedBroadcastResult(acceptedBroadcast{
			fingerprint: hash,
			deduped:     true,
			delivery:    delivery,
			rebroadcast: rebroadcast,
		}), nil
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
	case OutMsgQueueProofBroadcast:
		// cppnode currently leaves application processing disabled, while the
		// overlay still relays an admitted broadcast.
		return acceptedBroadcastResult(acceptedBroadcast{}), nil
	case IhrMessageBroadcast:
		kind := "tonNode.ihrMessageBroadcast"
		if s.spec.dropsIHRBroadcasts() {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "unsupported_custom_broadcast")
			return ignoredBroadcastResult(), nil
		}
		if len(data.Message.Data) == 0 {
			s.node.noteBroadcastDrop(s.spec.Name, kind, "invalid_payload")
			return ignoredBroadcastResult(), nil
		}
		fingerprint, err := payload.fingerprint(s.spec.ShortID)
		if err != nil {
			return broadcastResult{}, err
		}
		rebroadcast, err := s.inboundRebroadcastPayload(kind, payload, peer, delivery)
		if err != nil {
			return broadcastResult{}, err
		}
		return acceptedBroadcastResult(acceptedBroadcast{
			fingerprint: fingerprint,
			delivery:    delivery,
			rebroadcast: rebroadcast,
		}), nil
	default:
		return ignoredBroadcastResult(), nil
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
) (broadcastResult, error) {
	if !s.allowCustomBroadcastSource(kind, sourcePeerID, customBroadcastRoleBlock) {
		return ignoredBroadcastResult(), nil
	}
	if !valid {
		s.node.noteBroadcastDrop(s.spec.Name, kind, "invalid_payload")
		return ignoredBroadcastResult(), nil
	}
	if s.node.alreadyAppliedBroadcast(block) {
		s.node.noteBroadcastDrop(s.spec.Name, kind, "already_applied")
		return ignoredBroadcastResult(), nil
	}

	fingerprint, err := payload.fingerprint(s.spec.ShortID)
	if err != nil {
		return broadcastResult{}, err
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
) (broadcastResult, error) {
	if !s.allowCustomBroadcastSource(kind, sourcePeerID, customBroadcastRoleBlock) {
		return ignoredBroadcastResult(), nil
	}
	if !valid {
		s.node.noteBroadcastDrop(s.spec.Name, kind, "invalid_payload")
		return ignoredBroadcastResult(), nil
	}
	if s.node.alreadyAppliedBroadcast(block) {
		s.node.noteBroadcastDrop(s.spec.Name, kind, "already_applied")
		return ignoredBroadcastResult(), nil
	}

	fingerprint, err := payload.fingerprint(s.spec.ShortID)
	if err != nil {
		return broadcastResult{}, err
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
) (broadcastResult, error) {
	if !s.allowCustomBroadcastSource(kind, sourcePeerID, customBroadcastRoleBlock) {
		return ignoredBroadcastResult(), nil
	}
	if !storage.BlockIDHashesKnown(block) {
		s.node.noteBroadcastDrop(s.spec.Name, kind, "invalid_payload")
		return ignoredBroadcastResult(), nil
	}
	if s.node.alreadyAppliedBroadcast(block) {
		s.node.noteBroadcastDrop(s.spec.Name, kind, "already_applied")
		return ignoredBroadcastResult(), nil
	}

	fingerprint, err := payload.fingerprint(s.spec.ShortID)
	if err != nil {
		return broadcastResult{}, err
	}
	if !s.node.deduper.Mark(fingerprint, time.Now()) {
		s.node.noteBroadcastDrop(s.spec.Name, kind, "seen")
		return ignoredBroadcastResult(), nil
	}

	signatures, err := broadcastSignatureSetFromTL(signatureSet)
	if err != nil {
		s.log.Debug().
			Err(err).
			Str("block", storage.FormatBlockRef(block)).
			Str("kind", kind).
			Msg("dropping block finality broadcast because validator signatures cannot be parsed")
		s.node.noteBroadcastDrop(s.spec.Name, kind, "signature_parse_failed")
		return ignoredBroadcastResult(), nil
	}
	if !signatures.IsSimplex() {
		s.node.noteBroadcastDrop(s.spec.Name, kind, "signature_parse_failed")
		return ignoredBroadcastResult(), nil
	}
	if isMasterchainBlock(block) && !signatures.Final() {
		s.node.noteBroadcastDrop(s.spec.Name, kind, "signature_parse_failed")
		return ignoredBroadcastResult(), nil
	}

	checkStarted := s.node.startBroadcastPipelineStage()
	signatureCheck, err := s.node.checkBlockFinalitySignatures(kind, block, signatures)
	checkResult := broadcastPipelineResultSuccess
	if err != nil {
		checkResult = broadcastPipelineResultError
	}
	s.node.observeBroadcastPipelineStageSince(checkStarted, broadcastPipelineStageFinalitySigCheck, kind, delivery, checkResult)
	if err != nil {
		return s.dropUnverifiedBroadcast(fingerprint, kind, block, sourcePeerID, err)
	}

	rebroadcast, err := s.inboundRebroadcastPayload(kind, payload, peer, delivery)
	if err != nil {
		s.node.deduper.Forget(fingerprint)
		return broadcastResult{}, err
	}
	if err = rebroadcast.materializePayload(); err != nil {
		s.node.deduper.Forget(fingerprint)
		return broadcastResult{}, err
	}

	payloadLen := rebroadcast.payloadLen()
	assembleStarted := s.node.startBroadcastPipelineStage()
	assembled, assembleErr := s.node.rememberBlockFinality(checkedBlockFinality{
		block:                 block,
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
	return acceptedBroadcastResult(accepted), nil
}

func (s *overlaySubscription) allowCustomBroadcastSource(kind string, sourcePeerID PeerID, role customBroadcastRole) bool {
	if !s.spec.authorizesBroadcastSenders() {
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
		subscription: s,
		kind:         kind,
		payload:      payload,
		sourcePeerID: sourcePeerID,
		skipOverlayRebroadcast: delivery == DeliveryTwoStep ||
			delivery == DeliveryPlumtree ||
			(delivery == DeliveryFEC && s.spec.relaysFECBroadcasts()),
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

func (s *overlaySubscription) acceptedBlockBroadcast(fingerprint string, delivery Delivery, trusted bool, kind string, block ton.BlockIDExt, sourcePeerID PeerID) acceptedBroadcast {
	return acceptedBroadcast{
		fingerprint: fingerprint,
		delivery:    delivery,
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

func (s *overlaySubscription) acceptedShardBlockBroadcast(fingerprint string, delivery Delivery, trusted bool, block ton.BlockIDExt, sourcePeerID PeerID, desc *ShardBlockDescription) acceptedBroadcast {
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

func (s *overlaySubscription) acceptedFullBlockBroadcast(fingerprint string, delivery Delivery, trusted bool, kind string, block ton.BlockIDExt, sourcePeerID PeerID, msg any, payload *broadcastPayload, peer *overlayPeer) (broadcastResult, error) {
	if !s.node.deduper.Mark(fingerprint, time.Now()) {
		s.node.noteBroadcastDrop(s.spec.Name, kind, "seen")
		return ignoredBroadcastResult(), nil
	}

	predecodedSignatureCheck, err := predecodeBlockBroadcastSignatureCheck(block, msg)
	signaturesChecked := false
	switch {
	case errors.Is(err, errBlockBroadcastSignatureCheckNotApplicable):
		err = nil
	case err != nil:
		s.log.Debug().
			Err(err).
			Str("block", storage.FormatBlockRef(block)).
			Str("kind", kind).
			Msg("dropping block broadcast because validator signatures cannot be prepared before decode")
		s.node.noteBroadcastDrop(s.spec.Name, kind, "signature_parse_failed")
		return ignoredBroadcastResult(), nil
	default:
		signaturesChecked = true
	}
	proofRoot := predecodedSignatureCheck.proofRoot
	preSigSet := predecodedSignatureCheck.signatureSet

	// Nothing expensive may run on an unverified payload: the signature pass
	// needs only the proof root and the signature set parsed above, so it goes
	// first and the decode below is reached by verified broadcasts alone. This
	// is the reference ordering — process_broadcast(blockBroadcastCompressedV2)
	// gates obtain_state_for_decompression on validate_block_broadcast_signatures
	// — and it is what keeps a replayed payload from buying a decode worker.
	if signaturesChecked {
		checkStarted := s.node.startBroadcastPipelineStage()
		sigErr := s.node.checkBlockBroadcastSignatures(kind, block, proofRoot, preSigSet)
		checkResult := broadcastPipelineResultSuccess
		if sigErr != nil {
			checkResult = broadcastPipelineResultError
		}
		s.node.observeBroadcastPipelineStageSince(checkStarted, broadcastPipelineStageBlockSigCheck, kind, delivery, checkResult)
		if sigErr != nil {
			return s.dropUnverifiedBroadcast(fingerprint, kind, block, sourcePeerID, sigErr)
		}
	}

	// the same block arrives once per delivery path (public FEC + custom
	// two-step) under different broadcast fingerprints; the decode result is
	// pinned by the block id, so reuse it instead of decoding again
	downloaded, sigSet, cacheErr := s.node.decodedBroadcasts.get(kind, block)
	cached := cacheErr == nil
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
		s.node.noteBroadcast("decode_reused", s.spec.Name, kind, delivery)
		if preSigSet != nil {
			sigSet = preSigSet
		}
	}

	// kinds whose signatures are verified above can leave the expensive decode
	// to the bounded pool and return right away, so the overlay relays the
	// hash-verified, signature-verified payload without waiting out the decode
	var rebroadcast *rebroadcastRequest
	if !cached && signaturesChecked {
		var rbErr error
		rebroadcast, rbErr = s.inboundRebroadcastPayload(kind, payload, peer, delivery)
		if rbErr != nil {
			s.node.deduper.Forget(fingerprint)
			return broadcastResult{}, rbErr
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
			accepted := acceptedBroadcast{
				fingerprint: fingerprint,
				deduped:     true,
				delivery:    delivery,
				block:       &blockCopy,
				rebroadcast: rebroadcast,
			}
			if block.Workchain == -1 && block.Shard == topShard {
				wake := block
				accepted.masterchainWake = &wake
			}
			return acceptedBroadcastResult(accepted), nil
		}
		// pool refused: the decode below runs inline on this thread
	}

	if !cached {
		// Reached either because the payload kind cannot be offloaded (v1
		// carries its signatures inside the payload) or because the decode pool
		// refused it. The latter means this multi-MB decode now runs on a
		// transport receive goroutine, so it is measured separately from the
		// pool's decode_async stage.
		decodeStarted := s.node.startBroadcastPipelineStage()
		downloaded, sigSet, err = s.node.decodeBroadcastBlock(s.node.runCtx, msg, proofRoot, preSigSet)
		decodeResult := broadcastPipelineResultSuccess
		if err != nil {
			decodeResult = broadcastPipelineResultError
			if isBroadcastDecompressionStateNotReady(err) {
				decodeResult = broadcastPipelineResultMiss
			}
		}
		s.node.observeBroadcastPipelineStageSince(decodeStarted, broadcastPipelineStageDecodeInline, kind, delivery, decodeResult)
		// v1 payloads carry unverified signatures at this point, and their
		// deliveries never read the cache anyway — cache only verified kinds
		if err == nil && signaturesChecked {
			s.node.decodedBroadcasts.put(kind, block, downloaded, sigSet)
		}
	}
	if err != nil {
		stateNotReady := isBroadcastDecompressionStateNotReady(err)
		if stateNotReady {
			pendingState, ok := broadcastDecompressionStateNotReady(err)
			if !ok {
				s.log.Debug().
					Err(err).
					Str("block", storage.FormatBlockRef(block)).
					Str("kind", kind).
					Msg("dropping pending block broadcast because state-ready artifact is missing")
				s.node.noteBroadcastDrop(s.spec.Name, kind, "state_artifact_missing")
				return ignoredBroadcastResult(), nil
			}

			_, isV2 := msg.(tonnodeapi.BlockBroadcastCompressedV2)
			if !isV2 {
				s.log.Debug().
					Err(err).
					Str("block", storage.FormatBlockRef(block)).
					Str("kind", kind).
					Msg("dropping pending block broadcast because payload type is not compressed-v2")
				s.node.noteBroadcastDrop(s.spec.Name, kind, "invalid_payload")
				return ignoredBroadcastResult(), nil
			}
			if rebroadcast == nil {
				var payloadErr error
				rebroadcast, payloadErr = s.inboundRebroadcastPayload(kind, payload, peer, delivery)
				if payloadErr != nil {
					s.node.deduper.Forget(fingerprint)
					return broadcastResult{}, payloadErr
				}
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
				Str("block", storage.FormatBlockRef(block)).
				Str("kind", kind).
				Msg("queued block broadcast until previous state is available")
		} else {
			s.log.Debug().
				Err(err).
				Str("block", storage.FormatBlockRef(block)).
				Str("kind", kind).
				Msg("dropping block broadcast because payload decode failed")
			s.node.noteBroadcastDrop(s.spec.Name, kind, "decode_failed")
		}

		if !stateNotReady {
			return ignoredBroadcastResult(), nil
		}
		accepted := acceptedBroadcast{
			fingerprint: fingerprint,
			deduped:     true,
			delivery:    delivery,
			rebroadcast: rebroadcast,
		}
		if block.Workchain == -1 && block.Shard == topShard {
			wake := block
			accepted.masterchainWake = &wake
		}
		return acceptedBroadcastResult(accepted), nil
	}

	if !signaturesChecked && sigSet == nil {
		s.node.noteBroadcastDrop(s.spec.Name, kind, "signature_parse_failed")
		return ignoredBroadcastResult(), nil
	}
	if !signaturesChecked {
		// v1 compressed payloads carry their signatures inside the payload, so
		// this kind alone pays the decode before it can be verified
		checkStarted := s.node.startBroadcastPipelineStage()
		err = s.node.checkBlockBroadcastSignatures(kind, block, downloaded.Proof, sigSet)
		checkResult := broadcastPipelineResultSuccess
		if err != nil {
			checkResult = broadcastPipelineResultError
		}
		s.node.observeBroadcastPipelineStageSince(checkStarted, broadcastPipelineStageBlockSigCheck, kind, delivery, checkResult)
	}
	if err != nil {
		return s.dropUnverifiedBroadcast(fingerprint, kind, block, sourcePeerID, err)
	}

	if rebroadcast == nil {
		rebroadcast, err = s.inboundRebroadcastPayload(kind, payload, peer, delivery)
		if err != nil {
			s.node.deduper.Forget(fingerprint)
			return broadcastResult{}, err
		}
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
	return acceptedBroadcastResult(accepted), nil
}

// dropUnverifiedBroadcast is the single policy point for signature-check
// failures: retryable ones are forgotten so a later delivery re-runs
// admission (and propagate err so the receiver retries), permanent ones stay
// deduplicated and are ignored.
func (s *overlaySubscription) dropUnverifiedBroadcast(fingerprint, kind string, block ton.BlockIDExt, sourcePeerID PeerID, err error) (broadcastResult, error) {
	s.node.forgetBroadcastFingerprintIfRetryable(fingerprint, err)
	s.log.Debug().
		Err(err).
		Str("block", storage.FormatBlockRef(block)).
		Str("kind", kind).
		Str("source_peer_id", sourcePeerID.String()).
		Msg("dropping broadcast because validator signatures are not verified")
	s.node.noteBroadcastDrop(s.spec.Name, kind, "signature_check_failed")

	if errors.Is(err, ErrBroadcastSignatureRetryable) {
		return broadcastResult{}, err
	}
	return ignoredBroadcastResult(), nil
}

func (n *Node) forgetBroadcastFingerprintIfRetryable(fingerprint string, err error) {
	if fingerprint == "" || !errors.Is(err, ErrBroadcastSignatureRetryable) {
		return
	}

	n.deduper.Forget(fingerprint)
}

func (s *overlaySubscription) acceptedBlockCandidateBroadcast(fingerprint string, delivery Delivery, trusted bool, kind string, block ton.BlockIDExt, msg any, payload *broadcastPayload, peer *overlayPeer, sourcePeerID PeerID) (broadcastResult, error) {
	if !s.node.deduper.Mark(fingerprint, time.Now()) {
		s.node.noteBroadcastDrop(s.spec.Name, kind, "seen")
		return ignoredBroadcastResult(), nil
	}

	downloaded, _, cacheErr := s.node.decodedBroadcasts.get(kind, block)
	cached := cacheErr == nil
	if cached {
		s.node.noteBroadcast("decode_reused", s.spec.Name, kind, delivery)
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
				Str("block", storage.FormatBlockRef(block)).
				Str("kind", kind).
				Msg("dropping block candidate broadcast because payload decode failed")
			s.node.noteBroadcastDrop(s.spec.Name, kind, "decode_failed")
			return ignoredBroadcastResult(), nil
		}
		s.node.decodedBroadcasts.put(kind, block, downloaded, nil)
	}

	rebroadcast, err := s.inboundRebroadcastPayload(kind, payload, peer, delivery)
	if err != nil {
		s.node.deduper.Forget(fingerprint)
		return broadcastResult{}, err
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
	if !isMasterchainBlock(downloaded.ID) {
		s.node.observeBlockReceived(s.node.runCtx, downloaded, false)
	}

	accepted := acceptedBroadcast{
		fingerprint: fingerprint,
		deduped:     true,
		delivery:    delivery,
		block:       block.Copy(),
		rebroadcast: rebroadcast,
	}
	accepted.extraEvents = s.blockFinalityBroadcastEvents(delivery, trusted, assembled)

	return acceptedBroadcastResult(accepted), nil
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
			n.noteBroadcast(
				"accepted",
				accepted.rebroadcast.subscription.spec.Name,
				accepted.rebroadcast.kind,
				accepted.delivery,
			)
		}
		if !accepted.rebroadcast.skipOverlayRebroadcast {
			accepted.rebroadcast.subscription.enqueueRebroadcast(*accepted.rebroadcast)
		}
	}

	n.enqueueBlockOverlayFanout(accepted)
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
		n.observeBroadcastBlockReceived(n.runCtx, event.Downloaded, true)
	}
	if !n.eventQueue.Push(event) {
		return false
	}
	if event.Delivery == DeliveryPlumtree {
		n.plumtreeBroadcastLogOnce.Do(func() {
			n.log.Info().
				Str("overlay", event.Overlay).
				Str("kind", event.Kind).
				Str("block", storage.FormatBlockRef(event.Block)).
				Str("source_peer_id", event.SourcePeerID.String()).
				Msg("accepted first Plumtree broadcast")
		})
	}
	if !skipAcceptedMetric {
		n.noteBroadcast("accepted", event.Overlay, event.Kind, event.Delivery)
		n.noteBroadcastSourcePeer(event.Overlay, event.SourcePeerID)
	}
	return true
}

func (n *Node) enqueueBlockOverlayFanout(accepted acceptedBroadcast) {
	if accepted.rebroadcast == nil || accepted.block == nil {
		return
	}

	class, ok := blockOverlayFanoutClass(accepted.rebroadcast.kind)
	if !ok {
		return
	}

	customTargets := n.customOverlayFanoutTargets(accepted)
	fastSyncTarget := n.fastSyncFanoutTarget(accepted)
	if len(customTargets) == 0 && fastSyncTarget == nil {
		return
	}

	source := *accepted.rebroadcast
	if err := source.materializePayload(); err != nil {
		n.log.Debug().
			Err(err).
			Str("kind", source.kind).
			Msg("dropping overlay fanout because payload cannot be serialized")
		return
	}
	if len(source.payload) == 0 {
		return
	}

	key := blockOverlayFanoutKey(class, *accepted.block)
	if !n.overlayFanoutDeduper.Mark(key, time.Now()) {
		return
	}

	for _, sub := range customTargets {
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
	if fastSyncTarget != nil {
		n.sendFastSyncFanout(
			fastSyncTarget,
			source.kind,
			source.payload,
			*accepted.block,
		)
	}
}

func (n *Node) customOverlayFanoutTargets(accepted acceptedBroadcast) []*overlaySubscription {
	targets := make([]*overlaySubscription, 0)
	for _, sub := range n.subscriptionsSnapshot() {
		if sub == nil || sub == accepted.rebroadcast.subscription {
			continue
		}
		if !sub.spec.originatesLocalBroadcasts() || !sub.isActive() {
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

// fastSyncFanoutTarget picks the FastSync overlay to re-originate an accepted
// broadcast into. This is deliberately NOT what the reference node does: its
// receive path relays into custom overlays only, and its FastSync sends come
// exclusively from blocks the node produced itself. We have no block-origination
// API yet, so the only way this node can feed a FastSync overlay is to forward
// what it accepted elsewhere — a public-to-FastSync gateway. Revisit when a
// send API exists: with one, a validator should originate its own blocks here
// instead of re-emitting observed ones, which the FastSync relay already carries.
func (n *Node) fastSyncFanoutTarget(
	accepted acceptedBroadcast,
) *overlaySubscription {
	var sub *overlaySubscription
	if accepted.rebroadcast.kind == "tonNode.newShardBlockBroadcast" {
		sub = n.fastSyncSubscriptionForBlock(ton.BlockIDExt{
			Workchain: -1,
			Shard:     topShard,
		})
	} else {
		sub = n.fastSyncSubscriptionForBlock(*accepted.block)
	}
	if sub == nil ||
		sub == accepted.rebroadcast.subscription ||
		!sub.isActive() ||
		!sub.fastSync.spec.localValidator {
		return nil
	}
	return sub
}

func (n *Node) sendFastSyncFanout(
	sub *overlaySubscription,
	kind string,
	payload []byte,
	block ton.BlockIDExt,
) {
	switch kind {
	case "tonNode.newBlockCandidateBroadcast",
		"tonNode.newBlockCandidateBroadcastCompressed",
		"tonNode.newBlockCandidateBroadcastCompressedV2":
		if sub.fastSync.spec.plumtreeEnabled {
			n.originateFastSyncPlumtreeFEC(sub, kind, payload)
			return
		}
	case blockFinalityBroadcastKind:
		if !sub.fastSync.spec.plumtreeEnabled {
			return
		}

		broadcastID, err := finalityPlumtreeBroadcastID(block)
		if err != nil {
			sub.log.Debug().
				Err(err).
				Str("kind", kind).
				Msg("dropping FastSync finality fanout")
			return
		}
		n.originateFastSyncPlumtreeSimple(
			sub,
			kind,
			broadcastID,
			payload,
		)
		return
	}

	sub.enqueueRebroadcast(rebroadcastRequest{
		subscription: sub,
		kind:         kind,
		payload:      payload,
		local:        true,
	})
}

func (n *Node) originateFastSyncPlumtreeFEC(
	sub *overlaySubscription,
	kind string,
	payload []byte,
) {
	if !n.canAcceptBroadcast(kind, true) {
		n.noteBroadcastDrop(
			sub.spec.Name,
			kind,
			"broadcast_admission_closed",
		)
		return
	}
	if err := sub.plumtree.OriginateFEC(
		overlay.BroadcastFlagAnySender,
		payload,
	); err != nil {
		sub.log.Debug().
			Err(err).
			Str("kind", kind).
			Msg("failed to originate FastSync Plumtree FEC broadcast")
	}
}

func (n *Node) originateFastSyncPlumtreeSimple(
	sub *overlaySubscription,
	kind string,
	broadcastID [sha256.Size]byte,
	payload []byte,
) {
	if !n.canAcceptBroadcast(kind, true) {
		n.noteBroadcastDrop(
			sub.spec.Name,
			kind,
			"broadcast_admission_closed",
		)
		return
	}
	if err := sub.plumtree.OriginateSimple(
		overlay.BroadcastFlagAnySender,
		broadcastID,
		payload,
	); err != nil {
		sub.log.Debug().
			Err(err).
			Str("kind", kind).
			Msg("failed to originate FastSync Plumtree broadcast")
	}
}

func finalityPlumtreeBroadcastID(
	block ton.BlockIDExt,
) ([sha256.Size]byte, error) {
	hash, err := tl.Hash(FinalityBroadcastID{ID: block})
	if err != nil {
		return [sha256.Size]byte{}, err
	}

	var id [sha256.Size]byte
	copy(id[:], hash)
	return id, nil
}

func blockOverlayFanoutClass(kind string) (string, bool) {
	switch kind {
	case "tonNode.blockBroadcast",
		"tonNode.blockBroadcastCompressed",
		"tonNode.blockBroadcastCompressedV2":
		return "block", true
	case "tonNode.newBlockCandidateBroadcast",
		"tonNode.newBlockCandidateBroadcastCompressed",
		"tonNode.newBlockCandidateBroadcastCompressedV2":
		return "candidate", true
	case blockFinalityBroadcastKind:
		return "finality", true
	case "tonNode.newShardBlockBroadcast":
		return "shard-desc", true
	default:
		return "", false
	}
}

func blockOverlayFanoutKey(class string, block ton.BlockIDExt) string {
	return class + ":" + storage.FormatBlockRef(block)
}

func fastSyncBroadcastSupported(msg any) bool {
	switch msg.(type) {
	case tonnodeapi.BlockBroadcast,
		tonnodeapi.BlockBroadcastCompressed,
		tonnodeapi.BlockBroadcastCompressedV2,
		tonnodeapi.NewShardBlockBroadcast,
		tonnodeapi.NewBlockCandidateBroadcast,
		tonnodeapi.NewBlockCandidateBroadcastCompressed,
		tonnodeapi.NewBlockCandidateBroadcastCompressedV2,
		BlockFinalityBroadcast,
		OutMsgQueueProofBroadcast:
		return true
	default:
		return false
	}
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
	case OutMsgQueueProofBroadcast:
		return "tonNode.outMsgQueueProofBroadcast"
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
		n.noteBroadcastDrop(accepted.rebroadcast.subscription.spec.Name, accepted.rebroadcast.kind, "seen")
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
	return storage.BlockIDHashesKnown(block) && len(proof) > 0 && len(data) > 0
}

func validCompressedBroadcast(block ton.BlockIDExt, data []byte) bool {
	return storage.BlockIDHashesKnown(block) && len(data) > 0
}

func validBlockCandidateBroadcast(block ton.BlockIDExt, data []byte) bool {
	if !storage.BlockIDHashesKnown(block) || len(data) == 0 || len(data) > maxOverlayPayloadSize {
		return false
	}
	return bytes.Equal(hashSimpleBroadcastPayload(data), block.FileHash)
}
