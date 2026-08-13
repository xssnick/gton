package p2p

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/pierrec/lz4/v4"
	"github.com/xssnick/gton/service/blockproof"
	sharddomain "github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/adnl/keys"
	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	blockPublicationQueueMaxItems       = 256
	blockPublicationQueueMaxBytes       = int64(512 << 20)
	blockPublicationCertificateLifetime = time.Hour
	blockPublicationCertificateRefresh  = 5 * time.Minute

	blockBroadcastCompressedV2Kind        = "tonNode.blockBroadcastCompressedV2"
	blockCandidateBroadcastCompressedKind = "tonNode.newBlockCandidateBroadcastCompressed"
)

// AcceptedBlockPublication is an accepted block and its Simplex certificate.
// Public selects the scheduled-leader full-block publication; the certificate
// is published to every eligible finality overlay independently.
type AcceptedBlockPublication struct {
	Block             DownloadedBlock
	Signatures        *blockproof.ValidatorSignatureSet
	Public            bool
	CertificateSigner overlay.BroadcastSigner
}

// BlockCandidatePublication is the canonical block material needed to relay a
// candidate into the node's configured block overlays.
type BlockCandidatePublication struct {
	Block             ton.BlockIDExt
	BlockBOC          []byte
	CatchainSeqno     uint32
	ValidatorSetHash  uint32
	CertificateSigner overlay.BroadcastSigner
}

type blockPublicationKind uint8

const (
	blockPublicationAccepted blockPublicationKind = iota + 1
	blockPublicationCandidate
)

type blockPublicationRequest struct {
	kind      blockPublicationKind
	accepted  AcceptedBlockPublication
	candidate BlockCandidatePublication
}

type blockPublicationCertificateKey struct {
	overlay PeerID
	issuer  PeerID
}

type blockPublicationCertificate struct {
	value     overlay.Certificate
	refreshAt time.Time
}

// BlockBroadcasts owns the bounded, non-blocking publication boundary for one
// Node. A successful Try call transfers ownership of the request's immutable
// byte slices and referenced objects until the worker has processed it.
type BlockBroadcasts struct {
	node         *Node
	queue        *boundedQueue[blockPublicationRequest]
	certificates map[blockPublicationCertificateKey]blockPublicationCertificate

	producersMu sync.Mutex
	producers   uint32
}

type blockProducerLease struct {
	broadcasts *BlockBroadcasts
	once       sync.Once
}

func newBlockBroadcasts(node *Node) *BlockBroadcasts {
	return &BlockBroadcasts{
		node:         node,
		certificates: make(map[blockPublicationCertificateKey]blockPublicationCertificate),
		queue: newBoundedQueue(
			blockPublicationQueueMaxItems,
			blockPublicationQueueMaxBytes,
			blockPublicationRequestBytes,
		),
	}
}

// RegisterPlumtreeProducer marks every public shard overlay as an original
// sender while at least one local validator session is active. C++ derives the
// role from membership in the total validator sets, not from a particular
// shard assignment. The returned lease must be closed when the session stops;
// overlapping sessions are reference-counted across rotations.
func (b *BlockBroadcasts) RegisterPlumtreeProducer() io.Closer {
	b.producersMu.Lock()
	b.producers++
	b.producersMu.Unlock()
	b.applyProducerRoles()

	return &blockProducerLease{broadcasts: b}
}

func (l *blockProducerLease) Close() error {
	l.once.Do(func() {
		l.broadcasts.releasePlumtreeProducer()
	})
	return nil
}

func (b *BlockBroadcasts) releasePlumtreeProducer() {
	b.producersMu.Lock()
	if b.producers > 0 {
		b.producers--
	}
	b.producersMu.Unlock()
	b.applyProducerRoles()
}

func (b *BlockBroadcasts) applyProducerRoles() {
	for _, sub := range b.node.subscriptionsSnapshot() {
		b.applyProducerRole(sub)
	}
}

func (b *BlockBroadcasts) applyProducerRole(sub *overlaySubscription) {
	if !sub.spec.tracksPlumtreeProducerRole() || sub.plumtree == nil {
		return
	}

	b.producersMu.Lock()
	original := b.producers > 0
	b.producersMu.Unlock()

	sub.plumtree.engine.SetOriginalSender(original)
}

// BlockBroadcasts returns the node-owned block publication capability.
func (n *Node) BlockBroadcasts() *BlockBroadcasts {
	return n.blockBroadcasts
}

// TryPublishAccepted transfers an accepted block to the publication worker
// without waiting. It returns false when the bounded queue is full or closed.
func (b *BlockBroadcasts) TryPublishAccepted(publication AcceptedBlockPublication) bool {
	return b.queue.Push(blockPublicationRequest{
		kind:     blockPublicationAccepted,
		accepted: publication,
	})
}

// TryRelayCandidate transfers a candidate to the publication worker without
// waiting. Local and private-overlay candidates use this same deduplicated
// path. It returns false when the bounded queue is full or closed.
func (b *BlockBroadcasts) TryRelayCandidate(publication BlockCandidatePublication) bool {
	return b.queue.Push(blockPublicationRequest{
		kind:      blockPublicationCandidate,
		candidate: publication,
	})
}

func blockPublicationRequestBytes(request blockPublicationRequest) int64 {
	const overhead = int64(512)

	switch request.kind {
	case blockPublicationAccepted:
		size := overhead + int64(len(request.accepted.Block.BlockBOC)) +
			int64(len(request.accepted.Block.ProofBOC))
		if request.accepted.Signatures != nil {
			size += int64(request.accepted.Signatures.SignatureCount()) * 128
		}
		return size
	case blockPublicationCandidate:
		return overhead + int64(len(request.candidate.BlockBOC))
	default:
		return overhead
	}
}

func (b *BlockBroadcasts) run(ctx context.Context) {
	for {
		request, ok := b.queue.Pop(ctx)
		if !ok {
			return
		}

		switch request.kind {
		case blockPublicationAccepted:
			b.publishAccepted(request.accepted)
		case blockPublicationCandidate:
			b.publishCandidate(request.candidate)
		}
	}
}

func (b *BlockBroadcasts) publishAccepted(publication AcceptedBlockPublication) {
	block := publication.Block.ID
	signatureSet, err := acceptedBlockBroadcastSignatureSet(publication)
	if err != nil {
		b.logDrop(block, "accepted", err)
		return
	}
	customTargets := b.customTargets(block)

	finalityPayload, finalityErr := tl.Serialize(BlockFinalityBroadcast{
		ID:           block,
		SignatureSet: signatureSet,
	}, true)
	if finalityErr != nil {
		b.logDrop(block, "finality", fmt.Errorf("serialize finality broadcast: %w", finalityErr))
	} else if b.node.overlayFanoutDeduper.Mark(
		blockOverlayFanoutKey("finality", block),
		time.Now(),
	) {
		b.publishFinality(
			block,
			finalityPayload,
			customTargets,
			publication.CertificateSigner,
		)
	}

	if !publication.Public && len(customTargets) == 0 {
		return
	}
	now := time.Now()
	if len(customTargets) == 0 || !b.node.overlayFanoutDeduper.Mark(
		blockOverlayRouteFanoutKey(blockOverlayFanoutRouteCustom, "block", block),
		now,
	) {
		customTargets = nil
	}

	var publicTarget *overlaySubscription
	var publicCertificate any
	if publication.Public && publication.CertificateSigner != nil {
		publicTarget, err = b.node.subscriptionForBlock(block)
		if err != nil {
			b.logDrop(block, "block.public", err)
			publicTarget = nil
		} else if publicCertificate, err = b.publicationCertificate(
			publicTarget,
			publication.CertificateSigner,
			now,
		); err != nil {
			b.logDrop(block, "block.public.certificate", err)
			publicTarget = nil
		} else if !b.node.overlayFanoutDeduper.Mark(
			blockOverlayRouteFanoutKey(blockOverlayFanoutRoutePublic, "block", block),
			now,
		) {
			publicTarget = nil
		}
	}
	if len(customTargets) == 0 && publicTarget == nil {
		return
	}

	blockPayload, err := serializeAcceptedBlockBroadcast(publication, signatureSet)
	if err != nil {
		b.logDrop(block, "block", err)
		return
	}
	b.publishFullBlock(blockPayload, customTargets, publicTarget, publicCertificate)
}

func (b *BlockBroadcasts) publishCandidate(publication BlockCandidatePublication) {
	payload, err := serializeBlockCandidateBroadcast(publication)
	if err != nil {
		b.logDrop(publication.Block, "candidate", err)
		return
	}
	now := time.Now()
	customTargets := b.customTargets(publication.Block)
	if len(customTargets) > 0 && b.node.overlayFanoutDeduper.Mark(
		blockOverlayRouteFanoutKey(blockOverlayFanoutRouteCustom, "candidate", publication.Block),
		now,
	) {
		for _, sub := range customTargets {
			enqueueLocalBlockFEC(sub, blockCandidateBroadcastCompressedKind, payload)
		}
	}

	if publication.CertificateSigner != nil {
		public, publicErr := b.node.subscriptionForBlock(publication.Block)
		if publicErr != nil {
			b.logDrop(publication.Block, "candidate.public", publicErr)
		} else if certificate, certErr := b.publicationCertificate(
			public,
			publication.CertificateSigner,
			now,
		); certErr != nil {
			b.logDrop(publication.Block, "candidate.public.certificate", certErr)
		} else if b.node.overlayFanoutDeduper.Mark(
			blockOverlayRouteFanoutKey(blockOverlayFanoutRoutePublic, "candidate", publication.Block),
			now,
		) {
			b.originatePlumtreeFEC(public, blockCandidateBroadcastCompressedKind, payload, certificate)
		}

		fastSync, fastSyncErr := b.fastSyncPlumtreeTarget(publication.Block)
		if fastSyncErr != nil {
			b.logDrop(publication.Block, "candidate.fast_sync", fastSyncErr)
		} else if fastSync != nil {
			certificate, certErr := b.publicationCertificate(
				fastSync,
				publication.CertificateSigner,
				now,
			)
			if certErr != nil {
				b.logDrop(publication.Block, "candidate.fast_sync.certificate", certErr)
			} else if b.node.overlayFanoutDeduper.Mark(
				blockOverlayRouteFanoutKey(blockOverlayFanoutRouteFastSync, "candidate", publication.Block),
				now,
			) {
				b.originatePlumtreeFEC(fastSync, blockCandidateBroadcastCompressedKind, payload, certificate)
			}
		}
	}
}

func acceptedBlockBroadcastSignatureSet(
	publication AcceptedBlockPublication,
) (tonnodeapi.SignatureSetSimplex, error) {
	if err := validateBlockPublicationID(publication.Block.ID); err != nil {
		return tonnodeapi.SignatureSetSimplex{}, err
	}
	if publication.Signatures == nil {
		return tonnodeapi.SignatureSetSimplex{}, errors.New("accepted block has no validator signatures")
	}
	signatureSet, err := publication.Signatures.SimplexBroadcastSignatureSet()
	if err != nil {
		return tonnodeapi.SignatureSetSimplex{}, fmt.Errorf("build block broadcast signature set: %w", err)
	}
	return signatureSet, nil
}

func serializeAcceptedBlockBroadcast(
	publication AcceptedBlockPublication,
	signatureSet tonnodeapi.SignatureSetSimplex,
) ([]byte, error) {
	block := publication.Block
	if block.Block == nil {
		return nil, errors.New("accepted block root is nil")
	}
	if len(block.BlockBOC) == 0 {
		return nil, errors.New("accepted block BOC is empty")
	}
	if len(block.ProofBOC) == 0 {
		return nil, errors.New("accepted block proof BOC is empty")
	}
	if !bytes.Equal(block.Block.Hash(), block.ID.RootHash) {
		return nil, errors.New("accepted block root hash does not match block ID")
	}
	fileHash := sha256.Sum256(block.BlockBOC)
	if !bytes.Equal(fileHash[:], block.ID.FileHash) {
		return nil, errors.New("accepted block file hash does not match block ID")
	}

	compressed, err := cell.CompressBOC(
		[]*cell.Cell{block.Block},
		cell.CompressionImprovedStructureLZ4,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("compress accepted block: %w", err)
	}
	payload, err := tl.Serialize(tonnodeapi.BlockBroadcastCompressedV2{
		ID:             block.ID,
		SignatureSet:   signatureSet,
		Flags:          0,
		Proof:          block.ProofBOC,
		DataCompressed: compressed,
	}, true)
	if err != nil {
		return nil, fmt.Errorf("serialize accepted block broadcast: %w", err)
	}
	if len(payload) > maxOverlayPayloadSize {
		return nil, fmt.Errorf("accepted block broadcast is %d bytes, limit is %d", len(payload), maxOverlayPayloadSize)
	}
	return payload, nil
}

func serializeBlockCandidateBroadcast(publication BlockCandidatePublication) ([]byte, error) {
	if err := validateBlockPublicationID(publication.Block); err != nil {
		return nil, err
	}
	if len(publication.BlockBOC) == 0 {
		return nil, errors.New("candidate block BOC is empty")
	}
	if len(publication.BlockBOC) > maxDecompressedBlockSize {
		return nil, fmt.Errorf("candidate block BOC is %d bytes, limit is %d", len(publication.BlockBOC), maxDecompressedBlockSize)
	}
	fileHash := sha256.Sum256(publication.BlockBOC)
	if !bytes.Equal(fileHash[:], publication.Block.FileHash) {
		return nil, errors.New("candidate block file hash does not match block ID")
	}

	roots, modeTwoBOC, err := cell.ReserializeBOC(
		publication.BlockBOC,
		cell.BOCSerializeOptions{WithCRC32C: true},
	)
	if err != nil {
		return nil, fmt.Errorf("reserialize candidate block in mode 2: %w", err)
	}
	if len(roots) != 1 {
		return nil, fmt.Errorf("candidate block BOC has %d roots, want 1", len(roots))
	}
	if !bytes.Equal(roots[0].Hash(), publication.Block.RootHash) {
		return nil, errors.New("candidate block root hash does not match block ID")
	}

	compressed := make([]byte, lz4.CompressBlockBound(len(modeTwoBOC)))
	compressedSize, err := lz4.CompressBlock(modeTwoBOC, compressed, nil)
	if err != nil {
		return nil, fmt.Errorf("compress candidate block: %w", err)
	}
	if compressedSize == 0 {
		return nil, errors.New("compress candidate block: empty result")
	}

	payload, err := tl.Serialize(tonnodeapi.NewBlockCandidateBroadcastCompressed{
		ID:               publication.Block,
		CatchainSeqno:    int32(publication.CatchainSeqno),
		ValidatorSetHash: int32(publication.ValidatorSetHash),
		CollatorSignature: tonnodeapi.BlockSignature{
			Who: make([]byte, sha256.Size),
		},
		Flags:      0,
		Compressed: compressed[:compressedSize],
	}, true)
	if err != nil {
		return nil, fmt.Errorf("serialize candidate broadcast: %w", err)
	}
	if len(payload) > maxOverlayPayloadSize {
		return nil, fmt.Errorf("candidate broadcast is %d bytes, limit is %d", len(payload), maxOverlayPayloadSize)
	}
	return payload, nil
}

func validateBlockPublicationID(block ton.BlockIDExt) error {
	if err := storage.ValidateBlockIDHashes(block); err != nil {
		return err
	}
	if err := sharddomain.Validate(block.Shard); err != nil {
		return err
	}
	return nil
}

func (b *BlockBroadcasts) publishFullBlock(
	payload []byte,
	customTargets []*overlaySubscription,
	publicTarget *overlaySubscription,
	publicCertificate any,
) {
	for _, sub := range customTargets {
		enqueueLocalBlockFEC(sub, blockBroadcastCompressedV2Kind, payload)
	}
	if publicTarget != nil {
		enqueueLocalBlockFECWithCertificate(
			publicTarget,
			blockBroadcastCompressedV2Kind,
			payload,
			publicCertificate,
		)
	}
}

func (b *BlockBroadcasts) publishFinality(
	block ton.BlockIDExt,
	payload []byte,
	customTargets []*overlaySubscription,
	certificateSigner overlay.BroadcastSigner,
) {
	for _, sub := range customTargets {
		enqueueLocalBlockFEC(sub, blockFinalityBroadcastKind, payload)
	}
	if certificateSigner == nil {
		return
	}

	broadcastID, err := finalityPlumtreeBroadcastID(block)
	if err != nil {
		b.logDrop(block, "finality.id", err)
		return
	}
	public, err := b.node.subscriptionForBlock(block)
	if err != nil {
		b.logDrop(block, "finality.public", err)
	} else if certificate, certErr := b.publicationCertificate(
		public,
		certificateSigner,
		time.Now(),
	); certErr != nil {
		b.logDrop(block, "finality.public.certificate", certErr)
	} else {
		b.originatePlumtreeSimple(
			public,
			blockFinalityBroadcastKind,
			broadcastID,
			payload,
			certificate,
		)
	}

	fastSync, err := b.fastSyncPlumtreeTarget(block)
	if err != nil {
		b.logDrop(block, "finality.fast_sync", err)
	} else if fastSync != nil {
		certificate, certErr := b.publicationCertificate(
			fastSync,
			certificateSigner,
			time.Now(),
		)
		if certErr != nil {
			b.logDrop(block, "finality.fast_sync.certificate", certErr)
		} else {
			b.originatePlumtreeSimple(
				fastSync,
				blockFinalityBroadcastKind,
				broadcastID,
				payload,
				certificate,
			)
		}
	}
}

func (b *BlockBroadcasts) customTargets(block ton.BlockIDExt) []*overlaySubscription {
	targets := make([]*overlaySubscription, 0)
	for _, sub := range b.node.subscriptionsSnapshot() {
		if !sub.spec.originatesLocalBroadcasts() || !sub.isActive() {
			continue
		}
		if _, ok := sub.spec.BlockSenders[b.node.localID]; !ok {
			continue
		}
		if !sub.spec.sendsShard(block.Workchain, block.Shard) {
			continue
		}
		targets = append(targets, sub)
	}
	return targets
}

func (b *BlockBroadcasts) fastSyncPlumtreeTarget(
	block ton.BlockIDExt,
) (*overlaySubscription, error) {
	sub, err := b.node.fastSyncSubscriptionForBlock(block)
	if err != nil {
		return nil, err
	}
	if sub == nil || !sub.isActive() || sub.fastSync == nil ||
		!sub.fastSync.spec.localValidator || !sub.fastSync.spec.plumtreeEnabled ||
		sub.plumtree == nil {
		return nil, nil
	}
	return sub, nil
}

func enqueueLocalBlockFEC(sub *overlaySubscription, kind string, payload []byte) bool {
	return enqueueLocalBlockFECWithCertificate(sub, kind, payload, nil)
}

func enqueueLocalBlockFECWithCertificate(
	sub *overlaySubscription,
	kind string,
	payload []byte,
	certificate any,
) bool {
	return sub.enqueueRebroadcast(rebroadcastRequest{
		subscription:   sub,
		kind:           kind,
		payload:        payload,
		local:          true,
		forceLegacyFEC: true,
		certificate:    certificate,
	})
}

func (b *BlockBroadcasts) originatePlumtreeFEC(
	sub *overlaySubscription,
	kind string,
	payload []byte,
	certificate any,
) {
	if !b.node.canAcceptBroadcast(kind, true) {
		b.node.noteBroadcastDrop(sub.spec.Name, kind, "broadcast_admission_closed")
		return
	}
	if sub.plumtree == nil {
		b.logDrop(ton.BlockIDExt{}, kind, errPlumtreeDisabled)
		return
	}
	if err := sub.plumtree.originateFECWithCertificate(
		certificate,
		overlay.BroadcastFlagAnySender,
		payload,
	); err != nil {
		sub.log.Debug().Err(err).Str("kind", kind).Msg("failed to originate local Plumtree FEC broadcast")
	}
}

func (b *BlockBroadcasts) originatePlumtreeSimple(
	sub *overlaySubscription,
	kind string,
	broadcastID [sha256.Size]byte,
	payload []byte,
	certificate any,
) {
	if !b.node.canAcceptBroadcast(kind, true) {
		b.node.noteBroadcastDrop(sub.spec.Name, kind, "broadcast_admission_closed")
		return
	}
	if sub.plumtree == nil {
		b.logDrop(ton.BlockIDExt{}, kind, errPlumtreeDisabled)
		return
	}
	if err := sub.plumtree.originateSimpleWithCertificate(
		certificate,
		overlay.BroadcastFlagAnySender,
		broadcastID,
		payload,
	); err != nil {
		sub.log.Debug().Err(err).Str("kind", kind).Msg("failed to originate local Plumtree broadcast")
	}
}

func (b *BlockBroadcasts) publicationCertificate(
	sub *overlaySubscription,
	signer overlay.BroadcastSigner,
	now time.Time,
) (overlay.Certificate, error) {
	overlayID, err := NewPeerID(sub.spec.ShortID)
	if err != nil {
		return overlay.Certificate{}, fmt.Errorf("invalid publication overlay ID: %w", err)
	}
	publicKey := signer.PublicKey()
	issuerID, err := peerIDFromED25519PublicKey(publicKey)
	if err != nil {
		return overlay.Certificate{}, err
	}
	key := blockPublicationCertificateKey{overlay: overlayID, issuer: issuerID}
	if cached, ok := b.certificates[key]; ok && now.Before(cached.refreshAt) {
		return cached.value, nil
	}
	for cachedKey, cached := range b.certificates {
		if now.Unix() > int64(cached.value.ExpireAt) {
			delete(b.certificates, cachedKey)
		}
	}

	expireAt := uint32(now.Add(blockPublicationCertificateLifetime).Unix())
	// TON canonicalizes Trusted|AllowFEC certificates to the legacy constructor
	// and signs CertificateId even when a V2 value was supplied on the wire.
	toSign, err := tl.Serialize(overlay.CertificateId{
		OverlayID: overlayID[:],
		Node:      b.node.localID[:],
		ExpireAt:  expireAt,
		MaxSize:   maxOverlayPayloadSize,
	}, true)
	if err != nil {
		return overlay.Certificate{}, fmt.Errorf("serialize publication certificate: %w", err)
	}
	signature, err := signer.Sign(toSign)
	if err != nil {
		return overlay.Certificate{}, fmt.Errorf("sign publication certificate: %w", err)
	}
	if len(signature) != ed25519.SignatureSize {
		return overlay.Certificate{}, fmt.Errorf(
			"publication certificate signature is %d bytes, want %d",
			len(signature),
			ed25519.SignatureSize,
		)
	}
	certificate := overlay.Certificate{
		IssuedBy: keys.PublicKeyED25519{
			Key: append(ed25519.PublicKey(nil), publicKey...),
		},
		ExpireAt:  expireAt,
		MaxSize:   maxOverlayPayloadSize,
		Signature: append([]byte(nil), signature...),
	}
	b.certificates[key] = blockPublicationCertificate{
		value:     certificate,
		refreshAt: time.Unix(int64(expireAt), 0).Add(-blockPublicationCertificateRefresh),
	}
	return certificate, nil
}

func (b *BlockBroadcasts) logDrop(block ton.BlockIDExt, publication string, err error) {
	event := b.node.log.Warn().Err(err).Str("publication", publication)
	if storage.BlockIDHashesKnown(block) {
		event = event.Str("block", storage.FormatBlockRef(block))
	}
	event.Msg("dropping local block publication")
}
