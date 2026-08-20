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
	trustedConsensusCandidateKind         = "consensus.blockSyncCandidate"
)

// BlockBroadcastMode selects the full-node overlay routes for one publication.
// Its bit values match fullnode::FullNode::broadcast_mode_* in the C++ node.
type BlockBroadcastMode uint8

const (
	BlockBroadcastModePublic BlockBroadcastMode = 1 << iota
	BlockBroadcastModeFastSync
	BlockBroadcastModeCustom
)

const blockBroadcastModeAll = BlockBroadcastModePublic |
	BlockBroadcastModeFastSync |
	BlockBroadcastModeCustom

func (m BlockBroadcastMode) includes(route BlockBroadcastMode) bool {
	return m&route != 0
}

func (m BlockBroadcastMode) validate() error {
	if m & ^blockBroadcastModeAll != 0 {
		return fmt.Errorf("unknown block broadcast mode bits %02x", uint8(m&^blockBroadcastModeAll))
	}
	return nil
}

// AcceptedBlockPublication is an accepted block and its Simplex certificate.
// BlockMode and FinalityMode are independent because protocols 0 and 1 publish
// legacy full blocks without a standalone finality broadcast. Plumtree selects
// the public and FastSync finality transport explicitly; full blocks always use
// the conventional FEC transport selected by their route bits.
type AcceptedBlockPublication struct {
	Block             DownloadedBlock
	Signatures        *blockproof.ValidatorSignatureSet
	BlockMode         BlockBroadcastMode
	FinalityMode      BlockBroadcastMode
	Plumtree          bool
	CertificateSigner overlay.BroadcastSigner
}

// BlockCandidatePublication is the canonical block material needed to relay a
// candidate into the node's configured block overlays. Plumtree explicitly
// selects the public and FastSync candidate transport. Custom candidates always
// use conventional FEC, and a legacy candidate cannot use the public route.
type BlockCandidatePublication struct {
	Block             ton.BlockIDExt
	BlockBOC          []byte
	CatchainSeqno     uint32
	ValidatorSetHash  uint32
	Mode              BlockBroadcastMode
	Plumtree          bool
	CertificateSigner overlay.BroadcastSigner
}

// TrustedBlockCandidate is block data whose consensus-overlay source and
// candidate signature have already been authenticated. A prepared instance
// carries the decoder's immutable root/BOC/hash binding; a raw fallback keeps
// compatibility with receiver implementations outside the built-in codec and
// is fully parsed and verified by the cache worker.
type TrustedBlockCandidate struct {
	id       ton.BlockIDExt
	root     *cell.Cell
	blockBOC []byte
}

func NewTrustedBlockCandidate(prepared *storage.PreparedBlockCandidate) (TrustedBlockCandidate, error) {
	if prepared == nil {
		return TrustedBlockCandidate{}, errors.New("trusted block candidate is absent")
	}

	return TrustedBlockCandidate{
		id:       prepared.ID(),
		root:     prepared.Root(),
		blockBOC: prepared.BlockBOC(),
	}, nil
}

func NewTrustedRawBlockCandidate(id ton.BlockIDExt, blockBOC []byte) (TrustedBlockCandidate, error) {
	if len(blockBOC) == 0 {
		return TrustedBlockCandidate{}, errors.New("trusted raw block candidate is empty")
	}

	return TrustedBlockCandidate{id: *id.Copy(), blockBOC: bytes.Clone(blockBOC)}, nil
}

type blockPublicationKind uint8

const (
	blockPublicationAccepted blockPublicationKind = iota + 1
	blockPublicationCandidate
	blockCacheTrustedCandidate
)

type blockPublicationRequest struct {
	kind             blockPublicationKind
	accepted         AcceptedBlockPublication
	candidate        BlockCandidatePublication
	trustedCandidate TrustedBlockCandidate
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

// TryRelayCandidate transfers a locally generated candidate to the publication
// worker without waiting. Received consensus candidates use TryCacheCandidate
// instead and are not re-originated into full-node overlays. It returns false
// when the bounded queue is full or closed.
func (b *BlockBroadcasts) TryRelayCandidate(publication BlockCandidatePublication) bool {
	return b.queue.Push(blockPublicationRequest{
		kind:      blockPublicationCandidate,
		candidate: publication,
	})
}

// TryCacheCandidate transfers an authenticated consensus candidate to the
// bounded cache worker without publishing it into any full-node overlay. It
// returns false when the queue is full or closed.
func (b *BlockBroadcasts) TryCacheCandidate(candidate TrustedBlockCandidate) bool {
	if len(candidate.blockBOC) == 0 {
		return false
	}

	return b.queue.Push(blockPublicationRequest{
		kind:             blockCacheTrustedCandidate,
		trustedCandidate: candidate,
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
	case blockCacheTrustedCandidate:
		return overhead + int64(len(request.trustedCandidate.blockBOC))
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
		case blockCacheTrustedCandidate:
			b.cacheTrustedCandidate(request.trustedCandidate)
		}
	}
}

func (b *BlockBroadcasts) publishAccepted(publication AcceptedBlockPublication) {
	if err := validateAcceptedBlockPublication(publication); err != nil {
		b.logDrop(publication.Block.ID, "accepted.mode", err)
		return
	}
	if publication.BlockMode == 0 && publication.FinalityMode == 0 {
		return
	}

	block := publication.Block.ID
	signatureSet, err := acceptedBlockBroadcastSignatureSet(publication)
	if err != nil {
		b.logDrop(block, "accepted", err)
		return
	}

	var customTargets []*overlaySubscription
	if publication.BlockMode.includes(BlockBroadcastModeCustom) ||
		publication.FinalityMode.includes(BlockBroadcastModeCustom) {
		customTargets = b.customTargets(block)
	}
	if publication.FinalityMode != 0 {
		finalityPayload, finalityErr := tl.Serialize(BlockFinalityBroadcast{
			ID:           block,
			SignatureSet: signatureSet,
		}, true)
		if finalityErr != nil {
			b.logDrop(block, "finality", fmt.Errorf("serialize finality broadcast: %w", finalityErr))
		} else if b.node.overlayFanoutDeduper.Mark(
			blockPublicationFanoutKey("finality", block),
			time.Now(),
		) {
			b.publishFinality(
				block,
				finalityPayload,
				publication.FinalityMode,
				customTargets,
				publication.Plumtree,
				publication.CertificateSigner,
			)
		}
	}

	now := time.Now()
	customBlockTargets := customTargets
	if !publication.BlockMode.includes(BlockBroadcastModeCustom) ||
		len(customBlockTargets) == 0 || !b.node.overlayFanoutDeduper.Mark(
		blockPublicationRouteFanoutKey(
			blockOverlayFanoutRouteCustom,
			"block",
			block,
			publication.Plumtree,
		),
		now,
	) {
		customBlockTargets = nil
	}

	var publicTarget *overlaySubscription
	var publicCertificate any
	if publication.BlockMode.includes(BlockBroadcastModePublic) {
		publicTarget, err = b.node.subscriptionForBlock(block)
		if err != nil {
			b.logDrop(block, "block.public", err)
			publicTarget = nil
		} else if publicCertificate, err = b.optionalPublicationCertificate(
			publicTarget,
			publication.CertificateSigner,
			now,
		); err != nil {
			b.logDrop(block, "block.public.certificate", err)
			publicTarget = nil
		} else if !b.node.overlayFanoutDeduper.Mark(
			blockPublicationRouteFanoutKey(
				blockOverlayFanoutRoutePublic,
				"block",
				block,
				publication.Plumtree,
			),
			now,
		) {
			publicTarget = nil
		}
	}

	var fastSyncTarget *overlaySubscription
	if publication.BlockMode.includes(BlockBroadcastModeFastSync) {
		fastSyncTarget, err = b.fastSyncPublicationTarget(block, publication.Plumtree)
		if err != nil {
			b.logDrop(block, "block.fast_sync", err)
			fastSyncTarget = nil
		} else if fastSyncTarget != nil && !b.node.overlayFanoutDeduper.Mark(
			blockPublicationRouteFanoutKey(
				blockOverlayFanoutRouteFastSync,
				"block",
				block,
				publication.Plumtree,
			),
			now,
		) {
			fastSyncTarget = nil
		}
	}
	if len(customBlockTargets) == 0 && publicTarget == nil && fastSyncTarget == nil {
		return
	}

	blockPayload, err := serializeAcceptedBlockBroadcast(publication, signatureSet)
	if err != nil {
		b.logDrop(block, "block", err)
		return
	}
	b.publishFullBlock(
		blockPayload,
		customBlockTargets,
		publicTarget,
		publicCertificate,
		fastSyncTarget,
	)
}

func (b *BlockBroadcasts) publishCandidate(publication BlockCandidatePublication) {
	if err := validateBlockCandidatePublication(publication); err != nil {
		b.logDrop(publication.Block, "candidate.mode", err)
		return
	}
	if publication.Mode == 0 {
		return
	}

	payload, err := serializeBlockCandidateBroadcast(publication)
	if err != nil {
		b.logDrop(publication.Block, "candidate", err)
		return
	}
	now := time.Now()
	if publication.Mode.includes(BlockBroadcastModeCustom) {
		customTargets := b.customTargets(publication.Block)
		if len(customTargets) > 0 && b.node.overlayFanoutDeduper.Mark(
			blockPublicationRouteFanoutKey(
				blockOverlayFanoutRouteCustom,
				"candidate",
				publication.Block,
				publication.Plumtree,
			),
			now,
		) {
			for _, sub := range customTargets {
				enqueueLocalBlockFEC(sub, blockCandidateBroadcastCompressedKind, payload)
			}
		}
	}

	if publication.Mode.includes(BlockBroadcastModePublic) {
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
			blockPublicationRouteFanoutKey(
				blockOverlayFanoutRoutePublic,
				"candidate",
				publication.Block,
				publication.Plumtree,
			),
			now,
		) {
			b.originatePlumtreeFEC(public, blockCandidateBroadcastCompressedKind, payload, certificate)
		}
	}

	if publication.Mode.includes(BlockBroadcastModeFastSync) {
		fastSync, fastSyncErr := b.fastSyncPublicationTarget(publication.Block, publication.Plumtree)
		if fastSyncErr != nil {
			b.logDrop(publication.Block, "candidate.fast_sync", fastSyncErr)
			return
		}
		if fastSync == nil {
			return
		}

		if !publication.Plumtree {
			if !b.node.overlayFanoutDeduper.Mark(
				blockPublicationRouteFanoutKey(
					blockOverlayFanoutRouteFastSync,
					"candidate",
					publication.Block,
					publication.Plumtree,
				),
				now,
			) {
				return
			}
			enqueueLocalBlockFEC(fastSync, blockCandidateBroadcastCompressedKind, payload)
			return
		}

		certificate, certErr := b.publicationCertificate(
			fastSync,
			publication.CertificateSigner,
			now,
		)
		if certErr != nil {
			b.logDrop(publication.Block, "candidate.fast_sync.certificate", certErr)
			return
		}
		if !b.node.overlayFanoutDeduper.Mark(
			blockPublicationRouteFanoutKey(
				blockOverlayFanoutRouteFastSync,
				"candidate",
				publication.Block,
				publication.Plumtree,
			),
			now,
		) {
			return
		}
		b.originatePlumtreeFEC(fastSync, blockCandidateBroadcastCompressedKind, payload, certificate)
	}
}

func blockPublicationRouteFanoutKey(
	route string,
	class string,
	block ton.BlockIDExt,
	plumtree bool,
) string {
	if !plumtree && class == "candidate" &&
		(route == blockOverlayFanoutRouteCustom || route == blockOverlayFanoutRouteFastSync) {
		class = "block"
	}

	return blockPublicationFanoutKey(route+":"+class, block)
}

func blockPublicationFanoutKey(class string, block ton.BlockIDExt) string {
	return blockOverlayFanoutKey(class, block) + ":" +
		string(block.RootHash) + string(block.FileHash)
}

func (b *BlockBroadcasts) cacheTrustedCandidate(candidate TrustedBlockCandidate) {
	if len(candidate.blockBOC) == 0 {
		b.node.log.Warn().Msg("dropping absent trusted consensus candidate")
		return
	}
	id := candidate.id
	var downloaded *DownloadedBlock
	var err error
	if candidate.root != nil {
		downloaded, err = newParsedBlockCandidateBroadcast(
			trustedConsensusCandidateKind,
			id,
			candidate.blockBOC,
			candidate.root,
		)
	} else {
		downloaded, err = decodeRawBlockCandidateBroadcast(
			trustedConsensusCandidateKind,
			id,
			candidate.blockBOC,
		)
	}
	if err != nil {
		b.node.log.Warn().
			Err(err).
			Str("block", storage.FormatBlockRef(id)).
			Msg("dropping trusted consensus candidate")
		return
	}

	assembled, finalityErr := b.node.rememberBlockFinalityCandidate(downloaded)
	b.node.rememberShardBlockCandidate(downloaded)
	b.node.publishNonfinalDownloadedBlock(downloaded, storage.LiveBlockNonfinalCandidate)
	if !isMasterchainBlock(downloaded.ID) {
		b.node.observeBlockReceived(b.node.runCtx, downloaded, false)
	}
	if finalityErr != nil {
		return
	}

	for i := range assembled {
		block := assembled[i]
		b.node.acceptBroadcastEvent(BroadcastEvent{
			Overlay:      trustedConsensusCandidateKind,
			Kind:         blockFinalityBroadcastKind,
			Delivery:     DeliveryFEC,
			Trusted:      true,
			Block:        block.ID,
			Downloaded:   &block,
			SourcePeerID: block.SourcePeerID,
			ReceivedAt:   time.Now(),
		}, true)
	}
}

func validateAcceptedBlockPublication(publication AcceptedBlockPublication) error {
	if err := publication.BlockMode.validate(); err != nil {
		return fmt.Errorf("block mode: %w", err)
	}
	if err := publication.FinalityMode.validate(); err != nil {
		return fmt.Errorf("finality mode: %w", err)
	}

	plumtreeFinality := publication.FinalityMode &
		(BlockBroadcastModePublic | BlockBroadcastModeFastSync)
	if plumtreeFinality != 0 && !publication.Plumtree {
		return errors.New("public and FastSync finality routes require Plumtree")
	}
	if plumtreeFinality != 0 && publication.CertificateSigner == nil {
		return errors.New("Plumtree finality routes require a certificate signer")
	}

	return nil
}

func validateBlockCandidatePublication(publication BlockCandidatePublication) error {
	if err := publication.Mode.validate(); err != nil {
		return err
	}
	if publication.Mode.includes(BlockBroadcastModePublic) && !publication.Plumtree {
		return errors.New("public candidate route requires Plumtree")
	}
	plumtreeRoutes := publication.Mode &
		(BlockBroadcastModePublic | BlockBroadcastModeFastSync)
	if publication.Plumtree && plumtreeRoutes != 0 && publication.CertificateSigner == nil {
		return errors.New("Plumtree candidate routes require a certificate signer")
	}

	return nil
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
	fastSyncTarget *overlaySubscription,
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
	if fastSyncTarget != nil {
		enqueueLocalBlockFEC(fastSyncTarget, blockBroadcastCompressedV2Kind, payload)
	}
}

func (b *BlockBroadcasts) publishFinality(
	block ton.BlockIDExt,
	payload []byte,
	mode BlockBroadcastMode,
	customTargets []*overlaySubscription,
	plumtree bool,
	certificateSigner overlay.BroadcastSigner,
) {
	if mode.includes(BlockBroadcastModeCustom) {
		for _, sub := range customTargets {
			enqueueLocalBlockFEC(sub, blockFinalityBroadcastKind, payload)
		}
	}
	if !plumtree || mode&(BlockBroadcastModePublic|BlockBroadcastModeFastSync) == 0 {
		return
	}

	broadcastID, err := finalityPlumtreeBroadcastID(block)
	if err != nil {
		b.logDrop(block, "finality.id", err)
		return
	}
	if mode.includes(BlockBroadcastModePublic) {
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
	}

	if mode.includes(BlockBroadcastModeFastSync) {
		fastSync, err := b.fastSyncPublicationTarget(block, true)
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

func (b *BlockBroadcasts) fastSyncPublicationTarget(
	block ton.BlockIDExt,
	plumtree bool,
) (*overlaySubscription, error) {
	sub, err := b.node.fastSyncSubscriptionForBlock(block)
	if err != nil {
		return nil, err
	}
	if sub == nil || !sub.isActive() || sub.fastSync == nil ||
		!sub.fastSync.spec.localValidator {
		return nil, nil
	}
	if sub.fastSync.spec.plumtreeEnabled != plumtree {
		return nil, fmt.Errorf(
			"FastSync overlay Plumtree setting is %t, publication requires %t",
			sub.fastSync.spec.plumtreeEnabled,
			plumtree,
		)
	}
	if plumtree && sub.plumtree == nil {
		return nil, errors.New("FastSync Plumtree runtime is unavailable")
	}
	return sub, nil
}

func (b *BlockBroadcasts) optionalPublicationCertificate(
	sub *overlaySubscription,
	signer overlay.BroadcastSigner,
	now time.Time,
) (any, error) {
	if signer == nil {
		return nil, nil
	}

	return b.publicationCertificate(sub, signer, now)
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
