package p2p

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/blockproof"
	sharddomain "github.com/xssnick/gton/service/shard"
	"github.com/xssnick/tonutils-go/adnl/keys"
	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestBlockBroadcastsQueueIsBoundedAndTransfersOwnership(t *testing.T) {
	node := newTestNode(t)
	broadcasts := node.BlockBroadcasts()
	broadcasts.queue = newBoundedQueue(1, 1<<20, blockPublicationRequestBytes)

	owned := []byte{1, 2, 3, 4}
	first := BlockCandidatePublication{BlockBOC: owned}
	if !broadcasts.TryRelayCandidate(first) {
		t.Fatal("first candidate was not queued")
	}
	if broadcasts.TryRelayCandidate(BlockCandidatePublication{BlockBOC: []byte{5}}) {
		t.Fatal("candidate was queued above the item bound")
	}

	request, ok := broadcasts.queue.TryPop()
	if !ok {
		t.Fatal("queued candidate is missing")
	}
	if len(request.candidate.BlockBOC) == 0 || &request.candidate.BlockBOC[0] != &owned[0] {
		t.Fatal("successful enqueue copied instead of taking ownership of the candidate BOC")
	}

	broadcasts.queue.Close()
	if broadcasts.TryPublishAccepted(AcceptedBlockPublication{}) {
		t.Fatal("accepted block was queued after capability shutdown")
	}
}

func TestBlockBroadcastsQueueClosesWithNode(t *testing.T) {
	node := newTestNode(t)
	node.stop()

	if node.BlockBroadcasts().TryRelayCandidate(BlockCandidatePublication{BlockBOC: []byte{1}}) {
		t.Fatal("candidate was queued after Node stopped")
	}
}

func TestBlockBroadcastsPlumtreeProducerLeaseTracksAllPublicShards(t *testing.T) {
	node := newTestNode(t)
	overlayID := testPeerID("plumtree-producer-lease")
	sub, err := node.getOrCreateSubscription(overlaySpec{
		Name:      "basechain",
		Kind:      overlayKindPublicShard,
		Workchain: 0,
		Shard:     sharddomain.Root,
		ShortID:   overlayID[:],
	})
	if err != nil {
		t.Fatalf("create public subscription: %v", err)
	}
	otherOverlayID := testPeerID("plumtree-producer-lease-other-shard")
	otherSub, err := node.getOrCreateSubscription(overlaySpec{
		Name:      "other-basechain",
		Kind:      overlayKindPublicShard,
		Workchain: 1,
		Shard:     sharddomain.Root,
		ShortID:   otherOverlayID[:],
	})
	if err != nil {
		t.Fatalf("create second public subscription: %v", err)
	}

	first := node.BlockBroadcasts().RegisterPlumtreeProducer()
	second := node.BlockBroadcasts().RegisterPlumtreeProducer()
	if !sub.plumtree.engine.isOriginalSender ||
		sub.plumtree.engine.localEagerLimit != plumtreeOriginalEagerLimit ||
		!otherSub.plumtree.engine.isOriginalSender {
		t.Fatal("active validator session did not mark every public overlay as original")
	}

	if err = first.Close(); err != nil {
		t.Fatal(err)
	}
	if !sub.plumtree.engine.isOriginalSender {
		t.Fatal("overlapping producer lease was not reference-counted")
	}
	if err = second.Close(); err != nil {
		t.Fatal(err)
	}
	if sub.plumtree.engine.isOriginalSender ||
		sub.plumtree.engine.localEagerLimit != plumtreeRegularEagerLimit ||
		otherSub.plumtree.engine.isOriginalSender {
		t.Fatal("last producer lease did not restore the receiver role")
	}
}

func TestBlockBroadcastsProducerAppliesToSubscriptionCreatedLater(t *testing.T) {
	node := newTestNode(t)
	lease := node.BlockBroadcasts().RegisterPlumtreeProducer()
	t.Cleanup(func() { _ = lease.Close() })

	overlayID := testPeerID("plumtree-producer-late-subscription")
	sub, err := node.getOrCreateSubscription(overlaySpec{
		Name:      "basechain-late",
		Kind:      overlayKindPublicShard,
		Workchain: 0,
		Shard:     sharddomain.Root,
		ShortID:   overlayID[:],
	})
	if err != nil {
		t.Fatal(err)
	}
	if !sub.plumtree.engine.isOriginalSender {
		t.Fatal("new public subscription missed the active producer role")
	}
}

func TestSerializeBlockCandidateBroadcastUsesModeTwoLZ4(t *testing.T) {
	root := blockPublicationTestRoot()
	blockBOC := serializeCompressedBlockRoot(root)
	block := blockPublicationTestID(root, blockBOC)
	publication := BlockCandidatePublication{
		Block:            block,
		BlockBOC:         blockBOC,
		CatchainSeqno:    0x80000001,
		ValidatorSetHash: 0xfedcba98,
	}

	payload, err := serializeBlockCandidateBroadcast(publication)
	if err != nil {
		t.Fatalf("serialize candidate broadcast: %v", err)
	}
	var message tonnodeapi.NewBlockCandidateBroadcastCompressed
	rest, err := tl.Parse(&message, payload, true)
	if err != nil || len(rest) != 0 {
		t.Fatalf("parse candidate broadcast: rest=%x err=%v", rest, err)
	}
	if !message.ID.Equals(&block) ||
		uint32(message.CatchainSeqno) != publication.CatchainSeqno ||
		uint32(message.ValidatorSetHash) != publication.ValidatorSetHash {
		t.Fatalf("candidate metadata = %+v, want %+v", message, publication)
	}
	if len(message.CollatorSignature.Who) != sha256.Size ||
		!bytes.Equal(message.CollatorSignature.Who, make([]byte, sha256.Size)) ||
		len(message.CollatorSignature.Signature) != 0 {
		t.Fatalf("collator signature = %+v, want empty", message.CollatorSignature)
	}

	decompressed, err := decompressLZ4Block(message.Compressed)
	if err != nil {
		t.Fatalf("decompress candidate broadcast: %v", err)
	}
	wantModeTwo := root.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: true})
	if !bytes.Equal(decompressed, wantModeTwo) {
		t.Fatalf("candidate BOC mode differs: got %x, want %x", decompressed, wantModeTwo)
	}
}

func TestSerializeAcceptedBlockBroadcastUsesCompressedV2(t *testing.T) {
	publication := blockPublicationTestAccepted(t, true)
	wantMode31 := publication.Block.Block.ToBOCWithOptions(cell.BOCSerializeOptions{
		WithCRC32C:    true,
		WithIndex:     true,
		WithCacheBits: true,
		WithIntHashes: true,
	})
	if !bytes.Equal(publication.Block.BlockBOC, wantMode31) {
		t.Fatal("accepted block publication is not canonical BOC mode 31")
	}

	signatureSet, err := acceptedBlockBroadcastSignatureSet(publication)
	if err != nil {
		t.Fatalf("build signature set: %v", err)
	}
	payload, err := serializeAcceptedBlockBroadcast(publication, signatureSet)
	if err != nil {
		t.Fatalf("serialize accepted block broadcast: %v", err)
	}

	var message tonnodeapi.BlockBroadcastCompressedV2
	rest, err := tl.Parse(&message, payload, true)
	if err != nil || len(rest) != 0 {
		t.Fatalf("parse accepted block broadcast: rest=%x err=%v", rest, err)
	}
	if !message.ID.Equals(&publication.Block.ID) || message.Flags != 0 ||
		!bytes.Equal(message.Proof, publication.Block.ProofBOC) {
		t.Fatalf("accepted block envelope = %+v", message)
	}
	wireSignatures, ok := message.SignatureSet.(tonnodeapi.SignatureSetSimplex)
	if !ok || !wireSignatures.Final {
		t.Fatalf("signature set = %T %+v, want final simplex", message.SignatureSet, message.SignatureSet)
	}

	roots, canonical, err := cell.DecompressBOCSerialized(
		message.DataCompressed,
		maxDecompressedBlockSize,
		nil,
		compressedBlockRootSerializeOptions,
	)
	if err != nil {
		t.Fatalf("decompress accepted block: %v", err)
	}
	if len(roots) != 1 || !bytes.Equal(roots[0].Hash(), publication.Block.ID.RootHash) ||
		!bytes.Equal(canonical, publication.Block.BlockBOC) {
		t.Fatalf("decompressed accepted block does not match publication")
	}
}

func TestAcceptedBlockSignatureSetAllowsNotarization(t *testing.T) {
	publication := blockPublicationTestAccepted(t, false)
	signatureSet, err := acceptedBlockBroadcastSignatureSet(publication)
	if err != nil {
		t.Fatalf("build notarized signature set: %v", err)
	}
	if signatureSet.Final {
		t.Fatal("notarized signature set became final")
	}
}

func TestBlockBroadcastsAcceptedCustomModes(t *testing.T) {
	t.Run("notarized full block and finality", func(t *testing.T) {
		node := newPrivateOverlayTestNode(t)
		sub, peer := blockPublicationTestCustomOverlay(t, node, "notarized")
		publication := blockPublicationTestAccepted(t, false)
		publication.Public = false

		node.BlockBroadcasts().publishAccepted(publication)
		queue := blockPublicationTestLegacyFECQueue(t, sub, peer)
		finality, ok := queue.TryPop()
		if !ok || finality.kind != blockFinalityBroadcastKind {
			t.Fatalf("first custom notarized request = %+v, ok=%v", finality, ok)
		}
		if delivery := sub.rebroadcastDelivery(finality); delivery != DeliveryFEC {
			t.Fatalf("custom notarized finality delivery = %s, want legacy FEC", delivery)
		}
		full, ok := queue.TryPop()
		if !ok || full.kind != blockBroadcastCompressedV2Kind {
			t.Fatalf("second custom notarized request = %+v, ok=%v", full, ok)
		}
		if delivery := sub.rebroadcastDelivery(full); delivery != DeliveryFEC {
			t.Fatalf("custom notarized block delivery = %s, want legacy FEC", delivery)
		}
	})

	t.Run("final full block and finality", func(t *testing.T) {
		node := newPrivateOverlayTestNode(t)
		sub, peer := blockPublicationTestCustomOverlay(t, node, "final")
		publication := blockPublicationTestAccepted(t, true)
		publication.Public = false

		node.BlockBroadcasts().publishAccepted(publication)
		queue := blockPublicationTestLegacyFECQueue(t, sub, peer)
		finality, ok := queue.TryPop()
		if !ok || finality.kind != blockFinalityBroadcastKind {
			t.Fatalf("first custom final request = %+v, ok=%v", finality, ok)
		}
		if delivery := sub.rebroadcastDelivery(finality); delivery != DeliveryFEC {
			t.Fatalf("custom finality delivery = %s, want legacy FEC", delivery)
		}
		full, ok := queue.TryPop()
		if !ok || full.kind != blockBroadcastCompressedV2Kind {
			t.Fatalf("second custom final request = %+v, ok=%v", full, ok)
		}
		if delivery := sub.rebroadcastDelivery(full); delivery != DeliveryFEC {
			t.Fatalf("custom final block delivery = %s, want legacy FEC", delivery)
		}
	})
}

func TestBlockBroadcastsCandidateCustomPathIsDeduplicated(t *testing.T) {
	node := newPrivateOverlayTestNode(t)
	sub, peer := blockPublicationTestCustomOverlay(t, node, "candidate")
	root := blockPublicationTestRoot()
	blockBOC := serializeCompressedBlockRoot(root)
	publication := BlockCandidatePublication{
		Block:            blockPublicationTestID(root, blockBOC),
		BlockBOC:         blockBOC,
		CatchainSeqno:    11,
		ValidatorSetHash: 12,
	}

	node.BlockBroadcasts().publishCandidate(publication)
	node.BlockBroadcasts().publishCandidate(publication)
	queue := blockPublicationTestLegacyFECQueue(t, sub, peer)
	request, ok := queue.TryPop()
	if !ok || request.kind != blockCandidateBroadcastCompressedKind {
		t.Fatalf("custom candidate request = %+v, ok=%v", request, ok)
	}
	if delivery := sub.rebroadcastDelivery(request); delivery != DeliveryFEC {
		t.Fatalf("custom candidate delivery = %s, want legacy FEC", delivery)
	}
	if _, ok = queue.TryPop(); ok {
		t.Fatal("duplicate candidate was enqueued twice")
	}
}

func TestBlockBroadcastsCustomCandidateAndFullBlockShareDedupe(t *testing.T) {
	for _, candidateFirst := range []bool{true, false} {
		name := "full-first"
		if candidateFirst {
			name = "candidate-first"
		}
		t.Run(name, func(t *testing.T) {
			node := newPrivateOverlayTestNode(t)
			sub, peer := blockPublicationTestCustomOverlay(t, node, name)
			root := blockPublicationTestRoot()
			blockBOC := serializeCompressedBlockRoot(root)
			block := blockPublicationTestID(root, blockBOC)
			candidate := BlockCandidatePublication{
				Block:            block,
				BlockBOC:         blockBOC,
				CatchainSeqno:    11,
				ValidatorSetHash: 12,
			}
			accepted := blockPublicationTestAcceptedForBlock(t, block, root, blockBOC, false)
			accepted.Public = false

			if candidateFirst {
				node.BlockBroadcasts().publishCandidate(candidate)
				node.BlockBroadcasts().publishAccepted(accepted)
			} else {
				node.BlockBroadcasts().publishAccepted(accepted)
				node.BlockBroadcasts().publishCandidate(candidate)
			}

			queue := blockPublicationTestLegacyFECQueue(t, sub, peer)
			var blockPublications, finalityPublications int
			for {
				request, ok := queue.TryPop()
				if !ok {
					break
				}
				switch request.kind {
				case blockCandidateBroadcastCompressedKind, blockBroadcastCompressedV2Kind:
					blockPublications++
				case blockFinalityBroadcastKind:
					finalityPublications++
				}
			}
			if blockPublications != 1 {
				t.Fatalf("custom candidate/full publications = %d, want 1", blockPublications)
			}
			if finalityPublications != 1 {
				t.Fatalf("custom finality publications = %d, want 1", finalityPublications)
			}
		})
	}
}

func TestBlockBroadcastsCandidateFirstSkipsUnroutableFullBlockCompression(t *testing.T) {
	node := newPrivateOverlayTestNode(t)
	var logs bytes.Buffer
	node.log = zerolog.New(&logs)
	blockPublicationTestCustomOverlay(t, node, "skip-full-compression")
	root := blockPublicationTestRoot()
	blockBOC := serializeCompressedBlockRoot(root)
	block := blockPublicationTestID(root, blockBOC)

	node.BlockBroadcasts().publishCandidate(BlockCandidatePublication{
		Block:            block,
		BlockBOC:         blockBOC,
		CatchainSeqno:    11,
		ValidatorSetHash: 12,
	})
	logs.Reset()

	accepted := blockPublicationTestAcceptedForBlock(t, block, root, blockBOC, false)
	accepted.Public = false
	accepted.Block.Block = nil
	node.BlockBroadcasts().publishAccepted(accepted)
	if strings.Contains(logs.String(), "accepted block root is nil") {
		t.Fatal("candidate-first custom dedupe still entered full-block compression")
	}
}

func TestBlockOverlayFanoutDedupeIsRouteScoped(t *testing.T) {
	block := testBlockID(0, topShard, 17)
	customCandidate := blockOverlayRouteFanoutKey(
		blockOverlayFanoutRouteCustom,
		"candidate",
		block,
	)
	customFull := blockOverlayRouteFanoutKey(
		blockOverlayFanoutRouteCustom,
		"block",
		block,
	)
	if customCandidate != customFull {
		t.Fatalf("custom candidate key %q differs from full-block key %q", customCandidate, customFull)
	}

	keys := []string{
		blockOverlayRouteFanoutKey(blockOverlayFanoutRoutePublic, "candidate", block),
		blockOverlayRouteFanoutKey(blockOverlayFanoutRoutePublic, "block", block),
		blockOverlayRouteFanoutKey(blockOverlayFanoutRouteFastSync, "candidate", block),
		blockOverlayRouteFanoutKey(blockOverlayFanoutRouteFastSync, "block", block),
	}
	for i, key := range keys {
		if key == customFull {
			t.Fatalf("route key %q shares custom key", key)
		}
		for j := 0; j < i; j++ {
			if key == keys[j] {
				t.Fatalf("route keys %q and %q unexpectedly match", key, keys[j])
			}
		}
	}
}

func TestBlockBroadcastsFullBlockUsesPublicOnlyWhenSelected(t *testing.T) {
	node := newPrivateOverlayTestNode(t)
	node.zeroStateFileHash = make([]byte, sha256.Size)
	publication := blockPublicationTestAccepted(t, false)
	publication.CertificateSigner = privateOverlayTestSigner{
		key: ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x61}, ed25519.SeedSize)),
	}
	publication.Public = false

	publicSub, err := node.subscriptionForBlock(publication.Block.ID)
	if err != nil {
		t.Fatalf("create public publication overlay: %v", err)
	}
	peer := &overlayPeer{
		id:          testPeerID("block-publication-public-peer"),
		fixedMember: true,
		alive:       true,
	}
	publicSub.mx.Lock()
	publicSub.peers[peer.id] = peer
	testOverlaySubscription(publicSub)
	peer.route.DeferQUICDial(time.Now())
	publicSub.mx.Unlock()
	publicSub.broadcastTargets.Store(nil)

	node.BlockBroadcasts().publishAccepted(publication)
	if _, _, ok := peer.rebroadcastQueueSnapshots(); ok {
		t.Fatal("Public=false created a public full-block queue")
	}

	publication.Public = true
	node.BlockBroadcasts().publishAccepted(publication)
	peer.rebroadcastMx.Lock()
	queue := peer.localRebroadcastQueue
	peer.rebroadcastMx.Unlock()
	if queue == nil {
		t.Fatal("Public=true did not create a public full-block queue")
	}
	request, ok := queue.TryPop()
	if !ok || request.kind != blockBroadcastCompressedV2Kind {
		t.Fatalf("public full-block request = %+v, ok=%v", request, ok)
	}
	if delivery := publicSub.rebroadcastDelivery(request); delivery != DeliveryFEC {
		t.Fatalf("public full-block delivery = %s, want FEC", delivery)
	}
	certificate, ok := request.certificate.(overlay.Certificate)
	if !ok {
		t.Fatalf("public full-block certificate = %T, want overlay.Certificate", request.certificate)
	}
	result, err := certificate.Check(node.localID[:], publicSub.spec.ShortID, 1, true)
	if err != nil || result != overlay.CertCheckResultTrusted {
		t.Fatalf("public full-block certificate check = %d, err=%v", result, err)
	}
}

func TestBlockBroadcastsPlumtreeModes(t *testing.T) {
	node := newPrivateOverlayTestNode(t)
	node.zeroStateFileHash = make([]byte, sha256.Size)
	certificateSigner := privateOverlayTestSigner{
		key: ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x62}, ed25519.SeedSize)),
	}
	issuerID, err := peerIDFromED25519PublicKey(certificateSigner.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	node.SetPlumtreePolicy(NewPlumtreePolicy([]PeerID{issuerID}))

	root := blockPublicationTestRoot()
	blockBOC := serializeCompressedBlockRoot(root)
	block := blockPublicationTestID(root, blockBOC)
	publicSub, err := node.subscriptionForBlock(block)
	if err != nil {
		t.Fatalf("create public publication overlay: %v", err)
	}
	fastSyncSub := blockPublicationTestFastSyncOverlay(t, node, block)

	candidate := BlockCandidatePublication{
		Block:             block,
		BlockBOC:          blockBOC,
		CatchainSeqno:     11,
		ValidatorSetHash:  12,
		CertificateSigner: certificateSigner,
	}
	node.BlockBroadcasts().publishCandidate(candidate)
	if got := plumtreeFECStateCount(publicSub); got != 1 {
		t.Fatalf("public candidate Plumtree FEC states = %d, want 1", got)
	}
	if got := plumtreeFECStateCount(fastSyncSub); got != 1 {
		t.Fatalf("FastSync candidate Plumtree FEC states = %d, want 1", got)
	}
	requirePublicationCertificate(t, publicSub, node.localID, issuerID)
	requirePublicationCertificate(t, fastSyncSub, node.localID, issuerID)

	accepted := blockPublicationTestAcceptedForBlock(t, block, root, blockBOC, false)
	accepted.Public = false
	accepted.CertificateSigner = certificateSigner
	node.BlockBroadcasts().publishAccepted(accepted)
	finalityID, err := finalityPlumtreeBroadcastID(block)
	if err != nil {
		t.Fatalf("build finality id: %v", err)
	}
	if !plumtreeHasSimpleState(publicSub, finalityID) {
		t.Fatal("public finality Plumtree simple state is missing")
	}
	if !plumtreeHasSimpleState(fastSyncSub, finalityID) {
		t.Fatal("FastSync finality Plumtree simple state is missing")
	}
	if got := plumtreeFECStateCount(fastSyncSub); got != 1 {
		t.Fatalf("accepted full block entered FastSync FEC: states=%d, want candidate-only 1", got)
	}
}

func requirePublicationCertificate(
	t *testing.T,
	sub *overlaySubscription,
	source PeerID,
	issuer PeerID,
) {
	t.Helper()

	sub.plumtree.engine.mu.Lock()
	var part *plumtreePartState
	for _, state := range sub.plumtree.engine.fecStates {
		part = state.parts[0]
		break
	}
	sub.plumtree.engine.mu.Unlock()
	if part == nil || part.sourceKey.Key == nil {
		t.Fatal("originated Plumtree FEC part is missing")
	}
	sourceID, err := peerIDFromED25519PublicKey(part.sourceKey.Key)
	if err != nil {
		t.Fatal(err)
	}
	if sourceID != source {
		t.Fatalf("publication source = %s, want node ADNL %s", sourceID, source)
	}
	certificate, ok := part.certificate.protocolValue().(overlay.Certificate)
	if !ok {
		t.Fatalf("publication certificate = %T, want overlay.Certificate", part.certificate.protocolValue())
	}
	issuerKey, ok := certificate.IssuedBy.(keys.PublicKeyED25519)
	if !ok {
		t.Fatalf("publication certificate issuer = %T", certificate.IssuedBy)
	}
	issuerID, err := peerIDFromED25519PublicKey(issuerKey.Key)
	if err != nil {
		t.Fatal(err)
	}
	if issuerID != issuer {
		t.Fatalf("publication certificate issuer = %s, want %s", issuerID, issuer)
	}
	result, err := certificate.Check(source[:], sub.spec.ShortID, 1, true)
	if err != nil || result != overlay.CertCheckResultTrusted {
		t.Fatalf("publication certificate check = %d, err=%v", result, err)
	}
}

func TestBlockBroadcastsSkipsUnroutableFullBlockCompression(t *testing.T) {
	node := newTestNode(t)
	publication := blockPublicationTestAccepted(t, false)
	publication.Public = false
	publication.Block.Block = nil

	node.BlockBroadcasts().publishAccepted(publication)
	key := blockOverlayRouteFanoutKey(
		blockOverlayFanoutRouteCustom,
		"block",
		publication.Block.ID,
	)
	if !node.overlayFanoutDeduper.Mark(key, time.Now()) {
		t.Fatal("unroutable full block was compressed and marked as published")
	}
}

func blockPublicationTestAccepted(t *testing.T, final bool) AcceptedBlockPublication {
	t.Helper()

	root := blockPublicationTestRoot()
	blockBOC := serializeCompressedBlockRoot(root)
	block := blockPublicationTestID(root, blockBOC)
	return blockPublicationTestAcceptedForBlock(t, block, root, blockBOC, final)
}

func blockPublicationTestAcceptedForBlock(
	t *testing.T,
	block ton.BlockIDExt,
	root *cell.Cell,
	blockBOC []byte,
	final bool,
) AcceptedBlockPublication {
	t.Helper()

	candidateData, err := tl.Serialize(ton.ConsensusCandidateHashDataOrdinary{
		Block:            block,
		CollatedFileHash: bytes.Repeat([]byte{0x55}, sha256.Size),
		Parent:           ton.ConsensusCandidateWithoutParents{},
	}, true)
	if err != nil {
		t.Fatalf("serialize simplex candidate: %v", err)
	}
	signatures := blockproof.NewSimplexValidatorSignatureSet(
		11,
		0xfedcba98,
		nil,
		final,
		bytes.Repeat([]byte{0x66}, sha256.Size),
		17,
		candidateData,
	)

	return AcceptedBlockPublication{
		Block: DownloadedBlock{
			ID:       block,
			Block:    root,
			Proof:    cell.BeginCell().MustStoreUInt(0x77, 8).EndCell(),
			BlockBOC: blockBOC,
			ProofBOC: cell.BeginCell().MustStoreUInt(0x77, 8).EndCell().ToBOC(),
		},
		Signatures: signatures,
		Public:     true,
	}
}

func blockPublicationTestCustomOverlay(
	t *testing.T,
	node *Node,
	name string,
) (*overlaySubscription, *overlayPeer) {
	t.Helper()

	overlayID := testPeerID("block-publication-custom-" + name)
	remoteID := testPeerID("block-publication-custom-peer-" + name)
	sub, err := node.newOverlaySubscription(overlaySpec{
		Name:         "custom." + name,
		Kind:         overlayKindCustomFixed,
		ShortID:      overlayID[:],
		FixedNodes:   []PeerID{node.localID, remoteID},
		FixedNodeIDs: map[PeerID]struct{}{node.localID: {}, remoteID: {}},
		BlockSenders: map[PeerID]struct{}{node.localID: {}},
		AuthorizedKeys: map[string]uint32{
			string(node.localID[:]): maxOverlayPayloadSize,
		},
	})
	if err != nil {
		t.Fatalf("create custom publication overlay: %v", err)
	}
	peer := &overlayPeer{
		id:          remoteID,
		fixedMember: true,
		alive:       true,
	}
	sub.peers[remoteID] = peer
	testOverlaySubscription(sub)

	node.subscriptionsMx.Lock()
	node.subscriptions[overlaySpecKey(sub.spec)] = sub
	node.subscriptionsMx.Unlock()
	return sub, peer
}

func blockPublicationTestLegacyFECQueue(
	t *testing.T,
	sub *overlaySubscription,
	peer *overlayPeer,
) *boundedQueue[rebroadcastRequest] {
	t.Helper()

	sub.mx.Lock()
	twoStepQueue := sub.twoStepQueue
	sub.mx.Unlock()
	if twoStepQueue != nil {
		t.Fatal("BlockBroadcasts routed custom legacy FEC through two-step")
	}

	peer.rebroadcastMx.Lock()
	queue := peer.localRebroadcastQueue
	peer.rebroadcastMx.Unlock()
	if queue == nil {
		t.Fatal("custom legacy FEC queue is missing")
	}
	return queue
}

func blockPublicationTestFastSyncOverlay(
	t *testing.T,
	node *Node,
	block ton.BlockIDExt,
) *overlaySubscription {
	t.Helper()

	roster := NewFastSyncValidatorRoster(nil, []FastSyncValidator{
		fastSyncOverlayTestValidator(0x31, node.localID),
	}, nil)
	if err := node.SetFastSyncOverlays(FastSyncState{
		Roster: roster,
		Shards: []FastSyncShard{{
			Workchain: block.Workchain,
			Shard:     block.Shard,
		}},
		MasterchainPlumtreeEnabled: true,
		ShardPlumtreeEnabled:       true,
	}); err != nil {
		t.Fatalf("create FastSync publication overlay: %v", err)
	}
	sub, err := node.fastSyncSubscriptionForBlock(block)
	if err != nil {
		t.Fatalf("resolve FastSync publication overlay: %v", err)
	}
	if sub == nil {
		t.Fatal("FastSync publication overlay is missing")
	}
	return sub
}

func plumtreeFECStateCount(sub *overlaySubscription) int {
	sub.plumtree.engine.mu.Lock()
	defer sub.plumtree.engine.mu.Unlock()
	return len(sub.plumtree.engine.fecStates)
}

func plumtreeHasSimpleState(
	sub *overlaySubscription,
	id [sha256.Size]byte,
) bool {
	sub.plumtree.engine.mu.Lock()
	defer sub.plumtree.engine.mu.Unlock()
	return sub.plumtree.engine.simpleStates[id] != nil
}

func blockPublicationTestRoot() *cell.Cell {
	builder := cell.BeginCell()
	for i := 0; i < 64; i++ {
		builder.MustStoreUInt(uint64(i%4), 2)
	}
	return builder.EndCell()
}

func blockPublicationTestID(root *cell.Cell, blockBOC []byte) ton.BlockIDExt {
	rootHash := root.Hash()
	fileHash := sha256.Sum256(blockBOC)
	return ton.BlockIDExt{
		Workchain: 0,
		Shard:     topShard,
		SeqNo:     17,
		RootHash:  bytes.Clone(rootHash),
		FileHash:  fileHash[:],
	}
}
