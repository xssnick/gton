package p2p

import (
	"context"
	"errors"
	"testing"
	"time"

	tnstore "github.com/xssnick/gton/service/storage"

	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// testHeldBlockNode wires a node whose signature verifier counts its calls and
// whose compressed-state provider signals the moment a state-aware decode
// starts, so a test proves a decode never began rather than that it merely
// produced nothing.
func testHeldBlockNode(t *testing.T, seqno uint32) (*Node, *overlaySubscription, *countingBroadcastSignatureVerifier, *testSignalingCompressedStateProvider, *lockedBroadcastPipelineObserver, tonnodeapi.BlockBroadcastCompressedV2, ton.BlockIDExt) {
	t.Helper()

	node := newTestNode(t)
	node.stateArtifacts = newTestPebbleStore(t)
	verifier := &countingBroadcastSignatureVerifier{}
	node.signatureVerifier = verifier
	state := cell.BeginCell().MustStoreUInt(uint64(seqno), 16).EndCell()
	provider := &testSignalingCompressedStateProvider{state: state, called: make(chan struct{})}
	node.compressedState = provider
	observer := newAwaitingBroadcastPipelineObserver(broadcastPipelineStageDecodeAsync, broadcastPipelineResultSuccess)
	node.SetBroadcastPipelineObserver(observer)

	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			Kind:    overlayKindPublicShard,
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	})

	msg, block := testStateAwareCompressedV2Broadcast(t, seqno, state)
	return node, sub, verifier, provider, observer, msg, block
}

func requireNoBlockBroadcastDecode(t *testing.T, provider *testSignalingCompressedStateProvider, observer *lockedBroadcastPipelineObserver) {
	t.Helper()

	select {
	case <-provider.called:
		t.Fatal("payload decode started for a block the node already holds")
	default:
	}
	for _, stage := range []string{broadcastPipelineStageDecodeAsync, broadcastPipelineStageDecodeInline} {
		for _, result := range []string{broadcastPipelineResultSuccess, broadcastPipelineResultError, broadcastPipelineResultMiss, broadcastPipelineResultDrop} {
			if got := observer.count(stage, result); got != 0 {
				t.Fatalf("held block produced %d %s/%s decode samples", got, stage, result)
			}
		}
	}
}

// holdBlockInLiveCache stands in for the service: a block this node accepted
// in consensus or applied and has not flushed yet is published to the shared
// live block cache with its data.
func holdBlockInLiveCache(t *testing.T, node *Node, block ton.BlockIDExt) {
	t.Helper()

	if err := node.liveBlockCache.PublishLiveBlockArtifacts(tnstore.LiveBlockCacheArtifacts{
		Block:     block,
		BlockData: serializeCompressedBlockRoot(testPeerBlockRoot(t, block.Workchain, block.SeqNo)),
	}); err != nil {
		t.Fatalf("publish live block: %v", err)
	}
}

// holdBlockInShardBroadcastCache stands in for an earlier delivery: a signed
// copy of the block was decoded and handed to the apply path already.
func holdBlockInShardBroadcastCache(t *testing.T, node *Node, block ton.BlockIDExt) {
	t.Helper()

	downloaded := testShardBroadcastDownloadedBlock(t, block.SeqNo, 0)
	if !downloaded.ID.Equals(&block) {
		t.Fatalf("hot cache fixture %s does not match broadcast block %s", downloaded.BlockRef(), tnstore.FormatBlockRef(block))
	}
	if !node.rememberShardBroadcastBlock(&downloaded) {
		t.Fatal("hot cache refused the decoded shard block")
	}
}

// TestClassifyRelaysHeldShardBlockBroadcastWithoutDecode pins the pre-decode
// exit: a full block broadcast for a block this node already holds is decided
// from the TL header alone — no validator-signature pass, no decode job, no
// event — accounted as an already_applied drop, while the payload is still
// deduplicated and relayed like a processed block.
func TestClassifyRelaysHeldShardBlockBroadcastWithoutDecode(t *testing.T) {
	tests := []struct {
		name string
		hold func(t *testing.T, node *Node, block ton.BlockIDExt)
	}{
		{name: "accepted or applied block in the live block cache", hold: holdBlockInLiveCache},
		{name: "signed copy in the shard broadcast hot cache", hold: holdBlockInShardBroadcastCache},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, sub, verifier, provider, observer, msg, block := testHeldBlockNode(t, 220+uint32(i))
			tt.hold(t, node, block)

			payload, err := tl.Serialize(msg, true)
			if err != nil {
				t.Fatalf("serialize broadcast: %v", err)
			}
			fingerprint := broadcastFingerprint(sub.spec.ShortID, payload)

			result, err := sub.classifyBroadcastPayload(nil, msg, newKnownBroadcastPayload(payload), DeliverySimple, false, testPeerID("source"))
			if err != nil {
				t.Fatalf("classify held block broadcast: %v", err)
			}
			if result.disposition != broadcastDispositionAccept {
				t.Fatalf("held block broadcast disposition = %v, want accept (relay-only)", result.disposition)
			}
			if result.accepted.event != nil {
				t.Fatalf("held block broadcast produced an application event: %+v", result.accepted.event)
			}
			if result.accepted.block == nil || !result.accepted.block.Equals(&block) {
				t.Fatalf("held block broadcast block = %+v, want %s", result.accepted.block, tnstore.FormatBlockRef(block))
			}
			if result.accepted.rebroadcast == nil {
				t.Fatal("held block broadcast lost its relay payload")
			}
			if result.accepted.fingerprint != fingerprint || !result.accepted.deduped {
				t.Fatalf("held block broadcast is not deduplicated under its fingerprint: %+v", result.accepted)
			}

			if calls := verifier.calls.Load(); calls != 0 {
				t.Fatalf("held block broadcast reached the signature verifier: %d calls", calls)
			}
			requireNoBlockBroadcastDecode(t, provider, observer)
			if got := testBroadcastDropStatCount(node, "basechain", "tonNode.blockBroadcastCompressedV2", "already_applied"); got != 1 {
				t.Fatalf("already_applied drop count = %d, want 1", got)
			}
			if _, _, err = node.decodedBroadcasts.get("tonNode.blockBroadcastCompressedV2", block); !errors.Is(err, tnstore.ErrNotFound) {
				t.Fatalf("held block broadcast touched the decode cache: %v", err)
			}
			if _, ok := node.eventQueue.TryPop(); ok {
				t.Fatal("held block broadcast produced a broadcast event")
			}

			// the overlay relays the accepted payload; a replay of the same
			// payload is a plain duplicate, not a second already_applied drop
			if disposition := sub.handleOverlayBroadcastPayload(nil, msg, newKnownBroadcastPayload(payload), DeliverySimple, false, testPeerID("source")); disposition != overlay.BroadcastDispositionIgnore {
				t.Fatalf("replayed held block broadcast disposition = %v, want ignore", disposition)
			}
			if got := testBroadcastDropStatCount(node, "basechain", "tonNode.blockBroadcastCompressedV2", "seen"); got != 1 {
				t.Fatalf("seen drop count = %d, want 1", got)
			}
			if got := testBroadcastDropStatCount(node, "basechain", "tonNode.blockBroadcastCompressedV2", "already_applied"); got != 1 {
				t.Fatalf("already_applied drop count after replay = %d, want 1", got)
			}
			if calls := verifier.calls.Load(); calls != 0 {
				t.Fatalf("replayed held block broadcast reached the signature verifier: %d calls", calls)
			}
			requireNoBlockBroadcastDecode(t, provider, observer)
		})
	}
}

// TestClassifyDecodesUnknownShardBlockBroadcast is the other half of the gate:
// the first copy of a block the node does not hold still pays the signature
// pass and the decode and reaches the apply path, and only once that copy has
// been delivered does the next copy of the same block become relay-only.
func TestClassifyDecodesUnknownShardBlockBroadcast(t *testing.T) {
	node, sub, verifier, provider, observer, msg, block := testHeldBlockNode(t, 225)
	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize broadcast: %v", err)
	}

	if disposition := sub.handleOverlayBroadcastPayload(nil, msg, newKnownBroadcastPayload(payload), DeliverySimple, false, testPeerID("source")); disposition != overlay.BroadcastDispositionAcceptAndRelay {
		t.Fatalf("unknown block broadcast disposition = %v, want accept-and-relay", disposition)
	}
	if calls := verifier.calls.Load(); calls != 1 {
		t.Fatalf("unknown block broadcast made %d signature checks, want 1", calls)
	}

	select {
	case <-provider.called:
	case <-time.After(5 * time.Second):
		t.Fatal("unknown block broadcast was never decoded")
	}
	observer.waitAwaited(t)

	eventCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	event, ok := node.eventQueue.Pop(eventCtx)
	if !ok {
		t.Fatal("unknown block broadcast produced no broadcast event")
	}
	if event.Downloaded == nil || !event.Downloaded.ID.Equals(&block) {
		t.Fatalf("unexpected broadcast event: %#v", event)
	}
	if got := testBroadcastDropStatCount(node, "basechain", "tonNode.blockBroadcastCompressedV2", "already_applied"); got != 0 {
		t.Fatalf("unknown block broadcast was dropped as already applied: %d", got)
	}

	// the delivered copy is now in the hot shard cache, so the same block
	// arriving on another overlay (a different fingerprint) is relayed without
	// buying a second signature pass or decode
	if !node.shardBroadcastCache.HasBlock(block) {
		t.Fatal("delivered block is missing from the shard broadcast hot cache")
	}
	other := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "custom",
			Kind:    overlayKindPublicShard,
			ShortID: []byte{0x07, 0x08, 0x09},
		},
		log: discardLogger(),
	})
	if disposition := other.handleOverlayBroadcastPayload(nil, msg, newKnownBroadcastPayload(payload), DeliveryTwoStep, false, testPeerID("second")); disposition != overlay.BroadcastDispositionAcceptAndRelay {
		t.Fatalf("second copy disposition = %v, want accept-and-relay", disposition)
	}
	if calls := verifier.calls.Load(); calls != 1 {
		t.Fatalf("second copy of a delivered block reached the signature verifier: %d calls, want 1", calls)
	}
	if got := observer.count(broadcastPipelineStageDecodeAsync, broadcastPipelineResultSuccess); got != 1 {
		t.Fatalf("decode_async success samples = %d, want 1", got)
	}
	if got := testBroadcastDropStatCount(node, "custom", "tonNode.blockBroadcastCompressedV2", "already_applied"); got != 1 {
		t.Fatalf("already_applied drop count on the second overlay = %d, want 1", got)
	}
	if _, ok = node.eventQueue.TryPop(); ok {
		t.Fatal("second copy of a delivered block produced a broadcast event")
	}
}

// TestDecodeWorkersDropBlockHeldSinceClassify covers the late safety net: a
// payload classify let through, but whose block the node obtained by another
// route while the job waited, is dropped by the decode pool worker and by the
// pending-decode processor before the decode, under the same reason.
func TestDecodeWorkersDropBlockHeldSinceClassify(t *testing.T) {
	const kind = "tonNode.blockBroadcastCompressedV2"

	t.Run("offloaded decode", func(t *testing.T) {
		node, _, _, provider, observer, msg, block := testHeldBlockNode(t, 230)
		proofRoot, err := parseDownloadedBlockProof(msg.Proof)
		if err != nil {
			t.Fatalf("parse proof: %v", err)
		}
		preSigSet, err := broadcastSignatureSetFromTL(msg.SignatureSet)
		if err != nil {
			t.Fatalf("parse signature set: %v", err)
		}
		holdBlockInLiveCache(t, node, block)

		node.processOffloadedBroadcastDecode(context.Background(), offloadedBroadcastDecode{
			fingerprint:  "held-offloaded",
			overlay:      "basechain",
			delivery:     DeliverySimple,
			kind:         kind,
			block:        block,
			sourcePeerID: testPeerID("source"),
			receivedAt:   time.Now(),
			msg:          msg,
			proofRoot:    proofRoot,
			preSigSet:    preSigSet,
		})

		requireNoBlockBroadcastDecode(t, provider, observer)
		if got := testBroadcastDropStatCount(node, "basechain", kind, "already_applied"); got != 1 {
			t.Fatalf("already_applied drop count = %d, want 1", got)
		}
		if _, ok := node.eventQueue.TryPop(); ok {
			t.Fatal("held block produced a broadcast event from the decode pool")
		}
	})

	t.Run("pending decode", func(t *testing.T) {
		node, _, _, provider, observer, msg, block := testHeldBlockNode(t, 231)
		proofRoot, err := parseDownloadedBlockProof(msg.Proof)
		if err != nil {
			t.Fatalf("parse proof: %v", err)
		}
		prev, err := compressedBlockPreviousState(block, proofRoot)
		if err != nil {
			t.Fatalf("resolve previous state: %v", err)
		}
		req := pendingBlockBroadcastDecode{
			fingerprint:  "held-pending",
			overlay:      "basechain",
			delivery:     DeliverySimple,
			kind:         kind,
			block:        block,
			prev:         prev,
			sourcePeerID: testPeerID("source"),
			receivedAt:   time.Now(),
			msg:          msg,
			proofRoot:    proofRoot,
		}
		node.pendingBroadcastMx.Lock()
		req.expiresAt = time.Now().Add(pendingBroadcastDecodeTTL)
		req.bytes = pendingBlockBroadcastDecodeBytes(req)
		node.pendingBroadcasts[req.fingerprint] = req
		node.addPendingBlockBroadcastPrevIndexLocked(req)
		node.pendingBroadcastBytes += req.bytes
		node.pendingBroadcastMx.Unlock()
		holdBlockInShardBroadcastCache(t, node, block)

		node.processPendingBlockBroadcastDecodeRequests(context.Background(), []pendingBlockBroadcastDecode{req})

		requireNoBlockBroadcastDecode(t, provider, observer)
		if got := testBroadcastDropStatCount(node, "basechain", kind, "already_applied"); got != 1 {
			t.Fatalf("already_applied drop count = %d, want 1", got)
		}
		if _, ok := node.eventQueue.TryPop(); ok {
			t.Fatal("held block produced a broadcast event from the pending processor")
		}
		node.pendingBroadcastMx.Lock()
		_, pending := node.pendingBroadcasts[req.fingerprint]
		bytes := node.pendingBroadcastBytes
		node.pendingBroadcastMx.Unlock()
		if pending || bytes != 0 {
			t.Fatalf("held block stayed pending (pending=%v, bytes=%d)", pending, bytes)
		}
	})
}
