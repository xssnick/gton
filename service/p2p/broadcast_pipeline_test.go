package p2p

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xssnick/gton/internal/extmsg"
	tnstore "github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/address"
	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type testBroadcastPipelineObserver struct {
	observations []BroadcastPipelineStageObservation
}

type testFailOnceCompressedStateProvider struct {
	failOnce atomic.Bool
	state    *cell.Cell
}

func (p *testFailOnceCompressedStateProvider) StateRootForCompressedBlock(context.Context, ton.BlockIDExt) (*cell.Cell, error) {
	if p.failOnce.Swap(false) {
		return nil, tnstore.ErrNotFound
	}
	return p.state, nil
}

func (p *testFailOnceCompressedStateProvider) RememberCompressedBlockState(*tnstore.BlockState) bool {
	return false
}

func (o *testBroadcastPipelineObserver) ObserveBroadcastPipelineStage(observation BroadcastPipelineStageObservation) {
	o.observations = append(o.observations, observation)
}

func (o *testBroadcastPipelineObserver) requireStage(t *testing.T, stage string, kind string, delivery Delivery, result string) {
	t.Helper()

	for _, observation := range o.observations {
		if observation.Stage == stage &&
			observation.Kind == kind &&
			observation.Delivery == delivery &&
			observation.Result == result {
			return
		}
	}
	t.Fatalf("missing broadcast pipeline observation stage=%s kind=%s delivery=%s result=%s in %#v", stage, kind, delivery, result, o.observations)
}

func TestClassifyInvalidCompressedBlockBroadcastDoesNotWakeBeforeSignaturePrecheck(t *testing.T) {
	node := newTestNode(t)
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "masterchain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	})

	tests := []struct {
		name  string
		block ton.BlockIDExt
		msg   any
	}{
		{
			name:  "compressed",
			block: testBlockID(-1, topShard, 200),
			msg: tonnodeapi.BlockBroadcastCompressed{
				ID:         testBlockID(-1, topShard, 200),
				Compressed: []byte{0x01},
			},
		},
		{
			name:  "compressed-v2",
			block: testBlockID(-1, topShard, 201),
			msg: tonnodeapi.BlockBroadcastCompressedV2{
				ID: testBlockID(-1, topShard, 201),
				SignatureSet: tonnodeapi.SignatureSetOrdinary{
					Signatures: []tonnodeapi.BlockSignature{},
				},
				Proof:          []byte{0x02},
				DataCompressed: []byte{0x03},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload, err := tl.Serialize(tt.msg, true)
			if err != nil {
				t.Fatalf("serialize broadcast: %v", err)
			}

			first := sub.classifyBroadcast(nil, tt.msg, payload, DeliverySimple, false, testPeerID("peer"))
			if first != nil {
				t.Fatalf("invalid masterchain broadcast was accepted before signature precheck: %+v", first)
			}
			if seqno, _ := node.MasterchainBroadcastAfter(tt.block.SeqNo - 1); seqno != 0 {
				t.Fatal("invalid masterchain broadcast should not wake before signature precheck")
			}

			second := sub.classifyBroadcast(nil, tt.msg, payload, DeliverySimple, false, testPeerID("peer"))
			if second != nil {
				t.Fatalf("duplicate broadcast was accepted after fingerprint dedupe: %+v", second)
			}
		})
	}
}

func TestClassifyBroadcastUsesPeerAsFECSourcePeerID(t *testing.T) {
	node := newTestNode(t)
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	})
	peer := &overlayPeer{id: testPeerID("peer-a"), addr: "peer-a"}
	block := testBlockID(0, topShard, 202)
	msg := tonnodeapi.NewShardBlockBroadcast{
		Block: tonnodeapi.NewShardBlock{
			ID:      block,
			CCSeqno: 7,
			Data:    []byte{0x01},
		},
	}

	accepted := sub.classifyBroadcast(peer, msg, []byte{0x01}, DeliveryFEC, false, PeerID{})
	if accepted == nil || accepted.event == nil {
		t.Fatal("expected shard block broadcast event")
	}
	if accepted.event.SourcePeerID != testPeerID("peer-a") {
		t.Fatalf("source key = %q, want peer-a", accepted.event.SourcePeerID)
	}
}

func TestClassifyBroadcastDoesNotSerializeInvalidPayload(t *testing.T) {
	node := newTestNode(t)
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	})
	payload := newSerializedBroadcastPayload(make(chan struct{}))

	result, err := sub.classifyBroadcastPayload(nil, tonnodeapi.NewExternalMessageBroadcast{}, payload, DeliveryFEC, false, testPeerID("peer"))
	if err != nil {
		t.Fatalf("classify invalid broadcast: %v", err)
	}
	if result.disposition != broadcastDispositionIgnore {
		t.Fatalf("invalid external broadcast was accepted: %+v", result.accepted)
	}
	if payload.serialized {
		t.Fatal("invalid broadcast serialized payload before cheap drop")
	}
}

func TestFastSyncOutMsgQueueProofBroadcastIsRelayOnly(t *testing.T) {
	sub := testOverlaySubscription(&overlaySubscription{
		node: newTestNode(t),
		spec: overlaySpec{
			Name:    "fast-sync.masterchain",
			Kind:    overlayKindFastSync,
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	})
	msg := OutMsgQueueProofBroadcast{}

	if !fastSyncBroadcastSupported(msg) {
		t.Fatal("out-msg queue proof broadcast is not enabled on FastSync")
	}

	result, err := sub.classifyBroadcastPayload(
		nil,
		msg,
		newKnownBroadcastPayload([]byte{0x01}),
		DeliverySimple,
		false,
		testPeerID("peer"),
	)
	if err != nil {
		t.Fatalf("classify out-msg queue proof broadcast: %v", err)
	}
	if result.disposition != broadcastDispositionAccept {
		t.Fatal("out-msg queue proof broadcast was not admitted for relay")
	}
	accepted := result.accepted
	if accepted.fingerprint != "" ||
		accepted.deduped ||
		accepted.block != nil ||
		accepted.event != nil ||
		len(accepted.extraEvents) != 0 ||
		accepted.masterchainWake != nil ||
		accepted.rebroadcast != nil ||
		accepted.skipAcceptedMetric {
		t.Fatalf("relay-only broadcast scheduled application work: %+v", result.accepted)
	}
}

func TestClassifyDuplicateIdentifiedBroadcastDoesNotSerializePayload(t *testing.T) {
	node := newTestNode(t)
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	})

	downloaded := testShardBroadcastDownloadedBlock(t, 207, 0x207)
	msg := tonnodeapi.NewBlockCandidateBroadcast{
		ID:   downloaded.ID,
		Data: downloaded.BlockBOC,
	}
	broadcastID := bytes.Repeat([]byte{0xAB}, 32)
	node.deduper.Mark(broadcastFingerprint(sub.spec.ShortID, broadcastID), time.Now())
	payload := newIdentifiedBroadcastPayload(make(chan struct{}), broadcastID)

	result, err := sub.classifyBroadcastPayload(nil, msg, payload, DeliveryFEC, false, testPeerID("peer"))
	if err != nil {
		t.Fatalf("classify duplicate identified broadcast: %v", err)
	}
	if result.disposition != broadcastDispositionIgnore {
		t.Fatalf("duplicate identified broadcast was accepted: %+v", result.accepted)
	}
	if payload.serialized {
		t.Fatal("duplicate identified broadcast serialized payload before dedupe")
	}
}

func TestProcessedCandidateBroadcastSkipsDecodedPipeline(t *testing.T) {
	node := newTestNode(t)
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	})

	downloaded := testBlockFinalityCandidate(t, 208)
	msg := tonnodeapi.NewBlockCandidateBroadcast{
		ID:   downloaded.ID,
		Data: downloaded.BlockBOC,
	}
	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize candidate broadcast: %v", err)
	}

	firstSource := testPeerID("first-source")
	first, err := sub.classifyBroadcastPayload(
		nil,
		msg,
		newKnownIdentifiedBroadcastPayload(payload, bytes.Repeat([]byte{0x11}, 32)),
		DeliveryFEC,
		false,
		firstSource,
	)
	if err != nil {
		t.Fatalf("classify first candidate: %v", err)
	}
	if first.disposition != broadcastDispositionAccept {
		t.Fatal("first candidate was not accepted")
	}
	node.acceptBroadcast(first.accepted)
	if _, _, err = node.decodedBroadcasts.get("tonNode.newBlockCandidateBroadcast", downloaded.ID); !errors.Is(err, errDecodedBroadcastProcessed) {
		t.Fatalf("first candidate cache state = %v, want processed", err)
	}

	secondSource := testPeerID("second-source")
	second, err := sub.classifyBroadcastPayload(
		nil,
		msg,
		newKnownIdentifiedBroadcastPayload(payload, bytes.Repeat([]byte{0x22}, 32)),
		DeliveryTwoStep,
		false,
		secondSource,
	)
	if err != nil {
		t.Fatalf("classify repeated candidate: %v", err)
	}
	if second.disposition != broadcastDispositionAccept {
		t.Fatal("repeated candidate was not accepted for relay")
	}
	node.acceptBroadcast(second.accepted)

	key := tnstore.BlockKey(downloaded.ID)
	cached := node.shardCandidateCache.candidates[key]
	if cached.block.sourcePeerID != firstSource {
		t.Fatalf("processed candidate ran pipeline again: source=%x want=%x", cached.block.sourcePeerID, firstSource)
	}
}

func TestAcceptedIdentifiedBroadcastWithoutTargetsDoesNotSerializePayload(t *testing.T) {
	node := newTestNode(t)
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	})
	msg := IhrMessageBroadcast{Message: IhrMessage{Data: []byte{0x01}}}
	payload := newIdentifiedBroadcastPayload(msg, bytes.Repeat([]byte{0xBC}, 32))

	result, err := sub.classifyBroadcastPayload(nil, msg, payload, DeliveryTwoStep, false, testPeerID("source"))
	if err != nil {
		t.Fatalf("classify identified broadcast: %v", err)
	}
	if result.disposition != broadcastDispositionAccept || result.accepted.rebroadcast == nil {
		t.Fatal("identified broadcast was not accepted")
	}
	node.acceptBroadcast(result.accepted)

	if payload.serialized {
		t.Fatal("accepted broadcast without rebroadcast targets serialized payload")
	}
}

func TestBroadcastPipelineObserverCapturesHotPathStages(t *testing.T) {
	node := newTestNode(t)
	node.SetBlockCacheObserver(&testBlockCacheObserver{nonfinalEnabled: true})
	observer := &testBroadcastPipelineObserver{}
	node.SetBroadcastPipelineObserver(observer)

	sourceID := testPeerID("source")
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:         "custom.private-a",
			Kind:         overlayKindCustomFixed,
			ShortID:      []byte{0x01, 0x02, 0x03},
			BlockSenders: map[PeerID]struct{}{sourceID: {}},
		},
		log: discardLogger(),
	})

	downloaded := testShardBroadcastDownloadedBlock(t, 206, 0x206)
	candidate := tonnodeapi.NewBlockCandidateBroadcast{
		ID:   downloaded.ID,
		Data: downloaded.BlockBOC,
	}
	if err := sub.handleOverlayBroadcast(nil, candidate, DeliveryTwoStep, true, sourceID); !errors.Is(err, overlay.ErrBroadcastRejected) {
		t.Fatalf("handle candidate broadcast error = %v, want broadcast rejected", err)
	}
	observer.requireStage(t, broadcastPipelineStageClassify, "tonNode.newBlockCandidateBroadcast", DeliveryTwoStep, broadcastPipelineResultDrop)
	observer.requireStage(t, broadcastPipelineStageCandidateDecode, "tonNode.newBlockCandidateBroadcast", DeliveryTwoStep, broadcastPipelineResultError)

	desc := tonnodeapi.NewShardBlockBroadcast{
		Block: tonnodeapi.NewShardBlock{
			ID:      downloaded.ID,
			CCSeqno: 7,
			Data:    []byte{0x01},
		},
	}
	if err := sub.handleOverlayBroadcast(nil, desc, DeliveryFEC, true, sourceID); err != nil {
		t.Fatalf("handle shard description broadcast: %v", err)
	}
	observer.requireStage(t, broadcastPipelineStageShardDescValidate, "tonNode.newShardBlockBroadcast", DeliveryFEC, broadcastPipelineResultSuccess)

	finality := BlockFinalityBroadcast{
		ID: downloaded.ID,
		SignatureSet: tonnodeapi.SignatureSetSimplex{
			Final:            false,
			CatchainSeqno:    7,
			ValidatorSetHash: 8,
			SessionID:        bytes.Repeat([]byte{0x44}, 32),
			Slot:             3,
			Candidate: ton.ConsensusCandidateHashDataOrdinary{
				Block:            downloaded.ID,
				CollatedFileHash: bytes.Repeat([]byte{0x45}, 32),
				Parent:           ton.ConsensusCandidateWithoutParents{},
			},
		},
	}
	if err := sub.handleOverlayBroadcast(nil, finality, DeliverySimple, true, sourceID); err != nil {
		t.Fatalf("handle block finality broadcast: %v", err)
	}
	observer.requireStage(t, broadcastPipelineStageFinalitySigCheck, blockFinalityBroadcastKind, DeliverySimple, broadcastPipelineResultSuccess)
	observer.requireStage(t, broadcastPipelineStageFinalityAssemble, blockFinalityBroadcastKind, DeliverySimple, broadcastPipelineResultMiss)

	node.acceptBroadcast(acceptedBroadcast{
		fingerprint: "hot-cache",
		deduped:     true,
		event: &BroadcastEvent{
			Overlay:    "basechain",
			Kind:       downloaded.Kind,
			Delivery:   DeliverySimple,
			Block:      downloaded.ID,
			Downloaded: &downloaded,
		},
	})
	observer.requireStage(t, broadcastPipelineStageHotCacheNotify, "tonNode.blockBroadcast", DeliverySimple, broadcastPipelineResultSuccess)

	if _, err := node.shardBroadcastBlock(downloaded.ID); err != nil {
		t.Fatalf("pop hot shard block: %v", err)
	}
	observer.requireStage(t, broadcastPipelineStageExactPop, "tonNode.blockBroadcast", "", broadcastPipelineResultSuccess)
}

func TestClassifyShardBlockBroadcastDropsWhenSignaturePrecheckFails(t *testing.T) {
	node := newTestNode(t)
	node.signatureVerifier = testRejectBroadcastSignatureVerifier{err: errors.New("bad signatures")}
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	})

	block := testBlockID(0, topShard, 202)
	msg := tonnodeapi.NewShardBlockBroadcast{
		Block: tonnodeapi.NewShardBlock{
			ID:      block,
			CCSeqno: 7,
			Data:    []byte{0x01},
		},
	}

	accepted := sub.classifyBroadcast(nil, msg, []byte{0x01}, DeliveryFEC, false, testPeerID("peer"))
	if accepted != nil {
		t.Fatalf("shard block broadcast with failed signature precheck was accepted: %+v", accepted)
	}
	if got := testBroadcastDropStatCount(node, "basechain", "tonNode.newShardBlockBroadcast", "signature_check_failed"); got != 1 {
		t.Fatalf("dropped broadcast count = %d, want 1", got)
	}

	duplicate := sub.classifyBroadcast(nil, msg, []byte{0x01}, DeliveryFEC, false, testPeerID("peer"))
	if duplicate != nil {
		t.Fatalf("duplicate shard block broadcast was accepted: %+v", duplicate)
	}
	if got := testBroadcastDropStatCount(node, "basechain", "tonNode.newShardBlockBroadcast", "seen"); got != 1 {
		t.Fatalf("duplicate shard block drop count = %d, want 1", got)
	}
	if got := testBroadcastDropStatCount(node, "basechain", "tonNode.newShardBlockBroadcast", "signature_check_failed"); got != 1 {
		t.Fatalf("signature precheck drop count after duplicate = %d, want 1", got)
	}
}

func TestBroadcastTransientSignatureFailureReleasesDeduper(t *testing.T) {
	node := newTestNode(t)
	now := time.Now()
	fingerprint := "transient-signature-failure"
	if !node.deduper.Mark(fingerprint, now) {
		t.Fatal("fingerprint was not marked")
	}

	node.forgetBroadcastFingerprintIfRetryable(fingerprint, ErrBroadcastSignatureRetryable)
	if node.deduper.Seen(fingerprint, time.Now()) {
		t.Fatal("transient signature failure poisoned the broadcast deduper")
	}

	if !node.deduper.Mark(fingerprint, now.Add(time.Second)) {
		t.Fatal("fingerprint was not accepted after transient failure")
	}

	for _, err := range []error{errors.New("bad signatures"), tnstore.ErrNotFound} {
		node.forgetBroadcastFingerprintIfRetryable(fingerprint, err)
		if !node.deduper.Seen(fingerprint, now.Add(2*time.Second)) {
			t.Fatalf("permanent signature failure %v should stay deduped", err)
		}
	}
}

func TestHandleShardBroadcastRetryableSignatureFailureReturnsRetry(t *testing.T) {
	node := newTestNode(t)
	node.signatureVerifier = testRejectBroadcastSignatureVerifier{
		err: fmt.Errorf("%w: validator set is not ready", ErrBroadcastSignatureRetryable),
	}
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			Kind:    overlayKindPublicShard,
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	})
	msg := tonnodeapi.NewShardBlockBroadcast{
		Block: tonnodeapi.NewShardBlock{
			ID:      testBlockID(0, topShard, 202),
			CCSeqno: 7,
			Data:    []byte{0x01},
		},
	}
	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize shard broadcast: %v", err)
	}
	fingerprint := broadcastFingerprint(sub.spec.ShortID, payload)

	for attempt := 0; attempt < 2; attempt++ {
		disposition := sub.handleOverlayBroadcastPayload(
			nil,
			msg,
			newKnownBroadcastPayload(payload),
			DeliveryFEC,
			false,
			testPeerID("retryable-source"),
		)
		if disposition != overlay.BroadcastDispositionRetry {
			t.Fatalf("attempt %d disposition=%v, want retry", attempt+1, disposition)
		}
		if node.deduper.Seen(fingerprint, time.Now()) {
			t.Fatalf("attempt %d left retryable fingerprint committed", attempt+1)
		}
	}
}

func TestHandleFullBlockRetryableSignatureFailureReturnsRetry(t *testing.T) {
	node := newTestNode(t)
	node.signatureVerifier = testRejectBroadcastSignatureVerifier{
		err: fmt.Errorf("%w: validator set is not ready", ErrBroadcastSignatureRetryable),
	}
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			Kind:    overlayKindPublicShard,
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	})
	blockCell := testPeerBlockRoot(t, 0, topShard, 207)
	blockBOC := serializeCompressedBlockRoot(blockCell)
	rootHash := blockCell.HashKey()
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     topShard,
		SeqNo:     207,
		RootHash:  rootHash[:],
		FileHash:  hashSimpleBroadcastPayload(blockBOC),
	}
	msg := tonnodeapi.BlockBroadcastCompressedV2{
		ID: block,
		SignatureSet: tonnodeapi.SignatureSetSimplex{
			SessionID: bytes.Repeat([]byte{0x44}, 32),
			Candidate: ton.ConsensusCandidateHashDataOrdinary{
				Block:            block,
				CollatedFileHash: bytes.Repeat([]byte{0x46}, 32),
				Parent:           ton.ConsensusCandidateWithoutParents{},
			},
		},
		Proof:          testBlockProofCell(t, block, nil).ToBOC(),
		DataCompressed: []byte{0x01},
	}
	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize full block broadcast: %v", err)
	}
	fingerprint := broadcastFingerprint(sub.spec.ShortID, payload)

	for attempt := 0; attempt < 2; attempt++ {
		disposition := sub.handleOverlayBroadcastPayload(
			nil,
			msg,
			newKnownBroadcastPayload(payload),
			DeliveryFEC,
			false,
			testPeerID("retryable-full-source"),
		)
		if disposition != overlay.BroadcastDispositionRetry {
			t.Fatalf("attempt %d disposition=%v, want retry", attempt+1, disposition)
		}
		if node.deduper.Seen(fingerprint, time.Now()) {
			t.Fatalf("attempt %d left retryable full-block fingerprint committed", attempt+1)
		}
	}
}

func TestHandleShardBroadcastPermanentDropsReturnIgnore(t *testing.T) {
	node := newTestNode(t)
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			Kind:    overlayKindPublicShard,
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	})
	sourceID := testPeerID("permanent-drop-source")
	invalid := tonnodeapi.NewShardBlockBroadcast{
		Block: tonnodeapi.NewShardBlock{ID: testBlockID(0, topShard, 205)},
	}
	if disposition := sub.handleOverlayBroadcastPayload(
		nil,
		invalid,
		newKnownBroadcastPayload([]byte{0x01}),
		DeliverySimple,
		false,
		sourceID,
	); disposition != overlay.BroadcastDispositionIgnore {
		t.Fatalf("invalid payload disposition=%v, want ignore", disposition)
	}

	valid := tonnodeapi.NewShardBlockBroadcast{
		Block: tonnodeapi.NewShardBlock{
			ID:      testBlockID(0, topShard, 206),
			CCSeqno: 7,
			Data:    []byte{0x01},
		},
	}
	payload, err := tl.Serialize(valid, true)
	if err != nil {
		t.Fatalf("serialize valid shard broadcast: %v", err)
	}
	if disposition := sub.handleOverlayBroadcastPayload(
		nil,
		valid,
		newKnownBroadcastPayload(payload),
		DeliverySimple,
		false,
		sourceID,
	); disposition != overlay.BroadcastDispositionAcceptAndRelay {
		t.Fatalf("first valid payload disposition=%v, want accept", disposition)
	}
	if disposition := sub.handleOverlayBroadcastPayload(
		nil,
		valid,
		newKnownBroadcastPayload(payload),
		DeliverySimple,
		false,
		sourceID,
	); disposition != overlay.BroadcastDispositionIgnore {
		t.Fatalf("seen payload disposition=%v, want ignore", disposition)
	}
}

func TestAcceptedShardBlockBroadcastSkipsSameOverlayFECRebroadcast(t *testing.T) {
	node := newTestNode(t)
	source := testRebroadcastQueuePeer("source")
	target := testRebroadcastQueuePeer("target")
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
		peers: map[PeerID]*overlayPeer{
			source.id: source,
			target.id: target,
		},
	})
	block := testBlockID(0, topShard, 203)
	msg := tonnodeapi.NewShardBlockBroadcast{
		Block: tonnodeapi.NewShardBlock{
			ID:      block,
			CCSeqno: 7,
			Data:    []byte{0x01, 0x02},
		},
	}
	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize shard broadcast: %v", err)
	}

	accepted := sub.classifyBroadcast(source, msg, payload, DeliveryFEC, false, PeerID{})
	if accepted == nil {
		t.Fatal("expected shard block broadcast to be accepted")
	}
	if accepted.rebroadcast == nil || !accepted.rebroadcast.skipOverlayRebroadcast {
		t.Fatal("expected ordinary FEC broadcast to skip app-level same-overlay rebroadcast")
	}
	node.acceptBroadcast(*accepted)

	if _, ok := source.rebroadcastQueue.TryPop(); ok {
		t.Fatal("source peer should not receive its own shard block rebroadcast")
	}
	if _, ok := target.rebroadcastQueue.TryPop(); ok {
		t.Fatal("target peer should not receive app-level shard block rebroadcast when FEC relay is enabled")
	}
	if got := testBroadcastStatCount(
		node,
		"accepted",
		"basechain",
		"tonNode.newShardBlockBroadcast",
		DeliveryFEC,
	); got != 1 {
		t.Fatalf("accepted broadcast count = %d, want 1", got)
	}

	duplicate := sub.classifyBroadcast(source, msg, payload, DeliveryFEC, false, PeerID{})
	if duplicate != nil {
		t.Fatalf("duplicate shard block broadcast was accepted: %+v", duplicate)
	}
	if got := testBroadcastDropStatCount(node, "basechain", "tonNode.newShardBlockBroadcast", "seen"); got != 1 {
		t.Fatalf("seen broadcast drop count = %d, want 1", got)
	}
	if got := testBroadcastStatCount(
		node,
		"accepted",
		"basechain",
		"tonNode.newShardBlockBroadcast",
		DeliveryFEC,
	); got != 1 {
		t.Fatalf("duplicate accepted broadcast count = %d, want 1", got)
	}
}

func TestAcceptedSimpleBroadcastUsesBoundedAppRebroadcastOnly(t *testing.T) {
	node := newTestNode(t)
	source := testRebroadcastQueuePeer("simple-source")
	peers := []*overlayPeer{source}
	peerMap := map[PeerID]*overlayPeer{source.id: source}
	for i := 0; i < rebroadcastFanout+2; i++ {
		peer := testRebroadcastQueuePeer(fmt.Sprintf("simple-target-%d", i))
		peers = append(peers, peer)
		peerMap[peer.id] = peer
	}
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			Kind:    overlayKindPublicShard,
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log:   discardLogger(),
		peers: peerMap,
	})
	msg := tonnodeapi.NewShardBlockBroadcast{
		Block: tonnodeapi.NewShardBlock{
			ID:      testBlockID(0, topShard, 204),
			CCSeqno: 7,
			Data:    []byte{0x01, 0x02},
		},
	}
	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize shard broadcast: %v", err)
	}

	accepted := sub.classifyBroadcast(source, msg, payload, DeliverySimple, false, source.id)
	if accepted == nil || accepted.rebroadcast == nil {
		t.Fatal("expected simple shard broadcast to be accepted")
	}
	if accepted.rebroadcast.skipOverlayRebroadcast {
		t.Fatal("simple broadcast incorrectly skipped the bounded app rebroadcast path")
	}
	if fanout := sub.rebroadcastFanoutForRequest(*accepted.rebroadcast); fanout != rebroadcastFanout {
		t.Fatalf("simple app rebroadcast fanout=%d, want %d", fanout, rebroadcastFanout)
	}

	node.acceptBroadcast(*accepted)
	if got := countQueuedRebroadcasts(peerMap, false); got != rebroadcastFanout {
		t.Fatalf("queued simple app rebroadcasts=%d, want %d", got, rebroadcastFanout)
	}
	if _, ok := source.rebroadcastQueue.TryPop(); ok {
		t.Fatal("simple source received its own app rebroadcast")
	}
}

func TestCustomOverlayRejectsUnauthorizedBlockSender(t *testing.T) {
	node := newTestNode(t)
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:         "custom.private-a",
			Kind:         overlayKindCustomFixed,
			ShortID:      []byte{0x02, 0x03, 0x04},
			BlockSenders: map[PeerID]struct{}{testPeerID("allowed"): {}},
		},
		log: discardLogger(),
	})
	block := testBlockID(0, topShard, 204)
	msg := tonnodeapi.NewShardBlockBroadcast{
		Block: tonnodeapi.NewShardBlock{
			ID:      block,
			CCSeqno: 7,
			Data:    []byte{0x01},
		},
	}
	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize shard broadcast: %v", err)
	}

	accepted := sub.classifyBroadcast(nil, msg, payload, DeliveryFEC, false, testPeerID("blocked"))
	if accepted != nil {
		t.Fatalf("unauthorized custom broadcast was accepted: %+v", accepted)
	}
	if got := testBroadcastDropStatCount(node, "custom.private-a", "tonNode.newShardBlockBroadcast", "unauthorized_sender"); got != 1 {
		t.Fatalf("unauthorized drop count = %d, want 1", got)
	}
}

func TestCustomOverlayRejectsUnauthorizedMessageSender(t *testing.T) {
	node := newTestNode(t)
	admission := &testExternalMessageAdmission{}
	node.externalMessageAdmission = admission
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:       "custom.private-a",
			Kind:       overlayKindCustomFixed,
			ShortID:    []byte{0x02, 0x03, 0x04},
			MsgSenders: map[PeerID]int{testPeerID("allowed"): 17},
		},
		log: discardLogger(),
	})
	msg := tonnodeapi.NewExternalMessageBroadcast{
		Message: tonnodeapi.ExternalMessage{Data: testExternalMessageBOC(t)},
	}
	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize external broadcast: %v", err)
	}

	accepted := sub.classifyBroadcast(nil, msg, payload, DeliveryFEC, false, testPeerID("blocked"))
	if accepted != nil {
		t.Fatalf("unauthorized custom external broadcast was accepted: %+v", accepted)
	}
	if len(admission.events) != 0 {
		t.Fatalf("unauthorized admission events = %d, want 0", len(admission.events))
	}
	if got := testBroadcastDropStatCount(node, "custom.private-a", "tonNode.externalMessageBroadcast", "unauthorized_sender"); got != 1 {
		t.Fatalf("unauthorized drop count = %d, want 1", got)
	}
}

func TestCustomTwoStepBroadcastSkipsSameOverlayRebroadcastButKeepsFanoutPayload(t *testing.T) {
	node := newTestNode(t)
	observer := &testBroadcastPipelineObserver{}
	node.SetBroadcastPipelineObserver(observer)
	sourceID := testPeerID("source")
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:         "custom.private-a",
			Kind:         overlayKindCustomFixed,
			ShortID:      []byte{0x02, 0x03, 0x04},
			BlockSenders: map[PeerID]struct{}{sourceID: {}},
		},
		log: discardLogger(),
	})

	block := testBlockID(0, topShard, 204)
	msg := tonnodeapi.NewShardBlockBroadcast{
		Block: tonnodeapi.NewShardBlock{
			ID:      block,
			CCSeqno: 7,
			Data:    []byte{0x01},
		},
	}
	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize shard block broadcast: %v", err)
	}

	accepted := sub.classifyBroadcast(nil, msg, payload, DeliveryTwoStep, true, sourceID)
	if accepted == nil {
		t.Fatal("expected custom two-step broadcast to be accepted")
	}
	if accepted.rebroadcast == nil {
		t.Fatal("expected accepted two-step broadcast to keep fanout payload")
	}
	if !accepted.rebroadcast.skipOverlayRebroadcast {
		t.Fatal("expected two-step broadcast to skip same-overlay app rebroadcast")
	}
	if !bytes.Equal(accepted.rebroadcast.payload, payload) {
		t.Fatal("expected two-step rebroadcast payload to be preserved for custom fanout")
	}
	observer.requireStage(t, broadcastPipelineStageShardDescValidate, "tonNode.newShardBlockBroadcast", DeliveryTwoStep, broadcastPipelineResultSuccess)
}

func TestBlockCandidateDecodeFailureDropsBeforeRebroadcast(t *testing.T) {
	raw := testShardBroadcastDownloadedBlock(t, 208, 0x208)
	tests := []struct {
		name string
		kind string
		msg  any
	}{
		{
			name: "raw_parse_failure",
			kind: "tonNode.newBlockCandidateBroadcast",
			msg: tonnodeapi.NewBlockCandidateBroadcast{
				ID:   raw.ID,
				Data: raw.BlockBOC,
			},
		},
		{
			name: "compressed_decode_failure",
			kind: "tonNode.newBlockCandidateBroadcastCompressed",
			msg: tonnodeapi.NewBlockCandidateBroadcastCompressed{
				ID:         testBlockID(0, topShard, 209),
				Compressed: []byte{0x01},
			},
		},
		{
			name: "compressed_v2_decode_failure",
			kind: "tonNode.newBlockCandidateBroadcastCompressedV2",
			msg: tonnodeapi.NewBlockCandidateBroadcastCompressedV2{
				ID:         testBlockID(0, topShard, 210),
				Compressed: []byte{0x01},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := newTestNode(t)
			observer := &testBroadcastPipelineObserver{}
			node.SetBroadcastPipelineObserver(observer)

			source := testRebroadcastQueuePeer("source")
			target := testRebroadcastQueuePeer("target")
			sub := testOverlaySubscription(&overlaySubscription{
				node: node,
				spec: overlaySpec{
					Name:    "basechain",
					ShortID: []byte{0x01, 0x02, 0x03},
				},
				log: discardLogger(),
				peers: map[PeerID]*overlayPeer{
					source.id: source,
					target.id: target,
				},
			})
			payload, err := tl.Serialize(tt.msg, true)
			if err != nil {
				t.Fatalf("serialize candidate broadcast: %v", err)
			}

			disposition := sub.handleOverlayBroadcastPayload(source, tt.msg, newKnownBroadcastPayload(payload), DeliverySimple, false, source.id)
			if disposition != overlay.BroadcastDispositionIgnore {
				t.Fatalf("handle candidate broadcast disposition = %v, want ignore", disposition)
			}

			observer.requireStage(t, broadcastPipelineStageClassify, tt.kind, DeliverySimple, broadcastPipelineResultDrop)
			observer.requireStage(t, broadcastPipelineStageCandidateDecode, tt.kind, DeliverySimple, broadcastPipelineResultError)
			if got := testBroadcastDropStatCount(node, "basechain", tt.kind, "decode_failed"); got != 1 {
				t.Fatalf("decode failure drop count = %d, want 1", got)
			}
			if !node.deduper.Seen(broadcastFingerprint(sub.spec.ShortID, payload), time.Now()) {
				t.Fatal("decode-failed candidate should stay deduped")
			}
			if _, ok := target.rebroadcastQueue.TryPop(); ok {
				t.Fatal("decode-failed candidate reached same-overlay rebroadcast queue")
			}
		})
	}
}

func TestCustomOverlayDropsIHRBroadcast(t *testing.T) {
	node := newTestNode(t)
	sourceID := testPeerID("source")
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:       "custom.private-a",
			Kind:       overlayKindCustomFixed,
			ShortID:    []byte{0x01, 0x02, 0x03},
			MsgSenders: map[PeerID]int{sourceID: 1},
		},
		log: discardLogger(),
	})
	msg := IhrMessageBroadcast{
		Message: IhrMessage{Data: []byte{0x01}},
	}
	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize ihr broadcast: %v", err)
	}

	accepted := sub.classifyBroadcast(nil, msg, payload, DeliverySimple, false, sourceID)
	if accepted != nil {
		t.Fatalf("custom IHR broadcast was accepted: %+v", accepted)
	}
	if got := testBroadcastDropStatCount(node, "custom.private-a", "tonNode.ihrMessageBroadcast", "unsupported_custom_broadcast"); got != 1 {
		t.Fatalf("unsupported custom IHR drop count = %d, want 1", got)
	}
}

func TestCustomOverlayDropsSelfBroadcast(t *testing.T) {
	node := newTestNode(t)
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:         "custom.private-a",
			Kind:         overlayKindCustomFixed,
			ShortID:      []byte{0x01, 0x02, 0x03},
			BlockSenders: map[PeerID]struct{}{node.localID: {}},
		},
		log: discardLogger(),
	})
	block := testBlockID(0, topShard, 211)
	msg := tonnodeapi.NewShardBlockBroadcast{
		Block: tonnodeapi.NewShardBlock{
			ID:      block,
			CCSeqno: 7,
			Data:    []byte{0x01},
		},
	}
	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize shard broadcast: %v", err)
	}

	accepted := sub.classifyBroadcast(nil, msg, payload, DeliverySimple, false, node.localID)
	if accepted != nil {
		t.Fatalf("custom self broadcast was accepted: %+v", accepted)
	}
	if got := testBroadcastDropStatCount(node, "custom.private-a", "tonNode.newShardBlockBroadcast", "self"); got != 1 {
		t.Fatalf("custom self drop count = %d, want 1", got)
	}
}

func TestPublicOverlayAcceptsIHRBroadcast(t *testing.T) {
	node := newTestNode(t)
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	})
	msg := IhrMessageBroadcast{
		Message: IhrMessage{Data: []byte{0x01}},
	}
	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize ihr broadcast: %v", err)
	}

	accepted := sub.classifyBroadcast(nil, msg, payload, DeliverySimple, false, testPeerID("source"))
	if accepted == nil || accepted.rebroadcast == nil {
		t.Fatal("expected public IHR broadcast to be accepted")
	}
}

func TestAcceptedPublicExternalDoesNotFanOutToCustomOverlay(t *testing.T) {
	node := newTestNode(t)
	publicSpec := overlaySpec{
		Name:    "basechain",
		Kind:    overlayKindPublicShard,
		ShortID: bytes.Repeat([]byte{0x01}, PeerIDSize),
	}
	publicSub := mustGetOrCreateSubscription(t, node, publicSpec)

	customSpec := overlaySpec{
		Name:       "custom.private-a",
		Kind:       overlayKindCustomFixed,
		ShortID:    bytes.Repeat([]byte{0x04}, PeerIDSize),
		MsgSenders: map[PeerID]int{node.localID: 9},
	}
	customSub := mustGetOrCreateSubscription(t, node, customSpec)
	customSub.setActive(true, time.Time{})
	customPeer := testRebroadcastQueuePeer("custom-peer")
	customSub.peers[customPeer.id] = customPeer

	msg := tonnodeapi.NewExternalMessageBroadcast{
		Message: tonnodeapi.ExternalMessage{Data: testExternalMessageBOC(t)},
	}
	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize external broadcast: %v", err)
	}

	accepted := publicSub.classifyBroadcast(nil, msg, payload, DeliveryFEC, false, testPeerID("remote"))
	if accepted == nil {
		t.Fatal("expected public external broadcast to be accepted")
	}
	node.acceptBroadcast(*accepted)

	if snapshot, ok := customSub.twoStepQueueStatusSnapshot(); ok && snapshot.Items != 0 {
		t.Fatalf("custom two-step queue items = %d, want 0", snapshot.Items)
	}
	if _, ok := customPeer.localRebroadcastQueue.TryPop(); ok {
		t.Fatal("public external broadcast reached custom peer queue")
	}
}

func TestAcceptedShardBlockBroadcastFansOutToCustomOverlay(t *testing.T) {
	node := newTestNode(t)
	publicSpec := overlaySpec{
		Name:    "basechain",
		Kind:    overlayKindPublicShard,
		ShortID: bytes.Repeat([]byte{0x01}, PeerIDSize),
	}
	publicSub := mustGetOrCreateSubscription(t, node, publicSpec)

	customSpec := overlaySpec{
		Name:         "custom.private-a",
		Kind:         overlayKindCustomFixed,
		ShortID:      bytes.Repeat([]byte{0x04}, PeerIDSize),
		BlockSenders: map[PeerID]struct{}{node.localID: {}},
	}
	customSub := mustGetOrCreateSubscription(t, node, customSpec)
	customPeer := testRebroadcastQueuePeer("custom-peer")
	customSub.peers[customPeer.id] = customPeer

	block := testBlockID(0, topShard, 205)
	msg := tonnodeapi.NewShardBlockBroadcast{
		Block: tonnodeapi.NewShardBlock{
			ID:      block,
			CCSeqno: 8,
			Data:    []byte{0x02},
		},
	}
	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize shard broadcast: %v", err)
	}

	accepted := publicSub.classifyBroadcast(nil, msg, payload, DeliveryFEC, false, testPeerID("remote"))
	if accepted == nil {
		t.Fatal("expected public shard block broadcast to be accepted")
	}
	node.acceptBroadcast(*accepted)

	got, ok := customPeer.localRebroadcastQueue.TryPop()
	if !ok {
		t.Fatal("expected custom overlay fanout")
	}
	if got.kind != "tonNode.newShardBlockBroadcast" || !got.local {
		t.Fatalf("unexpected custom fanout request: %#v", got)
	}
}

func TestPendingBlockBroadcastDecodeFansOutToCustomOverlay(t *testing.T) {
	node := newTestNode(t)
	publicSpec := overlaySpec{
		Name:    "basechain",
		Kind:    overlayKindPublicShard,
		ShortID: bytes.Repeat([]byte{0x01}, PeerIDSize),
	}
	publicSub := mustGetOrCreateSubscription(t, node, publicSpec)
	source := testRebroadcastQueuePeer("source")
	target := testRebroadcastQueuePeer("target")
	publicSub.peers[source.id] = source
	publicSub.peers[target.id] = target

	customSpec := overlaySpec{
		Name:         "custom.private-a",
		Kind:         overlayKindCustomFixed,
		ShortID:      bytes.Repeat([]byte{0x04}, PeerIDSize),
		BlockSenders: map[PeerID]struct{}{node.localID: {}},
	}
	customSub := mustGetOrCreateSubscription(t, node, customSpec)
	customPeer := testRebroadcastQueuePeer("custom-peer")
	customSub.peers[customPeer.id] = customPeer

	blockCell := testPeerBlockRoot(t, 0, topShard, 212)
	blockBOC := serializeCompressedBlockRoot(blockCell)
	fileHash := hashSimpleBroadcastPayload(blockBOC)
	blockHash := blockCell.HashKey()
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     topShard,
		SeqNo:     212,
		RootHash:  blockHash[:],
		FileHash:  fileHash,
	}
	proofCell := testBlockProofCell(t, block, nil)
	compressed, err := cell.CompressBOC([]*cell.Cell{blockCell}, cell.CompressionImprovedStructureLZ4, nil)
	if err != nil {
		t.Fatalf("compress block broadcast v2: %v", err)
	}
	msg := tonnodeapi.BlockBroadcastCompressedV2{
		ID: block,
		SignatureSet: tonnodeapi.SignatureSetSimplex{
			Final:     false,
			SessionID: bytes.Repeat([]byte{0x44}, 32),
			Candidate: ton.ConsensusCandidateHashDataOrdinary{
				Block:            block,
				CollatedFileHash: bytes.Repeat([]byte{0x46}, 32),
				Parent:           ton.ConsensusCandidateWithoutParents{},
			},
		},
		Proof:          proofCell.ToBOC(),
		DataCompressed: compressed,
	}
	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize pending broadcast: %v", err)
	}

	req := rebroadcastRequest{
		subscription: publicSub,
		kind:         "tonNode.blockBroadcastCompressedV2",
		payload:      payload,
		sourcePeerID: source.id,
	}
	node.processPendingBlockBroadcastDecodeRequests(context.Background(), []pendingBlockBroadcastDecode{{
		fingerprint:  "pending-v2",
		overlay:      publicSpec.Name,
		delivery:     DeliverySimple,
		kind:         "tonNode.blockBroadcastCompressedV2",
		block:        block,
		sourcePeerID: source.id,
		receivedAt:   time.Now(),
		msg:          msg,
		proofRoot:    proofCell,
		rebroadcast:  &req,
	}})

	if _, ok := target.rebroadcastQueue.TryPop(); ok {
		t.Fatal("pending completion repeated same-overlay rebroadcast")
	}
	snapshot, ok := customSub.twoStepQueueStatusSnapshot()
	if !ok {
		t.Fatal("custom two-step queue was not initialized")
	}
	if snapshot.Items != 1 {
		t.Fatalf("custom fanout queue items = %d, want 1", snapshot.Items)
	}
}

func TestPendingCompressedBroadcastRetriesAfterMissedReadyNotify(t *testing.T) {
	node := newTestNode(t)
	node.storage = newTestPebbleStore(t)

	state := cell.BeginCell().MustStoreUInt(0x1234, 16).EndCell()
	provider := &testFailOnceCompressedStateProvider{state: state}
	provider.failOnce.Store(true)
	node.compressedState = provider

	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			Kind:    overlayKindPublicShard,
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	})

	blockCell := testPeerBlockRoot(t, 0, topShard, 213)
	blockBOC := serializeCompressedBlockRoot(blockCell)
	fileHash := hashSimpleBroadcastPayload(blockBOC)
	blockHash := blockCell.HashKey()
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     topShard,
		SeqNo:     213,
		RootHash:  blockHash[:],
		FileHash:  fileHash,
	}
	proofBOC := testPeerBlockProofEnvelopeBOC(t, block, blockCell, nil)
	proofCell, err := parseDownloadedBlockProof(proofBOC)
	if err != nil {
		t.Fatalf("parse pending broadcast proof: %v", err)
	}
	prev, err := compressedBlockPreviousState(block, proofCell)
	if err != nil {
		t.Fatalf("previous block from proof: %v", err)
	}

	node.processPendingBlockBroadcastDecodesForPrevAsync(prev)

	compressed, err := cell.CompressBOC([]*cell.Cell{blockCell}, cell.CompressionImprovedStructureLZ4WithState, state)
	if err != nil {
		t.Fatalf("compress state-aware block broadcast: %v", err)
	}
	msg := tonnodeapi.BlockBroadcastCompressedV2{
		ID: block,
		SignatureSet: tonnodeapi.SignatureSetSimplex{
			Final:     false,
			SessionID: bytes.Repeat([]byte{0x44}, 32),
			Candidate: ton.ConsensusCandidateHashDataOrdinary{
				Block:            block,
				CollatedFileHash: bytes.Repeat([]byte{0x46}, 32),
				Parent:           ton.ConsensusCandidateWithoutParents{},
			},
		},
		Proof:          proofBOC,
		DataCompressed: compressed,
	}
	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize pending broadcast: %v", err)
	}

	result, err := sub.classifyBroadcastPayload(nil, msg, newKnownBroadcastPayload(payload), DeliverySimple, false, testPeerID("source"))
	if err != nil {
		t.Fatalf("classify pending broadcast: %v", err)
	}
	if result.disposition != broadcastDispositionAccept {
		t.Fatal("expected pending broadcast placeholder")
	}
	node.acceptBroadcast(result.accepted)

	eventCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	event, ok := node.eventQueue.Pop(eventCtx)
	if !ok {
		t.Fatal("pending broadcast was not retried after enqueue")
	}
	if event.Downloaded == nil || !event.Downloaded.ID.Equals(&block) {
		t.Fatalf("unexpected pending broadcast event: %#v", event)
	}

	node.pendingBroadcastMx.Lock()
	pending := len(node.pendingBroadcasts)
	node.pendingBroadcastMx.Unlock()
	if pending != 0 {
		t.Fatalf("pending broadcasts = %d, want 0", pending)
	}
}

func TestClassifyExternalMessageBroadcastCachesByBodyHash(t *testing.T) {
	node := newTestNode(t)
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	})
	data := testExternalMessageBOC(t)
	msg := tonnodeapi.NewExternalMessageBroadcast{
		Message: tonnodeapi.ExternalMessage{Data: data},
	}
	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize external broadcast: %v", err)
	}

	first := sub.classifyBroadcast(nil, msg, payload, DeliveryFEC, false, testPeerID("peer"))
	if first == nil || first.rebroadcast == nil {
		t.Fatal("expected first external broadcast to be accepted")
	}

	second := sub.classifyBroadcast(nil, msg, payload, DeliveryFEC, false, testPeerID("peer"))
	if second != nil {
		t.Fatalf("duplicate external broadcast was accepted: %+v", second)
	}
}

func TestClassifyExternalMessageBroadcastRunsAdmission(t *testing.T) {
	node := newTestNode(t)
	admission := &testExternalMessageAdmission{}
	node.externalMessageAdmission = admission
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	})
	data := testExternalMessageBOC(t)
	msg := tonnodeapi.NewExternalMessageBroadcast{
		Message: tonnodeapi.ExternalMessage{Data: data},
	}
	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize external broadcast: %v", err)
	}

	accepted := sub.classifyBroadcast(nil, msg, payload, DeliveryFEC, false, testPeerID("peer"))
	if accepted == nil || accepted.rebroadcast == nil {
		t.Fatal("expected external broadcast to be accepted")
	}

	if len(admission.events) != 1 {
		t.Fatalf("admission events = %d, want 1", len(admission.events))
	}
	event := admission.events[0]
	if !bytes.Equal(event.Body, data) {
		t.Fatalf("admission body mismatch")
	}
	if event.Root == nil || event.Message == nil {
		t.Fatalf("admission parsed data is missing")
	}
	if event.IsLocal {
		t.Fatalf("broadcast external message IsLocal = true, want false")
	}
	if event.Priority != 0 {
		t.Fatalf("public external message priority = %d, want 0", event.Priority)
	}
}

func TestCustomOverlayExternalMessageUsesSenderPriority(t *testing.T) {
	node := newTestNode(t)
	admission := &testExternalMessageAdmission{}
	node.externalMessageAdmission = admission
	sourceID := testPeerID("custom-message-source")
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:       "custom.private-a",
			Kind:       overlayKindCustomFixed,
			ShortID:    []byte{0x02, 0x03, 0x04},
			MsgSenders: map[PeerID]int{sourceID: 17},
		},
		log: discardLogger(),
	})
	data := testExternalMessageBOC(t)
	msg := tonnodeapi.NewExternalMessageBroadcast{
		Message: tonnodeapi.ExternalMessage{Data: data},
	}
	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize external broadcast: %v", err)
	}

	accepted := sub.classifyBroadcast(nil, msg, payload, DeliveryFEC, false, sourceID)
	if accepted == nil {
		t.Fatal("expected custom external broadcast to be accepted")
	}
	if len(admission.events) != 1 {
		t.Fatalf("admission events = %d, want 1", len(admission.events))
	}
	event := admission.events[0]
	if event.IsLocal {
		t.Fatal("custom external message IsLocal = true, want false")
	}
	if event.Priority != 17 {
		t.Fatalf("custom external message priority = %d, want 17", event.Priority)
	}
}

func TestClassifyExternalMessageBroadcastDropsWhenAdmissionFails(t *testing.T) {
	node := newTestNode(t)
	wantErr := errors.New("reject external")
	node.externalMessageAdmission = &testExternalMessageAdmission{err: wantErr}
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	})
	data := testExternalMessageBOC(t)
	msg := tonnodeapi.NewExternalMessageBroadcast{
		Message: tonnodeapi.ExternalMessage{Data: data},
	}
	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize external broadcast: %v", err)
	}

	if accepted := sub.classifyBroadcast(nil, msg, payload, DeliveryFEC, false, testPeerID("peer")); accepted != nil {
		t.Fatalf("external broadcast was accepted after admission failure: %+v", accepted)
	}
}

func TestHandleOverlayBroadcastRejectsInvalidUntrustedFECForRelay(t *testing.T) {
	node := newTestNode(t)
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	})

	err := sub.handleOverlayBroadcast(nil, tonnodeapi.NewExternalMessageBroadcast{}, DeliveryFEC, false, testPeerID("peer"))
	if !errors.Is(err, overlay.ErrBroadcastRejected) {
		t.Fatalf("expected rejected untrusted FEC broadcast, got %v", err)
	}
}

func TestClassifyExternalMessageBroadcastLimitsPerAddress(t *testing.T) {
	node := newTestNode(t)
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	})

	for i := 0; i < extmsg.DefaultAddressLimit; i++ {
		data := testExternalMessageBOCWithBody(t, uint64(i))
		msg := tonnodeapi.NewExternalMessageBroadcast{
			Message: tonnodeapi.ExternalMessage{Data: data},
		}
		payload, err := tl.Serialize(msg, true)
		if err != nil {
			t.Fatalf("serialize external broadcast %d: %v", i, err)
		}
		accepted := sub.classifyBroadcast(nil, msg, payload, DeliveryFEC, false, testPeerID("peer"))
		if accepted == nil || accepted.rebroadcast == nil {
			t.Fatalf("external broadcast %d was not accepted", i)
		}
	}

	data := testExternalMessageBOCWithBody(t, uint64(extmsg.DefaultAddressLimit))
	msg := tonnodeapi.NewExternalMessageBroadcast{
		Message: tonnodeapi.ExternalMessage{Data: data},
	}
	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize external broadcast over limit: %v", err)
	}
	if accepted := sub.classifyBroadcast(nil, msg, payload, DeliveryFEC, false, testPeerID("peer")); accepted != nil {
		t.Fatalf("over-limit external broadcast was accepted: %+v", accepted)
	}
}

func TestClassifyExternalMessageBroadcastSkipsOwnMessage(t *testing.T) {
	node := newTestNode(t)
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	})
	data := []byte{0xAA, 0xBB}
	msg := tonnodeapi.NewExternalMessageBroadcast{
		Message: tonnodeapi.ExternalMessage{Data: data},
	}
	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize external broadcast: %v", err)
	}

	node.myExternalMessages.Mark(externalMessageFingerprint(sub.spec.ShortID, data), time.Now())

	accepted := sub.classifyBroadcast(nil, msg, payload, DeliveryFEC, false, testPeerID("peer"))
	if accepted != nil {
		t.Fatalf("own external broadcast was accepted: %+v", accepted)
	}
}

func TestClassifyExternalMessageBroadcastRejectsOversizeData(t *testing.T) {
	node := newTestNode(t)
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	})
	msg := tonnodeapi.NewExternalMessageBroadcast{
		Message: tonnodeapi.ExternalMessage{Data: make([]byte, maxOverlayPayloadSize+1)},
	}

	accepted := sub.classifyBroadcast(nil, msg, []byte{0x01}, DeliveryFEC, false, testPeerID("peer"))
	if accepted != nil {
		t.Fatalf("oversize external broadcast was accepted: %+v", accepted)
	}
}

func TestHandleOverlayBroadcastAdmissionClosedRejectsRelayedBroadcastWithoutProcessing(t *testing.T) {
	for _, delivery := range []Delivery{DeliverySimple, DeliveryFEC, DeliveryTwoStep} {
		t.Run(string(delivery), func(t *testing.T) {
			node := newTestNode(t)
			node.broadcastAdmission = testBroadcastAdmission(false)
			sub := testOverlaySubscription(&overlaySubscription{
				node: node,
				spec: overlaySpec{
					Name:    "basechain",
					ShortID: []byte{0x01, 0x02, 0x03},
				},
				log: discardLogger(),
			})
			peer := testRebroadcastQueuePeer("peer")
			block := testBlockID(0, topShard, 77)
			msg := tonnodeapi.NewBlockCandidateBroadcast{
				ID:   block,
				Data: []byte{0x01},
			}
			payload, err := tl.Serialize(msg, true)
			if err != nil {
				t.Fatalf("serialize candidate broadcast: %v", err)
			}

			err = sub.handleOverlayBroadcast(peer, msg, delivery, false, peer.id)
			if !errors.Is(err, overlay.ErrBroadcastRejected) {
				t.Fatalf("closed admission error = %v, want %v", err, overlay.ErrBroadcastRejected)
			}
			if _, ok := node.eventQueue.TryPop(); ok {
				t.Fatal("closed admission broadcast reached event queue")
			}
			if node.deduper.Seen(broadcastFingerprint(sub.spec.ShortID, payload), time.Now()) {
				t.Fatal("closed admission broadcast was deduped")
			}
			if got := testBroadcastDropStatCount(node, "basechain", "tonNode.newBlockCandidateBroadcast", "broadcast_admission_closed"); got != 1 {
				t.Fatalf("admission drop count = %d, want 1", got)
			}
		})
	}
}

func testBroadcastStatCount(
	node *Node,
	direction string,
	overlay string,
	kind string,
	delivery Delivery,
) uint64 {
	for _, stat := range node.broadcastStatusSnapshot() {
		if stat.Direction == direction &&
			stat.Overlay == overlay &&
			stat.Kind == kind &&
			stat.Delivery == delivery {
			return stat.Count
		}
	}
	return 0
}

func TestBroadcastStatusSeparatesDeliveryModes(t *testing.T) {
	node := newTestNode(t)

	node.noteBroadcast("accepted", "masterchain", "tonNode.blockBroadcast", DeliverySimple)
	node.noteBroadcast("accepted", "masterchain", "tonNode.blockBroadcast", DeliveryPlumtree)

	if got := testBroadcastStatCount(
		node,
		"accepted",
		"masterchain",
		"tonNode.blockBroadcast",
		DeliverySimple,
	); got != 1 {
		t.Fatalf("simple accepted broadcast count = %d, want 1", got)
	}
	if got := testBroadcastStatCount(
		node,
		"accepted",
		"masterchain",
		"tonNode.blockBroadcast",
		DeliveryPlumtree,
	); got != 1 {
		t.Fatalf("Plumtree accepted broadcast count = %d, want 1", got)
	}
}

func testBroadcastDropStatCount(node *Node, overlay, kind, reason string) uint64 {
	for _, stat := range node.broadcastDropStatusSnapshot() {
		if stat.Overlay == overlay && stat.Kind == kind && stat.Reason == reason {
			return stat.Count
		}
	}
	return 0
}

func testExternalMessageBOCWithBody(t *testing.T, value uint64) []byte {
	t.Helper()

	root, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr:   address.MustParseRawAddr("0:1111111111111111111111111111111111111111111111111111111111111111"),
		ImportFee: tlb.ZeroCoins,
		Body:      cell.BeginCell().MustStoreUInt(value, 64).EndCell(),
	})
	if err != nil {
		t.Fatalf("build external message: %v", err)
	}
	return root.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false})
}

// blockOverlayFanoutKey feeds overlayFanoutDeduper, so its shape is a dedup
// contract and not just a log string: it must stay per (class, chain position)
// and must not start distinguishing competing blocks at the same position.
func TestBlockOverlayFanoutKeyIsPerClassAndChainPosition(t *testing.T) {
	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     topShard,
		SeqNo:     7,
		RootHash:  bytes.Repeat([]byte{0x01}, 32),
		FileHash:  bytes.Repeat([]byte{0x02}, 32),
	}
	fork := block
	fork.RootHash = bytes.Repeat([]byte{0x03}, 32)
	fork.FileHash = bytes.Repeat([]byte{0x04}, 32)

	key := blockOverlayFanoutKey("block", block)
	if want := "block:" + tnstore.FormatBlockRef(block); key != want {
		t.Fatalf("fanout key = %q, want %q", key, want)
	}
	if got := blockOverlayFanoutKey("block", fork); got != key {
		t.Fatalf("competing block at the same position got key %q, want the shared %q", got, key)
	}
	if got := blockOverlayFanoutKey("candidate", block); got == key {
		t.Fatalf("candidate class shares the block class key %q", key)
	}

	next := block
	next.SeqNo++
	if got := blockOverlayFanoutKey("block", next); got == key {
		t.Fatalf("next seqno shares the key %q", key)
	}

	shard := block
	shard.Workchain = 0
	if got := blockOverlayFanoutKey("block", shard); got == key {
		t.Fatalf("other workchain shares the key %q", key)
	}
}

// TestFastSyncDeclinedBroadcastsAreDropped covers the send-only case: when this
// node publishes DoNotReceiveBroadcasts in its member flags, a peer that
// ignores the flag must not make us spend decode work on the payload.
func TestFastSyncDeclinedBroadcastsAreDropped(t *testing.T) {
	node := newTestNode(t)

	// receiveBroadcasts is false only for a validator under plumtree, which is
	// exactly when the member flags say DoNotReceiveBroadcasts.
	newSub := func(receiveBroadcasts bool) *overlaySubscription {
		return testOverlaySubscription(&overlaySubscription{
			node: node,
			spec: overlaySpec{
				Name:    "fast-sync.0",
				Kind:    overlayKindFastSync,
				ShortID: []byte{0x04, 0x05, 0x06},
			},
			fastSync: &fastSyncOverlayRuntime{
				spec: fastSyncOverlaySpec{
					localValidator:    true,
					receiveBroadcasts: receiveBroadcasts,
				},
			},
			log: discardLogger(),
		})
	}

	msg := tonnodeapi.NewShardBlockBroadcast{}
	payload := newSerializedBroadcastPayload(make(chan struct{}))

	declined := newSub(false)
	result, err := declined.classifyBroadcastPayload(nil, msg, payload, DeliveryFEC, false, testPeerID("peer"))
	if err != nil {
		t.Fatalf("classify declined broadcast: %v", err)
	}
	if result.disposition != broadcastDispositionIgnore {
		t.Fatalf("declined broadcast was accepted: %+v", result.accepted)
	}
	if payload.serialized {
		t.Fatal("declined broadcast was serialized before being dropped")
	}

	// The gate is narrow: it asks the runtime, and a receiving overlay — which
	// is every overlay that is not a plumtree validator, including one built
	// from a zero-valued spec — never answers yes.
	if !declined.fastSync.declinesBroadcasts() {
		t.Fatal("a validator under plumtree must decline broadcasts")
	}
	if newSub(true).fastSync.declinesBroadcasts() {
		t.Fatal("a receiving overlay must not decline broadcasts")
	}
	zeroValued := &fastSyncOverlayRuntime{}
	if zeroValued.declinesBroadcasts() {
		t.Fatal("a zero-valued runtime must read as receiving, not declining")
	}
}
