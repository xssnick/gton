package p2p

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/xssnick/gton/internal/extmsg"
	tnstore "github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/address"
	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type testBroadcastPipelineObserver struct {
	observations []BroadcastPipelineStageObservation
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

func (o *testBroadcastPipelineObserver) requireNoStage(t *testing.T, stage string, kind string, delivery Delivery) {
	t.Helper()

	for _, observation := range o.observations {
		if observation.Stage == stage &&
			observation.Kind == kind &&
			observation.Delivery == delivery {
			t.Fatalf("unexpected broadcast pipeline observation stage=%s kind=%s delivery=%s in %#v", stage, kind, delivery, o.observations)
		}
	}
}

func TestClassifyInvalidCompressedBlockBroadcastDoesNotWakeBeforeSignaturePrecheck(t *testing.T) {
	node := newTestNode(t)
	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "masterchain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	}

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
			if _, ready := node.MasterchainBroadcastAfter(tt.block.SeqNo - 1); ready {
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
	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	}
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
	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	}
	payload := newSerializedBroadcastPayload(make(chan struct{}))

	accepted, err := sub.classifyBroadcastPayload(nil, tonnodeapi.NewExternalMessageBroadcast{}, payload, DeliveryFEC, false, testPeerID("peer"))
	if err != nil {
		t.Fatalf("classify invalid broadcast: %v", err)
	}
	if accepted != nil {
		t.Fatalf("invalid external broadcast was accepted: %+v", accepted)
	}
	if payload.serialized {
		t.Fatal("invalid broadcast serialized payload before cheap drop")
	}
}

func TestClassifyDuplicateIdentifiedBroadcastDoesNotSerializePayload(t *testing.T) {
	node := newTestNode(t)
	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	}

	downloaded := testShardBroadcastDownloadedBlock(t, 207, 0x207)
	msg := tonnodeapi.NewBlockCandidateBroadcast{
		ID:   downloaded.ID,
		Data: downloaded.BlockBOC,
	}
	broadcastID := bytes.Repeat([]byte{0xAB}, 32)
	node.deduper.Mark(broadcastFingerprint(sub.spec.ShortID, broadcastID), time.Now())
	payload := newIdentifiedBroadcastPayload(make(chan struct{}), broadcastID)

	accepted, err := sub.classifyBroadcastPayload(nil, msg, payload, DeliveryFEC, false, testPeerID("peer"))
	if err != nil {
		t.Fatalf("classify duplicate identified broadcast: %v", err)
	}
	if accepted != nil {
		t.Fatalf("duplicate identified broadcast was accepted: %+v", accepted)
	}
	if payload.serialized {
		t.Fatal("duplicate identified broadcast serialized payload before dedupe")
	}
}

func TestBroadcastPipelineObserverCapturesHotPathStages(t *testing.T) {
	node := newTestNode(t)
	node.SetBlockCacheObserver(&testBlockCacheObserver{nonfinalEnabled: true})
	observer := &testBroadcastPipelineObserver{}
	node.SetBroadcastPipelineObserver(observer)

	sourceID := testPeerID("source")
	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:         "custom.private-a",
			Kind:         overlayKindCustomFixed,
			ShortID:      []byte{0x01, 0x02, 0x03},
			BlockSenders: map[PeerID]struct{}{sourceID: {}},
		},
		log: discardLogger(),
	}

	downloaded := testShardBroadcastDownloadedBlock(t, 206, 0x206)
	candidate := tonnodeapi.NewBlockCandidateBroadcast{
		ID:   downloaded.ID,
		Data: downloaded.BlockBOC,
	}
	if err := sub.handleOverlayBroadcast(nil, candidate, DeliveryTwoStep, true, sourceID); err != nil {
		t.Fatalf("handle candidate broadcast: %v", err)
	}
	observer.requireStage(t, broadcastPipelineStageClassify, "tonNode.newBlockCandidateBroadcast", DeliveryTwoStep, broadcastPipelineResultSuccess)
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
	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	}

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

func TestAcceptedShardBlockBroadcastSkipsSameOverlayFECRebroadcast(t *testing.T) {
	node := newTestNode(t)
	source := testRebroadcastQueuePeer("source")
	target := testRebroadcastQueuePeer("target")
	sub := &overlaySubscription{
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
	}
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
	if got := testBroadcastStatCount(node, "accepted", "basechain", "tonNode.newShardBlockBroadcast"); got != 1 {
		t.Fatalf("accepted broadcast count = %d, want 1", got)
	}

	node.acceptBroadcast(*accepted)
	if got := testBroadcastDropStatCount(node, "basechain", "tonNode.newShardBlockBroadcast", "seen"); got != 1 {
		t.Fatalf("seen broadcast drop count = %d, want 1", got)
	}
	if got := testBroadcastStatCount(node, "accepted", "basechain", "tonNode.newShardBlockBroadcast"); got != 1 {
		t.Fatalf("duplicate accepted broadcast count = %d, want 1", got)
	}
}

func TestCustomOverlayRejectsUnauthorizedBlockSender(t *testing.T) {
	node := newTestNode(t)
	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:         "custom.private-a",
			Kind:         overlayKindCustomFixed,
			ShortID:      []byte{0x02, 0x03, 0x04},
			BlockSenders: map[PeerID]struct{}{testPeerID("allowed"): {}},
		},
		log: discardLogger(),
	}
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

func TestCustomTwoStepBroadcastSkipsSameOverlayRebroadcastButKeepsFanoutPayload(t *testing.T) {
	node := newTestNode(t)
	observer := &testBroadcastPipelineObserver{}
	node.SetBroadcastPipelineObserver(observer)
	sourceID := testPeerID("source")
	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:         "custom.private-a",
			Kind:         overlayKindCustomFixed,
			ShortID:      []byte{0x02, 0x03, 0x04},
			BlockSenders: map[PeerID]struct{}{sourceID: {}},
		},
		log: discardLogger(),
	}

	downloaded := testShardBroadcastDownloadedBlock(t, 204, 0x204)
	block := downloaded.ID
	data := downloaded.BlockBOC
	msg := tonnodeapi.NewBlockCandidateBroadcast{
		ID:   block,
		Data: data,
	}
	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize block candidate broadcast: %v", err)
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
	observer.requireStage(t, broadcastPipelineStageCandidateDecode, "tonNode.newBlockCandidateBroadcast", DeliveryTwoStep, broadcastPipelineResultError)
}

func TestAcceptedShardBlockBroadcastFansOutToCustomOverlay(t *testing.T) {
	node := newTestNode(t)
	publicSpec := overlaySpec{
		Name:    "basechain",
		Kind:    overlayKindPublicShard,
		ShortID: []byte{0x01, 0x02, 0x03},
	}
	publicSub, _ := node.getOrCreateSubscription(publicSpec)

	customSpec := overlaySpec{
		Name:         "custom.private-a",
		Kind:         overlayKindCustomFixed,
		ShortID:      []byte{0x04, 0x05, 0x06},
		BlockSenders: map[PeerID]struct{}{node.localID: {}},
	}
	customSub, _ := node.getOrCreateSubscription(customSpec)
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

func TestClassifyExternalMessageBroadcastCachesByBodyHash(t *testing.T) {
	node := newTestNode(t)
	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	}
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

func TestHandleOverlayBroadcastRejectsInvalidUntrustedFECForRelay(t *testing.T) {
	node := newTestNode(t)
	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	}

	err := sub.handleOverlayBroadcast(nil, tonnodeapi.NewExternalMessageBroadcast{}, DeliveryFEC, false, testPeerID("peer"))
	if !errors.Is(err, errBroadcastRejected) {
		t.Fatalf("expected rejected untrusted FEC broadcast, got %v", err)
	}
}

func TestClassifyExternalMessageBroadcastLimitsPerAddress(t *testing.T) {
	node := newTestNode(t)
	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	}

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
	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	}
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
	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	}
	msg := tonnodeapi.NewExternalMessageBroadcast{
		Message: tonnodeapi.ExternalMessage{Data: make([]byte, maxOverlayPayloadSize+1)},
	}

	accepted := sub.classifyBroadcast(nil, msg, []byte{0x01}, DeliveryFEC, false, testPeerID("peer"))
	if accepted != nil {
		t.Fatalf("oversize external broadcast was accepted: %+v", accepted)
	}
}

func TestHandleOverlayBroadcastAdmissionClosedRejectsRelayedBroadcastWithoutProcessing(t *testing.T) {
	for _, delivery := range []Delivery{DeliveryFEC, DeliveryTwoStep} {
		t.Run(string(delivery), func(t *testing.T) {
			node := newTestNode(t)
			node.broadcastAdmission = testBroadcastAdmission(false)
			sub := &overlaySubscription{
				node: node,
				spec: overlaySpec{
					Name:    "basechain",
					ShortID: []byte{0x01, 0x02, 0x03},
				},
				log: discardLogger(),
			}
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
			if !errors.Is(err, errBroadcastRejected) {
				t.Fatalf("closed admission error = %v, want %v", err, errBroadcastRejected)
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

func testBroadcastStatCount(node *Node, direction, overlay, kind string) uint64 {
	for _, stat := range node.broadcastStatusSnapshot() {
		if stat.Direction == direction && stat.Overlay == overlay && stat.Kind == kind {
			return stat.Count
		}
	}
	return 0
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
