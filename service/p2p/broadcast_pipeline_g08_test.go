package p2p

import (
	"bytes"
	"context"
	"errors"
	"hash/crc64"
	"math/bits"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/address"
	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// testCustomFanoutOverlays wires a public shard overlay that receives
// broadcasts and a custom overlay this node is a block sender in, so every
// custom fanout of an accepted broadcast lands in the custom two-step queue.
func testCustomFanoutOverlays(t *testing.T) (*Node, *overlaySubscription, *overlaySubscription, *overlayPeer) {
	t.Helper()

	node := newTestNode(t)
	publicSub := mustGetOrCreateSubscription(t, node, overlaySpec{
		Name:    "basechain",
		Kind:    overlayKindPublicShard,
		ShortID: bytes.Repeat([]byte{0x01}, PeerIDSize),
	})
	customSub := mustGetOrCreateSubscription(t, node, overlaySpec{
		Name:         "custom.private-a",
		Kind:         overlayKindCustomFixed,
		ShortID:      bytes.Repeat([]byte{0x04}, PeerIDSize),
		BlockSenders: map[PeerID]struct{}{node.localID: {}},
	})
	customPeer := testRebroadcastQueuePeer("custom-peer")
	customSub.peers[customPeer.id] = customPeer
	return node, publicSub, customSub, customPeer
}

func drainCustomTwoStepFanouts(sub *overlaySubscription) []rebroadcastRequest {
	sub.mx.Lock()
	queue := sub.twoStepQueue
	sub.mx.Unlock()
	if queue == nil {
		return nil
	}

	var requests []rebroadcastRequest
	for {
		req, ok := queue.TryPop()
		if !ok {
			return requests
		}
		requests = append(requests, req)
	}
}

func deliverTestBlockBroadcast(t *testing.T, sub *overlaySubscription, msg any, source string) []byte {
	t.Helper()

	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize broadcast: %v", err)
	}
	if disposition := sub.handleOverlayBroadcastPayload(nil, msg, newKnownBroadcastPayload(payload), DeliverySimple, false, testPeerID(source)); disposition != overlay.BroadcastDispositionAcceptAndRelay {
		t.Fatalf("broadcast from %s disposition = %v, want accept-and-relay", source, disposition)
	}
	return payload
}

func takeOffloadedBroadcastDecode(t *testing.T, node *Node) offloadedBroadcastDecode {
	t.Helper()

	select {
	case req := <-node.decodeQueue:
		return req
	default:
		t.Fatal("block broadcast decode was not offloaded")
		return offloadedBroadcastDecode{}
	}
}

// TestOffloadedGarbageBlockBroadcastDoesNotTakeOverlayFanout pins that a full
// block broadcast reaches custom and FastSync overlays only once its payload
// decoded. The validator signatures cover the block id and proof alone, so a
// peer can pair a real header with garbage data_compressed. The reference node
// sends to custom overlays only after deserialize_block_broadcast succeeds; a
// garbage copy fanned out at the acknowledgement would take the per-block
// fanout key and keep the honest copy away from the custom overlay.
func TestOffloadedGarbageBlockBroadcastDoesNotTakeOverlayFanout(t *testing.T) {
	const kind = "tonNode.blockBroadcastCompressedV2"

	node, publicSub, customSub, _ := testCustomFanoutOverlays(t)
	// lanes without workers, so the test runs every offloaded decode itself
	node.decodeWorkersOnce.Do(func() {
		node.decodeQueue = make(chan offloadedBroadcastDecode, broadcastDecodeQueueSize)
		node.masterDecodeQueue = make(chan offloadedBroadcastDecode, broadcastMasterDecodeQueueSize)
	})

	honest, block := testCompressedV2Broadcast(t, 241)
	garbage := honest
	garbage.DataCompressed = []byte{honest.DataCompressed[0], 0xde, 0xad, 0xbe, 0xef}
	fanoutKey := blockOverlayRouteFanoutKey(blockOverlayFanoutRouteCustom, "block", block)

	deliverTestBlockBroadcast(t, publicSub, garbage, "attacker")
	if fanouts := drainCustomTwoStepFanouts(customSub); len(fanouts) != 0 {
		t.Fatalf("undecoded block broadcast was fanned out to the custom overlay: %d requests", len(fanouts))
	}
	node.processOffloadedBroadcastDecode(context.Background(), takeOffloadedBroadcastDecode(t, node))
	if got := testBroadcastDropStatCount(node, "basechain", kind, "decode_failed"); got != 1 {
		t.Fatalf("decode_failed drop count = %d, want 1", got)
	}
	if fanouts := drainCustomTwoStepFanouts(customSub); len(fanouts) != 0 {
		t.Fatalf("garbage block broadcast was fanned out after its decode failed: %d requests", len(fanouts))
	}
	if node.overlayFanoutDeduper.Seen(fanoutKey, time.Now()) {
		t.Fatal("garbage block broadcast took the custom fanout key")
	}

	honestPayload := deliverTestBlockBroadcast(t, publicSub, honest, "honest")
	if fanouts := drainCustomTwoStepFanouts(customSub); len(fanouts) != 0 {
		t.Fatalf("block broadcast was fanned out before its decode: %d requests", len(fanouts))
	}
	node.processOffloadedBroadcastDecode(context.Background(), takeOffloadedBroadcastDecode(t, node))
	fanouts := drainCustomTwoStepFanouts(customSub)
	if len(fanouts) != 1 {
		t.Fatalf("decoded honest block broadcast custom fanouts = %d, want 1", len(fanouts))
	}
	if fanouts[0].kind != kind || !bytes.Equal(fanouts[0].payload, honestPayload) {
		t.Fatalf("custom fanout carries kind %s and a payload other than the honest copy", fanouts[0].kind)
	}
	event, ok := node.eventQueue.TryPop()
	if !ok || event.Downloaded == nil || !event.Downloaded.ID.Equals(&block) {
		t.Fatalf("honest block broadcast event = %+v, ok=%v", event, ok)
	}
}

// TestHeldGarbageBlockBroadcastDoesNotTakeOverlayFanout covers the pre-decode
// exit for a block this node already holds: the copy is relayed on the
// transport without a decode, so its bytes are never verified and must not be
// re-originated into custom overlays or claim their fanout key.
func TestHeldGarbageBlockBroadcastDoesNotTakeOverlayFanout(t *testing.T) {
	node, publicSub, customSub, _ := testCustomFanoutOverlays(t)
	honest, block := testCompressedV2Broadcast(t, 242)
	holdBlockInLiveCache(t, node, block)
	garbage := honest
	garbage.DataCompressed = []byte{honest.DataCompressed[0], 0xde, 0xad, 0xbe, 0xef}

	deliverTestBlockBroadcast(t, publicSub, garbage, "attacker")
	if fanouts := drainCustomTwoStepFanouts(customSub); len(fanouts) != 0 {
		t.Fatalf("held block broadcast was fanned out without a decode: %d requests", len(fanouts))
	}
	if node.overlayFanoutDeduper.Seen(blockOverlayRouteFanoutKey(blockOverlayFanoutRouteCustom, "block", block), time.Now()) {
		t.Fatal("held block broadcast took the custom fanout key")
	}
}

func testRawBlockCandidate(t *testing.T, seqno uint32) (tonnodeapi.NewBlockCandidateBroadcast, []byte) {
	t.Helper()

	blockCell := testPeerBlockRoot(t, 0, seqno)
	blockBOC := serializeCompressedBlockRoot(blockCell)
	blockHash := blockCell.HashKey()
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     topShard,
		SeqNo:     seqno,
		RootHash:  blockHash[:],
		FileHash:  hashSimpleBroadcastPayload(blockBOC),
	}
	return tonnodeapi.NewBlockCandidateBroadcast{
		ID:   block,
		Data: blockBOC,
	}, blockBOC
}

// TestInboundCandidateFanoutSkipsCandidateThisNodePublished pins one sent
// cache for this node's own custom publications and the relay of accepted
// broadcasts: a block sender that published its candidate to a custom overlay
// must not send another validator's copy of the same block there again. The
// reference node keys both by the full block id in
// custom_overlays_sent_broadcasts_.
func TestInboundCandidateFanoutSkipsCandidateThisNodePublished(t *testing.T) {
	node, publicSub, customSub, customPeer := testCustomFanoutOverlays(t)

	published, publishedBOC := testRawBlockCandidate(t, 243)
	node.BlockBroadcasts().publishCandidate(BlockCandidatePublication{
		Block:            published.ID,
		BlockBOC:         publishedBOC,
		CatchainSeqno:    11,
		ValidatorSetHash: 12,
		Mode:             BlockBroadcastModeCustom,
	})
	if _, ok := customPeer.localRebroadcastQueue.TryPop(); !ok {
		t.Fatal("local candidate publication did not reach the custom overlay")
	}

	deliverTestBlockBroadcast(t, publicSub, published, "other-validator")
	if fanouts := drainCustomTwoStepFanouts(customSub); len(fanouts) != 0 {
		t.Fatalf("candidate this node published was fanned out to the custom overlay again: %d requests", len(fanouts))
	}
	if _, ok := customPeer.localRebroadcastQueue.TryPop(); ok {
		t.Fatal("candidate this node published was queued to the custom overlay again")
	}

	relayed, _ := testRawBlockCandidate(t, 244)
	deliverTestBlockBroadcast(t, publicSub, relayed, "other-validator")
	if fanouts := drainCustomTwoStepFanouts(customSub); len(fanouts) != 1 {
		t.Fatalf("candidate this node did not publish custom fanouts = %d, want 1", len(fanouts))
	}
}

// TestExternalMessageDedupSurvivesCRC64Collision pins the external message
// dedup key against forgery. CRC-64 is affine over GF(2), so a peer that saw an
// external message can solve for a sibling with the same checksum of (overlay
// id, body) in microseconds. The sibling's key is marked before admission
// rejects it, so a forgeable key would keep the honest message out of the pool
// and away from the liteserver send path for the whole cache TTL.
func TestExternalMessageDedupSurvivesCRC64Collision(t *testing.T) {
	node := newTestNode(t)
	admission := &testExternalMessageAdmission{err: errors.New("forged external message")}
	node.externalMessageAdmission = admission
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	})

	body := make([]byte, crc64CollisionBodyBytes)
	for i := range body {
		body[i] = byte(0x5a + i)
	}
	honest := testExternalMessageBOCWithBodyBytes(t, body)
	forged := testExternalMessageBOCWithBodyBytes(t, testCRC64CollidingExternalBody(t, sub.spec.ShortID, body))

	forgedMsg := tonnodeapi.NewExternalMessageBroadcast{
		Message: tonnodeapi.ExternalMessage{Data: forged},
	}
	forgedPayload, err := tl.Serialize(forgedMsg, true)
	if err != nil {
		t.Fatalf("serialize forged external broadcast: %v", err)
	}
	if accepted := sub.classifyBroadcast(nil, forgedMsg, forgedPayload, DeliveryFEC, false, testPeerID("attacker")); accepted != nil {
		t.Fatalf("forged external broadcast was accepted: %+v", accepted)
	}

	if node.externalMessageSeen(externalMessageFingerprint(sub.spec.ShortID, honest), time.Now()) {
		t.Fatal("honest external message reads as seen after a colliding forgery was rejected")
	}

	admission.err = nil
	honestMsg := tonnodeapi.NewExternalMessageBroadcast{
		Message: tonnodeapi.ExternalMessage{Data: honest},
	}
	honestPayload, err := tl.Serialize(honestMsg, true)
	if err != nil {
		t.Fatalf("serialize honest external broadcast: %v", err)
	}
	if accepted := sub.classifyBroadcast(nil, honestMsg, honestPayload, DeliveryFEC, false, testPeerID("peer")); accepted == nil {
		t.Fatal("honest external broadcast was dropped behind a colliding forgery")
	}
	if len(admission.events) != 2 {
		t.Fatalf("admission events = %d, want 2", len(admission.events))
	}
}

const crc64CollisionBodyBytes = 32

// crc64CollisionRow is one reduced vector of the elimination below: the
// checksum difference a set of body bit flips produces, and that set.
type crc64CollisionRow struct {
	image uint64
	flips [crc64CollisionBodyBytes * 8 / 64]uint64
}

// testCRC64CollidingExternalBody returns a different body of the same length
// whose external message has the same CRC-64/ECMA of (overlay id, BOC) as the
// message with body. Flipping a body bit flips a fixed BOC bit, so the checksum
// difference of a flip set is the xor of the single-flip differences; those live
// in a 64-bit space, so among more than 64 of them a nonempty subset cancels.
func testCRC64CollidingExternalBody(t *testing.T, overlayID []byte, body []byte) []byte {
	t.Helper()

	table := crc64.MakeTable(crc64.ECMA)
	checksum := func(body []byte) uint64 {
		data := testExternalMessageBOCWithBodyBytes(t, body)
		return crc64.Update(crc64.Update(0, table, overlayID), table, data)
	}
	base := checksum(body)

	var basis [64]crc64CollisionRow
	for bit := 0; bit < len(body)*8; bit++ {
		flipped := append([]byte(nil), body...)
		flipped[bit/8] ^= 1 << (bit % 8)

		current := crc64CollisionRow{image: checksum(flipped) ^ base}
		current.flips[bit/64] |= 1 << (bit % 64)
		for current.image != 0 {
			top := 63 - bits.LeadingZeros64(current.image)
			if basis[top].image == 0 {
				basis[top] = current
				break
			}
			current.image ^= basis[top].image
			for i := range current.flips {
				current.flips[i] ^= basis[top].flips[i]
			}
		}
		if current.image != 0 {
			continue
		}

		forged := append([]byte(nil), body...)
		for flip := 0; flip < len(body)*8; flip++ {
			if current.flips[flip/64]&(1<<(flip%64)) != 0 {
				forged[flip/8] ^= 1 << (flip % 8)
			}
		}
		if checksum(forged) != base {
			t.Fatal("constructed body does not collide under CRC-64")
		}
		return forged
	}
	t.Fatal("no CRC-64 collision among single-bit body flips")
	return nil
}

func testExternalMessageBOCWithBodyBytes(t *testing.T, body []byte) []byte {
	t.Helper()

	root, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr:   address.MustParseRawAddr("0:1111111111111111111111111111111111111111111111111111111111111111"),
		ImportFee: tlb.ZeroCoins,
		Body:      cell.BeginCell().MustStoreSlice(body, uint(len(body)*8)).EndCell(),
	})
	if err != nil {
		t.Fatalf("build external message: %v", err)
	}
	return root.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false})
}
