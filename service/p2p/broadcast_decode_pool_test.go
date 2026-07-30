package p2p

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/xssnick/gton/service/storage"

	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func testCompressedV2Broadcast(t *testing.T, seqno uint32) (tonnodeapi.BlockBroadcastCompressedV2, ton.BlockIDExt) {
	t.Helper()

	blockCell := testPeerBlockRoot(t, 0, topShard, seqno)
	blockBOC := serializeCompressedBlockRoot(blockCell)
	fileHash := hashSimpleBroadcastPayload(blockBOC)
	blockHash := blockCell.HashKey()
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     topShard,
		SeqNo:     seqno,
		RootHash:  blockHash[:],
		FileHash:  fileHash,
	}
	proofBOC := testPeerBlockProofEnvelopeBOC(t, block, blockCell, nil)

	compressed, err := cell.CompressBOC([]*cell.Cell{blockCell}, cell.CompressionImprovedStructureLZ4, nil)
	if err != nil {
		t.Fatalf("compress block broadcast: %v", err)
	}
	return tonnodeapi.BlockBroadcastCompressedV2{
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
	}, block
}

func TestOffloadedBroadcastDecodeDeliversEventAndCachesResult(t *testing.T) {
	node := newTestNode(t)

	firstSub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			Kind:    overlayKindPublicShard,
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	})
	secondSub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "custom",
			Kind:    overlayKindPublicShard,
			ShortID: []byte{0x07, 0x08, 0x09},
		},
		log: discardLogger(),
	})

	msg, block := testCompressedV2Broadcast(t, 977)
	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize broadcast: %v", err)
	}

	// first delivery: signatures verified synchronously, decode offloaded
	result, err := firstSub.classifyBroadcastPayload(nil, msg, newKnownBroadcastPayload(payload), DeliverySimple, false, testPeerID("first"))
	if err != nil {
		t.Fatalf("classify first delivery: %v", err)
	}
	if result.disposition != broadcastDispositionAccept || result.accepted.event != nil {
		t.Fatalf("first delivery should be acknowledged without an event, got %+v", result)
	}
	if result.accepted.rebroadcast == nil {
		t.Fatal("first delivery acknowledgement is missing the rebroadcast payload")
	}

	eventCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	event, ok := node.eventQueue.Pop(eventCtx)
	if !ok {
		t.Fatal("offloaded decode produced no broadcast event")
	}
	if event.Kind != "tonNode.blockBroadcastCompressedV2" {
		t.Fatalf("event kind = %s", event.Kind)
	}
	if event.Downloaded == nil || !event.Downloaded.ID.Equals(&block) {
		t.Fatalf("event downloaded block mismatch: %+v", event.Downloaded)
	}
	if event.Downloaded.SourcePeerID != testPeerID("first") {
		t.Fatalf("event source peer = %s", event.Downloaded.SourcePeerID.String())
	}

	// second delivery of the same block on another overlay: different
	// fingerprint, so the deduper does not collapse it, but the decode cache
	// completes it inline with an immediate event
	result, err = secondSub.classifyBroadcastPayload(nil, msg, newKnownBroadcastPayload(payload), DeliveryTwoStep, false, testPeerID("second"))
	if err != nil {
		t.Fatalf("classify second delivery: %v", err)
	}
	if result.disposition != broadcastDispositionAccept || result.accepted.event == nil || result.accepted.event.Downloaded == nil {
		t.Fatalf("second delivery should reuse the cached decode inline, got %+v", result)
	}
	if !result.accepted.event.Downloaded.ID.Equals(&block) {
		t.Fatalf("cached downloaded block mismatch: %+v", result.accepted.event.Downloaded)
	}
	if result.accepted.event.Downloaded.SourcePeerID != testPeerID("second") {
		t.Fatalf("cached delivery source peer = %s", result.accepted.event.Downloaded.SourcePeerID.String())
	}
}

func TestEnqueueBroadcastDecodeRoutesMasterchainToDedicatedLane(t *testing.T) {
	node := newTestNode(t)

	// initialize the pool lanes without relying on worker scheduling
	node.decodeWorkersOnce.Do(func() {
		node.decodeQueue = make(chan offloadedBroadcastDecode, broadcastDecodeQueueSize)
		node.masterDecodeQueue = make(chan offloadedBroadcastDecode, broadcastMasterDecodeQueueSize)
	})

	master := testBlockID(-1, topShard, 10)
	shard := testBlockID(0, topShard, 10)

	if !node.enqueueBroadcastDecode(offloadedBroadcastDecode{fingerprint: "m", kind: "tonNode.blockBroadcastCompressedV2", block: master}) {
		t.Fatal("masterchain decode enqueue refused")
	}
	if !node.enqueueBroadcastDecode(offloadedBroadcastDecode{fingerprint: "s", kind: "tonNode.blockBroadcastCompressedV2", block: shard}) {
		t.Fatal("shard decode enqueue refused")
	}

	if got := len(node.masterDecodeQueue); got != 1 {
		t.Fatalf("masterchain lane depth = %d, want 1", got)
	}
	if got := len(node.decodeQueue); got != 1 {
		t.Fatalf("shared lane depth = %d, want 1", got)
	}

	select {
	case req := <-node.masterDecodeQueue:
		if !req.block.Equals(&master) {
			t.Fatalf("masterchain lane holds %s", storage.FormatBlockRef(req.block))
		}
	default:
		t.Fatal("masterchain lane is empty")
	}
}

func TestDecodedBroadcastCacheTTLAndCopySemantics(t *testing.T) {
	var cache decodedBroadcastCache
	block := testBlockID(0, topShard, 5)
	downloaded := &DownloadedBlock{ID: block, Kind: "tonNode.blockBroadcastCompressedV2"}

	cache.put("kind", block, downloaded, nil)
	got, _, err := cache.get("kind", block)
	if err != nil {
		t.Fatal("cache miss after put")
	}
	if got == downloaded {
		t.Fatal("cache returned the stored pointer instead of a copy")
	}
	got.SourcePeerID = testPeerID("mutated")

	again, _, err := cache.get("kind", block)
	if err != nil {
		t.Fatal("cache miss on second get")
	}
	if again.SourcePeerID == testPeerID("mutated") {
		t.Fatal("mutating a returned copy leaked into the cache")
	}

	if _, _, err := cache.get("other-kind", block); err == nil {
		t.Fatal("cache hit across kinds")
	}

	cache.entries[decodedBroadcastKey("kind", block)] = decodedBroadcastEntry{
		downloaded: *downloaded,
		storedAt:   time.Now().Add(-2 * decodedBroadcastCacheTTL),
	}
	if _, _, err := cache.get("kind", block); err == nil {
		t.Fatal("expired entry served from cache")
	}
}

func TestBroadcastDecodeWorkerCountFor(t *testing.T) {
	tests := []struct {
		procs int
		want  int
	}{
		{procs: 1, want: broadcastDecodeWorkerMin},
		{procs: 2, want: broadcastDecodeWorkerMin},
		{procs: 4, want: broadcastDecodeWorkerMin},
		{procs: 8, want: 4},
		{procs: 16, want: broadcastDecodeWorkerMax},
		{procs: 64, want: broadcastDecodeWorkerMax},
	}

	for _, tc := range tests {
		if got := broadcastDecodeWorkerCountFor(tc.procs); got != tc.want {
			t.Fatalf("broadcastDecodeWorkerCountFor(%d) = %d, want %d", tc.procs, got, tc.want)
		}
	}

	// The pool is never allowed to run without workers: the alternative to a
	// free worker is an inline decode on a transport receive goroutine.
	if got := broadcastDecodeWorkerCountFor(0); got < broadcastDecodeWorkerMin {
		t.Fatalf("broadcastDecodeWorkerCountFor(0) = %d, want at least %d", got, broadcastDecodeWorkerMin)
	}
	if got := broadcastDecodeWorkerCountFor(-1); got < broadcastDecodeWorkerMin {
		t.Fatalf("broadcastDecodeWorkerCountFor(-1) = %d, want at least %d", got, broadcastDecodeWorkerMin)
	}
}
