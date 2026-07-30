package p2p

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	tnstore "github.com/xssnick/gton/service/storage"

	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// testGatedBroadcastSignatureVerifier blocks every full-block signature check
// until the test feeds (or closes) release, so a test can order the decode
// against the signature join deterministically. It intentionally ignores the
// check context: the test always releases it.
type testGatedBroadcastSignatureVerifier struct {
	release chan error
}

func (v *testGatedBroadcastSignatureVerifier) CheckBlockBroadcastSignatures(context.Context, BlockBroadcastSignatureCheck) error {
	return <-v.release
}

func (v *testGatedBroadcastSignatureVerifier) CheckBlockFinalitySignatures(context.Context, BlockFinalitySignatureCheck) (*BlockFinalitySignatureCheckResult, error) {
	return nil, errors.New("finality signatures are not supported by the gated test verifier")
}

func (v *testGatedBroadcastSignatureVerifier) ValidateShardDescriptionBroadcast(context.Context, ShardDescriptionSignatureCheck) (*ShardBlockDescription, error) {
	return nil, errors.New("shard descriptions are not supported by the gated test verifier")
}

// testSignalingCompressedStateProvider closes called when the state-aware
// decode asks for the previous state, which marks the point where the decode
// is genuinely running.
type testSignalingCompressedStateProvider struct {
	state  *cell.Cell
	once   sync.Once
	called chan struct{}
}

func (p *testSignalingCompressedStateProvider) StateRootForCompressedBlock(context.Context, ton.BlockIDExt) (*cell.Cell, error) {
	p.once.Do(func() { close(p.called) })
	return p.state, nil
}

func (p *testSignalingCompressedStateProvider) RememberCompressedBlockState(*tnstore.BlockState) bool {
	return false
}

// lockedBroadcastPipelineObserver is safe for observations arriving from the
// decode pool workers concurrently with the transport thread, and signals the
// awaited stage so tests join the worker instead of polling for it.
type lockedBroadcastPipelineObserver struct {
	mu           sync.Mutex
	observations []BroadcastPipelineStageObservation
	awaitStage   string
	awaitResult  string
	awaited      chan BroadcastPipelineStageObservation
}

func newAwaitingBroadcastPipelineObserver(stage, result string) *lockedBroadcastPipelineObserver {
	return &lockedBroadcastPipelineObserver{
		awaitStage:  stage,
		awaitResult: result,
		awaited:     make(chan BroadcastPipelineStageObservation, 1),
	}
}

func (o *lockedBroadcastPipelineObserver) ObserveBroadcastPipelineStage(observation BroadcastPipelineStageObservation) {
	o.mu.Lock()
	o.observations = append(o.observations, observation)
	o.mu.Unlock()

	if observation.Stage == o.awaitStage && observation.Result == o.awaitResult {
		select {
		case o.awaited <- observation:
		default:
		}
	}
}

func (o *lockedBroadcastPipelineObserver) waitAwaited(t *testing.T) BroadcastPipelineStageObservation {
	t.Helper()

	select {
	case observation := <-o.awaited:
		return observation
	case <-time.After(5 * time.Second):
		t.Fatalf("stage %s/%s was never observed", o.awaitStage, o.awaitResult)
		return BroadcastPipelineStageObservation{}
	}
}

func (o *lockedBroadcastPipelineObserver) count(stage, result string) int {
	o.mu.Lock()
	defer o.mu.Unlock()

	n := 0
	for _, observation := range o.observations {
		if observation.Stage == stage && observation.Result == result {
			n++
		}
	}
	return n
}

func testStateAwareCompressedV2Broadcast(t *testing.T, seqno uint32, state *cell.Cell) (tonnodeapi.BlockBroadcastCompressedV2, ton.BlockIDExt) {
	t.Helper()

	blockCell := testPeerBlockRoot(t, 0, topShard, seqno)
	blockBOC := serializeCompressedBlockRoot(blockCell)
	blockHash := blockCell.HashKey()
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     topShard,
		SeqNo:     seqno,
		RootHash:  blockHash[:],
		FileHash:  hashSimpleBroadcastPayload(blockBOC),
	}
	proofBOC := testPeerBlockProofEnvelopeBOC(t, block, blockCell, nil)

	compressed, err := cell.CompressBOC([]*cell.Cell{blockCell}, cell.CompressionImprovedStructureLZ4WithState, state)
	if err != nil {
		t.Fatalf("compress state-aware block broadcast: %v", err)
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

func testGatedDecodeNode(t *testing.T, seqno uint32) (*Node, *overlaySubscription, *testGatedBroadcastSignatureVerifier, *testSignalingCompressedStateProvider, tonnodeapi.BlockBroadcastCompressedV2, ton.BlockIDExt) {
	t.Helper()

	node := newTestNode(t)
	node.storage = newTestPebbleStore(t)
	verifier := &testGatedBroadcastSignatureVerifier{release: make(chan error, 1)}
	node.signatureVerifier = verifier
	state := cell.BeginCell().MustStoreUInt(uint64(seqno), 16).EndCell()
	provider := &testSignalingCompressedStateProvider{state: state, called: make(chan struct{})}
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

	msg, block := testStateAwareCompressedV2Broadcast(t, seqno, state)
	return node, sub, verifier, provider, msg, block
}

// The decode job is enqueued before the transport thread joins the concurrent
// signature check, so the multi-MB decode overlaps the ed25519 pass instead of
// waiting it out.
func TestFullBlockBroadcastDecodeStartsBeforeSignatureJoin(t *testing.T) {
	node, sub, verifier, provider, msg, block := testGatedDecodeNode(t, 214)
	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize broadcast: %v", err)
	}

	dispositions := make(chan overlay.BroadcastDisposition, 1)
	go func() {
		dispositions <- sub.handleOverlayBroadcastPayload(nil, msg, newKnownBroadcastPayload(payload), DeliverySimple, false, testPeerID("source"))
	}()

	// the pool worker reaches the previous-state fetch inside the decode while
	// the signature check is still gated
	select {
	case <-provider.called:
	case disposition := <-dispositions:
		t.Fatalf("classification finished before the signature check resolved: %v", disposition)
	case <-time.After(5 * time.Second):
		t.Fatal("decode did not start while the signature check was in flight")
	}

	close(verifier.release)

	select {
	case disposition := <-dispositions:
		if disposition != overlay.BroadcastDispositionAcceptAndRelay {
			t.Fatalf("disposition = %v, want accept-and-relay", disposition)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("classification did not finish after the signature release")
	}

	eventCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	event, ok := node.eventQueue.Pop(eventCtx)
	if !ok {
		t.Fatal("offloaded decode produced no broadcast event")
	}
	if event.Downloaded == nil || !event.Downloaded.ID.Equals(&block) {
		t.Fatalf("unexpected broadcast event: %#v", event)
	}
	if _, _, err := node.decodedBroadcasts.get(event.Kind, block); err != nil {
		t.Fatal("verified decode was not cached")
	}
}

// A signature failure that resolves after the decode was enqueued must keep
// all drop bookkeeping on the transport thread while the worker discards its
// decode silently: no event, no decode cache entry, relay suppressed.
func TestFullBlockBroadcastSignatureFailureAfterDecodeDropsSilently(t *testing.T) {
	node, sub, verifier, provider, msg, block := testGatedDecodeNode(t, 215)
	observer := newAwaitingBroadcastPipelineObserver(broadcastPipelineStageDecodeAsync, broadcastPipelineResultDrop)
	node.SetBroadcastPipelineObserver(observer)
	relayTarget := testRebroadcastQueuePeer("relay-target")
	sub.peers = map[PeerID]*overlayPeer{relayTarget.id: relayTarget}

	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize broadcast: %v", err)
	}
	fingerprint := broadcastFingerprint(sub.spec.ShortID, payload)

	dispositions := make(chan overlay.BroadcastDisposition, 1)
	go func() {
		dispositions <- sub.handleOverlayBroadcastPayload(nil, msg, newKnownBroadcastPayload(payload), DeliverySimple, false, testPeerID("source"))
	}()

	select {
	case <-provider.called:
	case <-time.After(5 * time.Second):
		t.Fatal("decode did not start while the signature check was in flight")
	}

	verifier.release <- errors.New("bad signatures")

	select {
	case disposition := <-dispositions:
		if disposition != overlay.BroadcastDispositionIgnore {
			t.Fatalf("disposition = %v, want ignore", disposition)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("classification did not finish after the signature release")
	}

	// join the worker: it records the decode as dropped once it sees the failed
	// signature outcome
	observer.waitAwaited(t)

	if _, _, err := node.decodedBroadcasts.get("tonNode.blockBroadcastCompressedV2", block); err == nil {
		t.Fatal("unverified decode was cached")
	}
	if _, ok := node.eventQueue.TryPop(); ok {
		t.Fatal("unverified broadcast produced an event")
	}
	if _, ok := relayTarget.rebroadcastQueue.TryPop(); ok {
		t.Fatal("unverified broadcast reached the app-level relay queue")
	}
	if observer.count(broadcastPipelineStageDecodeAsync, broadcastPipelineResultSuccess) != 0 {
		t.Fatal("dropped decode was also accounted as a success")
	}

	// the transport thread is the single owner of the drop bookkeeping: the
	// worker must not repeat the drop metric under any reason, and the
	// permanent failure keeps the fingerprint deduped
	if got := testBroadcastDropStatCount(node, "basechain", "tonNode.blockBroadcastCompressedV2", "signature_check_failed"); got != 1 {
		t.Fatalf("signature drop count = %d, want 1", got)
	}
	for _, reason := range []string{"decode_failed", "state_artifact_missing", "broadcast_admission_closed", "seen"} {
		if got := testBroadcastDropStatCount(node, "basechain", "tonNode.blockBroadcastCompressedV2", reason); got != 0 {
			t.Fatalf("decode worker repeated drop bookkeeping as %q: count = %d", reason, got)
		}
	}
	if !node.deduper.Seen(fingerprint, time.Now()) {
		t.Fatal("permanent signature failure released the deduper")
	}
}
