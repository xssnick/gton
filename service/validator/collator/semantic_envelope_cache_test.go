package collator

import (
	"testing"

	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestSemanticEnvelopeCacheReusesEqualContent(t *testing.T) {
	original := semanticDispatchBatchEnvelope(t, [32]byte{1}, 10, false).root
	twin, err := cell.FromBOC(original.ToBOC())
	if err != nil {
		t.Fatal(err)
	}
	var cache semanticEnvelopeCache
	first, err := cache.parse(original)
	if err != nil {
		t.Fatal(err)
	}
	second, err := cache.parse(twin)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("equal envelope content was parsed into separate objects")
	}

	other := semanticDispatchBatchEnvelope(t, [32]byte{1}, 11, false).root
	distinct, err := cache.parse(other)
	if err != nil {
		t.Fatal(err)
	}
	if distinct == first || distinct.internal.CreatedLT != 11 {
		t.Fatal("the cache returned another envelope's message")
	}
	malformed := original.ToBuilder().MustStoreBoolBit(true).EndCell()
	if _, err = cache.parse(malformed); err == nil {
		t.Fatal("the cache accepted an envelope with trailing data")
	}
}

// Descriptor consumers must share the objects cached during queue preparation;
// observing their identity catches a parser call that bypasses the replay cache.
func TestSemanticReplayDescriptorsShareCachedEnvelopes(t *testing.T) {
	request := benchMainnetCollatedRequest(t, benchMainnetHeavyRepeat)
	candidate, err := testBuilder().BuildShard(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	recorder := &recordingCandidateTransitionVerifier{}
	verification := shardVerificationRequest(request, candidate)
	verification.NeighborShardEndLT = request.NeighborShardEndLT
	verification.Semantics = recorder
	if err = verifyShardCandidateForTest(t.Context(), verification); err != nil {
		t.Fatal(err)
	}
	replay, err := newSemanticReplay(t.Context(), NewSemanticVerifier(tvm.NewTVM()), recorder.transition)
	if err != nil {
		t.Fatal(err)
	}
	if err = replay.precheckAccountUpdates(); err != nil {
		t.Fatal(err)
	}
	queues, err := replay.prepareQueueValidation()
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	check := func(envelope *semanticEnvelope) {
		t.Helper()
		if envelope == nil {
			return
		}
		count++
		if replay.envelopes.entries[envelope.root.HashKey()] != envelope {
			t.Fatal("a descriptor bypassed the replay envelope cache")
		}
	}
	for _, descriptor := range replay.parsedIn {
		check(descriptor.envelope)
		check(descriptor.outEnvelope)
	}
	for _, descriptor := range replay.parsedOut {
		check(descriptor.envelope)
	}
	if count == 0 {
		t.Fatal("the replay contains no envelope descriptors")
	}
	if err = replay.verifyAccounts(); err != nil {
		t.Fatal(err)
	}
	if err = queues.verifyAfterReplay(); err != nil {
		t.Fatal(err)
	}
}
