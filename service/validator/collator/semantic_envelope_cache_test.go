package collator

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// TestValidationParsesEachEnvelopeOnce pins the envelope cache's whole point.
// The same envelope reaches one validation through up to five doors — inbound
// descriptor, outbound descriptor, candidate queue entry, predecessor queue
// entry, dequeue — and before the cache each parsed it from scratch: 4 047
// parses for 855 distinct envelopes on this very workload, a quarter of the
// validation's allocations and nine milliseconds of its wall time. The gate
// counts real parses through the cache's own miss path, so a call site that
// bypasses the cache — the regression this protects against, since every new
// consumer of parseSemanticEnvelope starts as exactly that — moves the count
// off the distinct-envelope floor.
func TestValidationParsesEachEnvelopeOnce(t *testing.T) {
	req := benchMainnetCollatedRequest(t, benchMainnetHeavyRepeat)
	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	verification := shardVerificationRequest(req, candidate)
	verification.NeighborShardEndLT = req.NeighborShardEndLT
	verification.Semantics = NewSemanticVerifier(tvm.NewTVM())

	var parses, unique atomic.Int64
	seen := make(map[cell.Hash]bool)
	previous := semanticEnvelopeParseProbe
	semanticEnvelopeParseProbe = func(root *cell.Cell) {
		parses.Add(1)
		if key := root.HashKey(); !seen[key] {
			seen[key] = true
			unique.Add(1)
		}
	}
	defer func() { semanticEnvelopeParseProbe = previous }()

	if err = VerifyShardCandidate(context.Background(), verification); err != nil {
		t.Fatal(err)
	}
	if parses.Load() == 0 {
		t.Fatal("the probe saw no envelope parses; the gate is measuring nothing")
	}
	// The masterchain and neighbor paths legitimately parse outside the cache,
	// and a shard validation has none of either — so on this workload the parse
	// count must sit exactly on the distinct-envelope floor.
	if parses.Load() != unique.Load() {
		t.Fatalf("validation parsed envelopes %d times for %d distinct envelopes: a call site bypasses the cache",
			parses.Load(), unique.Load())
	}
}
