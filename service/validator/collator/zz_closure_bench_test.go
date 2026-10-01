package collator

import (
	"context"
	"sort"
	"testing"
	"time"
)

func TestZZClosureAccountsCost(t *testing.T) {
	for _, fx := range []struct {
		name string
		req  func(testing.TB) ShardRequest
	}{
		{"full-collated 345tx", fullCollatedMainnetRequest},
		{"repeat=3 753tx", func(tb testing.TB) ShardRequest { return benchMainnetCollatedRequest(tb, 3) }},
		{"repeat=3 store-shaped", func(tb testing.TB) ShardRequest {
			return lazyParentPredecessor(tb, benchMainnetCollatedRequest(tb, 3))
		}},
	} {
		req := fx.req(t)
		b := testBuilder()
		samples := map[string][]time.Duration{}
		for range 12 {
			d := &candidateAssemblyDurations{}
			req.assembly = d
			if _, err := b.BuildShard(context.Background(), req); err != nil {
				t.Fatal(err)
			}
			for _, s := range d.substages {
				if s.stage == CollationStageValidationClosure {
					samples[s.name] = append(samples[s.name], s.duration)
				}
			}
			samples["stage"] = append(samples["stage"], d.stages[CollationStageValidationClosure])
		}
		mn := func(s []time.Duration) float64 {
			sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
			return float64(s[0].Microseconds()) / 1000
		}
		t.Logf("%-24s stage %6.2f ms | accounts %6.2f  out_queue %6.2f  processed %6.2f  dispatch %6.2f  immediate %6.2f",
			fx.name, mn(samples["stage"]), mn(samples["accounts"]), mn(samples["out_queue"]),
			mn(samples["processed_queue"]), mn(samples["dispatch_queue"]), mn(samples["immediate_queue"]))
	}
}
