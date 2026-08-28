package collator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

// The three arms of the inbound internal phase must produce one block. The
// sequential loop is the reference; the inline wave arm exercises speculation
// and replay through every lane tracer with no concurrency, so a divergence
// there is a capture or replay defect; the concurrent arm adds the scheduling,
// so a divergence there alone is a race. Every fixture that pins bytes
// elsewhere is run through all three.
func TestInternalWavesProduceTheSequentialBlock(t *testing.T) {
	arms := []struct {
		name    string
		workers int
	}{
		{"sequential", -1},
		{"waves-inline", 1},
		{"waves-16", 16},
	}
	fixtures := []struct {
		name    string
		request func(testing.TB) ShardRequest
		// fills marks a fixture whose block fills before the queue is drained.
		// On such a fixture the wave holding the stopping point has speculated
		// past it, and the plans beyond it are discarded with their reads
		// unreplayed. A leaked read would reach the collated proof — it is
		// selected from the record — so byte identity on these arms is what
		// proves the discard path drops everything it must.
		fills bool
	}{
		{"mainnet full-collated", fullCollatedMainnetRequest, false},
		{"mainnet full-collated lazy parent", func(tb testing.TB) ShardRequest {
			return lazyParentPredecessor(tb, fullCollatedMainnetRequest(tb))
		}, false},
		{"mainnet repeat=3 collated", func(tb testing.TB) ShardRequest {
			return benchMainnetCollatedRequest(tb, 3)
		}, true},
		{"mainnet repeat=3 store-shaped", func(tb testing.TB) ShardRequest {
			return lazyParentPredecessor(tb, benchMainnetCollatedRequest(tb, 3))
		}, true},
		{"mainnet heavy plain", func(tb testing.TB) ShardRequest {
			req, _ := benchMainnetRequestRepeated(tb, benchMainnetFiller, benchMainnetHeavyRepeat)
			return req
		}, true},
		// The repeat=3 arms fill the block, but every account they discard was
		// already touched by an earlier repeat, so a leaked read of a discarded
		// lane would add nothing the record did not hold — measured: all 26
		// discards were existing lanes. This arm fills the block on repeat=1,
		// where 229 of 257 destinations are distinct, so the plans past the
		// stopping point are fresh lanes whose reads nobody else recorded.
		// It is the arm that makes a leaked discard visible.
		{"mainnet full-collated, tight bytes", func(tb testing.TB) ShardRequest {
			req := fullCollatedMainnetRequest(tb)
			withTightBlockBytes(tb, &req, 96<<10)
			return req
		}, true},
	}
	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			var reference *Candidate
			for _, arm := range arms {
				req := fixture.request(t)
				req.internalWaveWorkers = arm.workers
				candidate, err := testBuilder().BuildShard(context.Background(), req)
				if err != nil {
					t.Fatalf("%s: %v", arm.name, err)
				}
				if candidate.Stats.Transactions == 0 {
					t.Fatalf("%s: the fixture executed nothing, so it compares nothing", arm.name)
				}
				if fixture.fills {
					if candidate.Stats.OverloadReason != OverloadBlockLimit || candidate.Stats.EnqueuedMessages == 0 {
						t.Fatalf("%s: the block did not fill (overload %v, %d enqueued), so the discard path was not exercised",
							arm.name, candidate.Stats.OverloadReason, candidate.Stats.EnqueuedMessages)
					}
					if int(candidate.Stats.InternalsImported) >= len(req.Internals.Messages) {
						t.Fatalf("%s: every queued internal was imported, so nothing was left to discard", arm.name)
					}
				}
				if reference == nil {
					reference = candidate
					blockSum := sha256.Sum256(candidate.BlockBOC)
					t.Logf("%s: %d tx, %d internals, block %d B %s, collated %d B", arm.name,
						candidate.Stats.Transactions, candidate.Stats.InternalsImported,
						len(candidate.BlockBOC), hex.EncodeToString(blockSum[:8]), len(candidate.CollatedData))
					continue
				}
				if !bytes.Equal(candidate.BlockBOC, reference.BlockBOC) {
					t.Fatalf("%s produced a different block (%d B against %d B)", arm.name,
						len(candidate.BlockBOC), len(reference.BlockBOC))
				}
				if !bytes.Equal(candidate.CollatedData, reference.CollatedData) {
					t.Fatalf("%s produced different collated data (%d B against %d B)", arm.name,
						len(candidate.CollatedData), len(reference.CollatedData))
				}
				// The speculation counters describe the scheduling, not the block,
				// and are the one thing allowed to differ between arms.
				got, want := candidate.Stats, reference.Stats
				got.InternalsSpeculated, got.InternalsDiscarded, got.InternalsChained = 0, 0, 0
				want.InternalsSpeculated, want.InternalsDiscarded, want.InternalsChained = 0, 0, 0
				if got != want {
					t.Fatalf("%s produced different stats: %+v against %+v", arm.name, got, want)
				}
				if arm.workers > 0 {
					t.Logf("%s: speculated %d, discarded %d, chained %d", arm.name,
						candidate.Stats.InternalsSpeculated, candidate.Stats.InternalsDiscarded,
						candidate.Stats.InternalsChained)
					if fixture.fills && candidate.Stats.InternalsDiscarded == 0 {
						t.Fatalf("%s: the block filled but no speculated plan was discarded — the stopping point "+
							"landed on a wave boundary and the discard path went untested", arm.name)
					}
					if arm.workers > 1 && candidate.Stats.InternalsChained == 0 {
						t.Fatalf("%s: no in-wave successor was chained — the fixture carries account chains "+
							"in every wave, so the worker handoff went untested", arm.name)
					}
				}
				if arm.workers == 1 && candidate.Stats.InternalsChained != 0 {
					t.Fatalf("%s: the inline arm chained %d successors; chaining belongs to workers only",
						arm.name, candidate.Stats.InternalsChained)
				}
			}
		})
	}
}

// withTightBlockBytes lowers the basechain block byte limits so the block
// fills well before the queue drains. The config is the request's own copy.
func withTightBlockBytes(tb testing.TB, req *ShardRequest, soft uint64) {
	tb.Helper()
	limits := &req.Masterchain.Config.basechain.limits
	limits.bytes = limitThresholds{soft / 2, soft, soft + soft/2, soft * 2}
}
