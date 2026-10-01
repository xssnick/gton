package validator

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/simplex"
)

// This is the previous full scan, retained as a differential oracle and a
// benchmark baseline. It deliberately does not inspect the running counters.
func retentionCapFloorByScan(r *candidateResolver, tip uint32) uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	backstop := r.budget.capSlots(r.params.TargetRate)
	floor := uint32(0)
	if tip > backstop {
		floor = tip - backstop
	}
	payloads := r.retentionScratch[:0]
	for id, entry := range r.retained {
		if id.Slot > tip || id.Slot < floor || entry.candidate == nil {
			continue
		}
		bytes := candidatePayloadBytes(entry.candidate, entry.wire)
		if bytes > 0 {
			payloads = append(payloads, retainedPayload{slot: id.Slot, bytes: bytes})
		}
	}
	r.retentionScratch = payloads
	slices.SortFunc(payloads, func(a, b retainedPayload) int { return cmp.Compare(b.slot, a.slot) })
	var bytes int64
	var count int
	for _, payload := range payloads {
		if bytes+payload.bytes > r.budget.Bytes || count+1 > r.budget.Payloads {
			return min(payload.slot+1, tip)
		}
		bytes += payload.bytes
		count++
	}
	return floor
}

func TestRetentionCapFloorMatchesFullScan(t *testing.T) {
	resolver := newResolverForTest(newRuntimeTestStorage(), nil, 1, simplex.DefaultParams())
	t.Cleanup(resolver.close)
	random := rand.New(rand.NewSource(713))
	// Include forks at one slot, empty/zero-byte artifacts, skipped and future
	// slots, and IDs at the uint32 boundary. Stage/release keep the same exact
	// accounting used by real admissions, lazy materialization and sweeps.
	for i := range 160 {
		slot := uint32(random.Intn(1200))
		if i < 4 {
			slot = math.MaxUint32 - uint32(i)
		}
		artifact := &CandidateArtifact{
			Candidate: simplex.Candidate{
				ID:    simplex.CandidateID{Slot: slot, Hash: [32]byte{byte(i), 0xc1}},
				Empty: i%11 == 0,
			},
		}
		if i%7 != 0 && !artifact.Candidate.Empty {
			artifact.BlockBOC = make([]byte, random.Intn(1000)+1)
			artifact.CollatedData = make([]byte, random.Intn(100))
		}
		wire := make([]byte, random.Intn(60))
		if err := resolver.stage(artifact, wire); err != nil {
			t.Fatal(err)
		}
		if i%13 == 0 {
			resolver.mu.Lock()
			resolver.releasePayloadLocked(artifact.Candidate.ID, resolver.entries[artifact.Candidate.ID])
			resolver.mu.Unlock()
		}
	}
	stats := resolver.cacheStats()
	if projection := resolver.cacheProjection(); projection != stats {
		t.Fatalf("counter projection %+v differs from scan %+v", projection, stats)
	}
	for _, rate := range []time.Duration{0, 200 * time.Millisecond, 2400 * time.Millisecond} {
		for _, tip := range []uint32{0, 1, 250, 600, 1200, math.MaxUint32 - 1, math.MaxUint32} {
			for _, bytes := range []int64{0, 1, stats.Bytes - 1, stats.Bytes, stats.Bytes + 1} {
				for _, payloads := range []int{0, 1, stats.Candidates - 1, stats.Candidates, stats.Candidates + 1} {
					resolver.mu.Lock()
					resolver.params.TargetRate = rate
					resolver.budget = retentionBudget{Bytes: bytes, Payloads: payloads, Duration: retentionFloorCapDuration}
					resolver.mu.Unlock()
					got, want := resolver.retentionCapFloor(tip), retentionCapFloorByScan(resolver, tip)
					if got != want {
						t.Fatalf("rate %v tip %d bytes %d count %d: floor %d, full scan %d", rate, tip, bytes, payloads, got, want)
					}
				}
			}
		}
	}
}

func TestRetentionCapFloorCountersFollowLazyMaterializationAndRelease(t *testing.T) {
	_, source, artifacts := storedLineageForTest(t, 3)
	source.close()
	resolver := newResolverForTest(newRuntimeTestStorage(), nil, 1, simplex.DefaultParams())
	t.Cleanup(resolver.close)
	for _, artifact := range artifacts {
		wire, _, err := resolver.codec.encodeForBroadcast(artifact)
		if err != nil {
			t.Fatal(err)
		}
		decoded, lazy, err := resolver.codec.decodeDeferred(wire, &artifact.Candidate.ID)
		if err != nil {
			t.Fatal(err)
		}
		if err := resolver.stageDeferred(decoded, lazy); err != nil {
			t.Fatal(err)
		}
	}
	check := func() {
		t.Helper()
		stats := resolver.cacheStats()
		if projection := resolver.cacheProjection(); projection != stats {
			t.Fatalf("counter projection %+v differs from scan %+v", projection, stats)
		}
		resolver.mu.Lock()
		resolver.budget = retentionBudget{Bytes: stats.Bytes, Payloads: stats.Candidates, Duration: retentionFloorCapDuration}
		resolver.mu.Unlock()
		if got, want := resolver.retentionCapFloor(250), retentionCapFloorByScan(resolver, 250); got != want {
			t.Fatalf("floor at exact counter boundaries = %d, want %d", got, want)
		}
	}
	check()
	for _, artifact := range artifacts {
		if err := resolver.store(context.Background(), artifact.Candidate.ID); err != nil {
			t.Fatal(err)
		}
		check()
	}
	releaseLineagePayloads(resolver, artifacts)
	check()
}

func BenchmarkRetentionCapFloor(b *testing.B) {
	for _, length := range []int{64, 1024} {
		for _, scan := range []bool{true, false} {
			name := "counters"
			if scan {
				name = "scan"
			}
			b.Run(fmt.Sprintf("%d/%s", length, name), func(b *testing.B) {
				resolver := newResolverForTest(newRuntimeTestStorage(), nil, 1, simplex.DefaultParams())
				defer resolver.close()
				resolver.params.TargetRate = 200 * time.Millisecond
				for i := range length {
					artifact := &CandidateArtifact{
						Candidate: simplex.Candidate{ID: simplex.CandidateID{Slot: uint32(i)}},
						BlockBOC:  make([]byte, 1024),
					}
					if err := resolver.stage(artifact, []byte{1}); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportAllocs()
				for b.Loop() {
					if scan {
						retentionCapFloorByScan(resolver, uint32(length))
					} else {
						resolver.retentionCapFloor(uint32(length))
					}
				}
			})
		}
	}
}
