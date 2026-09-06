package collator

import (
	"bytes"
	"context"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
)

// The creator-statistics dictionary holds one entry per validator that produced
// a block recently, and entries only leave it when a producer sweeps them. The
// reference collator does that on every masterchain block; until this landed we
// did not, so on the blocks we produced the dictionary could only grow while
// every other node's blocks shrank it.
//
// The sweep is producer-local — a validator accepts a block whether or not it
// deletes anything — but its two properties are not negotiable: it must delete
// exactly the entries the reference's predicate calls outdated, and the block
// it produces must be reproducible, because a re-collation of the same slot
// compares bytes.
func TestSweepStaleCreatorStatsDeletesOnlyDecayedEntries(t *testing.T) {
	const now = uint32(1_800_000_000)

	stale := creatorStats{
		masterchain: discountedCounter{lastUpdated: 1_000, total: 3, count2048: 3 << 32, count65536: 3 << 32},
		shardchain:  discountedCounter{lastUpdated: 1_000, total: 7, count2048: 7 << 32, count65536: 7 << 32},
	}
	// One block produced a second ago: the 65536-second counter is still far
	// from zero, so the entry must survive a scan that visits it.
	fresh := creatorStats{
		masterchain: discountedCounter{lastUpdated: now - 1, total: 1, count2048: 1 << 32, count65536: 1 << 32},
	}

	for _, test := range []struct {
		name    string
		entries int
		value   creatorStats
		removed int
	}{
		{name: "decayed entries are swept up to the budget", entries: 1_500, value: stale, removed: creatorStatsScanKeys},
		{name: "recent entries are never swept", entries: 1_500, value: fresh, removed: 0},
		{name: "a dictionary smaller than the budget is scanned to its end", entries: 30, value: stale, removed: -1},
	} {
		t.Run(test.name, func(t *testing.T) {
			seeded := make(map[[32]byte]creatorStats, test.entries)
			for i := range test.entries {
				seeded[creatorStatsPruneKey(i)] = test.value
			}
			dict := blockCreateStatsTestStats(t, seeded).dict.Copy()

			start := creatorStatsScanStart([32]byte{0x11}, 7, now)
			// The scan walks forward from its start key and stops at the end of
			// the dictionary rather than wrapping, exactly as the reference's
			// lookup_nearest_key loop does, so what it can reach is the keys at
			// or above the start.
			want := test.removed
			if want < 0 {
				want = 0
				for key := range seeded {
					if bytes.Compare(key[:], start[:]) >= 0 {
						want++
					}
				}
				want = min(want, creatorStatsScanKeys)
			}
			removed, err := sweepStaleCreatorStats(dict, start, now, creatorStatsScanKeys)
			if err != nil {
				t.Fatalf("sweep: %v", err)
			}
			if removed != want {
				t.Fatalf("removed = %d, want %d", removed, want)
			}
			remaining, err := dict.LoadAll()
			if err != nil {
				t.Fatal(err)
			}
			if len(remaining) != test.entries-removed {
				t.Fatalf("dictionary holds %d entries, want %d", len(remaining), test.entries-removed)
			}
		})
	}

	t.Run("a disabled sweep touches nothing", func(t *testing.T) {
		seeded := map[[32]byte]creatorStats{creatorStatsPruneKey(1): stale}
		dict := blockCreateStatsTestStats(t, seeded).dict.Copy()
		removed, err := sweepStaleCreatorStats(dict, [32]byte{}, now, 0)
		if err != nil || removed != 0 {
			t.Fatalf("disabled sweep removed %d, err %v", removed, err)
		}
	})
}

// The reference draws the scan's start key from a real PRNG. We cannot: the
// size-limit retry re-enters the build for the same slot with the same request,
// the speculative self-window handoff re-collates, and the goldens compare
// bytes — so the start has to be a function of the candidate and of nothing
// else.
func TestCreatorStatsScanStartIsDeterministic(t *testing.T) {
	seed := [32]byte{0x41, 0x42}
	base := creatorStatsScanStart(seed, 100, 1_800_000_000)
	if again := creatorStatsScanStart(seed, 100, 1_800_000_000); base != again {
		t.Fatal("the same candidate produced two different scan starts")
	}
	for _, test := range []struct {
		name string
		key  [32]byte
	}{
		{name: "seed", key: creatorStatsScanStart([32]byte{0x41, 0x43}, 100, 1_800_000_000)},
		{name: "seqno", key: creatorStatsScanStart(seed, 101, 1_800_000_000)},
		{name: "generation time", key: creatorStatsScanStart(seed, 100, 1_800_000_001)},
	} {
		if test.key == base {
			t.Fatalf("changing the %s left the scan start unchanged", test.name)
		}
	}
}

// A block that deletes entries has to be one our own validator accepts. The
// deletion arrives at the validator as a diff entry with an old value and no
// new one, and its counter check has to read that as a legitimate removal
// rather than as a skipped increment — if it does not, every masterchain block
// we produce would be rejected by the committee and by ourselves.
func TestBuildMasterSweepProducesAVerifiableBlock(t *testing.T) {
	const entries = 400

	seeded := make(map[[32]byte]creatorStats, entries)
	for i := range entries {
		seeded[creatorStatsPruneKey(i)] = creatorStats{
			masterchain: discountedCounter{lastUpdated: 1_000, total: 3, count2048: 3 << 32, count65536: 3 << 32},
			shardchain:  discountedCounter{lastUpdated: 1_000, total: 7, count2048: 7 << 32, count65536: 7 << 32},
		}
	}
	fixture := newMasterBuildFixtureWith(t, masterBuildFixtureOptions{
		creatorStats: blockCreateStatsTestCell(t, seeded),
	})
	if fixture.request.Config.capabilities&capCreateStats == 0 {
		t.Skip("fixture configuration does not enable creator statistics")
	}

	builder := testBuilder()
	candidate, err := builder.BuildMaster(context.Background(), fixture.request)
	if err != nil {
		t.Fatalf("build masterchain candidate: %v", err)
	}
	if err = VerifyMasterCandidate(context.Background(), MasterVerificationRequest{
		Previous:  fixture.request.Previous,
		Config:    fixture.request.Config,
		Groups:    fixture.request.Groups,
		ShardTops: fixture.request.ShardTops,
		Semantics: testCandidateTransitionVerifier,
		Candidate: candidate,
	}); err != nil {
		t.Fatalf("verify a candidate whose sweep deleted entries: %v", err)
	}

	// The sweep really ran on this block, or the assertion above verified a
	// block that deleted nothing. Counted over the seeded keys alone: the block
	// also INSERTS entries — its own creator, the creators of the shard tops it
	// registers, and the aggregate — so the dictionary's size is not the
	// measure.
	produced := masterBuildCreatorStatsEntries(t, candidate)
	swept := 0
	for key := range seeded {
		if _, kept := produced[key]; !kept {
			swept++
		}
	}
	if swept != creatorStatsScanKeys {
		t.Fatalf("the sweep removed %d seeded entries, want %d", swept, creatorStatsScanKeys)
	}

	// Re-collating the same request must reproduce the block byte for byte;
	// this is the fence on the deterministic scan start.
	again, err := builder.BuildMaster(context.Background(), fixture.request)
	if err != nil {
		t.Fatalf("rebuild masterchain candidate: %v", err)
	}
	if !bytes.Equal(candidate.BlockBOC, again.BlockBOC) {
		t.Fatal("two builds of the same request produced different blocks")
	}
}

// masterBuildCreatorStatsEntries reads the creator entries the candidate's own
// state carries.
func masterBuildCreatorStatsEntries(t *testing.T, candidate *Candidate) map[[32]byte]creatorStats {
	t.Helper()

	var state tlb.ShardStateUnsplit
	if err := parseExact(&state, candidate.State); err != nil {
		t.Fatalf("decode candidate masterchain state: %v", err)
	}
	var extra tlb.McStateExtra
	if err := parseExact(&extra, state.McStateExtra); err != nil {
		t.Fatalf("decode candidate masterchain state extra: %v", err)
	}
	info, err := parseMasterStateInfo(extra.Info)
	if err != nil {
		t.Fatalf("decode candidate masterchain state info: %v", err)
	}
	if info.BlockCreateStats == nil {
		t.Fatal("candidate state carries no creator statistics")
	}
	stats, err := openBlockCreateStats(info.BlockCreateStats)
	if err != nil {
		t.Fatalf("open candidate creator statistics: %v", err)
	}

	return blockCreateStatsTestEntries(t, stats)
}
