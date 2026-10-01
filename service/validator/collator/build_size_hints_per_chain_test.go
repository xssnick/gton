package collator

import (
	"context"
	"testing"
)

// A build's sizes are for the next build of its own chain. One Builder serves
// the masterchain and the shards interleaved, so what one chain reports must
// never size the other chain's next build.
func TestBuildSizeHintsArePerChain(t *testing.T) {
	b := testBuilder()
	b.observeBuildSizes(MetricChainShardchain, 12_599, 11_189, 7_796, 10_388)
	b.observeBuildSizes(MetricChainMasterchain, 3_447, 146, 45, 558)

	for _, want := range []struct {
		chain                                MetricChain
		readSet, storage, storageProof, memo int
	}{
		{chain: MetricChainShardchain, readSet: 12_599, storage: 11_189, storageProof: 7_796, memo: 10_388},
		{chain: MetricChainMasterchain, readSet: 3_447, storage: 146, storageProof: 45, memo: 558},
	} {
		if got := b.readSetHint(want.chain); got != want.readSet {
			t.Fatalf("chain %d read set hint %d, want %d", want.chain, got, want.readSet)
		}
		cells, proofCells := b.storageHints(want.chain)
		if cells != want.storage || proofCells != want.storageProof {
			t.Fatalf("chain %d storage hints %d/%d, want %d/%d",
				want.chain, cells, proofCells, want.storage, want.storageProof)
		}
		if got := b.updateMemoHint(want.chain); got != want.memo+want.memo/8 {
			t.Fatalf("chain %d memo hint %d, want %d", want.chain, got, want.memo+want.memo/8)
		}
	}
}

// A finished build reports under the chain it built: a masterchain build fills
// the masterchain's hints and leaves the shard's alone, and the other way round.
func TestFinishedBuildReportsItsOwnChain(t *testing.T) {
	ctx := context.Background()
	builder := testBuilder()

	if _, err := builder.BuildMaster(ctx, newMasterBuildFixture(t, false).request); err != nil {
		t.Fatal(err)
	}
	master := builder.readSetHint(MetricChainMasterchain)
	if master == 0 {
		t.Fatal("the masterchain build did not report its read set")
	}
	if got := builder.readSetHint(MetricChainShardchain); got != 0 {
		t.Fatalf("the masterchain build reported %d cells into the shard's hint", got)
	}

	if _, err := builder.BuildShard(ctx, emptyCandidateRequest(t)); err != nil {
		t.Fatal(err)
	}
	if builder.readSetHint(MetricChainShardchain) == 0 {
		t.Fatal("the shard build did not report its read set")
	}
	if got := builder.readSetHint(MetricChainMasterchain); got != master {
		t.Fatalf("the shard build moved the masterchain's hint from %d to %d", master, got)
	}
}

// One Builder serves every chain the node collates, interleaved, so the build
// timed here always follows a build of the other chain — the one that runs with
// the timer stopped. What it measures is how a build is sized after that switch.
func BenchmarkCollateAlternatingChains(b *testing.B) {
	shard := benchWorkloadFor(b, benchProfiles[4])
	master := benchMasterWorkloadFor(b, benchMasterProfiles[1])
	ctx := context.Background()
	buildShard := func(builder *Builder) error {
		_, err := builder.BuildShard(ctx, shard.request)
		return err
	}
	buildMaster := func(builder *Builder) error {
		_, err := builder.BuildMaster(ctx, master.request)
		return err
	}

	for _, run := range []struct {
		name         string
		timed, other func(*Builder) error
	}{
		{name: "master-after-" + shard.profile.name + "-shard", timed: buildMaster, other: buildShard},
		{name: "shard-after-" + master.profile.name + "-master", timed: buildShard, other: buildMaster},
	} {
		b.Run(run.name, func(b *testing.B) {
			builder := testBuilder()
			if err := run.timed(builder); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				b.StopTimer()
				if err := run.other(builder); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				if err := run.timed(builder); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
