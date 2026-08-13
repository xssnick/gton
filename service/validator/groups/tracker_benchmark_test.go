package groups

import (
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

var benchmarkActiveSessions []Session

// benchmarkActiveSessionFixture builds a mainnet-shaped masterchain state: a
// 100 validator set, the masterchain group and one shard group.
func benchmarkActiveSessionFixture(b *testing.B) (*Tracker, *Snapshot, *State) {
	b.Helper()

	wires := make([]testValidatorWire, 100)
	for i := range wires {
		wires[i] = testValidatorWire{
			index:    uint16(i),
			key:      groupTestBytes(byte(i)),
			adnl:     groupTestBytes(byte(i + 100)),
			weight:   uint64(i%17 + 1),
			withADNL: true,
		}
	}
	totalWeight := uint64(0)
	for i := range wires {
		totalWeight += wires[i].weight
	}

	shard := ShardID{Workchain: 0, Shard: -1 << 63}
	fixture := buildStateFixture(b, stateFixtureOptions{
		Seqno:            100,
		GenUTime:         1_700_000_000,
		CatchainSeqno:    44,
		RotatedAllShards: true,
		ConfigRoot: buildTestConfig(b, map[uint32]*cell.Cell{
			configParamCurrentValidators: buildTestValidatorSet(b, wires, true, totalWeight),
		}),
		ShardHashes: testShardHashes(b, testBinTreeLeaf(
			testParsedShardDescription(b, shard, 17, 91, tlb.FutureSplitMergeNone{}),
		)),
	})

	tracker, err := NewTracker(TrackerOptions{MaximalVerticalSeqno: 3})
	if err != nil {
		b.Fatal(err)
	}
	applied, err := tracker.Apply(ApplyInput{
		Block: fixture.Block,
		Root:  fixture.Root,
		AsOf:  time.Unix(1_700_000_000, 0),
	})
	if err != nil {
		b.Fatalf("apply fixture state: %v", err)
	}
	state, err := ParseState(StateInput{Block: fixture.Block, Root: fixture.Root})
	if err != nil {
		b.Fatalf("parse fixture state: %v", err)
	}

	return tracker, applied.Snapshot, state
}

func BenchmarkBuildActiveSessions(b *testing.B) {
	tracker, previous, state := benchmarkActiveSessionFixture(b)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		sessions, err := tracker.buildActiveSessions(nil, state, previous.Config, 0)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkActiveSessions = sessions
	}
}

func BenchmarkBuildActiveSessionsReused(b *testing.B) {
	tracker, previous, state := benchmarkActiveSessionFixture(b)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		sessions, err := tracker.buildActiveSessions(previous, state, previous.Config, 0)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkActiveSessions = sessions
	}
}
