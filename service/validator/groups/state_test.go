package groups

import (
	"bytes"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type stateFixtureOptions struct {
	Seqno            uint32
	GenUTime         uint32
	CatchainSeqno    uint32
	RotatedAllShards bool
	AfterKeyBlock    bool
	LastKeyBlock     *tlb.ExtBlkRef
	ShardHashes      *cell.Dictionary
	TrailingStateBit bool
	TrailingInfoBit  bool
	BlockCreateStats *cell.Cell
	ConfigRoot       *cell.Cell
	ConfigAddress    [32]byte
	Accounts         *tlb.ShardAccountsAugDict
}

type lastKeyBlockTestCase struct {
	name          string
	afterKeyBlock bool
	lastKeyBlock  *tlb.ExtBlkRef
	want          uint32
	wantError     bool
}

type shardValidityTestCase struct {
	name  string
	shard ShardID
	want  bool
}

type shardPrefixBitsTestCase struct {
	name      string
	shard     ShardID
	want      uint32
	wantError bool
}

func TestShardIDIsValid(t *testing.T) {
	t.Parallel()

	cases := []shardValidityTestCase{
		{name: "masterchain root", shard: masterchainShardID(), want: true},
		{name: "basechain root", shard: ShardID{Workchain: 0, Shard: masterchainShard}, want: true},
		{name: "maximum depth", shard: ShardID{Workchain: 0, Shard: 1 << 3}, want: true},
		{name: "past maximum depth", shard: ShardID{Workchain: 0, Shard: 1 << 2}},
		{name: "zero shard", shard: ShardID{Workchain: 0}},
		{name: "invalid workchain", shard: ShardID{Workchain: invalidWorkchain, Shard: masterchainShard}},
		{name: "masterchain child", shard: ShardID{Workchain: masterchainWorkchain, Shard: int64(tlb.ShardID(masterchainShardBits).GetChild(true))}},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := testCase.shard.IsValid(); got != testCase.want {
				t.Fatalf("IsValid() = %t, want %t", got, testCase.want)
			}
		})
	}
}

func TestShardIDPrefixBits(t *testing.T) {
	t.Parallel()

	cases := []shardPrefixBitsTestCase{
		{name: "masterchain root", shard: masterchainShardID()},
		{name: "basechain root", shard: ShardID{Workchain: 0, Shard: masterchainShard}},
		{
			name:  "basechain child",
			shard: ShardID{Workchain: 0, Shard: int64(tlb.ShardID(masterchainShardBits).GetChild(true))},
			want:  1,
		},
		{name: "maximum depth", shard: ShardID{Workchain: 0, Shard: 1 << 3}, want: 60},
		{name: "past maximum depth", shard: ShardID{Workchain: 0, Shard: 1 << 2}, wantError: true},
		{name: "zero shard", shard: ShardID{Workchain: 0}, wantError: true},
		{name: "invalid workchain", shard: ShardID{Workchain: invalidWorkchain, Shard: masterchainShard}, wantError: true},
		{
			name: "masterchain child",
			shard: ShardID{
				Workchain: masterchainWorkchain,
				Shard:     int64(tlb.ShardID(masterchainShardBits).GetChild(true)),
			},
			wantError: true,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := testCase.shard.PrefixBits()
			if testCase.wantError {
				if err == nil {
					t.Fatalf("PrefixBits() = %d, want error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("PrefixBits(): %v", err)
			}
			if got != testCase.want {
				t.Fatalf("PrefixBits() = %d, want %d", got, testCase.want)
			}
		})
	}
}

func TestParseState(t *testing.T) {
	t.Parallel()

	shard := ShardID{Workchain: 0, Shard: masterchainShard}
	description := testParsedShardDescription(t, shard, 17, 91, tlb.FutureSplitMergeNone{})
	input := buildStateFixture(t, stateFixtureOptions{
		Seqno:            100,
		GenUTime:         1_700_000_000,
		CatchainSeqno:    44,
		RotatedAllShards: true,
		AfterKeyBlock:    true,
		ShardHashes:      testShardHashes(t, testBinTreeLeaf(description)),
	})

	state, err := ParseState(input)
	if err != nil {
		t.Fatalf("ParseState: %v", err)
	}
	if state.Block.SeqNo != 100 || state.GenUTime != 1_700_000_000 {
		t.Fatalf("unexpected state header: %+v", state)
	}
	if state.CatchainSeqno != 44 || !state.RotatedAllShards {
		t.Fatalf("unexpected validator info: catchain=%d rotated=%t", state.CatchainSeqno, state.RotatedAllShards)
	}
	if !state.IsKeyState {
		t.Fatal("after_key_block was not projected as a key state")
	}
	if state.LastKeyBlockSeqno != 100 {
		t.Fatalf("last key block seqno = %d, want 100", state.LastKeyBlockSeqno)
	}
	if state.ConfigRoot == nil {
		t.Fatal("config root is absent")
	}
	if len(state.Shards) != 1 {
		t.Fatalf("shard descriptions = %d, want 1", len(state.Shards))
	}
	got := state.Shards[0]
	if got.Shard != shard || got.Block.SeqNo != 17 || got.NextCatchainSeqno != 91 {
		t.Fatalf("unexpected shard description: %+v", got)
	}
}

func TestParseStateLastKeyBlockSemantics(t *testing.T) {
	t.Parallel()

	cases := []lastKeyBlockTestCase{
		{
			name:          "current state is after key block",
			afterKeyBlock: true,
			want:          100,
		},
		{
			name:         "embedded previous key block",
			lastKeyBlock: testExtBlockRef(77),
			want:         77,
		},
		{
			name: "no previous key block",
		},
		{
			name:         "future key block",
			lastKeyBlock: testExtBlockRef(101),
			wantError:    true,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			input := buildStateFixture(t, stateFixtureOptions{
				Seqno:         100,
				AfterKeyBlock: testCase.afterKeyBlock,
				LastKeyBlock:  testCase.lastKeyBlock,
				ShardHashes: testShardHashes(t, testBinTreeLeaf(
					testParsedShardDescription(
						t,
						ShardID{Workchain: 0, Shard: masterchainShard},
						1,
						1,
						tlb.FutureSplitMergeNone{},
					),
				)),
			})

			state, err := ParseState(input)
			if testCase.wantError {
				if err == nil {
					t.Fatal("ParseState accepted future last key block")
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseState: %v", err)
			}
			if state.LastKeyBlockSeqno != testCase.want {
				t.Fatalf("last key block seqno = %d, want %d", state.LastKeyBlockSeqno, testCase.want)
			}
		})
	}
}

func TestParseStateRejectsMismatchesAndTrailingData(t *testing.T) {
	t.Parallel()

	description := testParsedShardDescription(
		t,
		ShardID{Workchain: 0, Shard: masterchainShard},
		1,
		1,
		tlb.FutureSplitMergeNone{},
	)
	shards := testShardHashes(t, testBinTreeLeaf(description))

	t.Run("invalid block root hash", func(t *testing.T) {
		input := buildStateFixture(t, stateFixtureOptions{Seqno: 10, ShardHashes: shards})
		input.Block.RootHash = input.Block.RootHash[:31]
		if _, err := ParseState(input); err == nil {
			t.Fatal("ParseState accepted invalid block root hash")
		}
	})

	t.Run("seqno", func(t *testing.T) {
		input := buildStateFixture(t, stateFixtureOptions{Seqno: 10, ShardHashes: shards})
		input.Block.SeqNo++
		if _, err := ParseState(input); err == nil {
			t.Fatal("ParseState accepted mismatched seqno")
		}
	})

	t.Run("non masterchain block", func(t *testing.T) {
		input := buildStateFixture(t, stateFixtureOptions{Seqno: 10, ShardHashes: shards})
		input.Block.Workchain = 0
		if _, err := ParseState(input); err == nil {
			t.Fatal("ParseState accepted non-masterchain block")
		}
	})

	t.Run("state trailing bit", func(t *testing.T) {
		input := buildStateFixture(t, stateFixtureOptions{
			Seqno:            10,
			ShardHashes:      shards,
			TrailingStateBit: true,
		})
		_, err := ParseState(input)
		if err == nil || !strings.Contains(err.Error(), "masterchain state has 1 trailing bits") {
			t.Fatalf("ParseState error = %v, want state trailing data", err)
		}
	})

	t.Run("info trailing bit", func(t *testing.T) {
		input := buildStateFixture(t, stateFixtureOptions{
			Seqno:           10,
			ShardHashes:     shards,
			TrailingInfoBit: true,
		})
		_, err := ParseState(input)
		if err == nil || !strings.Contains(err.Error(), "masterchain state info has 1 trailing bits") {
			t.Fatalf("ParseState error = %v, want info trailing data", err)
		}
	})

	t.Run("descriptor trailing bit", func(t *testing.T) {
		trailingDescription := description.ToBuilder().MustStoreBoolBit(true).EndCell()
		input := buildStateFixture(t, stateFixtureOptions{
			Seqno:       10,
			ShardHashes: testShardHashes(t, testBinTreeLeaf(trailingDescription)),
		})
		_, err := ParseState(input)
		if err == nil || !strings.Contains(err.Error(), "trailing bits") {
			t.Fatalf("ParseState error = %v, want descriptor trailing data", err)
		}
	})

	t.Run("workchain wrapper trailing bit", func(t *testing.T) {
		dict := cell.NewDict(32)
		value := cell.BeginCell().MustStoreBoolBit(true).MustStoreRef(testBinTreeLeaf(description)).EndCell()
		if err := dict.Set(cell.BeginCell().MustStoreInt(0, 32).EndCell(), value); err != nil {
			t.Fatalf("store malformed shard hashes: %v", err)
		}
		input := buildStateFixture(t, stateFixtureOptions{Seqno: 10, ShardHashes: dict})
		_, err := ParseState(input)
		if err == nil || !strings.Contains(err.Error(), "exactly one reference") {
			t.Fatalf("ParseState error = %v, want wrapper shape error", err)
		}
	})
}

func TestParseStateAllowsEmptyShardHashesOnlyAtZeroState(t *testing.T) {
	t.Parallel()

	zeroInput := buildStateFixture(t, stateFixtureOptions{Seqno: 0})
	state, err := ParseState(zeroInput)
	if err != nil {
		t.Fatalf("ParseState zero state: %v", err)
	}
	if len(state.Shards) != 0 {
		t.Fatalf("zero state shards = %d, want 0", len(state.Shards))
	}
	future, err := state.FutureShards(time.Unix(1_000, 0), time.Minute)
	if err != nil {
		t.Fatalf("zero state FutureShards: %v", err)
	}
	if len(future) != 1 || future[0] != masterchainShardID() {
		t.Fatalf("zero state future shards = %+v, want masterchain", future)
	}

	nonzeroInput := buildStateFixture(t, stateFixtureOptions{Seqno: 1})
	if _, err = ParseState(nonzeroInput); err == nil {
		t.Fatal("ParseState accepted absent nonzero shard hashes")
	}
}

func TestParseStateAcceptsOpaqueBlockCreateStatsVariants(t *testing.T) {
	t.Parallel()

	variants := []*cell.Cell{
		cell.BeginCell().MustStoreUInt(0x17, 8).MustStoreBoolBit(false).EndCell(),
		cell.BeginCell().MustStoreUInt(0x34, 8).MustStoreBoolBit(false).MustStoreUInt(0, 32).EndCell(),
	}
	for _, stats := range variants {
		input := buildStateFixture(t, stateFixtureOptions{
			Seqno:            0,
			BlockCreateStats: stats,
		})
		if _, err := ParseState(input); err != nil {
			t.Fatalf("ParseState block create stats %x: %v", stats.Hash(), err)
		}
	}
}

func TestParseStateRejectsInvalidBlockCreateStatsVariant(t *testing.T) {
	t.Parallel()

	t.Run("unknown magic", func(t *testing.T) {
		input := buildStateFixture(t, stateFixtureOptions{
			Seqno:            0,
			BlockCreateStats: cell.BeginCell().MustStoreUInt(0xff, 8).EndCell(),
		})
		if _, err := ParseState(input); err == nil {
			t.Fatal("ParseState accepted unknown block create stats magic")
		}
	})

	t.Run("truncated magic", func(t *testing.T) {
		input := buildStateFixture(t, stateFixtureOptions{
			Seqno:            0,
			BlockCreateStats: cell.BeginCell().MustStoreUInt(0, 7).EndCell(),
		})
		if _, err := ParseState(input); err == nil {
			t.Fatal("ParseState accepted truncated block create stats magic")
		}
	})
}

func TestParseStateShardDescriptionVariantsAreSorted(t *testing.T) {
	t.Parallel()

	root := ShardID{Workchain: 0, Shard: masterchainShard}
	left, right := mustShardChildren(t, root)
	leftCell, err := tlb.ToCell(tlb.ShardDesc{
		SeqNo:              10,
		RootHash:           testHash(0x10),
		FileHash:           testHash(0x11),
		NextCatchainSeqNo:  7,
		NextValidatorShard: left.Shard,
		SplitMergeAt:       tlb.FutureMerge{MergeUtime: 1_234, Interval: 5},
	})
	if err != nil {
		t.Fatalf("build #a shard description: %v", err)
	}
	rightCell, err := tlb.ToCell(tlb.ShardDescB{
		SeqNo:              11,
		RootHash:           testHash(0x20),
		FileHash:           testHash(0x21),
		NextCatchainSeqNo:  8,
		NextValidatorShard: right.Shard,
		SplitMergeAt:       tlb.FutureMerge{MergeUtime: 1_235, Interval: 6},
	})
	if err != nil {
		t.Fatalf("build #b shard description: %v", err)
	}
	tree := cell.BeginCell().
		MustStoreUInt(1, 1).
		MustStoreRef(testBinTreeLeaf(leftCell)).
		MustStoreRef(testBinTreeLeaf(rightCell)).
		EndCell()
	input := buildStateFixture(t, stateFixtureOptions{
		Seqno:       100,
		ShardHashes: testShardHashes(t, tree),
	})

	state, err := ParseState(input)
	if err != nil {
		t.Fatalf("ParseState: %v", err)
	}
	if len(state.Shards) != 2 || state.Shards[0].Shard != left || state.Shards[1].Shard != right {
		t.Fatalf("shards are not sorted left/right: %+v", state.Shards)
	}
	if state.Shards[0].FSM != (ShardFSM{Kind: ShardFSMMerge, UTime: 1_234, Interval: 5}) {
		t.Fatalf("#a FSM = %+v", state.Shards[0].FSM)
	}
	if state.Shards[1].FSM != (ShardFSM{Kind: ShardFSMMerge, UTime: 1_235, Interval: 6}) {
		t.Fatalf("#b FSM = %+v", state.Shards[1].FSM)
	}
}

func TestStateCurrentTargets(t *testing.T) {
	t.Parallel()

	root := ShardID{Workchain: 0, Shard: masterchainShard}
	left, right := mustShardChildren(t, root)

	t.Run("normal", func(t *testing.T) {
		description := testTopologyDescription(root, 1)
		targets, err := testTopologyState(description).CurrentTargets()
		if err != nil {
			t.Fatalf("CurrentTargets: %v", err)
		}
		if len(targets) != 2 || targets[1].Shard != root || !sameBlockID(targets[1].Genesis[0], description.Block) {
			t.Fatalf("unexpected normal targets: %+v", targets)
		}
		if len(targets[1].Registered) != 1 || !sameBlockID(targets[1].Registered[0].Block, description.Block) {
			t.Fatalf("normal target registered context = %+v", targets[1].Registered)
		}
	})

	t.Run("split", func(t *testing.T) {
		description := testTopologyDescription(root, 2)
		description.BeforeSplit = true
		targets, err := testTopologyState(description).CurrentTargets()
		if err != nil {
			t.Fatalf("CurrentTargets: %v", err)
		}
		if len(targets) != 3 || targets[1].Shard != left || targets[2].Shard != right {
			t.Fatalf("unexpected split targets: %+v", targets)
		}
		if !sameBlockID(targets[1].Genesis[0], description.Block) || !sameBlockID(targets[2].Genesis[0], description.Block) {
			t.Fatal("split targets do not use the parent top block")
		}
		if len(targets[1].Registered) != 1 || len(targets[2].Registered) != 1 ||
			!sameBlockID(targets[1].Registered[0].Block, description.Block) ||
			!sameBlockID(targets[2].Registered[0].Block, description.Block) {
			t.Fatalf("split target registered context = %+v / %+v", targets[1].Registered, targets[2].Registered)
		}
	})

	t.Run("merge left right order", func(t *testing.T) {
		leftDescription := testTopologyDescription(left, 3)
		leftDescription.BeforeMerge = true
		rightDescription := testTopologyDescription(right, 4)
		rightDescription.BeforeMerge = true
		targets, err := testTopologyState(rightDescription, leftDescription).CurrentTargets()
		if err != nil {
			t.Fatalf("CurrentTargets: %v", err)
		}
		if len(targets) != 2 || targets[1].Shard != root || len(targets[1].Genesis) != 2 {
			t.Fatalf("unexpected merge targets: %+v", targets)
		}
		if !sameBlockID(targets[1].Genesis[0], leftDescription.Block) || !sameBlockID(targets[1].Genesis[1], rightDescription.Block) {
			t.Fatalf("merge genesis is not left/right ordered: %+v", targets[1].Genesis)
		}
		if len(targets[1].Registered) != 2 ||
			!sameBlockID(targets[1].Registered[0].Block, leftDescription.Block) ||
			!sameBlockID(targets[1].Registered[1].Block, rightDescription.Block) {
			t.Fatalf("merge target registered context = %+v", targets[1].Registered)
		}
	})

	t.Run("missing merge sibling", func(t *testing.T) {
		description := testTopologyDescription(left, 5)
		description.BeforeMerge = true
		if _, err := testTopologyState(description).CurrentTargets(); err == nil {
			t.Fatal("CurrentTargets accepted missing merge sibling")
		}
	})

	t.Run("mismatched merge sibling", func(t *testing.T) {
		leftDescription := testTopologyDescription(left, 6)
		leftDescription.BeforeMerge = true
		rightDescription := testTopologyDescription(right, 7)
		if _, err := testTopologyState(leftDescription, rightDescription).CurrentTargets(); err == nil {
			t.Fatal("CurrentTargets accepted mismatched merge sibling")
		}
	})
}

func TestStateGroupLifecycleIgnoresCustomWorkchains(t *testing.T) {
	t.Parallel()

	basechain := testTopologyDescription(ShardID{Workchain: basechainWorkchain, Shard: masterchainShard}, 1)
	custom := testTopologyDescription(ShardID{Workchain: 1, Shard: masterchainShard}, 2)
	custom.BeforeSplit = true
	custom.FSM = ShardFSM{Kind: ShardFSMSplit, UTime: 1}
	state := testTopologyState(custom, basechain)

	targets, err := state.CurrentTargets()
	if err != nil {
		t.Fatalf("CurrentTargets: %v", err)
	}
	if len(targets) != 2 || targets[0].Shard != masterchainShardID() || targets[1].Shard != basechain.Shard {
		t.Fatalf("active targets = %+v, want masterchain and basechain", targets)
	}

	future, err := state.FutureShards(time.Unix(1_000, 0), time.Minute)
	if err != nil {
		t.Fatalf("FutureShards: %v", err)
	}
	if len(future) != 2 || future[0] != masterchainShardID() || future[1] != basechain.Shard {
		t.Fatalf("future shards = %+v, want masterchain and basechain", future)
	}
}

func TestStateFutureShardsUsesStrictBoundary(t *testing.T) {
	t.Parallel()

	root := ShardID{Workchain: 0, Shard: masterchainShard}
	left, right := mustShardChildren(t, root)
	asOf := time.Unix(1_000, 0)

	description := testTopologyDescription(root, 1)
	description.FSM = ShardFSM{Kind: ShardFSMSplit, UTime: 1_060}
	shards, err := testTopologyState(description).FutureShards(asOf, time.Minute)
	if err != nil {
		t.Fatalf("FutureShards at boundary: %v", err)
	}
	if len(shards) != 2 || shards[0] != masterchainShardID() || shards[1] != root {
		t.Fatalf("FSM at strict boundary activated: %+v", shards)
	}

	description.FSM.UTime--
	shards, err = testTopologyState(description).FutureShards(asOf, time.Minute)
	if err != nil {
		t.Fatalf("FutureShards before boundary: %v", err)
	}
	if len(shards) != 3 || shards[0] != masterchainShardID() || shards[1] != left || shards[2] != right {
		t.Fatalf("due split shards = %+v, want left/right", shards)
	}

	// The deadline retains sub-second precision, so integer second 1060 is
	// already due when the deadline is 1060.5.
	description.FSM.UTime = 1_060
	shards, err = testTopologyState(description).FutureShards(asOf.Add(500*time.Millisecond), time.Minute)
	if err != nil {
		t.Fatalf("FutureShards fractional boundary: %v", err)
	}
	if len(shards) != 3 {
		t.Fatalf("fractional deadline did not activate split: %+v", shards)
	}
}

func TestStateFutureShardsMergeAndTimeValidation(t *testing.T) {
	t.Parallel()

	root := ShardID{Workchain: 0, Shard: masterchainShard}
	left, right := mustShardChildren(t, root)
	leftDescription := testTopologyDescription(left, 1)
	leftDescription.FSM = ShardFSM{Kind: ShardFSMMerge, UTime: 1_001}
	rightDescription := testTopologyDescription(right, 2)
	rightDescription.FSM = ShardFSM{Kind: ShardFSMMerge, UTime: 1_002}

	shards, err := testTopologyState(rightDescription, leftDescription).FutureShards(time.Unix(1_000, 0), time.Minute)
	if err != nil {
		t.Fatalf("FutureShards merge: %v", err)
	}
	if len(shards) != 2 || shards[0] != masterchainShardID() || shards[1] != root {
		t.Fatalf("future merge shards = %+v, want root", shards)
	}

	if _, err = testTopologyState(leftDescription).FutureShards(time.Unix(1_000, 0), time.Minute); err == nil {
		t.Fatal("FutureShards accepted incomplete merge pair")
	}
	if _, err = testTopologyState(leftDescription).FutureShards(time.Time{}, time.Minute); err == nil {
		t.Fatal("FutureShards accepted zero observation time")
	}
	if _, err = testTopologyState(leftDescription).FutureShards(time.Unix(1_000, 0), -time.Second); err == nil {
		t.Fatal("FutureShards accepted negative horizon")
	}
	if _, err = testTopologyState(leftDescription).FutureShards(time.Unix(math.MaxUint32, 0), time.Second); err == nil {
		t.Fatal("FutureShards accepted uint32 time overflow")
	}
}

func TestStateCatchainSeqnoFor(t *testing.T) {
	t.Parallel()

	root := ShardID{Workchain: 0, Shard: masterchainShard}
	left, right := mustShardChildren(t, root)
	leftLeft, _ := mustShardChildren(t, left)

	rootDescription := testTopologyDescription(root, 1)
	rootDescription.NextCatchainSeqno = 7
	state := testTopologyState(rootDescription)
	state.CatchainSeqno = 99

	got, err := state.CatchainSeqnoFor(masterchainShardID())
	if err != nil || got != 99 {
		t.Fatalf("masterchain catchain seqno = %d, %v; want 99", got, err)
	}
	got, err = state.CatchainSeqnoFor(leftLeft)
	if err != nil || got != 7 {
		t.Fatalf("ancestor catchain seqno = %d, %v; want 7", got, err)
	}

	leftDescription := testTopologyDescription(left, 2)
	leftDescription.NextCatchainSeqno = 8
	rightDescription := testTopologyDescription(right, 3)
	rightDescription.NextCatchainSeqno = 10
	got, err = testTopologyState(leftDescription, rightDescription).CatchainSeqnoFor(root)
	if err != nil || got != 11 {
		t.Fatalf("merge catchain seqno = %d, %v; want 11", got, err)
	}

	rightDescription.NextCatchainSeqno = math.MaxUint32
	if _, err = testTopologyState(leftDescription, rightDescription).CatchainSeqnoFor(root); err == nil {
		t.Fatal("CatchainSeqnoFor accepted merge seqno overflow")
	}

	unknown := ShardID{Workchain: 1, Shard: masterchainShard}
	if _, err = state.CatchainSeqnoFor(unknown); !errors.Is(err, ErrNotFound) {
		t.Fatalf("CatchainSeqnoFor error = %v, want ErrNotFound", err)
	}
}

func TestExactFinalizedBlockUsesExactShard(t *testing.T) {
	t.Parallel()

	root := ShardID{Workchain: 0, Shard: masterchainShard}
	left, right := mustShardChildren(t, root)
	leftDescription := testTopologyDescription(left, 1)
	rightDescription := testTopologyDescription(right, 2)
	state := testTopologyState(leftDescription, rightDescription)

	master := exactFinalizedBlock(state, masterchainShardID())
	if master == nil || !sameBlockID(*master, state.Block) {
		t.Fatalf("masterchain finalized block = %+v", master)
	}
	leftBlock := exactFinalizedBlock(state, left)
	if leftBlock == nil || !sameBlockID(*leftBlock, leftDescription.Block) {
		t.Fatalf("left finalized block = %+v", leftBlock)
	}
	if block := exactFinalizedBlock(state, root); block != nil {
		t.Fatalf("merge parent finalized block = %+v, want absent", block)
	}

	splitState := testTopologyState(testTopologyDescription(root, 3))
	if block := exactFinalizedBlock(splitState, left); block != nil {
		t.Fatalf("split child finalized block = %+v, want absent", block)
	}
}

func buildStateFixture(t testing.TB, options stateFixtureOptions) StateInput {
	t.Helper()

	configRoot := options.ConfigRoot
	if configRoot == nil {
		config := cell.NewDict(32)
		configValue := cell.BeginCell().MustStoreRef(cell.BeginCell().EndCell()).EndCell()
		if err := config.Set(cell.BeginCell().MustStoreUInt(0, 32).EndCell(), configValue); err != nil {
			t.Fatalf("store fixture config: %v", err)
		}
		configRoot = config.AsCell()
	}

	infoFlags := uint64(0)
	if options.BlockCreateStats != nil {
		infoFlags = 1
	}
	info := cell.BeginCell().
		MustStoreUInt(infoFlags, 16).
		MustStoreUInt(0, 32).
		MustStoreUInt(uint64(options.CatchainSeqno), 32).
		MustStoreBoolBit(options.RotatedAllShards).
		MustStoreBuilder(testEmptyOldMcBlocksInfo().ToBuilder()).
		MustStoreBoolBit(options.AfterKeyBlock)
	if options.LastKeyBlock == nil {
		info.MustStoreBoolBit(false)
	} else {
		lastKey, err := tlb.ToCell(options.LastKeyBlock)
		if err != nil {
			t.Fatalf("build last key block: %v", err)
		}
		info.MustStoreBoolBit(true).MustStoreBuilder(lastKey.ToBuilder())
	}
	if options.TrailingInfoBit {
		info.MustStoreBoolBit(true)
	}
	if options.BlockCreateStats != nil {
		info.MustStoreBuilder(options.BlockCreateStats.ToBuilder())
	}

	extra := cell.BeginCell().
		MustStoreUInt(0xcc26, 16).
		MustStoreDict(options.ShardHashes).
		MustStoreSlice(options.ConfigAddress[:], 256).
		MustStoreRef(configRoot).
		MustStoreRef(info.EndCell()).
		MustStoreCoins(0).
		MustStoreDict(nil).
		EndCell()

	accounts := options.Accounts
	var err error
	if accounts == nil {
		accounts, err = tlb.NewShardAccountsAugDict()
		if err != nil {
			t.Fatalf("create empty shard accounts: %v", err)
		}
	}
	accountsRoot, err := tlb.ToCell(accounts)
	if err != nil {
		t.Fatalf("build empty shard accounts: %v", err)
	}

	state := cell.BeginCell().
		MustStoreUInt(0x9023afe2, 32).
		MustStoreInt(0, 32).
		MustStoreUInt(0, 2).
		MustStoreUInt(0, 6).
		MustStoreInt(int64(masterchainWorkchain), 32).
		MustStoreUInt(0, 64).
		MustStoreUInt(uint64(options.Seqno), 32).
		MustStoreUInt(0, 32).
		MustStoreUInt(uint64(options.GenUTime), 32).
		MustStoreUInt(0, 64).
		MustStoreUInt(0, 32).
		MustStoreRef(cell.BeginCell().EndCell()).
		MustStoreBoolBit(false).
		MustStoreRef(accountsRoot).
		MustStoreRef(cell.BeginCell().EndCell()).
		MustStoreBoolBit(true).
		MustStoreRef(extra)
	if options.TrailingStateBit {
		state.MustStoreBoolBit(true)
	}
	root := state.EndCell()

	return StateInput{
		Block: ton.BlockIDExt{
			Workchain: masterchainWorkchain,
			Shard:     masterchainShard,
			SeqNo:     options.Seqno,
			RootHash:  testHash(0xe0),
			FileHash:  testHash(0xf0),
		},
		Root: root,
	}
}

func testShardHashes(t testing.TB, tree *cell.Cell) *cell.Dictionary {
	t.Helper()

	dict := cell.NewDict(32)
	key := cell.BeginCell().MustStoreInt(0, 32).EndCell()
	value := cell.BeginCell().MustStoreRef(tree).EndCell()
	if err := dict.Set(key, value); err != nil {
		t.Fatalf("store fixture shard hashes: %v", err)
	}
	return dict
}

func testBinTreeLeaf(description *cell.Cell) *cell.Cell {
	return cell.BeginCell().
		MustStoreUInt(0, 1).
		MustStoreBuilder(description.ToBuilder()).
		EndCell()
}

func testParsedShardDescription(
	t testing.TB,
	shard ShardID,
	seqno uint32,
	catchainSeqno uint32,
	fsm any,
) *cell.Cell {
	t.Helper()

	description := tlb.ShardDesc{
		SeqNo:              seqno,
		RootHash:           testHash(byte(seqno)),
		FileHash:           testHash(byte(seqno + 1)),
		NextCatchainSeqNo:  catchainSeqno,
		NextValidatorShard: shard.Shard,
		SplitMergeAt:       fsm,
	}
	root, err := tlb.ToCell(description)
	if err != nil {
		t.Fatalf("encode shard description: %v", err)
	}
	return root
}

func testEmptyOldMcBlocksInfo() *cell.Cell {
	return cell.BeginCell().
		MustStoreUInt(0, 1).
		MustStoreBoolBit(false).
		MustStoreUInt(0, 64).
		EndCell()
}

func testExtBlockRef(seqno uint32) *tlb.ExtBlkRef {
	return &tlb.ExtBlkRef{
		SeqNo:    seqno,
		RootHash: testHash(byte(seqno)),
		FileHash: testHash(byte(seqno + 1)),
	}
}

func testTopologyState(descriptions ...ShardDescription) *State {
	shardIndex := make(map[ShardID]int, len(descriptions))
	for i := range descriptions {
		shardIndex[descriptions[i].Shard] = i
	}

	return &State{
		Block: ton.BlockIDExt{
			Workchain: masterchainWorkchain,
			Shard:     masterchainShard,
			SeqNo:     100,
			RootHash:  testHash(0xa0),
			FileHash:  testHash(0xa1),
		},
		Shards:     descriptions,
		shardIndex: shardIndex,
	}
}

func testTopologyDescription(shard ShardID, marker byte) ShardDescription {
	return ShardDescription{
		Shard: shard,
		Block: ton.BlockIDExt{
			Workchain: shard.Workchain,
			Shard:     shard.Shard,
			SeqNo:     uint32(marker),
			RootHash:  testHash(marker),
			FileHash:  testHash(marker + 1),
		},
	}
}

func mustShardChildren(t *testing.T, parent ShardID) (ShardID, ShardID) {
	t.Helper()

	left, right, err := shardChildren(parent)
	if err != nil {
		t.Fatalf("shardChildren(%v): %v", parent, err)
	}
	return left, right
}

func testHash(marker byte) []byte {
	hash := make([]byte, 32)
	for i := range hash {
		hash[i] = marker + byte(i)
	}
	return hash
}

func sameBlockID(left, right ton.BlockIDExt) bool {
	return left.Workchain == right.Workchain &&
		left.Shard == right.Shard &&
		left.SeqNo == right.SeqNo &&
		bytes.Equal(left.RootHash, right.RootHash) &&
		bytes.Equal(left.FileHash, right.FileHash)
}
