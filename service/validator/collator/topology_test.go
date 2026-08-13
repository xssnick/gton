package collator

import (
	"errors"
	"math"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"

	"github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/validator/groups"
)

// resolveShardTopology is the state-then-session composition the collator
// performs in two separate steps: preparePredecessor resolves the state and
// prepare binds the session after the header exists, and verifyCandidate runs
// the two back to back. The tests below exercise the pair as a unit, so the
// composition lives here rather than as a third production spelling nothing
// calls.
func resolveShardTopology(
	req ShardRequest,
	first *tlb.ShardStateUnsplit,
	second *tlb.ShardStateUnsplit,
) (shardTopology, error) {
	topology, err := resolveShardTopologyState(req, first, second)
	if err != nil {
		return shardTopology{}, err
	}
	if err = bindShardTopologySession(req, &topology); err != nil {
		return shardTopology{}, err
	}
	return topology, nil
}

func TestResolveShardTopology(t *testing.T) {
	tests := []struct {
		name                string
		kind                topologyKind
		wantSeqno           uint32
		wantPreviousGenLT   uint64
		wantPreviousUtime   uint32
		wantPreviousVertSeq uint32
	}{
		{
			name:                "linear",
			kind:                topologyLinear,
			wantSeqno:           8,
			wantPreviousGenLT:   1_000,
			wantPreviousUtime:   11,
			wantPreviousVertSeq: 5,
		},
		{
			name:                "after split",
			kind:                topologyAfterSplit,
			wantSeqno:           8,
			wantPreviousGenLT:   1_000,
			wantPreviousUtime:   11,
			wantPreviousVertSeq: 5,
		},
		{
			name:                "after merge uses predecessor maxima",
			kind:                topologyAfterMerge,
			wantSeqno:           10,
			wantPreviousGenLT:   1_100,
			wantPreviousUtime:   11,
			wantPreviousVertSeq: 7,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newTopologyFixture(t, test.kind)

			got, err := resolveShardTopology(fixture.req, fixture.first, fixture.second)
			if err != nil {
				t.Fatalf("resolveShardTopology: %v", err)
			}

			wantTarget := mustTopologyIdent(t, fixture.req.Shard)
			if got.kind != test.kind {
				t.Fatalf("kind = %d, want %d", got.kind, test.kind)
			}
			if got.target != wantTarget {
				t.Fatalf("target = %+v, want %+v", got.target, wantTarget)
			}
			if got.session != &fixture.req.Masterchain.Groups.Active[0] {
				t.Fatal("session does not point into the immutable active snapshot")
			}
			if got.seqno != test.wantSeqno {
				t.Fatalf("seqno = %d, want %d", got.seqno, test.wantSeqno)
			}
			if got.previousGenLT != test.wantPreviousGenLT {
				t.Fatalf("previous gen lt = %d, want %d", got.previousGenLT, test.wantPreviousGenLT)
			}
			if got.previousGenUtime != test.wantPreviousUtime {
				t.Fatalf("previous gen utime = %d, want %d", got.previousGenUtime, test.wantPreviousUtime)
			}
			if got.previousVertSeqno != test.wantPreviousVertSeq {
				t.Fatalf("previous vertical seqno = %d, want %d", got.previousVertSeqno, test.wantPreviousVertSeq)
			}
		})
	}
}

func TestExpectedShardNeighborsMatchesDeepHypercubeTopology(t *testing.T) {
	target := groups.ShardID{Workchain: 0, Shard: int64(0x1800000000000000)}
	neighbor := groups.ShardID{Workchain: 0, Shard: -0x2000000000000000}
	targetBlock := testBlockID(target.Workchain, target.Shard, 17, 0x31)
	neighborBlock := testBlockID(neighbor.Workchain, neighbor.Shard, 29, 0x32)

	expected, err := expectedShardNeighbors(MasterchainContext{
		Groups: &groups.Snapshot{Active: []groups.Session{
			{Registered: []groups.ShardDescription{{Shard: target, Block: targetBlock}}},
			{Registered: []groups.ShardDescription{{Shard: neighbor, Block: neighborBlock}}},
		}},
	}, target)
	if err != nil {
		t.Fatalf("expected shard neighbors: %v", err)
	}

	key := neighborShardKey{workchain: neighbor.Workchain, shard: neighbor.Shard}
	if block, exists := expected[key]; !exists || !block.Equals(&neighborBlock) {
		t.Fatalf("deep hypercube neighbor = %+v, exists = %t, want %v", block, exists, neighborBlock)
	}
}

func TestExpectedShardNeighborsIncludesBothMergePredecessors(t *testing.T) {
	target := groups.ShardID{Workchain: 0, Shard: 0x1000000000000000}
	left, err := shard.Child(target.Shard, true)
	if err != nil {
		t.Fatal(err)
	}
	right, err := shard.Child(target.Shard, false)
	if err != nil {
		t.Fatal(err)
	}
	leftBlock := testBlockID(0, left, 88_038, 0x41)
	rightBlock := testBlockID(0, right, 88_120, 0x42)

	expected, err := expectedShardNeighbors(MasterchainContext{
		Groups: &groups.Snapshot{Active: []groups.Session{
			{Registered: []groups.ShardDescription{{
				Shard: groups.ShardID{Workchain: 0, Shard: left},
				Block: leftBlock,
			}}},
			{Registered: []groups.ShardDescription{{
				Shard: groups.ShardID{Workchain: 0, Shard: right},
				Block: rightBlock,
			}}},
		}},
	}, target)
	if err != nil {
		t.Fatalf("expected shard neighbors: %v", err)
	}

	for _, block := range []ton.BlockIDExt{leftBlock, rightBlock} {
		key := neighborShardKey{workchain: block.Workchain, shard: block.Shard}
		listed, exists := expected[key]
		if !exists || !listed.Equals(&block) {
			t.Fatalf("merge predecessor %016x = %+v, exists = %t, want %v",
				uint64(block.Shard), listed, exists, block)
		}
	}
}

func TestResolveShardTopologyChecksRegisteredLinearChain(t *testing.T) {
	tests := []struct {
		name      string
		delta     uint32
		mutate    func(*topologyFixture)
		wantError bool
	}{
		{
			name: "exact registered predecessor",
		},
		{
			name: "equal height fork",
			mutate: func(fixture *topologyFixture) {
				registered := &fixture.req.Masterchain.Groups.Active[0].Registered[0]
				registered.Block = topologyCopyBlock(registered.Block)
				registered.Block.RootHash[0]++
			},
			wantError: true,
		},
		{
			name: "newer registered block",
			mutate: func(fixture *topologyFixture) {
				registered := &fixture.req.Masterchain.Groups.Active[0].Registered[0]
				registered.Block = topologyTestBlock(registered.Shard, fixture.req.Previous.ID.SeqNo+1, 0x71)
			},
			wantError: true,
		},
		{
			name:  "seven unregistered blocks",
			delta: 7,
		},
		{
			name:      "eight unregistered blocks",
			delta:     8,
			wantError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newTopologyFixture(t, topologyLinear)
			if test.delta != 0 {
				listed := fixture.req.Masterchain.Groups.Active[0].Registered[0].Block
				fixture.req.Previous.ID = topologyTestBlock(fixture.req.Shard, listed.SeqNo+test.delta, 0x72)
				fixture.first = topologyTestState(t, fixture.req.Previous.ID, false, 1_000, 11, 5)
			}
			if test.mutate != nil {
				test.mutate(fixture)
			}

			_, err := resolveShardTopology(fixture.req, fixture.first, fixture.second)
			if test.wantError {
				if !errors.Is(err, ErrInvalidInput) {
					t.Fatalf("error = %v, want ErrInvalidInput", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveShardTopology: %v", err)
			}
		})
	}
}

func TestResolveShardTopologyChecksUnregisteredTransitionChains(t *testing.T) {
	tests := []struct {
		name       string
		registered topologyKind
		delta      uint32
		wantError  bool
	}{
		{name: "split first unregistered successor", registered: topologyAfterSplit, delta: 1},
		{name: "split seventh unregistered successor", registered: topologyAfterSplit, delta: 7},
		{name: "split eighth unregistered successor", registered: topologyAfterSplit, delta: 8, wantError: true},
		{name: "merge first unregistered successor", registered: topologyAfterMerge, delta: 1},
		{name: "merge seventh unregistered successor", registered: topologyAfterMerge, delta: 7},
		{name: "merge eighth unregistered successor", registered: topologyAfterMerge, delta: 8, wantError: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newTopologyFixture(t, test.registered)
			registered := fixture.req.Masterchain.Groups.Active[0].Registered
			listedSeqno := registered[0].Block.SeqNo
			if len(registered) == 2 {
				listedSeqno = max(listedSeqno, registered[1].Block.SeqNo)
			}

			fixture.req.Previous.ID = topologyTestBlock(fixture.req.Shard, listedSeqno+test.delta, 0x73)
			fixture.req.Previous2 = nil
			fixture.first = topologyTestState(t, fixture.req.Previous.ID, false, 1_100, 12, 6)
			fixture.second = nil

			got, err := resolveShardTopology(fixture.req, fixture.first, fixture.second)
			if test.wantError {
				if !errors.Is(err, ErrInvalidInput) {
					t.Fatalf("error = %v, want ErrInvalidInput", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveShardTopology: %v", err)
			}
			if got.kind != topologyLinear {
				t.Fatalf("topology kind = %d, want linear", got.kind)
			}
		})
	}
}

func TestResolveShardTopologyRequiresExactImmediateTransitionPredecessors(t *testing.T) {
	tests := []struct {
		name string
		kind topologyKind
	}{
		{name: "split", kind: topologyAfterSplit},
		{name: "merge", kind: topologyAfterMerge},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newTopologyFixture(t, test.kind)
			registered := fixture.req.Masterchain.Groups.Active[0].Registered
			index := len(registered) - 1
			registered[index].Block = topologyCopyBlock(registered[index].Block)
			registered[index].Block.FileHash[0]++

			_, err := resolveShardTopology(fixture.req, fixture.first, fixture.second)
			if !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("error = %v, want ErrInvalidInput", err)
			}
		})
	}
}

func TestResolveShardTopologyChecksBeforeSplitWindow(t *testing.T) {
	tests := []struct {
		name        string
		genUtime    uint32
		beforeSplit bool
		fsm         groups.ShardFSM
		wantError   bool
	}{
		{
			name:        "window start",
			genUtime:    100,
			beforeSplit: true,
			fsm:         groups.ShardFSM{Kind: groups.ShardFSMSplit, UTime: 100, Interval: 10},
		},
		{
			name:        "last second in window",
			genUtime:    109,
			beforeSplit: true,
			fsm:         groups.ShardFSM{Kind: groups.ShardFSMSplit, UTime: 100, Interval: 10},
		},
		{
			name:        "before window",
			genUtime:    99,
			beforeSplit: true,
			fsm:         groups.ShardFSM{Kind: groups.ShardFSMSplit, UTime: 100, Interval: 10},
			wantError:   true,
		},
		{
			name:        "window end",
			genUtime:    110,
			beforeSplit: true,
			fsm:         groups.ShardFSM{Kind: groups.ShardFSMSplit, UTime: 100, Interval: 10},
			wantError:   true,
		},
		{
			name:        "merge announcement",
			genUtime:    105,
			beforeSplit: true,
			fsm:         groups.ShardFSM{Kind: groups.ShardFSMMerge, UTime: 100, Interval: 10},
			wantError:   true,
		},
		{
			name:     "flag remains optional inside window",
			genUtime: 105,
			fsm:      groups.ShardFSM{Kind: groups.ShardFSMSplit, UTime: 100, Interval: 10},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newTopologyFixture(t, topologyLinear)
			fixture.req.Header.GenUtime = test.genUtime
			fixture.req.BeforeSplit = test.beforeSplit
			fixture.req.Masterchain.Groups.Active[0].Registered[0].FSM = test.fsm

			_, err := resolveShardTopology(fixture.req, fixture.first, fixture.second)
			if test.wantError {
				if !errors.Is(err, ErrInvalidInput) {
					t.Fatalf("error = %v, want ErrInvalidInput", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveShardTopology: %v", err)
			}
		})
	}
}

func TestResolveShardTopologyRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name   string
		kind   topologyKind
		mutate func(*testing.T, *topologyFixture)
	}{
		{
			name: "zero target shard",
			kind: topologyLinear,
			mutate: func(_ *testing.T, fixture *topologyFixture) {
				fixture.req.Shard.Shard = 0
			},
		},
		{
			name: "target shard deeper than protocol limit",
			kind: topologyLinear,
			mutate: func(t *testing.T, fixture *topologyFixture) {
				id := shard.Root
				for range 61 {
					id = mustTopologyChild(t, id, true)
				}
				fixture.req.Shard.Shard = id
			},
		},
		{
			name: "masterchain target",
			kind: topologyLinear,
			mutate: func(_ *testing.T, fixture *topologyFixture) {
				fixture.req.Shard = groups.ShardID{Workchain: -1, Shard: shard.Root}
			},
		},
		{
			name: "absent first state",
			kind: topologyLinear,
			mutate: func(_ *testing.T, fixture *topologyFixture) {
				fixture.first = nil
			},
		},
		{
			name: "second state without predecessor",
			kind: topologyAfterMerge,
			mutate: func(_ *testing.T, fixture *topologyFixture) {
				fixture.req.Previous2 = nil
			},
		},
		{
			name: "second predecessor without state",
			kind: topologyAfterMerge,
			mutate: func(_ *testing.T, fixture *topologyFixture) {
				fixture.second = nil
			},
		},
		{
			name: "first state sequence differs from block",
			kind: topologyLinear,
			mutate: func(_ *testing.T, fixture *topologyFixture) {
				fixture.first.Seqno++
			},
		},
		{
			name: "second state sequence differs from block",
			kind: topologyAfterMerge,
			mutate: func(_ *testing.T, fixture *topologyFixture) {
				fixture.second.Seqno++
			},
		},
		{
			name: "non canonical predecessor state ident",
			kind: topologyLinear,
			mutate: func(_ *testing.T, fixture *topologyFixture) {
				fixture.first.ShardIdent.ShardPrefix = uint64(fixture.req.Previous.ID.Shard)
			},
		},
		{
			name: "linear predecessor before split",
			kind: topologyLinear,
			mutate: func(_ *testing.T, fixture *topologyFixture) {
				fixture.first.BeforeSplit = true
			},
		},
		{
			name: "split predecessor is not direct parent",
			kind: topologyAfterSplit,
			mutate: func(t *testing.T, fixture *topologyFixture) {
				fixture.req.Shard.Shard = mustTopologyChild(t, fixture.req.Shard.Shard, true)
				fixture.req.Masterchain.Groups.Active[0].Shard = fixture.req.Shard
			},
		},
		{
			name: "split predecessor lacks before split marker",
			kind: topologyAfterSplit,
			mutate: func(_ *testing.T, fixture *topologyFixture) {
				fixture.first.BeforeSplit = false
			},
		},
		{
			name: "split genesis has extra block",
			kind: topologyAfterSplit,
			mutate: func(_ *testing.T, fixture *topologyFixture) {
				fixture.req.Masterchain.Groups.Active[0].Genesis = append(
					fixture.req.Masterchain.Groups.Active[0].Genesis,
					topologyTestBlock(fixture.req.Shard, 99, 0x70),
				)
			},
		},
		{
			name: "split genesis hash differs",
			kind: topologyAfterSplit,
			mutate: func(_ *testing.T, fixture *topologyFixture) {
				fixture.req.Masterchain.Groups.Active[0].Genesis[0].RootHash[0]++
			},
		},
		{
			name: "merge predecessors reversed",
			kind: topologyAfterMerge,
			mutate: func(_ *testing.T, fixture *topologyFixture) {
				firstPrevious := fixture.req.Previous
				fixture.req.Previous = *fixture.req.Previous2
				*fixture.req.Previous2 = firstPrevious
				fixture.first, fixture.second = fixture.second, fixture.first
			},
		},
		{
			name: "merge predecessor is not direct child",
			kind: topologyAfterMerge,
			mutate: func(t *testing.T, fixture *topologyFixture) {
				leftLeft := mustTopologyChild(t, fixture.req.Previous.ID.Shard, true)
				fixture.req.Previous2.ID = topologyTestBlock(
					groups.ShardID{Workchain: fixture.req.Shard.Workchain, Shard: leftLeft},
					fixture.req.Previous2.ID.SeqNo,
					0x52,
				)
				fixture.second.ShardIdent = mustTopologyIdent(t, groups.ShardID{
					Workchain: fixture.req.Shard.Workchain,
					Shard:     leftLeft,
				})
			},
		},
		{
			name: "merge predecessor before split",
			kind: topologyAfterMerge,
			mutate: func(_ *testing.T, fixture *topologyFixture) {
				fixture.second.BeforeSplit = true
			},
		},
		{
			name: "merge global ids differ",
			kind: topologyAfterMerge,
			mutate: func(_ *testing.T, fixture *topologyFixture) {
				fixture.second.GlobalID++
			},
		},
		{
			name: "merge genesis order differs",
			kind: topologyAfterMerge,
			mutate: func(_ *testing.T, fixture *topologyFixture) {
				genesis := fixture.req.Masterchain.Groups.Active[0].Genesis
				genesis[0], genesis[1] = genesis[1], genesis[0]
			},
		},
		{
			name: "block sequence overflow",
			kind: topologyLinear,
			mutate: func(_ *testing.T, fixture *topologyFixture) {
				fixture.req.Previous.ID.SeqNo = math.MaxUint32
				fixture.first.Seqno = math.MaxUint32
			},
		},
		{
			name: "snapshot not ready",
			kind: topologyLinear,
			mutate: func(_ *testing.T, fixture *topologyFixture) {
				fixture.req.Masterchain.Groups.Ready = false
			},
		},
		{
			name: "snapshot belongs to another masterchain block",
			kind: topologyLinear,
			mutate: func(_ *testing.T, fixture *topologyFixture) {
				fixture.req.Masterchain.Groups.MasterchainBlock.SeqNo++
			},
		},
		{
			name: "active target session absent",
			kind: topologyLinear,
			mutate: func(_ *testing.T, fixture *topologyFixture) {
				fixture.req.Masterchain.Groups.Active = nil
			},
		},
		{
			name: "active target session duplicated",
			kind: topologyLinear,
			mutate: func(_ *testing.T, fixture *topologyFixture) {
				active := fixture.req.Masterchain.Groups.Active
				fixture.req.Masterchain.Groups.Active = append(active, active[0])
			},
		},
		{
			name: "registered shard context absent",
			kind: topologyLinear,
			mutate: func(_ *testing.T, fixture *topologyFixture) {
				fixture.req.Masterchain.Groups.Active[0].Registered = nil
			},
		},
		{
			name: "registered split parent lacks marker",
			kind: topologyAfterSplit,
			mutate: func(_ *testing.T, fixture *topologyFixture) {
				fixture.req.Masterchain.Groups.Active[0].Registered[0].BeforeSplit = false
			},
		},
		{
			name: "registered merge child lacks marker",
			kind: topologyAfterMerge,
			mutate: func(_ *testing.T, fixture *topologyFixture) {
				fixture.req.Masterchain.Groups.Active[0].Registered[1].BeforeMerge = false
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newTopologyFixture(t, test.kind)
			test.mutate(t, fixture)

			_, err := resolveShardTopology(fixture.req, fixture.first, fixture.second)
			if !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("error = %v, want ErrInvalidInput", err)
			}
		})
	}
}

type topologyFixture struct {
	req    ShardRequest
	first  *tlb.ShardStateUnsplit
	second *tlb.ShardStateUnsplit
}

func newTopologyFixture(t *testing.T, kind topologyKind) *topologyFixture {
	t.Helper()

	root := groups.ShardID{Workchain: 0, Shard: shard.Root}
	left := groups.ShardID{Workchain: 0, Shard: mustTopologyChild(t, root.Shard, true)}
	right := groups.ShardID{Workchain: 0, Shard: mustTopologyChild(t, root.Shard, false)}
	masterchain := topologyTestBlock(groups.ShardID{Workchain: -1, Shard: shard.Root}, 50, 0xa0)

	fixture := &topologyFixture{}
	switch kind {
	case topologyLinear:
		fixture.req.Shard = root
		fixture.req.Previous.ID = topologyTestBlock(root, 7, 0x10)
		fixture.first = topologyTestState(t, fixture.req.Previous.ID, false, 1_000, 11, 5)
	case topologyAfterSplit:
		fixture.req.Shard = left
		fixture.req.Previous.ID = topologyTestBlock(root, 7, 0x20)
		fixture.first = topologyTestState(t, fixture.req.Previous.ID, true, 1_000, 11, 5)
	case topologyAfterMerge:
		fixture.req.Shard = root
		fixture.req.Previous.ID = topologyTestBlock(left, 7, 0x30)
		second := PreviousBlock{ID: topologyTestBlock(right, 9, 0x40)}
		fixture.req.Previous2 = &second
		fixture.first = topologyTestState(t, fixture.req.Previous.ID, false, 1_000, 11, 5)
		fixture.second = topologyTestState(t, fixture.req.Previous2.ID, false, 1_100, 9, 7)
	default:
		t.Fatalf("unsupported topology fixture kind %d", kind)
	}

	genesis := []ton.BlockIDExt{topologyTestBlock(fixture.req.Shard, 1, 0x60)}
	registered := []groups.ShardDescription{{
		Shard: fixture.req.Shard,
		Block: topologyCopyBlock(fixture.req.Previous.ID),
	}}
	if kind == topologyAfterSplit {
		genesis = []ton.BlockIDExt{topologyCopyBlock(fixture.req.Previous.ID)}
		registered = []groups.ShardDescription{{
			Shard:       root,
			Block:       topologyCopyBlock(fixture.req.Previous.ID),
			BeforeSplit: true,
		}}
	}
	if kind == topologyAfterMerge {
		genesis = []ton.BlockIDExt{
			topologyCopyBlock(fixture.req.Previous.ID),
			topologyCopyBlock(fixture.req.Previous2.ID),
		}
		registered = []groups.ShardDescription{
			{
				Shard:       left,
				Block:       topologyCopyBlock(fixture.req.Previous.ID),
				BeforeMerge: true,
			},
			{
				Shard:       right,
				Block:       topologyCopyBlock(fixture.req.Previous2.ID),
				BeforeMerge: true,
			},
		}
	}

	fixture.req.Masterchain = MasterchainContext{
		ID: masterchain,
		Groups: &groups.Snapshot{
			MasterchainBlock: topologyCopyBlock(masterchain),
			Ready:            true,
			Active: []groups.Session{{
				Shard:      fixture.req.Shard,
				Genesis:    genesis,
				Registered: registered,
			}},
		},
	}

	return fixture
}

func topologyTestState(
	t *testing.T,
	block ton.BlockIDExt,
	beforeSplit bool,
	genLT uint64,
	genUtime uint32,
	vertSeqno uint32,
) *tlb.ShardStateUnsplit {
	t.Helper()

	return &tlb.ShardStateUnsplit{
		GlobalID: 42,
		ShardIdent: mustTopologyIdent(t, groups.ShardID{
			Workchain: block.Workchain,
			Shard:     block.Shard,
		}),
		Seqno:       block.SeqNo,
		VertSeqno:   vertSeqno,
		GenUTime:    genUtime,
		GenLT:       genLT,
		BeforeSplit: beforeSplit,
	}
}

// topologyTestBlock only adapts the shard identifier to testBlockID: no
// topology test depends on the shape of the hash bytes, just on distinctness.
func topologyTestBlock(shardID groups.ShardID, seqno uint32, marker byte) ton.BlockIDExt {
	return testBlockID(shardID.Workchain, shardID.Shard, seqno, marker)
}

func topologyCopyBlock(block ton.BlockIDExt) ton.BlockIDExt {
	return *block.Copy()
}

func mustTopologyIdent(t *testing.T, id groups.ShardID) tlb.ShardIdent {
	t.Helper()

	ident, err := topologyShardIdent(id)
	if err != nil {
		t.Fatalf("topologyShardIdent(%+v): %v", id, err)
	}
	return ident
}

func mustTopologyChild(t *testing.T, parent int64, left bool) int64 {
	t.Helper()

	child, err := shard.Child(parent, left)
	if err != nil {
		t.Fatalf("shard.Child(%016x, %t): %v", uint64(parent), left, err)
	}
	return child
}
