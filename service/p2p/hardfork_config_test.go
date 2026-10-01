package p2p

import (
	"bytes"
	"errors"
	"math"
	"testing"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/ton"
)

func TestHardforkAfter(t *testing.T) {
	t.Parallel()

	immediate := testBlockID(-1, topShard, 101)
	replacement := immediate
	replacement.FileHash = bytes.Repeat([]byte{0xfe}, 32)
	maximum := testBlockID(-1, topShard, math.MaxUint32)

	type testCase struct {
		name   string
		prev   ton.BlockIDExt
		forks  []ton.BlockIDExt
		want   ton.BlockIDExt
		absent bool
	}
	tests := []testCase{
		{
			name:   "none",
			prev:   testBlockID(-1, topShard, 100),
			absent: true,
		},
		{
			name:   "historical",
			prev:   testBlockID(-1, topShard, 100),
			forks:  []ton.BlockIDExt{testBlockID(-1, topShard, 10), testBlockID(-1, topShard, 100)},
			absent: true,
		},
		{
			name:   "future gap",
			prev:   testBlockID(-1, topShard, 100),
			forks:  []ton.BlockIDExt{testBlockID(-1, topShard, 102)},
			absent: true,
		},
		{
			name:  "immediate",
			prev:  testBlockID(-1, topShard, 100),
			forks: []ton.BlockIDExt{immediate},
			want:  immediate,
		},
		{
			name:  "immediate between historical and future",
			prev:  testBlockID(-1, topShard, 100),
			forks: []ton.BlockIDExt{testBlockID(-1, topShard, 10), immediate, testBlockID(-1, topShard, 120)},
			want:  immediate,
		},
		{
			name:   "later rollback invalidates immediate",
			prev:   testBlockID(-1, topShard, 100),
			forks:  []ton.BlockIDExt{immediate, testBlockID(-1, topShard, 90)},
			absent: true,
		},
		{
			name:  "replacement uses full configured identity",
			prev:  testBlockID(-1, topShard, 100),
			forks: []ton.BlockIDExt{immediate, replacement},
			want:  replacement,
		},
		{
			name:   "shard predecessor",
			prev:   testBlockID(0, topShard, 100),
			forks:  []ton.BlockIDExt{immediate},
			absent: true,
		},
		{
			name:   "non-full masterchain shard",
			prev:   testBlockID(-1, 0x4000000000000000, 100),
			forks:  []ton.BlockIDExt{immediate},
			absent: true,
		},
		{
			name:   "maximum predecessor does not overflow",
			prev:   maximum,
			forks:  []ton.BlockIDExt{testBlockID(-1, topShard, 0), maximum},
			absent: true,
		},
		{
			name:  "maximum immediate successor",
			prev:  testBlockID(-1, topShard, math.MaxUint32-1),
			forks: []ton.BlockIDExt{maximum},
			want:  maximum,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			node := testNodeWithHardforkConfig(t, tt.forks)
			got, err := node.HardforkAfter(tt.prev)
			if tt.absent {
				if !errors.Is(err, storage.ErrNotFound) {
					t.Fatalf("HardforkAfter error = %v, want ErrNotFound", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("HardforkAfter: %v", err)
			}
			if !got.Equals(&tt.want) {
				t.Fatalf("HardforkAfter = %+v, want %+v", got, tt.want)
			}
			if !node.IsHardfork(got) {
				t.Fatal("selected block is not an active configured hardfork")
			}
		})
	}
}

func testNodeWithHardforkConfig(tb testing.TB, forks []ton.BlockIDExt) *Node {
	tb.Helper()

	config := make([]liteclient.ConfigBlock, len(forks))
	for i, fork := range forks {
		config[i] = configBlockFromID(fork)
	}
	hardforks, set, err := hardforksFromConfig(config)
	if err != nil {
		tb.Fatalf("parse hardfork config: %v", err)
	}
	return &Node{hardforks: hardforks, hardforkSet: set}
}

func TestHardforkAfterDoesNotUseInitBlock(t *testing.T) {
	t.Parallel()

	node := &Node{initBlock: testBlockID(-1, topShard, 101)}
	_, err := node.HardforkAfter(testBlockID(-1, topShard, 100))
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("HardforkAfter error = %v, want ErrNotFound", err)
	}
}

func BenchmarkHardforkAfter(b *testing.B) {
	prev := testBlockID(-1, topShard, 100)

	type benchmarkCase struct {
		name  string
		forks []ton.BlockIDExt
	}
	cases := []benchmarkCase{
		{name: "none"},
		{
			name:  "historical",
			forks: []ton.BlockIDExt{testBlockID(-1, topShard, 10), testBlockID(-1, topShard, 100)},
		},
		{name: "immediate", forks: []ton.BlockIDExt{testBlockID(-1, topShard, 101)}},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			node := testNodeWithHardforkConfig(b, tc.forks)
			b.ReportAllocs()
			for b.Loop() {
				node.HardforkAfter(prev)
			}
		})
	}
}
