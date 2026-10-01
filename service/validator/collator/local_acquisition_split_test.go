package collator

import (
	"errors"
	"math"
	"testing"

	"github.com/xssnick/gton/service/validator/groups"
)

type splitWindowTestCase struct {
	name     string
	tmpNow   uint64
	blockNow uint64
	want     bool
}

func TestSplitWindowAllowsBeforeSplit(t *testing.T) {
	const (
		start = uint64(100)
		end   = uint64(120)
	)
	tests := []splitWindowTestCase{
		{name: "before start", tmpNow: start - 1, blockNow: start},
		{name: "thirteen second margin remains", tmpNow: end - 14, blockNow: end - 11, want: true},
		{name: "thirteen second margin reached", tmpNow: end - 13, blockNow: end - 11},
		{name: "twelve second margin remains", tmpNow: end - 12, blockNow: end - 11},
		{name: "block at end minus eleven", tmpNow: start, blockNow: end - 11, want: true},
		{name: "block at end minus ten", tmpNow: start, blockNow: end - 10},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := splitWindowAllowsBeforeSplit(test.tmpNow, test.blockNow, start, end)
			if got != test.want {
				t.Fatalf("splitWindowAllowsBeforeSplit() = %v, want %v", got, test.want)
			}
		})
	}
}

type deriveBeforeSplitTestCase struct {
	name       string
	masterTime uint32
	blockTime  uint32
	describe   func(groups.ShardDescription) groups.ShardDescription
	want       bool
}

func TestDeriveBeforeSplitUsesRegisteredMasterchainFSM(t *testing.T) {
	target := groups.ShardID{Workchain: 0, Shard: math.MinInt64}
	description := groups.ShardDescription{
		Shard: target,
		FSM: groups.ShardFSM{
			Kind:     groups.ShardFSMSplit,
			UTime:    100,
			Interval: 20,
		},
	}
	master := MasterchainContext{}
	tests := []deriveBeforeSplitTestCase{
		{
			name:       "slot time reaches window",
			masterTime: 99,
			blockTime:  100,
			want:       true,
		},
		{
			name:       "masterchain time reaches window",
			masterTime: 100,
			blockTime:  99,
			want:       true,
		},
		{
			name:       "both times precede window",
			masterTime: 99,
			blockTime:  99,
		},
		{
			name:       "merge announcement",
			masterTime: 100,
			blockTime:  100,
			describe: func(value groups.ShardDescription) groups.ShardDescription {
				value.FSM.Kind = groups.ShardFSMMerge
				return value
			},
		},
		{
			name:       "different registered shard",
			masterTime: 100,
			blockTime:  100,
			describe: func(value groups.ShardDescription) groups.ShardDescription {
				value.Shard.Shard = int64(uint64(1) << 62)
				return value
			},
		},
		{
			name:       "already before split",
			masterTime: 100,
			blockTime:  100,
			describe: func(value groups.ShardDescription) groups.ShardDescription {
				value.BeforeSplit = true
				return value
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registered := description
			if test.describe != nil {
				registered = test.describe(registered)
			}
			master.GenUtime = test.masterTime

			got, err := deriveBeforeSplit(
				master,
				HeaderParams{GenUtime: test.blockTime},
				target,
				[]groups.ShardDescription{registered},
			)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("deriveBeforeSplit() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestDeriveBeforeSplitTrustsInheritedFSMWithoutCurrentWorkchainPolicy(t *testing.T) {
	target := groups.ShardID{Workchain: 0, Shard: math.MinInt64}
	master := MasterchainContext{
		GenUtime: 100,
	}
	description := groups.ShardDescription{
		Shard: target,
		FSM: groups.ShardFSM{
			Kind:     groups.ShardFSMSplit,
			UTime:    100,
			Interval: 20,
		},
	}

	got, err := deriveBeforeSplit(
		master,
		HeaderParams{GenUtime: 100},
		target,
		[]groups.ShardDescription{description},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !got {
		t.Fatal("deriveBeforeSplit() = false, want true")
	}
}

func TestDeriveBeforeSplitDoesNotSplitMaximumCanonicalDepth(t *testing.T) {
	target := groups.ShardID{Workchain: 0, Shard: 8}
	description := groups.ShardDescription{
		Shard: target,
		FSM: groups.ShardFSM{
			Kind:     groups.ShardFSMSplit,
			UTime:    100,
			Interval: 20,
		},
	}

	got, err := deriveBeforeSplit(
		MasterchainContext{GenUtime: 100},
		HeaderParams{GenUtime: 100},
		target,
		[]groups.ShardDescription{description},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got {
		t.Fatal("deriveBeforeSplit() = true at maximum shard depth")
	}
}

func TestDeriveBeforeSplitRejectsInvalidShardEncoding(t *testing.T) {
	target := groups.ShardID{Workchain: 0}
	description := groups.ShardDescription{
		Shard: target,
		FSM: groups.ShardFSM{
			Kind:     groups.ShardFSMSplit,
			UTime:    100,
			Interval: 20,
		},
	}

	_, err := deriveBeforeSplit(
		MasterchainContext{GenUtime: 100},
		HeaderParams{GenUtime: 100},
		target,
		[]groups.ShardDescription{description},
	)
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("error = %v, want ErrInvalidInput", err)
	}
}
