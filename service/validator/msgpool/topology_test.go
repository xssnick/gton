package msgpool

import (
	"errors"
	"testing"

	"github.com/xssnick/gton/service/validator/groups"
)

func TestTopologyPublishesFutureDestinationsAndPrunesInactiveSources(t *testing.T) {
	pool := New(Config{})
	defer pool.Close()

	left := ShardIdent{Workchain: 0, Shard: 1 << 63}
	right := ShardIdent{Workchain: 0, Shard: 3 << 62}
	master := ShardIdent{Workchain: -1, Shard: ShardAll}
	current := &groups.Snapshot{
		Active: []groups.Session{{
			Shard: groups.ShardID{Workchain: left.Workchain, Shard: int64(left.Shard)},
			Registered: []groups.ShardDescription{{
				Shard: groups.ShardID{Workchain: left.Workchain, Shard: int64(left.Shard)},
			}},
		}},
		Future: []groups.Session{{
			Shard: groups.ShardID{Workchain: right.Workchain, Shard: int64(right.Shard)},
		}},
	}
	topology := NewTopology(current)
	if !topology.ContainsSource(left) || !topology.ContainsSource(master) || topology.ContainsSource(right) {
		t.Fatal("active, masterchain, and future source roles are incorrect")
	}
	if err := topology.Reconcile(pool.Internals()); err != nil {
		t.Fatal(err)
	}

	for i, source := range []ShardIdent{left, right, master} {
		if err := pool.Internals().Seed(left, source, SourceRef{
			Seqno:    1,
			RootHash: [32]byte{byte(i + 1)},
		}, nil, 0); err != nil {
			t.Fatal(err)
		}
	}

	next := &groups.Snapshot{Active: current.Active}
	nextTopology := NewTopology(next)
	if err := nextTopology.Reconcile(pool.Internals()); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Internals().SourceTop(left, right); !errors.Is(err, ErrNotFound) {
		t.Fatalf("inactive source survived topology change: %v", err)
	}
	for _, source := range []ShardIdent{left, master} {
		if _, err := pool.Internals().SourceTop(left, source); err != nil {
			t.Fatalf("live source %+v was pruned: %v", source, err)
		}
	}
	if err := pool.Internals().Seed(right, left, SourceRef{Seqno: 1}, nil, 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("retired future destination survived topology change: %v", err)
	}
}
