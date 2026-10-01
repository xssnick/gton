package collator

import (
	"testing"

	sharddomain "github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
)

// The split seed walk must return what the sequential walk returns: the same
// messages per destination, in the same order, and the same queue total. The
// queue is made deep enough that the prefix split actually fires — a queue
// that yields one prefix takes the sequential walk and would compare it with
// itself — and it carries entries bound for both chains, so the routing is
// exercised across the split.
func TestSeedWalkSplitMatchesTheSequentialWalk(t *testing.T) {
	for _, depth := range []int{1024, 8192} {
		fixture := newMasterBuildFixture(t, false)
		source := blockShardIdent(fixture.request.Previous.ID)
		previous := fixture.request.Previous
		previous.State = previousStateWithDenseOutQueue(t, previous.State, depth)
		view, err := localViewFromPrevious(previous, true, true)
		if err != nil {
			t.Fatal(err)
		}
		ref, err := localSourceRef(previous.ID)
		if err != nil {
			t.Fatal(err)
		}
		master := targetShardIdent(groups.ShardID{Workchain: masterchainWorkchainID, Shard: sharddomain.Root})
		pool := msgpool.New(msgpool.Config{})
		defer pool.Close()
		if err = pool.Internals().ReconcileDestinations([]msgpool.ShardIdent{master, source}); err != nil {
			t.Fatal(err)
		}

		queueInfo, err := msgpool.StateOutMsgQueueInfo(view.previous.State)
		if err != nil {
			t.Fatal(err)
		}
		prefixes, err := queueInfo.OutQueue.KeyPrefixes(32+6, 512)
		if err != nil {
			t.Fatal(err)
		}
		if len(prefixes) < 8 {
			t.Fatalf("depth %d: %d key prefixes; the split would not engage", depth, len(prefixes))
		}

		split, splitTotal, err := pool.Internals().SeedsFromStateRoot(source, ref, view.previous.State)
		if err != nil {
			t.Fatal(err)
		}
		seq, seqTotal, err := msgpool.SeedsFromStateRootSequential(pool.Internals(), source, ref, view.previous.State)
		if err != nil {
			t.Fatal(err)
		}
		if splitTotal != seqTotal {
			t.Fatalf("depth %d: split walk counted %d entries, sequential %d", depth, splitTotal, seqTotal)
		}
		if len(split) != len(seq) {
			t.Fatalf("depth %d: %d destinations against %d", depth, len(split), len(seq))
		}
		for i := range seq {
			if split[i].Destination != seq[i].Destination {
				t.Fatalf("depth %d: destination %d differs", depth, i)
			}
			if len(split[i].Messages) != len(seq[i].Messages) {
				t.Fatalf("depth %d: destination %v has %d messages split, %d sequential",
					depth, seq[i].Destination, len(split[i].Messages), len(seq[i].Messages))
			}
			for j := range seq[i].Messages {
				if split[i].Messages[j].Key != seq[i].Messages[j].Key ||
					split[i].Messages[j].EnqueuedLT != seq[i].Messages[j].EnqueuedLT {
					t.Fatalf("depth %d: destination %v message %d differs", depth, seq[i].Destination, j)
				}
			}
		}
		t.Logf("depth %d: %d prefixes, %d entries, %d destinations — identical", depth, len(prefixes), seqTotal, len(seq))
	}
}
