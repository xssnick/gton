package msgpool

import (
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/groups"
)

// TestFeedReconcileOfAnUnchangedTopologyDoesNotWaitForFeeds pins the read-locked
// comparison in Reconcile. A feed in flight holds the topology read lock for its
// whole delta or reseed; a masterchain apply that republishes the same
// projection must not queue for the exclusive lock behind it, because a waiting
// writer stalls every new feed of every other source. A changed projection is
// still published.
func TestFeedReconcileOfAnUnchangedTopologyDoesNotWaitForFeeds(t *testing.T) {
	feed := newTestFeed(t, nil)
	shard := groups.ShardID{Workchain: 0, Shard: -1 << 63}
	snapshot := &groups.Snapshot{Active: []groups.Session{{
		Shard:      shard,
		Registered: []groups.ShardDescription{{Shard: shard}},
	}}}
	if err := feed.Reconcile(NewTopology(snapshot)); err != nil {
		t.Fatal(err)
	}

	feed.topologyMu.RLock()
	done := make(chan error, 1)
	go func() {
		done <- feed.Reconcile(NewTopology(snapshot))
	}()
	select {
	case err := <-done:
		feed.topologyMu.RUnlock()
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		feed.topologyMu.RUnlock()
		<-done
		t.Fatal("reconcile of an unchanged topology waited for a feed in flight")
	}

	changed := NewTopology(&groups.Snapshot{})
	if err := feed.Reconcile(changed); err != nil {
		t.Fatal(err)
	}
	if !feed.topology.Equal(changed) {
		t.Fatal("a changed topology was not published")
	}
}
