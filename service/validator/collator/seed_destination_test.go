package collator

import (
	"bytes"
	"sort"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	sharddomain "github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
)

// Paired A/B of the two queue walks on one input. The host is contended, so the
// arms alternate inside one process and the minima are compared.
func TestSeedsForDestinationMatchesTheFullWalk(t *testing.T) {
	for _, depth := range []int{1024, 8192} {
		fixture := newMasterBuildFixture(t, false)
		source := blockShardIdent(fixture.request.Previous.ID)
		state := previousStateWithDenseOutQueue(t, fixture.request.Previous.State, depth)
		// A handful of masterchain-bound entries on top of the dense basechain
		// ones, so the narrowed walk has something to return and the parity
		// check below compares real messages rather than two empty runs.
		state = stateWithMasterchainBoundEntries(t, state, 7)
		previous := fixture.request.Previous
		previous.State = state
		view, err := localViewFromPrevious(previous, true, true)
		if err != nil {
			t.Fatal(err)
		}
		ref, err := localSourceRef(previous.ID)
		if err != nil {
			t.Fatal(err)
		}
		// The masterchain root, which is what AcquireMaster asks for. The dense
		// entries are addressed into the basechain, so almost none of them are
		// bound here — the shape the narrowed walk exists for.
		destination := targetShardIdent(groups.ShardID{Workchain: masterchainWorkchainID, Shard: sharddomain.Root})
		pool := msgpool.New(msgpool.Config{})
		defer pool.Close()
		if err = pool.Internals().ReconcileDestinations([]msgpool.ShardIdent{destination, source}); err != nil {
			t.Fatal(err)
		}
		internals := pool.Internals()

		var full, narrow []time.Duration
		var fullCount, narrowCount int
		for i := range 24 {
			if i%2 == 0 {
				started := time.Now()
				routed, _, err := internals.SeedsFromStateRoot(source, ref, view.previous.State)
				full = append(full, time.Since(started))
				if err != nil {
					t.Fatal(err)
				}
				for j := range routed {
					if routed[j].Destination == destination {
						fullCount = len(routed[j].Messages)
					}
				}
				continue
			}
			started := time.Now()
			messages, err := internals.SeedsForDestination(source, ref, view.previous.State, destination)
			narrow = append(narrow, time.Since(started))
			if err != nil {
				t.Fatal(err)
			}
			narrowCount = len(messages)
		}
		if fullCount != narrowCount {
			t.Fatalf("depth %d: narrowed walk returned %d messages, full walk %d", depth, narrowCount, fullCount)
		}
		// Element-wise, not by count: the narrowed walk must return the same
		// run in the same order, or the master build it feeds diverges.
		routed, _, err := internals.SeedsFromStateRoot(source, ref, view.previous.State)
		if err != nil {
			t.Fatal(err)
		}
		narrowed, err := internals.SeedsForDestination(source, ref, view.previous.State, destination)
		if err != nil {
			t.Fatal(err)
		}
		for j := range routed {
			if routed[j].Destination != destination {
				continue
			}
			if len(routed[j].Messages) != len(narrowed) {
				t.Fatalf("depth %d: %d against %d messages", depth, len(narrowed), len(routed[j].Messages))
			}
			for k := range narrowed {
				full := routed[j].Messages[k]
				got := narrowed[k]
				if !bytes.Equal(full.Key[:], got.Key[:]) || full.EnqueuedLT != got.EnqueuedLT ||
					full.QueueLT != got.QueueLT || full.Source != got.Source || full.SourceSeqno != got.SourceSeqno {
					t.Fatalf("depth %d: message %d differs between the walks", depth, k)
				}
			}
		}
		mn := func(s []time.Duration) float64 {
			sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
			return float64(s[0].Microseconds()) / 1000
		}
		t.Logf("queue %5d entries, %d bound for the masterchain: full %7.3f ms  narrowed %7.3f ms  (%.2fx)",
			depth, narrowCount, mn(full), mn(narrow), mn(full)/mn(narrow))
	}
}

// stateWithMasterchainBoundEntries inserts count out-queue entries whose next
// hop is the masterchain root, alongside whatever the queue already holds.
func stateWithMasterchainBoundEntries(t *testing.T, root *cell.Cell, count int) *cell.Cell {
	t.Helper()

	var state tlb.ShardStateUnsplit
	if err := parseExact(&state, root); err != nil {
		t.Fatal(err)
	}
	var queue tlb.OutMsgQueueInfo
	if err := parseExact(&queue, state.OutMsgQueueInfo); err != nil {
		t.Fatal(err)
	}
	fee := tlb.FromNanoTONU(70_000)
	for i := range count {
		var sourceID, destinationID [32]byte
		sourceID[0], sourceID[31] = 0x31, byte(i)
		destinationID[0], destinationID[31] = 0x55, byte(i)
		message, enqueued := queuedInternalWithReferencedBody(
			t,
			address.NewAddress(0, 0, sourceID[:]),
			address.NewAddress(0, 255, destinationID[:]),
			2_000_000+uint64(i),
			1_900_000_000,
			fee,
			fee,
			0,
			msgpool.ShardIdent{Workchain: masterchainWorkchainID, Shard: msgpool.ShardAll},
		)
		keyCell := cell.BeginCell().MustStoreSlice(message.Key[:], 352).EndCell()
		inserted, err := queue.OutQueue.SetWithMode(keyCell, enqueued, cell.DictSetModeAdd)
		if err != nil || !inserted {
			t.Fatalf("insert masterchain-bound entry %d: inserted=%t err=%v", i, inserted, err)
		}
	}
	var err error
	state.OutMsgQueueInfo, err = queue.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	if root, err = tlb.ToCell(&state); err != nil {
		t.Fatal(err)
	}
	return root
}
