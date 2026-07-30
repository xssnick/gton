package p2p

import (
	"fmt"
	"math"
	"testing"
)

// A neighbours entry without a matching peers entry is the state that crashed
// the node live (nil deref in worstRotatableNeighbourLocked via
// reloadNeighbours): a candidate snapshotted outside the lock was appended to
// neighbours after attachPooledPeer had already evicted it from the roster.
// These tests pin every reader of the neighbours list against that state.

func danglingNeighbourSubscription() (*overlaySubscription, PeerID) {
	var dangling PeerID
	dangling[0] = 0xD1
	sub := testOverlaySubscription(&overlaySubscription{
		log:        discardLogger(),
		peers:      map[PeerID]*overlayPeer{},
		neighbours: []PeerID{dangling},
	})
	return sub, dangling
}

func TestPruneNeighboursDropsDanglingEntry(t *testing.T) {
	sub, dangling := danglingNeighbourSubscription()
	sub.lastPingedNeighbour = dangling

	sub.mx.Lock()
	sub.pruneNeighboursLocked()
	neighbours := len(sub.neighbours)
	lastPinged := sub.lastPingedNeighbour
	sub.mx.Unlock()

	if neighbours != 0 {
		t.Fatalf("dangling neighbour must be pruned, %d left", neighbours)
	}
	if !lastPinged.IsZero() {
		t.Fatalf("last pinged neighbour must be reset with the dangling entry")
	}
}

func TestWorstRotatableNeighbourEvictsDanglingEntry(t *testing.T) {
	sub, dangling := danglingNeighbourSubscription()

	sub.mx.Lock()
	worstID, worstScore := sub.worstRotatableNeighbourLocked(nil)
	sub.mx.Unlock()

	if worstID != dangling {
		t.Fatalf("dangling neighbour must be the eviction candidate")
	}
	if worstScore != math.MaxFloat64 {
		t.Fatalf("dangling neighbour must rank as maximally unreliable, got %v", worstScore)
	}
}

func TestNeighbourPeerSnapshotsSkipDanglingEntry(t *testing.T) {
	sub, _ := danglingNeighbourSubscription()

	if peers := sub.neighbourPeerSnapshots(); len(peers) != 0 {
		t.Fatalf("dangling neighbour must not be snapshotted, got %d peers", len(peers))
	}
}

// TestReloadNeighboursHealsDanglingEntry replays the state the live crash left
// behind: a roster full enough to exercise the replacement path, with one
// neighbours entry whose peers entry disappeared without removeNeighbourLocked.
// On the pre-fix code this panics inside reloadNeighbours (nil overlayPeer);
// now the reload must survive and restore the neighbours ⊆ peers invariant.
func TestReloadNeighboursHealsDanglingEntry(t *testing.T) {
	roster := map[PeerID]*overlayPeer{}
	for i := 0; i < maxQueryNeighbours+2; i++ {
		peer := testArchiveCandidate(fmt.Sprintf("roster-%03d", i))
		roster[peer.id] = peer
	}
	sub := testOverlaySubscription(&overlaySubscription{
		log:   discardLogger(),
		peers: roster,
	})

	sub.reloadNeighbours()
	sub.mx.Lock()
	if len(sub.neighbours) == 0 {
		sub.mx.Unlock()
		t.Fatalf("reload must select neighbours from an alive roster")
	}
	dangling := sub.neighbours[0]
	// Simulate the historical race outcome directly: the roster entry vanishes
	// while the neighbours entry stays.
	delete(sub.peers, dangling)
	sub.mx.Unlock()

	sub.reloadNeighbours()

	sub.mx.Lock()
	defer sub.mx.Unlock()
	for _, id := range sub.neighbours {
		if id == dangling {
			t.Fatalf("dangling neighbour must be dropped by the reload")
		}
		if sub.peers[id] == nil {
			t.Fatalf("neighbours must be a subset of the roster after reload")
		}
	}
}
