package p2p

import (
	"testing"
	"time"
)

func TestPrioritizeStateSnapshotPeers(t *testing.T) {
	node := &Node{
		downloadPeerLeases: map[PeerID]int{
			testPeerID("peer-a"): 2,
			testPeerID("peer-b"): 0,
			testPeerID("peer-c"): 0,
		},
	}

	peers := []*overlayPeer{
		{id: testPeerID("peer-a"), addr: "peer-a"},
		{id: testPeerID("peer-b"), addr: "peer-b"},
		{id: testPeerID("peer-c"), addr: "peer-c"},
	}

	prioritized := node.prioritizeStateSnapshotPeers(peers)
	if len(prioritized) != len(peers) {
		t.Fatalf("unexpected peers count: %d", len(prioritized))
	}
	if prioritized[0].addr != "peer-b" || prioritized[1].addr != "peer-c" || prioritized[2].addr != "peer-a" {
		t.Fatalf("unexpected prioritized order: %q, %q, %q", prioritized[0].addr, prioritized[1].addr, prioritized[2].addr)
	}

	if peers[0].addr != "peer-a" || peers[1].addr != "peer-b" || peers[2].addr != "peer-c" {
		t.Fatal("prioritizeStateSnapshotPeers mutated original slice order")
	}
}

func TestPrioritizeBlockDownloadPeersUsesDownloadScore(t *testing.T) {
	node := &Node{
		downloadPeerLeases: map[PeerID]int{
			testPeerID("busy"): 2,
		},
	}
	now := time.Now()
	peers := []*overlayPeer{
		{id: testPeerID("slow"), addr: "slow", alive: true, downloadBytesSec: 20 << 20, downloadSlowUntil: now.Add(time.Minute)},
		{id: testPeerID("unknown"), addr: "unknown", alive: true},
		{id: testPeerID("busy"), addr: "busy", alive: true, downloadBytesSec: 10 << 20, roundtrip: 20 * time.Millisecond},
		{id: testPeerID("fast"), addr: "fast", alive: true, downloadBytesSec: 8 << 20, roundtrip: 20 * time.Millisecond},
	}

	prioritized := node.prioritizeBlockDownloadPeers(peers)
	if len(prioritized) != len(peers) {
		t.Fatalf("unexpected peers count: %d", len(prioritized))
	}
	if prioritized[0].addr != "fast" || prioritized[1].addr != "busy" || prioritized[2].addr != "unknown" || prioritized[3].addr != "slow" {
		t.Fatalf("unexpected prioritized order: %q, %q, %q, %q", prioritized[0].addr, prioritized[1].addr, prioritized[2].addr, prioritized[3].addr)
	}
	if peers[0].addr != "slow" || peers[1].addr != "unknown" || peers[2].addr != "busy" || peers[3].addr != "fast" {
		t.Fatal("prioritizeBlockDownloadPeers mutated original slice order")
	}
}

func TestAcquirePreferredStateSnapshotProbePrefersLessBusyPeer(t *testing.T) {
	node := &Node{
		downloadPeerLeases: map[PeerID]int{},
	}

	peerA := &overlayPeer{id: testPeerID("peer-a"), addr: "peer-a"}
	peerB := &overlayPeer{id: testPeerID("peer-b"), addr: "peer-b"}
	peerC := &overlayPeer{id: testPeerID("peer-c"), addr: "peer-c"}

	probes := []persistentStatePeerProbe{
		{
			candidate: persistentStateCandidate{peer: peerA},
			bytes:     12 << 20,
			elapsed:   time.Second,
		},
		{
			candidate: persistentStateCandidate{peer: peerB},
			bytes:     11 << 20,
			elapsed:   time.Second,
		},
		{
			candidate: persistentStateCandidate{peer: peerC},
			bytes:     10 << 20,
			elapsed:   time.Second,
		},
	}

	selected, release := node.acquirePreferredStateSnapshotProbe(probes)
	if selected.candidate.peer != peerA {
		t.Fatalf("expected fastest peer first, got %q", selected.candidate.peer.addr)
	}

	selectedNext, releaseNext := node.acquirePreferredStateSnapshotProbe(probes)
	if selectedNext.candidate.peer != peerB {
		t.Fatalf("expected less busy peer on second selection, got %q", selectedNext.candidate.peer.addr)
	}

	releaseNext()
	release()
}

func TestPersistentStateCandidateProbeChunksUsesSmallestCandidate(t *testing.T) {
	candidates := []persistentStateCandidate{
		{chunkCount: persistentStatePeerProbeChunks + 10},
		{chunkCount: 2},
		{chunkCount: persistentStatePeerProbeChunks},
	}

	if got := persistentStateCandidateProbeChunks(candidates); got != 2 {
		t.Fatalf("unexpected probe chunks: got %d want 2", got)
	}
}
