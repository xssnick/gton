package p2p

import (
	"flexserver/service/archive"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl/overlay"
)

func TestArchivePeerDenylistFiltersOnlyArchivePool(t *testing.T) {
	sub := &overlaySubscription{
		log:          discardLogger(),
		archivePeers: map[string]*archivePeerState{},
	}
	peerA := &overlayPeer{id: "peer-a", addr: "peer-a"}
	peerB := &overlayPeer{id: "peer-b", addr: "peer-b"}
	basechain := archive.ShardID{Workchain: 0, Shard: topShard}
	masterchain := archive.ShardID{Workchain: -1, Shard: topShard}

	sub.denyArchivePeer(basechain, peerA, "test")

	got := sub.availableArchivePeers(basechain, []*overlayPeer{peerA, peerB})
	if len(got) != 1 || got[0] != peerB {
		t.Fatalf("unexpected basechain peers after deny: %#v", got)
	}

	got = sub.availableArchivePeers(masterchain, []*overlayPeer{peerA, peerB})
	if len(got) != 2 {
		t.Fatalf("denylist leaked into masterchain pool: %#v", got)
	}

	state := sub.archivePeers[archivePeerPoolKey(basechain)]
	state.deniedPeers[archivePeerKey(peerA)] = time.Now().Add(-time.Second)

	got = sub.availableArchivePeers(basechain, []*overlayPeer{peerA, peerB})
	if len(got) != 2 {
		t.Fatalf("expired denylist entry was not restored: %#v", got)
	}
}

func TestArchivePeerDenylistClearsStickyPeer(t *testing.T) {
	sub := &overlaySubscription{
		log:          discardLogger(),
		archivePeers: map[string]*archivePeerState{},
	}
	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	peerA := &overlayPeer{id: "peer-a", addr: "peer-a"}
	peerB := &overlayPeer{id: "peer-b", addr: "peer-b"}
	state := sub.archivePeerState(archivePeerPoolKey(shard))
	state.peer = peerA
	state.speed = 42

	sub.denyArchivePeer(shard, peerA, "test")

	if got := sub.currentArchivePeer(shard, []*overlayPeer{peerA, peerB}); got != nil {
		t.Fatalf("denied sticky peer should not be reused: %#v", got)
	}

	available := sub.availableArchivePeers(shard, []*overlayPeer{peerA, peerB})
	if len(available) != 1 || available[0] != peerB {
		t.Fatalf("unexpected available peers after sticky deny: %#v", available)
	}

	if got := sub.chooseArchivePeer(shard, available); got != peerB {
		t.Fatalf("expected non-denied peer to be selected, got %#v", got)
	}
}

func TestArchiveQueryCandidatesUseNeighboursBeforeKnownPeers(t *testing.T) {
	now := int32(time.Now().Unix())
	overlayWrapper := &overlay.ADNLOverlayWrapper{}
	sub := &overlaySubscription{
		log: discardLogger(),
		spec: overlaySpec{
			ProtoVersionMajor: shardchainProtoVersionMajor,
			ProtoVersionMinor: shardchainProtoVersionMinor,
		},
		peers: map[string]*overlayPeer{
			"peer-1": {id: "peer-1", overlay: overlayWrapper, announced: &overlay.Node{Version: now}, alive: true},
			"peer-2": {id: "peer-2", overlay: overlayWrapper, announced: &overlay.Node{Version: now}, alive: true},
			"peer-3": {id: "peer-3", overlay: overlayWrapper, announced: &overlay.Node{Version: now}, alive: true},
		},
		neighbours: []string{"peer-1", "peer-2"},
	}

	got := sub.archiveQueryCandidates()
	if len(got) != 2 {
		t.Fatalf("archive candidates should use only neighbours when present, got %d", len(got))
	}
	seen := map[string]struct{}{
		got[0].id: {},
		got[1].id: {},
	}
	if _, ok := seen["peer-1"]; !ok {
		t.Fatalf("expected peer-1 in archive neighbours, got %#v", got)
	}
	if _, ok := seen["peer-2"]; !ok {
		t.Fatalf("expected peer-2 in archive neighbours, got %#v", got)
	}
}

func TestArchiveQueryCandidatesDoNotUseKnownPeersWithoutNeighbours(t *testing.T) {
	now := int32(time.Now().Unix())
	overlayWrapper := &overlay.ADNLOverlayWrapper{}
	sub := &overlaySubscription{
		log: discardLogger(),
		spec: overlaySpec{
			ProtoVersionMajor: shardchainProtoVersionMajor,
			ProtoVersionMinor: shardchainProtoVersionMinor,
		},
		peers: map[string]*overlayPeer{
			"peer-1": {id: "peer-1", overlay: overlayWrapper, announced: &overlay.Node{Version: now}, alive: true},
			"peer-2": {id: "peer-2", overlay: overlayWrapper, announced: &overlay.Node{Version: now}, alive: true},
		},
	}

	got := sub.archiveQueryCandidates()
	if len(got) != 0 {
		t.Fatalf("archive candidates should use only neighbours, got %d known peers", len(got))
	}
}

func TestArchiveQueryCandidatesSkipDeadKnownPeers(t *testing.T) {
	now := int32(time.Now().Unix())
	overlayWrapper := &overlay.ADNLOverlayWrapper{}
	sub := &overlaySubscription{
		log: discardLogger(),
		spec: overlaySpec{
			ProtoVersionMajor: shardchainProtoVersionMajor,
			ProtoVersionMinor: shardchainProtoVersionMinor,
		},
		peers: map[string]*overlayPeer{
			"alive": {id: "alive", overlay: overlayWrapper, announced: &overlay.Node{Version: now}, alive: true},
			"dead":  {id: "dead", overlay: overlayWrapper, announced: &overlay.Node{Version: now}, alive: false},
		},
		neighbours: []string{"dead", "alive"},
	}

	got := sub.archiveQueryCandidates()
	if len(got) != 1 {
		t.Fatalf("unexpected archive candidate count: got %d want 1", len(got))
	}
	if got[0].id != "alive" {
		t.Fatalf("expected only alive peer, got %q", got[0].id)
	}
}

func TestShouldRaceArchiveDownloadForUnknownOrMediocrePeer(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	unknown := &overlayPeer{id: "unknown", addr: "unknown", alive: true}
	alternative := &overlayPeer{id: "alternative", addr: "alternative", alive: true}

	if !shouldRaceArchiveDownload(shard, []*overlayPeer{unknown, alternative}) {
		t.Fatal("unknown sticky peer should trigger archive probe race")
	}

	mediocre := &overlayPeer{
		id:               "mediocre",
		addr:             "mediocre",
		alive:            true,
		downloadCount:    3,
		downloadBytesSec: 2 << 20,
	}
	if !shouldRaceArchiveDownload(shard, []*overlayPeer{mediocre, alternative}) {
		t.Fatal("mediocre sticky peer should trigger archive probe race")
	}
}

func TestShouldNotRaceArchiveDownloadForGoodPeer(t *testing.T) {
	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	fast := &overlayPeer{
		id:               "fast",
		addr:             "fast",
		alive:            true,
		downloadCount:    4,
		downloadBytesSec: 16 << 20,
	}
	alternative := &overlayPeer{id: "alternative", addr: "alternative", alive: true}

	if shouldRaceArchiveDownload(shard, []*overlayPeer{fast, alternative}) {
		t.Fatal("good sticky peer should not trigger archive probe race")
	}
}

func TestCurrentArchivePeerNeedsGoodSpeedForExclusiveUse(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	unknown := &overlayPeer{id: "unknown", addr: "unknown", alive: true}
	if shouldUseCurrentArchivePeerWithoutRace(shard, unknown) {
		t.Fatal("unknown current peer should still race archive probes")
	}

	mediocre := &overlayPeer{
		id:               "mediocre",
		addr:             "mediocre",
		alive:            true,
		downloadCount:    2,
		downloadBytesSec: 4 << 20,
	}
	if shouldUseCurrentArchivePeerWithoutRace(shard, mediocre) {
		t.Fatal("mediocre current peer should still race archive probes")
	}

	fast := &overlayPeer{
		id:               "fast",
		addr:             "fast",
		alive:            true,
		downloadCount:    2,
		downloadBytesSec: 12 << 20,
	}
	if !shouldUseCurrentArchivePeerWithoutRace(shard, fast) {
		t.Fatal("good current peer should be used without archive probe race")
	}
}
