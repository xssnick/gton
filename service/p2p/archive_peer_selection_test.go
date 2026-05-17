package p2p

import (
	"github.com/xssnick/gton/service/archive"
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

func TestArchiveSmallSeedDoesNotUpdatePeerSpeed(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := &overlayPeer{id: "peer", addr: "peer", alive: true}

	noteArchivePeerSeedSuccess(shard, peer, archiveSpeedSampleMinBytes/4, time.Second)

	if stats := peer.statsSnapshot(); stats.downloadCount != 0 || stats.downloadBytesSec != 0 {
		t.Fatalf("small archive seed should not update peer speed: %#v", stats)
	}

	noteArchivePeerSeedSuccess(shard, peer, archiveSpeedSampleMinBytes, time.Second)

	stats := peer.statsSnapshot()
	if stats.downloadCount != 1 || stats.downloadBytesSec == 0 {
		t.Fatalf("reliable archive seed should update peer speed: %#v", stats)
	}
}

func TestArchiveSmallDownloadCanMarkPeerSlow(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := &overlayPeer{id: "peer", addr: "peer", alive: true}

	if noteArchivePeerDownload(shard, peer, archiveSpeedSampleMinBytes/4, time.Second) {
		t.Fatal("fast small archive download should not mark peer slow")
	}
	if stats := peer.statsSnapshot(); stats.downloadCount != 0 || stats.downloadBytesSec != 0 {
		t.Fatalf("small archive download should not update speed score: %#v", stats)
	}

	if !noteArchivePeerDownload(shard, peer, archiveSpeedSampleMinBytes/4, archiveSmallPackSlowElapsed+time.Second) {
		t.Fatal("slow small archive download should mark peer slow")
	}
	if !peer.statsSnapshot().downloadSlowUntil.After(time.Now()) {
		t.Fatal("slow small archive download should set slow penalty")
	}
}

func TestArchiveLargeDownloadUpdatesLargePackSpeed(t *testing.T) {
	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	peer := &overlayPeer{id: "peer", addr: "peer", alive: true}

	noteArchivePeerDownload(shard, peer, archiveSpeedSampleMinBytes, time.Second)
	if stats := peer.statsSnapshot(); stats.archiveLargeDownloads != 0 || stats.archiveLargeBytesSec != 0 {
		t.Fatalf("regular archive sample should not update large-pack speed: %#v", stats)
	}

	noteArchivePeerDownload(shard, peer, archiveLargeSpeedSampleMinBytes, time.Second)
	stats := peer.statsSnapshot()
	if stats.archiveLargeDownloads != 1 || stats.archiveLargeBytesSec == 0 {
		t.Fatalf("large archive sample should update large-pack speed: %#v", stats)
	}
}

func TestArchiveLargePackSpeedHasPriorityOverSmallPackSpeed(t *testing.T) {
	node := &Node{}
	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	largePackFast := &overlayPeer{
		id:                    "large-pack-fast",
		addr:                  "large-pack-fast",
		alive:                 true,
		downloadCount:         2,
		downloadBytesSec:      float64(5 << 20),
		archiveLargeBytesSec:  float64(60 << 20),
		archiveLargeDownloads: 1,
	}
	probeFast := &overlayPeer{
		id:               "small-pack-fast",
		addr:             "small-pack-fast",
		alive:            true,
		downloadCount:    2,
		downloadBytesSec: float64(100 << 20),
	}

	ordered := node.prioritizeArchivePeers(shard, []*overlayPeer{probeFast, largePackFast})
	if ordered[0] != largePackFast {
		t.Fatalf("large-pack speed should outrank small-pack speed, got %q", ordered[0].addr)
	}
}

func TestArchiveLargePackPeerCanUseParallelCapacity(t *testing.T) {
	node := &Node{
		downloadPeerLeases: map[string]int{
			"large-pack-fast": 2,
		},
	}
	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	largePackFast := &overlayPeer{
		id:                    "large-pack-fast",
		addr:                  "large-pack-fast",
		alive:                 true,
		downloadCount:         3,
		downloadBytesSec:      float64(20 << 20),
		archiveLargeBytesSec:  float64(24 << 20),
		archiveLargeDownloads: 2,
	}
	freeMedium := &overlayPeer{
		id:               "free-medium",
		addr:             "free-medium",
		alive:            true,
		downloadCount:    2,
		downloadBytesSec: float64(10 << 20),
	}

	ordered := node.prioritizeArchivePeers(shard, []*overlayPeer{freeMedium, largePackFast})
	if ordered[0] != largePackFast {
		t.Fatalf("large-pack peer should keep priority within parallel capacity, got %q", ordered[0].addr)
	}
}

func TestArchiveLargePackPriorityStopsAfterParallelCapacity(t *testing.T) {
	node := &Node{
		downloadPeerLeases: map[string]int{
			"large-pack-fast": 3,
		},
	}
	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	largePackFast := &overlayPeer{
		id:                    "large-pack-fast",
		addr:                  "large-pack-fast",
		alive:                 true,
		downloadCount:         3,
		downloadBytesSec:      float64(18 << 20),
		archiveLargeBytesSec:  float64(18 << 20),
		archiveLargeDownloads: 2,
	}
	freeProbeFast := &overlayPeer{
		id:               "free-probe-fast",
		addr:             "free-probe-fast",
		alive:            true,
		downloadCount:    2,
		downloadBytesSec: float64(12 << 20),
	}

	ordered := node.prioritizeArchivePeers(shard, []*overlayPeer{largePackFast, freeProbeFast})
	if ordered[0] != freeProbeFast {
		t.Fatalf("large-pack peer at capacity should not have absolute priority, got %q", ordered[0].addr)
	}
}
