package p2p

import (
	"context"
	"encoding/binary"
	"strings"
	"testing"
	"time"

	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/gton/service/archive/packfile"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/tl"
)

func TestArchivePackDownloadLimitRejectsEndlessFullSlices(t *testing.T) {
	var offset int64
	fullSlices := int(archivePackMaxBytes / archiveSliceSize)
	for i := 0; i < fullSlices; i++ {
		if err := checkArchivePackDownloadSize(offset, archiveSliceSize); err != nil {
			t.Fatalf("slice %d rejected before limit: %v", i, err)
		}
		offset += archiveSliceSize
	}
	if offset != archivePackMaxBytes {
		t.Fatalf("test setup reached offset=%d want=%d", offset, archivePackMaxBytes)
	}

	err := checkArchivePackDownloadSize(offset, archiveSliceSize)
	if err == nil || !strings.Contains(err.Error(), "exceeds max size") {
		t.Fatalf("next full slice error = %v, want max size rejection", err)
	}
}

func TestArchivePackMagicRejectsInvalidFirstSlice(t *testing.T) {
	valid := make([]byte, packfile.HeaderSize)
	binary.LittleEndian.PutUint32(valid, packfile.PackageMagic)
	if err := checkArchivePackMagic(valid); err != nil {
		t.Fatalf("valid archive magic rejected: %v", err)
	}

	if err := checkArchivePackMagic([]byte{1, 2, 3}); err == nil || !strings.Contains(err.Error(), "too short") {
		t.Fatalf("short archive magic error = %v, want too short", err)
	}

	invalid := make([]byte, packfile.HeaderSize)
	binary.LittleEndian.PutUint32(invalid, 0xdeadbeef)
	if err := checkArchivePackMagic(invalid); err == nil || !strings.Contains(err.Error(), "magic mismatch") {
		t.Fatalf("invalid archive magic error = %v, want mismatch", err)
	}
}

func TestSlowCompletedArchiveDownloadKeepsPeerPinned(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := &overlayPeer{id: testPeerID("slow-archive"), addr: "slow-archive", alive: true}
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := &overlaySubscription{
		log:          discardLogger(),
		node:         node,
		archivePeers: map[string]*archivePeerPoolState{},
	}
	session := node.BeginArchiveSession()
	defer session.Close()

	seed := make([]byte, packfile.HeaderSize)
	binary.LittleEndian.PutUint32(seed, packfile.PackageMagic)

	downloaded, err := sub.downloadArchiveFromPeer(context.Background(), session, resolvedArchive{
		MasterchainSeqno: 10,
		Shard:            shard,
	}, archiveCandidate{
		peer:        peer,
		archiveID:   100,
		seedSlice:   seed,
		seedElapsed: archiveSmallPackSlowElapsed + time.Second,
		hasSeed:     true,
	})
	if err != nil {
		t.Fatalf("download archive: %v", err)
	}
	if downloaded.Bytes != int64(len(seed)) {
		t.Fatalf("downloaded bytes = %d, want %d", downloaded.Bytes, len(seed))
	}
	if _, ok := node.archiveSessionPinnedPeerIDs()[peer.id]; !ok {
		t.Fatal("slow completed archive download should keep peer pinned")
	}
	if sub.archivePeerCoolingDown(shard, peer) {
		t.Fatal("slow completed archive download should not cool down peer")
	}
	if !peer.statsSnapshot().downloadSlowUntil.After(time.Now()) {
		t.Fatal("slow completed archive download should still update slow score")
	}
}

func TestArchiveInfoPinsPeerAndSeedProbeIsOptional(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peerID := testPeerID("archive-info")
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := &overlaySubscription{
		log:          discardLogger(),
		node:         node,
		spec:         overlaySpec{ShortID: []byte{1}},
		archivePeers: map[string]*archivePeerPoolState{},
		peers:        map[PeerID]*overlayPeer{},
	}
	session := node.BeginArchiveSession()
	defer session.Close()

	peerOverlay, _ := newTestOverlayWrapper()
	archiveRLDP := &testArchiveRLDP{
		adnl:        newTestOverlayADNL(),
		queryResult: ArchiveInfo{ID: 777},
		asyncErr:    context.DeadlineExceeded,
	}
	peer := &overlayPeer{
		id:          peerID,
		addr:        "archive-info",
		overlay:     peerOverlay,
		rldpOverlay: overlay.CreateExtendedRLDP(archiveRLDP).CreateOverlay([]byte{1}),
		alive:       true,
	}
	sub.peers[peerID] = peer

	candidate, err := sub.fetchArchiveCandidate(context.Background(), session, peer, 10, shard)
	if err != nil {
		t.Fatalf("fetch archive candidate: %v", err)
	}
	if candidate.archiveID != 777 {
		t.Fatalf("archive id = %d, want 777", candidate.archiveID)
	}
	if candidate.hasSeed {
		t.Fatal("seed probe failure should return candidate without seed")
	}
	if _, ok := node.archiveSessionPinnedPeerIDs()[peer.id]; !ok {
		t.Fatal("archive info should pin peer")
	}
	failures, pinned := session.archivePeerDeadlineFailures(peer)
	if !pinned || failures != 1 {
		t.Fatalf("deadline failures = %d pinned=%v, want 1 and pinned", failures, pinned)
	}
	if sub.archivePeerCoolingDown(shard, peer) {
		t.Fatal("seed probe deadline should not cool down peer")
	}
}

type testArchiveRLDP struct {
	adnl        *testOverlayADNL
	queryResult tl.Serializable
	asyncErr    error
}

func (r *testArchiveRLDP) GetADNL() rldp.ADNL {
	return r.adnl
}

func (r *testArchiveRLDP) GetRateInfo() (int64, int64) {
	return 0, 0
}

func (r *testArchiveRLDP) Close() {}

func (r *testArchiveRLDP) DoQuery(_ context.Context, _ uint64, _ tl.Serializable, result tl.Serializable) error {
	if out, ok := result.(*tl.Serializable); ok {
		*out = r.queryResult
	}
	return nil
}

func (r *testArchiveRLDP) DoQueryAsync(_ context.Context, _ uint64, _ []byte, _ tl.Serializable, _ chan<- rldp.AsyncQueryResult) error {
	return r.asyncErr
}

func (r *testArchiveRLDP) SetOnQuery(func([]byte, *rldp.Query) error) {}

func (r *testArchiveRLDP) SetOnMessage(func([]byte, []byte) error) {}

func (r *testArchiveRLDP) SetOnDisconnect(func()) {}

func (r *testArchiveRLDP) SendAnswer(context.Context, uint64, uint32, []byte, []byte, tl.Serializable) error {
	return nil
}
