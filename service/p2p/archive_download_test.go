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
		log:  discardLogger(),
		node: node,
	}
	pool := testArchivePool(sub)
	session := node.BeginArchiveSession()
	defer session.Close()

	seed := make([]byte, packfile.HeaderSize)
	binary.LittleEndian.PutUint32(seed, packfile.PackageMagic)

	downloaded, err := sub.downloadArchiveFromPeer(context.Background(), session, pool, resolvedArchive{
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
	if _, ok := node.pinnedPeerIDs()[peer.id]; !ok {
		t.Fatal("slow completed archive download should keep peer pinned")
	}
	if pool.coolingDown(shard, peer) {
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
		log:   discardLogger(),
		node:  node,
		spec:  overlaySpec{ShortID: []byte{1}},
		peers: map[PeerID]*overlayPeer{},
	}
	pool := testArchivePool(sub)
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
	pool.addBorrowedPeer(peer)

	candidate, err := sub.fetchArchiveCandidate(context.Background(), session, pool, peer, 10, shard)
	if err != nil {
		t.Fatalf("fetch archive candidate: %v", err)
	}
	if candidate.archiveID != 777 {
		t.Fatalf("archive id = %d, want 777", candidate.archiveID)
	}
	if candidate.hasSeed {
		t.Fatal("seed probe failure should return candidate without seed")
	}
	if _, ok := node.pinnedPeerIDs()[peer.id]; !ok {
		t.Fatal("archive info should pin peer")
	}
	failures, pinned := session.archivePeerDeadlineFailures(peer)
	if !pinned || failures != 1 {
		t.Fatalf("deadline failures = %d pinned=%v, want 1 and pinned", failures, pinned)
	}
	if pool.coolingDown(shard, peer) {
		t.Fatal("seed probe deadline should not cool down peer")
	}
}

func TestArchiveHedgedDownloadReturnsFastHedgePeer(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := &overlaySubscription{
		log:  discardLogger(),
		node: node,
		spec: overlaySpec{ShortID: []byte{1}},
	}
	pool := testArchivePool(sub)
	session := node.BeginArchiveSession()
	defer session.Close()

	primaryPack := testArchivePackBytes("primary")
	fastPack := testArchivePackBytes("fast")
	slowPack := testArchivePackBytes("slow")
	primary := testArchiveDownloadPeer(t, "primary", 100, primaryPack, 120*time.Millisecond)
	fast := testArchiveDownloadPeer(t, "fast", 200, fastPack, 10*time.Millisecond)
	slow := testArchiveDownloadPeer(t, "slow", 300, slowPack, 120*time.Millisecond)
	pool.addArchiveOnlyPeer(primary)
	pool.addArchiveOnlyPeer(fast)
	pool.addArchiveOnlyPeer(slow)

	downloaded, err := sub.downloadArchiveFromPeers(context.Background(), session, pool, resolvedArchive{
		MasterchainSeqno: 10,
		Shard:            shard,
		peer:             primary,
	}, []*overlayPeer{primary, fast, slow}, map[PeerID]archiveCandidate{
		primary.id: {
			peer:      primary,
			archiveID: 100,
		},
	}, ArchiveDownloadOptions{Hedge: true})
	if err != nil {
		t.Fatalf("hedged download archive: %v", err)
	}
	if downloaded.Peer != fast.addr {
		t.Fatalf("hedged archive winner = %q, want %q", downloaded.Peer, fast.addr)
	}
	if string(downloaded.Data) != string(fastPack) {
		t.Fatal("hedged archive returned data from the wrong peer")
	}
	if pool.coolingDown(shard, primary) {
		t.Fatal("hedge-lost primary should not be cooled down")
	}
}

func testArchiveDownloadPeer(t *testing.T, label string, archiveID int64, data []byte, delay time.Duration) *overlayPeer {
	t.Helper()

	peerOverlay, _ := newTestOverlayWrapper()
	archiveRLDP := &testArchiveRLDP{
		adnl:        newTestOverlayADNL(),
		queryResult: ArchiveInfo{ID: archiveID},
		asyncResult: data,
		asyncDelay:  delay,
	}
	return &overlayPeer{
		id:          testPeerID(label),
		addr:        label,
		overlay:     peerOverlay,
		rldpOverlay: overlay.CreateExtendedRLDP(archiveRLDP).CreateOverlay([]byte{1}),
		announced:   &overlay.Node{Version: int32(time.Now().Unix())},
		alive:       true,
	}
}

func testArchivePackBytes(label string) []byte {
	data := make([]byte, packfile.HeaderSize+len(label))
	binary.LittleEndian.PutUint32(data, packfile.PackageMagic)
	copy(data[packfile.HeaderSize:], label)
	return data
}

type testArchiveRLDP struct {
	adnl        *testOverlayADNL
	queryResult tl.Serializable
	asyncErr    error
	asyncResult []byte
	asyncDelay  time.Duration
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

func (r *testArchiveRLDP) DoQueryAsync(ctx context.Context, _ uint64, _ []byte, _ tl.Serializable, result chan<- rldp.AsyncQueryResult) error {
	if r.asyncErr == nil {
		data := append([]byte(nil), r.asyncResult...)
		go func() {
			if r.asyncDelay > 0 {
				timer := time.NewTimer(r.asyncDelay)
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-ctx.Done():
					return
				}
			}
			select {
			case result <- rldp.AsyncQueryResult{ResultBytes: data}:
			case <-ctx.Done():
			}
		}()
		return nil
	}
	return r.asyncErr
}

func (r *testArchiveRLDP) SetOnQuery(func([]byte, *rldp.Query) error) {}

func (r *testArchiveRLDP) SetOnMessage(func([]byte, []byte) error) {}

func (r *testArchiveRLDP) SetOnDisconnect(func()) {}

func (r *testArchiveRLDP) SendAnswer(context.Context, uint64, uint32, []byte, []byte, tl.Serializable) error {
	return nil
}
