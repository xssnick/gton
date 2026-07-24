package p2p

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/gton/service/archive/packfile"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/tl"
)

func TestDownloadArchiveCanceledContextDoesNotCreateSubscription(t *testing.T) {
	node := newTestNode(t)
	node.zeroStateFileHash = make([]byte, 32)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	session := node.BeginArchiveSession()
	_, err := session.DownloadArchive(ctx, 1, archive.ShardID{Workchain: 0, Shard: topShard}, ArchiveDownloadOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("download archive error = %v, want context.Canceled", err)
	}
	if len(node.subscriptions) != 0 {
		t.Fatalf("subscriptions after canceled archive download = %d, want 0", len(node.subscriptions))
	}
}

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

func TestArchiveNetworkProbeLimits(t *testing.T) {
	tests := []struct {
		name string
		got  time.Duration
		want time.Duration
	}{
		{name: "archive info timeout", got: archiveInfoTimeout, want: 6 * time.Second},
		{name: "probe timeout", got: archiveSliceProbeTimeout, want: 10 * time.Second},
		{name: "initial full slice timeout", got: archiveSliceInitialTimeout, want: 12 * time.Second},
		{name: "slice timeout", got: archiveSliceTimeout, want: 15 * time.Second},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.got != test.want {
				t.Fatalf("timeout = %s, want %s", test.got, test.want)
			}
		})
	}
	if archiveSliceProbeSize != 256<<10 {
		t.Fatalf("probe size = %d, want %d", archiveSliceProbeSize, 256<<10)
	}
}

func TestResolveArchiveWithoutCandidatesReturnsNoPeers(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: node,
	})
	pool := testArchivePool(t, sub)
	session := node.BeginArchiveSession()
	defer session.Close()

	_, err := sub.resolveArchive(context.Background(), session, pool, 10, shard, ArchiveDownloadOptions{})
	if !errors.Is(err, ErrNoArchivePeers) {
		t.Fatalf("resolve archive error = %v, want no archive peers", err)
	}
	if errors.Is(err, archive.ErrNotAvailable) {
		t.Fatalf("no archive peers error must not be classified as archive not available: %v", err)
	}
}

func TestCompletedArchiveDownloadKeepsStickyPeer(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := testArchiveCandidate("slow-archive")
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: node,
	})
	pool := testArchivePool(t, sub)
	if !addTestArchiveOnlyPeer(pool, peer) {
		t.Fatal("add archive peer")
	}
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
		seedElapsed: time.Second,
		hasSeed:     true,
	}, true)
	if err != nil {
		t.Fatalf("download archive: %v", err)
	}
	if downloaded.Bytes != int64(len(seed)) {
		t.Fatalf("downloaded bytes = %d, want %d", downloaded.Bytes, len(seed))
	}
	if _, ok := node.protectedPeerIDs()[peer.id]; ok {
		t.Fatal("completed archive download entered live peer protection")
	}
	if selected := session.selectedArchivePeerID(shard); selected != peer.id {
		t.Fatalf("completed archive download selected peer = %s, want %s", selected.String(), peer.id.String())
	}
	if pool.coolingDown(shard, peer) {
		t.Fatal("completed archive download should not cool down peer")
	}
	if peer.statsSnapshot().downloadSlowUntil.After(time.Now()) {
		t.Fatal("completed archive download should not set fixed slow penalty")
	}
}

func TestArchiveInfoWithoutSeedDoesNotProvePeer(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peerID := testPeerID("archive-info")
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{
		log:   discardLogger(),
		node:  node,
		spec:  overlaySpec{ShortID: []byte{1}},
		peers: map[PeerID]*overlayPeer{},
	})
	pool := testArchivePool(t, sub)
	session := node.BeginArchiveSession()
	defer session.Close()

	peerOverlay, base := newTestOverlayWrapper()
	archiveRLDP := &testArchiveRLDP{
		adnl:        newTestOverlayADNL(),
		queryResult: ArchiveInfo{ID: 777},
		asyncErr:    context.DeadlineExceeded,
	}
	var archiveInfoQueries int
	base.queryResponder = func(req tl.Serializable, result tl.Serializable) error {
		payload := testOverlayQueryPayload(req)
		if _, ok := payload.(GetArchiveInfo); !ok {
			t.Fatalf("unexpected archive info query payload %T", payload)
		}

		archiveInfoQueries++
		if out, ok := result.(*tl.Serializable); ok {
			*out = ArchiveInfo{ID: 777}
		}
		return nil
	}
	peer := &overlayPeer{
		id:          peerID,
		addr:        "archive-info",
		overlay:     peerOverlay,
		rldpOverlay: overlay.CreateExtendedRLDP(archiveRLDP).CreateOverlay([]byte{1}),
		alive:       true,
	}
	sub.peers[peerID] = peer
	addTestArchiveOnlyPeer(pool, peer)

	_, err := sub.fetchArchiveCandidate(context.Background(), session, pool, peer, 10, shard, true)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("fetch archive candidate error = %v, want context.DeadlineExceeded", err)
	}
	if archiveInfoQueries != 1 {
		t.Fatalf("archive info ADNL queries = %d, want 1", archiveInfoQueries)
	}
	snapshot := archiveRLDP.snapshot()
	if snapshot.queryCalls != 0 {
		t.Fatalf("archive info used RLDP DoQuery %d times, want 0", snapshot.queryCalls)
	}
	if len(snapshot.asyncQueries) != 1 {
		t.Fatalf("archive probe queries = %d, want 1", len(snapshot.asyncQueries))
	}
	query := snapshot.asyncQueries[0]
	probe, ok := testOverlayQueryPayload(query.request).(GetArchiveSlice)
	if !ok {
		t.Fatalf("archive probe payload = %T, want GetArchiveSlice", testOverlayQueryPayload(query.request))
	}
	if probe.MaxSize != archiveSliceProbeSize {
		t.Fatalf("archive probe max size = %d, want %d", probe.MaxSize, archiveSliceProbeSize)
	}
	if query.maxAnswer != archiveSliceProbeSize+4096 {
		t.Fatalf("archive probe max answer = %d, want %d", query.maxAnswer, archiveSliceProbeSize+4096)
	}
	if _, ok := node.protectedPeerIDs()[peer.id]; ok {
		t.Fatal("archive info without bytes should not pin peer")
	}
	if got := pool.provenUsableSize(time.Now()); got != 0 {
		t.Fatalf("proven peers = %d, want 0", got)
	}
	if pool.coolingDown(shard, peer) {
		t.Fatal("unhandled seed probe error should not cool down peer")
	}
}

func TestArchiveHedgeSeedProbeDoesNotPinLoser(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: node,
		spec: overlaySpec{ShortID: []byte{1}},
	})
	pool := testArchivePool(t, sub)
	probe, releaseDemand, err := pool.beginArchiveRequest(shard, 10)
	if err != nil {
		t.Fatalf("begin archive demand: %v", err)
	}
	defer releaseDemand()
	session := node.BeginArchiveSession()
	defer session.Close()

	data := make([]byte, archiveSliceProbeSize)
	binary.LittleEndian.PutUint32(data, packfile.PackageMagic)
	peer := testArchiveDownloadPeer(t, "hedge-loser", 777, data, 0)
	if !addTestArchiveOnlyPeer(pool, peer) {
		t.Fatal("add archive hedge peer")
	}

	candidate, err := sub.fetchArchiveCandidate(context.Background(), session, pool, peer, 10, shard, false)
	if err != nil {
		t.Fatalf("fetch archive hedge candidate: %v", err)
	}
	if !candidate.hasSeed || len(candidate.seedSlice) == 0 {
		t.Fatal("archive hedge probe did not return real bytes")
	}
	if selected := session.selectedArchivePeerID(shard); !selected.IsZero() {
		t.Fatalf("hedge loser became selected: %s", selected.String())
	}
	if _, ok := node.protectedPeerIDs()[peer.id]; ok {
		t.Fatal("hedge loser was pinned by a non-selecting seed probe")
	}
	pool.mx.Lock()
	evidence := pool.demands[probe.demandID].peers[peer.id].evidence
	pool.mx.Unlock()
	if evidence != archivePeerDemandProven {
		t.Fatalf("foreground seed evidence = %d, want proven", evidence)
	}
}

func TestArchiveForegroundEmptyProbeRecordsDemandNotAvailable(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: node,
		spec: overlaySpec{ShortID: []byte{1}},
	})
	pool := testArchivePool(t, sub)
	probe, releaseDemand, err := pool.beginArchiveRequest(shard, 10)
	if err != nil {
		t.Fatalf("begin archive demand: %v", err)
	}
	defer releaseDemand()
	session := node.BeginArchiveSession()
	defer session.Close()

	peer := testArchiveDownloadPeer(t, "empty-foreground-probe", 777, nil, 0)
	if !addTestArchiveOnlyPeer(pool, peer) {
		t.Fatal("add archive peer")
	}
	if _, err = sub.fetchArchiveCandidate(context.Background(), session, pool, peer, 10, shard, false); !errors.Is(err, archive.ErrNotAvailable) {
		t.Fatalf("empty archive probe error = %v, want not available", err)
	}

	pool.mx.Lock()
	state := pool.demands[probe.demandID].peers[peer.id]
	pool.mx.Unlock()
	if state.evidence != archivePeerDemandNotAvailable || !state.rejectedUntil.After(time.Now()) {
		t.Fatalf("foreground negative evidence = %+v", state)
	}
}

func TestArchiveSmallProbeContinuesFullDownload(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: node,
		spec: overlaySpec{ShortID: []byte{1}},
	})
	pool := testArchivePool(t, sub)
	session := node.BeginArchiveSession()
	defer session.Close()

	data := make([]byte, archiveSliceProbeSize+1024)
	binary.LittleEndian.PutUint32(data, packfile.PackageMagic)
	for i := packfile.HeaderSize; i < len(data); i++ {
		data[i] = byte(i)
	}
	peer := testArchiveDownloadPeer(t, "probe-continue", 777, data, 0)
	addTestArchiveOnlyPeer(pool, peer)

	candidate, err := sub.fetchArchiveCandidate(context.Background(), session, pool, peer, 10, shard, true)
	if err != nil {
		t.Fatalf("fetch archive candidate: %v", err)
	}
	if len(candidate.seedSlice) != archiveSliceProbeSize {
		t.Fatalf("seed bytes = %d, want %d", len(candidate.seedSlice), archiveSliceProbeSize)
	}

	downloaded, err := sub.downloadArchiveFromPeer(context.Background(), session, pool, resolvedArchive{
		MasterchainSeqno: 10,
		Shard:            shard,
	}, candidate, true)
	if err != nil {
		t.Fatalf("download archive after probe: %v", err)
	}
	if string(downloaded.Data) != string(data) {
		t.Fatalf("downloaded archive bytes = %d, want %d", len(downloaded.Data), len(data))
	}
}

func TestArchiveInitialFullSliceUsesShortTimeout(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: node,
		spec: overlaySpec{ShortID: []byte{1}},
	})
	pool := testArchivePool(t, sub)
	session := node.BeginArchiveSession()
	defer session.Close()

	data := make([]byte, archiveSliceProbeSize+1)
	binary.LittleEndian.PutUint32(data, packfile.PackageMagic)
	peer, archiveRLDP := testArchiveDownloadPeerWithRLDP(t, "initial-full-timeout", 777, data, 0)
	addTestArchiveOnlyPeer(pool, peer)

	candidate, err := sub.fetchArchiveCandidate(context.Background(), session, pool, peer, 10, shard, true)
	if err != nil {
		t.Fatalf("fetch archive candidate: %v", err)
	}
	if _, err = sub.downloadArchiveFromPeer(context.Background(), session, pool, resolvedArchive{
		MasterchainSeqno: 10,
		Shard:            shard,
	}, candidate, true); err != nil {
		t.Fatalf("download archive: %v", err)
	}

	snapshot := archiveRLDP.snapshot()
	if len(snapshot.asyncQueries) != 3 {
		t.Fatalf("archive slice queries = %d, want probe plus 2 pipelined slices", len(snapshot.asyncQueries))
	}
	var got time.Duration
	for _, record := range snapshot.asyncQueries {
		query, ok := testOverlayQueryPayload(record.request).(GetArchiveSlice)
		if ok && query.Offset == int64(len(candidate.seedSlice)) {
			got = record.timeout
			break
		}
	}
	if got == 0 {
		t.Fatal("initial full slice query was not recorded")
	}
	if got < archiveSliceInitialTimeout-time.Second || got > archiveSliceInitialTimeout {
		t.Fatalf("initial full slice timeout = %s, want about %s", got, archiveSliceInitialTimeout)
	}
}

func TestArchiveSliceDownloadPipelinesTwoRequests(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: node,
		spec: overlaySpec{ShortID: []byte{1}},
	})
	pool := testArchivePool(t, sub)
	session := node.BeginArchiveSession()
	defer session.Close()

	data := make([]byte, 2*archiveSliceSize+128)
	binary.LittleEndian.PutUint32(data, packfile.PackageMagic)
	peer, archiveRLDP := testArchiveDownloadPeerWithRLDP(t, "slice-pipeline", 777, data, 30*time.Millisecond)
	addTestArchiveOnlyPeer(pool, peer)

	downloadStarted := time.Now()
	downloaded, err := sub.downloadArchiveFromPeer(context.Background(), session, pool, resolvedArchive{
		MasterchainSeqno: 10,
		Shard:            shard,
	}, archiveCandidate{peer: peer, archiveID: 777}, true)
	wallElapsed := time.Since(downloadStarted)
	if err != nil {
		t.Fatalf("download archive: %v", err)
	}
	if !bytes.Equal(downloaded.Data, data) {
		t.Fatalf("downloaded archive bytes = %d, want %d", len(downloaded.Data), len(data))
	}
	if got := archiveRLDP.snapshot().asyncMaxActive; got != archiveSliceParallelism {
		t.Fatalf("maximum concurrent archive slices = %d, want %d", got, archiveSliceParallelism)
	}
	if downloaded.DownloadElapsed > wallElapsed {
		t.Fatalf("archive download elapsed = %s, exceeds wall time %s", downloaded.DownloadElapsed, wallElapsed)
	}
}

func TestArchiveSliceDownloadAssemblesOutOfOrder(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: node,
		spec: overlaySpec{ShortID: []byte{1}},
	})
	pool := testArchivePool(t, sub)
	session := node.BeginArchiveSession()
	defer session.Close()

	data := make([]byte, 2*archiveSliceSize+97)
	binary.LittleEndian.PutUint32(data, packfile.PackageMagic)
	peer, archiveRLDP := testArchiveDownloadPeerWithRLDP(t, "slice-out-of-order", 777, data, 0)
	archiveRLDP.asyncDelays = map[int64]time.Duration{
		0:                60 * time.Millisecond,
		archiveSliceSize: 5 * time.Millisecond,
	}
	addTestArchiveOnlyPeer(pool, peer)

	downloaded, err := sub.downloadArchiveFromPeer(context.Background(), session, pool, resolvedArchive{
		MasterchainSeqno: 10,
		Shard:            shard,
	}, archiveCandidate{peer: peer, archiveID: 777}, true)
	if err != nil {
		t.Fatalf("download archive: %v", err)
	}
	if !bytes.Equal(downloaded.Data, data) {
		t.Fatalf("downloaded archive bytes = %d, want %d", len(downloaded.Data), len(data))
	}

	completed := archiveRLDP.snapshot().asyncCompleted
	if len(completed) == 0 || completed[0] != archiveSliceSize {
		t.Fatalf("first completed archive offset = %v, want %d", completed, archiveSliceSize)
	}
}

func TestArchiveSliceDownloadCancelsRequestBeyondEOF(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: node,
		spec: overlaySpec{ShortID: []byte{1}},
	})
	pool := testArchivePool(t, sub)
	session := node.BeginArchiveSession()
	defer session.Close()

	data := make([]byte, archiveSliceSize+97)
	binary.LittleEndian.PutUint32(data, packfile.PackageMagic)
	peer, archiveRLDP := testArchiveDownloadPeerWithRLDP(t, "slice-eof-cancel", 777, data, 0)
	canceled := make(chan int64, 1)
	archiveRLDP.asyncDelays = map[int64]time.Duration{
		0:                5 * time.Millisecond,
		archiveSliceSize: 40 * time.Millisecond,
	}
	archiveRLDP.asyncWaitForCancel = map[int64]bool{2 * int64(archiveSliceSize): true}
	archiveRLDP.asyncCanceled = canceled
	addTestArchiveOnlyPeer(pool, peer)

	downloaded, err := sub.downloadArchiveFromPeer(context.Background(), session, pool, resolvedArchive{
		MasterchainSeqno: 10,
		Shard:            shard,
	}, archiveCandidate{peer: peer, archiveID: 777}, true)
	if err != nil {
		t.Fatalf("download archive: %v", err)
	}
	if !bytes.Equal(downloaded.Data, data) {
		t.Fatalf("downloaded archive bytes = %d, want %d", len(downloaded.Data), len(data))
	}

	select {
	case offset := <-canceled:
		if offset != 2*int64(archiveSliceSize) {
			t.Fatalf("canceled archive offset = %d, want %d", offset, 2*archiveSliceSize)
		}
	case <-time.After(time.Second):
		t.Fatal("request beyond archive EOF was not canceled")
	}
	if active := archiveRLDP.snapshot().asyncActive; active != 0 {
		t.Fatalf("active archive requests after EOF = %d, want 0", active)
	}
}

func TestArchiveSliceDownloadCancellationStopsOutstandingRequests(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: node,
		spec: overlaySpec{ShortID: []byte{1}},
	})
	pool := testArchivePool(t, sub)
	session := node.BeginArchiveSession()
	defer session.Close()

	peer, archiveRLDP := testArchiveDownloadPeerWithRLDP(t, "slice-context-cancel", 777, nil, 0)
	started := make(chan GetArchiveSlice, archiveSliceParallelism)
	canceled := make(chan int64, archiveSliceParallelism)
	archiveRLDP.asyncWaitForCancel = map[int64]bool{
		0:                true,
		archiveSliceSize: true,
	}
	archiveRLDP.asyncStarted = started
	archiveRLDP.asyncCanceled = canceled
	addTestArchiveOnlyPeer(pool, peer)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := sub.downloadArchiveFromPeer(ctx, session, pool, resolvedArchive{
			MasterchainSeqno: 10,
			Shard:            shard,
		}, archiveCandidate{peer: peer, archiveID: 777}, true)
		errCh <- err
	}()

	for i := 0; i < archiveSliceParallelism; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("archive pipeline did not start both requests")
		}
	}
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("download archive error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled archive download did not return")
	}
	for i := 0; i < archiveSliceParallelism; i++ {
		select {
		case <-canceled:
		case <-time.After(time.Second):
			t.Fatal("outstanding archive request did not stop")
		}
	}
	if active := archiveRLDP.snapshot().asyncActive; active != 0 {
		t.Fatalf("active archive requests after cancellation = %d, want 0", active)
	}
}

func TestArchiveSliceDownloadErrorCancelsOutstandingRequest(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: node,
		spec: overlaySpec{ShortID: []byte{1}},
	})
	pool := testArchivePool(t, sub)
	session := node.BeginArchiveSession()
	defer session.Close()

	sliceErr := errors.New("archive slice failed")
	peer, archiveRLDP := testArchiveDownloadPeerWithRLDP(t, "slice-error-cancel", 777, nil, 0)
	canceled := make(chan int64, 1)
	archiveRLDP.asyncErrors = map[int64]error{0: sliceErr}
	archiveRLDP.asyncWaitForCancel = map[int64]bool{archiveSliceSize: true}
	archiveRLDP.asyncCanceled = canceled
	addTestArchiveOnlyPeer(pool, peer)

	_, err := sub.downloadArchiveFromPeer(context.Background(), session, pool, resolvedArchive{
		MasterchainSeqno: 10,
		Shard:            shard,
	}, archiveCandidate{peer: peer, archiveID: 777}, true)
	if !errors.Is(err, sliceErr) {
		t.Fatalf("download archive error = %v, want %v", err, sliceErr)
	}
	if !strings.Contains(err.Error(), "offset=0") {
		t.Fatalf("download archive error = %q, want offset=0", err)
	}

	select {
	case offset := <-canceled:
		if offset != archiveSliceSize {
			t.Fatalf("canceled archive offset = %d, want %d", offset, archiveSliceSize)
		}
	case <-time.After(time.Second):
		t.Fatal("outstanding archive request was not canceled after slice error")
	}

	snapshot := archiveRLDP.snapshot()
	if snapshot.asyncActive != 0 {
		t.Fatalf("active archive requests after slice error = %d, want 0", snapshot.asyncActive)
	}
	if len(snapshot.asyncQueries) != archiveSliceParallelism {
		t.Fatalf("archive slice queries = %d, want %d", len(snapshot.asyncQueries), archiveSliceParallelism)
	}
	queried := make(map[int64]bool, archiveSliceParallelism)
	for _, record := range snapshot.asyncQueries {
		query, ok := testOverlayQueryPayload(record.request).(GetArchiveSlice)
		if !ok {
			t.Fatalf("archive slice payload = %T, want GetArchiveSlice", testOverlayQueryPayload(record.request))
		}
		queried[query.Offset] = true
	}
	if !queried[0] || !queried[archiveSliceSize] {
		t.Fatalf("archive slice offsets = %v, want 0 and %d", queried, archiveSliceSize)
	}
}

func TestArchiveProbeBytesDoNotResetFullSliceFailures(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: node,
		spec: overlaySpec{ShortID: []byte{1}},
	})
	pool := testArchivePool(t, sub)
	session := node.BeginArchiveSession()
	defer session.Close()

	data := make([]byte, archiveSliceProbeSize+1)
	binary.LittleEndian.PutUint32(data, packfile.PackageMagic)
	peer := testArchiveDownloadPeer(t, "probe-resets-full-failure", 777, data, 0)
	addTestArchiveOnlyPeer(pool, peer)

	attempts := archivePeerErrorRotateThreshold
	for i := 0; i < attempts; i++ {
		if _, err := sub.fetchArchiveCandidate(context.Background(), session, pool, peer, 10, shard, true); err != nil {
			t.Fatalf("fetch archive candidate %d: %v", i+1, err)
		}
		sub.noteArchiveDownloadError(context.Background(), session, pool, 1, shard, peer, context.DeadlineExceeded)
	}

	if selected := session.selectedArchivePeerID(shard); !selected.IsZero() {
		t.Fatalf("full slice failures were reset by probe bytes: selected peer = %s", selected.String())
	}
	if testArchivePoolHasPeer(pool, peer.id) {
		t.Fatal("archive-only peer survived repeated full slice failures")
	}
}

func TestArchiveHedgedDownloadReturnsFastHedgePeer(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: node,
		spec: overlaySpec{ShortID: []byte{1}},
	})
	pool := testArchivePool(t, sub)
	session := node.BeginArchiveSession()
	defer session.Close()

	primaryPack := testArchivePackBytes("primary")
	fastPack := testArchivePackBytes("fast")
	slowPack := testArchivePackBytes("slow")
	primary := testArchiveDownloadPeer(t, "primary", 100, primaryPack, 120*time.Millisecond)
	fast := testArchiveDownloadPeer(t, "fast", 200, fastPack, 10*time.Millisecond)
	slow := testArchiveDownloadPeer(t, "slow", 300, slowPack, 120*time.Millisecond)
	addTestArchiveOnlyPeer(pool, primary)
	addTestArchiveOnlyPeer(pool, fast)
	addTestArchiveOnlyPeer(pool, slow)

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

func TestArchiveHedgedDownloadCancellationDrainsAttempts(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: node,
		spec: overlaySpec{ShortID: []byte{1}},
	})
	pool := testArchivePool(t, sub)
	session := node.BeginArchiveSession()
	defer session.Close()

	primary, primaryRLDP := testArchiveDownloadPeerWithRLDP(t, "cancel-drain-primary", 100, nil, 0)
	secondary, secondaryRLDP := testArchiveDownloadPeerWithRLDP(t, "cancel-drain-secondary", 200, nil, 0)
	primaryStarted := make(chan GetArchiveSlice, archiveSliceParallelism)
	secondaryStarted := make(chan GetArchiveSlice, archiveSliceParallelism)
	primaryGate := make(chan struct{})
	secondaryGate := make(chan struct{})
	releasePrimary := sync.OnceFunc(func() { close(primaryGate) })
	releaseSecondary := sync.OnceFunc(func() { close(secondaryGate) })
	t.Cleanup(releasePrimary)
	t.Cleanup(releaseSecondary)
	primaryRLDP.asyncStarted = primaryStarted
	primaryRLDP.asyncDoQueryGate = primaryGate
	secondaryRLDP.asyncStarted = secondaryStarted
	secondaryRLDP.asyncDoQueryGate = secondaryGate
	addTestArchiveOnlyPeer(pool, primary)
	addTestArchiveOnlyPeer(pool, secondary)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan []error, 1)
	go func() {
		_, _, errs := sub.downloadArchiveFromPeersHedged(ctx, session, pool, resolvedArchive{
			MasterchainSeqno: 10,
			Shard:            shard,
		}, []*overlayPeer{primary, secondary}, map[PeerID]archiveCandidate{
			primary.id: {
				peer:      primary,
				archiveID: 100,
			},
			secondary.id: {
				peer:      secondary,
				archiveID: 200,
			},
		})
		result <- errs
	}()

	waitArchiveSliceStarted(t, primaryStarted)
	waitArchiveSliceStarted(t, secondaryStarted)
	cancel()
	releasePrimary()
	waitArchivePeerLeases(t, pool, primary, 0)

	select {
	case <-result:
		t.Fatal("hedged download returned before the remaining attempt stopped")
	case <-time.After(50 * time.Millisecond):
	}

	releaseSecondary()
	select {
	case errs := <-result:
		if !errors.Is(errors.Join(errs...), context.Canceled) {
			t.Fatalf("hedged download error = %v, want context.Canceled", errors.Join(errs...))
		}
	case <-time.After(time.Second):
		t.Fatal("hedged download did not return after all attempts stopped")
	}
}

func TestArchiveInfoHedgeCancellationDrainsAttempts(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: node,
		spec: overlaySpec{ShortID: []byte{1}},
	})
	pool := testArchivePool(t, sub)
	session := node.BeginArchiveSession()
	defer session.Close()

	primary, primaryRLDP := testArchiveDownloadPeerWithRLDP(t, "info-cancel-drain-primary", 100, nil, 0)
	secondary, secondaryRLDP := testArchiveDownloadPeerWithRLDP(t, "info-cancel-drain-secondary", 200, nil, 0)
	primaryStarted := make(chan GetArchiveSlice, 1)
	secondaryStarted := make(chan GetArchiveSlice, 1)
	primaryGate := make(chan struct{})
	secondaryGate := make(chan struct{})
	releasePrimary := sync.OnceFunc(func() { close(primaryGate) })
	releaseSecondary := sync.OnceFunc(func() { close(secondaryGate) })
	t.Cleanup(releasePrimary)
	t.Cleanup(releaseSecondary)
	primaryRLDP.asyncStarted = primaryStarted
	primaryRLDP.asyncDoQueryGate = primaryGate
	secondaryRLDP.asyncStarted = secondaryStarted
	secondaryRLDP.asyncDoQueryGate = secondaryGate
	addTestArchiveOnlyPeer(pool, primary)
	addTestArchiveOnlyPeer(pool, secondary)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := sub.findArchiveInfoHedged(ctx, session, pool, []*overlayPeer{primary, secondary}, 10, shard)
		result <- err
	}()

	waitArchiveSliceStarted(t, primaryStarted)
	waitArchiveSliceStarted(t, secondaryStarted)
	cancel()
	releasePrimary()
	waitArchivePeerLeases(t, pool, primary, 0)

	select {
	case <-result:
		t.Fatal("archive info hedge returned before the remaining attempt stopped")
	case <-time.After(50 * time.Millisecond):
	}

	releaseSecondary()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("archive info hedge error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("archive info hedge did not return after all attempts stopped")
	}
}

func TestArchiveComparativeHedgeReplacesStickyWithFasterPeer(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: node,
		spec: overlaySpec{ShortID: []byte{1}},
	})
	pool := testArchivePool(t, sub)
	session := node.BeginArchiveSession()
	defer session.Close()

	primaryPack := testArchivePackBytes("primary")
	fastPack := testArchivePackBytes("fast")
	primary := testArchiveDownloadPeer(t, "primary", 100, primaryPack, 120*time.Millisecond)
	fast := testArchiveDownloadPeer(t, "fast", 200, fastPack, 10*time.Millisecond)
	addTestArchiveOnlyPeer(pool, primary)
	addTestArchiveOnlyPeer(pool, fast)
	session.selectArchivePeer(shard, primary)

	downloaded, err := sub.downloadArchiveFromResolved(context.Background(), session, pool, resolvedArchive{
		MasterchainSeqno: 10,
		Shard:            shard,
		ArchiveID:        100,
		peer:             primary,
		peers:            []*overlayPeer{primary, fast},
	}, ArchiveDownloadOptions{})
	if err != nil {
		t.Fatalf("comparative hedge archive: %v", err)
	}
	if downloaded.Peer != fast.addr {
		t.Fatalf("comparative hedge winner = %q, want %q", downloaded.Peer, fast.addr)
	}
	if string(downloaded.Data) != string(fastPack) {
		t.Fatal("comparative hedge returned data from the wrong peer")
	}
	if selected := session.selectedArchivePeerID(shard); selected != fast.id {
		t.Fatalf("selected archive peer = %s, want %s", selected.String(), fast.id.String())
	}
}

func TestArchiveComparativeHedgeRacesSmallPackSeedProbe(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: node,
		spec: overlaySpec{ShortID: []byte{1}},
	})
	pool := testArchivePool(t, sub)
	session := node.BeginArchiveSession()
	defer session.Close()

	primaryPack := testArchivePackBytes("primary")
	fastPack := testArchivePackBytes("fast")
	primary := testArchiveDownloadPeer(t, "primary", 100, primaryPack, 120*time.Millisecond)
	fast := testArchiveDownloadPeer(t, "fast", 200, fastPack, 10*time.Millisecond)
	addTestArchiveOnlyPeer(pool, primary)
	addTestArchiveOnlyPeer(pool, fast)
	session.selectArchivePeer(shard, primary)

	resolved, err := sub.resolveArchive(context.Background(), session, pool, 10, shard, ArchiveDownloadOptions{})
	if err != nil {
		t.Fatalf("resolve archive with comparative hedge: %v", err)
	}
	if resolved.Peer != fast.addr {
		t.Fatalf("resolved archive peer = %q, want %q", resolved.Peer, fast.addr)
	}
	if !resolved.hasSeed || string(resolved.seedSlice) != string(fastPack) {
		t.Fatal("resolved archive did not use fast peer seed")
	}
	if selected := session.selectedArchivePeerID(shard); selected != fast.id {
		t.Fatalf("selected archive peer = %s, want %s", selected.String(), fast.id.String())
	}
}

func TestArchiveComparativeInfoHedgeAlsoRacesFullDownload(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: node,
		spec: overlaySpec{ShortID: []byte{1}},
	})
	pool := testArchivePool(t, sub)
	session := node.BeginArchiveSession()
	defer session.Close()

	primaryPack := make([]byte, archiveSliceProbeSize+128)
	fastPack := make([]byte, archiveSliceProbeSize+128)
	binary.LittleEndian.PutUint32(primaryPack, packfile.PackageMagic)
	binary.LittleEndian.PutUint32(fastPack, packfile.PackageMagic)
	copy(primaryPack[packfile.HeaderSize:], "primary")
	copy(fastPack[packfile.HeaderSize:], "fast")

	primary, primaryRLDP := testArchiveDownloadPeerWithRLDP(t, "primary-full-slow", 100, primaryPack, 0)
	fast, fastRLDP := testArchiveDownloadPeerWithRLDP(t, "fast-full-fast", 200, fastPack, 0)
	primaryRLDP.asyncDelays = map[int64]time.Duration{
		0:                     5 * time.Millisecond,
		archiveSliceProbeSize: 120 * time.Millisecond,
	}
	fastRLDP.asyncDelays = map[int64]time.Duration{
		0:                     20 * time.Millisecond,
		archiveSliceProbeSize: 5 * time.Millisecond,
	}
	addTestArchiveOnlyPeer(pool, primary)
	addTestArchiveOnlyPeer(pool, fast)
	session.selectArchivePeer(shard, primary)

	resolved, err := sub.resolveArchive(context.Background(), session, pool, 10, shard, ArchiveDownloadOptions{})
	if err != nil {
		t.Fatalf("resolve archive with comparative hedge: %v", err)
	}
	if resolved.Peer != primary.addr {
		t.Fatalf("probe hedge winner = %q, want %q", resolved.Peer, primary.addr)
	}

	downloaded, err := sub.downloadArchiveFromResolved(context.Background(), session, pool, resolved, ArchiveDownloadOptions{})
	if err != nil {
		t.Fatalf("download archive after comparative probe hedge: %v", err)
	}
	if downloaded.Peer != fast.addr {
		t.Fatalf("full archive hedge winner = %q, want %q", downloaded.Peer, fast.addr)
	}
	if !bytes.Equal(downloaded.Data, fastPack) {
		t.Fatal("full archive hedge returned data from the probe winner")
	}
}

func testArchiveDownloadPeer(t *testing.T, label string, archiveID int64, data []byte, delay time.Duration) *overlayPeer {
	t.Helper()

	peer, _ := testArchiveDownloadPeerWithRLDP(t, label, archiveID, data, delay)
	return peer
}

func waitArchiveSliceStarted(t *testing.T, started <-chan GetArchiveSlice) {
	t.Helper()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("archive slice did not start")
	}
}

func waitArchivePeerLeases(t *testing.T, pool *archivePeerPool, peer *overlayPeer, want int) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if got := pool.leaseSnapshot([]*overlayPeer{peer})[peer.id]; got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}

	got := pool.leaseSnapshot([]*overlayPeer{peer})[peer.id]
	t.Fatalf("archive peer leases = %d, want %d", got, want)
}

func testArchiveDownloadPeerWithRLDP(t *testing.T, label string, archiveID int64, data []byte, delay time.Duration) (*overlayPeer, *testArchiveRLDP) {
	t.Helper()

	peerOverlay, base := newTestOverlayWrapper()
	archiveRLDP := &testArchiveRLDP{
		adnl:        newTestOverlayADNL(),
		queryResult: ArchiveInfo{ID: archiveID},
		asyncResult: data,
		asyncDelay:  delay,
	}
	base.queryResponder = func(req tl.Serializable, result tl.Serializable) error {
		payload := testOverlayQueryPayload(req)
		if _, ok := payload.(GetArchiveInfo); !ok {
			t.Fatalf("unexpected archive info query payload %T", payload)
		}
		if out, ok := result.(*tl.Serializable); ok {
			*out = ArchiveInfo{ID: archiveID}
		}
		return nil
	}
	return &overlayPeer{
		id:          testPeerID(label),
		addr:        label,
		overlay:     peerOverlay,
		rldpOverlay: overlay.CreateExtendedRLDP(archiveRLDP).CreateOverlay([]byte{1}),
		announced:   &overlay.Node{Version: int32(time.Now().Unix())},
		alive:       true,
		release:     func() {},
	}, archiveRLDP
}

func testArchivePackBytes(label string) []byte {
	data := make([]byte, packfile.HeaderSize+len(label))
	binary.LittleEndian.PutUint32(data, packfile.PackageMagic)
	copy(data[packfile.HeaderSize:], label)
	return data
}

type testArchiveRLDP struct {
	mu sync.Mutex

	adnl        *testOverlayADNL
	queryResult tl.Serializable
	queryCalls  int

	asyncErr           error
	asyncErrors        map[int64]error
	asyncResult        []byte
	asyncDelay         time.Duration
	asyncDelays        map[int64]time.Duration
	asyncWaitForCancel map[int64]bool
	asyncStarted       chan<- GetArchiveSlice
	asyncDoQueryGate   <-chan struct{}
	asyncCanceled      chan<- int64
	asyncQueries       []testArchiveAsyncQuery
	asyncCompleted     []int64
	asyncActive        int
	asyncMaxActive     int
}

type testArchiveAsyncQuery struct {
	request   tl.Serializable
	maxAnswer uint64
	timeout   time.Duration
}

type testArchiveRLDPSnapshot struct {
	queryCalls     int
	asyncQueries   []testArchiveAsyncQuery
	asyncCompleted []int64
	asyncActive    int
	asyncMaxActive int
}

func (r *testArchiveRLDP) GetADNL() rldp.ADNL {
	return r.adnl
}

func (r *testArchiveRLDP) GetRateInfo() (int64, int64) {
	return 0, 0
}

func (r *testArchiveRLDP) Stats() rldp.Stats {
	return rldp.Stats{}
}

func (r *testArchiveRLDP) Close() {}

func (r *testArchiveRLDP) DoQuery(_ context.Context, _ uint64, _ tl.Serializable, result tl.Serializable) error {
	r.mu.Lock()
	r.queryCalls++
	queryResult := r.queryResult
	r.mu.Unlock()

	if out, ok := result.(*tl.Serializable); ok {
		*out = queryResult
	}
	return nil
}

func (r *testArchiveRLDP) DoQueryAsync(ctx context.Context, maxAnswer uint64, _ []byte, req tl.Serializable, result chan<- rldp.AsyncQueryResult) error {
	var timeout time.Duration
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
	}

	payload := testOverlayQueryPayload(req)
	query, isArchiveSlice := payload.(GetArchiveSlice)
	stateQuery, isPersistentStateSlice := payload.(DownloadPersistentStateSliceV2)
	isSlice := isArchiveSlice || isPersistentStateSlice
	offset := query.Offset
	if isPersistentStateSlice {
		offset = stateQuery.Offset
	}
	r.mu.Lock()
	r.asyncQueries = append(r.asyncQueries, testArchiveAsyncQuery{
		request:   req,
		maxAnswer: maxAnswer,
		timeout:   timeout,
	})
	asyncErr := r.asyncErr
	data := append([]byte(nil), r.asyncResult...)
	delay := r.asyncDelay
	waitForCancel := false
	if isSlice {
		if configured, ok := r.asyncErrors[offset]; ok {
			asyncErr = configured
		}
		if configured, ok := r.asyncDelays[offset]; ok {
			delay = configured
		}
		waitForCancel = r.asyncWaitForCancel[offset]
	}
	if asyncErr == nil {
		r.asyncActive++
		if r.asyncActive > r.asyncMaxActive {
			r.asyncMaxActive = r.asyncActive
		}
	}
	started := r.asyncStarted
	doQueryGate := r.asyncDoQueryGate
	r.mu.Unlock()

	if asyncErr != nil {
		return asyncErr
	}
	if isArchiveSlice && started != nil {
		select {
		case started <- query:
		default:
		}
	}
	if doQueryGate != nil {
		<-doQueryGate
	}
	if isArchiveSlice {
		start := int(query.Offset)
		if start > len(data) {
			start = len(data)
		}
		end := start + int(query.MaxSize)
		if end > len(data) {
			end = len(data)
		}
		data = data[start:end]
	}

	go func() {
		wasCanceled := false
		defer func() {
			r.mu.Lock()
			r.asyncActive--
			canceled := r.asyncCanceled
			r.mu.Unlock()

			if wasCanceled && isArchiveSlice && canceled != nil {
				select {
				case canceled <- query.Offset:
				default:
				}
			}
		}()

		if waitForCancel {
			<-ctx.Done()
			wasCanceled = true
			return
		}
		if delay > 0 {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
				wasCanceled = true
				return
			}
		}

		r.mu.Lock()
		r.asyncCompleted = append(r.asyncCompleted, offset)
		r.mu.Unlock()
		select {
		case result <- rldp.AsyncQueryResult{ResultBytes: data}:
		case <-ctx.Done():
			wasCanceled = true
		}
	}()
	return nil
}

func (r *testArchiveRLDP) snapshot() testArchiveRLDPSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	return testArchiveRLDPSnapshot{
		queryCalls:     r.queryCalls,
		asyncQueries:   append([]testArchiveAsyncQuery(nil), r.asyncQueries...),
		asyncCompleted: append([]int64(nil), r.asyncCompleted...),
		asyncActive:    r.asyncActive,
		asyncMaxActive: r.asyncMaxActive,
	}
}

func (r *testArchiveRLDP) SetOnQuery(func([]byte, *rldp.Query) error) {}

func (r *testArchiveRLDP) SetOnMessage(func([]byte, []byte) error) {}

func (r *testArchiveRLDP) SetOnDisconnect(func()) {}

func (r *testArchiveRLDP) SendAnswer(context.Context, uint64, uint32, []byte, []byte, tl.Serializable) error {
	return nil
}
