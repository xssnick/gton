package p2p

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"os"
	"testing"
	"time"

	"github.com/xssnick/gton/internal/logutil"
	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/gton/service/p2p/internal/peerroute"
	"github.com/xssnick/gton/service/storage/pebblestore"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func discardLogger() zerolog.Logger {
	return zerolog.Nop()
}

func stdoutLogger(level zerolog.Level) zerolog.Logger {
	return logutil.New(os.Stdout, level, false)
}

func testPeerID(label string) PeerID {
	return PeerID(sha256.Sum256([]byte(label)))
}

func testShardDescriptionData(marker uint8) []byte {
	return cell.BeginCell().MustStoreUInt(uint64(marker), 8).EndCell().ToBOC()
}

func newTestPeerRoute(address string) *peerroute.Route {
	return peerroute.NewRoute(address, peerRouteRetryPolicy)
}

func newTestPeerRouteWithRetry(address string, minDelay, maxDelay time.Duration) *peerroute.Route {
	return peerroute.NewRoute(address, peerroute.RetryPolicy{
		MinDelay: minDelay,
		MaxDelay: maxDelay,
	})
}

func newTestNode(tb testing.TB) *Node {
	tb.Helper()

	logger := discardLogger()
	node, err := New(Options{
		Logger:        &logger,
		PeerStorage:   newTestPeerStore(),
		StateFilesDir: tb.TempDir(),
	})
	if err != nil {
		tb.Fatalf("create test node: %v", err)
	}
	if err = node.BindRuntimeCallbacks(RuntimeCallbacks{
		SignatureVerifier: testAcceptBroadcastSignatureVerifier{},
	}); err != nil {
		tb.Fatalf("bind test runtime callbacks: %v", err)
	}
	tb.Cleanup(node.closeSubscriptions)
	return node
}

func testOverlaySubscription(sub *overlaySubscription) *overlaySubscription {
	if sub.node == nil {
		sub.node = &Node{}
	}
	if sub.quicEnvelope == nil && len(sub.spec.ShortID) == PeerIDSize {
		envelope, err := newQUICOverlayEnvelope(sub.spec.ShortID, nil)
		if err != nil {
			panic(err)
		}
		sub.quicEnvelope = envelope
	}
	for _, peer := range sub.peers {
		if peer.route == nil {
			peer.route = newTestPeerRoute("")
		}
		if peer.overlay == nil {
			peer.overlay = &overlay.ADNLOverlayWrapper{}
		}
		if peer.broadcastPeer == nil {
			peer.broadcastPeer = peer.overlay
		}
		if peer.release == nil {
			peer.release = func() {}
		}
	}
	if sub.node.runCtx == nil {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		sub.node.runCtx = ctx
	}
	if sub.node.pool == nil {
		sub.node.pool = &peerPool{peers: map[PeerID]*pooledPeer{}}
	}
	if sub.peerNotify == nil {
		sub.peerNotify = make(chan struct{}, 1)
	}
	if sub.broadcastReceiver == nil {
		if len(sub.spec.ShortID) != PeerIDSize {
			overlayID := testPeerID("test-subscription-overlay:" + string(sub.spec.ShortID))
			sub.spec.ShortID = overlayID[:]
		}
		receiver, err := overlay.NewBroadcastReceiver(
			sub.spec.ShortID,
			maxOverlayPayloadSize,
			sub.spec.Kind != overlayKindCustomFixed,
			false,
		)
		if err != nil {
			panic(err)
		}
		sub.broadcastReceiver = receiver
	}

	return sub
}

func mustGetOrCreateSubscription(tb testing.TB, node *Node, spec overlaySpec) *overlaySubscription {
	tb.Helper()

	sub, err := node.getOrCreateSubscription(spec)
	if err != nil {
		tb.Fatalf("create test overlay subscription: %v", err)
	}
	return sub
}

func ensureTestBroadcastReceiver(tb testing.TB, sub *overlaySubscription) {
	tb.Helper()
	if sub.broadcastReceiver != nil {
		return
	}

	if len(sub.spec.ShortID) != PeerIDSize {
		overlayID := testPeerID("test-subscription-overlay:" + string(sub.spec.ShortID))
		sub.spec.ShortID = overlayID[:]
	}
	receiver, err := overlay.NewBroadcastReceiver(
		sub.spec.ShortID,
		maxOverlayPayloadSize,
		sub.spec.Kind != overlayKindCustomFixed,
		false,
	)
	if err != nil {
		tb.Fatalf("create test broadcast receiver: %v", err)
	}
	sub.broadcastReceiver = receiver
	tb.Cleanup(receiver.Close)
}

func (s *overlaySubscription) inactiveExpiresAt() (time.Time, bool) {
	s.mx.Lock()
	defer s.mx.Unlock()

	if !s.inactive || s.inactiveDeleteAt.IsZero() {
		return time.Time{}, false
	}
	return s.inactiveDeleteAt, true
}

func (p *peerPool) snapshot() []*pooledPeer {
	p.mx.RLock()
	defer p.mx.RUnlock()

	list := make([]*pooledPeer, 0, len(p.peers))
	for _, peer := range p.peers {
		list = append(list, peer)
	}
	return list
}

func (p *archivePeerPool) peerPerformance(shard archive.ShardID, peerID PeerID) (archivePeerPerformance, bool) {
	p.mx.Lock()
	defer p.mx.Unlock()

	entry := p.peers[peerID]
	if entry == nil {
		return archivePeerPerformance{}, false
	}
	return archivePeerPerformanceFromState(p.shards[archivePeerPoolKey(shard)], peerID), true
}

func (p *archivePeerPool) rememberRejected(peerID PeerID, ttl time.Duration) {
	p.mx.Lock()
	if !p.closed {
		p.rememberRejectedLocked(peerID, time.Now(), ttl)
	}
	p.mx.Unlock()
}

func (p *archivePeerPool) size() int {
	p.pruneClosedPeers()

	p.mx.Lock()
	defer p.mx.Unlock()
	return len(p.peers)
}

func (p *archivePeerPool) usableSize(now time.Time) int {
	p.mx.Lock()
	defer p.mx.Unlock()

	if p.closed {
		return 0
	}

	count := 0
	for _, entry := range p.peers {
		if archivePeerUsable(entry, now) {
			count++
		}
	}
	return count
}

func (p *archivePeerPool) provenUsableSize(now time.Time) int {
	p.mx.Lock()
	defer p.mx.Unlock()

	return p.provenUsableSizeLocked(now)
}

func (p *archivePeerPool) probeSnapshot() (archivePeerProbe, bool) {
	probes := p.probeSnapshots(1)
	if len(probes) == 0 {
		return archivePeerProbe{}, false
	}
	return probes[0], true
}

func (a *ArchiveSession) selectArchivePeer(shard archive.ShardID, peer *overlayPeer) {
	a.selectArchivePeerFromPool(shard, peer, nil)
}

func (s *overlaySubscription) aliveNeighbourPeers(requiredVersionMajor, requiredVersionMinor int32) []*overlayPeer {
	candidates := s.neighbourPeerSnapshots()
	if len(candidates) == 0 {
		return nil
	}

	now := time.Now()
	alive := candidates[:0]
	for _, peer := range candidates {
		if !peer.isAliveKnownOverlayPeer(now) {
			continue
		}
		if !peerEligible(peer.statsSnapshot(), requiredVersionMajor, requiredVersionMinor) {
			continue
		}
		alive = append(alive, peer)
	}

	return s.orderPreferredPeers(alive, requiredVersionMajor, requiredVersionMinor)
}

type testAcceptBroadcastSignatureVerifier struct{}

func (testAcceptBroadcastSignatureVerifier) CheckBlockBroadcastSignatures(context.Context, BlockBroadcastSignatureCheck) error {
	return nil
}

func (testAcceptBroadcastSignatureVerifier) CheckBlockFinalitySignatures(_ context.Context, req BlockFinalitySignatureCheck) (*BlockFinalitySignatureCheckResult, error) {
	return &BlockFinalitySignatureCheckResult{
		SignaturesVerifiedKey: []byte("test-finality"),
	}, nil
}

func (testAcceptBroadcastSignatureVerifier) ValidateShardDescriptionBroadcast(_ context.Context, req ShardDescriptionSignatureCheck) (*ShardBlockDescription, error) {
	return &ShardBlockDescription{
		Block:         req.Block,
		CatchainSeqno: uint32(req.CatchainSeqno),
	}, nil
}

type testRejectBroadcastSignatureVerifier struct {
	err error
}

func (v testRejectBroadcastSignatureVerifier) CheckBlockBroadcastSignatures(context.Context, BlockBroadcastSignatureCheck) error {
	return v.err
}

func (v testRejectBroadcastSignatureVerifier) CheckBlockFinalitySignatures(context.Context, BlockFinalitySignatureCheck) (*BlockFinalitySignatureCheckResult, error) {
	return nil, v.err
}

func (v testRejectBroadcastSignatureVerifier) ValidateShardDescriptionBroadcast(context.Context, ShardDescriptionSignatureCheck) (*ShardBlockDescription, error) {
	return nil, v.err
}

type testBroadcastAdmission bool

func (a testBroadcastAdmission) CanAcceptBroadcast(BroadcastAdmissionRequest) bool {
	return bool(a)
}

type testExternalMessageAdmission struct {
	events []ExternalMessageEvent
	err    error
}

func (a *testExternalMessageAdmission) AcceptExternalMessage(_ context.Context, event ExternalMessageEvent) error {
	a.events = append(a.events, event)
	return a.err
}

func (a *testExternalMessageAdmission) AcceptCheckedExternalMessage(_ context.Context, event ExternalMessageEvent) error {
	a.events = append(a.events, event)
	return a.err
}

type testBlockReceivedObserver struct {
	events []BlockReceivedEvent
}

func (o *testBlockReceivedObserver) ObserveBlockReceived(_ context.Context, event BlockReceivedEvent) {
	o.events = append(o.events, event)
}

func newTestPebbleStore(tb testing.TB) *pebblestore.Store {
	tb.Helper()

	store, err := pebblestore.Open(pebblestore.Options{Dir: tb.TempDir()})
	if err != nil {
		tb.Fatalf("open pebble store: %v", err)
	}
	tb.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

func testBlockID(workchain int32, shard int64, seqno uint32) ton.BlockIDExt {
	root := testBlockHash(0x01, workchain, shard, seqno)
	file := testBlockHash(0x02, workchain, shard, seqno)
	return ton.BlockIDExt{
		Workchain: workchain,
		Shard:     shard,
		SeqNo:     seqno,
		RootHash:  root,
		FileHash:  file,
	}
}

func testBlockHash(kind byte, workchain int32, shard int64, seqno uint32) []byte {
	var data [17]byte
	data[0] = kind
	binary.BigEndian.PutUint32(data[1:5], uint32(workchain))
	binary.BigEndian.PutUint64(data[5:13], uint64(shard))
	binary.BigEndian.PutUint32(data[13:17], seqno)
	hash := sha256.Sum256(data[:])
	return append([]byte(nil), hash[:]...)
}
