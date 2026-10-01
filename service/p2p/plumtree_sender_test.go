package p2p

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	adnladdr "github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/overlay"
)

type failingPlumtreeRouteDHT struct {
	dhtBackend

	calls atomic.Int32
}

var plumtreeWireBatchBenchmarkSink plumtreeWireBatch

func (d *failingPlumtreeRouteDHT) FindAddresses(
	context.Context,
	[]byte,
) (*adnladdr.List, ed25519.PublicKey, error) {
	d.calls.Add(1)
	return nil, nil, errors.New("route unavailable")
}

func TestPlumtreeOutboundQueuePreservesPerPeerOrder(t *testing.T) {
	firstPeer := plumtreeEngineTestPeer(1)
	secondPeer := plumtreeEngineTestPeer(2)
	firstWire := []byte{1}
	secondWire := []byte{2}
	thirdWire := []byte{3}

	queue := newPlumtreeOutboundQueue()
	if dropped := queue.add(plumtreeWireBatch{
		sends: []plumtreeWireSend{
			{to: firstPeer, wire: firstWire},
			{to: secondPeer, wire: secondWire},
			{to: firstPeer, wire: thirdWire},
		},
	}); dropped != 0 {
		t.Fatalf("dropped messages = %d, want 0", dropped)
	}

	peer, sends, ok := queue.take()
	if !ok || peer != firstPeer {
		t.Fatalf("first ready peer = %v, %v, want %v, true", peer, ok, firstPeer)
	}
	if len(sends) != 2 ||
		!bytes.Equal(sends[0], firstWire) ||
		!bytes.Equal(sends[1], thirdWire) {
		t.Fatalf("first peer sends = %+v, want wires 1 then 3", sends)
	}

	peer, sends, ok = queue.take()
	if !ok || peer != secondPeer || len(sends) != 1 ||
		!bytes.Equal(sends[0], secondWire) {
		t.Fatalf("second ready batch = %v, %+v, %v", peer, sends, ok)
	}

	fourthWire := []byte{4}
	if dropped := queue.add(plumtreeWireBatch{
		sends: []plumtreeWireSend{{to: firstPeer, wire: fourthWire}},
	}); dropped != 0 {
		t.Fatalf("dropped message queued behind active peer = %d, want 0", dropped)
	}
	if _, _, ready := queue.take(); ready {
		t.Fatal("active peer became ready before its current batch finished")
	}

	queue.finish(firstPeer)
	peer, sends, ok = queue.take()
	if !ok || peer != firstPeer || len(sends) != 1 ||
		!bytes.Equal(sends[0], fourthWire) {
		t.Fatalf("rescheduled batch = %v, %+v, %v", peer, sends, ok)
	}

	queue.finish(secondPeer)
	queue.finish(firstPeer)
	if len(queue.peers) != 0 {
		t.Fatalf("peer queue retained %d idle peers", len(queue.peers))
	}
	if queue.size != 0 {
		t.Fatalf("peer queue retained size %d, want 0", queue.size)
	}
}

func TestPlumtreeOutboundQueueBoundsStalledPeer(t *testing.T) {
	peer := plumtreeEngineTestPeer(3)
	sends := make([]plumtreeWireSend, plumtreeOutboundPerPeerLimit+1)
	for i := range sends {
		sends[i] = plumtreeWireSend{
			to:   peer,
			wire: []byte{byte(i)},
		}
	}

	queue := newPlumtreeOutboundQueue()
	if dropped := queue.add(plumtreeWireBatch{sends: sends}); dropped != 1 {
		t.Fatalf("dropped messages = %d, want 1", dropped)
	}

	readyPeer, queued, ok := queue.take()
	if !ok || readyPeer != peer || len(queued) != plumtreeOutboundPerPeerLimit {
		t.Fatalf(
			"queued batch = %v, %d, %v; want %v, %d, true",
			readyPeer,
			len(queued),
			ok,
			peer,
			plumtreeOutboundPerPeerLimit,
		)
	}
	if dropped := queue.add(plumtreeWireBatch{
		sends: []plumtreeWireSend{{
			to:   peer,
			wire: []byte{0xff},
		}},
	}); dropped != 1 {
		t.Fatalf("dropped behind full in-flight batch = %d, want 1", dropped)
	}

	queue.finish(peer)
	if dropped := queue.add(plumtreeWireBatch{
		sends: []plumtreeWireSend{{
			to:   peer,
			wire: []byte{0xff},
		}},
	}); dropped != 0 {
		t.Fatalf("message dropped after in-flight batch finished = %d, want 0", dropped)
	}
}

func TestPlumtreeOutboundQueueBoundsPeerChurn(t *testing.T) {
	sends := make([]plumtreeWireSend, plumtreeOutboundQueueLimit+1)
	for index := range sends {
		var peer PeerID
		peer[0] = byte(index)
		peer[1] = byte(index >> 8)
		sends[index] = plumtreeWireSend{
			to:   peer,
			wire: []byte{byte(index)},
		}
	}

	queue := newPlumtreeOutboundQueue()
	if dropped := queue.add(plumtreeWireBatch{sends: sends}); dropped != 1 {
		t.Fatalf("dropped messages = %d, want 1", dropped)
	}
	if queue.size != plumtreeOutboundQueueLimit {
		t.Fatalf(
			"queued messages = %d, want %d",
			queue.size,
			plumtreeOutboundQueueLimit,
		)
	}
}

func TestPlumtreeOutboundWorkerReusesAndStops(t *testing.T) {
	node := newTestNode(t)
	runtime := &plumtreeRuntime{
		sub: &overlaySubscription{
			node:  node,
			log:   discardLogger(),
			peers: map[PeerID]*overlayPeer{},
		},
	}
	jobs := make(chan plumtreeOutboundJob)
	done := make(chan PeerID, 2)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	exited := make(chan struct{})
	go func() {
		runtime.runOutboundWorker(ctx, jobs, done)
		close(exited)
	}()

	for index := byte(1); index <= 2; index++ {
		peer := plumtreeEngineTestPeer(index)
		jobs <- plumtreeOutboundJob{
			peer:  peer,
			wires: [][]byte{{index}},
		}
		if completed := <-done; completed != peer {
			t.Fatalf("completed peer = %v, want %v", completed, peer)
		}
	}

	cancel()
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("outbound worker did not stop after parent cancellation")
	}
}

func TestPlumtreeOutboundSendWindowExposesDeadline(t *testing.T) {
	ctx, cancel := newPlumtreeOutboundSendWindow(t.Context())
	defer cancel()

	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("outbound send window has no deadline")
	}
	remaining := time.Until(deadline)
	if remaining <= 0 || remaining > plumtreeOutboundTimeout {
		t.Fatalf("outbound send window remaining time = %s, want (0, %s]", remaining, plumtreeOutboundTimeout)
	}
}

func BenchmarkPlumtreeOutboundQueueRoundTrip(b *testing.B) {
	peer := plumtreeEngineTestPeer(1)
	batch := plumtreeWireBatch{
		sends: []plumtreeWireSend{{
			to:   peer,
			wire: []byte{1},
		}},
	}
	queue := newPlumtreeOutboundQueue()

	b.ReportAllocs()
	for b.Loop() {
		if dropped := queue.add(batch); dropped != 0 {
			b.Fatalf("dropped messages = %d, want 0", dropped)
		}

		readyPeer, sends, ok := queue.take()
		if !ok || readyPeer != peer || len(sends) != 1 {
			b.Fatalf("unexpected ready batch")
		}
		queue.finish(peer)
	}
}

func TestPlumtreeOutboundSendDefersFailedRosterRoute(t *testing.T) {
	dht := &failingPlumtreeRouteDHT{}
	node := newTestNode(t)
	node.dht = dht

	remoteKey := quicOutboundTestKey(t)
	remotePublicKey := remoteKey.Public().(ed25519.PublicKey)
	peerID := peerIDForQUICOutboundTest(t, remotePublicKey)
	route := newTestPeerRouteWithRetry("", time.Millisecond, time.Millisecond)
	firstPeer := &overlayPeer{
		node:  node,
		id:    peerID,
		pub:   remotePublicKey,
		route: route,
	}
	secondPeer := &overlayPeer{
		node:  node,
		id:    peerID,
		pub:   remotePublicKey,
		route: route,
	}
	firstRuntime := &plumtreeRuntime{sub: &overlaySubscription{
		node:  node,
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{peerID: firstPeer},
	}}
	secondRuntime := &plumtreeRuntime{sub: &overlaySubscription{
		node:  node,
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{peerID: secondPeer},
	}}
	wires := [][]byte{{1}}

	firstRuntime.sendOutboundBatch(context.Background(), peerID, wires)
	secondRuntime.sendOutboundBatch(context.Background(), peerID, wires)

	if got := dht.calls.Load(); got != 1 {
		t.Fatalf("DHT lookups during retry delay = %d, want 1", got)
	}
	if route.QUICReady(time.Now()) {
		t.Fatal("failed QUIC route remained eligible for Plumtree fanout")
	}

	deadline := time.Now().Add(time.Second)
	for !route.QUICDialPermitted(time.Now()) {
		if time.Now().After(deadline) {
			t.Fatal("QUIC retry delay did not expire")
		}
		time.Sleep(time.Millisecond)
	}
	secondRuntime.sendOutboundBatch(context.Background(), peerID, wires)
	if got := dht.calls.Load(); got != 1 {
		t.Fatalf("DHT lookups after retry delay = %d, want cached failure", got)
	}
}

func TestPlumtreeFailedAuthenticatedPathSharesCooldown(t *testing.T) {
	dht := &failingPlumtreeRouteDHT{}
	node := newTestNode(t)
	node.dht = dht

	remoteKey := quicOutboundTestKey(t)
	remotePublicKey := remoteKey.Public().(ed25519.PublicKey)
	peerID := peerIDForQUICOutboundTest(t, remotePublicKey)
	route := newTestPeerRoute("")
	node.quicPeers[peerID] = &authenticatedQUICPeer{
		node:      node,
		route:     route,
		id:        peerID,
		publicKey: remotePublicKey,
	}
	sub := &overlaySubscription{
		node:  node,
		peers: map[PeerID]*overlayPeer{},
	}
	path, err := sub.quicPeerPath(peerID)
	if err != nil {
		t.Fatalf("resolve authenticated Plumtree path: %v", err)
	}

	const attempts = 16
	start := make(chan struct{})
	results := make(chan error, attempts)
	var ready sync.WaitGroup
	ready.Add(attempts)
	for range attempts {
		go func() {
			ready.Done()
			<-start
			_, dialErr := path.dialGated(context.Background())
			results <- dialErr
		}()
	}
	ready.Wait()
	close(start)
	for range attempts {
		if err = <-results; err == nil {
			t.Fatal("authenticated QUIC dial unexpectedly succeeded")
		}
	}

	if _, err = path.dialGated(context.Background()); !errors.Is(err, errQUICDialDeferred) {
		t.Fatalf("authenticated QUIC retry error = %v, want deferred", err)
	}
	if got := dht.calls.Load(); got != 1 {
		t.Fatalf("DHT lookups during authenticated cooldown = %d, want 1", got)
	}
}

func TestPlumtreeRepairSkipsCooledQUICPath(t *testing.T) {
	dht := &failingPlumtreeRouteDHT{}
	node := newTestNode(t)
	node.dht = dht

	peerID := testPeerID("cooled-plumtree-repair-peer")
	route := newTestPeerRoute("")
	route.DeferQUICDial(time.Now())
	sub := &overlaySubscription{
		node: node,
		peers: map[PeerID]*overlayPeer{
			peerID: {
				node:  node,
				id:    peerID,
				route: route,
			},
		},
	}
	const repairID = 1
	engine := &plumtreeEngine{
		repairs: map[uint64]plumtreeRepairAttempt{
			repairID: {},
		},
	}
	runtime := &plumtreeRuntime{
		sub:    sub,
		engine: engine,
	}

	if err := runtime.runRepair(context.Background(), plumtreeRepairAction{
		ID: repairID,
		To: peerID,
	}); err != nil {
		t.Fatalf("skip cooled Plumtree repair: %v", err)
	}
	if len(engine.repairs) != 0 {
		t.Fatal("cooled Plumtree repair attempt remained active")
	}
	if got := dht.calls.Load(); got != 0 {
		t.Fatalf("DHT lookups for cooled Plumtree repair = %d, want 0", got)
	}
}

func TestPlumtreeOutboundBatchReusesIdenticalControlWire(t *testing.T) {
	logger := zerolog.Nop()
	overlayID := bytes.Repeat([]byte{0x41}, PeerIDSize)
	envelope, err := newQUICOverlayEnvelope(overlayID, nil)
	if err != nil {
		t.Fatalf("create envelope: %v", err)
	}
	runtime := &plumtreeRuntime{
		sub: &overlaySubscription{
			spec:         overlaySpec{ShortID: overlayID},
			log:          logger,
			quicEnvelope: envelope,
		},
	}

	control := plumtreeOutboundControl{
		key: plumtreePartKey{
			broadcastID: plumtreeEngineTestID(4),
			partIndex:   6,
			treeIndex:   7,
		},
		timestamp: 5,
	}
	actions := []plumtreeOutboundAction{
		{
			Kind:    plumtreeOutboundUseful,
			To:      plumtreeEngineTestPeer(4),
			Control: control,
		},
		{
			Kind:    plumtreeOutboundUseful,
			To:      plumtreeEngineTestPeer(5),
			Control: control,
		},
	}
	batch := runtime.prepareOutboundBatch(actions)

	if len(batch.sends) != 2 {
		t.Fatalf("wire sends = %d, want 2", len(batch.sends))
	}
	if len(batch.sends[0].wire) == 0 ||
		&batch.sends[0].wire[0] != &batch.sends[1].wire[0] {
		t.Fatal("identical controls were serialized into separate buffers")
	}

	want, err := envelope.Message(plumtreeOutboundMessage(&actions[0]))
	if err != nil {
		t.Fatalf("serialize generic control wire: %v", err)
	}
	if !bytes.Equal(batch.sends[0].wire, want) {
		t.Fatal("optimized control serialization changed the wire")
	}
}

func TestPlumtreeOutboundIHaveUsesRetainedPartMetadata(t *testing.T) {
	overlayID := bytes.Repeat([]byte{0x41}, PeerIDSize)
	envelope, err := newQUICOverlayEnvelope(overlayID, nil)
	if err != nil {
		t.Fatalf("create envelope: %v", err)
	}
	runtime := &plumtreeRuntime{
		sub: &overlaySubscription{
			log:          zerolog.Nop(),
			quicEnvelope: envelope,
		},
	}

	broadcastID := plumtreeEngineTestID(4)
	dataHash := plumtreeEngineTestID(5)
	part := plumtreePartState{
		partIndex:   6,
		treeIndex:   7,
		timestamp:   8,
		dataSize:    9,
		dataHash:    dataHash,
		sourceKey:   plumtreeEngineTestSource(0x42),
		certificate: plumtreeCertificate{kind: plumtreeCertificateEmpty},
		signature:   bytes.Repeat([]byte{0x43}, ed25519.SignatureSize),
	}
	control := plumtreeIHaveControl(time.Unix(1_000_000, 0), broadcastID, &part)
	actions := []plumtreeOutboundAction{
		{
			Kind:    plumtreeOutboundIHave,
			To:      plumtreeEngineTestPeer(1),
			Control: control,
		},
		{
			Kind:    plumtreeOutboundIHave,
			To:      plumtreeEngineTestPeer(2),
			Control: control,
		},
	}

	batch := runtime.prepareOutboundBatch(actions)
	if len(batch.sends) != 2 ||
		&batch.sends[0].wire[0] != &batch.sends[1].wire[0] {
		t.Fatal("identical IHAVE controls did not share one wire buffer")
	}

	_, body, err := parseQUICMessageEnvelope(batch.sends[0].wire)
	if err != nil {
		t.Fatalf("parse message envelope: %v", err)
	}
	value, err := parseOneQUICOverlayObject(body)
	if err != nil {
		t.Fatalf("parse IHAVE body: %v", err)
	}
	got, ok := value.(BroadcastPlumtreeIHave)
	if !ok {
		t.Fatalf("message type = %T, want BroadcastPlumtreeIHave", value)
	}
	want := BroadcastPlumtreeIHave{
		BroadcastID:      broadcastID[:],
		Timestamp:        plumtreeUnixSeconds(time.Unix(1_000_000, 0)),
		PartIndex:        part.partIndex,
		TreeIndex:        part.treeIndex,
		Source:           part.sourceKey,
		Certificate:      overlay.CertificateEmpty{},
		PayloadTimestamp: part.timestamp,
		DataSize:         part.dataSize,
		DataHash:         part.dataHash[:],
		Signature:        part.signature,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("IHAVE message = %+v, want %+v", got, want)
	}
}

func BenchmarkPlumtreePrepareOutboundIHave(b *testing.B) {
	logger := zerolog.Nop()
	overlayID := bytes.Repeat([]byte{0x41}, PeerIDSize)
	envelope, err := newQUICOverlayEnvelope(overlayID, nil)
	if err != nil {
		b.Fatalf("create envelope: %v", err)
	}
	runtime := &plumtreeRuntime{
		sub: &overlaySubscription{
			spec:         overlaySpec{ShortID: overlayID},
			log:          logger,
			quicEnvelope: envelope,
		},
	}

	part := plumtreePartState{
		partIndex:   6,
		treeIndex:   7,
		timestamp:   8,
		dataSize:    9,
		sourceKey:   plumtreeEngineTestSource(0x42),
		certificate: plumtreeCertificate{kind: plumtreeCertificateEmpty},
		signature:   bytes.Repeat([]byte{0x43}, ed25519.SignatureSize),
	}
	control := plumtreeIHaveControl(
		time.Unix(1_000_000, 0),
		plumtreeEngineTestID(4),
		&part,
	)
	actions := make([]plumtreeOutboundAction, plumtreeActiveNeighbourLimit)
	for index := range actions {
		actions[index] = plumtreeOutboundAction{
			Kind:    plumtreeOutboundIHave,
			To:      plumtreeEngineTestPeer(byte(index + 1)),
			Control: control,
		}
	}

	b.ReportAllocs()
	for b.Loop() {
		plumtreeWireBatchBenchmarkSink = runtime.prepareOutboundBatch(actions)
	}
}
