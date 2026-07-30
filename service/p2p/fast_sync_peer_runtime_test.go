package p2p

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

func TestFastSyncPeerRuntimeLocalNode(t *testing.T) {
	now := time.Unix(1_000, 0)
	overlayID := peerRuntimeTestOverlayID(0x31)

	t.Run("permanent", func(t *testing.T) {
		privateKey := peerRuntimeTestKey(0x11)
		publicKey := privateKey.Public().(ed25519.PublicKey)
		localID := peerRuntimeTestPeerID(publicKey)
		roster := peerRuntimeTestRoster(
			publicKey,
			FastSyncValidator{ADNLID: localID},
		)
		membership := newFastSyncMembership(roster, 0x01)

		runtime, err := newFastSyncPeerRuntime(
			privateKey,
			overlayID,
			0x80,
			overlay.EmptyMemberCertificate{},
			membership,
			now,
		)
		if err != nil {
			t.Fatalf("create runtime: %v", err)
		}
		privateKey[0] ^= 0xff

		node, err := runtime.LocalNode(now)
		if err != nil {
			t.Fatalf("local node: %v", err)
		}
		if runtime.LocalID() != localID {
			t.Fatalf("local id = %v, want %v", runtime.LocalID(), localID)
		}
		if node.Flags != 0x80 {
			t.Fatalf("node flags = %#x, want %#x", node.Flags, uint32(0x80))
		}
		if node.Version != int32(now.Unix()) {
			t.Fatalf("node version = %d, want %d", node.Version, now.Unix())
		}
		if !bytes.Equal(node.Overlay, overlayID[:]) {
			t.Fatalf("node overlay = %x, want %x", node.Overlay, overlayID)
		}
		if _, ok := node.ID.(keys.PublicKeyED25519); !ok {
			t.Fatalf("node id type = %T, want value ED25519 key", node.ID)
		}
		if _, ok := node.Certificate.(overlay.EmptyMemberCertificate); !ok {
			t.Fatalf(
				"node certificate type = %T, want value empty certificate",
				node.Certificate,
			)
		}
		if err = node.CheckSignature(); err != nil {
			t.Fatalf("check local node signature: %v", err)
		}
	})

	t.Run("certified flags are independent", func(t *testing.T) {
		issuerKey := peerRuntimeTestKey(0x21)
		issuerPublic := issuerKey.Public().(ed25519.PublicKey)
		issuerID := peerRuntimeTestPeerID(issuerPublic)
		localKey := peerRuntimeTestKey(0x22)
		localID := peerRuntimeTestPeerID(
			localKey.Public().(ed25519.PublicKey),
		)
		roster := peerRuntimeTestRoster(
			issuerPublic,
			FastSyncValidator{ADNLID: issuerID},
		)
		membership := newFastSyncMembership(roster, 0x01)
		certificate := peerRuntimeTestCertificate(
			t,
			issuerKey,
			localID,
			0xa5,
			2,
			2_000,
		)

		runtime, err := newFastSyncPeerRuntime(
			localKey,
			overlayID,
			0x18,
			certificate,
			membership,
			now,
		)
		if err != nil {
			t.Fatalf("create certified runtime: %v", err)
		}

		node, err := runtime.LocalNode(now)
		if err != nil {
			t.Fatalf("certified local node: %v", err)
		}
		member, ok := node.Certificate.(overlay.MemberCertificate)
		if !ok {
			t.Fatalf(
				"node certificate type = %T, want value member certificate",
				node.Certificate,
			)
		}
		if node.Flags != 0x18 {
			t.Fatalf("node flags = %#x, want %#x", node.Flags, uint32(0x18))
		}
		if member.Flags != 0xa5 {
			t.Fatalf(
				"certificate flags = %#x, want %#x",
				member.Flags,
				uint32(0xa5),
			)
		}
	})
}

func TestFastSyncPeerRuntimeNodeAdmission(t *testing.T) {
	now := time.Unix(10_000, 0)
	overlayID := peerRuntimeTestOverlayID(0x41)

	t.Run("inclusive version limits", func(t *testing.T) {
		runtime, issuerKey := peerRuntimeTestRootRuntime(
			t,
			overlayID,
			now,
		)

		for index, version := range []int32{
			int32(now.Add(-fastSyncPeerVersionTTL).Unix()),
			int32(now.Add(fastSyncPeerFutureClockSkew).Unix()),
		} {
			remoteKey := peerRuntimeTestKey(byte(0x50 + index))
			remoteID := peerRuntimeTestPeerID(
				remoteKey.Public().(ed25519.PublicKey),
			)
			certificate := peerRuntimeTestCertificate(
				t,
				issuerKey,
				remoteID,
				0,
				int32(index),
				20_000,
			)
			node := peerRuntimeTestNode(
				t,
				remoteKey,
				overlayID,
				uint32(index+1),
				version,
				certificate,
			)

			if _, err := runtime.EnrollNode(node, now); err != nil {
				t.Fatalf("enroll boundary node %d: %v", index, err)
			}
		}

		if counts := runtime.Counts(); counts.Known != 2 {
			t.Fatalf("known nodes = %d, want 2", counts.Known)
		}
	})

	tests := []struct {
		name   string
		change func(*overlay.NodeV2)
	}{
		{
			name: "too old",
			change: func(node *overlay.NodeV2) {
				node.Version = int32(
					now.Add(-fastSyncPeerVersionTTL - time.Second).Unix(),
				)
			},
		},
		{
			name: "too far in future",
			change: func(node *overlay.NodeV2) {
				node.Version = int32(
					now.Add(fastSyncPeerFutureClockSkew + time.Second).Unix(),
				)
			},
		},
		{
			name: "different overlay",
			change: func(node *overlay.NodeV2) {
				node.Overlay[0] ^= 0xff
			},
		},
		{
			name: "pointer id",
			change: func(node *overlay.NodeV2) {
				id := node.ID.(keys.PublicKeyED25519)
				node.ID = &id
			},
		},
		{
			name: "pointer certificate",
			change: func(node *overlay.NodeV2) {
				certificate := node.Certificate.(overlay.MemberCertificate)
				node.Certificate = &certificate
			},
		},
		{
			name: "bad node signature",
			change: func(node *overlay.NodeV2) {
				node.Signature[0] ^= 0xff
			},
		},
		{
			name: "empty client certificate",
			change: func(node *overlay.NodeV2) {
				node.Certificate = overlay.EmptyMemberCertificate{}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime, issuerKey := peerRuntimeTestRootRuntime(
				t,
				overlayID,
				now,
			)
			remoteKey := peerRuntimeTestKey(0x61)
			remoteID := peerRuntimeTestPeerID(
				remoteKey.Public().(ed25519.PublicKey),
			)
			certificate := peerRuntimeTestCertificate(
				t,
				issuerKey,
				remoteID,
				0,
				0,
				20_000,
			)
			node := peerRuntimeTestNode(
				t,
				remoteKey,
				overlayID,
				1,
				int32(now.Unix()),
				certificate,
			)
			test.change(&node)

			if _, err := runtime.EnrollNode(node, now); err == nil {
				t.Fatal("invalid node was accepted")
			}
			if runtime.membership.Contains(remoteID, now) {
				t.Fatal("invalid node changed membership")
			}
			if counts := runtime.Counts(); counts.Known != 0 {
				t.Fatalf("known nodes = %d, want 0", counts.Known)
			}
		})
	}
}

func TestFastSyncPeerRuntimeDescriptorAndCertificateUpdates(t *testing.T) {
	now := time.Unix(20_000, 0)
	overlayID := peerRuntimeTestOverlayID(0x71)
	runtime, issuerKey := peerRuntimeTestRootRuntime(t, overlayID, now)
	remoteKey := peerRuntimeTestKey(0x72)
	remoteID := peerRuntimeTestPeerID(
		remoteKey.Public().(ed25519.PublicKey),
	)

	firstCertificate := peerRuntimeTestCertificate(
		t,
		issuerKey,
		remoteID,
		1,
		0,
		21_000,
	)
	first := peerRuntimeTestNode(
		t,
		remoteKey,
		overlayID,
		0x10,
		int32(now.Unix()),
		firstCertificate,
	)
	if _, err := runtime.EnrollNode(first, now); err != nil {
		t.Fatalf("enroll first node: %v", err)
	}

	first.ID.(keys.PublicKeyED25519).Key[0] ^= 0xff
	first.Signature[0] ^= 0xff
	first.Certificate.(overlay.MemberCertificate).Signature[0] ^= 0xff
	stored, err := peerRuntimeStoredNode(runtime, remoteID)
	if err != nil {
		t.Fatalf("stored node: %v", err)
	}
	if err = stored.CheckSignature(); err != nil {
		t.Fatalf("stored node aliases caller-owned bytes: %v", err)
	}

	newCertificate := peerRuntimeTestCertificate(
		t,
		issuerKey,
		remoteID,
		2,
		0,
		22_000,
	)
	sameVersion := peerRuntimeTestNode(
		t,
		remoteKey,
		overlayID,
		0x20,
		int32(now.Unix()),
		newCertificate,
	)
	if _, err = runtime.EnrollNode(sameVersion, now); err != nil {
		t.Fatalf("enroll same-version node: %v", err)
	}

	stored, err = peerRuntimeStoredNode(runtime, remoteID)
	if err != nil {
		t.Fatalf("stored same-version node: %v", err)
	}
	if stored.Flags != 0x10 || stored.Version != int32(now.Unix()) {
		t.Fatalf(
			"same version replaced descriptor: flags=%#x version=%d",
			stored.Flags,
			stored.Version,
		)
	}
	if got := stored.Certificate.(overlay.MemberCertificate).ExpireAt; got != 22_000 {
		t.Fatalf("certificate expiry = %d, want 22000", got)
	}
	memberFlags, err := runtime.membership.PeerFlags(remoteID)
	if err != nil {
		t.Fatalf("member flags after same-version update: %v", err)
	}
	if memberFlags != 0x10 {
		t.Fatalf(
			"member flags after same-version update = %#x, want %#x",
			memberFlags,
			uint32(0x10),
		)
	}

	newer := peerRuntimeTestNode(
		t,
		remoteKey,
		overlayID,
		0x30,
		int32(now.Add(time.Second).Unix()),
		newCertificate,
	)
	if _, err = runtime.EnrollNode(newer, now); err != nil {
		t.Fatalf("enroll newer node: %v", err)
	}
	stored, err = peerRuntimeStoredNode(runtime, remoteID)
	if err != nil {
		t.Fatalf("stored newer node: %v", err)
	}
	if stored.Flags != 0x30 ||
		stored.Version != int32(now.Add(time.Second).Unix()) {
		t.Fatalf(
			"newer descriptor not installed: flags=%#x version=%d",
			stored.Flags,
			stored.Version,
		)
	}
	memberFlags, err = runtime.membership.PeerFlags(remoteID)
	if err != nil {
		t.Fatalf("member flags after newer update: %v", err)
	}
	if memberFlags != 0x30 {
		t.Fatalf(
			"member flags after newer update = %#x, want %#x",
			memberFlags,
			uint32(0x30),
		)
	}
}

func TestFastSyncPeerRuntimeRandomPeers(t *testing.T) {
	now := time.Unix(30_000, 0)
	overlayID := peerRuntimeTestOverlayID(0x81)
	localKey := peerRuntimeTestKey(0x82)
	localPublic := localKey.Public().(ed25519.PublicKey)
	localID := peerRuntimeTestPeerID(localPublic)
	permanentKey := peerRuntimeTestKey(0x83)
	permanentPublic := permanentKey.Public().(ed25519.PublicKey)
	permanentID := peerRuntimeTestPeerID(permanentPublic)
	roster := peerRuntimeTestRoster(
		localPublic,
		FastSyncValidator{ADNLID: localID},
		FastSyncValidator{ADNLID: permanentID},
	)
	membership := newFastSyncMembership(roster, 1)
	runtime, err := newFastSyncPeerRuntime(
		localKey,
		overlayID,
		0,
		overlay.EmptyMemberCertificate{},
		membership,
		now,
	)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}

	permanentNode := peerRuntimeTestNode(
		t,
		permanentKey,
		overlayID,
		0,
		int32(now.Unix()),
		overlay.EmptyMemberCertificate{},
	)
	if _, err = runtime.EnrollNode(permanentNode, now); err != nil {
		t.Fatalf("enroll permanent node: %v", err)
	}
	if err = runtime.SetAlive(permanentID, true); err != nil {
		t.Fatalf("mark permanent node alive: %v", err)
	}

	alive := make(map[PeerID]struct{}, 4)
	for index := 0; index < 5; index++ {
		remoteKey := peerRuntimeTestKey(byte(0x90 + index))
		remoteID := peerRuntimeTestPeerID(
			remoteKey.Public().(ed25519.PublicKey),
		)
		certificate := peerRuntimeTestCertificate(
			t,
			localKey,
			remoteID,
			0,
			int32(index),
			40_000,
		)
		node := peerRuntimeTestNode(
			t,
			remoteKey,
			overlayID,
			uint32(index),
			int32(now.Unix()),
			certificate,
		)
		if _, err = runtime.EnrollNode(node, now); err != nil {
			t.Fatalf("enroll client %d: %v", index, err)
		}
		if index == 4 {
			continue
		}
		if err = runtime.SetAlive(remoteID, true); err != nil {
			t.Fatalf("mark client %d alive: %v", index, err)
		}
		alive[remoteID] = struct{}{}
	}

	counts := runtime.Counts()
	if counts.Known != 6 ||
		counts.Alive != 5 ||
		counts.NonPermanent != 5 ||
		counts.AliveNonPermanent != 4 {
		t.Fatalf("unexpected counts: %+v", counts)
	}

	response, err := runtime.RandomPeers(now, 7)
	if err != nil {
		t.Fatalf("random peers: %v", err)
	}
	if len(response.Nodes) != fastSyncRandomPeerResultLimit {
		t.Fatalf(
			"response node count = %d, want %d",
			len(response.Nodes),
			fastSyncRandomPeerResultLimit,
		)
	}

	seen := make(map[PeerID]struct{}, len(response.Nodes))
	for index, node := range response.Nodes {
		id := peerRuntimeTestNodeID(t, node)
		if _, duplicate := seen[id]; duplicate {
			t.Fatalf("duplicate response node %v", id)
		}
		seen[id] = struct{}{}
		if index == 0 {
			if id != localID {
				t.Fatalf("first response node = %v, want local %v", id, localID)
			}
			continue
		}
		if id == permanentID {
			t.Fatal("permanent peer was included in random response")
		}
		if _, ok := alive[id]; !ok {
			t.Fatalf("non-alive peer %v was included", id)
		}
	}

	// The two windows are different, as they are upstream. The random draw
	// tolerates a peer until version+3600 (overlay-peers.cpp:613/:656)...
	pastIntake := now.Add(fastSyncPeerVersionTTL + time.Second)
	response, err = runtime.RandomPeers(pastIntake, 9)
	if err != nil {
		t.Fatalf("random peers past the intake window: %v", err)
	}
	if len(response.Nodes) == 1 {
		t.Fatal("the draw dropped peers at the intake window, want them retained")
	}

	// ...while the periodic sweep drops it at overlay_peer_ttl, which is what
	// update_neighbours does (overlay-peers.cpp:548).
	if removed := runtime.Prune(pastIntake); removed == 0 {
		t.Fatal("the sweep kept peers past the intake window")
	}
	if response, err = runtime.RandomPeers(pastIntake, 9); err != nil {
		t.Fatalf("random peers after the sweep: %v", err)
	}
	if len(response.Nodes) != 1 {
		t.Fatalf("swept response node count = %d, want self only", len(response.Nodes))
	}
	counts = runtime.Counts()
	if counts.Known != 1 ||
		counts.NonPermanent != 0 ||
		counts.AliveNonPermanent != 0 {
		t.Fatalf("counts after pruning: %+v", counts)
	}
}

func TestFastSyncRandomPeersMaxAnswerMatchesFourFullNodes(t *testing.T) {
	// Boxed nodesV2 overhead is 8 bytes; every full bare Ed25519 NodeV2 with
	// a member certificate occupies 264 bytes.
	const maximumWireSize = 8 + fastSyncRandomPeerResultLimit*264

	node := overlay.NodeV2{
		ID:        keys.PublicKeyED25519{Key: make([]byte, ed25519.PublicKeySize)},
		Overlay:   make([]byte, ed25519.PublicKeySize),
		Flags:     1,
		Version:   1,
		Signature: make([]byte, ed25519.SignatureSize),
		Certificate: overlay.MemberCertificate{
			IssuedBy: keys.PublicKeyED25519{
				Key: make([]byte, ed25519.PublicKeySize),
			},
			Flags:     1,
			Slot:      1,
			ExpireAt:  1,
			Signature: make([]byte, ed25519.SignatureSize),
		},
	}
	nodes := make([]overlay.NodeV2, fastSyncRandomPeerResultLimit)
	for i := range nodes {
		nodes[i] = node
	}

	wire, err := tl.Serialize(overlay.NodesV2{Nodes: nodes}, true)
	if err != nil {
		t.Fatalf("serialize maximum random peers answer: %v", err)
	}
	if len(wire) != maximumWireSize {
		t.Fatalf(
			"maximum random peers answer size = %d, want %d",
			len(wire),
			maximumWireSize,
		)
	}
}

func TestFastSyncPeerRuntimeRosterUpdate(t *testing.T) {
	now := time.Unix(40_000, 0)
	overlayID := peerRuntimeTestOverlayID(0xa1)
	runtime, issuerKey := peerRuntimeTestRootRuntime(t, overlayID, now)
	localID := runtime.LocalID()
	issuerPublic := issuerKey.Public().(ed25519.PublicKey)
	clientKey := peerRuntimeTestKey(0xa2)
	clientID := peerRuntimeTestPeerID(
		clientKey.Public().(ed25519.PublicKey),
	)
	certificate := peerRuntimeTestCertificate(
		t,
		issuerKey,
		clientID,
		0,
		0,
		50_000,
	)
	node := peerRuntimeTestNode(
		t,
		clientKey,
		overlayID,
		0x40,
		int32(now.Unix()),
		certificate,
	)
	if _, err := runtime.EnrollNode(node, now); err != nil {
		t.Fatalf("enroll client: %v", err)
	}
	if err := runtime.SetAlive(clientID, true); err != nil {
		t.Fatalf("mark client alive: %v", err)
	}

	promoted := peerRuntimeTestRoster(
		issuerPublic,
		FastSyncValidator{ADNLID: localID},
		FastSyncValidator{ADNLID: clientID},
	)
	runtime.UpdateRoster(promoted, 1, now)
	counts := runtime.Counts()
	if counts.NonPermanent != 0 || counts.AliveNonPermanent != 0 {
		t.Fatalf("counts after promotion: %+v", counts)
	}
	stored, err := peerRuntimeStoredNode(runtime, clientID)
	if err != nil {
		t.Fatalf("promoted node: %v", err)
	}
	if _, ok := stored.Certificate.(overlay.EmptyMemberCertificate); !ok {
		t.Fatalf(
			"promoted node certificate = %T, want empty",
			stored.Certificate,
		)
	}

	removed := peerRuntimeTestRoster(
		issuerPublic,
		FastSyncValidator{ADNLID: localID},
	)
	runtime.UpdateRoster(removed, 1, now)
	if _, err = peerRuntimeStoredNode(runtime, clientID); !errors.Is(err, ErrFastSyncNotFound) {
		t.Fatalf("removed peer lookup error = %v, want %v", err, ErrFastSyncNotFound)
	}
}

func TestFastSyncPeerRuntimeConcurrentAccess(t *testing.T) {
	now := time.Unix(50_000, 0)
	overlayID := peerRuntimeTestOverlayID(0xb1)
	runtime, issuerKey := peerRuntimeTestRootRuntime(t, overlayID, now)
	remoteKey := peerRuntimeTestKey(0xb2)
	remoteID := peerRuntimeTestPeerID(
		remoteKey.Public().(ed25519.PublicKey),
	)
	certificate := peerRuntimeTestCertificate(
		t,
		issuerKey,
		remoteID,
		0,
		0,
		60_000,
	)

	nodes := make([]overlay.NodeV2, 50)
	for index := range nodes {
		nodes[index] = peerRuntimeTestNode(
			t,
			remoteKey,
			overlayID,
			uint32(index),
			int32(now.Add(time.Duration(index)*time.Second).Unix()),
			certificate,
		)
	}
	if _, err := runtime.EnrollNode(nodes[0], now); err != nil {
		t.Fatalf("enroll initial node: %v", err)
	}

	var group sync.WaitGroup
	group.Add(4)
	go func() {
		defer group.Done()
		for _, node := range nodes {
			_, _ = runtime.EnrollNode(node, now)
		}
	}()
	go func() {
		defer group.Done()
		for index := 0; index < len(nodes); index++ {
			_ = runtime.SetAlive(remoteID, index%2 == 0)
		}
	}()
	go func() {
		defer group.Done()
		for index := 0; index < len(nodes); index++ {
			_, _ = runtime.RandomPeers(now, uint64(index))
		}
	}()
	go func() {
		defer group.Done()
		for range nodes {
			_ = runtime.Counts()
			_, _ = peerRuntimeStoredNode(runtime, remoteID)
			runtime.Prune(now)
		}
	}()
	group.Wait()
}

func TestFastSyncPeerRuntimeCountsDoNotAllocate(t *testing.T) {
	now := time.Unix(60_000, 0)
	runtime, _ := peerRuntimeTestRootRuntime(
		t,
		peerRuntimeTestOverlayID(0xc1),
		now,
	)

	allocations := testing.AllocsPerRun(1_000, func() {
		_ = runtime.Counts()
	})
	if allocations != 0 {
		t.Fatalf("counts allocations = %f, want 0", allocations)
	}
}

// fastSyncRuntimeAllowPeerDescriptor mirrors how admitPeerDescriptor consumes
// the descriptor window: check availability, then record on success. Callers
// must already hold r.mu.
func fastSyncRuntimeAllowPeerDescriptor(
	r *fastSyncPeerRuntime,
	now time.Time,
) bool {
	if !r.peerDescriptorAvailableLocked(now) {
		return false
	}

	r.recordPeerDescriptorLocked(now)
	return true
}

func TestFastSyncPeerDescriptorRateLimit(t *testing.T) {
	now := time.Unix(70_000, 0)
	runtime, issuerKey := peerRuntimeTestRootRuntime(
		t,
		peerRuntimeTestOverlayID(0xd1),
		now,
	)

	runtime.mu.Lock()
	for index := 0; index < fastSyncPeerDescriptorLimit; index++ {
		if !fastSyncRuntimeAllowPeerDescriptor(runtime, now) {
			runtime.mu.Unlock()
			t.Fatalf("descriptor %d was rate limited", index)
		}
	}
	if fastSyncRuntimeAllowPeerDescriptor(runtime, now) {
		runtime.mu.Unlock()
		t.Fatal("descriptor beyond the window limit was accepted")
	}
	if fastSyncRuntimeAllowPeerDescriptor(runtime,
		now.Add(fastSyncPeerDescriptorWindow),
	) {
		runtime.mu.Unlock()
		t.Fatal("descriptor at the inclusive window boundary was accepted")
	}
	if !fastSyncRuntimeAllowPeerDescriptor(runtime,
		now.Add(fastSyncPeerDescriptorWindow+time.Nanosecond),
	) {
		runtime.mu.Unlock()
		t.Fatal("expired descriptor window was not released")
	}
	if runtime.descriptorCount != 1 {
		runtime.mu.Unlock()
		t.Fatalf(
			"descriptor count after expiration = %d, want 1",
			runtime.descriptorCount,
		)
	}
	runtime.mu.Unlock()

	remoteKey := peerRuntimeTestKey(0xd2)
	remoteID := peerRuntimeTestPeerID(
		remoteKey.Public().(ed25519.PublicKey),
	)
	node := peerRuntimeTestNode(
		t,
		remoteKey,
		runtime.overlayID,
		0,
		int32(now.Unix()),
		peerRuntimeTestCertificate(
			t,
			issuerKey,
			remoteID,
			0,
			0,
			int32(now.Add(time.Hour).Unix()),
		),
	)
	node.Signature[0] ^= 0xff

	verifierRuntime, _ := peerRuntimeTestRootRuntime(t, runtime.overlayID, now)
	if _, err := verifierRuntime.EnrollNode(node, now); err == nil ||
		errors.Is(err, errFastSyncPeerDescriptorsRateLimited) {
		t.Fatalf("invalid signature admission error = %v", err)
	}

	limitedRuntime, _ := peerRuntimeTestRootRuntime(t, runtime.overlayID, now)
	limitedRuntime.mu.Lock()
	for index := 0; index < fastSyncPeerDescriptorLimit; index++ {
		if !fastSyncRuntimeAllowPeerDescriptor(limitedRuntime, now) {
			limitedRuntime.mu.Unlock()
			t.Fatalf("prefill descriptor %d was rate limited", index)
		}
	}
	limitedRuntime.mu.Unlock()

	if _, err := limitedRuntime.EnrollNode(node, now); !errors.Is(
		err,
		errFastSyncPeerDescriptorsRateLimited,
	) {
		t.Fatalf(
			"rate-limited invalid signature error = %v, want %v",
			err,
			errFastSyncPeerDescriptorsRateLimited,
		)
	}
}

func BenchmarkFastSyncPeerDescriptorLimiterFull(b *testing.B) {
	now := time.Unix(80_000, 0)
	runtime := fastSyncPeerRuntime{
		descriptorTimes: make(
			[]time.Time,
			fastSyncPeerDescriptorLimit,
		),
		descriptorCount: fastSyncPeerDescriptorLimit,
	}
	for index := range runtime.descriptorTimes {
		runtime.descriptorTimes[index] = now
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if fastSyncRuntimeAllowPeerDescriptor(&runtime, now) {
			b.Fatal("full descriptor limiter admitted a descriptor")
		}
	}
}

func peerRuntimeTestRootRuntime(
	t *testing.T,
	overlayID FastSyncOverlayShortID,
	now time.Time,
) (*fastSyncPeerRuntime, ed25519.PrivateKey) {
	t.Helper()

	issuerKey := peerRuntimeTestKey(0xd1)
	issuerPublic := issuerKey.Public().(ed25519.PublicKey)
	issuerID := peerRuntimeTestPeerID(issuerPublic)
	roster := peerRuntimeTestRoster(
		issuerPublic,
		FastSyncValidator{ADNLID: issuerID},
	)
	membership := newFastSyncMembership(roster, 1)
	runtime, err := newFastSyncPeerRuntime(
		issuerKey,
		overlayID,
		0,
		overlay.EmptyMemberCertificate{},
		membership,
		now,
	)
	if err != nil {
		t.Fatalf("create root runtime: %v", err)
	}
	return runtime, issuerKey
}

func peerRuntimeTestRoster(
	rootPublic ed25519.PublicKey,
	validators ...FastSyncValidator,
) FastSyncValidatorRoster {
	var root FastSyncValidatorPublicKey
	copy(root[:], rootPublic)
	for index := range validators {
		validators[index].PublicKey = root
	}
	return NewFastSyncValidatorRoster(nil, validators, nil)
}

func peerRuntimeTestKey(marker byte) ed25519.PrivateKey {
	return ed25519.NewKeyFromSeed(
		bytes.Repeat([]byte{marker}, ed25519.SeedSize),
	)
}

func peerRuntimeTestPeerID(publicKey ed25519.PublicKey) PeerID {
	var value FastSyncValidatorPublicKey
	copy(value[:], publicKey)
	return fastSyncValidatorShortID(value)
}

func peerRuntimeTestOverlayID(marker byte) FastSyncOverlayShortID {
	var id FastSyncOverlayShortID
	for index := range id {
		id[index] = marker + byte(index)
	}
	return id
}

func peerRuntimeTestCertificate(
	t *testing.T,
	issuerKey ed25519.PrivateKey,
	node PeerID,
	flags uint32,
	slot int32,
	expireAt int32,
) overlay.MemberCertificate {
	t.Helper()

	certificate := overlay.MemberCertificate{
		IssuedBy: keys.PublicKeyED25519{
			Key: issuerKey.Public().(ed25519.PublicKey),
		},
		Flags:    flags,
		Slot:     slot,
		ExpireAt: expireAt,
	}
	toSign, err := tl.Serialize(overlay.MemberCertificateID{
		Node:     node[:],
		Flags:    flags,
		Slot:     slot,
		ExpireAt: expireAt,
	}, true)
	if err != nil {
		t.Fatalf("serialize certificate: %v", err)
	}
	certificate.Signature = ed25519.Sign(issuerKey, toSign)
	return certificate
}

func peerRuntimeTestNode(
	t *testing.T,
	privateKey ed25519.PrivateKey,
	overlayID FastSyncOverlayShortID,
	flags uint32,
	version int32,
	certificate any,
) overlay.NodeV2 {
	t.Helper()

	node := overlay.NodeV2{
		ID: keys.PublicKeyED25519{
			Key: privateKey.Public().(ed25519.PublicKey),
		},
		Overlay:     overlayID[:],
		Flags:       flags,
		Version:     version,
		Certificate: certificate,
	}
	if err := node.Sign(privateKey); err != nil {
		t.Fatalf("sign node: %v", err)
	}
	return node
}

func peerRuntimeTestNodeID(
	t *testing.T,
	node overlay.NodeV2,
) PeerID {
	t.Helper()

	publicKey, ok := node.ID.(keys.PublicKeyED25519)
	if !ok {
		t.Fatalf("node id type = %T, want ED25519", node.ID)
	}
	return peerRuntimeTestPeerID(publicKey.Key)
}

func peerRuntimeStoredNode(
	runtime *fastSyncPeerRuntime,
	id PeerID,
) (overlay.NodeV2, error) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()

	peer := runtime.peers[id]
	if peer == nil {
		return overlay.NodeV2{}, ErrFastSyncNotFound
	}
	return peer.node(runtime.overlayID), nil
}
