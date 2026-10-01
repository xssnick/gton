package p2p

import (
	"bytes"
	"crypto/ed25519"
	"sync"
	"testing"
	"time"
)

// Without an eager peer a part is asked right away, but it stays missing until
// its deadline, as in C++ (sent_repair_targets): a peer announcing it again is
// not asked twice, and the part is never asked of more than
// plumtreeRepairTargetLimit peers.
func TestPlumtreeImmediateRepairKeepsAnnouncersUntilDeadline(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_002_000, 0)
	engine := plumtreeEngineTestNew(
		t,
		plumtreeEngineTestPeer(1),
		false,
		&plumtreeEngineTestPeers{},
		&plumtreeEngineTestVerifier{},
		func() time.Time { return now },
	)
	message := plumtreeAnnouncementTestIHave(
		plumtreeEngineTestID(9100),
		plumtreeEngineTestSource(0x80),
		bytes.Repeat([]byte{1}, ed25519.SignatureSize),
		now,
	)

	asked := map[PeerID]int{}
	announce := func(peer PeerID) {
		t.Helper()

		actions, err := engine.HandleIHave(t.Context(), now, peer, message)
		if err != nil {
			t.Fatalf("handle announcement from %v: %v", peer, err)
		}
		for _, candidate := range actions.Candidates {
			asked[candidate.Peer]++
		}
	}

	first := plumtreeEngineTestPeer(20)
	announce(first)
	announce(first)
	for i := range plumtreeRepairTargetLimit + 2 {
		announce(plumtreeEngineTestPeer(byte(21 + i)))
	}

	if asked[first] != 1 {
		t.Fatalf("repeating announcer asked %d times, want 1", asked[first])
	}
	if len(asked) != plumtreeRepairTargetLimit {
		t.Fatalf("peers asked for one part = %d, want %d", len(asked), plumtreeRepairTargetLimit)
	}
	for peer, count := range asked {
		if count != 1 {
			t.Fatalf("peer %v asked %d times, want 1", peer, count)
		}
	}
	assertAnnouncerListsMatchQueues(t, engine)

	actions := engine.Alarm(now.Add(plumtreeRepairDelay))
	if len(actions.Candidates) != 0 {
		t.Fatalf("deadline asked %d already asked peers again", len(actions.Candidates))
	}

	engine.mu.Lock()
	defer engine.mu.Unlock()
	if len(engine.missing) != 0 || len(engine.announcements) != 0 {
		t.Fatalf(
			"deadline left %d missing parts and %d announcement queues",
			len(engine.missing),
			len(engine.announcements),
		)
	}
}

// Dropping an announcer that was already asked must not count one that was not
// as asked.
func TestPlumtreeDroppedAskedAnnouncerKeepsUnaskedPending(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_002_050, 0)
	eager := plumtreeEngineTestPeer(2)
	engine := plumtreeEngineTestNew(
		t,
		plumtreeEngineTestPeer(1),
		false,
		&plumtreeEngineTestPeers{receives: map[PeerID]bool{eager: true}},
		&plumtreeEngineTestVerifier{},
		func() time.Time { return now },
	)
	message := plumtreeAnnouncementTestIHave(
		plumtreeEngineTestID(9150),
		plumtreeEngineTestSource(0x81),
		bytes.Repeat([]byte{1}, ed25519.SignatureSize),
		now,
	)

	asked := plumtreeEngineTestPeer(20)
	actions, err := engine.HandleIHave(t.Context(), now, asked, message)
	if err != nil {
		t.Fatalf("handle announcement without eager peer: %v", err)
	}
	if len(actions.Candidates) != 1 {
		t.Fatalf("immediate candidates = %d, want 1", len(actions.Candidates))
	}

	engine.mu.Lock()
	engine.promoteEagerLocked(&engine.slots[message.TreeIndex], eager, true, now)
	engine.mu.Unlock()

	waiting := plumtreeEngineTestPeer(21)
	if actions, err = engine.HandleIHave(t.Context(), now, waiting, message); err != nil {
		t.Fatalf("handle announcement with eager peer: %v", err)
	}
	if len(actions.Candidates) != 0 {
		t.Fatalf("deferred announcement asked immediately: %d", len(actions.Candidates))
	}

	engine.DropPeerAnnouncements(asked)
	assertAnnouncerListsMatchQueues(t, engine)

	actions = engine.Alarm(now.Add(plumtreeRepairDelay))
	if len(actions.Candidates) != 1 || actions.Candidates[0].Peer != waiting {
		t.Fatalf("deadline candidates = %+v, want only the unasked announcer", actions.Candidates)
	}
}

// The producer lease flips the role while QUIC handlers deliver IHAVEs, so the
// role check has to read it under the engine lock. Run with -race.
func TestPlumtreeIHaveReadsOriginalSenderUnderLock(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_002_100, 0)
	engine := plumtreeEngineTestNew(
		t,
		plumtreeEngineTestPeer(1),
		false,
		&plumtreeEngineTestPeers{},
		&plumtreeEngineTestVerifier{},
		func() time.Time { return now },
	)
	message := plumtreeAnnouncementTestIHave(
		plumtreeEngineTestID(9200),
		plumtreeEngineTestSource(0x82),
		bytes.Repeat([]byte{1}, ed25519.SignatureSize),
		now,
	)

	var roles sync.WaitGroup
	roles.Add(1)
	go func() {
		defer roles.Done()
		for i := range 500 {
			engine.SetOriginalSender(i%2 == 0)
		}
	}()

	for i := range 500 {
		if _, err := engine.HandleIHave(
			t.Context(),
			now,
			plumtreeEngineTestPeer(byte(20+i%200)),
			message,
		); err != nil {
			t.Fatalf("handle announcement %d: %v", i, err)
		}
	}
	roles.Wait()

	engine.SetOriginalSender(true)
	if _, err := engine.HandleIHave(
		t.Context(),
		now,
		plumtreeEngineTestPeer(250),
		plumtreeAnnouncementTestIHave(
			plumtreeEngineTestID(9201),
			plumtreeEngineTestSource(0x82),
			bytes.Repeat([]byte{1}, ed25519.SignatureSize),
			now,
		),
	); err != nil {
		t.Fatalf("handle announcement as original sender: %v", err)
	}

	engine.mu.Lock()
	defer engine.mu.Unlock()
	if engine.announcements[plumtreeEngineTestPeer(250)] != nil {
		t.Fatal("original sender buffered an IHAVE")
	}
}

// The engine accounts a send before the runtime queues it, so a batch lost in
// between is a part that peer never gets and can no longer ask for. Everything
// the outbound queue admits must reach it while the run loop is busy.
func TestPlumtreeOutboundBurstReachesQueue(t *testing.T) {
	node := newTestNode(t)
	overlayID := testPeerID("plumtree-outbound-burst")
	sub, err := node.newOverlaySubscription(overlaySpec{
		Name:      "plumtree-outbound-burst",
		Kind:      overlayKindPublicShard,
		Workchain: 0,
		ShortID:   overlayID[:],
	})
	if err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	t.Cleanup(sub.close)

	for i := range plumtreeOutboundQueueLimit {
		sub.plumtree.enqueueOutbounds(plumtreeWireBatch{
			sends: []plumtreeWireSend{{
				to:   plumtreeEngineTestPeer(byte(1 + i%plumtreeOutboundParallelism)),
				wire: []byte{byte(i)},
			}},
		})
	}

	queue := newPlumtreeOutboundQueue()
	for len(sub.plumtree.outbound) > 0 {
		if dropped := queue.add(<-sub.plumtree.outbound); dropped != 0 {
			t.Fatalf("queue dropped %d messages of an admissible burst", dropped)
		}
	}
	if queue.size != plumtreeOutboundQueueLimit {
		t.Fatalf("queued messages = %d, want %d", queue.size, plumtreeOutboundQueueLimit)
	}
}
