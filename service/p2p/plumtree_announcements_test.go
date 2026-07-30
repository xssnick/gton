package p2p

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
)

// An engine whose slot for the announced part already has an eager peer, which is
// what routes an IHAVE onto the deferred path, plus verified state so a deadline
// can be armed.
func plumtreeAnnouncementTestEngine(
	t *testing.T,
	verifier plumtreeVerifier,
	now time.Time,
) (*plumtreeEngine, [sha256.Size]byte) {
	t.Helper()

	eager := plumtreeEngineTestPeer(2)
	engine := plumtreeEngineTestNew(
		t,
		plumtreeEngineTestPeer(1),
		false,
		&plumtreeEngineTestPeers{receives: map[PeerID]bool{eager: true}},
		verifier,
		func() time.Time { return now },
	)

	broadcastID := plumtreeEngineTestID(7)
	engine.mu.Lock()
	engine.promoteEagerLocked(&engine.slots[1], eager, true, now)
	state := &plumtreeFECState{broadcastID: broadcastID, firstSeen: now}
	state.parts[1] = &plumtreePartState{partIndex: 1, treeIndex: 2}
	engine.addFECStateLocked(state)
	engine.mu.Unlock()
	return engine, broadcastID
}

func plumtreeAnnouncementTestIHave(
	broadcastID [sha256.Size]byte,
	source keys.PublicKeyED25519,
	signature []byte,
	now time.Time,
) BroadcastPlumtreeIHave {
	dataHash := sha256.Sum256([]byte{9})
	return BroadcastPlumtreeIHave{
		BroadcastID:      broadcastID[:],
		Timestamp:        plumtreeUnixSeconds(now),
		PartIndex:        0,
		TreeIndex:        1,
		Source:           source,
		Certificate:      overlay.CertificateEmpty{},
		PayloadTimestamp: plumtreeUnixSeconds(now),
		DataSize:         1,
		DataHash:         dataHash[:],
		Signature:        signature,
	}
}

// The property that makes deferring safe: an unverified announcement only ever
// competes with other announcements from the same peer.
func TestPlumtreeAnnouncementSpamDoesNotEvictAnotherPeer(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_500, 0)
	engine, broadcastID := plumtreeAnnouncementTestEngine(
		t,
		&plumtreeEngineTestVerifier{},
		now,
	)
	source := plumtreeEngineTestSource(0x71)
	signature := bytes.Repeat([]byte{1}, ed25519.SignatureSize)

	honest := plumtreeEngineTestPeer(10)
	if _, err := engine.HandleIHave(
		t.Context(),
		now,
		honest,
		plumtreeAnnouncementTestIHave(broadcastID, source, signature, now),
	); err != nil {
		t.Fatalf("handle honest announcement: %v", err)
	}

	// Far past its own cap, with distinct broadcasts so nothing dedups.
	spammer := plumtreeEngineTestPeer(11)
	for i := range plumtreeAnnouncementPeerLimit * 4 {
		id := plumtreeEngineTestID(uint32(1000 + i))
		message := plumtreeAnnouncementTestIHave(id, source, signature, now)
		if _, err := engine.HandleIHave(t.Context(), now, spammer, message); err != nil {
			t.Fatalf("handle spam %d: %v", i, err)
		}
	}

	engine.mu.Lock()
	defer engine.mu.Unlock()
	honestQueue := engine.announcements[honest]
	if honestQueue == nil || honestQueue.count != 1 {
		t.Fatalf("honest announcement was evicted by spam: %v", honestQueue)
	}
	if spamQueue := engine.announcements[spammer]; spamQueue.count != plumtreeAnnouncementPeerLimit {
		t.Fatalf(
			"spammer queue = %d, want capped at %d",
			spamQueue.count,
			plumtreeAnnouncementPeerLimit,
		)
	}
	if len(engine.missing) != 0 {
		t.Fatalf("unverified announcements created %d missing parts", len(engine.missing))
	}
}

// A fabricated announcement costs one verification and nothing on the wire.
func TestPlumtreeAnnouncementForgeryProducesNoRepairQuery(t *testing.T) {
	t.Parallel()

	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	sourceID, err := peerIDFromED25519PublicKey(publicKey)
	if err != nil {
		t.Fatalf("derive source id: %v", err)
	}

	node := &Node{}
	node.SetPlumtreePolicy(NewPlumtreePolicy(2, 2, []PeerID{sourceID}))
	verifier := newPlumtreeSignatureVerifier(
		nodePlumtreePolicySource{node: node},
		testPeerID("announcement-forgery-overlay"),
		0,
	)

	now := time.Now()
	engine, broadcastID := plumtreeAnnouncementTestEngine(t, verifier, now)
	liar := plumtreeEngineTestPeer(12)
	message := plumtreeAnnouncementTestIHave(
		broadcastID,
		keys.PublicKeyED25519{Key: publicKey},
		bytes.Repeat([]byte{0xAA}, ed25519.SignatureSize),
		now,
	)
	if _, err = engine.HandleIHave(t.Context(), now, liar, message); err != nil {
		t.Fatalf("buffer forged announcement: %v", err)
	}
	if _, misses := verifier.SignatureCacheStats(); misses != 0 {
		t.Fatalf("forged announcement verified on arrival: %d", misses)
	}

	actions := engine.Alarm(now.Add(plumtreeRepairDelay))
	if len(actions.Candidates) != 1 {
		t.Fatalf("candidates = %d, want 1", len(actions.Candidates))
	}
	if len(actions.Repairs) != 0 {
		t.Fatalf("forged announcement produced %d repair queries", len(actions.Repairs))
	}

	candidate := actions.Candidates[0]
	if _, err = verifier.verifyAt(plumtreeVerification{
		From:        candidate.Peer,
		Source:      candidate.Source,
		Certificate: candidate.Certificate,
		DataSize:    uint32(candidate.DataSize),
		SignedData:  candidate.CandidateSignedData(),
		Signature:   candidate.Signature,
	}, now); err == nil {
		t.Fatal("forged candidate signature verified")
	}
	// The peer is banned exactly as it would have been at arrival time.
	if _, err = verifier.verifyAt(plumtreeVerification{
		From:        candidate.Peer,
		Source:      candidate.Source,
		Certificate: candidate.Certificate,
		DataSize:    uint32(candidate.DataSize),
		SignedData:  candidate.CandidateSignedData(),
		Signature:   candidate.Signature,
	}, now.Add(time.Second)); err == nil {
		t.Fatal("liar was not banned after a forged candidate")
	}
}

// Deferring the signature must not delay the repair: the deadline is measured from
// when the part was announced, even though state arrived later.
func TestPlumtreeAnnouncementDeadlineUsesAnnouncementTime(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_600, 0)
	eager := plumtreeEngineTestPeer(2)
	engine := plumtreeEngineTestNew(
		t,
		plumtreeEngineTestPeer(1),
		false,
		&plumtreeEngineTestPeers{receives: map[PeerID]bool{eager: true}},
		&plumtreeEngineTestVerifier{},
		func() time.Time { return now },
	)
	engine.mu.Lock()
	engine.promoteEagerLocked(&engine.slots[1], eager, true, now)
	engine.mu.Unlock()

	broadcastID := plumtreeEngineTestID(8)
	source := plumtreeEngineTestSource(0x72)
	signature := bytes.Repeat([]byte{1}, ed25519.SignatureSize)
	if _, err := engine.HandleIHave(
		t.Context(),
		now,
		plumtreeEngineTestPeer(13),
		plumtreeAnnouncementTestIHave(broadcastID, source, signature, now),
	); err != nil {
		t.Fatalf("buffer announcement before state: %v", err)
	}

	// State shows up 150ms later; the deadline stays announcement+200ms.
	stateAt := now.Add(150 * time.Millisecond)
	engine.mu.Lock()
	state := &plumtreeFECState{broadcastID: broadcastID, firstSeen: stateAt}
	state.parts[1] = &plumtreePartState{partIndex: 1, treeIndex: 2}
	engine.addFECStateLocked(state)
	engine.armFECRepairForStateLocked(state)
	engine.mu.Unlock()

	if actions := engine.Alarm(now.Add(plumtreeRepairDelay - time.Nanosecond)); len(actions.Candidates) != 0 {
		t.Fatal("candidate handed out before the announcement deadline")
	}
	if actions := engine.Alarm(now.Add(plumtreeRepairDelay)); len(actions.Candidates) != 1 {
		t.Fatal("deadline was measured from state arrival instead of the announcement")
	}
}

// Announcements for broadcasts we never get state for disappear on their own.
func TestPlumtreeAnnouncementTTLDrainsWithoutEviction(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_700, 0)
	engine, _ := plumtreeAnnouncementTestEngine(
		t,
		&plumtreeEngineTestVerifier{},
		now,
	)
	// No state means no repair deadline, so nothing consumes the announcement and
	// only the TTL can remove it. This is what a fabricated broadcast id looks like.
	unknown := plumtreeEngineTestID(999)
	from := plumtreeEngineTestPeer(14)
	if _, err := engine.HandleIHave(
		t.Context(),
		now,
		from,
		plumtreeAnnouncementTestIHave(
			unknown,
			plumtreeEngineTestSource(0x73),
			bytes.Repeat([]byte{1}, ed25519.SignatureSize),
			now,
		),
	); err != nil {
		t.Fatalf("buffer announcement: %v", err)
	}

	engine.Alarm(now.Add(plumtreeAnnouncementTTL / 2))
	engine.mu.Lock()
	held := engine.announcements[from] != nil
	engine.mu.Unlock()
	if !held {
		t.Fatal("announcement dropped before its TTL")
	}

	engine.Alarm(now.Add(plumtreeAnnouncementTTL + time.Second))
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if engine.announcements[from] != nil {
		t.Fatal("announcement outlived its TTL")
	}
}

// Deferring must not widen how many peers get asked for one part: the cap
// missing.targets applies on the eager path is applied at selection here.
func TestPlumtreeAnnouncementCapsPeersAskedPerPart(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_800, 0)
	engine, broadcastID := plumtreeAnnouncementTestEngine(
		t,
		&plumtreeEngineTestVerifier{},
		now,
	)
	source := plumtreeEngineTestSource(0x74)
	signature := bytes.Repeat([]byte{1}, ed25519.SignatureSize)
	for i := range plumtreeRepairTargetLimit + 3 {
		if _, err := engine.HandleIHave(
			t.Context(),
			now,
			plumtreeEngineTestPeer(byte(20+i)),
			plumtreeAnnouncementTestIHave(broadcastID, source, signature, now),
		); err != nil {
			t.Fatalf("handle announcer %d: %v", i, err)
		}
	}

	actions := engine.Alarm(now.Add(plumtreeRepairDelay))
	if len(actions.Candidates) != plumtreeRepairTargetLimit {
		t.Fatalf(
			"peers asked for one part = %d, want %d",
			len(actions.Candidates),
			plumtreeRepairTargetLimit,
		)
	}
}

// The by-broadcast index must stay in step with the per-peer queues through
// every removal path, or arming a new state silently misses announcements (a
// part is never repaired) or walks freed entries.
func TestPlumtreeAnnouncementIndexTracksEveryRemovalPath(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_700, 0)
	engine, broadcastID := plumtreeAnnouncementTestEngine(
		t,
		&plumtreeEngineTestVerifier{},
		now,
	)
	source := plumtreeEngineTestSource(0x73)
	signature := bytes.Repeat([]byte{1}, ed25519.SignatureSize)

	peers := []PeerID{
		plumtreeEngineTestPeer(20),
		plumtreeEngineTestPeer(21),
		plumtreeEngineTestPeer(22),
	}
	for _, peer := range peers {
		if _, err := engine.HandleIHave(
			t.Context(),
			now,
			peer,
			plumtreeAnnouncementTestIHave(broadcastID, source, signature, now),
		); err != nil {
			t.Fatalf("handle announcement: %v", err)
		}
		// A second broadcast id per peer, so the index has to separate them.
		other := plumtreeAnnouncementTestIHave(
			plumtreeEngineTestID(4242),
			source,
			signature,
			now,
		)
		if _, err := engine.HandleIHave(t.Context(), now, peer, other); err != nil {
			t.Fatalf("handle other announcement: %v", err)
		}
	}

	assertIndexMatchesQueues(t, engine)

	// Removal path 1: consumed as a repair candidate.
	engine.mu.Lock()
	engine.takeCandidatesLocked(plumtreePartKey{
		broadcastID: broadcastID,
		partIndex:   0,
		treeIndex:   1,
	}, nil)
	engine.mu.Unlock()
	assertIndexMatchesQueues(t, engine)

	// Removal path 2: the whole peer goes away.
	engine.DropPeerAnnouncements(peers[0])
	assertIndexMatchesQueues(t, engine)

	// Removal path 3: TTL expiry.
	engine.mu.Lock()
	engine.expireAnnouncementsLocked(now.Add(2 * plumtreeAnnouncementTTL))
	engine.mu.Unlock()
	assertIndexMatchesQueues(t, engine)

	engine.mu.Lock()
	defer engine.mu.Unlock()
	if len(engine.announcementsByBroadcast) != 0 {
		t.Fatalf(
			"index kept %d broadcasts after every announcement expired",
			len(engine.announcementsByBroadcast),
		)
	}
}

// A new broadcast's state must be armed from that broadcast's announcements
// only: it walks the index, so a wrong thread would arm from a stranger's parts.
func TestPlumtreeArmFECRepairUsesOnlyItsOwnBroadcast(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_800, 0)
	engine, broadcastID := plumtreeAnnouncementTestEngine(
		t,
		&plumtreeEngineTestVerifier{},
		now,
	)
	source := plumtreeEngineTestSource(0x74)
	signature := bytes.Repeat([]byte{1}, ed25519.SignatureSize)

	foreign := plumtreeEngineTestID(9999)
	announcer := plumtreeEngineTestPeer(30)
	for _, id := range [][sha256.Size]byte{broadcastID, foreign} {
		if _, err := engine.HandleIHave(
			t.Context(),
			now,
			announcer,
			plumtreeAnnouncementTestIHave(id, source, signature, now),
		); err != nil {
			t.Fatalf("handle announcement: %v", err)
		}
	}

	engine.mu.Lock()
	defer engine.mu.Unlock()

	state := &plumtreeFECState{broadcastID: foreign, firstSeen: now}
	engine.armFECRepairForStateLocked(state)

	armed := 0
	for i := range state.repairAt {
		if !state.repairAt[i].IsZero() {
			armed++
		}
	}
	if armed != 1 {
		t.Fatalf("armed %d parts from one announcement, want 1", armed)
	}
	// Part 0 is what the foreign announcement covers; anything else means the
	// index handed us the other broadcast's entry.
	if state.repairAt[0].IsZero() {
		t.Fatal("the announced part was not armed")
	}
}

func assertIndexMatchesQueues(t *testing.T, engine *plumtreeEngine) {
	t.Helper()

	engine.mu.Lock()
	defer engine.mu.Unlock()

	queued := map[*plumtreeAnnouncement]struct{}{}
	for _, queue := range engine.announcements {
		for announcement := queue.oldest; announcement != nil; announcement = announcement.next {
			queued[announcement] = struct{}{}
		}
	}

	indexed := map[*plumtreeAnnouncement]struct{}{}
	for id, head := range engine.announcementsByBroadcast {
		if head == nil {
			t.Fatalf("index holds a nil head for %x", id)
		}
		for announcement := head; announcement != nil; announcement = announcement.broadcastNext {
			if announcement.key.broadcastID != id {
				t.Fatalf("announcement for %x is threaded under %x", announcement.key.broadcastID, id)
			}
			if _, dup := indexed[announcement]; dup {
				t.Fatal("index threads the same announcement twice")
			}
			indexed[announcement] = struct{}{}
		}
	}

	if len(queued) != len(indexed) {
		t.Fatalf("queues hold %d announcements, index holds %d", len(queued), len(indexed))
	}
	for announcement := range queued {
		if _, ok := indexed[announcement]; !ok {
			t.Fatalf("announcement %v is queued but not indexed", announcement.key)
		}
	}
}
