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
