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

// A verified payload part proves the broadcast, so its remaining IHAVEs cost no
// key math at all.
func TestPlumtreeProvenBroadcastSkipsIHaveVerification(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_500, 0)
	verifier := &plumtreeEngineTestVerifier{}
	engine, broadcastID := plumtreeAnnouncementTestEngine(t, verifier, now)

	source := plumtreeEngineTestSource(0x71)
	signature := bytes.Repeat([]byte{1}, ed25519.SignatureSize)
	if _, err := engine.HandleIHave(
		t.Context(),
		now,
		plumtreeEngineTestPeer(10),
		plumtreeAnnouncementTestIHave(broadcastID, source, signature, now),
	); err != nil {
		t.Fatalf("handle announcement: %v", err)
	}

	if calls := verifier.calls.Load(); calls != 0 {
		t.Fatalf("IHAVE of a broadcast we hold a part of was verified: %d", calls)
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if len(engine.missing) != 1 {
		t.Fatalf("missing parts = %d, want the announcement to arm one", len(engine.missing))
	}
}

// The first IHAVE of an unknown broadcast is the one that pays, and it marks the
// broadcast so the next announcer does not pay again.
func TestPlumtreeFirstIHaveVerifiesAndProvesBroadcast(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_550, 0)
	verifier := &plumtreeEngineTestVerifier{}
	engine, _ := plumtreeAnnouncementTestEngine(t, verifier, now)

	unknown := plumtreeEngineTestID(4242)
	source := plumtreeEngineTestSource(0x72)
	signature := bytes.Repeat([]byte{1}, ed25519.SignatureSize)
	for i := range 3 {
		if _, err := engine.HandleIHave(
			t.Context(),
			now,
			plumtreeEngineTestPeer(byte(20+i)),
			plumtreeAnnouncementTestIHave(unknown, source, signature, now),
		); err != nil {
			t.Fatalf("handle announcer %d: %v", i, err)
		}
	}

	if calls := verifier.calls.Load(); calls != 1 {
		t.Fatalf("verifications = %d, want exactly the first announcer", calls)
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if !engine.provenLocked(unknown) {
		t.Fatal("a verified IHAVE did not prove its broadcast")
	}
	missing := engine.missing[plumtreePartKey{broadcastID: unknown, partIndex: 0, treeIndex: 1}]
	if missing == nil || missing.announcerCount != 3 {
		t.Fatalf("announcers = %v, want all three recorded", missing)
	}
}

func TestPlumtreeIHaveStartsRepairImmediatelyWithoutEagerPeer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		proven bool
		simple bool
	}{
		{name: "first verified IHAVE"},
		{name: "deferred IHAVE of proven broadcast", proven: true},
		{name: "simple IHAVE", simple: true},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			now := time.Unix(1_800_000_575, 0)
			engine := plumtreeEngineTestNew(
				t,
				plumtreeEngineTestPeer(1),
				false,
				&plumtreeEngineTestPeers{},
				&plumtreeEngineTestVerifier{},
				func() time.Time { return now },
			)
			broadcastID := plumtreeEngineTestID(4250 + uint32(index))
			if test.proven {
				engine.mu.Lock()
				engine.proven.add(broadcastID)
				engine.mu.Unlock()
			}

			message := plumtreeAnnouncementTestIHave(
				broadcastID,
				plumtreeEngineTestSource(0x73),
				bytes.Repeat([]byte{1}, ed25519.SignatureSize),
				now,
			)
			if test.simple {
				message.TreeIndex = plumtreeSimpleTree
			}

			actions, err := engine.HandleIHave(
				t.Context(),
				now,
				plumtreeEngineTestPeer(20),
				message,
			)
			if err != nil {
				t.Fatalf("handle announcement: %v", err)
			}
			if len(actions.Candidates) != 1 {
				t.Fatalf(
					"immediate repair candidates = %d, want 1",
					len(actions.Candidates),
				)
			}

			engine.mu.Lock()
			defer engine.mu.Unlock()
			if len(engine.missing) != 0 {
				t.Fatalf(
					"%d delayed repairs survived immediate handoff",
					len(engine.missing),
				)
			}
			if len(engine.announcements) != 0 {
				t.Fatalf(
					"%d announcement queues survived immediate handoff",
					len(engine.announcements),
				)
			}
		})
	}
}

// Simple broadcasts have a single part, so there is no "rest" to defer: every
// simple IHAVE verifies on arrival.
func TestPlumtreeSimpleIHaveAlwaysVerifies(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_560, 0)
	verifier := &plumtreeEngineTestVerifier{}
	engine, _ := plumtreeAnnouncementTestEngine(t, verifier, now)

	broadcastID := plumtreeEngineTestID(555)
	source := plumtreeEngineTestSource(0x73)
	dataHash := sha256.Sum256([]byte{9})
	for i := range 2 {
		message := BroadcastPlumtreeIHave{
			BroadcastID:      broadcastID[:],
			Timestamp:        plumtreeUnixSeconds(now),
			PartIndex:        0,
			TreeIndex:        plumtreeSimpleTree,
			Source:           source,
			Certificate:      overlay.CertificateEmpty{},
			PayloadTimestamp: plumtreeUnixSeconds(now),
			DataSize:         1,
			DataHash:         dataHash[:],
			Signature:        bytes.Repeat([]byte{1}, ed25519.SignatureSize),
		}
		if _, err := engine.HandleIHave(
			t.Context(),
			now,
			plumtreeEngineTestPeer(byte(30+i)),
			message,
		); err != nil {
			t.Fatalf("handle simple announcer %d: %v", i, err)
		}
	}

	if calls := verifier.calls.Load(); calls != 2 {
		t.Fatalf("simple verifications = %d, want one per announcer", calls)
	}
}

// A fabricated announcement of a proven broadcast costs one deferred
// verification and nothing on the wire.
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
	candidate := actions.Candidates[0]
	verification := plumtreeVerification{
		From:        candidate.Peer,
		Source:      candidate.Source,
		Certificate: candidate.Certificate,
		DataSize:    uint32(candidate.DataSize),
		SignedData:  candidate.CandidateSignedData(),
		Signature:   candidate.Signature,
	}
	if _, err = verifier.verifyAt(verification, now); err == nil {
		t.Fatal("forged candidate signature verified")
	}
	// The peer is banned exactly as it would have been at arrival time.
	if _, err = verifier.verifyAt(verification, now.Add(time.Second)); err == nil {
		t.Fatal("liar was not banned after a forged candidate")
	}
}

// The deadline is measured from the announcement, so state arriving later
// neither delays nor advances the repair.
func TestPlumtreeRepairDeadlineUsesAnnouncementTime(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_600, 0)
	eager := plumtreeEngineTestPeer(2)
	engine := plumtreeEngineTestNew(
		t,
		plumtreeEngineTestPeer(1),
		false,
		&plumtreeEngineTestPeers{
			receives: map[PeerID]bool{eager: true},
		},
		&plumtreeEngineTestVerifier{},
		func() time.Time { return now },
	)
	engine.mu.Lock()
	engine.promoteEagerLocked(&engine.slots[1], eager, true, now)
	engine.mu.Unlock()

	broadcastID := plumtreeEngineTestID(8)
	source := plumtreeEngineTestSource(0x74)
	signature := bytes.Repeat([]byte{1}, ed25519.SignatureSize)
	if _, err := engine.HandleIHave(
		t.Context(),
		now,
		plumtreeEngineTestPeer(13),
		plumtreeAnnouncementTestIHave(broadcastID, source, signature, now),
	); err != nil {
		t.Fatalf("handle announcement before state: %v", err)
	}

	// State shows up 150ms later; the deadline stays announcement+200ms.
	engine.mu.Lock()
	state := &plumtreeFECState{
		broadcastID: broadcastID,
		firstSeen:   now.Add(150 * time.Millisecond),
	}
	state.parts[1] = &plumtreePartState{partIndex: 1, treeIndex: 2}
	engine.addFECStateLocked(state)
	engine.mu.Unlock()

	if actions := engine.Alarm(now.Add(plumtreeRepairDelay - time.Nanosecond)); len(actions.Candidates) != 0 {
		t.Fatal("candidate handed out before the announcement deadline")
	}
	if actions := engine.Alarm(now.Add(plumtreeRepairDelay)); len(actions.Candidates) != 1 {
		t.Fatal("deadline was measured from state arrival instead of the announcement")
	}
}

// Announcements live and die with the part they announce: once its deadline has
// been drained nothing is left buffered, so no separate sweep is needed.
func TestPlumtreeAnnouncementsReleasedWithTheirPart(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_700, 0)
	engine, broadcastID := plumtreeAnnouncementTestEngine(
		t,
		&plumtreeEngineTestVerifier{},
		now,
	)
	from := plumtreeEngineTestPeer(14)
	if _, err := engine.HandleIHave(
		t.Context(),
		now,
		from,
		plumtreeAnnouncementTestIHave(
			broadcastID,
			plumtreeEngineTestSource(0x75),
			bytes.Repeat([]byte{1}, ed25519.SignatureSize),
			now,
		),
	); err != nil {
		t.Fatalf("handle announcement: %v", err)
	}

	engine.mu.Lock()
	held := engine.announcements[from] != nil
	engine.mu.Unlock()
	if !held {
		t.Fatal("announcement was not buffered")
	}

	engine.Alarm(now.Add(plumtreeRepairDelay))
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if engine.announcements[from] != nil {
		t.Fatal("announcement outlived the part it announced")
	}
	if len(engine.missing) != 0 {
		t.Fatalf("drained part left %d entries behind", len(engine.missing))
	}
}

// Trusting the rest of a broadcast's IHAVEs must not widen how many peers get
// asked for one part.
func TestPlumtreeAnnouncementCapsPeersAskedPerPart(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_800, 0)
	engine, broadcastID := plumtreeAnnouncementTestEngine(
		t,
		&plumtreeEngineTestVerifier{},
		now,
	)
	source := plumtreeEngineTestSource(0x76)
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

// A part's announcer list and the per-peer queues are two views of the same
// entries. If they drift, a part is either asked of a peer that no longer holds
// the announcement or is never repaired at all.
func TestPlumtreeAnnouncerListTracksEveryRemovalPath(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_900, 0)
	engine, broadcastID := plumtreeAnnouncementTestEngine(
		t,
		&plumtreeEngineTestVerifier{},
		now,
	)
	source := plumtreeEngineTestSource(0x77)
	signature := bytes.Repeat([]byte{1}, ed25519.SignatureSize)

	peers := []PeerID{
		plumtreeEngineTestPeer(20),
		plumtreeEngineTestPeer(21),
		plumtreeEngineTestPeer(22),
	}
	for _, peer := range peers {
		for _, id := range [][sha256.Size]byte{broadcastID, plumtreeEngineTestID(4242)} {
			if _, err := engine.HandleIHave(
				t.Context(),
				now,
				peer,
				plumtreeAnnouncementTestIHave(id, source, signature, now),
			); err != nil {
				t.Fatalf("handle announcement: %v", err)
			}
		}
	}
	assertAnnouncerListsMatchQueues(t, engine)

	// Removal path 1: the whole peer goes away.
	engine.RemovePeer(peers[0])
	assertAnnouncerListsMatchQueues(t, engine)

	// Removal path 2: the part arrives, so we stop waiting for it.
	engine.mu.Lock()
	engine.eraseMissingLocked(plumtreePartKey{
		broadcastID: broadcastID,
		partIndex:   0,
		treeIndex:   1,
	})
	engine.mu.Unlock()
	assertAnnouncerListsMatchQueues(t, engine)

	// Removal path 3: the deadline fires and the candidates are handed out.
	engine.Alarm(now.Add(plumtreeRepairDelay))
	assertAnnouncerListsMatchQueues(t, engine)

	engine.mu.Lock()
	defer engine.mu.Unlock()
	if len(engine.announcements) != 0 {
		t.Fatalf("%d peer queues survived every removal path", len(engine.announcements))
	}
}

// The Alarm drain relies on e.missing being ordered by deadline, which holds
// only while every entry is created with the same delay. NextAlarm reads the
// head and the drain stops at the first entry that is not due.
func TestPlumtreeMissingListStaysOrderedByDeadline(t *testing.T) {
	t.Parallel()

	base := time.Unix(1_800_001_000, 0)
	now := base
	eager := plumtreeEngineTestPeer(2)
	peers := &plumtreeEngineTestPeers{
		receives: map[PeerID]bool{eager: true},
	}
	engine := plumtreeEngineTestNew(
		t,
		plumtreeEngineTestPeer(1),
		false,
		peers,
		&plumtreeEngineTestVerifier{},
		func() time.Time { return now },
	)
	engine.mu.Lock()
	engine.promoteEagerLocked(&engine.slots[1], eager, true, now)
	engine.mu.Unlock()

	source := plumtreeEngineTestSource(0x78)
	signature := bytes.Repeat([]byte{1}, ed25519.SignatureSize)
	for i := range 5 {
		now = base.Add(time.Duration(i) * 10 * time.Millisecond)
		if _, err := engine.HandleIHave(
			t.Context(),
			now,
			plumtreeEngineTestPeer(byte(40+i)),
			plumtreeAnnouncementTestIHave(
				plumtreeEngineTestID(uint32(7000+i)),
				source,
				signature,
				now,
			),
		); err != nil {
			t.Fatalf("handle announcement %d: %v", i, err)
		}
	}

	engine.mu.Lock()
	previous := time.Time{}
	count := 0
	for entry := engine.missingOldest; entry != nil; entry = entry.next {
		if entry.repairAt.Before(previous) {
			t.Fatalf("missing list is not ordered by deadline at entry %d", count)
		}
		previous = entry.repairAt
		count++
	}
	engine.mu.Unlock()
	if count != 5 {
		t.Fatalf("walked %d entries, want 5", count)
	}

	next, ok := engine.NextAlarm()
	if !ok {
		t.Fatal("NextAlarm reported nothing pending")
	}
	if want := base.Add(plumtreeRepairDelay); !next.Equal(want) {
		t.Fatalf("NextAlarm = %v, want the earliest deadline %v", next, want)
	}
}

// Refusing new entries rather than evicting the oldest is what keeps the
// imminent repairs: the oldest entry is the one about to fire.
func TestPlumtreeMissingLimitRefusesNewestNotOldest(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_001_100, 0)
	engine := plumtreeEngineTestNew(
		t,
		plumtreeEngineTestPeer(1),
		false,
		&plumtreeEngineTestPeers{},
		&plumtreeEngineTestVerifier{},
		func() time.Time { return now },
	)

	first := plumtreePartKey{
		broadcastID: plumtreeEngineTestID(8000),
		partIndex:   0,
		treeIndex:   1,
	}
	engine.mu.Lock()
	for i := range plumtreeMissingLimit {
		key := plumtreePartKey{
			broadcastID: plumtreeEngineTestID(uint32(8000 + i)),
			partIndex:   0,
			treeIndex:   1,
		}
		engine.addMissingLocked(&plumtreeMissingPart{
			key:      key,
			repairAt: now.Add(plumtreeRepairDelay),
		})
	}
	engine.mu.Unlock()

	overflow := plumtreeEngineTestID(7000)
	if _, err := engine.HandleIHave(
		t.Context(),
		now,
		plumtreeEngineTestPeer(50),
		plumtreeAnnouncementTestIHave(
			overflow,
			plumtreeEngineTestSource(0x79),
			bytes.Repeat([]byte{1}, ed25519.SignatureSize),
			now,
		),
	); err != nil {
		t.Fatalf("handle announcement at the limit: %v", err)
	}

	engine.mu.Lock()
	defer engine.mu.Unlock()
	if len(engine.missing) != plumtreeMissingLimit {
		t.Fatalf("missing = %d, want the limit held", len(engine.missing))
	}
	if engine.missing[first] == nil {
		t.Fatal("the entry closest to firing was evicted to make room")
	}
	if engine.missing[plumtreePartKey{broadcastID: overflow, partIndex: 0, treeIndex: 1}] != nil {
		t.Fatal("an entry was admitted past the limit")
	}
}

func assertAnnouncerListsMatchQueues(t *testing.T, engine *plumtreeEngine) {
	t.Helper()

	engine.mu.Lock()
	defer engine.mu.Unlock()

	queued := map[*plumtreeAnnouncement]struct{}{}
	for peer, queue := range engine.announcements {
		count := 0
		for announcement := queue.oldest; announcement != nil; announcement = announcement.next {
			if announcement.peer != peer {
				t.Fatalf("announcement of %v sits in %v's queue", announcement.peer, peer)
			}
			if queue.byKey[announcement.key] != announcement {
				t.Fatalf("queue map and list disagree for %v", peer)
			}
			queued[announcement] = struct{}{}
			count++
		}
		if count != queue.count {
			t.Fatalf("queue for %v counts %d but holds %d", peer, queue.count, count)
		}
	}

	listed := map[*plumtreeAnnouncement]struct{}{}
	for key, missing := range engine.missing {
		if missing.announcerCount == 0 {
			t.Fatalf("part %v is waiting with nobody to ask", key)
		}
		for i := range int(missing.announcerCount) {
			announcement := missing.announcers[i]
			if announcement.key != key {
				t.Fatalf("part %v lists an announcer of %v", key, announcement.key)
			}
			if _, ok := queued[announcement]; !ok {
				t.Fatalf("part %v lists an announcer no queue holds", key)
			}
			listed[announcement] = struct{}{}
		}
	}

	if len(listed) != len(queued) {
		t.Fatalf("%d announcements queued but %d reachable from parts", len(queued), len(listed))
	}
}
