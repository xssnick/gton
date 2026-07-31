package p2p

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
)

// Drives the real signature verifier through the engine exactly as the wire path
// does: one FEC part announced by several distinct peers. The first announcer
// proves the broadcast and is the only arrival that runs key math; the rest are
// deferred, and when the repair deadline hands them back they carry the same
// signature over the same preimage, so re-verifying them all is free.
func TestPlumtreeIHaveSameFECPartReusesVerifiedSignature(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate source key: %v", err)
	}
	sourceID, err := peerIDFromED25519PublicKey(publicKey)
	if err != nil {
		t.Fatalf("derive source peer ID: %v", err)
	}
	source := keys.PublicKeyED25519{Key: publicKey}

	node := &Node{}
	node.SetPlumtreePolicy(NewPlumtreePolicy(2, 2, []PeerID{sourceID}))
	verifier := newPlumtreeSignatureVerifier(
		nodePlumtreePolicySource{node: node},
		testPeerID("plumtree-ihave-cache-overlay"),
		0,
	)

	now := time.Now()
	peers := &plumtreeEngineTestPeers{receives: map[PeerID]bool{}}
	const announcers = 5
	for i := range announcers {
		peers.receives[plumtreeEngineTestPeer(byte(i+1))] = true
	}
	engine := plumtreeEngineTestNew(
		t,
		plumtreeEngineTestPeer(200),
		false,
		peers,
		verifier,
		func() time.Time { return now },
	)

	broadcastID := plumtreeEngineTestID(7)
	const partIndex = int32(3)
	treeIndex := partIndex + plumtreeFECTreeOffset
	eager := plumtreeEngineTestPeer(100)
	peers.receives[eager] = true
	engine.mu.Lock()
	engine.promoteEagerLocked(&engine.slots[treeIndex], eager, true, now)
	engine.mu.Unlock()
	symbol := []byte("plumtree ihave cache symbol")
	partHash := sha256.Sum256(symbol)
	payloadTimestamp := plumtreeUnixSeconds(now)

	signedData := serializeValidatedPlumtreeFECToSign(BroadcastPlumtreeFECToSign{
		ID:        broadcastID[:],
		Timestamp: payloadTimestamp,
		PartIndex: partIndex,
		TreeIndex: treeIndex,
		DataSize:  int32(len(symbol)),
		DataHash:  partHash[:],
	})
	signature := ed25519.Sign(privateKey, signedData)

	for i := range announcers {
		if _, err = engine.HandleIHave(
			t.Context(),
			now,
			plumtreeEngineTestPeer(byte(i+1)),
			BroadcastPlumtreeIHave{
				BroadcastID:      broadcastID[:],
				Timestamp:        plumtreeUnixSeconds(now),
				PartIndex:        partIndex,
				TreeIndex:        treeIndex,
				Source:           source,
				Certificate:      overlay.CertificateEmpty{},
				PayloadTimestamp: payloadTimestamp,
				DataSize:         int32(len(symbol)),
				DataHash:         partHash[:],
				Signature:        signature,
			},
		); err != nil {
			t.Fatalf("handle IHAVE from announcer %d: %v", i+1, err)
		}
	}

	hits, misses := verifier.SignatureCacheStats()
	if misses != 1 {
		t.Fatalf("arrival verifications = %d, want only the first announcer", misses)
	}
	if hits != 0 {
		t.Fatalf("deferred announcements consulted the cache %d times", hits)
	}

	actions := engine.Alarm(now.Add(plumtreeRepairDelay))
	if len(actions.Candidates) != plumtreeRepairTargetLimit {
		t.Fatalf(
			"candidates = %d, want %d",
			len(actions.Candidates),
			plumtreeRepairTargetLimit,
		)
	}
	for i, candidate := range actions.Candidates {
		if _, err = verifier.verifyAt(plumtreeVerification{
			From:        candidate.Peer,
			Source:      candidate.Source,
			Certificate: candidate.Certificate,
			DataSize:    uint32(candidate.DataSize),
			SignedData:  candidate.CandidateSignedData(),
			Signature:   candidate.Signature,
		}, now); err != nil {
			t.Fatalf("verify candidate %d: %v", i, err)
		}
	}

	hits, misses = verifier.SignatureCacheStats()
	if misses != 1 {
		t.Fatalf("repair verifications = %d, want the arrival one to still be the only key math", misses)
	}
	if hits != plumtreeRepairTargetLimit {
		t.Fatalf("signature cache hits = %d, want %d", hits, plumtreeRepairTargetLimit)
	}
}
