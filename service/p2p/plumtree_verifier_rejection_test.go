package p2p

import (
	"errors"
	"testing"
	"time"
)

// recordRejectionLocked (plumtree_verifier.go) refuses to shorten an active ban
// with an older timestamp (`!until.After(current) -> return`), and
// expireRejectedLocked deletes only the queue entry whose `until` still matches
// the map. Both guard the concurrent VerifyPlumtree path, where a verifier that
// stalled between precheck and its post-gate rejection records with a timestamp
// older than a ban another verifier installed meanwhile. The existing
// concurrent test uses a distinct `from` per goroutine, so that older-timestamp
// record never lands on an already-banned peer; this test reproduces it
// deterministically for one fixed `from` using the same two rejection records
// the concurrent path performs.
func TestPlumtreeSignatureVerifierStaleRejectionDoesNotShortenBan(t *testing.T) {
	_, sourcePrivate, sourceID := plumtreeVerifierKey(t)
	node := &Node{}
	node.SetPlumtreePolicy(NewPlumtreePolicy([]PeerID{sourceID}))
	verifier := newPlumtreeSignatureVerifier(
		nodePlumtreePolicySource{node: node},
		testPeerID("plumtree-stale-rejection-overlay"),
	)

	from := testPeerID("plumtree-stale-rejection-from")
	request := plumtreeVerifierRequest(
		sourcePrivate,
		plumtreeCertificate{kind: plumtreeCertificateEmpty},
		from,
	)
	request.Signature[0] ^= 1 // invalid signature

	laterNow := time.Unix(1_800_003_000, 0)
	earlierNow := laterNow.Add(-2 * time.Second)
	wantUntil := laterNow.Add(plumtreeSignatureRejectionDuration)

	// Install the active ban through the full clock-seam entry point.
	if _, err := verifier.verifyAt(request, laterNow); !errors.Is(err, errPlumtreeSignatureInvalid) {
		t.Fatalf("initial invalid-signature verify error = %v", err)
	}
	if got := verifierRejectedUntil(verifier, from); !got.Equal(wantUntil) {
		t.Fatalf("initial ban until = %v, want %v", got, wantUntil)
	}

	// A verifier that stalled between precheck and its post-gate record now
	// records the same peer with its older timestamp. This is exactly the call
	// checkSourceSignature makes after re-acquiring the gate.
	verifier.gate <- struct{}{}
	verifier.recordRejectionLocked(from, earlierNow)
	<-verifier.gate

	// (a) the stale record must not shorten the active ban.
	if got := verifierRejectedUntil(verifier, from); !got.Equal(wantUntil) {
		t.Fatalf("ban until after stale record = %v, want %v (unchanged)", got, wantUntil)
	}

	// (b) the peer is still rejected right up to the last nanosecond of the ban.
	if _, err := verifier.verifyAt(request, wantUntil.Add(-time.Nanosecond)); !errors.Is(
		err,
		errPlumtreePeerSignaturesRejected,
	) {
		t.Fatalf("verify just before ban expiry error = %v, want peer rejected", err)
	}

	// (c) the rejection queue stays ordered by `until`, so expiry and the
	// until.Equal deletion check remain coherent with the map.
	verifier.gate <- struct{}{}
	untils := make([]time.Time, 0, len(verifier.rejectionQueue))
	for _, entry := range verifier.rejectionQueue[verifier.rejectionHead:] {
		untils = append(untils, entry.until)
	}
	<-verifier.gate
	for i := 1; i < len(untils); i++ {
		if untils[i].Before(untils[i-1]) {
			t.Fatalf("rejection queue untils not non-decreasing: %v", untils)
		}
	}
}

func verifierRejectedUntil(v *plumtreeSignatureVerifier, from PeerID) time.Time {
	v.gate <- struct{}{}
	defer func() { <-v.gate }()
	return v.rejected[from]
}
