package p2p

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

func TestPeerIDFromED25519PublicKeyMatchesTLHash(t *testing.T) {
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	got, err := peerIDFromED25519PublicKey(publicKey)
	if err != nil {
		t.Fatalf("derive peer ID: %v", err)
	}
	rawWant, err := tl.Hash(keys.PublicKeyED25519{Key: publicKey})
	if err != nil {
		t.Fatalf("hash TL public key: %v", err)
	}
	want, err := NewPeerID(rawWant)
	if err != nil {
		t.Fatalf("parse expected peer ID: %v", err)
	}
	if got != want {
		t.Fatalf("peer ID = %x, want %x", got, want)
	}
}

func TestPlumtreeSignatureVerifierAcceptsAuthorizedSource(t *testing.T) {
	publicKey, privateKey, sourceID := plumtreeVerifierKey(t)
	node := &Node{}
	node.SetPlumtreePolicy(NewPlumtreePolicy([]PeerID{sourceID}))
	verifier := newPlumtreeSignatureVerifier(
		nodePlumtreePolicySource{node: node},
		testPeerID("plumtree-verifier-overlay"),
	)
	request := plumtreeVerifierRequest(
		privateKey,
		plumtreeCertificate{kind: plumtreeCertificateEmpty},
		testPeerID("plumtree-verifier-from"),
	)

	got, err := verifier.verifyAt(request, time.Now())
	if err != nil {
		t.Fatalf("verify authorized source: %v", err)
	}
	if got != sourceID {
		t.Fatalf("verified source = %x, want %x", got, sourceID)
	}
	if len(publicKey) != ed25519.PublicKeySize {
		t.Fatalf("generated public key size = %d", len(publicKey))
	}
}

func TestPlumtreeSignatureVerifierUsesFixedFastSyncAuthorization(t *testing.T) {
	_, sourcePrivate, sourceID := plumtreeVerifierKey(t)
	source := &fixedPlumtreePolicySource{
		policy: NewPlumtreePolicy([]PeerID{sourceID}),
	}
	verifier := newPlumtreeSignatureVerifier(
		source,
		testPeerID("fast-sync-plumtree-verifier-overlay"),
	)

	if _, err := verifier.verifyAt(
		plumtreeVerifierRequest(
			sourcePrivate,
			plumtreeCertificate{kind: plumtreeCertificateEmpty},
			testPeerID("fast-sync-plumtree-verifier-from"),
		),
		time.Now(),
	); err != nil {
		t.Fatalf("verify FastSync ADNL source: %v", err)
	}

	_, otherPrivate, _ := plumtreeVerifierKey(t)
	if _, err := verifier.verifyAt(
		plumtreeVerifierRequest(
			otherPrivate,
			plumtreeCertificate{kind: plumtreeCertificateEmpty},
			testPeerID("fast-sync-plumtree-other-from"),
		),
		time.Now(),
	); !errors.Is(err, errPlumtreeSourceNotAllowed) {
		t.Fatalf("unauthorized FastSync source error = %v", err)
	}
}

func TestPlumtreeSignatureVerifierUsesTrustedCertificate(t *testing.T) {
	issuerPublic, issuerPrivate, issuerID := plumtreeVerifierKey(t)
	_, sourcePrivate, sourceID := plumtreeVerifierKey(t)
	overlayID := testPeerID("plumtree-certificate-overlay")
	now := time.Now()
	expireAt := int32(now.Unix() + 3600)
	maxSize := uint32(4096)

	node := &Node{}
	node.SetPlumtreePolicy(NewPlumtreePolicy([]PeerID{issuerID}))
	verifier := newPlumtreeSignatureVerifier(
		nodePlumtreePolicySource{node: node},
		overlayID,
	)
	certificate := overlay.Certificate{
		IssuedBy: keys.PublicKeyED25519{Key: issuerPublic},
		ExpireAt: uint32(expireAt),
		MaxSize:  maxSize,
	}
	certificate.Signature = plumtreeSignTL(t, issuerPrivate, overlay.CertificateId{
		OverlayID: overlayID[:],
		Node:      sourceID[:],
		ExpireAt:  uint32(expireAt),
		MaxSize:   maxSize,
	})
	request := plumtreeVerifierRequest(
		sourcePrivate,
		plumtreeCertificate{
			kind:   plumtreeCertificateLegacy,
			legacy: certificate,
		},
		testPeerID("plumtree-certified-from"),
	)

	got, err := verifier.verifyAt(request, now)
	if err != nil {
		t.Fatalf("verify certified source: %v", err)
	}
	if got != sourceID {
		t.Fatalf("verified source = %x, want %x", got, sourceID)
	}
}

func TestPlumtreeSignatureVerifierCanonicalizesDefaultV2Certificate(t *testing.T) {
	issuerPublic, issuerPrivate, issuerID := plumtreeVerifierKey(t)
	_, sourcePrivate, sourceID := plumtreeVerifierKey(t)
	overlayID := testPeerID("plumtree-v2-canonical-overlay")
	now := time.Now()
	expireAt := int32(now.Unix() + 3600)
	maxSize := uint32(4096)
	flags := int32(overlayCertificateAllowFEC | overlayCertificateTrusted)

	node := &Node{}
	node.SetPlumtreePolicy(NewPlumtreePolicy([]PeerID{issuerID}))
	verifier := newPlumtreeSignatureVerifier(
		nodePlumtreePolicySource{node: node},
		overlayID,
	)
	certificate := overlay.CertificateV2{
		IssuedBy: keys.PublicKeyED25519{Key: issuerPublic},
		ExpireAt: uint32(expireAt),
		MaxSize:  maxSize,
		Flags:    flags,
	}
	certificate.Signature = plumtreeSignTL(t, issuerPrivate, overlay.CertificateId{
		OverlayID: overlayID[:],
		Node:      sourceID[:],
		ExpireAt:  uint32(expireAt),
		MaxSize:   maxSize,
	})

	request := plumtreeVerifierRequest(
		sourcePrivate,
		plumtreeCertificate{
			kind: plumtreeCertificateV2,
			v2:   certificate,
		},
		testPeerID("plumtree-v2-canonical-from"),
	)
	if _, err := verifier.verifyAt(request, now); err != nil {
		t.Fatalf("verify canonicalized V2 certificate: %v", err)
	}
}

func TestPlumtreeSignatureVerifierRejectsNegativeCertificateExpiry(t *testing.T) {
	issuerPublic, _, issuerID := plumtreeVerifierKey(t)
	_, sourcePrivate, _ := plumtreeVerifierKey(t)
	node := &Node{}
	node.SetPlumtreePolicy(NewPlumtreePolicy([]PeerID{issuerID}))
	verifier := newPlumtreeSignatureVerifier(
		nodePlumtreePolicySource{node: node},
		testPeerID("plumtree-negative-expiry-overlay"),
	)
	request := plumtreeVerifierRequest(
		sourcePrivate,
		plumtreeCertificate{
			kind: plumtreeCertificateV2,
			v2: overlay.CertificateV2{
				IssuedBy: keys.PublicKeyED25519{Key: issuerPublic},
				ExpireAt: math.MaxUint32,
				MaxSize:  4096,
				Flags: int32(
					overlayCertificateAllowFEC |
						overlayCertificateTrusted,
				),
			},
		},
		testPeerID("plumtree-negative-expiry-from"),
	)

	_, err := verifier.verifyAt(request, time.Now())
	if !errors.Is(err, errPlumtreeSourceNotAllowed) {
		t.Fatalf("negative certificate expiry error = %v", err)
	}
}

func TestPlumtreeSignatureVerifierTemporarilyRejectsBadSignaturePeer(t *testing.T) {
	_, sourcePrivate, sourceID := plumtreeVerifierKey(t)
	node := &Node{}
	node.SetPlumtreePolicy(NewPlumtreePolicy([]PeerID{sourceID}))
	verifier := newPlumtreeSignatureVerifier(
		nodePlumtreePolicySource{node: node},
		testPeerID("plumtree-signature-ban-overlay"),
	)
	from := testPeerID("plumtree-signature-ban-from")
	request := plumtreeVerifierRequest(
		sourcePrivate,
		plumtreeCertificate{kind: plumtreeCertificateEmpty},
		from,
	)
	now := time.Now()
	request.Signature[0] ^= 1

	if _, err := verifier.verifyAt(request, now); !errors.Is(err, errPlumtreeSignatureInvalid) {
		t.Fatalf("invalid signature error = %v", err)
	}
	request.Signature = ed25519.Sign(sourcePrivate, request.SignedData)
	if _, err := verifier.verifyAt(request, now.Add(time.Second)); !errors.Is(
		err,
		errPlumtreePeerSignaturesRejected,
	) {
		t.Fatalf("temporarily rejected signature error = %v", err)
	}
	if _, err := verifier.verifyAt(
		request,
		now.Add(plumtreeSignatureRejectionDuration+time.Nanosecond),
	); err != nil {
		t.Fatalf("verify after signature rejection expiry: %v", err)
	}
}

func TestPlumtreeSignatureVerifierCertificateRateLimitAndCache(t *testing.T) {
	issuerPublic, issuerPrivate, issuerID := plumtreeVerifierKey(t)
	overlayID := testPeerID("plumtree-rate-overlay")
	from := testPeerID("plumtree-rate-from")
	now := time.Now()
	expireAt := int32(now.Unix() + 3600)
	maxSize := uint32(4096)

	node := &Node{}
	node.SetPlumtreePolicy(NewPlumtreePolicy([]PeerID{issuerID}))
	verifier := newPlumtreeSignatureVerifier(
		nodePlumtreePolicySource{node: node},
		overlayID,
	)

	first := plumtreeCertifiedVerifierRequest(
		t,
		issuerPublic,
		issuerPrivate,
		overlayID,
		from,
		expireAt,
		maxSize,
	)
	if _, err := verifier.verifyAt(first, now); err != nil {
		t.Fatalf("verify first certificate: %v", err)
	}
	if _, err := verifier.verifyAt(first, now); err != nil {
		t.Fatalf("verify cached certificate: %v", err)
	}

	for i := 1; i < plumtreeCertificateCheckLimit; i++ {
		request := plumtreeCertifiedVerifierRequest(
			t,
			issuerPublic,
			issuerPrivate,
			overlayID,
			from,
			expireAt,
			maxSize,
		)
		if _, err := verifier.verifyAt(request, now); err != nil {
			t.Fatalf("verify certificate %d: %v", i+1, err)
		}
	}

	limited := plumtreeCertifiedVerifierRequest(
		t,
		issuerPublic,
		issuerPrivate,
		overlayID,
		from,
		expireAt,
		maxSize,
	)
	if _, err := verifier.verifyAt(limited, now); !errors.Is(
		err,
		errPlumtreeCertificateRateLimited,
	) {
		t.Fatalf("certificate rate limit error = %v", err)
	}
}

func TestPlumtreeSignatureVerifierRunsSourceChecksConcurrently(t *testing.T) {
	t.Parallel()

	_, sourcePrivate, sourceID := plumtreeVerifierKey(t)
	node := &Node{}
	node.SetPlumtreePolicy(NewPlumtreePolicy([]PeerID{sourceID}))
	verifier := newPlumtreeSignatureVerifier(
		nodePlumtreePolicySource{node: node},
		testPeerID("plumtree-concurrent-overlay"),
	)

	const verifiers = 8
	var wg sync.WaitGroup
	errs := make([]error, verifiers)
	for i := range verifiers {
		request := plumtreeVerifierRequest(
			sourcePrivate,
			plumtreeCertificate{kind: plumtreeCertificateEmpty},
			testPeerID(fmt.Sprintf("plumtree-concurrent-from-%d", i)),
		)
		if i%2 == 1 {
			request.Signature[0] ^= 1
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = verifier.VerifyPlumtree(context.Background(), request)
		}()
	}
	wg.Wait()

	for i, err := range errs {
		if i%2 == 0 && err != nil {
			t.Fatalf("concurrent verify %d: %v", i, err)
		}
		if i%2 == 1 && !errors.Is(err, errPlumtreeSignatureInvalid) {
			t.Fatalf("concurrent invalid signature %d error = %v", i, err)
		}
	}
}

func TestPlumtreeCertificateIssuerUsesDecodedValueType(t *testing.T) {
	publicKey, _, _ := plumtreeVerifierKey(t)
	_, err := plumtreeCertificateData(plumtreeCertificate{
		kind: plumtreeCertificateLegacy,
		legacy: overlay.Certificate{
			IssuedBy: &keys.PublicKeyED25519{Key: publicKey},
			MaxSize:  4096,
		},
	})
	if err == nil {
		t.Fatal("pointer certificate issuer was accepted")
	}
}

func TestPlumtreeSignatureVerifierCachesVerifiedSourceSignature(t *testing.T) {
	_, sourcePrivate, sourceID := plumtreeVerifierKey(t)
	node := &Node{}
	node.SetPlumtreePolicy(NewPlumtreePolicy([]PeerID{sourceID}))
	verifier := newPlumtreeSignatureVerifier(
		nodePlumtreePolicySource{node: node},
		testPeerID("plumtree-signature-cache-overlay"),
	)
	request := plumtreeVerifierRequest(
		sourcePrivate,
		plumtreeCertificate{kind: plumtreeCertificateEmpty},
		testPeerID("plumtree-signature-cache-from"),
	)
	now := time.Now()

	signatureKey, cacheable := verifiedSignatureCacheKey(request)
	if !cacheable {
		t.Fatal("well-formed request reported as non-cacheable")
	}
	if _, err := verifier.verifyAt(request, now); err != nil {
		t.Fatalf("verify authorized source: %v", err)
	}
	if !verifier.verified.contains(signatureKey) {
		t.Fatal("verified signature tuple was not cached")
	}

	// The same part announced by another peer reuses the cached source
	// signature.
	request.From = testPeerID("plumtree-signature-cache-other-from")
	if _, err := verifier.verifyAt(request, now); err != nil {
		t.Fatalf("verify cached signature from another peer: %v", err)
	}
}

func TestPlumtreeSignatureVerifierSignatureCacheHitSkipsKeyMath(t *testing.T) {
	_, sourcePrivate, sourceID := plumtreeVerifierKey(t)
	node := &Node{}
	node.SetPlumtreePolicy(NewPlumtreePolicy([]PeerID{sourceID}))
	verifier := newPlumtreeSignatureVerifier(
		nodePlumtreePolicySource{node: node},
		testPeerID("plumtree-signature-hit-overlay"),
	)
	request := plumtreeVerifierRequest(
		sourcePrivate,
		plumtreeCertificate{kind: plumtreeCertificateEmpty},
		testPeerID("plumtree-signature-hit-from"),
	)
	request.Signature[0] ^= 1

	// Seeding the cache with this exact tuple must make verification pass
	// without re-running ed25519.Verify, proving hits skip the key math and
	// that lookups cover the full tuple including the signature bytes.
	signatureKey, cacheable := verifiedSignatureCacheKey(request)
	if !cacheable {
		t.Fatal("well-formed request reported as non-cacheable")
	}
	verifier.verified.add(signatureKey)
	if _, err := verifier.verifyAt(request, time.Now()); err != nil {
		t.Fatalf("verify seeded signature tuple: %v", err)
	}
}

func TestPlumtreeSignatureVerifierDoesNotCacheInvalidSignature(t *testing.T) {
	_, sourcePrivate, sourceID := plumtreeVerifierKey(t)
	node := &Node{}
	node.SetPlumtreePolicy(NewPlumtreePolicy([]PeerID{sourceID}))
	verifier := newPlumtreeSignatureVerifier(
		nodePlumtreePolicySource{node: node},
		testPeerID("plumtree-signature-nocache-overlay"),
	)
	request := plumtreeVerifierRequest(
		sourcePrivate,
		plumtreeCertificate{kind: plumtreeCertificateEmpty},
		testPeerID("plumtree-signature-nocache-from"),
	)
	request.Signature[0] ^= 1

	if _, err := verifier.verifyAt(request, time.Now()); !errors.Is(
		err,
		errPlumtreeSignatureInvalid,
	) {
		t.Fatalf("invalid signature error = %v", err)
	}
	signatureKey, _ := verifiedSignatureCacheKey(request)
	if verifier.verified.contains(signatureKey) {
		t.Fatal("invalid signature tuple was cached")
	}
}

func TestPlumtreeSignatureVerifierRejectionPrecedesSignatureCache(t *testing.T) {
	_, sourcePrivate, sourceID := plumtreeVerifierKey(t)
	node := &Node{}
	node.SetPlumtreePolicy(NewPlumtreePolicy([]PeerID{sourceID}))
	verifier := newPlumtreeSignatureVerifier(
		nodePlumtreePolicySource{node: node},
		testPeerID("plumtree-signature-reject-cache-overlay"),
	)
	from := testPeerID("plumtree-signature-reject-cache-from")
	now := time.Now()

	request := plumtreeVerifierRequest(
		sourcePrivate,
		plumtreeCertificate{kind: plumtreeCertificateEmpty},
		from,
	)
	if _, err := verifier.verifyAt(request, now); err != nil {
		t.Fatalf("verify authorized source: %v", err)
	}

	bad := request
	bad.Signature = append([]byte(nil), request.Signature...)
	bad.Signature[0] ^= 1
	if _, err := verifier.verifyAt(bad, now); !errors.Is(
		err,
		errPlumtreeSignatureInvalid,
	) {
		t.Fatalf("invalid signature error = %v", err)
	}

	// The peer rejection window must still apply even though this exact
	// tuple sits in the verified signature cache.
	if _, err := verifier.verifyAt(request, now.Add(time.Second)); !errors.Is(
		err,
		errPlumtreePeerSignaturesRejected,
	) {
		t.Fatalf("temporarily rejected signature error = %v", err)
	}
}

func BenchmarkPlumtreeSignatureVerifierCachedSignature(b *testing.B) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		b.Fatalf("generate key: %v", err)
	}
	sourceID, err := peerIDFromED25519PublicKey(publicKey)
	if err != nil {
		b.Fatalf("derive peer ID: %v", err)
	}
	node := &Node{}
	node.SetPlumtreePolicy(NewPlumtreePolicy([]PeerID{sourceID}))
	verifier := newPlumtreeSignatureVerifier(
		nodePlumtreePolicySource{node: node},
		testPeerID("plumtree-signature-bench-overlay"),
	)
	request := plumtreeVerifierRequest(
		privateKey,
		plumtreeCertificate{kind: plumtreeCertificateEmpty},
		testPeerID("plumtree-signature-bench-from"),
	)
	now := time.Now()
	if _, err = verifier.verifyAt(request, now); err != nil {
		b.Fatalf("warm signature cache: %v", err)
	}

	b.ReportAllocs()
	for b.Loop() {
		if _, err = verifier.verifyAt(request, now); err != nil {
			b.Fatalf("verify cached signature: %v", err)
		}
	}
}

func plumtreeCertifiedVerifierRequest(
	t *testing.T,
	issuerPublic ed25519.PublicKey,
	issuerPrivate ed25519.PrivateKey,
	overlayID,
	from PeerID,
	expireAt int32,
	maxSize uint32,
) plumtreeVerification {
	t.Helper()

	_, sourcePrivate, sourceID := plumtreeVerifierKey(t)
	certificate := overlay.Certificate{
		IssuedBy: keys.PublicKeyED25519{Key: issuerPublic},
		ExpireAt: uint32(expireAt),
		MaxSize:  maxSize,
	}
	certificate.Signature = plumtreeSignTL(t, issuerPrivate, overlay.CertificateId{
		OverlayID: overlayID[:],
		Node:      sourceID[:],
		ExpireAt:  uint32(expireAt),
		MaxSize:   maxSize,
	})
	return plumtreeVerifierRequest(
		sourcePrivate,
		plumtreeCertificate{
			kind:   plumtreeCertificateLegacy,
			legacy: certificate,
		},
		from,
	)
}

func plumtreeVerifierRequest(
	sourcePrivate ed25519.PrivateKey,
	certificate plumtreeCertificate,
	from PeerID,
) plumtreeVerification {
	signedData := []byte("plumtree verifier payload")
	return plumtreeVerification{
		From: from,
		Source: keys.PublicKeyED25519{
			Key: sourcePrivate.Public().(ed25519.PublicKey),
		},
		Certificate: certificate,
		DataSize:    uint32(len(signedData)),
		SignedData:  signedData,
		Signature:   ed25519.Sign(sourcePrivate, signedData),
	}
}

func plumtreeVerifierKey(
	t *testing.T,
) (ed25519.PublicKey, ed25519.PrivateKey, PeerID) {
	t.Helper()

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate verifier key: %v", err)
	}
	id, err := peerIDFromED25519PublicKey(publicKey)
	if err != nil {
		t.Fatalf("derive verifier peer ID: %v", err)
	}
	return publicKey, privateKey, id
}

func plumtreeSignTL(
	t *testing.T,
	privateKey ed25519.PrivateKey,
	value any,
) []byte {
	t.Helper()

	data, err := tl.Serialize(value, true)
	if err != nil {
		t.Fatalf("serialize signed TL value: %v", err)
	}
	return ed25519.Sign(privateKey, data)
}
