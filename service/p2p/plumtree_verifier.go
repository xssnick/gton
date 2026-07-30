package p2p

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"expvar"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

const (
	plumtreeCertificateAllowFEC uint32 = 1
	plumtreeCertificateTrusted  uint32 = 2

	plumtreeCertificateCacheLimit       = 100
	plumtreeCertificateCheckLimit       = 60
	plumtreeCertificateCheckWindow      = 60 * time.Second
	plumtreeSignatureRejectionDuration  = 5 * time.Second
	plumtreeVerifiedSignatureCacheLimit = 4096
)

var (
	errPlumtreeDisabled               = errors.New("Plumtree is disabled")
	errPlumtreeSourceNotAllowed       = errors.New("Plumtree source is not allowed")
	errPlumtreeCertificateRateLimited = errors.New("Plumtree certificate checks are rate limited")
	errPlumtreePeerSignaturesRejected = errors.New("Plumtree peer signatures are temporarily rejected")
	errPlumtreeSignatureInvalid       = errors.New("invalid Plumtree signature")

	ed25519PublicKeyConstructorID = tl.CRC("pub.ed25519 key:int256 = PublicKey")
)

type plumtreeCertificateFields struct {
	issuer    keys.PublicKeyED25519
	expireAt  int32
	maxSize   uint32
	flags     uint32
	signature []byte
}

type plumtreeCertificateCacheKey [PeerIDSize * 2]byte

type plumtreeRingSet[K comparable] struct {
	values map[K]struct{}
	order  []K
	head   int
	size   int
}

func newPlumtreeRingSet[K comparable](limit int) plumtreeRingSet[K] {
	return plumtreeRingSet[K]{
		values: make(map[K]struct{}, limit),
		order:  make([]K, limit),
	}
}

func (c *plumtreeRingSet[K]) contains(key K) bool {
	_, ok := c.values[key]
	return ok
}

func (c *plumtreeRingSet[K]) add(key K) {
	if c.contains(key) {
		return
	}

	if c.size < len(c.order) {
		index := (c.head + c.size) % len(c.order)
		c.order[index] = key
		c.size++
		c.values[key] = struct{}{}
		return
	}

	delete(c.values, c.order[c.head])
	c.order[c.head] = key
	c.head = (c.head + 1) % len(c.order)
	c.values[key] = struct{}{}
}

type plumtreeCertificateLimiter struct {
	checks    [plumtreeCertificateCheckLimit]time.Time
	checkHead int
	checkSize int
	checked   plumtreeRingSet[plumtreeCertificateCacheKey]
}

func newPlumtreeCertificateLimiter() *plumtreeCertificateLimiter {
	return &plumtreeCertificateLimiter{
		checked: newPlumtreeRingSet[plumtreeCertificateCacheKey](
			plumtreeCertificateCacheLimit,
		),
	}
}

func (l *plumtreeCertificateLimiter) allowCheck(now time.Time) bool {
	for l.checkSize > 0 &&
		now.Sub(l.checks[l.checkHead]) > plumtreeCertificateCheckWindow {
		l.checkHead = (l.checkHead + 1) % plumtreeCertificateCheckLimit
		l.checkSize--
	}
	return l.checkSize < plumtreeCertificateCheckLimit
}

func (l *plumtreeCertificateLimiter) addCheck(now time.Time) {
	index := (l.checkHead + l.checkSize) % plumtreeCertificateCheckLimit
	l.checks[index] = now
	l.checkSize++
}

type plumtreeRejectedPeer struct {
	id    PeerID
	until time.Time
}

type plumtreeSignatureVerifier struct {
	policySource plumtreePolicySource
	overlayID    PeerID
	workchain    int32

	gate           chan struct{}
	policy         *PlumtreePolicy
	limiters       map[PeerID]*plumtreeCertificateLimiter
	rejected       map[PeerID]time.Time
	rejectionQueue []plumtreeRejectedPeer
	rejectionHead  int
	scratch        []byte
	verified       plumtreeRingSet[plumtreeVerifiedSignature]

	signatureHits   atomic.Uint64
	signatureMisses atomic.Uint64
}

// SignatureCacheStats reports how often the cache spared a key verification.
// Monotonic since process start.
func (v *plumtreeSignatureVerifier) SignatureCacheStats() (hits, misses uint64) {
	return v.signatureHits.Load(), v.signatureMisses.Load()
}

// Process-wide counters behind /debug/vars, aggregated across every overlay's
// verifier so the IHAVE signature load can be read from a running node.
var (
	plumtreeSignatureHitsTotal   atomic.Uint64
	plumtreeSignatureMissesTotal atomic.Uint64
	plumtreeRepairRequestsTotal  atomic.Uint64
	plumtreeIHaveVerifiableTotal atomic.Uint64
)

func init() {
	expvar.Publish("plumtree_signatures", expvar.Func(func() any {
		return map[string]uint64{
			"cache_hits":      plumtreeSignatureHitsTotal.Load(),
			"verifications":   plumtreeSignatureMissesTotal.Load(),
			"ihave_verified":  plumtreeIHaveVerifiableTotal.Load(),
			"repairs_started": plumtreeRepairRequestsTotal.Load(),
		}
	}))
}

var _ plumtreeVerifier = (*plumtreeSignatureVerifier)(nil)

func newPlumtreeSignatureVerifier(
	policySource plumtreePolicySource,
	overlayID PeerID,
	workchain int32,
) *plumtreeSignatureVerifier {
	return &plumtreeSignatureVerifier{
		policySource: policySource,
		overlayID:    overlayID,
		workchain:    workchain,
		gate:         make(chan struct{}, 1),
		limiters:     make(map[PeerID]*plumtreeCertificateLimiter),
		rejected:     make(map[PeerID]time.Time),
		scratch:      make([]byte, 0, 256),
		verified: newPlumtreeRingSet[plumtreeVerifiedSignature](
			plumtreeVerifiedSignatureCacheLimit,
		),
	}
}

func (v *plumtreeSignatureVerifier) VerifyPlumtree(
	ctx context.Context,
	request plumtreeVerification,
) (PeerID, error) {
	signatureKey, cacheable := verifiedSignatureCacheKey(request)
	select {
	case v.gate <- struct{}{}:
	case <-ctx.Done():
		return PeerID{}, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		<-v.gate
		return PeerID{}, err
	}

	now := time.Now()
	source, verified, err := v.precheckLocked(request, signatureKey, cacheable, now)
	<-v.gate
	if err != nil {
		return PeerID{}, err
	}
	if !verified {
		if err = v.checkSourceSignature(request, signatureKey, cacheable, now); err != nil {
			return PeerID{}, err
		}
	}
	if err = ctx.Err(); err != nil {
		return PeerID{}, err
	}
	return source, nil
}

func (v *plumtreeSignatureVerifier) verifyAt(
	request plumtreeVerification,
	now time.Time,
) (PeerID, error) {
	signatureKey, cacheable := verifiedSignatureCacheKey(request)
	v.gate <- struct{}{}
	source, verified, err := v.precheckLocked(request, signatureKey, cacheable, now)
	<-v.gate
	if err != nil {
		return PeerID{}, err
	}
	if !verified {
		if err = v.checkSourceSignature(request, signatureKey, cacheable, now); err != nil {
			return PeerID{}, err
		}
	}
	return source, nil
}

// A comparable copy of the whole verified tuple, so a hit certifies exactly what
// ed25519.Verify would re-establish. Raw bytes rather than a digest: lookups stay
// on the map's native hash and collisions are ruled out. dataLen disambiguates
// zero-padded payloads of different sizes.
type plumtreeVerifiedSignature struct {
	source     [ed25519.PublicKeySize]byte
	signature  [ed25519.SignatureSize]byte
	signedData [plumtreeFECToSignWireSize]byte
	dataLen    uint8
}

// verifiedSignatureCacheKey reports oversized or malformed components as
// non-cacheable; those take the full verification path.
func verifiedSignatureCacheKey(
	request plumtreeVerification,
) (plumtreeVerifiedSignature, bool) {
	if len(request.Source.Key) != ed25519.PublicKeySize ||
		len(request.Signature) != ed25519.SignatureSize ||
		len(request.SignedData) > plumtreeFECToSignWireSize {
		return plumtreeVerifiedSignature{}, false
	}

	var key plumtreeVerifiedSignature
	copy(key.source[:], request.Source.Key)
	copy(key.signature[:], request.Signature)
	copy(key.signedData[:], request.SignedData)
	key.dataLen = uint8(len(request.SignedData))
	return key, true
}

// Every check that reads or writes verifier state, so that state stays gate-only.
// The source signature is deliberately left to the caller, which verifies it
// after releasing the gate: ~60us of key math off the per-overlay serialization
// point. The certificate check stays because its memoization cache is verifier
// state. The returned flag reports an already-verified tuple.
func (v *plumtreeSignatureVerifier) precheckLocked(
	request plumtreeVerification,
	signatureKey plumtreeVerifiedSignature,
	cacheable bool,
	now time.Time,
) (PeerID, bool, error) {
	policy := v.policySource.current()
	v.updatePolicyLocked(policy)
	if !policy.enabled(v.workchain) {
		return PeerID{}, false, errPlumtreeDisabled
	}
	if request.DataSize == 0 || request.DataSize > uint32(plumtreeMaxPayloadSize) {
		return PeerID{}, false, errPlumtreeSourceNotAllowed
	}

	sourceID, err := peerIDFromED25519PublicKey(request.Source.Key)
	if err != nil {
		return PeerID{}, false, err
	}
	if !policy.authorizes(sourceID, request.DataSize) {
		if err = v.checkCertificateLocked(
			policy,
			sourceID,
			request.Certificate,
			request.DataSize,
			request.From,
			now,
		); err != nil {
			return PeerID{}, false, err
		}
	}

	v.expireRejectedLocked(now)
	if until, ok := v.rejected[request.From]; ok && now.Before(until) {
		return PeerID{}, false, errPlumtreePeerSignaturesRejected
	}
	if len(request.Source.Key) != ed25519.PublicKeySize {
		return PeerID{}, false, fmt.Errorf(
			"invalid Plumtree Ed25519 public key size %d",
			len(request.Source.Key),
		)
	}
	if cacheable && v.verified.contains(signatureKey) {
		v.signatureHits.Add(1)
		plumtreeSignatureHitsTotal.Add(1)
		return sourceID, true, nil
	}
	v.signatureMisses.Add(1)
	plumtreeSignatureMissesTotal.Add(1)
	return sourceID, false, nil
}

func (v *plumtreeSignatureVerifier) checkSourceSignature(
	request plumtreeVerification,
	signatureKey plumtreeVerifiedSignature,
	cacheable bool,
	now time.Time,
) error {
	valid := ed25519.Verify(request.Source.Key, request.SignedData, request.Signature)
	if valid && !cacheable {
		return nil
	}

	// Only to record the outcome; concurrent verifiers landing the same signature
	// or rejection are harmless duplicates.
	v.gate <- struct{}{}
	if valid {
		v.verified.add(signatureKey)
	} else {
		v.recordRejectionLocked(request.From, now)
	}
	<-v.gate
	if !valid {
		return errPlumtreeSignatureInvalid
	}
	return nil
}

func (v *plumtreeSignatureVerifier) updatePolicyLocked(policy *PlumtreePolicy) {
	if v.policy == policy {
		return
	}
	for issuer := range v.limiters {
		if _, ok := policy.authorizedKeys[issuer]; !ok {
			delete(v.limiters, issuer)
		}
	}
	v.policy = policy
}

func (v *plumtreeSignatureVerifier) checkCertificateLocked(
	policy *PlumtreePolicy,
	source PeerID,
	certificate plumtreeCertificate,
	dataSize uint32,
	from PeerID,
	now time.Time,
) error {
	fields, err := plumtreeCertificateData(certificate)
	if err != nil {
		return err
	}
	if dataSize > fields.maxSize ||
		int32(now.Unix()) > fields.expireAt ||
		fields.flags&plumtreeCertificateAllowFEC == 0 {
		return errPlumtreeSourceNotAllowed
	}

	issuer, err := peerIDFromED25519PublicKey(fields.issuer.Key)
	if err != nil {
		return err
	}
	if !policy.authorizes(issuer, dataSize) {
		return errPlumtreeSourceNotAllowed
	}

	certificateHash, err := v.certificateHashLocked(fields)
	if err != nil {
		return fmt.Errorf("hash Plumtree certificate: %w", err)
	}
	var cacheKey plumtreeCertificateCacheKey
	copy(cacheKey[:PeerIDSize], source[:])
	copy(cacheKey[PeerIDSize:], certificateHash[:])

	limiter := v.limiters[issuer]
	if limiter == nil {
		limiter = newPlumtreeCertificateLimiter()
		v.limiters[issuer] = limiter
	}
	if !limiter.checked.contains(cacheKey) {
		if !limiter.allowCheck(now) {
			return errPlumtreeCertificateRateLimited
		}

		toSign, err := v.certificateToSignLocked(fields, source)
		if err != nil {
			return fmt.Errorf("serialize Plumtree certificate signature payload: %w", err)
		}
		if err = v.checkSignatureLocked(
			fields.issuer.Key,
			toSign,
			fields.signature,
			from,
			now,
		); err != nil {
			return err
		}

		limiter.addCheck(now)
		limiter.checked.add(cacheKey)
	}
	if fields.flags&plumtreeCertificateTrusted == 0 {
		return errPlumtreeSourceNotAllowed
	}
	return nil
}

func plumtreeCertificateData(
	certificate plumtreeCertificate,
) (plumtreeCertificateFields, error) {
	var issuerValue any
	var fields plumtreeCertificateFields

	switch certificate.kind {
	case plumtreeCertificateLegacy:
		issuerValue = certificate.legacy.IssuedBy
		fields.expireAt = int32(certificate.legacy.ExpireAt)
		fields.maxSize = certificate.legacy.MaxSize
		fields.flags = plumtreeLegacyCertificateFlags(fields.maxSize)
		fields.signature = certificate.legacy.Signature
	case plumtreeCertificateV2:
		issuerValue = certificate.v2.IssuedBy
		fields.expireAt = int32(certificate.v2.ExpireAt)
		fields.maxSize = certificate.v2.MaxSize
		fields.flags = uint32(certificate.v2.Flags)
		fields.signature = certificate.v2.Signature
	default:
		return plumtreeCertificateFields{}, errPlumtreeSourceNotAllowed
	}

	issuer, ok := issuerValue.(keys.PublicKeyED25519)
	if !ok {
		return plumtreeCertificateFields{}, fmt.Errorf(
			"unsupported Plumtree certificate issuer type %T",
			issuerValue,
		)
	}
	fields.issuer = issuer
	return fields, nil
}

func plumtreeLegacyCertificateFlags(maxSize uint32) uint32 {
	flags := plumtreeCertificateTrusted
	if maxSize > ordinarySimpleBroadcastMaxSize {
		flags |= plumtreeCertificateAllowFEC
	}
	return flags
}

func (v *plumtreeSignatureVerifier) certificateHashLocked(
	fields plumtreeCertificateFields,
) ([sha256.Size]byte, error) {
	var certificate any
	if fields.flags == plumtreeLegacyCertificateFlags(fields.maxSize) {
		certificate = overlay.Certificate{
			IssuedBy:  fields.issuer,
			ExpireAt:  uint32(fields.expireAt),
			MaxSize:   fields.maxSize,
			Signature: fields.signature,
		}
	} else {
		certificate = overlay.CertificateV2{
			IssuedBy:  fields.issuer,
			ExpireAt:  uint32(fields.expireAt),
			MaxSize:   fields.maxSize,
			Flags:     int32(fields.flags),
			Signature: fields.signature,
		}
	}

	encoded, err := tl.Append(v.scratch[:0], certificate, true)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	v.scratch = encoded
	return sha256.Sum256(encoded), nil
}

func (v *plumtreeSignatureVerifier) certificateToSignLocked(
	fields plumtreeCertificateFields,
	source PeerID,
) ([]byte, error) {
	var certificateID any
	if fields.flags == plumtreeLegacyCertificateFlags(fields.maxSize) {
		certificateID = overlay.CertificateId{
			OverlayID: v.overlayID[:],
			Node:      source[:],
			ExpireAt:  uint32(fields.expireAt),
			MaxSize:   fields.maxSize,
		}
	} else {
		certificateID = overlay.CertificateIdV2{
			OverlayID: v.overlayID[:],
			Node:      source[:],
			ExpireAt:  uint32(fields.expireAt),
			MaxSize:   fields.maxSize,
			Flags:     int32(fields.flags),
		}
	}

	encoded, err := tl.Append(v.scratch[:0], certificateID, true)
	if err != nil {
		return nil, err
	}
	v.scratch = encoded
	return encoded, nil
}

func (v *plumtreeSignatureVerifier) checkSignatureLocked(
	publicKey ed25519.PublicKey,
	message,
	signature []byte,
	from PeerID,
	now time.Time,
) error {
	v.expireRejectedLocked(now)
	if until, ok := v.rejected[from]; ok && now.Before(until) {
		return errPlumtreePeerSignaturesRejected
	}
	if len(publicKey) != ed25519.PublicKeySize {
		return fmt.Errorf("invalid Plumtree Ed25519 public key size %d", len(publicKey))
	}
	if ed25519.Verify(publicKey, message, signature) {
		return nil
	}

	v.recordRejectionLocked(from, now)
	return errPlumtreeSignatureInvalid
}

func (v *plumtreeSignatureVerifier) recordRejectionLocked(
	from PeerID,
	now time.Time,
) {
	if from.IsZero() {
		return
	}
	until := now.Add(plumtreeSignatureRejectionDuration)
	// A verifier stalled between precheck and the outside-gate Verify records
	// with its older timestamp; never let that shorten an active rejection.
	if current, ok := v.rejected[from]; ok && !until.After(current) {
		return
	}
	v.rejected[from] = until
	v.rejectionQueue = append(v.rejectionQueue, plumtreeRejectedPeer{
		id:    from,
		until: until,
	})
}

func (v *plumtreeSignatureVerifier) expireRejectedLocked(now time.Time) {
	for v.rejectionHead < len(v.rejectionQueue) {
		rejected := v.rejectionQueue[v.rejectionHead]
		if now.Before(rejected.until) {
			break
		}
		if until, ok := v.rejected[rejected.id]; ok && until.Equal(rejected.until) {
			delete(v.rejected, rejected.id)
		}
		v.rejectionHead++
	}

	if v.rejectionHead == len(v.rejectionQueue) {
		v.rejectionQueue = v.rejectionQueue[:0]
		v.rejectionHead = 0
		return
	}
	if v.rejectionHead < 1024 || v.rejectionHead*2 < len(v.rejectionQueue) {
		return
	}

	copy(v.rejectionQueue, v.rejectionQueue[v.rejectionHead:])
	v.rejectionQueue = v.rejectionQueue[:len(v.rejectionQueue)-v.rejectionHead]
	v.rejectionHead = 0
}

func peerIDFromED25519PublicKey(publicKey ed25519.PublicKey) (PeerID, error) {
	if len(publicKey) != ed25519.PublicKeySize {
		return PeerID{}, fmt.Errorf("invalid Ed25519 public key size %d", len(publicKey))
	}

	var boxed [4 + ed25519.PublicKeySize]byte
	binary.LittleEndian.PutUint32(boxed[:4], ed25519PublicKeyConstructorID)
	copy(boxed[4:], publicKey)
	return PeerID(sha256.Sum256(boxed[:])), nil
}
