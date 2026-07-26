package p2p

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

const (
	plumtreeCertificateAllowFEC uint32 = 1
	plumtreeCertificateTrusted  uint32 = 2

	plumtreeCertificateCacheLimit      = 100
	plumtreeCertificateCheckLimit      = 60
	plumtreeCertificateCheckWindow     = 60 * time.Second
	plumtreeSignatureRejectionDuration = 5 * time.Second
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

type plumtreeCertificateCache struct {
	values map[plumtreeCertificateCacheKey]struct{}
	order  [plumtreeCertificateCacheLimit]plumtreeCertificateCacheKey
	head   int
	size   int
}

func (c *plumtreeCertificateCache) contains(key plumtreeCertificateCacheKey) bool {
	_, ok := c.values[key]
	return ok
}

func (c *plumtreeCertificateCache) add(key plumtreeCertificateCacheKey) {
	if c.contains(key) {
		return
	}

	if c.size < plumtreeCertificateCacheLimit {
		index := (c.head + c.size) % plumtreeCertificateCacheLimit
		c.order[index] = key
		c.size++
		c.values[key] = struct{}{}
		return
	}

	delete(c.values, c.order[c.head])
	c.order[c.head] = key
	c.head = (c.head + 1) % plumtreeCertificateCacheLimit
	c.values[key] = struct{}{}
}

type plumtreeCertificateLimiter struct {
	checks    [plumtreeCertificateCheckLimit]time.Time
	checkHead int
	checkSize int
	checked   plumtreeCertificateCache
}

func newPlumtreeCertificateLimiter() *plumtreeCertificateLimiter {
	return &plumtreeCertificateLimiter{
		checked: plumtreeCertificateCache{
			values: make(
				map[plumtreeCertificateCacheKey]struct{},
				plumtreeCertificateCacheLimit,
			),
		},
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
	}
}

func (v *plumtreeSignatureVerifier) VerifyPlumtree(
	ctx context.Context,
	request plumtreeVerification,
) (PeerID, error) {
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
	source, err := v.precheckLocked(request, now)
	<-v.gate
	if err != nil {
		return PeerID{}, err
	}
	if err = v.checkSourceSignature(request, now); err != nil {
		return PeerID{}, err
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
	v.gate <- struct{}{}
	source, err := v.precheckLocked(request, now)
	<-v.gate
	if err != nil {
		return PeerID{}, err
	}
	if err = v.checkSourceSignature(request, now); err != nil {
		return PeerID{}, err
	}
	return source, nil
}

// precheckLocked performs every check that reads or writes verifier state, so
// that state stays gate-only. The one deliberately unchecked piece is the
// source signature itself: the caller runs that ed25519.Verify after releasing
// the gate, keeping the ~60us of key math off the per-overlay serialization
// point. The rate-limited certificate signature check stays here because its
// memoization cache is verifier state.
func (v *plumtreeSignatureVerifier) precheckLocked(
	request plumtreeVerification,
	now time.Time,
) (PeerID, error) {
	policy := v.policySource.current()
	v.updatePolicyLocked(policy)
	if !policy.enabled(v.workchain) {
		return PeerID{}, errPlumtreeDisabled
	}
	if request.DataSize == 0 || request.DataSize > uint32(plumtreeMaxPayloadSize) {
		return PeerID{}, errPlumtreeSourceNotAllowed
	}

	sourceID, err := peerIDFromED25519PublicKey(request.Source.Key)
	if err != nil {
		return PeerID{}, err
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
			return PeerID{}, err
		}
	}

	v.expireRejectedLocked(now)
	if until, ok := v.rejected[request.From]; ok && now.Before(until) {
		return PeerID{}, errPlumtreePeerSignaturesRejected
	}
	if len(request.Source.Key) != ed25519.PublicKeySize {
		return PeerID{}, fmt.Errorf(
			"invalid Plumtree Ed25519 public key size %d",
			len(request.Source.Key),
		)
	}
	return sourceID, nil
}

func (v *plumtreeSignatureVerifier) checkSourceSignature(
	request plumtreeVerification,
	now time.Time,
) error {
	if ed25519.Verify(request.Source.Key, request.SignedData, request.Signature) {
		return nil
	}

	// Re-acquire the gate only to record the rejection; concurrent verifiers
	// recording the same peer are harmless duplicate map writes.
	v.gate <- struct{}{}
	v.recordRejectionLocked(request.From, now)
	<-v.gate
	return errPlumtreeSignatureInvalid
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
