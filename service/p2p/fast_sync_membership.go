package p2p

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

const (
	// OverlayImpl keeps these limits independently for every root issuer.
	fastSyncMemberCertificateCacheLimit = 100
	fastSyncMemberCertificateCheckLimit = 60

	fastSyncMemberCertificateCheckWindow = 60 * time.Second
	fastSyncBadSignatureSourceCooldown   = 5 * time.Second
)

var (
	errFastSyncMemberCertificateRequired = errors.New(
		"fast sync membership: member certificate required",
	)
	errFastSyncUnknownMemberCertificateIssuer = errors.New(
		"fast sync membership: unknown member certificate issuer",
	)
	errFastSyncMemberCertificateSuperseded = errors.New(
		"fast sync membership: member certificate superseded",
	)
	errFastSyncMemberCertificateRateLimited = errors.New(
		"fast sync membership: member certificate check rate limited",
	)
	errFastSyncSignatureSourceBlocked = errors.New(
		"fast sync membership: signature source temporarily blocked",
	)
)

type fastSyncMembershipCounts struct {
	Roots              int
	Permanent          int
	Enrolled           int
	CachedCertificates int
}

type fastSyncMembership struct {
	mu sync.Mutex

	roots              map[PeerID]*fastSyncMembershipRoot
	permanent          map[PeerID]fastSyncPermanentMember
	enrolled           map[PeerID]fastSyncEnrolledMember
	blockedSources     map[PeerID]time.Time
	blockedDeadlines   []fastSyncBlockedSource
	cachedCertificates int
}

type fastSyncMembershipRoot struct {
	slots        [FastSyncMemberSlotCount]fastSyncMemberSlotWinner
	checks       fastSyncCertificateCheckWindow
	certificates fastSyncMemberCertificateCache
}

type fastSyncMemberSlotWinner struct {
	node     PeerID
	expireAt int32
}

type fastSyncStoredMemberCertificate struct {
	value  overlay.MemberCertificate
	issuer PeerID
	hash   [sha256.Size]byte
}

type fastSyncPermanentMember struct {
	// Permanent authorization ignores the optional certificate; roster
	// demotion may retain it.
	flags          uint32
	certificate    fastSyncStoredMemberCertificate
	hasCertificate bool
}

type fastSyncEnrolledMember struct {
	flags       uint32
	certificate fastSyncStoredMemberCertificate
}

type fastSyncPreparedMemberCertificate struct {
	issuer    PeerID
	issuerKey keys.PublicKeyED25519
	hash      [sha256.Size]byte
}

type fastSyncMemberCertificateCacheKey struct {
	node PeerID
	hash [sha256.Size]byte
}

type fastSyncMemberCertificateCache struct {
	entries map[fastSyncMemberCertificateCacheKey]struct{}
	order   [fastSyncMemberCertificateCacheLimit]fastSyncMemberCertificateCacheKey
	head    uint8
}

type fastSyncCertificateCheckWindow struct {
	times [fastSyncMemberCertificateCheckLimit]time.Time
	head  uint8
	count uint8
}

type fastSyncBlockedSource struct {
	node  PeerID
	until time.Time
}

func newFastSyncMembership(
	roster FastSyncValidatorRoster,
	permanentFlags uint32,
) *fastSyncMembership {
	membership := &fastSyncMembership{
		roots:     make(map[PeerID]*fastSyncMembershipRoot),
		permanent: make(map[PeerID]fastSyncPermanentMember),
		enrolled:  make(map[PeerID]fastSyncEnrolledMember),
	}

	for _, root := range roster.RootPublicKeyIDs() {
		membership.roots[root] = newFastSyncMembershipRoot()
	}
	for _, peer := range roster.ADNLIDs() {
		membership.permanent[peer] = fastSyncPermanentMember{
			flags: permanentFlags,
		}
	}

	return membership
}

func newFastSyncMembershipRoot() *fastSyncMembershipRoot {
	return &fastSyncMembershipRoot{
		certificates: newFastSyncMemberCertificateCache(),
	}
}

func newFastSyncMemberCertificateCache() fastSyncMemberCertificateCache {
	return fastSyncMemberCertificateCache{
		entries: make(
			map[fastSyncMemberCertificateCacheKey]struct{},
			fastSyncMemberCertificateCacheLimit,
		),
	}
}

func (m *fastSyncMembership) UpdateRoster(
	roster FastSyncValidatorRoster,
	permanentFlags uint32,
	now time.Time,
) {
	rootIDs := roster.RootPublicKeyIDs()
	permanentIDs := roster.ADNLIDs()

	m.mu.Lock()
	defer m.mu.Unlock()

	roots := make(map[PeerID]*fastSyncMembershipRoot, len(rootIDs))
	cachedCertificates := 0
	for _, id := range rootIDs {
		root := m.roots[id]
		if root == nil {
			root = newFastSyncMembershipRoot()
		}

		roots[id] = root
		cachedCertificates += root.certificates.len()
	}
	m.roots = roots
	m.cachedCertificates = cachedCertificates

	permanent := make(
		map[PeerID]fastSyncPermanentMember,
		len(permanentIDs),
	)
	for _, id := range permanentIDs {
		if member, found := m.permanent[id]; found {
			permanent[id] = fastSyncPermanentMember{flags: member.flags}
			continue
		}

		if member, found := m.enrolled[id]; found {
			permanent[id] = fastSyncPermanentMember{flags: member.flags}
			continue
		}

		permanent[id] = fastSyncPermanentMember{flags: permanentFlags}
	}

	enrolled := make(
		map[PeerID]fastSyncEnrolledMember,
		len(m.enrolled)+len(m.permanent),
	)
	for node, member := range m.enrolled {
		if _, nowPermanent := permanent[node]; !nowPermanent {
			enrolled[node] = member
		}
	}
	for node, member := range m.permanent {
		if _, stillPermanent := permanent[node]; stillPermanent ||
			!member.hasCertificate {
			continue
		}

		enrolled[node] = fastSyncEnrolledMember{
			flags:       member.flags,
			certificate: member.certificate,
		}
	}

	m.permanent = permanent
	for node, member := range enrolled {
		if err := m.validatePreparedMemberCertificateLocked(
			node,
			member.certificate.value,
			fastSyncPreparedMemberCertificate{
				issuer: member.certificate.issuer,
				hash:   member.certificate.hash,
			},
			now,
			false,
		); err != nil {
			delete(enrolled, node)
		}
	}
	m.enrolled = enrolled
}

func (m *fastSyncMembership) AuthorizeMember(
	node PeerID,
	certificate overlay.MemberCertificate,
	now time.Time,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	prepared, err := m.validateMemberCertificateLocked(
		node,
		certificate,
		now,
		true,
	)
	if err != nil {
		return err
	}

	if member, found := m.permanent[node]; found {
		if !member.hasCertificate ||
			shouldReplaceFastSyncStoredCertificate(
				member.certificate,
				certificate,
				now,
			) {
			member.certificate = storeFastSyncMemberCertificate(
				certificate,
				prepared,
			)
			member.hasCertificate = true
			m.permanent[node] = member
		}
		return nil
	}

	if member, found := m.enrolled[node]; found {
		if shouldReplaceFastSyncStoredCertificate(
			member.certificate,
			certificate,
			now,
		) {
			member.certificate = storeFastSyncMemberCertificate(
				certificate,
				prepared,
			)
			m.enrolled[node] = member
		}
	}

	return nil
}

func (m *fastSyncMembership) AuthorizeOmitted(
	node PeerID,
	now time.Time,
) error {
	return m.authorizeStoredMember(node, now)
}

func (m *fastSyncMembership) authorizeStoredMember(
	node PeerID,
	now time.Time,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, permanent := m.permanent[node]; permanent {
		return nil
	}

	member, found := m.enrolled[node]
	if !found {
		return errFastSyncMemberCertificateRequired
	}

	return m.validatePreparedMemberCertificateLocked(
		node,
		member.certificate.value,
		fastSyncPreparedMemberCertificate{
			issuer: member.certificate.issuer,
			hash:   member.certificate.hash,
		},
		now,
		true,
	)
}

func (m *fastSyncMembership) EnrollNode(
	node PeerID,
	nodeFlags uint32,
	certificate overlay.MemberCertificate,
	now time.Time,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	prepared, err := m.validateMemberCertificateLocked(
		node,
		certificate,
		now,
		false,
	)
	if err != nil {
		return err
	}

	if member, permanent := m.permanent[node]; permanent {
		member.flags = nodeFlags
		if !member.hasCertificate ||
			shouldReplaceFastSyncStoredCertificate(
				member.certificate,
				certificate,
				now,
			) {
			member.certificate = storeFastSyncMemberCertificate(
				certificate,
				prepared,
			)
			member.hasCertificate = true
		}
		m.permanent[node] = member
		return nil
	}

	member, enrolled := m.enrolled[node]
	member.flags = nodeFlags
	if !enrolled || shouldReplaceFastSyncStoredCertificate(
		member.certificate,
		certificate,
		now,
	) {
		member.certificate = storeFastSyncMemberCertificate(
			certificate,
			prepared,
		)
	}
	m.enrolled[node] = member
	return nil
}

func (m *fastSyncMembership) EnrollPermanentNode(
	node PeerID,
	nodeFlags uint32,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	member, permanent := m.permanent[node]
	if !permanent {
		return ErrFastSyncNotFound
	}

	member.flags = nodeFlags
	m.permanent[node] = member
	return nil
}

func (m *fastSyncMembership) PeerFlags(node PeerID) (uint32, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if member, permanent := m.permanent[node]; permanent {
		return member.flags, nil
	}
	if member, enrolled := m.enrolled[node]; enrolled {
		return member.flags, nil
	}

	return 0, ErrFastSyncNotFound
}

func (m *fastSyncMembership) Contains(node PeerID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, permanent := m.permanent[node]; permanent {
		return true
	}
	_, enrolled := m.enrolled[node]
	return enrolled
}

func (m *fastSyncMembership) IsPermanent(node PeerID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, permanent := m.permanent[node]
	return permanent
}

func (m *fastSyncMembership) Counts() fastSyncMembershipCounts {
	m.mu.Lock()
	defer m.mu.Unlock()

	return fastSyncMembershipCounts{
		Roots:              len(m.roots),
		Permanent:          len(m.permanent),
		Enrolled:           len(m.enrolled),
		CachedCertificates: m.cachedCertificates,
	}
}

func (m *fastSyncMembership) validateMemberCertificateLocked(
	node PeerID,
	certificate overlay.MemberCertificate,
	now time.Time,
	direct bool,
) (fastSyncPreparedMemberCertificate, error) {
	if certificate.IsExpired(now) {
		return fastSyncPreparedMemberCertificate{}, fmt.Errorf(
			"fast sync membership: member certificate expired",
		)
	}
	if err := certificate.CheckSlot(FastSyncMemberSlotCount); err != nil {
		return fastSyncPreparedMemberCertificate{}, fmt.Errorf(
			"fast sync membership: %w",
			err,
		)
	}

	issuerKey, valueType := certificate.IssuedBy.(keys.PublicKeyED25519)
	if !valueType {
		return fastSyncPreparedMemberCertificate{}, fmt.Errorf(
			"fast sync membership: unsupported member certificate issuer type %T",
			certificate.IssuedBy,
		)
	}

	// Senders attach the same certificate to every packet, so resolving the
	// issuer and hashing the certificate would otherwise run on each one:
	// IssuerID and the digest below each allocate a serialization buffer, and
	// the digest is what keys the verification cache, so the cache alone cannot
	// avoid them. A member we already store presents an identical certificate
	// until it rotates; reuse the issuer and digest computed back then. Every
	// check still runs below - only the derivation is skipped.
	issuer, reusedDigest, reused := m.storedCertificateDigestLocked(
		node,
		certificate,
	)
	if !reused {
		issuerBytes, err := certificate.IssuerID()
		if err != nil {
			return fastSyncPreparedMemberCertificate{}, fmt.Errorf(
				"fast sync membership: resolve member certificate issuer: %w",
				err,
			)
		}
		issuer, err = NewPeerID(issuerBytes)
		if err != nil {
			return fastSyncPreparedMemberCertificate{}, fmt.Errorf(
				"fast sync membership: resolve member certificate issuer: %w",
				err,
			)
		}
	}

	root, slot, err := m.precheckMemberCertificateLocked(
		node,
		certificate,
		issuer,
	)
	if err != nil {
		return fastSyncPreparedMemberCertificate{}, err
	}
	if len(certificate.Signature) != ed25519.SignatureSize {
		if !root.checks.allows(now) {
			return fastSyncPreparedMemberCertificate{}, errFastSyncMemberCertificateRateLimited
		}
		if direct && m.signatureSourceBlockedLocked(node, now) {
			return fastSyncPreparedMemberCertificate{}, errFastSyncSignatureSourceBlocked
		}

		m.rejectSignatureSourceLocked(node, now, direct)
		return fastSyncPreparedMemberCertificate{}, fmt.Errorf(
			"fast sync membership: invalid member certificate signature size %d",
			len(certificate.Signature),
		)
	}

	digest := reusedDigest
	if !reused {
		encoded, err := tl.Serialize(certificate, true)
		if err != nil {
			return fastSyncPreparedMemberCertificate{}, fmt.Errorf(
				"fast sync membership: serialize member certificate: %w",
				err,
			)
		}
		digest = sha256.Sum256(encoded)
	}
	prepared := fastSyncPreparedMemberCertificate{
		issuer:    issuer,
		issuerKey: issuerKey,
		hash:      digest,
	}

	if err := m.verifyMemberCertificateLocked(
		node,
		certificate,
		prepared,
		root,
		slot,
		now,
		direct,
	); err != nil {
		return fastSyncPreparedMemberCertificate{}, err
	}

	return prepared, nil
}

func (m *fastSyncMembership) validatePreparedMemberCertificateLocked(
	node PeerID,
	certificate overlay.MemberCertificate,
	prepared fastSyncPreparedMemberCertificate,
	now time.Time,
	direct bool,
) error {
	if certificate.IsExpired(now) {
		return fmt.Errorf(
			"fast sync membership: member certificate expired",
		)
	}
	if err := certificate.CheckSlot(FastSyncMemberSlotCount); err != nil {
		return fmt.Errorf("fast sync membership: %w", err)
	}

	root, slot, err := m.precheckMemberCertificateLocked(
		node,
		certificate,
		prepared.issuer,
	)
	if err != nil {
		return err
	}

	return m.verifyMemberCertificateLocked(
		node,
		certificate,
		prepared,
		root,
		slot,
		now,
		direct,
	)
}

func (m *fastSyncMembership) precheckMemberCertificateLocked(
	node PeerID,
	certificate overlay.MemberCertificate,
	issuer PeerID,
) (
	*fastSyncMembershipRoot,
	*fastSyncMemberSlotWinner,
	error,
) {
	root := m.roots[issuer]
	if root == nil {
		return nil, nil, errFastSyncUnknownMemberCertificateIssuer
	}

	slot := &root.slots[certificate.Slot]
	if certificate.ExpireAt < slot.expireAt {
		return nil, nil, errFastSyncMemberCertificateSuperseded
	}
	if certificate.ExpireAt == slot.expireAt &&
		bytes.Compare(node[:], slot.node[:]) < 0 {
		return nil, nil, errFastSyncMemberCertificateSuperseded
	}

	return root, slot, nil
}

func (m *fastSyncMembership) verifyMemberCertificateLocked(
	node PeerID,
	certificate overlay.MemberCertificate,
	prepared fastSyncPreparedMemberCertificate,
	root *fastSyncMembershipRoot,
	slot *fastSyncMemberSlotWinner,
	now time.Time,
	direct bool,
) error {
	cacheKey := fastSyncMemberCertificateCacheKey{
		node: node,
		hash: prepared.hash,
	}
	if !root.certificates.contains(cacheKey) {
		if !root.checks.allows(now) {
			return errFastSyncMemberCertificateRateLimited
		}
		if direct && m.signatureSourceBlockedLocked(node, now) {
			return errFastSyncSignatureSourceBlocked
		}
		if err := certificate.CheckSignature(node.Bytes()); err != nil {
			m.rejectSignatureSourceLocked(node, now, direct)
			return fmt.Errorf(
				"fast sync membership: invalid member certificate signature: %w",
				err,
			)
		}

		root.checks.record(now)
		m.cachedCertificates += root.certificates.add(cacheKey)
	}

	slot.expireAt = certificate.ExpireAt
	slot.node = node
	return nil
}

func (m *fastSyncMembership) rejectSignatureSourceLocked(
	node PeerID,
	now time.Time,
	direct bool,
) {
	if !direct || node.IsZero() {
		return
	}

	until := now.Add(fastSyncBadSignatureSourceCooldown)
	if m.blockedSources == nil {
		m.blockedSources = make(map[PeerID]time.Time)
	}
	m.blockedSources[node] = until
	m.pushBlockedDeadlineLocked(fastSyncBlockedSource{
		node:  node,
		until: until,
	})
}

func (m *fastSyncMembership) signatureSourceBlockedLocked(
	node PeerID,
	now time.Time,
) bool {
	m.pruneBlockedSourcesLocked(now)

	until, blocked := m.blockedSources[node]
	return blocked && now.Before(until)
}

func (m *fastSyncMembership) pushBlockedDeadlineLocked(
	source fastSyncBlockedSource,
) {
	m.blockedDeadlines = append(m.blockedDeadlines, source)
	index := len(m.blockedDeadlines) - 1
	for index > 0 {
		parent := (index - 1) / 2
		if !m.blockedDeadlines[index].until.Before(
			m.blockedDeadlines[parent].until,
		) {
			break
		}

		m.blockedDeadlines[index], m.blockedDeadlines[parent] =
			m.blockedDeadlines[parent], m.blockedDeadlines[index]
		index = parent
	}
}

func (m *fastSyncMembership) pruneBlockedSourcesLocked(now time.Time) {
	pruned := false
	for len(m.blockedDeadlines) > 0 &&
		!now.Before(m.blockedDeadlines[0].until) {
		expired := m.popBlockedDeadlineLocked()
		pruned = true
		if m.blockedSources[expired.node] == expired.until {
			delete(m.blockedSources, expired.node)
		}
	}
	if pruned && len(m.blockedDeadlines) == 0 {
		// Release buckets retained after a hostile burst of distinct bad
		// sources. The map is recreated only if another bad signature arrives.
		m.blockedDeadlines = nil
		m.blockedSources = nil
	}
}

func (m *fastSyncMembership) popBlockedDeadlineLocked() fastSyncBlockedSource {
	expired := m.blockedDeadlines[0]
	last := len(m.blockedDeadlines) - 1
	m.blockedDeadlines[0] = m.blockedDeadlines[last]
	m.blockedDeadlines[last] = fastSyncBlockedSource{}
	m.blockedDeadlines = m.blockedDeadlines[:last]

	index := 0
	for {
		left := 2*index + 1
		if left >= len(m.blockedDeadlines) {
			break
		}

		child := left
		right := left + 1
		if right < len(m.blockedDeadlines) &&
			m.blockedDeadlines[right].until.Before(
				m.blockedDeadlines[left].until,
			) {
			child = right
		}
		if !m.blockedDeadlines[child].until.Before(
			m.blockedDeadlines[index].until,
		) {
			break
		}

		m.blockedDeadlines[index], m.blockedDeadlines[child] =
			m.blockedDeadlines[child], m.blockedDeadlines[index]
		index = child
	}

	return expired
}

func cloneFastSyncMemberCertificate(
	certificate overlay.MemberCertificate,
	issuer keys.PublicKeyED25519,
) overlay.MemberCertificate {
	issuer.Key = slices.Clone(issuer.Key)
	certificate.IssuedBy = issuer
	certificate.Signature = slices.Clone(certificate.Signature)
	return certificate
}

// storedCertificateDigestLocked returns the issuer and digest already computed
// for this node's stored certificate, when the presented certificate is
// identical to it. A certificate is a fixed-size record, so comparing it is far
// cheaper than re-deriving the digest.
func (m *fastSyncMembership) storedCertificateDigestLocked(
	node PeerID,
	certificate overlay.MemberCertificate,
) (PeerID, [sha256.Size]byte, bool) {
	if member, found := m.permanent[node]; found && member.hasCertificate {
		if sameFastSyncMemberCertificate(member.certificate.value, certificate) {
			return member.certificate.issuer, member.certificate.hash, true
		}
		return PeerID{}, [sha256.Size]byte{}, false
	}
	if member, found := m.enrolled[node]; found {
		if sameFastSyncMemberCertificate(member.certificate.value, certificate) {
			return member.certificate.issuer, member.certificate.hash, true
		}
	}
	return PeerID{}, [sha256.Size]byte{}, false
}

// sameFastSyncMemberCertificate reports whether two certificates carry the same
// bytes on the wire. It compares every serialized field, so equal certificates
// necessarily share a digest.
func sameFastSyncMemberCertificate(
	stored overlay.MemberCertificate,
	incoming overlay.MemberCertificate,
) bool {
	if stored.Flags != incoming.Flags ||
		stored.Slot != incoming.Slot ||
		stored.ExpireAt != incoming.ExpireAt ||
		!bytes.Equal(stored.Signature, incoming.Signature) {
		return false
	}
	storedKey, valueType := stored.IssuedBy.(keys.PublicKeyED25519)
	if !valueType {
		return false
	}
	incomingKey, valueType := incoming.IssuedBy.(keys.PublicKeyED25519)
	if !valueType {
		return false
	}
	return bytes.Equal(storedKey.Key, incomingKey.Key)
}

func storeFastSyncMemberCertificate(
	certificate overlay.MemberCertificate,
	prepared fastSyncPreparedMemberCertificate,
) fastSyncStoredMemberCertificate {
	return fastSyncStoredMemberCertificate{
		value: cloneFastSyncMemberCertificate(
			certificate,
			prepared.issuerKey,
		),
		issuer: prepared.issuer,
		hash:   prepared.hash,
	}
}

func shouldReplaceFastSyncStoredCertificate(
	stored fastSyncStoredMemberCertificate,
	incoming overlay.MemberCertificate,
	now time.Time,
) bool {
	// OverlayNode keeps its unexpired certificate unless the expiry advances.
	return stored.value.IsExpired(now) ||
		incoming.ExpireAt > stored.value.ExpireAt
}

func (c *fastSyncMemberCertificateCache) contains(
	key fastSyncMemberCertificateCacheKey,
) bool {
	// C++ checks LRUCache::contains, whose const lookup intentionally does not
	// refresh eviction order.
	_, found := c.entries[key]
	return found
}

func (c *fastSyncMemberCertificateCache) add(
	key fastSyncMemberCertificateCacheKey,
) int {
	if c.contains(key) {
		return 0
	}

	size := len(c.entries)
	if size < fastSyncMemberCertificateCacheLimit {
		index := (int(c.head) + size) %
			fastSyncMemberCertificateCacheLimit
		c.order[index] = key
		c.entries[key] = struct{}{}
		return 1
	}

	delete(c.entries, c.order[c.head])
	c.order[c.head] = key
	c.entries[key] = struct{}{}
	c.head = uint8(
		(int(c.head) + 1) % fastSyncMemberCertificateCacheLimit,
	)
	return 0
}

func (c *fastSyncMemberCertificateCache) len() int {
	return len(c.entries)
}

func (w *fastSyncCertificateCheckWindow) allows(now time.Time) bool {
	w.prune(now)
	return int(w.count) < fastSyncMemberCertificateCheckLimit
}

func (w *fastSyncCertificateCheckWindow) record(now time.Time) {
	index := (int(w.head) + int(w.count)) %
		fastSyncMemberCertificateCheckLimit
	w.times[index] = now
	w.count++
}

func (w *fastSyncCertificateCheckWindow) prune(now time.Time) {
	for w.count > 0 &&
		now.Sub(w.times[w.head]) > fastSyncMemberCertificateCheckWindow {
		w.times[w.head] = time.Time{}
		w.head = uint8(
			(int(w.head) + 1) % fastSyncMemberCertificateCheckLimit,
		)
		w.count--
	}
}
