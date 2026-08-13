package fastsync

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

// MembershipCounts is a point-in-time membership state summary.
type MembershipCounts struct {
	Roots              int
	Permanent          int
	Enrolled           int
	CachedCertificates int
}

// Membership owns certificate verification, enrollment, and roster state. It
// is safe for concurrent use.
type Membership struct {
	mu sync.Mutex

	roots              map[ID]*membershipRoot
	permanent          map[ID]permanentMember
	enrolled           map[ID]enrolledMember
	blockedSources     map[ID]time.Time
	blockedDeadlines   []blockedSource
	cachedCertificates int
}

type membershipRoot struct {
	slots        [memberSlotCount]memberSlotWinner
	checks       certificateCheckWindow
	certificates memberCertificateCache
}

type memberSlotWinner struct {
	node     ID
	expireAt int32
}

type storedMemberCertificate struct {
	value  overlay.MemberCertificate
	issuer ID
	hash   [sha256.Size]byte
}

type permanentMember struct {
	// Permanent authorization ignores the optional certificate; roster
	// demotion may retain it.
	flags          uint32
	certificate    storedMemberCertificate
	hasCertificate bool
}

type enrolledMember struct {
	flags       uint32
	certificate storedMemberCertificate
}

type preparedMemberCertificate struct {
	issuer    ID
	issuerKey keys.PublicKeyED25519
	hash      [sha256.Size]byte
}

type memberCertificateCacheKey struct {
	node ID
	hash [sha256.Size]byte
}

type memberCertificateCache struct {
	entries map[memberCertificateCacheKey]struct{}
	order   [fastSyncMemberCertificateCacheLimit]memberCertificateCacheKey
	head    uint8
}

type certificateCheckWindow struct {
	times [fastSyncMemberCertificateCheckLimit]time.Time
	head  uint8
	count uint8
}

type blockedSource struct {
	node  ID
	until time.Time
}

// NewMembership builds membership state without retaining either input slice.
func NewMembership[SourceID ID32](
	rootIDs,
	permanentIDs []SourceID,
	permanentFlags uint32,
) *Membership {
	membership := &Membership{
		roots:     make(map[ID]*membershipRoot, len(rootIDs)),
		permanent: make(map[ID]permanentMember, len(permanentIDs)),
		enrolled:  make(map[ID]enrolledMember),
	}

	for _, source := range rootIDs {
		root := ID(source)
		membership.roots[root] = newMembershipRoot()
	}
	for _, source := range permanentIDs {
		peer := ID(source)
		membership.permanent[peer] = permanentMember{
			flags: permanentFlags,
		}
	}

	return membership
}

func newMembershipRoot() *membershipRoot {
	return &membershipRoot{
		certificates: newMemberCertificateCache(),
	}
}

func newMemberCertificateCache() memberCertificateCache {
	return memberCertificateCache{
		entries: make(
			map[memberCertificateCacheKey]struct{},
			fastSyncMemberCertificateCacheLimit,
		),
	}
}

func (m *Membership) updateRoster(
	rootIDs,
	permanentIDs []ID,
	permanentFlags uint32,
	now time.Time,
) {
	m.mu.Lock()
	defer m.mu.Unlock()

	roots := make(map[ID]*membershipRoot, len(rootIDs))
	cachedCertificates := 0
	for _, id := range rootIDs {
		root := m.roots[id]
		if root == nil {
			root = newMembershipRoot()
		}

		roots[id] = root
		cachedCertificates += root.certificates.len()
	}
	m.roots = roots
	m.cachedCertificates = cachedCertificates

	permanent := make(
		map[ID]permanentMember,
		len(permanentIDs),
	)
	for _, id := range permanentIDs {
		if member, found := m.permanent[id]; found {
			permanent[id] = permanentMember{flags: member.flags}
			continue
		}

		if member, found := m.enrolled[id]; found {
			permanent[id] = permanentMember{flags: member.flags}
			continue
		}

		permanent[id] = permanentMember{flags: permanentFlags}
	}

	enrolled := make(
		map[ID]enrolledMember,
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

		enrolled[node] = enrolledMember{
			flags:       member.flags,
			certificate: member.certificate,
		}
	}

	m.permanent = permanent
	for node, member := range enrolled {
		if err := m.validatePreparedMemberCertificateLocked(
			node,
			member.certificate.value,
			preparedMemberCertificate{
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

func (m *Membership) AuthorizeMember(
	node ID,
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
			shouldReplaceStoredCertificate(
				member.certificate,
				certificate,
				now,
			) {
			member.certificate = storeMemberCertificate(
				certificate,
				prepared,
			)
			member.hasCertificate = true
			m.permanent[node] = member
		}
		return nil
	}

	if member, found := m.enrolled[node]; found {
		if shouldReplaceStoredCertificate(
			member.certificate,
			certificate,
			now,
		) {
			member.certificate = storeMemberCertificate(
				certificate,
				prepared,
			)
			m.enrolled[node] = member
		}
	}

	return nil
}

func (m *Membership) AuthorizeOmitted(
	node ID,
	now time.Time,
) error {
	return m.authorizeStoredMember(node, now)
}

func (m *Membership) authorizeStoredMember(
	node ID,
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
		preparedMemberCertificate{
			issuer: member.certificate.issuer,
			hash:   member.certificate.hash,
		},
		now,
		true,
	)
}

func (m *Membership) enrollCertifiedNode(
	node ID,
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
			shouldReplaceStoredCertificate(
				member.certificate,
				certificate,
				now,
			) {
			member.certificate = storeMemberCertificate(
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
	if !enrolled || shouldReplaceStoredCertificate(
		member.certificate,
		certificate,
		now,
	) {
		member.certificate = storeMemberCertificate(
			certificate,
			prepared,
		)
	}
	m.enrolled[node] = member
	return nil
}

func (m *Membership) updatePermanentNode(
	node ID,
	nodeFlags uint32,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	member, permanent := m.permanent[node]
	if !permanent {
		return ErrNotFound
	}

	member.flags = nodeFlags
	m.permanent[node] = member
	return nil
}

func (m *Membership) PeerFlags(node ID) (uint32, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if member, permanent := m.permanent[node]; permanent {
		return member.flags, nil
	}
	if member, enrolled := m.enrolled[node]; enrolled {
		return member.flags, nil
	}

	return 0, ErrNotFound
}

// Contains reports whether a peer is a member right now. Enrollment alone is not
// enough: an enrolled member whose certificate has expired keeps its map entry
// until the next roster update, and answering "member" for it would keep serving
// over ADNL/RLDP a peer the QUIC path already rejects. Only the stored expiry is
// checked - the signature was verified when the certificate was enrolled.
func (m *Membership) Contains(node ID, now time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, permanent := m.permanent[node]; permanent {
		return true
	}
	member, enrolled := m.enrolled[node]
	return enrolled && !member.certificate.value.IsExpired(now)
}

func (m *Membership) IsPermanent(node ID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, permanent := m.permanent[node]
	return permanent
}

func (m *Membership) counts() MembershipCounts {
	m.mu.Lock()
	defer m.mu.Unlock()

	return MembershipCounts{
		Roots:              len(m.roots),
		Permanent:          len(m.permanent),
		Enrolled:           len(m.enrolled),
		CachedCertificates: m.cachedCertificates,
	}
}

func (m *Membership) validateMemberCertificateLocked(
	node ID,
	certificate overlay.MemberCertificate,
	now time.Time,
	direct bool,
) (preparedMemberCertificate, error) {
	if certificate.IsExpired(now) {
		return preparedMemberCertificate{}, fmt.Errorf(
			"fast sync membership: member certificate expired",
		)
	}
	if err := certificate.CheckSlot(memberSlotCount); err != nil {
		return preparedMemberCertificate{}, fmt.Errorf(
			"fast sync membership: %w",
			err,
		)
	}

	issuerKey, valueType := certificate.IssuedBy.(keys.PublicKeyED25519)
	if !valueType {
		return preparedMemberCertificate{}, fmt.Errorf(
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
			return preparedMemberCertificate{}, fmt.Errorf(
				"fast sync membership: resolve member certificate issuer: %w",
				err,
			)
		}
		issuer, err = idFromBytes(issuerBytes)
		if err != nil {
			return preparedMemberCertificate{}, fmt.Errorf(
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
		return preparedMemberCertificate{}, err
	}
	if len(certificate.Signature) != ed25519.SignatureSize {
		if !root.checks.allows(now) {
			return preparedMemberCertificate{}, errFastSyncMemberCertificateRateLimited
		}
		if direct && m.signatureSourceBlockedLocked(node, now) {
			return preparedMemberCertificate{}, errFastSyncSignatureSourceBlocked
		}

		m.rejectSignatureSourceLocked(node, now, direct)
		return preparedMemberCertificate{}, fmt.Errorf(
			"fast sync membership: invalid member certificate signature size %d",
			len(certificate.Signature),
		)
	}

	digest := reusedDigest
	if !reused {
		encoded, err := tl.Serialize(certificate, true)
		if err != nil {
			return preparedMemberCertificate{}, fmt.Errorf(
				"fast sync membership: serialize member certificate: %w",
				err,
			)
		}
		digest = sha256.Sum256(encoded)
	}
	prepared := preparedMemberCertificate{
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
		return preparedMemberCertificate{}, err
	}

	return prepared, nil
}

func (m *Membership) validatePreparedMemberCertificateLocked(
	node ID,
	certificate overlay.MemberCertificate,
	prepared preparedMemberCertificate,
	now time.Time,
	direct bool,
) error {
	if certificate.IsExpired(now) {
		return fmt.Errorf(
			"fast sync membership: member certificate expired",
		)
	}
	if err := certificate.CheckSlot(memberSlotCount); err != nil {
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

func (m *Membership) precheckMemberCertificateLocked(
	node ID,
	certificate overlay.MemberCertificate,
	issuer ID,
) (
	*membershipRoot,
	*memberSlotWinner,
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

func (m *Membership) verifyMemberCertificateLocked(
	node ID,
	certificate overlay.MemberCertificate,
	prepared preparedMemberCertificate,
	root *membershipRoot,
	slot *memberSlotWinner,
	now time.Time,
	direct bool,
) error {
	cacheKey := memberCertificateCacheKey{
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
		nodeValue := [IDSize]byte(node)
		if err := certificate.CheckSignature(nodeValue[:]); err != nil {
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

func (m *Membership) rejectSignatureSourceLocked(
	node ID,
	now time.Time,
	direct bool,
) {
	if !direct || idIsZero(node) {
		return
	}

	until := now.Add(fastSyncBadSignatureSourceCooldown)
	if m.blockedSources == nil {
		m.blockedSources = make(map[ID]time.Time)
	}
	m.blockedSources[node] = until
	m.pushBlockedDeadlineLocked(blockedSource{
		node:  node,
		until: until,
	})
}

func (m *Membership) signatureSourceBlockedLocked(
	node ID,
	now time.Time,
) bool {
	m.pruneBlockedSourcesLocked(now)

	until, blocked := m.blockedSources[node]
	return blocked && now.Before(until)
}

func (m *Membership) pushBlockedDeadlineLocked(
	source blockedSource,
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

func (m *Membership) pruneBlockedSourcesLocked(now time.Time) {
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

func (m *Membership) popBlockedDeadlineLocked() blockedSource {
	expired := m.blockedDeadlines[0]
	last := len(m.blockedDeadlines) - 1
	m.blockedDeadlines[0] = m.blockedDeadlines[last]
	m.blockedDeadlines[last] = blockedSource{}
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

func cloneMemberCertificate(
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
func (m *Membership) storedCertificateDigestLocked(
	node ID,
	certificate overlay.MemberCertificate,
) (ID, [sha256.Size]byte, bool) {
	if member, found := m.permanent[node]; found && member.hasCertificate {
		if sameFastSyncMemberCertificate(member.certificate.value, certificate) {
			return member.certificate.issuer, member.certificate.hash, true
		}
		var zero ID
		return zero, [sha256.Size]byte{}, false
	}
	if member, found := m.enrolled[node]; found {
		if sameFastSyncMemberCertificate(member.certificate.value, certificate) {
			return member.certificate.issuer, member.certificate.hash, true
		}
	}
	var zero ID
	return zero, [sha256.Size]byte{}, false
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

func storeMemberCertificate(
	certificate overlay.MemberCertificate,
	prepared preparedMemberCertificate,
) storedMemberCertificate {
	return storedMemberCertificate{
		value: cloneMemberCertificate(
			certificate,
			prepared.issuerKey,
		),
		issuer: prepared.issuer,
		hash:   prepared.hash,
	}
}

func shouldReplaceStoredCertificate(
	stored storedMemberCertificate,
	incoming overlay.MemberCertificate,
	now time.Time,
) bool {
	// OverlayNode keeps its unexpired certificate unless the expiry advances.
	return stored.value.IsExpired(now) ||
		incoming.ExpireAt > stored.value.ExpireAt
}

func (c *memberCertificateCache) contains(
	key memberCertificateCacheKey,
) bool {
	// C++ checks LRUCache::contains, whose const lookup intentionally does not
	// refresh eviction order.
	_, found := c.entries[key]
	return found
}

func (c *memberCertificateCache) add(
	key memberCertificateCacheKey,
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

func (c *memberCertificateCache) len() int {
	return len(c.entries)
}

func (w *certificateCheckWindow) allows(now time.Time) bool {
	w.prune(now)
	return int(w.count) < fastSyncMemberCertificateCheckLimit
}

func (w *certificateCheckWindow) record(now time.Time) {
	index := (int(w.head) + int(w.count)) %
		fastSyncMemberCertificateCheckLimit
	w.times[index] = now
	w.count++
}

func (w *certificateCheckWindow) prune(now time.Time) {
	for w.count > 0 &&
		now.Sub(w.times[w.head]) > fastSyncMemberCertificateCheckWindow {
		w.times[w.head] = time.Time{}
		w.head = uint8(
			(int(w.head) + 1) % fastSyncMemberCertificateCheckLimit,
		)
		w.count--
	}
}
