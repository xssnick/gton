package p2p

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
)

const (
	fastSyncPeerVersionTTL        = 10 * time.Minute
	fastSyncPeerFutureClockSkew   = 60 * time.Second
	fastSyncRandomPeerResultLimit = 4
	fastSyncPeerDescriptorWindow  = 10 * time.Second
	// Matches OverlayImpl's ten bidirectional four-node peer exchanges per
	// second retained for one descriptor window.
	fastSyncPeerDescriptorLimit = 800
)

var (
	errFastSyncPeerIsLocal = errors.New(
		"fast sync peer runtime: local node descriptor",
	)
	errFastSyncMemberNodeCertificateRequired = errors.New(
		"fast sync peer runtime: nonpermanent node requires member certificate",
	)
	errFastSyncPeerDescriptorsRateLimited = errors.New(
		"fast sync peer runtime: advertised peer descriptor rate limit exceeded",
	)
)

type fastSyncPeerRuntimeCounts struct {
	Known             int
	Alive             int
	NonPermanent      int
	AliveNonPermanent int
	Membership        fastSyncMembershipCounts
}

type fastSyncPeerRuntime struct {
	mu sync.Mutex

	membership *fastSyncMembership
	privateKey ed25519.PrivateKey
	localID    PeerID
	overlayID  FastSyncOverlayShortID
	local      *fastSyncLocalPeer

	peers             map[PeerID]*fastSyncPeerDescriptor
	aliveNonPermanent []PeerID
	aliveCount        int
	nonPermanentCount int

	descriptorTimes []time.Time
	descriptorHead  int
	descriptorCount int
}

type fastSyncLocalPeer struct {
	publicKey   [ed25519.PublicKeySize]byte
	flags       uint32
	certificate fastSyncOwnedMemberCertificate
}

type fastSyncPeerDescriptor struct {
	publicKey   [ed25519.PublicKeySize]byte
	signature   [ed25519.SignatureSize]byte
	flags       uint32
	version     int32
	certificate fastSyncOwnedMemberCertificate

	permanent  bool
	alive      bool
	aliveIndex int
}

type fastSyncOwnedMemberCertificate struct {
	present   bool
	issuer    [ed25519.PublicKeySize]byte
	flags     uint32
	slot      int32
	expireAt  int32
	signature [ed25519.SignatureSize]byte
}

func newFastSyncPeerRuntime(
	privateKey ed25519.PrivateKey,
	overlayID FastSyncOverlayShortID,
	localFlags uint32,
	certificate any,
	membership *fastSyncMembership,
	now time.Time,
) (*fastSyncPeerRuntime, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf(
			"fast sync peer runtime: invalid local private key size %d",
			len(privateKey),
		)
	}

	publicKey := privateKey.Public().(ed25519.PublicKey)
	var publicKeyValue FastSyncValidatorPublicKey
	copy(publicKeyValue[:], publicKey)
	localID := fastSyncValidatorShortID(publicKeyValue)

	ownedCertificate, err := ownFastSyncMemberCertificate(certificate)
	if err != nil {
		return nil, err
	}
	if err = enrollFastSyncNode(
		membership,
		localID,
		localFlags,
		&ownedCertificate,
		now,
	); err != nil {
		return nil, fmt.Errorf(
			"fast sync peer runtime: authorize local node: %w",
			err,
		)
	}

	runtime := &fastSyncPeerRuntime{
		membership: membership,
		privateKey: append(ed25519.PrivateKey(nil), privateKey...),
		localID:    localID,
		overlayID:  overlayID,
		peers:      make(map[PeerID]*fastSyncPeerDescriptor),
		descriptorTimes: make(
			[]time.Time,
			fastSyncPeerDescriptorLimit,
		),
	}
	local := &fastSyncLocalPeer{
		flags:       localFlags,
		certificate: ownedCertificate,
	}
	copy(local.publicKey[:], publicKey)
	runtime.local = local

	return runtime, nil
}

func (r *fastSyncPeerRuntime) LocalID() PeerID {
	return r.localID
}

func (r *fastSyncPeerRuntime) UpdateLocal(
	flags uint32,
	certificate any,
	now time.Time,
) error {
	ownedCertificate, err := ownFastSyncMemberCertificate(certificate)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if err = enrollFastSyncNode(
		r.membership,
		r.localID,
		flags,
		&ownedCertificate,
		now,
	); err != nil {
		return fmt.Errorf(
			"fast sync peer runtime: authorize local node: %w",
			err,
		)
	}

	local := &fastSyncLocalPeer{
		publicKey:   r.local.publicKey,
		flags:       flags,
		certificate: ownedCertificate,
	}
	r.local = local
	return nil
}

func (r *fastSyncPeerRuntime) LocalNode(
	now time.Time,
) (overlay.NodeV2, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.localNodeLocked(now)
}

func (r *fastSyncPeerRuntime) localNodeLocked(
	now time.Time,
) (overlay.NodeV2, error) {
	if err := r.membership.AuthorizeOmitted(r.localID, now); err != nil {
		return overlay.NodeV2{}, fmt.Errorf(
			"fast sync peer runtime: local node is not authorized: %w",
			err,
		)
	}

	node := overlay.NodeV2{
		ID: keys.PublicKeyED25519{
			Key: r.local.publicKey[:],
		},
		Overlay:     r.overlayID[:],
		Flags:       r.local.flags,
		Version:     int32(now.Unix()),
		Certificate: r.local.certificate.value(),
	}
	if err := node.Sign(r.privateKey); err != nil {
		return overlay.NodeV2{}, fmt.Errorf(
			"fast sync peer runtime: sign local node: %w",
			err,
		)
	}

	return node, nil
}

func (r *fastSyncPeerRuntime) EnrollNode(
	node overlay.NodeV2,
	now time.Time,
) (PeerID, error) {
	candidate, id, err := r.admitPeerDescriptor(node, now)
	if err != nil {
		return PeerID{}, err
	}

	verifiedNode := candidate.node(r.overlayID)
	if err = verifiedNode.CheckSignature(); err != nil {
		return PeerID{}, fmt.Errorf(
			"fast sync peer runtime: invalid node signature: %w",
			err,
		)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	existing := r.peers[id]
	enrolledFlags := candidate.flags
	if existing != nil && candidate.version <= existing.version {
		enrolledFlags = existing.flags
	}
	if err = enrollFastSyncNode(
		r.membership,
		id,
		enrolledFlags,
		&candidate.certificate,
		now,
	); err != nil {
		return PeerID{}, fmt.Errorf(
			"fast sync peer runtime: authorize node: %w",
			err,
		)
	}

	candidate.permanent = r.membership.IsPermanent(id)
	if existing == nil {
		candidate.aliveIndex = -1
		r.peers[id] = candidate
		if !candidate.permanent {
			r.nonPermanentCount++
		}
		return id, nil
	}

	updated := *existing
	if candidate.version > existing.version {
		updated.publicKey = candidate.publicKey
		updated.signature = candidate.signature
		updated.flags = candidate.flags
		updated.version = candidate.version
	}
	if candidate.certificate.newerThan(
		&existing.certificate,
		now,
	) {
		updated.certificate = candidate.certificate
	}
	r.setPermanentLocked(id, &updated, candidate.permanent)
	r.peers[id] = &updated
	return id, nil
}

func (r *fastSyncPeerRuntime) admitPeerDescriptor(
	node overlay.NodeV2,
	now time.Time,
) (*fastSyncPeerDescriptor, PeerID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.peerDescriptorAvailableLocked(now) {
		return nil, PeerID{}, errFastSyncPeerDescriptorsRateLimited
	}

	candidate, id, err := newFastSyncPeerDescriptor(
		node,
		r.overlayID,
		now,
	)
	if err != nil {
		return nil, PeerID{}, err
	}
	if id == r.localID {
		return nil, PeerID{}, errFastSyncPeerIsLocal
	}

	r.recordPeerDescriptorLocked(now)
	return candidate, id, nil
}

func (r *fastSyncPeerRuntime) peerDescriptorAvailableLocked(
	now time.Time,
) bool {
	for r.descriptorCount > 0 &&
		now.Sub(r.descriptorTimes[r.descriptorHead]) >
			fastSyncPeerDescriptorWindow {
		r.descriptorTimes[r.descriptorHead] = time.Time{}
		r.descriptorHead = (r.descriptorHead + 1) %
			fastSyncPeerDescriptorLimit
		r.descriptorCount--
	}
	return r.descriptorCount < fastSyncPeerDescriptorLimit
}

func (r *fastSyncPeerRuntime) recordPeerDescriptorLocked(now time.Time) {
	index := (r.descriptorHead + r.descriptorCount) %
		fastSyncPeerDescriptorLimit
	r.descriptorTimes[index] = now
	r.descriptorCount++
}

func (r *fastSyncPeerRuntime) SetAlive(id PeerID, alive bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	peer := r.peers[id]
	if peer == nil {
		return ErrFastSyncNotFound
	}
	if peer.alive == alive {
		return nil
	}

	peer.alive = alive
	if alive {
		r.aliveCount++
		if !peer.permanent {
			r.addAliveNonPermanentLocked(id, peer)
		}
		return nil
	}

	r.aliveCount--
	r.removeAliveNonPermanentLocked(peer)
	return nil
}

func (r *fastSyncPeerRuntime) Forget(id PeerID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	peer := r.peers[id]
	if peer == nil {
		return ErrFastSyncNotFound
	}

	r.removePeerLocked(id, peer)
	return nil
}

func (r *fastSyncPeerRuntime) UpdateRoster(
	roster FastSyncValidatorRoster,
	permanentFlags uint32,
	now time.Time,
) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.membership.UpdateRoster(roster, permanentFlags, now)
	for id, peer := range r.peers {
		if !r.membership.Contains(id) {
			r.removePeerLocked(id, peer)
			continue
		}

		permanent := r.membership.IsPermanent(id)
		if permanent {
			updated := *peer
			updated.certificate = fastSyncOwnedMemberCertificate{}
			r.setPermanentLocked(id, &updated, true)
			r.peers[id] = &updated
			continue
		}

		r.setPermanentLocked(id, peer, false)
	}
}

func (r *fastSyncPeerRuntime) Prune(now time.Time) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	removed := 0
	for id, peer := range r.peers {
		if peer.permanent {
			continue
		}
		if fastSyncPeerVersionValid(peer.version, now) &&
			r.membership.AuthorizeOmitted(id, now) == nil {
			continue
		}

		r.removePeerLocked(id, peer)
		removed++
	}
	return removed
}

func (r *fastSyncPeerRuntime) RandomPeers(
	now time.Time,
	random uint64,
) (overlay.NodesV2, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	local, err := r.localNodeLocked(now)
	if err != nil {
		return overlay.NodesV2{}, err
	}

	nodes := make(
		[]overlay.NodeV2,
		1,
		fastSyncRandomPeerResultLimit,
	)
	nodes[0] = local
	var selected [fastSyncRandomPeerResultLimit - 1]PeerID
	selectedCount := 0

	for len(nodes) < fastSyncRandomPeerResultLimit &&
		len(r.aliveNonPermanent) > selectedCount {
		random = nextFastSyncPeerRandom(random)
		index := int(random % uint64(len(r.aliveNonPermanent)))

		scanned := 0
		for scanned < len(r.aliveNonPermanent) &&
			fastSyncPeerSelected(
				r.aliveNonPermanent[index],
				selected[:selectedCount],
			) {
			index++
			if index == len(r.aliveNonPermanent) {
				index = 0
			}
			scanned++
		}
		if scanned == len(r.aliveNonPermanent) {
			break
		}

		id := r.aliveNonPermanent[index]
		peer := r.peers[id]
		if !fastSyncPeerVersionValid(peer.version, now) ||
			r.membership.AuthorizeOmitted(id, now) != nil {
			r.removePeerLocked(id, peer)
			continue
		}

		selected[selectedCount] = id
		selectedCount++
		nodes = append(nodes, peer.node(r.overlayID))
	}

	return overlay.NodesV2{Nodes: nodes}, nil
}

func (r *fastSyncPeerRuntime) Counts() fastSyncPeerRuntimeCounts {
	r.mu.Lock()
	defer r.mu.Unlock()

	return fastSyncPeerRuntimeCounts{
		Known:             len(r.peers),
		Alive:             r.aliveCount,
		NonPermanent:      r.nonPermanentCount,
		AliveNonPermanent: len(r.aliveNonPermanent),
		Membership:        r.membership.Counts(),
	}
}

func (r *fastSyncPeerRuntime) setPermanentLocked(
	id PeerID,
	peer *fastSyncPeerDescriptor,
	permanent bool,
) {
	if peer.permanent == permanent {
		return
	}

	peer.permanent = permanent
	if permanent {
		r.nonPermanentCount--
		r.removeAliveNonPermanentLocked(peer)
		return
	}

	r.nonPermanentCount++
	if peer.alive {
		r.addAliveNonPermanentLocked(id, peer)
	}
}

func (r *fastSyncPeerRuntime) addAliveNonPermanentLocked(
	id PeerID,
	peer *fastSyncPeerDescriptor,
) {
	peer.aliveIndex = len(r.aliveNonPermanent)
	r.aliveNonPermanent = append(r.aliveNonPermanent, id)
}

func (r *fastSyncPeerRuntime) removeAliveNonPermanentLocked(
	peer *fastSyncPeerDescriptor,
) {
	if peer.aliveIndex < 0 {
		return
	}

	lastIndex := len(r.aliveNonPermanent) - 1
	if peer.aliveIndex != lastIndex {
		movedID := r.aliveNonPermanent[lastIndex]
		r.aliveNonPermanent[peer.aliveIndex] = movedID
		r.peers[movedID].aliveIndex = peer.aliveIndex
	}
	r.aliveNonPermanent = r.aliveNonPermanent[:lastIndex]
	peer.aliveIndex = -1
}

func (r *fastSyncPeerRuntime) removePeerLocked(
	id PeerID,
	peer *fastSyncPeerDescriptor,
) {
	if peer.alive {
		r.aliveCount--
	}
	if !peer.permanent {
		r.nonPermanentCount--
	}
	r.removeAliveNonPermanentLocked(peer)
	delete(r.peers, id)
}

func newFastSyncPeerDescriptor(
	node overlay.NodeV2,
	expectedOverlay FastSyncOverlayShortID,
	now time.Time,
) (*fastSyncPeerDescriptor, PeerID, error) {
	publicKey, ok := node.ID.(keys.PublicKeyED25519)
	if !ok {
		return nil, PeerID{}, fmt.Errorf(
			"fast sync peer runtime: unsupported node id type %T",
			node.ID,
		)
	}
	if len(publicKey.Key) != ed25519.PublicKeySize {
		return nil, PeerID{}, fmt.Errorf(
			"fast sync peer runtime: invalid node public key size %d",
			len(publicKey.Key),
		)
	}
	if len(node.Overlay) != len(expectedOverlay) ||
		!bytes.Equal(node.Overlay, expectedOverlay[:]) {
		return nil, PeerID{}, fmt.Errorf(
			"fast sync peer runtime: node belongs to another overlay",
		)
	}
	if len(node.Signature) != ed25519.SignatureSize {
		return nil, PeerID{}, fmt.Errorf(
			"fast sync peer runtime: invalid node signature size %d",
			len(node.Signature),
		)
	}
	if !fastSyncPeerVersionValid(node.Version, now) {
		return nil, PeerID{}, fmt.Errorf(
			"fast sync peer runtime: node version %d is outside accepted interval",
			node.Version,
		)
	}

	certificate, err := ownFastSyncMemberCertificate(node.Certificate)
	if err != nil {
		return nil, PeerID{}, err
	}

	descriptor := &fastSyncPeerDescriptor{
		flags:       node.Flags,
		version:     node.Version,
		certificate: certificate,
		aliveIndex:  -1,
	}
	copy(descriptor.publicKey[:], publicKey.Key)
	copy(descriptor.signature[:], node.Signature)

	var publicKeyValue FastSyncValidatorPublicKey
	copy(publicKeyValue[:], descriptor.publicKey[:])
	return descriptor, fastSyncValidatorShortID(publicKeyValue), nil
}

func (p *fastSyncPeerDescriptor) node(
	overlayID FastSyncOverlayShortID,
) overlay.NodeV2 {
	return overlay.NodeV2{
		ID: keys.PublicKeyED25519{
			Key: p.publicKey[:],
		},
		Overlay:     overlayID[:],
		Flags:       p.flags,
		Version:     p.version,
		Signature:   p.signature[:],
		Certificate: p.certificate.value(),
	}
}

func ownFastSyncMemberCertificate(
	value any,
) (fastSyncOwnedMemberCertificate, error) {
	switch certificate := value.(type) {
	case overlay.EmptyMemberCertificate:
		return fastSyncOwnedMemberCertificate{}, nil
	case overlay.MemberCertificate:
		issuer, ok := certificate.IssuedBy.(keys.PublicKeyED25519)
		if !ok {
			return fastSyncOwnedMemberCertificate{}, fmt.Errorf(
				"fast sync peer runtime: unsupported member certificate issuer type %T",
				certificate.IssuedBy,
			)
		}
		if len(issuer.Key) != ed25519.PublicKeySize {
			return fastSyncOwnedMemberCertificate{}, fmt.Errorf(
				"fast sync peer runtime: invalid member certificate issuer size %d",
				len(issuer.Key),
			)
		}
		if len(certificate.Signature) != ed25519.SignatureSize {
			return fastSyncOwnedMemberCertificate{}, fmt.Errorf(
				"fast sync peer runtime: invalid member certificate signature size %d",
				len(certificate.Signature),
			)
		}

		owned := fastSyncOwnedMemberCertificate{
			present:  true,
			flags:    certificate.Flags,
			slot:     certificate.Slot,
			expireAt: certificate.ExpireAt,
		}
		copy(owned.issuer[:], issuer.Key)
		copy(owned.signature[:], certificate.Signature)
		return owned, nil
	default:
		return fastSyncOwnedMemberCertificate{}, fmt.Errorf(
			"fast sync peer runtime: unsupported member certificate type %T",
			value,
		)
	}
}

func (c *fastSyncOwnedMemberCertificate) value() any {
	if !c.present {
		return overlay.EmptyMemberCertificate{}
	}

	return c.member()
}

func (c *fastSyncOwnedMemberCertificate) member() overlay.MemberCertificate {
	return overlay.MemberCertificate{
		IssuedBy: keys.PublicKeyED25519{
			Key: c.issuer[:],
		},
		Flags:     c.flags,
		Slot:      c.slot,
		ExpireAt:  c.expireAt,
		Signature: c.signature[:],
	}
}

func (c *fastSyncOwnedMemberCertificate) newerThan(
	current *fastSyncOwnedMemberCertificate,
	now time.Time,
) bool {
	if !c.present {
		return false
	}
	if !current.present || current.member().IsExpired(now) {
		return true
	}
	return c.expireAt > current.expireAt
}

func enrollFastSyncNode(
	membership *fastSyncMembership,
	id PeerID,
	flags uint32,
	certificate *fastSyncOwnedMemberCertificate,
	now time.Time,
) error {
	if certificate.present {
		return membership.EnrollNode(
			id,
			flags,
			certificate.member(),
			now,
		)
	}
	if !membership.IsPermanent(id) {
		return errFastSyncMemberNodeCertificateRequired
	}
	return membership.EnrollPermanentNode(id, flags)
}

func fastSyncPeerVersionValid(version int32, now time.Time) bool {
	createdAt := time.Unix(int64(version), 0)
	return !createdAt.Add(fastSyncPeerVersionTTL).Before(now) &&
		!createdAt.After(now.Add(fastSyncPeerFutureClockSkew))
}

func nextFastSyncPeerRandom(value uint64) uint64 {
	return value*6364136223846793005 + 1442695040888963407
}

func fastSyncPeerSelected(id PeerID, selected []PeerID) bool {
	for _, candidate := range selected {
		if candidate == id {
			return true
		}
	}
	return false
}
