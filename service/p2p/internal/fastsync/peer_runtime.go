package fastsync

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
	// fastSyncPeerVersionTTL is the intake window: a descriptor older than this
	// is not admitted (overlay-peers.cpp add_peer, Overlays::overlay_peer_ttl).
	fastSyncPeerVersionTTL = 10 * time.Minute
	// fastSyncPeerRetentionTTL is the window the random peer draw tolerates.
	// Upstream uses two different ones: its periodic sweep drops a peer at
	// overlay_peer_ttl (update_neighbours, overlay-peers.cpp:548), while
	// get_random_peer and get_overlay_random_peers only discard at
	// version+3600 (overlay-peers.cpp:613 and :656). Membership is unaffected
	// either way — removal here costs a peer its place in the gossip answer,
	// not its right to connect, which acceptsPeerID takes from the membership
	// runtime.
	fastSyncPeerRetentionTTL    = time.Hour
	fastSyncPeerFutureClockSkew = 60 * time.Second
	// RandomPeerResultLimit matches OverlayImpl's getRandomPeersV2 bound.
	RandomPeerResultLimit        = 4
	fastSyncPeerDescriptorWindow = 10 * time.Second
	// Matches OverlayImpl's ten bidirectional four-node peer exchanges per
	// second retained for one descriptor window.
	fastSyncPeerDescriptorLimit = 800
)

var (
	errFastSyncMemberNodeCertificateRequired = errors.New(
		"fast sync peer runtime: nonpermanent node requires member certificate",
	)
	errFastSyncPeerDescriptorsRateLimited = errors.New(
		"fast sync peer runtime: advertised peer descriptor rate limit exceeded",
	)
)

// PeerRuntimeCounts is a point-in-time peer runtime summary.
type PeerRuntimeCounts struct {
	Known             int
	Alive             int
	NonPermanent      int
	AliveNonPermanent int
	Membership        MembershipCounts
}

// PeerRuntime owns local and learned NodeV2 descriptors. It is safe for
// concurrent use and has no background goroutines.
type PeerRuntime struct {
	mu sync.Mutex

	membership *Membership
	privateKey ed25519.PrivateKey
	localID    ID
	overlayID  OverlayID
	local      *localPeer

	peers             map[ID]*peerDescriptor
	aliveNonPermanent []ID
	aliveCount        int
	nonPermanentCount int

	descriptorTimes []time.Time
	descriptorHead  int
	descriptorCount int
}

type localPeer struct {
	publicKey   [ed25519.PublicKeySize]byte
	flags       uint32
	certificate ownedMemberCertificate
}

type peerDescriptor struct {
	publicKey   [ed25519.PublicKeySize]byte
	signature   [ed25519.SignatureSize]byte
	flags       uint32
	version     int32
	certificate ownedMemberCertificate

	permanent  bool
	alive      bool
	aliveIndex int
}

type ownedMemberCertificate struct {
	present   bool
	issuer    [ed25519.PublicKeySize]byte
	flags     uint32
	slot      int32
	expireAt  int32
	signature [ed25519.SignatureSize]byte
}

// NewPeerRuntime initializes descriptor state and enrolls the local node.
func NewPeerRuntime[SourceOverlay ID32](
	privateKey ed25519.PrivateKey,
	overlayID SourceOverlay,
	localFlags uint32,
	certificate any,
	membership *Membership,
	now time.Time,
) (*PeerRuntime, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf(
			"fast sync peer runtime: invalid local private key size %d",
			len(privateKey),
		)
	}

	publicKey := privateKey.Public().(ed25519.PublicKey)
	var publicKeyValue [ed25519.PublicKeySize]byte
	copy(publicKeyValue[:], publicKey)
	localID := ValidatorID[ID](publicKeyValue)

	ownedCertificate, err := ownMemberCertificate(certificate)
	if err != nil {
		return nil, err
	}
	if err = enrollNode(
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

	runtime := &PeerRuntime{
		membership: membership,
		privateKey: append(ed25519.PrivateKey(nil), privateKey...),
		localID:    localID,
		overlayID:  OverlayID(overlayID),
		peers:      make(map[ID]*peerDescriptor),
		descriptorTimes: make(
			[]time.Time,
			fastSyncPeerDescriptorLimit,
		),
	}
	local := &localPeer{
		flags:       localFlags,
		certificate: ownedCertificate,
	}
	copy(local.publicKey[:], publicKey)
	runtime.local = local

	return runtime, nil
}

func (r *PeerRuntime) LocalID() ID {
	return r.localID
}

func (r *PeerRuntime) UpdateLocal(
	flags uint32,
	certificate any,
	now time.Time,
) error {
	ownedCertificate, err := ownMemberCertificate(certificate)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if err = enrollNode(
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

	local := &localPeer{
		publicKey:   r.local.publicKey,
		flags:       flags,
		certificate: ownedCertificate,
	}
	r.local = local
	return nil
}

func (r *PeerRuntime) LocalNode(
	now time.Time,
) (overlay.NodeV2, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.localNodeLocked(now)
}

func (r *PeerRuntime) localNodeLocked(
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

func (r *PeerRuntime) EnrollNode(
	node overlay.NodeV2,
	now time.Time,
) (ID, error) {
	candidate, id, err := r.admitPeerDescriptor(node, now)
	if err != nil {
		return ID{}, err
	}

	verifiedNode := candidate.node(r.overlayID)
	if err = verifiedNode.CheckSignature(); err != nil {
		return ID{}, fmt.Errorf(
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
	if err = enrollNode(
		r.membership,
		id,
		enrolledFlags,
		&candidate.certificate,
		now,
	); err != nil {
		return ID{}, fmt.Errorf(
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

func (r *PeerRuntime) admitPeerDescriptor(
	node overlay.NodeV2,
	now time.Time,
) (*peerDescriptor, ID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.peerDescriptorAvailableLocked(now) {
		return nil, ID{}, errFastSyncPeerDescriptorsRateLimited
	}

	candidate, id, err := newPeerDescriptor(
		node,
		r.overlayID,
		now,
	)
	if err != nil {
		return nil, ID{}, err
	}
	if id == r.localID {
		return nil, ID{}, ErrPeerIsLocal
	}

	r.recordPeerDescriptorLocked(now)
	return candidate, id, nil
}

func (r *PeerRuntime) peerDescriptorAvailableLocked(
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

func (r *PeerRuntime) recordPeerDescriptorLocked(now time.Time) {
	index := (r.descriptorHead + r.descriptorCount) %
		fastSyncPeerDescriptorLimit
	r.descriptorTimes[index] = now
	r.descriptorCount++
}

func (r *PeerRuntime) SetAlive(id ID, alive bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	peer := r.peers[id]
	if peer == nil {
		return ErrNotFound
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

func (r *PeerRuntime) updateRoster(
	rootIDs,
	permanentIDs []ID,
	permanentFlags uint32,
	now time.Time,
) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.membership.updateRoster(rootIDs, permanentIDs, permanentFlags, now)
	for id, peer := range r.peers {
		if !r.membership.Contains(id, now) {
			r.removePeerLocked(id, peer)
			continue
		}

		permanent := r.membership.IsPermanent(id)
		if permanent {
			updated := *peer
			updated.certificate = ownedMemberCertificate{}
			r.setPermanentLocked(id, &updated, true)
			r.peers[id] = &updated
			continue
		}

		r.setPermanentLocked(id, peer, false)
	}
}

func (r *PeerRuntime) Prune(now time.Time) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	removed := 0
	for id, peer := range r.peers {
		if peer.permanent {
			continue
		}
		if peerVersionValid(peer.version, now) &&
			r.membership.AuthorizeOmitted(id, now) == nil {
			continue
		}

		r.removePeerLocked(id, peer)
		removed++
	}
	return removed
}

func (r *PeerRuntime) RandomPeers(
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
		RandomPeerResultLimit,
	)
	nodes[0] = local
	var selected [RandomPeerResultLimit - 1]ID
	selectedCount := 0

	for len(nodes) < RandomPeerResultLimit &&
		len(r.aliveNonPermanent) > selectedCount {
		random = nextPeerRandom(random)
		index := int(random % uint64(len(r.aliveNonPermanent)))

		scanned := 0
		for scanned < len(r.aliveNonPermanent) &&
			peerSelected(
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
		if !peerRetained(peer.version, now) ||
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

func (r *PeerRuntime) Counts() PeerRuntimeCounts {
	r.mu.Lock()
	defer r.mu.Unlock()

	return PeerRuntimeCounts{
		Known:             len(r.peers),
		Alive:             r.aliveCount,
		NonPermanent:      r.nonPermanentCount,
		AliveNonPermanent: len(r.aliveNonPermanent),
		Membership:        r.membership.counts(),
	}
}

func (r *PeerRuntime) setPermanentLocked(
	id ID,
	peer *peerDescriptor,
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

func (r *PeerRuntime) addAliveNonPermanentLocked(
	id ID,
	peer *peerDescriptor,
) {
	peer.aliveIndex = len(r.aliveNonPermanent)
	r.aliveNonPermanent = append(r.aliveNonPermanent, id)
}

func (r *PeerRuntime) removeAliveNonPermanentLocked(
	peer *peerDescriptor,
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

func (r *PeerRuntime) removePeerLocked(
	id ID,
	peer *peerDescriptor,
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

func newPeerDescriptor(
	node overlay.NodeV2,
	expectedOverlay OverlayID,
	now time.Time,
) (*peerDescriptor, ID, error) {
	publicKey, ok := node.ID.(keys.PublicKeyED25519)
	if !ok {
		return nil, ID{}, fmt.Errorf(
			"fast sync peer runtime: unsupported node id type %T",
			node.ID,
		)
	}
	if len(publicKey.Key) != ed25519.PublicKeySize {
		return nil, ID{}, fmt.Errorf(
			"fast sync peer runtime: invalid node public key size %d",
			len(publicKey.Key),
		)
	}
	if len(node.Overlay) != len(expectedOverlay) ||
		!bytes.Equal(node.Overlay, expectedOverlay[:]) {
		return nil, ID{}, fmt.Errorf(
			"fast sync peer runtime: node belongs to another overlay",
		)
	}
	if len(node.Signature) != ed25519.SignatureSize {
		return nil, ID{}, fmt.Errorf(
			"fast sync peer runtime: invalid node signature size %d",
			len(node.Signature),
		)
	}
	if !peerVersionValid(node.Version, now) {
		return nil, ID{}, fmt.Errorf(
			"fast sync peer runtime: node version %d is outside accepted interval",
			node.Version,
		)
	}

	certificate, err := ownMemberCertificate(node.Certificate)
	if err != nil {
		return nil, ID{}, err
	}

	descriptor := &peerDescriptor{
		flags:       node.Flags,
		version:     node.Version,
		certificate: certificate,
		aliveIndex:  -1,
	}
	copy(descriptor.publicKey[:], publicKey.Key)
	copy(descriptor.signature[:], node.Signature)

	var publicKeyValue [ed25519.PublicKeySize]byte
	copy(publicKeyValue[:], descriptor.publicKey[:])
	return descriptor, ValidatorID[ID](publicKeyValue), nil
}

func (p *peerDescriptor) node(
	overlayID OverlayID,
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

func ownMemberCertificate(
	value any,
) (ownedMemberCertificate, error) {
	switch certificate := value.(type) {
	case overlay.EmptyMemberCertificate:
		return ownedMemberCertificate{}, nil
	case overlay.MemberCertificate:
		issuer, ok := certificate.IssuedBy.(keys.PublicKeyED25519)
		if !ok {
			return ownedMemberCertificate{}, fmt.Errorf(
				"fast sync peer runtime: unsupported member certificate issuer type %T",
				certificate.IssuedBy,
			)
		}
		if len(issuer.Key) != ed25519.PublicKeySize {
			return ownedMemberCertificate{}, fmt.Errorf(
				"fast sync peer runtime: invalid member certificate issuer size %d",
				len(issuer.Key),
			)
		}
		if len(certificate.Signature) != ed25519.SignatureSize {
			return ownedMemberCertificate{}, fmt.Errorf(
				"fast sync peer runtime: invalid member certificate signature size %d",
				len(certificate.Signature),
			)
		}

		owned := ownedMemberCertificate{
			present:  true,
			flags:    certificate.Flags,
			slot:     certificate.Slot,
			expireAt: certificate.ExpireAt,
		}
		copy(owned.issuer[:], issuer.Key)
		copy(owned.signature[:], certificate.Signature)
		return owned, nil
	default:
		return ownedMemberCertificate{}, fmt.Errorf(
			"fast sync peer runtime: unsupported member certificate type %T",
			value,
		)
	}
}

func (c *ownedMemberCertificate) value() any {
	if !c.present {
		return overlay.EmptyMemberCertificate{}
	}

	return c.member()
}

func (c *ownedMemberCertificate) member() overlay.MemberCertificate {
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

func (c *ownedMemberCertificate) newerThan(
	current *ownedMemberCertificate,
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

func enrollNode(
	membership *Membership,
	id ID,
	flags uint32,
	certificate *ownedMemberCertificate,
	now time.Time,
) error {
	if certificate.present {
		return membership.enrollCertifiedNode(
			id,
			flags,
			certificate.member(),
			now,
		)
	}
	if !membership.IsPermanent(id) {
		return errFastSyncMemberNodeCertificateRequired
	}
	return membership.updatePermanentNode(id, flags)
}

func peerVersionValid(version int32, now time.Time) bool {
	createdAt := time.Unix(int64(version), 0)
	return !createdAt.Add(fastSyncPeerVersionTTL).Before(now) &&
		!createdAt.After(now.Add(fastSyncPeerFutureClockSkew))
}

// peerRetained reports whether a peer already in the map survives the
// sweep. Intake uses the shorter fastSyncPeerVersionTTL instead.
func peerRetained(version int32, now time.Time) bool {
	createdAt := time.Unix(int64(version), 0)
	return !createdAt.Add(fastSyncPeerRetentionTTL).Before(now) &&
		!createdAt.Add(-fastSyncPeerFutureClockSkew).After(now)
}

func nextPeerRandom(value uint64) uint64 {
	return value*6364136223846793005 + 1442695040888963407
}

func peerSelected(id ID, selected []ID) bool {
	for _, candidate := range selected {
		if candidate == id {
			return true
		}
	}
	return false
}
