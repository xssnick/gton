package p2p

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/xssnick/gton/service/p2p/internal/fastsync"
	sharddomain "github.com/xssnick/gton/service/shard"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

const fastSyncDoNotReceiveBroadcasts = uint32(1)

const fastSyncRandomPeersTimeout = 5 * time.Second

// FastSyncState is the config and current shard set needed to reconcile
// FastSync overlays after a masterchain state transition.
type FastSyncState struct {
	Roster                     FastSyncValidatorRoster
	Shards                     []FastSyncShard
	MasterchainPlumtreeEnabled bool
	ShardPlumtreeEnabled       bool
}

type fastSyncOverlaySpec struct {
	roster            FastSyncValidatorRoster
	rosterFingerprint [sha256.Size]byte
	certificate       *overlay.MemberCertificate
	certificateHash   [sha256.Size]byte
	localValidator    bool
	receiveBroadcasts bool
	plumtreeEnabled   bool
	permanentFlags    uint32
}

type fastSyncOverlayRuntime struct {
	spec       fastSyncOverlaySpec
	membership *fastsync.Membership
	peers      *fastsync.PeerRuntime
	envelope   *quicOverlayEnvelope

	certificateMu   sync.Mutex
	certificateHash [sha256.Size]byte

	rootsMu        sync.Mutex
	aliveRoots     []PeerID
	aliveRootIndex map[PeerID]int
}

func newFastSyncOverlayRuntime(
	node *Node,
	shortID []byte,
	spec fastSyncOverlaySpec,
	envelope *quicOverlayEnvelope,
) (*fastSyncOverlayRuntime, error) {
	overlayID, err := fastSyncShortIDFromBytes(shortID)
	if err != nil {
		return nil, err
	}

	membership := fastsync.NewMembership(
		spec.roster.rootPublicKeyIDsRef(),
		spec.roster.adnlIDsRef(),
		spec.permanentFlags,
	)
	certificate := fastSyncCertificateValue(spec.certificate)
	peers, err := fastsync.NewPeerRuntime(
		node.privKey,
		overlayID,
		fastSyncLocalMemberFlags(spec.receiveBroadcasts),
		certificate,
		membership,
		time.Now(),
	)
	if err != nil {
		return nil, err
	}
	if PeerID(peers.LocalID()) != node.localID {
		return nil, errors.New("fast sync local peer id does not match ADNL id")
	}

	return &fastSyncOverlayRuntime{
		spec:            spec,
		membership:      membership,
		peers:           peers,
		envelope:        envelope,
		certificateHash: spec.certificateHash,
		aliveRoots:      make([]PeerID, 0, spec.roster.Len()),
		aliveRootIndex:  make(map[PeerID]int, spec.roster.Len()),
	}, nil
}

func fastSyncShortIDFromBytes(
	value []byte,
) (FastSyncOverlayShortID, error) {
	var id FastSyncOverlayShortID
	if len(value) != len(id) {
		return id, fmt.Errorf(
			"fast sync overlay id length is %d, want %d",
			len(value),
			len(id),
		)
	}
	copy(id[:], value)
	return id, nil
}

func fastSyncCertificateValue(
	certificate *overlay.MemberCertificate,
) any {
	if certificate == nil {
		return overlay.EmptyMemberCertificate{}
	}
	return *certificate
}

func fastSyncLocalMemberFlags(receiveBroadcasts bool) uint32 {
	if receiveBroadcasts {
		return 0
	}
	return fastSyncDoNotReceiveBroadcasts
}

func (r *fastSyncOverlayRuntime) matches(spec fastSyncOverlaySpec) bool {
	return r.spec.rosterFingerprint == spec.rosterFingerprint &&
		r.spec.localValidator == spec.localValidator &&
		r.spec.receiveBroadcasts == spec.receiveBroadcasts &&
		r.spec.plumtreeEnabled == spec.plumtreeEnabled &&
		r.spec.permanentFlags == spec.permanentFlags
}

func (r *fastSyncOverlayRuntime) updateCertificate(
	certificate *overlay.MemberCertificate,
	hash [sha256.Size]byte,
) error {
	r.certificateMu.Lock()
	defer r.certificateMu.Unlock()

	if r.certificateHash == hash {
		return nil
	}
	envelopeState, err := r.envelope.prepareCertificate(certificate)
	if err != nil {
		return err
	}
	if err := r.peers.UpdateLocal(
		fastSyncLocalMemberFlags(r.spec.receiveBroadcasts),
		fastSyncCertificateValue(certificate),
		time.Now(),
	); err != nil {
		return err
	}

	r.envelope.state.Store(envelopeState)
	r.certificateHash = hash
	return nil
}

func (r *fastSyncOverlayRuntime) contains(id PeerID) bool {
	return r.membership.Contains(fastsync.ID(id), time.Now())
}

func (r *fastSyncOverlayRuntime) permanent(id PeerID) bool {
	return r.membership.IsPermanent(fastsync.ID(id))
}

// declinesBroadcasts reports whether this overlay is send-only for us, i.e. we
// published DoNotReceiveBroadcasts in our own member flags. Stated as the
// conjunction rather than as !receiveBroadcasts so a zero-valued spec means
// "receiving", which is what every overlay that is not a plumtree validator is.
func (r *fastSyncOverlayRuntime) declinesBroadcasts() bool {
	return r.spec.localValidator && !r.spec.receiveBroadcasts
}

func (r *fastSyncOverlayRuntime) peerReceivesBroadcasts(id PeerID) bool {
	flags, err := r.membership.PeerFlags(fastsync.ID(id))
	return err == nil && flags&fastSyncDoNotReceiveBroadcasts == 0
}

func (r *fastSyncOverlayRuntime) setValidatorAlive(
	id PeerID,
	alive bool,
) {
	if !r.spec.roster.ContainsADNL(id) {
		return
	}

	r.rootsMu.Lock()
	index, found := r.aliveRootIndex[id]
	if alive == found {
		r.rootsMu.Unlock()
		return
	}

	if alive {
		r.aliveRootIndex[id] = len(r.aliveRoots)
		r.aliveRoots = append(r.aliveRoots, id)
		r.rootsMu.Unlock()
		return
	}

	last := len(r.aliveRoots) - 1
	if index != last {
		moved := r.aliveRoots[last]
		r.aliveRoots[index] = moved
		r.aliveRootIndex[moved] = index
	}
	r.aliveRoots = r.aliveRoots[:last]
	delete(r.aliveRootIndex, id)
	r.rootsMu.Unlock()
}

// aliveRootsSnapshot copies the validators currently known to answer, so a
// rebuilt overlay can start from what the old one had learned.
func (r *fastSyncOverlayRuntime) aliveRootsSnapshot() []PeerID {
	r.rootsMu.Lock()
	defer r.rootsMu.Unlock()
	return slices.Clone(r.aliveRoots)
}

// seedAliveRoots re-arms liveness after a rebuild. setValidatorAlive filters
// against the new roster, so validators dropped by the key block are discarded
// here rather than needing to be filtered by the caller.
func (r *fastSyncOverlayRuntime) seedAliveRoots(ids []PeerID) {
	for _, id := range ids {
		r.setValidatorAlive(id, true)
	}
}

// seedFastSyncLiveness carries liveness into a rebuilt overlay, but only for
// validators this subscription already holds an open transport to.
//
// The filter is the whole point. ready() is a count of aliveRoots and nothing
// else, so seeding before the transports are attached would make the overlay
// announce itself query-ready while owning no peers: readyFastSyncQuerySubscription
// would hand it to querySubscriptionForBlock in preference to the public
// overlay, and fastSyncQueryCandidates would then walk the whole roster finding
// no peer, mark every seeded validator dead and return nothing — a download
// failure where the unseeded path would simply have fallen back. Call this only
// after attachSubscriptionPeers.
func (s *overlaySubscription) seedFastSyncLiveness(ids []PeerID) {
	if s.fastSync == nil || len(ids) == 0 {
		return
	}
	connected := make([]PeerID, 0, len(ids))
	for _, id := range ids {
		peer := s.peerByID(id)
		if peer == nil || !peer.hasOpenConnection() {
			continue
		}
		connected = append(connected, id)
	}
	s.fastSync.seedAliveRoots(connected)
}

func (r *fastSyncOverlayRuntime) setPeerAlive(
	id PeerID,
	alive bool,
) {
	r.setValidatorAlive(id, alive)
	// Permanent validators can be queried before they publish NodeV2.
	// Their readiness lives in aliveRoots; the descriptor state is optional.
	_ = r.peers.SetAlive(fastsync.ID(id), alive)
}

func (r *fastSyncOverlayRuntime) ready() bool {
	r.rootsMu.Lock()
	ready := len(r.aliveRoots) > min(r.spec.roster.Len()/2, 20)
	r.rootsMu.Unlock()
	return ready
}

func (r *fastSyncOverlayRuntime) selectValidator(
	random uint64,
) (PeerID, error) {
	r.rootsMu.Lock()
	defer r.rootsMu.Unlock()

	if len(r.aliveRoots) <= min(r.spec.roster.Len()/2, 20) {
		return PeerID{}, ErrFastSyncNotReady
	}
	return r.aliveRoots[random%uint64(len(r.aliveRoots))], nil
}

func (r *fastSyncOverlayRuntime) validatorPingTargets(
	random uint64,
	limit int,
	localID PeerID,
) []PeerID {
	ids := r.spec.roster.adnlIDsRef()
	if limit <= 0 || len(ids) == 0 {
		return nil
	}
	if limit > len(ids) {
		limit = len(ids)
	}

	selected := make([]PeerID, 0, limit)
	start := int(random % uint64(len(ids)))
	step := 1
	if len(ids) > 1 {
		step = int((random>>32)%uint64(len(ids)-1)) + 1
		for greatestCommonDivisor(step, len(ids)) != 1 {
			step++
			if step == len(ids) {
				step = 1
			}
		}
	}
	for visited := 0; visited < len(ids) && len(selected) < limit; visited++ {
		id := ids[(start+visited*step)%len(ids)]
		if id != localID {
			selected = append(selected, id)
		}
	}
	return selected
}

func greatestCommonDivisor(left, right int) int {
	for right != 0 {
		left, right = right, left%right
	}
	return left
}

func (n *Node) SetFastSyncOverlays(state FastSyncState) error {
	n.fastSyncStateMx.Lock()
	defer n.fastSyncStateMx.Unlock()

	n.fastSyncState = &state
	return n.applyFastSyncOverlaysLocked(state)
}

// applyFastSyncOverlaysLocked reconciles overlays against a state. Callers hold
// fastSyncStateMx so that a certificate import and a masterchain apply cannot
// reconcile concurrently.
func (n *Node) applyFastSyncOverlaysLocked(state FastSyncState) error {
	if len(n.zeroStateFileHash) == 0 {
		return nil
	}

	certificate, localValidator := n.fastSyncLocalCertificate(
		state.Roster,
		time.Now(),
	)

	desiredShards, err := n.fastSyncDesiredShards(state.Shards)
	if err != nil {
		return err
	}
	if !localValidator && certificate == nil {
		desiredShards = nil
	}

	desired := make(map[FastSyncShard]overlaySpec, len(desiredShards))
	for _, shard := range desiredShards {
		plumtreeEnabled := state.ShardPlumtreeEnabled
		if shard.Workchain == -1 {
			plumtreeEnabled = state.MasterchainPlumtreeEnabled
		}
		spec, err := n.buildFastSyncOverlaySpec(
			state.Roster,
			shard,
			certificate,
			localValidator,
			plumtreeEnabled,
		)
		if err != nil {
			return err
		}
		desired[shard] = spec
	}

	return n.reconcileFastSyncOverlays(desired)
}

// fastSyncLocalCertificate picks the stored certificate that still authorises
// this node, or reports that the node is a validator and needs none.
//
// An unusable certificate is skipped, never fatal. The reference node does the
// same, and the difference matters: this runs on every masterchain block, and
// failing the whole call would leave every FastSync overlay — masterchain
// included — frozen at its previous roster until some peer happened to push a
// fresh certificate.
func (n *Node) fastSyncLocalCertificate(
	roster FastSyncValidatorRoster,
	now time.Time,
) (*overlay.MemberCertificate, bool) {
	if roster.ContainsADNL(n.localID) {
		return nil, true
	}
	// Not an error path, but the transition it precedes is invisible otherwise:
	// once this node is in the roster, plumtree turns FastSync into a
	// send-only overlay for it (receiveBroadcasts goes false), so a silent flip
	// would look like FastSync simply stopped delivering.

	skip := func(i int, err error) {
		n.log.Debug().
			Err(err).
			Int("certificate", i).
			Msg("skipping unusable FastSync certificate")
	}

	certificates := n.fastSyncCertificateSnapshot()
	for i := range certificates {
		certificate := certificates[i]
		if certificate.IsExpired(now) {
			continue
		}
		if err := certificate.CheckSlot(FastSyncMemberSlotCount); err != nil {
			skip(i, err)
			continue
		}
		if err := certificate.CheckSignature(n.localID[:]); err != nil {
			skip(i, err)
			continue
		}

		issuer, err := certificate.IssuerID()
		if err != nil {
			skip(i, err)
			continue
		}
		issuerID, err := NewPeerID(issuer)
		if err != nil {
			skip(i, err)
			continue
		}
		if !roster.ContainsRoot(issuerID) {
			continue
		}

		cloned, err := cloneFastSyncCertificate(certificate)
		if err != nil {
			skip(i, err)
			continue
		}
		return &cloned, false
	}
	return nil, false
}

func cloneFastSyncCertificate(
	certificate overlay.MemberCertificate,
) (overlay.MemberCertificate, error) {
	issuer, ok := certificate.IssuedBy.(keys.PublicKeyED25519)
	if !ok {
		return overlay.MemberCertificate{}, fmt.Errorf(
			"unsupported issuer type %T",
			certificate.IssuedBy,
		)
	}

	certificate.IssuedBy = keys.PublicKeyED25519{
		Key: slices.Clone(issuer.Key),
	}
	certificate.Signature = slices.Clone(certificate.Signature)
	return certificate, nil
}

func (n *Node) fastSyncDesiredShards(
	shards []FastSyncShard,
) ([]FastSyncShard, error) {
	desired := make([]FastSyncShard, 0, len(shards)+1)
	desired = append(desired, FastSyncShard{
		Workchain: -1,
		Shard:     topShard,
	})
	for _, candidate := range shards {
		prefixLen, err := sharddomain.PrefixLength(candidate.Shard)
		if err != nil {
			return nil, fmt.Errorf("invalid fast-sync shard %d:%016x: %w", candidate.Workchain, uint64(candidate.Shard), err)
		}
		if candidate.Workchain == -1 {
			continue
		}
		depth := n.monitorMinSplitDepthForWorkchain(candidate.Workchain)
		if prefixLen > depth {
			ancestor, err := sharddomain.Ancestor(candidate.Shard, depth)
			if err != nil {
				return nil, fmt.Errorf("normalize fast-sync shard %d:%016x: %w", candidate.Workchain, uint64(candidate.Shard), err)
			}
			candidate.Shard = ancestor
		}
		desired = append(desired, candidate)
	}
	return NewFastSyncShardSet(desired).shardsRef(), nil
}

func (n *Node) buildFastSyncOverlaySpec(
	roster FastSyncValidatorRoster,
	shard FastSyncShard,
	certificate *overlay.MemberCertificate,
	localValidator,
	plumtreeEnabled bool,
) (overlaySpec, error) {
	var zeroHash FastSyncFileHash
	if len(n.zeroStateFileHash) != len(zeroHash) {
		return overlaySpec{}, fmt.Errorf(
			"zero-state file hash length is %d, want %d",
			len(n.zeroStateFileHash),
			len(zeroHash),
		)
	}
	copy(zeroHash[:], n.zeroStateFileHash)

	identity := NewFastSyncOverlayIdentity(zeroHash, shard)
	// No FixedNodeIDs: acceptsPeerID answers from the live membership runtime
	// for a FastSync overlay and never reaches the configured-roster lookup, so
	// a roster-sized set per shard per masterchain block would only be built to
	// be ignored.
	fixedNodes := roster.adnlIDsRef()
	authorizedKeys := make(map[string]uint32, len(fixedNodes))
	for _, id := range fixedNodes {
		authorizedKeys[string(id[:])] = maxOverlayPayloadSize
	}

	receiveBroadcasts := true
	if plumtreeEnabled {
		receiveBroadcasts = !localValidator
	}
	permanentFlags := uint32(0)
	if plumtreeEnabled || shard.Workchain != -1 {
		permanentFlags = fastSyncDoNotReceiveBroadcasts
	}

	fastSyncSpec := fastSyncOverlaySpec{
		roster:            roster,
		rosterFingerprint: roster.Fingerprint(),
		certificate:       certificate,
		localValidator:    localValidator,
		receiveBroadcasts: receiveBroadcasts,
		plumtreeEnabled:   plumtreeEnabled,
		permanentFlags:    permanentFlags,
	}
	if certificate != nil {
		encoded, err := tl.Serialize(*certificate, true)
		if err != nil {
			return overlaySpec{}, fmt.Errorf(
				"serialize fast sync certificate: %w",
				err,
			)
		}
		fastSyncSpec.certificateHash = sha256.Sum256(encoded)
	}

	protoMajor := int32(shardchainProtoVersionMajor)
	protoMinor := int32(shardchainProtoVersionMinor)
	if shard.Workchain == -1 {
		protoMajor = int32(masterchainProtoVersionMajor)
		protoMinor = int32(masterchainProtoVersionMinor)
	}

	return overlaySpec{
		Name: fmt.Sprintf(
			"fast-sync.%d.%016x",
			shard.Workchain,
			uint64(shard.Shard),
		),
		Kind:              overlayKindFastSync,
		Workchain:         shard.Workchain,
		Shard:             shard.Shard,
		FullID:            identity.FullID[:],
		ShortID:           identity.ShortID[:],
		ProtoVersionMajor: protoMajor,
		ProtoVersionMinor: protoMinor,
		FixedNodes:        fixedNodes,
		QueryAcceptors:    fixedNodes,
		AuthorizedKeys:    authorizedKeys,
		UseQUIC:           true,
		SendQueries:       true,
		AcceptQueries:     true,
		RandomPeers:       true,
		QueryCapabilities: true,
		FastSync:          &fastSyncSpec,
	}, nil
}

func (n *Node) reconcileFastSyncOverlays(
	desired map[FastSyncShard]overlaySpec,
) error {
	var removed []*overlaySubscription
	type pendingFastSyncSubscription struct {
		shard      FastSyncShard
		spec       overlaySpec
		sub        *overlaySubscription
		aliveRoots []PeerID
	}
	start := make([]pendingFastSyncSubscription, 0, len(desired))
	// A roster change rebuilds the overlay because the authorized-key set is
	// baked into its broadcast receiver, but the knowledge of which validators
	// answer is not roster-specific and is expensive to relearn: without this
	// every key block would leave selectValidator unready until the ping sweep
	// walked the set again. Upstream keeps the same state alive across its own
	// delete_overlay+init for the same reason.
	carriedAlive := make(map[FastSyncShard][]PeerID)

	n.subscriptionsMx.Lock()
	for shard, sub := range n.fastSyncSubscriptions {
		spec, keep := desired[shard]
		if keep && sub.fastSync != nil &&
			sub.fastSync.matches(*spec.FastSync) {
			if err := sub.fastSync.updateCertificate(
				spec.FastSync.certificate,
				spec.FastSync.certificateHash,
			); err != nil {
				n.subscriptionsMx.Unlock()
				return fmt.Errorf(
					"update FastSync certificate for %s: %w",
					spec.Name,
					err,
				)
			}
			delete(desired, shard)
			continue
		}

		if sub.fastSync != nil {
			carriedAlive[shard] = sub.fastSync.aliveRootsSnapshot()
		}
		removed = append(removed, sub)
	}

	for shard, spec := range desired {
		sub, err := n.newOverlaySubscription(spec)
		if err != nil {
			n.subscriptionsMx.Unlock()
			for _, pending := range start {
				pending.sub.close()
			}
			return err
		}
		start = append(start, pendingFastSyncSubscription{
			shard:      shard,
			spec:       spec,
			sub:        sub,
			aliveRoots: carriedAlive[shard],
		})
	}
	for _, sub := range removed {
		delete(
			n.fastSyncSubscriptions,
			FastSyncShard{
				Workchain: sub.spec.Workchain,
				Shard:     sub.spec.Shard,
			},
		)
		delete(n.subscriptions, overlaySpecKey(sub.spec))
	}
	for _, pending := range start {
		n.subscriptions[overlaySpecKey(pending.spec)] = pending.sub
		n.fastSyncSubscriptions[pending.shard] = pending.sub
	}
	n.publishPublicBroadcastReceiversLocked()
	n.subscriptionsMx.Unlock()

	for _, sub := range removed {
		sub.close()
	}
	for _, pending := range start {
		n.attachSubscriptionPeers(pending.sub)
		pending.sub.seedFastSyncLiveness(pending.aliveRoots)
		if n.networkStarted.Load() {
			n.startSubscription(pending.sub)
		}
	}
	return nil
}

func (s *overlaySubscription) fastSyncQueryCandidates(
	requiredVersionMajor,
	requiredVersionMinor int32,
) []*overlayPeer {
	if s.fastSync == nil {
		return nil
	}

	for attempts := 0; attempts < s.fastSync.spec.roster.Len(); attempts++ {
		id, err := s.fastSync.selectValidator(rand.Uint64())
		if err != nil {
			return nil
		}
		peer := s.peerByID(id)
		if peer == nil || !peer.hasOpenConnection() {
			s.fastSync.setPeerAlive(id, false)
			continue
		}
		stats := peer.statsSnapshot()
		if !peerEligibleVersion(
			stats.versionMajor,
			stats.versionMinor,
			requiredVersionMajor,
			requiredVersionMinor,
		) {
			return nil
		}
		return []*overlayPeer{peer}
	}
	return nil
}

func (s *overlaySubscription) warmupFastSyncPeer(
	ctx context.Context,
	peer *overlayPeer,
) {
	if !s.probeFastSyncPeerCapabilities(ctx, peer) {
		return
	}
	s.exchangeFastSyncRandomPeers(ctx, peer)
}

func (s *overlaySubscription) pingFastSyncValidators(
	ctx context.Context,
) {
	if !s.isActive() || s.fastSync == nil {
		return
	}

	// Sweep before picking targets. Upstream bounds its peer map at insert time
	// (del_some_peers from add_peer); ours is bounded only by certificate
	// issuance policy, so without this a node that stays up across many
	// validator sets keeps every certified client it ever heard of.
	s.fastSync.peers.Prune(time.Now())

	ids := s.fastSync.validatorPingTargets(
		rand.Uint64(),
		fastSyncPingFanout,
		s.node.localID,
	)
	peers := make([]*overlayPeer, 0, len(ids))
	for _, id := range ids {
		if peer := s.peerByID(id); peer != nil {
			peers = append(peers, peer)
		}
	}
	s.runPeerMaintenance(
		ctx,
		peers,
		fastSyncPingFanout,
		s.pingFastSyncPeer,
	)
}

func (s *overlaySubscription) pingFastSyncPeer(
	ctx context.Context,
	peer *overlayPeer,
) {
	s.probeFastSyncPeerCapabilities(ctx, peer)
}

func (s *overlaySubscription) probeFastSyncPeerCapabilities(
	ctx context.Context,
	peer *overlayPeer,
) bool {
	queryCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	startedAt := time.Now()
	var capabilities Capabilities
	err := peer.queryTransport.Query(
		queryCtx,
		fastSyncPingMaxAnswer,
		GetCapabilities{},
		&capabilities,
	)
	if err != nil {
		s.fastSync.setPeerAlive(peer.id, false)
		s.handlePeerQueryFailure(peer, err)
		s.log.Debug().
			Err(err).
			Str("peer", peer.addr).
			Msg("FastSync capabilities query failed")
		return false
	}

	peer.applyCapabilities(capabilities)
	s.fastSync.setPeerAlive(peer.id, true)
	if peer.querySuccess(time.Since(startedAt)) {
		s.peerPromoted(peer)
	}
	return true
}

func (s *overlaySubscription) startFastSyncPeerPing(peer *overlayPeer) {
	if !s.isActive() || !peer.tryBeginWarmup() {
		return
	}

	s.node.runAsync(func() {
		defer peer.finishWarmup()
		s.pingFastSyncPeer(s.node.runCtx, peer)
	})
}

func (s *overlaySubscription) exchangeFastSyncRandomPeers(
	ctx context.Context,
	peer *overlayPeer,
) {
	if !s.isActive() || s.fastSync == nil {
		return
	}

	local, err := s.fastSync.peers.LocalNode(time.Now())
	if err != nil {
		s.log.Debug().
			Err(err).
			Msg("failed to create local FastSync node")
		return
	}

	queryCtx, cancel := context.WithTimeout(
		ctx,
		fastSyncRandomPeersTimeout,
	)
	defer cancel()

	var response overlay.NodesV2
	// cppnode keeps overlay peer discovery on OverlayManager's ADNL path;
	// only the FastSync application query sender is QUIC-backed.
	err = peer.overlay.Query(
		queryCtx,
		overlay.GetRandomPeersV2{
			Peers: overlay.NodesV2{
				Nodes: []overlay.NodeV2{local},
			},
		},
		&response,
	)
	if err != nil {
		s.log.Debug().
			Err(err).
			Str("peer", peer.addr).
			Msg("FastSync random peer exchange failed")
		return
	}
	if len(response.Nodes) > fastsync.RandomPeerResultLimit {
		s.log.Debug().
			Int("nodes", len(response.Nodes)).
			Int("maximum", fastsync.RandomPeerResultLimit).
			Str("peer", peer.addr).
			Msg("rejected oversized FastSync random peer response")
		return
	}

	s.learnFastSyncNodes(response.Nodes, time.Now())
}

func (s *overlaySubscription) handleFastSyncRandomPeers(
	query overlay.GetRandomPeersV2,
) (overlay.NodesV2, error) {
	if len(query.Peers.Nodes) > fastsync.RandomPeerResultLimit {
		return overlay.NodesV2{}, fmt.Errorf(
			"FastSync random peer request contains %d nodes, maximum is %d",
			len(query.Peers.Nodes),
			fastsync.RandomPeerResultLimit,
		)
	}

	now := time.Now()
	s.learnFastSyncNodes(query.Peers.Nodes, now)
	return s.fastSync.peers.RandomPeers(now, rand.Uint64())
}

func (s *overlaySubscription) learnFastSyncNodes(
	nodes []overlay.NodeV2,
	now time.Time,
) {
	if len(nodes) == 0 {
		return
	}

	// Enrol first, unconditionally: gossip arrives about once a second, while a
	// dial round walks its whole list at dhtSeedPeerTimeout each, so gating
	// enrolment on the dial guard would drop most of what we learn whenever a
	// batch of unreachable peers is being tried. EnrollNode is already bounded
	// by the peer runtime's descriptor window.
	learned := make([]PeerID, 0, len(nodes))
	for _, node := range nodes {
		id, err := s.fastSync.peers.EnrollNode(node, now)
		if errors.Is(err, fastsync.ErrPeerIsLocal) {
			continue
		}
		if err != nil {
			s.log.Debug().
				Err(err).
				Msg("rejected FastSync node descriptor")
			continue
		}
		learned = append(learned, PeerID(id))
	}

	if len(learned) == 0 {
		return
	}

	// Only the dial loop is single-flight.
	if !s.advertisedPeerLearning.CompareAndSwap(false, true) {
		return
	}

	s.node.runAsync(func() {
		defer s.advertisedPeerLearning.Store(false)

		for _, id := range learned {
			if s.node.runCtx.Err() != nil || s.peerByID(id) != nil {
				continue
			}
			connectCtx, cancel := context.WithTimeout(
				s.node.runCtx,
				dhtSeedPeerTimeout,
			)
			_, err := s.connectFixedNode(connectCtx, id)
			cancel()
			if err != nil {
				s.log.Debug().
					Err(err).
					Str("peer_id", id.String()).
					Msg("failed to connect learned FastSync peer")
			}
		}
	})
}

func (n *Node) readyFastSyncQuerySubscription(
	block ton.BlockIDExt,
) (*overlaySubscription, error) {
	sub, err := n.fastSyncSubscriptionForBlock(block)
	if err != nil {
		return nil, err
	}
	if sub == nil || sub.fastSync == nil || !sub.fastSync.ready() {
		return nil, nil
	}
	return sub, nil
}

func (n *Node) fastSyncSubscriptionForBlock(
	block ton.BlockIDExt,
) (*overlaySubscription, error) {
	depth, err := sharddomain.PrefixLength(block.Shard)
	if err != nil {
		return nil, fmt.Errorf("invalid fast-sync block shard %d:%016x: %w", block.Workchain, uint64(block.Shard), err)
	}

	shard := FastSyncShard{
		Workchain: block.Workchain,
		Shard:     block.Shard,
	}
	if shard.Workchain == -1 {
		shard.Shard = topShard
		depth = 0
	}

	n.subscriptionsMx.RLock()
	defer n.subscriptionsMx.RUnlock()

	for {
		sub := n.fastSyncSubscriptions[shard]
		if sub != nil {
			return sub, nil
		}
		if depth == 0 {
			return nil, nil
		}
		parent, err := sharddomain.Parent(shard.Shard)
		if err != nil {
			return nil, fmt.Errorf("parent fast-sync shard %d:%016x: %w", shard.Workchain, uint64(shard.Shard), err)
		}
		shard.Shard = parent
		depth--
	}
}
