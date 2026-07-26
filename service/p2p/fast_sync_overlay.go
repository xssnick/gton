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
	membership *fastSyncMembership
	peers      *fastSyncPeerRuntime
	envelope   *quicOverlayEnvelope

	certificateMu   sync.Mutex
	certificateHash [sha256.Size]byte

	rootsMu        sync.Mutex
	aliveRoots     []PeerID
	aliveRootIndex map[PeerID]int
}

type fastSyncOverlayCounts struct {
	ValidatorsAlive int
	ValidatorsTotal int
	Peers           fastSyncPeerRuntimeCounts
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

	membership := newFastSyncMembership(spec.roster, spec.permanentFlags)
	certificate := fastSyncCertificateValue(spec.certificate)
	peers, err := newFastSyncPeerRuntime(
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
	if peers.LocalID() != node.localID {
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
	return r.membership.Contains(id)
}

func (r *fastSyncOverlayRuntime) permanent(id PeerID) bool {
	return r.membership.IsPermanent(id)
}

func (r *fastSyncOverlayRuntime) peerReceivesBroadcasts(id PeerID) bool {
	flags, err := r.membership.PeerFlags(id)
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

func (r *fastSyncOverlayRuntime) setPeerAlive(
	id PeerID,
	alive bool,
) {
	r.setValidatorAlive(id, alive)
	// Permanent validators can be queried before they publish NodeV2.
	// Their readiness lives in aliveRoots; the descriptor state is optional.
	_ = r.peers.SetAlive(id, alive)
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
	ids := r.spec.roster.adnlIDs
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

func (r *fastSyncOverlayRuntime) counts() fastSyncOverlayCounts {
	r.rootsMu.Lock()
	alive := len(r.aliveRoots)
	r.rootsMu.Unlock()

	return fastSyncOverlayCounts{
		ValidatorsAlive: alive,
		ValidatorsTotal: r.spec.roster.Len(),
		Peers:           r.peers.Counts(),
	}
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

	certificate, localValidator, err := n.fastSyncLocalCertificate(
		state.Roster,
		time.Now(),
	)
	if err != nil {
		return err
	}

	desiredShards := n.fastSyncDesiredShards(state.Shards)
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

func (n *Node) fastSyncLocalCertificate(
	roster FastSyncValidatorRoster,
	now time.Time,
) (*overlay.MemberCertificate, bool, error) {
	if roster.ContainsADNL(n.localID) {
		return nil, true, nil
	}

	certificates := n.fastSyncCertificateSnapshot()
	for i := range certificates {
		certificate := certificates[i]
		if certificate.IsExpired(now) {
			continue
		}
		if err := certificate.CheckSlot(FastSyncMemberSlotCount); err != nil {
			return nil, false, fmt.Errorf(
				"fast sync certificate %d: %w",
				i,
				err,
			)
		}
		if err := certificate.CheckSignature(n.localID[:]); err != nil {
			return nil, false, fmt.Errorf(
				"fast sync certificate %d: %w",
				i,
				err,
			)
		}

		issuer, err := certificate.IssuerID()
		if err != nil {
			return nil, false, fmt.Errorf(
				"fast sync certificate %d issuer: %w",
				i,
				err,
			)
		}
		issuerID, err := NewPeerID(issuer)
		if err != nil {
			return nil, false, fmt.Errorf(
				"fast sync certificate %d issuer: %w",
				i,
				err,
			)
		}
		if !roster.ContainsRoot(issuerID) {
			continue
		}

		cloned, err := cloneFastSyncCertificate(certificate)
		if err != nil {
			return nil, false, fmt.Errorf(
				"fast sync certificate %d: %w",
				i,
				err,
			)
		}
		return &cloned, false, nil
	}
	return nil, false, nil
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

func prepareFastSyncCertificates(
	certificates []overlay.MemberCertificate,
	localID PeerID,
) ([]overlay.MemberCertificate, error) {
	prepared := make(
		[]overlay.MemberCertificate,
		len(certificates),
	)
	for i, certificate := range certificates {
		if err := certificate.CheckSlot(FastSyncMemberSlotCount); err != nil {
			return nil, fmt.Errorf(
				"fast sync certificate %d: %w",
				i,
				err,
			)
		}
		if err := certificate.CheckSignature(localID[:]); err != nil {
			return nil, fmt.Errorf(
				"fast sync certificate %d: %w",
				i,
				err,
			)
		}

		cloned, err := cloneFastSyncCertificate(certificate)
		if err != nil {
			return nil, fmt.Errorf(
				"fast sync certificate %d: %w",
				i,
				err,
			)
		}
		prepared[i] = cloned
	}
	return prepared, nil
}

func (n *Node) fastSyncDesiredShards(
	shards []FastSyncShard,
) []FastSyncShard {
	desired := make([]FastSyncShard, 0, len(shards)+1)
	desired = append(desired, FastSyncShard{
		Workchain: -1,
		Shard:     topShard,
	})
	for _, shard := range shards {
		if shard.Shard == 0 || shard.Workchain == -1 {
			continue
		}

		depth := n.monitorMinSplitDepthForWorkchain(shard.Workchain)
		if uint32(fastSyncShardPrefixLength(shard.Shard)) > depth {
			shard.Shard = shardPrefix(shard.Shard, depth)
		}
		desired = append(desired, shard)
	}
	return NewFastSyncShardSet(desired).shards
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
	fixedNodes := roster.adnlIDs
	fixedIDs := make(map[PeerID]struct{}, len(fixedNodes))
	authorizedKeys := make(map[string]uint32, len(fixedNodes))
	for _, id := range fixedNodes {
		fixedIDs[id] = struct{}{}
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
		FixedNodeIDs:      fixedIDs,
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
		shard FastSyncShard
		spec  overlaySpec
		sub   *overlaySubscription
	}
	start := make([]pendingFastSyncSubscription, 0, len(desired))

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
			shard: shard,
			spec:  spec,
			sub:   sub,
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
	if len(response.Nodes) > fastSyncRandomPeerResultLimit {
		s.log.Debug().
			Int("nodes", len(response.Nodes)).
			Int("maximum", fastSyncRandomPeerResultLimit).
			Str("peer", peer.addr).
			Msg("rejected oversized FastSync random peer response")
		return
	}

	s.learnFastSyncNodes(response.Nodes, time.Now())
}

func (s *overlaySubscription) handleFastSyncRandomPeers(
	query overlay.GetRandomPeersV2,
) (overlay.NodesV2, error) {
	if len(query.Peers.Nodes) > fastSyncRandomPeerResultLimit {
		return overlay.NodesV2{}, fmt.Errorf(
			"FastSync random peer request contains %d nodes, maximum is %d",
			len(query.Peers.Nodes),
			fastSyncRandomPeerResultLimit,
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
	if len(nodes) == 0 ||
		!s.advertisedPeerLearning.CompareAndSwap(false, true) {
		return
	}

	learned := make([]PeerID, 0, len(nodes))
	for _, node := range nodes {
		id, err := s.fastSync.peers.EnrollNode(node, now)
		if errors.Is(err, errFastSyncPeerIsLocal) {
			continue
		}
		if err != nil {
			s.log.Debug().
				Err(err).
				Msg("rejected FastSync node descriptor")
			continue
		}
		learned = append(learned, id)
	}

	if len(learned) == 0 {
		s.advertisedPeerLearning.Store(false)
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
) *overlaySubscription {
	sub := n.fastSyncSubscriptionForBlock(block)
	if sub == nil || sub.fastSync == nil || !sub.fastSync.ready() {
		return nil
	}
	return sub
}

func (n *Node) fastSyncSubscriptionForBlock(
	block ton.BlockIDExt,
) *overlaySubscription {
	shard := FastSyncShard{
		Workchain: block.Workchain,
		Shard:     block.Shard,
	}
	if shard.Workchain == -1 {
		shard.Shard = topShard
	}

	n.subscriptionsMx.RLock()
	defer n.subscriptionsMx.RUnlock()

	for {
		sub := n.fastSyncSubscriptions[shard]
		if sub != nil {
			return sub
		}
		if fastSyncShardPrefixLength(shard.Shard) == 0 {
			return nil
		}
		shard.Shard = fastSyncShardParent(shard.Shard)
	}
}

func (s *overlaySubscription) fastSyncCounts() (
	fastSyncOverlayCounts,
	error,
) {
	if s.fastSync == nil {
		return fastSyncOverlayCounts{}, ErrFastSyncNotFound
	}
	return s.fastSync.counts(), nil
}
