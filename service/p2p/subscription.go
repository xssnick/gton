package p2p

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/adnl"
	adnladdr "github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/tl"
)

type overlaySubscription struct {
	node *Node
	spec overlaySpec
	log  zerolog.Logger

	mx                  sync.Mutex
	seedMx              sync.Mutex
	archivePeerMx       sync.Mutex
	peers               map[string]*overlayPeer
	neighbours          []string
	lastPingedNeighbour string
	archivePeers        map[string]*archivePeerState
}

type archivePeerState struct {
	peer        *overlayPeer
	speed       float64
	probeAt     time.Time
	deniedPeers map[string]time.Time
}

type overlayPeer struct {
	id          string
	addr        string
	pub         ed25519.PublicKey
	announced   *overlay.Node
	overlay     *overlay.ADNLOverlayWrapper
	rldp        *overlay.RLDPWrapper
	rldpOverlay *overlay.RLDPOverlayWrapper

	statsMx           sync.Mutex
	versionMajor      int32
	versionMinor      int32
	capabilitiesFlags uint32
	roundtrip         time.Duration
	unreliability     float64
	missedPings       uint32
	alive             bool
	lastReceiveAt     time.Time
	lastSuccessAt     time.Time
	failedQueries     uint64
	archiveBytesSec   float64
	archiveDownloads  uint64
	archiveSlowUntil  time.Time
}

func (s *overlaySubscription) run(ctx context.Context) {
	s.log.Info().Msg("starting overlay peer discovery")
	s.seedFromDHT(ctx)

	dhtTicker := time.NewTicker(dhtRefreshInterval)
	defer dhtTicker.Stop()
	refreshTimer := time.NewTimer(nextPeerRefreshDelay())
	defer refreshTimer.Stop()
	neighbourTimer := time.NewTimer(0)
	defer neighbourTimer.Stop()
	pingTimer := time.NewTimer(nextPeerPingDelay())
	defer pingTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-dhtTicker.C:
			s.seedFromDHT(ctx)
		case <-refreshTimer.C:
			s.refreshPeers(ctx)
			refreshTimer.Reset(nextPeerRefreshDelay())
		case <-neighbourTimer.C:
			s.reloadNeighbours()
			neighbourTimer.Reset(nextNeighbourReloadDelay())
		case <-pingTimer.C:
			s.pingPeers(ctx)
			pingTimer.Reset(nextPeerPingDelay())
		}
	}
}

func (s *overlaySubscription) seedFromDHT(ctx context.Context) {
	s.seedMx.Lock()
	defer s.seedMx.Unlock()

	var (
		cont       *dht.Continuation
		err        error
		requests   int
		nodesSeen  int
		connected  int
		startedAt  = time.Now()
		knownStart = len(s.knownPeersSnapshot())
		aliveStart = s.aliveKnownPeerCount()
	)

	logSearch := aliveStart == 0
	if logSearch {
		s.log.Info().
			Int("known_peers", knownStart).
			Int("alive_peers", aliveStart).
			Msg("searching overlay peers in DHT")
	}

	for i := 0; i < 8 && s.aliveKnownPeerCount() < maxPeersPerOverlay; i++ {
		lookupCtx, cancel := context.WithTimeout(ctx, dhtFindTimeout)
		var nodes *overlay.NodesList
		if cont == nil {
			nodes, cont, err = s.node.dht.FindOverlayNodes(lookupCtx, s.spec.FullID)
		} else {
			nodes, cont, err = s.node.dht.FindOverlayNodes(lookupCtx, s.spec.FullID, cont)
		}
		cancel()
		if err != nil {
			if logSearch {
				s.log.Debug().
					Err(err).
					Dur("elapsed", time.Since(startedAt)).
					Int("dht_requests", requests+1).
					Msg("DHT overlay peer search failed")
				return
			}
			s.log.Debug().Err(err).Msg("DHT lookup failed")
			return
		}

		requests++
		nodesSeen += len(nodes.List)

		for _, node := range nodes.List {
			if s.aliveKnownPeerCount() >= maxPeersPerOverlay {
				break
			}
			attached, err := s.connectOverlayNodeV1(ctx, node)
			if err != nil {
				s.log.Debug().Err(err).Msg("failed to connect overlay node")
				continue
			}
			if attached {
				connected++
			}
		}

		if cont == nil {
			break
		}
	}

	if logSearch || connected > 0 {
		s.log.Debug().
			Dur("elapsed", time.Since(startedAt)).
			Int("dht_requests", requests).
			Int("dht_nodes", nodesSeen).
			Int("connected_peers", connected).
			Int("known_peers", len(s.knownPeersSnapshot())).
			Int("alive_peers", s.aliveKnownPeerCount()).
			Msg("overlay peer DHT search finished")
	}
}

func (s *overlaySubscription) connectOverlayNodeV1(ctx context.Context, node overlay.Node) (bool, error) {
	if err := node.CheckSignature(); err != nil {
		return false, fmt.Errorf("overlay node signature: %w", err)
	}
	now := time.Now()
	nodeTime := time.Unix(int64(node.Version), 0)
	if nodeTime.Before(now.Add(-overlayPeerTTL)) || nodeTime.After(now.Add(overlayFutureSkew)) {
		return false, fmt.Errorf("stale overlay node version: %d", node.Version)
	}
	if !bytes.Equal(node.Overlay, s.spec.ShortID) {
		return false, fmt.Errorf("overlay id mismatch")
	}

	pub, ok := node.ID.(keys.PublicKeyED25519)
	if !ok {
		return false, fmt.Errorf("unsupported overlay node key type %T", node.ID)
	}

	nodeID, err := tl.Hash(node.ID)
	if err != nil {
		return false, fmt.Errorf("hash overlay node id: %w", err)
	}

	peerID := hex.EncodeToString(nodeID)

	s.mx.Lock()
	if peer := s.peers[peerID]; peer != nil {
		peer.mergeAnnouncement(&node)
		s.mx.Unlock()
		s.reloadNeighbours()
		return false, nil
	}
	s.mx.Unlock()

	addrList, _, err := findPeerAddresses(ctx, s.node.dht, nodeID)
	if err != nil {
		return false, fmt.Errorf("find ADNL addresses: %w", err)
	}
	if len(addrList.Addresses) == 0 {
		return false, fmt.Errorf("overlay node has no addresses")
	}

	for _, addr := range addrList.Addresses {
		udpAddr, err := adnladdr.DialString(addr)
		if err != nil {
			continue
		}
		pooled, err := s.node.pool.Get(udpAddr, pub.Key)
		if err != nil {
			continue
		}
		attached := s.attachPooledPeer(pooled, &node)
		if attached {
			event := s.log.Debug()
			if s.aliveKnownPeerCount() <= 3 {
				event = s.log.Info()
			}
			event.
				Str("peer", pooled.addr).
				Str("peer_id", pooled.id).
				Msg("connected overlay peer")
		}
		return attached, nil
	}

	return false, fmt.Errorf("failed to dial any address for overlay peer")
}

func (s *overlaySubscription) attachPooledPeer(pooled *pooledPeer, announced *overlay.Node) bool {
	s.mx.Lock()
	if peer := s.peers[pooled.id]; peer != nil {
		peer.mergeAnnouncement(announced)
		s.mx.Unlock()
		return false
	}

	state := &overlayPeer{
		id:            pooled.id,
		addr:          pooled.addr,
		pub:           append(ed25519.PublicKey(nil), pooled.pub...),
		announced:     cloneOverlayNode(announced),
		overlay:       pooled.adnl.CreateOverlayWithSettings(s.spec.ShortID, maxOverlayPayloadSize, true, false),
		rldp:          pooled.rldp,
		rldpOverlay:   pooled.rldp.CreateOverlay(s.spec.ShortID),
		alive:         true,
		lastReceiveAt: time.Now(),
	}
	s.peers[pooled.id] = state
	s.mx.Unlock()

	s.installHandlers(state)
	if announced != nil {
		s.reloadNeighbours()
	}
	return true
}

func (s *overlaySubscription) installHandlers(peer *overlayPeer) {
	peer.overlay.SetBroadcastHandler(func(msg tl.Serializable, trusted bool) error {
		return s.handleOverlayBroadcast(peer, msg, DeliveryFEC, trusted, "")
	})
	peer.overlay.SetCustomMessageHandler(func(msg *adnl.MessageCustom) error {
		switch data := msg.Data.(type) {
		case overlay.Broadcast:
			return s.handleSimpleBroadcast(peer, data)
		case ForgetPeer:
			s.removePeer(peer.id)
			return nil
		default:
			return nil
		}
	})
	peer.overlay.SetQueryHandler(func(msg *adnl.MessageQuery) error {
		return s.answerADNLQuery(peer, msg)
	})
	peer.rldpOverlay.SetOnQuery(func(transferID []byte, query *rldp.Query) error {
		return s.answerRLDPQuery(peer, transferID, query)
	})
	peer.overlay.SetDisconnectHandler(func(_ string, key ed25519.PublicKey) {
		id, err := tl.Hash(keys.PublicKeyED25519{Key: key})
		if err != nil {
			return
		}
		s.removePeer(hex.EncodeToString(id))
	})
}

func (s *overlaySubscription) refreshPeers(ctx context.Context) {
	peers := s.refreshTargets()
	if len(peers) == 0 {
		return
	}

	for _, peer := range peers {
		select {
		case <-ctx.Done():
			return
		case <-peer.overlay.GetCloserCtx().Done():
			s.removePeer(peer.id)
			continue
		default:
		}
		s.exchangeRandomPeers(ctx, peer)
	}
}

func (s *overlaySubscription) exchangeRandomPeers(ctx context.Context, peer *overlayPeer) {
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var advertised overlay.NodesList
	self, err := s.node.selfOverlayNode(s.spec)
	if err != nil {
		s.log.Debug().Err(err).Msg("failed to create self overlay node")
		return
	}
	advertised.List = append(advertised.List, *self)
	for _, node := range s.overlayNodesSnapshot() {
		if len(advertised.List) >= maxRandomPeerReply {
			break
		}
		advertised.List = append(advertised.List, node)
	}

	startedAt := time.Now()
	var res overlay.NodesList
	err = peer.overlay.Query(queryCtx, overlay.GetRandomPeers{
		List: advertised,
	}, &res)
	if err != nil {
		s.handlePeerQueryFailure(peer, err)
		s.log.Debug().
			Err(err).
			Str("peer", peer.addr).
			Msg("overlay.getRandomPeers failed")
		return
	}
	peer.querySuccess(time.Since(startedAt))

	for _, node := range res.List {
		if s.aliveKnownPeerCount() >= maxPeersPerOverlay {
			return
		}
		if _, err := s.connectOverlayNodeV1(ctx, node); err != nil {
			s.log.Debug().Err(err).Msg("failed to connect peer learned from overlay")
		}
	}
}

func (s *overlaySubscription) pingPeers(ctx context.Context) {
	for _, peer := range s.pingTargets() {
		select {
		case <-ctx.Done():
			return
		case <-peer.overlay.GetCloserCtx().Done():
			s.removePeer(peer.id)
			continue
		default:
		}
		s.pingPeer(ctx, peer)
	}
}

func (s *overlaySubscription) pingPeer(ctx context.Context, peer *overlayPeer) {
	queryCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	startedAt := time.Now()
	var res Capabilities
	if err := peer.overlay.Query(queryCtx, GetCapabilities{}, &res); err != nil {
		s.handlePeerQueryFailure(peer, err)
		s.log.Debug().
			Err(err).
			Str("peer", peer.addr).
			Msg("tonNode.getCapabilities failed")
		return
	}

	peer.applyCapabilities(res)
	peer.querySuccess(time.Since(startedAt))
}

func (s *overlaySubscription) handleSimpleBroadcast(peer *overlayPeer, msg overlay.Broadcast) error {
	if peer != nil {
		peer.noteReceive()
	}
	if !checkSimpleBroadcastDate(msg.Date) {
		return nil
	}

	source, ok := msg.Source.(keys.PublicKeyED25519)
	if !ok {
		return nil
	}
	if len(msg.Data) == 0 || len(msg.Data) > maxOverlayPayloadSize {
		return nil
	}

	sourceID := make([]byte, 32)
	if msg.Flags&1 == 0 {
		var err error
		sourceID, err = tl.Hash(msg.Source)
		if err != nil {
			return nil
		}
	}

	broadcastHash, err := tl.Hash(OverlayBroadcastID{
		Source:   sourceID,
		DataHash: hashSimpleBroadcastPayload(msg.Data),
		Flags:    msg.Flags,
	})
	if err != nil {
		return nil
	}

	toSign, err := tl.Serialize(overlay.BroadcastToSign{
		Hash: broadcastHash,
		Date: uint32(msg.Date),
	}, true)
	if err != nil {
		return nil
	}

	if !ed25519.Verify(source.Key, toSign, msg.Signature) {
		return nil
	}

	var parsed any
	if _, err = tl.Parse(&parsed, msg.Data, true); err != nil {
		return nil
	}

	return s.handleOverlayBroadcast(nil, parsed, DeliverySimple, false, hex.EncodeToString(sourceID))
}

func (s *overlaySubscription) removePeer(id string) {
	s.mx.Lock()
	delete(s.peers, id)
	s.removeNeighbourLocked(id)
	s.mx.Unlock()
}

func (s *overlaySubscription) aliveKnownPeerCount() int {
	s.mx.Lock()
	defer s.mx.Unlock()

	count := 0
	now := time.Now()
	for _, peer := range s.peers {
		if !peer.isAliveKnownOverlayPeer(now) {
			continue
		}
		count++
	}
	return count
}

func (s *overlaySubscription) ensurePeers(ctx context.Context) error {
	for {
		if s.aliveKnownPeerCount() > 0 {
			return nil
		}

		s.seedFromDHT(ctx)
		if s.aliveKnownPeerCount() > 0 {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(downloadRetryDelay):
		}
	}
}

func (s *overlaySubscription) peersSnapshot() []*overlayPeer {
	s.mx.Lock()
	defer s.mx.Unlock()

	peers := make([]*overlayPeer, 0, len(s.peers))
	for _, peer := range s.peers {
		peers = append(peers, peer)
	}
	return peers
}

func (s *overlaySubscription) knownPeersSnapshot() []*overlayPeer {
	s.mx.Lock()
	defer s.mx.Unlock()

	peers := make([]*overlayPeer, 0, len(s.peers))
	now := time.Now()
	for _, peer := range s.peers {
		if !peer.isKnownOverlayPeer(now) || !peer.hasOpenConnection() {
			continue
		}
		peers = append(peers, peer)
	}
	return peers
}

func (s *overlaySubscription) aliveKnownPeersSnapshot() []*overlayPeer {
	s.mx.Lock()
	defer s.mx.Unlock()

	peers := make([]*overlayPeer, 0, len(s.peers))
	now := time.Now()
	for _, peer := range s.peers {
		if !peer.isAliveKnownOverlayPeer(now) || !peer.hasOpenConnection() {
			continue
		}
		peers = append(peers, peer)
	}
	return peers
}

func (s *overlaySubscription) overlayNodesSnapshot() []overlay.Node {
	s.mx.Lock()
	defer s.mx.Unlock()

	list := make([]overlay.Node, 0, len(s.peers))
	now := time.Now()
	for _, peer := range s.peers {
		if !peer.canAdvertise(now) {
			continue
		}
		list = append(list, *cloneOverlayNode(peer.announced))
	}
	return list
}
