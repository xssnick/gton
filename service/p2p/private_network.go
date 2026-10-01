package p2p

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"net"
	"net/netip"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/p2p/internal/peerroute"
	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/keys"
	adnlquic "github.com/xssnick/tonutils-go/adnl/quic"
	"github.com/xssnick/tonutils-go/tl"
)

// PrivateNetworkOptions assigns private overlays to a separate identity and
// UDP listener. QUIC uses the ADNL port plus 1000, as on the primary network.
// All fields are required; the public address is published in the shared DHT.
type PrivateNetworkOptions struct {
	PrivateKey   ed25519.PrivateKey
	ListenAddr   string
	ExternalIP   net.IP
	ExternalPort uint16
}

func validatePrivateNetworkOptions(opts Options) error {
	private := opts.PrivateNetwork
	if len(private.PrivateKey) != ed25519.PrivateKeySize {
		return fmt.Errorf("private network ADNL key: expected %d bytes, got %d", ed25519.PrivateKeySize, len(private.PrivateKey))
	}
	if bytes.Equal(private.PrivateKey, opts.PrivateKey) || bytes.Equal(private.PrivateKey, opts.DHTPrivateKey) {
		return errors.New("private network ADNL key must differ from primary and DHT keys")
	}

	listen, err := netip.ParseAddrPort(private.ListenAddr)
	if err != nil {
		return fmt.Errorf("parse private network listen endpoint: %w", err)
	}
	if listen.Port() == 0 || listen.Port()+1000 == 0 {
		return errors.New("private network ADNL and QUIC listen ports must not be zero")
	}
	ip := private.ExternalIP.To4()
	if ip == nil || ip.IsUnspecified() || ip.IsMulticast() {
		return errors.New("private network external IP must be a concrete IPv4 address")
	}
	if private.ExternalPort == 0 || private.ExternalPort+1000 == 0 {
		return errors.New("private network ADNL and QUIC external ports must not be zero")
	}

	_, privateQUIC := quicListenEndpoint(listen)
	endpoints := []netip.AddrPort{listen, privateQUIC}
	privateIP, _ := netip.AddrFromSlice(ip)
	privateAddress := netip.AddrPortFrom(privateIP.Unmap(), private.ExternalPort)
	privateQUICAddress := netip.AddrPortFrom(privateIP.Unmap(), private.ExternalPort+1000)
	if opts.ListenAddr != "" {
		primary, err := netip.ParseAddrPort(opts.ListenAddr)
		if err != nil {
			return fmt.Errorf("parse primary network listen endpoint: %w", err)
		}
		_, primaryQUIC := quicListenEndpoint(primary)
		for _, endpoint := range endpoints {
			if privateListenEndpointsOverlap(endpoint, primary) || privateListenEndpointsOverlap(endpoint, primaryQUIC) {
				return fmt.Errorf("private network listener %s overlaps primary network", endpoint)
			}
		}

		primaryIP, _ := netip.AddrFromSlice(opts.ExternalIP)
		if len(opts.ExternalIP) == 0 {
			primaryIP = primary.Addr()
		}
		if primaryIP.IsValid() && !primaryIP.IsUnspecified() {
			port := opts.ExternalPort
			if port == 0 {
				port = primary.Port()
			}
			primaryAddress := netip.AddrPortFrom(primaryIP.Unmap(), port)
			primaryQUICAddress := netip.AddrPortFrom(primaryIP.Unmap(), port+1000)
			if privateAddress == primaryAddress || privateAddress == primaryQUICAddress ||
				privateQUICAddress == primaryAddress || privateQUICAddress == primaryQUICAddress {
				return errors.New("private network advertised address overlaps primary network")
			}
		}
	}
	if opts.DHTListenAddr != "" {
		dht, err := netip.ParseAddrPort(opts.DHTListenAddr)
		if err != nil {
			return fmt.Errorf("parse DHT listen endpoint: %w", err)
		}
		for _, endpoint := range endpoints {
			if privateListenEndpointsOverlap(endpoint, dht) {
				return fmt.Errorf("private network listener %s overlaps DHT", endpoint)
			}
		}
		dhtIP, _ := netip.AddrFromSlice(opts.ExternalIP)
		if dhtIP.IsValid() && !dhtIP.IsUnspecified() {
			dhtAddress := netip.AddrPortFrom(dhtIP.Unmap(), dht.Port())
			if privateAddress == dhtAddress || privateQUICAddress == dhtAddress {
				return errors.New("private network advertised address overlaps DHT")
			}
		}
	}
	return nil
}

func privateListenEndpointsOverlap(a, b netip.AddrPort) bool {
	if a.Port() != b.Port() {
		return false
	}

	aIP, bIP := a.Addr().Unmap(), b.Addr().Unmap()
	if aIP == bIP {
		return true
	}
	// ADNL's IPv6 wildcard also receives IPv4 packets.
	if (aIP.Is6() && aIP.IsUnspecified()) || (bIP.Is6() && bIP.IsUnspecified()) {
		return true
	}
	if aIP.Is4() != bIP.Is4() {
		return false
	}
	return aIP.IsUnspecified() || bIP.IsUnspecified()
}

// newPrivateNetwork constructs only the transport and subscription owners used
// by consensus and FastSync overlays. It deliberately does not call New: chain storage,
// public broadcasts, state directories and a DHT gateway belong to the parent.
func newPrivateNetwork(opts PrivateNetworkOptions, logger zerolog.Logger) (*Node, error) {
	priv := append(ed25519.PrivateKey(nil), opts.PrivateKey...)
	pub := priv.Public().(ed25519.PublicKey)
	id, err := tl.Hash(keys.PublicKeyED25519{Key: pub})
	if err != nil {
		return nil, fmt.Errorf("compute private network ADNL id: %w", err)
	}
	quicGateway, err := adnlquic.NewGatewayWithLimits(nodeQUICLimits(), priv)
	if err != nil {
		return nil, fmt.Errorf("create private network QUIC gateway: %w", err)
	}

	n := &Node{
		log:                   logger.With().Str("network", "private").Logger(),
		privKey:               priv,
		quicPublicKey:         pub,
		localID:               PeerID(id),
		listenAddr:            opts.ListenAddr,
		externalIP:            append(net.IP(nil), opts.ExternalIP...),
		externalPort:          opts.ExternalPort,
		gateway:               adnl.NewGateway(priv),
		quicGateway:           quicGateway,
		quicQuerySlots:        make(chan struct{}, inboundQUICQueryParallelism),
		quicOutboundDialSlots: make(chan struct{}, outboundQUICDialParallelism),
		quicPeers:             make(map[PeerID]*authenticatedQUICPeer),
		peerRoutes:            peerroute.NewTable[PeerID](peerRouteRetryPolicy),
		subscriptions:         make(map[string]*overlaySubscription),
		fastSyncSubscriptions: make(map[FastSyncShard]*overlaySubscription),
		peerUse:               make(map[PeerID]peerUse),
		runCtx:                context.Background(),
		stopped:               make(chan struct{}),
	}
	n.privateOverlays = newPrivateOverlayRegistry(n)
	// FastSync certificates are bound to the validator/collator ADNL identity.
	n.pool = newPeerPool(n.gateway, n.resolvePublicBroadcastReceiver, n.handlePeerCustomMessage, n.peerRoutes)
	n.pool.detachedQuery = &detachedQueryHandlers{
		adnl: n.serveDetachedADNLQuery,
		rldp: n.serveDetachedRLDPQuery,
	}
	return n, nil
}

// startPrivateNetwork is called once by the parent's startup owner, after its
// DHT has been initialized. All private workers finish before that DHT closes.
func (n *Node) startPrivateNetwork(ctx context.Context) error {
	private := n.privateNetwork
	private.runCtx, private.runCancel = context.WithCancel(ctx)
	private.dht = n.dht
	private.gateway.SetConnectionHandler(private.handleInboundPeer)
	private.quicGateway.SetConnectionHandler(private.handleInboundQUICPeer)
	if err := private.startGateway(); err != nil {
		return fmt.Errorf("start private network gateways: %w", err)
	}
	private.networkStarted.Store(true)
	if err := private.runCtx.Err(); err != nil {
		return err
	}
	if err := private.checkQUICServer(); err != nil {
		return fmt.Errorf("start private network QUIC: %w", err)
	}

	private.runAsync(func() {
		private.runSubscriptionLifecycleLoop(private.runCtx)
	})
	private.runAsync(func() {
		private.runQUICIdleSweepLoop(private.runCtx)
	})
	private.runAsync(func() {
		private.runAnnounceLoop(private.runCtx)
	})

	// This watcher can stop the parent itself, so it belongs to lifecycleWG,
	// which Wait joins after shutdown, rather than to the workers stop joins.
	n.lifecycleWG.Add(1)
	go func() {
		defer n.lifecycleWG.Done()
		select {
		case <-ctx.Done():
			return
		case <-private.quicServeDone:
		}
		if ctx.Err() != nil {
			return
		}
		reason := "private network QUIC gateway stopped unexpectedly"
		if private.quicServeErr != nil {
			reason = fmt.Sprintf("%s: %v", reason, private.quicServeErr)
		}
		private.log.Error().Str("reason", reason).Msg("private network failed")
		n.enterOfflineFailure(reason)
	}()

	private.log.Info().
		Str("adnl_id", private.localID.String()).
		Str("listen_addr", private.listenAddr).
		Str("quic_listen_addr", private.quicPacketConn.LocalAddr().String()).
		IPAddr("external_ip", private.externalIP).
		Uint16("external_port", private.externalPort).
		Msg("started private overlay network")
	return nil
}

// stopPrivateNetwork also handles an unstarted or partially started child. It
// never closes the DHT or touches the parent's storage and public queues.
func (n *Node) stopPrivateNetwork() {
	n.onceStop.Do(func() {
		if n.runCancel != nil {
			n.runCancel()
		}
		n.stopAcceptingInbound()
		if n.networkStarted.Swap(false) {
			_ = n.gateway.Close()
		}
		if n.quicServeDone != nil {
			_ = n.closeQUICGateway()
		}
		n.closeSubscriptions()

		n.asyncMx.Lock()
		n.asyncStopped = true
		n.asyncMx.Unlock()
		n.wg.Wait()
		n.inboundWG.Wait()
		close(n.stopped)
	})
}

// chainNode selects the shared chain workflow behind an overlay transport.
// A dedicated network owns its identity, peers and subscriptions; block serving,
// validation, caches and broadcast admission still belong to the primary node.
func (n *Node) chainNode() *Node {
	if n.primaryNetwork != nil {
		return n.primaryNetwork
	}
	return n
}
