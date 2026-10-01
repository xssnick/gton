package p2p

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/tl"
)

type privateNetworkDelivery struct {
	source  PeerID
	payload []byte
	err     error
}

type privateNetworkLoopback struct {
	node      *Node
	remote    *Node
	addresses []string
}

func TestPrivateNetworkLoopbackKeepsLocalIdentitiesSeparate(t *testing.T) {
	harness := startPrivateNetworkLoopback(t)
	node := harness.node
	remote := harness.remote
	privateID := node.PrivateOverlays().LocalID()
	if privateID == node.LocalID() {
		t.Fatal("dedicated network retained the ordinary ADNL identity")
	}
	wantPrivateID := probeTestPeerID(t, ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x92}, ed25519.SeedSize)))
	if privateID != wantPrivateID {
		t.Fatalf("private ADNL ID = %s, want configured key ID %s", privateID, wantPrivateID)
	}

	for _, protocol := range []string{"adnl", "rldp", "quic"} {
		t.Run(protocol, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			request := GetCapabilities{}
			requestBytes, err := tl.Serialize(request, true)
			if err != nil {
				t.Fatalf("serialize request: %v", err)
			}
			remoteMessages := make(chan privateNetworkDelivery, 4)
			remoteQueries := make(chan privateNetworkDelivery, 4)
			localMessages := make(chan privateNetworkDelivery, 2)
			privateMessages := make(chan privateNetworkDelivery, 2)
			cfg := PrivateOverlayConfig{
				FullID:  []byte("dedicated network same overlay " + protocol),
				Members: []PeerID{node.LocalID(), privateID, remote.LocalID()},
				UseQUIC: protocol == "quic",
			}
			remoteHandle := openPrivateNetworkLoopbackOverlay(t, remote.PrivateOverlays(), cfg, remoteMessages, remoteQueries)
			mainHandle := openPrivateNetworkLoopbackOverlay(t, node.privateOverlays, cfg, localMessages, nil)
			privateHandle := openPrivateNetworkLoopbackOverlay(t, node.PrivateOverlays(), cfg, privateMessages, nil)

			connectPrivateNetworkLoopbackPeer(t, mainHandle, remote)
			connectPrivateNetworkLoopbackPeer(t, privateHandle, remote)
			connectPrivateNetworkLoopbackPeer(t, remoteHandle, node)
			connectPrivateNetworkLoopbackPeer(t, remoteHandle, privateHandle.sub.node)

			for _, handle := range []*PrivateOverlay{mainHandle, privateHandle} {
				wantSource := handle.registry.LocalID()
				var answer Capabilities
				if protocol == "adnl" {
					peer, peerErr := handle.peer(remote.LocalID())
					if peerErr != nil {
						t.Fatalf("find remote ADNL peer: %v", peerErr)
					}
					err = peer.overlay.Query(ctx, request, &answer)
				} else {
					err = handle.Query(ctx, remote.LocalID(), 1024, request, &answer)
				}
				if err != nil {
					t.Fatalf("query from %s: %v", wantSource, err)
				}
				if answer.VersionMajor != 17 {
					t.Fatalf("query answer = %+v, want private callback response", answer)
				}
				assertPrivateNetworkDelivery(t, ctx, remoteQueries, wantSource, requestBytes)

				if protocol == "rldp" {
					err = handle.SendRLDPMessage(ctx, remote.LocalID(), request)
				} else {
					err = handle.SendMessage(ctx, remote.LocalID(), request)
				}
				if err != nil {
					t.Fatalf("send message from %s: %v", wantSource, err)
				}
				assertPrivateNetworkDelivery(t, ctx, remoteMessages, wantSource, requestBytes)
			}

			for _, target := range []PeerID{node.LocalID(), privateID} {
				if protocol == "rldp" {
					err = remoteHandle.SendRLDPMessage(ctx, target, request)
				} else {
					err = remoteHandle.SendMessage(ctx, target, request)
				}
				if err != nil {
					t.Fatalf("send reply to %s: %v", target, err)
				}
			}
			assertPrivateNetworkDelivery(t, ctx, localMessages, remote.LocalID(), requestBytes)
			assertPrivateNetworkDelivery(t, ctx, privateMessages, remote.LocalID(), requestBytes)
		})
	}
}

func TestPrivateNetworkTwoStepBroadcastUsesDedicatedIdentity(t *testing.T) {
	harness := startPrivateNetworkLoopback(t)
	private := harness.node.PrivateOverlays().node
	remotes := []*Node{harness.remote}
	for marker := byte(0x94); len(remotes) < overlay.DefaultBroadcastTwoStepFECMinPeers; marker++ {
		remotes = append(remotes, startPrivateNetworkLoopbackRemote(t, marker))
	}
	networks := append([]*Node{private}, remotes...)
	members := make([]PeerID, 0, len(networks))
	authorized := make(map[PeerID]uint32, len(networks))
	for _, network := range networks {
		members = append(members, network.LocalID())
		authorized[network.LocalID()] = 1 << 20
	}

	for _, protocol := range []string{"rldp", "quic"} {
		t.Run(protocol, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			cfg := PrivateOverlayConfig{
				FullID:                     []byte("dedicated two-step candidate " + protocol),
				Members:                    members,
				AuthorizedBroadcastSources: authorized,
				UseQUIC:                    protocol == "quic",
				EnableTwoStep:              true,
				TwoStepIntermediateMembers: members,
			}
			handles := make([]*PrivateOverlay, len(networks))
			deliveries := make([]chan PrivateOverlayBroadcast, len(networks))
			for i, network := range networks {
				delivered := make(chan PrivateOverlayBroadcast, 4)
				handle, err := network.PrivateOverlays().Open(cfg, PrivateOverlayCallbacks{
					Broadcast: func(_ context.Context, broadcast PrivateOverlayBroadcast) PrivateOverlayBroadcastDisposition {
						broadcast.Payload = bytes.Clone(broadcast.Payload)
						broadcast.Extra = bytes.Clone(broadcast.Extra)
						broadcast.SourceADNL = bytes.Clone(broadcast.SourceADNL)
						delivered <- broadcast
						return PrivateOverlayBroadcastAcceptAndRelay
					},
				})
				if err != nil {
					t.Fatalf("open candidate overlay on %s: %v", network.LocalID(), err)
				}
				t.Cleanup(func() { _ = handle.Close() })
				handles[i] = handle
				deliveries[i] = delivered
			}

			// The main identity has an active receiver for this same wire overlay
			// ID. Only the dedicated identity participates in the remote roster.
			mainCfg := cfg
			mainCfg.Members = append([]PeerID{harness.node.LocalID()}, members...)
			var mainDeliveries atomic.Int32
			mainHandle, err := harness.node.privateOverlays.Open(mainCfg, PrivateOverlayCallbacks{
				Broadcast: func(context.Context, PrivateOverlayBroadcast) PrivateOverlayBroadcastDisposition {
					mainDeliveries.Add(1)
					return PrivateOverlayBroadcastIgnore
				},
			})
			if err != nil {
				t.Fatalf("open same overlay on main identity: %v", err)
			}
			t.Cleanup(func() { _ = mainHandle.Close() })

			for i, handle := range handles {
				for j, network := range networks {
					if i == j {
						continue
					}
					connectPrivateNetworkLoopbackPeer(t, handle, network)
					if cfg.UseQUIC {
						peer, peerErr := handle.peer(network.LocalID())
						if peerErr != nil {
							t.Fatalf("find candidate QUIC peer: %v", peerErr)
						}
						if _, err = peer.dialQUIC(ctx); err != nil {
							t.Fatalf("connect candidate QUIC peer: %v", err)
						}
					}
				}
			}

			for _, mode := range []string{"simple", "fec"} {
				t.Run(mode, func(t *testing.T) {
					payloadSize := 16
					if mode == "fec" {
						payloadSize = 64 << 10
					}
					payload, err := tl.Serialize(IhrMessage{Data: bytes.Repeat([]byte{0xc7}, payloadSize)}, true)
					if err != nil {
						t.Fatalf("serialize candidate payload: %v", err)
					}
					extra := tl.Raw{0x01, 0x02, 0x03, 0x04}
					// The second sender exercises FEC decoding and relay on the sparse
					// dedicated Node, in addition to candidate origination from it.
					for source := range 2 {
						outcome, err := handles[source].BroadcastTwoStep(ctx, nil, payload, extra, 0)
						if err != nil {
							t.Fatalf("broadcast %s candidate from %s: %v", mode, networks[source].LocalID(), err)
						}
						if outcome.Attempted != len(remotes) || outcome.Sent != len(remotes) || outcome.Pending != 0 {
							t.Fatalf("candidate send outcome = %+v, want %d successful recipients", outcome, len(remotes))
						}
						for recipient, delivered := range deliveries {
							if recipient == source {
								continue
							}
							select {
							case broadcast := <-delivered:
								wantSource := networks[source].LocalID()
								if broadcast.Source != wantSource || !bytes.Equal(broadcast.SourceADNL, wantSource[:]) {
									t.Fatalf("candidate source/source ADNL = %s/%x, want %s", broadcast.Source, broadcast.SourceADNL, wantSource)
								}
								if broadcast.Delivery != DeliveryTwoStep || !bytes.Equal(broadcast.ID[:], outcome.BroadcastID) ||
									!bytes.Equal(broadcast.Payload, payload) || !bytes.Equal(broadcast.Extra, extra) {
									t.Fatalf("candidate delivery on %s did not preserve signed ID, payload, extra, and two-step mode", networks[recipient].LocalID())
								}
							case <-ctx.Done():
								t.Fatalf("wait for %s candidate on %s: %v", mode, networks[recipient].LocalID(), ctx.Err())
							}
						}
					}
				})
			}

			// Closing the receivers drains relay workers and callbacks before the
			// absence check, so it does not depend on a sleep or a polling window.
			for _, handle := range handles {
				_ = handle.Close()
			}
			_ = mainHandle.Close()
			if got := mainDeliveries.Load(); got != 0 {
				t.Fatalf("main identity received %d private candidate broadcasts", got)
			}
		})
	}
}

func TestPrivateNetworkRejectsOrdinaryOverlayIngress(t *testing.T) {
	harness := startPrivateNetworkLoopback(t)
	node := harness.node
	private := node.PrivateOverlays().node
	remote := harness.remote
	var publicOverlay *overlaySubscription
	for _, sub := range node.subscriptionsSnapshot() {
		if sub.spec.servesPublicIngress() {
			publicOverlay = sub
			break
		}
	}
	if publicOverlay == nil {
		t.Fatal("ordinary network has no public overlay")
	}

	query := overlay.WrapQuery(publicOverlay.spec.ShortID, GetCapabilities{})
	pooled := &pooledPeer{id: remote.LocalID()}
	if err := private.serveDetachedADNLQuery(pooled, &adnl.MessageQuery{
		ID:   make([]byte, 32),
		Data: query,
	}); !errors.Is(err, errDetachedUnknownOverlay) {
		t.Fatalf("private ADNL fullnode query error = %v, want unknown overlay", err)
	}
	if err := private.serveDetachedRLDPQuery(pooled, make([]byte, 32), &rldp.Query{
		ID:   make([]byte, 32),
		Data: query,
	}); !errors.Is(err, errDetachedUnknownOverlay) {
		t.Fatalf("private RLDP fullnode query error = %v, want unknown overlay", err)
	}

	wire, err := publicOverlay.quicEnvelope.Query(GetCapabilities{})
	if err != nil {
		t.Fatalf("serialize public QUIC query: %v", err)
	}
	if _, err = private.handleQUICQuery(context.Background(), &authenticatedQUICPeer{
		id: remote.LocalID(),
	}, wire); !errors.Is(err, errQUICOverlayNotFound) {
		t.Fatalf("private QUIC fullnode query error = %v, want unknown overlay", err)
	}
	if _, err = private.resolvePublicBroadcastReceiver(publicOverlay.spec.ShortID); !errors.Is(err, overlay.ErrBroadcastReceiverNotFound) {
		t.Fatalf("private public broadcast receiver error = %v, want not found", err)
	}
}

func TestPrivateNetworkParentShutdownReleasesBothTransports(t *testing.T) {
	harness := startPrivateNetworkLoopback(t)
	registry := harness.node.PrivateOverlays()
	handle := openPrivateNetworkLoopbackOverlay(t, registry, PrivateOverlayConfig{
		FullID:  []byte("dedicated network shutdown"),
		Members: []PeerID{registry.LocalID(), harness.remote.LocalID()},
		UseQUIC: true,
	}, nil, nil)
	connectPrivateNetworkLoopbackPeer(t, handle, harness.remote)

	harness.node.EnterOffline("loopback shutdown")
	waitForNodeStop(t, harness.node)
	if _, err := registry.Open(PrivateOverlayConfig{
		FullID:  []byte("after dedicated network shutdown"),
		Members: []PeerID{registry.LocalID()},
	}, PrivateOverlayCallbacks{}); !errors.Is(err, ErrOffline) {
		t.Fatalf("open after parent shutdown = %v, want ErrOffline", err)
	}
	if err := handle.SendMessage(context.Background(), harness.remote.LocalID(), GetCapabilities{}); !errors.Is(err, ErrPrivateOverlayClosed) {
		t.Fatalf("send after parent shutdown = %v, want closed overlay", err)
	}

	for _, addr := range harness.addresses {
		conn, err := net.ListenPacket("udp4", addr)
		if err != nil {
			t.Fatalf("bind released parent/private port %s: %v", addr, err)
		}
		if err = conn.Close(); err != nil {
			t.Fatalf("close rebound port %s: %v", addr, err)
		}
	}
}

func startPrivateNetworkLoopback(t *testing.T) privateNetworkLoopback {
	t.Helper()
	mainAddr, mainQUIC := testFreeQUICDerivedEndpoint(t)
	privateAddr, privateQUIC := testFreeQUICDerivedEndpoint(t)
	logger := discardLogger()
	node, err := New(Options{
		Logger:        &logger,
		GlobalConfig:  lifecycleTestGlobalConfig(),
		PrivateKey:    ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x91}, ed25519.SeedSize)),
		ListenAddr:    mainAddr.String(),
		PeerStorage:   newTestPeerStore(),
		StateFilesDir: t.TempDir(),
		PrivateNetwork: &PrivateNetworkOptions{
			PrivateKey:   ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x92}, ed25519.SeedSize)),
			ListenAddr:   privateAddr.String(),
			ExternalIP:   net.ParseIP("127.0.0.1"),
			ExternalPort: privateAddr.Port(),
		},
	})
	if err != nil {
		t.Fatalf("create node with private network: %v", err)
	}
	t.Cleanup(func() {
		node.EnterOffline("private network test complete")
		waitForNodeStop(t, node)
	})
	if err = node.Start(context.Background()); err != nil {
		t.Fatalf("start node with private network: %v", err)
	}

	return privateNetworkLoopback{
		node:   node,
		remote: startPrivateNetworkLoopbackRemote(t, 0x93),
		addresses: []string{
			mainAddr.String(), mainQUIC.String(), privateAddr.String(), privateQUIC.String(),
		},
	}
}

func startPrivateNetworkLoopbackRemote(t *testing.T, marker byte) *Node {
	t.Helper()
	remoteAddr, _ := testFreeQUICDerivedEndpoint(t)
	logger := discardLogger()
	remote, err := New(Options{
		Logger:        &logger,
		GlobalConfig:  lifecycleTestGlobalConfig(),
		PrivateKey:    ed25519.NewKeyFromSeed(bytes.Repeat([]byte{marker}, ed25519.SeedSize)),
		ListenAddr:    remoteAddr.String(),
		PeerStorage:   newTestPeerStore(),
		StateFilesDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create remote node: %v", err)
	}
	t.Cleanup(func() {
		remote.EnterOffline("private network test complete")
		waitForNodeStop(t, remote)
	})
	if err = remote.Start(context.Background()); err != nil {
		t.Fatalf("start remote node: %v", err)
	}

	return remote
}

func openPrivateNetworkLoopbackOverlay(
	t *testing.T,
	registry *PrivateOverlayRegistry,
	cfg PrivateOverlayConfig,
	messages chan<- privateNetworkDelivery,
	queries chan<- privateNetworkDelivery,
) *PrivateOverlay {
	t.Helper()
	handle, err := registry.Open(cfg, PrivateOverlayCallbacks{
		Message: func(_ context.Context, source PeerID, message tl.Serializable) {
			if messages != nil {
				payload, err := tl.Serialize(message, true)
				messages <- privateNetworkDelivery{source: source, payload: payload, err: err}
			}
		},
		Query: func(_ context.Context, source PeerID, query tl.Serializable) (tl.Serializable, error) {
			if queries != nil {
				payload, err := tl.Serialize(query, true)
				queries <- privateNetworkDelivery{source: source, payload: payload, err: err}
			}
			return Capabilities{VersionMajor: 17}, nil
		},
	})
	if err != nil {
		t.Fatalf("open loopback overlay: %v", err)
	}
	t.Cleanup(func() { _ = handle.Close() })
	return handle
}

func connectPrivateNetworkLoopbackPeer(t *testing.T, handle *PrivateOverlay, remote *Node) {
	t.Helper()
	addr, err := netip.ParseAddrPort(remote.listenAddr)
	if err != nil {
		t.Fatalf("parse remote endpoint: %v", err)
	}
	endpoint := peerEndpoint{
		adnlAddr: remote.listenAddr,
		quicAddr: netip.AddrPortFrom(addr.Addr(), addr.Port()+1000).String(),
	}
	pooled, release, err := handle.sub.node.acquirePeerEndpoint(remote.LocalID(), endpoint, remote.privKey.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatalf("acquire loopback peer: %v", err)
	}
	defer release()
	if !handle.sub.attachPooledPeer(pooled, nil) && handle.sub.peerByID(remote.LocalID()) == nil {
		t.Fatal("loopback peer was not attached")
	}
}

func assertPrivateNetworkDelivery(
	t *testing.T,
	ctx context.Context,
	deliveries <-chan privateNetworkDelivery,
	wantSource PeerID,
	wantPayload []byte,
) {
	t.Helper()
	select {
	case got := <-deliveries:
		if got.err != nil {
			t.Fatalf("serialize received payload: %v", got.err)
		}
		if got.source != wantSource || !bytes.Equal(got.payload, wantPayload) {
			t.Fatalf("received source/payload %s/%x, want %s/%x", got.source, got.payload, wantSource, wantPayload)
		}
	case <-ctx.Done():
		t.Fatalf("wait for private network delivery: %v", ctx.Err())
	}
}
