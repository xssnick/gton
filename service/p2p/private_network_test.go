package p2p

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl/address"
)

func TestPrivateNetworkOptionsRequireDistinctExplicitNetwork(t *testing.T) {
	type testCase struct {
		name   string
		change func(*Options)
		want   string
	}
	tests := []testCase{
		{name: "missing key", change: func(o *Options) { o.PrivateNetwork.PrivateKey = nil }, want: "ADNL key"},
		{name: "seed instead of key", change: func(o *Options) { o.PrivateNetwork.PrivateKey = make([]byte, ed25519.SeedSize) }, want: "ADNL key"},
		{name: "primary key", change: func(o *Options) { o.PrivateNetwork.PrivateKey = o.PrivateKey }, want: "must differ"},
		{name: "DHT key", change: func(o *Options) { o.DHTPrivateKey = o.PrivateNetwork.PrivateKey }, want: "must differ"},
		{name: "missing listener", change: func(o *Options) { o.PrivateNetwork.ListenAddr = "" }, want: "listen endpoint"},
		{name: "hostname listener", change: func(o *Options) { o.PrivateNetwork.ListenAddr = "localhost:30305" }, want: "listen endpoint"},
		{name: "zero listener", change: func(o *Options) { o.PrivateNetwork.ListenAddr = "0.0.0.0:0" }, want: "ports must not be zero"},
		{name: "zero QUIC listener", change: func(o *Options) { o.PrivateNetwork.ListenAddr = "0.0.0.0:64536" }, want: "ports must not be zero"},
		{name: "missing external IP", change: func(o *Options) { o.PrivateNetwork.ExternalIP = nil }, want: "external IP"},
		{name: "unspecified external IP", change: func(o *Options) { o.PrivateNetwork.ExternalIP = net.IPv4zero }, want: "external IP"},
		{name: "multicast external IP", change: func(o *Options) { o.PrivateNetwork.ExternalIP = net.ParseIP("224.0.0.1") }, want: "external IP"},
		{name: "zero external port", change: func(o *Options) { o.PrivateNetwork.ExternalPort = 0 }, want: "ports must not be zero"},
		{name: "zero external QUIC port", change: func(o *Options) { o.PrivateNetwork.ExternalPort = 64536 }, want: "ports must not be zero"},
		{name: "primary ADNL collision", change: func(o *Options) { o.ListenAddr = "0.0.0.0:30305" }, want: "overlaps primary"},
		{name: "primary QUIC collision", change: func(o *Options) { o.ListenAddr = "0.0.0.0:29305" }, want: "overlaps primary"},
		{name: "private QUIC collision", change: func(o *Options) { o.ListenAddr = "0.0.0.0:31305" }, want: "overlaps primary"},
		{name: "DHT ADNL collision", change: func(o *Options) { o.DHTListenAddr = "0.0.0.0:30305" }, want: "overlaps DHT"},
		{name: "DHT QUIC collision", change: func(o *Options) { o.DHTListenAddr = "0.0.0.0:31305" }, want: "overlaps DHT"},
		{name: "dual stack collision", change: func(o *Options) { o.DHTListenAddr = "[::]:30305" }, want: "overlaps DHT"},
		{name: "DHT different interface", change: func(o *Options) { o.DHTListenAddr = "127.0.0.2:30305" }},
		{name: "IPv6 listener", change: func(o *Options) { o.PrivateNetwork.ListenAddr = "[::]:30305" }},
		{name: "wrapped QUIC ports", change: func(o *Options) {
			o.PrivateNetwork.ListenAddr = "0.0.0.0:65000"
			o.PrivateNetwork.ExternalPort = 65000
		}},
		{name: "advertised ADNL collision", change: func(o *Options) {
			o.ListenAddr = "0.0.0.0:30303"
			o.ExternalIP = net.ParseIP("127.0.0.1")
			o.ExternalPort = 30305
		}, want: "advertised address overlaps primary"},
		{name: "advertised QUIC collision", change: func(o *Options) {
			o.ListenAddr = "0.0.0.0:30303"
			o.ExternalIP = net.ParseIP("127.0.0.1")
			o.ExternalPort = 29305
		}, want: "advertised address overlaps primary"},
		{name: "advertised private QUIC collision", change: func(o *Options) {
			o.ListenAddr = "0.0.0.0:30303"
			o.ExternalIP = net.ParseIP("127.0.0.1")
			o.ExternalPort = 31305
		}, want: "advertised address overlaps primary"},
		{name: "advertised address derived from listener", change: func(o *Options) {
			o.PrivateNetwork.ListenAddr = "0.0.0.0:30306"
			o.ListenAddr = "127.0.0.1:30305"
		}, want: "advertised address overlaps primary"},
		{name: "advertised DHT collision", change: func(o *Options) {
			o.DHTListenAddr = "127.0.0.2:30305"
			o.ExternalIP = net.ParseIP("127.0.0.1")
		}, want: "advertised address overlaps DHT"},
		{name: "advertised DHT private QUIC collision", change: func(o *Options) {
			o.PrivateNetwork.ListenAddr = "127.0.0.1:30306"
			o.DHTListenAddr = "127.0.0.2:31305"
			o.ExternalIP = net.ParseIP("127.0.0.1")
		}, want: "advertised address overlaps DHT"},
		{name: "advertised different IP", change: func(o *Options) {
			o.ListenAddr = "0.0.0.0:30303"
			o.ExternalIP = net.ParseIP("127.0.0.2")
			o.ExternalPort = 30305
		}},
		{name: "primary client has no public listener", change: func(o *Options) {
			o.ExternalIP = net.ParseIP("127.0.0.1")
			o.ExternalPort = 30305
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := privateNetworkTestOptions(t)
			opts.PrivateNetwork.ListenAddr = "127.0.0.1:30305"
			tt.change(&opts)
			node, err := New(opts)
			if tt.want != "" {
				if err == nil || !strings.Contains(err.Error(), tt.want) {
					t.Fatalf("New error = %v, want %q", err, tt.want)
				}
				return
			}
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			node.EnterOffline("test complete before startup")
			waitForNodeStop(t, node)
		})
	}
}

func TestPrivateNetworkPartialStartFailureClosesListeners(t *testing.T) {
	for _, protocol := range []string{"ADNL", "QUIC"} {
		t.Run(protocol, func(t *testing.T) {
			opts := privateNetworkTestOptions(t)
			adnlEndpoint, quicEndpoint := testFreeQUICDerivedEndpoint(t)
			opts.PrivateNetwork.ListenAddr = adnlEndpoint.String()
			opts.PrivateNetwork.ExternalPort = adnlEndpoint.Port()
			busyEndpoint := adnlEndpoint
			if protocol == "QUIC" {
				busyEndpoint = quicEndpoint
			}
			busy, err := net.ListenPacket("udp4", busyEndpoint.String())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = busy.Close() })

			node, err := New(opts)
			if err != nil {
				t.Fatal(err)
			}
			err = node.Start(t.Context())
			if err == nil || !strings.Contains(err.Error(), "start private network gateways") {
				node.EnterOffline("test complete")
				t.Fatalf("Start error = %v, want private listener failure", err)
			}
			waitForNodeStop(t, node)
			if node.privateNetwork.networkStarted.Load() {
				t.Fatal("private network remained started after failure")
			}
			select {
			case <-node.privateNetwork.stopped:
			default:
				t.Fatal("private network shutdown did not finish")
			}
			if err = busy.Close(); err != nil {
				t.Fatal(err)
			}
			for _, endpoint := range []string{adnlEndpoint.String(), quicEndpoint.String()} {
				conn, err := net.ListenPacket("udp4", endpoint)
				if err != nil {
					t.Fatalf("private listener %s leaked after startup failure: %v", endpoint, err)
				}
				_ = conn.Close()
			}
		})
	}
}

func TestPrivateNetworkQUICFailureStopsParent(t *testing.T) {
	opts := privateNetworkTestOptions(t)
	endpoint, _ := testFreeQUICDerivedEndpoint(t)
	opts.PrivateNetwork.ListenAddr = endpoint.String()
	opts.PrivateNetwork.ExternalPort = endpoint.Port()
	node, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		node.EnterOffline("test complete")
		waitForNodeStop(t, node)
	})
	if err = node.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = node.privateNetwork.quicPacketConn.Close(); err != nil {
		t.Fatal(err)
	}
	waitForNodeStop(t, node)
	if !node.IsOffline() || !node.OfflineFailed() {
		t.Fatal("private QUIC failure did not mark the parent offline and failed")
	}
	if !strings.Contains(node.OfflineReason(), "private network QUIC") {
		t.Fatalf("unexpected failure reason: %s", node.OfflineReason())
	}
	select {
	case <-node.Failed():
	default:
		t.Fatal("private QUIC failure did not signal Failed")
	}
}

func TestPrivateNetworkStopBeforeStartSealsRegistry(t *testing.T) {
	node, err := New(privateNetworkTestOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	node.EnterOffline("test complete before startup")
	waitForNodeStop(t, node)
	_, err = node.PrivateOverlays().Open(PrivateOverlayConfig{
		FullID:  []byte("stopped private network"),
		Members: []PeerID{node.PrivateOverlays().LocalID()},
	}, PrivateOverlayCallbacks{})
	if !errors.Is(err, ErrOffline) {
		t.Fatalf("Open after parent stop = %v, want ErrOffline", err)
	}
}

type privateNetworkAddressDHT struct {
	dhtBackend
	key       ed25519.PrivateKey
	addresses address.List
}

func (d *privateNetworkAddressDHT) StoreAddress(_ context.Context, addresses address.List, _ time.Duration, key ed25519.PrivateKey) (int, []byte, error) {
	d.key = key
	d.addresses = addresses
	return 1, nil, nil
}

func TestPrivateNetworkAnnouncesOwnIdentityAndAddress(t *testing.T) {
	opts := privateNetworkTestOptions(t)
	node, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { node.EnterOffline("test complete") })
	private := node.privateNetwork
	backend := &privateNetworkAddressDHT{}
	private.dht = backend
	if err = private.configurePublicAddress(); err != nil {
		t.Fatal(err)
	}
	if err = private.announceSelf(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(backend.key, opts.PrivateNetwork.PrivateKey) {
		t.Fatal("announced private address using the wrong identity")
	}
	if len(backend.addresses.Addresses) != 1 {
		t.Fatalf("announced %d addresses, want 1", len(backend.addresses.Addresses))
	}
	addr, ok := backend.addresses.Addresses[0].(*address.UDP)
	if !ok || !addr.IP.Equal(opts.PrivateNetwork.ExternalIP) || addr.Port != int32(opts.PrivateNetwork.ExternalPort) {
		t.Fatalf("announced address = %#v, want configured private endpoint", backend.addresses.Addresses[0])
	}
}

func privateNetworkTestOptions(t *testing.T) Options {
	t.Helper()
	logger := discardLogger()
	return Options{
		Logger:        &logger,
		GlobalConfig:  lifecycleTestGlobalConfig(),
		PrivateKey:    ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0xa1}, ed25519.SeedSize)),
		PeerStorage:   newTestPeerStore(),
		StateFilesDir: t.TempDir(),
		PrivateNetwork: &PrivateNetworkOptions{
			PrivateKey:   ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0xa2}, ed25519.SeedSize)),
			ListenAddr:   "127.0.0.1:30305",
			ExternalIP:   net.ParseIP("127.0.0.1"),
			ExternalPort: 30305,
		},
	}
}
