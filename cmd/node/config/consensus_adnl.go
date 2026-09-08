package config

import (
	"bytes"
	"crypto/ed25519"
	"fmt"
	"net/netip"
	"strings"

	"github.com/xssnick/gton/service/p2p"
)

type udpConfigEndpoint struct {
	field string
	addr  netip.AddrPort
}

func privateNetworkOptionsFromConfig(cfg Config) (*p2p.PrivateNetworkOptions, error) {
	network := cfg.ConsensusADNL
	if len(network.Key) != ed25519.SeedSize {
		return nil, fmt.Errorf("invalid consensus_adnl.key: expected %d-byte seed, got %d bytes",
			ed25519.SeedSize, len(network.Key))
	}
	if bytes.Equal(network.Key, cfg.ADNL.Key) {
		return nil, fmt.Errorf("consensus_adnl.key must differ from adnl.key")
	}
	if bytes.Equal(network.Key, cfg.DHT.Key) {
		return nil, fmt.Errorf("consensus_adnl.key must differ from dht.key")
	}

	listenAddr := strings.TrimSpace(network.ListenAddr)
	if err := validateConsensusListenEndpoints(cfg, listenAddr); err != nil {
		return nil, err
	}

	ip, port, err := parseExternalAddr(strings.TrimSpace(network.ExternalAddr), "consensus_adnl.external_addr")
	if err != nil {
		return nil, err
	}
	ip = ip.To4()
	if ip == nil || ip.IsUnspecified() || ip.IsMulticast() {
		return nil, fmt.Errorf("consensus_adnl.external_addr must advertise a concrete IPv4 address")
	}
	if port+1000 == 0 {
		return nil, fmt.Errorf("consensus_adnl.external_addr derives a zero QUIC port")
	}

	return &p2p.PrivateNetworkOptions{
		PrivateKey:   ed25519.NewKeyFromSeed(network.Key),
		ListenAddr:   listenAddr,
		ExternalIP:   ip,
		ExternalPort: port,
	}, nil
}

func validateConsensusListenEndpoints(cfg Config, listenAddr string) error {
	consensus, err := parseUDPListenEndpoint(listenAddr, "consensus_adnl.listen_addr")
	if err != nil {
		return err
	}
	consensusQUIC, err := quicConfigEndpoint(consensus)
	if err != nil {
		return err
	}

	endpoints := []udpConfigEndpoint{consensus, consensusQUIC}
	if raw := strings.TrimSpace(cfg.ADNL.ListenAddr); raw != "" {
		primary, err := parseUDPListenEndpoint(raw, "adnl.listen_addr")
		if err != nil {
			return err
		}
		primaryQUIC, err := quicConfigEndpoint(primary)
		if err != nil {
			return err
		}
		endpoints = append(endpoints, primary, primaryQUIC)
	}
	if raw := strings.TrimSpace(cfg.DHT.ListenAddr); raw != "" {
		dhtEndpoint, err := parseUDPListenEndpoint(raw, "dht.listen_addr")
		if err != nil {
			return err
		}
		endpoints = append(endpoints, dhtEndpoint)
	}

	for i, endpoint := range endpoints {
		for _, other := range endpoints[:i] {
			if udpListenEndpointsOverlap(endpoint.addr, other.addr) {
				return fmt.Errorf("%s (%s) conflicts with %s (%s)",
					endpoint.field, endpoint.addr, other.field, other.addr)
			}
		}
	}

	return nil
}

func validateConsensusExternalEndpoints(opts p2p.Options) error {
	var externalIP netip.Addr
	if ip := opts.ExternalIP.To4(); ip != nil && !ip.IsUnspecified() {
		externalIP = netip.AddrFrom4([4]byte(ip))
	}

	endpoints := make([]udpConfigEndpoint, 0, 3)
	if opts.ListenAddr != "" {
		listen, err := parseUDPListenEndpoint(opts.ListenAddr, "adnl.listen_addr")
		if err != nil {
			return err
		}
		primaryIP := externalIP
		// Match configurePublicAddress: only an absent external address derives
		// its IP from a concrete bind address. A wildcard advertises no IP.
		if len(opts.ExternalIP) == 0 {
			bindIP := listen.addr.Addr().Unmap()
			if bindIP.Is4() && !bindIP.IsUnspecified() {
				primaryIP = bindIP
			}
		}
		if primaryIP.IsValid() {
			port := opts.ExternalPort
			if port == 0 {
				port = listen.addr.Port()
			}
			endpoints = append(endpoints,
				udpConfigEndpoint{field: "adnl.external_addr", addr: netip.AddrPortFrom(primaryIP, port)},
				udpConfigEndpoint{field: "adnl.external_addr QUIC", addr: netip.AddrPortFrom(primaryIP, port+1000)},
			)
		}
	}
	// DHT publication uses the explicit main external IP and its own bind port;
	// unlike ordinary ADNL, it does not derive an IP from a listen address.
	if opts.DHTListenAddr != "" && externalIP.IsValid() {
		dht, err := parseUDPListenEndpoint(opts.DHTListenAddr, "dht.listen_addr")
		if err != nil {
			return err
		}
		endpoints = append(endpoints, udpConfigEndpoint{
			field: "dht advertised endpoint",
			addr:  netip.AddrPortFrom(externalIP, dht.addr.Port()),
		})
	}

	private := opts.PrivateNetwork
	privateIP := netip.AddrFrom4([4]byte(private.ExternalIP.To4()))
	privateEndpoints := []udpConfigEndpoint{
		{field: "consensus_adnl.external_addr", addr: netip.AddrPortFrom(privateIP, private.ExternalPort)},
		{field: "consensus_adnl.external_addr QUIC", addr: netip.AddrPortFrom(privateIP, private.ExternalPort+1000)},
	}
	for _, privateEndpoint := range privateEndpoints {
		for _, endpoint := range endpoints {
			if privateEndpoint.addr == endpoint.addr {
				return fmt.Errorf("%s (%s) conflicts with %s",
					privateEndpoint.field, privateEndpoint.addr, endpoint.field)
			}
		}
	}

	return nil
}

func parseUDPListenEndpoint(raw string, field string) (udpConfigEndpoint, error) {
	addr, err := netip.ParseAddrPort(raw)
	if err != nil {
		return udpConfigEndpoint{}, fmt.Errorf("invalid %s %q: %w", field, raw, err)
	}
	if addr.Port() == 0 {
		return udpConfigEndpoint{}, fmt.Errorf("%s port must not be zero", field)
	}

	return udpConfigEndpoint{field: field, addr: addr}, nil
}

func quicConfigEndpoint(adnlEndpoint udpConfigEndpoint) (udpConfigEndpoint, error) {
	// Match the C++-compatible IPv4 wildcard binding and uint16 port addition
	// in p2p.quicListenEndpoint, including wraparound at port 65535.
	port := adnlEndpoint.addr.Port() + 1000
	if port == 0 {
		return udpConfigEndpoint{}, fmt.Errorf("%s derives a zero QUIC port", adnlEndpoint.field)
	}

	return udpConfigEndpoint{
		field: adnlEndpoint.field + " QUIC",
		addr:  netip.AddrPortFrom(netip.IPv4Unspecified(), port),
	}, nil
}

func udpListenEndpointsOverlap(a, b netip.AddrPort) bool {
	if a.Port() != b.Port() {
		return false
	}

	aIP, bIP := a.Addr().Unmap(), b.Addr().Unmap()
	if aIP == bIP {
		return true
	}
	// ADNL's IPv6 wildcard socket also accepts IPv4 traffic.
	if (aIP.Is6() && aIP.IsUnspecified()) || (bIP.Is6() && bIP.IsUnspecified()) {
		return true
	}
	if aIP.Is4() != bIP.Is4() {
		return false
	}

	return aIP.IsUnspecified() || bIP.IsUnspecified()
}
