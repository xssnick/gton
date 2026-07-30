package p2p

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	adnlquic "github.com/xssnick/tonutils-go/adnl/quic"
)

func testQUICLimitsKey() ed25519.PrivateKey {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		panic(err)
	}
	return priv
}

func defaultQUICLimits() adnlquic.Limits { return adnlquic.DefaultLimits() }

// limitsValidate exercises the library's own validation through the only
// exported entry point that runs it.
func limitsValidate(limits adnlquic.Limits) error {
	_, err := adnlquic.NewGatewayWithLimits(limits, testQUICLimitsKey())
	return err
}

// The QUIC limits are the node's only defence against a remote party filling
// its connection tables. These pin the invariants that make them a defence
// rather than a formality.
func TestNodeQUICLimitsAreSaneForThisNode(t *testing.T) {
	limits := nodeQUICLimits()

	if err := limitsValidate(limits); err != nil {
		t.Fatalf("node limits rejected by the library: %v", err)
	}

	// The reserve must strictly exceed what our own sweep allows us to dial,
	// otherwise the sweep frees outbound slots straight into an inbound flood.
	if limits.OutboundPathReserve <= maxOutboundQUICPaths {
		t.Fatalf("outbound reserve %d must exceed the outbound cap %d",
			limits.OutboundPathReserve, maxOutboundQUICPaths)
	}

	// The reserve is only meaningful while it is smaller than the table.
	if limits.OutboundPathReserve >= limits.MaxPeerPaths {
		t.Fatalf("reserve %d must be smaller than the path table %d",
			limits.OutboundPathReserve, limits.MaxPeerPaths)
	}

	// Inbound must still have room for a healthy public node's peers.
	inbound := limits.MaxPeerPaths - limits.OutboundPathReserve
	if inbound < 4*maxLivePeersPerOverlay {
		t.Fatalf("inbound budget %d is too tight for %d live peers per overlay",
			inbound, maxLivePeersPerOverlay)
	}

	// Per-IP must be far below the global cap, or one host can fill everything.
	if limits.MaxConnectionsPerIP*4 > limits.MaxConnections {
		t.Fatalf("per-IP cap %d is too close to the global cap %d",
			limits.MaxConnectionsPerIP, limits.MaxConnections)
	}
}

func TestNodeQUICLimitsAreTighterThanLibraryDefaults(t *testing.T) {
	limits := nodeQUICLimits()
	defaults := defaultQUICLimits()

	if limits.MaxConnections >= defaults.MaxConnections {
		t.Fatalf("node inbound cap %d does not tighten the default %d",
			limits.MaxConnections, defaults.MaxConnections)
	}
	if limits.MaxConnectionsPerIP >= defaults.MaxConnectionsPerIP {
		t.Fatalf("node per-IP cap %d does not tighten the default %d",
			limits.MaxConnectionsPerIP, defaults.MaxConnectionsPerIP)
	}
	if limits.MaxPeerPaths >= defaults.MaxPeerPaths {
		t.Fatalf("node path table %d does not tighten the default %d",
			limits.MaxPeerPaths, defaults.MaxPeerPaths)
	}
}
