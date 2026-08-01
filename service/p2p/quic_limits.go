package p2p

import (
	adnlquic "github.com/xssnick/tonutils-go/adnl/quic"
)

// The library defaults size QUIC for a general-purpose node and mirror the C++
// reference, where an accepted connection costs little. Here each one costs ~4
// goroutines and its buffers, and the working set is tens of peers: plumtree
// needs at most plumtreeActiveNeighbourLimit per overlay, and the node caps its
// own dialing at maxOutboundQUICPaths. So the defaults are narrowed for this
// node rather than for every tonutils user.
const (
	// nodeMaxQUICConnections bounds accepted inbound connections. Reaching it
	// costs no handshake — quic-go rejects at the ClientInfo hook, before the
	// TLS exchange — but every accepted connection is real memory, so the bound
	// is set from the working set plus a wide margin.
	nodeMaxQUICConnections = 2048
	// nodeMaxQUICConnectionsPerIP: a legitimate peer opens one path to us. Tens
	// allow for several nodes behind one address; a thousand only ever means a
	// flood from a single host.
	nodeMaxQUICConnectionsPerIP = 32
	// nodeMaxQUICPeerPaths bounds the shared inbound/outbound path table.
	nodeMaxQUICPeerPaths = 2048
	// nodeQUICOutboundPathReserve keeps room for our own dialing no matter how
	// much inbound pressure there is. It must exceed maxOutboundQUICPaths, or
	// the sweep would free slots straight into a flood.
	nodeQUICOutboundPathReserve = 512
)

func nodeQUICLimits() adnlquic.Limits {
	limits := adnlquic.DefaultLimits()
	limits.MaxConnections = nodeMaxQUICConnections
	limits.MaxConnectionsPerIP = nodeMaxQUICConnectionsPerIP
	limits.MaxPeerPaths = nodeMaxQUICPeerPaths
	limits.OutboundPathReserve = nodeQUICOutboundPathReserve
	return limits
}
