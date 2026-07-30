package p2p

import "github.com/xssnick/tonutils-go/adnl/overlay"

const publicBroadcastFECMaxActiveStreams = 4096

// broadcastFECRelayFanout bounds how many peers a received FEC part is
// forwarded to. Matches the C++ reference OverlayOptions::propagate_broadcast_to
// (5 of its neighbours, independent of how many peers it knows) and the node's
// own rebroadcastFanout.
const broadcastFECRelayFanout = 5

type overlayFECRelayPeerSet struct {
	sub *overlaySubscription
}

func (set overlayFECRelayPeerSet) Peers() []overlay.BroadcastPeer {
	// Called for every received FEC part, and the relay emits one send per
	// peer per part, so this must be the bounded relay subset and never the
	// whole roster: with a 300-peer roster the latter costs ~40k UDP sends/s
	// (~550 Mbit/s) of pure relay egress at head rates.
	return set.sub.broadcastTargetsSnapshot().relay
}
