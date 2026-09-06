package p2p

import "github.com/xssnick/tonutils-go/adnl/overlay"

const publicBroadcastFECMaxActiveStreams = 4096

// publicBroadcastFECMaxActiveBytes is the receiver's reservation budget for
// FEC broadcasts in flight on one public overlay: a bound on reserved
// estimates — decoders, relay parts, workspace, several times a payload — not
// on bytes held, and nothing to do with link bandwidth. Under a shard split on
// the stand the basechain overlay carried sixty block broadcasts a second over
// a saturated link; the streams that never completed reached the library
// default of one gibibyte, and every new stream was refused for minutes —
// external messages among them, which travel as FEC past 768 bytes — while
// the committee's blocks stayed full. Four gibibytes of estimate holds a few
// thousand block streams; the small-stream reserve inside the receiver keeps
// externals admitted even when that fills.
const publicBroadcastFECMaxActiveBytes = int64(4) << 30

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
