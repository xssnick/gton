package p2p

import (
	"testing"

	"github.com/xssnick/gton/service/p2p/internal/peerroute"
)

var peerRouteBenchmarkSink *peerroute.Route

func BenchmarkPeerRouteTableGetHit(b *testing.B) {
	table := peerroute.NewTable[PeerID](peerRouteRetryPolicy)
	var id PeerID
	table.Get(id)
	b.ReportAllocs()

	for b.Loop() {
		peerRouteBenchmarkSink = table.Get(id)
	}
}
