package p2p

import (
	"fmt"
	"testing"
	"time"
)

// orderPreferredPeers runs per download, per proof fetch and per masterchain
// probe, and it is quadratic in the roster size. The roster cap moved from 20
// to 300, so this benchmark guards the cost at the new size.
func benchmarkOrderPreferredPeers(b *testing.B, n int) {
	sub := testOverlaySubscription(&overlaySubscription{log: discardLogger()})
	candidates := make([]*overlayPeer, 0, n)
	now := time.Now()
	for i := 0; i < n; i++ {
		peer := testArchiveCandidate(fmt.Sprintf("bench-%03d", i))
		peer.unreliability = float64(i % 5)
		peer.roundtrip = time.Duration(20+i%50) * time.Millisecond
		peer.lastSuccessAt = now
		candidates = append(candidates, peer)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := sub.orderPreferredPeers(candidates, 0, 0); len(got) != n {
			b.Fatalf("ordered %d peers, want %d", len(got), n)
		}
	}
}

func BenchmarkOrderPreferredPeers20(b *testing.B)  { benchmarkOrderPreferredPeers(b, 20) }
func BenchmarkOrderPreferredPeers300(b *testing.B) { benchmarkOrderPreferredPeers(b, 300) }
