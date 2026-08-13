package p2p

import (
	"context"
	"math/rand/v2"
	"time"
)

// promoteBatchSize bounds how many peers one top-up pass attaches. Promotions
// cost a handshake and a warmup each, so the live tier refills over a few
// seconds rather than in one burst — which also spreads the load across
// different directory rows as gossip keeps arriving.
const promoteBatchSize = 4

// topUpLiveTier promotes directory rows while the live tier is below its limit.
// The live tier drains naturally (peers disconnect, get evicted, or are demoted
// when idle), and nothing else refills it deliberately: gossip only attaches
// what it happens to learn, so without this the node would slowly slide towards
// an empty live tier while its directory stayed full.
func (s *overlaySubscription) topUpLiveTier(ctx context.Context) {
	if !s.isActive() || !s.spec.hasDirectoryTier() {
		return
	}

	candidates := s.promotionCandidates()
	promoted := 0
	for _, entry := range candidates {
		if promoted >= promoteBatchSize || ctx.Err() != nil {
			break
		}
		if s.liveTierRoom() <= 0 {
			break
		}
		if s.promoteDirectoryPeer(entry) {
			promoted++
		}
	}
	if promoted > 0 {
		s.log.Debug().
			Int("promoted", promoted).
			Int("directory", s.directorySize()).
			Msg("promoted directory peers into the live tier")
	}
}

func (s *overlaySubscription) liveTierRoom() int {
	s.mx.Lock()
	defer s.mx.Unlock()

	return s.livePeerLimit() - len(s.peers)
}

// promotionCandidates are directory rows worth attaching: not already live,
// reachable without a DHT lookup and carrying a fresh announcement. Shuffled so
// every node does not converge on the same few peers.
func (s *overlaySubscription) promotionCandidates() []*directoryEntry {
	now := time.Now()

	s.mx.Lock()
	candidates := make([]*directoryEntry, 0, len(s.directory))
	for id, entry := range s.directory {
		if entry.live {
			continue
		}
		if _, attached := s.peers[id]; attached {
			continue
		}
		if entry.announced == nil || !announcedNodeIsFresh(entry.announced, now) {
			continue
		}
		if !s.directoryEntryReachableLocked(entry) {
			continue
		}
		candidates = append(candidates, entry.clone())
	}
	s.mx.Unlock()

	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})
	return candidates
}

// promoteDirectoryPeer attaches one directory row. It goes through
// acquirePeerEndpoint like every other attach path, so identity is validated and
// the transport is released rather than leaked when the attach does not happen.
func (s *overlaySubscription) promoteDirectoryPeer(entry *directoryEntry) bool {
	pooled, release, err := s.node.acquirePeerEndpoint(entry.id, peerEndpoint{
		adnlAddr: entry.adnlAddr,
		quicAddr: entry.quicAddr,
	}, entry.pub)
	if err != nil {
		return false
	}
	attached := s.attachPooledPeer(pooled, entry.announced)
	release()
	return attached
}
