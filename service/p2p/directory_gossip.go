package p2p

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/xssnick/tonutils-go/adnl/overlay"
)

// directoryGossipFanout is how many directory-only peers one refresh tick polls.
// C++ likewise sends a single getRandomPeers per second, one to a neighbour and
// one to a uniform draw over its whole peer list.
const directoryGossipFanout = 1

// gossipWithDirectoryPeers exchanges peer lists with rows that hold no
// attachment, over an ephemeral transport that is released immediately. This is
// what keeps the directory fresh — and keeps us present in the directories of
// peers we are not attached to — without paying an attachment per contact.
func (s *overlaySubscription) gossipWithDirectoryPeers(ctx context.Context) {
	if !s.isActive() || !s.spec.usesV1PeerGossip() {
		return
	}

	for _, entry := range s.directoryGossipTargets() {
		if ctx.Err() != nil {
			return
		}
		s.exchangeRandomPeersEphemeral(ctx, entry)
	}
}

func (s *overlaySubscription) directoryGossipTargets() []*directoryEntry {
	now := time.Now()

	s.mx.Lock()
	candidates := make([]*directoryEntry, 0, len(s.directory))
	for id, entry := range s.directory {
		if entry.live || entry.adnlAddr == "" {
			continue
		}
		if _, attached := s.peers[id]; attached {
			continue
		}
		// Only rows whose announcement is still fresh: a stale row cannot be
		// re-advertised anyway, and the peer is probably gone.
		if entry.announced == nil || !announcedNodeIsFresh(entry.announced, now) {
			continue
		}
		candidates = append(candidates, entry.clone())
	}
	s.mx.Unlock()

	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})
	if len(candidates) > directoryGossipFanout {
		candidates = candidates[:directoryGossipFanout]
	}
	return candidates
}

// exchangeRandomPeersEphemeral borrows a transport, runs one getRandomPeers
// exchange and gives it straight back. The released transport carries no
// references, so the pool's idle prune can reclaim it — unlike an attachment,
// which pins its transport for as long as it lives.
func (s *overlaySubscription) exchangeRandomPeersEphemeral(ctx context.Context, entry *directoryEntry) {
	pooled, release, err := s.node.acquirePeerEndpoint(entry.id, peerEndpoint{
		adnlAddr: entry.adnlAddr,
		quicAddr: entry.quicAddr,
	}, entry.pub)
	if err != nil {
		return
	}
	defer release()

	adnlOverlay, _, releaseOverlay, err := s.node.pool.acquireOverlay(pooled, s.broadcastReceiver, false)
	if err != nil {
		return
	}
	defer releaseOverlay()

	advertised, err := s.randomPeerAdvertisement()
	if err != nil {
		return
	}

	queryCtx, cancel := context.WithTimeout(ctx, dhtSeedPeerTimeout)
	defer cancel()

	var res overlay.NodesList
	if err = adnlOverlay.Query(queryCtx, overlay.GetRandomPeers{List: advertised}, &res); err != nil {
		s.log.Debug().
			Err(err).
			Str("peer_id", entry.id.String()).
			Msg("directory getRandomPeers exchange failed")
		return
	}

	s.noteDirectoryActivity(entry.id, entry.adnlAddr)
	for _, node := range res.List {
		s.learnAdvertisedPeer(ctx, node)
	}
}
