package p2p

import (
	"crypto/ed25519"
	"time"

	"github.com/xssnick/tonutils-go/adnl/overlay"
)

// The overlay keeps two tiers, mirroring the C++ reference.
//
// The DIRECTORY is what the node knows: signed announcements plus a dial
// endpoint, up to maxPeersPerOverlay rows of a few hundred bytes each, with no
// transport, no goroutines and no obligation to ever contact the peer. It is
// what we advertise through overlay.getRandomPeers — that breadth is what keeps
// us in other nodes' peer lists and therefore in their broadcast fanout — and it
// is the pool promotions draw from.
//
// The LIVE tier (s.peers) is what the node talks to: every row there owns a
// pooled ADNL transport, overlay and RLDP wrappers and rebroadcast workers, and
// costs an ADNL handshake plus warmup to create. C++ keeps ~30-46 of these per
// overlay (10 broadcast neighbours, 20 plumtree active, 16 query neighbours)
// while knowing 300, and a Go node paying ~50 KB and 5 goroutines per live row
// has even more reason to keep the two apart.
//
// Evicting a live row therefore no longer means forgetting the peer: its
// directory row survives, it is still advertised, and it can be promoted again.
// Serving it while unattached is handled by the detached query path.
type directoryEntry struct {
	id       PeerID
	pub      ed25519.PublicKey
	adnlAddr string
	quicAddr string
	// announced is the peer's own signed overlay node record. Advertising it is
	// only meaningful while it is fresh, which advertisedDirectoryNode checks.
	announced *overlay.Node
	// lastSeenAt is the last time this peer was observed alive: attached,
	// answered a query, or arrived in a fresh gossip response.
	lastSeenAt time.Time
	// live marks rows that currently hold an attachment, so the directory can
	// report how much of itself is promoted without touching s.peers.
	live bool
}

func (e *directoryEntry) clone() *directoryEntry {
	if e == nil {
		return nil
	}
	copied := *e
	copied.pub = append(ed25519.PublicKey(nil), e.pub...)
	copied.announced = cloneOverlayNode(e.announced)
	return &copied
}

// rememberDirectoryPeer records or refreshes a directory row. Called with s.mx
// held.
func (s *overlaySubscription) rememberDirectoryPeerLocked(
	id PeerID,
	pub ed25519.PublicKey,
	adnlAddr string,
	quicAddr string,
	announced *overlay.Node,
	now time.Time,
) {
	if id.IsZero() || len(pub) != ed25519.PublicKeySize {
		return
	}
	if s.directory == nil {
		s.directory = map[PeerID]*directoryEntry{}
	}

	entry := s.directory[id]
	if entry == nil {
		s.evictDirectoryLocked(now)
		entry = &directoryEntry{
			id:  id,
			pub: append(ed25519.PublicKey(nil), pub...),
		}
		s.directory[id] = entry
	}
	if adnlAddr != "" {
		entry.adnlAddr = adnlAddr
	}
	if quicAddr != "" {
		entry.quicAddr = quicAddr
	}
	if announced != nil &&
		(entry.announced == nil || announced.Version >= entry.announced.Version) {
		entry.announced = cloneOverlayNode(announced)
	}
	entry.lastSeenAt = now
}

// evictDirectoryLocked makes room for one new row, dropping the least recently
// seen entry that is not currently live.
func (s *overlaySubscription) evictDirectoryLocked(now time.Time) {
	if len(s.directory) < maxPeersPerOverlay {
		return
	}

	var victim *directoryEntry
	for _, entry := range s.directory {
		if entry.live {
			continue
		}
		if victim == nil || entry.lastSeenAt.Before(victim.lastSeenAt) {
			victim = entry
		}
	}
	if victim != nil {
		delete(s.directory, victim.id)
	}
}

func (s *overlaySubscription) markDirectoryLiveLocked(id PeerID, live bool) {
	if entry := s.directory[id]; entry != nil {
		entry.live = live
		if live {
			entry.lastSeenAt = time.Now()
		}
	}
}

// noteDirectoryActivity records that a peer contacted us. For rows with no
// attachment this is the only liveness signal besides gossip, and without it a
// peer that talks to us constantly would still age out of the directory and
// stop being advertised.
func (s *overlaySubscription) noteDirectoryActivity(id PeerID, pub ed25519.PublicKey, addr string) {
	s.mx.Lock()
	defer s.mx.Unlock()

	if entry := s.directory[id]; entry != nil {
		entry.lastSeenAt = time.Now()
		if addr != "" {
			entry.adnlAddr = addr
		}
		return
	}
	// An inbound peer we have never filed: record it, so a peer that found us
	// first is a promotion candidate like any other.
	s.rememberDirectoryPeerLocked(id, pub, addr, "", nil, time.Now())
}

func (s *overlaySubscription) forgetDirectoryPeer(id PeerID) {
	s.mx.Lock()
	delete(s.directory, id)
	s.mx.Unlock()
}

// directorySnapshot returns copies of the directory rows, newest first.
func (s *overlaySubscription) directorySnapshot() []*directoryEntry {
	s.mx.Lock()
	defer s.mx.Unlock()

	entries := make([]*directoryEntry, 0, len(s.directory))
	for _, entry := range s.directory {
		entries = append(entries, entry.clone())
	}
	return entries
}

// knownPeerCount is how many peers this overlay knows: the directory, which is
// the quantity every discovery gate is really about. Counting the live tier
// instead makes those gates compare a number capped at maxLivePeersPerOverlay
// against maxPeersPerOverlay, so they never fire and discovery never stops.
func (s *overlaySubscription) knownPeerCount() int {
	s.mx.Lock()
	defer s.mx.Unlock()

	known := len(s.directory)
	for id := range s.peers {
		if _, filed := s.directory[id]; !filed {
			known++
		}
	}
	return known
}

func (s *overlaySubscription) directorySize() int {
	s.mx.Lock()
	defer s.mx.Unlock()

	return len(s.directory)
}

// advertisedDirectoryNodes are the announcements this node gossips. Breadth here
// is what keeps us present in other nodes' directories, so it is drawn from the
// full directory rather than the small live set.
func (s *overlaySubscription) advertisedDirectoryNodes(now time.Time) []overlay.Node {
	s.mx.Lock()
	defer s.mx.Unlock()

	list := make([]overlay.Node, 0, len(s.directory)+len(s.peers))
	seen := make(map[PeerID]struct{}, len(s.directory)+len(s.peers))
	add := func(id PeerID, node *overlay.Node) {
		if node == nil || !announcedNodeIsFresh(node, now) || !overlayNodeHasSerializableID(node) {
			return
		}
		if _, dup := seen[id]; dup {
			return
		}
		seen[id] = struct{}{}
		list = append(list, *cloneOverlayNode(node))
	}

	for id, entry := range s.directory {
		add(id, entry.announced)
	}
	// A live peer is known by definition; advertise it even if its directory
	// row has not caught up yet.
	for id, peer := range s.peers {
		add(id, peer.advertisedNodeSnapshot(now))
	}
	return list
}
