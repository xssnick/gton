package p2p

import (
	"crypto/ed25519"
	"math/rand/v2"
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
	// verified marks rows backed by contact with that peer rather than by
	// hearsay: it queried us over its own authenticated transport, or we dialled
	// it. Only these rows are protected from eviction, because only these cost
	// an attacker more than an ed25519 keypair to create.
	verified bool
}

// directoryTrust says how much a write may displace when the directory is full.
//
// A signed overlay.Node proves nothing about who sent it: identities are free to
// mint and announcements free to sign, so a peer forwarding a list of 300
// strangers must not be able to flush the rows we actually gossip with and
// promote from. Trust is a property of the write, not of the record.
type directoryTrust uint8

const (
	// directoryHearsay is an announcement a third party forwarded to us. It may
	// only take a slot that is itself unproven or already unusable.
	directoryHearsay directoryTrust = iota
	// directoryContacted is a row a peer created by talking to us: its transport
	// authenticated its key, so the row counts as verified and stops being
	// displaceable from here on. It is admitted on the hearsay allowance all the
	// same — handshaking from generated keys is cheap enough that granting the
	// full allowance would let an attacker flush the directory the long way
	// round. Incumbents keep their slots; newcomers take the dead ones.
	directoryContacted
	// directoryProven is a row we created ourselves: we resolved the peer in the
	// DHT and dialled it, or we attached it. Nothing a remote sends can drive
	// this, so it may displace another proven row.
	directoryProven
)

func (e *directoryEntry) clone() *directoryEntry {
	if e == nil {
		return nil
	}
	copied := *e
	copied.pub = append(ed25519.PublicKey(nil), e.pub...)
	copied.announced = cloneOverlayNode(e.announced)
	return &copied
}

// rememberDirectoryPeer records or refreshes a directory row and reports
// whether the row is now filed. Called with s.mx held.
func (s *overlaySubscription) rememberDirectoryPeerLocked(
	id PeerID,
	pub ed25519.PublicKey,
	adnlAddr string,
	quicAddr string,
	announced *overlay.Node,
	now time.Time,
	trust directoryTrust,
) bool {
	if id.IsZero() || len(pub) != ed25519.PublicKeySize {
		return false
	}
	if s.directory == nil {
		s.directory = map[PeerID]*directoryEntry{}
	}

	entry := s.directory[id]
	if entry == nil {
		if !s.evictDirectoryLocked(now, trust) {
			return false
		}
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
	if trust >= directoryContacted {
		entry.verified = true
	}
	entry.lastSeenAt = now
	return true
}

// evictDirectoryLocked makes room for one new row and reports whether the caller
// may file it. The directory never grows past maxPeersPerOverlay, so a write
// that finds nothing it is allowed to displace is refused.
//
// Victims are taken in classes, cheapest first. Everything a stranger can mint
// at will - rows whose announcement is stale or missing (unadvertisable and
// unpromotable anyway), then rows we never had contact with - goes before a row
// that proved itself, and a proven row is only ever displaced by another proven
// write. That is what keeps a flood of forged announcements, which is one
// getRandomPeers away, from flushing the peers we gossip with and promote from:
// the flood can only recycle its own class.
func (s *overlaySubscription) evictDirectoryLocked(now time.Time, trust directoryTrust) bool {
	if len(s.directory) < maxPeersPerOverlay {
		return true
	}

	var unusable, unverified, proven *directoryEntry
	for _, entry := range s.directory {
		if entry.live {
			continue
		}
		switch {
		case !announcedNodeIsFresh(entry.announced, now):
			unusable = colderDirectoryEntry(unusable, entry)
		case !entry.verified:
			unverified = colderDirectoryEntry(unverified, entry)
		default:
			proven = colderDirectoryEntry(proven, entry)
		}
	}

	victim := unusable
	if victim == nil {
		victim = unverified
	}
	if victim == nil && trust == directoryProven {
		victim = proven
	}
	if victim == nil {
		return false
	}
	delete(s.directory, victim.id)
	return true
}

func colderDirectoryEntry(current, candidate *directoryEntry) *directoryEntry {
	if current == nil || candidate.lastSeenAt.Before(current.lastSeenAt) {
		return candidate
	}
	return current
}

// directoryEntryReachableLocked reports whether we can contact a row without a
// DHT lookup: it either carries an address to dial, or the pool still holds a
// transport for it, which acquirePeerEndpoint reuses by id.
//
// Requiring an address alone silently excluded every peer that reached us
// first. On a public overlay an inbound peer never joins the roster, and the
// gossip that files its row carries no address - overlay.node has no address
// field - so those rows were neither gossip targets nor promotion candidates
// even while their transport sat in the pool.
func (s *overlaySubscription) directoryEntryReachableLocked(entry *directoryEntry) bool {
	if entry.adnlAddr != "" {
		return true
	}
	return s.node != nil && s.node.pool != nil && s.node.pool.hasTransport(entry.id)
}

func (s *overlaySubscription) markDirectoryLiveLocked(id PeerID, live bool) {
	if entry := s.directory[id]; entry != nil {
		entry.live = live
		if live {
			entry.lastSeenAt = time.Now()
		}
	}
}

// noteDirectoryActivity refreshes a row we already keep. For rows with no
// attachment this is the only liveness signal besides gossip, and without it a
// peer that talks to us constantly would still age out of the directory and
// stop being advertised.
//
// It deliberately does NOT file an unknown peer. This runs before admission and
// before the rate limiter, on a path any peer reaches with one unauthenticated
// overlay query, and a row filed here carries no signed announcement: useless to
// advertise, useless to promote, but it occupies one of maxPeersPerOverlay slots
// and evicts a row that does carry one. Cheaply repeated, that pins
// knownPeerCount at the cap, which stops learning from gossip and parks DHT
// discovery in refresh-only mode. Rows are filed where there is proof the peer
// is worth keeping - a signed overlay.Node, or an attachment we made ourselves.
func (s *overlaySubscription) noteDirectoryActivity(id PeerID, addr string) {
	s.mx.Lock()
	defer s.mx.Unlock()

	entry := s.directory[id]
	if entry == nil {
		return
	}
	entry.lastSeenAt = time.Now()
	// Contact over a transport authenticated with this peer's key: that is the
	// proof directoryProven stands for, so the row stops being displaceable by
	// forged announcements from here on.
	entry.verified = true
	if addr != "" {
		entry.adnlAddr = addr
	}
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
// is what keeps us present in other nodes' directories, so candidates are drawn
// from the full directory rather than the small live set.
//
// Only `limit` of them are ever returned, so only `limit` are copied, and the
// copies happen after s.mx is dropped. Building the whole list under the lock
// cost ~300 deep clones to answer with three - and s.mx is taken from under the
// plumtree engine lock on every forwarded broadcast part, so that time lands
// directly on block propagation.
func (s *overlaySubscription) advertisedDirectoryNodes(now time.Time, limit int) []overlay.Node {
	if limit <= 0 {
		return nil
	}

	selected := s.sampleAdvertisedNodes(now, limit)
	list := make([]overlay.Node, 0, len(selected))
	for _, node := range selected {
		list = append(list, *cloneOverlayNode(node))
	}
	return list
}

// sampleAdvertisedNodes reservoir-samples up to limit advertisable nodes. The
// returned pointers are never mutated in place by their owners (both
// directoryEntry.announced and overlayPeer.announced are replaced wholesale), so
// they stay safe to copy once the lock is gone.
func (s *overlaySubscription) sampleAdvertisedNodes(now time.Time, limit int) []*overlay.Node {
	s.mx.Lock()
	defer s.mx.Unlock()

	advertisable := func(node *overlay.Node) bool {
		return node != nil &&
			announcedNodeIsFresh(node, now) &&
			overlayNodeHasSerializableID(node)
	}

	reservoir := make([]*overlay.Node, 0, limit)
	considered := 0
	add := func(node *overlay.Node) {
		if !advertisable(node) {
			return
		}
		considered++
		if len(reservoir) < limit {
			reservoir = append(reservoir, node)
			return
		}
		if at := rand.IntN(considered); at < limit {
			reservoir[at] = node
		}
	}

	for _, entry := range s.directory {
		add(entry.announced)
	}
	// A live peer is known by definition; advertise it even if its directory
	// row has not caught up yet. Both maps are keyed by peer id, so checking the
	// directory row is the whole dedup - no seen-set needed.
	for id, peer := range s.peers {
		if entry, filed := s.directory[id]; filed && advertisable(entry.announced) {
			continue
		}
		add(peer.advertisedNodeRef(now))
	}
	return reservoir
}
