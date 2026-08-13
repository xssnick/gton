package p2p

import (
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"sort"
	"time"

	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/tl"
)

func peerIDFromPublicKey(pub ed25519.PublicKey) (PeerID, error) {
	raw, err := tl.Hash(keys.PublicKeyED25519{Key: pub})
	if err != nil {
		return PeerID{}, err
	}
	return NewPeerID(raw)
}

// The overlay peer cache persists dial endpoints of proven roster peers so a
// restarted node rebuilds its edges immediately instead of waiting out DHT
// discovery and gossip lotteries. Entries are dial CANDIDATES only: nothing
// from the cache enters the roster without a live handshake and exchange, so
// stale records cost one failed dial and cannot poison anything. Composition
// is hardened against identity floods: only outbound-verified peers are
// stored, per-IP and per-subnet caps bound Sybil farms, and half of the slots
// go to peers that actually fed us broadcasts first (srcScore) — a
// proof-of-usefulness an attacker can only earn by being a good peer.
const (
	peerCacheFormatVersion = 1
	peerCacheMaxEntries    = 150
	// peerCacheScoreSlots slots are reserved for the best broadcast sources;
	// the rest is a random sample of verified peers for mesh diversity.
	peerCacheScoreSlots   = peerCacheMaxEntries / 2
	peerCacheMaxEntryAge  = 45 * 24 * time.Hour
	peerCacheMaxPerIP     = 2
	peerCacheMaxPerSubnet = 8 // per /24
	peerCacheSaveInterval = time.Minute
	// peerCacheSeedLimit bounds how much of the roster the cache may refill on
	// start; the rest is left to gossip and DHT so a stale cache cannot pin the
	// roster to yesterday's peers.
	peerCacheSeedLimit = 50
	// peerCacheScoreHalfLife lazily halves srcScore per idle period at save
	// time, so long-gone feeders drift down the ranking instead of pinning it.
	peerCacheScoreHalfLife = 24 * time.Hour
)

type peerCacheEntry struct {
	pub      ed25519.PublicKey
	addr     string
	quicAddr string
	lastSeen uint32
	srcScore uint16
}

func encodePeerCacheSnapshot(entries []peerCacheEntry) []byte {
	buf := make([]byte, 0, 1+len(entries)*64)
	buf = append(buf, peerCacheFormatVersion)
	buf = binary.AppendUvarint(buf, uint64(len(entries)))
	for _, entry := range entries {
		buf = append(buf, entry.pub...)
		buf = binary.AppendUvarint(buf, uint64(len(entry.addr)))
		buf = append(buf, entry.addr...)
		buf = binary.AppendUvarint(buf, uint64(len(entry.quicAddr)))
		buf = append(buf, entry.quicAddr...)
		buf = binary.BigEndian.AppendUint32(buf, entry.lastSeen)
		buf = binary.BigEndian.AppendUint16(buf, entry.srcScore)
	}
	return buf
}

func decodePeerCacheSnapshot(data []byte) ([]peerCacheEntry, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if data[0] != peerCacheFormatVersion {
		return nil, fmt.Errorf("unsupported peer cache version %d", data[0])
	}
	data = data[1:]

	count, n := binary.Uvarint(data)
	if n <= 0 || count > peerCacheMaxEntries*4 {
		return nil, errors.New("malformed peer cache header")
	}
	data = data[n:]

	readBytes := func(limit uint64) ([]byte, error) {
		size, n := binary.Uvarint(data)
		if n <= 0 || size > limit || uint64(len(data)-n) < size {
			return nil, errors.New("malformed peer cache field")
		}
		field := data[n : n+int(size)]
		data = data[n+int(size):]
		return field, nil
	}

	entries := make([]peerCacheEntry, 0, count)
	for i := uint64(0); i < count; i++ {
		if len(data) < ed25519.PublicKeySize {
			return nil, errors.New("malformed peer cache entry")
		}
		entry := peerCacheEntry{
			pub: append(ed25519.PublicKey(nil), data[:ed25519.PublicKeySize]...),
		}
		data = data[ed25519.PublicKeySize:]

		addr, err := readBytes(64)
		if err != nil {
			return nil, err
		}
		entry.addr = string(addr)
		quicAddr, err := readBytes(64)
		if err != nil {
			return nil, err
		}
		entry.quicAddr = string(quicAddr)

		if len(data) < 4+2 {
			return nil, errors.New("malformed peer cache entry tail")
		}
		entry.lastSeen = binary.BigEndian.Uint32(data)
		entry.srcScore = binary.BigEndian.Uint16(data[4:])
		data = data[6:]
		entries = append(entries, entry)
	}
	return entries, nil
}

func decayedPeerCacheScore(score uint32, idle time.Duration) uint16 {
	if idle > 0 {
		if halvings := int64(idle / peerCacheScoreHalfLife); halvings > 0 {
			if halvings >= 16 {
				return 0
			}
			score >>= uint(halvings)
		}
	}
	if score > 0xFFFF {
		return 0xFFFF
	}
	return uint16(score)
}

func peerCacheSubnet(addr string) (ip string, subnet string) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr, addr
	}
	parsed := net.ParseIP(host)
	if v4 := parsed.To4(); v4 != nil {
		return host, fmt.Sprintf("%d.%d.%d.0", v4[0], v4[1], v4[2])
	}
	return host, host
}

// selectPeerCacheEntries orders candidates (best sources first, then fresh),
// applies the diversity caps and fills the score slots before topping up with
// a shuffled remainder.
func selectPeerCacheEntries(candidates []peerCacheEntry) []peerCacheEntry {
	perIP := map[string]int{}
	perSubnet := map[string]int{}
	admit := func(entry peerCacheEntry) bool {
		ip, subnet := peerCacheSubnet(entry.addr)
		if perIP[ip] >= peerCacheMaxPerIP || perSubnet[subnet] >= peerCacheMaxPerSubnet {
			return false
		}
		perIP[ip]++
		perSubnet[subnet]++
		return true
	}

	scored := make([]peerCacheEntry, 0, len(candidates))
	rest := make([]peerCacheEntry, 0, len(candidates))
	for _, entry := range candidates {
		if entry.srcScore > 0 {
			scored = append(scored, entry)
		} else {
			rest = append(rest, entry)
		}
	}
	sort.Slice(scored, func(i, j int) bool {
		if scored[i].srcScore != scored[j].srcScore {
			return scored[i].srcScore > scored[j].srcScore
		}
		return scored[i].lastSeen > scored[j].lastSeen
	})

	selected := make([]peerCacheEntry, 0, peerCacheMaxEntries)
	for _, entry := range scored {
		if len(selected) >= peerCacheScoreSlots {
			rest = append(rest, entry)
			continue
		}
		if admit(entry) {
			selected = append(selected, entry)
		}
	}

	rand.Shuffle(len(rest), func(i, j int) {
		rest[i], rest[j] = rest[j], rest[i]
	})
	for _, entry := range rest {
		if len(selected) >= peerCacheMaxEntries {
			break
		}
		if admit(entry) {
			selected = append(selected, entry)
		}
	}
	return selected
}

func peerCacheEntryUsable(entry peerCacheEntry, now time.Time) bool {
	if len(entry.pub) != ed25519.PublicKeySize || entry.addr == "" {
		return false
	}
	age := now.Sub(time.Unix(int64(entry.lastSeen), 0))
	return age >= -time.Hour && age <= peerCacheMaxEntryAge
}

func (s *overlaySubscription) peerCacheEnabled() bool {
	return s.node.peerCache != nil && s.spec.persistsPeerCache()
}

// savePeerCacheSnapshot persists the current roster view merged with the
// still-usable remainder of the previous snapshot, so peers briefly offline
// across a save interval are not forgotten.
func (s *overlaySubscription) savePeerCacheSnapshot() {
	now := time.Now()
	lastSeen := uint32(now.Unix())

	s.mx.Lock()
	current := make([]peerCacheEntry, 0, len(s.peers))
	seen := make(map[PeerID]struct{}, len(s.peers))
	for id, peer := range s.peers {
		if !peer.outboundOK.Load() || len(peer.pub) != ed25519.PublicKeySize || peer.addr == "" {
			continue
		}
		if !peer.isAliveKnownOverlayPeer(now) || !peer.hasOpenConnection() {
			continue
		}
		entry := peerCacheEntry{
			pub:      append(ed25519.PublicKey(nil), peer.pub...),
			addr:     peer.addr,
			lastSeen: lastSeen,
			srcScore: decayedPeerCacheScore(peer.srcScore.Load(), 0),
		}
		if peer.route != nil {
			entry.quicAddr = peer.route.QUICAddress()
		}
		current = append(current, entry)
		seen[id] = struct{}{}
	}
	s.mx.Unlock()

	s.peerCacheMu.Lock()
	for id, entry := range s.peerCachePrev {
		if _, ok := seen[id]; ok {
			continue
		}
		carried := *entry
		carried.srcScore = decayedPeerCacheScore(uint32(carried.srcScore), now.Sub(time.Unix(int64(carried.lastSeen), 0)))
		if peerCacheEntryUsable(carried, now) {
			current = append(current, carried)
		}
	}
	selected := selectPeerCacheEntries(current)
	next := make(map[PeerID]*peerCacheEntry, len(selected))
	for i := range selected {
		id, err := peerIDFromPublicKey(selected[i].pub)
		if err != nil {
			continue
		}
		next[id] = &selected[i]
	}
	s.peerCachePrev = next
	s.peerCacheMu.Unlock()

	if err := s.node.peerCache.SaveOverlayPeerCache(s.spec.ShortID, encodePeerCacheSnapshot(selected)); err != nil {
		s.log.Debug().Err(err).Msg("failed to save overlay peer cache")
	}
}

// seedFromPeerCache re-attaches the persisted endpoints in usefulness order.
// Attaching is purely local — the transport registration performs no network
// I/O and the peer is only proven later, asynchronously, by the attach warmup
// (and never enters neighbours, plumtree or the next snapshot until it is
// alive) — so this needs no concurrency and no dial accounting: a stale entry
// costs one map insert and one warmup that times out.
func (s *overlaySubscription) seedFromPeerCache(ctx context.Context) {
	raw, err := s.node.peerCache.LoadOverlayPeerCache(s.spec.ShortID)
	if err != nil || len(raw) == 0 {
		return
	}
	entries, err := decodePeerCacheSnapshot(raw)
	if err != nil {
		s.log.Debug().Err(err).Msg("dropping malformed overlay peer cache")
		_ = s.node.peerCache.SaveOverlayPeerCache(s.spec.ShortID, nil)
		return
	}

	now := time.Now()
	usable := entries[:0]
	for _, entry := range entries {
		if peerCacheEntryUsable(entry, now) {
			usable = append(usable, entry)
		}
	}
	sort.Slice(usable, func(i, j int) bool {
		if usable[i].srcScore != usable[j].srcScore {
			return usable[i].srcScore > usable[j].srcScore
		}
		return usable[i].lastSeen > usable[j].lastSeen
	})

	s.peerCacheMu.Lock()
	if s.peerCachePrev == nil {
		s.peerCachePrev = make(map[PeerID]*peerCacheEntry, len(usable))
	}
	for i := range usable {
		id, err := peerIDFromPublicKey(usable[i].pub)
		if err != nil {
			continue
		}
		if _, ok := s.peerCachePrev[id]; !ok {
			entry := usable[i]
			s.peerCachePrev[id] = &entry
		}
	}
	s.peerCacheMu.Unlock()

	started := time.Now()
	attached := 0
	for _, entry := range usable {
		if attached >= peerCacheSeedLimit || ctx.Err() != nil || !s.isActive() {
			break
		}
		id, err := peerIDFromPublicKey(entry.pub)
		if err != nil || s.hasPeer(id) {
			continue
		}
		if s.attachCachedPeer(id, entry) {
			attached++
		}
	}

	if attached > 0 {
		s.log.Info().
			Int("candidates", len(usable)).
			Int("attached", attached).
			Dur("elapsed", time.Since(started)).
			Msg("seeded overlay peers from local cache")
	}
}

// attachCachedPeer puts a cached endpoint back into the roster. It goes through
// acquirePeerEndpoint like every other attach path, so identity is validated
// and the transport is released (never leaked) on any failure.
func (s *overlaySubscription) attachCachedPeer(id PeerID, entry peerCacheEntry) bool {
	pooled, release, err := s.node.acquirePeerEndpoint(id, peerEndpoint{
		adnlAddr: entry.addr,
		quicAddr: entry.quicAddr,
	}, entry.pub)
	if err != nil {
		return false
	}
	attached := s.attachPooledPeer(pooled, nil)
	release()
	return attached
}

// noteBroadcastSourcePeer credits the roster peer a first-accepted broadcast
// came from; the counter ranks peer cache entries by feeding usefulness. Runs
// per accepted broadcast (external messages included), so it must not allocate:
// the subscription is looked up in place rather than through a snapshot.
func (n *Node) noteBroadcastSourcePeer(overlayName string, src PeerID) {
	if src.IsZero() {
		return
	}
	sub := n.subscriptionByName(overlayName)
	if sub == nil || !sub.peerCacheEnabled() {
		return
	}
	if peer, ok := sub.rosterPeerIfActive(src); ok && peer != nil {
		peer.srcScore.Add(1)
	}
}
