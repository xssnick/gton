package p2p

import (
	"context"
	"crypto/ed25519"
	"errors"
	"sync"
	"time"

	adnladdr "github.com/xssnick/tonutils-go/adnl/address"
)

const (
	peerAddressCacheTTL        = time.Minute
	peerAddressErrorCacheTTL   = 10 * time.Second
	peerAddressCacheMaxEntries = 4096
)

type peerAddressResult struct {
	list *adnladdr.List
	pub  ed25519.PublicKey
	err  error
}

type peerAddressCacheEntry struct {
	result    peerAddressResult
	cachedAt  time.Time
	expiresAt time.Time
}

type peerAddressLookup struct {
	done   chan struct{}
	result peerAddressResult
}

// peerAddressResolver keeps recent signed DHT address results and collapses
// concurrent lookups for the same peer. Address lists remain immutable after
// parsing, so callers can safely share the result.
type peerAddressResolver struct {
	mx       sync.Mutex
	cache    map[PeerID]peerAddressCacheEntry
	inFlight map[PeerID]*peerAddressLookup
}

func (n *Node) resolvePeerAddresses(
	ctx context.Context,
	id PeerID,
) (*adnladdr.List, ed25519.PublicKey, error) {
	return n.peerAddresses.resolve(ctx, n.dht, id, peerAddressCacheTTL)
}

func (n *Node) resolvePeerAddressesFresh(
	ctx context.Context,
	id PeerID,
	maxAge time.Duration,
) (*adnladdr.List, ed25519.PublicKey, error) {
	return n.peerAddresses.resolve(ctx, n.dht, id, maxAge)
}

func (r *peerAddressResolver) resolve(
	ctx context.Context,
	backend dhtBackend,
	id PeerID,
	maxAge time.Duration,
) (*adnladdr.List, ed25519.PublicKey, error) {
	now := time.Now()

	r.mx.Lock()
	if entry, ok := r.cachedLocked(id, now, maxAge); ok {
		r.mx.Unlock()
		return entry.result.list, entry.result.pub, entry.result.err
	}
	if lookup := r.inFlight[id]; lookup != nil {
		done := lookup.done
		r.mx.Unlock()

		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-done:
			return lookup.result.list, lookup.result.pub, lookup.result.err
		}
	}
	if r.inFlight == nil {
		r.inFlight = make(map[PeerID]*peerAddressLookup)
	}
	lookup := &peerAddressLookup{done: make(chan struct{})}
	r.inFlight[id] = lookup
	r.mx.Unlock()

	list, pub, err := findPeerAddresses(ctx, backend, id[:])
	result := peerAddressResult{
		list: list,
		pub:  pub,
		err:  err,
	}

	now = time.Now()
	r.mx.Lock()
	lookup.result = result
	delete(r.inFlight, id)
	if ttl := peerAddressResultTTL(ctx, result, now); ttl > 0 {
		r.storeLocked(id, peerAddressCacheEntry{
			result:    result,
			cachedAt:  now,
			expiresAt: now.Add(ttl),
		}, now)
	}
	close(lookup.done)
	r.mx.Unlock()

	return result.list, result.pub, result.err
}

func (r *peerAddressResolver) cachedLocked(
	id PeerID,
	now time.Time,
	maxAge time.Duration,
) (peerAddressCacheEntry, bool) {
	entry, ok := r.cache[id]
	if !ok {
		return peerAddressCacheEntry{}, false
	}
	if !now.Before(entry.expiresAt) {
		delete(r.cache, id)
		return peerAddressCacheEntry{}, false
	}
	if entry.result.err == nil && now.Sub(entry.cachedAt) >= maxAge {
		return peerAddressCacheEntry{}, false
	}
	return entry, true
}

func (r *peerAddressResolver) storeLocked(
	id PeerID,
	entry peerAddressCacheEntry,
	now time.Time,
) {
	if r.cache == nil {
		r.cache = make(map[PeerID]peerAddressCacheEntry)
	}
	if _, exists := r.cache[id]; !exists && len(r.cache) >= peerAddressCacheMaxEntries {
		r.evictLocked(now)
	}
	r.cache[id] = entry
}

func (r *peerAddressResolver) evictLocked(now time.Time) {
	for id, entry := range r.cache {
		if !now.Before(entry.expiresAt) {
			delete(r.cache, id)
		}
	}
	if len(r.cache) < peerAddressCacheMaxEntries {
		return
	}

	var (
		oldestID PeerID
		oldestAt time.Time
		found    bool
	)
	for id, entry := range r.cache {
		if !found || entry.cachedAt.Before(oldestAt) {
			oldestID = id
			oldestAt = entry.cachedAt
			found = true
		}
	}
	if found {
		delete(r.cache, oldestID)
	}
}

func peerAddressResultTTL(
	ctx context.Context,
	result peerAddressResult,
	now time.Time,
) time.Duration {
	if result.err != nil {
		if ctx.Err() != nil ||
			errors.Is(result.err, context.Canceled) ||
			errors.Is(result.err, context.DeadlineExceeded) {
			return 0
		}
		return peerAddressErrorCacheTTL
	}

	ttl := peerAddressCacheTTL
	if result.list.ExpireAt == 0 {
		return ttl
	}
	untilExpiry := time.Unix(int64(result.list.ExpireAt), 0).Sub(now)
	if untilExpiry <= 0 {
		return 0
	}
	if untilExpiry < ttl {
		return untilExpiry
	}
	return ttl
}
