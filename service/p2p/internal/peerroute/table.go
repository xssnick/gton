package peerroute

import (
	"cmp"
	"slices"
	"sync"
	"time"
)

type sweepCandidate[ID comparable] struct {
	id       ID
	isUseful bool
	lastUsed int64
}

// Table keeps one stable Route per peer ID for the lifetime of the table.
// The table is a leaf in the lock graph and does not call into its consumers.
type Table[ID comparable] struct {
	mu          sync.RWMutex
	routes      map[ID]*Route
	retryPolicy RetryPolicy
}

func NewTable[ID comparable](retryPolicy RetryPolicy) *Table[ID] {
	return &Table[ID]{
		routes:      make(map[ID]*Route),
		retryPolicy: retryPolicy,
	}
}

// Get returns the route of a peer, creating it on first use. The returned
// pointer remains stable while any transport retains it.
func (t *Table[ID]) Get(id ID) *Route {
	now := time.Now()

	t.mu.RLock()
	route := t.routes[id]
	t.mu.RUnlock()
	if route != nil {
		route.touch(now)
		return route
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if route = t.routes[id]; route != nil {
		route.touch(now)
		return route
	}

	route = NewRoute("", t.retryPolicy)
	route.touch(now)
	t.routes[id] = route

	return route
}

// Sweep bounds the table to maxRoutes while preserving every route held by a
// live transport. Address-less routes are discarded before learned routes;
// within each group, the least recently used routes are discarded first.
func (t *Table[ID]) Sweep(held map[ID]struct{}, maxRoutes int) int {
	t.mu.Lock()
	defer t.mu.Unlock()

	over := len(t.routes) - maxRoutes
	if over <= 0 {
		return 0
	}

	candidates := make([]sweepCandidate[ID], 0, len(t.routes))
	for id, route := range t.routes {
		if _, isHeld := held[id]; isHeld {
			continue
		}

		candidates = append(candidates, sweepCandidate[ID]{
			id:       id,
			isUseful: route.QUICAddress() != "",
			lastUsed: route.lastUsed(),
		})
	}

	slices.SortFunc(candidates, func(a, b sweepCandidate[ID]) int {
		if a.isUseful != b.isUseful {
			if a.isUseful {
				return 1
			}

			return -1
		}

		return cmp.Compare(a.lastUsed, b.lastUsed)
	})

	if over > len(candidates) {
		over = len(candidates)
	}
	for _, candidate := range candidates[:over] {
		delete(t.routes, candidate.id)
	}

	return over
}

func (t *Table[ID]) Size() int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return len(t.routes)
}
