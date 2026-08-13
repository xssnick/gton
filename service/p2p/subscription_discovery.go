package p2p

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/overlay"
)

func (s *overlaySubscription) startPeerDiscovery(ctx context.Context) {
	if s.spec.seedsFromFixedNodes() {
		s.startSeedFromFixedNodes(ctx)
		return
	}
	if s.peerCacheEnabled() {
		// Once per subscription: redial the persisted roster alongside the
		// DHT seed so restart warm-up does not wait on discovery.
		s.peerCacheSeedOnce.Do(func() {
			s.node.runAsync(func() {
				s.seedFromPeerCache(ctx)
			})
		})
	}
	s.startSeedFromDHT(ctx)
}

func (s *overlaySubscription) startSeedFromDHT(ctx context.Context) {
	s.startSeedFromDHTTarget(ctx, maxPeersPerOverlay)
}

func (s *overlaySubscription) startSeedFromDHTTarget(ctx context.Context, targetPeers int) {
	if !s.isActive() {
		return
	}
	if targetPeers < maxPeersPerOverlay {
		targetPeers = maxPeersPerOverlay
	}

	now := time.Now()

	s.seedMx.Lock()
	if targetPeers > s.seedTarget {
		s.seedTarget = targetPeers
	}
	if s.seedRunning {
		s.seedMx.Unlock()
		return
	}
	if !s.nextSeedAt.IsZero() && now.Before(s.nextSeedAt) {
		if s.seedScheduled {
			s.seedMx.Unlock()
			return
		}
		delay := time.Until(s.nextSeedAt)
		s.seedScheduled = true
		s.seedMx.Unlock()
		s.scheduleSeedFromDHT(ctx, delay)
		return
	}
	s.seedRunning = true
	targetPeers = s.seedTarget
	s.seedMx.Unlock()

	s.runSeedFromDHT(ctx, targetPeers)
}

func (s *overlaySubscription) runSeedFromDHT(ctx context.Context, targetPeers int) {
	run := func() {
		defer s.finishSeedFromDHT()
		s.seedFromDHT(ctx, targetPeers)
	}
	s.node.runAsync(run)
}

func (s *overlaySubscription) scheduleSeedFromDHT(ctx context.Context, delay time.Duration) {
	if delay < 0 {
		delay = 0
	}

	run := func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			s.clearScheduledSeedFromDHT()
			return
		case <-s.node.runCtx.Done():
			s.clearScheduledSeedFromDHT()
			return
		case <-timer.C:
		}

		s.seedMx.Lock()
		targetPeers := s.seedTarget
		s.seedScheduled = false
		s.seedMx.Unlock()

		s.startSeedFromDHTTarget(ctx, targetPeers)
	}
	s.node.runAsync(run)
}

func (s *overlaySubscription) clearScheduledSeedFromDHT() {
	s.seedMx.Lock()
	s.seedScheduled = false
	s.seedMx.Unlock()
}

func (s *overlaySubscription) finishSeedFromDHT() {
	s.seedMx.Lock()
	s.seedRunning = false
	s.seedTarget = 0
	s.nextSeedAt = time.Now().Add(nextDHTSeedCooldownDelay())
	s.seedMx.Unlock()
}

func (s *overlaySubscription) startSeedFromFixedNodes(ctx context.Context) {
	if !s.isActive() || s.node.dht == nil {
		return
	}

	s.seedMx.Lock()
	if s.seedRunning {
		s.seedMx.Unlock()
		return
	}
	s.seedRunning = true
	s.seedMx.Unlock()

	run := func() {
		defer s.finishFixedNodeSeed()
		s.seedFromFixedNodes(ctx)
	}
	s.node.runAsync(run)
}

func (s *overlaySubscription) finishFixedNodeSeed() {
	s.seedMx.Lock()
	s.seedRunning = false
	s.seedMx.Unlock()
}

func (s *overlaySubscription) currentSeedTarget(defaultTarget int) int {
	s.seedMx.Lock()
	defer s.seedMx.Unlock()

	if s.seedTarget > defaultTarget {
		return s.seedTarget
	}
	return defaultTarget
}

type seedConnectResult struct {
	attached bool
	err      error
}

// seedConnectPool fans overlay nodes found during a DHT seed search out to a
// bounded set of connect workers and counts successful attachments. It is
// shared by the subscription seed search and the archive peer pool discovery.
type seedConnectPool struct {
	ctx       context.Context
	jobs      chan overlay.Node
	results   chan seedConnectResult
	workers   sync.WaitGroup
	collector sync.WaitGroup
	connected atomic.Int64
	finished  bool
}

// runSeedConnectPool starts parallelism connect workers plus a collector that
// logs failed connects with connectErrMsg. Feed nodes with send and always
// call finish (idempotent, single-goroutine use) before reading connected.
func runSeedConnectPool(ctx context.Context, log zerolog.Logger, connectErrMsg string, parallelism int, connect func(overlay.Node) (bool, error)) *seedConnectPool {
	p := &seedConnectPool{
		ctx:     ctx,
		jobs:    make(chan overlay.Node),
		results: make(chan seedConnectResult, parallelism),
	}

	for i := 0; i < parallelism; i++ {
		p.workers.Add(1)
		go func() {
			defer p.workers.Done()
			for node := range p.jobs {
				attached, err := connect(node)
				p.results <- seedConnectResult{attached: attached, err: err}
			}
		}()
	}

	p.collector.Add(1)
	go func() {
		defer p.collector.Done()
		for res := range p.results {
			if res.err != nil {
				log.Debug().Err(res.err).Msg(connectErrMsg)
				continue
			}
			if res.attached {
				p.connected.Add(1)
			}
		}
	}()

	return p
}

// send hands a node to the workers; it reports false once ctx is done.
func (p *seedConnectPool) send(node overlay.Node) bool {
	select {
	case p.jobs <- node:
		return true
	case <-p.ctx.Done():
		return false
	}
}

// finish stops accepting nodes and waits for workers and collector to drain.
func (p *seedConnectPool) finish() {
	if p.finished {
		return
	}
	p.finished = true

	close(p.jobs)
	p.workers.Wait()
	close(p.results)
	p.collector.Wait()
}

func (s *overlaySubscription) seedFromDHT(ctx context.Context, targetPeers int) {
	if s.node.dht == nil {
		return
	}
	if !s.isActive() {
		return
	}

	var (
		cont       *dht.Continuation
		err        error
		requests   int
		nodesSeen  int
		startedAt  = time.Now()
		knownStart = s.knownPeerCount()
		aliveStart = s.aliveKnownPeerCount()
	)

	logSearch := aliveStart == 0
	refreshOnly := knownStart >= s.currentSeedTarget(targetPeers)
	if logSearch {
		s.log.Info().
			Int("known_peers", knownStart).
			Int("alive_peers", aliveStart).
			Msg("searching overlay peers in DHT")
	}

	seedPool := runSeedConnectPool(ctx, s.log, "failed to connect overlay node", dhtSeedConnectParallelism, func(node overlay.Node) (bool, error) {
		return s.connectDHTOverlayNode(ctx, node)
	})
	defer seedPool.finish()

	maxRequests := 8
	if refreshOnly {
		maxRequests = 1
	}

	replacements := 0
	for i := 0; i < maxRequests && (refreshOnly || s.knownPeerCount() < s.currentSeedTarget(targetPeers)); i++ {
		lookupCtx, cancel := context.WithTimeout(ctx, dhtFindTimeout)
		var nodes *overlay.NodesList
		if cont == nil {
			nodes, cont, err = s.node.dht.FindOverlayNodes(lookupCtx, s.spec.FullID)
		} else {
			nodes, cont, err = s.node.dht.FindOverlayNodes(lookupCtx, s.spec.FullID, cont)
		}
		cancel()
		if err != nil {
			if logSearch {
				s.log.Debug().
					Err(err).
					Dur("elapsed", time.Since(startedAt)).
					Int("dht_requests", requests+1).
					Msg("DHT overlay peer search failed")
				return
			}
			s.log.Debug().Err(err).Msg("DHT lookup failed")
			return
		}

		requests++
		nodesSeen += len(nodes.List)

		for _, node := range nodes.List {
			if !refreshOnly && s.knownPeerCount() >= s.currentSeedTarget(targetPeers) {
				break
			}
			if refreshOnly {
				send, replaced := s.prepareDHTRefreshNode(node, replacements)
				if !send {
					continue
				}
				if replaced {
					replacements++
				}
			}
			if !seedPool.send(node) {
				break
			}
		}

		if cont == nil || refreshOnly {
			break
		}
	}

	seedPool.finish()

	if logSearch || seedPool.connected.Load() > 0 {
		s.log.Debug().
			Dur("elapsed", time.Since(startedAt)).
			Int("dht_requests", requests).
			Int("dht_nodes", nodesSeen).
			Int64("connected_peers", seedPool.connected.Load()).
			Int("known_peers", len(s.knownPeersSnapshot())).
			Int("alive_peers", s.aliveKnownPeerCount()).
			Msg("overlay peer DHT search finished")
	}
}

func (s *overlaySubscription) prepareDHTRefreshNode(node overlay.Node, replacements int) (bool, bool) {
	identity, err := s.overlayNodeIdentity(node)
	if err != nil || identity.self {
		return false, false
	}
	if s.hasPeer(identity.peerID) {
		return true, false
	}
	if replacements >= dhtRefreshReplacementLimit {
		return false, false
	}
	if !s.hasPeerReplacementCandidate(identity.peerID) {
		return false, false
	}
	return true, true
}

func (s *overlaySubscription) seedFromFixedNodes(ctx context.Context) {
	if s.node.dht == nil || !s.isActive() {
		return
	}

	atPeerLimit := s.aliveKnownPeerCount() >= s.peerLimit()-1
	if atPeerLimit && !s.hasMissingFixedQUICRoute() {
		return
	}

	candidates := make([]PeerID, 0, len(s.spec.FixedNodes))
	for _, id := range s.spec.FixedNodes {
		if id == s.node.localID {
			continue
		}

		peer := s.peerByID(id)
		if peer != nil {
			if !s.spec.UseQUIC || peer.route.QUICAddress() != "" {
				continue
			}
		} else if atPeerLimit {
			continue
		}
		candidates = append(candidates, id)
	}
	if s.spec.shufflesSeedOrder() {
		rand.Shuffle(len(candidates), func(i, j int) {
			candidates[i], candidates[j] = candidates[j], candidates[i]
		})
	}

	jobs := make(chan PeerID)
	var (
		workers   sync.WaitGroup
		connected atomic.Int64
	)
	for range min(dhtSeedConnectParallelism, len(candidates)) {
		workers.Add(1)
		go func() {
			defer workers.Done()

			for id := range jobs {
				connectCtx, cancel := context.WithTimeout(
					ctx,
					dhtSeedPeerTimeout,
				)
				attached, err := s.connectFixedNode(connectCtx, id)
				cancel()
				if err != nil {
					s.log.Debug().
						Err(err).
						Str("peer_id", id.String()).
						Msg("failed to connect fixed overlay peer")
					continue
				}
				if attached {
					connected.Add(1)
				}
			}
		}()
	}
	for _, id := range candidates {
		select {
		case jobs <- id:
		case <-ctx.Done():
			close(jobs)
			workers.Wait()
			return
		}
	}
	close(jobs)
	workers.Wait()

	if connected.Load() > 0 {
		s.log.Debug().
			Int64("connected_peers", connected.Load()).
			Int("known_peers", len(s.knownPeersSnapshot())).
			Msg("fixed overlay peer search finished")
	}
}

func (s *overlaySubscription) hasMissingFixedQUICRoute() bool {
	if !s.spec.UseQUIC {
		return false
	}

	s.mx.Lock()
	defer s.mx.Unlock()

	for _, id := range s.spec.FixedNodes {
		peer := s.peers[id]
		if peer != nil && peer.route.QUICAddress() == "" {
			return true
		}
	}
	return false
}
