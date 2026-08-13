package p2p

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/adnl"
	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

type peerQueryNotReadyError struct {
	err error
}

func (e peerQueryNotReadyError) Error() string {
	return e.err.Error()
}

func (e peerQueryNotReadyError) Unwrap() error {
	return e.err
}

func asPeerQueryNotReady(err error) error {
	return peerQueryNotReadyError{err: err}
}

func isPeerQueryNotReady(err error) bool {
	var notReady peerQueryNotReadyError
	return errors.As(err, &notReady) ||
		errors.Is(err, ErrBlockNotAvailable) ||
		errors.Is(err, ErrStateNotAvailable)
}

type peerQueryOperation struct {
	subscription *overlaySubscription
	peer         *overlayPeer
	finished     sync.Once
}

func (s *overlaySubscription) beginPeerQueryOperation(peer *overlayPeer) *peerQueryOperation {
	return &peerQueryOperation{
		subscription: s,
		peer:         peer,
	}
}

func (q *peerQueryOperation) finish(err error) {
	q.finished.Do(func() {
		q.subscription.finishPeerQueryOperation(q.peer, err)
	})
}

func (s *overlaySubscription) finishPeerQueryOperation(peer *overlayPeer, err error) {
	// Concurrent and hedged callers cancel work that lost to another peer.
	// That cancellation is not a peer outcome and must not affect readiness.
	if errors.Is(err, context.Canceled) {
		return
	}

	if s.spec.probeDrivenQueryReadiness() {
		if err == nil {
			return
		}
		if errors.Is(err, adnl.ErrPeerConnClosed) || !peer.hasOpenConnection() {
			s.removePeerIfCurrent(peer)
			return
		}

		peer.ignoreApplicationQueries(time.Now().Add(customQueryFailureCooldown))
		return
	}

	if err == nil || isPeerQueryNotReady(err) {
		// An operation spans a whole download - an archive package is up to 2 GiB
		// and a persistent state slice is streamed to disk - so its duration is a
		// throughput measurement, not a roundtrip. Feeding it to the RTT average
		// would invert downloadPeerScore, which divides by 1+roundtrip and would
		// then rank a fast peer that moved a lot of bytes below a slow one that
		// moved few. Record the success only; short probes and pings keep the
		// roundtrip average honest.
		peer.querySuccess(0)
		return
	}

	if s.spec.pingsPeerOnQueryTimeout() &&
		errors.Is(err, context.DeadlineExceeded) {
		s.startFastSyncPeerPing(peer)
	}
	s.handlePeerQueryFailure(peer, err)
}

const (
	liveNextProbeSoftTimeout      = 2500 * time.Millisecond
	liveNextProbeEarlyFailDelay   = 1200 * time.Millisecond
	liveNextProbeEarlyFailReserve = 4
	customQueryFailureCooldown    = 3 * time.Second
)

func (n *Node) downloadFromOverlay(ctx context.Context, block ton.BlockIDExt, req tl.Serializable) (*DownloadedBlock, error) {
	sub, err := n.querySubscriptionForBlock(block)
	if err != nil {
		return nil, err
	}

	if err = sub.ensurePeers(ctx); err != nil {
		return nil, fmt.Errorf("bootstrap overlay peers: %w", err)
	}

	return sub.downloadFull(ctx, block, req)
}

func (n *Node) probeBlockFromOverlay(ctx context.Context, block ton.BlockIDExt, opts ProbeBlockFullOptions) (*DownloadedBlock, error) {
	sub, err := n.querySubscriptionForBlock(block)
	if err != nil {
		return nil, err
	}

	if err = sub.ensurePeers(ctx); err != nil {
		return nil, fmt.Errorf("bootstrap overlay peers: %w", err)
	}

	return sub.probeBlockFull(ctx, block, opts)
}

func (s *overlaySubscription) downloadFull(ctx context.Context, block ton.BlockIDExt, req tl.Serializable) (*DownloadedBlock, error) {
	peers := s.chainBlockDownloadCandidates(block)
	if len(peers) == 0 {
		return nil, errors.New("overlay has no connected peers")
	}

	return s.downloadFullFromPeers(ctx, block, peers, req)
}

func (s *overlaySubscription) probeBlockFull(ctx context.Context, block ton.BlockIDExt, opts ProbeBlockFullOptions) (*DownloadedBlock, error) {
	peers := s.chainBlockDownloadCandidates(block)
	peers = preferDownloadPeer(peers, opts.PreferredPeerID)
	if len(peers) == 0 {
		return nil, errors.New("overlay has no connected peers")
	}

	peerLimit := opts.PeerLimit
	if peerLimit <= 0 {
		peerLimit = blockDownloadParallelism(peers)
	}
	var stagedPeerLimit int
	peers, peerLimit, stagedPeerLimit = clampStagedProbePeers(peers, peerLimit, opts.StagedPeerLimit, opts.StageDelay)

	req := tonnodeapi.DownloadBlockFull{Block: block}
	probeOpts := probeFullPeerOptions{
		peerLimit:       peerLimit,
		stagedPeerLimit: stagedPeerLimit,
		stageDelay:      opts.StageDelay,
	}
	downloaded, err := probeFullFromPeersWithOptions(ctx, peers, probeOpts, func(ctx context.Context, peer *overlayPeer) (DownloadedBlock, error) {
		return s.downloadBlockFullFromPeer(ctx, block, peer, req, opts.Speculative)
	}, func(peer *overlayPeer, err error) {
		s.noteChainBlockDownloadFailure(block, peer, err)
	})
	if err != nil {
		return nil, err
	}
	s.node.observeDownloadedBlockReceived(ctx, downloaded)
	return downloaded, nil
}

// downloadBlockFullFromPeer queries one peer for a full block, decodes and
// verifies it and records the download stats; shared by the parallel and the
// staged-probe block download paths.
func (s *overlaySubscription) downloadBlockFullFromPeer(ctx context.Context, requested ton.BlockIDExt, peer *overlayPeer, req tl.Serializable, speculative bool) (result DownloadedBlock, err error) {
	query := s.beginPeerQueryOperation(peer)
	defer func() {
		// Same rule as the live-tail next-block probe: when this node asks for a
		// block ahead of the peer's own apply, "not available" is the expected
		// answer and must not be charged to the peer. An overlay whose readiness
		// is probe-driven demotes on every non-nil outcome, and shard
		// descriptions arrive several times per master round, so counting these
		// kept a custom overlay's acceptors in the cooldown almost permanently.
		if speculative && errors.Is(err, ErrBlockNotAvailable) {
			query.finish(nil)
			return
		}
		query.finish(err)
	}()

	started := time.Now()
	resp, err := s.queryFromPeer(ctx, peer, req)
	if err != nil {
		return DownloadedBlock{}, err
	}

	downloaded, err := s.node.decodeDownloadedBlock(ctx, resp)
	if err != nil {
		return DownloadedBlock{}, err
	}
	if !downloaded.ID.Equals(&requested) {
		return DownloadedBlock{}, fmt.Errorf("peer returned %s for requested block %s", downloaded.BlockRef(), tnstore.FormatBlockRef(requested))
	}
	s.noteChainBlockDownloadSuccess(requested, peer, downloaded, time.Since(started))
	return *downloaded, nil
}

// clampStagedProbePeers raises the staged limit to at least the base limit and
// trims the candidate list to whichever limit the stage delay makes effective.
func clampStagedProbePeers(peers []*overlayPeer, peerLimit, stagedPeerLimit int, stageDelay time.Duration) ([]*overlayPeer, int, int) {
	if stagedPeerLimit < peerLimit {
		stagedPeerLimit = peerLimit
	}

	maxPeerLimit := peerLimit
	if stageDelay > 0 && stagedPeerLimit > peerLimit {
		maxPeerLimit = stagedPeerLimit
	}
	if maxPeerLimit > 0 && len(peers) > maxPeerLimit {
		peers = peers[:maxPeerLimit]
	}
	return peers, peerLimit, stagedPeerLimit
}

func (s *overlaySubscription) downloadNextFull(ctx context.Context, prev ton.BlockIDExt) (*DownloadedBlock, error) {
	peers := s.chainBlockDownloadCandidates(prev)
	if len(peers) == 0 {
		return nil, errors.New("overlay has no connected peers")
	}

	req := DownloadNextBlockFull{PrevBlock: prev}
	return s.downloadNextFullFromPeers(ctx, prev, peers, blockDownloadParallelism(peers), req)
}

func (s *overlaySubscription) probeNextFull(ctx context.Context, prev ton.BlockIDExt, opts ProbeNextBlockFullOptions) (*DownloadedBlock, error) {
	var peers []*overlayPeer
	if opts.LiveTail && isMasterchainBlock(prev) {
		peers = s.liveNextBlockDownloadCandidates(opts.PreferredPeerID)
	} else {
		peers = s.chainBlockDownloadCandidates(prev)
		peers = preferDownloadPeer(peers, opts.PreferredPeerID)
	}
	if len(peers) == 0 {
		return nil, errors.New("overlay has no connected peers")
	}

	peerLimit := opts.PeerLimit
	if peerLimit <= 0 {
		peerLimit = len(peers)
	}
	var stagedPeerLimit int
	peers, peerLimit, stagedPeerLimit = clampStagedProbePeers(peers, peerLimit, opts.StagedPeerLimit, opts.StageDelay)

	req := DownloadNextBlockFull{PrevBlock: prev}
	var liveNotAvailableMisses atomic.Int64
	probeOpts := probeFullPeerOptions{
		peerLimit:       peerLimit,
		stagedPeerLimit: stagedPeerLimit,
		stageDelay:      opts.StageDelay,
	}
	if opts.LiveTail && isMasterchainBlock(prev) {
		probeOpts.maxElapsed = liveNextProbeSoftTimeout
		probeOpts.earlyFailureDelay = liveNextProbeEarlyFailDelay
		probeOpts.earlyFailureCount = liveNextEarlyFailureCount(len(peers))
	}
	downloaded, err := probeFullFromPeersWithOptions(ctx, peers, probeOpts, func(ctx context.Context, peer *overlayPeer) (DownloadedBlock, error) {
		return s.downloadNextFullFromPeer(ctx, prev, peer, req, opts.LiveTail, liveNotAvailableMisses.Load())
	}, func(peer *overlayPeer, err error) {
		if opts.LiveTail {
			if errors.Is(err, ErrBlockNotAvailable) {
				liveNotAvailableMisses.Add(1)
			}
			s.noteLiveNextDownloadFailure(prev, peer, err)
			return
		}
		s.noteChainBlockDownloadFailure(prev, peer, err)
	})
	if err != nil {
		return nil, err
	}
	s.node.observeDownloadedBlockReceived(ctx, downloaded)
	return downloaded, nil
}

func preferDownloadPeer(peers []*overlayPeer, preferredPeerID PeerID) []*overlayPeer {
	if preferredPeerID.IsZero() || len(peers) < 2 {
		return peers
	}

	for idx, peer := range peers {
		if peer == nil || peer.id != preferredPeerID {
			continue
		}

		prioritized := make([]*overlayPeer, 0, len(peers))
		prioritized = append(prioritized, peer)
		prioritized = append(prioritized, peers[:idx]...)
		prioritized = append(prioritized, peers[idx+1:]...)
		return prioritized
	}
	return peers
}

func (s *overlaySubscription) nextBlockDescription(ctx context.Context, prev ton.BlockIDExt) (ton.BlockIDExt, error) {
	peers := s.chainBlockDownloadCandidates(prev)
	if len(peers) == 0 {
		return ton.BlockIDExt{}, errors.New("overlay has no connected peers")
	}

	req := GetNextBlockDescription{PrevBlock: prev}
	res, err := runFirstPeerRequest(ctx, peers, peerRequestOptions{
		parallelism: blockDownloadParallelism(peers),
		hedgeDelay:  downloadQueryHedgeDelay,
		onFailure: func(peer *overlayPeer, err error) {
			s.noteChainBlockDownloadFailure(prev, peer, err)
		},
	}, func(ctx context.Context, peer *overlayPeer) (ton.BlockIDExt, error) {
		query := s.beginPeerQueryOperation(peer)

		resp, err := s.queryFromPeerWithLimits(ctx, peer, req, downloadNextDescTimeout, maxKeyBlockLookupAnswerSize)
		if err != nil {
			query.finish(err)
			return ton.BlockIDExt{}, err
		}

		switch desc := resp.(type) {
		case BlockDescription:
			query.finish(nil)
			return desc.ID, nil
		case BlockDescriptionEmpty:
			query.finish(ErrBlockNotAvailable)
			return ton.BlockIDExt{}, ErrBlockNotAvailable
		default:
			err = fmt.Errorf("unexpected next block description response %T", resp)
			query.finish(err)
			return ton.BlockIDExt{}, err
		}
	})
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	return res.value, nil
}

func (s *overlaySubscription) downloadNextFullFromPeers(ctx context.Context, chain ton.BlockIDExt, peers []*overlayPeer, parallelism int, req DownloadNextBlockFull) (*DownloadedBlock, error) {
	res, err := runFirstPeerRequest(ctx, peers, peerRequestOptions{
		parallelism: parallelism,
		hedgeDelay:  downloadQueryHedgeDelay,
		onFailure: func(peer *overlayPeer, err error) {
			s.noteChainBlockDownloadFailure(chain, peer, err)
		},
	}, func(ctx context.Context, peer *overlayPeer) (DownloadedBlock, error) {
		return s.downloadNextFullFromPeer(ctx, chain, peer, req, false, 0)
	})
	if err != nil {
		return nil, err
	}
	block := res.value
	s.node.observeDownloadedBlockReceived(ctx, &block)
	return &block, nil
}

func (s *overlaySubscription) downloadNextFullFromPeer(ctx context.Context, chain ton.BlockIDExt, peer *overlayPeer, req DownloadNextBlockFull, liveTail bool, liveNotAvailableMisses int64) (result DownloadedBlock, err error) {
	query := s.beginPeerQueryOperation(peer)
	defer func() {
		// On the live tail this node probes ahead of block production, so
		// "the next block does not exist yet" is the expected answer, not a
		// fault by the peer. An overlay whose readiness is probe-driven charges
		// every non-nil outcome to the acceptor, so leaving this as an error put
		// all of a custom overlay's acceptors into the cooldown on nearly every
		// round and took the overlay out of query selection entirely. Only this
		// probe is exempt: elsewhere "not available" does mean the acceptor
		// could not serve what was asked for.
		if liveTail && errors.Is(err, ErrBlockNotAvailable) {
			query.finish(nil)
			return
		}
		query.finish(err)
	}()

	started := time.Now()
	resp, err := s.queryFromPeerWithLimits(ctx, peer, req, downloadNextQueryTimeout, maxBlockDownloadAnswerSize)
	if err != nil {
		return DownloadedBlock{}, err
	}

	block, err := s.node.decodeDownloadedBlock(ctx, resp)
	if err != nil {
		return DownloadedBlock{}, err
	}
	elapsed := time.Since(started)
	if liveTail && isMasterchainBlock(chain) {
		s.noteLiveNextDownloadSuccess(chain, peer, block, elapsed, liveNotAvailableMisses)
	} else {
		s.noteChainBlockDownloadSuccess(chain, peer, block, elapsed)
	}
	return *block, nil
}

type probeFullPeerOptions struct {
	peerLimit         int
	stagedPeerLimit   int
	stageDelay        time.Duration
	maxElapsed        time.Duration
	earlyFailureCount int
	earlyFailureDelay time.Duration
}

func probeFullFromPeersWithOptions(ctx context.Context, peers []*overlayPeer, opts probeFullPeerOptions, query func(context.Context, *overlayPeer) (DownloadedBlock, error), onFailure func(*overlayPeer, error)) (*DownloadedBlock, error) {
	if len(peers) == 0 {
		return nil, errors.New("overlay has no connected peers")
	}
	peerLimit := opts.peerLimit
	if peerLimit <= 0 || peerLimit > len(peers) {
		peerLimit = len(peers)
	}
	stagedPeerLimit := opts.stagedPeerLimit
	if stagedPeerLimit < peerLimit {
		stagedPeerLimit = peerLimit
	}

	results, errs := runPeerRequests(ctx, peers, peerRequestOptions{
		parallelism:          peerLimit,
		stageParallelism:     stagedPeerLimit,
		stageDelay:           opts.stageDelay,
		maxElapsed:           opts.maxElapsed,
		earlyFailureCount:    opts.earlyFailureCount,
		earlyFailureDelay:    opts.earlyFailureDelay,
		cancelOnFirstSuccess: true,
		onFailure:            onFailure,
	}, query)
	if len(results) > 0 {
		block := results[0].value
		return &block, nil
	}

	return nil, errors.Join(errs...)
}

func liveNextEarlyFailureCount(peerCount int) int {
	count := peerCount - liveNextProbeEarlyFailReserve
	if count < 4 {
		return 0
	}
	return count
}

func (s *overlaySubscription) blockDownloadCandidates() []*overlayPeer {
	peers := s.queryCandidates(0, 0)
	return s.node.prioritizeBlockDownloadPeers(peers)
}

func (s *overlaySubscription) chainBlockDownloadCandidates(block ton.BlockIDExt) []*overlayPeer {
	peers := s.blockDownloadCandidates()
	sticky := s.currentChainBlockPeer(block, peers)
	if sticky == nil {
		return peers
	}

	return moveDownloadPeerFirst(peers, sticky)
}

func (s *overlaySubscription) liveNextBlockDownloadCandidates(preferredPeerID PeerID) []*overlayPeer {
	peers := s.blockDownloadCandidates()
	if len(peers) == 0 {
		return peers
	}

	return s.prioritizeLiveNextPeers(peers, preferredPeerID, time.Now())
}

func moveDownloadPeerFirst(peers []*overlayPeer, first *overlayPeer) []*overlayPeer {
	if first == nil || len(peers) < 2 {
		return peers
	}

	firstID := first.id
	ordered := make([]*overlayPeer, 0, len(peers))
	ordered = append(ordered, first)
	for _, peer := range peers {
		if peer.id != firstID {
			ordered = append(ordered, peer)
		}
	}
	return ordered
}

type chainDownloadKey struct {
	workchain int32
	shard     int64
}

func chainDownloadKeyFromBlock(block ton.BlockIDExt) chainDownloadKey {
	return chainDownloadKey{
		workchain: block.Workchain,
		shard:     block.Shard,
	}
}

func (k chainDownloadKey) String() string {
	return fmt.Sprintf("wc=%d shard=%016x", k.workchain, uint64(k.shard))
}

func (s *overlaySubscription) currentChainBlockPeer(block ton.BlockIDExt, peers []*overlayPeer) *overlayPeer {
	if len(peers) == 0 {
		return nil
	}

	key := chainDownloadKeyFromBlock(block)
	now := time.Now()
	s.chainDownloadMx.Lock()
	defer s.chainDownloadMx.Unlock()

	state := s.chainDownloads[key]
	if state == nil || state.peer == nil {
		return nil
	}

	stickyID := state.peer.id
	if stickyID.IsZero() {
		s.clearChainBlockPeerLocked(key)
		return nil
	}
	for _, peer := range peers {
		if peer == nil || peer.id != stickyID {
			continue
		}

		stats := peer.statsSnapshot()
		if !stats.alive || stats.downloadSlowUntil.After(now) {
			s.clearChainBlockPeerLocked(key)
			return nil
		}

		state.peer = peer
		return peer
	}

	s.clearChainBlockPeerLocked(key)
	return nil
}

func (s *overlaySubscription) queryFromPeer(ctx context.Context, peer *overlayPeer, req tl.Serializable) (tl.Serializable, error) {
	return s.queryFromPeerWithLimits(ctx, peer, req, downloadQueryTimeout, maxBlockDownloadAnswerSize)
}

func (s *overlaySubscription) queryFromPeerWithLimits(ctx context.Context, peer *overlayPeer, req tl.Serializable, timeout time.Duration, maxAnswerSize uint64) (tl.Serializable, error) {
	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var resp tl.Serializable
	if err := peer.queryTransport.Query(queryCtx, maxAnswerSize, req, &resp); err != nil {
		return nil, err
	}

	return resp, nil
}

func (s *overlaySubscription) handlePeerQueryFailure(peer *overlayPeer, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}

	if errors.Is(err, adnl.ErrPeerConnClosed) || !peer.hasOpenConnection() {
		s.removePeerIfCurrent(peer)
		return
	}

	s.markPeerQueryFailed(peer)
}

func (s *overlaySubscription) queryRawFromPeerWithLimits(ctx context.Context, peer *overlayPeer, req tl.Serializable, timeout time.Duration, maxAnswerSize uint64) ([]byte, error) {
	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	answer, err := peer.queryTransport.QueryRaw(queryCtx, maxAnswerSize, req)
	if err != nil {
		return nil, err
	}
	return answer, nil
}

func (s *overlaySubscription) downloadFullFromPeers(ctx context.Context, requested ton.BlockIDExt, peers []*overlayPeer, req tl.Serializable) (*DownloadedBlock, error) {
	downloaded, err := runConcurrentBlockDownloads(ctx, peers, blockDownloadParallelism(peers), func(peer *overlayPeer, err error) {
		s.noteChainBlockDownloadFailure(requested, peer, err)
	}, func(ctx context.Context, peer *overlayPeer) (DownloadedBlock, error) {
		return s.downloadBlockFullFromPeer(ctx, requested, peer, req, false)
	})
	if err != nil {
		return nil, err
	}
	s.node.observeDownloadedBlockReceived(ctx, downloaded)
	return downloaded, nil
}

func blockDownloadParallelism(peers []*overlayPeer) int {
	if len(peers) == 0 {
		return 0
	}
	if shouldGiveFirstBlockPeerExclusiveTry(peers[0]) {
		return 1
	}
	return downloadQueryParallelism
}

func shouldGiveFirstBlockPeerExclusiveTry(peer *overlayPeer) bool {
	stats := peer.statsSnapshot()
	if !stats.alive || stats.unreliability > 0 {
		return false
	}
	if stats.downloadSlowUntil.After(time.Now()) {
		return false
	}
	return stats.roundtrip > 0 && stats.roundtrip <= downloadQueryHedgeDelay
}

func runConcurrentBlockDownloads(ctx context.Context, peers []*overlayPeer, parallelism int, onFailure func(*overlayPeer, error), query func(context.Context, *overlayPeer) (DownloadedBlock, error)) (*DownloadedBlock, error) {
	res, err := runFirstPeerRequest(ctx, peers, peerRequestOptions{
		parallelism: parallelism,
		hedgeDelay:  downloadQueryHedgeDelay,
		onFailure:   onFailure,
	}, query)
	if err != nil {
		return nil, err
	}
	block := res.value
	return &block, nil
}
