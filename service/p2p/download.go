package p2p

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/xssnick/gton/internal/logutil"
	"github.com/xssnick/gton/service/blockproof"
	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/pierrec/lz4/v4"
	"github.com/xssnick/tonutils-go/adnl"
	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	adnloverlay "github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

var ErrBlockNotAvailable = errors.New("peer does not have the requested block")
var ErrCompressedBlockV2Unsupported = errors.New("tonNode.dataFullCompressedV2 is not supported without state-aware BOC decompression")

const (
	liveNextUnavailablePenalty    = 3 * time.Second
	liveNextProbeSoftTimeout      = 2500 * time.Millisecond
	liveNextProbeEarlyFailDelay   = 1200 * time.Millisecond
	liveNextProbeEarlyFailReserve = 4
	shardDescriptionBroadcastKind = "tonNode.newShardBlockBroadcast"
)

type ProbeNextBlockFullOptions struct {
	PeerLimit        int
	StagedPeerLimit  int
	StageDelay       time.Duration
	PreferredPeerKey string
	LiveTail         bool
}

type ProbeBlockFullOptions struct {
	PeerLimit            int
	StagedPeerLimit      int
	StageDelay           time.Duration
	PreferredPeerKey     string
	BroadcastPreferDelay time.Duration
}

type CompressedBlockStateProvider interface {
	StateRootForCompressedBlock(ctx context.Context, block ton.BlockIDExt) (*cell.Cell, error)
}

type CompressedBlockStateProviderFunc func(context.Context, ton.BlockIDExt) (*cell.Cell, error)

func (f CompressedBlockStateProviderFunc) StateRootForCompressedBlock(ctx context.Context, block ton.BlockIDExt) (*cell.Cell, error) {
	return f(ctx, block)
}

func (n *Node) DownloadBlockFull(ctx context.Context, block ton.BlockIDExt) (*DownloadedBlock, error) {
	if cached, err := n.popShardBroadcastBlock(block); err == nil {
		return cached, nil
	} else if !errors.Is(err, tnstore.ErrNotFound) {
		n.log.Debug().
			Err(err).
			Str("block", formatBlockRef(block)).
			Msg("failed to load cached shard broadcast block")
	}
	if cached, err := n.cachedBlockFull(ctx, block); err == nil {
		return cached, nil
	} else if !errors.Is(err, tnstore.ErrNotFound) {
		n.log.Debug().
			Err(err).
			Str("block", formatBlockRef(block)).
			Msg("failed to load cached full block")
	}

	res, err := n.downloadBlockFullFromOverlayOrShardBroadcast(ctx, block)
	if err != nil {
		return nil, err
	}
	if !res.ID.Equals(&block) {
		return nil, fmt.Errorf("peer returned %s for requested block %s", res.BlockRef(), formatBlockRef(block))
	}
	n.rememberDownloadedBlockAsync(nil, res)
	return res, nil
}

func (n *Node) ProbeBlockFull(ctx context.Context, block ton.BlockIDExt, opts ProbeBlockFullOptions) (*DownloadedBlock, error) {
	if cached, err := n.popShardBroadcastBlock(block); err == nil {
		return cached, nil
	} else if !errors.Is(err, tnstore.ErrNotFound) {
		n.log.Debug().
			Err(err).
			Str("block", formatBlockRef(block)).
			Msg("failed to load cached shard broadcast block")
	}
	if cached, err := n.cachedBlockFull(ctx, block); err == nil {
		return cached, nil
	} else if !errors.Is(err, tnstore.ErrNotFound) {
		n.log.Debug().
			Err(err).
			Str("block", formatBlockRef(block)).
			Msg("failed to load cached full block")
	}

	res, err := n.probeBlockFullFromOverlayOrShardBroadcast(ctx, block, opts)
	if err != nil {
		return nil, err
	}
	if !res.ID.Equals(&block) {
		return nil, fmt.Errorf("peer returned %s for requested block %s", res.BlockRef(), formatBlockRef(block))
	}
	n.rememberDownloadedBlockAsync(nil, res)
	return res, nil
}

func (n *Node) downloadBlockFullFromOverlayOrShardBroadcast(ctx context.Context, block ton.BlockIDExt) (*DownloadedBlock, error) {
	wake, unwatch := n.watchShardBroadcastBlock(block)
	if wake == nil {
		return n.downloadFromOverlay(ctx, block, tonnodeapi.DownloadBlockFull{Block: block})
	}
	defer unwatch()

	if cached, err := n.popShardBroadcastBlock(block); err == nil {
		return cached, nil
	} else if !errors.Is(err, tnstore.ErrNotFound) {
		n.log.Debug().
			Err(err).
			Str("block", formatBlockRef(block)).
			Msg("failed to load cached shard broadcast block")
	}

	downloadCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type downloadResult struct {
		block *DownloadedBlock
		err   error
	}
	result := make(chan downloadResult, 1)
	go func() {
		res, err := n.downloadFromOverlay(downloadCtx, block, tonnodeapi.DownloadBlockFull{Block: block})
		result <- downloadResult{block: res, err: err}
	}()

	for {
		select {
		case res := <-result:
			return res.block, res.err
		case <-wake:
			if cached, err := n.popShardBroadcastBlock(block); err == nil {
				cancel()
				return cached, nil
			} else if !errors.Is(err, tnstore.ErrNotFound) {
				n.log.Debug().
					Err(err).
					Str("block", formatBlockRef(block)).
					Msg("failed to load cached shard broadcast block")
			}
			wake = nil
		case <-ctx.Done():
			cancel()
			return nil, ctx.Err()
		}
	}
}

func (n *Node) probeBlockFullFromOverlayOrShardBroadcast(ctx context.Context, block ton.BlockIDExt, opts ProbeBlockFullOptions) (*DownloadedBlock, error) {
	wake, unwatch := n.watchShardBroadcastBlock(block)
	if wake == nil {
		return n.probeBlockFromOverlay(ctx, block, opts)
	}
	defer unwatch()

	if cached, err := n.popShardBroadcastBlock(block); err == nil {
		return cached, nil
	} else if !errors.Is(err, tnstore.ErrNotFound) {
		n.log.Debug().
			Err(err).
			Str("block", formatBlockRef(block)).
			Msg("failed to load cached shard broadcast block")
	}

	downloadCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type downloadResult struct {
		block *DownloadedBlock
		err   error
	}
	result := make(chan downloadResult, 1)
	go func() {
		res, err := n.probeBlockFromOverlay(downloadCtx, block, opts)
		result <- downloadResult{block: res, err: err}
	}()

	for {
		select {
		case res := <-result:
			if opts.BroadcastPreferDelay > 0 {
				if cached, ok, err := n.waitPreferredShardBroadcastBlock(ctx, block, wake, opts.BroadcastPreferDelay); ok || err != nil {
					cancel()
					return cached, err
				}
			}
			return res.block, res.err
		case <-wake:
			if cached, err := n.popShardBroadcastBlock(block); err == nil {
				cancel()
				return cached, nil
			} else if !errors.Is(err, tnstore.ErrNotFound) {
				n.log.Debug().
					Err(err).
					Str("block", formatBlockRef(block)).
					Msg("failed to load cached shard broadcast block")
			}
			wake = nil
		case <-ctx.Done():
			cancel()
			return nil, ctx.Err()
		}
	}
}

func (n *Node) waitPreferredShardBroadcastBlock(ctx context.Context, block ton.BlockIDExt, wake <-chan struct{}, delay time.Duration) (*DownloadedBlock, bool, error) {
	if delay <= 0 || wake == nil {
		return nil, false, nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, false, ctx.Err()
	case <-wake:
		cached, err := n.popShardBroadcastBlock(block)
		if err == nil {
			return cached, true, nil
		}
		if !errors.Is(err, tnstore.ErrNotFound) {
			n.log.Debug().
				Err(err).
				Str("block", formatBlockRef(block)).
				Msg("failed to load cached shard broadcast block")
		}
		return nil, false, nil
	case <-timer.C:
		return nil, false, nil
	}
}

func (n *Node) PrefetchShardBlockFull(ctx context.Context, block ton.BlockIDExt) error {
	return n.prefetchShardBlockFull(ctx, block, "")
}

func (n *Node) PrefetchShardBlockFullFromBroadcastHint(ctx context.Context, block ton.BlockIDExt) error {
	return n.prefetchShardBlockFull(ctx, block, shardDescriptionBroadcastKind)
}

func (n *Node) prefetchShardBlockFull(ctx context.Context, block ton.BlockIDExt, cacheKind string) error {
	if isMasterchainBlock(block) {
		return fmt.Errorf("masterchain block %s is not a shard prefetch candidate", formatBlockRef(block))
	}
	if n.shardBroadcastCache != nil && n.shardBroadcastCache.HasBlock(block) {
		return nil
	}

	res, err := n.prefetchShardBlockFullFromOverlayOrBroadcast(ctx, block)
	if err != nil {
		return err
	}
	if res == nil {
		return nil
	}
	if !res.ID.Equals(&block) {
		return fmt.Errorf("peer returned %s for requested block %s", res.BlockRef(), formatBlockRef(block))
	}
	if cacheKind != "" {
		res.Kind = cacheKind
	}

	n.rememberShardBroadcastBlock(res)
	return nil
}

func (n *Node) prefetchShardBlockFullFromOverlayOrBroadcast(ctx context.Context, block ton.BlockIDExt) (*DownloadedBlock, error) {
	wake, unwatch := n.watchShardBroadcastBlock(block)
	if wake == nil {
		return n.downloadFromOverlay(ctx, block, tonnodeapi.DownloadBlockFull{Block: block})
	}
	defer unwatch()

	if n.shardBroadcastCache.HasBlock(block) {
		return nil, nil
	}

	downloadCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type downloadResult struct {
		block *DownloadedBlock
		err   error
	}
	result := make(chan downloadResult, 1)
	go func() {
		res, err := n.downloadFromOverlay(downloadCtx, block, tonnodeapi.DownloadBlockFull{Block: block})
		result <- downloadResult{block: res, err: err}
	}()

	for {
		select {
		case res := <-result:
			return res.block, res.err
		case <-wake:
			if n.shardBroadcastCache.HasBlock(block) {
				cancel()
				return nil, nil
			}
			wake = nil
		case <-ctx.Done():
			cancel()
			return nil, ctx.Err()
		}
	}
}

func (n *Node) DownloadNextBlockFull(ctx context.Context, prev ton.BlockIDExt) (*DownloadedBlock, error) {
	if cached, err := n.cachedNextBlockFull(ctx, prev); err == nil {
		return cached, nil
	} else if !errors.Is(err, tnstore.ErrNotFound) {
		n.log.Debug().
			Err(err).
			Str("prev", formatBlockRef(prev)).
			Msg("failed to load cached next full block")
	}

	res, err := n.downloadNextFromOverlay(ctx, prev)
	if err != nil {
		return nil, err
	}
	n.rememberDownloadedBlockAsync(&prev, res)
	return res, nil
}

func (n *Node) ProbeNextBlockFull(ctx context.Context, prev ton.BlockIDExt, opts ProbeNextBlockFullOptions) (*DownloadedBlock, error) {
	if cached, err := n.cachedNextBlockFull(ctx, prev); err == nil {
		return cached, nil
	} else if !errors.Is(err, tnstore.ErrNotFound) {
		n.log.Debug().
			Err(err).
			Str("prev", formatBlockRef(prev)).
			Msg("failed to load cached next full block")
	}

	sub, err := n.subscriptionForBlock(prev)
	if err != nil {
		return nil, err
	}
	if err = sub.ensurePeers(ctx); err != nil {
		return nil, fmt.Errorf("bootstrap overlay peers: %w", err)
	}

	res, err := sub.probeNextFull(ctx, prev, opts)
	if err != nil {
		return nil, err
	}
	n.rememberDownloadedBlockAsync(&prev, res)
	return res, nil
}

func (n *Node) NextBlockDescription(ctx context.Context, prev ton.BlockIDExt) (ton.BlockIDExt, error) {
	full, err := n.peerStorage.NextBlockFull(ctx, prev)
	if err == nil && full != nil {
		return full.ID, nil
	}
	if err != nil && !errors.Is(err, tnstore.ErrNotFound) {
		n.log.Debug().
			Err(err).
			Str("prev", formatBlockRef(prev)).
			Msg("failed to load cached next block description")
	}

	sub, err := n.subscriptionForBlock(prev)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if err = sub.ensurePeers(ctx); err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("bootstrap overlay peers: %w", err)
	}
	return sub.nextBlockDescription(ctx, prev)
}

func (n *Node) downloadNextFromOverlay(ctx context.Context, prev ton.BlockIDExt) (*DownloadedBlock, error) {
	sub, err := n.subscriptionForBlock(prev)
	if err != nil {
		return nil, err
	}

	if err = sub.ensurePeers(ctx); err != nil {
		return nil, fmt.Errorf("bootstrap overlay peers: %w", err)
	}

	return sub.downloadNextFull(ctx, prev)
}

func (n *Node) cachedBlockFull(ctx context.Context, block ton.BlockIDExt) (*DownloadedBlock, error) {
	full, err := n.peerStorage.BlockFull(ctx, block)
	if err != nil {
		return nil, err
	}
	return downloadedBlockFromStored("local full block cache", full)
}

func (n *Node) cachedNextBlockFull(ctx context.Context, prev ton.BlockIDExt) (*DownloadedBlock, error) {
	full, err := n.peerStorage.NextBlockFull(ctx, prev)
	if err != nil {
		return nil, err
	}
	return downloadedBlockFromStored("local next block cache", full)
}

func downloadedBlockFromStored(kind string, full *tnstore.ServedBlockFull) (*DownloadedBlock, error) {
	if full == nil {
		return nil, tnstore.ErrNotFound
	}
	return normalizeDownloadedBlock(kind, full.ID, full.Proof, full.Block, full.IsLink, true, nil)
}

func (n *Node) downloadFromOverlay(ctx context.Context, block ton.BlockIDExt, req tl.Serializable) (*DownloadedBlock, error) {
	sub, err := n.subscriptionForBlock(block)
	if err != nil {
		return nil, err
	}

	if err = sub.ensurePeers(ctx); err != nil {
		return nil, fmt.Errorf("bootstrap overlay peers: %w", err)
	}

	return sub.downloadFull(ctx, block, req)
}

func (n *Node) probeBlockFromOverlay(ctx context.Context, block ton.BlockIDExt, opts ProbeBlockFullOptions) (*DownloadedBlock, error) {
	sub, err := n.subscriptionForBlock(block)
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
	peers = preferDownloadPeer(peers, opts.PreferredPeerKey)
	if len(peers) == 0 {
		return nil, errors.New("overlay has no connected peers")
	}

	peerLimit := opts.PeerLimit
	if peerLimit <= 0 {
		peerLimit = blockDownloadParallelism(peers)
	}
	stagedPeerLimit := opts.StagedPeerLimit
	if stagedPeerLimit < peerLimit {
		stagedPeerLimit = peerLimit
	}

	maxPeerLimit := peerLimit
	if opts.StageDelay > 0 && stagedPeerLimit > peerLimit {
		maxPeerLimit = stagedPeerLimit
	}
	if maxPeerLimit > 0 && len(peers) > maxPeerLimit {
		peers = peers[:maxPeerLimit]
	}

	req := tonnodeapi.DownloadBlockFull{Block: block}
	probeOpts := probeFullPeerOptions{
		peerLimit:       peerLimit,
		stagedPeerLimit: stagedPeerLimit,
		stageDelay:      opts.StageDelay,
	}
	return probeFullFromPeersWithOptions(ctx, peers, probeOpts, func(ctx context.Context, peer *overlayPeer) (DownloadedBlock, error) {
		started := time.Now()
		resp, err := s.queryFromPeer(ctx, peer, req)
		if err != nil {
			return DownloadedBlock{}, err
		}

		downloaded, err := s.node.decodeDownloadedBlock(ctx, resp)
		if err != nil {
			return DownloadedBlock{}, err
		}
		if !downloaded.ID.Equals(&block) {
			return DownloadedBlock{}, fmt.Errorf("peer returned %s for requested block %s", downloaded.BlockRef(), formatBlockRef(block))
		}
		s.noteChainBlockDownloadSuccess(block, peer, downloaded, time.Since(started))
		return *downloaded, nil
	}, func(peer *overlayPeer, err error) {
		s.noteChainBlockDownloadFailure(block, peer, err)
	})
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
		peers = s.liveNextBlockDownloadCandidates(prev, opts.PreferredPeerKey)
	} else {
		peers = s.chainBlockDownloadCandidates(prev)
		peers = preferDownloadPeer(peers, opts.PreferredPeerKey)
	}
	if len(peers) == 0 {
		return nil, errors.New("overlay has no connected peers")
	}

	peerLimit := opts.PeerLimit
	if peerLimit <= 0 {
		peerLimit = len(peers)
	}
	stagedPeerLimit := opts.StagedPeerLimit
	if stagedPeerLimit < peerLimit {
		stagedPeerLimit = peerLimit
	}

	maxPeerLimit := peerLimit
	if opts.StageDelay > 0 && stagedPeerLimit > peerLimit {
		maxPeerLimit = stagedPeerLimit
	}
	if maxPeerLimit > 0 && len(peers) > maxPeerLimit {
		peers = peers[:maxPeerLimit]
	}

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
	return probeFullFromPeersWithOptions(ctx, peers, probeOpts, func(ctx context.Context, peer *overlayPeer) (DownloadedBlock, error) {
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
}

func preferDownloadPeer(peers []*overlayPeer, preferredKey string) []*overlayPeer {
	if preferredKey == "" || len(peers) < 2 {
		return peers
	}

	for idx, peer := range peers {
		if downloadPeerKey(peer) != preferredKey {
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
		resp, err := s.queryFromPeerWithLimits(ctx, peer, req, downloadNextDescTimeout, maxKeyBlockLookupAnswerSize)
		if err != nil {
			return ton.BlockIDExt{}, err
		}

		switch desc := resp.(type) {
		case BlockDescription:
			return desc.ID, nil
		case BlockDescriptionEmpty:
			return ton.BlockIDExt{}, ErrBlockNotAvailable
		default:
			return ton.BlockIDExt{}, fmt.Errorf("unexpected next block description response %T", resp)
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
	return &block, nil
}

func (s *overlaySubscription) downloadNextFullFromPeer(ctx context.Context, chain ton.BlockIDExt, peer *overlayPeer, req DownloadNextBlockFull, liveTail bool, liveNotAvailableMisses int64) (DownloadedBlock, error) {
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

func probeNextFullFromPeers(ctx context.Context, peers []*overlayPeer, query func(context.Context, *overlayPeer) (DownloadedBlock, error), onFailure func(*overlayPeer, error)) (*DownloadedBlock, error) {
	return probeFullFromPeersWithOptions(ctx, peers, probeFullPeerOptions{
		peerLimit:       len(peers),
		stagedPeerLimit: len(peers),
	}, query, onFailure)
}

func probeNextFullFromPeersStaged(ctx context.Context, peers []*overlayPeer, peerLimit int, stagedPeerLimit int, stageDelay time.Duration, query func(context.Context, *overlayPeer) (DownloadedBlock, error), onFailure func(*overlayPeer, error)) (*DownloadedBlock, error) {
	return probeFullFromPeersWithOptions(ctx, peers, probeFullPeerOptions{
		peerLimit:       peerLimit,
		stagedPeerLimit: stagedPeerLimit,
		stageDelay:      stageDelay,
	}, query, onFailure)
}

type probeFullPeerOptions struct {
	peerLimit         int
	stagedPeerLimit   int
	stageDelay        time.Duration
	maxElapsed        time.Duration
	earlyFailureCount int
	earlyFailureDelay time.Duration
}

func probeNextFullFromPeersWithOptions(ctx context.Context, peers []*overlayPeer, opts probeFullPeerOptions, query func(context.Context, *overlayPeer) (DownloadedBlock, error), onFailure func(*overlayPeer, error)) (*DownloadedBlock, error) {
	return probeFullFromPeersWithOptions(ctx, peers, opts, query, onFailure)
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
	if s.node == nil {
		return peers
	}
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

func (s *overlaySubscription) liveNextBlockDownloadCandidates(block ton.BlockIDExt, preferredPeerKey string) []*overlayPeer {
	peers := s.blockDownloadCandidates()
	if len(peers) == 0 {
		return peers
	}

	now := time.Now()
	peers = s.prioritizeLiveNextPeers(peers, preferredPeerKey, now)
	if isMasterchainBlock(block) {
		return peers
	}

	sticky := s.currentLiveNextChainBlockPeer(block, peers, now)
	if sticky == nil {
		return peers
	}
	if preferredPeerKey != "" && downloadPeerKey(sticky) != preferredPeerKey && downloadPeerKey(peers[0]) == preferredPeerKey {
		return peers
	}
	return moveDownloadPeerFirst(peers, sticky)
}

func moveDownloadPeerFirst(peers []*overlayPeer, first *overlayPeer) []*overlayPeer {
	if first == nil || len(peers) < 2 {
		return peers
	}

	firstKey := downloadPeerKey(first)
	ordered := make([]*overlayPeer, 0, len(peers))
	ordered = append(ordered, first)
	for _, peer := range peers {
		if downloadPeerKey(peer) != firstKey {
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
	return s.currentChainBlockPeerFiltered(block, peers, nil)
}

func (s *overlaySubscription) currentLiveNextChainBlockPeer(block ton.BlockIDExt, peers []*overlayPeer, now time.Time) *overlayPeer {
	if !isMasterchainBlock(block) {
		return s.currentChainBlockPeer(block, peers)
	}
	return s.currentChainBlockPeerFiltered(block, peers, func(peerKey string) bool {
		state := s.liveNextPeers[peerKey]
		return state == nil || !state.unavailableUntil.After(now)
	})
}

func (s *overlaySubscription) currentChainBlockPeerFiltered(block ton.BlockIDExt, peers []*overlayPeer, allow func(string) bool) *overlayPeer {
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

	stickyKey := downloadPeerKey(state.peer)
	if allow != nil && !allow(stickyKey) {
		s.clearChainBlockPeerLocked(key)
		return nil
	}
	for _, peer := range peers {
		if downloadPeerKey(peer) != stickyKey {
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

func (s *overlaySubscription) query(ctx context.Context, req tl.Serializable) (tl.Serializable, error) {
	peers := s.queryCandidates(0, 0)
	if len(peers) == 0 {
		return nil, errors.New("overlay has no connected peers")
	}

	var errs []error
	for _, peer := range peers {
		resp, err := s.queryFromPeer(ctx, peer, req)
		if err == nil {
			return resp, nil
		}
		errs = append(errs, fmt.Errorf("%s: %w", peer.addr, err))
	}

	return nil, errors.Join(errs...)
}

func (s *overlaySubscription) queryFromPeer(ctx context.Context, peer *overlayPeer, req tl.Serializable) (tl.Serializable, error) {
	return s.queryFromPeerWithLimits(ctx, peer, req, downloadQueryTimeout, maxBlockDownloadAnswerSize)
}

func (s *overlaySubscription) queryFromPeerWithLimits(ctx context.Context, peer *overlayPeer, req tl.Serializable, timeout time.Duration, maxAnswerSize uint64) (tl.Serializable, error) {
	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var resp tl.Serializable
	startedAt := time.Now()
	if err := peer.rldpOverlay.DoQuery(queryCtx, maxAnswerSize, req, &resp); err != nil {
		s.handlePeerQueryFailure(peer, err)
		return nil, err
	}
	peer.querySuccess(time.Since(startedAt))

	return resp, nil
}

func (s *overlaySubscription) handlePeerQueryFailure(peer *overlayPeer, err error) {
	if peer == nil {
		return
	}
	if errors.Is(err, context.Canceled) {
		return
	}

	if errors.Is(err, adnl.ErrPeerConnClosed) || !peer.hasOpenConnection() {
		s.removePeer(peer.id)
		return
	}

	s.markPeerQueryFailed(peer)
}

func (s *overlaySubscription) queryRawFromPeerWithLimits(ctx context.Context, peer *overlayPeer, req tl.Serializable, timeout time.Duration, maxAnswerSize uint64) ([]byte, error) {
	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	queryID := make([]byte, 32)
	if _, err := rand.Read(queryID); err != nil {
		return nil, err
	}

	result := make(chan rldp.AsyncQueryResult, 1)
	startedAt := time.Now()
	if err := peer.rldpOverlay.DoQueryAsync(queryCtx, maxAnswerSize, queryID, adnloverlay.WrapQuery(s.spec.ShortID, req), result); err != nil {
		s.handlePeerQueryFailure(peer, err)
		return nil, err
	}

	select {
	case resp := <-result:
		peer.querySuccess(time.Since(startedAt))
		return resp.ResultBytes, nil
	case <-queryCtx.Done():
		s.handlePeerQueryFailure(peer, queryCtx.Err())
		return nil, fmt.Errorf("response deadline exceeded, err: %w", queryCtx.Err())
	}
}

func (s *overlaySubscription) downloadFullFromPeers(ctx context.Context, requested ton.BlockIDExt, peers []*overlayPeer, req tl.Serializable) (*DownloadedBlock, error) {
	return runConcurrentBlockDownloads(ctx, peers, blockDownloadParallelism(peers), func(peer *overlayPeer, err error) {
		s.noteChainBlockDownloadFailure(requested, peer, err)
	}, func(ctx context.Context, peer *overlayPeer) (DownloadedBlock, error) {
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
			return DownloadedBlock{}, fmt.Errorf("peer returned %s for requested block %s", downloaded.BlockRef(), formatBlockRef(requested))
		}
		s.noteChainBlockDownloadSuccess(requested, peer, downloaded, time.Since(started))
		return *downloaded, nil
	})
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
	if peer == nil {
		return false
	}

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

func noteBlockDownloadSuccess(peer *overlayPeer, block *DownloadedBlock, elapsed time.Duration) {
	bytes := downloadedBlockPayloadBytes(block)
	if bytes <= 0 {
		bytes = 1
	}
	slowThreshold := blockSlowPeerSpeed
	slowPenalty := blockSlowPeerPenalty
	if bytes < blockSpeedSampleMin {
		slowThreshold = 0
		slowPenalty = 0
	}
	peer.downloadSuccess(bytes, elapsed, slowThreshold, slowPenalty)
}

func (s *overlaySubscription) noteChainBlockDownloadSuccess(chain ton.BlockIDExt, peer *overlayPeer, block *DownloadedBlock, elapsed time.Duration) {
	noteBlockDownloadSuccess(peer, block, elapsed)

	bytes := downloadedBlockPayloadBytes(block)
	if peer == nil || bytes <= 0 || elapsed <= 0 {
		return
	}
	speed := float64(bytes) / elapsed.Seconds()
	winnerKey := downloadPeerKey(peer)
	confirmedAt := time.Now()

	key := chainDownloadKeyFromBlock(chain)
	var penalized []chainUnavailablePeer
	s.chainDownloadMx.Lock()

	if s.chainDownloads == nil {
		s.chainDownloads = map[chainDownloadKey]*chainDownloadState{}
	}
	state := s.chainDownloads[key]
	if state == nil {
		state = &chainDownloadState{}
		s.chainDownloads[key] = state
	}

	if state.peer == nil || downloadPeerKey(state.peer) != downloadPeerKey(peer) {
		s.log.Debug().
			Str("chain", key.String()).
			Str("peer", peer.addr).
			Str("speed", logutil.FormatByteRate(bytes, elapsed)).
			Msg("selected chain block download peer")
	}
	state.peer = peer
	if state.speed == 0 {
		state.speed = speed
	} else {
		state.speed = state.speed*0.7 + speed*0.3
	}
	for missedKey, missed := range state.unavailable {
		if missedKey == winnerKey || missed.peer == nil || confirmedAt.Sub(missed.at) > blockUnavailableConfirmWindow {
			delete(state.unavailable, missedKey)
			continue
		}
		penalized = append(penalized, missed)
		delete(state.unavailable, missedKey)
	}
	s.chainDownloadMx.Unlock()

	for _, missed := range penalized {
		missed.peer.downloadFailed(blockUnavailablePeerPenalty)
		s.log.Debug().
			Str("chain", key.String()).
			Str("peer", missed.peer.addr).
			Str("winner", peer.addr).
			Dur("penalty", blockUnavailablePeerPenalty).
			Msg("temporarily deprioritized peer after confirmed unavailable response")
	}
}

func (s *overlaySubscription) noteLiveNextDownloadSuccess(chain ton.BlockIDExt, peer *overlayPeer, block *DownloadedBlock, elapsed time.Duration, liveNotAvailableMisses int64) {
	s.noteChainBlockDownloadSuccess(chain, peer, block, elapsed)
	if peer == nil || !isMasterchainBlock(chain) || elapsed <= 0 {
		return
	}

	key := downloadPeerKey(peer)
	if key == "" {
		return
	}

	s.chainDownloadMx.Lock()
	defer s.chainDownloadMx.Unlock()

	if s.liveNextPeers == nil {
		s.liveNextPeers = map[string]*liveNextPeerState{}
	}
	state := s.liveNextPeers[key]
	if state == nil {
		state = &liveNextPeerState{}
		s.liveNextPeers[key] = state
	}

	state.successes++
	state.unavailableUntil = time.Time{}
	bytes := downloadedBlockPayloadBytes(block)
	speed := liveNextDownloadBytesPerSecond(bytes, elapsed)
	if speed > 0 {
		if state.bytesSec == 0 {
			state.bytesSec = speed
		} else {
			state.bytesSec = state.bytesSec*0.7 + speed*0.3
		}
	}

	availability := liveNextAvailabilitySample(bytes, liveNotAvailableMisses)
	if state.availability == 0 {
		state.availability = availability
	} else {
		state.availability = state.availability*0.7 + availability*0.3
	}
	if state.availability < 0.01 {
		state.availability = 0
	}

	if state.latency == 0 {
		state.latency = elapsed
		return
	}
	state.latency = time.Duration(float64(state.latency)*0.7 + float64(elapsed)*0.3)
}

func liveNextDownloadBytesPerSecond(bytes int64, elapsed time.Duration) float64 {
	if bytes <= 0 || elapsed <= 0 {
		return 0
	}
	return float64(bytes) / elapsed.Seconds()
}

func liveNextAvailabilitySample(bytes int64, misses int64) float64 {
	if misses <= 0 {
		return 0
	}
	sizeWeight := 1.0
	if bytes > 0 {
		sizeWeight += float64(bytes) / float64(1<<20)
		if sizeWeight > 5 {
			sizeWeight = 5
		}
	}
	return float64(misses) * sizeWeight
}

func (s *overlaySubscription) noteChainBlockDownloadFailure(chain ton.BlockIDExt, peer *overlayPeer, err error) {
	if peer == nil {
		return
	}
	if errors.Is(err, ErrBlockNotAvailable) {
		s.noteChainBlockUnavailable(chain, peer)
		return
	}
	peer.downloadFailed(blockSlowPeerPenalty)

	key := chainDownloadKeyFromBlock(chain)
	s.chainDownloadMx.Lock()
	defer s.chainDownloadMx.Unlock()

	state := s.chainDownloads[key]
	if state != nil && state.peer != nil && downloadPeerKey(state.peer) == downloadPeerKey(peer) {
		s.log.Debug().
			Err(err).
			Str("chain", key.String()).
			Str("peer", peer.addr).
			Msg("cleared chain block download peer after failure")
		s.clearChainBlockPeerLocked(key)
	}
}

func (s *overlaySubscription) noteChainBlockUnavailable(chain ton.BlockIDExt, peer *overlayPeer) {
	key := chainDownloadKeyFromBlock(chain)
	peerKey := downloadPeerKey(peer)
	if peerKey == "" {
		return
	}

	s.chainDownloadMx.Lock()
	defer s.chainDownloadMx.Unlock()

	state := s.chainDownloads[key]
	if state == nil {
		state = &chainDownloadState{}
		if s.chainDownloads == nil {
			s.chainDownloads = map[chainDownloadKey]*chainDownloadState{}
		}
		s.chainDownloads[key] = state
	}
	if state.unavailable == nil {
		state.unavailable = map[string]chainUnavailablePeer{}
	}
	state.unavailable[peerKey] = chainUnavailablePeer{
		peer: peer,
		at:   time.Now(),
	}
}

func (s *overlaySubscription) noteLiveNextDownloadFailure(chain ton.BlockIDExt, peer *overlayPeer, err error) {
	if peer == nil || !isMasterchainBlock(chain) {
		s.noteChainBlockDownloadFailure(chain, peer, err)
		return
	}
	if !errors.Is(err, ErrBlockNotAvailable) {
		s.noteChainBlockDownloadFailure(chain, peer, err)
		return
	}

	peerKey := downloadPeerKey(peer)
	if peerKey == "" {
		return
	}

	chainKey := chainDownloadKeyFromBlock(chain)
	unavailableUntil := time.Now().Add(liveNextUnavailablePenalty)

	s.chainDownloadMx.Lock()
	if s.liveNextPeers == nil {
		s.liveNextPeers = map[string]*liveNextPeerState{}
	}
	state := s.liveNextPeers[peerKey]
	if state == nil {
		state = &liveNextPeerState{}
		s.liveNextPeers[peerKey] = state
	}
	state.unavailableUntil = unavailableUntil

	sticky := s.chainDownloads[chainKey]
	wasSticky := sticky != nil && sticky.peer != nil && downloadPeerKey(sticky.peer) == peerKey
	if wasSticky {
		s.clearChainBlockPeerLocked(chainKey)
	}
	s.chainDownloadMx.Unlock()

	event := s.log.Debug().
		Str("chain", chainKey.String()).
		Str("peer", peer.addr).
		Dur("penalty", liveNextUnavailablePenalty)
	if wasSticky {
		event.Bool("cleared_sticky", true)
	}
	event.Msg("temporarily deprioritized live next masterchain peer")
}

func (s *overlaySubscription) clearChainBlockPeerLocked(key chainDownloadKey) {
	delete(s.chainDownloads, key)
}

func downloadedBlockPayloadBytes(block *DownloadedBlock) int64 {
	if block == nil {
		return 0
	}
	return int64(len(block.BlockBOC) + len(block.ProofBOC))
}

func runConcurrentOverlayQueries(ctx context.Context, peers []*overlayPeer, parallelism int, hedgeDelay time.Duration, query func(context.Context, *overlayPeer) (tl.Serializable, error)) (tl.Serializable, error) {
	res, err := runFirstPeerRequest(ctx, peers, peerRequestOptions{
		parallelism: parallelism,
		hedgeDelay:  hedgeDelay,
	}, query)
	if err != nil {
		return nil, err
	}
	return res.value, nil
}

func (n *Node) decodeDownloadedBlock(ctx context.Context, resp tl.Serializable) (*DownloadedBlock, error) {
	block, err := decodeDownloadedBlock(resp)
	if !errors.Is(err, ErrCompressedBlockV2Unsupported) {
		return block, err
	}

	data, ok := resp.(DataFullCompressedV2)
	if !ok {
		return nil, err
	}
	state, stateErr := n.stateForCompressedBlockDecompression(ctx, data.ID, data.Proof)
	if stateErr != nil {
		return nil, fmt.Errorf("%w: %v", err, stateErr)
	}
	return decodeCompressedBlockV2(data, state)
}

func decodeDownloadedBlock(resp tl.Serializable) (*DownloadedBlock, error) {
	switch data := resp.(type) {
	case tonnodeapi.DataFull:
		return normalizeDownloadedBlock("tonNode.dataFull", data.ID, data.Proof, data.Block, data.IsLink, true, nil)
	case tonnodeapi.DataFullEmpty:
		return nil, ErrBlockNotAvailable
	case DataFullCompressed:
		return decodeCompressedBlock(data)
	case DataFullCompressedV2:
		return decodeCompressedBlockV2(data, nil)
	default:
		return nil, fmt.Errorf("unexpected download response %T", resp)
	}
}

func decodeCompressedBlock(data DataFullCompressed) (*DownloadedBlock, error) {
	decompressed, err := decompressLZ4Block(data.Compressed, maxDecompressedBlockSize)
	if err != nil {
		return nil, fmt.Errorf("decompress tonNode.dataFullCompressed: %w", err)
	}

	roots, err := cell.FromBOCMultiRoot(decompressed)
	if err != nil {
		return nil, fmt.Errorf("parse decompressed multi-root boc: %w", err)
	}
	if len(roots) != 2 {
		return nil, fmt.Errorf("expected 2 roots in tonNode.dataFullCompressed, got %d", len(roots))
	}

	proof := cell.ToBOCWithOptions([]*cell.Cell{roots[0]}, cell.BOCSerializeOptions{WithCRC32C: false})
	block := serializeCompressedBlockRoot(roots[1])

	return normalizeDownloadedBlock("tonNode.dataFullCompressed", data.ID, proof, block, data.IsLink, true, roots[1])
}

func decodeCompressedBlockV2(data DataFullCompressedV2, state *cell.Cell) (*DownloadedBlock, error) {
	needState, err := cell.NeedStateForDecompression(data.BlockCompressed)
	if err != nil {
		return nil, fmt.Errorf("check tonNode.dataFullCompressedV2 compression: %w", err)
	}
	if needState && state == nil {
		return nil, ErrCompressedBlockV2Unsupported
	}

	roots, err := cell.DecompressBOC(data.BlockCompressed, maxDecompressedBlockSize, state)
	if err != nil {
		return nil, fmt.Errorf("decompress tonNode.dataFullCompressedV2: %w", err)
	}
	if len(roots) != 1 {
		return nil, fmt.Errorf("expected 1 root in tonNode.dataFullCompressedV2, got %d", len(roots))
	}

	block := serializeCompressedBlockRoot(roots[0])
	return normalizeDownloadedBlock("tonNode.dataFullCompressedV2", data.ID, data.Proof, block, data.IsLink, true, roots[0])
}

func (n *Node) stateForCompressedBlockDecompression(ctx context.Context, block ton.BlockIDExt, proof []byte) (*cell.Cell, error) {
	if n.storage == nil {
		return nil, fmt.Errorf("state storage is not configured")
	}

	prevBlocks, err := prevBlocksFromBlockProof(block, proof)
	if err != nil {
		return nil, err
	}
	if len(prevBlocks) != 1 {
		return nil, fmt.Errorf("state-aware decompression with %d previous blocks is not supported", len(prevBlocks))
	}

	prev := prevBlocks[0]
	if n.compressedState != nil {
		state, err := n.compressedState.StateRootForCompressedBlock(ctx, prev)
		if err == nil {
			return state, nil
		}
		if !errors.Is(err, tnstore.ErrNotFound) {
			return nil, err
		}
	}

	meta, err := n.storage.BlockMeta(ctx, prev)
	if errors.Is(err, tnstore.ErrNotFound) {
		state, stateErr := n.storage.BlockState(ctx, prev)
		if stateErr != nil {
			return nil, stateErr
		}
		meta = tnstore.BuildBlockMetaFromState(*state)
	} else if err != nil {
		return nil, err
	}
	if len(meta.StateRootHash) != 32 {
		return nil, fmt.Errorf("%w: previous state root hash is not known for %s", tnstore.ErrNotFound, formatBlockRef(prev))
	}

	root, err := n.storage.LoadStateCellTree(ctx, prev, meta.StateRootHash)
	if err != nil {
		return nil, err
	}
	return root, nil
}

func prevBlocksFromBlockProof(block ton.BlockIDExt, proof []byte) ([]ton.BlockIDExt, error) {
	root, err := cell.FromBOC(proof)
	if err != nil {
		return nil, fmt.Errorf("parse block proof boc: %w", err)
	}
	parsed, err := parseBlockProofForBlock(block, root)
	if err != nil {
		return nil, fmt.Errorf("parse previous blocks from proof: %w", err)
	}
	meta, err := tnstore.BuildBlockMetaFromParsedBlock(block, parsed)
	if err != nil {
		return nil, err
	}
	return meta.PrevRefs, nil
}

func normalizeDownloadedBlock(kind string, id ton.BlockIDExt, proof []byte, data []byte, isLink bool, requireFileHash bool, parsedBlock *cell.Cell) (*DownloadedBlock, error) {
	if len(proof) == 0 {
		return nil, fmt.Errorf("%s proof is empty", kind)
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("%s block is empty", kind)
	}

	proofRoot, err := cell.FromBOC(proof)
	if err != nil {
		return nil, fmt.Errorf("%s proof is not a valid BOC: %w", kind, err)
	}
	if err = blockproof.CheckProofShape(id, proofRoot, isLink); err != nil {
		return nil, fmt.Errorf("%s proof shape: %w", kind, err)
	}

	if parsedBlock == nil {
		parsedBlock, err = cell.FromBOC(data)
		if err != nil {
			return nil, fmt.Errorf("%s block is not a valid BOC: %w", kind, err)
		}
	}
	rootHash := parsedBlock.HashKey()
	if !bytes.Equal(rootHash[:], id.RootHash) {
		return nil, fmt.Errorf("%s root hash mismatch for %s", kind, formatBlockRef(id))
	}

	sum := sha256.Sum256(data)
	verifiedFileHash := bytes.Equal(sum[:], id.FileHash)
	if requireFileHash && !verifiedFileHash {
		return nil, fmt.Errorf("%s file hash mismatch for %s", kind, formatBlockRef(id))
	}

	return &DownloadedBlock{
		ID:               id,
		Kind:             kind,
		Block:            parsedBlock,
		Proof:            proofRoot,
		BlockBOC:         data,
		ProofBOC:         proof,
		IsLink:           isLink,
		VerifiedRootHash: true,
		VerifiedFileHash: verifiedFileHash,
	}, nil
}

func decompressLZ4Block(data []byte, maxSize int) ([]byte, error) {
	if maxSize <= 0 {
		return nil, fmt.Errorf("invalid max size %d", maxSize)
	}

	size := 1 << 20
	if size > maxSize {
		size = maxSize
	}

	for {
		buf := make([]byte, size)
		n, err := lz4.UncompressBlock(data, buf)
		switch {
		case err == nil:
			return buf[:n], nil
		case !errors.Is(err, lz4.ErrInvalidSourceShortBuffer):
			return nil, err
		case size == maxSize:
			return nil, fmt.Errorf("decompressed data exceeds %d bytes", maxSize)
		}

		size *= 2
		if size > maxSize {
			size = maxSize
		}
	}
}

func serializeCompressedBlockRoot(root *cell.Cell) []byte {
	return root.ToBOCWithOptions(cell.BOCSerializeOptions{
		WithCRC32C:    true,
		WithIndex:     true,
		WithCacheBits: true,
		WithIntHashes: true,
	})
}
