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
var ErrCompressedBlockStateNotReady = errors.New("compressed block previous state is not ready")

const (
	liveNextUnavailablePenalty    = 3 * time.Second
	liveNextProbeSoftTimeout      = 2500 * time.Millisecond
	liveNextProbeEarlyFailDelay   = 1200 * time.Millisecond
	liveNextProbeEarlyFailReserve = 4
	shardDescriptionBroadcastKind = "tonNode.newShardBlockBroadcast"
)

type ProbeNextBlockFullOptions struct {
	PeerLimit       int
	StagedPeerLimit int
	StageDelay      time.Duration
	PreferredPeerID PeerID
	LiveTail        bool
}

type ProbeBlockFullOptions struct {
	PeerLimit       int
	StagedPeerLimit int
	StageDelay      time.Duration
	PreferredPeerID PeerID
}

type CompressedBlockStateProvider interface {
	StateRootForCompressedBlock(ctx context.Context, block ton.BlockIDExt) (*cell.Cell, error)
	// RememberCompressedBlockState offers a materialized state produced on
	// the broadcast decode path so subsequent state-aware decompressions of
	// the next block find an in-memory tree instead of a lazy celldb root.
	RememberCompressedBlockState(state *tnstore.BlockState) bool
}

func (n *Node) DownloadBlockFull(ctx context.Context, block ton.BlockIDExt) (*DownloadedBlock, error) {
	if n.IsOffline() {
		return nil, ErrOffline
	}
	return n.blockFullFromLocalOrOverlay(ctx, block, func(ctx context.Context, block ton.BlockIDExt) (*DownloadedBlock, error) {
		return n.blockFullFromOverlayOrShardBroadcast(ctx, block, func(ctx context.Context, block ton.BlockIDExt) (*DownloadedBlock, error) {
			return n.downloadFromOverlay(ctx, block, tonnodeapi.DownloadBlockFull{Block: block})
		})
	})
}

func (n *Node) ProbeBlockFull(ctx context.Context, block ton.BlockIDExt, opts ProbeBlockFullOptions) (*DownloadedBlock, error) {
	if n.IsOffline() {
		return nil, ErrOffline
	}
	return n.blockFullFromLocalOrOverlay(ctx, block, func(ctx context.Context, block ton.BlockIDExt) (*DownloadedBlock, error) {
		return n.blockFullFromOverlayOrShardBroadcast(ctx, block, func(ctx context.Context, block ton.BlockIDExt) (*DownloadedBlock, error) {
			return n.probeBlockFromOverlay(ctx, block, opts)
		})
	})
}

func (n *Node) blockFullFromLocalOrOverlay(ctx context.Context, block ton.BlockIDExt, load func(context.Context, ton.BlockIDExt) (*DownloadedBlock, error)) (*DownloadedBlock, error) {
	if cached, err := n.cachedShardBroadcastBlock(block); err == nil {
		return cached, nil
	}
	if cached, err := n.cachedLocalBlockFull(ctx, block); err == nil {
		return cached, nil
	}

	res, err := load(ctx, block)
	if err != nil {
		return nil, err
	}
	if !res.ID.Equals(&block) {
		return nil, fmt.Errorf("peer returned %s for requested block %s", res.BlockRef(), tnstore.FormatBlockRef(block))
	}
	return res, nil
}

func (n *Node) blockFullFromOverlayOrShardBroadcast(ctx context.Context, block ton.BlockIDExt, load func(context.Context, ton.BlockIDExt) (*DownloadedBlock, error)) (*DownloadedBlock, error) {
	wake, unwatch := n.watchShardBroadcastBlock(block)
	if wake == nil {
		return load(ctx, block)
	}
	defer unwatch()

	return raceDownloadWithBroadcastWake(ctx, wake, func() (*DownloadedBlock, error) {
		return n.cachedShardBroadcastBlock(block)
	}, func(downloadCtx context.Context) (*DownloadedBlock, error) {
		return load(downloadCtx, block)
	})
}

// raceDownloadWithBroadcastWake races download against a broadcast arrival:
// cached is consulted up front, whenever wake fires and once the download
// returns, so a block delivered by broadcast wins over (and cancels) an
// in-flight download.
func raceDownloadWithBroadcastWake(
	ctx context.Context,
	wake <-chan struct{},
	cached func() (*DownloadedBlock, error),
	download func(context.Context) (*DownloadedBlock, error),
) (*DownloadedBlock, error) {
	if block, err := cached(); err == nil {
		return block, nil
	}

	downloadCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type downloadResult struct {
		block *DownloadedBlock
		err   error
	}
	result := make(chan downloadResult, 1)
	go func() {
		res, err := download(downloadCtx)
		result <- downloadResult{block: res, err: err}
	}()

	for {
		select {
		case res := <-result:
			if block, err := cached(); err == nil {
				return block, nil
			}
			return res.block, res.err
		case <-wake:
			if block, err := cached(); err == nil {
				return block, nil
			}
			wake = nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (n *Node) cachedShardBroadcastBlock(block ton.BlockIDExt) (*DownloadedBlock, error) {
	cached, err := n.shardBroadcastBlock(block)
	if err != nil {
		if errors.Is(err, tnstore.ErrNotFound) {
			return nil, err
		}
		n.log.Debug().
			Err(err).
			Str("block", tnstore.FormatBlockRef(block)).
			Msg("failed to load cached shard broadcast block")
		return nil, err
	}
	return cached, nil
}

func (n *Node) cachedLocalBlockFull(ctx context.Context, block ton.BlockIDExt) (*DownloadedBlock, error) {
	cached, err := n.cachedBlockFull(ctx, block)
	if err != nil {
		if errors.Is(err, tnstore.ErrNotFound) {
			return nil, err
		}
		n.log.Debug().
			Err(err).
			Str("block", tnstore.FormatBlockRef(block)).
			Msg("failed to load cached full block")
		return nil, err
	}
	return cached, nil
}

func (n *Node) PrefetchShardBlockFull(ctx context.Context, block ton.BlockIDExt) error {
	if n.IsOffline() {
		return ErrOffline
	}
	return n.prefetchShardBlockFull(ctx, block, "")
}

func (n *Node) PrefetchShardBlockFullFromBroadcastHint(ctx context.Context, block ton.BlockIDExt) error {
	if n.IsOffline() {
		return ErrOffline
	}
	return n.prefetchShardBlockFull(ctx, block, shardDescriptionBroadcastKind)
}

func (n *Node) prefetchShardBlockFull(ctx context.Context, block ton.BlockIDExt, cacheKind string) error {
	if isMasterchainBlock(block) {
		return fmt.Errorf("masterchain block %s is not a shard prefetch candidate", tnstore.FormatBlockRef(block))
	}
	if n.shardBroadcastCache.HasBlock(block) {
		return nil
	}

	wake, unwatch := n.watchShardBroadcastBlock(block)
	defer unwatch()

	prefetchCtx, cancel := context.WithTimeout(ctx, prefetchShardBlockTimeout)
	defer cancel()

	res, err := raceDownloadWithBroadcastWake(prefetchCtx, wake, func() (*DownloadedBlock, error) {
		return n.cachedShardBroadcastBlock(block)
	}, func(downloadCtx context.Context) (*DownloadedBlock, error) {
		return n.probeBlockFromOverlay(downloadCtx, block, ProbeBlockFullOptions{
			PeerLimit:       prefetchShardBlockPeers,
			StagedPeerLimit: prefetchShardBlockWidePeers,
			StageDelay:      prefetchShardBlockStageDelay,
		})
	})
	if err != nil {
		return err
	}
	if !res.ID.Equals(&block) {
		return fmt.Errorf("peer returned %s for requested block %s", res.BlockRef(), tnstore.FormatBlockRef(block))
	}
	if n.shardBroadcastCache.HasBlock(block) {
		return nil
	}
	if cacheKind != "" {
		res.Kind = cacheKind
	}

	n.rememberShardBroadcastBlock(res)
	return nil
}

// cachedMasterchainNextBroadcast mirrors cachedShardBroadcastBlock for the
// masterchain next-broadcast cache: unexpected lookup errors are logged and
// treated as a miss.
func (n *Node) cachedMasterchainNextBroadcast(prev ton.BlockIDExt) (*DownloadedBlock, error) {
	cached, err := n.masterchainNextBroadcastCache.BlockAfter(prev)
	if err != nil {
		if errors.Is(err, tnstore.ErrNotFound) {
			return nil, err
		}
		n.log.Debug().
			Err(err).
			Str("prev", tnstore.FormatBlockRef(prev)).
			Msg("failed to load cached masterchain next broadcast block")
		return nil, err
	}
	return cached, nil
}

func (n *Node) DownloadNextBlockFull(ctx context.Context, prev ton.BlockIDExt) (*DownloadedBlock, error) {
	return n.nextBlockFullFromCacheOrOverlay(ctx, prev, func(queryCtx context.Context) (*DownloadedBlock, error) {
		return n.downloadNextFromOverlay(queryCtx, prev)
	})
}

func (n *Node) ProbeNextBlockFull(ctx context.Context, prev ton.BlockIDExt, opts ProbeNextBlockFullOptions) (*DownloadedBlock, error) {
	return n.nextBlockFullFromCacheOrOverlay(ctx, prev, func(queryCtx context.Context) (*DownloadedBlock, error) {
		sub, err := n.subscriptionForBlock(prev)
		if err != nil {
			return nil, err
		}
		if err = sub.ensurePeers(queryCtx); err != nil {
			return nil, fmt.Errorf("bootstrap overlay peers: %w", err)
		}
		return sub.probeNextFull(queryCtx, prev, opts)
	})
}

func (n *Node) nextBlockFullFromCacheOrOverlay(ctx context.Context, prev ton.BlockIDExt, download func(context.Context) (*DownloadedBlock, error)) (*DownloadedBlock, error) {
	if n.IsOffline() {
		return nil, ErrOffline
	}
	if cached, err := n.cachedMasterchainNextBroadcast(prev); err == nil {
		return cached, nil
	}

	if cached, err := n.cachedNextBlockFull(ctx, prev); err == nil {
		return cached, nil
	} else if !errors.Is(err, tnstore.ErrNotFound) {
		n.log.Debug().
			Err(err).
			Str("prev", tnstore.FormatBlockRef(prev)).
			Msg("failed to load cached next full block")
	}

	return n.downloadNextFromOverlayOrMasterBroadcast(ctx, prev, download)
}

func (n *Node) NextBlockDescription(ctx context.Context, prev ton.BlockIDExt) (ton.BlockIDExt, error) {
	if n.IsOffline() {
		return ton.BlockIDExt{}, ErrOffline
	}
	if cached, err := n.cachedMasterchainNextBroadcast(prev); err == nil {
		return cached.ID, nil
	}

	full, err := n.localNextServedBlockFull(ctx, prev)
	if err == nil {
		return full.ID, nil
	}
	if !errors.Is(err, tnstore.ErrNotFound) {
		n.log.Debug().
			Err(err).
			Str("prev", tnstore.FormatBlockRef(prev)).
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

func (n *Node) downloadNextFromOverlayOrMasterBroadcast(ctx context.Context, prev ton.BlockIDExt, download func(context.Context) (*DownloadedBlock, error)) (*DownloadedBlock, error) {
	wake, unwatch := n.WatchMasterchainNextBroadcastBlock(prev)
	if wake == nil {
		return download(ctx)
	}
	defer unwatch()

	return raceDownloadWithBroadcastWake(ctx, wake, func() (*DownloadedBlock, error) {
		return n.cachedMasterchainNextBroadcast(prev)
	}, download)
}

func (n *Node) cachedBlockFull(ctx context.Context, block ton.BlockIDExt) (*DownloadedBlock, error) {
	full, err := n.localServedBlockFull(ctx, block)
	if err != nil {
		return nil, err
	}
	return decodeRawDownloadedBlock("local full block cache", full.ID, full.Proof, full.Block, full.IsLink)
}

func (n *Node) cachedNextBlockFull(ctx context.Context, prev ton.BlockIDExt) (*DownloadedBlock, error) {
	full, err := n.localNextServedBlockFull(ctx, prev)
	if err != nil {
		return nil, err
	}
	return decodeRawDownloadedBlock("local next block cache", full.ID, full.Proof, full.Block, full.IsLink)
}

func (n *Node) localServedBlockFull(ctx context.Context, block ton.BlockIDExt) (*tnstore.ServedBlockFull, error) {
	full, err := n.liveBlockCache.BlockFull(ctx, block)
	if err == nil {
		return full, nil
	}
	if !errors.Is(err, tnstore.ErrNotFound) {
		return nil, err
	}

	return n.peerStorage.BlockFull(ctx, block)
}

func (n *Node) localNextServedBlockFull(ctx context.Context, prev ton.BlockIDExt) (*tnstore.ServedBlockFull, error) {
	full, err := n.liveBlockCache.NextBlockFull(ctx, prev)
	if err == nil {
		return full, nil
	}
	if !errors.Is(err, tnstore.ErrNotFound) {
		return nil, err
	}

	return n.peerStorage.NextBlockFull(ctx, prev)
}

func (n *Node) localBlockData(ctx context.Context, block ton.BlockIDExt) ([]byte, error) {
	data, err := n.liveBlockCache.BlockData(ctx, block)
	if err == nil {
		return data, nil
	}
	if !errors.Is(err, tnstore.ErrNotFound) {
		return nil, err
	}

	data, err = n.peerStorage.BlockData(ctx, block)
	if err == nil {
		return data, nil
	}
	if !errors.Is(err, tnstore.ErrNotFound) {
		return nil, err
	}

	full, err := n.localServedBlockFull(ctx, block)
	if err != nil {
		return nil, err
	}
	return full.Block, nil
}

func (n *Node) localBlockProof(ctx context.Context, kind tnstore.ServedProofKind, block ton.BlockIDExt) ([]byte, error) {
	proof, err := n.liveBlockCache.BlockProof(ctx, kind, block)
	if err == nil {
		return proof, nil
	}
	if !errors.Is(err, tnstore.ErrNotFound) {
		return nil, err
	}

	return n.peerStorage.BlockProof(ctx, kind, block)
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
		return s.downloadBlockFullFromPeer(ctx, block, peer, req)
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
func (s *overlaySubscription) downloadBlockFullFromPeer(ctx context.Context, requested ton.BlockIDExt, peer *overlayPeer, req tl.Serializable) (DownloadedBlock, error) {
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
	s.node.observeDownloadedBlockReceived(ctx, &block)
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
	startedAt := time.Now()
	if err := peer.rldpOverlay.DoQuery(queryCtx, maxAnswerSize, req, &resp); err != nil {
		s.handlePeerQueryFailure(peer, err)
		return nil, err
	}
	peer.querySuccess(time.Since(startedAt))

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
	downloaded, err := runConcurrentBlockDownloads(ctx, peers, blockDownloadParallelism(peers), func(peer *overlayPeer, err error) {
		s.noteChainBlockDownloadFailure(requested, peer, err)
	}, func(ctx context.Context, peer *overlayPeer) (DownloadedBlock, error) {
		return s.downloadBlockFullFromPeer(ctx, requested, peer, req)
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
	if bytes <= 0 || elapsed <= 0 {
		return
	}
	speed := float64(bytes) / elapsed.Seconds()
	winnerID := peer.id
	if winnerID.IsZero() {
		return
	}
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

	if state.peer == nil || state.peer.id != peer.id {
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
		if missedKey == winnerID || missed.peer == nil || confirmedAt.Sub(missed.at) > blockUnavailableConfirmWindow {
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
	if !isMasterchainBlock(chain) || elapsed <= 0 {
		return
	}

	key := peer.id
	if key.IsZero() {
		return
	}

	s.chainDownloadMx.Lock()
	defer s.chainDownloadMx.Unlock()

	if s.liveNextPeers == nil {
		s.liveNextPeers = map[PeerID]*liveNextPeerState{}
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
	if errors.Is(err, ErrBlockNotAvailable) {
		s.noteChainBlockUnavailable(chain, peer)
		return
	}
	peer.downloadFailed(blockSlowPeerPenalty)

	key := chainDownloadKeyFromBlock(chain)
	s.chainDownloadMx.Lock()
	defer s.chainDownloadMx.Unlock()

	state := s.chainDownloads[key]
	if state != nil && state.peer != nil && state.peer.id == peer.id {
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
	peerID := peer.id
	if peerID.IsZero() {
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
		state.unavailable = map[PeerID]chainUnavailablePeer{}
	}
	state.unavailable[peerID] = chainUnavailablePeer{
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

	peerID := peer.id
	if peerID.IsZero() {
		return
	}

	chainKey := chainDownloadKeyFromBlock(chain)
	unavailableUntil := time.Now().Add(liveNextUnavailablePenalty)

	s.chainDownloadMx.Lock()
	if s.liveNextPeers == nil {
		s.liveNextPeers = map[PeerID]*liveNextPeerState{}
	}
	state := s.liveNextPeers[peerID]
	if state == nil {
		state = &liveNextPeerState{}
		s.liveNextPeers[peerID] = state
	}
	state.unavailableUntil = unavailableUntil

	sticky := s.chainDownloads[chainKey]
	wasSticky := sticky != nil && sticky.peer != nil && sticky.peer.id == peerID
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
	switch data := resp.(type) {
	case tonnodeapi.DataFull:
		downloaded, err := decodeRawDownloadedBlock("tonNode.dataFull", data.ID, data.Proof, data.Block, data.IsLink)
		if err == nil || !n.IsHardfork(data.ID) {
			return downloaded, err
		}
		return decodeRawDownloadedHardforkBlock("tonNode.dataFull", data.ID, data.Proof, data.Block, data.IsLink, err)
	case DataFullCompressed:
		downloaded, err := decodeCompressedBlock(data)
		if err == nil || !n.IsHardfork(data.ID) {
			return downloaded, err
		}
		return decodeCompressedHardforkBlock(data, err)
	case DataFullCompressedV2:
		return n.decodeDataFullCompressedV2(ctx, data)
	default:
		return decodeDownloadedBlock(resp)
	}
}

func decodeDownloadedBlock(resp tl.Serializable) (*DownloadedBlock, error) {
	switch data := resp.(type) {
	case tonnodeapi.DataFull:
		return decodeRawDownloadedBlock("tonNode.dataFull", data.ID, data.Proof, data.Block, data.IsLink)
	case tonnodeapi.DataFullEmpty:
		return nil, ErrBlockNotAvailable
	case DataFullCompressed:
		return decodeCompressedBlock(data)
	case DataFullCompressedV2:
		return nil, fmt.Errorf("tonNode.dataFullCompressedV2 requires state-aware decode")
	default:
		return nil, fmt.Errorf("unexpected download response %T", resp)
	}
}

func decodeCompressedBlock(data DataFullCompressed) (*DownloadedBlock, error) {
	decompressed, err := decompressLZ4Block(data.Compressed)
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

	return newVerifiedDownloadedBlockWithProofShape(
		"tonNode.dataFullCompressed",
		data.ID,
		proof,
		block,
		data.IsLink,
		roots[0],
		roots[1],
		false,
	)
}

func decodeCompressedHardforkBlock(data DataFullCompressed, cause error) (*DownloadedBlock, error) {
	decompressed, err := decompressLZ4Block(data.Compressed)
	if err != nil {
		return nil, cause
	}

	roots, err := cell.FromBOCMultiRoot(decompressed)
	if err != nil || len(roots) != 2 {
		return nil, cause
	}

	proof := cell.ToBOCWithOptions([]*cell.Cell{roots[0]}, cell.BOCSerializeOptions{WithCRC32C: false})
	block := serializeCompressedBlockRoot(roots[1])
	return newVerifiedDownloadedHardforkBlock("tonNode.dataFullCompressed", data.ID, proof, block, data.IsLink, roots[0], roots[1], cause)
}

func (n *Node) decodeDataFullCompressedV2(ctx context.Context, data DataFullCompressedV2) (*DownloadedBlock, error) {
	proofRoot, err := parseDownloadedBlockProof(data.Proof)
	if err != nil {
		return nil, fmt.Errorf("parse tonNode.dataFullCompressedV2 proof: %w", err)
	}

	needState, err := cell.NeedStateForDecompression(data.BlockCompressed)
	if err != nil {
		return nil, fmt.Errorf("check tonNode.dataFullCompressedV2 compression: %w", err)
	}
	if !needState {
		downloaded, err := decodeCompressedBlockV2WithProofRoot(data, nil, proofRoot)
		if err == nil || !n.IsHardfork(data.ID) {
			return downloaded, err
		}
		return decodeCompressedBlockV2WithProofRootForHardfork(data, nil, proofRoot, err)
	}

	state, err := n.stateForCompressedBlockDecompression(ctx, data.ID, proofRoot)
	if err != nil {
		if errors.Is(err, tnstore.ErrNotFound) {
			return nil, fmt.Errorf("%w: %v", ErrCompressedBlockStateNotReady, err)
		}
		return nil, err
	}
	downloaded, err := decodeCompressedBlockV2WithProofRoot(data, state, proofRoot)
	if err == nil || !n.IsHardfork(data.ID) {
		return downloaded, err
	}
	return decodeCompressedBlockV2WithProofRootForHardfork(data, state, proofRoot, err)
}

func decodeCompressedBlockV2WithProofRoot(data DataFullCompressedV2, state *cell.Cell, proofRoot *cell.Cell) (*DownloadedBlock, error) {
	needState, err := cell.NeedStateForDecompression(data.BlockCompressed)
	if err != nil {
		return nil, fmt.Errorf("check tonNode.dataFullCompressedV2 compression: %w", err)
	}
	if needState && state == nil {
		return nil, ErrCompressedBlockStateNotReady
	}

	roots, block, err := cell.DecompressBOCSerialized(data.BlockCompressed, maxDecompressedBlockSize, state, compressedBlockRootSerializeOptions)
	if err != nil {
		return nil, fmt.Errorf("decompress tonNode.dataFullCompressedV2: %w", err)
	}
	if len(roots) != 1 {
		return nil, fmt.Errorf("expected 1 root in tonNode.dataFullCompressedV2, got %d", len(roots))
	}

	return newVerifiedDownloadedBlockWithProofShape(
		"tonNode.dataFullCompressedV2",
		data.ID,
		data.Proof,
		block,
		data.IsLink,
		proofRoot,
		roots[0],
		false,
	)
}

func decodeCompressedBlockV2WithProofRootForHardfork(data DataFullCompressedV2, state *cell.Cell, proofRoot *cell.Cell, cause error) (*DownloadedBlock, error) {
	needState, err := cell.NeedStateForDecompression(data.BlockCompressed)
	if err != nil {
		return nil, cause
	}
	if needState && state == nil {
		return nil, cause
	}

	roots, block, err := cell.DecompressBOCSerialized(data.BlockCompressed, maxDecompressedBlockSize, state, compressedBlockRootSerializeOptions)
	if err != nil || len(roots) != 1 {
		return nil, cause
	}

	return newVerifiedDownloadedHardforkBlock("tonNode.dataFullCompressedV2", data.ID, data.Proof, block, data.IsLink, proofRoot, roots[0], cause)
}

func (n *Node) stateForCompressedBlockDecompression(ctx context.Context, block ton.BlockIDExt, proofRoot *cell.Cell) (*cell.Cell, error) {
	if n.storage == nil {
		return nil, fmt.Errorf("state storage is not configured")
	}

	prev, err := compressedBlockPreviousState(block, proofRoot)
	if err != nil {
		return nil, err
	}
	return n.stateForCompressedBlockDecompressionPrev(ctx, prev)
}

func compressedBlockPreviousState(block ton.BlockIDExt, proofRoot *cell.Cell) (ton.BlockIDExt, error) {
	prevBlocks, err := prevBlocksFromBlockProof(block, proofRoot)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if len(prevBlocks) != 1 {
		return ton.BlockIDExt{}, fmt.Errorf("state-aware decompression with %d previous blocks is not supported", len(prevBlocks))
	}
	return prevBlocks[0], nil
}

func (n *Node) stateForCompressedBlockDecompressionPrev(ctx context.Context, prev ton.BlockIDExt) (*cell.Cell, error) {
	if n.storage == nil {
		return nil, fmt.Errorf("state storage is not configured")
	}

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
		meta, err = tnstore.BuildBlockMetaFromState(*state)
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	if len(meta.StateRootHash) != 32 {
		return nil, fmt.Errorf("%w: previous state root hash is not known for %s", tnstore.ErrNotFound, tnstore.FormatBlockRef(prev))
	}

	root, err := n.storage.LoadStateCellTree(ctx, prev, meta.StateRootHash)
	if err != nil {
		return nil, err
	}
	return root, nil
}

func prevBlocksFromBlockProof(block ton.BlockIDExt, proofRoot *cell.Cell) ([]ton.BlockIDExt, error) {
	parsed, err := parseBlockProofForBlock(block, proofRoot)
	if err != nil {
		return nil, fmt.Errorf("parse previous blocks from proof: %w", err)
	}
	meta, err := tnstore.BuildBlockMetaFromParsedBlock(block, parsed)
	if err != nil {
		return nil, err
	}
	return meta.PrevRefs, nil
}

func decodeRawDownloadedBlock(kind string, id ton.BlockIDExt, proof []byte, data []byte, isLink bool) (*DownloadedBlock, error) {
	proofRoot, err := parseDownloadedBlockProof(proof)
	if err != nil {
		return nil, fmt.Errorf("parse %s proof: %w", kind, err)
	}
	return decodeRawDownloadedBlockWithProofRoot(kind, id, proof, data, isLink, proofRoot)
}

func decodeRawDownloadedBlockWithProofRoot(kind string, id ton.BlockIDExt, proof []byte, data []byte, isLink bool, proofRoot *cell.Cell) (*DownloadedBlock, error) {
	return decodeRawDownloadedBlockWithShape(kind, id, proof, data, isLink, proofRoot, true)
}

func decodeRawDownloadedBlockWithShape(kind string, id ton.BlockIDExt, proof []byte, data []byte, isLink bool, proofRoot *cell.Cell, validateStateUpdate bool) (*DownloadedBlock, error) {
	blockRoot, err := parseDownloadedBlockData(data)
	if err != nil {
		return nil, fmt.Errorf("parse %s block: %w", kind, err)
	}
	return newVerifiedDownloadedBlockWithShape(
		kind,
		id,
		proof,
		data,
		isLink,
		proofRoot,
		blockRoot,
		false,
		validateStateUpdate,
	)
}

func decodeRawDownloadedHardforkBlock(kind string, id ton.BlockIDExt, proof []byte, data []byte, isLink bool, cause error) (*DownloadedBlock, error) {
	proofRoot, err := parseDownloadedBlockProof(proof)
	if err != nil {
		return nil, cause
	}
	blockRoot, err := parseDownloadedBlockData(data)
	if err != nil {
		return nil, cause
	}
	return newVerifiedDownloadedHardforkBlock(kind, id, proof, data, isLink, proofRoot, blockRoot, cause)
}

func parseDownloadedBlockProof(proof []byte) (*cell.Cell, error) {
	if len(proof) == 0 {
		return nil, fmt.Errorf("proof is empty")
	}

	proofRoot, err := cell.FromBOC(proof)
	if err != nil {
		return nil, fmt.Errorf("proof is not a valid BOC: %w", err)
	}
	return proofRoot, nil
}

func parseDownloadedBlockData(data []byte) (*cell.Cell, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("block is empty")
	}

	blockRoot, err := cell.FromBOC(data)
	if err != nil {
		return nil, fmt.Errorf("block is not a valid BOC: %w", err)
	}
	return blockRoot, nil
}

func newVerifiedDownloadedHardforkBlock(kind string, id ton.BlockIDExt, proof []byte, data []byte, isLink bool, proofRoot *cell.Cell, blockRoot *cell.Cell, cause error) (*DownloadedBlock, error) {
	downloaded, err := newVerifiedDownloadedBlockWithProofShape(kind, id, proof, data, isLink, proofRoot, blockRoot, true)
	if err != nil {
		return nil, fmt.Errorf("%v; hardfork decode: %w", cause, err)
	}
	return downloaded, nil
}

func newVerifiedDownloadedBlockWithProofShape(kind string, id ton.BlockIDExt, proof []byte, data []byte, isLink bool, proofRoot *cell.Cell, blockRoot *cell.Cell, hardfork bool) (*DownloadedBlock, error) {
	return newVerifiedDownloadedBlockWithShape(kind, id, proof, data, isLink, proofRoot, blockRoot, hardfork, true)
}

// newVerifiedDownloadedBlockWithShape verifies and assembles a downloaded
// block. validateStateUpdate runs the standalone merkle-update consistency
// walk; broadcast decode paths skip it because the block content is anchored
// by the root/file hash checks below plus validator signatures, and the apply
// path re-validates the update against the actual previous state.
func newVerifiedDownloadedBlockWithShape(kind string, id ton.BlockIDExt, proof []byte, data []byte, isLink bool, proofRoot *cell.Cell, blockRoot *cell.Cell, hardfork bool, validateStateUpdate bool) (*DownloadedBlock, error) {
	if len(proof) == 0 {
		return nil, fmt.Errorf("%s proof is empty", kind)
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("%s block is empty", kind)
	}

	var err error
	if hardfork {
		err = blockproof.CheckHardforkProofShape(id, proofRoot, isLink)
	} else {
		err = blockproof.CheckProofShape(id, proofRoot, isLink)
	}
	if err != nil {
		return nil, fmt.Errorf("%s proof shape: %w", kind, err)
	}

	effectiveRoot, err := effectiveDownloadedBlockRoot(id, isLink, blockRoot)
	if err != nil {
		return nil, fmt.Errorf("%s block root: %w", kind, err)
	}

	rootHash := effectiveRoot.HashKey()
	if !bytes.Equal(rootHash[:], id.RootHash) {
		return nil, fmt.Errorf("%s root hash mismatch for %s", kind, tnstore.FormatBlockRef(id))
	}

	sum := sha256.Sum256(data)
	if !bytes.Equal(sum[:], id.FileHash) {
		return nil, fmt.Errorf("%s file hash mismatch for %s", kind, tnstore.FormatBlockRef(id))
	}

	parsed, err := tnstore.ParseVerifiedBlockCell(id, effectiveRoot)
	if err != nil {
		return nil, fmt.Errorf("%s parse verified block %s: %w", kind, tnstore.FormatBlockRef(id), err)
	}
	if hardfork {
		if err = blockproof.ValidateHardforkBlock(id, parsed); err != nil {
			return nil, fmt.Errorf("%s validate hardfork block %s: %w", kind, tnstore.FormatBlockRef(id), err)
		}
	}
	if parsed.StateUpdate == nil {
		return nil, fmt.Errorf("%s block %s has no state update", kind, tnstore.FormatBlockRef(id))
	}
	if validateStateUpdate {
		if err := cell.ValidateMerkleUpdate(parsed.StateUpdate); err != nil {
			return nil, fmt.Errorf("%s validate state update %s: %w", kind, tnstore.FormatBlockRef(id), err)
		}
	}
	meta, err := tnstore.BuildBlockMetaFromParsedBlock(id, parsed)
	if err != nil {
		return nil, fmt.Errorf("%s build block meta %s: %w", kind, tnstore.FormatBlockRef(id), err)
	}

	return &DownloadedBlock{
		ID:               id,
		Kind:             kind,
		Block:            effectiveRoot,
		Proof:            proofRoot,
		BlockBOC:         data,
		ProofBOC:         proof,
		Meta:             meta,
		StateUpdate:      parsed.StateUpdate,
		IsLink:           isLink,
		VerifiedRootHash: true,
	}, nil
}

func effectiveDownloadedBlockRoot(id ton.BlockIDExt, isLink bool, root *cell.Cell) (*cell.Cell, error) {
	if root == nil {
		return nil, fmt.Errorf("block %s has no parsed root", tnstore.FormatBlockRef(id))
	}
	if !isLink || root.GetType() != cell.MerkleProofCellType {
		return root, nil
	}
	unwrapped, err := cell.UnwrapProof(root, id.RootHash)
	if err != nil {
		return nil, fmt.Errorf("unwrap merkle proof link for %s: %w", tnstore.FormatBlockRef(id), err)
	}
	return unwrapped, nil
}

func decompressLZ4Block(data []byte) ([]byte, error) {
	// blocks compress roughly 2-4x, so 4x the compressed size lands within
	// one attempt for almost every payload; every retry re-decodes the whole
	// prefix, so undershooting is far more expensive than overshooting
	size := 1 << 20
	if estimated := 4 * len(data); estimated > size {
		size = estimated
	}
	if size > maxDecompressedBlockSize {
		size = maxDecompressedBlockSize
	}

	for {
		buf := make([]byte, size)
		n, err := lz4.UncompressBlock(data, buf)
		switch {
		case err == nil:
			return buf[:n], nil
		case !errors.Is(err, lz4.ErrInvalidSourceShortBuffer):
			return nil, err
		case size == maxDecompressedBlockSize:
			return nil, fmt.Errorf("decompressed data exceeds %d bytes", maxDecompressedBlockSize)
		}

		size *= 4
		if size > maxDecompressedBlockSize {
			size = maxDecompressedBlockSize
		}
	}
}

// compressedBlockRootSerializeOptions reproduce the canonical block file
// serialization, so the sha256 of the produced bytes matches the block file
// hash.
var compressedBlockRootSerializeOptions = cell.BOCSerializeOptions{
	WithCRC32C:    true,
	WithIndex:     true,
	WithCacheBits: true,
	WithIntHashes: true,
}

func serializeCompressedBlockRoot(root *cell.Cell) []byte {
	return root.ToBOCWithOptions(compressedBlockRootSerializeOptions)
}
