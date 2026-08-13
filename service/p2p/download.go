package p2p

import (
	"context"
	"errors"
	"fmt"
	"time"

	tnstore "github.com/xssnick/gton/service/storage"

	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

var ErrBlockNotAvailable = errors.New("peer does not have the requested block")
var ErrCompressedBlockStateNotReady = errors.New("compressed block previous state is not ready")

const shardDescriptionBroadcastKind = "tonNode.newShardBlockBroadcast"

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
	// Speculative marks a probe for a block the peer is not expected to have
	// yet, so "not available" is an answer rather than a fault by the peer. The
	// shard prefetch runs off a broadcast description and deliberately asks
	// ahead of the peer's own apply; catch-up, which asks for blocks that must
	// already exist, leaves this false.
	Speculative bool
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
			Speculative:     true,
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
		sub, err := n.querySubscriptionForBlock(prev)
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

	sub, err := n.querySubscriptionForBlock(prev)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if err = sub.ensurePeers(ctx); err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("bootstrap overlay peers: %w", err)
	}
	return sub.nextBlockDescription(ctx, prev)
}

func (n *Node) downloadNextFromOverlay(ctx context.Context, prev ton.BlockIDExt) (*DownloadedBlock, error) {
	sub, err := n.querySubscriptionForBlock(prev)
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
