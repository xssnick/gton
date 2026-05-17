package p2p

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"github.com/xssnick/gton/internal/logutil"
	"github.com/xssnick/gton/service/blockproof"
	tnstore "github.com/xssnick/gton/service/storage"
	"fmt"
	"time"

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

type CompressedBlockStateProvider interface {
	StateRootForCompressedBlock(ctx context.Context, block ton.BlockIDExt) (*cell.Cell, error)
}

func (n *Node) SetCompressedBlockStateProvider(provider CompressedBlockStateProvider) {
	n.compressedState = provider
}

func (n *Node) DownloadBlockFull(ctx context.Context, block ton.BlockIDExt) (*DownloadedBlock, error) {
	if cached, err := n.cachedBlockFull(ctx, block); err == nil {
		return cached, nil
	} else if !errors.Is(err, tnstore.ErrNotFound) {
		n.log.Debug().
			Err(err).
			Str("block", formatBlockRef(block)).
			Msg("failed to load cached full block")
	}
	if cached, err := n.popShardBroadcastBlock(block); err == nil {
		return cached, nil
	} else if !errors.Is(err, tnstore.ErrNotFound) {
		n.log.Debug().
			Err(err).
			Str("block", formatBlockRef(block)).
			Msg("failed to load cached shard broadcast block")
	}

	res, err := n.downloadFromOverlay(ctx, block, tonnodeapi.DownloadBlockFull{Block: block})
	if err != nil {
		return nil, err
	}
	if !res.ID.Equals(&block) {
		return nil, fmt.Errorf("peer returned %s for requested block %s", res.BlockRef(), formatBlockRef(block))
	}
	n.rememberDownloadedBlockAsync(nil, res)
	return res, nil
}

func (n *Node) PrefetchShardBlockFull(ctx context.Context, block ton.BlockIDExt) error {
	if isMasterchainBlock(block) {
		return fmt.Errorf("masterchain block %s is not a shard prefetch candidate", formatBlockRef(block))
	}
	if n.shardBroadcastCache != nil && n.shardBroadcastCache.HasBlock(block) {
		return nil
	}

	res, err := n.downloadFromOverlay(ctx, block, tonnodeapi.DownloadBlockFull{Block: block})
	if err != nil {
		return err
	}
	if !res.ID.Equals(&block) {
		return fmt.Errorf("peer returned %s for requested block %s", res.BlockRef(), formatBlockRef(block))
	}

	n.rememberShardBroadcastBlock(res)
	return nil
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

func (n *Node) ProbeNextBlockFull(ctx context.Context, prev ton.BlockIDExt, peerLimit int) (*DownloadedBlock, error) {
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

	res, err := sub.probeNextFull(ctx, prev, peerLimit)
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

func (s *overlaySubscription) downloadFull(ctx context.Context, block ton.BlockIDExt, req tl.Serializable) (*DownloadedBlock, error) {
	peers := s.chainBlockDownloadCandidates(block)
	if len(peers) == 0 {
		return nil, errors.New("overlay has no connected peers")
	}

	return s.downloadFullFromPeers(ctx, block, peers, req)
}

func (s *overlaySubscription) downloadNextFull(ctx context.Context, prev ton.BlockIDExt) (*DownloadedBlock, error) {
	peers := s.chainBlockDownloadCandidates(prev)
	if len(peers) == 0 {
		return nil, errors.New("overlay has no connected peers")
	}

	req := DownloadNextBlockFull{PrevBlock: prev}
	return s.downloadNextFullFromPeers(ctx, prev, peers, blockDownloadParallelism(peers), req)
}

func (s *overlaySubscription) probeNextFull(ctx context.Context, prev ton.BlockIDExt, peerLimit int) (*DownloadedBlock, error) {
	peers := s.chainBlockDownloadCandidates(prev)
	if len(peers) == 0 {
		return nil, errors.New("overlay has no connected peers")
	}
	if peerLimit > 0 && len(peers) > peerLimit {
		peers = peers[:peerLimit]
	}

	req := DownloadNextBlockFull{PrevBlock: prev}
	return probeNextFullFromPeers(ctx, peers, func(ctx context.Context, peer *overlayPeer) (DownloadedBlock, error) {
		return s.downloadNextFullFromPeer(ctx, prev, peer, req)
	}, func(peer *overlayPeer, err error) {
		s.noteChainBlockDownloadFailure(prev, peer, err)
	})
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
		return s.downloadNextFullFromPeer(ctx, chain, peer, req)
	})
	if err != nil {
		return nil, err
	}
	block := res.value
	return &block, nil
}

func (s *overlaySubscription) downloadNextFullFromPeer(ctx context.Context, chain ton.BlockIDExt, peer *overlayPeer, req DownloadNextBlockFull) (DownloadedBlock, error) {
	started := time.Now()
	resp, err := s.queryFromPeerWithLimits(ctx, peer, req, downloadNextQueryTimeout, maxBlockDownloadAnswerSize)
	if err != nil {
		return DownloadedBlock{}, err
	}

	block, err := s.node.decodeDownloadedBlock(ctx, resp)
	if err != nil {
		return DownloadedBlock{}, err
	}
	s.noteChainBlockDownloadSuccess(chain, peer, block, time.Since(started))
	return *block, nil
}

func probeNextFullFromPeers(ctx context.Context, peers []*overlayPeer, query func(context.Context, *overlayPeer) (DownloadedBlock, error), onFailure func(*overlayPeer, error)) (*DownloadedBlock, error) {
	if len(peers) == 0 {
		return nil, errors.New("overlay has no connected peers")
	}

	results, errs := runPeerRequests(ctx, peers, peerRequestOptions{
		parallelism:          len(peers),
		cancelOnFirstSuccess: true,
		onFailure:            onFailure,
	}, query)
	if len(results) > 0 {
		block := results[0].value
		return &block, nil
	}

	return nil, errors.Join(errs...)
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

	stickyKey := downloadPeerKey(sticky)
	ordered := make([]*overlayPeer, 0, len(peers))
	ordered = append(ordered, sticky)
	for _, peer := range peers {
		if downloadPeerKey(peer) != stickyKey {
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

	stickyKey := downloadPeerKey(state.peer)
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
	peer.downloadSuccess(bytes, elapsed, blockSlowPeerSpeed, blockSlowPeerPenalty)
}

func (s *overlaySubscription) noteChainBlockDownloadSuccess(chain ton.BlockIDExt, peer *overlayPeer, block *DownloadedBlock, elapsed time.Duration) {
	noteBlockDownloadSuccess(peer, block, elapsed)

	bytes := downloadedBlockPayloadBytes(block)
	if peer == nil || bytes <= 0 || elapsed <= 0 {
		return
	}
	speed := float64(bytes) / elapsed.Seconds()
	if speed < blockSlowPeerSpeed {
		return
	}

	key := chainDownloadKeyFromBlock(chain)
	s.chainDownloadMx.Lock()
	defer s.chainDownloadMx.Unlock()

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
}

func (s *overlaySubscription) noteChainBlockDownloadFailure(chain ton.BlockIDExt, peer *overlayPeer, err error) {
	if peer == nil || errors.Is(err, ErrBlockNotAvailable) {
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
		return nil, fmt.Errorf("previous state root hash is not known for %s", formatBlockRef(prev))
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
		WithTopHash:   true,
		WithIntHashes: true,
	})
}
