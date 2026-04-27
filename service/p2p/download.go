package p2p

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	tnstore "flexserver/service/storage"
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

func (n *Node) DownloadBlockFull(ctx context.Context, block ton.BlockIDExt) (*DownloadedBlock, error) {
	if cached, err := n.cachedBlockFull(ctx, block); err == nil {
		return cached, nil
	} else if !errors.Is(err, tnstore.ErrNotFound) {
		n.log.Debug().
			Err(err).
			Str("block", formatBlockRef(block)).
			Msg("failed to load cached full block")
	}

	res, err := n.downloadFromOverlay(ctx, block, tonnodeapi.DownloadBlockFull{Block: block})
	if err != nil {
		return nil, err
	}
	if !res.ID.Equals(&block) {
		return nil, fmt.Errorf("peer returned %s for requested block %s", res.BlockRef(), formatBlockRef(block))
	}
	n.rememberDownloadedBlock(nil, res)
	return res, nil
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
	n.rememberDownloadedBlock(&prev, res)
	return res, nil
}

func (n *Node) downloadNextFromOverlay(ctx context.Context, prev ton.BlockIDExt) (*DownloadedBlock, error) {
	sub, err := n.subscriptionForBlock(prev)
	if err != nil {
		return nil, err
	}

	if err = sub.ensurePeers(ctx); err != nil {
		return nil, fmt.Errorf("bootstrap overlay peers: %w", err)
	}

	return sub.downloadFullFromBestPeer(ctx, DownloadNextBlockFull{PrevBlock: prev})
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

	return sub.downloadFull(ctx, req)
}

func (s *overlaySubscription) downloadFull(ctx context.Context, req tl.Serializable) (*DownloadedBlock, error) {
	peers := s.queryCandidates(0, 0)
	if len(peers) == 0 {
		return nil, errors.New("overlay has no connected peers")
	}

	return s.downloadFullFromPeers(ctx, peers, req)
}

func (s *overlaySubscription) downloadFullFromBestPeer(ctx context.Context, req tl.Serializable) (*DownloadedBlock, error) {
	peers := s.queryCandidates(0, 0)
	if len(peers) == 0 {
		return nil, errors.New("overlay has no connected peers")
	}

	var errs []error
	for _, peer := range peers {
		resp, err := s.queryFromPeerWithLimits(ctx, peer, req, downloadNextQueryTimeout, maxBlockDownloadAnswerSize)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", peer.addr, err))
			continue
		}

		block, err := decodeDownloadedBlock(resp)
		if err == nil {
			return block, nil
		}
		errs = append(errs, fmt.Errorf("%s: %w", peer.addr, err))
	}

	return nil, errors.Join(errs...)
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

type blockDownloadResult struct {
	block DownloadedBlock
	err   error
}

type overlayQueryResult struct {
	resp tl.Serializable
	err  error
}

func (s *overlaySubscription) downloadFullFromPeers(ctx context.Context, peers []*overlayPeer, req tl.Serializable) (*DownloadedBlock, error) {
	return runConcurrentBlockDownloads(ctx, peers, downloadQueryParallelism, func(ctx context.Context, peer *overlayPeer) (DownloadedBlock, error) {
		resp, err := s.queryFromPeer(ctx, peer, req)
		if err != nil {
			return DownloadedBlock{}, err
		}

		block, err := decodeDownloadedBlock(resp)
		if err != nil {
			return DownloadedBlock{}, err
		}
		return *block, nil
	})
}

func runConcurrentBlockDownloads(ctx context.Context, peers []*overlayPeer, parallelism int, query func(context.Context, *overlayPeer) (DownloadedBlock, error)) (*DownloadedBlock, error) {
	parallelism = minInt(parallelism, len(peers))
	if parallelism <= 0 {
		return nil, errors.New("overlay has no connected peers")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan blockDownloadResult, len(peers))
	var (
		nextIdx  int
		inFlight int
	)

	launch := func(peer *overlayPeer) {
		inFlight++
		go func() {
			block, err := query(ctx, peer)
			if err != nil {
				err = fmt.Errorf("%s: %w", peer.addr, err)

				select {
				case results <- blockDownloadResult{err: err}:
				case <-ctx.Done():
				}
				return
			}

			select {
			case results <- blockDownloadResult{block: block}:
			case <-ctx.Done():
			}
		}()
	}

	for nextIdx < len(peers) && inFlight < parallelism {
		launch(peers[nextIdx])
		nextIdx++
	}

	var hedgeTimer *time.Timer
	if nextIdx < len(peers) {
		hedgeTimer = time.NewTimer(downloadQueryHedgeDelay)
		defer hedgeTimer.Stop()
	}

	var errs []error
	for inFlight > 0 || nextIdx < len(peers) {
		var hedgeC <-chan time.Time
		if hedgeTimer != nil {
			hedgeC = hedgeTimer.C
		}

		select {
		case <-ctx.Done():
			if len(errs) > 0 {
				return nil, errors.Join(errs...)
			}
			return nil, ctx.Err()
		case <-hedgeC:
			if nextIdx < len(peers) {
				launch(peers[nextIdx])
				nextIdx++
			}
			if nextIdx < len(peers) {
				hedgeTimer.Reset(downloadQueryHedgeDelay)
			} else {
				hedgeTimer = nil
			}
		case res := <-results:
			inFlight--
			if res.err == nil {
				cancel()
				block := res.block
				return &block, nil
			}

			errs = append(errs, res.err)

			if nextIdx < len(peers) {
				launch(peers[nextIdx])
				nextIdx++
			}
		}
	}

	return nil, errors.Join(errs...)
}

func runConcurrentOverlayQueries(ctx context.Context, peers []*overlayPeer, parallelism int, hedgeDelay time.Duration, query func(context.Context, *overlayPeer) (tl.Serializable, error)) (tl.Serializable, error) {
	parallelism = minInt(parallelism, len(peers))
	if parallelism <= 0 {
		return nil, errors.New("overlay has no connected peers")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan overlayQueryResult, len(peers))
	var (
		nextIdx  int
		inFlight int
	)

	launch := func(peer *overlayPeer) {
		inFlight++
		go func() {
			resp, err := query(ctx, peer)
			if err != nil {
				err = fmt.Errorf("%s: %w", peer.addr, err)

				select {
				case results <- overlayQueryResult{err: err}:
				case <-ctx.Done():
				}
				return
			}

			select {
			case results <- overlayQueryResult{resp: resp}:
			case <-ctx.Done():
			}
		}()
	}

	for nextIdx < len(peers) && inFlight < parallelism {
		launch(peers[nextIdx])
		nextIdx++
	}

	var hedgeTimer *time.Timer
	if nextIdx < len(peers) {
		hedgeTimer = time.NewTimer(hedgeDelay)
		defer hedgeTimer.Stop()
	}

	var errs []error
	for inFlight > 0 || nextIdx < len(peers) {
		var hedgeC <-chan time.Time
		if hedgeTimer != nil {
			hedgeC = hedgeTimer.C
		}

		select {
		case <-ctx.Done():
			if len(errs) > 0 {
				return nil, errors.Join(errs...)
			}
			return nil, ctx.Err()
		case <-hedgeC:
			if nextIdx < len(peers) {
				launch(peers[nextIdx])
				nextIdx++
			}
			if nextIdx < len(peers) {
				hedgeTimer.Reset(hedgeDelay)
			} else {
				hedgeTimer = nil
			}
		case res := <-results:
			inFlight--
			if res.err == nil {
				cancel()
				return res.resp, nil
			}

			errs = append(errs, res.err)

			if nextIdx < len(peers) {
				launch(peers[nextIdx])
				nextIdx++
			}
		}
	}

	return nil, errors.Join(errs...)
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
		return nil, ErrCompressedBlockV2Unsupported
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

	proof := cell.ToBOCWithOptions([]*cell.Cell{roots[0]}, cell.BOCOptions{WithCRC32C: false})
	block, _ := serializeBlockRootBestEffort(roots[1], data.ID.FileHash)

	return normalizeDownloadedBlock("tonNode.dataFullCompressed", data.ID, proof, block, data.IsLink, false, roots[1])
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

func serializeBlockRootBestEffort(root *cell.Cell, expectedFileHash []byte) ([]byte, bool) {
	first := root.ToBOCWithOptions(cell.BOCOptions{WithCRC32C: false})
	if bocFileHashMatches(first, expectedFileHash) {
		return first, true
	}

	candidate := root.ToBOC()
	if bocFileHashMatches(candidate, expectedFileHash) {
		return candidate, true
	}

	candidate = cell.ToBOCWithOptions([]*cell.Cell{root}, cell.BOCOptions{WithIndex: true})
	if bocFileHashMatches(candidate, expectedFileHash) {
		return candidate, true
	}

	candidate = cell.ToBOCWithOptions([]*cell.Cell{root}, cell.BOCOptions{WithCRC32C: true, WithIndex: true})
	if bocFileHashMatches(candidate, expectedFileHash) {
		return candidate, true
	}

	return first, false
}

func bocFileHashMatches(boc []byte, want []byte) bool {
	sum := sha256.Sum256(boc)
	return bytes.Equal(sum[:], want)
}
