package p2p

import (
	"context"
	"errors"
	"fmt"

	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

func (n *Node) NextKeyBlocks(ctx context.Context, block ton.BlockIDExt, limit int32) ([]ton.BlockIDExt, bool, error) {
	if limit <= 0 {
		limit = 16
	}

	sub, err := n.subscriptionForBlock(block)
	if err != nil {
		return nil, false, err
	}
	if err = sub.ensurePeers(ctx); err != nil {
		return nil, false, fmt.Errorf("bootstrap overlay peers: %w", err)
	}

	resp, err := sub.queryKeyBlocks(ctx, GetNextKeyBlockIDs{
		Block:   block,
		MaxSize: limit,
	})
	if err != nil {
		return nil, false, err
	}

	keyBlocks, ok := resp.(KeyBlocks)
	if !ok {
		return nil, false, fmt.Errorf("unexpected getNextKeyBlockIds response %T", resp)
	}
	if keyBlocks.Error {
		return nil, keyBlocks.Incomplete, fmt.Errorf("fullnode returned key block lookup error")
	}

	return keyBlocks.Blocks, keyBlocks.Incomplete, nil
}

func (s *overlaySubscription) queryKeyBlocks(ctx context.Context, req tl.Serializable) (tl.Serializable, error) {
	peers := s.queryCandidates(0, 0)
	if len(peers) == 0 {
		return nil, errors.New("overlay has no connected peers")
	}

	return runConcurrentOverlayQueries(ctx, peers, keyBlockLookupParallelism, keyBlockLookupHedgeDelay, func(ctx context.Context, peer *overlayPeer) (tl.Serializable, error) {
		return s.queryFromPeerWithLimits(ctx, peer, req, keyBlockLookupTimeout, maxKeyBlockLookupAnswerSize)
	})
}
