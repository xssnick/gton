package p2p

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

type emptyKeyBlockResponse struct{}

func (e emptyKeyBlockResponse) Error() string {
	return "empty key block response"
}

type KeyBlockBatch struct {
	Blocks     []ton.BlockIDExt
	Incomplete bool
}

func (n *Node) NextKeyBlocks(ctx context.Context, block ton.BlockIDExt, limit int32) (KeyBlockBatch, error) {
	if block.Workchain != -1 || block.Shard != topShard {
		return KeyBlockBatch{}, fmt.Errorf("next key block lookup requires masterchain block, got %s", formatBlockRef(block))
	}

	if limit <= 0 {
		limit = 16
	}

	sub, err := n.subscriptionForBlock(block)
	if err != nil {
		return KeyBlockBatch{}, err
	}
	if err = sub.ensurePeers(ctx); err != nil {
		return KeyBlockBatch{}, fmt.Errorf("bootstrap overlay peers: %w", err)
	}
	sub.startSeedFromDHTTarget(ctx, bootstrapDiscoveryTarget)

	resp, err := sub.queryKeyBlocks(ctx, GetNextKeyBlockIDs{
		Block:   block,
		MaxSize: limit,
	})
	if err != nil {
		return KeyBlockBatch{}, err
	}

	keyBlocks, ok := resp.(KeyBlocks)
	if !ok {
		return KeyBlockBatch{}, fmt.Errorf("unexpected getNextKeyBlockIds response %T", resp)
	}
	if keyBlocks.Error {
		return KeyBlockBatch{Incomplete: keyBlocks.Incomplete}, fmt.Errorf("fullnode returned key block lookup error")
	}

	return KeyBlockBatch{
		Blocks:     keyBlocks.Blocks,
		Incomplete: keyBlocks.Incomplete,
	}, nil
}

func (s *overlaySubscription) queryKeyBlocks(ctx context.Context, req tl.Serializable) (tl.Serializable, error) {
	peers := s.hedgedQueryCandidates(0, 0, keyBlockLookupPeerLimit)
	if len(peers) == 0 {
		return nil, errors.New("overlay has no connected peers")
	}

	queryCtx, cancel := context.WithTimeout(ctx, keyBlockLookupRoundTimeout)
	defer cancel()

	return queryKeyBlocksFromPeers(queryCtx, peers, keyBlockLookupParallelism, keyBlockLookupHedgeDelay, func(ctx context.Context, peer *overlayPeer) (tl.Serializable, error) {
		return s.queryFromPeerWithLimits(ctx, peer, req, keyBlockLookupTimeout, maxKeyBlockLookupAnswerSize)
	}, func(peer *overlayPeer) {
		s.markPeerQueryFailed(peer)
	})
}

func queryKeyBlocksFromPeers(ctx context.Context, peers []*overlayPeer, parallelism int, hedgeDelay time.Duration, query func(context.Context, *overlayPeer) (tl.Serializable, error), semanticFailure func(*overlayPeer)) (tl.Serializable, error) {
	if len(peers) == 0 {
		return nil, errors.New("overlay has no connected peers")
	}

	var (
		emptyMu    sync.Mutex
		emptyResp  tl.Serializable
		emptyPeers []*overlayPeer
	)

	results, errs := runPeerRequests(ctx, peers, peerRequestOptions{
		parallelism: parallelism,
		hedgeDelay:  hedgeDelay,
	}, func(ctx context.Context, peer *overlayPeer) (tl.Serializable, error) {
		resp, err := query(ctx, peer)
		if err != nil {
			return nil, err
		}

		if keyBlocks, ok := resp.(KeyBlocks); ok {
			if keyBlocks.Error {
				if semanticFailure != nil {
					semanticFailure(peer)
				}
				return nil, fmt.Errorf("key block lookup error")
			}
			if len(keyBlocks.Blocks) == 0 {
				emptyMu.Lock()
				if emptyResp == nil {
					emptyResp = resp
				}
				emptyPeers = append(emptyPeers, peer)
				emptyMu.Unlock()
				return nil, emptyKeyBlockResponse{}
			}
		}

		return resp, nil
	})
	if len(results) > 0 {
		emptyMu.Lock()
		if semanticFailure != nil {
			for _, peer := range emptyPeers {
				semanticFailure(peer)
			}
		}
		emptyMu.Unlock()

		return results[0].value, nil
	}
	emptyMu.Lock()
	resp := emptyResp
	emptyMu.Unlock()
	if resp != nil {
		return resp, nil
	}
	return nil, errors.Join(errs...)
}
