package p2p

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

type ProofDownload struct {
	Data []byte
	Link bool
}

func (n *Node) DownloadBlockProof(ctx context.Context, block ton.BlockIDExt, allowPartial bool) (ProofDownload, error) {
	return n.downloadProof(ctx, block, allowPartial, false)
}

func (n *Node) DownloadKeyBlockProof(ctx context.Context, block ton.BlockIDExt, allowPartial bool) (ProofDownload, error) {
	return n.downloadProof(ctx, block, allowPartial, true)
}

func (n *Node) downloadProof(ctx context.Context, block ton.BlockIDExt, allowPartial bool, keyBlock bool) (ProofDownload, error) {
	sub, err := n.subscriptionForBlock(block)
	if err != nil {
		return ProofDownload{}, err
	}
	if err = sub.ensurePeers(ctx); err != nil {
		return ProofDownload{}, fmt.Errorf("bootstrap overlay peers: %w", err)
	}
	sub.startSeedFromDHTTarget(ctx, bootstrapDiscoveryTarget)

	tried := map[string]struct{}{}
	var errs []error
	for wave := 0; wave < proofDownloadWaves; wave++ {
		peers := sub.proofQueryCandidates(tried)
		if len(peers) == 0 {
			if err = sub.waitForProofPeerDiscovery(ctx); err != nil {
				if len(errs) > 0 {
					return ProofDownload{}, errors.Join(errs...)
				}
				return ProofDownload{}, err
			}
			continue
		}
		if len(peers) > proofDownloadPeerLimit {
			peers = peers[:proofDownloadPeerLimit]
		}
		for _, peer := range peers {
			tried[proofPeerKey(peer)] = struct{}{}
		}

		resp, err := runConcurrentOverlayQueries(ctx, peers, proofDownloadParallelism, proofDownloadHedgeDelay, func(ctx context.Context, peer *overlayPeer) (tl.Serializable, error) {
			return sub.downloadProofFromPeer(ctx, peer, block, allowPartial, keyBlock)
		})
		if err != nil {
			errs = append(errs, err)
			if wave+1 < proofDownloadWaves {
				sub.reloadNeighbours()
				sub.startSeedFromDHTTarget(ctx, bootstrapDiscoveryTarget)
				if waitErr := sub.waitForProofPeerDiscovery(ctx); waitErr != nil && !errors.Is(waitErr, context.Canceled) {
					errs = append(errs, waitErr)
				}
			}
			continue
		}

		downloaded, ok := resp.(ProofDownload)
		if !ok {
			return ProofDownload{}, fmt.Errorf("unexpected proof download response %T", resp)
		}
		return downloaded, nil
	}

	if len(errs) > 0 {
		return ProofDownload{}, errors.Join(errs...)
	}
	return ProofDownload{}, errors.New("overlay has no proof download peers")
}

func (s *overlaySubscription) proofQueryCandidates(tried map[string]struct{}) []*overlayPeer {
	peers := s.queryCandidates(0, 0)
	if len(peers) == 0 {
		s.reloadNeighbours()
		peers = s.queryCandidates(0, 0)
	}
	if len(peers) == 0 || len(tried) == 0 {
		return peers
	}

	filtered := peers[:0]
	for _, peer := range peers {
		if _, ok := tried[proofPeerKey(peer)]; ok {
			continue
		}
		filtered = append(filtered, peer)
	}
	return filtered
}

func (s *overlaySubscription) waitForProofPeerDiscovery(ctx context.Context) error {
	notify := s.peerNotifySnapshot()
	s.startSeedFromDHTTarget(ctx, bootstrapDiscoveryTarget)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-notify:
		return nil
	case <-time.After(proofPeerDiscoveryDelay):
		return nil
	}
}

func proofPeerKey(peer *overlayPeer) string {
	if peer.id != "" {
		return peer.id
	}
	return peer.addr
}

func (s *overlaySubscription) downloadProofFromPeer(ctx context.Context, peer *overlayPeer, block ton.BlockIDExt, allowPartial bool, keyBlock bool) (ProofDownload, error) {
	prepare := tl.Serializable(PrepareBlockProof{Block: block, AllowPartial: allowPartial})
	if keyBlock {
		prepare = PrepareKeyBlockProof{Block: block, AllowPartial: allowPartial}
	}

	resp, err := s.queryFromPeerWithLimits(ctx, peer, prepare, proofPrepareTimeout, maxKeyBlockLookupAnswerSize)
	if err != nil {
		return ProofDownload{}, err
	}

	isLink := false
	switch resp.(type) {
	case PreparedProof:
	case PreparedProofLink:
		if !allowPartial {
			return ProofDownload{}, ErrBlockNotAvailable
		}
		isLink = true
	case PreparedProofEmpty:
		return ProofDownload{}, ErrBlockNotAvailable
	default:
		return ProofDownload{}, fmt.Errorf("unexpected prepare proof response %T", resp)
	}

	var data []byte
	switch {
	case keyBlock && isLink:
		data, err = s.queryRawFromPeerWithLimits(ctx, peer, DownloadKeyBlockProofLink{Block: block}, proofDownloadTimeout, maxBlockDownloadAnswerSize)
	case keyBlock:
		data, err = s.queryRawFromPeerWithLimits(ctx, peer, DownloadKeyBlockProof{Block: block}, proofDownloadTimeout, maxBlockDownloadAnswerSize)
	case isLink:
		data, err = s.queryRawFromPeerWithLimits(ctx, peer, DownloadBlockProofLink{Block: block}, proofDownloadTimeout, maxBlockDownloadAnswerSize)
	default:
		data, err = s.queryRawFromPeerWithLimits(ctx, peer, DownloadBlockProof{Block: block}, proofDownloadTimeout, maxBlockDownloadAnswerSize)
	}
	if err != nil {
		return ProofDownload{}, err
	}

	if len(data) == 0 {
		return ProofDownload{}, ErrBlockNotAvailable
	}
	return ProofDownload{
		Data: data,
		Link: isLink,
	}, nil
}
