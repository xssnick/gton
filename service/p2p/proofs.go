package p2p

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/blockproof"
	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
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
	cached, err := n.cachedProofDownload(ctx, block, allowPartial, keyBlock)
	if err == nil {
		return cached, nil
	}
	if !errors.Is(err, tnstore.ErrNotFound) {
		n.log.Debug().
			Err(err).
			Str("block", formatBlockRef(block)).
			Bool("allow_partial", allowPartial).
			Bool("key_block", keyBlock).
			Msg("failed to load cached block proof")
	}

	sub, err := n.subscriptionForBlock(block)
	if err != nil {
		return ProofDownload{}, err
	}
	if err = sub.ensurePeers(ctx); err != nil {
		return ProofDownload{}, fmt.Errorf("bootstrap overlay peers: %w", err)
	}
	sub.startSeedFromDHTTarget(ctx, bootstrapDiscoveryTarget)

	tried := map[PeerID]struct{}{}
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
			peerID := proofPeerID(peer)
			if !peerID.IsZero() {
				tried[peerID] = struct{}{}
			}
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

func (n *Node) cachedProofDownload(ctx context.Context, block ton.BlockIDExt, allowPartial bool, keyBlock bool) (ProofDownload, error) {
	if keyBlock {
		return n.cachedKeyBlockProofDownload(ctx, block, allowPartial)
	}
	return n.cachedBlockProofDownload(ctx, block, allowPartial)
}

func (n *Node) cachedBlockProofDownload(ctx context.Context, block ton.BlockIDExt, allowPartial bool) (ProofDownload, error) {
	if isMasterchainBlock(block) {
		data, err := n.localBlockProof(ctx, tnstore.ServedProofBlock, block)
		if err == nil {
			return ProofDownload{Data: data}, nil
		}
		if !errors.Is(err, tnstore.ErrNotFound) {
			return ProofDownload{}, err
		}
	}

	if !allowPartial {
		return ProofDownload{}, tnstore.ErrNotFound
	}

	data, err := n.localBlockProof(ctx, tnstore.ServedProofBlockLink, block)
	if err != nil {
		return ProofDownload{}, err
	}
	return ProofDownload{Data: data, Link: true}, nil
}

func (n *Node) cachedKeyBlockProofDownload(ctx context.Context, block ton.BlockIDExt, allowPartial bool) (ProofDownload, error) {
	if allowPartial {
		data, err := n.localBlockProof(ctx, tnstore.ServedProofKeyBlockLink, block)
		if err == nil {
			return ProofDownload{Data: data, Link: true}, nil
		}
		if !errors.Is(err, tnstore.ErrNotFound) {
			return ProofDownload{}, err
		}
	}

	data, err := n.localBlockProof(ctx, tnstore.ServedProofKeyBlock, block)
	if err != nil {
		return ProofDownload{}, err
	}
	if !allowPartial {
		return ProofDownload{Data: data}, nil
	}

	link, err := blockproof.LinkBOC(block, data)
	if err != nil {
		return ProofDownload{}, err
	}
	return ProofDownload{Data: link, Link: true}, nil
}

func (s *overlaySubscription) proofQueryCandidates(tried map[PeerID]struct{}) []*overlayPeer {
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
		if _, ok := tried[proofPeerID(peer)]; ok {
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

func proofPeerID(peer *overlayPeer) PeerID {
	if peer == nil {
		return PeerID{}
	}
	return peer.id
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
	if err = validateDownloadedProof(block, data, isLink, keyBlock); err != nil {
		return ProofDownload{}, err
	}
	return ProofDownload{
		Data: data,
		Link: isLink,
	}, nil
}

func validateDownloadedProof(block ton.BlockIDExt, data []byte, isLink bool, keyBlock bool) error {
	root, err := cell.FromBOC(data)
	if err != nil {
		return fmt.Errorf("proof is not a valid BOC for %s: %w", formatBlockRef(block), err)
	}
	if err = blockproof.CheckProofShape(block, root, isLink); err != nil {
		return err
	}

	parsed, err := blockproof.ParseCell(block, root)
	if err != nil {
		return err
	}
	if keyBlock && !parsed.Block.BlockInfo.KeyBlock {
		return fmt.Errorf("proof for %s is not a key block", formatBlockRef(block))
	}
	return nil
}
