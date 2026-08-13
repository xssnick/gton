package p2p

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

const (
	zeroStatePeerDiscoveryDelay = dhtSeedCooldownMinDelay + dhtSeedCooldownJitter + time.Second
)

func (a *ArchiveSession) DownloadZeroState(ctx context.Context, block ton.BlockIDExt) (storage.DownloadedState, error) {
	ctx, finish, err := a.beginOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	if a.node.IsOffline() {
		return nil, ErrOffline
	}
	// Zero state is a bulk historical download served from the archive pool, so
	// it uses the historical selector like archive packages do. The overlay
	// chosen is the same either way - zero state always carries topShard, which
	// historicalOverlayBlockForDownload leaves untouched - but this keeps it off
	// FastSync, whose subscriptions have no DHT presence for the pool to work
	// with.
	sub, err := a.node.querySubscriptionForHistoricalBlock(block)
	if err != nil {
		return nil, err
	}

	shard := archiveShardFromBlock(block)
	pool, releasePool, err := a.useArchivePeerPool(sub)
	if err != nil {
		return nil, err
	}
	defer releasePool()
	_, releaseDemand, err := pool.beginZeroStateRequest(shard, block)
	if err != nil {
		return nil, err
	}
	defer releaseDemand()
	tried := map[PeerID]struct{}{}
	var errs []error

	for round := 0; round < archiveSessionDownloadRounds; round++ {
		if err = sub.ensureZeroStateArchivePeers(ctx, pool, shard); err != nil {
			if len(errs) > 0 {
				return nil, errors.Join(append(errs, err)...)
			}
			return nil, err
		}

		peers := archiveDownloadRoundPeers(zeroStateArchiveCandidates(pool, a, shard, tried))
		if len(peers) == 0 {
			pool.refill(ctx, true)
			if waitErr := sub.waitForZeroStateArchivePeer(ctx, pool, a, shard, tried); waitErr != nil {
				if len(errs) > 0 {
					return nil, errors.Join(append(errs, waitErr)...)
				}
				return nil, waitErr
			}
			continue
		}

		a.node.log.Info().
			Str("block", storage.FormatBlockRef(block)).
			Int("peers", len(peers)).
			Int("round", round+1).
			Msg("requesting zero state from archive peers")

		notAvailable := 0
		for _, peer := range peers {
			a.node.log.Info().
				Str("peer", peer.addr).
				Str("block", storage.FormatBlockRef(block)).
				Msg("requesting zero state from archive peer")

			artifact, err := a.downloadZeroStateFromPeer(ctx, sub, pool, shard, peer, block)
			if err == nil {
				if zero, ok := artifact.(*zeroStateSnapshotArtifact); ok && a.node.stateArtifacts != nil {
					zero.writer = a.node.stateArtifacts
				}
				return artifact, nil
			}
			if errors.Is(err, context.Canceled) {
				return nil, err
			}
			if errors.Is(err, ErrStateNotAvailable) {
				notAvailable++
				a.clearSelectedArchivePeerID(shard, downloadPeerID(peer))
			} else {
				a.noteZeroStatePeerError(ctx, pool, shard, peer, err)
			}
			if !peer.id.IsZero() {
				tried[peer.id] = struct{}{}
			}
			errs = append(errs, archiveDownloadError(peer, err))
		}

		if notAvailable == len(peers) {
			pool.refill(ctx, true)
		} else {
			pool.refill(ctx, false)
		}
	}

	if len(errs) == 0 {
		return nil, ErrStateNotAvailable
	}
	return nil, errors.Join(errs...)
}

func (s *overlaySubscription) ensureZeroStateArchivePeers(ctx context.Context, pool *archivePeerPool, shard archive.ShardID) error {
	if pool.ready(shard) {
		return nil
	}

	return s.waitForZeroStateArchivePeer(ctx, pool, nil, shard, nil)
}

func (s *overlaySubscription) waitForZeroStateArchivePeer(ctx context.Context, pool *archivePeerPool, session *ArchiveSession, shard archive.ShardID, tried map[PeerID]struct{}) error {
	discoveryDone := pool.refill(ctx, true)

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	timer := time.NewTimer(zeroStatePeerDiscoveryDelay)
	defer timer.Stop()
	timerC := timer.C

	for {
		if len(zeroStateArchiveCandidates(pool, session, shard, tried)) > 0 {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if done := pool.refill(ctx, true); done != nil {
				discoveryDone = done
			}
		case <-discoveryDone:
			discoveryDone = nil
			if len(zeroStateArchiveCandidates(pool, session, shard, tried)) > 0 {
				return nil
			}
			if timerC == nil {
				return ErrStateNotAvailable
			}
		case <-timerC:
			if len(zeroStateArchiveCandidates(pool, session, shard, tried)) > 0 {
				return nil
			}
			if discoveryDone == nil {
				return ErrStateNotAvailable
			}
			timerC = nil
		}
	}
}

func zeroStateArchiveCandidates(pool *archivePeerPool, session *ArchiveSession, shard archive.ShardID, tried map[PeerID]struct{}) []*overlayPeer {
	peers := pool.downloadCandidates(session, shard, pool.candidates(shard))
	if len(peers) == 0 || len(tried) == 0 {
		return peers
	}

	filtered := peers[:0]
	for _, peer := range peers {
		if _, ok := tried[peer.id]; ok {
			continue
		}
		filtered = append(filtered, peer)
	}
	return filtered
}

func (a *ArchiveSession) downloadZeroStateFromPeer(ctx context.Context, sub *overlaySubscription, pool *archivePeerPool, shard archive.ShardID, peer *overlayPeer, block ton.BlockIDExt) (downloaded storage.DownloadedState, err error) {
	archiveRelease, ok := pool.acquire(peer)
	if !ok {
		return nil, fmt.Errorf("archive peer left the pool: %w", ErrStateNotAvailable)
	}
	defer archiveRelease()

	query := sub.beginPeerQueryOperation(peer)
	defer func() {
		query.finish(err)
	}()

	resp, err := sub.queryArchiveFromPeerWithLimits(ctx, peer, PrepareZeroState{Block: block}, archiveInfoTimeout, persistentStateSmallAnswerMax)
	if err != nil {
		return nil, err
	}
	switch resp.(type) {
	case PreparedState:
	case NotFoundState:
		pool.recordZeroStateDemandNotAvailable(block, peer)
		return nil, ErrStateNotAvailable
	default:
		return nil, fmt.Errorf("unexpected prepareZeroState response %T", resp)
	}

	pool.recordZeroStateDemandEvidence(block, peer, archivePeerDemandAvailable)

	started := time.Now()
	data, err := sub.queryArchiveRawFromPeerWithLimits(ctx, peer, DownloadZeroState{Block: block}, archiveSliceTimeout, maxBlockDownloadAnswerSize)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("state boc is empty")
	}

	elapsed := time.Since(started)
	pool.noteArchiveDownload(shard, peer, int64(len(data)), elapsed)
	pool.markSuccess(shard, peer)
	pool.recordZeroStateDemandEvidence(block, peer, archivePeerDemandProven)
	a.selectArchivePeerFromPool(shard, peer, pool)

	return &zeroStateSnapshotArtifact{
		block: block,
		data:  data,
	}, nil
}

func (a *ArchiveSession) noteZeroStatePeerError(ctx context.Context, pool *archivePeerPool, shard archive.ShardID, peer *overlayPeer, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	pool.rememberTransportBlocked(downloadPeerID(peer))
	a.rejectArchivePeer(ctx, pool, shard, peer, archivePeerRejectStateDownloadFailed)
}

func archiveShardFromBlock(block ton.BlockIDExt) archive.ShardID {
	return archive.ShardID{Workchain: block.Workchain, Shard: block.Shard}
}
