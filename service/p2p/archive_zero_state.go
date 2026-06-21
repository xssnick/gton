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
	if a == nil || a.node == nil {
		return nil, errors.New("archive session is not initialized")
	}

	sub, err := a.node.subscriptionForBlock(block)
	if err != nil {
		return nil, err
	}

	shard := archiveShardFromBlock(block)
	pool := a.archivePeerPool(sub)
	tried := map[PeerID]struct{}{}
	var errs []error

	for round := 0; round < archiveSessionDownloadRounds; round++ {
		if err = sub.ensureZeroStateArchivePeers(ctx, pool, shard); err != nil {
			if len(errs) > 0 {
				return nil, errors.Join(append(errs, err)...)
			}
			return nil, err
		}

		peers := zeroStateArchiveCandidates(pool, a, shard, tried)
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
			Str("block", formatBlockRef(block)).
			Int("peers", len(peers)).
			Int("round", round+1).
			Msg("requesting zero state from archive peers")

		notAvailable := 0
		for _, peer := range peers {
			a.node.log.Info().
				Str("peer", peer.addr).
				Str("block", formatBlockRef(block)).
				Msg("requesting zero state from archive peer")

			artifact, err := a.downloadZeroStateFromPeer(ctx, sub, pool, shard, peer, block)
			if err == nil {
				if zero, ok := artifact.(*zeroStateSnapshotArtifact); ok {
					zero.writer, _ = a.node.peerStorage.(storage.PeerServingStorageWriter)
				}
				return artifact, nil
			}
			if errors.Is(err, context.Canceled) {
				return nil, err
			}
			retryable := false
			if errors.Is(err, ErrStateNotAvailable) {
				notAvailable++
				a.rejectArchivePeer(ctx, pool, shard, peer, archivePeerRejectStateNotAvailable)
			} else {
				retryable = a.noteZeroStatePeerError(ctx, pool, shard, peer, err)
			}
			if !retryable && !peer.id.IsZero() {
				tried[peer.id] = struct{}{}
			}
			errs = append(errs, archiveDownloadError(peer, err))
		}

		if notAvailable == len(peers) {
			pool.refreshUseless(ctx, shard)
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
	if err := s.ensurePeers(ctx); err != nil {
		return fmt.Errorf("bootstrap overlay peers: %w", err)
	}
	if pool.ready(shard) {
		return nil
	}

	pool.refill(ctx, true)
	return s.waitForZeroStateArchivePeer(ctx, pool, nil, shard, nil)
}

func (s *overlaySubscription) waitForZeroStateArchivePeer(ctx context.Context, pool *archivePeerPool, session *ArchiveSession, shard archive.ShardID, tried map[PeerID]struct{}) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	timer := time.NewTimer(zeroStatePeerDiscoveryDelay)
	defer timer.Stop()

	for {
		if len(zeroStateArchiveCandidates(pool, session, shard, tried)) > 0 {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			pool.refill(ctx, true)
		case <-timer.C:
			if len(zeroStateArchiveCandidates(pool, session, shard, tried)) > 0 {
				return nil
			}
			return ErrStateNotAvailable
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

func (a *ArchiveSession) downloadZeroStateFromPeer(ctx context.Context, sub *overlaySubscription, pool *archivePeerPool, shard archive.ShardID, peer *overlayPeer, block ton.BlockIDExt) (storage.DownloadedState, error) {
	archiveRelease := pool.acquire(peer)
	defer archiveRelease()
	release := a.node.acquireDownloadPeer(peer)
	defer release()

	resp, err := sub.queryFromPeerWithLimits(ctx, peer, PrepareZeroState{Block: block}, a.archivePeerInfoTimeout(peer), persistentStateSmallAnswerMax)
	if err != nil {
		return nil, err
	}
	switch resp.(type) {
	case PreparedState:
	case NotFoundState:
		return nil, ErrStateNotAvailable
	default:
		return nil, fmt.Errorf("unexpected prepareZeroState response %T", resp)
	}

	pool.markAvailable(shard, peer)
	a.noteArchivePeerAvailable(peer)

	started := time.Now()
	data, err := sub.queryRawFromPeerWithLimits(ctx, peer, DownloadZeroState{Block: block}, a.archivePeerSliceTimeout(peer), maxBlockDownloadAnswerSize)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("state boc is empty")
	}

	elapsed := time.Since(started)
	if noteArchivePeerDownload(shard, peer, int64(len(data)), elapsed) {
		a.node.log.Debug().
			Str("peer", peer.addr).
			Str("block", formatBlockRef(block)).
			Str("size", formatByteSize(int64(len(data)))).
			Dur("elapsed", elapsed).
			Msg("zero state download peer too slow")
	}
	pool.markSuccess(shard, peer)
	a.noteArchivePeerSuccess(peer)
	a.selectArchivePeer(shard, peer)

	return &zeroStateSnapshotArtifact{
		block: block,
		data:  data,
	}, nil
}

// noteZeroStatePeerError returns true when deadline grace keeps the peer eligible for another attempt.
func (a *ArchiveSession) noteZeroStatePeerError(ctx context.Context, pool *archivePeerPool, shard archive.ShardID, peer *overlayPeer, err error) bool {
	if a.archivePeerDeadlineGrace(peer, err) {
		return true
	}

	peer.downloadFailed(archiveSlowPeerPenalty)
	a.rejectArchivePeer(ctx, pool, shard, peer, archivePeerRejectStateDownloadFailed)
	return false
}

func archiveShardFromBlock(block ton.BlockIDExt) archive.ShardID {
	return archive.ShardID{Workchain: block.Workchain, Shard: block.Shard}
}
