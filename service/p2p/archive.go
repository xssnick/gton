package p2p

import (
	"context"
	"errors"
	"flexserver/service/archive"
	"fmt"
	"os"
	"time"

	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

const (
	archiveSliceSize       = 2 << 20
	archiveSliceMaxAnswer  = archiveSliceSize + 4096
	archiveInfoTimeout     = 3 * time.Second
	archiveSliceTimeout    = 15 * time.Second
	archiveInfoParallelism = 6
	archiveInfoHedgeDelay  = 250 * time.Millisecond

	archiveStickyProbeInterval = 30 * time.Second
	archiveStickySwitchRatio   = 1.15
	defaultArchivePeerSpeed    = float64(1 << 20)
	archiveSlowPeerSpeed       = float64(1 << 20)
	basechainSlowPeerSpeed     = float64(3 << 20)
	archiveSlowPeerPenalty     = 3 * time.Minute
	archiveUnknownPeerSpeed    = float64(256 << 10)
)

type archiveInfoResult struct {
	peer *overlayPeer
	info ArchiveInfo
	err  error
}

type resolvedArchive struct {
	MasterchainSeqno uint32
	Shard            archive.ShardID
	ArchiveID        int64
	Peer             string

	peer  *overlayPeer
	peers []*overlayPeer
}

func (n *Node) DownloadArchive(ctx context.Context, masterchainSeqno uint32, shard archive.ShardID, tmpDir string) (*archive.Downloaded, error) {
	sub, err := n.subscriptionForBlock(ton.BlockIDExt{Workchain: shard.Workchain, Shard: shard.Shard})
	if err != nil {
		return nil, err
	}
	if err = sub.ensurePeers(ctx); err != nil {
		return nil, fmt.Errorf("bootstrap overlay peers: %w", err)
	}

	resolved, err := sub.resolveArchive(ctx, masterchainSeqno, shard)
	if err != nil {
		return nil, err
	}
	return sub.downloadArchiveFromResolved(ctx, resolved, tmpDir)
}

func (s *overlaySubscription) resolveArchive(ctx context.Context, masterchainSeqno uint32, shard archive.ShardID) (resolvedArchive, error) {
	peers := s.node.prioritizeArchivePeers(shard, s.queryCandidates(0, 0))
	if len(peers) == 0 {
		return resolvedArchive{}, errors.New("overlay has no connected peers")
	}

	if peer := s.currentArchivePeer(shard, peers); peer != nil {
		info, err := s.queryArchiveInfo(ctx, peer, masterchainSeqno, shard)
		if err == nil {
			return resolvedArchive{
				MasterchainSeqno: masterchainSeqno,
				Shard:            shard,
				ArchiveID:        info.ID,
				Peer:             peer.addr,
				peer:             peer,
				peers:            peers,
			}, nil
		}
		if errors.Is(err, context.Canceled) {
			return resolvedArchive{}, err
		}
		if !errors.Is(err, archive.ErrNotAvailable) {
			peer.archiveDownloadFailed()
			s.noteArchivePeerFailure(shard, peer)
		}
	}

	info, err := s.findArchiveInfo(ctx, peers, masterchainSeqno, shard)
	if err != nil {
		return resolvedArchive{}, err
	}

	return resolvedArchive{
		MasterchainSeqno: masterchainSeqno,
		Shard:            shard,
		ArchiveID:        info.info.ID,
		Peer:             info.peer.addr,
		peer:             info.peer,
		peers:            peers,
	}, nil
}

func (s *overlaySubscription) downloadArchiveFromResolved(ctx context.Context, resolved resolvedArchive, tmpDir string) (*archive.Downloaded, error) {
	peers := s.node.prioritizeArchivePeers(resolved.Shard, resolved.peers)
	if len(peers) == 0 {
		peers = s.node.prioritizeArchivePeers(resolved.Shard, s.queryCandidates(0, 0))
	}
	if len(peers) == 0 {
		return nil, errors.New("overlay has no connected peers")
	}

	ordered := make([]*overlayPeer, 0, len(peers)+2)
	seen := make(map[string]struct{}, len(peers)+2)
	addPeer := func(peer *overlayPeer) {
		if peer == nil {
			return
		}
		key := archivePeerKey(peer)
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		ordered = append(ordered, peer)
	}
	addPeer(s.chooseArchivePeer(resolved.Shard, peers))
	addPeer(resolved.peer)
	for _, peer := range peers {
		addPeer(peer)
	}

	return s.downloadArchiveFromPeers(ctx, resolved, ordered, tmpDir)
}

func (s *overlaySubscription) downloadArchiveFromPeers(ctx context.Context, resolved resolvedArchive, peers []*overlayPeer, tmpDir string) (*archive.Downloaded, error) {
	var errs []error
	usedPeers := make(map[string]struct{}, len(peers))

	for _, peer := range peers {
		key := archivePeerKey(peer)
		if _, ok := usedPeers[key]; ok {
			continue
		}
		usedPeers[key] = struct{}{}

		archiveID := resolved.ArchiveID
		if key != archivePeerKey(resolved.peer) {
			info, err := s.queryArchiveInfo(ctx, peer, resolved.MasterchainSeqno, resolved.Shard)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return nil, err
				}
				if !errors.Is(err, archive.ErrNotAvailable) {
					peer.archiveDownloadFailed()
					s.noteArchivePeerFailure(resolved.Shard, peer)
				}
				errs = append(errs, fmt.Errorf("%s: %w", peer.addr, err))
				continue
			}
			archiveID = info.ID
		}

		downloaded, err := s.downloadArchiveFromPeer(ctx, resolved, peer, archiveID, tmpDir)
		if err == nil {
			return downloaded, nil
		}
		if errors.Is(err, context.Canceled) {
			return nil, err
		}

		peer.archiveDownloadFailed()
		s.noteArchivePeerFailure(resolved.Shard, peer)
		errs = append(errs, fmt.Errorf("%s: %w", peer.addr, err))

		s.log.Debug().
			Err(err).
			Uint32("masterchain_seqno", resolved.MasterchainSeqno).
			Int32("workchain", resolved.Shard.Workchain).
			Str("shard", fmt.Sprintf("%016x", uint64(resolved.Shard.Shard))).
			Int64("archive_id", archiveID).
			Str("peer", peer.addr).
			Msg("archive download peer failed")
	}

	if len(errs) == 0 {
		return nil, archive.ErrNotAvailable
	}
	return nil, errors.Join(errs...)
}

func (s *overlaySubscription) downloadArchiveFromPeer(ctx context.Context, resolved resolvedArchive, peer *overlayPeer, archiveID int64, tmpDir string) (*archive.Downloaded, error) {
	file, err := os.CreateTemp(tmpDir, "flexserver-archive-*.pack")
	if err != nil {
		return nil, fmt.Errorf("create archive temp file: %w", err)
	}

	path := file.Name()
	cleanup := func() {
		_ = file.Close()
		_ = os.Remove(path)
	}

	release := s.node.acquireArchivePeer(peer)
	defer release()

	startedAt := time.Now()
	lastLog := startedAt
	var offset int64
	var lastLogBytes int64

	s.log.Debug().
		Uint32("masterchain_seqno", resolved.MasterchainSeqno).
		Int32("workchain", resolved.Shard.Workchain).
		Str("shard", fmt.Sprintf("%016x", uint64(resolved.Shard.Shard))).
		Int64("archive_id", archiveID).
		Str("peer", peer.addr).
		Msg("selected archive download peer")

	for {
		data, err := s.queryArchiveSlice(ctx, peer, archiveID, offset)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("download archive offset=%d: %w", offset, err)
		}
		if len(data) > archiveSliceSize {
			cleanup()
			return nil, fmt.Errorf("archive slice response too large: %d", len(data))
		}
		if len(data) > 0 {
			n, err := file.Write(data)
			if err != nil {
				cleanup()
				return nil, fmt.Errorf("write archive temp file: %w", err)
			}
			if n != len(data) {
				cleanup()
				return nil, fmt.Errorf("short archive temp file write: %d of %d", n, len(data))
			}
		}

		offset += int64(len(data))

		if elapsed := time.Since(lastLog); elapsed >= 10*time.Second {
			s.log.Debug().
				Uint32("masterchain_seqno", resolved.MasterchainSeqno).
				Int32("workchain", resolved.Shard.Workchain).
				Str("shard", fmt.Sprintf("%016x", uint64(resolved.Shard.Shard))).
				Int64("archive_id", archiveID).
				Int64("bytes", offset).
				Str("peer", peer.addr).
				Str("speed", formatByteRate(offset-lastLogBytes, elapsed)).
				Msg("archive slice download progress")

			lastLog = time.Now()
			lastLogBytes = offset
		}

		if len(data) < archiveSliceSize {
			break
		}
	}

	if err = file.Close(); err != nil {
		_ = os.Remove(path)
		return nil, fmt.Errorf("close archive temp file: %w", err)
	}

	elapsed := time.Since(startedAt)
	slowThreshold := archiveSlowThreshold(resolved.Shard.Workchain == 0)
	peer.archiveDownloadSuccess(offset, elapsed, slowThreshold)
	if elapsed > 0 && float64(offset)/elapsed.Seconds() < slowThreshold {
		s.noteArchivePeerFailure(resolved.Shard, peer)
		s.log.Debug().
			Uint32("masterchain_seqno", resolved.MasterchainSeqno).
			Int32("workchain", resolved.Shard.Workchain).
			Str("shard", fmt.Sprintf("%016x", uint64(resolved.Shard.Shard))).
			Int64("archive_id", archiveID).
			Str("peer", peer.addr).
			Int64("bytes", offset).
			Dur("elapsed", elapsed).
			Str("speed", formatByteRate(offset, elapsed)).
			Msg("archive download peer too slow")
	} else {
		s.noteArchivePeerSuccess(resolved.Shard, peer, offset, elapsed)
	}

	s.log.Info().
		Uint32("masterchain_seqno", resolved.MasterchainSeqno).
		Int32("workchain", resolved.Shard.Workchain).
		Str("shard", fmt.Sprintf("%016x", uint64(resolved.Shard.Shard))).
		Int64("archive_id", archiveID).
		Int64("bytes", offset).
		Dur("elapsed", elapsed).
		Str("speed", formatByteRate(offset, elapsed)).
		Str("peer", peer.addr).
		Msg("archive slice downloaded")

	return &archive.Downloaded{
		MasterchainSeqno: resolved.MasterchainSeqno,
		Shard:            resolved.Shard,
		ArchiveID:        archiveID,
		Peer:             peer.addr,
		Path:             path,
		Bytes:            offset,
		DownloadElapsed:  elapsed,
	}, nil
}

func (s *overlaySubscription) queryArchiveSlice(ctx context.Context, peer *overlayPeer, archiveID int64, offset int64) ([]byte, error) {
	query := GetArchiveSlice{
		ArchiveID: archiveID,
		Offset:    offset,
		MaxSize:   archiveSliceSize,
	}
	return s.queryRawFromPeerWithLimits(ctx, peer, query, archiveSliceTimeout, archiveSliceMaxAnswer)
}

func (s *overlaySubscription) findArchiveInfo(ctx context.Context, peers []*overlayPeer, masterchainSeqno uint32, shard archive.ShardID) (*archiveInfoResult, error) {
	parallelism := minInt(archiveInfoParallelism, len(peers))
	if parallelism <= 0 {
		return nil, errors.New("overlay has no connected peers")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan archiveInfoResult, len(peers))
	var (
		nextIdx  int
		inFlight int
	)

	launch := func(peer *overlayPeer) {
		inFlight++
		go func() {
			info, err := s.queryArchiveInfo(ctx, peer, masterchainSeqno, shard)
			res := archiveInfoResult{peer: peer, info: info, err: err}
			if err != nil {
				res.err = fmt.Errorf("%s: %w", peer.addr, err)
			}
			select {
			case results <- res:
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
		hedgeTimer = time.NewTimer(archiveInfoHedgeDelay)
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
				hedgeTimer.Reset(archiveInfoHedgeDelay)
			} else {
				hedgeTimer = nil
			}
		case res := <-results:
			inFlight--
			if res.err == nil {
				cancel()
				return &res, nil
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

func (s *overlaySubscription) queryArchiveInfo(ctx context.Context, peer *overlayPeer, masterchainSeqno uint32, shard archive.ShardID) (ArchiveInfo, error) {
	resp, err := s.queryFromPeerWithLimits(ctx, peer, archiveInfoQuery(masterchainSeqno, shard), archiveInfoTimeout, 1024)
	if err != nil {
		return ArchiveInfo{}, err
	}

	info, ok := resp.(ArchiveInfo)
	if ok {
		return info, nil
	}
	if _, notFound := resp.(ArchiveNotFound); notFound {
		return ArchiveInfo{}, archive.ErrNotAvailable
	}
	return ArchiveInfo{}, fmt.Errorf("unexpected archive info response %T", resp)
}

func archiveInfoQuery(masterchainSeqno uint32, shard archive.ShardID) tl.Serializable {
	if shard.IsMasterchain() {
		return GetArchiveInfo{MasterchainSeqno: int32(masterchainSeqno)}
	}
	return GetShardArchiveInfo{
		MasterchainSeqno: int32(masterchainSeqno),
		ShardPrefix:      shard,
	}
}
