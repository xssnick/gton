package p2p

import (
	"context"
	"errors"
	"flexserver/service/archive"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

const (
	archiveSliceSize         = 2 << 20
	archiveSliceMaxAnswer    = archiveSliceSize + 4096
	archiveInfoTimeout       = 3 * time.Second
	archiveSliceProbeTimeout = 3 * time.Second
	archiveSliceTimeout      = 15 * time.Second
	archiveInfoParallelism   = 6
	archiveInfoHedgeDelay    = 250 * time.Millisecond

	archiveDownloadProbeParallelism   = 4
	archiveDownloadProbeCollectWindow = 750 * time.Millisecond

	archiveStickyProbeInterval = 30 * time.Second
	archiveStickySwitchRatio   = 1.15
	defaultArchivePeerSpeed    = float64(1 << 20)
	archiveSlowPeerSpeed       = float64(1 << 20)
	basechainSlowPeerSpeed     = float64(3 << 20)
	archiveGoodPeerSpeed       = float64(8 << 20)
	basechainGoodPeerSpeed     = float64(8 << 20)
	archiveSlowPeerPenalty     = 3 * time.Minute
	archiveUnknownPeerSpeed    = float64(256 << 10)
)

type archiveInfoResult struct {
	peer *overlayPeer
	info ArchiveInfo
	err  error
}

type archiveDownloadProbeResult struct {
	peer    *overlayPeer
	info    ArchiveInfo
	bytes   int64
	elapsed time.Duration
	err     error
}

type archiveStreamImportResult struct {
	imported *archive.Imported
	err      error
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
	peers := s.node.prioritizeArchivePeers(shard, s.archiveQueryCandidates())
	if len(peers) == 0 {
		return resolvedArchive{}, errors.New("overlay has no archive neighbours")
	}
	peers = s.availableArchivePeers(shard, peers)
	if len(peers) == 0 {
		return resolvedArchive{}, archive.ErrNotAvailable
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
		if errors.Is(err, archive.ErrNotAvailable) {
			s.denyArchivePeer(shard, peer, "archive_not_available")
		} else {
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
	peers = s.availableArchivePeers(resolved.Shard, peers)
	if len(peers) == 0 {
		candidates := s.node.prioritizeArchivePeers(resolved.Shard, s.archiveQueryCandidates())
		if len(candidates) == 0 {
			return nil, errors.New("overlay has no archive neighbours")
		}
		peers = s.availableArchivePeers(resolved.Shard, candidates)
	}
	if len(peers) == 0 {
		return nil, archive.ErrNotAvailable
	}

	ordered := make([]*overlayPeer, 0, len(peers)+2)
	seen := make(map[string]struct{}, len(peers)+2)
	addPeer := func(peer *overlayPeer) {
		if peer == nil {
			return
		}
		if s.archivePeerDenied(resolved.Shard, peer) {
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

	archiveIDs := map[string]int64{}
	if resolved.peer != nil {
		archiveIDs[archivePeerKey(resolved.peer)] = resolved.ArchiveID
	}
	if shouldRaceArchiveDownload(resolved.Shard, ordered) {
		ordered, archiveIDs = s.probeArchiveDownloadPeers(ctx, resolved, ordered, archiveIDs)
	}

	return s.downloadArchiveFromPeers(ctx, resolved, ordered, tmpDir, archiveIDs)
}

func (s *overlaySubscription) downloadArchiveFromPeers(ctx context.Context, resolved resolvedArchive, peers []*overlayPeer, tmpDir string, archiveIDs map[string]int64) (*archive.Downloaded, error) {
	var errs []error
	usedPeers := make(map[string]struct{}, len(peers))

	for _, peer := range peers {
		key := archivePeerKey(peer)
		if _, ok := usedPeers[key]; ok {
			continue
		}
		usedPeers[key] = struct{}{}

		archiveID, hasArchiveID := archiveIDs[key]
		if !hasArchiveID {
			info, err := s.queryArchiveInfoForDownload(ctx, peer, resolved.MasterchainSeqno, resolved.Shard)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return nil, err
				}
				if errors.Is(err, archive.ErrNotAvailable) {
					s.denyArchivePeer(resolved.Shard, peer, "archive_not_available")
				} else {
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

func (s *overlaySubscription) probeArchiveDownloadPeers(ctx context.Context, resolved resolvedArchive, peers []*overlayPeer, archiveIDs map[string]int64) ([]*overlayPeer, map[string]int64) {
	probeCount := minInt(archiveDownloadProbeParallelism, len(peers))
	if probeCount < 2 {
		return peers, archiveIDs
	}

	probeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan archiveDownloadProbeResult, probeCount)
	for _, peer := range peers[:probeCount] {
		go func(peer *overlayPeer) {
			info, bytes, elapsed, err := s.probeArchiveInfoForDownload(probeCtx, peer, resolved.MasterchainSeqno, resolved.Shard)
			res := archiveDownloadProbeResult{
				peer:    peer,
				info:    info,
				bytes:   bytes,
				elapsed: elapsed,
				err:     err,
			}
			select {
			case results <- res:
			case <-probeCtx.Done():
			}
		}(peer)
	}

	successes := make([]archiveDownloadProbeResult, 0, probeCount)
	inFlight := probeCount
	var collectTimer *time.Timer
	var collectC <-chan time.Time

	for inFlight > 0 {
		select {
		case <-ctx.Done():
			return peers, archiveIDs
		case <-collectC:
			return s.orderArchiveProbePeers(peers, successes, archiveIDs)
		case res := <-results:
			inFlight--
			if res.err != nil {
				s.noteArchiveDownloadProbeFailure(resolved.Shard, res.peer, res.err)
				continue
			}

			successes = append(successes, res)
			archiveIDs[archivePeerKey(res.peer)] = res.info.ID
			if collectTimer == nil {
				collectTimer = time.NewTimer(archiveDownloadProbeCollectWindow)
				defer collectTimer.Stop()
				collectC = collectTimer.C
			}
		}
	}

	return s.orderArchiveProbePeers(peers, successes, archiveIDs)
}

func (s *overlaySubscription) noteArchiveDownloadProbeFailure(shard archive.ShardID, peer *overlayPeer, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	if errors.Is(err, archive.ErrNotAvailable) {
		s.denyArchivePeer(shard, peer, "archive_not_available")
		return
	}

	peer.archiveDownloadFailed()
	s.noteArchivePeerFailure(shard, peer)
}

func (s *overlaySubscription) orderArchiveProbePeers(peers []*overlayPeer, successes []archiveDownloadProbeResult, archiveIDs map[string]int64) ([]*overlayPeer, map[string]int64) {
	if len(successes) == 0 {
		return peers, archiveIDs
	}

	sort.SliceStable(successes, func(i, j int) bool {
		return archiveProbeSpeed(successes[i]) > archiveProbeSpeed(successes[j])
	})

	ordered := make([]*overlayPeer, 0, len(peers))
	seen := make(map[string]struct{}, len(peers))
	addPeer := func(peer *overlayPeer) {
		key := archivePeerKey(peer)
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		ordered = append(ordered, peer)
	}
	for _, res := range successes {
		addPeer(res.peer)
	}
	for _, peer := range peers {
		addPeer(peer)
	}

	best := successes[0]
	s.log.Debug().
		Int64("archive_id", best.info.ID).
		Int("probes", len(successes)).
		Str("peer", best.peer.addr).
		Str("speed", formatByteRate(best.bytes, best.elapsed)).
		Msg("selected archive download peer from probes")

	return ordered, archiveIDs
}

func archiveProbeSpeed(res archiveDownloadProbeResult) float64 {
	if res.bytes <= 0 || res.elapsed <= 0 {
		return 0
	}
	return float64(res.bytes) / res.elapsed.Seconds()
}

func (s *overlaySubscription) downloadArchiveFromPeer(ctx context.Context, resolved resolvedArchive, peer *overlayPeer, archiveID int64, tmpDir string) (*archive.Downloaded, error) {
	file, err := os.CreateTemp(tmpDir, "flexserver-archive-*.pack.part")
	if err != nil {
		return nil, fmt.Errorf("create archive temp file: %w", err)
	}

	partPath := file.Name()
	path := strings.TrimSuffix(partPath, ".part")
	cleanup := func() {
		_ = file.Close()
		_ = os.Remove(partPath)
		_ = os.Remove(path)
	}

	release := s.node.acquireArchivePeer(peer)
	defer release()

	startedAt := time.Now()
	lastLog := startedAt
	var offset int64
	var lastLogBytes int64
	importWriter, importDone := startArchiveStreamImport(ctx, archive.Downloaded{
		MasterchainSeqno: resolved.MasterchainSeqno,
		Shard:            resolved.Shard,
		ArchiveID:        archiveID,
		Peer:             peer.addr,
	})

	fail := func(err error) (*archive.Downloaded, error) {
		_ = importWriter.CloseWithError(err)
		<-importDone
		cleanup()
		return nil, err
	}

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
			return fail(fmt.Errorf("download archive offset=%d: %w", offset, err))
		}
		if len(data) > archiveSliceSize {
			return fail(fmt.Errorf("archive slice response too large: %d", len(data)))
		}
		if len(data) > 0 {
			n, err := file.Write(data)
			if err != nil {
				return fail(fmt.Errorf("write archive temp file: %w", err))
			}
			if n != len(data) {
				return fail(fmt.Errorf("short archive temp file write: %d of %d", n, len(data)))
			}
			if _, err = importWriter.Write(data); err != nil {
				return fail(fmt.Errorf("stream archive import: %w", err))
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

	if err = importWriter.Close(); err != nil {
		cleanup()
		return nil, fmt.Errorf("finish archive stream import: %w", err)
	}
	importResult := <-importDone
	if importResult.err != nil {
		cleanup()
		return nil, fmt.Errorf("stream archive import: %w", importResult.err)
	}

	if err = file.Close(); err != nil {
		_ = os.Remove(partPath)
		return nil, fmt.Errorf("close archive temp file: %w", err)
	}
	if err = os.Rename(partPath, path); err != nil {
		_ = os.Remove(partPath)
		return nil, fmt.Errorf("rename archive temp file: %w", err)
	}

	elapsed := time.Since(startedAt)
	if importResult.imported != nil && importResult.imported.Stats != nil {
		importResult.imported.Stats.Bytes = offset
		importResult.imported.Stats.DownloadElapsed = elapsed
	}
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
		Imported:         importResult.imported,
	}, nil
}

func startArchiveStreamImport(ctx context.Context, downloaded archive.Downloaded) (*io.PipeWriter, <-chan archiveStreamImportResult) {
	reader, writer := io.Pipe()
	done := make(chan archiveStreamImportResult, 1)
	go func() {
		defer func() { _ = reader.Close() }()

		imported, err := archive.ImportStream(ctx, &downloaded, reader)
		done <- archiveStreamImportResult{
			imported: imported,
			err:      err,
		}
	}()
	return writer, done
}

func (s *overlaySubscription) queryArchiveSlice(ctx context.Context, peer *overlayPeer, archiveID int64, offset int64) ([]byte, error) {
	return s.queryArchiveSliceWithTimeout(ctx, peer, archiveID, offset, archiveSliceTimeout)
}

func (s *overlaySubscription) queryArchiveSliceWithTimeout(ctx context.Context, peer *overlayPeer, archiveID int64, offset int64, timeout time.Duration) ([]byte, error) {
	query := GetArchiveSlice{
		ArchiveID: archiveID,
		Offset:    offset,
		MaxSize:   archiveSliceSize,
	}
	return s.queryRawFromPeerWithLimits(ctx, peer, query, timeout, archiveSliceMaxAnswer)
}

func (s *overlaySubscription) queryArchiveInfoForDownload(ctx context.Context, peer *overlayPeer, masterchainSeqno uint32, shard archive.ShardID) (ArchiveInfo, error) {
	info, _, _, err := s.probeArchiveInfoForDownload(ctx, peer, masterchainSeqno, shard)
	return info, err
}

func (s *overlaySubscription) probeArchiveInfoForDownload(ctx context.Context, peer *overlayPeer, masterchainSeqno uint32, shard archive.ShardID) (ArchiveInfo, int64, time.Duration, error) {
	info, err := s.queryArchiveInfo(ctx, peer, masterchainSeqno, shard)
	if err != nil {
		return ArchiveInfo{}, 0, 0, err
	}

	started := time.Now()
	data, err := s.queryArchiveSliceWithTimeout(ctx, peer, info.ID, 0, archiveSliceProbeTimeout)
	if err != nil {
		return ArchiveInfo{}, 0, 0, fmt.Errorf("probe archive offset=0: %w", err)
	}
	if len(data) > archiveSliceSize {
		return ArchiveInfo{}, 0, 0, fmt.Errorf("archive probe response too large: %d", len(data))
	}
	if len(data) == 0 {
		return info, 0, time.Since(started), nil
	}

	elapsed := time.Since(started)
	bytes := int64(len(data))
	peer.archiveDownloadSuccess(bytes, elapsed, archiveSlowThreshold(shard.Workchain == 0))
	s.noteArchivePeerSuccess(shard, peer, bytes, elapsed)

	return info, bytes, elapsed, nil
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
			info, err := s.queryArchiveInfoForDownload(ctx, peer, masterchainSeqno, shard)
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
			if errors.Is(res.err, archive.ErrNotAvailable) {
				s.denyArchivePeer(shard, res.peer, "archive_not_available")
			} else if !errors.Is(res.err, context.Canceled) {
				res.peer.archiveDownloadFailed()
				s.noteArchivePeerFailure(shard, res.peer)
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
