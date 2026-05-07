package p2p

import (
	"context"
	"errors"
	"flexserver/service/archive"
	"fmt"
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
	archivePackMaxBytes      = int64(2 << 30)
	archiveInfoTimeout       = 3 * time.Second
	archiveSliceProbeTimeout = 3 * time.Second
	archiveSliceTimeout      = 15 * time.Second
	archiveInfoParallelism   = 6
	archiveInfoHedgeDelay    = 250 * time.Millisecond

	archiveDiscoveryPeerTarget        = 8
	archiveDiscoveryWait              = 2500 * time.Millisecond
	archiveDownloadProbeParallelism   = 4
	archiveDownloadProbeCollectWindow = 750 * time.Millisecond
	archiveBootstrapSlowLog           = 2 * time.Second

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
	peer      *overlayPeer
	candidate archiveCandidate
}

type archiveDownloadProbe struct {
	candidate archiveCandidate
	bytes     int64
	elapsed   time.Duration
}

type archiveCandidate struct {
	peer        *overlayPeer
	archiveID   int64
	seedSlice   []byte
	seedElapsed time.Duration
	hasSeed     bool
	speed       float64
}

type resolvedArchive struct {
	MasterchainSeqno uint32
	Shard            archive.ShardID
	ArchiveID        int64
	Peer             string
	seedSlice        []byte
	seedElapsed      time.Duration
	hasSeed          bool

	peer  *overlayPeer
	peers []*overlayPeer
}

func (n *Node) DownloadArchive(ctx context.Context, masterchainSeqno uint32, shard archive.ShardID, tmpDir string) (*archive.Downloaded, error) {
	sub, err := n.subscriptionForBlock(ton.BlockIDExt{Workchain: shard.Workchain, Shard: shard.Shard})
	if err != nil {
		return nil, err
	}

	var ensureElapsed time.Duration
	var resolveElapsed time.Duration
	logBootstrapTiming := func(peer string, err error) {
		candidates := sub.archiveQueryCandidates()
		available := sub.availableArchivePeers(shard, sub.node.prioritizeArchivePeers(shard, candidates))

		event := sub.log.Debug()
		if err != nil || ensureElapsed >= archiveBootstrapSlowLog || resolveElapsed >= archiveBootstrapSlowLog {
			event = sub.log.Info()
		}
		event.
			Uint32("masterchain_seqno", masterchainSeqno).
			Int32("workchain", shard.Workchain).
			Str("shard", fmt.Sprintf("%016x", uint64(shard.Shard))).
			Int("known_peers", len(sub.knownPeersSnapshot())).
			Int("alive_peers", sub.aliveKnownPeerCount()).
			Int("neighbours", len(sub.neighbourPeerSnapshots())).
			Int("archive_candidates", len(candidates)).
			Int("archive_available", len(available)).
			Dur("ensure_elapsed", ensureElapsed).
			Dur("resolve_elapsed", resolveElapsed).
			Str("peer", peer).
			Err(err).
			Msg("archive peer bootstrap timing")
	}

	ensureStarted := time.Now()
	if err = sub.ensureArchivePeers(ctx, shard); err != nil {
		return nil, fmt.Errorf("bootstrap overlay peers: %w", err)
	}
	ensureElapsed = time.Since(ensureStarted)

	resolveStarted := time.Now()
	resolved, err := sub.resolveArchive(ctx, masterchainSeqno, shard)
	resolveElapsed = time.Since(resolveStarted)
	if err != nil {
		logBootstrapTiming("", err)
		return nil, err
	}
	logBootstrapTiming(resolved.Peer, nil)

	return sub.downloadArchiveFromResolved(ctx, resolved, tmpDir)
}

func (s *overlaySubscription) ensureArchivePeers(ctx context.Context, shard archive.ShardID) error {
	if err := s.ensurePeers(ctx); err != nil {
		return err
	}
	if s.node == nil || s.node.dht == nil || s.archivePeersReady(shard) {
		return nil
	}

	s.startSeedFromDHTTarget(ctx, bootstrapDiscoveryTarget)
	s.reloadNeighbours()
	if s.archivePeersReady(shard) {
		return nil
	}

	timer := time.NewTimer(archiveDiscoveryWait)
	defer timer.Stop()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		notify := s.peerNotifySnapshot()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notify:
			s.reloadNeighbours()
			if s.archivePeersReady(shard) {
				return nil
			}
		case <-ticker.C:
			s.reloadNeighbours()
			if s.archivePeersReady(shard) {
				return nil
			}
		case <-timer.C:
			return nil
		}
	}
}

func (s *overlaySubscription) archivePeersReady(shard archive.ShardID) bool {
	if s.node == nil {
		return false
	}

	peers := s.node.prioritizeArchivePeers(shard, s.archiveQueryCandidates())
	peers = s.availableArchivePeers(shard, peers)
	if len(peers) >= archiveDiscoveryPeerTarget {
		return true
	}
	if peer := s.currentArchivePeer(shard, peers); peer != nil {
		return shouldUseCurrentArchivePeerWithoutRace(shard, peer)
	}
	return false
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
		if len(peers) == 1 || shouldUseCurrentArchivePeerWithoutRace(shard, peer) {
			candidate, _, _, err := s.queryArchiveCandidateForDownload(ctx, peer, masterchainSeqno, shard)
			if err == nil {
				return resolvedArchive{
					MasterchainSeqno: masterchainSeqno,
					Shard:            shard,
					ArchiveID:        candidate.archiveID,
					Peer:             peer.addr,
					seedSlice:        append([]byte(nil), candidate.seedSlice...),
					seedElapsed:      candidate.seedElapsed,
					hasSeed:          candidate.hasSeed,
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
				peer.downloadFailed(archiveSlowPeerPenalty)
				s.noteArchivePeerFailure(shard, peer)
			}
		}
	}

	info, err := s.findArchiveInfo(ctx, peers, masterchainSeqno, shard)
	if err != nil {
		return resolvedArchive{}, err
	}

	return resolvedArchive{
		MasterchainSeqno: masterchainSeqno,
		Shard:            shard,
		ArchiveID:        info.candidate.archiveID,
		Peer:             info.peer.addr,
		seedSlice:        append([]byte(nil), info.candidate.seedSlice...),
		seedElapsed:      info.candidate.seedElapsed,
		hasSeed:          info.candidate.hasSeed,
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

	candidates := map[string]archiveCandidate{}
	if resolved.peer != nil {
		candidates[archivePeerKey(resolved.peer)] = archiveCandidate{
			peer:        resolved.peer,
			archiveID:   resolved.ArchiveID,
			seedSlice:   append([]byte(nil), resolved.seedSlice...),
			seedElapsed: resolved.seedElapsed,
			hasSeed:     resolved.hasSeed,
		}
	}
	if shouldRaceArchiveDownload(resolved.Shard, ordered) {
		ordered, candidates = s.probeArchiveDownloadPeers(ctx, resolved, ordered, candidates)
	}

	return s.downloadArchiveFromPeers(ctx, resolved, ordered, tmpDir, candidates)
}

func (s *overlaySubscription) downloadArchiveFromPeers(ctx context.Context, resolved resolvedArchive, peers []*overlayPeer, tmpDir string, candidates map[string]archiveCandidate) (*archive.Downloaded, error) {
	var errs []error
	usedPeers := make(map[string]struct{}, len(peers))

	for _, peer := range peers {
		key := archivePeerKey(peer)
		if _, ok := usedPeers[key]; ok {
			continue
		}
		usedPeers[key] = struct{}{}

		candidate, hasCandidate := candidates[key]
		if !hasCandidate {
			var err error
			candidate, _, _, err = s.queryArchiveCandidateForDownload(ctx, peer, resolved.MasterchainSeqno, resolved.Shard)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return nil, err
				}
				if errors.Is(err, archive.ErrNotAvailable) {
					s.denyArchivePeer(resolved.Shard, peer, "archive_not_available")
				} else {
					peer.downloadFailed(archiveSlowPeerPenalty)
					s.noteArchivePeerFailure(resolved.Shard, peer)
				}
				errs = append(errs, fmt.Errorf("%s: %w", peer.addr, err))
				continue
			}
			candidates[key] = candidate
		}

		downloaded, err := s.downloadArchiveFromPeer(ctx, resolved, candidate, tmpDir)
		if err == nil {
			return downloaded, nil
		}
		if errors.Is(err, context.Canceled) {
			return nil, err
		}

		peer.downloadFailed(archiveSlowPeerPenalty)
		s.noteArchivePeerFailure(resolved.Shard, peer)
		errs = append(errs, fmt.Errorf("%s: %w", peer.addr, err))

		s.log.Debug().
			Err(err).
			Uint32("masterchain_seqno", resolved.MasterchainSeqno).
			Int32("workchain", resolved.Shard.Workchain).
			Str("shard", fmt.Sprintf("%016x", uint64(resolved.Shard.Shard))).
			Int64("archive_id", candidate.archiveID).
			Str("peer", peer.addr).
			Msg("archive download peer failed")
	}

	if len(errs) == 0 {
		return nil, archive.ErrNotAvailable
	}
	return nil, errors.Join(errs...)
}

func (s *overlaySubscription) probeArchiveDownloadPeers(ctx context.Context, resolved resolvedArchive, peers []*overlayPeer, candidates map[string]archiveCandidate) ([]*overlayPeer, map[string]archiveCandidate) {
	probeCount := minInt(archiveDownloadProbeParallelism, len(peers))
	if probeCount < 2 {
		return peers, candidates
	}

	results, _ := runPeerRequests(ctx, peers[:probeCount], peerRequestOptions{
		parallelism:         probeCount,
		collectAfterSuccess: archiveDownloadProbeCollectWindow,
		onFailure: func(peer *overlayPeer, err error) {
			s.noteArchiveDownloadProbeFailure(resolved.Shard, peer, err)
		},
	}, func(probeCtx context.Context, peer *overlayPeer) (archiveDownloadProbe, error) {
		if candidate, ok := candidates[archivePeerKey(peer)]; ok && candidate.hasSeed {
			return archiveDownloadProbe{
				candidate: candidate,
				bytes:     int64(len(candidate.seedSlice)),
				elapsed:   candidate.seedElapsed,
			}, nil
		}

		candidate, bytes, elapsed, err := s.queryArchiveCandidateForDownload(probeCtx, peer, resolved.MasterchainSeqno, resolved.Shard)
		return archiveDownloadProbe{
			candidate: candidate,
			bytes:     bytes,
			elapsed:   elapsed,
		}, err
	})

	successes := make([]peerRequestResult[archiveDownloadProbe], 0, len(results))
	for _, res := range results {
		successes = append(successes, res)
		candidates[archivePeerKey(res.peer)] = res.value.candidate
	}

	return s.orderArchiveProbePeers(peers, successes, candidates)
}

func (s *overlaySubscription) noteArchiveDownloadProbeFailure(shard archive.ShardID, peer *overlayPeer, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	if errors.Is(err, archive.ErrNotAvailable) {
		s.denyArchivePeer(shard, peer, "archive_not_available")
		return
	}

	peer.downloadFailed(archiveSlowPeerPenalty)
	s.noteArchivePeerFailure(shard, peer)
}

func (s *overlaySubscription) orderArchiveProbePeers(peers []*overlayPeer, successes []peerRequestResult[archiveDownloadProbe], candidates map[string]archiveCandidate) ([]*overlayPeer, map[string]archiveCandidate) {
	if len(successes) == 0 {
		return peers, candidates
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
		Int64("archive_id", best.value.candidate.archiveID).
		Int("probes", len(successes)).
		Str("peer", best.peer.addr).
		Str("speed", formatArchiveProbeRate(best)).
		Msg("selected archive download peer from probes")

	return ordered, candidates
}

func archiveProbeSpeed(res peerRequestResult[archiveDownloadProbe]) float64 {
	return archiveDownloadSpeed(res.value.candidate, res.value.bytes, res.value.elapsed)
}

func archiveCandidateSpeed(res peerRequestResult[archiveCandidate]) float64 {
	return archiveDownloadSpeed(res.value, int64(len(res.value.seedSlice)), res.value.seedElapsed)
}

func archiveDownloadSpeed(candidate archiveCandidate, bytes int64, elapsed time.Duration) float64 {
	if candidate.speed > 0 {
		return candidate.speed
	}
	if bytes <= 0 || elapsed <= 0 {
		return 0
	}
	return float64(bytes) / elapsed.Seconds()
}

func formatArchiveProbeRate(res peerRequestResult[archiveDownloadProbe]) string {
	if res.value.elapsed > 0 {
		return formatByteRate(res.value.bytes, res.value.elapsed)
	}
	if res.value.candidate.speed > 0 {
		return formatByteRate(int64(res.value.candidate.speed), time.Second)
	}
	return formatByteRate(0, time.Second)
}

func (s *overlaySubscription) downloadArchiveFromPeer(ctx context.Context, resolved resolvedArchive, candidate archiveCandidate, tmpDir string) (*archive.Downloaded, error) {
	peer := candidate.peer
	archiveID := candidate.archiveID
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

	release := s.node.acquireDownloadPeer(peer)
	defer release()

	startedAt := time.Now()
	lastLog := startedAt
	var offset int64
	var lastLogBytes int64
	var downloadElapsed time.Duration
	var lastLogDownloadElapsed time.Duration

	fail := func(err error) (*archive.Downloaded, error) {
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

	writeSlice := func(data []byte) error {
		if len(data) == 0 {
			return nil
		}
		if sizeErr := checkArchivePackDownloadSize(offset, len(data)); sizeErr != nil {
			return sizeErr
		}
		n, err := file.Write(data)
		if err != nil {
			return fmt.Errorf("write archive temp file: %w", err)
		}
		if n != len(data) {
			return fmt.Errorf("short archive temp file write: %d of %d", n, len(data))
		}
		offset += int64(len(data))
		return nil
	}

	seedComplete := false
	if candidate.hasSeed {
		if len(candidate.seedSlice) > archiveSliceSize {
			return fail(fmt.Errorf("archive seed response too large: %d", len(candidate.seedSlice)))
		}
		downloadElapsed += candidate.seedElapsed
		if err = writeSlice(candidate.seedSlice); err != nil {
			return fail(err)
		}
		seedComplete = len(candidate.seedSlice) < archiveSliceSize
	}

	for {
		if seedComplete {
			break
		}
		queryStarted := time.Now()
		data, err := s.queryArchiveSlice(ctx, peer, archiveID, offset)
		downloadElapsed += time.Since(queryStarted)
		if err != nil {
			return fail(fmt.Errorf("download archive offset=%d: %w", offset, err))
		}
		if len(data) > archiveSliceSize {
			return fail(fmt.Errorf("archive slice response too large: %d", len(data)))
		}
		if err = writeSlice(data); err != nil {
			return fail(err)
		}

		if elapsed := time.Since(lastLog); elapsed >= 10*time.Second {
			logDownloadElapsed := downloadElapsed - lastLogDownloadElapsed
			s.log.Debug().
				Uint32("masterchain_seqno", resolved.MasterchainSeqno).
				Int32("workchain", resolved.Shard.Workchain).
				Str("shard", fmt.Sprintf("%016x", uint64(resolved.Shard.Shard))).
				Int64("archive_id", archiveID).
				Int64("bytes", offset).
				Str("peer", peer.addr).
				Dur("elapsed", logDownloadElapsed).
				Dur("wall_elapsed", elapsed).
				Str("speed", formatByteRate(offset-lastLogBytes, logDownloadElapsed)).
				Msg("archive slice download progress")

			lastLog = time.Now()
			lastLogBytes = offset
			lastLogDownloadElapsed = downloadElapsed
		}

		if len(data) < archiveSliceSize {
			break
		}
	}

	if err = file.Close(); err != nil {
		_ = os.Remove(partPath)
		return nil, fmt.Errorf("close archive temp file: %w", err)
	}
	if err = os.Rename(partPath, path); err != nil {
		_ = os.Remove(partPath)
		return nil, fmt.Errorf("rename archive temp file: %w", err)
	}

	wallElapsed := time.Since(startedAt)
	if candidate.hasSeed && candidate.seedElapsed > 0 {
		wallElapsed += candidate.seedElapsed
	}
	slowThreshold := archiveSlowThreshold(resolved.Shard.Workchain == 0)
	peer.downloadSuccess(offset, downloadElapsed, slowThreshold, archiveSlowPeerPenalty)
	if downloadElapsed > 0 && float64(offset)/downloadElapsed.Seconds() < slowThreshold {
		s.noteArchivePeerFailure(resolved.Shard, peer)
		s.log.Debug().
			Uint32("masterchain_seqno", resolved.MasterchainSeqno).
			Int32("workchain", resolved.Shard.Workchain).
			Str("shard", fmt.Sprintf("%016x", uint64(resolved.Shard.Shard))).
			Int64("archive_id", archiveID).
			Str("peer", peer.addr).
			Int64("bytes", offset).
			Dur("elapsed", downloadElapsed).
			Dur("wall_elapsed", wallElapsed).
			Str("speed", formatByteRate(offset, downloadElapsed)).
			Msg("archive download peer too slow")
	} else {
		s.noteArchivePeerSuccess(resolved.Shard, peer, offset, downloadElapsed)
	}

	downloadEvent := s.log.Info().
		Uint32("masterchain_seqno", resolved.MasterchainSeqno).
		Int32("workchain", resolved.Shard.Workchain).
		Str("shard", fmt.Sprintf("%016x", uint64(resolved.Shard.Shard))).
		Int64("archive_id", archiveID).
		Int64("bytes", offset).
		Dur("elapsed", downloadElapsed).
		Dur("wall_elapsed", wallElapsed).
		Str("speed", formatByteRate(offset, downloadElapsed)).
		Str("peer", peer.addr)
	downloadEvent.Msg("archive slice downloaded")

	return &archive.Downloaded{
		MasterchainSeqno: resolved.MasterchainSeqno,
		Shard:            resolved.Shard,
		ArchiveID:        archiveID,
		Peer:             peer.addr,
		Path:             path,
		Bytes:            offset,
		DownloadElapsed:  downloadElapsed,
	}, nil
}

func checkArchivePackDownloadSize(offset int64, nextBytes int) error {
	if offset < 0 {
		return fmt.Errorf("archive download offset is invalid: %d", offset)
	}
	if nextBytes < 0 {
		return fmt.Errorf("archive download slice size is invalid: %d", nextBytes)
	}
	next := int64(nextBytes)
	if next > archivePackMaxBytes || offset > archivePackMaxBytes-next {
		return fmt.Errorf("archive pack exceeds max size: offset=%d slice=%d max=%d", offset, nextBytes, archivePackMaxBytes)
	}
	return nil
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

func (s *overlaySubscription) queryArchiveCandidateForDownload(ctx context.Context, peer *overlayPeer, masterchainSeqno uint32, shard archive.ShardID) (archiveCandidate, int64, time.Duration, error) {
	info, err := s.queryArchiveInfo(ctx, peer, masterchainSeqno, shard)
	if err != nil {
		return archiveCandidate{}, 0, 0, err
	}

	started := time.Now()
	data, err := s.queryArchiveSliceWithTimeout(ctx, peer, info.ID, 0, archiveSliceProbeTimeout)
	if err != nil {
		return archiveCandidate{}, 0, 0, fmt.Errorf("probe archive offset=0: %w", err)
	}
	if len(data) > archiveSliceSize {
		return archiveCandidate{}, 0, 0, fmt.Errorf("archive probe response too large: %d", len(data))
	}

	elapsed := time.Since(started)
	bytes := int64(len(data))
	speed := float64(0)
	if elapsed > 0 && bytes > 0 {
		speed = float64(bytes) / elapsed.Seconds()
	}
	if bytes > 0 {
		peer.downloadSuccess(bytes, elapsed, archiveSlowThreshold(shard.Workchain == 0), archiveSlowPeerPenalty)
		s.noteArchivePeerSuccess(shard, peer, bytes, elapsed)
	}

	candidate := archiveCandidate{
		peer:        peer,
		archiveID:   info.ID,
		seedElapsed: elapsed,
		speed:       speed,
	}
	if bytes > 0 {
		candidate.seedSlice = append([]byte(nil), data...)
		candidate.hasSeed = true
	}
	return candidate, bytes, elapsed, nil
}

func (s *overlaySubscription) findArchiveInfo(ctx context.Context, peers []*overlayPeer, masterchainSeqno uint32, shard archive.ShardID) (*archiveInfoResult, error) {
	results, errs := runPeerRequests(ctx, peers, peerRequestOptions{
		parallelism:         archiveInfoParallelism,
		hedgeDelay:          archiveInfoHedgeDelay,
		collectAfterSuccess: archiveDownloadProbeCollectWindow,
		onFailure: func(peer *overlayPeer, err error) {
			if errors.Is(err, archive.ErrNotAvailable) {
				s.denyArchivePeer(shard, peer, "archive_not_available")
				return
			}
			peer.downloadFailed(archiveSlowPeerPenalty)
			s.noteArchivePeerFailure(shard, peer)
		},
	}, func(queryCtx context.Context, peer *overlayPeer) (archiveCandidate, error) {
		candidate, _, _, err := s.queryArchiveCandidateForDownload(queryCtx, peer, masterchainSeqno, shard)
		return candidate, err
	})
	if len(results) == 0 {
		if len(errs) == 0 {
			return nil, archive.ErrNotAvailable
		}
		return nil, errors.Join(errs...)
	}

	sort.SliceStable(results, func(i, j int) bool {
		return archiveCandidateSpeed(results[i]) > archiveCandidateSpeed(results[j])
	})

	res := results[0]
	s.log.Debug().
		Uint32("masterchain_seqno", masterchainSeqno).
		Int32("workchain", shard.Workchain).
		Str("shard", fmt.Sprintf("%016x", uint64(shard.Shard))).
		Int64("archive_id", res.value.archiveID).
		Int("probes", len(results)).
		Str("peer", res.peer.addr).
		Str("speed", formatArchiveProbeRate(peerRequestResult[archiveDownloadProbe]{
			peer: res.peer,
			value: archiveDownloadProbe{
				candidate: res.value,
				bytes:     int64(len(res.value.seedSlice)),
				elapsed:   res.value.seedElapsed,
			},
		})).
		Msg("selected archive info peer from probes")

	return &archiveInfoResult{
		peer:      res.peer,
		candidate: res.value,
	}, nil
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
