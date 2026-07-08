package p2p

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/xssnick/gton/internal/logutil"
	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/gton/service/archive/packfile"
	"time"

	"github.com/xssnick/tonutils-go/tl"
)

const (
	archiveSliceSize                  = 2 << 20
	archiveSliceMaxAnswer             = archiveSliceSize + 4096
	archivePackMaxBytes               = int64(2 << 30)
	archiveInfoTimeout                = 3 * time.Second
	archiveSliceProbeTimeout          = 3 * time.Second
	archiveSliceTimeout               = 6 * time.Second
	archiveInfoPinnedMaxTimeout       = 9 * time.Second
	archiveSliceProbePinnedMaxTimeout = 9 * time.Second
	archiveSlicePinnedMaxTimeout      = 30 * time.Second

	archiveDiscoveryWait    = 2500 * time.Millisecond
	archiveBootstrapSlowLog = 2 * time.Second

	archiveSpeedSampleMinBytes      = int64(archiveSliceSize)
	archiveLargeSpeedSampleMinBytes = int64(32 << 20)
	defaultArchivePeerSpeed         = float64(1 << 20)
	archiveSlowPeerSpeed            = float64(1 << 20)
	basechainSlowPeerSpeed          = float64(3 << 20)
	archiveGoodPeerSpeed            = float64(8 << 20)
	basechainGoodPeerSpeed          = float64(8 << 20)
	basechainVeryGoodPeerSpeed      = float64(16 << 20)
	archiveSlowPeerPenalty          = largeDownloadSlowPeerPenalty
	archiveUnknownPeerSpeed         = float64(256 << 10)
	archiveHedgeExtraPeers          = 2
)

type archiveInfoResult struct {
	peer      *overlayPeer
	candidate archiveCandidate
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

	peer       *overlayPeer
	peers      []*overlayPeer
	infoHedged bool
}

type archiveDownloadAttempt struct {
	peer         *overlayPeer
	candidate    archiveCandidate
	hasCandidate bool
	primary      bool
}

type archiveDownloadAttemptResult struct {
	downloaded *archive.Downloaded
	candidate  archiveCandidate
	err        error
	primary    bool
}

type archiveInfoAttemptResult struct {
	peer      *overlayPeer
	candidate archiveCandidate
	err       error
	primary   bool
}

func (s *overlaySubscription) ensureArchivePeers(ctx context.Context, pool *archivePeerPool, shard archive.ShardID) error {
	if err := s.ensurePeers(ctx); err != nil {
		return err
	}
	if pool == nil || pool.ready(shard) {
		return nil
	}

	discoveryDone := pool.refill(ctx, false)
	if pool.ready(shard) {
		return nil
	}
	if done := pool.refreshUseless(ctx, shard); done != nil {
		discoveryDone = done
	}
	if pool.ready(shard) {
		return nil
	}

	timer := time.NewTimer(archiveDiscoveryWait)
	defer timer.Stop()
	timerC := timer.C

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		notify := s.peerNotifySnapshot()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notify:
			if pool.ready(shard) {
				return nil
			}
			if done := pool.refill(ctx, false); done != nil {
				discoveryDone = done
			}
			if done := pool.refreshUseless(ctx, shard); done != nil {
				discoveryDone = done
			}
			if pool.ready(shard) {
				return nil
			}
		case <-ticker.C:
			if pool.ready(shard) {
				return nil
			}
			if done := pool.refreshUseless(ctx, shard); done != nil {
				discoveryDone = done
			}
			if pool.ready(shard) {
				return nil
			}
		case <-discoveryDone:
			discoveryDone = nil
			if pool.ready(shard) {
				return nil
			}
			if timerC == nil {
				return nil
			}
		case <-timerC:
			if discoveryDone == nil {
				return nil
			}
			timerC = nil
		}
	}
}

func (s *overlaySubscription) resolveArchive(ctx context.Context, session *ArchiveSession, pool *archivePeerPool, masterchainSeqno uint32, shard archive.ShardID, options ArchiveDownloadOptions) (resolvedArchive, error) {
	// downloadCandidates never filters its input (it returns a permutation,
	// possibly with the sticky selected peer prepended), so an empty result
	// means the pool itself has no candidates.
	peers := pool.downloadCandidates(session, shard, pool.candidates(shard))
	if len(peers) == 0 {
		return resolvedArchive{}, errors.New("overlay has no archive peers")
	}

	hedge := options.Hedge && len(peers) > 1
	if session != nil {
		alternatives := len(peers) > 1
		if options.Hedge {
			session.noteArchiveHedge(shard, alternatives, time.Now())
		} else if session.shouldHedgeArchiveDownload(shard, alternatives, time.Now()) {
			hedge = true
		}
	}

	var info *archiveInfoResult
	var err error
	if hedge {
		info, err = s.findArchiveInfoHedged(ctx, session, pool, peers, masterchainSeqno, shard)
	} else {
		info, err = s.findArchiveInfo(ctx, session, pool, peers, masterchainSeqno, shard)
	}
	if err != nil {
		return resolvedArchive{}, err
	}

	return resolvedArchive{
		MasterchainSeqno: masterchainSeqno,
		Shard:            shard,
		ArchiveID:        info.candidate.archiveID,
		Peer:             info.peer.addr,
		seedSlice:        info.candidate.seedSlice,
		seedElapsed:      info.candidate.seedElapsed,
		hasSeed:          info.candidate.hasSeed,
		peer:             info.peer,
		peers:            peers,
		infoHedged:       hedge,
	}, nil
}

func (s *overlaySubscription) downloadArchiveFromResolved(ctx context.Context, session *ArchiveSession, pool *archivePeerPool, resolved resolvedArchive, options ArchiveDownloadOptions) (*archive.Downloaded, error) {
	// resolved.peers is non-empty (resolveArchive checked) and
	// downloadCandidates only reorders, so peers cannot be empty here.
	peers := pool.downloadCandidates(session, resolved.Shard, resolved.peers)

	ordered := make([]*overlayPeer, 0, len(peers)+1)
	seen := make(map[PeerID]struct{}, len(peers)+2)
	addPeer := func(peer *overlayPeer) {
		if peer == nil {
			return
		}
		if pool.coolingDown(resolved.Shard, peer) {
			return
		}
		peerID := downloadPeerID(peer)
		if peerID.IsZero() {
			return
		}
		if _, ok := seen[peerID]; ok {
			return
		}
		seen[peerID] = struct{}{}
		ordered = append(ordered, peer)
	}
	addPeer(resolved.peer)
	for _, peer := range peers {
		addPeer(peer)
	}

	if session != nil {
		alternatives := len(ordered) > 1
		if options.Hedge {
			session.noteArchiveHedge(resolved.Shard, alternatives, time.Now())
		} else if !resolved.infoHedged && session.shouldHedgeArchiveDownload(resolved.Shard, alternatives, time.Now()) {
			options.Hedge = true
		}
	}

	candidates := map[PeerID]archiveCandidate{}
	if resolved.peer != nil {
		candidates[downloadPeerID(resolved.peer)] = archiveCandidate{
			peer:        resolved.peer,
			archiveID:   resolved.ArchiveID,
			seedSlice:   resolved.seedSlice,
			seedElapsed: resolved.seedElapsed,
			hasSeed:     resolved.hasSeed,
		}
	}
	return s.downloadArchiveFromPeers(ctx, session, pool, resolved, ordered, candidates, options)
}

func (s *overlaySubscription) downloadArchiveFromPeers(ctx context.Context, session *ArchiveSession, pool *archivePeerPool, resolved resolvedArchive, peers []*overlayPeer, candidates map[PeerID]archiveCandidate, options ArchiveDownloadOptions) (*archive.Downloaded, error) {
	var errs []error
	usedPeers := make(map[PeerID]struct{}, len(peers))
	if options.Hedge {
		downloaded, attempted, hedgeErrs := s.downloadArchiveFromPeersHedged(ctx, session, pool, resolved, peers, candidates)
		if downloaded != nil {
			return downloaded, nil
		}
		errs = append(errs, hedgeErrs...)
		for peerID := range attempted {
			usedPeers[peerID] = struct{}{}
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
	}

	for _, peer := range peers {
		peerID := downloadPeerID(peer)
		if peerID.IsZero() {
			continue
		}
		if _, ok := usedPeers[peerID]; ok {
			continue
		}
		usedPeers[peerID] = struct{}{}

		candidate, hasCandidate := candidates[peerID]
		if !hasCandidate {
			var err error
			candidate, err = s.fetchArchiveCandidate(ctx, session, pool, peer, resolved.MasterchainSeqno, resolved.Shard, true)
			if err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return nil, ctxErr
				}
				s.noteArchiveDownloadError(ctx, session, pool, resolved.Shard, peer, err)
				errs = append(errs, archiveDownloadError(peer, err))
				continue
			}
			candidates[peerID] = candidate
		}

		downloaded, err := s.downloadArchiveFromPeer(ctx, session, pool, resolved, candidate, true)
		if err == nil {
			return downloaded, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		s.noteArchiveDownloadError(ctx, session, pool, resolved.Shard, peer, err)
		s.log.Debug().
			Err(err).
			Uint32("masterchain_seqno", resolved.MasterchainSeqno).
			Int32("workchain", resolved.Shard.Workchain).
			Str("shard", fmt.Sprintf("%016x", uint64(resolved.Shard.Shard))).
			Int64("archive_id", candidate.archiveID).
			Str("peer", peer.addr).
			Msg("archive download peer failed")
		errs = append(errs, archiveDownloadError(peer, err))
	}

	if len(errs) == 0 {
		return nil, archive.ErrNotAvailable
	}
	return nil, errors.Join(errs...)
}

func (s *overlaySubscription) downloadArchiveFromPeersHedged(ctx context.Context, session *ArchiveSession, pool *archivePeerPool, resolved resolvedArchive, peers []*overlayPeer, candidates map[PeerID]archiveCandidate) (*archive.Downloaded, map[PeerID]struct{}, []error) {
	attempts, attempted := archiveDownloadHedgeAttempts(peers, candidates)
	if len(attempts) < 2 {
		return nil, nil, nil
	}

	raceCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan archiveDownloadAttemptResult, len(attempts))
	for _, attempt := range attempts {
		go func(attempt archiveDownloadAttempt) {
			results <- s.downloadArchiveAttempt(raceCtx, session, pool, resolved, attempt)
		}(attempt)
	}

	errs := make([]error, 0, len(attempts))
	for pending := len(attempts); pending > 0; pending-- {
		res := <-results
		if res.err == nil {
			cancel()
			if session != nil {
				session.selectArchivePeer(resolved.Shard, res.candidate.peer)
			}
			s.logArchiveHedgeResult(resolved, res.candidate, res.primary, len(attempts))
			return res.downloaded, attempted, errs
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			cancel()
			return nil, attempted, []error{ctxErr}
		}
		if errors.Is(res.err, context.Canceled) {
			continue
		}
		s.noteArchiveDownloadError(ctx, session, pool, resolved.Shard, res.candidate.peer, res.err)
		s.log.Debug().
			Err(res.err).
			Uint32("masterchain_seqno", resolved.MasterchainSeqno).
			Int32("workchain", resolved.Shard.Workchain).
			Str("shard", fmt.Sprintf("%016x", uint64(resolved.Shard.Shard))).
			Int64("archive_id", res.candidate.archiveID).
			Str("peer", res.candidate.peer.addr).
			Bool("primary", res.primary).
			Msg("archive hedge download peer failed")
		errs = append(errs, archiveDownloadError(res.candidate.peer, res.err))
	}
	return nil, attempted, errs
}

func archiveDownloadHedgeAttempts(peers []*overlayPeer, candidates map[PeerID]archiveCandidate) ([]archiveDownloadAttempt, map[PeerID]struct{}) {
	limit := 1 + archiveHedgeExtraPeers
	if len(peers) < limit {
		limit = len(peers)
	}

	attempts := make([]archiveDownloadAttempt, 0, limit)
	attempted := make(map[PeerID]struct{}, limit)
	for _, peer := range peers {
		peerID := downloadPeerID(peer)
		if peerID.IsZero() {
			continue
		}
		if _, ok := attempted[peerID]; ok {
			continue
		}
		attempted[peerID] = struct{}{}

		candidate, ok := candidates[peerID]
		attempts = append(attempts, archiveDownloadAttempt{
			peer:         peer,
			candidate:    candidate,
			hasCandidate: ok,
			primary:      len(attempts) == 0,
		})
		if len(attempts) == limit {
			break
		}
	}
	return attempts, attempted
}

func (s *overlaySubscription) downloadArchiveAttempt(ctx context.Context, session *ArchiveSession, pool *archivePeerPool, resolved resolvedArchive, attempt archiveDownloadAttempt) archiveDownloadAttemptResult {
	peer := attempt.peer

	candidate := attempt.candidate
	if !attempt.hasCandidate {
		var err error
		candidate, err = s.fetchArchiveCandidate(ctx, session, pool, peer, resolved.MasterchainSeqno, resolved.Shard, false)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				err = ctxErr
			}
			return archiveDownloadAttemptResult{candidate: archiveCandidate{peer: peer}, err: err, primary: attempt.primary}
		}
	}
	if candidate.peer == nil {
		candidate.peer = peer
	}

	downloaded, err := s.downloadArchiveFromPeer(ctx, session, pool, resolved, candidate, false)
	return archiveDownloadAttemptResult{
		downloaded: downloaded,
		candidate:  candidate,
		err:        err,
		primary:    attempt.primary,
	}
}

func (s *overlaySubscription) logArchiveHedgeResult(resolved resolvedArchive, candidate archiveCandidate, primary bool, attempts int) {
	event := s.log.Debug()
	if !primary {
		event = s.log.Info()
	}
	event.
		Uint32("masterchain_seqno", resolved.MasterchainSeqno).
		Int32("workchain", resolved.Shard.Workchain).
		Str("shard", fmt.Sprintf("%016x", uint64(resolved.Shard.Shard))).
		Int64("archive_id", candidate.archiveID).
		Str("peer", candidate.peer.addr).
		Bool("primary", primary).
		Int("attempts", attempts).
		Msg("archive hedge download won")
}

func (s *overlaySubscription) noteArchiveDownloadError(ctx context.Context, session *ArchiveSession, pool *archivePeerPool, shard archive.ShardID, peer *overlayPeer, err error) {
	if errors.Is(err, archive.ErrNotAvailable) {
		session.rejectArchivePeer(ctx, pool, shard, peer, archivePeerRejectNotAvailable)
		return
	}
	if session.archivePeerDeadlineGrace(peer, err) {
		return
	}

	peer.downloadFailed(archiveSlowPeerPenalty)
	session.rejectArchivePeer(ctx, pool, shard, peer, archivePeerRejectDownloadFailed)
}

func formatArchiveCandidateRate(candidate archiveCandidate) string {
	if candidate.seedElapsed > 0 {
		return logutil.FormatByteRate(int64(len(candidate.seedSlice)), candidate.seedElapsed)
	}
	if candidate.speed > 0 {
		return logutil.FormatByteRate(int64(candidate.speed), time.Second)
	}
	return logutil.FormatByteRate(0, time.Second)
}

func (s *overlaySubscription) downloadArchiveFromPeer(ctx context.Context, session *ArchiveSession, pool *archivePeerPool, resolved resolvedArchive, candidate archiveCandidate, selectOnSuccess bool) (*archive.Downloaded, error) {
	peer := candidate.peer
	archiveID := candidate.archiveID

	archiveRelease := pool.acquire(peer)
	defer archiveRelease()
	release := s.node.acquireDownloadPeer(peer)
	defer release()

	startedAt := time.Now()
	lastLog := startedAt
	var offset int64
	var lastLogBytes int64
	var downloadElapsed time.Duration
	var lastLogDownloadElapsed time.Duration
	var buf []byte

	s.log.Debug().
		Uint32("masterchain_seqno", resolved.MasterchainSeqno).
		Int32("workchain", resolved.Shard.Workchain).
		Str("shard", fmt.Sprintf("%016x", uint64(resolved.Shard.Shard))).
		Int64("archive_id", archiveID).
		Str("peer", peer.addr).
		Msg("selected archive download peer")

	writeSlice := func(data []byte) error {
		if offset == 0 {
			if err := checkArchivePackMagic(data); err != nil {
				return err
			}
		}
		if len(data) == 0 {
			return nil
		}
		if sizeErr := checkArchivePackDownloadSize(offset, len(data)); sizeErr != nil {
			return sizeErr
		}
		buf = append(buf, data...)
		offset += int64(len(data))
		return nil
	}

	seedComplete := false
	if candidate.hasSeed {
		if len(candidate.seedSlice) > archiveSliceSize {
			return nil, fmt.Errorf("archive seed response too large: %d", len(candidate.seedSlice))
		}
		if err := checkArchivePackMagic(candidate.seedSlice); err != nil {
			return nil, err
		}
		if err := checkArchivePackDownloadSize(0, len(candidate.seedSlice)); err != nil {
			return nil, err
		}
		downloadElapsed += candidate.seedElapsed
		buf = candidate.seedSlice
		offset = int64(len(candidate.seedSlice))
		seedComplete = len(candidate.seedSlice) < archiveSliceSize
	}

	for !seedComplete {
		queryStarted := time.Now()
		data, err := s.queryArchiveSliceWithTimeout(ctx, peer, archiveID, offset, session.archivePeerSliceTimeout(peer))
		downloadElapsed += time.Since(queryStarted)
		if err != nil {
			return nil, fmt.Errorf("download archive offset=%d: %w", offset, err)
		}
		if len(data) > archiveSliceSize {
			return nil, fmt.Errorf("archive slice response too large: %d", len(data))
		}
		if err = writeSlice(data); err != nil {
			return nil, err
		}

		if elapsed := time.Since(lastLog); elapsed >= 10*time.Second {
			logDownloadElapsed := downloadElapsed - lastLogDownloadElapsed
			s.log.Debug().
				Uint32("masterchain_seqno", resolved.MasterchainSeqno).
				Int32("workchain", resolved.Shard.Workchain).
				Str("shard", fmt.Sprintf("%016x", uint64(resolved.Shard.Shard))).
				Int64("archive_id", archiveID).
				Int64("bytes", offset).
				Str("size", formatByteSize(offset)).
				Str("peer", peer.addr).
				Dur("elapsed", logDownloadElapsed).
				Dur("wall_elapsed", elapsed).
				Str("speed", logutil.FormatByteRate(offset-lastLogBytes, logDownloadElapsed)).
				Msg("archive slice download progress")

			lastLog = time.Now()
			lastLogBytes = offset
			lastLogDownloadElapsed = downloadElapsed
		}

		if len(data) < archiveSliceSize {
			break
		}
	}

	wallElapsed := time.Since(startedAt)
	if candidate.hasSeed && candidate.seedElapsed > 0 {
		wallElapsed += candidate.seedElapsed
	}
	noteArchivePeerDownload(resolved.Shard, peer, offset, downloadElapsed)
	if offset > 0 {
		pool.markSuccess(resolved.Shard, peer)
		session.noteArchivePeerSuccess(peer)
		if selectOnSuccess {
			session.selectArchivePeer(resolved.Shard, peer)
		}
	}

	downloadEvent := s.log.Info().
		Uint32("masterchain_seqno", resolved.MasterchainSeqno).
		Int32("workchain", resolved.Shard.Workchain).
		Str("shard", fmt.Sprintf("%016x", uint64(resolved.Shard.Shard))).
		Int64("archive_id", archiveID).
		Int64("bytes", offset).
		Str("size", formatByteSize(offset)).
		Dur("elapsed", downloadElapsed).
		Dur("wall_elapsed", wallElapsed).
		Str("speed", logutil.FormatByteRate(offset, downloadElapsed)).
		Str("peer", peer.addr)
	downloadEvent.Msg("archive slice downloaded")

	return &archive.Downloaded{
		MasterchainSeqno: resolved.MasterchainSeqno,
		Shard:            resolved.Shard,
		ArchiveID:        archiveID,
		Peer:             peer.addr,
		Data:             buf,
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

func checkArchivePackMagic(data []byte) error {
	if len(data) < packfile.HeaderSize {
		return fmt.Errorf("archive package is too short: %d bytes", len(data))
	}
	got := binary.LittleEndian.Uint32(data[:packfile.HeaderSize])
	if got != packfile.PackageMagic {
		return fmt.Errorf("archive package magic mismatch: got=%08x want=%08x", got, packfile.PackageMagic)
	}
	return nil
}

func (s *overlaySubscription) queryArchiveSliceWithTimeout(ctx context.Context, peer *overlayPeer, archiveID int64, offset int64, timeout time.Duration) ([]byte, error) {
	query := GetArchiveSlice{
		ArchiveID: archiveID,
		Offset:    offset,
		MaxSize:   archiveSliceSize,
	}
	return s.queryRawFromPeerWithLimits(ctx, peer, query, timeout, archiveSliceMaxAnswer)
}

func (s *overlaySubscription) fetchArchiveCandidate(ctx context.Context, session *ArchiveSession, pool *archivePeerPool, peer *overlayPeer, masterchainSeqno uint32, shard archive.ShardID, selectOnSeed bool) (archiveCandidate, error) {
	archiveRelease := pool.acquire(peer)
	defer archiveRelease()
	release := func() {}
	if s.node != nil {
		release = s.node.acquireDownloadPeer(peer)
	}
	defer release()

	info, err := s.queryArchiveInfo(ctx, peer, masterchainSeqno, shard, session.archivePeerInfoTimeout(peer))
	if err != nil {
		return archiveCandidate{}, err
	}
	pool.markAvailable(shard, peer)
	session.noteArchivePeerAvailable(peer)

	started := time.Now()
	candidate := archiveCandidate{
		peer:      peer,
		archiveID: info.ID,
	}
	data, err := s.queryArchiveSliceWithTimeout(ctx, peer, info.ID, 0, session.archivePeerSliceProbeTimeout(peer))
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return archiveCandidate{}, ctxErr
		}
		session.archivePeerDeadlineGrace(peer, err)
		s.log.Debug().
			Err(err).
			Uint32("masterchain_seqno", masterchainSeqno).
			Int32("workchain", shard.Workchain).
			Str("shard", fmt.Sprintf("%016x", uint64(shard.Shard))).
			Int64("archive_id", info.ID).
			Str("peer", peer.addr).
			Msg("archive seed probe failed, continuing without seed")
		return candidate, nil
	}
	if len(data) > archiveSliceSize {
		return archiveCandidate{}, fmt.Errorf("archive seed response too large: %d", len(data))
	}

	elapsed := time.Since(started)
	bytes := int64(len(data))
	speed := float64(0)
	if archiveSpeedSampleReliable(bytes) && elapsed > 0 {
		speed = float64(bytes) / elapsed.Seconds()
	}
	noteArchivePeerSeedSuccess(shard, peer, bytes, elapsed)
	if bytes > 0 {
		pool.markSuccess(shard, peer)
		session.noteArchivePeerSuccess(peer)
		if selectOnSeed {
			session.selectArchivePeer(shard, peer)
		}
	}

	candidate.seedElapsed = elapsed
	candidate.speed = speed
	if bytes > 0 {
		candidate.seedSlice = data
		candidate.hasSeed = true
	}
	return candidate, nil
}

func (s *overlaySubscription) findArchiveInfo(ctx context.Context, session *ArchiveSession, pool *archivePeerPool, peers []*overlayPeer, masterchainSeqno uint32, shard archive.ShardID) (*archiveInfoResult, error) {
	var errs []error
	for _, peer := range peers {
		candidate, err := s.fetchArchiveCandidate(ctx, session, pool, peer, masterchainSeqno, shard, true)
		if err == nil {
			s.log.Debug().
				Uint32("masterchain_seqno", masterchainSeqno).
				Int32("workchain", shard.Workchain).
				Str("shard", fmt.Sprintf("%016x", uint64(shard.Shard))).
				Int64("archive_id", candidate.archiveID).
				Str("peer", peer.addr).
				Str("speed", formatArchiveCandidateRate(candidate)).
				Msg("selected archive info peer")

			return &archiveInfoResult{
				peer:      peer,
				candidate: candidate,
			}, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		s.noteArchiveCandidateError(ctx, session, pool, shard, peer, err)
		errs = append(errs, archiveDownloadError(peer, err))
	}

	if len(errs) == 0 {
		return nil, archive.ErrNotAvailable
	}
	return nil, errors.Join(errs...)
}

func (s *overlaySubscription) findArchiveInfoHedged(ctx context.Context, session *ArchiveSession, pool *archivePeerPool, peers []*overlayPeer, masterchainSeqno uint32, shard archive.ShardID) (*archiveInfoResult, error) {
	attempts, _ := archiveDownloadHedgeAttempts(peers, nil)
	if len(attempts) < 2 {
		return s.findArchiveInfo(ctx, session, pool, peers, masterchainSeqno, shard)
	}

	raceCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan archiveInfoAttemptResult, len(attempts))
	for _, attempt := range attempts {
		go func(attempt archiveDownloadAttempt) {
			peer := attempt.peer
			candidate, err := s.fetchArchiveCandidate(raceCtx, session, pool, peer, masterchainSeqno, shard, false)
			if err != nil {
				if ctxErr := raceCtx.Err(); ctxErr != nil {
					err = ctxErr
				}
				candidate = archiveCandidate{peer: peer}
			}
			if candidate.peer == nil {
				candidate.peer = peer
			}
			results <- archiveInfoAttemptResult{
				peer:      peer,
				candidate: candidate,
				err:       err,
				primary:   attempt.primary,
			}
		}(attempt)
	}

	errs := make([]error, 0, len(attempts))
	for pending := len(attempts); pending > 0; pending-- {
		res := <-results
		if res.err == nil {
			cancel()
			if res.candidate.hasSeed {
				session.selectArchivePeer(shard, res.peer)
			}
			s.logArchiveInfoHedgeResult(masterchainSeqno, shard, res.candidate, res.primary, len(attempts))
			return &archiveInfoResult{
				peer:      res.peer,
				candidate: res.candidate,
			}, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			cancel()
			return nil, ctxErr
		}
		if errors.Is(res.err, context.Canceled) {
			continue
		}
		s.noteArchiveCandidateError(ctx, session, pool, shard, res.peer, res.err)
		errs = append(errs, archiveDownloadError(res.peer, res.err))
	}

	if len(errs) == 0 {
		return nil, archive.ErrNotAvailable
	}
	return nil, errors.Join(errs...)
}

func (s *overlaySubscription) logArchiveInfoHedgeResult(masterchainSeqno uint32, shard archive.ShardID, candidate archiveCandidate, primary bool, attempts int) {
	event := s.log.Debug()
	if !primary {
		event = s.log.Info()
	}
	event.
		Uint32("masterchain_seqno", masterchainSeqno).
		Int32("workchain", shard.Workchain).
		Str("shard", fmt.Sprintf("%016x", uint64(shard.Shard))).
		Int64("archive_id", candidate.archiveID).
		Str("peer", candidate.peer.addr).
		Bool("primary", primary).
		Int("attempts", attempts).
		Str("speed", formatArchiveCandidateRate(candidate)).
		Msg("archive info hedge won")
}

func (s *overlaySubscription) noteArchiveCandidateError(ctx context.Context, session *ArchiveSession, pool *archivePeerPool, shard archive.ShardID, peer *overlayPeer, err error) {
	if errors.Is(err, archive.ErrNotAvailable) {
		session.rejectArchivePeer(ctx, pool, shard, peer, archivePeerRejectNotAvailable)
		return
	}
	if session.archivePeerDeadlineGrace(peer, err) {
		return
	}
	peer.downloadFailed(archiveSlowPeerPenalty)
	session.rejectArchivePeer(ctx, pool, shard, peer, archivePeerRejectCandidateFailed)
}

func (s *overlaySubscription) queryArchiveInfo(ctx context.Context, peer *overlayPeer, masterchainSeqno uint32, shard archive.ShardID, timeout time.Duration) (ArchiveInfo, error) {
	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var resp tl.Serializable
	startedAt := time.Now()
	if err := peer.overlay.Query(queryCtx, archiveInfoQuery(masterchainSeqno, shard), &resp); err != nil {
		s.handlePeerQueryFailure(peer, err)
		return ArchiveInfo{}, err
	}
	peer.querySuccess(time.Since(startedAt))

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
