package p2p

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xssnick/gton/internal/logutil"
	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/gton/service/archive/packfile"

	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/tl"
)

const (
	archiveSliceSize = 2 << 20
	// 3 in-flight 2MB slices keep the sticky peer's pipe full between slice
	// round-trips during catch-up; going higher mostly concentrates more load
	// on that single peer (starved downloads already hedge to a second peer).
	archiveSliceParallelism    = 3
	archiveSliceProbeSize      = 256 << 10
	archivePackMaxBytes        = int64(2 << 30)
	archiveInfoTimeout         = 6 * time.Second
	archiveSliceProbeTimeout   = 10 * time.Second
	archiveSliceInitialTimeout = 12 * time.Second
	archiveSliceTimeout        = 15 * time.Second
	archiveInfoMaxAnswer       = 1024

	archiveDiscoveryWait    = 2500 * time.Millisecond
	archiveBootstrapSlowLog = 2 * time.Second

	archiveFailureCooldown        = largeDownloadSlowPeerPenalty
	archiveHedgeExtraPeers        = 1
	archiveDownloadRoundPeerLimit = 8
)

var ErrNoArchivePeers = errors.New("overlay has no archive peers")

type archiveInfoResult struct {
	peer      *overlayPeer
	candidate archiveCandidate
}

type archiveSliceResult struct {
	offset int64
	data   []byte
	err    error
}

type archiveCandidate struct {
	peer        *overlayPeer
	archiveID   int64
	seedSlice   []byte
	seedElapsed time.Duration
	seedMaxSize int32
	hasSeed     bool
	speed       float64
	query       *peerQueryOperation
}

type resolvedArchive struct {
	MasterchainSeqno uint32
	Shard            archive.ShardID
	ArchiveID        int64
	Peer             string
	seedSlice        []byte
	seedElapsed      time.Duration
	seedMaxSize      int32
	hasSeed          bool

	peer       *overlayPeer
	peers      []*overlayPeer
	infoHedged bool
	query      *peerQueryOperation
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

func (s *overlaySubscription) ensureArchivePeers(ctx context.Context, pool *archivePeerPool, masterchainSeqno uint32, shard archive.ShardID) error {
	discoveryDone := pool.refill(ctx, false)
	if pool.readyArchive(shard, masterchainSeqno) {
		return nil
	}
	if done := pool.refreshUseless(ctx, shard); done != nil {
		discoveryDone = done
	}
	if pool.readyArchive(shard, masterchainSeqno) {
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
			if pool.readyArchive(shard, masterchainSeqno) {
				return nil
			}
			if done := pool.refill(ctx, false); done != nil {
				discoveryDone = done
			}
			if done := pool.refreshUseless(ctx, shard); done != nil {
				discoveryDone = done
			}
			if pool.readyArchive(shard, masterchainSeqno) {
				return nil
			}
		case <-ticker.C:
			if pool.readyArchive(shard, masterchainSeqno) {
				return nil
			}
			if done := pool.refreshUseless(ctx, shard); done != nil {
				discoveryDone = done
			}
			if pool.readyArchive(shard, masterchainSeqno) {
				return nil
			}
			if discoveryDone == nil && pool.scoutingSize() == 0 {
				return nil
			}
		case <-discoveryDone:
			discoveryDone = nil
			if pool.readyArchive(shard, masterchainSeqno) {
				return nil
			}
			if pool.scoutingSize() == 0 {
				return nil
			}
		case <-timer.C:
			return nil
		}
	}
}

func (s *overlaySubscription) resolveArchive(ctx context.Context, session *ArchiveSession, pool *archivePeerPool, masterchainSeqno uint32, shard archive.ShardID, options ArchiveDownloadOptions) (resolvedArchive, error) {
	// downloadCandidates never filters its input (it returns a permutation,
	// possibly with the sticky selected peer prepended), so an empty result
	// means the pool itself has no candidates.
	peers := archiveDownloadRoundPeers(pool.downloadCandidatesForArchive(session, shard, masterchainSeqno, pool.candidates(shard)))
	if len(peers) == 0 {
		return resolvedArchive{}, ErrNoArchivePeers
	}

	hedge := options.Hedge && len(peers) > 1
	alternatives := len(peers) > 1
	if options.Hedge {
		session.noteArchiveHedge(shard, alternatives, time.Now())
	} else if session.shouldHedgeArchiveDownload(shard, alternatives, time.Now()) {
		hedge = true
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
		seedMaxSize:      info.candidate.seedMaxSize,
		hasSeed:          info.candidate.hasSeed,
		peer:             info.peer,
		peers:            peers,
		infoHedged:       hedge,
		query:            info.candidate.query,
	}, nil
}

func (s *overlaySubscription) downloadArchiveFromResolved(ctx context.Context, session *ArchiveSession, pool *archivePeerPool, resolved resolvedArchive, options ArchiveDownloadOptions) (*archive.Downloaded, error) {
	if resolved.query != nil {
		defer resolved.query.finish(context.Canceled)
	}

	// resolved.peers is non-empty (resolveArchive checked) and
	// downloadCandidates only reorders, so peers cannot be empty here.
	peers := pool.downloadCandidatesForArchive(session, resolved.Shard, resolved.MasterchainSeqno, resolved.peers)

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

	if resolved.infoHedged {
		options.Hedge = true
	}
	alternatives := len(ordered) > 1
	if options.Hedge {
		session.noteArchiveHedge(resolved.Shard, alternatives, time.Now())
	} else if session.shouldHedgeArchiveDownload(resolved.Shard, alternatives, time.Now()) {
		options.Hedge = true
	}

	candidates := map[PeerID]archiveCandidate{}
	if resolved.peer != nil {
		candidates[downloadPeerID(resolved.peer)] = archiveCandidate{
			peer:        resolved.peer,
			archiveID:   resolved.ArchiveID,
			seedSlice:   resolved.seedSlice,
			seedElapsed: resolved.seedElapsed,
			seedMaxSize: resolved.seedMaxSize,
			hasSeed:     resolved.hasSeed,
			query:       resolved.query,
		}
	}
	return s.downloadArchiveFromPeers(ctx, session, pool, resolved, ordered, candidates, options)
}

func (s *overlaySubscription) downloadArchiveFromPeers(ctx context.Context, session *ArchiveSession, pool *archivePeerPool, resolved resolvedArchive, peers []*overlayPeer, candidates map[PeerID]archiveCandidate, options ArchiveDownloadOptions) (*archive.Downloaded, error) {
	peers = archiveDownloadRoundPeers(peers)

	var errs []error
	usedPeers := make(map[PeerID]struct{}, len(peers))
	if options.Hedge {
		if release := session.tryAcquireArchiveHedge(); release != nil {
			downloaded, attempted, hedgeErrs := s.downloadArchiveFromPeersHedged(ctx, session, pool, resolved, peers, candidates)
			release()
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
				s.noteArchiveDownloadError(ctx, session, pool, resolved.MasterchainSeqno, resolved.Shard, peer, err)
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
		s.noteArchiveDownloadError(ctx, session, pool, resolved.MasterchainSeqno, resolved.Shard, peer, err)
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

func archiveDownloadRoundPeers(peers []*overlayPeer) []*overlayPeer {
	limited := make([]*overlayPeer, 0, min(len(peers), archiveDownloadRoundPeerLimit))
	seen := make(map[PeerID]struct{}, cap(limited))
	for _, peer := range peers {
		peerID := downloadPeerID(peer)
		if peerID.IsZero() {
			continue
		}
		if _, ok := seen[peerID]; ok {
			continue
		}

		seen[peerID] = struct{}{}
		limited = append(limited, peer)
		if len(limited) == archiveDownloadRoundPeerLimit {
			break
		}
	}
	return limited
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
	for pending := len(attempts); pending > 0; {
		var res archiveDownloadAttemptResult
		select {
		case <-ctx.Done():
			cancel()
			for ; pending > 0; pending-- {
				<-results
			}
			return nil, attempted, []error{ctx.Err()}
		case res = <-results:
			pending--
		}

		if res.err == nil {
			cancel()
			for ; pending > 0; pending-- {
				<-results
			}
			session.selectArchivePeerFromPool(resolved.Shard, res.candidate.peer, pool)
			s.logArchiveHedgeResult(resolved, res.candidate, res.primary, len(attempts))
			return res.downloaded, attempted, errs
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			cancel()
			for ; pending > 0; pending-- {
				<-results
			}
			return nil, attempted, []error{ctxErr}
		}
		if errors.Is(res.err, context.Canceled) {
			continue
		}
		s.noteArchiveDownloadError(ctx, session, pool, resolved.MasterchainSeqno, resolved.Shard, res.candidate.peer, res.err)
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

func (s *overlaySubscription) noteArchiveDownloadError(ctx context.Context, session *ArchiveSession, pool *archivePeerPool, masterchainSeqno uint32, shard archive.ShardID, peer *overlayPeer, err error) {
	if errors.Is(err, archive.ErrNotAvailable) {
		pool.recordArchiveDemandNotAvailable(shard, masterchainSeqno, peer)
		session.clearSelectedArchivePeerID(shard, downloadPeerID(peer))
		return
	}
	if errors.Is(err, context.Canceled) {
		return
	}
	pool.rememberTransportBlocked(downloadPeerID(peer))
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

func (s *overlaySubscription) downloadArchiveFromPeer(ctx context.Context, session *ArchiveSession, pool *archivePeerPool, resolved resolvedArchive, candidate archiveCandidate, selectOnSuccess bool) (downloaded *archive.Downloaded, err error) {
	peer := candidate.peer
	archiveID := candidate.archiveID

	archiveRelease, ok := pool.acquire(peer)
	if !ok {
		if candidate.query != nil {
			candidate.query.finish(context.Canceled)
		}
		return nil, fmt.Errorf("archive peer left the pool: %w", archive.ErrNotAvailable)
	}
	defer archiveRelease()

	query := candidate.query
	if query == nil {
		query = s.beginPeerQueryOperation(peer)
	}
	defer func() {
		finishArchivePeerQuery(query, err)
	}()

	startedAt := time.Now()
	lastLog := startedAt
	var offset int64
	var lastLogBytes int64
	var downloadElapsed time.Duration
	var lastLogDownloadElapsed time.Duration
	var sliceDownloadStartedAt time.Time
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
		seedMaxSize := candidate.seedMaxSize
		if seedMaxSize == 0 {
			seedMaxSize = archiveSliceSize
		}
		if len(candidate.seedSlice) > int(seedMaxSize) {
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
		seedComplete = len(candidate.seedSlice) < int(seedMaxSize)
	}
	seedDownloadElapsed := downloadElapsed

	if !seedComplete {
		sliceCtx, cancelSlices := context.WithCancel(ctx)
		results := make(chan archiveSliceResult, archiveSliceParallelism)
		var sliceWG sync.WaitGroup
		defer func() {
			cancelSlices()
			sliceWG.Wait()
		}()

		nextRequestOffset := offset
		outstanding := 0
		initialSlice := true
		// Out-of-order results park here until the contiguous offset arrives;
		// requests are issued in offset order, so at most parallelism-1 results
		// can precede the one being waited for.
		pending := make(map[int64]archiveSliceResult, archiveSliceParallelism-1)

		for {
			result, buffered := pending[offset]
			if buffered {
				delete(pending, offset)
			} else {
				for outstanding < archiveSliceParallelism && nextRequestOffset <= archivePackMaxBytes {
					sliceTimeout := archiveSliceTimeout
					if initialSlice {
						sliceTimeout = archiveSliceInitialTimeout
					}
					initialSlice = false

					requestOffset := nextRequestOffset
					nextRequestOffset += archiveSliceSize
					outstanding++
					if sliceDownloadStartedAt.IsZero() {
						sliceDownloadStartedAt = time.Now()
					}

					sliceWG.Add(1)
					go func(requestOffset int64, sliceTimeout time.Duration) {
						defer sliceWG.Done()

						data, err := s.queryArchiveSliceWithTimeout(
							sliceCtx,
							peer,
							archiveID,
							requestOffset,
							archiveSliceSize,
							sliceTimeout,
						)
						select {
						case results <- archiveSliceResult{offset: requestOffset, data: data, err: err}:
						case <-sliceCtx.Done():
						}
					}(requestOffset, sliceTimeout)
				}

				if outstanding == 0 {
					return nil, fmt.Errorf("archive download reached invalid offset: %d", offset)
				}

				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case result = <-results:
				}
				if result.offset != offset {
					if len(pending) >= archiveSliceParallelism-1 {
						return nil, fmt.Errorf("archive slice pipeline returned unexpected offset: got=%d want=%d", result.offset, offset)
					}
					pending[result.offset] = result
					continue
				}
			}

			outstanding--
			if result.err != nil {
				return nil, fmt.Errorf("download archive offset=%d: %w", result.offset, result.err)
			}
			if len(result.data) > archiveSliceSize {
				return nil, fmt.Errorf("archive slice response too large: %d", len(result.data))
			}
			if err := writeSlice(result.data); err != nil {
				return nil, err
			}

			now := time.Now()
			downloadElapsed = seedDownloadElapsed + now.Sub(sliceDownloadStartedAt)
			if elapsed := now.Sub(lastLog); elapsed >= 10*time.Second {
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

				lastLog = now
				lastLogBytes = offset
				lastLogDownloadElapsed = downloadElapsed
			}

			if len(result.data) < archiveSliceSize {
				cancelSlices()
				sliceWG.Wait()
				downloadElapsed = seedDownloadElapsed + time.Since(sliceDownloadStartedAt)
				break
			}
		}
	}

	wallElapsed := time.Since(startedAt)
	if candidate.hasSeed && candidate.seedElapsed > 0 {
		wallElapsed += candidate.seedElapsed
	}
	pool.noteArchiveDownload(resolved.Shard, peer, offset, downloadElapsed)
	if offset > 0 {
		pool.markSuccess(resolved.Shard, peer)
		if selectOnSuccess {
			session.selectArchivePeerFromPool(resolved.Shard, peer, pool)
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

func finishArchivePeerQuery(query *peerQueryOperation, err error) {
	if errors.Is(err, archive.ErrNotAvailable) {
		err = asPeerQueryNotReady(err)
	}
	query.finish(err)
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

func (s *overlaySubscription) queryArchiveSliceWithTimeout(
	ctx context.Context,
	peer *overlayPeer,
	archiveID int64,
	offset int64,
	maxSize int32,
	timeout time.Duration,
) ([]byte, error) {
	query := GetArchiveSlice{
		ArchiveID: archiveID,
		Offset:    offset,
		MaxSize:   maxSize,
	}
	return s.queryArchiveRawFromPeerWithLimits(ctx, peer, query, timeout, uint64(maxSize)+4096)
}

func (s *overlaySubscription) fetchArchiveCandidate(ctx context.Context, session *ArchiveSession, pool *archivePeerPool, peer *overlayPeer, masterchainSeqno uint32, shard archive.ShardID, selectOnSeed bool) (candidate archiveCandidate, err error) {
	archiveRelease, ok := pool.acquire(peer)
	if !ok {
		return archiveCandidate{}, fmt.Errorf("archive peer left the pool: %w", archive.ErrNotAvailable)
	}
	defer archiveRelease()

	query := s.beginPeerQueryOperation(peer)
	defer func() {
		if err != nil {
			finishArchivePeerQuery(query, err)
		}
	}()

	info, err := s.queryArchiveInfo(ctx, peer, masterchainSeqno, shard, archiveInfoTimeout)
	if err != nil {
		if errors.Is(err, archive.ErrNotAvailable) {
			pool.recordArchiveDemandNotAvailable(shard, masterchainSeqno, peer)
		}
		return archiveCandidate{}, err
	}
	started := time.Now()
	candidate = archiveCandidate{
		peer:        peer,
		archiveID:   info.ID,
		seedMaxSize: archiveSliceProbeSize,
		query:       query,
	}
	data, err := s.queryArchiveSliceWithTimeout(
		ctx,
		peer,
		info.ID,
		0,
		archiveSliceProbeSize,
		archiveSliceProbeTimeout,
	)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return archiveCandidate{}, ctxErr
		}
		return archiveCandidate{}, fmt.Errorf("probe archive slice: %w", err)
	}
	if len(data) == 0 {
		pool.recordArchiveDemandNotAvailable(shard, masterchainSeqno, peer)
		return archiveCandidate{}, fmt.Errorf("probe archive slice returned no data: %w", archive.ErrNotAvailable)
	}
	if len(data) > archiveSliceProbeSize {
		return archiveCandidate{}, fmt.Errorf("archive seed response too large: %d", len(data))
	}
	if err = checkArchivePackMagic(data); err != nil {
		return archiveCandidate{}, err
	}

	elapsed := time.Since(started)
	bytes := int64(len(data))
	speed := float64(0)
	if archiveSpeedSampleReliable(bytes) && elapsed > 0 {
		speed = float64(bytes) / elapsed.Seconds()
	}
	pool.noteArchiveSeedSuccess(shard, peer, bytes, elapsed)
	pool.markProven(shard, peer)
	pool.recordArchiveDemandEvidence(shard, masterchainSeqno, peer, archivePeerDemandProven)
	if selectOnSeed {
		session.selectArchivePeerFromPool(shard, peer, pool)
	}

	candidate.seedElapsed = elapsed
	candidate.speed = speed
	candidate.seedSlice = data
	candidate.hasSeed = true
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
		s.noteArchiveCandidateError(ctx, session, pool, masterchainSeqno, shard, peer, err)
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
	for pending := len(attempts); pending > 0; {
		var res archiveInfoAttemptResult
		select {
		case <-ctx.Done():
			cancel()
			for ; pending > 0; pending-- {
				discardArchiveInfoAttempt(<-results)
			}
			return nil, ctx.Err()
		case res = <-results:
			pending--
		}

		if res.err == nil {
			cancel()
			for ; pending > 0; pending-- {
				discardArchiveInfoAttempt(<-results)
			}
			if res.candidate.hasSeed {
				session.selectArchivePeerFromPool(shard, res.peer, pool)
			}
			s.logArchiveInfoHedgeResult(masterchainSeqno, shard, res.candidate, res.primary, len(attempts))
			return &archiveInfoResult{
				peer:      res.peer,
				candidate: res.candidate,
			}, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			cancel()
			for ; pending > 0; pending-- {
				discardArchiveInfoAttempt(<-results)
			}
			return nil, ctxErr
		}
		if errors.Is(res.err, context.Canceled) {
			continue
		}
		s.noteArchiveCandidateError(ctx, session, pool, masterchainSeqno, shard, res.peer, res.err)
		errs = append(errs, archiveDownloadError(res.peer, res.err))
	}

	if len(errs) == 0 {
		return nil, archive.ErrNotAvailable
	}
	return nil, errors.Join(errs...)
}

func discardArchiveInfoAttempt(result archiveInfoAttemptResult) {
	if result.candidate.query != nil {
		result.candidate.query.finish(context.Canceled)
	}
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

func (s *overlaySubscription) noteArchiveCandidateError(ctx context.Context, session *ArchiveSession, pool *archivePeerPool, masterchainSeqno uint32, shard archive.ShardID, peer *overlayPeer, err error) {
	if errors.Is(err, archive.ErrNotAvailable) {
		pool.recordArchiveDemandNotAvailable(shard, masterchainSeqno, peer)
		session.clearSelectedArchivePeerID(shard, downloadPeerID(peer))
		return
	}
	if errors.Is(err, context.Canceled) {
		return
	}
	pool.rememberTransportBlocked(downloadPeerID(peer))
	session.rejectArchivePeer(ctx, pool, shard, peer, archivePeerRejectCandidateFailed)
}

func (s *overlaySubscription) queryArchiveInfo(ctx context.Context, peer *overlayPeer, masterchainSeqno uint32, shard archive.ShardID, timeout time.Duration) (ArchiveInfo, error) {
	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var resp tl.Serializable
	if err := peer.queryTransport.Query(
		queryCtx,
		archiveInfoMaxAnswer,
		archiveInfoQuery(masterchainSeqno, shard),
		&resp,
	); err != nil {
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

func (s *overlaySubscription) queryArchiveFromPeerWithLimits(ctx context.Context, peer *overlayPeer, req tl.Serializable, timeout time.Duration, maxAnswerSize uint64) (tl.Serializable, error) {
	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var resp tl.Serializable
	if err := peer.queryTransport.Query(queryCtx, maxAnswerSize, req, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *overlaySubscription) queryArchiveRawFromPeerWithLimits(ctx context.Context, peer *overlayPeer, req tl.Serializable, timeout time.Duration, maxAnswerSize uint64) ([]byte, error) {
	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	answer, err := peer.queryTransport.QueryRaw(queryCtx, maxAnswerSize, req)
	if err != nil {
		return nil, err
	}
	return answer, nil
}

func archiveInfoQuery(masterchainSeqno uint32, shard archive.ShardID) tl.Serializable {
	if shard.IsMasterchain() {
		return GetArchiveInfo{MasterchainSeqno: int32(masterchainSeqno)}
	}
	return GetShardArchiveInfo{
		MasterchainSeqno: int32(masterchainSeqno),
		ShardPrefix: tonnodeapi.ShardID{
			Workchain: shard.Workchain,
			Shard:     shard.Shard,
		},
	}
}
