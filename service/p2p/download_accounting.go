package p2p

import (
	"context"
	"errors"
	"time"

	"github.com/xssnick/gton/internal/logutil"

	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

const liveNextUnavailablePenalty = 3 * time.Second

func noteBlockDownloadSuccess(peer *overlayPeer, block *DownloadedBlock, elapsed time.Duration) {
	bytes := downloadedBlockPayloadBytes(block)
	if bytes <= 0 {
		bytes = 1
	}
	slowThreshold := blockSlowPeerSpeed
	slowPenalty := blockSlowPeerPenalty
	if bytes < blockSpeedSampleMin {
		slowThreshold = 0
		slowPenalty = 0
	}
	peer.downloadSuccess(bytes, elapsed, slowThreshold, slowPenalty)
}

func (s *overlaySubscription) noteChainBlockDownloadSuccess(chain ton.BlockIDExt, peer *overlayPeer, block *DownloadedBlock, elapsed time.Duration) {
	noteBlockDownloadSuccess(peer, block, elapsed)

	bytes := downloadedBlockPayloadBytes(block)
	if bytes <= 0 || elapsed <= 0 {
		return
	}
	speed := float64(bytes) / elapsed.Seconds()
	winnerID := peer.id
	if winnerID.IsZero() {
		return
	}
	confirmedAt := time.Now()

	key := chainDownloadKeyFromBlock(chain)
	var penalized []chainUnavailablePeer
	s.chainDownloadMx.Lock()

	if s.chainDownloads == nil {
		s.chainDownloads = map[chainDownloadKey]*chainDownloadState{}
	}
	state := s.chainDownloads[key]
	if state == nil {
		state = &chainDownloadState{}
		s.chainDownloads[key] = state
	}

	if state.peer == nil || state.peer.id != peer.id {
		s.log.Debug().
			Str("chain", key.String()).
			Str("peer", peer.addr).
			Str("speed", logutil.FormatByteRate(bytes, elapsed)).
			Msg("selected chain block download peer")
	}
	state.peer = peer
	if state.speed == 0 {
		state.speed = speed
	} else {
		state.speed = state.speed*0.7 + speed*0.3
	}
	for missedKey, missed := range state.unavailable {
		if missedKey == winnerID || missed.peer == nil || confirmedAt.Sub(missed.at) > blockUnavailableConfirmWindow {
			delete(state.unavailable, missedKey)
			continue
		}
		penalized = append(penalized, missed)
		delete(state.unavailable, missedKey)
	}
	s.chainDownloadMx.Unlock()

	for _, missed := range penalized {
		missed.peer.downloadFailed(blockUnavailablePeerPenalty)
		s.log.Debug().
			Str("chain", key.String()).
			Str("peer", missed.peer.addr).
			Str("winner", peer.addr).
			Dur("penalty", blockUnavailablePeerPenalty).
			Msg("temporarily deprioritized peer after confirmed unavailable response")
	}
}

func (s *overlaySubscription) noteLiveNextDownloadSuccess(chain ton.BlockIDExt, peer *overlayPeer, block *DownloadedBlock, elapsed time.Duration, liveNotAvailableMisses int64) {
	s.noteChainBlockDownloadSuccess(chain, peer, block, elapsed)
	if !isMasterchainBlock(chain) || elapsed <= 0 {
		return
	}

	key := peer.id
	if key.IsZero() {
		return
	}

	s.chainDownloadMx.Lock()
	defer s.chainDownloadMx.Unlock()

	if s.liveNextPeers == nil {
		s.liveNextPeers = map[PeerID]*liveNextPeerState{}
	}
	state := s.liveNextPeers[key]
	if state == nil {
		state = &liveNextPeerState{}
		s.liveNextPeers[key] = state
	}

	state.successes++
	state.unavailableUntil = time.Time{}
	bytes := downloadedBlockPayloadBytes(block)
	speed := liveNextDownloadBytesPerSecond(bytes, elapsed)
	if speed > 0 {
		if state.bytesSec == 0 {
			state.bytesSec = speed
		} else {
			state.bytesSec = state.bytesSec*0.7 + speed*0.3
		}
	}

	availability := liveNextAvailabilitySample(bytes, liveNotAvailableMisses)
	if state.availability == 0 {
		state.availability = availability
	} else {
		state.availability = state.availability*0.7 + availability*0.3
	}
	if state.availability < 0.01 {
		state.availability = 0
	}

	if state.latency == 0 {
		state.latency = elapsed
		return
	}
	state.latency = time.Duration(float64(state.latency)*0.7 + float64(elapsed)*0.3)
}

func liveNextDownloadBytesPerSecond(bytes int64, elapsed time.Duration) float64 {
	if bytes <= 0 || elapsed <= 0 {
		return 0
	}
	return float64(bytes) / elapsed.Seconds()
}

func liveNextAvailabilitySample(bytes int64, misses int64) float64 {
	if misses <= 0 {
		return 0
	}
	sizeWeight := 1.0
	if bytes > 0 {
		sizeWeight += float64(bytes) / float64(1<<20)
		if sizeWeight > 5 {
			sizeWeight = 5
		}
	}
	return float64(misses) * sizeWeight
}

func (s *overlaySubscription) noteChainBlockDownloadFailure(chain ton.BlockIDExt, peer *overlayPeer, err error) {
	if errors.Is(err, ErrBlockNotAvailable) {
		s.noteChainBlockUnavailable(chain, peer)
		return
	}
	peer.downloadFailed(blockSlowPeerPenalty)

	key := chainDownloadKeyFromBlock(chain)
	s.chainDownloadMx.Lock()
	defer s.chainDownloadMx.Unlock()

	state := s.chainDownloads[key]
	if state != nil && state.peer != nil && state.peer.id == peer.id {
		s.log.Debug().
			Err(err).
			Str("chain", key.String()).
			Str("peer", peer.addr).
			Msg("cleared chain block download peer after failure")
		s.clearChainBlockPeerLocked(key)
	}
}

func (s *overlaySubscription) noteChainBlockUnavailable(chain ton.BlockIDExt, peer *overlayPeer) {
	key := chainDownloadKeyFromBlock(chain)
	peerID := peer.id
	if peerID.IsZero() {
		return
	}

	s.chainDownloadMx.Lock()
	defer s.chainDownloadMx.Unlock()

	state := s.chainDownloads[key]
	if state == nil {
		state = &chainDownloadState{}
		if s.chainDownloads == nil {
			s.chainDownloads = map[chainDownloadKey]*chainDownloadState{}
		}
		s.chainDownloads[key] = state
	}
	if state.unavailable == nil {
		state.unavailable = map[PeerID]chainUnavailablePeer{}
	}
	state.unavailable[peerID] = chainUnavailablePeer{
		peer: peer,
		at:   time.Now(),
	}
}

func (s *overlaySubscription) noteLiveNextDownloadFailure(chain ton.BlockIDExt, peer *overlayPeer, err error) {
	if peer == nil || !isMasterchainBlock(chain) {
		s.noteChainBlockDownloadFailure(chain, peer, err)
		return
	}
	if !errors.Is(err, ErrBlockNotAvailable) {
		s.noteChainBlockDownloadFailure(chain, peer, err)
		return
	}

	peerID := peer.id
	if peerID.IsZero() {
		return
	}

	chainKey := chainDownloadKeyFromBlock(chain)
	unavailableUntil := time.Now().Add(liveNextUnavailablePenalty)

	s.chainDownloadMx.Lock()
	if s.liveNextPeers == nil {
		s.liveNextPeers = map[PeerID]*liveNextPeerState{}
	}
	state := s.liveNextPeers[peerID]
	if state == nil {
		state = &liveNextPeerState{}
		s.liveNextPeers[peerID] = state
	}
	state.unavailableUntil = unavailableUntil

	sticky := s.chainDownloads[chainKey]
	wasSticky := sticky != nil && sticky.peer != nil && sticky.peer.id == peerID
	if wasSticky {
		s.clearChainBlockPeerLocked(chainKey)
	}
	s.chainDownloadMx.Unlock()

	event := s.log.Debug().
		Str("chain", chainKey.String()).
		Str("peer", peer.addr).
		Dur("penalty", liveNextUnavailablePenalty)
	if wasSticky {
		event.Bool("cleared_sticky", true)
	}
	event.Msg("temporarily deprioritized live next masterchain peer")
}

func (s *overlaySubscription) clearChainBlockPeerLocked(key chainDownloadKey) {
	delete(s.chainDownloads, key)
}

func downloadedBlockPayloadBytes(block *DownloadedBlock) int64 {
	return int64(len(block.BlockBOC) + len(block.ProofBOC))
}

func runConcurrentOverlayQueries(ctx context.Context, peers []*overlayPeer, parallelism int, hedgeDelay time.Duration, query func(context.Context, *overlayPeer) (tl.Serializable, error)) (tl.Serializable, error) {
	res, err := runFirstPeerRequest(ctx, peers, peerRequestOptions{
		parallelism: parallelism,
		hedgeDelay:  hedgeDelay,
	}, query)
	if err != nil {
		return nil, err
	}
	return res.value, nil
}
