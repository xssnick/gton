package service

import (
	"context"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

type exactBlockDownloadProbeState struct {
	started           time.Time
	consecutiveMisses int
}

type exactBlockDownloadProbeDecision struct {
	peerLimit         int
	stagedPeerLimit   int
	consecutiveMisses int
	waited            time.Duration
	wideFanout        bool
	urgentFanout      bool
}

func (s *SyncCoordinator) exactBlockDownloadProbeDecision(state exactBlockDownloadProbeState) exactBlockDownloadProbeDecision {
	decision := exactBlockDownloadProbeDecision{
		peerLimit:         exactBlockDownloadProbePeers,
		stagedPeerLimit:   exactBlockDownloadProbePeers,
		consecutiveMisses: state.consecutiveMisses,
	}
	if !state.started.IsZero() {
		decision.waited = time.Since(state.started)
	}

	if decision.shouldUseWideFanout() {
		decision.peerLimit = nextBlockBootstrapWidePeers
		decision.stagedPeerLimit = nextBlockBootstrapWidePeers
		decision.wideFanout = true
		return decision
	}
	if decision.shouldUseUrgentFanout() {
		decision.peerLimit = nextBlockBootstrapUrgentPeers
		decision.stagedPeerLimit = nextBlockBootstrapWidePeers
		decision.urgentFanout = true
	}
	return decision
}

func (d exactBlockDownloadProbeDecision) shouldUseUrgentFanout() bool {
	if d.consecutiveMisses >= nextBlockBootstrapUrgentMisses {
		return true
	}
	return d.waited >= time.Duration(nextBlockBootstrapUrgentLagSeconds)*time.Second
}

func (d exactBlockDownloadProbeDecision) shouldUseWideFanout() bool {
	if d.consecutiveMisses >= nextBlockBootstrapWideMisses {
		return true
	}
	return d.waited >= time.Duration(nextBlockBootstrapWideLagSeconds)*time.Second
}

func (d exactBlockDownloadProbeDecision) probeTimeout() time.Duration {
	if !d.urgentFanout && !d.wideFanout {
		return nextBlockBootstrapProbeTimeout
	}
	return nextBlockBootstrapLiveProbeTimeout
}

func (s *SyncCoordinator) downloadExactChainBlockProbe(ctx context.Context, block ton.BlockIDExt, state exactBlockDownloadProbeState) (p2p.DownloadedBlock, SyncBlockSource, error) {
	decision := s.exactBlockDownloadProbeDecision(state)
	queryCtx, cancel := context.WithTimeout(ctx, decision.probeTimeout())
	defer cancel()

	if decision.urgentFanout || decision.wideFanout {
		s.log.Debug().
			Str("block", storage.FormatBlockRef(block)).
			Int("probe_peers", decision.peerLimit).
			Int("staged_probe_peers", decision.stagedPeerLimit).
			Int("consecutive_misses", decision.consecutiveMisses).
			Dur("waited", decision.waited).
			Bool("wide_fanout", decision.wideFanout).
			Msg("probing exact chain block with urgent fanout")
	}

	downloaded, err := s.node.ProbeBlockFull(queryCtx, block, p2p.ProbeBlockFullOptions{
		PeerLimit:       decision.peerLimit,
		StagedPeerLimit: decision.stagedPeerLimit,
		StageDelay:      nextBlockBootstrapLiveStageDelay,
	})
	if err != nil {
		return p2p.DownloadedBlock{}, "", fmt.Errorf("probe block %s: %w", storage.FormatBlockRef(block), err)
	}
	return *downloaded, syncBlockSourceForKind(SyncBlockSourcePeerProbe, downloaded.Kind), nil
}
