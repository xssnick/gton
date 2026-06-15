package service

import (
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"
)

var _ p2p.BroadcastAdmission = (*Service)(nil)

func (s *Service) CanAcceptBroadcast(_ p2p.BroadcastAdmissionRequest) bool {
	if s == nil {
		return true
	}

	return !s.broadcastAdmissionClosedAtomic.Load()
}

func (s *Service) observeBroadcastLiveCurrentState(current *storage.CurrentState) {
	s.observeBroadcastCurrentState(current, false)
}

func (s *Service) observeBroadcastFlushedCurrentState(current *storage.CurrentState) {
	s.observeBroadcastCurrentState(current, true)
}

func (s *Service) observeBroadcastCurrentState(current *storage.CurrentState, flushed bool) {
	if s == nil || current == nil {
		return
	}

	s.broadcastAdmissionMu.Lock()
	defer s.broadcastAdmissionMu.Unlock()

	seqno := current.Masterchain.Block.SeqNo
	if !s.broadcastAdmissionInitialized {
		s.broadcastLiveMasterSeqno = seqno
		s.broadcastFlushedMasterSeqno = seqno
		s.broadcastAdmissionInitialized = true
	} else if seqno > s.broadcastLiveMasterSeqno {
		s.broadcastLiveMasterSeqno = seqno
	}
	if flushed && seqno > s.broadcastFlushedMasterSeqno {
		s.broadcastFlushedMasterSeqno = seqno
		if s.broadcastFlushedMasterSeqno > s.broadcastLiveMasterSeqno {
			s.broadcastLiveMasterSeqno = s.broadcastFlushedMasterSeqno
		}
	}

	s.updateBroadcastAdmissionLocked()
}

func (s *Service) updateBroadcastAdmissionLocked() {
	lag := s.broadcastLiveFlushLagLocked()
	closeLag := s.broadcastAdmissionCloseLag()
	openLag := s.broadcastAdmissionOpenLag(closeLag)
	wasClosed := s.broadcastAdmissionClosed

	if wasClosed {
		if lag <= openLag {
			s.broadcastAdmissionClosed = false
		}
	} else if lag >= closeLag {
		s.broadcastAdmissionClosed = true
	}
	if wasClosed == s.broadcastAdmissionClosed {
		return
	}
	s.broadcastAdmissionClosedAtomic.Store(s.broadcastAdmissionClosed)

	event := s.log.Info()
	if s.broadcastAdmissionClosed {
		event = s.log.Warn()
	}
	event.
		Bool("closed", s.broadcastAdmissionClosed).
		Uint32("live_master_seqno", s.broadcastLiveMasterSeqno).
		Uint32("flushed_master_seqno", s.broadcastFlushedMasterSeqno).
		Uint32("lag", lag).
		Uint32("open_lag", openLag).
		Uint32("close_lag", closeLag).
		Msg("updated broadcast admission circuit breaker")
}

func (s *Service) broadcastLiveFlushLagLocked() uint32 {
	if s.broadcastLiveMasterSeqno <= s.broadcastFlushedMasterSeqno {
		return 0
	}
	return s.broadcastLiveMasterSeqno - s.broadcastFlushedMasterSeqno
}

func (s *Service) broadcastAdmissionCloseLag() uint32 {
	liveWindow := s.nextCheckpointBlocks()
	closeLag := checkpointBackpressureBlocks(liveWindow, 2)
	hardLag := checkpointBackpressureBlocks(liveWindow, s.syncBackpressureWindowCount())
	if closeLag > hardLag {
		return hardLag
	}
	return closeLag
}

func (s *Service) broadcastAdmissionOpenLag(closeLag uint32) uint32 {
	return closeLag / 2
}
