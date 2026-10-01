package service

import (
	"sync"
	"sync/atomic"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
)

type BroadcastAdmission struct {
	log                 zerolog.Logger
	liveWindow          uint32
	backpressureWindows uint32

	mu                 sync.Mutex
	initialized        bool
	closed             bool
	closedAtomic       atomic.Bool
	liveMasterSeqno    uint32
	flushedMasterSeqno uint32
}

func NewBroadcastAdmission(log zerolog.Logger, liveWindow uint32, backpressureWindows uint32) *BroadcastAdmission {
	return &BroadcastAdmission{
		log:                 log,
		liveWindow:          liveWindow,
		backpressureWindows: backpressureWindows,
	}
}

var _ p2p.BroadcastAdmission = (*BroadcastAdmission)(nil)

func (a *BroadcastAdmission) CanAcceptBroadcast(_ p2p.BroadcastAdmissionRequest) bool {
	return !a.closedAtomic.Load()
}

func (a *BroadcastAdmission) ObserveLiveCurrentState(current *storage.CurrentState) {
	a.observeCurrentState(current, false)
}

func (a *BroadcastAdmission) ObserveFlushedCurrentState(current *storage.CurrentState) {
	a.observeCurrentState(current, true)
}

func (a *BroadcastAdmission) observeCurrentState(current *storage.CurrentState, flushed bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	seqno := current.Masterchain.Block.SeqNo
	if !a.initialized {
		a.liveMasterSeqno = seqno
		a.flushedMasterSeqno = seqno
		a.initialized = true
	} else if seqno > a.liveMasterSeqno {
		a.liveMasterSeqno = seqno
	}
	if flushed && seqno > a.flushedMasterSeqno {
		a.flushedMasterSeqno = seqno
		if a.flushedMasterSeqno > a.liveMasterSeqno {
			a.liveMasterSeqno = a.flushedMasterSeqno
		}
	}

	a.updateLocked()
}

func (a *BroadcastAdmission) updateLocked() {
	lag := a.liveFlushLagLocked()
	closeLag := a.closeLag()
	openLag := closeLag / 2
	wasClosed := a.closed

	if wasClosed {
		if lag <= openLag {
			a.closed = false
		}
	} else if lag >= closeLag {
		a.closed = true
	}
	if wasClosed == a.closed {
		return
	}
	a.closedAtomic.Store(a.closed)

	event := a.log.Info()
	if a.closed {
		event = a.log.Warn()
	}
	event.
		Bool("closed", a.closed).
		Uint32("live_master_seqno", a.liveMasterSeqno).
		Uint32("flushed_master_seqno", a.flushedMasterSeqno).
		Uint32("lag", lag).
		Uint32("open_lag", openLag).
		Uint32("close_lag", closeLag).
		Msg("updated broadcast admission circuit breaker")
}

func (a *BroadcastAdmission) liveFlushLagLocked() uint32 {
	if a.liveMasterSeqno <= a.flushedMasterSeqno {
		return 0
	}
	return a.liveMasterSeqno - a.flushedMasterSeqno
}

func (a *BroadcastAdmission) closeLag() uint32 {
	closeLag := checkpointBackpressureBlocks(a.liveWindow, 2)
	hardLag := checkpointBackpressureBlocks(a.liveWindow, a.backpressureWindows)
	if closeLag > hardLag {
		return hardLag
	}
	return closeLag
}
