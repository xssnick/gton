package service

import (
	"testing"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"
)

func TestBroadcastAdmissionCircuitUsesLiveFlushLagHysteresis(t *testing.T) {
	admission := NewBroadcastAdmission(zerolog.Nop(), 10, 4)
	req := p2p.BroadcastAdmissionRequest{Kind: "tonNode.blockBroadcast"}

	admission.ObserveFlushedCurrentState(testBroadcastAdmissionCurrent(100))
	if !admission.CanAcceptBroadcast(req) {
		t.Fatal("admission closed at initialized durable state")
	}

	admission.ObserveLiveCurrentState(testBroadcastAdmissionCurrent(119))
	if !admission.CanAcceptBroadcast(req) {
		t.Fatal("admission closed before close lag")
	}

	admission.ObserveLiveCurrentState(testBroadcastAdmissionCurrent(120))
	if admission.CanAcceptBroadcast(req) {
		t.Fatal("admission remained open at close lag")
	}

	admission.ObserveFlushedCurrentState(testBroadcastAdmissionCurrent(109))
	if admission.CanAcceptBroadcast(req) {
		t.Fatal("admission opened before open lag")
	}

	admission.ObserveFlushedCurrentState(testBroadcastAdmissionCurrent(110))
	if !admission.CanAcceptBroadcast(req) {
		t.Fatal("admission remained closed at open lag")
	}
}

func TestBroadcastAdmissionCloseLagClampsToSyncBackpressureWindow(t *testing.T) {
	admission := NewBroadcastAdmission(zerolog.Nop(), 10, 1)

	if got := admission.closeLag(); got != 10 {
		t.Fatalf("close lag = %d, want 10", got)
	}
}

func testBroadcastAdmissionCurrent(seqno uint32) *storage.CurrentState {
	return &storage.CurrentState{
		Masterchain: storage.BlockState{Block: testBlockID(-1, topShard, seqno)},
	}
}
