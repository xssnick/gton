package service

import (
	"testing"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"
)

func TestBroadcastAdmissionCircuitUsesLiveFlushLagHysteresis(t *testing.T) {
	svc := &Service{
		log:                       zerolog.Nop(),
		nextBlockCheckpointBlocks: 10,
		syncBackpressureWindows:   4,
	}
	req := p2p.BroadcastAdmissionRequest{Kind: "tonNode.blockBroadcast"}

	svc.observeBroadcastFlushedCurrentState(testBroadcastAdmissionCurrent(100))
	if !svc.CanAcceptBroadcast(req) {
		t.Fatal("admission closed at initialized durable state")
	}

	svc.observeBroadcastLiveCurrentState(testBroadcastAdmissionCurrent(119))
	if !svc.CanAcceptBroadcast(req) {
		t.Fatal("admission closed before close lag")
	}

	svc.observeBroadcastLiveCurrentState(testBroadcastAdmissionCurrent(120))
	if svc.CanAcceptBroadcast(req) {
		t.Fatal("admission remained open at close lag")
	}

	svc.observeBroadcastFlushedCurrentState(testBroadcastAdmissionCurrent(109))
	if svc.CanAcceptBroadcast(req) {
		t.Fatal("admission opened before open lag")
	}

	svc.observeBroadcastFlushedCurrentState(testBroadcastAdmissionCurrent(110))
	if !svc.CanAcceptBroadcast(req) {
		t.Fatal("admission remained closed at open lag")
	}
}

func TestBroadcastAdmissionCloseLagClampsToSyncBackpressureWindow(t *testing.T) {
	svc := &Service{
		log:                       zerolog.Nop(),
		nextBlockCheckpointBlocks: 10,
		syncBackpressureWindows:   1,
	}

	if got := svc.broadcastAdmissionCloseLag(); got != 10 {
		t.Fatalf("close lag = %d, want 10", got)
	}
}

func testBroadcastAdmissionCurrent(seqno uint32) *storage.CurrentState {
	return &storage.CurrentState{
		Masterchain: storage.BlockState{Block: testBlockID(-1, topShard, seqno)},
	}
}
