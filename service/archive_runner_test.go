package service

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

type archiveRunnerNetworkStub struct {
	observed ton.BlockIDExt
}

func (archiveRunnerNetworkStub) BeginArchiveSession() *p2p.ArchiveSession {
	return nil
}

func (s archiveRunnerNetworkStub) ObservedMasterchainBlock() (ton.BlockIDExt, error) {
	if s.observed.SeqNo == 0 {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	return s.observed, nil
}

type archiveRunnerTransitionsStub struct{}

func (archiveRunnerTransitionsStub) masterchainValidatorsForConsensus(*storage.BlockState, ton.BlockIDExt, masterchainValidatorCacheKey) (*blockproof.PreparedValidatorSet, error) {
	return nil, nil
}

func (archiveRunnerTransitionsStub) applyArchiveMasterBlock(context.Context, *storage.BlockState, *PreparedBlock, *masterchainConsensusProof, stateUpdateApplier) (*storage.BlockState, masterchainApplyTiming, time.Duration, error) {
	return nil, masterchainApplyTiming{}, 0, nil
}

func (archiveRunnerTransitionsStub) applyArchiveShardBlock(context.Context, ton.BlockIDExt, []*storage.BlockState, PreparedBlock, stateUpdateApplier, *storage.BlockState) (*storage.BlockState, error) {
	return nil, nil
}

func (archiveRunnerTransitionsStub) archiveCurrentAdvanced(*storage.CurrentState) {
}

func (archiveRunnerTransitionsStub) archiveCheckpointCommitted(*storage.CurrentState, []storage.StateCheckpointBlock) {
}

func (archiveRunnerTransitionsStub) enterSyncUntilOffline(*storage.CurrentState, PreparedBlock) {
}

func TestArchiveRunnerRequiresBindBeforeCatchUp(t *testing.T) {
	runner := &ArchiveRunner{}

	_, err := runner.CatchUp(context.Background(), &storage.CurrentState{}, ton.BlockIDExt{})
	if !errors.Is(err, errArchiveRunnerNotBound) {
		t.Fatalf("catch up without bind error = %v, want %v", err, errArchiveRunnerNotBound)
	}
}

func TestArchiveRunnerBindIsOneShotAndFrozenAfterUse(t *testing.T) {
	transitions := archiveRunnerTransitionsStub{}
	runner := &ArchiveRunner{}
	if err := runner.Bind(transitions, transitions, transitions, transitions); err != nil {
		t.Fatalf("bind archive runner: %v", err)
	}
	if err := runner.Bind(transitions, transitions, transitions, transitions); !errors.Is(err, errArchiveRunnerAlreadyBound) {
		t.Fatalf("second bind error = %v, want %v", err, errArchiveRunnerAlreadyBound)
	}

	current := &storage.CurrentState{
		ShardClientSeqno: 2,
		Masterchain: storage.BlockState{
			Block: ton.BlockIDExt{Workchain: -1, Shard: topShard, SeqNo: 1},
		},
	}
	_, err := runner.CatchUp(context.Background(), current, ton.BlockIDExt{})
	if err == nil || !strings.Contains(err.Error(), "differs from shard client seqno") {
		t.Fatalf("first archive use error = %v, want invalid current state", err)
	}
	if err = runner.Bind(transitions, transitions, transitions, transitions); !errors.Is(err, errArchiveRunnerStarted) {
		t.Fatalf("bind after use error = %v, want %v", err, errArchiveRunnerStarted)
	}
}

func TestArchiveWindowWaitHandsOffToNearbyObservedTarget(t *testing.T) {
	current := testMasterBlockID(150)
	run := &archiveCatchUpRun{
		archive: &ArchiveRunner{network: archiveRunnerNetworkStub{
			observed: testMasterBlockID(151),
		}},
		current: &storage.CurrentState{
			ShardClientSeqno: current.SeqNo,
			Masterchain:      storage.BlockState{Block: current},
		},
	}

	window, err := run.nextArchiveWindowWithProgress()
	if window != nil || !errors.Is(err, errArchiveNextBlockReady) {
		t.Fatalf("archive wait result = window=%v err=%v, want next-block handoff", window, err)
	}
}

func TestArchiveWindowPipelineStopJoinsAndReleasesBufferedWindow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	joined := make(chan struct{})
	done := make(chan archiveWindowResult, 1)
	releaseProducer := make(chan struct{})
	window := &shardClientArchiveWindow{
		masterSequence: []PreparedBlock{{}},
		masterStates:   map[uint32]*storage.BlockState{1: {}},
		archiveBlocks:  map[storage.BlockRootHash]PreparedBlock{},
	}
	done <- archiveWindowResult{window: window}

	pipeline := &archiveWindowPipeline{cancel: cancel, done: done, joined: joined}
	go func() {
		defer close(joined)
		defer close(done)
		<-ctx.Done()
		<-releaseProducer
	}()

	stopped := make(chan struct{})
	go func() {
		pipeline.stop()
		close(stopped)
	}()

	select {
	case <-stopped:
		t.Fatal("pipeline stop returned before its producer joined")
	case <-time.After(20 * time.Millisecond):
	}

	close(releaseProducer)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("pipeline stop did not join its producer")
	}
	if window.masterSequence != nil || window.masterStates != nil || window.archiveBlocks != nil {
		t.Fatal("pipeline stop retained a buffered archive window")
	}
}
