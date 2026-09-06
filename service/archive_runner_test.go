package service

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
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

func (archiveRunnerTransitionsStub) applyArchiveMasterBlock(context.Context, *storage.BlockState, *PreparedBlock, *masterchainConsensusProof, stateUpdateApplier, *blockAppliedObserverMeta) (*storage.BlockState, masterchainApplyTiming, time.Duration, error) {
	return nil, masterchainApplyTiming{}, 0, nil
}

func (archiveRunnerTransitionsStub) applyArchiveShardBlock(context.Context, ton.BlockIDExt, []*storage.BlockState, PreparedBlock, stateUpdateApplier, *blockAppliedObserverMeta) (*storage.BlockState, error) {
	return nil, nil
}

func (archiveRunnerTransitionsStub) processArchiveBlockApplied(context.Context, BlockAppliedEvent) error {
	return nil
}

func (archiveRunnerTransitionsStub) archiveBlockAppliedEnabled() bool {
	return false
}

func (archiveRunnerTransitionsStub) archiveCurrentAdvanced(*storage.CurrentState) {
}

func (archiveRunnerTransitionsStub) archiveCheckpointCommitted(*storage.CurrentState, []storage.StateCheckpointBlock) {
}

func (archiveRunnerTransitionsStub) enterSyncUntilOffline(*storage.CurrentState, PreparedBlock) {
}

type archiveBlockAppliedTransitionsRecorder struct {
	archiveRunnerTransitionsStub

	mu                    sync.Mutex
	processed             []ton.BlockIDExt
	masterStates          map[uint32]*cell.Cell
	process               func(context.Context, BlockAppliedEvent) error
	blockAppliedEnabled   bool
	advanced              []uint32
	processedAfterAdvance bool
}

type archiveDeferredShardEvent struct {
	master *storage.BlockState
	shard  *storage.BlockState
}

type retryArchiveCheckpointStore struct {
	stateCheckpointStore

	mu            sync.Mutex
	calls         int
	firstFailed   chan struct{}
	secondEntered chan struct{}
	allowSecond   chan struct{}
	failure       error
}

func (s *retryArchiveCheckpointStore) SaveStateCheckpointEntries(
	ctx context.Context,
	blocks []storage.StateCheckpointBlock,
	cells storage.StateCellRecords,
	current *storage.CurrentState,
) (storage.StateCheckpointTiming, error) {
	s.mu.Lock()
	s.calls++
	call := s.calls
	s.mu.Unlock()

	if call == 1 {
		close(s.firstFailed)
		return storage.StateCheckpointTiming{}, s.failure
	}
	if call == 2 {
		close(s.secondEntered)
		select {
		case <-s.allowSecond:
		case <-ctx.Done():
			return storage.StateCheckpointTiming{}, ctx.Err()
		}
	}

	return s.stateCheckpointStore.SaveStateCheckpointEntries(ctx, blocks, cells, current)
}

func (s *retryArchiveCheckpointStore) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.calls
}

func (r *archiveBlockAppliedTransitionsRecorder) archiveBlockAppliedEnabled() bool {
	return r.blockAppliedEnabled
}

func (r *archiveBlockAppliedTransitionsRecorder) applyArchiveMasterBlock(_ context.Context, _ *storage.BlockState, block *PreparedBlock, _ *masterchainConsensusProof, _ stateUpdateApplier, observerMeta *blockAppliedObserverMeta) (*storage.BlockState, masterchainApplyTiming, time.Duration, error) {
	root := r.masterStates[block.ID.SeqNo]
	next := &storage.BlockState{Block: block.ID, Cell: root}
	if observerMeta != nil && observerMeta.deferEvent != nil {
		observerMeta.deferEvent(BlockAppliedEvent{
			BlockRoot:    block.BlockRoot,
			Meta:         block.Meta,
			CurrentState: root,
		})
	}

	return next, masterchainApplyTiming{}, 0, nil
}

func (r *archiveBlockAppliedTransitionsRecorder) processArchiveBlockApplied(ctx context.Context, event BlockAppliedEvent) error {
	r.mu.Lock()
	if len(r.advanced) > 0 {
		r.processedAfterAdvance = true
	}
	r.processed = append(r.processed, event.Meta.ID)
	process := r.process
	r.mu.Unlock()

	if process == nil {
		return nil
	}
	return process(ctx, event)
}

func (r *archiveBlockAppliedTransitionsRecorder) archiveCurrentAdvanced(current *storage.CurrentState) {
	r.mu.Lock()
	r.advanced = append(r.advanced, current.ShardClientSeqno)
	r.mu.Unlock()
}

func (r *archiveBlockAppliedTransitionsRecorder) processedBlocks() []ton.BlockIDExt {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]ton.BlockIDExt(nil), r.processed...)
}

func (r *archiveBlockAppliedTransitionsRecorder) currentAdvanceSnapshot() ([]uint32, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]uint32(nil), r.advanced...), r.processedAfterAdvance
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

func TestArchiveMasterPreparationDefersBlockAppliedEvents(t *testing.T) {
	startBlock := testMasterBlockID(10)
	block := testMasterBlockID(11)
	stateRoot := cell.BeginCell().MustStoreUInt(11, 8).EndCell()
	blockRoot := cell.BeginCell().MustStoreUInt(0x11, 8).EndCell()
	transitions := &archiveBlockAppliedTransitionsRecorder{
		masterStates:        map[uint32]*cell.Cell{block.SeqNo: stateRoot},
		blockAppliedEnabled: true,
	}
	window := &shardClientArchiveWindow{
		startSeqno: 11,
		masterSequence: []PreparedBlock{{
			ID:        block,
			Meta:      &storage.BlockMeta{ID: block},
			BlockRoot: blockRoot,
		}},
		masterProofs: map[uint32]*masterchainConsensusProof{
			block.SeqNo: {block: block},
		},
		masterStates:  map[uint32]*storage.BlockState{},
		archiveBlocks: map[storage.BlockRootHash]PreparedBlock{},
		stateCells:    newTestStateCellWindowCache(nil),
	}
	run := &archiveCatchUpRun{
		archive: &ArchiveRunner{masterTransitions: transitions},
		target:  block,
	}

	last, err := run.applyArchiveMasterBlocks(t.Context(), &storage.BlockState{Block: startBlock}, window)
	if err != nil {
		t.Fatalf("prepare archive master window: %v", err)
	}
	if !last.Block.Equals(&block) {
		t.Fatalf("prepared master = %s, want %s", storage.FormatBlockRef(last.Block), storage.FormatBlockRef(block))
	}
	if got := transitions.processedBlocks(); len(got) != 0 {
		t.Fatalf("processor calls during speculative master preparation = %+v, want none", got)
	}

	window.blockApplied.mu.Lock()
	deferred := len(window.blockApplied.masters)
	window.blockApplied.mu.Unlock()
	if deferred != 1 {
		t.Fatalf("deferred master events = %d, want 1", deferred)
	}
}

func TestArchiveWindowHandoffDropsPrefetchedBlockAppliedEvents(t *testing.T) {
	currentBlock := testMasterBlockID(150)
	nextBlock := testMasterBlockID(151)
	masterState := &storage.BlockState{
		Block: nextBlock,
		Cell:  cell.BeginCell().MustStoreUInt(151, 8).EndCell(),
	}
	window := &shardClientArchiveWindow{
		startSeqno:   nextBlock.SeqNo,
		masterStates: map[uint32]*storage.BlockState{nextBlock.SeqNo: masterState},
	}
	enableArchiveBlockAppliedEvents(t, window)
	window.blockApplied.deferMaster(archiveTestBlockAppliedEvent(masterState, nil))

	done := make(chan archiveWindowResult, 1)
	done <- archiveWindowResult{window: window}
	close(done)
	joined := make(chan struct{})
	close(joined)
	_, cancel := context.WithCancel(context.Background())
	pipeline := &archiveWindowPipeline{
		cancel: cancel,
		done:   done,
		joined: joined,
	}
	transitions := &archiveBlockAppliedTransitionsRecorder{}
	run := &archiveCatchUpRun{
		archive: &ArchiveRunner{
			network:           archiveRunnerNetworkStub{observed: nextBlock},
			masterTransitions: transitions,
		},
		current: &storage.CurrentState{
			ShardClientSeqno: currentBlock.SeqNo,
			Masterchain:      storage.BlockState{Block: currentBlock},
		},
		pipeline: pipeline,
	}

	got, err := run.nextArchiveWindowWithProgress()
	if got != nil || !errors.Is(err, errArchiveNextBlockReady) {
		t.Fatalf("archive handoff result = window=%v err=%v, want next-block handoff", got, err)
	}
	pipeline.stop()
	if calls := transitions.processedBlocks(); len(calls) != 0 {
		t.Fatalf("processor calls for dropped prefetched window = %+v, want none", calls)
	}
}

func TestArchiveAcceptedWindowDispatchesMasterThenShardsExactlyOnce(t *testing.T) {
	currentBlock := testMasterBlockID(10)
	master11 := &storage.BlockState{
		Block: testMasterBlockID(11),
		Cell:  cell.BeginCell().MustStoreUInt(11, 8).EndCell(),
	}
	master12 := &storage.BlockState{
		Block: testMasterBlockID(12),
		Cell:  cell.BeginCell().MustStoreUInt(12, 8).EndCell(),
	}
	window := &shardClientArchiveWindow{
		startSeqno:         11,
		shardBlocksApplied: 3,
		masterStates: map[uint32]*storage.BlockState{
			11: master11,
			12: master12,
		},
	}
	enableArchiveBlockAppliedEvents(t, window)
	window.blockApplied.deferMaster(archiveTestBlockAppliedEvent(master11, nil))
	window.blockApplied.deferMaster(archiveTestBlockAppliedEvent(master12, nil))

	shard11Later := &storage.BlockState{Block: testBlockID(0, topShard, 7), Cell: cell.BeginCell().MustStoreUInt(0x17, 8).EndCell()}
	shard11Earlier := &storage.BlockState{Block: testBlockID(0, topShard>>1, 6), Cell: cell.BeginCell().MustStoreUInt(0x16, 8).EndCell()}
	shard12 := &storage.BlockState{Block: testBlockID(0, topShard, 8), Cell: cell.BeginCell().MustStoreUInt(0x18, 8).EndCell()}
	deferred := []archiveDeferredShardEvent{
		{master: master11, shard: shard11Later},
		{master: master12, shard: shard12},
		{master: master11, shard: shard11Earlier},
	}

	var wg sync.WaitGroup
	for _, item := range deferred {
		wg.Add(1)
		go func() {
			defer wg.Done()
			window.blockApplied.deferShard(item.master.Block.SeqNo, archiveTestBlockAppliedEvent(item.shard, item.master))
		}()
	}
	wg.Wait()

	current := &storage.CurrentState{
		ShardClientSeqno: currentBlock.SeqNo,
		Masterchain:      storage.BlockState{Block: currentBlock},
	}
	next := &storage.CurrentState{
		ShardClientSeqno: master12.Block.SeqNo,
		Masterchain:      *master12,
	}
	transitions := &archiveBlockAppliedTransitionsRecorder{}
	run := &archiveCatchUpRun{
		archive: &ArchiveRunner{
			masterTransitions:  transitions,
			currentTransitions: transitions,
		},
		ctx:     t.Context(),
		current: current,
	}

	if err := run.commitAcceptedArchiveWindow(window, next); err != nil {
		t.Fatalf("commit accepted archive window: %v", err)
	}
	if err := run.dispatchArchiveWindowBlockApplied(window, current, next); err != nil {
		t.Fatalf("repeat accepted archive dispatch: %v", err)
	}

	want := []ton.BlockIDExt{
		master11.Block,
		shard11Earlier.Block,
		shard11Later.Block,
		master12.Block,
		shard12.Block,
	}
	assertArchiveProcessedBlocks(t, transitions.processedBlocks(), want)
	advanced, processedAfterAdvance := transitions.currentAdvanceSnapshot()
	if processedAfterAdvance {
		t.Fatal("archive current advanced before every block-applied event was processed")
	}
	if len(advanced) != 1 || advanced[0] != next.ShardClientSeqno {
		t.Fatalf("archive current advances = %+v, want [%d]", advanced, next.ShardClientSeqno)
	}
	if run.current != next {
		t.Fatal("accepted archive window did not install next current state")
	}
}

func TestArchiveWindowRequiresExplicitBlockAppliedCapability(t *testing.T) {
	currentBlock := testMasterBlockID(10)
	master := &storage.BlockState{
		Block: testMasterBlockID(11),
		Cell:  cell.BeginCell().MustStoreUInt(11, 8).EndCell(),
	}
	current := &storage.CurrentState{ShardClientSeqno: currentBlock.SeqNo, Masterchain: storage.BlockState{Block: currentBlock}}
	next := &storage.CurrentState{ShardClientSeqno: master.Block.SeqNo, Masterchain: *master}
	transitions := &archiveBlockAppliedTransitionsRecorder{}
	run := &archiveCatchUpRun{
		archive: &ArchiveRunner{masterTransitions: transitions},
		ctx:     t.Context(),
	}

	unconfigured := &shardClientArchiveWindow{
		startSeqno:   master.Block.SeqNo,
		masterStates: map[uint32]*storage.BlockState{master.Block.SeqNo: master},
	}
	if err := run.dispatchArchiveWindowBlockApplied(unconfigured, current, next); err == nil || !strings.Contains(err.Error(), "capability was not configured") {
		t.Fatalf("unconfigured archive dispatch error = %v, want explicit capability error", err)
	}

	enabledWithoutEvents := &shardClientArchiveWindow{
		startSeqno:   master.Block.SeqNo,
		masterStates: map[uint32]*storage.BlockState{master.Block.SeqNo: master},
	}
	enableArchiveBlockAppliedEvents(t, enabledWithoutEvents)
	if err := run.dispatchArchiveWindowBlockApplied(enabledWithoutEvents, current, next); err == nil || !strings.Contains(err.Error(), "collected 0 master block-applied events, want 1") {
		t.Fatalf("empty enabled archive dispatch error = %v, want complete collection error", err)
	}

	disabled := &shardClientArchiveWindow{
		startSeqno:   master.Block.SeqNo,
		masterStates: map[uint32]*storage.BlockState{master.Block.SeqNo: master},
	}
	if err := disabled.blockApplied.configure(false); err != nil {
		t.Fatalf("configure disabled archive block-applied events: %v", err)
	}
	if err := run.dispatchArchiveWindowBlockApplied(disabled, current, next); err != nil {
		t.Fatalf("dispatch archive window without processor: %v", err)
	}
	if calls := transitions.processedBlocks(); len(calls) != 0 {
		t.Fatalf("processor calls for disabled archive block-applied collection = %+v, want none", calls)
	}
}

func TestArchiveWindowValidatesAllBlockAppliedEventsBeforeDispatch(t *testing.T) {
	currentBlock := testMasterBlockID(10)
	master := &storage.BlockState{
		Block: testMasterBlockID(11),
		Cell:  cell.BeginCell().MustStoreUInt(11, 8).EndCell(),
	}
	wrongMaster := &storage.BlockState{
		Block: testMasterBlockID(12),
		Cell:  cell.BeginCell().MustStoreUInt(12, 8).EndCell(),
	}
	shard := &storage.BlockState{
		Block: testBlockID(0, topShard, 7),
		Cell:  cell.BeginCell().MustStoreUInt(7, 8).EndCell(),
	}
	window := &shardClientArchiveWindow{
		startSeqno:         master.Block.SeqNo,
		masterStates:       map[uint32]*storage.BlockState{master.Block.SeqNo: master},
		shardBlocksApplied: 1,
	}
	enableArchiveBlockAppliedEvents(t, window)
	window.blockApplied.deferMaster(archiveTestBlockAppliedEvent(master, nil))
	window.blockApplied.deferShard(master.Block.SeqNo, archiveTestBlockAppliedEvent(shard, wrongMaster))

	transitions := &archiveBlockAppliedTransitionsRecorder{}
	run := &archiveCatchUpRun{
		archive: &ArchiveRunner{masterTransitions: transitions},
		ctx:     t.Context(),
	}
	current := &storage.CurrentState{ShardClientSeqno: currentBlock.SeqNo, Masterchain: storage.BlockState{Block: currentBlock}}
	next := &storage.CurrentState{ShardClientSeqno: master.Block.SeqNo, Masterchain: *master}

	err := run.dispatchArchiveWindowBlockApplied(window, current, next)
	if err == nil || !strings.Contains(err.Error(), "invalid inclusion master") {
		t.Fatalf("invalid archive event error = %v, want inclusion validation", err)
	}
	if calls := transitions.processedBlocks(); len(calls) != 0 {
		t.Fatalf("processor calls before complete window validation = %+v, want none", calls)
	}
}

func TestArchiveWindowRejectsIncompleteShardEventCollectionBeforeDispatch(t *testing.T) {
	currentBlock := testMasterBlockID(10)
	master := &storage.BlockState{
		Block: testMasterBlockID(11),
		Cell:  cell.BeginCell().MustStoreUInt(11, 8).EndCell(),
	}
	shard := &storage.BlockState{
		Block: testBlockID(0, topShard, 7),
		Cell:  cell.BeginCell().MustStoreUInt(7, 8).EndCell(),
	}
	window := &shardClientArchiveWindow{
		startSeqno:         master.Block.SeqNo,
		masterStates:       map[uint32]*storage.BlockState{master.Block.SeqNo: master},
		shardBlocksApplied: 2,
	}
	enableArchiveBlockAppliedEvents(t, window)
	window.blockApplied.deferMaster(archiveTestBlockAppliedEvent(master, nil))
	window.blockApplied.deferShard(master.Block.SeqNo, archiveTestBlockAppliedEvent(shard, master))

	transitions := &archiveBlockAppliedTransitionsRecorder{}
	run := &archiveCatchUpRun{
		archive: &ArchiveRunner{masterTransitions: transitions},
		ctx:     t.Context(),
	}
	current := &storage.CurrentState{ShardClientSeqno: currentBlock.SeqNo, Masterchain: storage.BlockState{Block: currentBlock}}
	next := &storage.CurrentState{ShardClientSeqno: master.Block.SeqNo, Masterchain: *master}

	err := run.dispatchArchiveWindowBlockApplied(window, current, next)
	if err == nil || !strings.Contains(err.Error(), "collected 1 shard block-applied events, want 2 applied blocks") {
		t.Fatalf("incomplete archive event error = %v, want shard event count validation", err)
	}
	if calls := transitions.processedBlocks(); len(calls) != 0 {
		t.Fatalf("processor calls before shard event count validation = %+v, want none", calls)
	}
}

func TestArchiveWindowDispatchStopsOnlyOnOuterCancellation(t *testing.T) {
	currentBlock := testMasterBlockID(10)
	master := &storage.BlockState{
		Block: testMasterBlockID(11),
		Cell:  cell.BeginCell().MustStoreUInt(11, 8).EndCell(),
	}
	shard := &storage.BlockState{
		Block: testBlockID(0, topShard, 7),
		Cell:  cell.BeginCell().MustStoreUInt(7, 8).EndCell(),
	}
	window := &shardClientArchiveWindow{
		startSeqno:         master.Block.SeqNo,
		masterStates:       map[uint32]*storage.BlockState{master.Block.SeqNo: master},
		shardBlocksApplied: 1,
	}
	enableArchiveBlockAppliedEvents(t, window)
	window.blockApplied.deferMaster(archiveTestBlockAppliedEvent(master, nil))
	window.blockApplied.deferShard(master.Block.SeqNo, archiveTestBlockAppliedEvent(shard, master))

	ctx, cancel := context.WithCancel(context.Background())
	transitions := &archiveBlockAppliedTransitionsRecorder{}
	transitions.process = func(ctx context.Context, event BlockAppliedEvent) error {
		if event.Meta.ID.Equals(&master.Block) {
			cancel()
			return nil
		}
		return ctx.Err()
	}
	run := &archiveCatchUpRun{
		archive: &ArchiveRunner{
			masterTransitions:  transitions,
			currentTransitions: transitions,
		},
		ctx: ctx,
	}
	current := &storage.CurrentState{ShardClientSeqno: currentBlock.SeqNo, Masterchain: storage.BlockState{Block: currentBlock}}
	next := &storage.CurrentState{ShardClientSeqno: master.Block.SeqNo, Masterchain: *master}
	run.current = current

	err := run.commitAcceptedArchiveWindow(window, next)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled archive dispatch error = %v, want context cancellation", err)
	}
	if run.current != current || run.current.ShardClientSeqno != currentBlock.SeqNo {
		t.Fatalf("canceled archive dispatch advanced current to %d", run.current.ShardClientSeqno)
	}
	if advanced, _ := transitions.currentAdvanceSnapshot(); len(advanced) != 0 {
		t.Fatalf("canceled archive dispatch published current advances %+v", advanced)
	}
}

func TestArchivePostDispatchFailurePersistsAcceptedFrontierBeforeReturn(t *testing.T) {
	store := openManualTestPebbleStorage(t)
	svc := newCurrentStatePersistenceTestService(t, store, nil)
	failure := errors.New("forced first archive checkpoint failure")
	checkpointStore := &retryArchiveCheckpointStore{
		stateCheckpointStore: svc.state.checkpointStore,
		firstFailed:          make(chan struct{}),
		secondEntered:        make(chan struct{}),
		allowSecond:          make(chan struct{}),
		failure:              failure,
	}
	svc.state.checkpointStore = checkpointStore

	previousBlock := testMasterBlockID(76)
	masterBlock := testMasterBlockID(77)
	root := testShardStateCell(t, masterBlock)
	rootHash := root.HashKey(0)
	masterState := &storage.BlockState{
		Block:         masterBlock,
		StateRootHash: rootHash[:],
		Cell:          root,
	}
	current := &storage.CurrentState{
		ShardClientSeqno: previousBlock.SeqNo,
		Masterchain:      storage.BlockState{Block: previousBlock},
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	next := &storage.CurrentState{
		ShardClientSeqno: masterBlock.SeqNo,
		Masterchain:      *masterState,
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	window := &shardClientArchiveWindow{
		startSeqno:   masterBlock.SeqNo,
		masterStates: map[uint32]*storage.BlockState{masterBlock.SeqNo: masterState},
	}
	enableArchiveBlockAppliedEvents(t, window)
	window.blockApplied.deferMaster(archiveTestBlockAppliedEvent(masterState, nil))

	transitions := &archiveBlockAppliedTransitionsRecorder{blockAppliedEnabled: true}
	archiveRunner := newArchivePersistenceTestRunner(svc)
	archiveRunner.masterTransitions = transitions
	archiveRunner.currentTransitions = transitions
	stateCells := newTestStateCellWindowCache(nil)
	if err := rememberAppliedForTest(stateCells, root, mustPreparedReachableStateCells(t, root)); err != nil {
		t.Fatalf("remember accepted archive state cells: %v", err)
	}
	run := &archiveCatchUpRun{
		archive:                archiveRunner,
		ctx:                    t.Context(),
		current:                current,
		lastCheckpointSeqno:    previousBlock.SeqNo,
		checkpointBlocksTarget: archiveRunner.checkpointBlocks,
		stateCells:             stateCells,
		importCache:            newArchiveImportCache(),
	}

	if err := run.commitAcceptedArchiveWindow(window, next); err != nil {
		t.Fatalf("commit accepted archive window: %v", err)
	}
	if run.current != next {
		t.Fatal("accepted hook dispatch did not advance the in-process frontier")
	}
	assertArchiveProcessedBlocks(t, transitions.processedBlocks(), []ton.BlockIDExt{masterBlock})

	rememberFullCheckpointStateForTest(t, &run.checkpointStates, masterState)
	if err := run.startCheckpoint("interval"); err != nil {
		t.Fatalf("start failing checkpoint: %v", err)
	}
	select {
	case <-checkpointStore.firstFailed:
	case <-time.After(5 * time.Second):
		t.Fatal("first archive checkpoint did not fail")
	}
	if _, err := run.finishCheckpoint(true); !errors.Is(err, failure) {
		t.Fatalf("first checkpoint error = %v, want %v", err, failure)
	}

	exitErr := errors.New("post-dispatch archive failure")
	returned := make(chan error, 1)
	go func() {
		returned <- run.returnWithProgress(exitErr)
	}()

	select {
	case err := <-returned:
		t.Fatalf("post-dispatch exit returned before persistence retry: %v", err)
	case <-checkpointStore.secondEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("post-dispatch persistence retry did not start")
	}
	select {
	case err := <-returned:
		t.Fatalf("post-dispatch exit returned while persistence retry was blocked: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(checkpointStore.allowSecond)
	select {
	case err := <-returned:
		if !errors.Is(err, exitErr) {
			t.Fatalf("post-dispatch exit error = %v, want %v", err, exitErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("post-dispatch exit did not return after durable retry")
	}
	if checkpointStore.callCount() != 2 {
		t.Fatalf("checkpoint store calls = %d, want 2", checkpointStore.callCount())
	}
	if run.lastCheckpointSeqno != masterBlock.SeqNo {
		t.Fatalf("durable archive frontier = %d, want %d", run.lastCheckpointSeqno, masterBlock.SeqNo)
	}
	persisted, err := store.CurrentState(t.Context())
	if err != nil {
		t.Fatalf("load persisted archive frontier: %v", err)
	}
	if persisted.ShardClientSeqno != masterBlock.SeqNo || !persisted.Masterchain.Block.Equals(&masterBlock) {
		t.Fatalf("persisted archive frontier = %s at %d, want %s at %d", storage.FormatBlockRef(persisted.Masterchain.Block), persisted.ShardClientSeqno, storage.FormatBlockRef(masterBlock), masterBlock.SeqNo)
	}
}

func archiveTestBlockAppliedEvent(state *storage.BlockState, inclusion *storage.BlockState) BlockAppliedEvent {
	event := BlockAppliedEvent{
		BlockRoot:    state.Cell,
		Meta:         &storage.BlockMeta{ID: state.Block},
		CurrentState: state.Cell,
	}
	if inclusion != nil {
		event.InclusionMasterRef = &inclusion.Block
		event.InclusionMasterState = inclusion.Cell
	}
	return event
}

func enableArchiveBlockAppliedEvents(t *testing.T, window *shardClientArchiveWindow) {
	t.Helper()

	if err := window.blockApplied.configure(true); err != nil {
		t.Fatalf("configure archive block-applied events: %v", err)
	}
}

func assertArchiveProcessedBlocks(t *testing.T, got, want []ton.BlockIDExt) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("processed blocks = %d, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if !got[i].Equals(&want[i]) {
			t.Fatalf("processed block[%d] = %s, want %s", i, storage.FormatBlockRef(got[i]), storage.FormatBlockRef(want[i]))
		}
	}
}
