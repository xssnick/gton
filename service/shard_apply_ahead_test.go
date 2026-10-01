package service

import (
	"context"
	"testing"
	"time"

	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
)

// newShardApplyAheadTestRunner builds a runner with only the pieces the
// apply-ahead stage touches. The stage never reads r.current or r.master, which
// is exactly what these tests pin.
func newShardApplyAheadTestRunner(ctx context.Context, env *fakeShardStateResolverEnv, current map[tnstore.ShardKey]tnstore.BlockState) *nextSyncRunner {
	runCtx, cancel := context.WithCancel(ctx)
	r := &nextSyncRunner{
		service: &SyncCoordinator{log: zerolog.Nop()},
		ctx:     runCtx,
		cancel:  cancel,
	}
	ctx = runCtx
	r.shardResolver = newShardStateResolver(ctx, shardStateResolverConfig{
		current:   current,
		loadState: env.loadState,
		loadBlock: env.loadBlock,
		apply: func(ctx context.Context, target ton.BlockIDExt, previous []*tnstore.BlockState, downloaded PreparedBlock) (*tnstore.BlockState, error) {
			master, err := shardApplyMasterFromContext(ctx)
			if err != nil {
				return nil, err
			}
			state, err := env.apply(ctx, target, previous, downloaded)
			if err != nil {
				return nil, err
			}
			setShardStateMasterchainRef(state, master.Block)
			setShardBlockMasterchainRef(downloaded.Meta, master.Block)
			return state, nil
		},
	})
	return r
}

func waitForResolved(t *testing.T, r *nextSyncRunner, block ton.BlockIDExt) *tnstore.BlockState {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		r.shardResolver.mu.Lock()
		state := r.shardResolver.cache[tnstore.BlockKey(block)]
		r.shardResolver.mu.Unlock()
		if state != nil {
			return state
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("shard block %s was not resolved by the apply-ahead stage", tnstore.FormatBlockRef(block))
	return nil
}

func resolvedShardState(r *nextSyncRunner, block ton.BlockIDExt) *tnstore.BlockState {
	r.shardResolver.mu.Lock()
	defer r.shardResolver.mu.Unlock()
	return r.shardResolver.cache[tnstore.BlockKey(block)]
}

func TestShardApplyAheadContiguousGate(t *testing.T) {
	tests := []struct {
		name                        string
		seqno, lastAhead, committed uint32
		want                        bool
	}{
		{name: "first job after committed head", seqno: 11, committed: 10, want: true},
		{name: "follows the previous ahead job", seqno: 12, lastAhead: 11, committed: 10, want: true},
		{name: "gap after the previous ahead job", seqno: 15, lastAhead: 11, committed: 10},
		{name: "gap after the committed head", seqno: 15, committed: 10},
		{name: "already committed", seqno: 10, committed: 10},
		{name: "no committed head yet", seqno: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := shardApplyAheadContiguous(tc.seqno, tc.lastAhead, tc.committed); got != tc.want {
				t.Fatalf("shardApplyAheadContiguous(%d, %d, %d) = %v, want %v", tc.seqno, tc.lastAhead, tc.committed, got, tc.want)
			}
		})
	}
}

func TestShardApplyAheadStampsIncludingMaster(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	base := testBlockID(0, topShard, 100)
	firstTarget := testBlockID(0, topShard, 101)
	secondTarget := testBlockID(0, topShard, 102)
	masterOne := &tnstore.BlockState{Block: testMasterBlockID(51)}
	masterTwo := &tnstore.BlockState{Block: testMasterBlockID(52)}

	env := newFakeShardStateResolverEnv()
	env.addState(base)
	env.addBlock(firstTarget, base)
	env.addBlock(secondTarget, firstTarget)

	r := newShardApplyAheadTestRunner(ctx, env, map[tnstore.ShardKey]tnstore.BlockState{
		tnstore.ShardKeyFromBlock(base): {Block: base},
	})
	r.committedMasterSeqno.Store(50)
	r.startShardApplyAhead()

	r.scheduleShardApplyAhead(masterOne, []ton.BlockIDExt{firstTarget})
	first := waitForResolved(t, r, firstTarget)
	if first.MasterchainRef == nil || !first.MasterchainRef.Equals(&masterOne.Block) {
		t.Fatalf("first target masterchain ref = %v, want %s", first.MasterchainRef, tnstore.FormatBlockRef(masterOne.Block))
	}

	r.scheduleShardApplyAhead(masterTwo, []ton.BlockIDExt{secondTarget})
	second := waitForResolved(t, r, secondTarget)
	if second.MasterchainRef == nil || !second.MasterchainRef.Equals(&masterTwo.Block) {
		t.Fatalf("second target masterchain ref = %v, want %s", second.MasterchainRef, tnstore.FormatBlockRef(masterTwo.Block))
	}

	// The block included by the first master must keep it, even though the
	// second master's resolution ran afterwards.
	if !first.MasterchainRef.Equals(&masterOne.Block) {
		t.Fatalf("first target masterchain ref moved to %s", tnstore.FormatBlockRef(*first.MasterchainRef))
	}
}

// Regression gate: the stage drops jobs when it falls behind, and resolving a
// master across such a gap would walk its targets' prev chain back into shard
// blocks belonging to the skipped masters and stamp them with the wrong
// inclusion master.
func TestShardApplyAheadSkipsNonContiguousMaster(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	base := testBlockID(0, topShard, 100)
	skipped := testBlockID(0, topShard, 101)
	target := testBlockID(0, topShard, 102)
	skippedMaster := testMasterBlockID(52)
	farMaster := &tnstore.BlockState{Block: testMasterBlockID(53)}

	env := newFakeShardStateResolverEnv()
	env.addState(base)
	env.addBlock(skipped, base)
	env.addBlock(target, skipped)

	r := newShardApplyAheadTestRunner(ctx, env, map[tnstore.ShardKey]tnstore.BlockState{
		tnstore.ShardKeyFromBlock(base): {Block: base},
	})
	// Committed head is 51, so master 53 is two blocks ahead: master 52's shard
	// block was never resolved and this stage must not own it.
	r.committedMasterSeqno.Store(51)
	r.startShardApplyAhead()

	r.scheduleShardApplyAhead(farMaster, []ton.BlockIDExt{target})

	// Give the stage a chance to (wrongly) do the work.
	time.Sleep(50 * time.Millisecond)
	if state := resolvedShardState(r, target); state != nil {
		t.Fatalf("non-contiguous master target was resolved by the apply-ahead stage: %v", state.MasterchainRef)
	}
	if state := resolvedShardState(r, skipped); state != nil {
		t.Fatalf("skipped master's shard block was resolved by the apply-ahead stage: %v", state.MasterchainRef)
	}

	// The commit stage still resolves it, stamping the ancestor with the master
	// that actually includes it.
	skippedMasterState := &tnstore.BlockState{Block: skippedMaster}
	commitCtx := contextWithShardApplyMaster(ctx, skippedMasterState)
	if _, err := r.shardResolver.resolveWithContext(commitCtx, skipped); err != nil {
		t.Fatalf("commit-stage resolve of the skipped master target: %v", err)
	}
	state := resolvedShardState(r, skipped)
	if state == nil || state.MasterchainRef == nil || !state.MasterchainRef.Equals(&skippedMaster) {
		t.Fatalf("skipped master target masterchain ref = %v, want %s", state, tnstore.FormatBlockRef(skippedMaster))
	}
}

func TestShardApplyAheadNeverResolvesCurrentTip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tip := testBlockID(0, topShard, 100)
	master := &tnstore.BlockState{Block: testMasterBlockID(51)}

	env := newFakeShardStateResolverEnv()
	env.addState(tip)

	r := newShardApplyAheadTestRunner(ctx, env, map[tnstore.ShardKey]tnstore.BlockState{
		tnstore.ShardKeyFromBlock(tip): {Block: tip},
	})
	r.committedMasterSeqno.Store(50)
	r.startShardApplyAhead()

	r.scheduleShardApplyAhead(master, []ton.BlockIDExt{tip})
	time.Sleep(50 * time.Millisecond)

	// The commit stage carries an unchanged tip over from current.Shards without
	// touching the resolver, which is what preserves its original inclusion
	// master; the ahead stage must do the same.
	if state := resolvedShardState(r, tip); state != nil {
		t.Fatalf("current shard tip was re-resolved by the apply-ahead stage: %v", state.MasterchainRef)
	}
	env.mu.Lock()
	loads := env.stateLoads[tnstore.BlockKey(tip)]
	env.mu.Unlock()
	if loads != 0 {
		t.Fatalf("current shard tip state loads = %d, want 0", loads)
	}
}

func TestShardApplyAheadErrorDoesNotPoisonCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	base := testBlockID(0, topShard, 100)
	target := testBlockID(0, topShard, 101)
	master := &tnstore.BlockState{Block: testMasterBlockID(51)}

	env := newFakeShardStateResolverEnv()
	env.addState(base)
	// The block is missing, so the ahead stage fails to resolve it.

	r := newShardApplyAheadTestRunner(ctx, env, map[tnstore.ShardKey]tnstore.BlockState{
		tnstore.ShardKeyFromBlock(base): {Block: base},
	})
	r.committedMasterSeqno.Store(50)
	r.startShardApplyAhead()

	r.scheduleShardApplyAhead(master, []ton.BlockIDExt{target})
	time.Sleep(50 * time.Millisecond)

	// Only successes enter the resolver cache, and a failed task is dropped, so
	// the commit stage retries from scratch and succeeds.
	env.addBlock(target, base)
	commitCtx := contextWithShardApplyMaster(ctx, master)
	state, err := r.shardResolver.resolveWithContext(commitCtx, target)
	if err != nil {
		t.Fatalf("commit-stage resolve after a failed apply-ahead: %v", err)
	}
	if state.MasterchainRef == nil || !state.MasterchainRef.Equals(&master.Block) {
		t.Fatalf("masterchain ref = %v, want %s", state.MasterchainRef, tnstore.FormatBlockRef(master.Block))
	}
}

func TestShardApplyAheadRecorderIsHandedToCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	master := testMasterBlockID(51)
	other := testMasterBlockID(52)

	r := &nextSyncRunner{service: &SyncCoordinator{log: zerolog.Nop()}, ctx: ctx}
	recorder := r.shardAheadRecorder(master)
	if recorder == nil {
		t.Fatal("apply-ahead recorder was not created")
	}
	if again := r.shardAheadRecorder(master); again != recorder {
		t.Fatal("apply-ahead recorder is not reused for the same master")
	}
	recorder.observe(time.Now().Add(-time.Second), time.Now())

	// Later masters must not evict the one the commit stage is about to take:
	// the commit always runs behind this stage.
	r.shardAheadRecorder(other)
	r.shardAheadRecorder(testMasterBlockID(53))

	commitCtx := r.shardApplyAheadContext(ctx, master)
	got := shardObtainRecorderFromContext(commitCtx)
	if got != recorder {
		t.Fatal("commit stage did not receive the apply-ahead recorder")
	}
	if got.count() != 1 {
		t.Fatalf("apply-ahead recorder observations = %d, want 1", got.count())
	}

	// When the commit gets there first the recorder is created for it, and the
	// stage then shares that same one instead of recording into its own.
	fresh := testMasterBlockID(99)
	commitFirst := shardObtainRecorderFromContext(r.shardApplyAheadContext(ctx, fresh))
	if commitFirst == nil {
		t.Fatal("commit stage did not get a recorder for a master the stage has not started")
	}
	if stageRecorder := r.shardAheadRecorder(fresh); stageRecorder != commitFirst {
		t.Fatal("apply-ahead stage recorded into a second recorder")
	}

	// Consumed masters are dropped once the committed head passes them, so an
	// aborted run cannot leak recorders.
	r.committedMasterSeqno.Store(60)
	r.shardAheadRecorder(testMasterBlockID(61))
	r.shardAheadMu.Lock()
	_, keptConsumed := r.shardAheadRecorders[tnstore.BlockKey(master)]
	_, keptFuture := r.shardAheadRecorders[tnstore.BlockKey(fresh)]
	r.shardAheadMu.Unlock()
	if keptConsumed {
		t.Fatal("recorder for a master behind the committed head was kept")
	}
	if !keptFuture {
		t.Fatal("recorder for a master ahead of the committed head was dropped")
	}
}

// A checkpoint taken while a master's shard blocks are only apply-ahead
// resolved must not contain them: their staged metadata would point at a
// master that is absent from storage after an unclean restart. The commit of
// the inclusion master stages them, before the master's own entry, so the two
// can only ever land in the same checkpoint.
func TestShardCheckpointStagingWaitsForInclusionMasterCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	base := testBlockID(0, topShard, 100)
	aheadTarget := testBlockID(0, topShard, 101)
	commitTarget := testBlockID(0, topShard, 102)
	keyBlock := testMasterBlockID(50)
	master := testMonitorMasterStateWithKeyBlock(t, testMasterBlockID(51), 7, keyBlock, false)

	env := newFakeShardStateResolverEnv()
	env.addState(base)
	env.addBlock(aheadTarget, base)
	env.addBlock(commitTarget, aheadTarget)

	prewriteStore := &artifactPrewriterTestStore{}
	svc := &SyncCoordinator{
		log:               zerolog.Nop(),
		monitorSplitDepth: make(map[monitorSplitDepthKey]uint32),
		state:             &StateLifecycle{artifactPrewrite: newTestArtifactPrewriter(prewriteStore, 0)},
	}
	runCtx, cancelRun := context.WithCancel(ctx)
	r := &nextSyncRunner{
		service:    svc,
		ctx:        runCtx,
		cancel:     cancelRun,
		stateCells: newTestStateCellWindowCache(nil),
	}
	r.shardResolver = newShardStateResolver(runCtx, shardStateResolverConfig{
		current: map[tnstore.ShardKey]tnstore.BlockState{
			tnstore.ShardKeyFromBlock(base): {Block: base},
		},
		loadState:       env.loadState,
		loadBlock:       env.loadBlock,
		apply:           env.apply,
		afterApplyState: r.afterApplyShardState,
	})
	r.committedMasterSeqno.Store(50)
	stop := r.startShardApplyAhead()
	defer stop()

	r.scheduleShardApplyAhead(master, []ton.BlockIDExt{aheadTarget})
	waitForResolved(t, r, aheadTarget)

	// The commit stage resolves the second target itself, through the same
	// callback: both stagings must be deferred the same way.
	commitCtx := contextWithShardApplyMaster(runCtx, master)
	if _, err := r.shardResolver.resolveWithContext(commitCtx, commitTarget); err != nil {
		t.Fatalf("commit-stage resolve: %v", err)
	}

	checkpoint, _ := r.checkpoint()
	if len(checkpoint.entries) != 0 {
		t.Fatalf("checkpoint before the master commit contains %d entries, want none", len(checkpoint.entries))
	}

	if err := r.stageDeferredShardCheckpointStates(master.Block.SeqNo); err != nil {
		t.Fatalf("stage deferred shard checkpoint states: %v", err)
	}
	item := nextAppliedMaster{
		master: master,
		block:  testPreparedMasterchainBlock(keyBlock, master.Block),
	}
	if err := r.rememberMasterCheckpointState(item); err != nil {
		t.Fatalf("remember master checkpoint state: %v", err)
	}

	staged, _ := r.checkpoint()
	stagedBlocks := map[tnstore.BlockRootHash]bool{}
	for _, entry := range staged.entries {
		stagedBlocks[tnstore.BlockKey(entry.State.Block)] = true
	}
	for _, block := range []ton.BlockIDExt{aheadTarget, commitTarget, master.Block} {
		if !stagedBlocks[tnstore.BlockKey(block)] {
			t.Fatalf("checkpoint after the master commit is missing %s", tnstore.FormatBlockRef(block))
		}
	}

	// The artifact prewrite order is the staging order: shards strictly before
	// their inclusion master.
	svc.state.artifactPrewrite.queue.mu.Lock()
	var prewriteOrder []ton.BlockIDExt
	for _, job := range svc.state.artifactPrewrite.queue.jobs {
		prewriteOrder = append(prewriteOrder, job.value.State.Block)
	}
	svc.state.artifactPrewrite.queue.mu.Unlock()
	assertBlockSeq(t, "artifact prewrite", prewriteOrder, []ton.BlockIDExt{aheadTarget, commitTarget, master.Block})

	r.shardStageMu.Lock()
	pending := len(r.shardStageDeferred)
	r.shardStageMu.Unlock()
	if pending != 0 {
		t.Fatalf("deferred shard staging entries after the commit = %d, want 0", pending)
	}
}

func TestShardApplyCallbacksRequireInclusionMaster(t *testing.T) {
	ctx := context.Background()
	target := testBlockID(0, topShard, 101)
	r := &nextSyncRunner{service: &SyncCoordinator{log: zerolog.Nop()}, ctx: ctx}

	if _, err := r.applyResolvedShardBlock(ctx, target, nil, PreparedBlock{ID: target}); err == nil {
		t.Fatal("shard apply without an inclusion master was accepted")
	}
	if err := r.afterApplyShardState(ctx, &tnstore.BlockState{Block: target}, PreparedBlock{ID: target}, 0); err == nil {
		t.Fatal("shard after-apply without an inclusion master was accepted")
	}
}

// The stage applies shard states into the run's cell window and collects their
// deferred checkpoint staging, so the run must not return while it is still
// working.
func TestShardApplyAheadStopDrainsTheStage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	base := testBlockID(0, topShard, 100)
	target := testBlockID(0, topShard, 101)
	master := &tnstore.BlockState{Block: testMasterBlockID(51)}

	env := newFakeShardStateResolverEnv()
	env.addState(base)
	env.addBlock(target, base)

	r := newShardApplyAheadTestRunner(ctx, env, map[tnstore.ShardKey]tnstore.BlockState{
		tnstore.ShardKeyFromBlock(base): {Block: base},
	})
	r.committedMasterSeqno.Store(50)
	stop := r.startShardApplyAhead()

	r.scheduleShardApplyAhead(master, []ton.BlockIDExt{target})

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		stop()
	}()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("stopping the apply-ahead stage did not return")
	}

	// The worker goroutine is gone, so a later schedule cannot start new work.
	r.scheduleShardApplyAhead(master, []ton.BlockIDExt{testBlockID(0, topShard, 102)})
	time.Sleep(20 * time.Millisecond)
	if state := resolvedShardState(r, testBlockID(0, topShard, 102)); state != nil {
		t.Fatal("apply-ahead stage kept working after it was stopped")
	}
}

// The stage must keep the masters the commit stage reaches next, not the newest
// ones: during catch-up the master-apply stage runs far ahead, and keeping the
// newest would leave a gap the contiguity gate has to reject forever.
func TestShardApplyAheadKeepsLowestPendingMasters(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := &nextSyncRunner{service: &SyncCoordinator{log: zerolog.Nop()}, ctx: ctx, cancel: cancel}
	r.shardAheadPending = map[uint32]shardApplyAheadJob{}
	r.shardAheadWake = make(chan struct{}, 1)
	r.committedMasterSeqno.Store(50)

	target := []ton.BlockIDExt{testBlockID(0, topShard, 100)}
	for seqno := uint32(51); seqno <= 60; seqno++ {
		r.scheduleShardApplyAhead(&tnstore.BlockState{Block: testMasterBlockID(seqno)}, target)
	}

	r.shardAheadMu.Lock()
	pending := make([]uint32, 0, len(r.shardAheadPending))
	for seqno := range r.shardAheadPending {
		pending = append(pending, seqno)
	}
	r.shardAheadMu.Unlock()

	if len(pending) != shardApplyAheadPendingLimit {
		t.Fatalf("pending masters = %d, want %d", len(pending), shardApplyAheadPendingLimit)
	}
	for _, seqno := range pending {
		if seqno > 50+shardApplyAheadPendingLimit {
			t.Fatalf("pending masters kept %d, want only the lowest ones after the committed head", seqno)
		}
	}

	// The stage picks them up in order, so a run of masters is resolved even
	// while the master-apply stage keeps racing ahead.
	job, ok := r.takeShardApplyAheadJob(0)
	if !ok || job.master.Block.SeqNo != 51 {
		t.Fatalf("first job = %v (ok=%v), want master 51", job.master, ok)
	}
	job, ok = r.takeShardApplyAheadJob(51)
	if !ok || job.master.Block.SeqNo != 52 {
		t.Fatalf("second job = %v (ok=%v), want master 52", job.master, ok)
	}

	// A master the stage never resolved is not admissible until the commit
	// stage covers the gap itself.
	if _, ok = r.takeShardApplyAheadJob(0); ok {
		t.Fatal("a master behind the gap was admitted after the stage reset")
	}
	r.committedMasterSeqno.Store(52)
	if job, ok = r.takeShardApplyAheadJob(0); !ok || job.master.Block.SeqNo != 53 {
		t.Fatalf("job after the commit caught up = %v (ok=%v), want master 53", job.master, ok)
	}
}

func TestShardApplyAheadDropsCommittedMasters(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := &nextSyncRunner{service: &SyncCoordinator{log: zerolog.Nop()}, ctx: ctx, cancel: cancel}
	r.shardAheadPending = map[uint32]shardApplyAheadJob{}
	r.shardAheadWake = make(chan struct{}, 1)
	r.committedMasterSeqno.Store(60)

	target := []ton.BlockIDExt{testBlockID(0, topShard, 100)}
	r.scheduleShardApplyAhead(&tnstore.BlockState{Block: testMasterBlockID(55)}, target)

	r.shardAheadMu.Lock()
	pending := len(r.shardAheadPending)
	r.shardAheadMu.Unlock()
	if pending != 0 {
		t.Fatalf("pending masters at or below the committed head = %d, want 0", pending)
	}
}
