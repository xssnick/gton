package collator

import (
	"bytes"
	"context"
	"math"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
)

// viewRaceShardDescriptor is masterBuildShardDescriptor with the two topology
// flags exposed. They are what CurrentTargets reads to decide whether a
// registered shard is its own session's top or the parent/children of one, and
// that decision is the whole subject of the projection test below.
func viewRaceShardDescriptor(
	t *testing.T,
	block ton.BlockIDExt,
	genUtime uint32,
	beforeSplit bool,
	beforeMerge bool,
) *cell.Cell {
	t.Helper()

	descriptor := tlb.ShardDesc{
		SeqNo:              block.SeqNo,
		StartLT:            uint64(block.SeqNo) * 1_000,
		EndLT:              uint64(block.SeqNo)*1_000 + 999,
		RootHash:           bytes.Clone(block.RootHash),
		FileHash:           bytes.Clone(block.FileHash),
		BeforeSplit:        beforeSplit,
		BeforeMerge:        beforeMerge,
		NextValidatorShard: block.Shard,
		GenUTime:           genUtime,
		SplitMergeAt:       tlb.FutureSplitMergeNone{},
	}
	root, err := tlb.ToCell(&descriptor)
	if err != nil {
		t.Fatal(err)
	}

	return root
}

// viewRaceShardHashes wraps descriptors into the workchain-0 ShardHashes
// dictionary. One descriptor becomes the bintree root leaf; two become the
// depth-one fork, which is the only shape a split or a merge ever produces in
// these fixtures.
func viewRaceShardHashes(t *testing.T, descriptors ...*cell.Cell) *cell.Dictionary {
	t.Helper()

	leaf := func(descriptor *cell.Cell) *cell.Cell {
		return cell.BeginCell().MustStoreBoolBit(false).MustStoreBuilder(descriptor.ToBuilder()).EndCell()
	}
	var tree *cell.Cell
	switch len(descriptors) {
	case 1:
		tree = leaf(descriptors[0])
	case 2:
		tree = cell.BeginCell().
			MustStoreBoolBit(true).
			MustStoreRef(leaf(descriptors[0])).
			MustStoreRef(leaf(descriptors[1])).
			EndCell()
	default:
		t.Fatalf("shard hashes fixture supports one or two descriptors, got %d", len(descriptors))
	}

	dict := cell.NewDict(32)
	key := cell.BeginCell().MustStoreInt(0, 32).EndCell()
	if err := dict.Set(key, cell.BeginCell().MustStoreRef(tree).EndCell()); err != nil {
		t.Fatal(err)
	}

	return dict
}

// viewRaceShardBlock names a registered shard top. Only its identity matters
// here: the projection copies block ids and never opens the block itself.
func viewRaceShardBlock(shard int64, seqno uint32, fill byte) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: 0,
		Shard:     shard,
		SeqNo:     seqno,
		RootHash:  bytes.Repeat([]byte{fill}, 32),
		FileHash:  bytes.Repeat([]byte{fill ^ 0x5a}, 32),
	}
}

// viewRaceGroupSnapshot re-stamps the masterchain build fixture with an explicit
// shard registry at a non-zero seqno — CurrentTargets returns nothing but the
// masterchain group at seqno zero — and runs it through the real tracker, so the
// sessions under test are the ones a live node would hold.
func viewRaceGroupSnapshot(t *testing.T, hashes *cell.Dictionary) *groups.Snapshot {
	t.Helper()

	fixture := newMasterBuildFixture(t, false)
	extra := fixture.oldExtra
	extra.ShardHashes = hashes
	extraRoot, err := tlb.ToCell(&extra)
	if err != nil {
		t.Fatal(err)
	}
	state := fixture.oldState
	state.Seqno = 1
	state.McStateExtra = extraRoot
	stateRoot, err := tlb.ToCell(&state)
	if err != nil {
		t.Fatal(err)
	}
	stateHash := stateRoot.HashKey()
	block := ton.BlockIDExt{
		Workchain: masterchainWorkchainID,
		Shard:     math.MinInt64,
		SeqNo:     state.Seqno,
		RootHash:  bytes.Clone(stateHash[:]),
		FileHash:  bytes.Repeat([]byte{0x42}, 32),
	}

	tracker, err := groups.NewTracker(groups.TrackerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracked, err := tracker.Apply(groups.ApplyInput{
		Block: block,
		Root:  stateRoot,
		AsOf:  time.Unix(int64(state.GenUTime), 0),
	})
	if err != nil {
		t.Fatalf("derive validator group snapshot: %v", err)
	}

	return tracked.Snapshot
}

// viewRaceSessionUpdate projects one group through the collator's own session
// projection, which is the only producer of the SessionUpdate the runtime
// feeds to observeMasterchainFinalized.
func viewRaceSessionUpdate(snapshot *groups.Snapshot, group groups.Session) SessionUpdate {
	consensus := groups.SimplexConfig{Version: 2, ProtocolVersion: 2, SlotsPerLeaderWindow: 16}

	return projectCollatorSession(
		snapshot,
		group,
		nil,
		nil,
		consensus,
		OverlayRoleCollator,
		nil,
		true,
	).update
}

// Step 10 of the leader-window fix advances EmptyBlockPolicy.LastMCFinalizedSeqno
// from update.FinalizedBlock before the production barrier, so the watermark now
// moves while our own leader window is building blocks. That is only safe
// because the watermark can never name a shard top the masterchain registry does
// not also register for this session: the per-slot view pick admits a view by
// running admitRegisteredShardChain against the very same registry, so the
// policy and the pick cannot reach opposite verdicts and let a real block out.
//
// This is the identity that argument rests on. If the projection ever starts
// reporting a finalized block for a shard whose registry entry is the split
// parent or the merge children — a top this session never produced — the
// watermark would jump past our own chain and the +8 shard rule would silently
// stop degrading slots that the network has not caught up with.
func TestSessionUpdateFinalizedBlockIsTheRegisteredTopWhenLinear(t *testing.T) {
	const (
		leftShard  = int64(1) << 62
		rightShard = -(int64(1) << 62)
	)
	parent := viewRaceShardBlock(math.MinInt64, 17, 0x31)
	left := viewRaceShardBlock(leftShard, 21, 0x41)
	right := viewRaceShardBlock(rightShard, 23, 0x51)

	for _, test := range []struct {
		name string
		// hashes is the registered shard configuration of the masterchain state.
		hashes func(*testing.T) *cell.Dictionary
		// wantShards is the set of shard sessions the topology must produce.
		wantShards []groups.ShardID
		// wantFinalized is the block each session's watermark must name, or nil
		// where the session's own top does not exist yet.
		wantFinalized map[groups.ShardID]*ton.BlockIDExt
		// wantRegistered is what the registry lists for each session.
		wantRegistered map[groups.ShardID][]ton.BlockIDExt
	}{
		{
			name: "linear",
			hashes: func(t *testing.T) *cell.Dictionary {
				return viewRaceShardHashes(t, viewRaceShardDescriptor(t, parent, 100, false, false))
			},
			wantShards: []groups.ShardID{{Workchain: 0, Shard: math.MinInt64}},
			wantFinalized: map[groups.ShardID]*ton.BlockIDExt{
				{Workchain: 0, Shard: math.MinInt64}: &parent,
			},
			wantRegistered: map[groups.ShardID][]ton.BlockIDExt{
				{Workchain: 0, Shard: math.MinInt64}: {parent},
			},
		},
		{
			name: "after split",
			hashes: func(t *testing.T) *cell.Dictionary {
				return viewRaceShardHashes(t, viewRaceShardDescriptor(t, parent, 100, true, false))
			},
			wantShards: []groups.ShardID{
				{Workchain: 0, Shard: leftShard},
				{Workchain: 0, Shard: rightShard},
			},
			wantFinalized: map[groups.ShardID]*ton.BlockIDExt{
				{Workchain: 0, Shard: leftShard}:  nil,
				{Workchain: 0, Shard: rightShard}: nil,
			},
			wantRegistered: map[groups.ShardID][]ton.BlockIDExt{
				{Workchain: 0, Shard: leftShard}:  {parent},
				{Workchain: 0, Shard: rightShard}: {parent},
			},
		},
		{
			name: "after merge",
			hashes: func(t *testing.T) *cell.Dictionary {
				return viewRaceShardHashes(
					t,
					viewRaceShardDescriptor(t, left, 100, false, true),
					viewRaceShardDescriptor(t, right, 100, false, true),
				)
			},
			wantShards: []groups.ShardID{{Workchain: 0, Shard: math.MinInt64}},
			wantFinalized: map[groups.ShardID]*ton.BlockIDExt{
				{Workchain: 0, Shard: math.MinInt64}: nil,
			},
			wantRegistered: map[groups.ShardID][]ton.BlockIDExt{
				{Workchain: 0, Shard: math.MinInt64}: {left, right},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot := viewRaceGroupSnapshot(t, test.hashes(t))

			var shards []groups.ShardID
			for i := range snapshot.Active {
				group := snapshot.Active[i]
				if group.Shard.IsMasterchain() {
					continue
				}
				shards = append(shards, group.Shard)
				update := viewRaceSessionUpdate(snapshot, group)

				// The identity itself, asked of every shard session the
				// topology produces rather than only of the linear one.
				if update.HasFinalizedBlock {
					if len(update.Registered) != 1 || update.Registered[0].Shard != group.Shard {
						t.Fatalf("shard %v names a finalized block with registry %+v",
							group.Shard, update.Registered)
					}
					if !update.FinalizedBlock.Equals(&update.Registered[0].Block) {
						t.Fatalf("shard %v finalized block %v is not its registered top %v",
							group.Shard, update.FinalizedBlock, update.Registered[0].Block)
					}
				}

				want := test.wantFinalized[group.Shard]
				if (want != nil) != update.HasFinalizedBlock {
					t.Fatalf("shard %v has finalized block = %t, want %t",
						group.Shard, update.HasFinalizedBlock, want != nil)
				}
				if want != nil && !update.FinalizedBlock.Equals(want) {
					t.Fatalf("shard %v finalized block = %v, want %v",
						group.Shard, update.FinalizedBlock, *want)
				}

				wantRegistered := test.wantRegistered[group.Shard]
				if len(update.Registered) != len(wantRegistered) {
					t.Fatalf("shard %v registry = %+v, want %d descriptors",
						group.Shard, update.Registered, len(wantRegistered))
				}
				for i := range wantRegistered {
					if !update.Registered[i].Block.Equals(&wantRegistered[i]) {
						t.Fatalf("shard %v registry entry %d = %v, want %v",
							group.Shard, i, update.Registered[i].Block, wantRegistered[i])
					}
				}
			}
			if len(shards) != len(test.wantShards) {
				t.Fatalf("shard sessions = %+v, want %+v", shards, test.wantShards)
			}
			for i := range test.wantShards {
				if shards[i] != test.wantShards[i] {
					t.Fatalf("shard session %d = %v, want %v", i, shards[i], test.wantShards[i])
				}
			}
		})
	}
}

// viewRaceAcquisition is a live shard session standing on a real chain of
// masterchain blocks: every view in it was built by the masterchain collator,
// so its masterchain history, shard registry and validator groups are the ones
// the per-slot view pick reads on a node.
type viewRaceAcquisition struct {
	acquisition *LocalAcquisition
	session     ActivatedSession
	update      SessionUpdate
	snapshots   []*groups.Snapshot
	blocks      []PreviousBlock
}

func newViewRaceAcquisition(t *testing.T, views int) *viewRaceAcquisition {
	t.Helper()

	fixture := newMasterBuildFixture(t, false)

	// A shard zerostate is the only predecessor that needs no block root and no
	// masterchain reference of its own, which keeps the floor of the view pick
	// at the session minimum instead of at a block this fixture never built.
	var shardState tlb.ShardStateUnsplit
	if err := parseExact(&shardState, emptyCandidateRequest(t).Previous.State); err != nil {
		t.Fatal(err)
	}
	stats, err := (tlb.ShardStateStats{}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	shardState.Seqno = 0
	shardState.VertSeqno = fixture.oldState.VertSeqno
	shardState.GenUTime = fixture.oldState.GenUTime - 1
	shardState.GenLT = fixture.oldState.GenLT - 1_000
	shardState.MinRefMCSeqno = 0
	shardState.McStateExtra = nil
	shardState.Stats = stats
	shardRoot, err := tlb.ToCell(&shardState)
	if err != nil {
		t.Fatal(err)
	}
	shardHash := shardRoot.HashKey()
	shardBlock := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(shardState.ShardIdent.GetShardID()),
		SeqNo:     0,
		RootHash:  bytes.Clone(shardHash[:]),
		FileHash:  bytes.Repeat([]byte{0x31}, 32),
	}

	extra := fixture.oldExtra
	extra.ShardHashes = viewRaceShardHashes(
		t,
		viewRaceShardDescriptor(t, shardBlock, shardState.GenUTime, false, false),
	)
	extraRoot, err := tlb.ToCell(&extra)
	if err != nil {
		t.Fatal(err)
	}
	masterState := fixture.oldState
	masterState.McStateExtra = extraRoot
	masterRoot, err := tlb.ToCell(&masterState)
	if err != nil {
		t.Fatal(err)
	}
	masterHash := masterRoot.HashKey()
	fixture.request.Previous.ID.RootHash = bytes.Clone(masterHash[:])
	fixture.request.Previous.State = masterRoot
	// The registry must keep naming the shard zerostate: a new shard top would
	// move the session genesis to a block this fixture has no state for.
	fixture.request.ShardTops = nil

	tracker, err := groups.NewTracker(groups.TrackerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracked, err := tracker.Apply(groups.ApplyInput{
		Block: fixture.request.Previous.ID,
		Root:  masterRoot,
		AsOf:  time.Unix(int64(masterState.GenUTime), 0),
	})
	if err != nil {
		t.Fatalf("apply genesis masterchain state: %v", err)
	}
	fixture.request.Groups = tracked.Snapshot

	states := []localValidationState{
		{block: shardBlock, root: shardRoot},
		{block: fixture.request.Previous.ID, root: masterRoot},
	}
	request := fixture.request
	result := &viewRaceAcquisition{}
	for range views {
		candidate, buildErr := testBuilder().BuildMaster(context.Background(), request)
		if buildErr != nil {
			t.Fatalf("build masterchain view: %v", buildErr)
		}
		previous := candidatePrevious(t, candidate)
		applied, applyErr := tracker.Apply(groups.ApplyInput{
			Block: candidate.ID,
			Root:  candidate.State,
			AsOf:  time.Unix(int64(request.Header.GenUtime), 0),
		})
		if applyErr != nil {
			t.Fatalf("apply masterchain view %d: %v", candidate.ID.SeqNo, applyErr)
		}
		states = append(states, localValidationState{
			block: candidate.ID,
			root:  candidate.State,
			data:  previous.Block,
		})
		result.snapshots = append(result.snapshots, applied.Snapshot)
		result.blocks = append(result.blocks, previous)

		request.Previous = previous
		request.Groups = applied.Snapshot
		request.Header.GenUtime++
		request.Header.GenUtimeMS += 1_000
	}

	first := result.snapshots[0]
	var active *groups.Session
	for i := range first.Active {
		if !first.Active[i].Shard.IsMasterchain() {
			active = &first.Active[i]
			break
		}
	}
	if active == nil {
		t.Fatal("masterchain view registers no shard session")
	}
	validators := make([]SessionValidator, len(active.Validators))
	for i := range active.Validators {
		validators[i] = SessionValidator{
			PublicKey: active.Validators[i].PublicKey,
			ADNLID:    active.Validators[i].ADNL,
			Weight:    active.Validators[i].Weight,
		}
	}
	result.session = ActivatedSession{
		Session: Session{
			ID:                   active.ID,
			Shard:                active.Shard,
			CatchainSeqno:        active.CatchainSeqno,
			ValidatorSetHash:     active.ValidatorSetHash,
			ConsensusVersion:     2,
			ProtocolVersion:      3,
			SlotsPerLeaderWindow: 16,
			Validators:           validators,
		},
		Genesis:        active.Genesis,
		MinMasterchain: active.MinMasterchain,
	}
	result.update = SessionUpdate{
		SessionID:                 active.ID,
		TargetRate:                400 * time.Millisecond,
		MasterchainBlock:          cloneBlockID(first.MasterchainBlock),
		Registered:                append([]groups.ShardDescription(nil), active.Registered...),
		HasCurrentWindow:          true,
		CurrentWindowStart:        result.session.SlotsPerLeaderWindow,
		CurrentWindowObservedSlot: result.session.SlotsPerLeaderWindow,
		CurrentWindowStartAt:      time.Unix(int64(masterState.GenUTime+10), 0),
	}
	if active.FinalizedBlock != nil {
		result.update.HasFinalizedBlock = true
		result.update.FinalizedBlock = cloneBlockID(*active.FinalizedBlock)
	}

	pool := msgpool.New(msgpool.Config{})
	t.Cleanup(pool.Close)
	if err = pool.Internals().ReconcileDestinations([]msgpool.ShardIdent{
		targetShardIdent(result.session.Shard),
	}); err != nil {
		t.Fatal(err)
	}
	result.acquisition, err = NewLocalAcquisition(LocalAcquisitionOptions{
		Builder:   testBuilder(),
		Store:     &localValidationStore{states: states},
		Groups:    &localAcquisitionTestGroups{snapshot: first},
		Messages:  pool,
		Semantics: testCandidateTransitionVerifier,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = result.acquisition.PublishMasterchainView(
		context.Background(),
		first,
		result.blocks[0].Block,
		result.blocks[0].State,
	); err != nil {
		t.Fatalf("publish first masterchain view: %v", err)
	}
	if err = result.acquisition.PrepareSession(
		context.Background(),
		result.session.Session,
		result.update,
	); err != nil {
		t.Fatalf("prepare shard session: %v", err)
	}
	if err = result.acquisition.ActivateSession(context.Background(), SessionActivation{
		SessionID:      result.session.ID,
		Genesis:        result.session.Genesis,
		MinMasterchain: result.session.MinMasterchain,
	}, result.update); err != nil {
		t.Fatalf("activate shard session: %v", err)
	}

	return result
}

// commitMasterchainSources stands in for the node's applied-block feed, which
// commits every masterchain source into the pool as it is applied. Without it
// only the view the session was activated on has its neighbour tops committed,
// the pick keeps preferring that one, and the concurrent publishes below would
// never reach the selection at all.
func (f *viewRaceAcquisition) commitMasterchainSources(t *testing.T) {
	t.Helper()

	managed, err := f.acquisition.session(f.session.ID)
	if err != nil {
		t.Fatal(err)
	}
	for i := range f.blocks {
		ref, refErr := localSourceRef(f.blocks[i].ID)
		if refErr != nil {
			t.Fatal(refErr)
		}
		if _, _, err = managed.branch.SeedSourceFromStateRoot(
			blockShardIdent(f.blocks[i].ID),
			ref,
			f.blocks[i].State,
		); err != nil {
			t.Fatalf("seed masterchain source %d: %v", f.blocks[i].ID.SeqNo, err)
		}
	}
}

// Until this change a shard build stamped managed.master — the view its session
// update had pinned, reachable under the session mutex alone. The per-slot pick
// replaced it with the resident view, and that added two lock edges nothing had
// before: AcquireShard now reads localMasterViewCache.mu while it holds the
// session mutex, and PublishMasterchainView now hands its warm-up goroutine the
// session mutex and the pool branch behind it. Both edges are taken on every
// masterchain block against every leader window, so an inverted one is not a
// rare deadlock, it is a node that stops producing the first time a refresh
// lands mid-window — the exact failure this whole change exists to remove.
//
// The membership assertion is the other half: a build must stamp a masterchain
// view that was actually installed, never a half-published one. Racing the
// publish loop against the acquisition loop is what makes a torn read possible
// at all.
func TestAcquireShardConcurrentWithMasterchainPublish(t *testing.T) {
	const acquisitions = 48

	fixture := newViewRaceAcquisition(t, 8)
	fixture.commitMasterchainSources(t)

	published := make(map[[32]byte]struct{}, len(fixture.blocks))
	for i := range fixture.blocks {
		key, err := blockRootKey(fixture.blocks[i].ID)
		if err != nil {
			t.Fatal(err)
		}
		published[key] = struct{}{}
	}

	request := BuildRequest{
		Session: fixture.session,
		Update:  fixture.update,
		Slot:    fixture.update.CurrentWindowStart,
	}
	done := make(chan struct{})
	var (
		wg         sync.WaitGroup
		publishErr error
		acquireErr error
		selected   []ton.BlockIDExt
		completed  int
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Cycles until the acquisitions finish: an install refuses to move the
		// resident view backwards, so the later sweeps take the same locks
		// without changing what the pick can see.
		for {
			for i := range fixture.snapshots {
				select {
				case <-done:
					return
				default:
				}
				if err := fixture.acquisition.PublishMasterchainView(
					context.Background(),
					fixture.snapshots[i],
					fixture.blocks[i].Block,
					fixture.blocks[i].State,
				); err != nil {
					publishErr = err

					return
				}
				runtime.Gosched()
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(done)
		for range acquisitions {
			result, err := fixture.acquisition.AcquireShard(context.Background(), request)
			if err != nil {
				acquireErr = err

				return
			}
			selected = append(selected, result.Masterchain.ID)
			completed++
		}
	}()
	wg.Wait()

	if publishErr != nil {
		t.Fatalf("publish masterchain view: %v", publishErr)
	}
	if acquireErr != nil {
		t.Fatalf("acquire shard beside a masterchain publish: %v", acquireErr)
	}
	if completed != acquisitions {
		t.Fatalf("completed %d acquisitions, want %d", completed, acquisitions)
	}
	for i := range selected {
		key, err := blockRootKey(selected[i])
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := published[key]; !ok {
			t.Fatalf("acquisition %d stamped masterchain view %v, which was never published", i, selected[i])
		}
	}

	// Installing is idempotent, so replaying the sweep only settles the case
	// where the acquisitions outran the publisher's first pass.
	for i := range fixture.snapshots {
		if err := fixture.acquisition.PublishMasterchainView(
			context.Background(),
			fixture.snapshots[i],
			fixture.blocks[i].Block,
			fixture.blocks[i].State,
		); err != nil {
			t.Fatalf("republish masterchain view %d: %v", fixture.blocks[i].ID.SeqNo, err)
		}
	}

	// With every view installed the pick has to see the newest one. Without
	// this the test would still pass if the pick silently ignored the resident
	// view and kept building on the one its session update pinned.
	newest := fixture.blocks[len(fixture.blocks)-1].ID
	result, err := fixture.acquisition.AcquireShard(context.Background(), request)
	if err != nil {
		t.Fatalf("acquire shard after the publish loop: %v", err)
	}
	if !result.Masterchain.ID.Equals(&newest) {
		t.Fatalf("settled masterchain view = %v, want the newest published %v", result.Masterchain.ID, newest)
	}
}
