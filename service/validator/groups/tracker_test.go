package groups

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestNewTrackerRejectsInvalidUnsafeRotations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		rules []UnsafeRotationRule
	}{
		{
			name:  "zero id",
			rules: []UnsafeRotationRule{{CatchainSeqno: 7}},
		},
		{
			name: "duplicate catchain seqno",
			rules: []UnsafeRotationRule{
				{CatchainSeqno: 7, ID: 1},
				{CatchainSeqno: 7, ID: 2},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if _, err := NewTracker(TrackerOptions{UnsafeRotations: test.rules}); err == nil {
				t.Fatal("NewTracker accepted invalid unsafe rotations")
			}
		})
	}
}

func TestUnsafeRotationStartsAtConfiguredMasterchainSeqno(t *testing.T) {
	t.Parallel()

	tracker, err := NewTracker(TrackerOptions{UnsafeRotations: []UnsafeRotationRule{{
		CatchainSeqno:        11,
		FromMasterchainSeqno: 100,
		ID:                   9,
	}}})
	if err != nil {
		t.Fatal(err)
	}

	if got := tracker.unsafeRotationID(99, 11); got != 0 {
		t.Fatalf("rotation before activation = %d, want 0", got)
	}
	if got := tracker.unsafeRotationID(100, 11); got != 9 {
		t.Fatalf("rotation at activation = %d, want 9", got)
	}
	if got := tracker.unsafeRotationID(101, 12); got != 0 {
		t.Fatalf("rotation for other catchain = %d, want 0", got)
	}
}

func TestSnapshotBeforeFirstApplyReturnsNotFound(t *testing.T) {
	t.Parallel()

	tracker, err := NewTracker(TrackerOptions{})
	if err != nil {
		t.Fatal(err)
	}

	_, err = tracker.Snapshot()
	if !errors.Is(err, ErrNoSnapshot) {
		t.Fatalf("Snapshot error = %v, want ErrNoSnapshot", err)
	}
}

func TestTrackerRejectsKeyStateWithoutAllShardsRotation(t *testing.T) {
	t.Parallel()

	current := buildTestValidatorSet(t, []testValidatorWire{{
		index: 0, key: groupTestBytes(1), weight: 1,
	}}, true, 1)
	shard := ShardID{Workchain: 0, Shard: masterchainShard}
	fixture := buildStateFixture(t, stateFixtureOptions{
		Seqno:         100,
		AfterKeyBlock: true,
		ConfigRoot: buildTestConfig(t, map[uint32]*cell.Cell{
			configParamCurrentValidators: current,
		}),
		ShardHashes: testShardHashes(t, testBinTreeLeaf(
			testParsedShardDescription(t, shard, 17, 91, tlb.FutureSplitMergeNone{}),
		)),
	})
	tracker, err := NewTracker(TrackerOptions{})
	if err != nil {
		t.Fatal(err)
	}

	_, err = tracker.Apply(ApplyInput{
		Block: fixture.Block,
		Root:  fixture.Root,
		AsOf:  time.Unix(1_700_000_000, 0),
	})
	if err == nil {
		t.Fatal("Tracker accepted a key state without rotated_all_shards")
	}
}

func TestTrackerApplyBuildsAndReusesSessions(t *testing.T) {
	current := buildTestValidatorSet(t, []testValidatorWire{{
		index: 0, key: groupTestBytes(1), weight: 10,
	}}, true, 10)
	configRoot := buildTestConfig(t, map[uint32]*cell.Cell{
		configParamCurrentValidators: current,
	})
	shard := ShardID{Workchain: 0, Shard: -1 << 63}
	first := buildStateFixture(t, stateFixtureOptions{
		Seqno:            100,
		GenUTime:         1_700_000_000,
		CatchainSeqno:    44,
		RotatedAllShards: true,
		ConfigRoot:       configRoot,
		ShardHashes: testShardHashes(t, testBinTreeLeaf(
			testParsedShardDescription(t, shard, 17, 91, tlb.FutureSplitMergeNone{}),
		)),
	})
	tracker, err := NewTracker(TrackerOptions{MaximalVerticalSeqno: 3})
	if err != nil {
		t.Fatal(err)
	}

	result, err := tracker.Apply(ApplyInput{
		Block: first.Block,
		Root:  first.Root,
		AsOf:  time.Unix(1_700_000_000, 0),
	})
	if err != nil {
		t.Fatalf("apply first state: %v", err)
	}
	if !result.Snapshot.Ready || len(result.Snapshot.Active) != 2 || len(result.Snapshot.Future) != 0 {
		t.Fatalf("first snapshot = %+v", result.Snapshot)
	}
	if !result.Snapshot.LifecycleEnabled {
		t.Fatal("default lifecycle threshold did not enable group management")
	}
	if len(result.Transitions) != 2 {
		t.Fatalf("first transitions = %+v, want two starts", result.Transitions)
	}

	activeMaster := trackerSessionForShard(t, result.Snapshot.Active, masterchainShardID())
	activeShard := trackerSessionForShard(t, result.Snapshot.Active, shard)
	if activeMaster.CatchainSeqno != 44 || activeShard.CatchainSeqno != 91 {
		t.Fatalf("unexpected catchain seqnos: active mc=%d shard=%d",
			activeMaster.CatchainSeqno, activeShard.CatchainSeqno)
	}
	if activeMaster.Validators[0].Weight != 10 || activeShard.Validators[0].Weight != 1 {
		t.Fatalf("unexpected selected weights: mc=%d shard=%d", activeMaster.Validators[0].Weight, activeShard.Validators[0].Weight)
	}
	if activeMaster.MinMasterchain.SeqNo != 100 || activeShard.MinMasterchain.SeqNo != 100 ||
		activeShard.Genesis[0].SeqNo != 17 {
		t.Fatalf("unexpected first genesis: mc=%+v shard=%+v", activeMaster, activeShard)
	}
	if activeMaster.FinalizedBlock.SeqNo != 100 || activeShard.FinalizedBlock.SeqNo != 17 {
		t.Fatalf("unexpected first finalized blocks: mc=%+v shard=%+v", activeMaster.FinalizedBlock, activeShard.FinalizedBlock)
	}
	if len(activeMaster.Registered) != 0 || len(activeShard.Registered) != 1 ||
		activeShard.Registered[0].Shard != shard || activeShard.Registered[0].Block.SeqNo != 17 {
		t.Fatalf("unexpected first registered shard context: mc=%+v shard=%+v", activeMaster.Registered, activeShard.Registered)
	}
	if len(result.Snapshot.PersistentOverlay) != 1 ||
		result.Snapshot.PersistentOverlay[0].ADNL != result.Snapshot.Config.ActiveValidators.Validators[0].PublicKeyHash {
		t.Fatalf("persistent overlay = %x", result.Snapshot.PersistentOverlay)
	}
	if keys := result.Snapshot.PersistentOverlay[0].ValidatorKeyIDs; len(keys) != 1 ||
		keys[0] != result.Snapshot.Config.ActiveValidators.Validators[0].PublicKeyHash {
		t.Fatalf("persistent overlay validator keys = %x", keys)
	}
	if want := mustDecodeHash(t, "0276a9216fc98397cc0b11a3f246bb526394e4dae7daa65cf4a6194aecf0b3c3"); activeMaster.ID != want {
		t.Fatalf("active masterchain session id = %x, want %x", activeMaster.ID, want)
	}
	if want := mustDecodeHash(t, "9fd1cf22f449e4b04d5a898ba378fede896cb298c59cce596f283f88916ddd9e"); activeShard.ID != want {
		t.Fatalf("active shard session id = %x, want %x", activeShard.ID, want)
	}
	replayed, err := tracker.Apply(ApplyInput{Block: first.Block, Root: first.Root, AsOf: time.Unix(1_700_000_001, 0)})
	if err != nil {
		t.Fatalf("replay first state: %v", err)
	}
	if replayed.Snapshot != result.Snapshot || len(replayed.Transitions) != 0 {
		t.Fatal("idempotent replay rebuilt the snapshot")
	}

	second := buildStateFixture(t, stateFixtureOptions{
		Seqno:         101,
		GenUTime:      1_700_000_001,
		CatchainSeqno: 44,
		ConfigRoot:    configRoot,
		ShardHashes: testShardHashes(t, testBinTreeLeaf(
			testParsedShardDescription(t, shard, 18, 91, tlb.FutureSplitMergeNone{}),
		)),
	})
	updated, err := tracker.Apply(ApplyInput{Block: second.Block, Root: second.Root, AsOf: time.Unix(1_700_000_001, 0)})
	if err != nil {
		t.Fatalf("apply second state: %v", err)
	}
	if !updated.Snapshot.Ready || updated.Snapshot.Config != result.Snapshot.Config || len(updated.Transitions) != 2 {
		t.Fatalf("second snapshot did not reuse sticky state/config: %+v", updated)
	}
	for i := range updated.Transitions {
		if updated.Transitions[i].Kind != TransitionPrepared {
			t.Fatalf("second transition %d = %s, want prepared", i, updated.Transitions[i].Kind)
		}
	}
	futureMaster := trackerSessionForShard(t, updated.Snapshot.Future, masterchainShardID())
	futureShard := trackerSessionForShard(t, updated.Snapshot.Future, shard)
	if futureMaster.CatchainSeqno != 45 || futureShard.CatchainSeqno != 92 {
		t.Fatalf("unexpected future catchain seqnos: mc=%d shard=%d", futureMaster.CatchainSeqno, futureShard.CatchainSeqno)
	}
	if want := mustDecodeHash(t, "7d8a0c6127217ee93aa6bed7e14acd553bc9b28c1e9f2caf56e7b850ad026398"); futureMaster.ID != want {
		t.Fatalf("future masterchain session id = %x, want %x", futureMaster.ID, want)
	}
	if want := mustDecodeHash(t, "25e01b7bc86ee8e3ba455e536443a252c2679b3c5b910c10611e5905e4b38647"); futureShard.ID != want {
		t.Fatalf("future shard session id = %x, want %x", futureShard.ID, want)
	}
	updatedShard := trackerSessionForShard(t, updated.Snapshot.Active, shard)
	if updatedShard.ID != activeShard.ID || updatedShard.MinMasterchain.SeqNo != 100 || updatedShard.Genesis[0].SeqNo != 17 ||
		updatedShard.FinalizedBlock.SeqNo != 18 {
		t.Fatalf("active session was not preserved and advanced: %+v", updatedShard)
	}
	if len(updatedShard.Registered) != 1 || updatedShard.Registered[0].Block.SeqNo != 18 {
		t.Fatalf("active session did not refresh registered shard context: %+v", updatedShard.Registered)
	}
	historical, err := tracker.Project(nil, ApplyInput{
		Block: first.Block,
		Root:  first.Root,
		AsOf:  time.Unix(1_700_000_002, 0),
	})
	if err != nil {
		t.Fatalf("load retained historical snapshot: %v", err)
	}
	if historical != result.Snapshot || !historical.Ready {
		t.Fatal("historical projection lost the canonical inherited lifecycle state")
	}

	if _, err = tracker.Apply(ApplyInput{Block: first.Block, Root: first.Root, AsOf: time.Unix(1_700_000_002, 0)}); !errors.Is(err, ErrStaleMasterchainState) {
		t.Fatalf("stale apply error = %v, want ErrStaleMasterchainState", err)
	}
	conflict := second.Block
	conflict.FileHash = testHash(0x33)
	if _, err = tracker.Apply(ApplyInput{Block: conflict, Root: second.Root, AsOf: time.Unix(1_700_000_002, 0)}); !errors.Is(err, ErrConflictingMasterchainState) {
		t.Fatalf("conflicting apply error = %v, want ErrConflictingMasterchainState", err)
	}
	badBlock := *second.Block.Copy()
	badBlock.SeqNo++
	if _, err = tracker.Apply(ApplyInput{
		Block: badBlock,
		Root:  cell.BeginCell().MustStoreUInt(0xbad, 32).EndCell(),
		AsOf:  time.Unix(1_700_000_002, 0),
	}); err == nil {
		t.Fatal("malformed state was accepted")
	}
	committed, err := tracker.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if committed != updated.Snapshot {
		t.Fatal("failed Apply replaced the committed snapshot")
	}
}

func TestTrackerProjectMatchesApplyWithoutPublishing(t *testing.T) {
	current := buildTestValidatorSet(t, []testValidatorWire{{
		index: 0, key: groupTestBytes(1), weight: 10,
	}}, true, 10)
	configRoot := buildTestConfig(t, map[uint32]*cell.Cell{
		configParamCurrentValidators: current,
	})
	shard := ShardID{Workchain: 0, Shard: -1 << 63}
	first := buildStateFixture(t, stateFixtureOptions{
		Seqno:            100,
		GenUTime:         1_700_000_000,
		CatchainSeqno:    44,
		RotatedAllShards: true,
		ConfigRoot:       configRoot,
		ShardHashes: testShardHashes(t, testBinTreeLeaf(
			testParsedShardDescription(t, shard, 17, 91, tlb.FutureSplitMergeNone{}),
		)),
	})
	second := buildStateFixture(t, stateFixtureOptions{
		Seqno:            101,
		GenUTime:         1_700_000_001,
		CatchainSeqno:    44,
		RotatedAllShards: false,
		ConfigRoot:       configRoot,
		ShardHashes: testShardHashes(t, testBinTreeLeaf(
			testParsedShardDescription(t, shard, 18, 91, tlb.FutureSplitMergeNone{}),
		)),
	})
	tracker, err := NewTracker(TrackerOptions{MaximalVerticalSeqno: 3})
	if err != nil {
		t.Fatal(err)
	}
	baseResult, err := tracker.Apply(ApplyInput{
		Block: first.Block,
		Root:  first.Root,
		AsOf:  time.Unix(1_700_000_000, 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	input := ApplyInput{
		Block: second.Block,
		Root:  second.Root,
		AsOf:  time.Unix(1_700_000_001, 0),
	}
	projected, err := tracker.Project(baseResult.Snapshot, input)
	if err != nil {
		t.Fatal(err)
	}
	stillBase, err := tracker.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if stillBase != baseResult.Snapshot {
		t.Fatal("Project published or rebuilt the tracker snapshot")
	}
	applied, err := tracker.Apply(input)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(projected, applied.Snapshot) {
		t.Fatal("Project and Apply derived different snapshots")
	}
}

// TestTrackerReusedActiveSessionsMatchFreshDerivation proves the immutable half
// of a reused active session is exactly what a from-scratch derivation of the
// same state produces, and that every gate of the reuse actually gates.
func TestTrackerReusedActiveSessionsMatchFreshDerivation(t *testing.T) {
	current := buildTestValidatorSet(t, []testValidatorWire{{
		index: 0, key: groupTestBytes(1), weight: 10,
	}}, true, 10)
	configRoot := buildTestConfig(t, map[uint32]*cell.Cell{
		configParamCurrentValidators: current,
	})
	shard := ShardID{Workchain: 0, Shard: -1 << 63}
	first := buildStateFixture(t, stateFixtureOptions{
		Seqno:            100,
		GenUTime:         1_700_000_000,
		CatchainSeqno:    44,
		RotatedAllShards: true,
		ConfigRoot:       configRoot,
		ShardHashes: testShardHashes(t, testBinTreeLeaf(
			testParsedShardDescription(t, shard, 17, 91, tlb.FutureSplitMergeNone{}),
		)),
	})
	second := buildStateFixture(t, stateFixtureOptions{
		Seqno:         101,
		GenUTime:      1_700_000_001,
		CatchainSeqno: 44,
		ConfigRoot:    configRoot,
		ShardHashes: testShardHashes(t, testBinTreeLeaf(
			testParsedShardDescription(t, shard, 18, 91, tlb.FutureSplitMergeNone{}),
		)),
	})

	tracker, err := NewTracker(TrackerOptions{MaximalVerticalSeqno: 3})
	if err != nil {
		t.Fatal(err)
	}
	base, err := tracker.Apply(ApplyInput{Block: first.Block, Root: first.Root, AsOf: time.Unix(1_700_000_000, 0)})
	if err != nil {
		t.Fatalf("apply first state: %v", err)
	}
	updated, err := tracker.Apply(ApplyInput{Block: second.Block, Root: second.Root, AsOf: time.Unix(1_700_000_001, 0)})
	if err != nil {
		t.Fatalf("apply second state: %v", err)
	}

	state, err := ParseState(StateInput{Block: second.Block, Root: second.Root})
	if err != nil {
		t.Fatalf("parse second state: %v", err)
	}
	if reused := reusableActiveSessions(base.Snapshot, updated.Snapshot.Config, state.LastKeyBlockSeqno, 0); len(reused) == 0 {
		t.Fatal("steady-state apply did not reuse any active session")
	}

	fresh, err := tracker.buildActiveSessions(nil, state, updated.Snapshot.Config, 0)
	if err != nil {
		t.Fatalf("derive active sessions from scratch: %v", err)
	}
	if len(fresh) != len(updated.Snapshot.Active) {
		t.Fatalf("fresh active sessions = %d, want %d", len(fresh), len(updated.Snapshot.Active))
	}
	for i := range fresh {
		applied := updated.Snapshot.Active[i]
		if applied.Shard != fresh[i].Shard || applied.CatchainSeqno != fresh[i].CatchainSeqno {
			t.Fatalf("session %d addresses %+v/%d, want %+v/%d",
				i, applied.Shard, applied.CatchainSeqno, fresh[i].Shard, fresh[i].CatchainSeqno)
		}
		if applied.ID != fresh[i].ID {
			t.Fatalf("session %d id = %x, want %x", i, applied.ID, fresh[i].ID)
		}
		if applied.ValidatorSetHash != fresh[i].ValidatorSetHash {
			t.Fatalf("session %d validator set hash = %08x, want %08x",
				i, applied.ValidatorSetHash, fresh[i].ValidatorSetHash)
		}
		if !reflect.DeepEqual(applied.Validators, fresh[i].Validators) {
			t.Fatalf("session %d roster = %+v, want %+v", i, applied.Validators, fresh[i].Validators)
		}
	}

	otherConfig, err := ParseConfig(configRoot)
	if err != nil {
		t.Fatalf("reparse config: %v", err)
	}
	gates := []struct {
		name              string
		config            *Config
		lastKeyBlockSeqno uint32
		rotationID        uint32
	}{
		{name: "reparsed config", config: otherConfig, lastKeyBlockSeqno: state.LastKeyBlockSeqno},
		{name: "last key block seqno", config: updated.Snapshot.Config, lastKeyBlockSeqno: state.LastKeyBlockSeqno + 1},
		{name: "rotation id", config: updated.Snapshot.Config, lastKeyBlockSeqno: state.LastKeyBlockSeqno, rotationID: 9},
	}
	for _, gate := range gates {
		t.Run(gate.name, func(t *testing.T) {
			if reused := reusableActiveSessions(base.Snapshot, gate.config, gate.lastKeyBlockSeqno, gate.rotationID); reused != nil {
				t.Fatalf("%s did not gate active session reuse", gate.name)
			}
		})
	}
}

// TestTrackerFutureSessionsFollowTentativeSetBoundary pins the one input that
// moves a tentative roster while the configuration cell is byte-identical:
// SelectTentativeValidatorSet flips the current set for the next one purely on
// gen_utime crossing (gen_utime/lifetime+1)*lifetime. Any future-session reuse
// keyed on the config pointer, the shard and the catchain seqno would serve a
// stale session id right here, which is a consensus hazard.
func TestTrackerFutureSessionsFollowTentativeSetBoundary(t *testing.T) {
	// The default catchain lifetime is 200 seconds, so the boundary for
	// gen_utime 1_699_999_999 is 1_700_000_000 and the next set is not eligible
	// yet, while for 1_700_000_001 it is 1_700_000_200 and the set is.
	const nextSetSince = uint32(1_700_000_200)

	current := buildTestValidatorSet(t, []testValidatorWire{{
		index: 0, key: groupTestBytes(1), weight: 10,
	}}, true, 10)
	next := buildTestValidatorSetSince(t, []testValidatorWire{{
		index: 0, key: groupTestBytes(2), weight: 10,
	}}, true, 10, nextSetSince)
	configRoot := buildTestConfig(t, map[uint32]*cell.Cell{
		configParamCurrentValidators: current,
		configParamNextValidators:    next,
	})
	shard := ShardID{Workchain: 0, Shard: -1 << 63}
	fixture := func(seqno, genUTime uint32, rotated bool, shardSeqno uint32) StateInput {
		return buildStateFixture(t, stateFixtureOptions{
			Seqno:            seqno,
			GenUTime:         genUTime,
			CatchainSeqno:    44,
			RotatedAllShards: rotated,
			ConfigRoot:       configRoot,
			ShardHashes: testShardHashes(t, testBinTreeLeaf(
				testParsedShardDescription(t, shard, shardSeqno, 91, tlb.FutureSplitMergeNone{}),
			)),
		})
	}

	tracker, err := NewTracker(TrackerOptions{MaximalVerticalSeqno: 3})
	if err != nil {
		t.Fatal(err)
	}
	rotation := fixture(100, 1_699_999_998, true, 17)
	if _, err = tracker.Apply(ApplyInput{
		Block: rotation.Block,
		Root:  rotation.Root,
		AsOf:  time.Unix(1_699_999_998, 0),
	}); err != nil {
		t.Fatalf("apply rotation state: %v", err)
	}

	beforeState := fixture(101, 1_699_999_999, false, 18)
	before, err := tracker.Apply(ApplyInput{
		Block: beforeState.Block,
		Root:  beforeState.Root,
		AsOf:  time.Unix(1_699_999_999, 0),
	})
	if err != nil {
		t.Fatalf("apply state before the tentative boundary: %v", err)
	}
	afterState := fixture(102, 1_700_000_001, false, 19)
	after, err := tracker.Apply(ApplyInput{
		Block: afterState.Block,
		Root:  afterState.Root,
		AsOf:  time.Unix(1_700_000_001, 0),
	})
	if err != nil {
		t.Fatalf("apply state after the tentative boundary: %v", err)
	}

	// Everything a memo could plausibly key on is unchanged across the boundary.
	if before.Snapshot.Config != after.Snapshot.Config {
		t.Fatal("configuration was reparsed, the boundary is no longer isolated")
	}
	if before.Snapshot.LastKeyBlockSeqno != after.Snapshot.LastKeyBlockSeqno {
		t.Fatalf("last key block seqno changed: %d then %d",
			before.Snapshot.LastKeyBlockSeqno, after.Snapshot.LastKeyBlockSeqno)
	}

	beforeSessions := trackerSessionsForShard(before.Snapshot.Future, masterchainShardID())
	afterSessions := trackerSessionsForShard(after.Snapshot.Future, masterchainShardID())
	if len(beforeSessions) != 1 {
		t.Fatalf("future masterchain sessions before the boundary = %d, want 1", len(beforeSessions))
	}
	if got := beforeSessions[0].Validators[0].PublicKey; got != groupTestBytes(1) {
		t.Fatalf("tentative roster before the boundary = %x, want the current set", got)
	}

	var promoted *Session
	for i := range afterSessions {
		if afterSessions[i].Validators[0].PublicKey == groupTestBytes(2) {
			promoted = &afterSessions[i]
		}
	}
	if promoted == nil {
		t.Fatal("tentative roster did not move to the next set across the boundary")
	}
	if promoted.CatchainSeqno != beforeSessions[0].CatchainSeqno {
		t.Fatalf("tentative catchain seqno changed: %d then %d",
			beforeSessions[0].CatchainSeqno, promoted.CatchainSeqno)
	}
	if promoted.ID == beforeSessions[0].ID {
		t.Fatal("tentative session id survived a validator set change")
	}
}

func TestTrackerStartGroupsFromSeqnoGatesLifecycle(t *testing.T) {
	current := buildTestValidatorSet(t, []testValidatorWire{{
		index: 0, key: groupTestBytes(1), weight: 10,
	}}, true, 10)
	configRoot := buildTestConfig(t, map[uint32]*cell.Cell{
		configParamCurrentValidators: current,
	})
	shard := ShardID{Workchain: 0, Shard: -1 << 63}
	asOf := time.Unix(1_700_000_000, 0)

	fixtures := []StateInput{
		buildStateFixture(t, stateFixtureOptions{
			Seqno:            100,
			GenUTime:         1_700_000_000,
			CatchainSeqno:    44,
			RotatedAllShards: true,
			ConfigRoot:       configRoot,
			ShardHashes: testShardHashes(t, testBinTreeLeaf(
				testParsedShardDescription(t, shard, 17, 91, tlb.FutureSplit{
					SplitUtime: 1_700_000_001,
				}),
			)),
		}),
		buildStateFixture(t, stateFixtureOptions{
			Seqno:         101,
			GenUTime:      1_700_000_001,
			CatchainSeqno: 44,
			ConfigRoot:    configRoot,
			ShardHashes: testShardHashes(t, testBinTreeLeaf(
				testParsedShardDescription(t, shard, 18, 91, tlb.FutureSplitMergeNone{}),
			)),
		}),
		buildStateFixture(t, stateFixtureOptions{
			Seqno:         102,
			GenUTime:      1_700_000_002,
			CatchainSeqno: 44,
			ConfigRoot:    configRoot,
			ShardHashes: testShardHashes(t, testBinTreeLeaf(
				testParsedShardDescription(t, shard, 19, 91, tlb.FutureSplitMergeNone{}),
			)),
		}),
	}

	tracker, err := NewTracker(TrackerOptions{StartGroupsFromSeqno: 102})
	if err != nil {
		t.Fatal(err)
	}

	first, err := tracker.Apply(ApplyInput{Block: fixtures[0].Block, Root: fixtures[0].Root, AsOf: asOf})
	if err != nil {
		t.Fatalf("apply first catch-up state: %v", err)
	}
	if len(first.Transitions) != 0 {
		t.Fatalf("first catch-up transitions = %+v, want none", first.Transitions)
	}
	if first.Snapshot.LifecycleEnabled {
		t.Fatal("first catch-up snapshot enabled group lifecycle before the threshold")
	}
	if len(first.Snapshot.Active) != 2 || len(first.Snapshot.Future) != 0 {
		t.Fatalf("first catch-up snapshot has active/future = %d/%d, want 2/0",
			len(first.Snapshot.Active), len(first.Snapshot.Future))
	}

	replayedCatchUp, err := tracker.Apply(ApplyInput{
		Block: fixtures[0].Block,
		Root:  fixtures[0].Root,
		AsOf:  asOf.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("replay catch-up state: %v", err)
	}
	if replayedCatchUp.Snapshot != first.Snapshot || len(replayedCatchUp.Transitions) != 0 {
		t.Fatal("idempotent catch-up replay rebuilt the snapshot or emitted transitions")
	}
	if replayedCatchUp.Snapshot.LifecycleEnabled {
		t.Fatal("catch-up replay enabled group lifecycle before the threshold")
	}

	second, err := tracker.Apply(ApplyInput{
		Block: fixtures[1].Block,
		Root:  fixtures[1].Root,
		AsOf:  asOf.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("apply second catch-up state: %v", err)
	}
	if len(second.Transitions) != 0 {
		t.Fatalf("second catch-up transitions = %+v, want none", second.Transitions)
	}
	if second.Snapshot.LifecycleEnabled {
		t.Fatal("second catch-up snapshot enabled group lifecycle before the threshold")
	}
	if len(second.Snapshot.Future) != 2 {
		t.Fatalf("second catch-up future sessions = %d, want current desired sessions", len(second.Snapshot.Future))
	}

	started, err := tracker.Apply(ApplyInput{
		Block: fixtures[2].Block,
		Root:  fixtures[2].Root,
		AsOf:  asOf.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("apply first eligible state: %v", err)
	}
	if len(started.Snapshot.Active) != 2 || len(started.Snapshot.Future) != 2 {
		t.Fatalf("eligible snapshot has active/future = %d/%d, want clean 2/2",
			len(started.Snapshot.Active), len(started.Snapshot.Future))
	}
	if !started.Snapshot.LifecycleEnabled {
		t.Fatal("eligible snapshot did not enable group lifecycle")
	}
	if active := trackerSessionForShard(t, started.Snapshot.Active, shard); active.MinMasterchain.SeqNo != 100 || active.Genesis[0].SeqNo != 17 {
		t.Fatalf("eligible active session lost its catch-up lifecycle origin: %+v", active)
	}

	wantTransitions := len(started.Snapshot.Active) + len(started.Snapshot.Future)
	if len(started.Transitions) != wantTransitions {
		t.Fatalf("eligible transitions = %+v, want %d starts/preparations", started.Transitions, wantTransitions)
	}
	for i := range started.Snapshot.Active {
		if started.Transitions[i].Kind != TransitionStarted {
			t.Fatalf("eligible active transition %d = %s, want started", i, started.Transitions[i].Kind)
		}
	}
	for i := range started.Snapshot.Future {
		transition := started.Transitions[len(started.Snapshot.Active)+i]
		if transition.Kind != TransitionPrepared {
			t.Fatalf("eligible future transition %d = %s, want prepared", i, transition.Kind)
		}
	}

	replayedStart, err := tracker.Apply(ApplyInput{
		Block: fixtures[2].Block,
		Root:  fixtures[2].Root,
		AsOf:  asOf.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatalf("replay eligible state: %v", err)
	}
	if replayedStart.Snapshot != started.Snapshot || len(replayedStart.Transitions) != 0 {
		t.Fatal("idempotent eligible replay rebuilt the snapshot or emitted transitions")
	}
	if !replayedStart.Snapshot.LifecycleEnabled {
		t.Fatal("eligible replay disabled group lifecycle")
	}
}

func TestTrackerOwnsAppliedBlockID(t *testing.T) {
	current := buildTestValidatorSet(t, []testValidatorWire{{
		index: 0, key: groupTestBytes(1), weight: 1,
	}}, true, 1)
	shard := ShardID{Workchain: 0, Shard: -1 << 63}
	fixture := buildStateFixture(t, stateFixtureOptions{
		Seqno:            100,
		GenUTime:         1_700_000_000,
		CatchainSeqno:    44,
		RotatedAllShards: true,
		ConfigRoot: buildTestConfig(t, map[uint32]*cell.Cell{
			configParamCurrentValidators: current,
		}),
		ShardHashes: testShardHashes(t, testBinTreeLeaf(
			testParsedShardDescription(t, shard, 17, 91, tlb.FutureSplitMergeNone{}),
		)),
	})
	tracker, err := NewTracker(TrackerOptions{})
	if err != nil {
		t.Fatal(err)
	}

	inputBlock := *fixture.Block.Copy()
	wantRootHash := append([]byte(nil), inputBlock.RootHash...)
	wantFileHash := append([]byte(nil), inputBlock.FileHash...)
	if _, err = tracker.Apply(ApplyInput{
		Block: inputBlock,
		Root:  fixture.Root,
		AsOf:  time.Unix(1_700_000_000, 0),
	}); err != nil {
		t.Fatal(err)
	}

	inputBlock.RootHash[0] ^= 0xff
	inputBlock.FileHash[0] ^= 0xff
	snapshot, err := tracker.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(snapshot.MasterchainBlock.RootHash, wantRootHash) ||
		!bytes.Equal(snapshot.MasterchainBlock.FileHash, wantFileHash) {
		t.Fatal("published block ID aliases Apply input")
	}
}

func TestSnapshotWhileApplying(t *testing.T) {
	current := buildTestValidatorSet(t, []testValidatorWire{{
		index: 0, key: groupTestBytes(1), weight: 1,
	}}, true, 1)
	configRoot := buildTestConfig(t, map[uint32]*cell.Cell{
		configParamCurrentValidators: current,
	})
	shard := ShardID{Workchain: 0, Shard: -1 << 63}
	fixtures := make([]StateInput, 32)
	for i := range fixtures {
		fixtures[i] = buildStateFixture(t, stateFixtureOptions{
			Seqno:            uint32(100 + i),
			GenUTime:         uint32(1_700_000_000 + i),
			CatchainSeqno:    44,
			RotatedAllShards: i == 0,
			ConfigRoot:       configRoot,
			ShardHashes: testShardHashes(t, testBinTreeLeaf(
				testParsedShardDescription(t, shard, uint32(17+i), 91, tlb.FutureSplitMergeNone{}),
			)),
		})
	}
	tracker, err := NewTracker(TrackerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tracker.Apply(ApplyInput{
		Block: fixtures[0].Block,
		Root:  fixtures[0].Root,
		AsOf:  time.Unix(1_700_000_000, 0),
	}); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		for i := 1; i < len(fixtures); i++ {
			if _, applyErr := tracker.Apply(ApplyInput{
				Block: fixtures[i].Block,
				Root:  fixtures[i].Root,
				AsOf:  time.Unix(int64(1_700_000_000+i), 0),
			}); applyErr != nil {
				done <- applyErr
				return
			}
		}
		done <- nil
	}()

	lastSeqno := uint32(0)
	for {
		select {
		case applyErr := <-done:
			if applyErr != nil {
				t.Fatal(applyErr)
			}
			snapshot, snapshotErr := tracker.Snapshot()
			if snapshotErr != nil {
				t.Fatal(snapshotErr)
			}
			if snapshot.MasterchainBlock.SeqNo != fixtures[len(fixtures)-1].Block.SeqNo {
				t.Fatalf("final snapshot seqno = %d", snapshot.MasterchainBlock.SeqNo)
			}
			return
		default:
			snapshot, snapshotErr := tracker.Snapshot()
			if snapshotErr != nil {
				t.Fatal(snapshotErr)
			}
			if snapshot.MasterchainBlock.SeqNo < lastSeqno {
				t.Fatalf("snapshot regressed from %d to %d", lastSeqno, snapshot.MasterchainBlock.SeqNo)
			}
			lastSeqno = snapshot.MasterchainBlock.SeqNo
		}
	}
}

func TestReconcileSnapshotPreservesSessionsAndPromotesFuture(t *testing.T) {
	t.Parallel()

	activeID := trackerTestID(1)
	promotedID := trackerTestID(2)
	discardedID := trackerTestID(3)
	preparedID := trackerTestID(4)
	stoppedID := trackerTestID(5)
	oldGenesis := []ton.BlockIDExt{{Workchain: 0, Shard: -1 << 63, SeqNo: 10}}
	oldValidators := []Validator{{Weight: 17}}
	futureValidators := []Validator{{Weight: 23}}

	previous := &Snapshot{
		Active: []Session{
			{ID: activeID, Shard: ShardID{Workchain: 0, Shard: -1 << 63}, Validators: oldValidators, Genesis: oldGenesis, MinMasterchain: ton.BlockIDExt{SeqNo: 8}},
			{ID: stoppedID, Shard: ShardID{Workchain: 0, Shard: 1}},
		},
		Future: []Session{
			{ID: promotedID, Shard: ShardID{Workchain: 0, Shard: 2}, Validators: futureValidators},
			{ID: discardedID, Shard: ShardID{Workchain: 0, Shard: 3}},
		},
	}
	next := &Snapshot{
		Active: []Session{
			{ID: activeID, Shard: ShardID{Workchain: 0, Shard: -1 << 63}, Validators: []Validator{{Weight: 99}}, Genesis: []ton.BlockIDExt{{SeqNo: 99}}, MinMasterchain: ton.BlockIDExt{SeqNo: 99}},
			{ID: promotedID, Shard: ShardID{Workchain: 0, Shard: 2}, Validators: []Validator{{Weight: 99}}},
		},
		Future: []Session{{ID: preparedID, Shard: ShardID{Workchain: 0, Shard: 4}}},
	}

	transitions, err := reconcileSnapshot(previous, next)
	if err != nil {
		t.Fatal(err)
	}
	wantKinds := []TransitionKind{
		TransitionStopped,
		TransitionDiscarded,
		TransitionPromoted,
		TransitionPrepared,
	}
	if len(transitions) != len(wantKinds) {
		t.Fatalf("transitions = %+v, want %d", transitions, len(wantKinds))
	}
	for i, want := range wantKinds {
		if transitions[i].Kind != want {
			t.Fatalf("transition %d = %s, want %s", i, transitions[i].Kind, want)
		}
	}

	if &next.Active[0].Validators[0] != &oldValidators[0] {
		t.Fatal("unchanged active session roster was reallocated")
	}
	if &next.Active[0].Genesis[0] != &oldGenesis[0] {
		t.Fatal("unchanged active session genesis was replaced")
	}
	if next.Active[0].MinMasterchain.SeqNo != 8 {
		t.Fatalf("unchanged active min masterchain = %d, want 8", next.Active[0].MinMasterchain.SeqNo)
	}
	if &next.Active[1].Validators[0] != &futureValidators[0] {
		t.Fatal("promoted future session roster was reallocated")
	}
}

func TestReconcileSnapshotRejectsDuplicateSessionID(t *testing.T) {
	t.Parallel()

	id := trackerTestID(1)
	next := &Snapshot{Active: []Session{
		{ID: id, Shard: ShardID{Workchain: 0, Shard: 1}},
		{ID: id, Shard: ShardID{Workchain: 0, Shard: 2}},
	}}

	if _, err := reconcileSnapshot(&Snapshot{}, next); err == nil {
		t.Fatal("reconcileSnapshot accepted duplicate session ID")
	}
}

func TestReconcileFutureSessionsKeepsTentativesUntilSuperseded(t *testing.T) {
	t.Parallel()

	root := ShardID{Workchain: 0, Shard: -1 << 63}
	left, _, err := shardChildren(root)
	if err != nil {
		t.Fatal(err)
	}
	oldChild := Session{ID: trackerTestID(1), Shard: left, CatchainSeqno: 11}
	oldSameShard := Session{ID: trackerTestID(2), Shard: root, CatchainSeqno: 11}
	desired := Session{ID: trackerTestID(3), Shard: root, CatchainSeqno: 13}
	previous := &Snapshot{Future: []Session{oldChild, oldSameShard}}

	got, err := reconcileFutureSessions(previous, []Session{{
		ID: trackerTestID(9), Shard: root, CatchainSeqno: 11,
	}}, []Session{desired})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != oldChild.ID || got[1].ID != desired.ID {
		t.Fatalf("future sessions = %+v, want desired parent and retained child", got)
	}

	got, err = reconcileFutureSessions(previous, []Session{{
		ID: trackerTestID(9), Shard: root, CatchainSeqno: 12,
	}}, []Session{desired})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != desired.ID {
		t.Fatalf("future sessions after related catchain advance = %+v, want only desired", got)
	}
}

func trackerTestID(value byte) [32]byte {
	var id [32]byte
	id[0] = value
	return id
}

func trackerSessionsForShard(sessions []Session, shard ShardID) []Session {
	var matched []Session
	for i := range sessions {
		if sessions[i].Shard == shard {
			matched = append(matched, sessions[i])
		}
	}

	return matched
}

func trackerSessionForShard(t *testing.T, sessions []Session, shard ShardID) Session {
	t.Helper()

	for i := range sessions {
		if sessions[i].Shard == shard {
			return sessions[i]
		}
	}

	t.Fatalf("session for shard %+v is absent", shard)
	return Session{}
}
