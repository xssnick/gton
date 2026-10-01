package service

import (
	"errors"
	"sync"
	"testing"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestShardTopValidationViewPublishesMonotonicallyAndRejectsForks(t *testing.T) {
	firstState := testMonitorMasterStateWithKeyBlock(t, testMasterBlockID(10), 7, testMasterBlockID(1), false)
	first, err := newShardTopValidationView(firstState)
	if err != nil {
		t.Fatal(err)
	}
	stale, err := newShardTopValidationView(
		testMonitorMasterStateWithKeyBlock(t, testMasterBlockID(9), 6, testMasterBlockID(1), false),
	)
	if err != nil {
		t.Fatal(err)
	}
	nextState := testMonitorMasterStateWithKeyBlock(t, testMasterBlockID(11), 8, testMasterBlockID(1), false)
	next, err := newShardTopValidationView(nextState)
	if err != nil {
		t.Fatal(err)
	}

	var cache broadcastValidatorCache
	got, err := cache.putShardTopView(first)
	if err != nil || got != first {
		t.Fatalf("publish first view: got=%p err=%v", got, err)
	}
	got, err = cache.putShardTopView(stale)
	if err != nil || got != first {
		t.Fatalf("stale publication: got=%p err=%v, want first=%p", got, err, first)
	}
	got, err = cache.putShardTopView(next)
	if err != nil || got != next {
		t.Fatalf("publish next view: got=%p err=%v", got, err)
	}
	got, err = cache.putShardTopView(next)
	if err != nil || got != next {
		t.Fatalf("idempotent publication: got=%p err=%v", got, err)
	}

	forkBlock := nextState.Block
	forkBlock.RootHash[0] ^= 0xff
	fork, err := newShardTopValidationView(
		testMonitorMasterStateWithKeyBlock(t, forkBlock, 8, testMasterBlockID(1), false),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = cache.putShardTopView(fork); !errors.Is(err, errShardTopValidationViewConflict) {
		t.Fatalf("same-height fork error = %v, want conflict", err)
	}

	conflictingConfig, err := newShardTopValidationView(
		testMonitorMasterStateWithKeyBlock(t, nextState.Block, 9, testMasterBlockID(1), false),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = cache.putShardTopView(conflictingConfig); !errors.Is(err, errShardTopValidationViewConflict) {
		t.Fatalf("same-block config error = %v, want conflict", err)
	}

	conflictingState := storage.CloneBlockState(nextState)
	conflictingState.Cell = cell.BeginCell().MustStoreUInt(1, 1).EndCell()
	conflictingStateView, err := newShardTopValidationView(conflictingState)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = cache.putShardTopView(conflictingStateView); !errors.Is(err, errShardTopValidationViewConflict) {
		t.Fatalf("same-block state-root error = %v, want conflict", err)
	}
}

func TestShardTopValidationViewCachesRosterIndependentlyFromDeclaredHash(t *testing.T) {
	block := testBlockID(0, topShard, 10)
	const catchainSeqno = uint32(7)
	key := shardTopValidatorCacheKey{
		workchain:     block.Workchain,
		shard:         block.Shard,
		catchainSeqno: catchainSeqno,
	}
	prepared := &blockproof.PreparedValidatorSet{}
	view := &shardTopValidationView{
		validatorSets: map[shardTopValidatorCacheKey]*blockproof.PreparedValidatorSet{key: prepared},
	}

	got, err := view.validatorSet(block, catchainSeqno, prepared.Hash())
	if err != nil || got != prepared {
		t.Fatalf("cached roster hit: got=%p want=%p err=%v", got, prepared, err)
	}
	if _, err = view.validatorSet(block, catchainSeqno, prepared.Hash()+1); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("bad declared hash error = %v, want not found", err)
	}
}

func TestCurrentShardTopValidationViewLoadsExactStartupState(t *testing.T) {
	state := testMonitorMasterStateWithKeyBlock(t, testMasterBlockID(20), 7, testMasterBlockID(1), false)
	cached := storage.CloneBlockState(state)
	coordinator := &SyncCoordinator{
		status: newTestStatusTrackerWithCurrent(nil, &storage.CurrentState{
			Masterchain: storage.BlockState{Block: state.Block},
			Shards:      map[storage.ShardKey]storage.BlockState{},
		}),
		masterStateCache: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(state.Block): cached,
		},
	}

	view, err := coordinator.currentShardTopValidationView(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !view.masterchain.Block.Equals(&state.Block) {
		t.Fatalf("view block = %s, want %s", storage.FormatBlockRef(view.masterchain.Block), storage.FormatBlockRef(state.Block))
	}
	if view.masterchain == state || view.masterchain == cached {
		t.Fatal("startup view retained mutable BlockState ownership")
	}
	if view.masterchain.Cell != state.Cell || view.masterchain.Parsed != state.Parsed {
		t.Fatal("startup view copied immutable state payloads")
	}
	expected, err := broadcastValidatorConfigFromMasterchainState(view.masterchain)
	if err != nil {
		t.Fatal(err)
	}
	if view.config.rootHash != expected.rootHash {
		t.Fatal("startup view paired the state with a different validator config")
	}
	if view.validatorContext.BlockchainConfig().Root != view.config.cfg.Root {
		t.Fatal("startup view paired authentication with a separately parsed config")
	}

	again, err := coordinator.currentShardTopValidationView(t.Context())
	if err != nil || again != view {
		t.Fatalf("cached startup view: got=%p err=%v, want=%p", again, err, view)
	}
}

func TestShardTopValidationViewReadersKeepOneCoherentEpochDuringRotation(t *testing.T) {
	first, err := newShardTopValidationView(
		testMonitorMasterStateWithKeyBlock(t, testMasterBlockID(30), 7, testMasterBlockID(1), false),
	)
	if err != nil {
		t.Fatal(err)
	}
	next, err := newShardTopValidationView(
		testMonitorMasterStateWithKeyBlock(t, testMasterBlockID(31), 9, testMasterBlockID(1), false),
	)
	if err != nil {
		t.Fatal(err)
	}

	var cache broadcastValidatorCache
	if _, err = cache.putShardTopView(first); err != nil {
		t.Fatal(err)
	}
	firstValidatorContext := first.validatorContext
	wantRoots := map[uint32]cell.Hash{
		first.masterchain.Block.SeqNo: first.config.rootHash,
		next.masterchain.Block.SeqNo:  next.config.rootHash,
	}

	var readers sync.WaitGroup
	for range 8 {
		readers.Add(1)
		go func() {
			defer readers.Done()

			for range 1000 {
				view, getErr := cache.getShardTopView()
				if getErr != nil {
					t.Errorf("read view: %v", getErr)
					return
				}
				want, ok := wantRoots[view.masterchain.Block.SeqNo]
				if !ok || view.config.rootHash != want {
					t.Errorf("incoherent view at masterchain seqno %d", view.masterchain.Block.SeqNo)
					return
				}
			}
		}()
	}
	if _, err = cache.putShardTopView(next); err != nil {
		t.Fatal(err)
	}
	readers.Wait()

	// A validation that captured the old pointer keeps that exact immutable
	// state/config pair even after publication advances.
	if first.masterchain.Block.SeqNo != 30 || first.config.rootHash != wantRoots[30] ||
		first.validatorContext != firstValidatorContext ||
		first.validatorContext.BlockchainConfig().Root != first.config.cfg.Root {
		t.Fatal("rotation changed an in-flight validation view")
	}
}
