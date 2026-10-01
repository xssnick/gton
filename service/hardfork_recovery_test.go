package service

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
)

func configureTestHardfork(tb testing.TB, node *p2p.Node, block ton.BlockIDExt) {
	tb.Helper()

	value := reflect.ValueOf(node).Elem()
	setUnexportedReflectValue(testReflectField(tb, value, "hardforks"), reflect.ValueOf([]ton.BlockIDExt{block}))
	field := testReflectField(tb, value, "hardforkSet")
	set := reflect.MakeMap(field.Type())
	set.SetMapIndex(testHardforkSetKey(tb, field.Type().Key(), block), reflect.ValueOf(struct{}{}))
	setUnexportedReflectValue(field, set)
}

func TestMasterchainCatchUpTargetPrioritizesImmediateHardfork(t *testing.T) {
	current := testMasterBlockID(100)
	hardfork := testMasterBlockID(101)
	conflicting := testMasterBlockID(101)
	conflicting.RootHash = testDummyHash(0xff, 101)

	for _, observed := range []ton.BlockIDExt{conflicting, testMasterBlockID(102), testMasterBlockID(1000)} {
		t.Run(storage.FormatBlockRef(observed), func(t *testing.T) {
			node := newServiceTestNodeWithStorage(t, &hardforkRecoveryTestStore{})
			configureTestHardfork(t, node, hardfork)
			node.RememberSeenMasterchainBlock(observed)
			svc := &SyncCoordinator{node: node}

			target, err := svc.masterchainCatchUpTarget(current)
			if err != nil || !target.Equals(&hardfork) {
				t.Fatalf("target = %v, %v; want configured hardfork %v", target, err, hardfork)
			}
			if !shouldPreferNextBlockTarget(current.SeqNo, target.SeqNo) {
				t.Fatal("immediate hardfork would not take priority over archive catch-up")
			}

			stillObserved, err := node.ObservedMasterchainBlock()
			if err != nil || !stillObserved.Equals(&observed) {
				t.Fatalf("selecting an unverified hardfork changed observed head: %v, %v", stillObserved, err)
			}
		})
	}
}

type hardforkTargetTestCase struct {
	name  string
	seqno uint32
}

func TestMasterchainCatchUpTargetPreservesOrdinarySelection(t *testing.T) {
	current := testMasterBlockID(100)
	observed := testMasterBlockID(102)
	for _, tc := range []hardforkTargetTestCase{
		{name: "no hardfork"},
		{name: "historical hardfork", seqno: 50},
		{name: "already applied hardfork", seqno: 100},
		{name: "not immediately next", seqno: 102},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := newServiceTestNodeWithStorage(t, &hardforkRecoveryTestStore{})
			if tc.seqno != 0 {
				configureTestHardfork(t, node, testMasterBlockID(tc.seqno))
			}
			svc := &SyncCoordinator{node: node}
			if _, err := svc.masterchainCatchUpTarget(current); !errors.Is(err, storage.ErrNotFound) {
				t.Fatalf("target without observed successor: %v", err)
			}

			node.RememberSeenMasterchainBlock(observed)
			target, err := svc.masterchainCatchUpTarget(current)
			if err != nil || !target.Equals(&observed) {
				t.Fatalf("target = %v, %v; want ordinary observed target %v", target, err, observed)
			}
		})
	}
}

type hardforkRecoveryTestStore struct {
	testStorage
	current       *storage.CurrentState
	full          *storage.ServedBlockFull
	stateErr      error
	indexErr      error
	requested     []ton.BlockIDExt
	stateRequests int
	indexRequests int
	nextRequests  int
}

func (s *hardforkRecoveryTestStore) CurrentState(context.Context) (*storage.CurrentState, error) {
	return s.current, nil
}

func (s *hardforkRecoveryTestStore) BlockState(context.Context, ton.BlockIDExt) (*storage.BlockState, error) {
	s.stateRequests++
	return nil, s.stateErr
}

func (s *hardforkRecoveryTestStore) BlockFull(_ context.Context, block ton.BlockIDExt) (*storage.ServedBlockFull, error) {
	s.requested = append(s.requested, block)
	if s.full != nil && block.Equals(&s.full.ID) {
		return s.full, nil
	}
	return nil, storage.ErrNotFound
}

func (s *hardforkRecoveryTestStore) LookupBlockBySeqNo(context.Context, storage.BlockSeqRef) (ton.BlockIDExt, error) {
	s.indexRequests++
	return ton.BlockIDExt{}, s.indexErr
}

func (s *hardforkRecoveryTestStore) NextBlockFull(context.Context, ton.BlockIDExt) (*storage.ServedBlockFull, error) {
	s.nextRequests++
	return nil, storage.ErrNotFound
}

func TestCatchUpCurrentStateChoosesHardforkBeforeArchive(t *testing.T) {
	prev := testMasterBlockID(100)
	stop := errors.New("entered ordinary next-sync state loading")
	store := &hardforkRecoveryTestStore{
		current: &storage.CurrentState{
			Masterchain: storage.BlockState{
				Block:  prev,
				Parsed: &tlb.ShardStateUnsplit{GenUTime: uint32(time.Now().Add(-24 * time.Hour).Unix())},
			},
			ShardClientSeqno: prev.SeqNo,
		},
		stateErr: stop,
	}
	svc := newCurrentStatePersistenceTestService(t, store, context.Background())
	configureTestHardfork(t, svc.node, testMasterBlockID(101))
	// No ArchiveRunner is installed: selecting archives would fail this test.
	// Stop at the existing pipeline's first state load, before any network IO.
	err := svc.catchUpCurrentState(context.Background())
	if !errors.Is(err, stop) || store.stateRequests != 1 {
		t.Fatalf("catch-up = %v, state loads = %d; want next-sync before archives", err, store.stateRequests)
	}
	if _, err := svc.node.ObservedMasterchainBlock(); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("configured target was published as observed before validation: %v", err)
	}
}

func TestDownloadImmediateHardforkUsesExactID(t *testing.T) {
	for _, wrongParent := range []bool{false, true} {
		name := "configured parent"
		if wrongParent {
			name = "wrong parent"
		}
		t.Run(name, func(t *testing.T) {
			fixture, current := newHardforkProofFixture(t)
			store := &hardforkRecoveryTestStore{
				full: &storage.ServedBlockFull{
					ID: fixture.ID, Block: fixture.BlockBOC, Proof: fixture.ProofBOC,
				},
				indexErr: errors.New("height lookup must not run for configured immediate hardfork"),
			}
			node := newServiceTestNodeWithStorage(t, store)
			configureTestHardfork(t, node, fixture.ID)
			svc := &SyncCoordinator{log: zerolog.Nop(), node: node, storage: store}
			prev := current.Block
			if wrongParent {
				prev.RootHash = testDummyHash(0xff, prev.SeqNo)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var received []shardStateDownload
			for item := range svc.downloadShardStateBlocks(ctx, prev, fixture.ID) {
				received = append(received, item)
			}
			if len(received) != 1 {
				t.Fatalf("received %d blocks, context error = %v", len(received), ctx.Err())
			}
			item := received[0]
			if wrongParent {
				if item.err == nil {
					t.Fatal("accepted hardfork with wrong predecessor")
				}
			} else {
				if item.err != nil || !item.block.ID.Equals(&fixture.ID) {
					t.Fatalf("download = %v, %v", item.block.ID, item.err)
				}
				if _, err := svc.checkedConsensusForPreparedBlock(current, item.block); err != nil {
					t.Fatalf("check downloaded hardfork consensus: %v", err)
				}
			}
			if len(store.requested) != 1 || !store.requested[0].Equals(&fixture.ID) {
				t.Fatalf("exact block requests = %v", store.requested)
			}
			if store.indexRequests != 0 || store.nextRequests != 0 {
				t.Fatalf("used branch discovery: index=%d next=%d", store.indexRequests, store.nextRequests)
			}
		})
	}
}

func TestDownloadOrdinaryTargetKeepsDiscoveryPath(t *testing.T) {
	stop := errors.New("ordinary index lookup")
	store := &hardforkRecoveryTestStore{indexErr: stop}
	svc := &SyncCoordinator{log: zerolog.Nop(), node: &p2p.Node{}, storage: store}
	prev := testMasterBlockID(100)
	var received []shardStateDownload
	for item := range svc.downloadShardStateBlocks(context.Background(), prev, testMasterBlockID(101)) {
		received = append(received, item)
	}
	if len(received) != 1 || !errors.Is(received[0].err, stop) {
		t.Fatalf("ordinary discovery = %+v", received)
	}
	if store.indexRequests != 1 || len(store.requested) != 0 {
		t.Fatalf("ordinary requests: index=%d exact=%d", store.indexRequests, len(store.requested))
	}
}

func TestDownloadImmediateHardforkStopsOnCancellation(t *testing.T) {
	prev := testMasterBlockID(100)
	hardfork := testMasterBlockID(101)
	node := newServiceTestNodeWithStorage(t, &hardforkRecoveryTestStore{})
	configureTestHardfork(t, node, hardfork)
	svc := &SyncCoordinator{log: zerolog.Nop(), node: node}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	downloads := svc.downloadShardStateBlocks(ctx, prev, hardfork)
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case item, ok := <-downloads:
			if !ok {
				return
			}
			if item.err == nil {
				t.Fatal("canceled hardfork download produced a block")
			}
		case <-deadline.C:
			t.Fatal("canceled hardfork download did not stop")
		}
	}
}

func BenchmarkMasterchainCatchUpTarget(b *testing.B) {
	current := testMasterBlockID(100)
	svc := &SyncCoordinator{node: &p2p.Node{}}
	b.Run("observed_only_baseline", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			svc.knownMasterchainTarget(current.SeqNo)
		}
	})
	b.Run("no_hardfork", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			svc.masterchainCatchUpTarget(current)
		}
	})
	b.Run("historical_hardfork", func(b *testing.B) {
		configureTestHardfork(b, svc.node, testMasterBlockID(50))
		b.ReportAllocs()
		for b.Loop() {
			svc.masterchainCatchUpTarget(current)
		}
	})
}
