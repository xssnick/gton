package msgpool

import (
	"errors"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

var (
	leftShard  = ShardIdent{Workchain: 0, Shard: 0x4000000000000000}
	rightShard = ShardIdent{Workchain: 0, Shard: 0xc000000000000000}
)

func TestInternalsManagerRoutesOverlappingDestinationsOnce(t *testing.T) {
	pool := New(Config{Clock: newFakeClock()})
	t.Cleanup(pool.Close)
	internals := pool.Internals()
	masterchain := ShardIdent{Workchain: -1, Shard: ShardAll}
	if err := internals.ReconcileDestinations([]ShardIdent{
		rightShard, testOwner, leftShard, masterchain, testOwner,
	}); err != nil {
		t.Fatal(err)
	}
	var got []ShardIdent
	internals.VisitDestinations(func(shard ShardIdent) bool {
		got = append(got, shard)
		return true
	})
	if len(got) != 4 ||
		got[0] != masterchain || got[1] != leftShard || got[2] != testOwner || got[3] != rightShard {
		t.Fatalf("destinations = %+v", got)
	}

	sourceAddress := deltaAddr(0, 0x11)
	leftMessage := deltaInternalMsg(t, sourceAddress, deltaAddr(0, 0x22), 100)
	rightMessage := deltaInternalMsg(t, sourceAddress, deltaAddr(0, 0x99), 200)
	masterMessage := deltaInternalMsg(t, sourceAddress, deltaAddr(-1, 0x33), 300)
	dictionary := newOutDescrDict(t)
	transaction := cell.BeginCell().MustStoreUInt(0xdead, 32).EndCell()
	for _, message := range []*cell.Cell{leftMessage, rightMessage, masterMessage} {
		setDescr(t, dictionary, message.Hash(), cell.BeginCell().
			MustStoreUInt(0b001, 3).
			MustStoreRef(deltaEnvelope(t, message, regularNext(96))).
			MustStoreRef(transaction).
			EndCell())
	}

	source := testOwner
	ref := sref(7, 0x77)
	routed, err := internals.DeltasFromBlockRoot(source, ref, deltaBlockRoot(t, dictionary.AsCell()), 0)
	if err != nil {
		t.Fatal(err)
	}
	byDestination := make(map[ShardIdent]*InternalsDelta, len(routed))
	for _, result := range routed {
		byDestination[result.Destination] = result.Delta
		if result.Delta.AddedTotal != 3 {
			t.Fatalf("%v total = %d", result.Destination, result.Delta.AddedTotal)
		}
	}
	if len(byDestination[leftShard].Added) != 1 || len(byDestination[rightShard].Added) != 1 ||
		len(byDestination[testOwner].Added) != 2 || len(byDestination[masterchain].Added) != 1 {
		t.Fatalf("routed counts: left=%d right=%d parent=%d master=%d",
			len(byDestination[leftShard].Added), len(byDestination[rightShard].Added),
			len(byDestination[testOwner].Added), len(byDestination[masterchain].Added))
	}
	if byDestination[leftShard].Added[0] != byDestination[testOwner].Added[0] {
		t.Fatal("overlapping parent and child did not share the immutable message")
	}
	for _, delta := range byDestination {
		for _, message := range delta.Added {
			if message.Source != source || message.SourceSeqno != ref.Seqno {
				t.Fatalf("message was not bound before fan-out: %+v", message)
			}
		}
	}
}

func TestInternalsManagerRoutesStateOnceAndReconcilesAtomically(t *testing.T) {
	pool := New(Config{Clock: newFakeClock()})
	t.Cleanup(pool.Close)
	internals := pool.Internals()
	if err := internals.ReconcileDestinations([]ShardIdent{testOwner, leftShard, rightShard}); err != nil {
		t.Fatal(err)
	}

	sourceAddress := deltaAddr(0, 0x11)
	leftMessage := deltaInternalMsg(t, sourceAddress, deltaAddr(0, 0x22), 100)
	rightMessage := deltaInternalMsg(t, sourceAddress, deltaAddr(0, 0x99), 200)
	leftEnvelope := deltaEnvelope(t, leftMessage, regularNext(96))
	rightEnvelope := deltaEnvelope(t, rightMessage, regularNext(96))
	leftPrefix, _ := AccountPrefixFromAddress(deltaAddr(0, 0x22))
	rightPrefix, _ := AccountPrefixFromAddress(deltaAddr(0, 0x99))
	state := stateRootWithQueue(t, queueDictCell(t, map[QueueKey]tlb.EnqueuedMsg{
		MakeQueueKey(leftPrefix, leftMessage.HashKey()): {
			EnqueuedLT: 100,
			Msg:        leftEnvelope,
		},
		MakeQueueKey(rightPrefix, rightMessage.HashKey()): {
			EnqueuedLT: 200,
			Msg:        rightEnvelope,
		},
	}), 2, true)

	ref := sref(10, 0xaa)
	seeds, total, err := internals.SeedsFromStateRoot(testOwner, ref, state)
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 {
		t.Fatalf("queue total = %d", total)
	}
	for _, seed := range seeds {
		if err = internals.Seed(seed.Destination, testOwner, ref, seed.Messages, total); err != nil {
			t.Fatal(err)
		}
	}
	if top, err := internals.SourceTop(leftShard, testOwner); err != nil || top != ref {
		t.Fatalf("left top = %+v, %v", top, err)
	}

	// Reconciliation publishes whole snapshots while readers continue to use
	// retained destination sections. No destination-global queue lock exists.
	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		for range 100 {
			_ = internals.ReconcileDestinations([]ShardIdent{leftShard, rightShard})
			_ = internals.ReconcileDestinations([]ShardIdent{testOwner, leftShard, rightShard})
		}
	}()
	go func() {
		defer wait.Done()
		for range 100 {
			seen := 0
			internals.VisitDestinations(func(ShardIdent) bool {
				seen++
				return true
			})
			if seen != 2 && seen != 3 {
				t.Errorf("partial topology length %d", seen)
				return
			}
		}
	}()
	wait.Wait()

	if err = internals.ReconcileDestinations([]ShardIdent{leftShard, rightShard}); err != nil {
		t.Fatal(err)
	}
	if _, err = internals.SourceTop(testOwner, testOwner); !errors.Is(err, ErrNotFound) {
		t.Fatalf("retired parent destination = %v", err)
	}
	if top, err := internals.SourceTop(leftShard, testOwner); err != nil || top != ref {
		t.Fatalf("surviving child lost state: %+v, %v", top, err)
	}
}

func TestInternalsDestinationsDoNotShareQueueLock(t *testing.T) {
	pool := New(Config{Clock: newFakeClock()})
	t.Cleanup(pool.Close)
	internals := pool.Internals()
	if err := internals.ReconcileDestinations([]ShardIdent{leftShard, rightShard}); err != nil {
		t.Fatal(err)
	}

	snapshot := internals.snapshot.Load()
	snapshot.byShard[leftShard].mu.Lock()
	defer snapshot.byShard[leftShard].mu.Unlock()

	done := make(chan error, 1)
	go func() {
		_, err := internals.SourceTop(rightShard, testOwner)
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("right destination lookup = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("right destination blocked on the left destination queue lock")
	}
}

func TestInternalsSharedRoutedMessageIsImmutableAcrossConcurrentApply(t *testing.T) {
	pool := New(Config{Clock: newFakeClock()})
	t.Cleanup(pool.Close)
	internals := pool.Internals()
	if err := internals.ReconcileDestinations([]ShardIdent{testOwner, leftShard}); err != nil {
		t.Fatal(err)
	}

	source := testOwner
	base := sref(1, 1)
	for _, destination := range []ShardIdent{testOwner, leftShard} {
		if err := internals.Seed(destination, source, base, nil, 0); err != nil {
			t.Fatal(err)
		}
	}
	message := deltaInternalMsg(t, deltaAddr(0, 0x11), deltaAddr(0, 0x22), 100)
	dictionary := newOutDescrDict(t)
	setDescr(t, dictionary, message.Hash(), cell.BeginCell().
		MustStoreUInt(0b001, 3).
		MustStoreRef(deltaEnvelope(t, message, regularNext(96))).
		MustStoreRef(cell.BeginCell().MustStoreUInt(0xdead, 32).EndCell()).
		EndCell())
	ref := sref(2, 2)
	routed, err := internals.DeltasFromBlockRoot(source, ref, deltaBlockRoot(t, dictionary.AsCell()), 0)
	if err != nil {
		t.Fatal(err)
	}

	var wait sync.WaitGroup
	for index := range routed {
		wait.Add(1)
		go func(result RoutedDelta) {
			defer wait.Done()
			if applyErr := internals.ApplyBlock(result.Destination, source, ref, result.Delta); applyErr != nil {
				t.Errorf("apply %v: %v", result.Destination, applyErr)
			}
		}(routed[index])
	}
	wait.Wait()

	parent, err := internals.Cut(testOwner, CutRequest{Sources: map[ShardIdent]CutSource{
		source: {Visible: ref},
	}})
	if err != nil {
		t.Fatal(err)
	}
	child, err := internals.Cut(leftShard, CutRequest{Sources: map[ShardIdent]CutSource{
		source: {Visible: ref},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(parent.Messages) != 1 || len(child.Messages) != 1 || parent.Messages[0] != child.Messages[0] {
		t.Fatal("overlapping destinations did not retain one shared message")
	}
}

func TestInternalsMergeCandidateUsesUnequalPredecessorTips(t *testing.T) {
	state := newDestinationState(testOwner)
	leftRef := sref(100, 0x41)
	rightRef := sref(103, 0xc1)
	left := []*InternalMessage{imsg(100, 1), imsg(300, 3)}
	right := []*InternalMessage{imsg(200, 2), imsg(400, 4)}
	if err := state.Seed(leftShard, leftRef, left, uint64(len(left))); err != nil {
		t.Fatal(err)
	}
	if err := state.Seed(rightShard, rightRef, right, uint64(len(right))); err != nil {
		t.Fatal(err)
	}

	added := imsg(250, 5)
	bindTestMessages(testOwner, 104, []*InternalMessage{added})
	firstID := sref(104, 0xa1).RootHash
	request := CandidateRequest{
		ID:    firstID,
		Seqno: 104,
		Base: []CandidateSource{
			{Source: leftShard, Visible: leftRef},
			{Source: rightShard, Visible: rightRef},
		},
		Delta: &InternalsDelta{
			Added:       []*InternalMessage{added},
			RemovedKeys: []QueueKey{left[0].Key, right[1].Key},
		},
	}
	if err := state.addCandidate(request); err != nil {
		t.Fatal(err)
	}
	// Commit retries are exact idempotent operations.
	if err := state.addCandidate(request); err != nil {
		t.Fatalf("idempotent candidate retry: %v", err)
	}
	if stats := state.statsSnapshot(); stats.CandidatesAdded != 1 || stats.Candidates != 1 {
		t.Fatalf("candidate retry changed stats: %+v", stats)
	}
	conflict := request
	conflict.Delta = &InternalsDelta{RemovedTotal: 1}
	if err := state.addCandidate(conflict); !errors.Is(err, ErrCutStale) {
		t.Fatalf("conflicting candidate retry = %v", err)
	}

	cut, err := state.cut(CutRequest{
		Sources: map[ShardIdent]CutSource{
			leftShard:  {Visible: leftRef},
			rightShard: {Visible: rightRef},
		},
		CandidateTip: &firstID,
	})
	if err != nil {
		t.Fatal(err)
	}
	requireLts(t, cut, 200, 250, 300)

	continued := imsg(500, 6)
	bindTestMessages(testOwner, 105, []*InternalMessage{continued})
	secondID := sref(105, 0xa2).RootHash
	if err = state.addCandidate(CandidateRequest{
		ID:     secondID,
		Parent: &firstID,
		Seqno:  105,
		Delta: &InternalsDelta{
			Added:       []*InternalMessage{continued},
			RemovedKeys: []QueueKey{right[0].Key},
		},
	}); err != nil {
		t.Fatal(err)
	}
	cut, err = state.cut(CutRequest{
		Sources: map[ShardIdent]CutSource{
			leftShard:  {Visible: leftRef},
			rightShard: {Visible: rightRef},
		},
		CandidateTip: &secondID,
	})
	if err != nil {
		t.Fatal(err)
	}
	requireLts(t, cut, 250, 300, 500)
}

func TestInternalsCandidateBaseRejectsInvalidTopology(t *testing.T) {
	t.Run("arbitrary neighbor", func(t *testing.T) {
		requireCandidateBaseTopologyRejected(t, leftShard, []CandidateSource{
			{Source: rightShard, Visible: sref(10, 0xc1)},
		})
	})

	t.Run("wrong-depth ancestor", func(t *testing.T) {
		leftGrandchild := ShardIdent{Workchain: 0, Shard: 0x2000000000000000}
		requireCandidateBaseTopologyRejected(t, leftGrandchild, []CandidateSource{
			{Source: testOwner, Visible: sref(10, 0x81)},
		})
	})

	t.Run("incomplete child pair", func(t *testing.T) {
		requireCandidateBaseTopologyRejected(t, testOwner, []CandidateSource{
			{Source: leftShard, Visible: sref(10, 0x41)},
		})
	})

	t.Run("masterchain child pair", func(t *testing.T) {
		masterchain := ShardIdent{Workchain: -1, Shard: ShardAll}
		masterLeft := ShardIdent{Workchain: -1, Shard: leftShard.Shard}
		masterRight := ShardIdent{Workchain: -1, Shard: rightShard.Shard}
		requireCandidateBaseTopologyRejected(t, masterchain, []CandidateSource{
			{Source: masterLeft, Visible: sref(10, 0x41)},
			{Source: masterRight, Visible: sref(10, 0xc1)},
		})
	})
}

func requireCandidateBaseTopologyRejected(
	t *testing.T,
	owner ShardIdent,
	base []CandidateSource,
) {
	t.Helper()

	state := newDestinationState(owner)
	var predecessorSeqno uint32
	for index := range base {
		if err := state.Seed(base[index].Source, base[index].Visible, nil, 0); err != nil {
			t.Fatal(err)
		}
		predecessorSeqno = max(predecessorSeqno, base[index].Visible.Seqno)
	}

	err := state.addCandidate(CandidateRequest{
		ID:    [32]byte{0xee},
		Seqno: predecessorSeqno + 1,
		Base:  base,
		Delta: &InternalsDelta{},
	})
	if !errors.Is(err, ErrCutStale) {
		t.Fatalf("invalid candidate base topology = %v", err)
	}
}

func TestInternalsMasterchainCandidateUsesLinearBase(t *testing.T) {
	masterchain := ShardIdent{Workchain: -1, Shard: ShardAll}
	state := newDestinationState(masterchain)
	base := sref(10, 0xaa)
	if err := state.Seed(masterchain, base, nil, 0); err != nil {
		t.Fatal(err)
	}

	id := sref(11, 0xbb).RootHash
	if err := state.addCandidate(CandidateRequest{
		ID:    id,
		Seqno: 11,
		Base:  []CandidateSource{{Source: masterchain, Visible: base}},
		Delta: &InternalsDelta{},
	}); err != nil {
		t.Fatal(err)
	}

	cut, err := state.cut(CutRequest{
		Sources:      map[ShardIdent]CutSource{masterchain: {Visible: base}},
		CandidateTip: &id,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(cut.Messages) != 0 {
		t.Fatalf("empty masterchain candidate cut returned %d messages", len(cut.Messages))
	}
}

func TestInternalsSplitCandidateKeepsPredecessorOwnership(t *testing.T) {
	state := newDestinationState(leftShard)
	parentRef := sref(10, 0x80)
	queued := imsg(100, 1)
	queued.Key = MakeQueueKey(
		AccountPrefix{Workchain: 0, Prefix: 0x2000000000000000},
		queued.Root.HashKey(),
	)
	if err := state.Seed(testOwner, parentRef, []*InternalMessage{queued}, 1); err != nil {
		t.Fatal(err)
	}

	added := imsg(200, 2)
	added.Key = MakeQueueKey(
		AccountPrefix{Workchain: 0, Prefix: 0x3000000000000000},
		added.Root.HashKey(),
	)
	bindTestMessages(leftShard, 11, []*InternalMessage{added})
	id := sref(11, 0x40).RootHash
	if err := state.addCandidate(CandidateRequest{
		ID:    id,
		Seqno: 11,
		Base:  []CandidateSource{{Source: testOwner, Visible: parentRef}},
		Delta: &InternalsDelta{Added: []*InternalMessage{added}},
	}); err != nil {
		t.Fatal(err)
	}

	cut, err := state.cut(CutRequest{
		Sources:      map[ShardIdent]CutSource{testOwner: {Visible: parentRef}},
		CandidateTip: &id,
	})
	if err != nil {
		t.Fatal(err)
	}
	requireLts(t, cut, 100, 200)
	if cut.Messages[0].Source != testOwner || cut.Messages[1].Source != leftShard {
		t.Fatalf("split queue ownership = %v, %v", cut.Messages[0].Source, cut.Messages[1].Source)
	}
}

func TestInternalsDropCandidatesRemovesMultipleBranchesIdempotently(t *testing.T) {
	pool := New(Config{Clock: newFakeClock()})
	t.Cleanup(pool.Close)
	internals := pool.Internals()
	if err := internals.ReconcileDestinations([]ShardIdent{testOwner}); err != nil {
		t.Fatal(err)
	}

	base := sref(10, 0x10)
	if err := internals.Seed(testOwner, testOwner, base, nil, 0); err != nil {
		t.Fatal(err)
	}
	first := sref(11, 0x11).RootHash
	child := sref(12, 0x12).RootHash
	second := sref(11, 0x21).RootHash
	for _, request := range []CandidateRequest{
		{ID: first, Seqno: 11, Base: []CandidateSource{{Source: testOwner, Visible: base}}, Delta: &InternalsDelta{}},
		{ID: child, Parent: &first, Seqno: 12, Delta: &InternalsDelta{}},
		{ID: second, Seqno: 11, Base: []CandidateSource{{Source: testOwner, Visible: base}}, Delta: &InternalsDelta{}},
	} {
		if err := internals.AddCandidate(testOwner, request); err != nil {
			t.Fatal(err)
		}
	}

	missing := sref(99, 0xff).RootHash
	if err := internals.DropCandidates(testOwner, [][32]byte{first, missing, second}); err != nil {
		t.Fatal(err)
	}
	stats := internals.Stats()
	if stats.Candidates != 0 || stats.CandidatesDropped != 3 {
		t.Fatalf("batch drop stats = %+v", stats)
	}
	if err := internals.DropCandidates(testOwner, [][32]byte{first, child, second}); err != nil {
		t.Fatal(err)
	}
	if got := internals.Stats().CandidatesDropped; got != 3 {
		t.Fatalf("idempotent batch drop count = %d, want 3", got)
	}
	if err := internals.DropCandidates(leftShard, nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown destination batch drop = %v", err)
	}
}

func TestInternalsHistoricalSourceRootsAreExact(t *testing.T) {
	state := newDestinationState(testOwner)
	ref10 := sref(10, 0x10)
	ref11 := sref(11, 0x11)
	ref12 := sref(12, 0x12)
	if err := state.Seed(testOwner, ref10, nil, 0); err != nil {
		t.Fatal(err)
	}
	if err := state.ApplyBlock(testOwner, ref11, &InternalsDelta{}); err != nil {
		t.Fatal(err)
	}
	if err := state.ApplyBlock(testOwner, ref12, &InternalsDelta{}); err != nil {
		t.Fatal(err)
	}
	if _, err := state.Cut(CutRequest{Sources: map[ShardIdent]CutSource{
		testOwner: {Visible: ref11},
	}}); err != nil {
		t.Fatalf("exact historical root: %v", err)
	}
	wrong := ref11
	wrong.RootHash[0] ^= 0xff
	if _, err := state.Cut(CutRequest{Sources: map[ShardIdent]CutSource{
		testOwner: {Visible: wrong},
	}}); !errors.Is(err, ErrCutStale) {
		t.Fatalf("historical fork = %v", err)
	}
	if err := state.ApplyBlock(testOwner, wrong, &InternalsDelta{}); !errors.Is(err, ErrApplyDisorder) {
		t.Fatalf("same-height fork replay = %v", err)
	}
	if top, err := state.SourceTop(testOwner); err != nil || top != ref12 {
		t.Fatalf("fork replay mutated top: %+v, %v", top, err)
	}
}

func BenchmarkInternalsRouteBlock(b *testing.B) {
	for _, destinationCount := range []int{1, 16, 64} {
		b.Run(new(big.Int).SetInt64(int64(destinationCount)).String(), func(b *testing.B) {
			pool := New(Config{})
			b.Cleanup(pool.Close)
			internals := pool.Internals()
			destinations := benchmarkDestinations(destinationCount)
			if err := internals.ReconcileDestinations(destinations); err != nil {
				b.Fatal(err)
			}

			dictionary := newOutDescrDict(b)
			transaction := cell.BeginCell().MustStoreUInt(0xdead, 32).EndCell()
			for index := range 1_000 {
				message := deltaInternalMsg(b, deltaAddr(0, 0x11), deltaAddr(0, byte(index)), uint64(index+1))
				setDescr(b, dictionary, message.Hash(), cell.BeginCell().
					MustStoreUInt(0b001, 3).
					MustStoreRef(deltaEnvelope(b, message, regularNext(96))).
					MustStoreRef(transaction).
					EndCell())
			}
			root := deltaBlockRoot(b, dictionary.AsCell())
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := internals.DeltasFromBlockRoot(testOwner, sref(1, 1), root, 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func benchmarkDestinations(count int) []ShardIdent {
	if count == 1 {
		return []ShardIdent{testOwner}
	}
	destinations := make([]ShardIdent, count)
	depth := 0
	for 1<<depth < count {
		depth++
	}
	marker := uint64(1) << (63 - depth)
	step := marker << 1
	for index := range destinations {
		destinations[index] = ShardIdent{
			Workchain: 0,
			Shard:     uint64(index)*step + marker,
		}
	}
	return destinations
}
