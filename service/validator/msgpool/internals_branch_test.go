package msgpool

import (
	"errors"
	"runtime"
	"sync"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestBranchCandidateLineageAndBoundedCut(t *testing.T) {
	pool, branch, base := branchFixture(t, 3)
	defer pool.Close()
	defer branch.Close()

	firstAdded := imsg(1_100, 11)
	bindTestMessages(testOwner, 11, []*InternalMessage{firstAdded})
	first := sref(11, 0xc1).RootHash
	if err := branch.AddCandidate(CandidateRequest{
		ID:    first,
		Seqno: 11,
		Base:  []CandidateSource{{Source: baseSource, Visible: base}},
		Delta: &InternalsDelta{
			Added:       []*InternalMessage{firstAdded},
			RemovedKeys: []QueueKey{imsg(1_000, 0).Key},
		},
	}); err != nil {
		t.Fatal(err)
	}

	secondAdded := imsg(1_200, 12)
	bindTestMessages(testOwner, 12, []*InternalMessage{secondAdded})
	second := sref(12, 0xc2).RootHash
	if err := branch.AddCandidate(CandidateRequest{
		ID:     second,
		Parent: &first,
		Seqno:  12,
		Delta: &InternalsDelta{
			Added:            []*InternalMessage{secondAdded},
			RemovedEnvHashes: [][32]byte{firstAdded.EnvHash},
		},
	}); err != nil {
		t.Fatal(err)
	}

	cut, err := branch.Cut(CutRequest{
		Sources:      map[ShardIdent]CutSource{baseSource: {Visible: base}},
		CandidateTip: &second,
		Limit:        2,
	})
	if err != nil {
		t.Fatal(err)
	}
	requireLts(t, cut, 1_001, 1_002)
	if !cut.More {
		t.Fatal("bounded branch cut did not report its remaining candidate addition")
	}

	cut, err = branch.Cut(CutRequest{
		Sources:      map[ShardIdent]CutSource{baseSource: {Visible: base}},
		CandidateTip: &second,
	})
	if err != nil {
		t.Fatal(err)
	}
	requireLts(t, cut, 1_001, 1_002, 1_200)
}

func TestBranchMergeBaseAndCandidateRetry(t *testing.T) {
	pool := New(Config{})
	defer pool.Close()
	internals := pool.Internals()
	if err := internals.ReconcileDestinations([]ShardIdent{testOwner}); err != nil {
		t.Fatal(err)
	}

	leftRef := sref(100, 0x41)
	rightRef := sref(103, 0xc1)
	left := []*InternalMessage{imsg(100, 1), imsg(300, 3)}
	right := []*InternalMessage{imsg(200, 2), imsg(400, 4)}
	bindTestMessages(leftShard, leftRef.Seqno, left)
	bindTestMessages(rightShard, rightRef.Seqno, right)
	if err := internals.Seed(testOwner, leftShard, leftRef, left, uint64(len(left))); err != nil {
		t.Fatal(err)
	}
	if err := internals.Seed(testOwner, rightShard, rightRef, right, uint64(len(right))); err != nil {
		t.Fatal(err)
	}
	branch, err := internals.OpenBranch(testOwner)
	if err != nil {
		t.Fatal(err)
	}
	defer branch.Close()

	added := imsg(250, 5)
	bindTestMessages(testOwner, 104, []*InternalMessage{added})
	tip := sref(104, 0xa1).RootHash
	request := CandidateRequest{
		ID:    tip,
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
	if err = branch.AddCandidate(request); err != nil {
		t.Fatal(err)
	}
	if err = branch.AddCandidate(request); err != nil {
		t.Fatalf("idempotent candidate retry: %v", err)
	}
	conflict := request
	conflict.Delta = &InternalsDelta{RemovedTotal: 1}
	if err = branch.AddCandidate(conflict); !errors.Is(err, ErrCutStale) {
		t.Fatalf("conflicting candidate retry = %v", err)
	}

	cut, err := branch.Cut(CutRequest{
		Sources: map[ShardIdent]CutSource{
			leftShard:  {Visible: leftRef},
			rightShard: {Visible: rightRef},
		},
		CandidateTip: &tip,
	})
	if err != nil {
		t.Fatal(err)
	}
	requireLts(t, cut, 200, 250, 300)
}

func TestBranchAllowsRemoveAndReaddAcrossCandidateLineage(t *testing.T) {
	pool, branch, base := branchFixture(t, 1)
	defer pool.Close()
	defer branch.Close()

	original := imsg(1_000, 0)
	first := sref(11, 0xc1).RootHash
	if err := branch.AddCandidate(CandidateRequest{
		ID:    first,
		Seqno: 11,
		Base:  []CandidateSource{{Source: baseSource, Visible: base}},
		Delta: &InternalsDelta{RemovedKeys: []QueueKey{original.Key}},
	}); err != nil {
		t.Fatal(err)
	}
	readded := imsg(1_000, 0)
	bindTestMessages(testOwner, 12, []*InternalMessage{readded})
	second := sref(12, 0xc2).RootHash
	if err := branch.AddCandidate(CandidateRequest{
		ID:     second,
		Parent: &first,
		Seqno:  12,
		Delta:  &InternalsDelta{Added: []*InternalMessage{readded}},
	}); err != nil {
		t.Fatal(err)
	}

	cut, err := branch.Cut(CutRequest{
		Sources:      map[ShardIdent]CutSource{baseSource: {Visible: base}},
		CandidateTip: &second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(cut.Messages) != 1 || cut.Messages[0] != readded {
		t.Fatalf("re-added queue identity resolved to %+v", cut.Messages)
	}
}

func TestBranchCandidatesAreSessionPrivate(t *testing.T) {
	pool := New(Config{})
	defer pool.Close()
	internals := pool.Internals()
	if err := internals.ReconcileDestinations([]ShardIdent{testOwner}); err != nil {
		t.Fatal(err)
	}
	base := sref(10, 0xaa)
	if err := internals.Seed(testOwner, baseSource, base, nil, 0); err != nil {
		t.Fatal(err)
	}
	left, err := internals.OpenBranch(testOwner)
	if err != nil {
		t.Fatal(err)
	}
	defer left.Close()
	right, err := internals.OpenBranch(testOwner)
	if err != nil {
		t.Fatal(err)
	}
	defer right.Close()

	for index := 0; index < maxTrackedCandidates; index++ {
		for marker, branch := range map[byte]*Branch{0x10: left, 0x80: right} {
			id := [32]byte{marker, byte(index + 1)}
			if err = branch.AddCandidate(CandidateRequest{
				ID:    id,
				Seqno: 11,
				Base:  []CandidateSource{{Source: baseSource, Visible: base}},
				Delta: &InternalsDelta{},
			}); err != nil {
				t.Fatalf("branch %x candidate %d: %v", marker, index, err)
			}
		}
	}
	if stats := internals.Stats(); stats.Candidates != 0 {
		t.Fatalf("session-private candidates leaked into shared destination stats: %+v", stats)
	}
}

func TestBranchSameCandidateAllowsSessionPrivateLineage(t *testing.T) {
	pool, parentBranch, base := branchFixture(t, 0)
	defer pool.Close()
	defer parentBranch.Close()

	parent := sref(11, 0xc1).RootHash
	if err := parentBranch.AddCandidate(CandidateRequest{
		ID:    parent,
		Seqno: 11,
		Base:  []CandidateSource{{Source: baseSource, Visible: base}},
		Delta: &InternalsDelta{},
	}); err != nil {
		t.Fatal(err)
	}
	committed := sref(11, 0xb1)
	if err := pool.Internals().Seed(testOwner, baseSource, committed, nil, 0); err != nil {
		t.Fatal(err)
	}
	baseBranch, err := pool.Internals().OpenBranch(testOwner)
	if err != nil {
		t.Fatal(err)
	}
	defer baseBranch.Close()

	target := sref(12, 0xc2).RootHash
	if err := parentBranch.AddCandidate(CandidateRequest{
		ID:     target,
		Parent: &parent,
		Seqno:  12,
		Delta:  &InternalsDelta{},
	}); err != nil {
		t.Fatal(err)
	}
	if err := baseBranch.AddCandidate(CandidateRequest{
		ID:    target,
		Seqno: 12,
		Base:  []CandidateSource{{Source: baseSource, Visible: committed}},
		Delta: &InternalsDelta{},
	}); err != nil {
		t.Fatalf("same candidate conflicted across private session lineages: %v", err)
	}
}

func TestBranchLineageIsNotLimitedBySharedCandidateCache(t *testing.T) {
	pool, branch, base := branchFixture(t, 1)
	defer pool.Close()
	defer branch.Close()

	var tip [32]byte
	for index := 0; index < maxTrackedCandidates+2; index++ {
		id := [32]byte{0xc0, byte(index), byte(index >> 8)}
		request := CandidateRequest{
			ID:    id,
			Seqno: base.Seqno + uint32(index) + 1,
			Delta: &InternalsDelta{},
		}
		if index == 0 {
			request.Base = []CandidateSource{{Source: baseSource, Visible: base}}
		} else {
			request.Parent = &tip
		}
		if err := branch.AddCandidate(request); err != nil {
			t.Fatalf("candidate %d: %v", index, err)
		}
		tip = id
	}

	cut, err := branch.Cut(CutRequest{
		Sources:      map[ShardIdent]CutSource{baseSource: {Visible: base}},
		CandidateTip: &tip,
		Limit:        1,
	})
	if err != nil {
		t.Fatal(err)
	}
	requireLts(t, cut, 1_000)
}

func TestBranchPinnedBaseSurvivesFeedCompactionAndTopologyReplacement(t *testing.T) {
	pool, branch, base := branchFixture(t, 256)
	defer pool.Close()
	defer branch.Close()

	tip := sref(11, 0xc1).RootHash
	if err := branch.AddCandidate(CandidateRequest{
		ID: tip, Seqno: 11,
		Base:  []CandidateSource{{Source: baseSource, Visible: base}},
		Delta: &InternalsDelta{},
	}); err != nil {
		t.Fatal(err)
	}
	removed := make([]QueueKey, 130)
	for index := range removed {
		removed[index] = imsg(uint64(1_000+index), uint16(index)).Key
	}
	if err := pool.Internals().ApplyBlock(testOwner, baseSource, sref(11, 0xbb), &InternalsDelta{
		RemovedKeys: removed, RemovedTotal: len(removed),
	}); err != nil {
		t.Fatal(err)
	}
	if err := pool.Internals().ReconcileDestinations(nil); err != nil {
		t.Fatal(err)
	}
	if err := pool.Internals().ReconcileDestinations([]ShardIdent{testOwner}); err != nil {
		t.Fatal(err)
	}

	cut, err := branch.Cut(CutRequest{
		Sources:      map[ShardIdent]CutSource{baseSource: {Visible: base}},
		CandidateTip: &tip,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(cut.Messages) != 256 {
		t.Fatalf("pinned pre-compaction base contains %d messages, want 256", len(cut.Messages))
	}
}

func TestBranchExplicitStateSeedUsesPinnedDestination(t *testing.T) {
	pool := New(Config{})
	defer pool.Close()
	internals := pool.Internals()
	if err := internals.ReconcileDestinations([]ShardIdent{testOwner}); err != nil {
		t.Fatal(err)
	}
	branch, err := internals.OpenBranch(testOwner)
	if err != nil {
		t.Fatal(err)
	}
	defer branch.Close()
	if err = internals.ReconcileDestinations([]ShardIdent{leftShard}); err != nil {
		t.Fatal(err)
	}

	source := testOwner
	visible := sref(10, 0xaa)
	message := deltaInternalMsg(t, deltaAddr(0, 0x11), deltaAddr(0, 0x22), 1_000)
	envelope := deltaEnvelope(t, message, regularNext(96))
	hop, err := AccountPrefixFromAddress(deltaAddr(0, 0x22))
	if err != nil {
		t.Fatal(err)
	}
	key := MakeQueueKey(hop, message.HashKey())
	state := stateRootWithQueue(t, queueDictCell(t, map[QueueKey]tlb.EnqueuedMsg{
		key: {EnqueuedLT: 1_000, Msg: envelope},
	}), 1, true)
	seeded, total, err := branch.SeedSourceFromStateRoot(source, visible, state)
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 {
		t.Fatalf("seed total = %d, want 1", total)
	}
	if len(seeded) != 1 || seeded[0].Key != key {
		t.Fatalf("returned seed messages = %+v, want queue key %x", seeded, key)
	}
	tip := sref(11, 0xc1).RootHash
	if err = branch.AddCandidate(CandidateRequest{
		ID: tip, Seqno: 11,
		Base:  []CandidateSource{{Source: source, Visible: visible}},
		Delta: &InternalsDelta{},
	}); err != nil {
		t.Fatal(err)
	}
	cut, err := branch.Cut(CutRequest{
		Sources:      map[ShardIdent]CutSource{source: {Visible: visible}},
		CandidateTip: &tip,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(cut.Messages) != 1 || cut.Messages[0].Key != key {
		t.Fatalf("pinned routing seed = %+v", cut.Messages)
	}
}

func TestBranchPinSourceStaleThenExplicitStateSeed(t *testing.T) {
	pool := New(Config{})
	defer pool.Close()
	internals := pool.Internals()
	if err := internals.ReconcileDestinations([]ShardIdent{testOwner}); err != nil {
		t.Fatal(err)
	}

	const count = 129
	source := testOwner
	visible := sref(10, 0xaa)
	queue := make(map[QueueKey]tlb.EnqueuedMsg, count)
	for index := range count {
		message := deltaInternalMsg(t, deltaAddr(0, 0xee), deltaAddr(0, byte(index+1)), uint64(1_000+index))
		envelope := deltaEnvelope(t, message, regularNext(96))
		hop, err := AccountPrefixFromAddress(deltaAddr(0, byte(index+1)))
		if err != nil {
			t.Fatal(err)
		}
		key := MakeQueueKey(hop, message.HashKey())
		queue[key] = tlb.EnqueuedMsg{EnqueuedLT: uint64(1_000 + index), Msg: envelope}
	}
	state := stateRootWithQueue(t, queueDictCell(t, queue), count, true)
	messages, total, err := seedFromStateRoot(state, testOwner)
	if err != nil {
		t.Fatal(err)
	}
	bindTestMessages(source, visible.Seqno, messages)
	if err = internals.Seed(testOwner, source, visible, messages, total); err != nil {
		t.Fatal(err)
	}
	branch, err := internals.OpenBranch(testOwner)
	if err != nil {
		t.Fatal(err)
	}
	defer branch.Close()

	removed := make([]QueueKey, 65)
	for index := range removed {
		removed[index] = messages[index].Key
	}
	if err = internals.ApplyBlock(testOwner, source, sref(11, 0xbb), &InternalsDelta{
		RemovedKeys:  removed,
		RemovedTotal: len(removed),
	}); err != nil {
		t.Fatal(err)
	}
	if err = branch.PinSource(source, visible); !errors.Is(err, ErrCutStale) {
		t.Fatalf("pin before compacted history floor = %v", err)
	}

	seeded, total, err := branch.SeedSourceFromStateRoot(source, visible, state)
	if err != nil {
		t.Fatal(err)
	}
	if total != count {
		t.Fatalf("seed total = %d, want %d", total, count)
	}
	if len(seeded) != count {
		t.Fatalf("returned seed messages = %d, want %d", len(seeded), count)
	}
	tip := sref(11, 0xc1).RootHash
	if err = branch.AddCandidate(CandidateRequest{
		ID: tip, Seqno: 11,
		Base:  []CandidateSource{{Source: source, Visible: visible}},
		Delta: &InternalsDelta{},
	}); err != nil {
		t.Fatal(err)
	}
	cut, err := branch.Cut(CutRequest{
		Sources:      map[ShardIdent]CutSource{source: {Visible: visible}},
		CandidateTip: &tip,
		Limit:        1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(cut.Messages) != 1 || !cut.More {
		t.Fatalf("seeded bounded cut = %+v", cut)
	}
}

func TestBranchDeltaUsesPinnedDestinationAfterTopologyChange(t *testing.T) {
	pool := New(Config{})
	defer pool.Close()
	internals := pool.Internals()
	if err := internals.ReconcileDestinations([]ShardIdent{testOwner}); err != nil {
		t.Fatal(err)
	}
	branch, err := internals.OpenBranch(testOwner)
	if err != nil {
		t.Fatal(err)
	}
	defer branch.Close()
	if err = internals.ReconcileDestinations([]ShardIdent{leftShard}); err != nil {
		t.Fatal(err)
	}

	message := deltaInternalMsg(t, deltaAddr(0, 0x11), deltaAddr(0, 0x22), 1_000)
	envelope := deltaEnvelope(t, message, regularNext(96))
	dictionary := newOutDescrDict(t)
	setDescr(t, dictionary, message.Hash(), cellForNewExport(envelope))
	delta, err := branch.DeltaFromBlockRoot(testOwner, sref(11, 0xbb), deltaBlockRoot(t, dictionary.AsCell()), 0)
	if err != nil {
		t.Fatal(err)
	}
	if delta.AddedTotal != 1 || len(delta.Added) != 1 || delta.Added[0].Source != testOwner {
		t.Fatalf("pinned candidate delta = %+v", delta)
	}
}

func TestBranchRetainDropAndClose(t *testing.T) {
	pool, branch, base := branchFixture(t, 0)
	defer pool.Close()

	root := sref(11, 0xc1).RootHash
	child := sref(12, 0xc2).RootHash
	sibling := sref(12, 0xd2).RootHash
	requests := []CandidateRequest{
		{ID: root, Seqno: 11, Base: []CandidateSource{{Source: baseSource, Visible: base}}, Delta: &InternalsDelta{}},
		{ID: child, Parent: &root, Seqno: 12, Delta: &InternalsDelta{}},
		{ID: sibling, Parent: &root, Seqno: 12, Delta: &InternalsDelta{}},
	}
	for _, request := range requests {
		if err := branch.AddCandidate(request); err != nil {
			t.Fatal(err)
		}
	}
	if err := branch.Retain(&child); err != nil {
		t.Fatal(err)
	}
	if _, err := branch.Cut(CutRequest{
		Sources: map[ShardIdent]CutSource{baseSource: {Visible: base}}, CandidateTip: &sibling,
	}); !errors.Is(err, ErrCutStale) {
		t.Fatalf("discarded sibling cut = %v", err)
	}
	branch.DropCandidate(root)
	if _, err := branch.Cut(CutRequest{
		Sources: map[ShardIdent]CutSource{baseSource: {Visible: base}}, CandidateTip: &child,
	}); !errors.Is(err, ErrCutStale) {
		t.Fatalf("dropped child cut = %v", err)
	}
	branch.Close()
	branch.Close()
	if err := branch.Retain(nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed branch retain = %v", err)
	}
}

func TestBranchConcurrentFeedCutRetainAndClose(t *testing.T) {
	pool, branch, base := branchFixture(t, 1)
	defer pool.Close()
	tip := sref(11, 0xc1).RootHash
	if err := branch.AddCandidate(CandidateRequest{
		ID: tip, Seqno: 11,
		Base:  []CandidateSource{{Source: baseSource, Visible: base}},
		Delta: &InternalsDelta{},
	}); err != nil {
		t.Fatal(err)
	}

	var workers sync.WaitGroup
	workers.Add(4)
	go func() {
		defer workers.Done()
		for range 128 {
			_, err := branch.Cut(CutRequest{
				Sources: map[ShardIdent]CutSource{baseSource: {Visible: base}}, CandidateTip: &tip,
			})
			if err != nil && !errors.Is(err, ErrClosed) {
				if !errors.Is(err, ErrCutStale) {
					panic(err)
				}
			}
		}
	}()
	go func() {
		defer workers.Done()
		for range 128 {
			if err := branch.Retain(&tip); err != nil && !errors.Is(err, ErrClosed) {
				panic(err)
			}
		}
	}()
	go func() {
		defer workers.Done()
		_ = pool.Internals().ApplyBlock(testOwner, baseSource, sref(11, 0xbb), &InternalsDelta{})
		_ = pool.Internals().ReconcileDestinations(nil)
		_ = pool.Internals().ReconcileDestinations([]ShardIdent{testOwner})
	}()
	go func() {
		defer workers.Done()
		runtime.Gosched()
		branch.Close()
	}()
	workers.Wait()
	branch.Close()
}

func branchFixture(t testing.TB, count int) (*Pool, *Branch, SourceRef) {
	t.Helper()
	pool := New(Config{})
	internals := pool.Internals()
	if err := internals.ReconcileDestinations([]ShardIdent{testOwner}); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	messages := make([]*InternalMessage, count)
	for index := range messages {
		messages[index] = imsg(uint64(1_000+index), uint16(index))
	}
	bindTestMessages(baseSource, 10, messages)
	base := sref(10, 0xaa)
	if err := internals.Seed(testOwner, baseSource, base, messages, uint64(count)); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	branch, err := internals.OpenBranch(testOwner)
	if err != nil {
		pool.Close()
		t.Fatal(err)
	}

	return pool, branch, base
}

func cellForNewExport(envelope *cell.Cell) *cell.Cell {
	return cell.BeginCell().MustStoreUInt(0b001, 3).MustStoreRef(envelope).
		MustStoreRef(cell.BeginCell().MustStoreUInt(1, 1).EndCell()).EndCell()
}
