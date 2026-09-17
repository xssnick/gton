package msgpool

import (
	"slices"
	"testing"
)

func TestBranchPagedCutMatchesUnboundedAcrossSourcesAndCandidates(t *testing.T) {
	pool := New(Config{})
	t.Cleanup(pool.Close)
	if err := pool.Internals().ReconcileDestinations([]ShardIdent{testOwner}); err != nil {
		t.Fatal(err)
	}

	baseMessages := []*InternalMessage{
		imsg(10, 1),
		imsg(20, 2),
		imsg(30, 3),
		imsg(40, 4),
		imsg(50, 5),
		imsg(60, 6),
	}
	slices.SortFunc(baseMessages, CompareLtHash)
	base := sref(10, 0xa1)
	bindTestMessages(baseSource, base.Seqno, baseMessages)
	if err := pool.Internals().Seed(testOwner, baseSource, base, baseMessages, uint64(len(baseMessages))); err != nil {
		t.Fatal(err)
	}

	neighbor := mcSource
	neighborMessages := []*InternalMessage{
		imsg(15, 11),
		imsg(20, 12),
		imsg(20, 13),
		imsg(35, 14),
		imsg(45, 15),
		imsg(55, 16),
	}
	slices.SortFunc(neighborMessages, CompareLtHash)
	neighborAt := sref(7, 0xb1)
	bindTestMessages(neighbor, neighborAt.Seqno, neighborMessages)
	if err := pool.Internals().Seed(testOwner, neighbor, neighborAt, neighborMessages, uint64(len(neighborMessages))); err != nil {
		t.Fatal(err)
	}

	branch, err := pool.Internals().OpenBranch(testOwner)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(branch.Close)

	addedAtTie := imsg(20, 101)
	bindTestMessages(testOwner, 11, []*InternalMessage{addedAtTie})
	first := sref(11, 0xc1).RootHash
	if err = branch.AddCandidate(CandidateRequest{
		ID:    first,
		Seqno: 11,
		Base:  []CandidateSource{{Source: baseSource, Visible: base}},
		Delta: &InternalsDelta{
			Added:       []*InternalMessage{addedAtTie},
			RemovedKeys: []QueueKey{baseMessages[3].Key},
		},
	}); err != nil {
		t.Fatal(err)
	}

	addedAfterTie := imsg(42, 102)
	bindTestMessages(testOwner, 12, []*InternalMessage{addedAfterTie})
	tip := sref(12, 0xc2).RootHash
	if err = branch.AddCandidate(CandidateRequest{
		ID:     tip,
		Parent: &first,
		Seqno:  12,
		Delta: &InternalsDelta{
			Added:            []*InternalMessage{addedAfterTie},
			RemovedEnvHashes: [][32]byte{baseMessages[4].EnvHash},
		},
	}); err != nil {
		t.Fatal(err)
	}

	request := CutRequest{
		Sources: map[ShardIdent]CutSource{
			baseSource: {Visible: base},
			neighbor:   {Visible: neighborAt},
		},
		CandidateTip: &tip,
	}
	eager, err := branch.Cut(request)
	if err != nil {
		t.Fatal(err)
	}
	const pageSize = 4
	if len(eager.Messages) != 12 || len(eager.Messages)%pageSize != 0 {
		t.Fatalf("fixture yielded %d messages, want an exact multiple of %d", len(eager.Messages), pageSize)
	}
	if eager.Messages[pageSize-1].EnqueuedLT != eager.Messages[pageSize].EnqueuedLT {
		t.Fatalf("page boundary does not split the equal-lt run: %d then %d",
			eager.Messages[pageSize-1].EnqueuedLT, eager.Messages[pageSize].EnqueuedLT)
	}

	request.Limit = pageSize
	paged, err := branch.Cut(request)
	if err != nil {
		t.Fatal(err)
	}
	if !paged.More || !paged.CanLoadMore() {
		t.Fatal("first bounded page has no continuation")
	}
	requireSameMessages(t, "first page", paged.Messages, eager.Messages[:pageSize])

	before := slices.Clone(paged.Messages)
	if loaded := paged.LoadMore(0); loaded != 0 {
		t.Fatalf("LoadMore(0) loaded %d messages", loaded)
	}
	if !paged.More || !paged.CanLoadMore() {
		t.Fatal("LoadMore(0) consumed the continuation")
	}
	requireSameMessages(t, "after LoadMore(0)", paged.Messages, before)

	pages := 1
	for paged.More {
		previous := len(paged.Messages)
		loaded := paged.LoadMore(pageSize)
		if loaded != pageSize {
			t.Fatalf("page %d loaded %d messages, want %d for an exact multiple", pages+1, loaded, pageSize)
		}
		pages++
		requireSameMessages(t, "paged prefix", paged.Messages, eager.Messages[:previous+loaded])
	}
	if pages != len(eager.Messages)/pageSize {
		t.Fatalf("loaded %d pages, want %d", pages, len(eager.Messages)/pageSize)
	}
	if paged.CanLoadMore() || paged.LoadMore(pageSize) != 0 {
		t.Fatal("drained exact-multiple cut retained its continuation")
	}
	requireSameMessages(t, "fully paged cut", paged.Messages, eager.Messages)
}

type branchPagingMutationFixture struct {
	pool       *Pool
	branch     *Branch
	base       SourceRef
	first      [32]byte
	tip        [32]byte
	fork       [32]byte
	firstDelta *InternalsDelta
	cutRequest CutRequest
}

func newBranchPagingMutationFixture(t *testing.T) branchPagingMutationFixture {
	t.Helper()

	pool, branch, base := branchFixture(t, 24)
	firstAdded := imsg(2_000, 101)
	bindTestMessages(testOwner, 11, []*InternalMessage{firstAdded})
	first := sref(11, 0xd1).RootHash
	firstDelta := &InternalsDelta{
		Added:       []*InternalMessage{firstAdded},
		RemovedKeys: []QueueKey{imsg(1_003, 3).Key},
	}
	if err := branch.AddCandidate(CandidateRequest{
		ID: first, Seqno: 11,
		Base:  []CandidateSource{{Source: baseSource, Visible: base}},
		Delta: firstDelta,
	}); err != nil {
		branch.Close()
		pool.Close()
		t.Fatal(err)
	}

	tipAdded := imsg(2_100, 102)
	bindTestMessages(testOwner, 12, []*InternalMessage{tipAdded})
	tip := sref(12, 0xd2).RootHash
	if err := branch.AddCandidate(CandidateRequest{
		ID: tip, Parent: &first, Seqno: 12,
		Delta: &InternalsDelta{
			Added:            []*InternalMessage{tipAdded},
			RemovedEnvHashes: [][32]byte{firstAdded.EnvHash},
		},
	}); err != nil {
		branch.Close()
		pool.Close()
		t.Fatal(err)
	}

	forkAdded := imsg(2_200, 103)
	bindTestMessages(testOwner, 12, []*InternalMessage{forkAdded})
	fork := sref(12, 0xdf).RootHash
	if err := branch.AddCandidate(CandidateRequest{
		ID: fork, Parent: &first, Seqno: 12,
		Delta: &InternalsDelta{Added: []*InternalMessage{forkAdded}},
	}); err != nil {
		branch.Close()
		pool.Close()
		t.Fatal(err)
	}

	return branchPagingMutationFixture{
		pool:       pool,
		branch:     branch,
		base:       base,
		first:      first,
		tip:        tip,
		fork:       fork,
		firstDelta: firstDelta,
		cutRequest: CutRequest{
			Sources:      map[ShardIdent]CutSource{baseSource: {Visible: base}},
			CandidateTip: &tip,
		},
	}
}

func requirePagingContinuationSurvives(
	t *testing.T,
	fixture branchPagingMutationFixture,
	mutate func(branchPagingMutationFixture),
) {
	t.Helper()
	defer fixture.pool.Close()
	defer fixture.branch.Close()

	eager, err := fixture.branch.Cut(fixture.cutRequest)
	if err != nil {
		t.Fatal(err)
	}
	request := fixture.cutRequest
	request.Limit = 3
	paged, err := fixture.branch.Cut(request)
	if err != nil {
		t.Fatal(err)
	}
	if !paged.More || !paged.CanLoadMore() {
		t.Fatal("fixture did not produce a continuation")
	}
	requireSameMessages(t, "initial page", paged.Messages, eager.Messages[:len(paged.Messages)])

	mutate(fixture)
	for page := 1; paged.More; page++ {
		limit := 1 + page%5
		if loaded := paged.LoadMore(limit); loaded == 0 {
			t.Fatalf("continuation made no progress with %d eager messages remaining",
				len(eager.Messages)-len(paged.Messages))
		}
		requireSameMessages(t, "continuation prefix", paged.Messages, eager.Messages[:len(paged.Messages)])
	}
	if paged.CanLoadMore() {
		t.Fatal("drained continuation remains loadable")
	}
	requireSameMessages(t, "snapshot after mutation", paged.Messages, eager.Messages)
}

func TestBranchPagedCutSnapshotSurvivesBranchMutations(t *testing.T) {
	t.Run("retain another fork", func(t *testing.T) {
		fixture := newBranchPagingMutationFixture(t)
		requirePagingContinuationSurvives(t, fixture, func(fixture branchPagingMutationFixture) {
			if err := fixture.branch.Retain(&fixture.fork); err != nil {
				t.Fatal(err)
			}
			if fixture.branch.HasCandidate(fixture.tip) {
				t.Fatal("Retain kept the sibling tip whose continuation is under test")
			}
		})
	})

	t.Run("rebase committed parent", func(t *testing.T) {
		fixture := newBranchPagingMutationFixture(t)
		requirePagingContinuationSurvives(t, fixture, func(fixture branchPagingMutationFixture) {
			if err := fixture.pool.Internals().ApplyBlock(
				testOwner,
				baseSource,
				SourceRef{Seqno: 11, RootHash: fixture.first},
				cloneAppliedDelta(fixture.firstDelta),
			); err != nil {
				t.Fatal(err)
			}
			if err := fixture.branch.RebaseCommitted(fixture.tip); err != nil {
				t.Fatal(err)
			}
		})
	})

	t.Run("drop candidate", func(t *testing.T) {
		fixture := newBranchPagingMutationFixture(t)
		requirePagingContinuationSurvives(t, fixture, func(fixture branchPagingMutationFixture) {
			fixture.branch.DropCandidate(fixture.tip)
			if fixture.branch.HasCandidate(fixture.tip) {
				t.Fatal("DropCandidate retained the dropped tip")
			}
		})
	})

	t.Run("close", func(t *testing.T) {
		fixture := newBranchPagingMutationFixture(t)
		requirePagingContinuationSurvives(t, fixture, func(fixture branchPagingMutationFixture) {
			fixture.branch.Close()
		})
	})
}

func TestBranchPagedCutSnapshotSurvivesFeedCompaction(t *testing.T) {
	pool, branch, base := branchFixture(t, 160)
	defer pool.Close()
	defer branch.Close()

	request := CutRequest{Sources: map[ShardIdent]CutSource{baseSource: {Visible: base}}}
	eager, err := branch.Cut(request)
	if err != nil {
		t.Fatal(err)
	}
	request.Limit = 7
	paged, err := branch.Cut(request)
	if err != nil {
		t.Fatal(err)
	}
	if !paged.More || !paged.CanLoadMore() {
		t.Fatal("fixture did not produce a continuation")
	}

	removed := make([]QueueKey, 100)
	for index := range removed {
		removed[index] = eager.Messages[index].Key
	}
	if err = pool.Internals().ApplyBlock(testOwner, baseSource, sref(11, 0xee), &InternalsDelta{
		RemovedKeys:  removed,
		RemovedTotal: len(removed),
	}); err != nil {
		t.Fatal(err)
	}
	if compactions := pool.Internals().Stats().Compactions; compactions != 1 {
		t.Fatalf("feed performed %d compactions, want 1", compactions)
	}

	for paged.More {
		if loaded := paged.LoadMore(11); loaded == 0 {
			t.Fatal("continuation stopped after feed compaction")
		}
	}
	requireSameMessages(t, "snapshot after feed compaction", paged.Messages, eager.Messages)
}
