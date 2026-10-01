package msgpool

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
)

// lineageModel is the test's own account of which queue identities a branch
// node holds, so random deltas only ever remove what is live and add what is
// not.
type lineageModel struct {
	id    [32]byte
	seqno uint32
	live  []*InternalMessage
	// removed keeps identities this lineage dropped, for re-adding them.
	removed []*InternalMessage
}

// referenceLineageCut is the liveness rule Cut had before it resolved a
// lineage once per call: every base entry and delta addition survives exactly
// when lookupOrder at the tip returns that very pointer.
func referenceLineageCut(t *testing.T, branch *Branch, tip [32]byte) []*InternalMessage {
	t.Helper()

	branch.mu.Lock()
	node := branch.candidates[tip]
	branch.mu.Unlock()
	if node == nil {
		t.Fatalf("reference cut of unknown candidate %x", tip[:4])
	}

	var messages []*InternalMessage
	keep := func(message *InternalMessage) {
		if branch.lookupOrder(node, node.base, orderKey(message)) == message {
			messages = append(messages, message)
		}
	}
	for _, message := range node.base.entries {
		keep(message)
	}
	for at := node; at != nil; at = at.parent {
		for _, message := range at.delta.added {
			keep(message)
		}
	}
	slices.SortStableFunc(messages, CompareLtHash)

	return messages
}

func requireSameMessages(t *testing.T, label string, got, want []*InternalMessage) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("%s: %d messages, want %d", label, len(got), len(want))
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("%s: message %d is %p (lt %d), want %p (lt %d)",
				label, index, got[index], got[index].EnqueuedLT, want[index], want[index].EnqueuedLT)
		}
	}
}

// randomLineageDelta removes a few live identities, by key or by envelope, and
// adds fresh ones. readd additionally brings back identities the lineage
// removed earlier, sometimes as the very pointer that was removed.
func randomLineageDelta(
	t *testing.T,
	rng *rand.Rand,
	parent lineageModel,
	seqno uint32,
	nextTag *uint16,
	readd bool,
) (lineageModel, *InternalsDelta) {
	child := lineageModel{
		seqno:   seqno,
		live:    slices.Clone(parent.live),
		removed: slices.Clone(parent.removed),
	}
	delta := &InternalsDelta{}

	removals := rng.IntN(4)
	for range removals {
		if len(child.live) == 0 {
			break
		}
		index := rng.IntN(len(child.live))
		message := child.live[index]
		if rng.IntN(2) == 0 {
			delta.RemovedKeys = append(delta.RemovedKeys, message.Key)
		} else {
			delta.RemovedEnvHashes = append(delta.RemovedEnvHashes, message.EnvHash)
		}
		child.live = slices.Delete(child.live, index, index+1)
		child.removed = append(child.removed, message)
	}

	var added []*InternalMessage
	for range rng.IntN(4) {
		*nextTag++
		added = append(added, imsg(3_000_000+uint64(*nextTag), *nextTag))
	}
	if readd && len(parent.removed) > 0 && rng.IntN(2) == 0 {
		index := rng.IntN(len(parent.removed))
		gone := parent.removed[index]
		again := gone
		if rng.IntN(2) == 0 {
			root, err := gone.Root.BeginParse()
			if err != nil {
				t.Fatal(err)
			}
			again = imsg(gone.EnqueuedLT, uint16(root.MustLoadUInt(16)))
		}
		added = append(added, again)
		child.removed = slices.DeleteFunc(child.removed, func(message *InternalMessage) bool {
			return message.Key == gone.Key
		})
	}
	bindTestMessages(testOwner, seqno, added)
	slices.SortFunc(added, CompareLtHash)
	delta.Added = added
	child.live = append(child.live, added...)

	return child, delta
}

func fixtureLineage(count int) lineageModel {
	model := lineageModel{}
	for index := range count {
		model.live = append(model.live, imsg(uint64(1_000+index), uint16(index)))
	}

	return model
}

// TestBranchCutLivenessMatchesLineageLookup pins the per-cut liveness
// resolution to the per-message lineage walk it replaced, over random trees
// that remove by key and by envelope and re-add removed identities — as new
// pointers and as the removed pointer itself.
func TestBranchCutLivenessMatchesLineageLookup(t *testing.T) {
	for seed := range uint64(8) {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 0x9e3779b97f4a7c15))
			pool, branch, base := branchFixture(t, 48)
			defer pool.Close()
			defer branch.Close()

			nextTag := uint16(1_000)
			nodes := []lineageModel{}
			for step := range 40 {
				parent := fixtureLineage(48)
				parent.seqno = 10
				request := CandidateRequest{Base: []CandidateSource{{Source: baseSource, Visible: base}}}
				if len(nodes) > 0 && rng.IntN(8) != 0 {
					parent = nodes[rng.IntN(len(nodes))]
					request = CandidateRequest{Parent: &parent.id}
				}
				child, delta := randomLineageDelta(t, rng, parent, parent.seqno+1, &nextTag, true)
				child.id = sref(child.seqno, byte(step+1)).RootHash
				child.id[0] = byte(seed)
				request.ID = child.id
				request.Seqno = child.seqno
				request.Delta = delta
				if err := branch.AddCandidate(request); err != nil {
					t.Fatalf("step %d: %v", step, err)
				}
				nodes = append(nodes, child)
			}

			for _, node := range nodes {
				cut, err := branch.Cut(CutRequest{
					Sources:      map[ShardIdent]CutSource{baseSource: {Visible: base}},
					CandidateTip: &node.id,
				})
				if err != nil {
					t.Fatal(err)
				}
				requireSameMessages(t, fmt.Sprintf("tip %d", node.seqno), cut.Messages, referenceLineageCut(t, branch, node.id))
			}
		})
	}
}

// cloneAppliedDelta is what the feed applies for a block the branch already
// holds: the same queue transformation carried by message objects of its own.
func cloneAppliedDelta(delta *InternalsDelta) *InternalsDelta {
	applied := &InternalsDelta{
		RemovedKeys:      delta.RemovedKeys,
		RemovedEnvHashes: delta.RemovedEnvHashes,
	}
	for _, message := range delta.Added {
		copied := *message
		applied.Added = append(applied.Added, &copied)
	}

	return applied
}

// TestBranchRebaseCommittedKeepsCutsAndBoundsLineage runs a session whose every
// window opens on the branch's own candidate, with a pipelined successor already
// installed on it and a stale fork beside it, while the destination applies the
// same blocks — as message objects of its own — zero to three positions behind
// the selected base. Every node that survives a rebase must cut the very same
// pointers in the same order as before it, and the lineage must shrink to the
// unapplied suffix instead of growing with the session.
func TestBranchRebaseCommittedKeepsCutsAndBoundsLineage(t *testing.T) {
	const windows = 32
	rng := rand.New(rand.NewPCG(7, 11))
	pool, branch, base := branchFixture(t, 64)
	defer pool.Close()
	defer branch.Close()

	nextTag := uint16(1_000)
	add := func(parent lineageModel, id [32]byte) (lineageModel, *InternalsDelta) {
		child, delta := randomLineageDelta(t, rng, parent, parent.seqno+1, &nextTag, false)
		child.id = id
		request := CandidateRequest{ID: id, Parent: &parent.id, Seqno: child.seqno, Delta: delta}
		if parent.seqno == base.Seqno {
			request.Parent = nil
			request.Base = []CandidateSource{{Source: baseSource, Visible: base}}
		}
		if err := branch.AddCandidate(request); err != nil {
			t.Fatalf("add candidate %d: %v", child.seqno, err)
		}

		return child, delta
	}
	cutOf := func(tip [32]byte) []*InternalMessage {
		cut, err := branch.Cut(CutRequest{
			Sources:      map[ShardIdent]CutSource{baseSource: {Visible: base}},
			CandidateTip: &tip,
		})
		if err != nil {
			t.Fatalf("cut %x: %v", tip[:3], err)
		}

		return cut.Messages
	}

	parent := fixtureLineage(64)
	parent.seqno = base.Seqno
	var (
		nodes  []lineageModel
		deltas []*InternalsDelta
	)
	frontier := base.Seqno
	for window := range windows {
		for slot := range 4 {
			child, delta := add(parent, [32]byte{0xd1, byte(window), byte(slot)})
			nodes = append(nodes, child)
			deltas = append(deltas, delta)
			parent = child
		}
		selected, successor := nodes[len(nodes)-2], nodes[len(nodes)-1]
		if window%3 == 0 {
			add(nodes[len(nodes)-3], [32]byte{0xf0, byte(window)})
		}
		lag := uint32(window % 4)
		for frontier < selected.seqno-lag {
			frontier++
			index := frontier - nodes[0].seqno
			if err := pool.Internals().ApplyBlock(
				testOwner, baseSource, SourceRef{Seqno: frontier, RootHash: nodes[index].id}, cloneAppliedDelta(deltas[index]),
			); err != nil {
				t.Fatalf("apply %d: %v", frontier, err)
			}
		}

		before := make(map[[32]byte][]*InternalMessage)
		for _, node := range nodes {
			if branch.HasCandidate(node.id) {
				before[node.id] = cutOf(node.id)
			}
		}
		if err := branch.Retain(&selected.id); err != nil {
			t.Fatal(err)
		}
		if err := branch.RebaseCommitted(selected.id); err != nil {
			t.Fatal(err)
		}

		committed := min(frontier, selected.seqno-1)
		for _, node := range nodes {
			retained := branch.HasCandidate(node.id)
			if retained != (node.seqno > committed) {
				t.Fatalf("window %d: candidate %d retained = %t with blocks applied through %d",
					window, node.seqno, retained, frontier)
			}
			if !retained {
				continue
			}
			label := fmt.Sprintf("window %d tip %d", window, node.seqno)
			got := cutOf(node.id)
			requireSameMessages(t, label+" against its cut before the rebase", got, before[node.id])
			requireSameMessages(t, label+" against the lineage walk", got, referenceLineageCut(t, branch, node.id))
		}
		branch.mu.Lock()
		lineage, pinned := len(branch.candidates), len(branch.sources)
		branch.mu.Unlock()
		if lineage != int(selected.seqno-committed)+1 || pinned != 0 {
			t.Fatalf("window %d: lineage holds %d candidates and %d pinned runs, want %d and none",
				window, lineage, pinned, selected.seqno-committed+1)
		}

		if lag == 0 {
			// The selected base itself is applied, so its parent went: a parked
			// successor that recorded the base on that parent still reinstalls.
			promoted := CandidateRequest{
				ID: selected.id, Parent: &nodes[len(nodes)-3].id, Seqno: selected.seqno, Delta: deltas[len(deltas)-2],
			}
			if err := branch.ReusePromotedCandidate(promoted); err != nil {
				t.Fatalf("window %d: verify Parent=P after P was rebased away: %v", window, err)
			}
			if ownership, err := branch.AddOrReusePromotedCandidate(promoted); err != nil ||
				ownership != CandidateInstallReused {
				t.Fatalf("window %d: reuse Parent=P after P was rebased away = (%d, %v)", window, ownership, err)
			}
		}
		if ownership, err := branch.AddOrReusePromotedCandidate(CandidateRequest{
			ID: successor.id, Parent: &selected.id, Seqno: successor.seqno, Delta: deltas[len(deltas)-1],
		}); err != nil || ownership != CandidateInstallReused {
			t.Fatalf("window %d: reinstall pipelined successor = (%d, %v)", window, ownership, err)
		}
	}

	if err := branch.RebaseCommitted([32]byte{0xee}); !errors.Is(err, ErrCutStale) {
		t.Fatalf("rebase of an unknown candidate = %v, want ErrCutStale", err)
	}
	branch.Close()
	if err := branch.RebaseCommitted(parent.id); !errors.Is(err, ErrClosed) {
		t.Fatalf("rebase of a closed branch = %v, want ErrClosed", err)
	}
}

// TestBranchRebaseCommittedDuringCut races rebases against cuts of the tips they
// replace: a cut keeps walking the nodes it resolved after the branch lock is
// released, so a rebase that edited them in place would show up under -race.
func TestBranchRebaseCommittedDuringCut(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 5))
	pool, branch, base := branchFixture(t, 32)
	defer pool.Close()
	defer branch.Close()

	var (
		tip    atomic.Pointer[[32]byte]
		done   = make(chan struct{})
		cutter sync.WaitGroup
	)
	cutter.Add(1)
	go func() {
		defer cutter.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			id := tip.Load()
			if id == nil {
				runtime.Gosched()
				continue
			}
			_, err := branch.Cut(CutRequest{
				Sources:      map[ShardIdent]CutSource{baseSource: {Visible: base}},
				CandidateTip: id,
			})
			if err != nil && !errors.Is(err, ErrCutStale) {
				t.Errorf("cut during rebase: %v", err)
				return
			}
		}
	}()

	nextTag := uint16(1_000)
	parent := fixtureLineage(32)
	parent.seqno = base.Seqno
	var parentDelta *InternalsDelta
	for step := range 64 {
		child, delta := randomLineageDelta(t, rng, parent, parent.seqno+1, &nextTag, false)
		child.id = [32]byte{0xe1, byte(step)}
		request := CandidateRequest{ID: child.id, Parent: &parent.id, Seqno: child.seqno, Delta: delta}
		if step == 0 {
			request.Parent = nil
			request.Base = []CandidateSource{{Source: baseSource, Visible: base}}
		} else if err := pool.Internals().ApplyBlock(
			testOwner, baseSource, SourceRef{Seqno: parent.seqno, RootHash: parent.id}, cloneAppliedDelta(parentDelta),
		); err != nil {
			t.Fatal(err)
		}
		if err := branch.AddCandidate(request); err != nil {
			t.Fatal(err)
		}
		tip.Store(&child.id)
		if err := branch.RebaseCommitted(child.id); err != nil {
			t.Fatal(err)
		}
		parent, parentDelta = child, delta
	}
	close(done)
	cutter.Wait()
}

// BenchmarkBranchRebaseBase measures what RebaseCommitted adds to a window
// transition on top of the node copies: materializing the committed lineage's
// queue and indexing it as the new base.
func BenchmarkBranchRebaseBase(b *testing.B) {
	const churn = 50
	for _, size := range []int{10_000} {
		for _, depth := range []int{16} {
			b.Run(fmt.Sprintf("entries=%d/depth=%d", size, depth), func(b *testing.B) {
				pool, branch, base := branchFixture(b, size)
				defer pool.Close()
				defer branch.Close()
				var parent *[32]byte
				for step := range depth {
					seqno := uint32(11 + step)
					id := sref(seqno, byte(0xc1+step)).RootHash
					delta := &InternalsDelta{}
					for index := range churn {
						removed := step*churn + index
						delta.RemovedKeys = append(delta.RemovedKeys, imsg(uint64(1_000+removed), uint16(removed)).Key)
						tag := size + step*churn + index
						delta.Added = append(delta.Added, imsg(uint64(2_000_000+tag), uint16(tag)))
					}
					bindTestMessages(testOwner, seqno, delta.Added)
					request := CandidateRequest{ID: id, Parent: parent, Seqno: seqno, Delta: delta}
					if parent == nil {
						request.Base = []CandidateSource{{Source: baseSource, Visible: base}}
					}
					if err := branch.AddCandidate(request); err != nil {
						b.Fatal(err)
					}
					parent = &id
				}
				tip := branch.candidates[*parent]
				sources := []CandidateSource{{Source: testOwner, Visible: SourceRef{Seqno: tip.seqno, RootHash: tip.id}}}

				b.ReportAllocs()
				for b.Loop() {
					rebased, err := newBranchBase(sources, mergeBranchCursors(lineageCursors(tip, nil), int(^uint(0)>>1)).Messages)
					if err != nil || len(rebased.entries) != size {
						b.Fatalf("rebased base holds %d messages: %v", len(rebased.entries), err)
					}
				}
			})
		}
	}
}

// BenchmarkBranchCutLineageDeltas is BenchmarkBranchCutProduction with deltas
// that actually dequeue and enqueue: the liveness check is only paid once a
// lineage has removed something.
func BenchmarkBranchCutLineageDeltas(b *testing.B) {
	const churn = 50
	for _, size := range []int{5_000} {
		for _, depth := range []int{1, 16} {
			b.Run(fmt.Sprintf("entries=%d/depth=%d", size, depth), func(b *testing.B) {
				pool, branch, base := branchFixture(b, size)
				defer pool.Close()
				defer branch.Close()
				var parent *[32]byte
				for step := range depth {
					seqno := uint32(11 + step)
					id := sref(seqno, byte(0xc1+step)).RootHash
					delta := &InternalsDelta{}
					for index := range churn {
						removed := step*churn + index
						delta.RemovedKeys = append(delta.RemovedKeys, imsg(uint64(1_000+removed), uint16(removed)).Key)
						tag := size + step*churn + index
						delta.Added = append(delta.Added, imsg(uint64(2_000_000+tag), uint16(tag)))
					}
					bindTestMessages(testOwner, seqno, delta.Added)
					request := CandidateRequest{ID: id, Parent: parent, Seqno: seqno, Delta: delta}
					if parent == nil {
						request.Base = []CandidateSource{{Source: baseSource, Visible: base}}
					}
					if err := branch.AddCandidate(request); err != nil {
						b.Fatal(err)
					}
					parent = &id
				}
				request := CutRequest{
					Sources:      map[ShardIdent]CutSource{baseSource: {Visible: base}},
					CandidateTip: parent,
				}

				b.ReportAllocs()
				for b.Loop() {
					cut, err := branch.Cut(request)
					if err != nil || len(cut.Messages) != size {
						b.Fatalf("Cut returned %d messages: %v", len(cut.Messages), err)
					}
				}
			})
		}
	}
}
