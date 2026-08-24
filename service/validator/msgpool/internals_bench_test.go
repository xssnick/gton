package msgpool

import (
	"fmt"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
)

func BenchmarkInternalsCut(b *testing.B) {
	for _, size := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("entries=%d", size), func(b *testing.B) {
			pool := New(Config{})
			b.Cleanup(pool.Close)
			internals := pool.Internals()
			if err := internals.ReconcileDestinations([]ShardIdent{testOwner}); err != nil {
				b.Fatal(err)
			}
			messages := make([]*InternalMessage, size)
			for i := range messages {
				messages[i] = imsg(uint64(1_000_000+i), uint16(i))
			}
			bindTestMessages(baseSource, 10, messages)
			if err := internals.Seed(testOwner, baseSource, sref(10, 0xaa), messages, uint64(size)); err != nil {
				b.Fatal(err)
			}
			req := CutRequest{
				Sources: map[ShardIdent]CutSource{baseSource: {Visible: sref(10, 0xaa)}},
				Limit:   256,
			}

			b.ReportAllocs()
			for b.Loop() {
				cut, err := internals.Cut(testOwner, req)
				if err != nil || len(cut.Messages) != 256 {
					b.Fatalf("Cut returned %d messages: %v", len(cut.Messages), err)
				}
			}
		})
	}
}

// BenchmarkInternalsCutCandidate measures the speculative path a validator
// collating on its own chain takes: the memoized overlay is invalidated by
// every applied block and every candidate registration, and rebuilding it
// walks the whole committed queue instead of stopping at Limit. The memo is
// dropped per iteration to measure that rebuild rather than the cache hit.
func BenchmarkInternalsCutCandidate(b *testing.B) {
	for _, size := range []int{1_000, 10_000} {
		for _, limit := range []int{0, 256} {
			b.Run(fmt.Sprintf("entries=%d/limit=%d", size, limit), func(b *testing.B) {
				n := newDestinationState(testOwner)
				messages := make([]*InternalMessage, size)
				for i := range messages {
					messages[i] = imsg(uint64(1_000_000+i), uint16(i))
				}
				bindTestMessages(baseSource, 10, messages)
				if err := n.seed(baseSource, sref(10, 0xaa), messages, uint64(size)); err != nil {
					b.Fatal(err)
				}

				// Three chained candidates on top of the committed base, each
				// dequeuing one entry and enqueuing three fresh ones.
				parent := sref(10, 0xaa).RootHash
				for step := range 3 {
					candidate := sref(uint32(11+step), byte(0xc1+step))
					added := make([]*InternalMessage, 3)
					for index := range added {
						added[index] = imsg(uint64(2_000_000+step*10+index), uint16(size+step*10+index))
					}
					delta := &InternalsDelta{
						Added:       added,
						RemovedKeys: []QueueKey{messages[step].Key},
					}
					if err := n.AddCandidate(baseSource, candidate.RootHash, parent, candidate.Seqno, delta); err != nil {
						b.Fatal(err)
					}
					parent = candidate.RootHash
				}

				tip := parent
				req := CutRequest{
					Sources:      map[ShardIdent]CutSource{baseSource: {Visible: sref(10, 0xaa)}},
					CandidateTip: &tip,
					Limit:        limit,
				}
				want := size + 3*3 - 3
				if limit > 0 {
					want = limit
				}

				b.ReportAllocs()
				for b.Loop() {
					n.mu.Lock()
					n.view = nil
					n.mu.Unlock()
					cut, err := n.cut(req)
					if err != nil || len(cut.Messages) != want {
						b.Fatalf("Cut returned %d messages, want %d: %v", len(cut.Messages), want, err)
					}
				}
			})
		}
	}
}

func BenchmarkBranchRootCandidate(b *testing.B) {
	for _, size := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("entries=%d", size), func(b *testing.B) {
			pool, branch, base := branchFixture(b, size)
			defer pool.Close()
			defer branch.Close()
			id := sref(11, 0xc1).RootHash
			request := CandidateRequest{
				ID: id, Seqno: 11,
				Base:  []CandidateSource{{Source: baseSource, Visible: base}},
				Delta: &InternalsDelta{},
			}

			b.ReportAllocs()
			for b.Loop() {
				if err := branch.Retain(nil); err != nil {
					b.Fatal(err)
				}
				if err := branch.AddCandidate(request); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkBranchChildCandidate(b *testing.B) {
	for _, size := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("entries=%d", size), func(b *testing.B) {
			pool, branch, base := branchFixture(b, size)
			defer pool.Close()
			defer branch.Close()
			root := sref(11, 0xc1).RootHash
			if err := branch.AddCandidate(CandidateRequest{
				ID: root, Seqno: 11,
				Base:  []CandidateSource{{Source: baseSource, Visible: base}},
				Delta: &InternalsDelta{},
			}); err != nil {
				b.Fatal(err)
			}
			added := imsg(2_000_000, uint16(size+1))
			bindTestMessages(testOwner, 12, []*InternalMessage{added})
			child := sref(12, 0xc2).RootHash
			request := CandidateRequest{
				ID: child, Parent: &root, Seqno: 12,
				Delta: &InternalsDelta{
					Added:       []*InternalMessage{added},
					RemovedKeys: []QueueKey{imsg(1_000, 0).Key},
				},
			}

			b.ReportAllocs()
			for b.Loop() {
				if err := branch.AddCandidate(request); err != nil {
					b.Fatal(err)
				}
				branch.DropCandidate(child)
			}
		})
	}
}

func BenchmarkBranchCutCandidate(b *testing.B) {
	for _, size := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("entries=%d/limit=256", size), func(b *testing.B) {
			pool, branch, base := branchFixture(b, size)
			defer pool.Close()
			defer branch.Close()
			var parent *[32]byte
			for step := range 3 {
				id := sref(uint32(11+step), byte(0xc1+step)).RootHash
				added := imsg(uint64(2_000_000+step), uint16(size+step+1))
				bindTestMessages(testOwner, uint32(11+step), []*InternalMessage{added})
				request := CandidateRequest{
					ID: id, Parent: parent, Seqno: uint32(11 + step),
					Delta: &InternalsDelta{
						Added:       []*InternalMessage{added},
						RemovedKeys: []QueueKey{imsg(uint64(1_000+step), uint16(step)).Key},
					},
				}
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
				Limit:        256,
			}

			b.ReportAllocs()
			for b.Loop() {
				cut, err := branch.Cut(request)
				if err != nil || len(cut.Messages) != 256 {
					b.Fatalf("Cut returned %d messages: %v", len(cut.Messages), err)
				}
			}
		})
	}
}

// BenchmarkBranchCutProduction measures the unbounded cut consumed by the
// collator. Depth models the number of non-empty candidates retained inside
// one leader window; queue size must remain the dominant input, while the
// session boundary prevents depth from accumulating across windows.
func BenchmarkBranchCutProduction(b *testing.B) {
	for _, size := range []int{1_000, 10_000} {
		for _, depth := range []int{1, 4, 16} {
			b.Run(fmt.Sprintf("entries=%d/depth=%d", size, depth), func(b *testing.B) {
				pool, branch, base := branchFixture(b, size)
				defer pool.Close()
				defer branch.Close()
				var parent *[32]byte
				for step := range depth {
					id := sref(uint32(11+step), byte(0xc1+step)).RootHash
					request := CandidateRequest{
						ID: id, Parent: parent, Seqno: uint32(11 + step), Delta: &InternalsDelta{},
					}
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

// BenchmarkInternalFromEnvelope isolates the per-message parse that both the
// applied-block delta and the reseed walk run for every queued envelope.
func BenchmarkInternalFromEnvelope(b *testing.B) {
	envelope := deltaEnvelope(b, deltaInternalMsgRich(b, deltaAddr(0, 0x11), deltaAddr(0, 0x22), 4_242), regularNext(96))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := InternalMessageFromEnvelope(envelope); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSeedFromState10k measures the reseed walk over a full out-queue —
// the same walk collation runs through SeedsFromStateRoot.
func BenchmarkSeedFromState10k(b *testing.B) {
	const size = 10_000
	source := deltaAddr(0, 0x11)
	entries := make(map[QueueKey]tlb.EnqueuedMsg, size)
	for index := range size {
		destination := deltaAddr(0, byte(index))
		message := deltaInternalMsg(b, source, destination, uint64(index+1))
		hop, err := AccountPrefixFromAddress(destination)
		if err != nil {
			b.Fatal(err)
		}
		entries[MakeQueueKey(hop, message.HashKey())] = tlb.EnqueuedMsg{
			EnqueuedLT: uint64(index + 1),
			Msg:        deltaEnvelope(b, message, regularNext(96)),
		}
	}
	if len(entries) != size {
		b.Fatalf("fixture collapsed to %d queue keys", len(entries))
	}
	state := stateRootWithQueue(b, queueDictCell(b, entries), size, true)

	b.ReportAllocs()
	for b.Loop() {
		messages, total, err := seedFromStateRoot(state, testOwner)
		if err != nil || total != size || len(messages) != size {
			b.Fatalf("seed returned %d/%d messages: %v", len(messages), total, err)
		}
	}
}
