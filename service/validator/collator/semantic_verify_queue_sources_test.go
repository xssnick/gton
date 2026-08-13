package collator

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/validator/msgpool"
)

func queueSourcesTestQueue(t *testing.T) (tlb.OutMsgQueueInfo, *cell.Cell) {
	t.Helper()

	outQueue, err := tlb.NewOutMsgQueueAugDict()
	if err != nil {
		t.Fatal(err)
	}
	info := tlb.OutMsgQueueInfo{OutQueue: outQueue, ProcInfo: cell.NewDict(processedInfoKeyBits)}
	root, err := info.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	return info, root
}

func queueSourcesTestNeighbor(t *testing.T, workchain int32, shardID int64) Neighbor {
	t.Helper()

	_, root := queueSourcesTestQueue(t)
	return Neighbor{
		Block:           testBlockID(workchain, shardID, 5, 0x21),
		Shard:           msgpool.ShardIdent{Workchain: workchain, Shard: uint64(shardID)},
		EndLT:           5_000,
		OutMsgQueueInfo: root,
	}
}

func queueSourcesTestValidation(
	t *testing.T,
	target msgpool.ShardIdent,
	afterMerge bool,
	neighbors []Neighbor,
) *semanticQueueValidation {
	t.Helper()

	old, _ := queueSourcesTestQueue(t)
	candidate := &verifiedCandidate{}
	candidate.block.BlockInfo.NotMaster = true
	candidate.block.BlockInfo.AfterMerge = afterMerge

	return &semanticQueueValidation{
		replay: &semanticReplay{
			transition: CandidateTransition{
				Previous:  PreviousBlock{ID: testBlockID(target.Workchain, int64(target.Shard), 9, 0x31)},
				Neighbors: neighbors,
			},
			candidate: candidate,
			previous:  &tlb.ShardStateUnsplit{GenLT: 9_999},
		},
		target: target,
		old:    old,
	}
}

func queueSourcesTestOwners(sources []semanticQueueSource) []msgpool.ShardIdent {
	owners := make([]msgpool.ShardIdent, len(sources))
	for i := range sources {
		owners[i] = sources[i].owner
	}
	return owners
}

// ValidateQuery::add_trivial_neighbor replaces every neighbor that intersects
// our own shard, so a queued message is reachable through exactly one source.
// Leaving the still-registered ancestor or the merged children in place would
// surface the same message twice, which verifySourceProcessed reports as an
// import occurring in multiple source queues.
func TestLoadQueueSourcesNormalizesSelfCoveringNeighbors(t *testing.T) {
	left, err := shard.Child(int64(shard.Root), true)
	if err != nil {
		t.Fatal(err)
	}
	right, err := shard.Child(int64(shard.Root), false)
	if err != nil {
		t.Fatal(err)
	}
	rootID := int64(shard.Root)
	child := msgpool.ShardIdent{Workchain: 0, Shard: uint64(left)}
	sibling := msgpool.ShardIdent{Workchain: 0, Shard: uint64(right)}
	root := msgpool.ShardIdent{Workchain: 0, Shard: uint64(rootID)}
	foreign := msgpool.ShardIdent{Workchain: 1, Shard: uint64(rootID)}

	t.Run("registered ancestor becomes our predecessor and its sibling half", func(t *testing.T) {
		validation := queueSourcesTestValidation(t, child, false, []Neighbor{
			queueSourcesTestNeighbor(t, 0, rootID),
			queueSourcesTestNeighbor(t, 1, rootID),
		})
		if err := validation.loadQueueSources(); err != nil {
			t.Fatal(err)
		}

		// C++ replaces the ancestor in place with the sibling and appends the
		// local predecessor after every originally registered neighbor.
		want := []msgpool.ShardIdent{sibling, foreign, child}
		got := queueSourcesTestOwners(validation.sources)
		if len(got) != len(want) {
			t.Fatalf("sources = %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("sources = %v, want %v", got, want)
			}
		}
		for i := range validation.sources {
			if validation.sources[i].neighbor == nil {
				t.Fatalf("source %d has no neighbor view", i)
			}
		}
		if validation.sources[2].neighbor.EndLT != 9_999 {
			t.Fatalf("predecessor end lt = %d, want the previous state gen lt",
				validation.sources[2].neighbor.EndLT)
		}
	})

	t.Run("merge predecessors collapse into the merged predecessor", func(t *testing.T) {
		leftNeighbor := queueSourcesTestNeighbor(t, 0, left)
		leftNeighbor.EndLT = 7_000
		rightNeighbor := queueSourcesTestNeighbor(t, 0, right)
		rightNeighbor.EndLT = 8_000
		foreignNeighbor := queueSourcesTestNeighbor(t, 1, rootID)
		validation := queueSourcesTestValidation(t, root, true, []Neighbor{foreignNeighbor, leftNeighbor, rightNeighbor})
		validation.oldRecords = []tlb.ProcessedUptoRecord{{
			ShardPrefix: root.Shard,
			MCSeqno:     1,
			LastMsgLT:   100,
		}}
		validation.shardEndLT = newShardEndLTResolver(nil)
		if err := validation.loadQueueSources(); err != nil {
			t.Fatal(err)
		}

		if len(validation.sources) != 2 || validation.sources[0].owner != foreign || validation.sources[1].owner != root {
			t.Fatalf("sources = %v, want foreign then merged predecessor", queueSourcesTestOwners(validation.sources))
		}
		if validation.sources[1].neighbor.EndLT != leftNeighbor.EndLT {
			t.Fatalf("merged dequeue end lt = %d, want first child %d",
				validation.sources[1].neighbor.EndLT, leftNeighbor.EndLT)
		}

		entry := semanticQueueEntry{
			envelope: semanticEnvelope{next: msgpool.AccountPrefix{Workchain: 0, Prefix: 0x11}},
			descr: tlb.ProcessedMsgDescr{
				CurWorkchain:  0,
				CurPrefix:     0x11,
				NextWorkchain: 0,
				NextPrefix:    0x11,
				LT:            50,
			},
		}
		if err := validation.verifyDelivered(entry, leftNeighbor.EndLT); err != nil {
			t.Fatalf("C++ after-merge dequeue lt rejected: %v", err)
		}
		if err := validation.verifyDelivered(entry, validation.replay.previous.GenLT); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("merged predecessor gen lt error = %v, want invalid input", err)
		}
	})

	t.Run("own previous block is replaced in place", func(t *testing.T) {
		validation := queueSourcesTestValidation(t, child, false, []Neighbor{
			queueSourcesTestNeighbor(t, 0, left),
		})
		if err := validation.loadQueueSources(); err != nil {
			t.Fatal(err)
		}

		if len(validation.sources) != 1 || validation.sources[0].owner != child {
			t.Fatalf("sources = %v, want a single predecessor", queueSourcesTestOwners(validation.sources))
		}
	})

	t.Run("continued after merge replaces both registered children", func(t *testing.T) {
		validation := queueSourcesTestValidation(t, root, false, []Neighbor{
			queueSourcesTestNeighbor(t, 0, left),
			queueSourcesTestNeighbor(t, 0, right),
		})
		if err := validation.loadQueueSources(); err != nil {
			t.Fatal(err)
		}

		if len(validation.sources) != 1 || validation.sources[0].owner != root {
			t.Fatalf("sources = %v, want the continued merged predecessor", queueSourcesTestOwners(validation.sources))
		}
	})

	t.Run("after merge requires exactly the two predecessors", func(t *testing.T) {
		validation := queueSourcesTestValidation(t, root, true, []Neighbor{
			queueSourcesTestNeighbor(t, 0, left),
		})
		err := validation.loadQueueSources()
		if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "want the two predecessors") {
			t.Fatalf("error = %v, want ErrInvalidInput about the merge predecessors", err)
		}
	})
}

func TestSiblingQueueCutConsumesCXXTargetPrefixProof(t *testing.T) {
	left, err := shard.Child(int64(shard.Root), true)
	if err != nil {
		t.Fatal(err)
	}
	right, err := shard.Child(int64(shard.Root), false)
	if err != nil {
		t.Fatal(err)
	}
	ancestor := msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll}
	target := msgpool.ShardIdent{Workchain: 0, Shard: uint64(left)}
	sibling := msgpool.ShardIdent{Workchain: 0, Shard: uint64(right)}
	source := address.NewAddress(0, 0, bytes.Repeat([]byte{0xe1}, 32))
	fee := tlb.FromNanoTONU(100_000)

	inside, insideValue := queuedInternalWithReferencedBody(
		t,
		source,
		address.NewAddress(0, 0, bytes.Repeat([]byte{0x21}, 32)),
		10_000_001,
		1_900_000_000,
		fee,
		fee,
		0,
		ancestor,
	)
	outside, outsideValue := queuedInternalWithReferencedBody(
		t,
		source,
		address.NewAddress(0, 0, bytes.Repeat([]byte{0xe2}, 32)),
		10_000_002,
		1_900_000_000,
		fee,
		fee,
		0,
		ancestor,
	)
	queue, err := tlb.NewOutMsgQueueAugDict()
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		message *msgpool.InternalMessage
		value   *cell.Cell
	}{
		{message: inside, value: insideValue},
		{message: outside, value: outsideValue},
	} {
		key := cell.BeginCell().MustStoreSlice(item.message.Key[:], 352).EndCell()
		inserted, setErr := queue.SetWithMode(key, item.value, cell.DictSetModeAdd)
		if setErr != nil || !inserted {
			t.Fatalf("insert queue entry: inserted=%t err=%v", inserted, setErr)
		}
	}
	queueInfoRoot, err := (tlb.OutMsgQueueInfo{
		OutQueue: queue,
		ProcInfo: cell.NewDict(processedInfoKeyBits),
	}).ToCell()
	if err != nil {
		t.Fatal(err)
	}

	usage := cell.NewReadSet(queueInfoRoot)
	var traced tlb.OutMsgQueueInfo
	if err = parseExact(&traced, usage.Root()); err != nil {
		t.Fatal(err)
	}
	producerCut := &tlb.OutMsgQueueAugDict{AugmentedDictionary: traced.OutQueue.Copy()}
	prefix, err := outQueueShardPrefix(target)
	if err != nil {
		t.Fatal(err)
	}
	if ok, cutErr := producerCut.CutPrefixSubdict(prefix, false); cutErr != nil || !ok {
		t.Fatalf("C++ target-prefix cut: ok=%t err=%v", ok, cutErr)
	}
	ancestorIdent, err := semanticShardIdent(ancestor)
	if err != nil {
		t.Fatal(err)
	}
	siblingIdent, err := semanticShardIdent(sibling)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = filterOutQueue(producerCut, ancestorIdent, siblingIdent); err != nil {
		t.Fatalf("C++ sibling filter: %v", err)
	}

	proof, err := usage.Proof()
	if err != nil {
		t.Fatal(err)
	}
	virtual, err := cell.UnwrapProofVirtualized(proof, queueInfoRoot.Hash())
	if err != nil {
		t.Fatal(err)
	}
	var proven tlb.OutMsgQueueInfo
	if err = parseProofExact(&proven, virtual); err != nil {
		t.Fatal(err)
	}
	uncut := &tlb.OutMsgQueueAugDict{AugmentedDictionary: proven.OutQueue.Copy()}
	if _, err = filterOutQueue(uncut, ancestorIdent, siblingIdent); err == nil {
		t.Fatal("full ancestor scan unexpectedly consumed the C++ target-prefix proof")
	}
	consumerCut, err := siblingQueueCut(proven.OutQueue, ancestor, target, sibling)
	if err != nil {
		t.Fatalf("consume C++ target-prefix proof: %v", err)
	}
	insideKey := cell.BeginCell().MustStoreSlice(inside.Key[:], 352).EndCell()
	if _, err = consumerCut.LoadValue(insideKey); err != nil {
		t.Fatalf("load retained sibling message: %v", err)
	}
	outsideKey := cell.BeginCell().MustStoreSlice(outside.Key[:], 352).EndCell()
	if _, err = consumerCut.LoadValue(outsideKey); !isMissingKey(err) {
		t.Fatalf("off-prefix message remains in sibling queue: %v", err)
	}
}

func TestPrepareShardNeighborQueuesTracesVirtualSiblingEnvelope(t *testing.T) {
	left, err := shard.Child(int64(shard.Root), true)
	if err != nil {
		t.Fatal(err)
	}
	right, err := shard.Child(int64(shard.Root), false)
	if err != nil {
		t.Fatal(err)
	}
	ancestor := msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll}
	target := msgpool.ShardIdent{Workchain: 0, Shard: uint64(left)}
	sibling := msgpool.ShardIdent{Workchain: 0, Shard: uint64(right)}
	source := address.NewAddress(0, 0, bytes.Repeat([]byte{0xe1}, 32))
	fee := tlb.FromNanoTONU(100_000)

	message, value := queuedInternalWithReferencedBody(
		t,
		source,
		address.NewAddress(0, 0, bytes.Repeat([]byte{0x21}, 32)),
		10_000_001,
		1_900_000_000,
		fee,
		fee,
		0,
		ancestor,
	)
	queue, err := tlb.NewOutMsgQueueAugDict()
	if err != nil {
		t.Fatal(err)
	}
	key := cell.BeginCell().MustStoreSlice(message.Key[:], 352).EndCell()
	if inserted, setErr := queue.SetWithMode(key, value, cell.DictSetModeAdd); setErr != nil || !inserted {
		t.Fatalf("insert queue entry: inserted=%t err=%v", inserted, setErr)
	}
	queueRoot, err := (tlb.OutMsgQueueInfo{
		OutQueue: queue,
		ProcInfo: cell.NewDict(processedInfoKeyBits),
	}).ToCell()
	if err != nil {
		t.Fatal(err)
	}

	buildProof := func(prepare bool) *cell.Cell {
		t.Helper()

		builder := cell.NewMerkleProofBuilder(queueRoot)
		var traced tlb.OutMsgQueueInfo
		if parseErr := parseExact(&traced, builder.Root()); parseErr != nil {
			t.Fatal(parseErr)
		}
		if prepare {
			neighbors := []Neighbor{{Shard: ancestor}}
			views := map[msgpool.ShardIdent]*localNeighborView{
				ancestor: {queue: traced},
			}
			if prepareErr := prepareShardNeighborQueues(target, neighbors, views, false); prepareErr != nil {
				t.Fatal(prepareErr)
			}
		}
		proof, proofErr := builder.CreateProof()
		if proofErr != nil {
			t.Fatal(proofErr)
		}
		virtual, proofErr := cell.UnwrapProofVirtualized(proof, queueRoot.Hash())
		if proofErr != nil {
			t.Fatal(proofErr)
		}

		return virtual
	}

	var unprepared tlb.OutMsgQueueInfo
	if err = parseProofExact(&unprepared, buildProof(false)); err != nil {
		t.Fatal(err)
	}
	if _, err = siblingQueueCut(unprepared.OutQueue, ancestor, target, sibling); err == nil {
		t.Fatal("unprepared neighbor proof unexpectedly covers the virtual sibling envelope")
	}

	var prepared tlb.OutMsgQueueInfo
	if err = parseProofExact(&prepared, buildProof(true)); err != nil {
		t.Fatal(err)
	}
	if _, err = siblingQueueCut(prepared.OutQueue, ancestor, target, sibling); err != nil {
		t.Fatalf("prepared neighbor proof does not cover the virtual sibling envelope: %v", err)
	}
}
