package collator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

func TestSemanticOutQueueDeltaRejectParity(t *testing.T) {
	source := address.NewAddress(0, 0, bytes.Repeat([]byte{0x11}, 32))
	destination := address.NewAddress(0, 0, bytes.Repeat([]byte{0x22}, 32))
	envelope := routeFixtureEnvelope(t, source, destination, 96)
	other := routeFixtureEnvelope(t, source, address.NewAddress(0, 0, bytes.Repeat([]byte{0x33}, 32)), 96)
	incomingValue := envelope.value
	incomingValue.CurAddr.UseDestBits = 0
	incomingValue.NextAddr.UseDestBits = 64
	incoming := outQueueDeltaEnvelope(t, incomingValue)
	changedValue := envelope.value
	changedValue.CurAddr.UseDestBits = 64
	changed := outQueueDeltaEnvelope(t, changedValue)

	key := msgpool.MakeQueueKey(envelope.next, envelope.message.HashKey())
	otherKey := msgpool.MakeQueueKey(other.next, other.message.HashKey())
	incomingKey := msgpool.MakeQueueKey(incoming.next, incoming.message.HashKey())
	old := map[msgpool.QueueKey]*cell.Cell{key: outQueueDeltaValue(t, envelope, 6_000)}
	added := map[msgpool.QueueKey]*cell.Cell{key: outQueueDeltaValue(t, envelope, 11_000)}
	newDescriptor := outQueueDeltaDescriptor(semanticOutNew, envelope)
	dequeueDescriptor := outQueueDeltaDescriptor(semanticOutDequeue, envelope)
	shortDescriptor := outQueueDeltaDescriptor(semanticOutDequeueShort, envelope)
	wrongShort := outQueueDeltaDescriptor(semanticOutDequeueShort, envelope)
	wrongShort.descriptor.next = other.next
	wrongEnvelopeShort := outQueueDeltaDescriptor(semanticOutDequeueShort, envelope)
	wrongEnvelopeShort.descriptor.envelopeHash = other.root.HashKey()
	zero, maximum, wrongSize := uint64(0), uint64(maxOutMsgQueueSize), uint64(9)

	tests := []semanticOutQueueDeltaCase{
		{name: "empty"},
		{name: "unchanged", old: old, next: old},
		{name: "new", next: added, out: []semanticOutDescriptorEntry{newDescriptor}},
		{name: "full dequeue", old: old, out: []semanticOutDescriptorEntry{dequeueDescriptor}},
		{name: "short dequeue", old: old, out: []semanticOutDescriptorEntry{shortDescriptor}},
		{name: "unexplained insertion", next: added, reject: true},
		{name: "unexplained deletion", old: old, reject: true},
		{name: "unchanged new descriptor", old: old, next: old, out: []semanticOutDescriptorEntry{newDescriptor}, reject: true},
		{name: "unchanged dequeue descriptor", old: old, next: old, out: []semanticOutDescriptorEntry{dequeueDescriptor}, reject: true},
		{name: "absent new descriptor", out: []semanticOutDescriptorEntry{newDescriptor}, reject: true},
		{name: "absent dequeue descriptor", out: []semanticOutDescriptorEntry{dequeueDescriptor}, reject: true},
		{name: "mutation at the same key", old: old, next: added, out: []semanticOutDescriptorEntry{newDescriptor}, reject: true},
		{name: "dequeue cannot hide a mutation", old: old, next: added, out: []semanticOutDescriptorEntry{shortDescriptor}, reject: true},
		{name: "wrong full dequeue envelope", old: old, out: []semanticOutDescriptorEntry{outQueueDeltaDescriptor(semanticOutDequeue, changed)}, reject: true},
		{name: "wrong short dequeue next hop", old: old, out: []semanticOutDescriptorEntry{wrongShort}, reject: true},
		{name: "wrong short dequeue envelope", old: old, out: []semanticOutDescriptorEntry{wrongEnvelopeShort}, reject: true},
		{name: "new envelope differs from descriptor", next: map[msgpool.QueueKey]*cell.Cell{key: outQueueDeltaValue(t, changed, 11_000)}, out: []semanticOutDescriptorEntry{newDescriptor}, reject: true},
		{name: "queue key differs from envelope", next: map[msgpool.QueueKey]*cell.Cell{otherKey: added[key]}, out: []semanticOutDescriptorEntry{newDescriptor}, reject: true},
		{name: "same message added at two hops", next: map[msgpool.QueueKey]*cell.Cell{key: added[key], incomingKey: outQueueDeltaValue(t, incoming, 11_000)}, out: []semanticOutDescriptorEntry{newDescriptor}, reject: true},
		{name: "same message deleted at two hops", old: map[msgpool.QueueKey]*cell.Cell{key: old[key], incomingKey: outQueueDeltaValue(t, incoming, 6_000)}, out: []semanticOutDescriptorEntry{shortDescriptor}, reject: true},
		{name: "new descriptor cannot remove", old: old, out: []semanticOutDescriptorEntry{newDescriptor}, reject: true},
		{name: "dequeue descriptor cannot add", next: added, out: []semanticOutDescriptorEntry{shortDescriptor}, reject: true},
		{name: "external descriptor cannot add", next: added, out: []semanticOutDescriptorEntry{outQueueDeltaDescriptor(semanticOutExternal, envelope)}, reject: true},
		{name: "external descriptor cannot remove", old: old, out: []semanticOutDescriptorEntry{outQueueDeltaDescriptor(semanticOutExternal, envelope)}, reject: true},
		{name: "immediate descriptor cannot add", next: added, out: []semanticOutDescriptorEntry{outQueueDeltaDescriptor(semanticOutImmediate, envelope)}, reject: true},
		{name: "deferred descriptor cannot add", next: added, out: []semanticOutDescriptorEntry{outQueueDeltaDescriptor(semanticOutNewDeferred, envelope)}, reject: true},
		{name: "queue size mismatch", next: added, out: []semanticOutDescriptorEntry{newDescriptor}, candidateSize: &wrongSize, reject: true},
		{name: "queue size overflow", next: added, out: []semanticOutDescriptorEntry{newDescriptor}, initialSize: &maximum, reject: true},
		{name: "queue size underflow", old: old, out: []semanticOutDescriptorEntry{dequeueDescriptor}, initialSize: &zero, reject: true},
		{name: "dequeued lt reaches block start", old: map[msgpool.QueueKey]*cell.Cell{key: outQueueDeltaValue(t, envelope, 10_000)}, out: []semanticOutDescriptorEntry{dequeueDescriptor}, reject: true},
		{name: "enqueued lt precedes block", next: old, out: []semanticOutDescriptorEntry{newDescriptor}, reject: true},
		{name: "enqueued lt reaches block end", next: map[msgpool.QueueKey]*cell.Cell{key: outQueueDeltaValue(t, envelope, 20_000)}, out: []semanticOutDescriptorEntry{newDescriptor}, reject: true},
		{name: "new while keeping an unrelated old entry", old: map[msgpool.QueueKey]*cell.Cell{otherKey: outQueueDeltaValue(t, other, 6_000)}, next: map[msgpool.QueueKey]*cell.Cell{otherKey: outQueueDeltaValue(t, other, 6_000), key: added[key]}, out: []semanticOutDescriptorEntry{newDescriptor}},
		{name: "add and remove different messages", old: map[msgpool.QueueKey]*cell.Cell{otherKey: outQueueDeltaValue(t, other, 6_000)}, next: added, out: []semanticOutDescriptorEntry{newDescriptor, outQueueDeltaDescriptor(semanticOutDequeueShort, other)}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			validation := newOutQueueDeltaValidation(t, test)
			oldRoot := validation.old.OutQueue.RootCell()
			newRoot := validation.candidate.OutQueue.RootCell()
			legacyErr := reconstructOutQueueDelta(validation)
			err := verifyOutQueueDelta(validation)
			if (legacyErr != nil) != test.reject {
				t.Fatalf("reconstruction oracle error = %v, reject = %v", legacyErr, test.reject)
			}
			if (err != nil) != test.reject {
				t.Fatalf("direct validation error = %v, reject = %v", err, test.reject)
			}
			if err != nil && !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("direct validation error = %v, want ErrInvalidInput", err)
			}
			if !equalCell(oldRoot, validation.old.OutQueue.RootCell()) || !equalCell(newRoot, validation.candidate.OutQueue.RootCell()) {
				t.Fatal("verification mutated an authenticated queue")
			}
		})
	}
}

func TestSemanticOutQueueTransitAndMerge(t *testing.T) {
	source := address.NewAddress(0, 0, bytes.Repeat([]byte{0x11}, 32))
	destination := address.NewAddress(0, 0, bytes.Repeat([]byte{0x22}, 32))
	envelope := routeFixtureEnvelope(t, source, destination, 96)
	incomingValue := envelope.value
	incomingValue.CurAddr.UseDestBits = 0
	incomingValue.NextAddr.UseDestBits = 64
	incoming := outQueueDeltaEnvelope(t, incomingValue)
	key := msgpool.MakeQueueKey(envelope.next, envelope.message.HashKey())
	incomingKey := msgpool.MakeQueueKey(incoming.next, incoming.message.HashKey())
	inbound := &semanticInDescriptor{
		tag: semanticInTransit, root: cell.BeginCell().EndCell(),
		envelope: incoming, outEnvelope: envelope,
	}
	transit := outQueueDeltaDescriptor(semanticOutTransit, envelope)
	transit.descriptor.reimport = inbound.root
	transitRequest := outQueueDeltaDescriptor(semanticOutTransitRequest, envelope)
	transitRequest.descriptor.reimport = inbound.root
	final := &semanticInDescriptor{tag: semanticInFinal, root: cell.BeginCell().EndCell(), envelope: envelope}
	dequeueImmediate := outQueueDeltaDescriptor(semanticOutDequeueImmediate, envelope)
	dequeueImmediate.descriptor.reimport = final.root

	tests := []semanticOutQueueDeltaCase{
		{name: "transit insertion", next: map[msgpool.QueueKey]*cell.Cell{key: outQueueDeltaValue(t, envelope, 11_000)}, out: []semanticOutDescriptorEntry{transit}, in: inbound},
		{name: "merge transit requeue", old: map[msgpool.QueueKey]*cell.Cell{incomingKey: outQueueDeltaValue(t, incoming, 6_000)}, next: map[msgpool.QueueKey]*cell.Cell{key: outQueueDeltaValue(t, envelope, 11_000)}, out: []semanticOutDescriptorEntry{transitRequest}, in: inbound},
		{name: "transit request missing removal", next: map[msgpool.QueueKey]*cell.Cell{key: outQueueDeltaValue(t, envelope, 11_000)}, out: []semanticOutDescriptorEntry{transitRequest}, in: inbound, reject: true},
		{name: "transit request missing insertion", old: map[msgpool.QueueKey]*cell.Cell{incomingKey: outQueueDeltaValue(t, incoming, 6_000)}, out: []semanticOutDescriptorEntry{transitRequest}, in: inbound, reject: true},
		{name: "transit request keeps imported entry", old: map[msgpool.QueueKey]*cell.Cell{incomingKey: outQueueDeltaValue(t, incoming, 6_000)}, next: map[msgpool.QueueKey]*cell.Cell{incomingKey: outQueueDeltaValue(t, incoming, 6_000), key: outQueueDeltaValue(t, envelope, 11_000)}, out: []semanticOutDescriptorEntry{transitRequest}, in: inbound, reject: true},
		{name: "transit request missing reimport", old: map[msgpool.QueueKey]*cell.Cell{incomingKey: outQueueDeltaValue(t, incoming, 6_000)}, next: map[msgpool.QueueKey]*cell.Cell{key: outQueueDeltaValue(t, envelope, 11_000)}, out: []semanticOutDescriptorEntry{transitRequest}, reject: true},
		{name: "merge immediate dequeue", old: map[msgpool.QueueKey]*cell.Cell{key: outQueueDeltaValue(t, envelope, 6_000)}, out: []semanticOutDescriptorEntry{dequeueImmediate}, in: final},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			validation := newOutQueueDeltaValidation(t, test)
			validation.replay.candidate.block.BlockInfo.AfterMerge = true
			legacyErr := reconstructOutQueueDelta(validation)
			err := verifyOutQueueDelta(validation)
			if (legacyErr != nil) != test.reject || (err != nil) != test.reject {
				t.Fatalf("reconstruction = %v, direct = %v, reject = %v", legacyErr, err, test.reject)
			}
		})
	}

	t.Run("delivered merged entry must be removed", func(t *testing.T) {
		old := map[msgpool.QueueKey]*cell.Cell{key: outQueueDeltaValue(t, envelope, 6_000)}
		validation := newOutQueueDeltaValidation(t, semanticOutQueueDeltaCase{old: old, next: old})
		if err := verifyOutQueueDelta(validation); err != nil {
			t.Fatal(err)
		}
		if err := validation.verifyMergedQueueCleanup(); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("unchanged delivered merge entry = %v, want ErrInvalidInput", err)
		}
		validation = newOutQueueDeltaValidation(t, semanticOutQueueDeltaCase{old: old, out: []semanticOutDescriptorEntry{outQueueDeltaDescriptor(semanticOutDequeueShort, envelope)}})
		if err := verifyOutQueueDelta(validation); err != nil {
			t.Fatal(err)
		}
		if err := validation.verifyMergedQueueCleanup(); err != nil {
			t.Fatalf("removed delivered merge entry = %v", err)
		}
	})
}

func TestSemanticOutQueueDiffChecksAugmentationAndPrunedChanges(t *testing.T) {
	envelope := routeFixtureEnvelope(t, address.NewAddress(0, 0, bytes.Repeat([]byte{0x11}, 32)), address.NewAddress(0, 0, bytes.Repeat([]byte{0x22}, 32)), 96)
	key := msgpool.MakeQueueKey(envelope.next, envelope.message.HashKey())
	value := outQueueDeltaValue(t, envelope, 11_000)
	test := semanticOutQueueDeltaCase{next: map[msgpool.QueueKey]*cell.Cell{key: value}, out: []semanticOutDescriptorEntry{outQueueDeltaDescriptor(semanticOutNew, envelope)}}
	validation := newOutQueueDeltaValidation(t, test)
	if err := verifyOutQueueDelta(validation); err != nil {
		t.Fatalf("valid control = %v", err)
	}

	t.Run("forged leaf augmentation", func(t *testing.T) {
		validation := newOutQueueDeltaValidation(t, test)
		root := cell.BeginCell().MustStoreUInt(2, 2).MustStoreUInt(352, 9).MustStoreSlice(key[:], 352).
			MustStoreUInt(envelope.bound.lt+1, 64).MustStoreBuilder(value.ToBuilder()).EndCell()
		dict, err := root.MustBeginParse().ToAugDictWithAugmentation(352, tlb.AugOutMsgQueue{})
		if err != nil {
			t.Fatal(err)
		}
		validation.candidate.OutQueue = &tlb.OutMsgQueueAugDict{AugmentedDictionary: dict}
		if err = validation.precheckOutQueueUpdate(); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("forged augmentation = %v, want ErrInvalidInput", err)
		}
	})

	t.Run("forged fork augmentation", func(t *testing.T) {
		other := routeFixtureEnvelope(t, envelope.internal.SrcAddr, address.NewAddress(0, 0, bytes.Repeat([]byte{0x33}, 32)), 96)
		otherKey := msgpool.MakeQueueKey(other.next, other.message.HashKey())
		validation := newOutQueueDeltaValidation(t, semanticOutQueueDeltaCase{
			next: map[msgpool.QueueKey]*cell.Cell{key: value, otherKey: outQueueDeltaValue(t, other, 11_000)},
			out:  []semanticOutDescriptorEntry{outQueueDeltaDescriptor(semanticOutNew, envelope), outQueueDeltaDescriptor(semanticOutNew, other)},
		})
		if err := verifyOutQueueDelta(validation); err != nil {
			t.Fatalf("valid fork control = %v", err)
		}
		loader := validation.candidate.OutQueue.RootCell().MustBeginParse()
		labelBits := loader.BitsLeft() - 64
		label := loader.MustLoadSlice(labelBits)
		if loader.RefsNum() != 2 {
			t.Fatal("fixture root is not a fork")
		}
		root := cell.BeginCell().MustStoreSlice(label, labelBits).MustStoreUInt(envelope.bound.lt+1, 64).
			MustStoreRef(loader.MustLoadRef().MustToCell()).MustStoreRef(loader.MustLoadRef().MustToCell()).EndCell()
		dict, err := root.MustBeginParse().ToAugDictWithAugmentation(352, tlb.AugOutMsgQueue{})
		if err != nil {
			t.Fatal(err)
		}
		validation.candidate.OutQueue = &tlb.OutMsgQueueAugDict{AugmentedDictionary: dict}
		if err = validation.precheckOutQueueUpdate(); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("forged fork augmentation = %v, want ErrInvalidInput", err)
		}
	})

	t.Run("unchanged pruned subtree remains opaque", func(t *testing.T) {
		proof, err := validation.candidate.OutQueue.RootCell().CreateProof(cell.CreateProofSkeleton())
		if err != nil {
			t.Fatal(err)
		}
		root, err := cell.UnwrapProof(proof, validation.candidate.OutQueue.RootCell().Hash())
		if err != nil {
			t.Fatal(err)
		}
		dict, err := root.MustBeginParse().ToAugDictWithAugmentation(352, tlb.AugOutMsgQueue{})
		if err != nil {
			t.Fatal(err)
		}
		pruned := &tlb.OutMsgQueueAugDict{AugmentedDictionary: dict}
		unchanged := newOutQueueDeltaValidation(t, semanticOutQueueDeltaCase{})
		unchanged.old.OutQueue, unchanged.candidate.OutQueue = pruned, pruned
		if err = unchanged.precheckOutQueueUpdate(); err != nil {
			t.Fatalf("unchanged proof subtree = %v", err)
		}
		changed := newOutQueueDeltaValidation(t, test)
		changed.candidate.OutQueue = pruned
		if err = changed.precheckOutQueueUpdate(); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("changed pruned envelope = %v, want ErrInvalidInput", err)
		}
	})
}

// C++ accepts both hml_short and hml_long (dict.cpp:283), then compares leaf
// contents after skipping the labels (dict.cpp:2404-2420). Re-encoding a label
// changes the authenticated root, but does not create a message-queue delta.
// The former reconstructed-root comparison rejected these valid encodings.
func TestSemanticOutQueueAcceptsEquivalentLabelEncoding(t *testing.T) {
	envelope := routeFixtureEnvelope(t, address.NewAddress(0, 0, bytes.Repeat([]byte{0x11}, 32)), address.NewAddress(0, 0, bytes.Repeat([]byte{0x22}, 32)), 96)
	key := msgpool.MakeQueueKey(envelope.next, envelope.message.HashKey())
	value := outQueueDeltaValue(t, envelope, 11_000)
	root := cell.BeginCell().MustStoreUInt(0, 1).MustStoreSlice(bytes.Repeat([]byte{0xff}, 44), 352).
		MustStoreUInt(0, 1).MustStoreSlice(key[:], 352).MustStoreUInt(envelope.bound.lt, 64).
		MustStoreBuilder(value.ToBuilder()).EndCell()
	dict, err := root.MustBeginParse().ToAugDictWithAugmentation(352, tlb.AugOutMsgQueue{})
	if err != nil {
		t.Fatal(err)
	}
	for _, unchanged := range []bool{false, true} {
		name := "new entry with short label"
		test := semanticOutQueueDeltaCase{
			next: map[msgpool.QueueKey]*cell.Cell{key: value},
			out:  []semanticOutDescriptorEntry{outQueueDeltaDescriptor(semanticOutNew, envelope)},
		}
		if unchanged {
			name = "unchanged entry with another label encoding"
			test.old, test.out = test.next, nil
		}
		t.Run(name, func(t *testing.T) {
			validation := newOutQueueDeltaValidation(t, test)
			if equalCell(validation.candidate.OutQueue.RootCell(), root) {
				t.Fatal("fixture label is already the default encoding")
			}
			validation.candidate.OutQueue = &tlb.OutMsgQueueAugDict{AugmentedDictionary: dict}
			if err := reconstructOutQueueDelta(validation); err == nil {
				t.Fatal("former root comparison unexpectedly accepted another encoding")
			}
			if err := verifyOutQueueDelta(validation); err != nil {
				t.Fatalf("reference-compatible label encoding = %v", err)
			}
		})
	}
}

type semanticOutQueueDeltaCase struct {
	name          string
	old, next     map[msgpool.QueueKey]*cell.Cell
	out           []semanticOutDescriptorEntry
	in            *semanticInDescriptor
	initialSize   *uint64
	candidateSize *uint64
	reject        bool
}

func newOutQueueDeltaValidation(t *testing.T, test semanticOutQueueDeltaCase) *semanticQueueValidation {
	t.Helper()
	dispatch, err := tlb.NewDispatchQueueAugDict()
	if err != nil {
		t.Fatal(err)
	}
	queueSize, candidateSize := uint64(len(test.old)), uint64(len(test.next))
	if test.initialSize != nil {
		queueSize = *test.initialSize
	}
	if test.candidateSize != nil {
		candidateSize = *test.candidateSize
	}
	candidate := &verifiedCandidate{}
	candidate.block.BlockInfo.StartLt, candidate.block.BlockInfo.EndLt = 10_000, 20_000
	target := msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll}
	validation := &semanticQueueValidation{
		replay:    &semanticReplay{ctx: context.Background(), candidate: candidate, transition: CandidateTransition{Config: &Config{}}},
		target:    target,
		old:       tlb.OutMsgQueueInfo{OutQueue: newQueueBatchTestQueue(t, test.old)},
		candidate: &tlb.OutMsgQueueInfo{OutQueue: newQueueBatchTestQueue(t, test.next), Extra: &tlb.OutMsgQueueExtra{DispatchQueue: dispatch, OutQueueSize: &candidateSize}},
		dispatch:  dispatch, queueSize: queueSize,
		out: make(map[cell.Hash]*semanticOutDescriptor), outOrder: test.out,
		in: make(map[cell.Hash]*semanticInDescriptor), shardEndLT: newShardEndLTResolver(nil),
		sources: []semanticQueueSource{{owner: target, neighbor: &Neighbor{Shard: target, EndLT: 7_000, Processed: []tlb.ProcessedUptoRecord{{ShardPrefix: target.Shard, MCSeqno: 1, LastMsgLT: 9_000}}}}},
	}
	for _, entry := range test.out {
		validation.out[entry.hash] = entry.descriptor
	}
	if test.in != nil {
		validation.in[test.in.envelope.message.HashKey()] = test.in
	}
	return validation
}

func outQueueDeltaEnvelope(t *testing.T, value tlb.MsgEnvelope) *semanticEnvelope {
	t.Helper()
	root, err := value.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := parseSemanticEnvelope(root)
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}

func outQueueDeltaValue(t *testing.T, envelope *semanticEnvelope, lt uint64) *cell.Cell {
	t.Helper()
	value, err := (tlb.EnqueuedMsg{EnqueuedLT: lt, Msg: envelope.root}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func outQueueDeltaDescriptor(tag uint8, envelope *semanticEnvelope) semanticOutDescriptorEntry {
	descriptor := &semanticOutDescriptor{tag: tag, message: envelope.message, envelope: envelope, importBlockLT: 7_000}
	if tag == semanticOutDequeueShort {
		descriptor.envelope, descriptor.message = nil, nil
		descriptor.next, descriptor.envelopeHash = envelope.next, envelope.root.HashKey()
	}
	return semanticOutDescriptorEntry{hash: envelope.message.HashKey(), descriptor: descriptor}
}

func verifyOutQueueDelta(validation *semanticQueueValidation) error {
	if err := validation.precheckOutQueueUpdate(); err != nil {
		return err
	}
	if err := validation.verifyOutQueueChanges(); err != nil {
		return err
	}
	return validation.verifyQueueRoots()
}

// reconstructOutQueueDelta is the previous queue relation oracle: apply the
// descriptors to a copy, then compare the complete authenticated root. Routing
// and transaction replay are covered separately by the semantic verifier tests.
func reconstructOutQueueDelta(v *semanticQueueValidation) error {
	queue := &tlb.OutMsgQueueAugDict{AugmentedDictionary: v.old.OutQueue.Copy()}
	size := v.queueSize
	add := func(envelope *semanticEnvelope) error {
		key := msgpool.MakeQueueKey(envelope.next, envelope.message.HashKey())
		entry, err := loadSemanticQueueEntry(v.candidate.OutQueue, key, &v.replay.envelopes)
		if err != nil {
			return err
		}
		if !equalCell(entry.envelope.root, envelope.root) || entry.enqueued.EnqueuedLT < 10_000 || entry.enqueued.EnqueuedLT >= 20_000 || entry.enqueued.EnqueuedLT < entry.envelope.bound.lt || size == maxOutMsgQueueSize {
			return ErrInvalidInput
		}
		value, err := entry.enqueued.ToCell()
		if err != nil {
			return err
		}
		inserted, err := queue.SetBuilderByBytesKeyWithMode(key[:], value.ToBuilder(), cell.DictSetModeAdd)
		if err != nil || !inserted {
			return ErrInvalidInput
		}
		size++
		return nil
	}
	remove := func(next msgpool.AccountPrefix, hash, envelopeHash cell.Hash) error {
		key := msgpool.MakeQueueKey(next, hash)
		var value cell.Slice
		if err := queue.LoadValueAndDeleteByBytesKeyInto(key[:], &value); err != nil {
			return err
		}
		entry, err := parseSemanticQueueEntryKeyWithMode(key, &value, nil, false, semanticQueueLeafCells{}, &v.replay.envelopes)
		if err != nil {
			return err
		}
		if entry.enqueued.EnqueuedLT >= 10_000 || entry.enqueued.Msg.HashKey() != envelopeHash || size == 0 {
			return ErrInvalidInput
		}
		size--
		return nil
	}
	for _, item := range v.outOrder {
		descriptor := item.descriptor
		var err error
		switch descriptor.tag {
		case semanticOutNew, semanticOutTransit, semanticOutDeferredTransit:
			err = add(descriptor.envelope)
		case semanticOutDequeue, semanticOutDequeueImmediate:
			err = remove(descriptor.envelope.next, item.hash, descriptor.envelope.root.HashKey())
		case semanticOutDequeueShort:
			err = remove(descriptor.next, item.hash, cell.Hash(descriptor.envelopeHash))
		case semanticOutTransitRequest:
			inbound := v.in[item.hash]
			if inbound == nil {
				return ErrInvalidInput
			}
			if err = remove(inbound.envelope.next, item.hash, inbound.envelope.root.HashKey()); err == nil {
				err = add(descriptor.envelope)
			}
		}
		if err != nil {
			return err
		}
	}
	if !equalCell(queue.RootCell(), v.candidate.OutQueue.RootCell()) || *v.candidate.Extra.OutQueueSize != size {
		return fmt.Errorf("%w: reconstruction differs", ErrInvalidInput)
	}
	return nil
}
