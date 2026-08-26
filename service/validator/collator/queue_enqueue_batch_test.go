package collator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

type queueBatchOperation struct {
	add   bool
	key   msgpool.QueueKey
	value *cell.Cell
	kind  queuePendingAddKind
}

func queueBatchKey(seed uint64) msgpool.QueueKey {
	hash := sha256.Sum256(binary.BigEndian.AppendUint64(nil, seed))
	var key msgpool.QueueKey
	binary.BigEndian.PutUint32(key[:4], 0)
	copy(key[4:12], hash[:8])
	copy(key[12:], hash[:])
	return key
}

func queueBatchValue(tb testing.TB, lt uint64) *cell.Cell {
	tb.Helper()

	source := address.NewAddress(0, 0, make([]byte, 32))
	destinationData := make([]byte, 32)
	binary.BigEndian.PutUint64(destinationData[24:], lt)
	destination := address.NewAddress(0, 0, destinationData)
	message, err := tlb.ToCell(&tlb.InternalMessage{
		IHRDisabled: true,
		SrcAddr:     source,
		DstAddr:     destination,
		Amount:      tlb.FromNanoTONU(1_000_000_000),
		FwdFee:      tlb.FromNanoTONU(100_000),
		CreatedLT:   lt,
		CreatedAt:   1_700_000_000,
		Body:        cell.BeginCell().MustStoreUInt(lt, 64).EndCell(),
	})
	if err != nil {
		tb.Fatal(err)
	}
	envelope, err := (tlb.MsgEnvelope{
		CurAddr:         tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular},
		NextAddr:        tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: 96},
		FwdFeeRemaining: tlb.FromNanoTONU(100_000),
		Msg:             message,
	}).ToCell()
	if err != nil {
		tb.Fatal(err)
	}
	value, err := (tlb.EnqueuedMsg{EnqueuedLT: lt, Msg: envelope}).ToCell()
	if err != nil {
		tb.Fatal(err)
	}
	return value
}

func newQueueBatchTestQueue(tb testing.TB, initial map[msgpool.QueueKey]*cell.Cell) *tlb.OutMsgQueueAugDict {
	tb.Helper()

	queue, err := tlb.NewOutMsgQueueAugDict()
	if err != nil {
		tb.Fatal(err)
	}
	for key, value := range initial {
		var builder cell.Builder
		value.ToBuilderInto(&builder)
		inserted, setErr := queue.SetBuilderByBytesKeyWithMode(key[:], &builder, cell.DictSetModeAdd)
		if setErr != nil {
			tb.Fatal(setErr)
		}
		if !inserted {
			tb.Fatalf("duplicate initial key %x", key)
		}
	}
	return queue
}

func runSequentialQueueBatchOperations(
	tb testing.TB,
	initial map[msgpool.QueueKey]*cell.Cell,
	operations []queueBatchOperation,
) *tlb.OutMsgQueueAugDict {
	tb.Helper()

	queue := newQueueBatchTestQueue(tb, initial)
	for i := range operations {
		op := &operations[i]
		if op.add {
			var builder cell.Builder
			op.value.ToBuilderInto(&builder)
			inserted, err := queue.SetBuilderByBytesKeyWithMode(op.key[:], &builder, cell.DictSetModeAdd)
			if err != nil {
				tb.Fatalf("sequential add %d: %v", i, err)
			}
			if !inserted {
				tb.Fatalf("sequential add %d duplicated key %x", i, op.key)
			}
			continue
		}

		var value cell.Slice
		if err := queue.LoadValueAndDeleteByBytesKeyInto(op.key[:], &value); err != nil {
			tb.Fatalf("sequential delete %d: %v", i, err)
		}
	}
	return queue
}

func runBatchedQueueBatchOperations(
	tb testing.TB,
	initial map[msgpool.QueueKey]*cell.Cell,
	operations []queueBatchOperation,
) *tlb.OutMsgQueueAugDict {
	tb.Helper()

	c := &collation{outQueue: newQueueBatchTestQueue(tb, initial)}
	for i := range operations {
		op := &operations[i]
		if op.add {
			if err := c.deferQueueAdd(op.key, op.value, op.kind); err != nil {
				tb.Fatalf("batched add %d: %v", i, err)
			}
			continue
		}

		if err := c.flushQueueAdds(); err != nil {
			tb.Fatalf("flush before delete %d: %v", i, err)
		}
		var value cell.Slice
		if err := c.outQueue.LoadValueByBytesKeyInto(op.key[:], &value); err != nil {
			tb.Fatalf("batched lookup before delete %d: %v", i, err)
		}
		if err := c.deferQueueDelete(op.key); err != nil {
			tb.Fatalf("batched delete %d: %v", i, err)
		}
	}
	if err := c.flushQueueDeletes(); err != nil {
		tb.Fatalf("final queue flush: %v", err)
	}
	return c.outQueue
}

func TestQueueAddBatchMatchesSequentialMutationOrder(t *testing.T) {
	keyA, keyB, keyC := queueBatchKey(1), queueBatchKey(2), queueBatchKey(3)
	valueA := queueBatchValue(t, 101)
	valueB := queueBatchValue(t, 102)
	valueC := queueBatchValue(t, 103)
	replacement := queueBatchValue(t, 201)

	tests := []struct {
		name       string
		initial    map[msgpool.QueueKey]*cell.Cell
		operations []queueBatchOperation
	}{
		{
			name: "add run",
			operations: []queueBatchOperation{
				{add: true, key: keyA, value: valueA, kind: queuePendingAddGenerated},
				{add: true, key: keyB, value: valueB, kind: queuePendingAddTransit},
				{add: true, key: keyC, value: valueC, kind: queuePendingAddGenerated},
			},
		},
		{
			name: "add then delete same key",
			operations: []queueBatchOperation{
				{add: true, key: keyA, value: valueA, kind: queuePendingAddGenerated},
				{key: keyA},
			},
		},
		{
			name:    "delete then add same key",
			initial: map[msgpool.QueueKey]*cell.Cell{keyA: valueA},
			operations: []queueBatchOperation{
				{key: keyA},
				{add: true, key: keyA, value: replacement, kind: queuePendingAddTransit},
			},
		},
		{
			name: "alternating kinds and keys",
			initial: map[msgpool.QueueKey]*cell.Cell{
				keyA: valueA,
				keyB: valueB,
			},
			operations: []queueBatchOperation{
				{key: keyA},
				{add: true, key: keyC, value: valueC, kind: queuePendingAddGenerated},
				{key: keyB},
				{add: true, key: keyA, value: replacement, kind: queuePendingAddTransit},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sequential := runSequentialQueueBatchOperations(t, test.initial, test.operations)
			batched := runBatchedQueueBatchOperations(t, test.initial, test.operations)
			if !equalCell(sequential.RootCell(), batched.RootCell()) {
				t.Fatal("sequential and batched roots differ")
			}
		})
	}
}

func TestQueueAddBatchRejectsDuplicatesAtCanonicalOperation(t *testing.T) {
	key := queueBatchKey(11)
	value := queueBatchValue(t, 111)

	t.Run("materialized generated key", func(t *testing.T) {
		queue := newQueueBatchTestQueue(t, map[msgpool.QueueKey]*cell.Cell{key: value})
		before := queue.RootCell().HashKey()
		c := &collation{outQueue: queue}

		err := c.deferQueueAdd(key, value, queuePendingAddGenerated)
		if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "duplicate outbound queue key") {
			t.Fatalf("duplicate error = %v", err)
		}
		if len(c.queuePendingAdd) != 0 {
			t.Fatalf("duplicate left %d pending additions", len(c.queuePendingAdd))
		}
		if got := queue.RootCell().HashKey(); got != before {
			t.Fatalf("duplicate changed root: got %x want %x", got, before)
		}
	})

	t.Run("pending transit key", func(t *testing.T) {
		c := &collation{outQueue: newQueueBatchTestQueue(t, nil)}
		if err := c.deferQueueAdd(key, value, queuePendingAddGenerated); err != nil {
			t.Fatal(err)
		}
		err := c.deferQueueAdd(key, value, queuePendingAddTransit)
		if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "duplicate transit queue key") {
			t.Fatalf("duplicate error = %v", err)
		}
		if len(c.queuePendingAdd) != 1 {
			t.Fatalf("pending additions = %d, want 1", len(c.queuePendingAdd))
		}
	})
}

func TestQueueAddBatchFlushCadenceAndLimits(t *testing.T) {
	value := queueBatchValue(t, 301)
	for _, count := range []int{63, 64, 65} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			sequentialQueue := newQueueBatchTestQueue(t, nil)
			batchedQueue := newQueueBatchTestQueue(t, nil)
			sequentialLimits := newBlockLimitStatus(blockLimits{}, 0, cell.NewReadSet(nil), 0, 0)
			batchedLimits := newBlockLimitStatus(blockLimits{}, 0, cell.NewReadSet(nil), 0, 0)
			batched := &collation{outQueue: batchedQueue, limits: batchedLimits}

			for i := range count {
				key := queueBatchKey(uint64(1_000 + i))
				var builder cell.Builder
				value.ToBuilderInto(&builder)
				inserted, err := sequentialQueue.SetBuilderByBytesKeyWithMode(key[:], &builder, cell.DictSetModeAdd)
				if err != nil || !inserted {
					t.Fatalf("sequential add %d: inserted=%v err=%v", i, inserted, err)
				}
				if (i+1)&63 == 0 {
					if err = sequentialLimits.addProof(sequentialQueue.RootCell()); err != nil {
						t.Fatal(err)
					}
				}

				if err = batched.deferQueueAdd(key, value, queuePendingAddGenerated); err != nil {
					t.Fatalf("batched add %d: %v", i, err)
				}
				if err = batched.registerQueueOp(); err != nil {
					t.Fatalf("register op %d: %v", i, err)
				}
			}

			wantPending := count % 64
			if got := len(batched.queuePendingAdd); got != wantPending {
				t.Fatalf("pending additions = %d, want %d", got, wantPending)
			}
			if err := batched.flushQueueDeletes(); err != nil {
				t.Fatal(err)
			}
			if sequentialQueue.RootCell().HashKey() != batchedQueue.RootCell().HashKey() {
				t.Fatal("final root differs")
			}
			if sequentialLimits.estimatedBytes() != batchedLimits.estimatedBytes() {
				t.Fatalf("sampled limit estimate = %d, want %d",
					batchedLimits.estimatedBytes(), sequentialLimits.estimatedBytes())
			}
		})
	}
}

func TestQueueMutationBatchFlushCadenceAndLimits(t *testing.T) {
	value := queueBatchValue(t, 351)
	for _, count := range []int{63, 64, 65} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			initial := make(map[msgpool.QueueKey]*cell.Cell, count/2+1)
			operations := make([]queueBatchOperation, count)
			deleteIndex := 0
			for i := range operations {
				if i%4 < 2 {
					operations[i] = queueBatchOperation{
						add:   true,
						key:   queueBatchKey(uint64(20_000 + i)),
						value: value,
						kind:  queuePendingAddGenerated,
					}
					continue
				}

				key := queueBatchKey(uint64(30_000 + deleteIndex))
				deleteIndex++
				initial[key] = value
				operations[i] = queueBatchOperation{key: key}
			}

			sequentialQueue := newQueueBatchTestQueue(t, initial)
			batchedQueue := newQueueBatchTestQueue(t, initial)
			sequentialLimits := newBlockLimitStatus(blockLimits{}, 0, cell.NewReadSet(nil), 0, 0)
			batchedLimits := newBlockLimitStatus(blockLimits{}, 0, cell.NewReadSet(nil), 0, 0)
			batched := &collation{outQueue: batchedQueue, limits: batchedLimits}

			for i := range operations {
				op := &operations[i]
				if op.add {
					var builder cell.Builder
					op.value.ToBuilderInto(&builder)
					inserted, err := sequentialQueue.SetBuilderByBytesKeyWithMode(
						op.key[:],
						&builder,
						cell.DictSetModeAdd,
					)
					if err != nil || !inserted {
						t.Fatalf("sequential add %d: inserted=%v err=%v", i, inserted, err)
					}
					if err = batched.deferQueueAdd(op.key, op.value, op.kind); err != nil {
						t.Fatalf("batched add %d: %v", i, err)
					}
				} else {
					var removed cell.Slice
					if err := sequentialQueue.LoadValueAndDeleteByBytesKeyInto(op.key[:], &removed); err != nil {
						t.Fatalf("sequential delete %d: %v", i, err)
					}
					if err := batched.deferQueueDelete(op.key); err != nil {
						t.Fatalf("batched delete %d: %v", i, err)
					}
				}

				if (i+1)&63 == 0 {
					if err := sequentialLimits.addProof(sequentialQueue.RootCell()); err != nil {
						t.Fatal(err)
					}
				}
				if err := batched.registerQueueOp(); err != nil {
					t.Fatalf("register op %d: %v", i, err)
				}
			}

			if err := batched.flushQueueDeletes(); err != nil {
				t.Fatal(err)
			}
			if sequentialQueue.RootCell().HashKey() != batchedQueue.RootCell().HashKey() {
				t.Fatal("final root differs")
			}
			if sequentialLimits.estimatedBytes() != batchedLimits.estimatedBytes() {
				t.Fatalf("sampled limit estimate = %d, want %d",
					batchedLimits.estimatedBytes(), sequentialLimits.estimatedBytes())
			}
		})
	}
}

func TestQueueAddBatchKeepsImmediateAndQueuedBranchesDistinct(t *testing.T) {
	sender := address.NewAddress(0, 0, bytes.Repeat([]byte{0x71}, 32))
	receiver := address.NewAddress(0, 0, bytes.Repeat([]byte{0x72}, 32))
	req := emptyCandidateRequest(t)
	req.Internals = &msgpool.Cut{}
	req.Previous.State = stateWithAccounts(t, req.Previous.State, activeContracts(t, req.Header.GenUtime,
		activeContract{address: sender, code: externalAcceptCode(t), balance: 100_000_000_000},
		activeContract{address: receiver, code: externalAcceptCode(t), balance: 100_000_000_000},
	))

	build := func(t *testing.T, queueOnly bool) *collation {
		t.Helper()

		c, err := testBuilder().prepare(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		pendingInShardMessage(t, c, sender, receiver, requestStartLT(t, req)+1)
		if queueOnly {
			c.blockFull = true
		}
		if err = c.processNewMessages(false); err != nil {
			t.Fatal(err)
		}
		if len(c.queuePendingAdd) != 0 {
			t.Fatalf("message phase returned with %d pending queue additions", len(c.queuePendingAdd))
		}
		return c
	}

	immediate := build(t, false)
	queued := build(t, true)
	if immediate.stats.ImmediateDelivered != 1 || immediate.stats.EnqueuedMessages != 0 {
		t.Fatalf("immediate branch: delivered=%d enqueued=%d",
			immediate.stats.ImmediateDelivered, immediate.stats.EnqueuedMessages)
	}
	if queued.stats.ImmediateDelivered != 0 || queued.stats.EnqueuedMessages != 1 {
		t.Fatalf("queued branch: delivered=%d enqueued=%d",
			queued.stats.ImmediateDelivered, queued.stats.EnqueuedMessages)
	}
	if equalCell(immediate.outQueue.RootCell(), queued.outQueue.RootCell()) {
		t.Fatal("queued branch did not change the outbound queue root")
	}
}

var queueAddBatchBenchmarkSink cell.Hash

func BenchmarkQueueAddBatch400(b *testing.B) {
	const depth = 8_192
	const additions = 400

	value := queueBatchValue(b, 401)
	baseEntries := make([]cell.AugmentedBytesEntry, depth)
	for i := range baseEntries {
		key := queueBatchKey(uint64(i))
		baseEntries[i] = cell.AugmentedBytesEntry{Key: key[:], Value: value, Mode: cell.DictSetModeAdd}
	}
	base := newQueueBatchTestQueue(b, nil)
	if err := base.SetManyByBytes(baseEntries, collationParallelism); err != nil {
		b.Fatal(err)
	}
	keys := make([]msgpool.QueueKey, additions)
	for i := range keys {
		keys[i] = queueBatchKey(uint64(depth + i))
	}

	b.Run("sequential", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			queue := &tlb.OutMsgQueueAugDict{AugmentedDictionary: base.AugmentedDictionary.Copy()}
			for i := range keys {
				var builder cell.Builder
				value.ToBuilderInto(&builder)
				inserted, err := queue.SetBuilderByBytesKeyWithMode(keys[i][:], &builder, cell.DictSetModeAdd)
				if err != nil || !inserted {
					b.Fatalf("add %d: inserted=%v err=%v", i, inserted, err)
				}
			}
			queueAddBatchBenchmarkSink = queue.RootCell().HashKey()
		}
	})

	b.Run("batched-prechecked", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			c := &collation{outQueue: &tlb.OutMsgQueueAugDict{AugmentedDictionary: base.AugmentedDictionary.Copy()}}
			for i := range keys {
				if err := c.deferQueueAdd(keys[i], value, queuePendingAddGenerated); err != nil {
					b.Fatalf("defer %d: %v", i, err)
				}
				if (i+1)&63 == 0 {
					if err := c.flushQueueAdds(); err != nil {
						b.Fatal(err)
					}
				}
			}
			if err := c.flushQueueAdds(); err != nil {
				b.Fatal(err)
			}
			queueAddBatchBenchmarkSink = c.outQueue.RootCell().HashKey()
		}
	})
}
