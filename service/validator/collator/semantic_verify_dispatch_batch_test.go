package collator

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

type semanticDispatchBatchAccount struct {
	id                  [32]byte
	old, removed, added []uint64
}

type semanticDispatchBatchFixture struct {
	old, candidate *tlb.DispatchQueueAugDict
	in             []semanticInDescriptorEntry
	out            []semanticOutDescriptorEntry
}

type semanticDispatchBatchCase struct {
	name     string
	accounts []semanticDispatchBatchAccount
	mutate   func(*testing.T, *semanticDispatchBatchFixture)
	reject   bool
}

func TestSemanticDispatchBatchMatchesPerMessage(t *testing.T) {
	a, b := [32]byte{1}, [32]byte{2}
	base := []semanticDispatchBatchAccount{{id: a, old: []uint64{10, 20}, removed: []uint64{10}, added: []uint64{30, 40}}}
	tests := []semanticDispatchBatchCase{
		{name: "unchanged", accounts: []semanticDispatchBatchAccount{{id: a, old: []uint64{10, 20}}}},
		{name: "new account repeated append", accounts: []semanticDispatchBatchAccount{{id: a, added: []uint64{10, 20, 30}}}},
		{name: "remove prefix in descriptor order", accounts: []semanticDispatchBatchAccount{{id: a, old: []uint64{10, 20, 30}, removed: []uint64{20, 10}}}},
		{name: "drain account", accounts: []semanticDispatchBatchAccount{{id: a, old: []uint64{10, 20}, removed: []uint64{10, 20}}}},
		{name: "empty then append", accounts: []semanticDispatchBatchAccount{{id: a, old: []uint64{10, 20}, removed: []uint64{10, 20}, added: []uint64{30, 40}}}},
		{name: "partial drain then append", accounts: base},
		{name: "multiple accounts", accounts: []semanticDispatchBatchAccount{
			{id: a, old: []uint64{10, 20}, removed: []uint64{10, 20}, added: []uint64{30, 40}},
			{id: b, old: []uint64{50, 60}, removed: []uint64{50}, added: []uint64{70}},
		}},
		{name: "remove absent account", accounts: []semanticDispatchBatchAccount{{id: a, removed: []uint64{10}}}, reject: true},
		{name: "remove absent message", accounts: []semanticDispatchBatchAccount{{id: a, old: []uint64{10}, removed: []uint64{20}}}, reject: true},
		{name: "duplicate removal", accounts: []semanticDispatchBatchAccount{{id: a, old: []uint64{10, 20}, removed: []uint64{10, 10}}}, reject: true},
		{name: "duplicate append", accounts: []semanticDispatchBatchAccount{{id: a, added: []uint64{10, 10}}}, reject: true},
		{name: "append existing", accounts: []semanticDispatchBatchAccount{{id: a, old: []uint64{10}, added: []uint64{10}}}, reject: true},
		{name: "remove non-prefix", accounts: []semanticDispatchBatchAccount{{id: a, old: []uint64{10, 20}, removed: []uint64{20}}}, reject: true},
		{name: "re-add removed lt", accounts: []semanticDispatchBatchAccount{{id: a, old: []uint64{10}, removed: []uint64{10}, added: []uint64{10}}}, reject: true},
		{name: "append below old max", accounts: []semanticDispatchBatchAccount{{id: a, old: []uint64{10, 30}, added: []uint64{20}}}, reject: true},
		{name: "count reaches zero with remainder before append", accounts: base, mutate: func(t *testing.T, f *semanticDispatchBatchFixture) {
			setSemanticDispatchBatchCount(t, f.old, a, 1)
		}, reject: true},
		{name: "count remains nonzero when drained before append", accounts: []semanticDispatchBatchAccount{{id: a, old: []uint64{10}, removed: []uint64{10}, added: []uint64{20}}}, mutate: func(t *testing.T, f *semanticDispatchBatchFixture) {
			setSemanticDispatchBatchCount(t, f.old, a, 2)
		}, reject: true},
		{name: "count overflow", accounts: base, mutate: func(t *testing.T, f *semanticDispatchBatchFixture) {
			setSemanticDispatchBatchCount(t, f.old, a, maxOutMsgQueueSize)
		}, reject: true},
		{name: "missing emitted lt", accounts: base, mutate: func(t *testing.T, f *semanticDispatchBatchFixture) {
			f.in[0].descriptor.envelope.value.EmittedLT = nil
		}, reject: true},
		{name: "emitted lt outside block", accounts: base, mutate: func(t *testing.T, f *semanticDispatchBatchFixture) {
			*f.in[0].descriptor.envelope.value.EmittedLT = 20_000
		}, reject: true},
		{name: "emitted lt out of order", accounts: []semanticDispatchBatchAccount{{id: a, old: []uint64{10, 20}, removed: []uint64{10, 20}}}, mutate: func(t *testing.T, f *semanticDispatchBatchFixture) {
			*f.in[1].descriptor.envelope.value.EmittedLT = *f.in[0].descriptor.envelope.value.EmittedLT
		}, reject: true},
		{name: "new envelope has emitted lt", accounts: base, mutate: func(t *testing.T, f *semanticDispatchBatchFixture) {
			lt := uint64(11_000)
			f.out[0].descriptor.envelope.value.EmittedLT = &lt
		}, reject: true},
		{name: "new envelope has routing", accounts: base, mutate: func(t *testing.T, f *semanticDispatchBatchFixture) {
			f.out[0].descriptor.envelope.value.NextAddr.UseDestBits = 1
		}, reject: true},
		{name: "candidate count mismatch", accounts: base, mutate: func(t *testing.T, f *semanticDispatchBatchFixture) {
			setSemanticDispatchBatchCount(t, f.candidate, a, 7)
		}, reject: true},
		{name: "candidate drops unchanged account", accounts: []semanticDispatchBatchAccount{{id: a, old: []uint64{10}}, {id: b, added: []uint64{20}}}, mutate: func(t *testing.T, f *semanticDispatchBatchFixture) {
			if err := f.candidate.DeleteByBytesKey(a[:]); err != nil {
				t.Fatal(err)
			}
		}, reject: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newSemanticDispatchBatchFixture(t, test.accounts)
			if test.mutate != nil {
				test.mutate(t, fixture)
			}
			oldRoot := fixture.old.RootCell()
			old, next := fixture.validation(), fixture.validation()
			oldErr := old.applyDispatchChangesPerMessage()
			if oldErr == nil {
				oldErr = old.verifyQueueRoots()
			}
			nextErr := next.applyDispatchChanges()
			if nextErr == nil {
				nextErr = next.verifyQueueRoots()
			}
			if (oldErr != nil) != test.reject || (nextErr != nil) != test.reject {
				t.Fatalf("per-message = %v, batched = %v, want reject %v", oldErr, nextErr, test.reject)
			}
			if test.reject && (!errors.Is(oldErr, ErrInvalidInput) || !errors.Is(nextErr, ErrInvalidInput)) {
				t.Fatalf("rejection classification differs: per-message = %v, batched = %v", oldErr, nextErr)
			}
			if !test.reject && !equalCell(old.dispatch.RootCell(), next.dispatch.RootCell()) {
				t.Fatal("accepted dispatch roots differ")
			}
			if !equalCell(oldRoot, fixture.old.RootCell()) {
				t.Fatal("validation mutated predecessor")
			}
		})
	}
}

func TestSemanticDispatchBatchReadsPredecessorProof(t *testing.T) {
	accounts := []semanticDispatchBatchAccount{
		{id: [32]byte{1}, old: []uint64{10, 20, 30}, removed: []uint64{10, 20}, added: []uint64{40, 50}},
		{id: [32]byte{2}, old: []uint64{60, 70}, removed: []uint64{60, 70}, added: []uint64{80}},
		{id: [32]byte{3}, old: []uint64{90}},
	}
	fixture := newSemanticDispatchBatchFixture(t, accounts)
	read := func(perMessage bool) *cell.Cell {
		usage := cell.NewReadSet(fixture.old.RootCell())
		validation := fixture.validation()
		validation.old.Extra.DispatchQueue = &tlb.DispatchQueueAugDict{AugmentedDictionary: fixture.old.CopyWithTrace(usage.Trace())}
		validation.dispatch = &tlb.DispatchQueueAugDict{AugmentedDictionary: fixture.old.CopyWithTrace(usage.Trace())}
		if err := validation.precheckDispatchQueueUpdate(); err != nil {
			t.Fatal(err)
		}
		var err error
		if perMessage {
			err = validation.applyDispatchChangesPerMessage()
		} else {
			err = validation.applyDispatchChanges()
		}
		if err != nil {
			t.Fatal(err)
		}
		if err = validation.verifyQueueRoots(); err != nil {
			t.Fatal(err)
		}
		proof, err := usage.Proof()
		if err != nil {
			t.Fatal(err)
		}
		return proof
	}
	// ReadSet also sees temporary dictionaries. Only reads from its predecessor
	// source determine the proof, so compare the resulting proof roots.
	if !equalCell(read(true), read(false)) {
		t.Fatal("batched validation changed predecessor proof reads")
	}
}

func BenchmarkSemanticDispatchBatch(b *testing.B) {
	for _, count := range []int{1, 8, 64} {
		accounts := make([]semanticDispatchBatchAccount, count)
		for i := range accounts {
			binary.BigEndian.PutUint64(accounts[i].id[24:], uint64(i+1))
			for j := 0; j < 512/count; j++ {
				lt := uint64(j + 1)
				accounts[i].old = append(accounts[i].old, lt)
				accounts[i].removed = append(accounts[i].removed, lt)
				accounts[i].added = append(accounts[i].added, lt+1_000)
			}
		}
		fixture := newSemanticDispatchBatchFixture(b, accounts)
		for _, perMessage := range []bool{true, false} {
			variant := "batched"
			if perMessage {
				variant = "per_message"
			}
			b.Run(fmt.Sprintf("accounts=%d/%s", count, variant), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					validation := fixture.validation()
					var err error
					if perMessage {
						err = validation.applyDispatchChangesPerMessage()
					} else {
						err = validation.applyDispatchChanges()
					}
					if err != nil {
						b.Fatal(err)
					}
					if err = validation.verifyQueueRoots(); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(1024, "messages/op")
			})
		}
	}
}

func newSemanticDispatchBatchFixture(t testing.TB, accounts []semanticDispatchBatchAccount) *semanticDispatchBatchFixture {
	t.Helper()
	old, err := tlb.NewDispatchQueueAugDict()
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := tlb.NewDispatchQueueAugDict()
	if err != nil {
		t.Fatal(err)
	}
	fixture := &semanticDispatchBatchFixture{old: old, candidate: candidate}
	for _, account := range accounts {
		remaining := make(map[uint64]*semanticEnvelope)
		for _, lt := range account.old {
			remaining[lt] = semanticDispatchBatchEnvelope(t, account.id, lt, false)
		}
		storeSemanticDispatchBatchAccount(t, old, account.id, remaining)
		for _, lt := range account.removed {
			envelope := semanticDispatchBatchEnvelope(t, account.id, lt, true)
			fixture.in = append(fixture.in, semanticInDescriptorEntry{hash: envelope.message.HashKey(), descriptor: &semanticInDescriptor{tag: semanticInDeferredFinal, message: envelope.message, envelope: envelope}})
			delete(remaining, lt)
		}
		for _, lt := range account.added {
			envelope := semanticDispatchBatchEnvelope(t, account.id, lt, false)
			fixture.out = append(fixture.out, semanticOutDescriptorEntry{hash: envelope.message.HashKey(), descriptor: &semanticOutDescriptor{tag: semanticOutNewDeferred, message: envelope.message, envelope: envelope}})
			remaining[lt] = envelope
		}
		storeSemanticDispatchBatchAccount(t, candidate, account.id, remaining)
	}
	// Production walks descriptors in hash order, not in per-account LT order.
	slices.SortFunc(fixture.in, func(a, b semanticInDescriptorEntry) int {
		return slices.Compare(a.hash[:], b.hash[:])
	})
	slices.SortFunc(fixture.out, func(a, b semanticOutDescriptorEntry) int {
		return slices.Compare(a.hash[:], b.hash[:])
	})
	return fixture
}

func (f *semanticDispatchBatchFixture) validation() *semanticQueueValidation {
	candidate := &verifiedCandidate{}
	candidate.block.BlockInfo.StartLt, candidate.block.BlockInfo.EndLt = 10_000, 20_000
	return &semanticQueueValidation{
		replay:    &semanticReplay{ctx: context.Background(), candidate: candidate, transition: CandidateTransition{Config: &Config{capabilities: capDeferMessages}}},
		target:    msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll},
		old:       tlb.OutMsgQueueInfo{Extra: &tlb.OutMsgQueueExtra{DispatchQueue: f.old}},
		candidate: &tlb.OutMsgQueueInfo{Extra: &tlb.OutMsgQueueExtra{DispatchQueue: f.candidate}},
		dispatch:  &tlb.DispatchQueueAugDict{AugmentedDictionary: f.old.Copy()},
		inOrder:   f.in, outOrder: f.out,
	}
}

func semanticDispatchBatchEnvelope(t testing.TB, accountID [32]byte, lt uint64, emitted bool) *semanticEnvelope {
	t.Helper()
	destination := accountID
	destination[31] ^= 0xff
	message, err := tlb.ToCell(&tlb.InternalMessage{
		IHRDisabled: true, SrcAddr: address.NewAddress(0, 0, accountID[:]), DstAddr: address.NewAddress(0, 0, destination[:]),
		Amount: tlb.FromNanoTONU(1_000_000), FwdFee: tlb.FromNanoTONU(1_000), CreatedLT: lt, CreatedAt: 1_900_000_000,
		Body: cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	envelope := tlb.MsgEnvelope{FwdFeeRemaining: tlb.FromNanoTONU(1_000), Msg: message}
	if emitted {
		emittedLT := uint64(10_000) + lt
		envelope.EmittedLT = &emittedLT
	}
	root, err := envelope.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parseSemanticEnvelope(root)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func storeSemanticDispatchBatchAccount(t testing.TB, queue *tlb.DispatchQueueAugDict, id [32]byte, messages map[uint64]*semanticEnvelope) {
	t.Helper()
	if len(messages) == 0 {
		return
	}
	account := &tlb.AccountDispatchQueue{Messages: cell.NewDict(64), Count: uint64(len(messages))}
	for lt, envelope := range messages {
		value, err := (tlb.EnqueuedMsg{EnqueuedLT: lt, Msg: envelope.root}).ToCell()
		if err != nil {
			t.Fatal(err)
		}
		if err = account.Messages.Set(dispatchLTKey(lt), value); err != nil {
			t.Fatal(err)
		}
	}
	if err := storeAccountDispatchQueue(queue.AugmentedDictionary, id, account); err != nil {
		t.Fatal(err)
	}
}

func setSemanticDispatchBatchCount(t testing.TB, queue *tlb.DispatchQueueAugDict, id [32]byte, count uint64) {
	t.Helper()
	account, err := loadAccountDispatchQueue(queue, id)
	if err != nil {
		t.Fatal(err)
	}
	account.Count = count
	if err = storeAccountDispatchQueue(queue.AugmentedDictionary, id, account); err != nil {
		t.Fatal(err)
	}
}
