package collator

import (
	"testing"

	"github.com/xssnick/gton/service/validator/msgpool"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type recordedAccountPrewarmer struct {
	accounts        []prewarmAccountKey
	roots           []cell.Hash
	immediate       int
	rejectImmediate bool
}

func (w *recordedAccountPrewarmer) EnqueueRoot(root cell.Hash) bool {
	w.roots = append(w.roots, root)
	return true
}

func (w *recordedAccountPrewarmer) EnqueueAccount(workchain int32, account [32]byte) bool {
	w.accounts = append(w.accounts, prewarmAccountKey{workchain: workchain, account: account})
	return true
}

func (w *recordedAccountPrewarmer) PrewarmAccountNow(workchain int32, account [32]byte) bool {
	w.immediate++
	if w.rejectImmediate {
		return false
	}
	return w.EnqueueAccount(workchain, account)
}

func TestLocalAcquisitionPrewarmsPooledCandidateInternals(t *testing.T) {
	warmer := &recordedAccountPrewarmer{}
	acquisition := &LocalAcquisition{accountPrewarmer: warmer}
	first := [32]byte{0x11}
	second := [32]byte{0x22}
	firstEnvelope := cell.Hash{0xa1}
	secondEnvelope := cell.Hash{0xa2}
	variableEnvelope := cell.Hash{0xa3}

	var hints prewarmHints
	acquisition.collectPooledInternals(&hints, []*msgpool.InternalMessage{
		{DestinationWorkchain: 0, DestinationAccount: first, DestinationPrewarmable: true, EnvHash: firstEnvelope},
		{DestinationWorkchain: 0, DestinationAccount: first, DestinationPrewarmable: true, EnvHash: firstEnvelope},
		{DestinationWorkchain: -1, DestinationAccount: second, DestinationPrewarmable: true, EnvHash: secondEnvelope},
		{DestinationWorkchain: 0, DestinationAccount: [32]byte{0x33}, EnvHash: variableEnvelope},
	})
	acquisition.issuePrewarmHints(&hints)

	want := []prewarmAccountKey{
		{workchain: 0, account: first},
		{workchain: -1, account: second},
	}
	if len(warmer.accounts) != len(want) {
		t.Fatalf("prewarmed accounts = %+v, want %+v", warmer.accounts, want)
	}
	for index := range want {
		if warmer.accounts[index] != want[index] {
			t.Fatalf("prewarmed account %d = %+v, want %+v", index, warmer.accounts[index], want[index])
		}
	}
	if warmer.immediate != 0 {
		t.Fatalf("candidate pool admission used %d immediate warms, want background queue", warmer.immediate)
	}
	wantRoots := []cell.Hash{firstEnvelope, secondEnvelope, variableEnvelope}
	if len(warmer.roots) != len(wantRoots) {
		t.Fatalf("prewarmed envelope roots = %x, want %x", warmer.roots, wantRoots)
	}
	for index := range wantRoots {
		if warmer.roots[index] != wantRoots[index] {
			t.Fatalf("prewarmed envelope root %d = %x, want %x", index, warmer.roots[index], wantRoots[index])
		}
	}
}

func TestLocalAcquisitionPrewarmsAdaptiveCanonicalLookahead(t *testing.T) {
	const capacity = 4
	warmer := &recordedAccountPrewarmer{}
	acquisition := &LocalAcquisition{accountPrewarmer: warmer, accountPrewarmCapacity: capacity}
	cut := &msgpool.Cut{}

	first := [32]byte{0x11}
	for range 96 {
		cut.Messages = append(cut.Messages, &msgpool.InternalMessage{
			DestinationWorkchain:   0,
			DestinationAccount:     first,
			DestinationPrewarmable: true,
		})
	}
	for tag := byte(0x22); tag <= 0x55; tag += 0x11 {
		cut.Messages = append(cut.Messages, &msgpool.InternalMessage{
			DestinationWorkchain:   int32(tag),
			DestinationAccount:     [32]byte{tag},
			DestinationPrewarmable: true,
		})
	}

	func() {
		var hints prewarmHints
		acquisition.collectCurrentInternals(&hints, cut)
		acquisition.issuePrewarmHints(&hints)
	}()

	if len(warmer.accounts) != capacity {
		t.Fatalf("prewarmed accounts = %d, want capacity %d", len(warmer.accounts), capacity)
	}
	if warmer.accounts[0] != (prewarmAccountKey{workchain: 0, account: first}) {
		t.Fatalf("first prewarm = %+v, want the canonical first cut account", warmer.accounts[0])
	}
	if warmer.accounts[1] != (prewarmAccountKey{workchain: 0x22, account: [32]byte{0x22}}) {
		t.Fatalf("second prewarm = %+v, want destination after the old 64-message horizon", warmer.accounts[1])
	}
}

func TestLocalAcquisitionPrewarmsFiveHundredDistinctDestinations(t *testing.T) {
	warmer := &recordedAccountPrewarmer{}
	acquisition := &LocalAcquisition{accountPrewarmer: warmer, accountPrewarmCapacity: 544}
	cut := &msgpool.Cut{Messages: make([]*msgpool.InternalMessage, 500)}
	for i := range cut.Messages {
		account := [32]byte{byte(i >> 8), byte(i)}
		cut.Messages[i] = &msgpool.InternalMessage{
			DestinationAccount:     account,
			DestinationPrewarmable: true,
		}
	}

	func() {
		var hints prewarmHints
		acquisition.collectCurrentInternals(&hints, cut)
		acquisition.issuePrewarmHints(&hints)
	}()

	if len(warmer.accounts) != len(cut.Messages) {
		t.Fatalf("prewarmed accounts = %d, want all %d block destinations", len(warmer.accounts), len(cut.Messages))
	}
}

func BenchmarkLocalAcquisitionPrewarmCurrentInternals(b *testing.B) {
	warmer := &recordedAccountPrewarmer{}
	acquisition := &LocalAcquisition{accountPrewarmer: warmer, accountPrewarmCapacity: 544}
	cut := &msgpool.Cut{Messages: make([]*msgpool.InternalMessage, 500)}
	for i := range cut.Messages {
		cut.Messages[i] = &msgpool.InternalMessage{
			DestinationAccount:     [32]byte{byte(i >> 8), byte(i)},
			DestinationPrewarmable: true,
		}
	}

	b.ReportAllocs()
	for b.Loop() {
		warmer.accounts = warmer.accounts[:0]
		func() {
			var hints prewarmHints
			acquisition.collectCurrentInternals(&hints, cut)
			acquisition.issuePrewarmHints(&hints)
		}()
	}
}

func TestCollationPrewarmsOnlyUnopenedImmediateAccount(t *testing.T) {
	warmer := &recordedAccountPrewarmer{}
	destinationID := [32]byte{0x44}
	destination := address.NewAddress(0, 0, destinationID[:])
	c := &collation{
		req:   collationRequest{accountPrewarmer: warmer},
		lanes: make(map[[32]byte]*accountLane),
	}

	c.prewarmImmediateAccount(destination)
	if len(warmer.accounts) != 1 || warmer.accounts[0] != (prewarmAccountKey{account: destinationID}) {
		t.Fatalf("immediate prewarms = %+v, want destination account", warmer.accounts)
	}
	if warmer.immediate != 1 {
		t.Fatalf("direct immediate calls = %d, want 1", warmer.immediate)
	}
	c.prewarmImmediateAccount(destination)
	if warmer.immediate != 1 {
		t.Fatalf("duplicate destination direct calls = %d, want 1", warmer.immediate)
	}

	c.lanes[destinationID] = &accountLane{}
	c.prewarmImmediateAccount(destination)
	if len(warmer.accounts) != 1 {
		t.Fatalf("opened destination was prewarmed again: %+v", warmer.accounts)
	}
	if warmer.immediate != 1 {
		t.Fatalf("opened destination direct calls = %d, want 1", warmer.immediate)
	}
}

func TestCollationPrewarmsGeneratedLocalOutputsEarly(t *testing.T) {
	warmer := &recordedAccountPrewarmer{}
	localID := [32]byte{0x44}
	local := address.NewAddress(0, 0, localID[:])
	foreignID := [32]byte{0x55}
	foreign := address.NewAddress(0, 1, foreignID[:])
	c := &collation{
		req: collationRequest{
			accountPrewarmer: warmer,
			internals:        &msgpool.Cut{},
		},
		shard: msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll},
		lanes: make(map[[32]byte]*accountLane),
	}
	result := &tvm.TransactionExecutionResult{OutMessages: []tvm.OutMessage{
		{Msg: &tlb.Message{MsgType: tlb.MsgTypeInternal, Msg: &tlb.InternalMessage{DstAddr: local}}},
		{Msg: &tlb.Message{MsgType: tlb.MsgTypeInternal, Msg: &tlb.InternalMessage{DstAddr: local}}},
		{Msg: &tlb.Message{MsgType: tlb.MsgTypeInternal, Msg: &tlb.InternalMessage{DstAddr: foreign}}},
	}}

	c.prewarmGeneratedOutputs(result)
	if len(warmer.accounts) != 1 || warmer.accounts[0] != (prewarmAccountKey{account: localID}) {
		t.Fatalf("early generated prewarms = %+v, want only local destination", warmer.accounts)
	}
	if warmer.immediate != 1 {
		t.Fatalf("early direct calls = %d, want one deduplicated call", warmer.immediate)
	}

	// The exact-immediate path remains the safety call, but the early hint has
	// already registered this destination for the collation.
	c.prewarmImmediateAccount(local)
	if warmer.immediate != 1 {
		t.Fatalf("safety direct calls = %d, want early hint to deduplicate it", warmer.immediate)
	}
}

func TestCollationPrewarmsExternalBatchBeforeExecution(t *testing.T) {
	warmer := &recordedAccountPrewarmer{rejectImmediate: true}
	localID := [32]byte{0x66}
	local := address.NewAddress(0, 0, localID[:])
	foreignID := [32]byte{0x77}
	foreign := address.NewAddress(0, 1, foreignID[:])
	c := &collation{
		req:   collationRequest{accountPrewarmer: warmer},
		shard: msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll},
		lanes: make(map[[32]byte]*accountLane),
	}
	inputs := []ExternalInput{
		prewarmExternalInput(t, local),
		prewarmExternalInput(t, local),
		prewarmExternalInput(t, foreign),
	}

	c.prewarmExternalInputs(inputs)

	if warmer.immediate != 1 {
		t.Fatalf("immediate attempts = %d, want one deduplicated local destination", warmer.immediate)
	}
	if len(warmer.accounts) != 1 || warmer.accounts[0] != (prewarmAccountKey{account: localID}) {
		t.Fatalf("queued fallback = %+v, want current local external destination", warmer.accounts)
	}
}

func prewarmExternalInput(tb testing.TB, destination *address.Address) ExternalInput {
	tb.Helper()

	root, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr: destination,
		Body:    cell.BeginCell().EndCell(),
	})
	if err != nil {
		tb.Fatal(err)
	}
	return externalInput(tb, root)
}
