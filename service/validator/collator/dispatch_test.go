package collator

import (
	"context"
	"errors"
	"math"
	"slices"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

type dispatchFixtureAccount struct {
	accountID [32]byte
	lts       []uint64
	metadata  *tlb.MsgMetadata
	bodyInRef bool
}

type dispatchEmissionKey struct {
	accountID [32]byte
	createdLT uint64
}

func TestProcessDispatchQueuePhasesAndEmittedLT(t *testing.T) {
	a := repeatedDispatchAccount(0x10)
	b := repeatedDispatchAccount(0x20)
	queue := makeDispatchQueue(t,
		dispatchFixtureAccount{accountID: a, lts: []uint64{10, 20, 30}},
		dispatchFixtureAccount{accountID: b, lts: []uint64{5, 15, 25}},
	)
	c := dispatchTestCollation(t, queue, DispatchPolicy{
		DeferMessagesAfter:         100,
		Phase2MaxTotal:             2,
		Phase2MaxPerInitiator:      100,
		Phase3MaxTotal:             0,
		Phase3MaxPerInitiator:      0,
		Phase3AdaptivePerInitiator: false,
	})

	if err := c.processDispatchQueue(); err != nil {
		t.Fatal(err)
	}

	assertDispatchLTs(t, c.dispatchQueue, a, []uint64{30})
	assertDispatchLTs(t, c.dispatchQueue, b, []uint64{25})
	if c.stats.DispatchedMessages != 4 || c.new.Len() != 4 {
		t.Fatalf("dispatched/new messages = %d/%d, want 4/4", c.stats.DispatchedMessages, c.new.Len())
	}
	if c.haveUnprocessedDispatchQueue {
		t.Fatal("phase 0 completed but the mandatory-pass flag remains set")
	}
	if c.peakLoad < LoadSoft {
		t.Fatal("phase-2 total limit did not mark dispatch overload")
	}
	if c.dispatchOps != 6 {
		t.Fatalf("dispatch operations = %d, want four removals plus two forced checkpoints", c.dispatchOps)
	}

	wantEmitted := map[dispatchEmissionKey]uint64{
		{accountID: a, createdLT: 10}: c.header.StartLt + 1,
		{accountID: a, createdLT: 20}: c.header.StartLt + 2,
		{accountID: b, createdLT: 5}:  c.header.StartLt + 1,
		{accountID: b, createdLT: 15}: c.header.StartLt + 2,
	}
	for i := range c.new {
		item := &c.new[i]
		internal := item.parsed.AsInternal()
		sourceID, err := accountIDFromAddress(internal.SrcAddr)
		if err != nil {
			t.Fatal(err)
		}
		var envelope tlb.MsgEnvelope
		if err = parseExact(&envelope, item.dispatchEnvelope); err != nil {
			t.Fatal(err)
		}
		key := dispatchEmissionKey{accountID: sourceID, createdLT: internal.CreatedLT}
		want, ok := wantEmitted[key]
		if !ok {
			t.Fatalf("unexpected dispatched message %x:%d", sourceID, internal.CreatedLT)
		}
		if envelope.EmittedLT == nil || *envelope.EmittedLT != want || item.lt != want {
			t.Fatalf("emitted lt for %x:%d = %v/item %d, want %d", sourceID, internal.CreatedLT, envelope.EmittedLT, item.lt, want)
		}
		delete(wantEmitted, key)
	}
	if len(wantEmitted) != 0 {
		t.Fatalf("missing emitted messages: %v", wantEmitted)
	}
}

func TestProcessDispatchQueueAttemptSkipsOptionalPhases(t *testing.T) {
	a := repeatedDispatchAccount(0x31)
	b := repeatedDispatchAccount(0x32)
	c := dispatchTestCollation(t, makeDispatchQueue(t,
		dispatchFixtureAccount{accountID: a, lts: []uint64{10, 20}},
		dispatchFixtureAccount{accountID: b, lts: []uint64{11, 21}},
	), DispatchPolicy{
		AttemptIndex:               1,
		DeferMessagesAfter:         100,
		Phase2MaxTotal:             100,
		Phase2MaxPerInitiator:      100,
		Phase3MaxTotal:             100,
		Phase3MaxPerInitiator:      100,
		Phase3AdaptivePerInitiator: false,
	})

	if err := c.processDispatchQueue(); err != nil {
		t.Fatal(err)
	}
	assertDispatchLTs(t, c.dispatchQueue, a, []uint64{20})
	assertDispatchLTs(t, c.dispatchQueue, b, []uint64{21})
	if c.stats.DispatchedMessages != 2 || c.dispatchOps != 3 {
		t.Fatalf("dispatched/ops = %d/%d, want 2/3", c.stats.DispatchedMessages, c.dispatchOps)
	}
}

func TestProcessDispatchQueuePriorityRoundRobinPrecedesMinimumLT(t *testing.T) {
	a := repeatedDispatchAccount(0x41)
	b := repeatedDispatchAccount(0x42)
	c := dispatchTestCollation(t, makeDispatchQueue(t,
		dispatchFixtureAccount{accountID: a, lts: []uint64{100, 200, 300}},
		dispatchFixtureAccount{accountID: b, lts: []uint64{1, 2, 3}},
	), DispatchPolicy{
		DeferMessagesAfter:    100,
		Phase2MaxTotal:        1,
		Phase2MaxPerInitiator: 100,
		PriorityList: []DispatchAccount{
			{Workchain: 0, AccountID: b},
			{Workchain: 0, AccountID: a},
			{Workchain: 0, AccountID: a},
		},
	})

	if err := c.processDispatchQueue(); err != nil {
		t.Fatal(err)
	}
	// std::set ordering canonicalizes the priority list to A,B. Phase 0 takes
	// A:100 and B:1, then the one-message phase 2 starts from A again even
	// though B:2 has the globally smaller logical time.
	assertDispatchLTs(t, c.dispatchQueue, a, []uint64{300})
	assertDispatchLTs(t, c.dispatchQueue, b, []uint64{2, 3})
}

func TestMinimumDispatchAccountFollowsAugmentedMinimum(t *testing.T) {
	left := repeatedDispatchAccount(0x10)
	equalMinimum := repeatedDispatchAccount(0x20)
	right := repeatedDispatchAccount(0xf0)
	queue := makeDispatchQueue(t,
		dispatchFixtureAccount{accountID: left, lts: []uint64{5}},
		dispatchFixtureAccount{accountID: equalMinimum, lts: []uint64{5}},
		dispatchFixtureAccount{accountID: right, lts: []uint64{1}},
	)

	selected, err := minimumDispatchAccount(queue)
	if err != nil {
		t.Fatal(err)
	}
	if selected.AccountID != right {
		t.Fatalf("minimum dispatch account = %x, want %x", selected.AccountID, right)
	}

	if err = queue.Delete(dispatchAccountKey(right)); err != nil {
		t.Fatal(err)
	}
	selected, err = minimumDispatchAccount(queue)
	if err != nil {
		t.Fatal(err)
	}
	if selected.AccountID != left {
		t.Fatalf("equal-lt dispatch account = %x, want lexicographically first %x", selected.AccountID, left)
	}

	compressedLeft := repeatedDispatchAccount(0xaa)
	compressedLeft[31] = 0xfe
	compressedRight := compressedLeft
	compressedRight[31] = 0xff
	compressed := makeDispatchQueue(t,
		dispatchFixtureAccount{accountID: compressedRight, lts: []uint64{7}},
		dispatchFixtureAccount{accountID: compressedLeft, lts: []uint64{7}},
	)
	selected, err = minimumDispatchAccount(compressed)
	if err != nil {
		t.Fatal(err)
	}
	if selected.AccountID != compressedLeft {
		t.Fatalf("compressed-prefix dispatch account = %x, want %x", selected.AccountID, compressedLeft)
	}

	empty, err := tlb.NewDispatchQueueAugDict()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = minimumDispatchAccount(empty); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("empty dispatch queue error = %v, want ErrInvalidInput", err)
	}
}

func TestProcessDispatchQueueCachesCanonicalPolicy(t *testing.T) {
	a := repeatedDispatchAccount(0x21)
	b := repeatedDispatchAccount(0x22)
	policy := DispatchPolicy{
		PriorityList: []DispatchAccount{
			{Workchain: 0, AccountID: b},
			{Workchain: 0, AccountID: a},
			{Workchain: 0, AccountID: b},
		},
		Whitelist: []DispatchAccount{
			{Workchain: 0, AccountID: b},
			{Workchain: 0, AccountID: a},
			{Workchain: 0, AccountID: b},
		},
	}
	originalPriority := slices.Clone(policy.PriorityList)
	originalWhitelist := slices.Clone(policy.Whitelist)
	c := dispatchTestCollation(t, makeDispatchQueue(t,
		dispatchFixtureAccount{accountID: a, lts: []uint64{1}},
		dispatchFixtureAccount{accountID: b, lts: []uint64{2}},
	), policy)

	if err := c.processDispatchQueue(); err != nil {
		t.Fatal(err)
	}
	want := []DispatchAccount{{Workchain: 0, AccountID: a}, {Workchain: 0, AccountID: b}}
	if !slices.Equal(c.req.dispatch.PriorityList, want) || !slices.Equal(c.req.dispatch.Whitelist, want) {
		t.Fatalf("canonical dispatch policy = priority %v, whitelist %v, want %v",
			c.req.dispatch.PriorityList, c.req.dispatch.Whitelist, want)
	}
	if !slices.Equal(policy.PriorityList, originalPriority) || !slices.Equal(policy.Whitelist, originalWhitelist) {
		t.Fatal("dispatch policy canonicalization mutated caller-owned slices")
	}
	if !dispatchAccountListed(c.req.dispatch.Whitelist, want[0]) ||
		dispatchAccountListed(c.req.dispatch.Whitelist, DispatchAccount{Workchain: -1, AccountID: a}) {
		t.Fatal("canonical whitelist lookup returned an incorrect result")
	}
}

func TestProcessDispatchQueuePerInitiatorLimitUsesMetadata(t *testing.T) {
	accountID := repeatedDispatchAccount(0x47)
	initiatorID := repeatedDispatchAccount(0x48)
	metadata := &tlb.MsgMetadata{
		Initiator:   address.NewAddress(0, 0, initiatorID[:]),
		InitiatorLT: 77,
	}

	tests := []struct {
		name     string
		metadata *tlb.MsgMetadata
		want     []uint64
	}{
		{name: "metadata caps source", metadata: metadata, want: []uint64{30, 40}},
		{name: "absent metadata is uncapped", want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := dispatchTestCollation(t, makeDispatchQueue(t,
				dispatchFixtureAccount{accountID: accountID, lts: []uint64{10, 20, 30, 40}, metadata: test.metadata},
			), DispatchPolicy{
				DeferMessagesAfter:    100,
				Phase2MaxTotal:        10,
				Phase2MaxPerInitiator: 1,
			})

			if err := c.processDispatchQueue(); err != nil {
				t.Fatal(err)
			}
			assertDispatchLTs(t, c.dispatchQueue, accountID, test.want)
		})
	}
}

func TestProcessDispatchQueueSenderAndInitiatorCapsCanRemoveSameAccount(t *testing.T) {
	accountID := repeatedDispatchAccount(0x48)
	initiatorID := repeatedDispatchAccount(0x49)
	metadata := &tlb.MsgMetadata{
		Initiator:   address.NewAddress(0, 0, initiatorID[:]),
		InitiatorLT: 77,
	}
	c := dispatchTestCollation(t, makeDispatchQueue(t,
		dispatchFixtureAccount{accountID: accountID, lts: []uint64{10, 20, 30}, metadata: metadata},
	), DispatchPolicy{
		DeferMessagesAfter:    2,
		Phase2MaxTotal:        10,
		Phase2MaxPerInitiator: 1,
	})

	if err := c.processDispatchQueue(); err != nil {
		t.Fatal(err)
	}
	assertDispatchLTs(t, c.dispatchQueue, accountID, []uint64{30})
}

func TestProcessDispatchQueuePhase2SenderThresholdHonorsWhitelist(t *testing.T) {
	limited := repeatedDispatchAccount(0x49)
	whitelisted := repeatedDispatchAccount(0x4a)
	c := dispatchTestCollation(t, makeDispatchQueue(t,
		dispatchFixtureAccount{accountID: limited, lts: []uint64{10, 20, 30}},
		dispatchFixtureAccount{accountID: whitelisted, lts: []uint64{11, 21, 31}},
	), DispatchPolicy{
		DeferMessagesAfter:    2,
		Phase2MaxTotal:        10,
		Phase2MaxPerInitiator: 10,
		Whitelist: []DispatchAccount{
			{Workchain: 0, AccountID: whitelisted},
		},
	})

	if err := c.processDispatchQueue(); err != nil {
		t.Fatal(err)
	}
	assertDispatchLTs(t, c.dispatchQueue, limited, []uint64{30})
	assertDispatchLTs(t, c.dispatchQueue, whitelisted, nil)
}

func TestProcessDispatchQueueHardQueueGate(t *testing.T) {
	a := repeatedDispatchAccount(0x51)
	c := dispatchTestCollation(t, makeDispatchQueue(t,
		dispatchFixtureAccount{accountID: a, lts: []uint64{10}},
	), DispatchPolicy{})
	c.queueSize = 257
	c.oldQueueSize = 257

	if err := c.processDispatchQueue(); err != nil {
		t.Fatal(err)
	}
	assertDispatchLTs(t, c.dispatchQueue, a, []uint64{10})
	if c.stats.DispatchedMessages != 0 || c.haveUnprocessedDispatchQueue {
		t.Fatal("hard queue gate changed dispatch state")
	}
}

func TestProcessDispatchQueueLimitLeavesMandatoryPassIncomplete(t *testing.T) {
	accountID := repeatedDispatchAccount(0x52)
	c := dispatchTestCollation(t, makeDispatchQueue(t,
		dispatchFixtureAccount{accountID: accountID, lts: []uint64{10}},
	), DispatchPolicy{})
	c.limits.limits.bytes[LoadNormal] = 0

	if err := c.processDispatchQueue(); err != nil {
		t.Fatal(err)
	}
	assertDispatchLTs(t, c.dispatchQueue, accountID, []uint64{10})
	if !c.blockFull || !c.haveUnprocessedDispatchQueue || c.dispatchOps != 1 {
		t.Fatalf("blockFull/incomplete/ops = %t/%t/%d, want true/true/1", c.blockFull, c.haveUnprocessedDispatchQueue, c.dispatchOps)
	}
}

func TestPhase3AdaptiveDispatchLimit(t *testing.T) {
	c := collation{req: collationRequest{dispatch: DispatchPolicy{Phase3AdaptivePerInitiator: true}}}
	tests := []struct {
		queueSize uint64
		want      uint64
	}{
		{queueSize: 0, want: 10},
		{queueSize: 256, want: 10},
		{queueSize: 257, want: 2},
		{queueSize: 512, want: 2},
		{queueSize: 513, want: 1},
		{queueSize: 1500, want: 1},
		{queueSize: 1501, want: 0},
	}
	for _, test := range tests {
		c.queueSize = test.queueSize
		if got := c.phase3DispatchPerInitiatorLimit(); got != test.want {
			t.Fatalf("queue size %d: adaptive limit = %d, want %d", test.queueSize, got, test.want)
		}
	}
}

func TestValidateDispatchPolicy(t *testing.T) {
	tests := []struct {
		name   string
		policy DispatchPolicy
	}{
		{name: "zero enabled threshold", policy: DispatchPolicy{DeferringEnabled: true}},
		{name: "adaptive and explicit phase 3", policy: DispatchPolicy{
			Phase3AdaptivePerInitiator: true,
			Phase3MaxPerInitiator:      1,
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := collationRequest{randSeed: [32]byte{1}, dispatch: test.policy}
			if err := validateCollationRequest(&req); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("validate dispatch policy error = %v, want ErrInvalidInput", err)
			}
		})
	}
}

func TestDeferredDescriptorTags(t *testing.T) {
	first := cell.BeginCell().MustStoreUInt(1, 1).EndCell()
	second := cell.BeginCell().MustStoreUInt(2, 2).EndCell()
	fee := tlb.FromNanoTONU(123)

	final, err := descriptorFee(0b00100, 5, first, second, fee)
	if err != nil {
		t.Fatal(err)
	}
	loader := final.MustBeginParse()
	if tag := loader.MustLoadUInt(5); tag != 0b00100 {
		t.Fatalf("deferred final tag = %05b", tag)
	}
	if loader.MustLoadRef().MustToCell().HashKey() != first.HashKey() || loader.MustLoadRef().MustToCell().HashKey() != second.HashKey() {
		t.Fatal("deferred final references differ")
	}
	if got := loader.MustLoadBigCoins(); got.Cmp(fee.Nano()) != 0 || loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		t.Fatalf("deferred final fee/tail = %s, %d bits, %d refs", got, loader.BitsLeft(), loader.RefsNum())
	}

	transit, err := descriptor(0b00101, 5, first, second)
	if err != nil {
		t.Fatal(err)
	}
	assertTwoRefDispatchDescriptor(t, transit, 0b00101, first, second)
	deferredNew, err := descriptor(0b10100, 5, first, second)
	if err != nil {
		t.Fatal(err)
	}
	assertTwoRefDispatchDescriptor(t, deferredNew, 0b10100, first, second)
	deferredTransit, err := descriptor(0b10101, 5, first, second)
	if err != nil {
		t.Fatal(err)
	}
	assertTwoRefDispatchDescriptor(t, deferredTransit, 0b10101, first, second)
}

func dispatchTestCollation(t *testing.T, queue *tlb.DispatchQueueAugDict, policy DispatchPolicy) *collation {
	t.Helper()
	header := tlb.BlockHeader{}
	header.StartLt = 1_000
	loose := limitThresholds{math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64}
	usage := cell.NewReadSet(nil)
	limits := newBlockLimitStatus(blockLimits{
		bytes:        loose,
		gas:          loose,
		ltDelta:      loose,
		collatedData: loose,
	}, 1_000, usage)
	oldQueue := &tlb.DispatchQueueAugDict{AugmentedDictionary: queue.Copy()}
	c := &collation{
		ctx:              context.Background(),
		req:              collationRequest{dispatch: policy},
		header:           header,
		shard:            msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll},
		config:           &Config{capabilities: capDeferMessages, deferOutQueueSizeLimit: 256},
		usage:            usage,
		limits:           limits,
		oldDispatchQueue: oldQueue,
		dispatchSources: [2]predecessorDispatchSource{{
			shard: tlb.ShardIdent{WorkchainID: 0},
			queue: oldQueue,
		}},
		dispatchSourceCount: 1,
		dispatchQueue:       queue,
		senderGenerated:     make(map[[32]byte]uint32),
		lastDispatchEmitted: make(map[[32]byte]uint64),
		unprocessedDeferred: make(map[[32]byte]uint32),
		lanes:               make(map[[32]byte]*accountLane),
		maxLT:               1_001,
	}
	if err := c.limits.addProof(queue.RootCell()); err != nil {
		t.Fatal(err)
	}
	return c
}

func makeDispatchQueue(t *testing.T, accounts ...dispatchFixtureAccount) *tlb.DispatchQueueAugDict {
	t.Helper()
	queue, err := tlb.NewDispatchQueueAugDict()
	if err != nil {
		t.Fatal(err)
	}
	for _, fixture := range accounts {
		messages := cell.NewDict(64)
		for _, lt := range fixture.lts {
			source := address.NewAddress(0, 0, fixture.accountID[:])
			destinationID := fixture.accountID
			destinationID[31] ^= 0xff
			destination := address.NewAddress(0, 0, destinationID[:])
			internal := &tlb.InternalMessage{
				IHRDisabled: true,
				SrcAddr:     source,
				DstAddr:     destination,
				Amount:      tlb.FromNanoTONU(1_000_000),
				FwdFee:      tlb.FromNanoTONU(1_000),
				CreatedLT:   lt,
				CreatedAt:   1_900_000_000,
				Body:        cell.BeginCell().EndCell(),
			}
			var message *cell.Cell
			var messageErr error
			if fixture.bodyInRef {
				internal.Body = cell.BeginCell().
					MustStoreUInt(lt, 64).
					MustStoreSlice(make([]byte, 43), 343).
					EndCell()
				builder := cell.BeginCell()
				messageErr = tlb.StoreMessageWithLayout(builder, &tlb.Message{
					MsgType: tlb.MsgTypeInternal,
					Msg:     internal,
				}, tlb.MessageLayout{BodyInRef: true})
				message = builder.EndCell()
			} else {
				message, messageErr = tlb.ToCell(internal)
			}
			if messageErr != nil {
				t.Fatal(messageErr)
			}
			envelope, envelopeErr := (tlb.MsgEnvelope{
				CurAddr:         tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular},
				NextAddr:        tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular},
				FwdFeeRemaining: tlb.FromNanoTONU(1_000),
				Msg:             message,
				Metadata:        fixture.metadata,
			}).ToCell()
			if envelopeErr != nil {
				t.Fatal(envelopeErr)
			}
			enqueued, enqueuedErr := (tlb.EnqueuedMsg{EnqueuedLT: lt, Msg: envelope}).ToCell()
			if enqueuedErr != nil {
				t.Fatal(enqueuedErr)
			}
			if err = messages.Set(dispatchLTKey(lt), enqueued); err != nil {
				t.Fatal(err)
			}
		}
		accountQueue, accountErr := (tlb.AccountDispatchQueue{Messages: messages, Count: uint64(len(fixture.lts))}).ToCell()
		if accountErr != nil {
			t.Fatal(accountErr)
		}
		if err = queue.Set(dispatchAccountKey(fixture.accountID), accountQueue); err != nil {
			t.Fatal(err)
		}
	}
	return queue
}

func assertDispatchLTs(t *testing.T, queue *tlb.DispatchQueueAugDict, accountID [32]byte, want []uint64) {
	t.Helper()
	accountQueue, err := loadAccountDispatchQueue(queue, accountID)
	if len(want) == 0 && errors.Is(err, cell.ErrNoSuchKeyInDict) {
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	items, err := accountQueue.Messages.LoadAll()
	if err != nil {
		t.Fatal(err)
	}
	got := make([]uint64, len(items))
	for i := range items {
		got[i] = items[i].Key.MustLoadUInt(64)
	}
	if len(got) != len(want) {
		t.Fatalf("dispatch lts for %x = %v, want %v", accountID, got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("dispatch lts for %x = %v, want %v", accountID, got, want)
		}
	}
}

func assertTwoRefDispatchDescriptor(t *testing.T, descriptor *cell.Cell, tag uint64, first, second *cell.Cell) {
	t.Helper()
	loader := descriptor.MustBeginParse()
	if got := loader.MustLoadUInt(5); got != tag {
		t.Fatalf("descriptor tag = %05b, want %05b", got, tag)
	}
	if loader.MustLoadRef().MustToCell().HashKey() != first.HashKey() || loader.MustLoadRef().MustToCell().HashKey() != second.HashKey() {
		t.Fatal("descriptor references differ")
	}
	if loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		t.Fatalf("descriptor has %d trailing bits and %d refs", loader.BitsLeft(), loader.RefsNum())
	}
}

func repeatedDispatchAccount(value byte) [32]byte {
	var accountID [32]byte
	for i := range accountID {
		accountID[i] = value
	}
	return accountID
}
