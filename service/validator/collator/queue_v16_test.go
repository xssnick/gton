package collator

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/xssnick/gton/service/validator/msgpool"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type dispatchBalanceCase struct {
	name                   string
	version                uint32
	old, removed, added    []uint64
	oldBalance, newBalance int64
	reject                 bool
}

func TestDispatchBalanceTransitions(t *testing.T) {
	const one = 1_001_000
	account := [32]byte{1}
	tests := []dispatchBalanceCase{
		{name: "new queue stores balance", version: 16, added: []uint64{30}, oldBalance: -1, newBalance: one},
		{name: "new queue missing balance", version: 16, added: []uint64{30}, oldBalance: -1, newBalance: -1, reject: true},
		{name: "known partial remove and append", version: 16, old: []uint64{10, 20}, removed: []uint64{10}, added: []uint64{30, 40}, oldBalance: 2 * one, newBalance: 3 * one},
		{name: "known cannot lose balance", version: 16, old: []uint64{10, 20}, removed: []uint64{10}, oldBalance: 2 * one, newBalance: -1, reject: true},
		{name: "legacy keeps unknown balance", version: 16, old: []uint64{10, 20}, removed: []uint64{10}, added: []uint64{30}, oldBalance: -1, newBalance: -1},
		{name: "legacy remaining cannot gain balance", version: 16, old: []uint64{10, 20}, removed: []uint64{10}, added: []uint64{30}, oldBalance: -1, newBalance: 2 * one, reject: true},
		{name: "legacy drained then replaced gains balance", version: 16, old: []uint64{10, 20}, removed: []uint64{10, 20}, added: []uint64{30}, oldBalance: -1, newBalance: one},
		{name: "legacy drained then replaced can remain legacy", version: 16, old: []uint64{10, 20}, removed: []uint64{10, 20}, added: []uint64{30}, oldBalance: -1, newBalance: -1},
		{name: "wrong balance rejected", version: 16, old: []uint64{10, 20}, removed: []uint64{10}, oldBalance: 2 * one, newBalance: one - 1, reject: true},
		{name: "balance-only removal forbidden", version: 16, old: []uint64{10, 20}, oldBalance: 2 * one, newBalance: -1, reject: true},
		{name: "downgrade preserves updated balance", version: 15, old: []uint64{10, 20}, removed: []uint64{10}, added: []uint64{30}, oldBalance: 2 * one, newBalance: 2 * one},
		{name: "downgrade drops balance", version: 15, old: []uint64{10, 20}, removed: []uint64{10}, oldBalance: 2 * one, newBalance: -1},
		{name: "downgrade drops balance without message change", version: 15, old: []uint64{10, 20}, oldBalance: 2 * one, newBalance: -1},
		{name: "downgrade cannot add balance to legacy", version: 15, old: []uint64{10}, removed: []uint64{10}, added: []uint64{30}, oldBalance: -1, newBalance: one, reject: true},
		{name: "downgrade cannot add balance to new queue", version: 15, added: []uint64{30}, oldBalance: -1, newBalance: one, reject: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newSemanticDispatchBatchFixture(t, []semanticDispatchBatchAccount{{id: account, old: test.old, removed: test.removed, added: test.added}})
			if len(test.old) != 0 {
				setDispatchBalance(t, fixture.old, account, test.oldBalance)
			}
			if len(test.old)-len(test.removed)+len(test.added) != 0 {
				setDispatchBalance(t, fixture.candidate, account, test.newBalance)
			}
			validation := fixture.validation()
			validation.replay.transition.Config.globalVersion = test.version
			err := validation.precheckDispatchQueueUpdate()
			if err == nil {
				err = validation.applyDispatchChanges()
			}
			if err == nil {
				err = validation.verifyQueueRoots()
			}
			if (err != nil) != test.reject {
				t.Fatalf("validation error = %v, reject = %v", err, test.reject)
			}
			if err != nil && !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func setDispatchBalance(t testing.TB, queue *tlb.DispatchQueueAugDict, accountID [32]byte, amount int64) {
	t.Helper()
	account, err := loadAccountDispatchQueue(queue, accountID)
	if err != nil {
		t.Fatal(err)
	}
	account.TotalBalance = nil
	if amount >= 0 {
		account.TotalBalance = &tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(uint64(amount))}
	}
	if err = storeAccountDispatchQueue(queue.AugmentedDictionary, accountID, account); err != nil {
		t.Fatal(err)
	}
}

func TestDispatchProducerBalanceAndPendingHardLimit(t *testing.T) {
	accountID := [32]byte{1}
	for _, version := range []uint32{15, 16} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			queue := makeDispatchQueue(t, dispatchFixtureAccount{accountID: accountID, lts: []uint64{10, 20, 30}})
			setDispatchBalance(t, queue, accountID, 3_003_000)
			c := dispatchTestCollation(t, queue, DispatchPolicy{Phase2MaxTotal: 10, Phase2MaxPerInitiator: 10, DeferMessagesAfter: 100})
			c.config.globalVersion = version
			c.config.outQueueHardLimit = 2
			if err := c.processDispatchQueue(); err != nil {
				t.Fatal(err)
			}
			if version < 16 {
				if c.pendingDispatchMessages != 3 {
					t.Fatalf("legacy pending = %d", c.pendingDispatchMessages)
				}
				return
			}
			assertDispatchLTs(t, c.dispatchQueue, accountID, []uint64{30})
			account, err := loadAccountDispatchQueue(c.dispatchQueue, accountID)
			if err != nil {
				t.Fatal(err)
			}
			if account.TotalBalance == nil || account.TotalBalance.Coins.Nano().Uint64() != 1_001_000 {
				t.Fatalf("remaining balance = %+v", account.TotalBalance)
			}
			if c.pendingDispatchMessages != 2 || c.haveUnprocessedDispatchQueue {
				t.Fatalf("pending=%d, mandatory=%v", c.pendingDispatchMessages, c.haveUnprocessedDispatchQueue)
			}
		})
	}
}

func TestDispatchMessageBalanceIncludesOriginalFeesAndCurrencies(t *testing.T) {
	extras := cell.NewDict(32)
	if err := extras.SetIntKey(big.NewInt(7), cell.BeginCell().MustStoreBigVarUInt(big.NewInt(100), 32).EndCell()); err != nil {
		t.Fatal(err)
	}
	message := tlb.InternalMessage{Amount: tlb.FromNanoTONU(100), FwdFee: tlb.FromNanoTONU(7), IHRFee: tlb.FromNanoTONU(3), ExtraCurrencies: extras}
	if balance := dispatchMessageBalance(&message, 16); balance.Coins.Nano().Uint64() != 107 || balance.ExtraCurrencies != extras {
		t.Fatalf("GV16 balance = %+v", balance)
	}
	if balance := dispatchMessageBalance(&message, 11); balance.Coins.Nano().Uint64() != 110 {
		t.Fatalf("GV11 balance = %+v", balance)
	}
	if err := checkDispatchExtraCurrencies(extras, 1); err != nil {
		t.Fatal(err)
	}
	if err := checkDispatchExtraCurrencies(extras, 0); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("extra currency limit error = %v", err)
	}
}

type queueLimitCase struct {
	name                 string
	version              uint32
	old, new             uint64
	storedOld, storedNew bool
	reject               bool
}

func TestOutboundQueueHardLimitUsesStoredPreviousSize(t *testing.T) {
	tests := []queueLimitCase{
		{name: "at hard", version: 16, new: 18, storedNew: true},
		{name: "over hard", version: 16, new: 19, storedNew: true, reject: true},
		{name: "old oversized stays equal", version: 16, old: 20, new: 20, storedOld: true, storedNew: true},
		{name: "old oversized shrinks", version: 16, old: 20, new: 19, storedOld: true, storedNew: true},
		{name: "old oversized cannot grow", version: 16, old: 20, new: 21, storedOld: true, storedNew: true, reject: true},
		{name: "unstored old does not raise limit", version: 16, old: 20, new: 20, storedNew: true, reject: true},
		{name: "legacy version", version: 15, new: 30, storedNew: true},
		{name: "size absent no check", version: 16, new: 30},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newSemanticDispatchBatchFixture(t, nil)
			validation := fixture.validation()
			validation.replay.transition.Config.globalVersion = test.version
			validation.replay.transition.Config.outQueueHardLimit = 18
			validation.queueSize = test.new
			if test.storedOld {
				validation.old.Extra.OutQueueSize = &test.old
			}
			if test.storedNew {
				validation.candidate.Extra.OutQueueSize = &test.new
			}
			err := validation.verifyQueueRoots()
			if (err != nil) != test.reject {
				t.Fatalf("error = %v, reject = %v", err, test.reject)
			}
		})
	}
}

func TestDispatchSoftLimitFirstAndGovernanceMessages(t *testing.T) {
	accountID := [32]byte{1}
	for _, oldSize := range []uint64{11, 12, 13} {
		for _, special := range []bool{false, true} {
			name := "ordinary"
			if special {
				name = "governance"
			}
			t.Run(name+"/"+tlb.FromNanoTONU(oldSize).String(), func(t *testing.T) {
				config := &Config{globalVersion: 16, capabilities: capDeferMessages, outQueueSoftLimit: 12}
				validation := semanticQueueValidation{old: tlb.OutMsgQueueInfo{Extra: &tlb.OutMsgQueueExtra{OutQueueSize: &oldSize}}, replay: &semanticReplay{
					transition: CandidateTransition{Config: config}, accounts: map[[32]byte]*semanticAccountResult{accountID: {outputTags: [][]uint8{{semanticOutNewDeferred}}}},
				}}
				if special {
					validation.replay.specials.set = map[[32]byte]struct{}{accountID: {}}
					validation.target.Workchain = masterchainWorkchainID
				}
				err := validation.verifyDispatchOutputPolicy(nil)
				if (err == nil) != (oldSize > 12) {
					t.Fatalf("old size %d: %v", oldSize, err)
				}
			})
		}
	}
}

func TestQueueMergeWishHardHalfBoundary(t *testing.T) {
	config := &Config{globalVersion: 16, outQueueHardLimit: 18}
	for _, size := range []uint64{8, 9, 10} {
		queue := tlb.OutMsgQueueInfo{Extra: &tlb.OutMsgQueueExtra{OutQueueSize: &size}}
		if got := queueAllowsMerge(config, &queue); got != (size <= 9) {
			t.Fatalf("size %d: merge allowed %v", size, got)
		}
		previous := tlb.ShardStateStats{UnderloadHistory: ^uint64(0)}
		next := previous
		header := tlb.BlockHeader{}
		header.WantMerge = size <= 9
		if err := verifyLoadHistory(&previous, &next, &header, false, queueAllowsMerge(config, &queue)); err != nil {
			t.Fatal(err)
		}
		header.WantMerge = !header.WantMerge
		if err := verifyLoadHistory(&previous, &next, &header, false, queueAllowsMerge(config, &queue)); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("wrong wish accepted: %v", err)
		}
	}
}

func TestGeneratedQueueHardLimitSerialAndWaves(t *testing.T) {
	var baseline *Candidate
	for _, workers := range []int{-1, 1, 4} {
		req := generatedWaveMultiSourceFixtureRequest(t, generatedWaveMinParallelWidth)
		req.internalWaveWorkers = workers
		req.Masterchain.Config.globalVersion = 16
		req.Masterchain.Config.capabilities |= capDeferMessages | capStoreOutMsgQueueSize
		req.Masterchain.Config.outQueueHardLimit = 1
		req.Masterchain.Config.outQueueSoftLimit = 0
		req.Masterchain.Config.maxMsgExtraCurrencies = 2
		req.Previous.State = previousStateWithDenseOutQueue(t, req.Previous.State, 1)
		size := uint64(1)
		req.Previous.OutQueueSize = &size
		var state tlb.ShardStateUnsplit
		if err := parseExact(&state, req.Previous.State); err != nil {
			t.Fatal(err)
		}
		var previousQueue tlb.OutMsgQueueInfo
		if err := parseExact(&previousQueue, state.OutMsgQueueInfo); err != nil {
			t.Fatal(err)
		}
		if previousQueue.Extra == nil {
			emptyDispatch, err := tlb.NewDispatchQueueAugDict()
			if err != nil {
				t.Fatal(err)
			}
			previousQueue.Extra = &tlb.OutMsgQueueExtra{DispatchQueue: emptyDispatch}
		}
		previousQueue.Extra.OutQueueSize = &size
		var err error
		state.OutMsgQueueInfo, err = previousQueue.ToCell()
		if err != nil {
			t.Fatal(err)
		}
		req.Previous.State, err = tlb.ToCell(&state)
		if err != nil {
			t.Fatal(err)
		}
		req.Internals = &msgpool.Cut{More: true}
		candidate, err := testBuilder().BuildShard(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		if candidate.Stats.DeferredMessages != generatedWaveMinParallelWidth || candidate.Stats.ImmediateDelivered != 0 {
			t.Fatalf("workers %d: %+v", workers, candidate.Stats)
		}
		queue := candidateQueueInfo(t, candidate)
		if err = queue.Extra.DispatchQueue.ForEachValueExtra(func(value, extra *cell.Slice) (bool, error) {
			var account tlb.AccountDispatchQueue
			if err := loadExactSlice(&account, value); err != nil {
				return false, err
			}
			if account.TotalBalance == nil {
				return false, errors.New("new account has no dispatch balance")
			}
			return true, nil
		}); err != nil {
			t.Fatal(err)
		}
		verification := shardVerificationRequest(req, candidate)
		verification.Semantics = NewSemanticVerifier(tvm.NewTVM())
		if err = verifyShardCandidateForTest(context.Background(), verification); err != nil {
			t.Fatal(err)
		}
		if baseline != nil && !equalCell(candidate.State, baseline.State) {
			t.Fatalf("workers %d changed state", workers)
		}
		baseline = candidate
	}
}

func TestQueueExactEnqueuedLTForEveryExport(t *testing.T) {
	envelope := semanticDispatchBatchEnvelope(t, [32]byte{1}, 20, false)
	hash := envelope.message.HashKey()
	key := msgpool.MakeQueueKey(envelope.next, hash)
	for _, tag := range []uint8{semanticOutNew, semanticOutDeferredTransit, semanticOutTransit, semanticOutTransitRequest} {
		expected := uint64(10)
		if tag == semanticOutNew || tag == semanticOutDeferredTransit {
			expected = 20
		}
		validation := semanticQueueValidation{replay: &semanticReplay{candidate: &verifiedCandidate{}}, out: map[cell.Hash]*semanticOutDescriptor{hash: {tag: tag, envelope: envelope}}}
		validation.replay.candidate.block.BlockInfo.StartLt = 10
		entry := semanticQueueEntry{envelope: *envelope, enqueued: tlb.EnqueuedMsg{EnqueuedLT: expected}}
		if err := validation.verifyOutQueueChange(key, &entry, true); err != nil {
			t.Fatalf("tag %d: %v", tag, err)
		}
		entry.enqueued.EnqueuedLT++
		if err := validation.verifyOutQueueChange(key, &entry, true); !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "enqueued lt") {
			t.Fatalf("tag %d: wrong lt error %v", tag, err)
		}
	}
}

func TestOutboundQueueChecksOnlyChangedNewCanonicalLabels(t *testing.T) {
	envelope := semanticDispatchBatchEnvelope(t, [32]byte{1}, 20, false)
	hash := envelope.message.HashKey()
	key := msgpool.MakeQueueKey(envelope.next, hash)
	enqueued, err := (tlb.EnqueuedMsg{EnqueuedLT: 20, Msg: envelope.root}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	empty, err := tlb.NewOutMsgQueueAugDict()
	if err != nil {
		t.Fatal(err)
	}
	canonical := &tlb.OutMsgQueueAugDict{AugmentedDictionary: empty.Copy()}
	if err = canonical.Set(cell.BeginCell().MustStoreSlice(key[:], 352).EndCell(), enqueued); err != nil {
		t.Fatal(err)
	}
	// hml_short stores this full 352-bit key less compactly than hml_long.
	label := cell.BeginCell().MustStoreBoolBit(false)
	for range 352 {
		label.MustStoreBoolBit(true)
	}
	label.MustStoreBoolBit(false).MustStoreSlice(key[:], 352).MustStoreUInt(20, 64)
	if err = label.StoreBuilder(enqueued.ToBuilder()); err != nil {
		t.Fatal(err)
	}
	bad := &tlb.OutMsgQueueAugDict{AugmentedDictionary: label.EndCell().AsAugDict(352, tlb.AugOutMsgQueue{})}
	validation := semanticQueueValidation{replay: &semanticReplay{candidate: &verifiedCandidate{}}, old: tlb.OutMsgQueueInfo{OutQueue: empty}, candidate: &tlb.OutMsgQueueInfo{OutQueue: canonical}, out: map[cell.Hash]*semanticOutDescriptor{hash: {tag: semanticOutNew, envelope: envelope}}}
	if err = validation.precheckOutQueueUpdate(); err != nil {
		t.Fatalf("canonical control: %v", err)
	}
	validation.candidate.OutQueue = bad
	if err = validation.precheckOutQueueUpdate(); !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "not canonical") {
		t.Fatalf("bad new label error = %v", err)
	}
	validation.old.OutQueue = bad
	if err = validation.precheckOutQueueUpdate(); err != nil {
		t.Fatalf("unchanged noncanonical old root: %v", err)
	}
	validation.candidate.OutQueue = empty
	validation.out[hash].tag = semanticOutDequeue
	if err = validation.precheckOutQueueUpdate(); err != nil {
		t.Fatalf("delete old noncanonical root: %v", err)
	}
}

func TestPendingDispatchForcesFirstGovernanceAndWhitelistedOutputs(t *testing.T) {
	accountID := [32]byte{1}
	for _, kind := range []string{"first", "governance", "whitelisted"} {
		t.Run(kind, func(t *testing.T) {
			c := dispatchTestCollation(t, makeDispatchQueue(t), DispatchPolicy{})
			c.config.globalVersion = 16
			c.config.outQueueHardLimit = 1
			c.pendingDispatchMessages = 1
			c.shard.Workchain = -1
			source := msgpool.AccountPrefix{Workchain: -1}
			if kind == "governance" {
				c.config.specials.set = map[[32]byte]struct{}{accountID: {}}
			}
			if kind == "whitelisted" {
				c.req.dispatch.Whitelist = []DispatchAccount{{Workchain: -1, AccountID: accountID}}
			}
			item := newMessage{index: 0}
			deferred, err := c.shouldDeferGenerated(&item, source, accountID)
			if err != nil || !deferred {
				t.Fatalf("serial pending-dispatch deferral = %v, %v", deferred, err)
			}
			state := c.newGeneratedWavePlanState()
			plan := generatedPlan{item: item, source: source, sourceID: accountID, destination: source}
			immediate, err := c.planGeneratedImmediate(&plan, &state)
			if err != nil || immediate {
				t.Fatalf("wave pending-dispatch immediate = %v, %v", immediate, err)
			}
			c.pendingDispatchMessages = 0
			deferred, err = c.shouldDeferGenerated(&item, source, accountID)
			if err != nil || deferred {
				t.Fatalf("below hard deferral = %v, %v", deferred, err)
			}
			state = c.newGeneratedWavePlanState()
			immediate, err = c.planGeneratedImmediate(&plan, &state)
			if err != nil || !immediate {
				t.Fatalf("below hard immediate = %v, %v", immediate, err)
			}
		})
	}
}

type messageQueueConfigCase struct {
	name      string
	parameter any
	expected  messageQueueLimits
}

func TestMessageQueueConfigDefaultsAndExplicitLimits(t *testing.T) {
	base := loadMainnetConfig(t).execution.Root()
	tests := []messageQueueConfigCase{
		{name: "v1 defaults", parameter: tlb.SizeLimitsConfigV1{}, expected: messageQueueLimits{deferThreshold: 256, hard: 18000, soft: 12000, maxExtraCurrencies: 2}},
		{name: "v2 defaults", parameter: tlb.SizeLimitsConfigV2{DeferOutQueueSizeLimit: 300, MaxMsgExtraCurrencies: 4}, expected: messageQueueLimits{deferThreshold: 300, hard: 18000, soft: 12000, maxExtraCurrencies: 4}},
		{name: "v3 explicit", parameter: tlb.SizeLimitsConfigV3{DeferOutQueueSizeLimit: 500, MaxMsgExtraCurrencies: 5, OutMsgQueueSizeHardLimit: 19000, OutMsgQueueSizeSoftLimit: 13000}, expected: messageQueueLimits{deferThreshold: 500, hard: 19000, soft: 13000, maxExtraCurrencies: 5}},
		{name: "v3 zero limits", parameter: tlb.SizeLimitsConfigV3{}, expected: messageQueueLimits{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parameter, err := tlb.ToCell(test.parameter)
			if err != nil {
				t.Fatal(err)
			}
			config := tlb.BlockchainConfig{Root: masterBuildConfigWithParam(t, base, 43, parameter)}
			got, err := configMessageQueueLimits(config)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.expected {
				t.Fatalf("got %+v, want %+v", got, test.expected)
			}
		})
	}
}
