package collator

import (
	"bytes"
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
	funcsop "github.com/xssnick/tonutils-go/tvm/op/funcs"
	stackop "github.com/xssnick/tonutils-go/tvm/op/stack"

	"github.com/xssnick/gton/service/validator/msgpool"
)

func TestBuildCandidateExecutesExternalTransaction(t *testing.T) {
	req := emptyCandidateRequest(t)
	addr := address.NewAddress(0, 0, bytes.Repeat([]byte{0x51}, 32))
	accounts := accountsWithActiveContract(t, addr, req.Header.GenUtime, 100_000_000_000)
	req.Previous.State = stateWithAccounts(t, req.Previous.State, accounts)

	message, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr: addr,
		Body:    cell.BeginCell().MustStoreUInt(0x1234, 16).EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	req.Externals = []ExternalInput{externalInput(t, message)}

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Stats.Transactions != 1 || candidate.Stats.ExternalIncluded != 1 {
		t.Fatalf("unexpected transaction stats: %+v", candidate.Stats)
	}
	if len(candidate.Externals) != 1 || candidate.Externals[0].Outcome != msgpool.ExternalIncluded {
		t.Fatalf("unexpected external result: %+v", candidate.Externals)
	}

	var block tlb.Block
	if err = parseExact(&block, candidateBlock(t, candidate)); err != nil {
		t.Fatal(err)
	}
	var flow tlb.ValueFlow
	if err = parseExact(&flow, block.ValueFlow); err != nil {
		t.Fatal(err)
	}
	if flow.FeesCollected.Coins.Nano().Cmp(flow.Created.Coins.Nano()) <= 0 {
		t.Fatalf("transaction fees were not collected: created=%s collected=%s", flow.Created.Coins, flow.FeesCollected.Coins)
	}
	if flow.ToNextBlock.Coins.Nano().Cmp(flow.FromPrevBlock.Coins.Nano()) >= 0 {
		t.Fatalf("transaction did not debit account fees: from=%s to=%s", flow.FromPrevBlock.Coins, flow.ToNextBlock.Coins)
	}

	var nextState tlb.ShardStateUnsplit
	if err = parseExact(&nextState, candidate.State); err != nil {
		t.Fatal(err)
	}
	key := cell.BeginCell().MustStoreSlice(addr.Data(), 256).EndCell()
	value, err := nextState.Accounts.ShardAccounts.LoadValue(key)
	if err != nil {
		t.Fatal(err)
	}
	var shardAccount tlb.ShardAccount
	if err = loadExactSlice(&shardAccount, value); err != nil {
		t.Fatal(err)
	}
	if shardAccount.LastTransLT == 0 || shardAccount.LastTransHash == nil {
		t.Fatalf("account transaction pointer was not updated: %+v", shardAccount)
	}
}

func TestBuildCandidateDistinguishesInvalidAndNotAcceptedExternals(t *testing.T) {
	req := emptyCandidateRequest(t)
	rejecting := address.NewAddress(0, 0, bytes.Repeat([]byte{0x52}, 32))
	req.Previous.State = stateWithAccounts(t, req.Previous.State, activeContracts(t, req.Header.GenUtime,
		activeContract{address: rejecting, code: externalRejectCode(t), balance: 100_000_000_000},
	))

	wrongShard, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr: address.NewAddress(0, 0xff, bytes.Repeat([]byte{0x53}, 32)),
		Body:    cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	notAccepted, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr: rejecting,
		Body:    cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	notExternal, err := tlb.ToCell(&tlb.InternalMessage{
		IHRDisabled: true,
		SrcAddr:     rejecting,
		DstAddr:     rejecting,
		Amount:      tlb.FromNanoTONU(1),
		CreatedLT:   requestStartLT(t, req) - 1,
		CreatedAt:   req.Header.GenUtime - 1,
		Body:        cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	req.Externals = []ExternalInput{
		externalInput(t, notExternal),
		externalInput(t, wrongShard),
		externalInput(t, notAccepted),
	}

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidate.Externals) != 3 || candidate.Externals[0].Outcome != msgpool.ExternalInvalid ||
		candidate.Externals[1].Outcome != msgpool.ExternalInvalid ||
		candidate.Externals[2].Outcome != msgpool.ExternalNotAccepted {
		t.Fatalf("unexpected external outcomes: %+v", candidate.Externals)
	}
	if candidate.Stats.Transactions != 0 || candidate.Stats.ExternalInvalid != 2 ||
		candidate.Stats.ExternalNotAccepted != 1 {
		t.Fatalf("unexpected external stats: %+v", candidate.Stats)
	}
}

func TestBuildCandidateBoundsRejectedExternalAttempts(t *testing.T) {
	req := emptyCandidateRequest(t)
	rejecting := address.NewAddress(0, 0, bytes.Repeat([]byte{0x5a}, 32))
	req.Previous.State = stateWithAccounts(t, req.Previous.State, activeContracts(t, req.Header.GenUtime,
		activeContract{address: rejecting, code: externalRejectCode(t), balance: 100_000_000_000},
	))

	req.Externals = make([]ExternalInput, 4)
	for i := range req.Externals {
		message, err := tlb.ToCell(&tlb.ExternalMessage{
			DstAddr: rejecting,
			Body:    cell.BeginCell().MustStoreUInt(uint64(i), 8).EndCell(),
		})
		if err != nil {
			t.Fatal(err)
		}
		req.Externals[i] = externalInput(t, message)
	}
	req.MaxExternalAttempts = 2

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Stats.ExternalAttempts != 2 || candidate.Stats.ExternalNotAccepted != 2 ||
		candidate.Stats.ExternalSkippedLimit != 2 {
		t.Fatalf("unexpected external budget stats: %+v", candidate.Stats)
	}
	if len(candidate.Externals) != 4 || candidate.Externals[0].Outcome != msgpool.ExternalNotAccepted ||
		candidate.Externals[1].Outcome != msgpool.ExternalNotAccepted ||
		candidate.Externals[2].Outcome != msgpool.ExternalSkippedLimit ||
		candidate.Externals[3].Outcome != msgpool.ExternalSkippedLimit {
		t.Fatalf("unexpected external budget outcomes: %+v", candidate.Externals)
	}
}

func TestBuildCandidateRequiresExternalAttemptBudget(t *testing.T) {
	req := emptyCandidateRequest(t)
	message, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr: address.NewAddress(0, 0, bytes.Repeat([]byte{0x5b}, 32)),
		Body:    cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	req.Externals = []ExternalInput{externalInput(t, message)}
	req.MaxExternalAttempts = 0

	_, err = testBuilder().BuildShard(context.Background(), req)
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "external attempt limit") {
		t.Fatalf("error = %v, want explicit external attempt limit error", err)
	}
}

func TestBuildCandidateRejectsUnscopedExternalInputs(t *testing.T) {
	base := emptyCandidateRequest(t)
	message, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr: address.NewAddress(0, 0, bytes.Repeat([]byte{0x5c}, 32)),
		Body:    cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	valid := externalInput(t, message)

	tests := []struct {
		name  string
		input ExternalInput
	}{
		{name: "missing_message", input: ExternalInput{Ref: valid.Ref}},
		{name: "missing_generation", input: ExternalInput{Ref: msgpool.ExternalRef{Hash: valid.Ref.Hash}, message: valid.message}},
		{name: "mismatched_hash", input: ExternalInput{Ref: msgpool.ExternalRef{Hash: [32]byte{1}, Generation: 1}, message: valid.message}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := base
			req.Externals = []ExternalInput{test.input}
			if _, err := testBuilder().BuildShard(context.Background(), req); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("error = %v, want ErrInvalidInput", err)
			}
		})
	}
}

func TestPoolSelectionCandidateFeedbackRoundTrip(t *testing.T) {
	req := emptyCandidateRequest(t)
	rejecting := address.NewAddress(0, 0, bytes.Repeat([]byte{0x5d}, 32))
	req.Previous.State = stateWithAccounts(t, req.Previous.State, activeContracts(t, req.Header.GenUtime,
		activeContract{address: rejecting, code: externalRejectCode(t), balance: 100_000_000_000},
	))
	message, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr: rejecting,
		Body:    cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}

	pool := msgpool.New(msgpool.Config{})
	t.Cleanup(pool.Close)
	if _, err = pool.AddExternal(len(message.ToBOC()), message, nil, 0); err != nil {
		t.Fatal(err)
	}
	selection := pool.SelectForBlock(msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll}, 1)
	if len(selection) != 1 {
		t.Fatalf("selection size = %d, want 1", len(selection))
	}
	input, err := NewExternalInput(selection[0])
	if err != nil {
		t.Fatal(err)
	}
	req.Externals = []ExternalInput{input}
	req.MaxExternalAttempts = 1

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidate.Externals) != 1 || candidate.Externals[0].Ref != selection[0].Reference() ||
		candidate.Externals[0].Outcome != msgpool.ExternalNotAccepted {
		t.Fatalf("candidate feedback = %+v", candidate.Externals)
	}
	if err = pool.Complete(candidate.Externals); err != nil {
		t.Fatal(err)
	}
	if next := pool.SelectForBlock(msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll}, 1); len(next) != 0 {
		t.Fatal("account-rejected external was immediately selected again")
	}
	if stats := pool.Stats(); stats.RejectedDelayed != 1 || stats.Pooled != 1 {
		t.Fatalf("pool stats after feedback = %+v", stats)
	}
}

func TestBuildCandidateRejectsDuplicateExternal(t *testing.T) {
	req := emptyCandidateRequest(t)
	addr := address.NewAddress(0, 0, bytes.Repeat([]byte{0x54}, 32))
	req.Previous.State = stateWithAccounts(t, req.Previous.State,
		accountsWithActiveContract(t, addr, req.Header.GenUtime, 100_000_000_000))

	message, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr: addr,
		Body:    cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	input := externalInput(t, message)
	req.Externals = []ExternalInput{input, input}

	if _, err = testBuilder().BuildShard(context.Background(), req); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("error = %v, want ErrInvalidInput", err)
	}
}

func TestAccountLaneRejectsMalformedContentAddressedStorageStat(t *testing.T) {
	req := emptyCandidateRequest(t)
	addr := address.NewAddress(0, 0, bytes.Repeat([]byte{0x56}, 32))
	shardAccountRoot := activeShardAccount(t, activeContract{
		address: addr,
		code:    externalAcceptCode(t),
		balance: 100_000_000_000,
	}, req.Header.GenUtime)

	var shardAccount tlb.ShardAccount
	if err := parseExact(&shardAccount, shardAccountRoot); err != nil {
		t.Fatal(err)
	}
	var account tlb.AccountState
	if err := parseExact(&account, shardAccount.Account); err != nil {
		t.Fatal(err)
	}
	statDict := cell.NewDict(256)
	if err := statDict.Set(
		cell.BeginCell().MustStoreSlice(bytes.Repeat([]byte{0x57}, 32), 256).EndCell(),
		cell.BeginCell().MustStoreUInt(1, 1).EndCell(),
	); err != nil {
		t.Fatal(err)
	}
	stat, err := statDict.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	hash := stat.HashKey()
	account.StorageInfo.StorageExtra = tlb.StorageExtraInfo{DictHash: hash[:]}
	shardAccount.Account, err = account.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	shardAccountRoot, err = tlb.ToCell(&shardAccount)
	if err != nil {
		t.Fatal(err)
	}
	accounts, err := tlb.NewShardAccountsAugDict()
	if err != nil {
		t.Fatal(err)
	}
	key := cell.BeginCell().MustStoreSlice(addr.Data(), 256).EndCell()
	if err = accounts.Set(key, shardAccountRoot); err != nil {
		t.Fatal(err)
	}
	req.Previous.State = stateWithAccounts(t, req.Previous.State, accounts)
	req.StorageStats = AccountStorageStats{hash: stat}

	c, err := testBuilder().prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.account(addr); err == nil {
		t.Fatal("malformed account storage stat cache entry was accepted")
	}
}

func TestBuildCandidateQueuesGeneratedInternalMessage(t *testing.T) {
	req := emptyCandidateRequest(t)
	// Undrained inbound queues force generated messages into the outbound
	// queue instead of immediate in-block delivery.
	req.Internals = &msgpool.Cut{More: true}
	sender := address.NewAddress(0, 0, bytes.Repeat([]byte{0x61}, 32))
	receiver := address.NewAddress(0, 0, bytes.Repeat([]byte{0x62}, 32))

	outbound, err := tlb.ToCell(&tlb.InternalMessage{
		IHRDisabled: true,
		SrcAddr:     address.NewAddressNone(),
		DstAddr:     receiver,
		Amount:      tlb.FromNanoTONU(1_000_000_000),
		Body:        cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	senderCode := externalSendCode(t, outbound)
	receiverCode := externalAcceptCode(t)
	accounts := activeContracts(t, req.Header.GenUtime,
		activeContract{address: sender, code: senderCode, balance: 100_000_000_000},
		activeContract{address: receiver, code: receiverCode, balance: 10_000_000_000},
	)
	req.Previous.State = stateWithAccounts(t, req.Previous.State, accounts)

	external, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr: sender,
		Body:    cell.BeginCell().MustStoreUInt(0x5678, 16).EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	req.Externals = []ExternalInput{externalInput(t, external)}

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Stats.Transactions != 1 || candidate.Stats.NewMessages != 1 ||
		candidate.Stats.EnqueuedMessages != 1 ||
		candidate.Stats.OutQueueSize != 1 {
		t.Fatalf("unexpected generated message stats: %+v", candidate.Stats)
	}

	var block tlb.Block
	if err = parseExact(&block, candidateBlock(t, candidate)); err != nil {
		t.Fatal(err)
	}
	inLoader := block.Extra.InMsgDesc.MustBeginParse()
	inMessages, err := tlb.LoadInMsgDescrAugDict(inLoader, req.Masterchain.Config.globalVersion)
	if err != nil {
		t.Fatal(err)
	}
	if inLoader.BitsLeft() != 0 || inLoader.RefsNum() != 0 {
		t.Fatal("in message descriptor has trailing data")
	}
	outLoader := block.Extra.OutMsgDesc.MustBeginParse()
	outMessages, err := tlb.LoadOutMsgDescrAugDict(outLoader, req.Masterchain.Config.globalVersion)
	if err != nil {
		t.Fatal(err)
	}
	if outLoader.BitsLeft() != 0 || outLoader.RefsNum() != 0 {
		t.Fatal("out message descriptor has trailing data")
	}
	inCount, err := inMessages.Count()
	if err != nil {
		t.Fatal(err)
	}
	outCount, err := outMessages.Count()
	if err != nil {
		t.Fatal(err)
	}
	if inCount != 1 || outCount != 1 {
		t.Fatalf("descriptor counts: in=%d out=%d", inCount, outCount)
	}
}

func TestBuildCandidateEnqueuesCrossWorkchainMessage(t *testing.T) {
	req := emptyCandidateRequest(t)
	sender := address.NewAddress(0, 0, bytes.Repeat([]byte{0x71}, 32))
	destination := address.NewAddress(0, 0xff, bytes.Repeat([]byte{0x72}, 32))
	outbound, err := tlb.ToCell(&tlb.InternalMessage{
		IHRDisabled: true,
		SrcAddr:     address.NewAddressNone(),
		DstAddr:     destination,
		Amount:      tlb.FromNanoTONU(1_000_000_000),
		Body:        cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	accounts := activeContracts(t, req.Header.GenUtime, activeContract{
		address: sender,
		code:    externalSendCode(t, outbound),
		balance: 100_000_000_000,
	})
	req.Previous.State = stateWithAccounts(t, req.Previous.State, accounts)
	external, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr: sender,
		Body:    cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	req.Externals = []ExternalInput{externalInput(t, external)}

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Stats.Transactions != 1 || candidate.Stats.NewMessages != 1 ||
		candidate.Stats.EnqueuedMessages != 1 || candidate.Stats.OutQueueSize != 1 {
		t.Fatalf("unexpected enqueue stats: %+v", candidate.Stats)
	}

	var state tlb.ShardStateUnsplit
	if err = parseExact(&state, candidate.State); err != nil {
		t.Fatal(err)
	}
	var queue tlb.OutMsgQueueInfo
	if err = parseExact(&queue, state.OutMsgQueueInfo); err != nil {
		t.Fatal(err)
	}
	count, err := queue.OutQueue.Count()
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("outbound queue count = %d, want 1", count)
	}

	var block tlb.Block
	if err = parseExact(&block, candidateBlock(t, candidate)); err != nil {
		t.Fatal(err)
	}
	var flow tlb.ValueFlow
	if err = parseExact(&flow, block.ValueFlow); err != nil {
		t.Fatal(err)
	}
	if flow.Exported.Coins.Nano().Sign() <= 0 {
		t.Fatalf("queued message is absent from exported value: %s", flow.Exported.Coins)
	}
}

func TestBuildCandidateLeavesExistingOutboundQueueUnchanged(t *testing.T) {
	req := emptyCandidateRequest(t)
	// The queued entry stays undrained: an incomplete cut keeps ProcessedInfo
	// untouched instead of claiming the queues with an infinity record.
	req.Internals = &msgpool.Cut{More: true}
	source := address.NewAddress(0, 0, bytes.Repeat([]byte{0x81}, 32))
	receiver := address.NewAddress(0, 0, bytes.Repeat([]byte{0x82}, 32))

	message, err := tlb.ToCell(&tlb.InternalMessage{
		IHRDisabled: true,
		SrcAddr:     source,
		DstAddr:     receiver,
		Amount:      tlb.FromNanoTONU(1_000_000_000),
		FwdFee:      tlb.FromNanoTONU(100_000),
		CreatedLT:   requestStartLT(t, req) - 10,
		CreatedAt:   req.Header.GenUtime - 1,
		Body:        cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := (tlb.MsgEnvelope{
		CurAddr:         tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: 96},
		NextAddr:        tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: 96},
		FwdFeeRemaining: tlb.FromNanoTONU(100_000),
		Msg:             message,
	}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	messageHash := message.HashKey()
	destination, err := accountPrefixFromAddress(receiver)
	if err != nil {
		t.Fatal(err)
	}
	key := msgpool.MakeQueueKey(destination, messageHash)
	enqueued, err := (tlb.EnqueuedMsg{
		EnqueuedLT: requestStartLT(t, req) - 10,
		Msg:        envelope,
	}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	req.Previous.State = stateWithQueueMessage(t, req.Previous.State, key, enqueued)
	queueSize := uint64(1)
	req.Previous.OutQueueSize = &queueSize

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Stats.Transactions != 0 || candidate.Stats.OutQueueSize != 1 {
		t.Fatalf("unexpected queue stats: %+v", candidate.Stats)
	}

	var state tlb.ShardStateUnsplit
	if err = parseExact(&state, candidate.State); err != nil {
		t.Fatal(err)
	}
	var queue tlb.OutMsgQueueInfo
	if err = parseExact(&queue, state.OutMsgQueueInfo); err != nil {
		t.Fatal(err)
	}
	if count, err := queue.OutQueue.Count(); err != nil || count != 1 {
		t.Fatalf("outbound queue count = %d, err = %v", count, err)
	}
	keyCell := cell.BeginCell().MustStoreSlice(key[:], 352).EndCell()
	value, err := queue.OutQueue.LoadValue(keyCell)
	if err != nil {
		t.Fatal(err)
	}
	valueCell, err := value.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	if valueCell.HashKey() != enqueued.HashKey() {
		t.Fatal("outbound queue entry changed")
	}
	if !queue.ProcInfo.IsEmpty() {
		t.Fatal("processed info advanced without inbound data")
	}
	assertUntracedCellTree(t, candidate.State)
	assertUntracedCellTree(t, candidate.StateUpdate)
}

func prepareExternal(tb testing.TB, root *cell.Cell) *tvm.PreparedMessage {
	tb.Helper()

	message, err := tvm.PrepareMessage(root)
	if err != nil {
		tb.Fatal(err)
	}
	return message
}

func externalInput(tb testing.TB, root *cell.Cell) ExternalInput {
	tb.Helper()

	return ExternalInput{
		Ref:     msgpool.ExternalRef{Hash: root.HashKey(), Generation: 1},
		message: prepareExternal(tb, root),
	}
}

func assertUntracedCellTree(tb testing.TB, root *cell.Cell) {
	tb.Helper()

	seen := map[*cell.Cell]struct{}{}
	var walk func(*cell.Cell)
	walk = func(current *cell.Cell) {
		if _, ok := seen[current]; ok {
			return
		}
		seen[current] = struct{}{}
		if current.Trace() != nil {
			tb.Fatal("candidate retained an internal traversal trace")
		}
		for i := 0; i < int(current.RefsNum()); i++ {
			ref, err := current.PeekRef(i)
			if err != nil {
				tb.Fatal(err)
			}
			walk(ref)
		}
	}
	walk(root)
}

func stateWithAccounts(tb testing.TB, root *cell.Cell, accounts *tlb.ShardAccountsAugDict) *cell.Cell {
	tb.Helper()

	var state tlb.ShardStateUnsplit
	if err := parseExact(&state, root); err != nil {
		tb.Fatal(err)
	}
	var stats tlb.ShardStateStats
	if err := parseExact(&stats, state.Stats); err != nil {
		tb.Fatal(err)
	}
	balance, err := loadAccountsBalance(accounts)
	if err != nil {
		tb.Fatal(err)
	}
	stats.TotalBalance = balance
	state.Stats, err = stats.ToCell()
	if err != nil {
		tb.Fatal(err)
	}
	state.Accounts.ShardAccounts = accounts
	next, err := tlb.ToCell(&state)
	if err != nil {
		tb.Fatal(err)
	}
	return next
}

func stateWithQueueMessage(tb testing.TB, root *cell.Cell, key msgpool.QueueKey, value *cell.Cell) *cell.Cell {
	tb.Helper()

	var state tlb.ShardStateUnsplit
	if err := parseExact(&state, root); err != nil {
		tb.Fatal(err)
	}
	var queue tlb.OutMsgQueueInfo
	if err := parseExact(&queue, state.OutMsgQueueInfo); err != nil {
		tb.Fatal(err)
	}
	keyCell := cell.BeginCell().MustStoreSlice(key[:], 352).EndCell()
	inserted, err := queue.OutQueue.SetWithMode(keyCell, value, cell.DictSetModeAdd)
	if err != nil {
		tb.Fatal(err)
	}
	if !inserted {
		tb.Fatal("queue message was not inserted")
	}
	state.OutMsgQueueInfo, err = queue.ToCell()
	if err != nil {
		tb.Fatal(err)
	}
	next, err := tlb.ToCell(&state)
	if err != nil {
		tb.Fatal(err)
	}
	return next
}

func accountsWithActiveContract(
	tb testing.TB,
	addr *address.Address,
	lastPaid uint32,
	balance uint64,
) *tlb.ShardAccountsAugDict {
	tb.Helper()
	return activeContracts(tb, lastPaid, activeContract{
		address: addr,
		code:    externalAcceptCode(tb),
		balance: balance,
	})
}

type activeContract struct {
	address *address.Address
	code    *cell.Cell
	balance uint64
}

func activeContracts(tb testing.TB, lastPaid uint32, contracts ...activeContract) *tlb.ShardAccountsAugDict {
	tb.Helper()

	accounts, err := tlb.NewShardAccountsAugDict()
	if err != nil {
		tb.Fatal(err)
	}
	for _, contract := range contracts {
		shardAccount := activeShardAccount(tb, contract, lastPaid)
		key := cell.BeginCell().MustStoreSlice(contract.address.Data(), 256).EndCell()
		if err = accounts.Set(key, shardAccount); err != nil {
			tb.Fatal(err)
		}
	}
	return accounts
}

func activeShardAccount(tb testing.TB, contract activeContract, lastPaid uint32) *cell.Cell {
	tb.Helper()

	stateInit, err := tlb.ToCell(&tlb.StateInit{
		Code: contract.code,
		Data: cell.BeginCell().EndCell(),
	})
	if err != nil {
		tb.Fatal(err)
	}
	storage, err := tlb.ToCell(&tlb.StorageInfo{
		StorageUsed: tlb.StorageUsed{
			CellsUsed: big.NewInt(0),
			BitsUsed:  big.NewInt(0),
		},
		StorageExtra: tlb.StorageExtraNone{},
		LastPaid:     lastPaid,
	})
	if err != nil {
		tb.Fatal(err)
	}
	account := cell.BeginCell().
		MustStoreBoolBit(true).
		MustStoreAddr(contract.address).
		MustStoreBuilder(storage.ToBuilder()).
		MustStoreUInt(0, 64).
		MustStoreBigCoins(new(big.Int).SetUint64(contract.balance)).
		MustStoreDict(nil).
		MustStoreBoolBit(true).
		MustStoreBuilder(stateInit.ToBuilder()).
		EndCell()
	shardAccount, err := tlb.ToCell(&tlb.ShardAccount{
		Account:       account,
		LastTransHash: make([]byte, 32),
	})
	if err != nil {
		tb.Fatal(err)
	}

	return shardAccount
}

func externalAcceptCode(tb testing.TB) *cell.Cell {
	tb.Helper()

	code := cell.BeginCell()
	for range 5 {
		if err := code.StoreBuilder(stackop.DROP().Serialize()); err != nil {
			tb.Fatal(err)
		}
	}
	if err := code.StoreBuilder(funcsop.ACCEPT().Serialize()); err != nil {
		tb.Fatal(err)
	}
	return code.EndCell()
}

func externalRejectCode(tb testing.TB) *cell.Cell {
	tb.Helper()

	code := cell.BeginCell()
	for range 5 {
		if err := code.StoreBuilder(stackop.DROP().Serialize()); err != nil {
			tb.Fatal(err)
		}
	}
	return code.EndCell()
}

func externalSendCode(tb testing.TB, message *cell.Cell) *cell.Cell {
	tb.Helper()

	code := externalAcceptCode(tb).ToBuilder()
	if err := code.StoreBuilder(stackop.PUSHREF(message).Serialize()); err != nil {
		tb.Fatal(err)
	}
	if err := code.StoreBuilder(stackop.PUSHINT(big.NewInt(0)).Serialize()); err != nil {
		tb.Fatal(err)
	}
	if err := code.StoreBuilder(funcsop.SENDRAWMSG().Serialize()); err != nil {
		tb.Fatal(err)
	}
	return code.EndCell()
}
