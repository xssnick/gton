package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"reflect"
	"testing"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestTryLocateSourceTransactionFromBlock(t *testing.T) {
	account := bytes.Repeat([]byte{0x55}, 32)
	destination := bytes.Repeat([]byte{0x20}, 32)
	foreignAccount := bytes.Repeat([]byte{0x01}, 32)
	transactionLT := uint64(55)
	createdLT := transactionLT + 3

	outputs := []*tlb.Message{
		testHTTPAPIInternalMessage(account, bytes.Repeat([]byte{0x31}, 32), transactionLT+1),
		testHTTPAPIInternalMessage(account, bytes.Repeat([]byte{0x32}, 32), transactionLT+2),
		testHTTPAPIInternalMessage(account, destination, createdLT),
	}
	txCell := testHTTPAPIMessageTransaction(t, account, transactionLT, 0, make([]byte, 32), nil, outputs)
	root := testHTTPAPIBlockWithAccountTxs(t, []testHTTPAPIAccountTxs{
		{account: foreignAccount, txs: map[uint64]*cell.Cell{1: cell.BeginCell().MustStoreUInt(0, 4).EndCell()}},
		{account: account, txs: map[uint64]*cell.Cell{transactionLT: txCell}},
	})
	block := testHTTPAPIBlockIDForRoot(1, root)
	lookup := testHTTPAPIAccountLTLookup(0, account, createdLT)
	store := newMessageLookupTestStore(map[accountLTLookup]ton.BlockIDExt{lookup: block}, []blockRoot{{block: block, root: root}})

	srv := newTestServer()
	srv.store = store
	result, apiErr := srv.handleTryLocateSourceTx(context.Background(), requestParams{query: mapValues(map[string]string{
		"source":      address.NewAddress(0, 0, account).String(),
		"destination": address.NewAddress(0, 0, destination).String(),
		"created_lt":  fmt.Sprintf("%d", createdLT),
	})})
	if apiErr != nil {
		t.Fatalf("tryLocateSourceTx error: %v", apiErr.message)
	}
	tx := result.(extTransaction)
	if tx.TransactionID.LT != fmt.Sprintf("%d", transactionLT) || len(tx.OutMsgs) != 3 {
		t.Fatalf("unexpected located source transaction: %+v", tx)
	}
	if !reflect.DeepEqual(store.lookupCalls, []accountLTLookup{lookup}) {
		t.Fatalf("lookup calls = %+v, want %+v", store.lookupCalls, []accountLTLookup{lookup})
	}
	if store.rootCalls[storage.BlockKey(block)] != 1 {
		t.Fatalf("block root loads = %v, want one", store.rootCalls)
	}
}

type tryLocateTestHandler struct {
	name string
	run  func(*Server, context.Context, requestParams) (any, *apiError)
}

func TestTryLocateResultTransactionFromBlocks(t *testing.T) {
	source := bytes.Repeat([]byte{0x10}, 32)
	destination := bytes.Repeat([]byte{0x55}, 32)
	foreignAccount := bytes.Repeat([]byte{0x01}, 32)
	createdLT := uint64(54)
	transactionLT := uint64(1_000_060)

	firstRoot := testHTTPAPIBlockWithAccountTxs(t, []testHTTPAPIAccountTxs{
		{account: foreignAccount, txs: map[uint64]*cell.Cell{60: cell.BeginCell().MustStoreUInt(0, 4).EndCell()}},
	})
	inbound := testHTTPAPIInternalMessage(source, destination, createdLT)
	txCell := testHTTPAPIMessageTransaction(t, destination, transactionLT, 0, make([]byte, 32), inbound, nil)
	resultRoot := testHTTPAPIBlockWithTransaction(t, destination, transactionLT, txCell)
	firstBlock := testHTTPAPIBlockIDForRoot(1, firstRoot)
	resultBlock := testHTTPAPIBlockIDForRoot(2, resultRoot)
	firstLookup := testHTTPAPIAccountLTLookup(0, destination, createdLT)
	resultLookup := testHTTPAPIAccountLTLookup(0, destination, createdLT+messageLookupLTStep)

	for _, handler := range []tryLocateTestHandler{
		{name: "tryLocateTx", run: func(s *Server, ctx context.Context, params requestParams) (any, *apiError) {
			return s.handleTryLocateInboundMessageTransaction(ctx, params)
		}},
		{name: "tryLocateResultTx", run: func(s *Server, ctx context.Context, params requestParams) (any, *apiError) {
			return s.handleTryLocateInboundMessageTransaction(ctx, params)
		}},
	} {
		t.Run(handler.name, func(t *testing.T) {
			store := newMessageLookupTestStore(map[accountLTLookup]ton.BlockIDExt{
				firstLookup:  firstBlock,
				resultLookup: resultBlock,
			}, []blockRoot{{block: firstBlock, root: firstRoot}, {block: resultBlock, root: resultRoot}})
			srv := newTestServer()
			srv.store = store

			result, apiErr := handler.run(srv, context.Background(), requestParams{query: mapValues(map[string]string{
				"source":      address.NewAddress(0, 0, source).String(),
				"destination": address.NewAddress(0, 0, destination).String(),
				"created_lt":  fmt.Sprintf("%d", createdLT),
			})})
			if apiErr != nil {
				t.Fatalf("locate result transaction: %v", apiErr.message)
			}
			tx := result.(extTransaction)
			if tx.TransactionID.LT != fmt.Sprintf("%d", transactionLT) || tx.InMsg == nil {
				t.Fatalf("unexpected located result transaction: %+v", tx)
			}
			if !reflect.DeepEqual(store.lookupCalls, []accountLTLookup{firstLookup, resultLookup}) {
				t.Fatalf("lookup calls = %+v, want two LT probes", store.lookupCalls)
			}
		})
	}
}

func TestTryLocateResultTransactionFollowsAccountHistory(t *testing.T) {
	source := bytes.Repeat([]byte{0x10}, 32)
	destination := bytes.Repeat([]byte{0x55}, 32)
	foreignAccount := bytes.Repeat([]byte{0x01}, 32)
	createdLT := uint64(54)

	resultTx := testHTTPAPIMessageTransaction(t, destination, 500_000, 0, make([]byte, 32),
		testHTTPAPIInternalMessage(source, destination, createdLT), nil)
	latestTx := testHTTPAPIMessageTransaction(t, destination, 1_000_060, 500_000, resultTx.Hash(),
		testHTTPAPIInternalMessage(bytes.Repeat([]byte{0x99}, 32), destination, createdLT), nil)
	foreignTx := testHTTPAPIChainTransaction(t, foreignAccount, 60, 0, make([]byte, 32))
	firstRoot := testHTTPAPIBlockWithTransaction(t, foreignAccount, 60, foreignTx)
	resultRoot := testHTTPAPIBlockWithTransaction(t, destination, 500_000, resultTx)
	latestRoot := testHTTPAPIBlockWithTransaction(t, destination, 1_000_060, latestTx)
	firstBlock := testHTTPAPIBlockIDForRoot(1, firstRoot)
	resultBlock := testHTTPAPIBlockIDForRoot(2, resultRoot)
	latestBlock := testHTTPAPIBlockIDForRoot(3, latestRoot)
	initialLookup := testHTTPAPIAccountLTLookup(0, destination, createdLT)
	probeLookup := testHTTPAPIAccountLTLookup(0, destination, createdLT+messageLookupLTStep)
	historyLookup := testHTTPAPIAccountLTLookup(0, destination, 500_000)
	store := newMessageLookupTestStore(map[accountLTLookup]ton.BlockIDExt{
		initialLookup: firstBlock,
		probeLookup:   latestBlock,
		historyLookup: resultBlock,
	}, []blockRoot{
		{block: firstBlock, root: firstRoot},
		{block: resultBlock, root: resultRoot},
		{block: latestBlock, root: latestRoot},
	})

	srv := newTestServer()
	srv.store = store
	result, apiErr := srv.handleTryLocateInboundMessageTransaction(context.Background(), requestParams{query: mapValues(map[string]string{
		"source":      address.NewAddress(0, 0, source).String(),
		"destination": address.NewAddress(0, 0, destination).String(),
		"created_lt":  fmt.Sprintf("%d", createdLT),
	})})
	if apiErr != nil {
		t.Fatalf("locate result transaction: %v", apiErr.message)
	}
	if tx := result.(extTransaction); tx.TransactionID.LT != "500000" {
		t.Fatalf("located transaction LT = %s, want 500000", tx.TransactionID.LT)
	}
	if !reflect.DeepEqual(store.lookupCalls, []accountLTLookup{initialLookup, probeLookup, historyLookup}) {
		t.Fatalf("lookup calls = %+v, want two probes and one history lookup", store.lookupCalls)
	}
}

func TestTryLocateTransactionNotFoundAfterBoundedProbes(t *testing.T) {
	source := bytes.Repeat([]byte{0x10}, 32)
	destination := bytes.Repeat([]byte{0x55}, 32)
	createdLT := uint64(54)
	store := newMessageLookupTestStore(nil, nil)
	srv := newTestServer()
	srv.store = store

	_, apiErr := srv.handleTryLocateInboundMessageTransaction(context.Background(), requestParams{query: mapValues(map[string]string{
		"source":      address.NewAddress(0, 0, source).String(),
		"destination": address.NewAddress(0, 0, destination).String(),
		"created_lt":  fmt.Sprintf("%d", createdLT),
	})})
	if apiErr == nil || apiErr.status != 404 || apiErr.message != "transaction was not found" {
		t.Fatalf("unexpected error: %+v", apiErr)
	}
	if len(store.lookupCalls) != 3 {
		t.Fatalf("lookup calls = %d, want three bounded probes", len(store.lookupCalls))
	}
	for i, call := range store.lookupCalls {
		want := createdLT + uint64(i)*messageLookupLTStep
		if call.lt != want || call.workchain != 0 || call.account != testHTTPAPIAccountArray(destination) {
			t.Fatalf("probe %d = %+v, want LT %d for destination", i, call, want)
		}
	}
}

func TestTryLocateTransactionRequiresFullMessageIdentity(t *testing.T) {
	expectedSource := bytes.Repeat([]byte{0x10}, 32)
	expectedDestination := bytes.Repeat([]byte{0x55}, 32)
	createdLT := uint64(54)

	t.Run("inbound source", func(t *testing.T) {
		wrongSource := bytes.Repeat([]byte{0x99}, 32)
		txCell := testHTTPAPIMessageTransaction(t, expectedDestination, 80, 0, make([]byte, 32),
			testHTTPAPIInternalMessage(wrongSource, expectedDestination, createdLT), nil)
		root := testHTTPAPIBlockWithTransaction(t, expectedDestination, 80, txCell)
		block := testHTTPAPIBlockIDForRoot(1, root)
		lookup := testHTTPAPIAccountLTLookup(0, expectedDestination, createdLT)
		store := newMessageLookupTestStore(map[accountLTLookup]ton.BlockIDExt{lookup: block},
			[]blockRoot{{block: block, root: root}})
		srv := newTestServer()
		srv.store = store

		_, apiErr := srv.handleTryLocateInboundMessageTransaction(context.Background(), requestParams{query: mapValues(map[string]string{
			"source":      address.NewAddress(0, 0, expectedSource).String(),
			"destination": address.NewAddress(0, 0, expectedDestination).String(),
			"created_lt":  fmt.Sprintf("%d", createdLT),
		})})
		if apiErr == nil || apiErr.status != 404 {
			t.Fatalf("wrong inbound source error = %+v, want 404", apiErr)
		}
	})

	t.Run("outbound destination", func(t *testing.T) {
		wrongDestination := bytes.Repeat([]byte{0x88}, 32)
		transactionLT := uint64(55)
		outboundCreatedLT := transactionLT + 1
		txCell := testHTTPAPIMessageTransaction(t, expectedSource, transactionLT, 0, make([]byte, 32), nil,
			[]*tlb.Message{testHTTPAPIInternalMessage(expectedSource, wrongDestination, outboundCreatedLT)})
		root := testHTTPAPIBlockWithTransaction(t, expectedSource, transactionLT, txCell)
		block := testHTTPAPIBlockIDForRoot(1, root)
		lookup := testHTTPAPIAccountLTLookup(0, expectedSource, outboundCreatedLT)
		store := newMessageLookupTestStore(map[accountLTLookup]ton.BlockIDExt{lookup: block},
			[]blockRoot{{block: block, root: root}})
		srv := newTestServer()
		srv.store = store

		_, apiErr := srv.handleTryLocateSourceTx(context.Background(), requestParams{query: mapValues(map[string]string{
			"source":      address.NewAddress(0, 0, expectedSource).String(),
			"destination": address.NewAddress(0, 0, expectedDestination).String(),
			"created_lt":  fmt.Sprintf("%d", outboundCreatedLT),
		})})
		if apiErr == nil || apiErr.status != 404 {
			t.Fatalf("wrong outbound destination error = %+v, want 404", apiErr)
		}
	})
}

func TestTransactionMessageHashesUseOriginalRefStoredCells(t *testing.T) {
	account := bytes.Repeat([]byte{0x55}, 32)
	source := bytes.Repeat([]byte{0x10}, 32)
	destination := bytes.Repeat([]byte{0x20}, 32)
	txCell, inCell, outCell := testHTTPAPITransactionWithRefStoredMessages(t, account, source, destination)

	tx, apiErr := parseAccountTransaction(txCell)
	if apiErr != nil {
		t.Fatalf("parse transaction: %s", apiErr.message)
	}
	normalizedIn, err := tx.IO.In.ToCell()
	if err != nil {
		t.Fatalf("normalize inbound message: %v", err)
	}
	outMessages, err := tx.IO.Out.ToSlice()
	if err != nil {
		t.Fatalf("parse outbound messages: %v", err)
	}
	normalizedOut, err := outMessages[0].ToCell()
	if err != nil {
		t.Fatalf("normalize outbound message: %v", err)
	}
	if bytes.Equal(normalizedIn.Hash(), inCell.Hash()) || bytes.Equal(normalizedOut.Hash(), outCell.Hash()) {
		t.Fatal("fixture message layout was preserved by reserialization")
	}

	raw, err := rawTransactionFromTLB(0, tx, txCell)
	if err != nil {
		t.Fatalf("format raw transaction: %v", err)
	}
	extended, err := extTransactionFromTLB(rawTransactionExtType, 0, tx, txCell)
	if err != nil {
		t.Fatalf("format extended transaction: %v", err)
	}

	wantIn := tonHash(inCell.Hash())
	wantOut := tonHash(outCell.Hash())
	if raw.InMsg == nil || raw.InMsg.Hash != wantIn || len(raw.OutMsgs) != 1 || raw.OutMsgs[0].Hash != wantOut {
		t.Fatalf("raw message hashes = in:%+v out:%+v, want %s/%s", raw.InMsg, raw.OutMsgs, wantIn, wantOut)
	}
	if extended.InMsg == nil || extended.InMsg.Hash != wantIn || len(extended.OutMsgs) != 1 || extended.OutMsgs[0].Hash != wantOut {
		t.Fatalf("extended message hashes = in:%+v out:%+v, want %s/%s", extended.InMsg, extended.OutMsgs, wantIn, wantOut)
	}
}

type blockRoot struct {
	block ton.BlockIDExt
	root  *cell.Cell
}

type locateTestStore struct {
	testStore
	root blockRoot
}

func (s *locateTestStore) BlockRoot(_ context.Context, block ton.BlockIDExt) (*cell.Cell, error) {
	if !block.Equals(&s.root.block) {
		return nil, storage.ErrNotFound
	}
	return s.root.root, nil
}

type accountLTLookup struct {
	workchain int32
	account   [32]byte
	lt        uint64
}

type messageLookupTestStore struct {
	testStore
	blocks      map[accountLTLookup]ton.BlockIDExt
	roots       map[storage.BlockRootHash]*cell.Cell
	lookupCalls []accountLTLookup
	rootCalls   map[storage.BlockRootHash]int
}

func newMessageLookupTestStore(blocks map[accountLTLookup]ton.BlockIDExt, roots []blockRoot) *messageLookupTestStore {
	store := &messageLookupTestStore{
		blocks:    blocks,
		roots:     make(map[storage.BlockRootHash]*cell.Cell, len(roots)),
		rootCalls: make(map[storage.BlockRootHash]int, len(roots)),
	}
	for _, entry := range roots {
		store.roots[storage.BlockKey(entry.block)] = entry.root
	}
	return store
}

func (s *messageLookupTestStore) BlockRoot(_ context.Context, block ton.BlockIDExt) (*cell.Cell, error) {
	key := storage.BlockKey(block)
	root, ok := s.roots[key]
	if !ok {
		return nil, storage.ErrNotFound
	}
	s.rootCalls[key]++
	return root, nil
}

func (s *messageLookupTestStore) LookupBlockByAccountLT(_ context.Context, workchain int32, account []byte, lt uint64) (ton.BlockIDExt, error) {
	lookup := testHTTPAPIAccountLTLookup(workchain, account, lt)
	s.lookupCalls = append(s.lookupCalls, lookup)
	block, ok := s.blocks[lookup]
	if !ok {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	return block, nil
}

func testHTTPAPIAccountLTLookup(workchain int32, account []byte, lt uint64) accountLTLookup {
	return accountLTLookup{workchain: workchain, account: testHTTPAPIAccountArray(account), lt: lt}
}

func testHTTPAPIAccountArray(account []byte) [32]byte {
	var value [32]byte
	copy(value[:], account)
	return value
}

func testHTTPAPIInternalMessage(source, destination []byte, createdLT uint64) *tlb.Message {
	return &tlb.Message{Msg: &tlb.InternalMessage{
		IHRDisabled: true,
		Bounce:      true,
		SrcAddr:     address.NewAddress(0, 0, source),
		DstAddr:     address.NewAddress(0, 0, destination),
		Amount:      tlb.ZeroCoins,
		CreatedLT:   createdLT,
		Body:        cell.BeginCell().EndCell(),
	}}
}

func testHTTPAPIMessageTransaction(t *testing.T, account []byte, lt, prevLT uint64, prevHash []byte, inbound *tlb.Message, outputs []*tlb.Message) *cell.Cell {
	t.Helper()

	var outList *tlb.MessagesList
	if len(outputs) > 0 {
		outDict := cell.NewDict(15)
		for i, output := range outputs {
			messageCell, err := output.ToCell()
			if err != nil {
				t.Fatalf("build outgoing message %d: %v", i, err)
			}
			if err = outDict.Set(cell.BeginCell().MustStoreUInt(uint64(i), 15).EndCell(),
				cell.BeginCell().MustStoreRef(messageCell).EndCell()); err != nil {
				t.Fatalf("set outgoing message %d: %v", i, err)
			}
		}
		outList = &tlb.MessagesList{List: outDict}
	}

	tx := tlb.Transaction{
		AccountAddr: bytes.Clone(account),
		LT:          lt,
		PrevTxHash:  bytes.Clone(prevHash),
		PrevTxLT:    prevLT,
		Now:         1700000000,
		OutMsgCount: uint16(len(outputs)),
		OrigStatus:  tlb.AccountStatusActive,
		EndStatus:   tlb.AccountStatusActive,
		TotalFees:   tlb.CurrencyCollection{Coins: tlb.ZeroCoins},
		StateUpdate: tlb.HashUpdate{
			OldHash: bytes.Repeat([]byte{0x02}, 32),
			NewHash: bytes.Repeat([]byte{0x03}, 32),
		},
		Description: tlb.TransactionDescriptionOrdinary{
			ComputePhase: tlb.ComputePhase{Phase: tlb.ComputePhaseSkipped{Reason: tlb.ComputeSkipReason{Type: tlb.ComputeSkipReasonNoState}}},
			Aborted:      true,
		},
	}
	tx.IO.In = inbound
	tx.IO.Out = outList
	txCell, err := tlb.ToCell(&tx)
	if err != nil {
		t.Fatalf("build transaction: %v", err)
	}
	return txCell
}

func testHTTPAPITransactionWithRefStoredMessages(t *testing.T, account, source, destination []byte) (*cell.Cell, *cell.Cell, *cell.Cell) {
	t.Helper()

	inCell := testHTTPAPIRefStoredMessage(t, source, account, 54, 0x11)
	outCell := testHTTPAPIRefStoredMessage(t, account, destination, 56, 0x22)
	var inMessage tlb.Message
	if err := tlb.LoadFromCell(&inMessage, inCell.MustBeginParse()); err != nil {
		t.Fatalf("parse inbound message: %v", err)
	}
	outDict := cell.NewDict(15)
	if err := outDict.Set(
		cell.BeginCell().MustStoreUInt(0, 15).EndCell(),
		cell.BeginCell().MustStoreRef(outCell).EndCell(),
	); err != nil {
		t.Fatalf("set outbound message: %v", err)
	}

	tx := tlb.Transaction{
		AccountAddr: account,
		LT:          55,
		PrevTxHash:  bytes.Repeat([]byte{0x01}, 32),
		PrevTxLT:    54,
		Now:         1700000000,
		OutMsgCount: 1,
		OrigStatus:  tlb.AccountStatusActive,
		EndStatus:   tlb.AccountStatusActive,
		TotalFees:   tlb.CurrencyCollection{Coins: tlb.ZeroCoins},
		StateUpdate: tlb.HashUpdate{
			OldHash: bytes.Repeat([]byte{0x02}, 32),
			NewHash: bytes.Repeat([]byte{0x03}, 32),
		},
		Description: tlb.TransactionDescriptionOrdinary{
			ComputePhase: tlb.ComputePhase{Phase: tlb.ComputePhaseSkipped{Reason: tlb.ComputeSkipReason{Type: tlb.ComputeSkipReasonNoState}}},
			Aborted:      true,
		},
	}
	tx.IO.In = &inMessage
	tx.IO.Out = &tlb.MessagesList{List: outDict}
	normalized, err := tlb.ToCell(&tx)
	if err != nil {
		t.Fatalf("build transaction: %v", err)
	}
	originalIO := cell.BeginCell().
		MustStoreBoolBit(true).
		MustStoreRef(inCell).
		MustStoreDict(outDict).
		EndCell()
	return testHTTPAPIReplaceTransactionIO(t, normalized, originalIO), inCell, outCell
}

func testHTTPAPIRefStoredMessage(t *testing.T, source, destination []byte, createdLT uint64, marker uint64) *cell.Cell {
	t.Helper()

	stateInit, err := tlb.ToCell(&tlb.StateInit{})
	if err != nil {
		t.Fatalf("build state init: %v", err)
	}
	body := cell.BeginCell().MustStoreUInt(marker, 32).EndCell()
	return cell.BeginCell().
		MustStoreBoolBit(false).
		MustStoreBoolBit(true).
		MustStoreBoolBit(true).
		MustStoreBoolBit(false).
		MustStoreAddr(address.NewAddress(0, 0, source)).
		MustStoreAddr(address.NewAddress(0, 0, destination)).
		MustStoreCoins(0).
		MustStoreDict(nil).
		MustStoreCoins(0).
		MustStoreCoins(0).
		MustStoreUInt(createdLT, 64).
		MustStoreUInt(1700000000, 32).
		MustStoreBoolBit(true).
		MustStoreBoolBit(true).
		MustStoreRef(stateInit).
		MustStoreBoolBit(true).
		MustStoreRef(body).
		EndCell()
}

func testHTTPAPIReplaceTransactionIO(t *testing.T, tx, io *cell.Cell) *cell.Cell {
	t.Helper()

	loader := tx.MustBeginParse()
	bits := loader.BitsLeft()
	data, err := loader.LoadSlice(bits)
	if err != nil {
		t.Fatalf("load transaction bits: %v", err)
	}
	refs := make([]*cell.Cell, loader.RefsNum())
	for i := range refs {
		refs[i], err = loader.LoadRefCell()
		if err != nil {
			t.Fatalf("load transaction ref %d: %v", i, err)
		}
	}
	if len(refs) != 3 {
		t.Fatalf("transaction refs = %d, want 3", len(refs))
	}
	refs[0] = io

	builder := cell.BeginCell().MustStoreSlice(data, bits)
	for _, ref := range refs {
		builder.MustStoreRef(ref)
	}
	return builder.EndCell()
}

func testHTTPAPIBlockWithTransaction(t *testing.T, account []byte, lt uint64, tx *cell.Cell) *cell.Cell {
	t.Helper()

	return testHTTPAPIBlockWithAccountTxs(t, []testHTTPAPIAccountTxs{
		{account: account, txs: map[uint64]*cell.Cell{lt: tx}},
	})
}

type testHTTPAPIAccountTxs struct {
	account []byte
	txs     map[uint64]*cell.Cell
}

func testHTTPAPIBlockWithAccountTxs(t testing.TB, accounts []testHTTPAPIAccountTxs) *cell.Cell {
	t.Helper()

	endLT := uint64(100)
	accountBlocks, err := cell.NewAugDict(256, testHTTPAPICurrencyCollectionAugmentation{})
	if err != nil {
		t.Fatalf("create account blocks dict: %v", err)
	}
	for _, entry := range accounts {
		txDict, err := cell.NewAugDict(64, testHTTPAPICurrencyCollectionAugmentation{})
		if err != nil {
			t.Fatalf("create transaction dict: %v", err)
		}
		for lt, tx := range entry.txs {
			if lt >= endLT {
				endLT = lt + 1
			}
			if err = txDict.Set(cell.BeginCell().MustStoreUInt(lt, 64).EndCell(), cell.BeginCell().MustStoreRef(tx).EndCell()); err != nil {
				t.Fatalf("set transaction: %v", err)
			}
		}

		accountBlock := cell.BeginCell().
			MustStoreUInt(0x5, 4).
			MustStoreSlice(entry.account, 256).
			MustStoreBuilder(testHTTPAPIAugDictRootCell(t, txDict).ToBuilder()).
			MustStoreRef(cell.BeginCell().EndCell()).
			EndCell()

		if err = accountBlocks.Set(testHTTPAPIAccountKey(entry.account), accountBlock); err != nil {
			t.Fatalf("set account block: %v", err)
		}
	}

	shardBlocks, err := tlb.ToCell(&tlb.ShardAccountBlocks{
		Accounts: &tlb.ShardAccountBlocksAugDict{AugmentedDictionary: accountBlocks},
	})
	if err != nil {
		t.Fatalf("build shard account blocks: %v", err)
	}

	var header tlb.BlockHeader
	header.Version = 1
	header.SeqNo = 1
	header.StartLt = 1
	header.EndLt = endLT
	header.GenUtime = 1000
	header.Shard = tlb.ShardIdent{PrefixBits: 0, WorkchainID: 0, ShardPrefix: 1 << 63}
	header.PrevRef = tlb.BlkPrevInfo{Prev1: tlb.ExtBlkRef{
		RootHash: bytes.Repeat([]byte{0x03}, 32),
		FileHash: bytes.Repeat([]byte{0x04}, 32),
	}}

	root, err := tlb.ToCell(&tlb.Block{
		GlobalID:    -239,
		BlockInfo:   header,
		ValueFlow:   cell.BeginCell().EndCell(),
		StateUpdate: cell.BeginCell().EndCell(),
		Extra: &tlb.BlockExtra{
			InMsgDesc:          cell.BeginCell().EndCell(),
			OutMsgDesc:         cell.BeginCell().EndCell(),
			ShardAccountBlocks: shardBlocks,
			RandSeed:           bytes.Repeat([]byte{0x01}, 32),
			CreatedBy:          bytes.Repeat([]byte{0x02}, 32),
		},
	})
	if err != nil {
		t.Fatalf("build block: %v", err)
	}
	return root
}

type testHTTPAPICurrencyCollectionAugmentation struct{}

func (testHTTPAPICurrencyCollectionAugmentation) SkipExtra(loader *cell.Slice) error {
	var extra tlb.CurrencyCollection
	return tlb.LoadFromCell(&extra, loader)
}

func (testHTTPAPICurrencyCollectionAugmentation) EmptyExtra(dst *cell.Builder) error {
	return dst.StoreUInt(0, 5)
}

func (testHTTPAPICurrencyCollectionAugmentation) LeafExtra(_ *cell.Slice, dst *cell.Builder) error {
	return dst.StoreUInt(0, 5)
}

func (testHTTPAPICurrencyCollectionAugmentation) CombineExtra(_, _ *cell.Slice, dst *cell.Builder) error {
	return dst.StoreUInt(0, 5)
}

func testHTTPAPIAugDictRootCell(t testing.TB, dict *cell.AugmentedDictionary) *cell.Cell {
	t.Helper()

	wrapped, err := dict.ToCell()
	if err != nil {
		t.Fatalf("serialize augmented dictionary: %v", err)
	}
	loader := wrapped.MustBeginParse()
	hasRoot, err := loader.LoadBoolBit()
	if err != nil {
		t.Fatalf("load augmented dictionary wrapper: %v", err)
	}
	if !hasRoot {
		t.Fatal("augmented dictionary wrapper has no root")
	}
	root, err := loader.LoadRefCell()
	if err != nil {
		t.Fatalf("load augmented dictionary root: %v", err)
	}
	return root
}

func BenchmarkWalkBlockTransactions(b *testing.B) {
	const (
		accountCount        = 16
		transactionsPerAcct = 32
	)

	txCell := cell.BeginCell().EndCell()
	accountTxs := make([]testHTTPAPIAccountTxs, accountCount)
	for accountIndex := range accountTxs {
		account := make([]byte, 32)
		account[31] = byte(accountIndex + 1)
		txs := make(map[uint64]*cell.Cell, transactionsPerAcct)
		for txIndex := range transactionsPerAcct {
			txs[uint64(txIndex+1)] = txCell
		}
		accountTxs[accountIndex] = testHTTPAPIAccountTxs{account: account, txs: txs}
	}
	root := testHTTPAPIBlockWithAccountTxs(b, accountTxs)
	accounts, err := blockAccountBlocks(root)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		count := 0
		if err := walkBlockTransactions(accounts, func(blockTxEntry) (bool, error) {
			count++
			return true, nil
		}); err != nil {
			b.Fatal(err)
		}
		if count != accountCount*transactionsPerAcct {
			b.Fatalf("transaction count = %d", count)
		}
	}
}

func BenchmarkFindBlockTransactionBefore(b *testing.B) {
	for _, foreignAccounts := range []int{1, 64, 256} {
		b.Run(fmt.Sprintf("foreign_accounts_%d", foreignAccounts), func(b *testing.B) {
			target := bytes.Repeat([]byte{0xff}, 32)
			accountTxs := make([]testHTTPAPIAccountTxs, 0, foreignAccounts+1)
			for i := 0; i < foreignAccounts; i++ {
				account := bytes.Repeat([]byte{0xff}, 32)
				account[30] = byte(i >> 8)
				account[31] = byte(i)
				accountTxs = append(accountTxs, testHTTPAPIAccountTxs{
					account: account,
					txs:     map[uint64]*cell.Cell{1: cell.BeginCell().EndCell()},
				})
			}
			wantCell := cell.BeginCell().MustStoreUInt(0x7, 4).EndCell()
			accountTxs = append(accountTxs, testHTTPAPIAccountTxs{
				account: target,
				txs:     map[uint64]*cell.Cell{55: wantCell},
			})
			root := testHTTPAPIBlockWithAccountTxs(b, accountTxs)
			accounts, err := blockAccountBlocks(root)
			if err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			for b.Loop() {
				entry, err := findBlockTransactionBefore(accounts, target, 56)
				if err != nil {
					b.Fatal(err)
				}
				if entry.lt != 55 || entry.cell != wantCell {
					b.Fatalf("unexpected transaction: LT %d", entry.lt)
				}
			}
		})
	}
}

func testHTTPAPIAccountKey(account []byte) *cell.Cell {
	return cell.BeginCell().MustStoreSlice(account, 256).EndCell()
}

func mapValues(values map[string]string) url.Values {
	out := make(url.Values, len(values))
	for key, value := range values {
		out.Set(key, value)
	}
	return out
}

func testHTTPAPIBlockIDForRoot(seqno uint32, root *cell.Cell) ton.BlockIDExt {
	data := root.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false})
	fileHash := sha256.Sum256(data)
	rootHash := root.HashKey(0)
	return ton.BlockIDExt{
		Workchain: 0,
		Shard:     masterchainShard,
		SeqNo:     seqno,
		RootHash:  append([]byte(nil), rootHash[:]...),
		FileHash:  append([]byte(nil), fileHash[:]...),
	}
}

func testHTTPAPIChainTransaction(t *testing.T, account []byte, lt uint64, prevLT uint64, prevHash []byte) *cell.Cell {
	t.Helper()

	inMsg := &tlb.Message{Msg: &tlb.InternalMessage{
		IHRDisabled: true,
		Bounce:      true,
		SrcAddr:     address.NewAddress(0, 0, bytes.Repeat([]byte{0x10}, 32)),
		DstAddr:     address.NewAddress(0, 0, account),
		Amount:      tlb.ZeroCoins,
		CreatedLT:   lt - 1,
		Body:        cell.BeginCell().MustStoreUInt(lt, 64).EndCell(),
	}}

	txCell, err := tlb.ToCell(&tlb.Transaction{
		AccountAddr: bytes.Clone(account),
		LT:          lt,
		PrevTxHash:  bytes.Clone(prevHash),
		PrevTxLT:    prevLT,
		Now:         1700000000,
		OrigStatus:  tlb.AccountStatusActive,
		EndStatus:   tlb.AccountStatusActive,
		IO: struct {
			In  *tlb.Message      `tlb:"maybe ^"`
			Out *tlb.MessagesList `tlb:"maybe ^"`
		}{In: inMsg},
		TotalFees: tlb.CurrencyCollection{Coins: tlb.ZeroCoins},
		StateUpdate: tlb.HashUpdate{
			OldHash: bytes.Repeat([]byte{0x02}, 32),
			NewHash: bytes.Repeat([]byte{0x03}, 32),
		},
		Description: tlb.TransactionDescriptionOrdinary{
			ComputePhase: tlb.ComputePhase{Phase: tlb.ComputePhaseSkipped{Reason: tlb.ComputeSkipReason{Type: tlb.ComputeSkipReasonNoState}}},
			Aborted:      true,
		},
	})
	if err != nil {
		t.Fatalf("build transaction: %v", err)
	}
	return txCell
}

// testHTTPAPIListTransactions decodes every transaction of the block the way
// the legacy handlers did, providing the reference order and content.
func testHTTPAPIListTransactions(t *testing.T, root *cell.Cell) []*tlb.Transaction {
	t.Helper()

	var block tlb.Block
	if err := tlb.Parse(&block, root); err != nil {
		t.Fatalf("parse block: %v", err)
	}
	txs, err := block.ListTransactions()
	if err != nil {
		t.Fatalf("list block transactions: %v", err)
	}
	return txs
}

func testHTTPAPIBlockParams(id ton.BlockIDExt, extra map[string]string) requestParams {
	values := map[string]string{
		"workchain": fmt.Sprintf("%d", id.Workchain),
		"shard":     fmt.Sprintf("%d", id.Shard),
		"seqno":     fmt.Sprintf("%d", id.SeqNo),
		"root_hash": hex.EncodeToString(id.RootHash),
		"file_hash": hex.EncodeToString(id.FileHash),
	}
	for key, value := range extra {
		values[key] = value
	}
	return requestParams{query: mapValues(values)}
}

func TestBlockTransactionsCursorMatchesListTransactions(t *testing.T) {
	acc1 := bytes.Repeat([]byte{0x11}, 32)
	acc2 := bytes.Repeat([]byte{0x22}, 32)
	acc3 := bytes.Repeat([]byte{0x33}, 32)
	prev := bytes.Repeat([]byte{0x01}, 32)

	root := testHTTPAPIBlockWithAccountTxs(t, []testHTTPAPIAccountTxs{
		{account: acc1, txs: map[uint64]*cell.Cell{
			60: testHTTPAPIChainTransaction(t, acc1, 60, 20, prev),
			20: testHTTPAPIChainTransaction(t, acc1, 20, 0, make([]byte, 32)),
		}},
		{account: acc2, txs: map[uint64]*cell.Cell{
			40: testHTTPAPIChainTransaction(t, acc2, 40, 0, make([]byte, 32)),
		}},
		{account: acc3, txs: map[uint64]*cell.Cell{
			10: testHTTPAPIChainTransaction(t, acc3, 10, 0, make([]byte, 32)),
			70: testHTTPAPIChainTransaction(t, acc3, 70, 30, prev),
			30: testHTTPAPIChainTransaction(t, acc3, 30, 10, prev),
		}},
	})
	block := testHTTPAPIBlockIDForRoot(1, root)

	reference := testHTTPAPIListTransactions(t, root)
	if len(reference) != 6 {
		t.Fatalf("reference transactions = %d, want 6", len(reference))
	}
	expected := make([]shortTxID, 0, len(reference))
	for _, tx := range reference {
		expected = append(expected, shortTxID{
			Type:    shortTxIDType,
			Mode:    shortTxIDMode,
			Account: fmt.Sprintf("%d:%x", block.Workchain, tx.AccountAddr),
			LT:      fmt.Sprintf("%d", tx.LT),
			Hash:    tonHash(tx.Hash),
		})
	}

	srv := newTestServer()
	srv.store = &locateTestStore{root: blockRoot{block: block, root: root}}

	t.Run("full listing without cursor", func(t *testing.T) {
		result, apiErr := srv.handleBlockTransactions(context.Background(), testHTTPAPIBlockParams(block, nil))
		if apiErr != nil {
			t.Fatalf("getBlockTransactions error: %v", apiErr.message)
		}
		listing := result.(blockTransactions)
		if listing.ReqCount != defaultTxCount || listing.Incomplete {
			t.Fatalf("unexpected listing envelope: %+v", listing)
		}
		if !reflect.DeepEqual(listing.Transactions, expected) {
			t.Fatalf("transactions mismatch:\n got %+v\nwant %+v", listing.Transactions, expected)
		}
	})

	t.Run("paginates with after cursor", func(t *testing.T) {
		collected := make([]shortTxID, 0, len(expected))
		params := map[string]string{"count": "2"}
		for page := 0; ; page++ {
			result, apiErr := srv.handleBlockTransactions(context.Background(), testHTTPAPIBlockParams(block, params))
			if apiErr != nil {
				t.Fatalf("page %d error: %v", page, apiErr.message)
			}
			listing := result.(blockTransactions)
			if listing.ReqCount != 2 {
				t.Fatalf("page %d req_count = %d, want 2", page, listing.ReqCount)
			}
			if len(listing.Transactions) > 2 {
				t.Fatalf("page %d size = %d, want <= 2", page, len(listing.Transactions))
			}
			collected = append(collected, listing.Transactions...)
			if !listing.Incomplete {
				break
			}
			last := listing.Transactions[len(listing.Transactions)-1]
			params = map[string]string{"count": "2", "after_lt": last.LT, "after_hash": last.Hash}
		}
		if !reflect.DeepEqual(collected, expected) {
			t.Fatalf("paginated transactions mismatch:\n got %+v\nwant %+v", collected, expected)
		}
	})

	t.Run("resumes by lt only", func(t *testing.T) {
		result, apiErr := srv.handleBlockTransactions(context.Background(), testHTTPAPIBlockParams(block, map[string]string{"after_lt": expected[2].LT}))
		if apiErr != nil {
			t.Fatalf("getBlockTransactions error: %v", apiErr.message)
		}
		listing := result.(blockTransactions)
		if listing.Incomplete {
			t.Fatal("unexpected incomplete listing")
		}
		if !reflect.DeepEqual(listing.Transactions, expected[3:]) {
			t.Fatalf("transactions mismatch:\n got %+v\nwant %+v", listing.Transactions, expected[3:])
		}
	})

	t.Run("resumes by hash only", func(t *testing.T) {
		result, apiErr := srv.handleBlockTransactions(context.Background(), testHTTPAPIBlockParams(block, map[string]string{"after_hash": expected[1].Hash, "count": "1"}))
		if apiErr != nil {
			t.Fatalf("getBlockTransactions error: %v", apiErr.message)
		}
		listing := result.(blockTransactions)
		if !listing.Incomplete {
			t.Fatal("expected incomplete listing")
		}
		if !reflect.DeepEqual(listing.Transactions, expected[2:3]) {
			t.Fatalf("transactions mismatch:\n got %+v\nwant %+v", listing.Transactions, expected[2:3])
		}
	})

	t.Run("unknown after cursor", func(t *testing.T) {
		_, apiErr := srv.handleBlockTransactions(context.Background(), testHTTPAPIBlockParams(block, map[string]string{"after_lt": "999"}))
		if apiErr == nil {
			t.Fatal("expected error for unknown after cursor")
		}
		if apiErr.message != "failed to parse request: after transaction was not found" {
			t.Fatalf("unexpected error: %v", apiErr.message)
		}
	})

	t.Run("ext form matches legacy formatting", func(t *testing.T) {
		expectedExt := make([]extTransaction, 0, len(reference))
		for _, tx := range reference {
			txCell, err := tx.ToCell()
			if err != nil {
				t.Fatalf("serialize reference transaction: %v", err)
			}
			formatted, err := extTransactionFromTLB(rawTransactionExtType, block.Workchain, tx, txCell)
			if err != nil {
				t.Fatalf("format reference transaction: %v", err)
			}
			expectedExt = append(expectedExt, formatted)
		}

		result, apiErr := srv.handleBlockTransactionsExt(context.Background(), testHTTPAPIBlockParams(block, map[string]string{"after_lt": expected[0].LT, "after_hash": expected[0].Hash}))
		if apiErr != nil {
			t.Fatalf("getBlockTransactionsExt error: %v", apiErr.message)
		}
		listing := result.(blockTransactionsExt)
		if listing.Incomplete {
			t.Fatal("unexpected incomplete listing")
		}
		if !reflect.DeepEqual(listing.Transactions, expectedExt[1:]) {
			t.Fatalf("ext transactions mismatch:\n got %+v\nwant %+v", listing.Transactions, expectedExt[1:])
		}
	})
}

type historyTestStore struct {
	testStore
	roots       map[uint32]blockRoot
	blockByLT   map[uint64]uint32
	rootCalls   map[uint32]int
	lookupCalls []uint64
}

func (s *historyTestStore) BlockRoot(_ context.Context, block ton.BlockIDExt) (*cell.Cell, error) {
	entry, ok := s.roots[block.SeqNo]
	if !ok || !block.Equals(&entry.block) {
		return nil, storage.ErrNotFound
	}
	s.rootCalls[block.SeqNo]++
	return entry.root, nil
}

func (s *historyTestStore) LookupBlockByAccountLT(_ context.Context, _ int32, _ []byte, lt uint64) (ton.BlockIDExt, error) {
	s.lookupCalls = append(s.lookupCalls, lt)
	seqno, ok := s.blockByLT[lt]
	if !ok {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	return s.roots[seqno].block, nil
}

func TestTransactionsHistoryWalkReusesCachedBlock(t *testing.T) {
	account := bytes.Repeat([]byte{0x55}, 32)

	tx50 := testHTTPAPIChainTransaction(t, account, 50, 0, make([]byte, 32))
	tx90 := testHTTPAPIChainTransaction(t, account, 90, 50, tx50.Hash())
	tx100 := testHTTPAPIChainTransaction(t, account, 100, 90, tx90.Hash())

	oldRoot := testHTTPAPIBlockWithAccountTxs(t, []testHTTPAPIAccountTxs{
		{account: account, txs: map[uint64]*cell.Cell{50: tx50}},
	})
	newRoot := testHTTPAPIBlockWithAccountTxs(t, []testHTTPAPIAccountTxs{
		{account: account, txs: map[uint64]*cell.Cell{90: tx90, 100: tx100}},
	})
	oldBlock := testHTTPAPIBlockIDForRoot(1, oldRoot)
	newBlock := testHTTPAPIBlockIDForRoot(2, newRoot)

	store := &historyTestStore{
		roots:     map[uint32]blockRoot{1: {block: oldBlock, root: oldRoot}, 2: {block: newBlock, root: newRoot}},
		blockByLT: map[uint64]uint32{100: 2, 90: 2, 50: 1},
		rootCalls: map[uint32]int{},
	}
	srv := newTestServer()
	srv.store = store

	referenceByLT := make(map[uint64]*tlb.Transaction)
	for _, root := range []*cell.Cell{oldRoot, newRoot} {
		for _, tx := range testHTTPAPIListTransactions(t, root) {
			referenceByLT[tx.LT] = tx
		}
	}

	params := requestParams{query: mapValues(map[string]string{
		"address": address.NewAddress(0, 0, account).String(),
		"lt":      "100",
		"hash":    hex.EncodeToString(tx100.Hash()),
	})}

	t.Run("std form", func(t *testing.T) {
		expected := make([]rawTransaction, 0, 3)
		for _, lt := range []uint64{100, 90, 50} {
			tx := referenceByLT[lt]
			txCell, err := tx.ToCell()
			if err != nil {
				t.Fatalf("serialize reference transaction: %v", err)
			}
			formatted, err := rawTransactionFromTLB(0, tx, txCell)
			if err != nil {
				t.Fatalf("format reference transaction: %v", err)
			}
			expected = append(expected, formatted)
		}

		result, apiErr := srv.handleTransactionsStd(context.Background(), params)
		if apiErr != nil {
			t.Fatalf("getTransactionsStd error: %v", apiErr.message)
		}
		history := result.(rawTransactions)
		if !reflect.DeepEqual(history.Transactions, expected) {
			t.Fatalf("transactions mismatch:\n got %+v\nwant %+v", history.Transactions, expected)
		}
		want := internalTransactionID{Type: internalTransactionIDType, LT: "0", Hash: tonHash(make([]byte, 32))}
		if history.PreviousTransactionID != want {
			t.Fatalf("previous transaction id = %+v, want %+v", history.PreviousTransactionID, want)
		}

		// lt=90 must be served from the cached block: no account lookup, no reload.
		if !reflect.DeepEqual(store.lookupCalls, []uint64{100, 50}) {
			t.Fatalf("lookup calls = %v, want [100 50]", store.lookupCalls)
		}
		if store.rootCalls[2] != 1 || store.rootCalls[1] != 1 {
			t.Fatalf("block root loads = %v, want one per block", store.rootCalls)
		}
	})

	t.Run("ext form", func(t *testing.T) {
		store.lookupCalls = nil
		store.rootCalls = map[uint32]int{}

		expected := make([]extTransaction, 0, 3)
		for _, lt := range []uint64{100, 90, 50} {
			tx := referenceByLT[lt]
			txCell, err := tx.ToCell()
			if err != nil {
				t.Fatalf("serialize reference transaction: %v", err)
			}
			formatted, err := extTransactionFromTLB(extTransactionType, 0, tx, txCell)
			if err != nil {
				t.Fatalf("format reference transaction: %v", err)
			}
			expected = append(expected, formatted)
		}

		result, apiErr := srv.handleTransactions(context.Background(), params)
		if apiErr != nil {
			t.Fatalf("getTransactions error: %v", apiErr.message)
		}
		history := result.([]extTransaction)
		if !reflect.DeepEqual(history, expected) {
			t.Fatalf("transactions mismatch:\n got %+v\nwant %+v", history, expected)
		}
		if !reflect.DeepEqual(store.lookupCalls, []uint64{100, 50}) {
			t.Fatalf("lookup calls = %v, want [100 50]", store.lookupCalls)
		}
		if store.rootCalls[2] != 1 || store.rootCalls[1] != 1 {
			t.Fatalf("block root loads = %v, want one per block", store.rootCalls)
		}
	})

	t.Run("hash mismatch", func(t *testing.T) {
		_, apiErr := srv.handleTransactionsStd(context.Background(), requestParams{query: mapValues(map[string]string{
			"address": address.NewAddress(0, 0, account).String(),
			"lt":      "100",
			"hash":    hex.EncodeToString(tx90.Hash()),
		})})
		if apiErr == nil || apiErr.message != "failed to parse request: transaction hash mismatch" {
			t.Fatalf("unexpected error: %+v", apiErr)
		}
	})
}
