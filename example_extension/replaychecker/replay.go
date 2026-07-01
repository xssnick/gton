package replaychecker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"github.com/xssnick/tonutils-go/tvm/tuple"
	vmcore "github.com/xssnick/tonutils-go/tvm/vm"
)

const (
	masterchainID    int32 = -1
	masterchainShard int64 = -1 << 63
)

var ErrMismatch = errors.New("tvm replay mismatch")

type ValidatorOptions struct {
	Logger zerolog.Logger
	Store  hooks.Store
}

type Validator struct {
	mu    sync.Mutex
	inner *replayValidator
	store hooks.Store
}

type Result struct {
	Accounts         int
	Transactions     int
	EmulationElapsed time.Duration
}

func NewValidator(opts ValidatorOptions) *Validator {
	return &Validator{
		store: opts.Store,
		inner: &replayValidator{
			cache:         newExecutionConfigCache(opts.Logger.With().Str("component", "replaychecker-cache").Logger()),
			tvm:           tvm.NewTVM(),
			log:           opts.Logger,
			mismatch:      map[string]accountMismatch{},
			mismatchLimit: -1,
		},
	}
}

func (v *Validator) ValidateAppliedBlock(ctx context.Context, event hooks.BlockAppliedEvent) (Result, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	block, err := loadedBlockFromAppliedEvent(event)
	if err != nil {
		return Result{}, err
	}

	accounts, err := accountBlocks(block.Parsed)
	if err != nil {
		return Result{}, fmt.Errorf("load account blocks %s: %w", storage.FormatBlockRef(block.ID), err)
	}
	if len(accounts) == 0 {
		return Result{}, nil
	}

	previousViews, expectedView, err := shardStateViewsFromAppliedRoots(block.ID, event.PreviousState, event.CurrentState)
	if err != nil {
		return Result{}, fmt.Errorf("parse applied state views for %s: %w", storage.FormatBlockRef(block.ID), err)
	}

	master, masterState, err := configMasterStateForAppliedBlock(ctx, event, block, v.store)
	if err != nil {
		return Result{}, err
	}
	execCtx, err := v.inner.cache.blockContext(master, masterState, block.Parsed.BlockInfo.GenUtime)
	if err != nil {
		return Result{}, fmt.Errorf("prepare execution context %s: %w", storage.FormatBlockRef(block.ID), err)
	}

	masterSeqno := block.ID.SeqNo
	if block.Parsed.BlockInfo.NotMaster {
		masterSeqno = master.SeqNo
	}
	validation, err := v.inner.validateBlockViews(ctx, masterSeqno, accounts, previousViews, expectedView, block, execCtx)
	result := Result{
		Accounts:         validation.Accounts,
		Transactions:     validation.Transactions,
		EmulationElapsed: validation.EmulationElapsed,
	}
	if err != nil {
		return result, err
	}
	if v.inner.hasAccountMismatch(masterSeqno, block.ID.Workchain, accounts) {
		return result, fmt.Errorf("%w: %s", ErrMismatch, storage.FormatBlockRef(block.ID))
	}
	return result, nil
}

func loadedBlockFromAppliedEvent(event hooks.BlockAppliedEvent) (loadedBlock, error) {
	id := event.Meta.ID

	loader, err := event.BlockRoot.BeginParse()
	if err != nil {
		return loadedBlock{}, fmt.Errorf("parse applied block %s: %w", storage.FormatBlockRef(id), err)
	}
	var parsed tlb.Block
	if err = tlb.LoadFromCell(&parsed, loader); err != nil {
		return loadedBlock{}, fmt.Errorf("load applied block %s: %w", storage.FormatBlockRef(id), err)
	}
	parsedMeta, err := storage.BuildBlockMetaFromParsedBlock(id, &parsed)
	if err != nil {
		return loadedBlock{}, err
	}
	meta := storage.MergeBlockMeta(parsedMeta, event.Meta)
	meta.ID = id

	return loadedBlock{
		ID:     id,
		Parsed: &parsed,
		Meta:   meta,
	}, nil
}

func shardStateViewsFromAppliedRoots(block ton.BlockIDExt, previousRoot *cell.Cell, currentRoot *cell.Cell) ([]*shardStateView, *shardStateView, error) {
	if previousRoot == nil {
		return nil, nil, fmt.Errorf("previous state root is missing")
	}
	if currentRoot == nil {
		return nil, nil, fmt.Errorf("current state root is missing")
	}

	previousViews, err := previousShardStateViewsFromRoot(previousRoot)
	if err != nil {
		return nil, nil, err
	}

	loader, err := currentRoot.BeginParse()
	if err != nil {
		return nil, nil, fmt.Errorf("parse current state root: %w", err)
	}
	var current tlb.ShardStateUnsplit
	if err = tlb.LoadFromCell(&current, loader); err != nil {
		return nil, nil, fmt.Errorf("load current state: %w", err)
	}
	return previousViews, newShardStateViewFromParsed(block, currentRoot, &current), nil
}

func configMasterStateForAppliedBlock(ctx context.Context, event hooks.BlockAppliedEvent, block loadedBlock, store hooks.Store) (ton.BlockIDExt, *cell.Cell, error) {
	ref, err := configMasterRefFromLoadedBlock(block)
	if err != nil {
		return ton.BlockIDExt{}, nil, fmt.Errorf("resolve config master for %s: %w", storage.FormatBlockRef(block.ID), err)
	}

	if block.ID.Workchain == masterchainID && block.ID.Shard == masterchainShard {
		if event.PreviousState == nil {
			return ton.BlockIDExt{}, nil, fmt.Errorf("applied masterchain block %s has no previous state", storage.FormatBlockRef(block.ID))
		}
		return ref, event.PreviousState, nil
	}

	if event.InclusionMasterRef != nil && event.InclusionMasterState != nil && event.InclusionMasterRef.Equals(&ref) {
		return ref, event.InclusionMasterState, nil
	}

	state, err := loadStoreStateRoot(ctx, store, ref)
	if err != nil {
		return ton.BlockIDExt{}, nil, fmt.Errorf("load config master state %s for applied block %s: %w", storage.FormatBlockRef(ref), storage.FormatBlockRef(block.ID), err)
	}
	return ref, state, nil
}

func configMasterRefFromLoadedBlock(block loadedBlock) (ton.BlockIDExt, error) {
	header := block.Parsed.BlockInfo
	if header.NotMaster {
		if header.MasterRef == nil {
			return ton.BlockIDExt{}, fmt.Errorf("shard block has no masterchain ref")
		}
		return extBlockRefToBlockIDExt(masterchainID, masterchainShard, *header.MasterRef), nil
	}

	return extBlockRefToBlockIDExt(masterchainID, masterchainShard, header.PrevRef.Prev1), nil
}

func loadStoreStateRoot(ctx context.Context, store hooks.Store, block ton.BlockIDExt) (*cell.Cell, error) {
	if store == nil {
		return nil, fmt.Errorf("store is not configured")
	}

	state, err := store.BlockState(ctx, block)
	if err != nil {
		return nil, err
	}
	if state.Cell != nil {
		return state.Cell, nil
	}
	if len(state.StateRootHash) != 32 {
		return nil, fmt.Errorf("state root hash has %d bytes", len(state.StateRootHash))
	}
	return store.LoadStateCellTree(ctx, block, state.StateRootHash)
}

func extBlockRefToBlockIDExt(workchain int32, shard int64, ref tlb.ExtBlkRef) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: workchain,
		Shard:     shard,
		SeqNo:     ref.SeqNo,
		RootHash:  bytes.Clone(ref.RootHash),
		FileHash:  bytes.Clone(ref.FileHash),
	}
}

type loadedBlock struct {
	ID     ton.BlockIDExt
	Parsed *tlb.Block
	Meta   *storage.BlockMeta
}

type accountBlockWork struct {
	Account  []byte
	Expected []byte
	Txs      []transactionWork
}

type transactionWork struct {
	Cell      *cell.Cell
	InMsgCell *cell.Cell
	Parsed    *tlb.Transaction
	Hash      cell.Hash
}

var (
	tvmIntMinusOne = big.NewInt(-1)
	tvmIntZero     = big.NewInt(0)

	defaultInMsgParamsTuple = tuple.NewTupleValue(
		tvmIntZero,
		tvmIntZero,
		cell.BeginCell().MustStoreUInt(0, 2).ToSlice(),
		tvmIntZero,
		tvmIntZero,
		tvmIntZero,
		tvmIntZero,
		tvmIntZero,
		nil,
		nil,
	)
	zeroCurrencyTuple = tuple.NewTupleValue(tvmIntZero, nil)
)

type replayValidator struct {
	cache         *executionConfigCache
	tvm           *tvm.TVM
	log           zerolog.Logger
	mismatchLimit int
	vmTraceAddr   string
	vmTraceLT     uint64
	mu            sync.Mutex
	mismatch      map[string]accountMismatch
}

type accountMismatch struct {
	MasterSeqno               uint32             `json:"master_seqno"`
	Workchain                 int32              `json:"workchain"`
	Address                   string             `json:"address"`
	FromShardAccountBOCBase64 string             `json:"from_shard_account_boc_base64,omitempty"`
	ToShardAccountBOCBase64   string             `json:"to_shard_account_boc_base64,omitempty"`
	GotShardAccountBOCBase64  string             `json:"got_shard_account_boc_base64,omitempty"`
	FirstTx                   *accountMismatchTx `json:"first_tx,omitempty"`
}

type accountMismatchTx struct {
	LT                  uint64                  `json:"lt"`
	ExpectedTxHash      string                  `json:"expected_tx_hash,omitempty"`
	GotTxHash           string                  `json:"got_tx_hash,omitempty"`
	ExpectedTxBOCBase64 string                  `json:"expected_tx_boc_base64,omitempty"`
	GotTxBOCBase64      string                  `json:"got_tx_boc_base64,omitempty"`
	InMsgBOCBase64      string                  `json:"in_msg_boc_base64,omitempty"`
	Config              accountMismatchTxConfig `json:"config"`
}

type accountMismatchTxConfig struct {
	GlobalVersion                int      `json:"global_version"`
	Now                          uint32   `json:"now"`
	BlockLT                      int64    `json:"block_lt"`
	LogicalTime                  int64    `json:"logical_time"`
	RandSeedBase64               string   `json:"rand_seed_base64,omitempty"`
	ConfigRootBOCBase64          string   `json:"config_root_boc_base64,omitempty"`
	PrevBlocksStackBOCBase64     string   `json:"prev_blocks_stack_boc_base64,omitempty"`
	UnpackedConfigStackBOCBase64 string   `json:"unpacked_config_stack_boc_base64,omitempty"`
	PrecompiledGasStackBOCBase64 string   `json:"precompiled_gas_stack_boc_base64,omitempty"`
	IncomingValueStackBOCBase64  string   `json:"incoming_value_stack_boc_base64,omitempty"`
	StorageFees                  int64    `json:"storage_fees"`
	DuePaymentNano               string   `json:"due_payment_nano,omitempty"`
	InMsgParamsStackBOCBase64    string   `json:"in_msg_params_stack_boc_base64,omitempty"`
	LibrariesBOCBase64           []string `json:"libraries_boc_base64,omitempty"`
}

type accountMismatchDetails struct {
	fromShardAccountBOCBase64 string
	toShardAccountBOCBase64   string
	gotShardAccountBOCBase64  string
	firstTx                   *accountMismatchTx
}

type blockValidationResult struct {
	Transactions     int
	Accounts         int
	EmulationElapsed time.Duration
}

func (v *replayValidator) validateBlockViews(ctx context.Context, masterSeqno uint32, accounts []accountBlockWork, previousViews []*shardStateView, expectedView *shardStateView, block loadedBlock, execCtx *blockExecutionContext) (blockValidationResult, error) {
	result := blockValidationResult{
		Accounts: len(accounts),
	}
	select {
	case <-ctx.Done():
		return result, ctx.Err()
	default:
	}

	blockLog := v.log.With().
		Uint32("master_seqno", masterSeqno).
		Str("block", storage.FormatBlockRef(block.ID)).
		Logger()
	blockLog.Info().
		Int("accounts", len(accounts)).
		Int("transactions", countTransactions(accounts)).
		Int("global_version", execCtx.master.bundle.globalVersion).
		Msg("replaying block transactions")

	machine, err := v.tvm.WithGlobalVersion(execCtx.master.bundle.globalVersion)
	if err != nil {
		return result, err
	}

	for _, account := range accounts {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		default:
		}

		emulationElapsed, txs, err := v.validateAccountBlock(masterSeqno, previousViews, expectedView, block, execCtx, &machine, account)
		result.EmulationElapsed += emulationElapsed
		result.Transactions += txs
		if err != nil {
			return result, err
		}
	}

	return result, nil
}

func (v *replayValidator) validateAccountBlock(masterSeqno uint32, previous []*shardStateView, expected *shardStateView, block loadedBlock, execCtx *blockExecutionContext, machine *tvm.TVM, account accountBlockWork) (time.Duration, int, error) {
	current, err := accountFromPreviousStates(previous, block.ID.Workchain, account.Account)
	if err != nil {
		return 0, 0, fmt.Errorf("load previous account %s: %w", accountAddressRaw(block.ID.Workchain, account.Account), err)
	}
	current, err = materializeShardAccount(current)
	if err != nil {
		return 0, 0, fmt.Errorf("materialize previous account %s: %w", accountAddressRaw(block.ID.Workchain, account.Account), err)
	}
	initial := current

	addr := accountAddress(block.ID.Workchain, account.Account)
	var details accountMismatchDetails
	addMismatch := func(got *tlb.ShardAccount) {
		v.fillAccountMismatchDetails(&details, expected, account.Account, initial, got)
		v.addMismatchWithDetails(masterSeqno, block.ID.Workchain, account.Account, details)
	}
	setFirstTx := func(before *tlb.ShardAccount, txDetails accountMismatchTx) {
		if !v.collectsMismatches() {
			return
		}
		if details.firstTx == nil {
			if details.fromShardAccountBOCBase64 == "" {
				details.fromShardAccountBOCBase64 = shardAccountBOCBase64(before)
			}
			details.firstTx = &txDetails
		}
	}
	var total time.Duration
	var txCount int
	var accountStorageStat *cell.Cell

	for _, tx := range account.Txs {
		txCount++
		var cfg tvm.TransactionEmulationConfig
		var res *tvm.TransactionExecutionResult
		var elapsed time.Duration
		var err error
		var finishTrace func()

		if tx.Parsed.IO.In == nil || tx.InMsgCell == nil {
			desc, ok := tx.Parsed.Description.(tlb.TransactionDescriptionTickTock)
			if !ok {
				addMismatch(current)
				v.log.Warn().
					Uint32("master_seqno", masterSeqno).
					Str("block", storage.FormatBlockRef(block.ID)).
					Str("address", addr.StringRaw()).
					Uint64("lt", tx.Parsed.LT).
					Str("tx_type", fmt.Sprintf("%T", tx.Parsed.Description)).
					Msg("transaction has no input message; go transaction emulator cannot replay it yet")
				break
			}

			cfg, err = execCtx.tickTockTransactionConfig(block, current, tx.Parsed)
			if err != nil {
				return total, txCount, fmt.Errorf("build tick/tock transaction config %s lt=%d: %w", addr.StringRaw(), tx.Parsed.LT, err)
			}
			cfg.AccountStorageStat = accountStorageStat

			if traceHook, finish := v.startVMTrace(addr.StringRaw(), tx.Parsed.LT); traceHook != nil {
				cfg.TraceHook = traceHook
				finishTrace = finish
			}
			started := time.Now()
			res, err = machine.EmulateTickTockTransaction(current, desc.IsTock, cfg)
			elapsed = time.Since(started)
			total += elapsed
		} else {
			cfg, err = execCtx.transactionConfig(block, current, tx.Parsed)
			if err != nil {
				return total, txCount, fmt.Errorf("build transaction config %s lt=%d: %w", addr.StringRaw(), tx.Parsed.LT, err)
			}
			cfg.AccountStorageStat = accountStorageStat

			if traceHook, finish := v.startVMTrace(addr.StringRaw(), tx.Parsed.LT); traceHook != nil {
				cfg.TraceHook = traceHook
				finishTrace = finish
			}
			started := time.Now()
			res, err = machine.EmulateTransaction(current, tx.InMsgCell, cfg)
			elapsed = time.Since(started)
			total += elapsed
		}
		if finishTrace != nil {
			finishTrace()
		}

		gasUsed := int64(0)
		if res != nil {
			gasUsed = res.GasUsed
		}

		if err != nil {
			txDetails := v.accountMismatchTxDetails(execCtx, tx, tx.InMsgCell, cfg)
			setFirstTx(current, txDetails)
			addMismatch(current)
			v.log.Warn().
				Err(err).
				Uint32("master_seqno", masterSeqno).
				Str("block", storage.FormatBlockRef(block.ID)).
				Str("address", addr.StringRaw()).
				Uint64("lt", tx.Parsed.LT).
				Int64("gas", gasUsed).
				Dur("elapsed", elapsed).
				Msg("transaction emulation failed")
			break
		}
		if res == nil || res.Transaction == nil || res.TransactionCell == nil || res.ShardAccount == nil || res.ShardAccountCell == nil {
			txDetails := v.accountMismatchTxDetails(execCtx, tx, tx.InMsgCell, cfg)
			setFirstTx(current, txDetails)
			addMismatch(current)
			v.log.Warn().
				Uint32("master_seqno", masterSeqno).
				Str("block", storage.FormatBlockRef(block.ID)).
				Str("address", addr.StringRaw()).
				Uint64("lt", tx.Parsed.LT).
				Int64("gas", gasUsed).
				Dur("elapsed", elapsed).
				Msg("transaction emulator returned no transaction")
			break
		}

		gotTxHash := res.TransactionCell.HashKey()
		if gotTxHash != tx.Hash {
			txDetails := v.accountMismatchTxDetails(execCtx, tx, tx.InMsgCell, cfg)
			txDetails.GotTxHash = hex.EncodeToString(gotTxHash[:])
			txDetails.GotTxBOCBase64 = cellBOCBase64(res.TransactionCell)
			setFirstTx(current, txDetails)
			addMismatch(res.ShardAccount)
			v.log.Warn().
				Uint32("master_seqno", masterSeqno).
				Str("block", storage.FormatBlockRef(block.ID)).
				Str("address", addr.StringRaw()).
				Uint64("lt", tx.Parsed.LT).
				Int64("gas", gasUsed).
				Dur("elapsed", elapsed).
				Str("expected_tx_hash", hex.EncodeToString(tx.Hash[:])).
				Str("got_tx_hash", hex.EncodeToString(gotTxHash[:])).
				Msg("transaction hash mismatch")
		} else {
			v.log.Debug().
				Uint32("master_seqno", masterSeqno).
				Str("block", storage.FormatBlockRef(block.ID)).
				Str("address", addr.StringRaw()).
				Uint64("lt", tx.Parsed.LT).
				Int64("gas", gasUsed).
				Dur("elapsed", elapsed).
				Msg("tx emulated")
		}

		current = res.ShardAccount
		accountStorageStat = res.AccountStorageStat
	}

	v.compareAccountResult(masterSeqno, block.ID, expected, account, details, initial, current)
	return total, txCount, nil
}

func (v *replayValidator) compareAccountResult(masterSeqno uint32, block ton.BlockIDExt, expectedView *shardStateView, account accountBlockWork, details accountMismatchDetails, from *tlb.ShardAccount, got *tlb.ShardAccount) {
	addr := accountAddress(block.Workchain, account.Account)
	expectedAccountHash := account.Expected
	var expectedAccountHashKey cell.Hash
	addMismatch := func() {
		v.fillAccountMismatchDetails(&details, expectedView, account.Account, from, got)
		v.addMismatchWithDetails(masterSeqno, block.Workchain, account.Account, details)
	}
	defer func() {
		if !v.accountMismatchExists(masterSeqno, block.Workchain, account.Account) {
			return
		}
		v.fillAccountMismatchDetails(&details, expectedView, account.Account, from, got)
		v.addMismatchDetailsIfExists(masterSeqno, block.Workchain, account.Account, details)
	}()

	expectedShard, err := expectedView.account(account.Account)
	if err == nil {
		expectedAccountHashKey = expectedShard.Account.HashKey()
		expectedAccountHash = expectedAccountHashKey[:]
	} else if !errors.Is(err, storage.ErrNotFound) {
		addMismatch()
		v.log.Warn().
			Err(err).
			Uint32("master_seqno", masterSeqno).
			Str("block", storage.FormatBlockRef(block)).
			Str("address", addr.StringRaw()).
			Msg("failed to load expected account from post-state")
		return
	}

	if !bytes.Equal(expectedAccountHash, account.Expected) {
		addMismatch()
		v.log.Warn().
			Uint32("master_seqno", masterSeqno).
			Str("block", storage.FormatBlockRef(block)).
			Str("address", addr.StringRaw()).
			Str("account_block_expected", hex.EncodeToString(account.Expected)).
			Str("post_state_expected", hex.EncodeToString(expectedAccountHash)).
			Msg("account block state update differs from post-state account root")
	}

	if got == nil || got.Account == nil {
		addMismatch()
		v.log.Warn().
			Uint32("master_seqno", masterSeqno).
			Str("block", storage.FormatBlockRef(block)).
			Str("address", addr.StringRaw()).
			Msg("missing emulated account result")
		return
	}

	gotAccountHash := got.Account.HashKey()
	if !bytes.Equal(gotAccountHash[:], expectedAccountHash) {
		addMismatch()
		v.log.Warn().
			Uint32("master_seqno", masterSeqno).
			Str("block", storage.FormatBlockRef(block)).
			Str("address", addr.StringRaw()).
			Str("expected_account_hash", hex.EncodeToString(expectedAccountHash)).
			Str("got_account_hash", hex.EncodeToString(gotAccountHash[:])).
			Msg("account root hash mismatch")
	}
}

func (v *replayValidator) collectsMismatches() bool {
	return v.mismatchLimit != 0
}

func (v *replayValidator) fillAccountMismatchDetails(details *accountMismatchDetails, expected *shardStateView, account []byte, from *tlb.ShardAccount, got *tlb.ShardAccount) {
	if !v.collectsMismatches() || details == nil {
		return
	}

	if details.fromShardAccountBOCBase64 == "" {
		details.fromShardAccountBOCBase64 = shardAccountBOCBase64(from)
	}
	if details.toShardAccountBOCBase64 == "" {
		details.toShardAccountBOCBase64 = shardAccountBOCBase64(expectedShardAccountForMismatch(expected, account))
	}
	if details.gotShardAccountBOCBase64 == "" {
		details.gotShardAccountBOCBase64 = shardAccountBOCBase64(got)
	}
}

func (v *replayValidator) accountMismatchTxDetails(execCtx *blockExecutionContext, tx transactionWork, msgCell *cell.Cell, cfg tvm.TransactionEmulationConfig) accountMismatchTx {
	if !v.collectsMismatches() {
		return accountMismatchTx{}
	}
	return execCtx.accountMismatchTxDetails(tx, msgCell, cfg)
}

func (v *replayValidator) startVMTrace(addr string, lt uint64) (vmcore.TraceHook, func()) {
	if v.vmTraceAddr == "" || v.vmTraceAddr != addr {
		return nil, nil
	}
	if v.vmTraceLT != 0 && v.vmTraceLT != lt {
		return nil, nil
	}

	lines := make([]string, 0, 256)
	traceHook := func(step vmcore.TraceStep) {
		if len(lines) >= 512 {
			return
		}
		lines = append(lines, step.String())
	}
	finish := func() {
		v.log.Warn().
			Str("address", addr).
			Uint64("lt", lt).
			Str("vm_trace", strings.Join(lines, "\n")).
			Msg("vm trace")
	}
	return traceHook, finish
}

func (v *replayValidator) addMismatchWithDetails(masterSeqno uint32, workchain int32, account []byte, details accountMismatchDetails) {
	if !v.collectsMismatches() {
		return
	}

	key := fmt.Sprintf("%d:%d:%x", masterSeqno, workchain, account)

	v.mu.Lock()
	defer v.mu.Unlock()

	if v.mismatchLimit > 0 {
		if _, exists := v.mismatch[key]; !exists && len(v.mismatch) >= v.mismatchLimit {
			return
		}
	}

	mismatch := accountMismatch{
		MasterSeqno:               masterSeqno,
		Workchain:                 workchain,
		Address:                   accountAddressRaw(workchain, account),
		FromShardAccountBOCBase64: details.fromShardAccountBOCBase64,
		ToShardAccountBOCBase64:   details.toShardAccountBOCBase64,
		GotShardAccountBOCBase64:  details.gotShardAccountBOCBase64,
		FirstTx:                   details.firstTx,
	}

	if existing, ok := v.mismatch[key]; ok {
		mismatch = mergeAccountMismatch(existing, mismatch)
	}
	v.mismatch[key] = mismatch
}

func (v *replayValidator) addMismatchDetailsIfExists(masterSeqno uint32, workchain int32, account []byte, details accountMismatchDetails) {
	if !v.collectsMismatches() {
		return
	}

	key := fmt.Sprintf("%d:%d:%x", masterSeqno, workchain, account)
	update := accountMismatch{
		MasterSeqno:               masterSeqno,
		Workchain:                 workchain,
		Address:                   accountAddressRaw(workchain, account),
		FromShardAccountBOCBase64: details.fromShardAccountBOCBase64,
		ToShardAccountBOCBase64:   details.toShardAccountBOCBase64,
		GotShardAccountBOCBase64:  details.gotShardAccountBOCBase64,
		FirstTx:                   details.firstTx,
	}

	v.mu.Lock()
	if existing, ok := v.mismatch[key]; ok {
		v.mismatch[key] = mergeAccountMismatch(existing, update)
	}
	v.mu.Unlock()
}

func (v *replayValidator) hasAccountMismatch(masterSeqno uint32, workchain int32, accounts []accountBlockWork) bool {
	v.mu.Lock()
	defer v.mu.Unlock()

	for _, account := range accounts {
		key := fmt.Sprintf("%d:%d:%x", masterSeqno, workchain, account.Account)
		if _, ok := v.mismatch[key]; ok {
			return true
		}
	}
	return false
}

func (v *replayValidator) accountMismatchExists(masterSeqno uint32, workchain int32, account []byte) bool {
	key := fmt.Sprintf("%d:%d:%x", masterSeqno, workchain, account)

	v.mu.Lock()
	_, ok := v.mismatch[key]
	v.mu.Unlock()
	return ok
}

func mergeAccountMismatch(dst accountMismatch, src accountMismatch) accountMismatch {
	if dst.FromShardAccountBOCBase64 == "" {
		dst.FromShardAccountBOCBase64 = src.FromShardAccountBOCBase64
	}
	if dst.ToShardAccountBOCBase64 == "" {
		dst.ToShardAccountBOCBase64 = src.ToShardAccountBOCBase64
	}
	if src.GotShardAccountBOCBase64 != "" {
		dst.GotShardAccountBOCBase64 = src.GotShardAccountBOCBase64
	}
	if dst.FirstTx == nil {
		dst.FirstTx = src.FirstTx
	} else if src.FirstTx != nil {
		dst.FirstTx = mergeAccountMismatchTx(dst.FirstTx, src.FirstTx)
	}
	return dst
}

func mergeAccountMismatchTx(dst, src *accountMismatchTx) *accountMismatchTx {
	if dst == nil {
		return src
	}
	if src == nil {
		return dst
	}
	if dst.GotTxHash == "" {
		dst.GotTxHash = src.GotTxHash
	}
	if dst.GotTxBOCBase64 == "" {
		dst.GotTxBOCBase64 = src.GotTxBOCBase64
	}
	return dst
}

func expectedShardAccountForMismatch(expected *shardStateView, account []byte) *tlb.ShardAccount {
	if expected == nil {
		return nil
	}

	shard, err := expected.account(account)
	if err == nil {
		return shard
	}
	if errors.Is(err, storage.ErrNotFound) {
		return emptyShardAccount()
	}
	return nil
}

func shardAccountBOCBase64(shard *tlb.ShardAccount) string {
	if shard == nil {
		return ""
	}

	root, err := tlb.ToCell(shard)
	if err != nil {
		return ""
	}
	return cellBOCBase64(root)
}

func cellBOCBase64(root *cell.Cell) string {
	if root == nil {
		return ""
	}
	boc := root.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false})
	return base64.StdEncoding.EncodeToString(boc)
}

func cellsBOCBase64(cells []*cell.Cell) []string {
	if len(cells) == 0 {
		return nil
	}
	out := make([]string, 0, len(cells))
	for _, root := range cells {
		if encoded := cellBOCBase64(root); encoded != "" {
			out = append(out, encoded)
		}
	}
	return out
}

func stackValueBOCBase64(value any) string {
	serializable, err := stackSerializableValue(value)
	if err != nil {
		return ""
	}
	root := cell.BeginCell()
	if err = tlb.SerializeStackValue(root, serializable); err != nil {
		return ""
	}
	return cellBOCBase64(root.EndCell())
}

func stackSerializableValue(value any) (any, error) {
	switch v := value.(type) {
	case tuple.Tuple:
		out := make([]any, 0, v.Len())
		for i := 0; i < v.Len(); i++ {
			item, err := v.Index(i)
			if err != nil {
				return nil, err
			}
			serializable, err := stackSerializableValue(item)
			if err != nil {
				return nil, err
			}
			out = append(out, serializable)
		}
		return out, nil
	case *big.Int:
		if v == nil {
			return nil, nil
		}
		return new(big.Int).Set(v), nil
	case *cell.Cell:
		if v == nil {
			return nil, nil
		}
		return v, nil
	case *cell.Slice:
		if v == nil {
			return nil, nil
		}
		return v, nil
	case *cell.Builder:
		if v == nil {
			return nil, nil
		}
		return v, nil
	default:
		return value, nil
	}
}

func bigIntString(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case *big.Int:
		if v == nil {
			return ""
		}
		return v.String()
	case big.Int:
		return v.String()
	default:
		return fmt.Sprint(v)
	}
}

func countTransactions(accounts []accountBlockWork) int {
	var count int
	for _, account := range accounts {
		count += len(account.Txs)
	}
	return count
}

func accountBlocks(block *tlb.Block) ([]accountBlockWork, error) {
	if block.Extra == nil || block.Extra.ShardAccountBlocks == nil {
		return nil, fmt.Errorf("block has no shard account blocks")
	}

	accounts, err := block.Extra.ShardAccountBlocks.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("load shard account blocks: %w", err)
	}
	hasAccounts, err := accounts.LoadBoolBit()
	if err != nil {
		return nil, fmt.Errorf("load shard account blocks root flag: %w", err)
	}
	if !hasAccounts {
		return nil, nil
	}

	root, err := accounts.LoadRefCell()
	if err != nil {
		return nil, fmt.Errorf("load shard account blocks root: %w", err)
	}
	accountList, err := root.AsDict(256).LoadAll()
	if err != nil {
		return nil, fmt.Errorf("load account blocks: %w", err)
	}

	out := make([]accountBlockWork, 0, len(accountList))
	for _, accountKV := range accountList {
		if err = skipCurrencyCollectionBoundary(accountKV.Value); err != nil {
			return nil, fmt.Errorf("load account block fees: %w", err)
		}

		var accountBlock tlb.AccountBlock
		if err = tlb.LoadFromCell(&accountBlock, accountKV.Value); err != nil {
			return nil, fmt.Errorf("load account block: %w", err)
		}

		var stateUpdate tlb.HashUpdate
		if accountBlock.StateUpdate != nil {
			if err = tlb.Parse(&stateUpdate, accountBlock.StateUpdate); err != nil {
				return nil, fmt.Errorf("load account state update: %w", err)
			}
		}

		work := accountBlockWork{
			Account:  append([]byte(nil), accountBlock.Addr...),
			Expected: append([]byte(nil), stateUpdate.NewHash...),
		}
		if len(work.Account) != 32 {
			return nil, fmt.Errorf("account block address is %d bits", len(work.Account)*8)
		}
		if accountBlock.Transactions == nil || accountBlock.Transactions.IsEmpty() {
			out = append(out, work)
			continue
		}

		txRoot := accountBlock.Transactions.AsCell()
		txList, err := txRoot.AsDict(64).LoadAll()
		if err != nil {
			return nil, fmt.Errorf("load account transactions: %w", err)
		}
		work.Txs = make([]transactionWork, 0, len(txList))
		for _, txKV := range txList {
			if err = skipCurrencyCollectionBoundary(txKV.Value); err != nil {
				return nil, fmt.Errorf("load tx fees: %w", err)
			}
			txCell, err := txKV.Value.LoadRefCell()
			if err != nil {
				return nil, fmt.Errorf("load tx ref: %w", err)
			}

			var tx tlb.Transaction
			if err = tlb.Parse(&tx, txCell); err != nil {
				return nil, fmt.Errorf("load tx from cell: %w", err)
			}
			hash := txCell.HashKey()
			inMsgCell, err := transactionInputMessageCell(txCell)
			if err != nil {
				return nil, fmt.Errorf("load tx input message cell: %w", err)
			}
			work.Txs = append(work.Txs, transactionWork{
				Cell:      txCell,
				InMsgCell: inMsgCell,
				Parsed:    &tx,
				Hash:      hash,
			})
		}
		sort.Slice(work.Txs, func(i, j int) bool {
			return work.Txs[i].Parsed.LT < work.Txs[j].Parsed.LT
		})
		out = append(out, work)
	}
	return out, nil
}

func transactionInputMessageCell(txCell *cell.Cell) (*cell.Cell, error) {
	loader, err := txCell.BeginParse()
	if err != nil {
		return nil, err
	}
	ioCell, err := loader.LoadRefCell()
	if err != nil {
		return nil, err
	}

	ioLoader, err := ioCell.BeginParse()
	if err != nil {
		return nil, err
	}
	hasInput, err := ioLoader.LoadBoolBit()
	if err != nil {
		return nil, err
	}
	if !hasInput {
		return nil, nil
	}
	return ioLoader.LoadRefCell()
}

func materializeShardAccount(shard *tlb.ShardAccount) (*tlb.ShardAccount, error) {
	root, err := tlb.ToCell(shard)
	if err != nil {
		return nil, err
	}
	root, err = materializeCell(root)
	if err != nil {
		return nil, err
	}

	var out tlb.ShardAccount
	if err = tlb.Parse(&out, root); err != nil {
		return nil, err
	}
	return &out, nil
}

func materializeCell(root *cell.Cell) (*cell.Cell, error) {
	if root == nil {
		return nil, nil
	}
	return cell.FromBOC(root.ToBOC())
}

type shardStateView struct {
	block  ton.BlockIDExt
	root   *cell.Cell
	parsed *tlb.ShardStateUnsplit
}

func previousShardStateViewsFromRoot(root *cell.Cell) ([]*shardStateView, error) {
	loader, err := root.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("parse state update source root: %w", err)
	}

	var state struct {
		Value any `tlb:"[ShardStateUnsplit,ShardStateSplit]"`
	}
	if err = tlb.LoadFromCell(&state, loader); err != nil {
		return nil, fmt.Errorf("load state update source state: %w", err)
	}

	switch value := state.Value.(type) {
	case tlb.ShardStateUnsplit:
		return []*shardStateView{newShardStateViewFromParsed(blockRefFromParsedShardState(&value), root, &value)}, nil
	case tlb.ShardStateSplit:
		left := value.Left
		right := value.Right
		return []*shardStateView{
			newShardStateViewFromParsed(blockRefFromParsedShardState(&left), nil, &left),
			newShardStateViewFromParsed(blockRefFromParsedShardState(&right), nil, &right),
		}, nil
	default:
		return nil, fmt.Errorf("unsupported state update source type %T", state.Value)
	}
}

func newShardStateViewFromParsed(block ton.BlockIDExt, root *cell.Cell, parsed *tlb.ShardStateUnsplit) *shardStateView {
	return &shardStateView{
		block:  block,
		root:   root,
		parsed: parsed,
	}
}

func blockRefFromParsedShardState(state *tlb.ShardStateUnsplit) ton.BlockIDExt {
	if state == nil {
		return ton.BlockIDExt{}
	}
	return ton.BlockIDExt{
		Workchain: state.ShardIdent.WorkchainID,
		Shard:     int64(state.ShardIdent.ShardPrefix),
		SeqNo:     state.Seqno,
	}
}

func (v *shardStateView) account(account []byte) (*tlb.ShardAccount, error) {
	if v == nil || v.parsed == nil {
		return nil, storage.ErrNotFound
	}
	if v.parsed.Accounts.ShardAccounts == nil || v.parsed.Accounts.ShardAccounts.IsEmpty() {
		return nil, storage.ErrNotFound
	}

	value, err := v.parsed.Accounts.ShardAccounts.LoadValue(accountKey(account))
	if errors.Is(err, cell.ErrNoSuchKeyInDict) {
		return nil, storage.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	var accountState tlb.ShardAccount
	if err = tlb.LoadFromCell(&accountState, value); err != nil {
		return nil, err
	}
	return &accountState, nil
}

func accountFromPreviousStates(previous []*shardStateView, workchain int32, account []byte) (*tlb.ShardAccount, error) {
	for _, state := range previous {
		if !stateContainsAccount(state.block, workchain, account) {
			continue
		}

		shardAccount, err := state.account(account)
		if err == nil {
			return shardAccount, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
	}
	return emptyShardAccount(), nil
}

func stateContainsAccount(block ton.BlockIDExt, workchain int32, account []byte) bool {
	if block.Workchain != workchain {
		return false
	}
	addr := accountAddress(workchain, account)
	return tlb.ShardID(uint64(block.Shard)).ContainsAddress(addr)
}

func emptyShardAccount() *tlb.ShardAccount {
	return &tlb.ShardAccount{
		Account:       cell.BeginCell().MustStoreBoolBit(false).EndCell(),
		LastTransHash: make([]byte, 32),
	}
}

func accountAddress(workchain int32, account []byte) *address.Address {
	return address.NewAddress(0, byte(int8(workchain)), append([]byte(nil), account...))
}

func accountAddressRaw(workchain int32, account []byte) string {
	return fmt.Sprintf("%d:%x", workchain, account)
}

func accountKey(accountID []byte) *cell.Cell {
	return cell.BeginCell().MustStoreSlice(accountID, 256).EndCell()
}

type executionConfigCache struct {
	log zerolog.Logger
	mu  sync.Mutex

	masters map[string]*masterExecutionContext
	bundles map[string]*configBundle
}

type masterExecutionContext struct {
	master     ton.BlockIDExt
	stateRoot  *cell.Cell
	prevBlocks tuple.Tuple
	bundle     *configBundle
}

type blockExecutionContext struct {
	master   *masterExecutionContext
	now      uint32
	unpacked tuple.Tuple
}

type configBundle struct {
	config        tlb.BlockchainConfig
	globalVersion int
	libraries     []*cell.Cell

	mu          sync.Mutex
	unpacked    map[uint32]tuple.Tuple
	precompiled map[cell.Hash]*big.Int
}

func newExecutionConfigCache(log zerolog.Logger) *executionConfigCache {
	return &executionConfigCache{
		log:     log,
		masters: map[string]*masterExecutionContext{},
		bundles: map[string]*configBundle{},
	}
}

func (c *executionConfigCache) blockContext(master ton.BlockIDExt, masterState *cell.Cell, now uint32) (*blockExecutionContext, error) {
	masterCtx, err := c.masterContext(master, masterState)
	if err != nil {
		return nil, err
	}
	unpacked, err := masterCtx.bundle.unpackedConfig(now)
	if err != nil {
		return nil, err
	}
	return &blockExecutionContext{
		master:   masterCtx,
		now:      now,
		unpacked: unpacked,
	}, nil
}

func (c *executionConfigCache) masterContext(master ton.BlockIDExt, masterState *cell.Cell) (*masterExecutionContext, error) {
	stateHash := masterState.HashKey(0)
	masterKey := fmt.Sprintf("%x:%x", storage.BlockKey(master), stateHash[:])

	c.mu.Lock()
	if cached := c.masters[masterKey]; cached != nil {
		c.mu.Unlock()
		return cached, nil
	}
	c.mu.Unlock()

	extra, err := mcStateExtra(masterState)
	if err != nil {
		return nil, err
	}
	if extra.ConfigParams.Config.Params == nil || extra.ConfigParams.Config.Params.IsEmpty() {
		return nil, fmt.Errorf("masterchain config is empty")
	}

	configRoot := extra.ConfigParams.Config.Params.AsCell()
	configHash := configRoot.Hash()
	libraries, librariesKey, err := globalLibraries(masterState)
	if err != nil {
		return nil, err
	}
	bundleKey := hex.EncodeToString(configHash) + ":" + librariesKey

	c.mu.Lock()
	bundle := c.bundles[bundleKey]
	c.mu.Unlock()
	if bundle == nil {
		config := tlb.BlockchainConfig{Root: configRoot}
		version, err := config.GetGlobalVersion()
		if err != nil {
			return nil, err
		}
		globalVersion := int(version.Version)
		if globalVersion < tvm.MinSupportedGlobalVersion {
			return nil, fmt.Errorf("unsupported global version %d, minimum supported is %d", globalVersion, tvm.MinSupportedGlobalVersion)
		}

		bundle = &configBundle{
			config:        config,
			globalVersion: globalVersion,
			libraries:     libraries,
			unpacked:      map[uint32]tuple.Tuple{},
			precompiled:   map[cell.Hash]*big.Int{},
		}
		c.mu.Lock()
		if existing := c.bundles[bundleKey]; existing != nil {
			bundle = existing
		} else {
			c.bundles[bundleKey] = bundle
			c.log.Debug().
				Str("config_hash", hex.EncodeToString(configHash)).
				Str("libraries_hash", librariesKey).
				Int("global_version", globalVersion).
				Msg("cached masterchain config bundle")
		}
		c.mu.Unlock()
	}

	prevBlocks, err := runMethodPrevBlocksInfo(master, masterState)
	if err != nil {
		return nil, err
	}
	masterCtx := &masterExecutionContext{
		master:     master,
		stateRoot:  masterState,
		prevBlocks: prevBlocks,
		bundle:     bundle,
	}

	c.mu.Lock()
	c.masters[masterKey] = masterCtx
	c.mu.Unlock()
	return masterCtx, nil
}

func (b *blockExecutionContext) transactionConfig(block loadedBlock, shard *tlb.ShardAccount, tx *tlb.Transaction) (tvm.TransactionEmulationConfig, error) {
	if tx == nil || tx.IO.In == nil {
		return tvm.TransactionEmulationConfig{}, fmt.Errorf("transaction input message is required")
	}

	logicalTime, err := uint64ToInt64(tx.LT, "transaction lt")
	if err != nil {
		return tvm.TransactionEmulationConfig{}, err
	}
	blockLT, err := uint64ToInt64(block.Parsed.BlockInfo.StartLt, "block lt")
	if err != nil {
		return tvm.TransactionEmulationConfig{}, err
	}

	code, err := transactionComputeCode(shard, tx.IO.In)
	if err != nil {
		return tvm.TransactionEmulationConfig{}, err
	}
	precompiled, err := b.master.bundle.precompiledGas(code)
	if err != nil {
		return tvm.TransactionEmulationConfig{}, err
	}
	storageFees := transactionStorageFeesBig(tx)
	incomingCoins, incomingExtra, err := transactionIncomingValue(shard, tx.IO.In, storageFees)
	if err != nil {
		return tvm.TransactionEmulationConfig{}, err
	}
	inMsgParams, err := transactionInMsgParams(tx.IO.In, incomingCoins, incomingExtra)
	if err != nil {
		return tvm.TransactionEmulationConfig{}, err
	}

	return tvm.TransactionEmulationConfig{
		Now:                 block.Parsed.BlockInfo.GenUtime,
		BlockLT:             blockLT,
		LogicalTime:         logicalTime,
		RandSeed:            transactionRandSeed(block.Parsed.Extra.RandSeed, tx.AccountAddr),
		ConfigRoot:          b.master.bundle.config.Root,
		PrevBlocks:          b.master.prevBlocks,
		UnpackedConfig:      b.unpacked,
		PrecompiledGasUsage: precompiled,
		Libraries:           b.master.bundle.libraries,
		IncomingValue:       currencyTuple(incomingCoins, incomingExtra),
		StorageFees:         storageFeesInt64(storageFees),
		DuePayment:          transactionDuePayment(tx),
		InMsgParams:         inMsgParams,
	}, nil
}

func (b *blockExecutionContext) tickTockTransactionConfig(block loadedBlock, shard *tlb.ShardAccount, tx *tlb.Transaction) (tvm.TransactionEmulationConfig, error) {
	if tx == nil {
		return tvm.TransactionEmulationConfig{}, fmt.Errorf("transaction is required")
	}

	logicalTime, err := uint64ToInt64(tx.LT, "transaction lt")
	if err != nil {
		return tvm.TransactionEmulationConfig{}, err
	}
	blockLT, err := uint64ToInt64(block.Parsed.BlockInfo.StartLt, "block lt")
	if err != nil {
		return tvm.TransactionEmulationConfig{}, err
	}

	code, err := transactionComputeCode(shard, nil)
	if err != nil {
		return tvm.TransactionEmulationConfig{}, err
	}
	precompiled, err := b.master.bundle.precompiledGas(code)
	if err != nil {
		return tvm.TransactionEmulationConfig{}, err
	}

	storageFees := transactionStorageFeesBig(tx)
	return tvm.TransactionEmulationConfig{
		Now:                 block.Parsed.BlockInfo.GenUtime,
		BlockLT:             blockLT,
		LogicalTime:         logicalTime,
		RandSeed:            transactionRandSeed(block.Parsed.Extra.RandSeed, tx.AccountAddr),
		ConfigRoot:          b.master.bundle.config.Root,
		PrevBlocks:          b.master.prevBlocks,
		UnpackedConfig:      b.unpacked,
		PrecompiledGasUsage: precompiled,
		Libraries:           b.master.bundle.libraries,
		IncomingValue:       currencyTuple(nil, nil),
		StorageFees:         storageFeesInt64(storageFees),
		DuePayment:          transactionDuePayment(tx),
		InMsgParams:         defaultInMsgParams(),
	}, nil
}

func (b *blockExecutionContext) accountMismatchTxDetails(tx transactionWork, msgCell *cell.Cell, cfg tvm.TransactionEmulationConfig) accountMismatchTx {
	return accountMismatchTx{
		LT:                  tx.Parsed.LT,
		ExpectedTxHash:      hex.EncodeToString(tx.Hash[:]),
		ExpectedTxBOCBase64: cellBOCBase64(tx.Cell),
		InMsgBOCBase64:      cellBOCBase64(msgCell),
		Config: accountMismatchTxConfig{
			GlobalVersion:                b.master.bundle.globalVersion,
			Now:                          cfg.Now,
			BlockLT:                      cfg.BlockLT,
			LogicalTime:                  cfg.LogicalTime,
			RandSeedBase64:               base64.StdEncoding.EncodeToString(cfg.RandSeed),
			ConfigRootBOCBase64:          cellBOCBase64(cfg.ConfigRoot),
			PrevBlocksStackBOCBase64:     stackValueBOCBase64(cfg.PrevBlocks),
			UnpackedConfigStackBOCBase64: stackValueBOCBase64(cfg.UnpackedConfig),
			PrecompiledGasStackBOCBase64: stackValueBOCBase64(cfg.PrecompiledGasUsage),
			IncomingValueStackBOCBase64:  stackValueBOCBase64(cfg.IncomingValue),
			StorageFees:                  cfg.StorageFees,
			DuePaymentNano:               bigIntString(cfg.DuePayment),
			InMsgParamsStackBOCBase64:    stackValueBOCBase64(cfg.InMsgParams),
			LibrariesBOCBase64:           cellsBOCBase64(cfg.Libraries),
		},
	}
}

func (b *configBundle) unpackedConfig(now uint32) (tuple.Tuple, error) {
	b.mu.Lock()
	if cached, ok := b.unpacked[now]; ok {
		b.mu.Unlock()
		return cached, nil
	}
	b.mu.Unlock()

	unpacked, err := runMethodUnpackedConfig(b.config, now)
	if err != nil {
		return tuple.Tuple{}, err
	}

	b.mu.Lock()
	b.unpacked[now] = unpacked
	b.mu.Unlock()
	return unpacked, nil
}

func (b *configBundle) precompiledGas(code *cell.Cell) (*big.Int, error) {
	if code == nil {
		return nil, nil
	}

	hash := code.HashKey()

	b.mu.Lock()
	if cached, ok := b.precompiled[hash]; ok {
		b.mu.Unlock()
		if cached == nil {
			return nil, nil
		}
		return new(big.Int).Set(cached), nil
	}
	b.mu.Unlock()

	gas, err := runMethodPrecompiledGasByHash(b.config, hash)
	if err != nil {
		return nil, err
	}
	var stored *big.Int
	if gas != nil {
		stored = new(big.Int).Set(gas)
	}

	b.mu.Lock()
	b.precompiled[hash] = stored
	b.mu.Unlock()
	if gas == nil {
		return nil, nil
	}
	return new(big.Int).Set(gas), nil
}

func uint64ToInt64(value uint64, name string) (int64, error) {
	if value > math.MaxInt64 {
		return 0, fmt.Errorf("%s %d exceeds int64", name, value)
	}
	return int64(value), nil
}

func transactionRandSeed(blockRandSeed []byte, account []byte) []byte {
	var stack [64]byte
	total := len(blockRandSeed) + len(account)
	var data []byte
	if total <= len(stack) {
		n := copy(stack[:], blockRandSeed)
		copy(stack[n:], account)
		data = stack[:total]
	} else {
		data = make([]byte, 0, total)
		data = append(data, blockRandSeed...)
		data = append(data, account...)
	}
	sum := sha256.Sum256(data)
	return sum[:]
}

func transactionComputeCode(shard *tlb.ShardAccount, msg *tlb.Message) (*cell.Cell, error) {
	if shard != nil && shard.Account != nil {
		var account tlb.AccountState
		if err := tlb.Parse(&account, shard.Account); err != nil {
			return nil, err
		}
		if account.IsValid && account.StateInit != nil && account.StateInit.Code != nil {
			return account.StateInit.Code, nil
		}
	}

	if msg != nil {
		switch m := msg.Msg.(type) {
		case *tlb.InternalMessage:
			if m.StateInit != nil {
				return m.StateInit.Code, nil
			}
		case *tlb.ExternalMessage:
			if m.StateInit != nil {
				return m.StateInit.Code, nil
			}
		}
	}
	return nil, nil
}

func transactionIncomingValue(shard *tlb.ShardAccount, msg *tlb.Message, storageFees *big.Int) (*big.Int, *cell.Dictionary, error) {
	if msg == nil || msg.MsgType != tlb.MsgTypeInternal {
		return nil, nil, nil
	}

	internal := msg.AsInternal()
	remaining := new(big.Int).Set(internal.Amount.Nano())
	if !internal.Bounce {
		balance, err := shardAccountBalance(shard)
		if err != nil {
			return nil, nil, err
		}

		afterStorage := new(big.Int).Add(balance, internal.Amount.Nano())
		if storageFees != nil {
			afterStorage.Sub(afterStorage, storageFees)
		}
		if afterStorage.Sign() < 0 {
			return nil, nil, fmt.Errorf("message value after storage fees is negative")
		}
		if remaining.Cmp(afterStorage) > 0 {
			remaining = afterStorage
		}
	}

	return remaining, internal.ExtraCurrencies, nil
}

func transactionInMsgParams(msg *tlb.Message, remainingCoins *big.Int, remainingExtra *cell.Dictionary) (tuple.Tuple, error) {
	if msg == nil {
		return defaultInMsgParams(), nil
	}

	switch msg.MsgType {
	case tlb.MsgTypeInternal:
		internal := msg.AsInternal()
		stateInit, err := messageStateInitCell(internal.StateInit)
		if err != nil {
			return tuple.Tuple{}, err
		}

		return tuple.NewTupleValue(
			boolToTVMInt(internal.Bounce),
			boolToTVMInt(internal.Bounced),
			cell.BeginCell().MustStoreAddr(internal.SrcAddr).ToSlice(),
			internal.FwdFee.Nano(),
			new(big.Int).SetUint64(internal.CreatedLT),
			new(big.Int).SetUint64(uint64(internal.CreatedAt)),
			internal.Amount.Nano(),
			transactionBigOrZero(remainingCoins),
			extraCurrencyCell(remainingExtra),
			stateInit,
		), nil
	case tlb.MsgTypeExternalIn:
		external := msg.AsExternalIn()
		stateInit, err := messageStateInitCell(external.StateInit)
		if err != nil {
			return tuple.Tuple{}, err
		}

		return tuple.NewTupleValue(
			tvmIntZero,
			tvmIntZero,
			cell.BeginCell().MustStoreAddr(external.SrcAddr).ToSlice(),
			tvmIntZero,
			tvmIntZero,
			tvmIntZero,
			tvmIntZero,
			tvmIntZero,
			nil,
			stateInit,
		), nil
	default:
		return defaultInMsgParams(), nil
	}
}

func messageStateInitCell(stateInit *tlb.StateInit) (*cell.Cell, error) {
	if stateInit == nil {
		return nil, nil
	}
	return tlb.ToCell(stateInit)
}

func defaultInMsgParams() tuple.Tuple {
	return defaultInMsgParamsTuple.Copy()
}

func boolToTVMInt(value bool) *big.Int {
	if value {
		return tvmIntMinusOne
	}
	return tvmIntZero
}

func currencyTuple(coins *big.Int, extra *cell.Dictionary) tuple.Tuple {
	if (coins == nil || coins.Sign() == 0) && (extra == nil || extra.IsEmpty()) {
		return zeroCurrencyTuple.Copy()
	}
	if coins == nil {
		coins = tvmIntZero
	}
	return tuple.NewTupleValue(new(big.Int).Set(coins), extraCurrencyCell(extra))
}

func extraCurrencyCell(extra *cell.Dictionary) *cell.Cell {
	if extra != nil && !extra.IsEmpty() {
		return extra.AsCell()
	}
	return nil
}

func transactionBigOrZero(value *big.Int) *big.Int {
	if value == nil {
		return big.NewInt(0)
	}
	return new(big.Int).Set(value)
}

func shardAccountBalance(shard *tlb.ShardAccount) (*big.Int, error) {
	if shard == nil || shard.Account == nil {
		return big.NewInt(0), nil
	}

	var account tlb.AccountState
	if err := tlb.Parse(&account, shard.Account); err != nil {
		return nil, err
	}
	if !account.IsValid {
		return big.NewInt(0), nil
	}
	return new(big.Int).Set(account.Balance.Nano()), nil
}

func storageFeesInt64(fees *big.Int) int64 {
	if !fees.IsInt64() {
		return math.MaxInt64
	}
	return fees.Int64()
}

func transactionStorageFeesBig(tx *tlb.Transaction) *big.Int {
	phase := transactionStoragePhase(tx)
	if phase == nil {
		return big.NewInt(0)
	}
	return new(big.Int).Set(phase.StorageFeesCollected.Nano())
}

func transactionDuePayment(tx *tlb.Transaction) *big.Int {
	phase := transactionStoragePhase(tx)
	if phase == nil || phase.StorageFeesDue == nil {
		return big.NewInt(0)
	}
	return phase.StorageFeesDue.Nano()
}

func transactionStoragePhase(tx *tlb.Transaction) *tlb.StoragePhase {
	if tx == nil {
		return nil
	}
	switch desc := tx.Description.(type) {
	case tlb.TransactionDescriptionOrdinary:
		return desc.StoragePhase
	case tlb.TransactionDescriptionTickTock:
		return &desc.StoragePhase
	case tlb.TransactionDescriptionStorage:
		return &desc.StoragePhase
	case tlb.TransactionDescriptionSplitPrepare:
		return desc.StoragePhase
	case tlb.TransactionDescriptionMergePrepare:
		return &desc.StoragePhase
	case tlb.TransactionDescriptionMergeInstall:
		return desc.StoragePhase
	default:
		return nil
	}
}

func globalLibraries(masterState *cell.Cell) ([]*cell.Cell, string, error) {
	libraries, err := librariesDict(masterState)
	if err != nil {
		return nil, "", err
	}
	if libraries == nil || libraries.IsEmpty() {
		return nil, "", nil
	}

	root := libraries.AsCell()
	return []*cell.Cell{root}, hex.EncodeToString(root.Hash()), nil
}

func mcStateExtra(root *cell.Cell) (*tlb.McStateExtra, error) {
	loader, err := root.BeginParse()
	if err != nil {
		return nil, err
	}

	var state tlb.ShardStateUnsplit
	if err = tlb.LoadFromCell(&state, loader); err != nil {
		return nil, err
	}
	if state.McStateExtra == nil {
		return nil, fmt.Errorf("state is missing mc_state_extra")
	}

	extraLoader, err := state.McStateExtra.BeginParse()
	if err != nil {
		return nil, err
	}

	var extra tlb.McStateExtra
	if err = tlb.LoadFromCell(&extra, extraLoader); err != nil {
		return nil, err
	}
	return &extra, nil
}

type runMethodMasterInfo struct {
	prevBlocks    *tlb.OldMcBlocksInfoAugDict
	afterKeyBlock bool
	lastKeyBlock  *tlb.ExtBlkRef
}

func runMethodPrevBlocksInfo(master ton.BlockIDExt, masterState *cell.Cell) (tuple.Tuple, error) {
	info, err := runMethodMasterInfoFromState(masterState)
	if err != nil {
		return tuple.Tuple{}, err
	}
	oldBlock := func(seqno uint32) (ton.BlockIDExt, error) {
		if seqno == master.SeqNo {
			return master, nil
		}
		return runMethodOldMasterBlockID(info.prevBlocks, seqno)
	}

	lastMCBlocks := []any{runMethodBlockIDTuple(master)}
	for seqno := master.SeqNo; seqno > 0 && len(lastMCBlocks) < 16; {
		seqno--
		block, err := oldBlock(seqno)
		if err != nil {
			return tuple.Tuple{}, fmt.Errorf("load previous master block id seqno=%d: %w", seqno, err)
		}
		lastMCBlocks = append(lastMCBlocks, runMethodBlockIDTuple(block))
	}

	lastKeyBlock, err := runMethodLastKeyBlock(master, info)
	if err != nil {
		return tuple.Tuple{}, err
	}

	lastMCBlocks100 := make([]any, 0, 16)
	for seqno := master.SeqNo / 100 * 100; len(lastMCBlocks100) < 16; {
		block, err := oldBlock(seqno)
		if err != nil {
			return tuple.Tuple{}, fmt.Errorf("load previous master block id seqno=%d: %w", seqno, err)
		}
		lastMCBlocks100 = append(lastMCBlocks100, runMethodBlockIDTuple(block))
		if seqno < 100 {
			break
		}
		seqno -= 100
	}

	return tuple.NewTupleValue(
		tuple.NewTupleValue(lastMCBlocks...),
		runMethodBlockIDTuple(lastKeyBlock),
		tuple.NewTupleValue(lastMCBlocks100...),
	), nil
}

func runMethodMasterInfoFromState(masterState *cell.Cell) (runMethodMasterInfo, error) {
	extra, err := mcStateExtra(masterState)
	if err != nil {
		return runMethodMasterInfo{}, err
	}
	if extra.Info == nil {
		return runMethodMasterInfo{}, fmt.Errorf("state is missing mc_state_extra info")
	}

	loader, err := extra.Info.BeginParse()
	if err != nil {
		return runMethodMasterInfo{}, err
	}
	if _, err = loader.LoadUInt(16); err != nil {
		return runMethodMasterInfo{}, err
	}
	if _, err = loader.LoadUInt(32); err != nil {
		return runMethodMasterInfo{}, err
	}
	if _, err = loader.LoadUInt(32); err != nil {
		return runMethodMasterInfo{}, err
	}
	if _, err = loader.LoadBoolBit(); err != nil {
		return runMethodMasterInfo{}, err
	}

	prevBlocks := &tlb.OldMcBlocksInfoAugDict{}
	if err = prevBlocks.LoadFromCell(loader); err != nil {
		return runMethodMasterInfo{}, err
	}

	afterKeyBlock, err := loader.LoadBoolBit()
	if err != nil {
		return runMethodMasterInfo{}, err
	}

	hasLastKeyBlock, err := loader.LoadBoolBit()
	if err != nil {
		return runMethodMasterInfo{}, err
	}

	var lastKeyBlock *tlb.ExtBlkRef
	if hasLastKeyBlock {
		ref := &tlb.ExtBlkRef{}
		if err = tlb.LoadFromCell(ref, loader); err != nil {
			return runMethodMasterInfo{}, err
		}
		lastKeyBlock = ref
	}

	return runMethodMasterInfo{
		prevBlocks:    prevBlocks,
		afterKeyBlock: afterKeyBlock,
		lastKeyBlock:  lastKeyBlock,
	}, nil
}

func runMethodOldMasterBlockID(prevBlocks *tlb.OldMcBlocksInfoAugDict, seqno uint32) (ton.BlockIDExt, error) {
	if prevBlocks == nil || prevBlocks.IsEmpty() {
		return ton.BlockIDExt{}, fmt.Errorf("cannot fetch old mc block")
	}

	value, err := prevBlocks.LoadValueByIntKey(new(big.Int).SetUint64(uint64(seqno)))
	if err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("cannot fetch old mc block: %w", err)
	}

	var ref tlb.KeyExtBlkRef
	if err = tlb.LoadFromCell(&ref, value); err != nil {
		return ton.BlockIDExt{}, err
	}
	if ref.BlkRef.SeqNo != seqno {
		return ton.BlockIDExt{}, fmt.Errorf("old mc block seqno mismatch: got %d want %d", ref.BlkRef.SeqNo, seqno)
	}

	return runMethodExtBlkRef(ref.BlkRef), nil
}

func runMethodLastKeyBlock(master ton.BlockIDExt, info runMethodMasterInfo) (ton.BlockIDExt, error) {
	if info.afterKeyBlock {
		return master, nil
	}
	if info.lastKeyBlock == nil {
		return ton.BlockIDExt{}, fmt.Errorf("cannot fetch last key block")
	}
	return runMethodExtBlkRef(*info.lastKeyBlock), nil
}

func runMethodExtBlkRef(ref tlb.ExtBlkRef) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: masterchainID,
		Shard:     masterchainShard,
		SeqNo:     ref.SeqNo,
		RootHash:  append([]byte(nil), ref.RootHash...),
		FileHash:  append([]byte(nil), ref.FileHash...),
	}
}

func runMethodBlockIDTuple(id ton.BlockIDExt) tuple.Tuple {
	return tuple.NewTupleValue(
		big.NewInt(int64(id.Workchain)),
		new(big.Int).SetUint64(uint64(id.Shard)),
		new(big.Int).SetUint64(uint64(id.SeqNo)),
		new(big.Int).SetBytes(id.RootHash),
		new(big.Int).SetBytes(id.FileHash),
	)
}

func runMethodUnpackedConfig(config tlb.BlockchainConfig, now uint32) (tuple.Tuple, error) {
	storagePrices, err := runMethodCurrentStoragePrices(config, now)
	if err != nil {
		return tuple.Tuple{}, err
	}

	values := []any{runMethodMaybeSlice(storagePrices)}
	for _, id := range []uint32{
		tlb.ConfigParamGlobalID,
		tlb.ConfigParamGasPricesMasterchain,
		tlb.ConfigParamGasPricesBasechain,
		tlb.ConfigParamMsgForwardPricesMasterchain,
		tlb.ConfigParamMsgForwardPricesBasechain,
		tlb.ConfigParamSizeLimits,
	} {
		param, err := runMethodConfigParamSlice(config, id)
		if err != nil {
			return tuple.Tuple{}, err
		}
		values = append(values, runMethodMaybeSlice(param))
	}

	return tuple.NewTupleValue(values...), nil
}

func runMethodCurrentStoragePrices(config tlb.BlockchainConfig, now uint32) (*cell.Slice, error) {
	root, err := runMethodConfigParamCell(config, tlb.ConfigParamStoragePrices)
	if err != nil || root == nil {
		return nil, err
	}

	entries, err := root.AsDict(32).LoadAll()
	if err != nil {
		return nil, err
	}

	var best *cell.Slice
	var bestSince uint64
	for _, entry := range entries {
		since, err := entry.Key.LoadUInt(32)
		if err != nil {
			return nil, err
		}
		if since > uint64(now) || best != nil && since <= bestSince {
			continue
		}
		best = entry.Value.Copy()
		bestSince = since
	}

	return best, nil
}

func runMethodConfigParamSlice(config tlb.BlockchainConfig, id uint32) (*cell.Slice, error) {
	param, err := runMethodConfigParamCell(config, id)
	if err != nil || param == nil {
		return nil, err
	}
	return param.BeginParse()
}

func runMethodConfigParamCell(config tlb.BlockchainConfig, id uint32) (*cell.Cell, error) {
	if config.Root == nil {
		return nil, nil
	}

	param, err := config.GetParam(id)
	if errors.Is(err, tlb.ErrBlockchainConfigParamAbsent) {
		return nil, nil
	}
	return param, err
}

func runMethodMaybeSlice(value *cell.Slice) any {
	if value == nil {
		return nil
	}
	return value
}

func runMethodPrecompiledGasByHash(config tlb.BlockchainConfig, hash cell.Hash) (*big.Int, error) {
	if config.Root == nil {
		return nil, nil
	}

	precompiled, err := config.GetPrecompiledContractsConfig()
	if err != nil {
		return nil, err
	}
	if precompiled.List == nil || precompiled.List.IsEmpty() {
		return nil, nil
	}

	value, err := precompiled.List.LoadValue(accountKey(hash[:]))
	if errors.Is(err, cell.ErrNoSuchKeyInDict) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var smc tlb.PrecompiledSmc
	if err = tlb.LoadFromCell(&smc, value); err != nil {
		return nil, err
	}

	return new(big.Int).SetUint64(smc.GasUsage), nil
}

func librariesDict(stateRoot *cell.Cell) (*cell.Dictionary, error) {
	stateLoader, err := stateRoot.BeginParse()
	if err != nil {
		return nil, err
	}

	var state tlb.ShardStateUnsplit
	if err := tlb.LoadFromCell(&state, stateLoader); err != nil {
		return nil, err
	}
	if state.Stats == nil {
		return nil, fmt.Errorf("state is missing shard state extras")
	}

	loader, err := state.Stats.BeginParse()
	if err != nil {
		return nil, err
	}
	if _, err := loader.LoadUInt(64); err != nil {
		return nil, err
	}
	if _, err := loader.LoadUInt(64); err != nil {
		return nil, err
	}
	if err := tlb.LoadFromCell(new(tlb.CurrencyCollection), loader); err != nil {
		return nil, err
	}
	if err := tlb.LoadFromCell(new(tlb.CurrencyCollection), loader); err != nil {
		return nil, err
	}
	return loader.LoadDict(256)
}

func skipCurrencyCollectionBoundary(loader *cell.Slice) error {
	if _, err := loader.LoadBigCoins(); err != nil {
		return err
	}
	_, err := loader.LoadMaybeRef()
	return err
}
