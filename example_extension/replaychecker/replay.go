package replaychecker

import (
	"bytes"
	"container/list"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
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

// configEpochCacheLimit bounds the per-config-epoch cache. The blockchain
// config changes only at key blocks, so a handful of entries cover the live
// window plus occasional historic replays. Mirrors gton's liveview cache.
const configEpochCacheLimit = 4

// storageStatCacheLimit bounds the cross-block account storage-stat LRU. Each
// entry is one hot contract's (pre-state hash -> stat cell) pair; 4096 keeps
// the busiest contracts warm across blocks while staying small.
const storageStatCacheLimit = 4096

var ErrMismatch = errors.New("tvm replay mismatch")

type ValidatorOptions struct {
	Logger zerolog.Logger
	Store  hooks.Store
}

type Validator struct {
	inner *replayValidator
	store hooks.Store
}

type Result struct {
	Accounts         int
	Transactions     int
	EmulationElapsed time.Duration
	// Mismatches is the number of accounts whose replay diverged from the
	// applied state for this block. Zero means the block replayed bit-identically.
	Mismatches int
}

func NewValidator(opts ValidatorOptions) *Validator {
	return &Validator{
		store: opts.Store,
		inner: &replayValidator{
			configs:       newConfigEpochCache(opts.Logger.With().Str("component", "replaychecker-cache").Logger()),
			storageStats:  newStorageStatCache(storageStatCacheLimit),
			tvm:           tvm.NewTVM(),
			log:           opts.Logger,
			mismatchLimit: -1,
		},
	}
}

func (v *Validator) ValidateAppliedBlock(ctx context.Context, event hooks.BlockAppliedEvent) (Result, error) {
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
	execCtx, err := v.inner.blockContext(master, masterState, block)
	if err != nil {
		return Result{}, fmt.Errorf("prepare execution context %s: %w", storage.FormatBlockRef(block.ID), err)
	}

	masterSeqno := block.ID.SeqNo
	if block.Parsed.BlockInfo.NotMaster {
		masterSeqno = master.SeqNo
	}

	// A fresh per-block mismatch scope; state never leaks across blocks and the
	// map cannot re-trip after a divergence has been reported once.
	scope := newMismatchScope(masterSeqno, block.ID.Workchain, v.inner.mismatchLimit)
	validation, err := v.inner.validateBlock(ctx, masterSeqno, accounts, previousViews, expectedView, block, execCtx, scope)
	result := Result{
		Accounts:         validation.Accounts,
		Transactions:     validation.Transactions,
		EmulationElapsed: validation.EmulationElapsed,
		Mismatches:       scope.len(),
	}
	if err != nil {
		return result, err
	}
	if result.Mismatches > 0 {
		return result, fmt.Errorf("%w: %s (%d accounts)", ErrMismatch, storage.FormatBlockRef(block.ID), result.Mismatches)
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

// blockExecutionContext is the per-block execution context: the immutable tvm
// block context (config epoch + prev blocks + global libraries + block time/lt),
// its global version, and the block rand seed used to derive per-account seeds.
type blockExecutionContext struct {
	block         *tvm.BlockContext
	globalVersion int
}

type replayValidator struct {
	configs       *configEpochCache
	storageStats  *storageStatCache
	tvm           *tvm.TVM
	log           zerolog.Logger
	mismatchLimit int
	vmTraceAddr   string
	vmTraceLT     uint64
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
	LT                  uint64 `json:"lt"`
	ExpectedTxHash      string `json:"expected_tx_hash,omitempty"`
	GotTxHash           string `json:"got_tx_hash,omitempty"`
	ExpectedTxBOCBase64 string `json:"expected_tx_boc_base64,omitempty"`
	GotTxBOCBase64      string `json:"got_tx_boc_base64,omitempty"`
	InMsgBOCBase64      string `json:"in_msg_boc_base64,omitempty"`
}

type accountMismatchDetails struct {
	fromShardAccountBOCBase64 string
	toShardAccountBOCBase64   string
	gotShardAccountBOCBase64  string
	firstTx                   *accountMismatchTx
}

// mismatchKey is a fixed struct key for the mismatch map: cheaper than a
// per-tx fmt.Sprintf and allocation-free.
type mismatchKey struct {
	masterSeqno uint32
	workchain   int32
	account     [32]byte
}

func newMismatchKey(masterSeqno uint32, workchain int32, account []byte) mismatchKey {
	var key mismatchKey
	key.masterSeqno = masterSeqno
	key.workchain = workchain
	copy(key.account[:], account)
	return key
}

// mismatchScope collects the account mismatches of a single block replay. It is
// created fresh per ValidateAppliedBlock so state never leaks across blocks, and
// it is safe for concurrent lanes (all access is behind mu).
type mismatchScope struct {
	masterSeqno uint32
	workchain   int32
	limit       int

	mu      sync.Mutex
	entries map[mismatchKey]accountMismatch
}

func newMismatchScope(masterSeqno uint32, workchain int32, limit int) *mismatchScope {
	return &mismatchScope{
		masterSeqno: masterSeqno,
		workchain:   workchain,
		limit:       limit,
		entries:     map[mismatchKey]accountMismatch{},
	}
}

func (s *mismatchScope) collects() bool {
	return s.limit != 0
}

func (s *mismatchScope) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}

func (s *mismatchScope) has(account []byte) bool {
	key := newMismatchKey(s.masterSeqno, s.workchain, account)
	s.mu.Lock()
	_, ok := s.entries[key]
	s.mu.Unlock()
	return ok
}

func (s *mismatchScope) add(account []byte, details accountMismatchDetails) {
	if !s.collects() {
		return
	}
	key := newMismatchKey(s.masterSeqno, s.workchain, account)

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.limit > 0 {
		if _, exists := s.entries[key]; !exists && len(s.entries) >= s.limit {
			return
		}
	}

	mismatch := accountMismatch{
		MasterSeqno:               s.masterSeqno,
		Workchain:                 s.workchain,
		Address:                   accountAddressRaw(s.workchain, account),
		FromShardAccountBOCBase64: details.fromShardAccountBOCBase64,
		ToShardAccountBOCBase64:   details.toShardAccountBOCBase64,
		GotShardAccountBOCBase64:  details.gotShardAccountBOCBase64,
		FirstTx:                   details.firstTx,
	}
	if existing, ok := s.entries[key]; ok {
		mismatch = mergeAccountMismatch(existing, mismatch)
	}
	s.entries[key] = mismatch
}

// updateIfExists merges freshly collected details into an already recorded
// mismatch, but does not create a new one.
func (s *mismatchScope) updateIfExists(account []byte, details accountMismatchDetails) {
	if !s.collects() {
		return
	}
	key := newMismatchKey(s.masterSeqno, s.workchain, account)
	update := accountMismatch{
		MasterSeqno:               s.masterSeqno,
		Workchain:                 s.workchain,
		Address:                   accountAddressRaw(s.workchain, account),
		FromShardAccountBOCBase64: details.fromShardAccountBOCBase64,
		ToShardAccountBOCBase64:   details.toShardAccountBOCBase64,
		GotShardAccountBOCBase64:  details.gotShardAccountBOCBase64,
		FirstTx:                   details.firstTx,
	}

	s.mu.Lock()
	if existing, ok := s.entries[key]; ok {
		s.entries[key] = mergeAccountMismatch(existing, update)
	}
	s.mu.Unlock()
}

// snapshot returns a copy of the collected mismatches for logging.
func (s *mismatchScope) snapshot() []accountMismatch {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.entries) == 0 {
		return nil
	}
	out := make([]accountMismatch, 0, len(s.entries))
	for _, m := range s.entries {
		out = append(out, m)
	}
	return out
}

type blockValidationResult struct {
	Transactions     int
	Accounts         int
	EmulationElapsed time.Duration
}

func (v *replayValidator) validateBlock(ctx context.Context, masterSeqno uint32, accounts []accountBlockWork, previousViews []*shardStateView, expectedView *shardStateView, block loadedBlock, execCtx *blockExecutionContext, scope *mismatchScope) (blockValidationResult, error) {
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
		Int("global_version", execCtx.globalVersion).
		Msg("replaying block transactions")

	// Per-account execution is independent: each account's inbound messages are
	// fixed by the block body, and the *BlockContext / *PreparedBlockchainConfig
	// are immutable and shared. Run accounts over a worker pool; each account's
	// tx chain stays sequential inside its lane while sharing the immutable TVM
	// dispatch tables.
	workers := runtime.GOMAXPROCS(0)
	if workers > len(accounts) {
		workers = len(accounts)
	}
	if workers < 1 {
		workers = 1
	}

	var (
		totalTxs     int64
		totalElapsed int64 // nanoseconds
		next         int64
		firstErr     atomic.Pointer[error]
	)

	lane := func() error {
		machine := v.tvm
		for {
			idx := int(atomic.AddInt64(&next, 1)) - 1
			if idx >= len(accounts) {
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			elapsed, txs, err := v.validateAccountBlock(masterSeqno, previousViews, expectedView, block, execCtx, machine, accounts[idx], scope)
			atomic.AddInt64(&totalTxs, int64(txs))
			atomic.AddInt64(&totalElapsed, int64(elapsed))
			if err != nil {
				return err
			}
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := lane(); err != nil {
				e := err
				firstErr.CompareAndSwap(nil, &e)
			}
		}()
	}
	wg.Wait()

	result.Transactions = int(atomic.LoadInt64(&totalTxs))
	result.EmulationElapsed = time.Duration(atomic.LoadInt64(&totalElapsed))
	if errp := firstErr.Load(); errp != nil {
		return result, *errp
	}
	return result, nil
}

func (v *replayValidator) validateAccountBlock(masterSeqno uint32, previous []*shardStateView, expected *shardStateView, block loadedBlock, execCtx *blockExecutionContext, machine *tvm.TVM, account accountBlockWork, scope *mismatchScope) (time.Duration, int, error) {
	addr := accountAddress(block.ID.Workchain, account.Account)

	// The previous account comes straight from a dict descent on the previous
	// state root; the lazy *tlb.ShardAccount carries trusted hashes and loads
	// on demand through the sharded decoded-cell cache. No BOC roundtrip.
	initialShard, err := accountFromPreviousStates(previous, block.ID.Workchain, account.Account)
	if err != nil {
		return 0, 0, fmt.Errorf("load previous account %s: %w", addr.StringRaw(), err)
	}
	preStateHash := shardAccountStateHash(initialShard)

	current, err := tvm.PrepareAccount(initialShard, addr)
	if err != nil {
		return 0, 0, fmt.Errorf("prepare previous account %s: %w", addr.StringRaw(), err)
	}

	var details accountMismatchDetails
	addMismatch := func(got *tlb.ShardAccount) {
		v.fillAccountMismatchDetails(scope, &details, expected, account.Account, initialShard, got)
		scope.add(account.Account, details)
	}
	setFirstTx := func(before *tlb.ShardAccount, txDetails accountMismatchTx) {
		if !scope.collects() {
			return
		}
		if details.firstTx == nil {
			if details.fromShardAccountBOCBase64 == "" {
				details.fromShardAccountBOCBase64 = shardAccountBOCBase64(before)
			}
			details.firstTx = &txDetails
		}
	}

	// Seed the storage stat from the cross-block LRU only when the cached entry
	// matches this account's PRE-state hash exactly (so a stale entry is simply
	// ignored). SAFETY: tvm only validates a supplied stat against the account's
	// StorageExtra dict hash for global version >= 11 non-masterchain large
	// accounts; for masterchain/small accounts it does not, so the exact
	// pre-state-hash gate is what keeps the seed sound.
	accountStorageStat := v.storageStats.get(block.ID.Workchain, account.Account, preStateHash)

	var total time.Duration
	var txCount int
	var currentShard = initialShard

	for _, tx := range account.Txs {
		txCount++
		opts := tvm.TransactionOptions{
			LogicalTime:        int64(tx.Parsed.LT),
			AccountStorageStat: accountStorageStat,
		}

		var (
			res         *tvm.TransactionExecutionResult
			elapsed     time.Duration
			finishTrace func()
			emuErr      error
		)

		if traceHook, finish := v.startVMTrace(addr.StringRaw(), tx.Parsed.LT); traceHook != nil {
			opts.TraceHook = traceHook
			finishTrace = finish
		}

		if tx.Parsed.IO.In == nil || tx.InMsgCell == nil {
			desc, ok := tx.Parsed.Description.(tlb.TransactionDescriptionTickTock)
			if !ok {
				addMismatch(currentShard)
				v.log.Warn().
					Uint32("master_seqno", masterSeqno).
					Str("block", storage.FormatBlockRef(block.ID)).
					Str("address", addr.StringRaw()).
					Uint64("lt", tx.Parsed.LT).
					Str("tx_type", fmt.Sprintf("%T", tx.Parsed.Description)).
					Msg("transaction has no input message; go transaction emulator cannot replay it yet")
				break
			}
			started := time.Now()
			res, emuErr = machine.EmulateTickTockTransaction(execCtx.block, current, desc.IsTock, opts)
			elapsed = time.Since(started)
		} else {
			var msg *tvm.PreparedMessage
			msg, emuErr = tvm.PrepareParsedMessage(tx.InMsgCell, tx.Parsed.IO.In)
			if emuErr == nil {
				started := time.Now()
				res, emuErr = machine.EmulateTransaction(execCtx.block, current, msg, opts)
				elapsed = time.Since(started)
			}
		}
		if finishTrace != nil {
			finishTrace()
		}
		total += elapsed

		gasUsed := int64(0)
		if res != nil {
			gasUsed = res.GasUsed
		}

		if emuErr != nil {
			txDetails := v.accountMismatchTxDetails(scope, tx)
			setFirstTx(currentShard, txDetails)
			addMismatch(currentShard)
			v.log.Warn().
				Err(emuErr).
				Uint32("master_seqno", masterSeqno).
				Str("block", storage.FormatBlockRef(block.ID)).
				Str("address", addr.StringRaw()).
				Uint64("lt", tx.Parsed.LT).
				Int64("gas", gasUsed).
				Dur("elapsed", elapsed).
				Msg("transaction emulation failed")
			break
		}
		nextShard := res.NextAccount.ShardAccount()
		gotTxHash := res.TransactionCell.HashKey()
		if gotTxHash != tx.Hash {
			txDetails := v.accountMismatchTxDetails(scope, tx)
			txDetails.GotTxHash = hex.EncodeToString(gotTxHash[:])
			txDetails.GotTxBOCBase64 = cellBOCBase64(res.TransactionCell)
			setFirstTx(currentShard, txDetails)
			addMismatch(nextShard)
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

		// tx N's result feeds tx N+1 with ZERO re-parse / re-materialize.
		current = res.NextAccount
		currentShard = nextShard
		accountStorageStat = res.AccountStorageStat
	}

	v.compareAccountResult(masterSeqno, block.ID, expected, account, details, initialShard, currentShard, scope)

	// Store the resulting (post-state hash -> stat) for the next block's replay
	// of this hot account.
	if accountStorageStat != nil && currentShard != nil {
		v.storageStats.put(block.ID.Workchain, account.Account, shardAccountStateHash(currentShard), accountStorageStat)
	}
	return total, txCount, nil
}

func (v *replayValidator) compareAccountResult(masterSeqno uint32, block ton.BlockIDExt, expectedView *shardStateView, account accountBlockWork, details accountMismatchDetails, from *tlb.ShardAccount, got *tlb.ShardAccount, scope *mismatchScope) {
	addr := accountAddress(block.Workchain, account.Account)

	if got == nil || got.Account == nil {
		v.fillAccountMismatchDetails(scope, &details, expectedView, account.Account, from, got)
		scope.add(account.Account, details)
		v.log.Warn().
			Uint32("master_seqno", masterSeqno).
			Str("block", storage.FormatBlockRef(block)).
			Str("address", addr.StringRaw()).
			Msg("missing emulated account result")
		return
	}

	// HAPPY PATH: the account block's HashUpdate.NewHash is the expected
	// post-state account root hash. Compare against the produced hash first; on
	// a match there is nothing more to do (no post-state dict descent, no parse).
	gotAccountHash := got.Account.HashKey()
	if bytes.Equal(gotAccountHash[:], account.Expected) {
		// Still reconcile any mismatch recorded during the tx loop (e.g. a tx
		// hash mismatch that nonetheless landed on the expected account root).
		if scope.has(account.Account) {
			v.fillAccountMismatchDetails(scope, &details, expectedView, account.Account, from, got)
			scope.updateIfExists(account.Account, details)
		}
		return
	}

	// DIVERGENCE: descend the expected post-state dict only now, for diagnostics.
	addMismatch := func() {
		v.fillAccountMismatchDetails(scope, &details, expectedView, account.Account, from, got)
		scope.add(account.Account, details)
	}
	defer func() {
		if !scope.has(account.Account) {
			return
		}
		v.fillAccountMismatchDetails(scope, &details, expectedView, account.Account, from, got)
		scope.updateIfExists(account.Account, details)
	}()

	expectedAccountHash := account.Expected
	expectedShard, err := expectedView.account(account.Account)
	if err == nil {
		expectedHashKey := expectedShard.Account.HashKey()
		expectedAccountHash = expectedHashKey[:]
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

	addMismatch()
	v.log.Warn().
		Uint32("master_seqno", masterSeqno).
		Str("block", storage.FormatBlockRef(block)).
		Str("address", addr.StringRaw()).
		Str("expected_account_hash", hex.EncodeToString(expectedAccountHash)).
		Str("got_account_hash", hex.EncodeToString(gotAccountHash[:])).
		Msg("account root hash mismatch")
}

func (v *replayValidator) fillAccountMismatchDetails(scope *mismatchScope, details *accountMismatchDetails, expected *shardStateView, account []byte, from *tlb.ShardAccount, got *tlb.ShardAccount) {
	if !scope.collects() || details == nil {
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

func (v *replayValidator) accountMismatchTxDetails(scope *mismatchScope, tx transactionWork) accountMismatchTx {
	if !scope.collects() {
		return accountMismatchTx{}
	}
	return accountMismatchTx{
		LT:                  tx.Parsed.LT,
		ExpectedTxHash:      hex.EncodeToString(tx.Hash[:]),
		ExpectedTxBOCBase64: cellBOCBase64(tx.Cell),
		InMsgBOCBase64:      cellBOCBase64(tx.InMsgCell),
	}
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
	if dst.GotTxHash == "" {
		dst.GotTxHash = src.GotTxHash
	}
	if dst.GotTxBOCBase64 == "" {
		dst.GotTxBOCBase64 = src.GotTxBOCBase64
	}
	return dst
}

func expectedShardAccountForMismatch(expected *shardStateView, account []byte) *tlb.ShardAccount {
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

// shardAccountStateHash returns the hash of the account state root (the value
// that identifies the account's stored state), or nil when unavailable.
func shardAccountStateHash(shard *tlb.ShardAccount) []byte {
	if shard == nil || shard.Account == nil {
		return nil
	}
	return shard.Account.Hash()
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

type shardStateView struct {
	block  ton.BlockIDExt
	root   *cell.Cell
	parsed *tlb.ShardStateUnsplit
}

type shardStateUnion struct {
	Value any `tlb:"[ShardStateUnsplit,ShardStateSplit]"`
}

func previousShardStateViewsFromRoot(root *cell.Cell) ([]*shardStateView, error) {
	loader, err := root.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("parse state update source root: %w", err)
	}

	var state shardStateUnion
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
	return ton.BlockIDExt{
		Workchain: state.ShardIdent.WorkchainID,
		Shard:     int64(state.ShardIdent.ShardPrefix),
		SeqNo:     state.Seqno,
	}
}

func (v *shardStateView) account(account []byte) (*tlb.ShardAccount, error) {
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

// ------------------------------------------------------------------------
// config epoch cache: one *tvm.PreparedBlockchainConfig per config root hash (bounded LRU)
// ------------------------------------------------------------------------

type configEpochCache struct {
	log zerolog.Logger

	mu      sync.Mutex
	entries map[cell.Hash]*list.Element
	order   *list.List
}

type configEpochEntry struct {
	key   cell.Hash
	epoch *configEpoch
}

// configEpoch is the immutable per-config-epoch state derived once from a config
// root: the prepared tvm config and its global version.
type configEpoch struct {
	prepared      *tvm.PreparedBlockchainConfig
	globalVersion int
}

func newConfigEpochCache(log zerolog.Logger) *configEpochCache {
	return &configEpochCache{
		log:     log,
		entries: map[cell.Hash]*list.Element{},
		order:   list.New(),
	}
}

// blockContext resolves the per-block execution context for an applied block:
// the config-epoch prepared config (LRU-cached by config root hash) plus this
// block's prev-blocks tuple, global libraries and block time/lt, assembled into
// an immutable *tvm.BlockContext shared by every account lane of the block.
func (v *replayValidator) blockContext(master ton.BlockIDExt, masterState *cell.Cell, block loadedBlock) (*blockExecutionContext, error) {
	extra, err := mcStateExtra(masterState)
	if err != nil {
		return nil, err
	}
	if extra.ConfigParams.Config.Params == nil || extra.ConfigParams.Config.Params.IsEmpty() {
		return nil, fmt.Errorf("masterchain config is empty")
	}
	configRoot := extra.ConfigParams.Config.Params.AsCell()

	epoch, err := v.configs.forRoot(configRoot)
	if err != nil {
		return nil, err
	}

	prevBlocks, err := runMethodPrevBlocksInfo(master, masterState)
	if err != nil {
		return nil, err
	}

	// BlockContext.Libraries carries GLOBAL libraries only; per-account libraries
	// come from PrepareAccount and the runtime merges them.
	libraries, _, err := globalLibraries(masterState)
	if err != nil {
		return nil, err
	}

	blockLT := block.Parsed.BlockInfo.StartLt
	blockCtx, err := epoch.prepared.NewBlockContext(tvm.BlockOptions{
		Now:        block.Parsed.BlockInfo.GenUtime,
		BlockLT:    int64(blockLT),
		RandSeed:   block.Parsed.Extra.RandSeed,
		PrevBlocks: prevBlocks,
		Libraries:  libraries,
	})
	if err != nil {
		return nil, err
	}

	return &blockExecutionContext{
		block:         blockCtx,
		globalVersion: epoch.globalVersion,
	}, nil
}

func (c *configEpochCache) forRoot(configRoot *cell.Cell) (*configEpoch, error) {
	key := configRoot.HashKey()

	c.mu.Lock()
	if element, ok := c.entries[key]; ok {
		c.order.MoveToFront(element)
		epoch := element.Value.(*configEpochEntry).epoch
		c.mu.Unlock()
		return epoch, nil
	}
	c.mu.Unlock()

	epoch, err := buildConfigEpoch(configRoot)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if element, ok := c.entries[key]; ok {
		// A concurrent replay built the same epoch; keep the published one.
		c.order.MoveToFront(element)
		return element.Value.(*configEpochEntry).epoch, nil
	}
	c.entries[key] = c.order.PushFront(&configEpochEntry{key: key, epoch: epoch})
	for len(c.entries) > configEpochCacheLimit {
		oldest := c.order.Back()
		c.order.Remove(oldest)
		delete(c.entries, oldest.Value.(*configEpochEntry).key)
	}
	c.log.Debug().
		Str("config_hash", hex.EncodeToString(key[:])).
		Int("global_version", epoch.globalVersion).
		Msg("cached prepared config epoch")
	return epoch, nil
}

func buildConfigEpoch(configRoot *cell.Cell) (*configEpoch, error) {
	prepared, err := tvm.PrepareBlockchainConfig(configRoot)
	if err != nil {
		return nil, err
	}
	globalVersion := int(prepared.GlobalVersion())
	return &configEpoch{
		prepared:      prepared,
		globalVersion: globalVersion,
	}, nil
}

// ------------------------------------------------------------------------
// cross-block account storage-stat LRU (thread-safe, bounded by entry count)
// ------------------------------------------------------------------------

// storageStatKey identifies a hot account across blocks.
type storageStatKey struct {
	workchain int32
	account   [32]byte
}

type storageStatValue struct {
	stateHash [32]byte // the shard-account state hash the stat corresponds to
	stat      *cell.Cell
}

// storageStatCache is a bounded LRU mapping an account to the storage-stat
// dictionary produced by its last replayed transaction, tagged with the
// resulting account state hash. A seed is only used when the tagged hash equals
// the account's PRE-state hash for the next replay, so a stale entry is ignored.
type storageStatCache struct {
	limit int

	mu      sync.Mutex
	entries map[storageStatKey]*list.Element
	order   *list.List
}

type storageStatCacheEntry struct {
	key   storageStatKey
	value storageStatValue
}

func newStorageStatCache(limit int) *storageStatCache {
	if limit < 1 {
		limit = 1
	}
	return &storageStatCache{
		limit:   limit,
		entries: map[storageStatKey]*list.Element{},
		order:   list.New(),
	}
}

func storageStatCacheKey(workchain int32, account []byte) (storageStatKey, bool) {
	var key storageStatKey
	if len(account) != len(key.account) {
		return storageStatKey{}, false
	}
	key.workchain = workchain
	copy(key.account[:], account)
	return key, true
}

// get returns the cached storage stat for the account only when the cached
// entry's state hash matches preStateHash exactly (else nil).
func (c *storageStatCache) get(workchain int32, account []byte, preStateHash []byte) *cell.Cell {
	if len(preStateHash) != 32 {
		return nil
	}
	key, ok := storageStatCacheKey(workchain, account)
	if !ok {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	element, ok := c.entries[key]
	if !ok {
		return nil
	}
	entry := element.Value.(*storageStatCacheEntry)
	if !bytes.Equal(entry.value.stateHash[:], preStateHash) {
		return nil
	}
	c.order.MoveToFront(element)
	return entry.value.stat
}

// put stores (stateHash -> stat) for the account, evicting the LRU tail when
// over the bound.
func (c *storageStatCache) put(workchain int32, account []byte, stateHash []byte, stat *cell.Cell) {
	if stat == nil || len(stateHash) != 32 {
		return
	}
	key, ok := storageStatCacheKey(workchain, account)
	if !ok {
		return
	}

	var value storageStatValue
	copy(value.stateHash[:], stateHash)
	value.stat = stat

	c.mu.Lock()
	defer c.mu.Unlock()
	if element, ok := c.entries[key]; ok {
		element.Value.(*storageStatCacheEntry).value = value
		c.order.MoveToFront(element)
		return
	}
	c.entries[key] = c.order.PushFront(&storageStatCacheEntry{key: key, value: value})
	for len(c.entries) > c.limit {
		oldest := c.order.Back()
		c.order.Remove(oldest)
		delete(c.entries, oldest.Value.(*storageStatCacheEntry).key)
	}
}

// ------------------------------------------------------------------------
// master config / prev-blocks / global libraries derivation (ported minimal
// versions; the tvm derives everything else internally)
// ------------------------------------------------------------------------

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
