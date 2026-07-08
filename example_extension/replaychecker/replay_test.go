package replaychecker

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"runtime"
	"sync"
	"testing"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"github.com/xssnick/tonutils-go/tvm/tuple"
)

// fatBlockReplayFixturePath points at the same real mainnet fat block fixture
// the tvm package replays (block 66519406, 269 txs / 234 accounts). The
// extension already builds against the local tonutils-go via a module replace,
// so the fixture is referenced through that same checkout instead of being
// duplicated (2.7MB).
const fatBlockReplayFixturePath = "../../../tonutils-go/tvm/testdata/tvm_replay_fat_block_66519406.json"

type fatFixture struct {
	MasterSeqno        uint32              `json:"master_seqno"`
	Block              fatFixtureBlockRef  `json:"block"`
	Accounts           int                 `json:"accounts"`
	Transactions       int                 `json:"transactions"`
	BlockBOCBase64     string              `json:"block_boc_base64"`
	PreviousAccounts   []fatFixtureAccount `json:"previous_accounts"`
	Config             fatFixtureConfig    `json:"config"`
	TransactionConfigs []fatFixtureTxC7    `json:"transaction_configs"`
}

type fatFixtureBlockRef struct {
	Workchain int32  `json:"workchain"`
	Shard     string `json:"shard"`
	Seqno     uint32 `json:"seqno"`
	RootHash  string `json:"root_hash"`
	FileHash  string `json:"file_hash"`
}

type fatFixtureAccount struct {
	Account               string `json:"account"`
	ShardAccountBOCBase64 string `json:"shard_account_boc_base64"`
	AccountRootHash       string `json:"account_root_hash"`
}

type fatFixtureConfig struct {
	GlobalVersion            int      `json:"global_version"`
	ConfigRootBOCBase64      string   `json:"config_root_boc_base64"`
	PrevBlocksStackBOCBase64 string   `json:"prev_blocks_stack_boc_base64"`
	LibrariesBOCBase64       []string `json:"libraries_boc_base64"`
}

type fatFixtureTxC7 struct {
	Account string `json:"account"`
	LT      uint64 `json:"lt"`
	Now     uint32 `json:"now"`
	BlockLT int64  `json:"block_lt"`
}

// loadedFatFixture is everything the checker needs to drive its per-account
// replay path against the fixture: the block-account work list, a reconstructed
// previous-state view, and the per-block execution context.
type loadedFatFixture struct {
	fixture       fatFixture
	blockID       ton.BlockIDExt
	accounts      []accountBlockWork
	previousViews []*shardStateView
	execCtx       *blockExecutionContext
	masterSeqno   uint32
}

func loadFatFixture(tb testing.TB) *loadedFatFixture {
	tb.Helper()

	raw, err := os.ReadFile(fatBlockReplayFixturePath)
	if err != nil {
		tb.Skipf("fat block fixture not available (%v)", err)
	}

	var fixture fatFixture
	if err = json.Unmarshal(raw, &fixture); err != nil {
		tb.Fatal(err)
	}

	blockRoot := fatCell(tb, fixture.BlockBOCBase64)
	var block tlb.Block
	if err = tlb.Parse(&block, blockRoot); err != nil {
		tb.Fatal(err)
	}

	loaded := loadedBlock{
		ID: ton.BlockIDExt{
			Workchain: fixture.Block.Workchain,
			Shard:     int64(mustHexUint(tb, fixture.Block.Shard)),
			SeqNo:     fixture.Block.Seqno,
			RootHash:  mustHex(tb, fixture.Block.RootHash),
			FileHash:  mustHex(tb, fixture.Block.FileHash),
		},
		Parsed: &block,
	}

	accounts, err := accountBlocks(&block)
	if err != nil {
		tb.Fatal(err)
	}
	if len(accounts) != fixture.Accounts {
		tb.Fatalf("loaded %d account blocks, want %d", len(accounts), fixture.Accounts)
	}
	if got := countTransactions(accounts); got != fixture.Transactions {
		tb.Fatalf("loaded %d transactions, want %d", got, fixture.Transactions)
	}

	previousView := reconstructPreviousView(tb, loaded.ID, fixture.PreviousAccounts)

	// Build the per-block execution context directly from the fixture config and
	// the block's own rand seed. This is the same immutable BlockContext the real
	// hook path assembles from the master state; here it is fed from the captured
	// config root / prev-blocks / libraries instead of a live master state.
	execCtx := fatBlockExecutionContext(tb, fixture, &block)

	masterSeqno := fixture.MasterSeqno
	if masterSeqno == 0 {
		masterSeqno = fixture.Block.Seqno
	}

	return &loadedFatFixture{
		fixture:       fixture,
		blockID:       loaded.ID,
		accounts:      accounts,
		previousViews: []*shardStateView{previousView},
		execCtx:       execCtx,
		masterSeqno:   masterSeqno,
	}
}

// reconstructPreviousView builds a shardStateView whose ShardAccounts dict is
// rebuilt from the fixture's previous_accounts. This exercises the checker's
// real accountFromPreviousStates dict descent instead of injecting accounts
// out of band.
func reconstructPreviousView(tb testing.TB, block ton.BlockIDExt, previous []fatFixtureAccount) *shardStateView {
	tb.Helper()

	dict, err := tlb.NewShardAccountsAugDict()
	if err != nil {
		tb.Fatal(err)
	}
	for _, acc := range previous {
		accountID := fatAccountBytes(tb, acc.Account)
		shardCell := fatCell(tb, acc.ShardAccountBOCBase64)
		if err = dict.Set(accountKey(accountID), shardCell); err != nil {
			tb.Fatalf("set previous account %s: %v", acc.Account, err)
		}
	}

	parsed := &tlb.ShardStateUnsplit{
		ShardIdent: tlb.ShardIdent{
			WorkchainID: block.Workchain,
			ShardPrefix: uint64(block.Shard),
		},
		Seqno: block.SeqNo,
	}
	parsed.Accounts.ShardAccounts = dict

	return &shardStateView{
		block:  block,
		parsed: parsed,
	}
}

func fatBlockExecutionContext(tb testing.TB, fixture fatFixture, block *tlb.Block) *blockExecutionContext {
	tb.Helper()

	if len(fixture.TransactionConfigs) == 0 {
		tb.Fatal("fixture has no transaction configs")
	}
	now := fixture.TransactionConfigs[0].Now
	blockLT := fixture.TransactionConfigs[0].BlockLT

	prepared, err := tvm.PrepareConfig(fatCell(tb, fixture.Config.ConfigRootBOCBase64))
	if err != nil {
		tb.Fatal(err)
	}
	blockCtx, err := prepared.NewBlockContext(tvm.BlockOptions{
		Now:        now,
		BlockLT:    blockLT,
		RandSeed:   block.Extra.RandSeed,
		PrevBlocks: fatTuple(tb, fixture.Config.PrevBlocksStackBOCBase64),
		Libraries:  fatCells(tb, fixture.Config.LibrariesBOCBase64),
	})
	if err != nil {
		tb.Fatal(err)
	}

	return &blockExecutionContext{
		block:         blockCtx,
		globalVersion: int(prepared.GlobalVersion()),
	}
}

// TestValidateAccountBlockFatFixture drives the migrated per-account replay path
// (validateAccountBlock) over every account of a real mainnet fat block and
// asserts the checker reports zero mismatches, i.e. every emulated transaction
// cell hash and the final account root hash matched the applied state.
//
// This exercises the whole migrated hot path against real data: the config
// epoch cache / prepared config, the per-block BlockContext with derived
// per-account rand seeds and per-tx logical time, PrepareAccount without a BOC
// roundtrip, single-parse chaining through NextAccount, the AccountStorageStat
// carry, and the happy-path compare.
func TestValidateAccountBlockFatFixture(t *testing.T) {
	loaded := loadFatFixture(t)
	v := newTestValidator()

	scope := newMismatchScope(loaded.masterSeqno, loaded.blockID.Workchain, v.mismatchLimit)

	var txs int
	block := loadedBlock{ID: loaded.blockID, Parsed: parsedBlock(t, loaded)}
	machine := tvm.NewTVM()
	for i := range loaded.accounts {
		_, n, err := v.validateAccountBlock(loaded.masterSeqno, loaded.previousViews, nil, block, loaded.execCtx, machine, loaded.accounts[i], scope)
		if err != nil {
			t.Fatalf("account %x: %v", loaded.accounts[i].Account, err)
		}
		txs += n
	}

	if txs != loaded.fixture.Transactions {
		t.Fatalf("replayed %d transactions, want %d", txs, loaded.fixture.Transactions)
	}
	if got := scope.len(); got != 0 {
		for _, m := range scope.snapshot() {
			t.Logf("mismatch: %s", m.Address)
		}
		t.Fatalf("expected 0 mismatches, got %d", got)
	}
}

// TestValidateAccountBlockDetectsDivergence corrupts the expected post-state
// hash of a transacting account and asserts the checker records a mismatch. This
// proves the mismatch DETECTION path (the whole point of the checker) still
// fires after the migration and happy-path optimization.
func TestValidateAccountBlockDetectsDivergence(t *testing.T) {
	loaded := loadFatFixture(t)
	v := newTestValidator()

	// Find a transacting account and corrupt its expected NewHash.
	var target *accountBlockWork
	accounts := make([]accountBlockWork, len(loaded.accounts))
	copy(accounts, loaded.accounts)
	for i := range accounts {
		if len(accounts[i].Txs) > 0 {
			corrupted := append([]byte(nil), accounts[i].Expected...)
			corrupted[0] ^= 0xFF
			accounts[i].Expected = corrupted
			target = &accounts[i]
			break
		}
	}
	if target == nil {
		t.Skip("fixture has no transacting account")
	}

	scope := newMismatchScope(loaded.masterSeqno, loaded.blockID.Workchain, v.mismatchLimit)
	block := loadedBlock{ID: loaded.blockID, Parsed: parsedBlock(t, loaded)}
	machine := tvm.NewTVM()

	if _, _, err := v.validateAccountBlock(loaded.masterSeqno, loaded.previousViews, nil, block, loaded.execCtx, machine, *target, scope); err != nil {
		t.Fatalf("validateAccountBlock returned error: %v", err)
	}
	if !scope.has(target.Account) {
		t.Fatalf("expected a mismatch to be recorded for corrupted account %x", target.Account)
	}
}

// TestValidateAccountBlockConcurrentLanes runs the same account set through the
// parallel lanes design (many goroutines, one shared immutable BlockContext /
// PreparedConfig, per-lane TVM handle) and asserts identical results to the
// sequential run. Run under -race to prove the lanes are data-race free.
func TestValidateAccountBlockConcurrentLanes(t *testing.T) {
	loaded := loadFatFixture(t)
	v := newTestValidator()

	scope := newMismatchScope(loaded.masterSeqno, loaded.blockID.Workchain, v.mismatchLimit)
	block := loadedBlock{ID: loaded.blockID, Parsed: parsedBlock(t, loaded)}

	workers := runtime.GOMAXPROCS(0)
	if workers > len(loaded.accounts) {
		workers = len(loaded.accounts)
	}
	if workers < 2 {
		workers = 2
	}

	var (
		wg   sync.WaitGroup
		next int64
		mu   sync.Mutex
		txs  int
		errs []error
	)
	nextIdx := func() int {
		mu.Lock()
		defer mu.Unlock()
		idx := int(next)
		next++
		return idx
	}
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			machine := tvm.NewTVM()
			if m, err := machine.WithGlobalVersion(loaded.execCtx.globalVersion); err == nil {
				machine = &m
			}
			for {
				idx := nextIdx()
				if idx >= len(loaded.accounts) {
					return
				}
				_, n, err := v.validateAccountBlock(loaded.masterSeqno, loaded.previousViews, nil, block, loaded.execCtx, machine, loaded.accounts[idx], scope)
				mu.Lock()
				txs += n
				if err != nil {
					errs = append(errs, err)
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	for _, err := range errs {
		t.Fatalf("lane error: %v", err)
	}
	if txs != loaded.fixture.Transactions {
		t.Fatalf("replayed %d transactions, want %d", txs, loaded.fixture.Transactions)
	}
	if got := scope.len(); got != 0 {
		for _, m := range scope.snapshot() {
			t.Logf("mismatch: %s", m.Address)
		}
		t.Fatalf("expected 0 mismatches across concurrent lanes, got %d", got)
	}
}

// TestValidateBlockParallelPath drives the full parallel validateBlock entry
// point (worker pool, shared block context, thread-safe mismatch scope) and
// asserts zero mismatches. This is the concurrency test to run under -race.
func TestValidateBlockParallelPath(t *testing.T) {
	loaded := loadFatFixture(t)
	v := newTestValidator()

	block := loadedBlock{ID: loaded.blockID, Parsed: parsedBlock(t, loaded)}
	scope := newMismatchScope(loaded.masterSeqno, loaded.blockID.Workchain, v.mismatchLimit)

	res, err := v.validateBlock(context.Background(), loaded.masterSeqno, loaded.accounts, loaded.previousViews, nil, block, loaded.execCtx, scope)
	if err != nil {
		t.Fatal(err)
	}
	if res.Transactions != loaded.fixture.Transactions {
		t.Fatalf("replayed %d transactions, want %d", res.Transactions, loaded.fixture.Transactions)
	}
	if got := scope.len(); got != 0 {
		t.Fatalf("expected 0 mismatches, got %d", got)
	}
}

// TestStorageStatCacheSeedGuard verifies the cross-block storage-stat LRU only
// seeds on an exact pre-state-hash match and evicts by entry count.
func TestStorageStatCacheSeedGuard(t *testing.T) {
	c := newStorageStatCache(2)
	acc := make([]byte, 32)
	acc[0] = 1
	hashA := bytesOf(0xAA)
	hashB := bytesOf(0xBB)
	stat := cell.BeginCell().MustStoreUInt(1, 8).EndCell()

	c.put(0, acc, hashA, stat)
	if got := c.get(0, acc, hashA); got == nil {
		t.Fatal("expected seed on exact pre-state hash match")
	}
	if got := c.get(0, acc, hashB); got != nil {
		t.Fatal("expected no seed on stale (mismatched) pre-state hash")
	}

	// Eviction by entry count: inserting two more distinct accounts drops the
	// oldest.
	acc2 := make([]byte, 32)
	acc2[0] = 2
	acc3 := make([]byte, 32)
	acc3[0] = 3
	c.put(0, acc2, hashA, stat)
	c.put(0, acc3, hashA, stat) // exceeds limit 2 -> evicts acc
	if got := c.get(0, acc, hashA); got != nil {
		t.Fatal("expected oldest entry to be evicted")
	}
	if got := c.get(0, acc3, hashA); got == nil {
		t.Fatal("expected newest entry to remain")
	}
}

func newTestValidator() *replayValidator {
	return &replayValidator{
		configs:       newConfigEpochCache(zerolog.Nop()),
		storageStats:  newStorageStatCache(storageStatCacheLimit),
		tvm:           tvm.NewTVM(),
		log:           zerolog.Nop(),
		mismatchLimit: -1,
	}
}

func parsedBlock(tb testing.TB, loaded *loadedFatFixture) *tlb.Block {
	tb.Helper()
	blockRoot := fatCell(tb, loaded.fixture.BlockBOCBase64)
	var block tlb.Block
	if err := tlb.Parse(&block, blockRoot); err != nil {
		tb.Fatal(err)
	}
	return &block
}

func bytesOf(b byte) []byte {
	out := make([]byte, 32)
	for i := range out {
		out[i] = b
	}
	return out
}

func fatCell(tb testing.TB, boc string) *cell.Cell {
	tb.Helper()
	raw, err := base64.StdEncoding.DecodeString(boc)
	if err != nil {
		tb.Fatal(err)
	}
	root, err := cell.FromBOC(raw)
	if err != nil {
		tb.Fatal(err)
	}
	return root
}

func fatCells(tb testing.TB, bocs []string) []*cell.Cell {
	tb.Helper()
	if len(bocs) == 0 {
		return nil
	}
	out := make([]*cell.Cell, 0, len(bocs))
	for _, boc := range bocs {
		out = append(out, fatCell(tb, boc))
	}
	return out
}

func fatTuple(tb testing.TB, boc string) tuple.Tuple {
	tb.Helper()
	if boc == "" {
		return tuple.Tuple{}
	}
	raw, err := base64.StdEncoding.DecodeString(boc)
	if err != nil {
		tb.Fatal(err)
	}
	root, err := cell.FromBOC(raw)
	if err != nil {
		tb.Fatal(err)
	}
	value, err := tlb.ParseStackValue(root.MustBeginParse())
	if err != nil {
		tb.Fatal(err)
	}
	return fatTupleValue(value).(tuple.Tuple)
}

func fatTupleValue(value any) any {
	items, ok := value.([]any)
	if !ok {
		if value == nil {
			return tuple.Tuple{}
		}
		return value
	}
	normalized := make([]any, len(items))
	for i, item := range items {
		normalized[i] = fatTupleValue(item)
	}
	return tuple.NewTupleValue(normalized...)
}

// fatAccountBytes decodes the fixture account id ("workchain:hex").
func fatAccountBytes(tb testing.TB, account string) []byte {
	tb.Helper()
	hexAcc := account
	if i := len(hexAcc) - 64; i > 0 {
		hexAcc = hexAcc[i:]
	}
	return mustHex(tb, hexAcc)
}

func mustHex(tb testing.TB, s string) []byte {
	tb.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		tb.Fatal(err)
	}
	return b
}

func mustHexUint(tb testing.TB, s string) uint64 {
	tb.Helper()
	b := mustHex(tb, s)
	var v uint64
	for _, x := range b {
		v = v<<8 | uint64(x)
	}
	return v
}
