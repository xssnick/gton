package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/big"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/xssnick/gton/internal/logutil"
	serviceState "github.com/xssnick/gton/service/state"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/storage/pebblestore"

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
	maxNodeBOCCells        = 4_000_000_000
)

type options struct {
	dbDir             string
	seqno             uint32
	blocks            uint
	untilKeyBlock     bool
	parallel          int
	prevBlocksSource  prevBlocksSource
	stateViewSource   stateViewSource
	mismatchesPath    string
	benchmarkPath     string
	mismatchLimit     int
	logLevel          zerolog.Level
	logJSON           bool
	cellCacheSize     int64
	cellMemTableSize  int
	cellShardMemTable int
	vmTraceAddress    string
	vmTraceLT         uint64
}

type loadedBlock struct {
	ID     ton.BlockIDExt
	Root   *cell.Cell
	Parsed *tlb.Block
	Meta   *storage.BlockMeta
	Data   []byte
}

type accountBlockWork struct {
	Account    []byte
	Expected   []byte
	ActualHash []byte
	Txs        []transactionWork
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
	store         *pebblestore.Store
	cache         *executionConfigCache
	tvm           *tvm.TVM
	log           zerolog.Logger
	stateViews    stateViewSource
	mismatchLimit int
	vmTraceAddr   string
	vmTraceLT     uint64
	benchmark     *tvmBenchmarkFixtureCollector
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

type mismatchReport struct {
	Accounts []accountMismatch `json:"accounts"`
}

type tvmBenchmarkFixture struct {
	Name                string                       `json:"name"`
	MasterSeqno         uint32                       `json:"master_seqno"`
	Block               benchmarkBlockRef            `json:"block"`
	Accounts            int                          `json:"accounts"`
	Transactions        int                          `json:"transactions"`
	BlockBOCBase64      string                       `json:"block_boc_base64"`
	PreviousStateProofs []benchmarkStateProof        `json:"previous_state_proofs"`
	PreviousAccounts    []benchmarkAccountState      `json:"previous_accounts"`
	Config              benchmarkExecutionConfig     `json:"config"`
	TransactionConfigs  []benchmarkTransactionConfig `json:"transaction_configs,omitempty"`
	Stats               benchmarkFixtureBuildStat    `json:"stats"`
}

type benchmarkBlockRef struct {
	Workchain int32  `json:"workchain"`
	Shard     string `json:"shard"`
	Seqno     uint32 `json:"seqno"`
	RootHash  string `json:"root_hash"`
	FileHash  string `json:"file_hash"`
}

type benchmarkStateProof struct {
	Block           benchmarkBlockRef `json:"block"`
	RootHash        string            `json:"root_hash"`
	ProofBOCBase64  string            `json:"proof_boc_base64"`
	ProofBOCBytes   int               `json:"proof_boc_bytes"`
	ProofRootHash   string            `json:"proof_root_hash"`
	ProofRootDepth  uint16            `json:"proof_root_depth"`
	ProofRootType   string            `json:"proof_root_type"`
	ProofRootIsLazy bool              `json:"proof_root_is_lazy"`
}

type benchmarkAccountState struct {
	Account               string `json:"account"`
	ShardAccountBOCBase64 string `json:"shard_account_boc_base64"`
	ShardAccountRootHash  string `json:"shard_account_root_hash"`
	AccountRootHash       string `json:"account_root_hash"`
}

type benchmarkExecutionConfig struct {
	GlobalVersion                int      `json:"global_version"`
	ConfigRootBOCBase64          string   `json:"config_root_boc_base64"`
	PrevBlocksStackBOCBase64     string   `json:"prev_blocks_stack_boc_base64"`
	UnpackedConfigStackBOCBase64 string   `json:"unpacked_config_stack_boc_base64"`
	LibrariesBOCBase64           []string `json:"libraries_boc_base64,omitempty"`
}

type benchmarkTransactionConfig struct {
	Account                      string `json:"account"`
	LT                           uint64 `json:"lt"`
	Now                          uint32 `json:"now"`
	BlockLT                      int64  `json:"block_lt"`
	LogicalTime                  int64  `json:"logical_time"`
	RandSeedBase64               string `json:"rand_seed_base64,omitempty"`
	IncomingValueStackBOCBase64  string `json:"incoming_value_stack_boc_base64,omitempty"`
	StorageFees                  int64  `json:"storage_fees"`
	DuePaymentNano               string `json:"due_payment_nano,omitempty"`
	PrecompiledGasStackBOCBase64 string `json:"precompiled_gas_stack_boc_base64,omitempty"`
	InMsgParamsStackBOCBase64    string `json:"in_msg_params_stack_boc_base64,omitempty"`
}

type benchmarkFixtureBuildStat struct {
	BlockBOCBytes int `json:"block_boc_bytes"`
	ProofBOCBytes int `json:"proof_boc_bytes"`
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
	State            *storage.BlockState
}

type prevBlocksSource string

const (
	prevBlocksSourceState prevBlocksSource = "state"
	prevBlocksSourceIndex prevBlocksSource = "index"
)

type stateViewSource string

const (
	stateViewSourceUpdate stateViewSource = "update"
	stateViewSourceDB     stateViewSource = "db"
)

func main() {
	opts, err := parseOptions()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(2)
	}

	logs := logutil.NewFactory(os.Stdout, logutil.Config{
		Level: opts.logLevel,
		JSON:  opts.logJSON,
	})
	logger := logs.Component("tvm-replay")
	cell.MaxBOCCells = maxNodeBOCCells

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info().
		Str("dir", opts.dbDir).
		Uint32("seqno", opts.seqno).
		Uint("blocks", opts.blocks).
		Bool("until_key_block", opts.untilKeyBlock).
		Int("parallel", opts.parallel).
		Str("prev_blocks_source", string(opts.prevBlocksSource)).
		Str("state_view_source", string(opts.stateViewSource)).
		Bool("read_only", true).
		Msg("opening node storage")

	storageLogger := logs.Category("pebblestore")
	store, err := pebblestore.Open(pebblestore.Options{
		Dir:                   opts.dbDir,
		Logger:                &storageLogger,
		ReadOnly:              true,
		CellCacheSize:         opts.cellCacheSize,
		CellMemTableSize:      opts.cellMemTableSize,
		CellShardMemTableSize: opts.cellShardMemTable,
	})
	if err != nil {
		logger.Error().Err(err).Str("dir", opts.dbDir).Msg("failed to open storage")
		os.Exit(1)
	}
	defer func() {
		if err := store.Close(); err != nil {
			logger.Error().Err(err).Msg("failed to close storage")
		}
	}()

	releaseCompactions := store.ThrottleCellCompactions()
	defer releaseCompactions()

	validator := &replayValidator{
		store:         store,
		cache:         newExecutionConfigCache(logger.With().Str("component", "tvm-replay-cache").Logger(), opts.prevBlocksSource),
		tvm:           tvm.NewTVM(),
		log:           logger,
		stateViews:    opts.stateViewSource,
		benchmark:     newTVMBenchmarkFixtureCollector(logger, opts.benchmarkPath),
		mismatch:      map[string]accountMismatch{},
		mismatchLimit: opts.mismatchLimit,
		vmTraceAddr:   opts.vmTraceAddress,
		vmTraceLT:     opts.vmTraceLT,
	}

	if err = validator.run(ctx, opts); err != nil {
		logger.Error().Err(err).Msg("replay failed")
		_ = validator.writeMismatches(opts.mismatchesPath)
		os.Exit(1)
	}
	if err = validator.writeMismatches(opts.mismatchesPath); err != nil {
		logger.Error().Err(err).Str("path", opts.mismatchesPath).Msg("failed to write mismatches")
		os.Exit(1)
	}
	if err = validator.writeBenchmarkFixture(opts.benchmarkPath); err != nil {
		logger.Error().Err(err).Str("path", opts.benchmarkPath).Msg("failed to write TVM benchmark fixture")
		os.Exit(1)
	}

	report := validator.report()
	logger.Info().
		Int("mismatches", len(report.Accounts)).
		Str("path", opts.mismatchesPath).
		Msg("replay finished")
}

func parseOptions() (options, error) {
	var opts options
	var seqnoSeen bool

	dbDir := flag.String("db", "data", "node storage directory")
	seqno := flag.Uint("seqno", 0, "masterchain seqno to start from")
	blocks := flag.Uint("blocks", 1, "number of next masterchain blocks to replay")
	untilKeyBlock := flag.Bool("until-key-block", false, "continue until the first key block after seqno is replayed")
	parallel := flag.Int("parallel", runtime.GOMAXPROCS(0), "maximum parallel master/shard validation jobs")
	prevBlocksSourceFlag := flag.String("prev-blocks-source", string(prevBlocksSourceState), "source for C7 prev blocks: state or index")
	stateViewSourceFlag := flag.String("state-view-source", string(stateViewSourceUpdate), "source for replay account state views: update or db")
	mismatchesPath := flag.String("mismatches", "tvm-replay-mismatches.json", "path to JSON mismatch report")
	benchmarkPath := flag.String("export-tvm-benchmark-fixture", "", "write the largest replayed block as a compact TVM benchmark fixture")
	mismatchLimit := flag.Int("mismatch-limit", -1, "maximum account mismatches to keep in JSON report, -1 keeps all, 0 disables collection")
	logLevel := flag.String("log-level", "info", "log level: trace, debug, info, warn, error")
	logJSON := flag.Bool("log-json", false, "write logs as JSON instead of pretty console")
	cellCacheSize := flag.Int64("cell-cache-size", 0, "pebble cell cache size, 0 uses storage default")
	cellMemTableSize := flag.Int("cell-memtable-size", 0, "pebble total cell memtable size, 0 uses storage default")
	cellShardMemTable := flag.Int("cell-shard-memtable-size", 0, "pebble cell shard memtable size, 0 uses storage default")
	vmTraceAddress := flag.String("vm-trace-address", "", "raw account address to trace in VM logs")
	vmTraceLT := flag.Uint64("vm-trace-lt", 0, "transaction LT to trace, 0 traces all traced-address transactions")
	flag.Parse()

	flag.Visit(func(f *flag.Flag) {
		if f.Name == "seqno" {
			seqnoSeen = true
		}
	})

	if !seqnoSeen {
		if flag.NArg() == 0 {
			return options{}, fmt.Errorf("seqno is required: pass -seqno N")
		}
		parsed, err := strconv.ParseUint(flag.Arg(0), 10, 32)
		if err != nil {
			return options{}, fmt.Errorf("parse positional seqno: %w", err)
		}
		*seqno = uint(parsed)
		seqnoSeen = true
	}
	if *seqno > math.MaxUint32 {
		return options{}, fmt.Errorf("seqno %d exceeds uint32", *seqno)
	}
	if *blocks == 0 && !*untilKeyBlock {
		return options{}, fmt.Errorf("blocks must be positive")
	}
	if *parallel <= 0 {
		return options{}, fmt.Errorf("parallel must be positive")
	}
	if *vmTraceAddress != "" && *parallel != 1 {
		return options{}, fmt.Errorf("vm trace requires -parallel 1")
	}
	prevSource := prevBlocksSource(*prevBlocksSourceFlag)
	switch prevSource {
	case prevBlocksSourceState, prevBlocksSourceIndex:
	default:
		return options{}, fmt.Errorf("invalid prev-blocks-source %q", *prevBlocksSourceFlag)
	}
	stateViews := stateViewSource(*stateViewSourceFlag)
	switch stateViews {
	case stateViewSourceUpdate, stateViewSourceDB:
	default:
		return options{}, fmt.Errorf("invalid state-view-source %q", *stateViewSourceFlag)
	}
	level, err := logutil.ParseLevel(*logLevel)
	if err != nil {
		return options{}, fmt.Errorf("invalid log level %q: %w", *logLevel, err)
	}
	if *cellCacheSize < 0 {
		return options{}, fmt.Errorf("cell-cache-size cannot be negative")
	}
	if *cellMemTableSize < 0 {
		return options{}, fmt.Errorf("cell-memtable-size cannot be negative")
	}
	if *cellShardMemTable < 0 {
		return options{}, fmt.Errorf("cell-shard-memtable-size cannot be negative")
	}
	if *mismatchLimit < -1 {
		return options{}, fmt.Errorf("mismatch-limit cannot be less than -1")
	}

	opts = options{
		dbDir:             *dbDir,
		seqno:             uint32(*seqno),
		blocks:            *blocks,
		untilKeyBlock:     *untilKeyBlock,
		parallel:          *parallel,
		prevBlocksSource:  prevSource,
		stateViewSource:   stateViews,
		mismatchesPath:    *mismatchesPath,
		benchmarkPath:     *benchmarkPath,
		mismatchLimit:     *mismatchLimit,
		logLevel:          level,
		logJSON:           *logJSON,
		cellCacheSize:     *cellCacheSize,
		cellMemTableSize:  *cellMemTableSize,
		cellShardMemTable: *cellShardMemTable,
		vmTraceAddress:    *vmTraceAddress,
		vmTraceLT:         *vmTraceLT,
	}
	return opts, nil
}

func (v *replayValidator) run(ctx context.Context, opts options) error {
	current, err := v.lookupMaster(ctx, opts.seqno)
	if err != nil {
		return fmt.Errorf("lookup start master block %d: %w", opts.seqno, err)
	}

	if opts.stateViewSource == stateViewSourceUpdate {
		return v.runWithStateUpdates(ctx, opts, current)
	}
	return v.runWithDBStates(ctx, opts, current)
}

func (v *replayValidator) runWithDBStates(ctx context.Context, opts options, current ton.BlockIDExt) error {
	processed := uint(0)
	for {
		if !opts.untilKeyBlock && processed >= opts.blocks {
			return nil
		}

		next, err := v.lookupMaster(ctx, current.SeqNo+1)
		if err != nil {
			return fmt.Errorf("lookup next master block %d: %w", current.SeqNo+1, err)
		}

		prevState, err := v.loadState(ctx, current)
		if err != nil {
			return fmt.Errorf("load previous master state %s: %w", storage.FormatBlockRef(current), err)
		}
		nextState, err := v.loadState(ctx, next)
		if err != nil {
			return fmt.Errorf("load next master state %s: %w", storage.FormatBlockRef(next), err)
		}

		started := time.Now()
		if err = v.validateMasterStep(ctx, prevState, nextState, opts.parallel); err != nil {
			return err
		}
		v.log.Info().
			Uint32("master_seqno", next.SeqNo).
			Dur("elapsed", time.Since(started)).
			Msg("master step replayed")

		processed++
		current = next
		if opts.untilKeyBlock {
			meta, err := v.store.BlockMeta(ctx, next)
			if err != nil {
				return fmt.Errorf("load next master meta %s: %w", storage.FormatBlockRef(next), err)
			}
			if meta.Has(storage.BlockMetaIsKeyBlock) {
				return nil
			}
		}
	}
}

func (v *replayValidator) runWithStateUpdates(ctx context.Context, opts options, current ton.BlockIDExt) error {
	currentMaster, err := v.loadStateRoot(ctx, current)
	if err != nil {
		return fmt.Errorf("load start master state root %s: %w", storage.FormatBlockRef(current), err)
	}

	currentMasterBlock, err := v.loadBlock(ctx, current)
	if err != nil {
		return fmt.Errorf("load start master block %s: %w", storage.FormatBlockRef(current), err)
	}
	currentShardBlocks, err := shardBlocksFromMasterBlock(currentMasterBlock.Parsed)
	if err != nil {
		return fmt.Errorf("load start master shard set %s: %w", storage.FormatBlockRef(currentMaster.Block), err)
	}
	currentShards, err := v.stateShells(ctx, currentShardBlocks)
	if err != nil {
		return err
	}

	processed := uint(0)
	for {
		if !opts.untilKeyBlock && processed >= opts.blocks {
			return nil
		}

		next, err := v.lookupMaster(ctx, currentMaster.Block.SeqNo+1)
		if err != nil {
			return fmt.Errorf("lookup next master block %d: %w", currentMaster.Block.SeqNo+1, err)
		}

		nextMasterBlock, err := v.loadBlock(ctx, next)
		if err != nil {
			return fmt.Errorf("load master block %s: %w", storage.FormatBlockRef(next), err)
		}

		started := time.Now()
		nextMaster, nextShards, err := v.validateMasterStepFromUpdates(ctx, currentMaster, currentShards, nextMasterBlock, opts.parallel)
		if err != nil {
			return err
		}
		v.log.Info().
			Uint32("master_seqno", next.SeqNo).
			Dur("elapsed", time.Since(started)).
			Msg("master step replayed")

		processed++
		currentMaster = nextMaster
		currentShards = nextShards
		if opts.untilKeyBlock && nextMasterBlock.Meta.Has(storage.BlockMetaIsKeyBlock) {
			return nil
		}
	}
}

func (v *replayValidator) lookupMaster(ctx context.Context, seqno uint32) (ton.BlockIDExt, error) {
	return v.store.LookupBlockBySeqNo(ctx, storage.BlockHistoryKey{
		Workchain: masterchainID,
		Shard:     masterchainShard,
	}, seqno)
}

func (v *replayValidator) validateMasterStep(ctx context.Context, previousMaster *storage.BlockState, nextMaster *storage.BlockState, parallel int) error {
	if previousMaster == nil || nextMaster == nil {
		return fmt.Errorf("master states are required")
	}

	nextMasterBlock, err := v.loadBlock(ctx, nextMaster.Block)
	if err != nil {
		return fmt.Errorf("load master block %s: %w", storage.FormatBlockRef(nextMaster.Block), err)
	}

	prevShardBlocks, err := serviceState.ShardBlocksFromMasterState(previousMaster)
	if err != nil {
		return fmt.Errorf("load previous master shard set %s: %w", storage.FormatBlockRef(previousMaster.Block), err)
	}
	nextShardBlocks, err := serviceState.ShardBlocksFromMasterState(nextMaster)
	if err != nil {
		return fmt.Errorf("load next master shard set %s: %w", storage.FormatBlockRef(nextMaster.Block), err)
	}

	currentShards := make(map[storage.BlockRootHash]*storage.BlockState, len(prevShardBlocks))
	for _, shard := range prevShardBlocks {
		shardState, err := v.loadState(ctx, shard)
		if err != nil {
			return fmt.Errorf("load previous shard state %s: %w", storage.FormatBlockRef(shard), err)
		}
		currentShards[storage.BlockKey(shard)] = shardState
	}

	runnerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	resolver := newReplayShardResolver(v, previousMaster, nextMaster.Block, currentShards)
	sem := make(chan struct{}, parallel)
	errCh := make(chan error, len(nextShardBlocks)+1)
	var wg sync.WaitGroup

	runJob := func(fn func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-runnerCtx.Done():
				return
			}
			defer func() { <-sem }()

			if err := fn(runnerCtx); err != nil {
				errCh <- err
				cancel()
			}
		}()
	}

	runJob(func(ctx context.Context) error {
		execCtx, err := v.cache.blockContext(ctx, v.lookupMaster, previousMaster.Block, previousMaster.Cell, nextMasterBlock.Parsed.BlockInfo.GenUtime)
		if err != nil {
			return fmt.Errorf("prepare master execution context %s: %w", storage.FormatBlockRef(nextMaster.Block), err)
		}
		_, err = v.validateBlock(ctx, nextMaster.Block.SeqNo, []*storage.BlockState{previousMaster}, nextMaster, nextMasterBlock, execCtx)
		return err
	})

	for _, target := range nextShardBlocks {
		target := target
		runJob(func(ctx context.Context) error {
			_, err := resolver.resolve(ctx, target)
			return err
		})
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func (v *replayValidator) validateMasterStepFromUpdates(ctx context.Context, previousMaster *storage.BlockState, currentShards map[storage.BlockRootHash]*storage.BlockState, nextMasterBlock loadedBlock, parallel int) (*storage.BlockState, map[storage.BlockRootHash]*storage.BlockState, error) {
	if previousMaster == nil {
		return nil, nil, fmt.Errorf("previous master state is required")
	}
	if previousMaster.Cell == nil {
		return nil, nil, fmt.Errorf("previous master state %s has no root cell", storage.FormatBlockRef(previousMaster.Block))
	}

	nextShardBlocks, err := shardBlocksFromMasterBlock(nextMasterBlock.Parsed)
	if err != nil {
		return nil, nil, fmt.Errorf("load next master shard set %s: %w", storage.FormatBlockRef(nextMasterBlock.ID), err)
	}

	runnerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	resolver := newReplayShardResolver(v, previousMaster, nextMasterBlock.ID, currentShards)
	sem := make(chan struct{}, parallel)
	errCh := make(chan error, len(nextShardBlocks)+1)
	var wg sync.WaitGroup

	var nextMaster *storage.BlockState
	nextShards := make(map[storage.BlockRootHash]*storage.BlockState, len(nextShardBlocks))
	var resultMu sync.Mutex

	runJob := func(fn func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-runnerCtx.Done():
				return
			}
			defer func() { <-sem }()

			if err := fn(runnerCtx); err != nil {
				errCh <- err
				cancel()
			}
		}()
	}

	runJob(func(ctx context.Context) error {
		execCtx, err := v.cache.blockContext(ctx, v.lookupMaster, previousMaster.Block, previousMaster.Cell, nextMasterBlock.Parsed.BlockInfo.GenUtime)
		if err != nil {
			return fmt.Errorf("prepare master execution context %s: %w", storage.FormatBlockRef(nextMasterBlock.ID), err)
		}
		result, err := v.validateBlock(ctx, nextMasterBlock.ID.SeqNo, []*storage.BlockState{previousMaster}, blockStateShellFromLoadedBlock(nextMasterBlock), nextMasterBlock, execCtx)
		if err != nil {
			return err
		}

		resultMu.Lock()
		nextMaster = result.State
		resultMu.Unlock()
		return nil
	})

	for _, target := range nextShardBlocks {
		target := target
		runJob(func(ctx context.Context) error {
			state, err := resolver.resolve(ctx, target)
			if err != nil {
				return err
			}

			resultMu.Lock()
			nextShards[storage.BlockKey(target)] = state
			resultMu.Unlock()
			return nil
		})
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return nil, nil, err
		}
	}
	if nextMaster == nil {
		return nil, nil, fmt.Errorf("master block %s produced no state", storage.FormatBlockRef(nextMasterBlock.ID))
	}
	for _, target := range nextShardBlocks {
		if nextShards[storage.BlockKey(target)] == nil {
			return nil, nil, fmt.Errorf("shard block %s produced no state", storage.FormatBlockRef(target))
		}
	}
	return nextMaster, nextShards, nil
}

func (v *replayValidator) stateShells(ctx context.Context, blocks []ton.BlockIDExt) (map[storage.BlockRootHash]*storage.BlockState, error) {
	states := make(map[storage.BlockRootHash]*storage.BlockState, len(blocks))
	for _, block := range blocks {
		state, err := v.stateShell(ctx, block)
		if err != nil {
			return nil, err
		}
		states[storage.BlockKey(block)] = state
	}
	return states, nil
}

func (v *replayValidator) stateShell(ctx context.Context, block ton.BlockIDExt) (*storage.BlockState, error) {
	meta, err := v.store.BlockMeta(ctx, block)
	if err != nil {
		return nil, fmt.Errorf("load block meta %s: %w", storage.FormatBlockRef(block), err)
	}
	if len(meta.StateRootHash) != 32 {
		return nil, fmt.Errorf("block meta for %s has no state root hash", storage.FormatBlockRef(block))
	}

	state := &storage.BlockState{
		Block:         block,
		StateRootHash: bytes.Clone(meta.StateRootHash),
		StateFileHash: bytes.Clone(meta.StateFileHash),
	}
	if err = v.setStateMasterchainRefFromMeta(ctx, state, meta); err != nil {
		return nil, err
	}
	return state, nil
}

func (v *replayValidator) setStateMasterchainRefFromMeta(ctx context.Context, state *storage.BlockState, meta *storage.BlockMeta) error {
	if state == nil || meta == nil || (state.Block.Workchain == masterchainID && state.Block.Shard == masterchainShard) {
		return nil
	}
	if !meta.MasterchainRefKnown() {
		return fmt.Errorf("%w: state %s has no masterchain ref", storage.ErrNotFound, storage.FormatBlockRef(state.Block))
	}

	ref, err := v.store.LookupBlockBySeqNo(ctx, storage.BlockHistoryKey{Workchain: masterchainID, Shard: masterchainShard}, meta.MasterchainRefSeqno)
	if err != nil {
		return fmt.Errorf("lookup masterchain ref #%d: %w", meta.MasterchainRefSeqno, err)
	}
	if ref.Workchain != masterchainID || ref.Shard != masterchainShard || ref.SeqNo != meta.MasterchainRefSeqno || len(ref.RootHash) != 32 || len(ref.FileHash) != 32 {
		return fmt.Errorf("lookup masterchain ref #%d returned invalid block %s", meta.MasterchainRefSeqno, storage.FormatBlockRef(ref))
	}
	state.MasterchainRef = &ref
	return nil
}

func shardBlocksFromMasterBlock(block *tlb.Block) ([]ton.BlockIDExt, error) {
	if block == nil || block.Extra == nil || block.Extra.Custom == nil {
		return nil, fmt.Errorf("master block has no custom extra")
	}
	if block.Extra.Custom.ShardHashes == nil {
		return nil, fmt.Errorf("master block has no shard hashes")
	}

	shards, err := ton.LoadShardsFromHashes(block.Extra.Custom.ShardHashes, false)
	if err != nil {
		return nil, err
	}

	res := make([]ton.BlockIDExt, 0, len(shards))
	for _, shard := range shards {
		if shard != nil {
			res = append(res, *shard)
		}
	}
	sort.Slice(res, func(i, j int) bool {
		if res[i].Workchain != res[j].Workchain {
			return res[i].Workchain < res[j].Workchain
		}
		if res[i].Shard != res[j].Shard {
			return uint64(res[i].Shard) < uint64(res[j].Shard)
		}
		return res[i].SeqNo < res[j].SeqNo
	})
	return res, nil
}

func (v *replayValidator) loadBlock(ctx context.Context, id ton.BlockIDExt) (loadedBlock, error) {
	data, err := v.store.BlockData(ctx, id)
	if err != nil {
		return loadedBlock{}, err
	}
	root, err := parseBlockBOC(data)
	if err != nil {
		return loadedBlock{}, fmt.Errorf("parse block BOC: %w", err)
	}
	rootHash := root.HashKey()
	if !bytes.Equal(rootHash[:], id.RootHash) {
		return loadedBlock{}, fmt.Errorf("block root hash mismatch")
	}
	fileHash := sha256.Sum256(data)
	if !bytes.Equal(fileHash[:], id.FileHash) {
		return loadedBlock{}, fmt.Errorf("block file hash mismatch")
	}

	parsed, err := storage.ParseVerifiedBlockCell(id, root)
	if err != nil {
		return loadedBlock{}, err
	}
	meta, err := storage.BuildBlockMetaFromParsedBlock(id, parsed)
	if err != nil {
		return loadedBlock{}, err
	}
	return loadedBlock{
		ID:     id,
		Root:   root,
		Parsed: parsed,
		Meta:   meta,
		Data:   data,
	}, nil
}

func parseBlockBOC(data []byte) (*cell.Cell, error) {
	roots, _, err := cell.FromBOCMultiRootReader(cell.NewBOCNoCopyReader(data), cell.BOCParseOptions{
		NoCopyPayload: true,
	})
	if err != nil {
		return nil, err
	}
	if len(roots) != 1 {
		return nil, fmt.Errorf("boc should contain exactly one root, got %d", len(roots))
	}
	return roots[0], nil
}

func (v *replayValidator) loadState(ctx context.Context, block ton.BlockIDExt) (*storage.BlockState, error) {
	meta, err := v.store.BlockMeta(ctx, block)
	if err != nil {
		return nil, err
	}
	if len(meta.StateRootHash) != 32 {
		return nil, fmt.Errorf("block meta for %s has no state root hash", storage.FormatBlockRef(block))
	}
	root, err := v.store.LoadStateCellTree(ctx, block, meta.StateRootHash)
	if err != nil {
		return nil, fmt.Errorf("load lazy state root: %w", err)
	}
	state, err := storage.ParseStateProof(&block, root, nil, meta.StateRootHash, meta.StateFileHash)
	if err != nil {
		return nil, err
	}
	if err = v.setStateMasterchainRefFromMeta(ctx, state, meta); err != nil {
		return nil, err
	}
	return state, nil
}

func (v *replayValidator) loadStateRoot(ctx context.Context, block ton.BlockIDExt) (*storage.BlockState, error) {
	meta, err := v.store.BlockMeta(ctx, block)
	if err != nil {
		return nil, err
	}
	if len(meta.StateRootHash) != 32 {
		return nil, fmt.Errorf("block meta for %s has no state root hash", storage.FormatBlockRef(block))
	}
	root, err := v.store.LoadStateCellTree(ctx, block, meta.StateRootHash)
	if err != nil {
		return nil, fmt.Errorf("load lazy state root: %w", err)
	}

	state := &storage.BlockState{
		Block:         block,
		StateRootHash: bytes.Clone(meta.StateRootHash),
		StateFileHash: bytes.Clone(meta.StateFileHash),
		Cell:          root,
	}
	if err = v.setStateMasterchainRefFromMeta(ctx, state, meta); err != nil {
		return nil, err
	}
	return state, nil
}

func (v *replayValidator) validateBlock(ctx context.Context, masterSeqno uint32, previous []*storage.BlockState, expected *storage.BlockState, block loadedBlock, execCtx *blockExecutionContext) (blockValidationResult, error) {
	select {
	case <-ctx.Done():
		return blockValidationResult{}, ctx.Err()
	default:
	}
	accounts, err := accountBlocks(block.Parsed)
	if err != nil {
		return blockValidationResult{}, fmt.Errorf("load account blocks %s: %w", storage.FormatBlockRef(block.ID), err)
	}

	previousViews, expectedView, err := v.blockStateViews(previous, expected, block)
	if err != nil {
		return blockValidationResult{}, err
	}

	result := blockValidationResult{
		Accounts: len(accounts),
		State:    blockStateFromView(block, expectedView),
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
		emulationElapsed, txs, err := v.validateAccountBlock(masterSeqno, previousViews, expectedView, block, execCtx, &machine, account)
		result.EmulationElapsed += emulationElapsed
		result.Transactions += txs
		if err != nil {
			return result, err
		}
	}

	blockLog.Info().
		Int("accounts", result.Accounts).
		Int("transactions", result.Transactions).
		Dur("tx_emulation_elapsed", result.EmulationElapsed).
		Msg("block transaction replay finished")

	if v.benchmark != nil {
		if err = v.benchmark.consider(masterSeqno, previousViews, expectedView, block, execCtx, v.tvm, accounts); err != nil {
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

	addr := accountAddress(block.ID.Workchain, account.Account)
	var details accountMismatchDetails
	if v.collectsMismatches() {
		details = accountMismatchDetails{
			fromShardAccountBOCBase64: shardAccountBOCBase64(current),
			toShardAccountBOCBase64:   shardAccountBOCBase64(expectedShardAccountForMismatch(expected, account.Account)),
		}
	}
	addMismatch := func() {
		v.addMismatchWithDetails(masterSeqno, block.ID.Workchain, account.Account, details)
	}
	setFirstTx := func(before *tlb.ShardAccount, txDetails accountMismatchTx) {
		if details.firstTx == nil {
			details.fromShardAccountBOCBase64 = shardAccountBOCBase64(before)
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
		var txDetails accountMismatchTx
		var elapsed time.Duration
		var err error
		var finishTrace func()

		if tx.Parsed.IO.In == nil || tx.InMsgCell == nil {
			desc, ok := tx.Parsed.Description.(tlb.TransactionDescriptionTickTock)
			if !ok {
				addMismatch()
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
			if v.collectsMismatches() {
				txDetails = execCtx.accountMismatchTxDetails(tx, nil, cfg)
			}

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
			if v.collectsMismatches() {
				txDetails = execCtx.accountMismatchTxDetails(tx, tx.InMsgCell, cfg)
			}

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
			setFirstTx(current, txDetails)
			addMismatch()
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
			setFirstTx(current, txDetails)
			addMismatch()
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
			txDetails.GotTxHash = hex.EncodeToString(gotTxHash[:])
			txDetails.GotTxBOCBase64 = cellBOCBase64(res.TransactionCell)
			setFirstTx(current, txDetails)
			addMismatch()
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

	v.compareAccountResult(masterSeqno, block.ID, expected, account, details, current)
	return total, txCount, nil
}

func (v *replayValidator) compareAccountResult(masterSeqno uint32, block ton.BlockIDExt, expectedView *shardStateView, account accountBlockWork, details accountMismatchDetails, got *tlb.ShardAccount) {
	addr := accountAddress(block.Workchain, account.Account)
	expectedAccountHash := account.Expected
	var expectedAccountHashKey cell.Hash
	if v.collectsMismatches() {
		details.gotShardAccountBOCBase64 = shardAccountBOCBase64(got)
	}
	addMismatch := func() {
		v.addMismatchWithDetails(masterSeqno, block.Workchain, account.Account, details)
	}
	defer v.addMismatchDetailsIfExists(masterSeqno, block.Workchain, account.Account, details)

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

func (v *replayValidator) report() mismatchReport {
	v.mu.Lock()
	defer v.mu.Unlock()

	accounts := make([]accountMismatch, 0, len(v.mismatch))
	for _, mismatch := range v.mismatch {
		accounts = append(accounts, mismatch)
	}
	sort.Slice(accounts, func(i, j int) bool {
		if accounts[i].MasterSeqno != accounts[j].MasterSeqno {
			return accounts[i].MasterSeqno < accounts[j].MasterSeqno
		}
		if accounts[i].Workchain != accounts[j].Workchain {
			return accounts[i].Workchain < accounts[j].Workchain
		}
		return accounts[i].Address < accounts[j].Address
	})
	return mismatchReport{Accounts: accounts}
}

func (v *replayValidator) writeMismatches(path string) error {
	report := v.report()
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func (v *replayValidator) writeBenchmarkFixture(path string) error {
	if path == "" || v.benchmark == nil {
		return nil
	}

	fixture, ok := v.benchmark.fixture()
	if !ok {
		return fmt.Errorf("no block was replayed for TVM benchmark fixture")
	}

	data, err := json.MarshalIndent(fixture, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

type tvmBenchmarkFixtureCollector struct {
	log zerolog.Logger

	mu   sync.Mutex
	best *tvmBenchmarkFixture
}

func newTVMBenchmarkFixtureCollector(log zerolog.Logger, path string) *tvmBenchmarkFixtureCollector {
	if path == "" {
		return nil
	}
	return &tvmBenchmarkFixtureCollector{log: log}
}

func (c *tvmBenchmarkFixtureCollector) fixture() (tvmBenchmarkFixture, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.best == nil {
		return tvmBenchmarkFixture{}, false
	}
	return *c.best, true
}

func (c *tvmBenchmarkFixtureCollector) consider(masterSeqno uint32, previous []*shardStateView, expected *shardStateView, block loadedBlock, execCtx *blockExecutionContext, baseTVM *tvm.TVM, accounts []accountBlockWork) error {
	txCount := countTransactions(accounts)
	if txCount == 0 {
		return nil
	}

	c.mu.Lock()
	bestTxCount := 0
	if c.best != nil {
		bestTxCount = c.best.Transactions
	}
	c.mu.Unlock()
	if txCount <= bestTxCount {
		return nil
	}

	fixture, err := buildTVMBenchmarkFixture(masterSeqno, previous, expected, block, execCtx, baseTVM, accounts)
	if err != nil {
		c.log.Warn().
			Err(err).
			Uint32("master_seqno", masterSeqno).
			Str("block", storage.FormatBlockRef(block.ID)).
			Int("transactions", txCount).
			Msg("skipping TVM benchmark fixture candidate")
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.best != nil && fixture.Transactions <= c.best.Transactions {
		return nil
	}
	c.best = &fixture
	c.log.Info().
		Uint32("master_seqno", masterSeqno).
		Str("block", storage.FormatBlockRef(block.ID)).
		Int("accounts", fixture.Accounts).
		Int("transactions", fixture.Transactions).
		Int("block_boc_bytes", fixture.Stats.BlockBOCBytes).
		Int("proof_boc_bytes", fixture.Stats.ProofBOCBytes).
		Msg("selected TVM benchmark fixture candidate")
	return nil
}

func buildTVMBenchmarkFixture(masterSeqno uint32, previous []*shardStateView, expected *shardStateView, block loadedBlock, execCtx *blockExecutionContext, baseTVM *tvm.TVM, accounts []accountBlockWork) (tvmBenchmarkFixture, error) {
	proofViews, proofs, err := benchmarkStateProofViews(previous)
	if err != nil {
		return tvmBenchmarkFixture{}, err
	}

	machine, err := baseTVM.WithGlobalVersion(execCtx.master.bundle.globalVersion)
	if err != nil {
		return tvmBenchmarkFixture{}, err
	}

	var txCount int
	txConfigs := make([]benchmarkTransactionConfig, 0, countTransactions(accounts))
	accountStates := make([]benchmarkAccountState, 0, len(accounts))
	for _, account := range accounts {
		accountState, configs, txs, err := replayAccountBlockForBenchmarkProof(block, execCtx, &machine, proofViews, expected, account)
		if err != nil {
			return tvmBenchmarkFixture{}, err
		}
		accountStates = append(accountStates, accountState)
		txConfigs = append(txConfigs, configs...)
		txCount += txs
	}

	stateProofs := make([]benchmarkStateProof, 0, len(proofs))
	var proofBOCBytes int
	for _, item := range proofs {
		proof, err := item.builder.CreateProof()
		if err != nil {
			return tvmBenchmarkFixture{}, fmt.Errorf("create state proof for %s: %w", storage.FormatBlockRef(item.block), err)
		}
		rootHash := item.root.HashKey(0)
		if err = cell.CheckProof(proof, rootHash[:]); err != nil {
			return tvmBenchmarkFixture{}, fmt.Errorf("check state proof for %s: %w", storage.FormatBlockRef(item.block), err)
		}

		proofBOC := proof.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false})
		proofBOCBytes += len(proofBOC)
		stateProofs = append(stateProofs, benchmarkStateProof{
			Block:           benchmarkBlockRefFromID(item.block),
			RootHash:        hex.EncodeToString(rootHash[:]),
			ProofBOCBase64:  base64.StdEncoding.EncodeToString(proofBOC),
			ProofBOCBytes:   len(proofBOC),
			ProofRootHash:   hex.EncodeToString(proof.Hash()),
			ProofRootDepth:  proof.Depth(),
			ProofRootType:   fmt.Sprint(proof.GetType()),
			ProofRootIsLazy: proof.IsLazy(),
		})
	}

	blockBOC := block.Root.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false})
	return tvmBenchmarkFixture{
		Name:                fmt.Sprintf("master_%d_%x", masterSeqno, storage.BlockKey(block.ID)),
		MasterSeqno:         masterSeqno,
		Block:               benchmarkBlockRefFromID(block.ID),
		Accounts:            len(accounts),
		Transactions:        txCount,
		BlockBOCBase64:      base64.StdEncoding.EncodeToString(blockBOC),
		PreviousStateProofs: stateProofs,
		PreviousAccounts:    accountStates,
		Config: benchmarkExecutionConfig{
			GlobalVersion:                execCtx.master.bundle.globalVersion,
			ConfigRootBOCBase64:          cellBOCBase64(execCtx.master.bundle.config.Root),
			PrevBlocksStackBOCBase64:     stackValueBOCBase64(execCtx.master.prevBlocks),
			UnpackedConfigStackBOCBase64: stackValueBOCBase64(execCtx.unpacked),
			LibrariesBOCBase64:           cellsBOCBase64(execCtx.master.bundle.libraries),
		},
		TransactionConfigs: txConfigs,
		Stats: benchmarkFixtureBuildStat{
			BlockBOCBytes: len(blockBOC),
			ProofBOCBytes: proofBOCBytes,
		},
	}, nil
}

type benchmarkStateProofView struct {
	block   ton.BlockIDExt
	root    *cell.Cell
	builder *cell.MerkleProofBuilder
}

func benchmarkStateProofViews(previous []*shardStateView) ([]*shardStateView, []benchmarkStateProofView, error) {
	views := make([]*shardStateView, 0, len(previous))
	proofs := make([]benchmarkStateProofView, 0, len(previous))
	for _, state := range previous {
		if state == nil || state.root == nil {
			return nil, nil, fmt.Errorf("benchmark fixture needs unsplit previous state root")
		}

		builder := cell.NewMerkleProofBuilder(state.root)
		loader, err := builder.Root().BeginParse()
		if err != nil {
			return nil, nil, fmt.Errorf("parse previous state %s: %w", storage.FormatBlockRef(state.block), err)
		}
		var parsed tlb.ShardStateUnsplit
		if err = tlb.LoadFromCell(&parsed, loader); err != nil {
			return nil, nil, fmt.Errorf("load previous state %s: %w", storage.FormatBlockRef(state.block), err)
		}

		views = append(views, newShardStateViewFromParsed(state.block, builder.Root(), &parsed))
		proofs = append(proofs, benchmarkStateProofView{
			block:   state.block,
			root:    state.root,
			builder: builder,
		})
	}
	return views, proofs, nil
}

func replayAccountBlockForBenchmarkProof(block loadedBlock, execCtx *blockExecutionContext, machine *tvm.TVM, previous []*shardStateView, expected *shardStateView, account accountBlockWork) (benchmarkAccountState, []benchmarkTransactionConfig, int, error) {
	current, err := accountFromPreviousStates(previous, block.ID.Workchain, account.Account)
	if err != nil {
		return benchmarkAccountState{}, nil, 0, fmt.Errorf("load previous account %s: %w", accountAddressRaw(block.ID.Workchain, account.Account), err)
	}
	if current != nil && current.Account != nil {
		if _, err = current.Account.PrewarmRecursive(0); err != nil {
			return benchmarkAccountState{}, nil, 0, fmt.Errorf("prewarm previous account %s: %w", accountAddressRaw(block.ID.Workchain, account.Account), err)
		}
	}
	current, err = materializeShardAccount(current)
	if err != nil {
		return benchmarkAccountState{}, nil, 0, fmt.Errorf("materialize previous account %s: %w", accountAddressRaw(block.ID.Workchain, account.Account), err)
	}

	accountState, err := benchmarkAccountStateFromShard(block.ID.Workchain, account.Account, current)
	if err != nil {
		return benchmarkAccountState{}, nil, 0, err
	}

	var accountStorageStat *cell.Cell
	var txCount int
	txConfigs := make([]benchmarkTransactionConfig, 0, len(account.Txs))
	for _, tx := range account.Txs {
		txCount++

		var res *tvm.TransactionExecutionResult
		var cfg tvm.TransactionEmulationConfig
		if tx.Parsed.IO.In == nil || tx.InMsgCell == nil {
			desc, ok := tx.Parsed.Description.(tlb.TransactionDescriptionTickTock)
			if !ok {
				return accountState, txConfigs, txCount, fmt.Errorf("transaction %s lt=%d has no input message", accountAddressRaw(block.ID.Workchain, account.Account), tx.Parsed.LT)
			}
			cfg, err = execCtx.tickTockTransactionConfig(block, current, tx.Parsed)
			if err != nil {
				return accountState, txConfigs, txCount, fmt.Errorf("build tick/tock config %s lt=%d: %w", accountAddressRaw(block.ID.Workchain, account.Account), tx.Parsed.LT, err)
			}
			cfg.AccountStorageStat = accountStorageStat
			res, err = machine.EmulateTickTockTransaction(current, desc.IsTock, cfg)
			if err != nil {
				return accountState, txConfigs, txCount, fmt.Errorf("emulate tick/tock %s lt=%d: %w", accountAddressRaw(block.ID.Workchain, account.Account), tx.Parsed.LT, err)
			}
		} else {
			cfg, err = execCtx.transactionConfig(block, current, tx.Parsed)
			if err != nil {
				return accountState, txConfigs, txCount, fmt.Errorf("build transaction config %s lt=%d: %w", accountAddressRaw(block.ID.Workchain, account.Account), tx.Parsed.LT, err)
			}
			cfg.AccountStorageStat = accountStorageStat
			res, err = machine.EmulateTransaction(current, tx.InMsgCell, cfg)
			if err != nil {
				return accountState, txConfigs, txCount, fmt.Errorf("emulate transaction %s lt=%d: %w", accountAddressRaw(block.ID.Workchain, account.Account), tx.Parsed.LT, err)
			}
		}
		if res == nil || res.TransactionCell == nil || res.ShardAccount == nil {
			return accountState, txConfigs, txCount, fmt.Errorf("emulate transaction %s lt=%d returned incomplete result", accountAddressRaw(block.ID.Workchain, account.Account), tx.Parsed.LT)
		}

		gotTxHash := res.TransactionCell.HashKey()
		if gotTxHash != tx.Hash {
			return accountState, txConfigs, txCount, fmt.Errorf("transaction hash mismatch %s lt=%d: got=%x want=%x", accountAddressRaw(block.ID.Workchain, account.Account), tx.Parsed.LT, gotTxHash[:], tx.Hash[:])
		}
		txConfigs = append(txConfigs, benchmarkTransactionConfig{
			Account:                      accountAddressRaw(block.ID.Workchain, account.Account),
			LT:                           tx.Parsed.LT,
			Now:                          cfg.Now,
			BlockLT:                      cfg.BlockLT,
			LogicalTime:                  cfg.LogicalTime,
			RandSeedBase64:               base64.StdEncoding.EncodeToString(cfg.RandSeed),
			IncomingValueStackBOCBase64:  stackValueBOCBase64(cfg.IncomingValue),
			StorageFees:                  cfg.StorageFees,
			DuePaymentNano:               bigIntString(cfg.DuePayment),
			PrecompiledGasStackBOCBase64: stackValueBOCBase64(cfg.PrecompiledGasUsage),
			InMsgParamsStackBOCBase64:    stackValueBOCBase64(cfg.InMsgParams),
		})

		current = res.ShardAccount
		accountStorageStat = res.AccountStorageStat
	}

	expectedHash := account.Expected
	var expectedHashKey cell.Hash
	expectedShard, err := expected.account(account.Account)
	if err == nil {
		expectedHashKey = expectedShard.Account.HashKey()
		expectedHash = expectedHashKey[:]
	} else if !errors.Is(err, storage.ErrNotFound) {
		return accountState, txConfigs, txCount, fmt.Errorf("load expected account %s: %w", accountAddressRaw(block.ID.Workchain, account.Account), err)
	}
	if current == nil || current.Account == nil {
		return accountState, txConfigs, txCount, fmt.Errorf("missing final account %s", accountAddressRaw(block.ID.Workchain, account.Account))
	}
	if gotHash := current.Account.HashKey(); !bytes.Equal(gotHash[:], expectedHash) {
		return accountState, txConfigs, txCount, fmt.Errorf("account hash mismatch %s: got=%x want=%x", accountAddressRaw(block.ID.Workchain, account.Account), gotHash[:], expectedHash)
	}
	return accountState, txConfigs, txCount, nil
}

func benchmarkAccountStateFromShard(workchain int32, account []byte, shard *tlb.ShardAccount) (benchmarkAccountState, error) {
	if shard == nil || shard.Account == nil {
		return benchmarkAccountState{}, fmt.Errorf("missing previous account %s", accountAddressRaw(workchain, account))
	}

	root, err := tlb.ToCell(shard)
	if err != nil {
		return benchmarkAccountState{}, fmt.Errorf("serialize previous account %s: %w", accountAddressRaw(workchain, account), err)
	}
	rootHash := root.HashKey()
	accountHash := shard.Account.HashKey()
	return benchmarkAccountState{
		Account:               accountAddressRaw(workchain, account),
		ShardAccountBOCBase64: cellBOCBase64(root),
		ShardAccountRootHash:  hex.EncodeToString(rootHash[:]),
		AccountRootHash:       hex.EncodeToString(accountHash[:]),
	}, nil
}

func benchmarkBlockRefFromID(id ton.BlockIDExt) benchmarkBlockRef {
	return benchmarkBlockRef{
		Workchain: id.Workchain,
		Shard:     fmt.Sprintf("%016x", uint64(id.Shard)),
		Seqno:     id.SeqNo,
		RootHash:  hex.EncodeToString(id.RootHash),
		FileHash:  hex.EncodeToString(id.FileHash),
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

func blockStateShellFromLoadedBlock(block loadedBlock) *storage.BlockState {
	state := &storage.BlockState{
		Block:         block.ID,
		StateRootHash: bytes.Clone(block.Meta.StateRootHash),
		StateFileHash: bytes.Clone(block.Meta.StateFileHash),
	}
	return state
}

func blockStateFromView(block loadedBlock, view *shardStateView) *storage.BlockState {
	state := blockStateShellFromLoadedBlock(block)
	if view != nil && view.root != nil {
		state.Cell = view.root
		if len(state.StateRootHash) == 0 {
			rootHash := view.root.HashKey(0)
			state.StateRootHash = rootHash[:]
		}
	}
	return state
}

func (v *replayValidator) blockStateViews(previous []*storage.BlockState, expected *storage.BlockState, block loadedBlock) ([]*shardStateView, *shardStateView, error) {
	if err := validateBlockStateUpdateRoots(previous, expected, block.Parsed.StateUpdate, v.stateViews); err != nil {
		return nil, nil, fmt.Errorf("validate state update roots for %s: %w", storage.FormatBlockRef(block.ID), err)
	}

	switch v.stateViews {
	case stateViewSourceUpdate:
		previousViews, expectedView, err := shardStateViewsFromUpdate(block.ID, block.Parsed.StateUpdate, v.store.LazyCellLoader())
		if err != nil {
			return nil, nil, fmt.Errorf("parse state update views for %s: %w", storage.FormatBlockRef(block.ID), err)
		}
		return previousViews, expectedView, nil
	case stateViewSourceDB:
		return shardStateViewsFromDB(previous, expected)
	default:
		return nil, nil, fmt.Errorf("invalid state view source %q", v.stateViews)
	}
}

func validateBlockStateUpdateRoots(previous []*storage.BlockState, expected *storage.BlockState, update *cell.Cell, source stateViewSource) error {
	updateFrom, updateTo, err := merkleUpdateRootRefs(update)
	if err != nil {
		return err
	}
	if previousStateCellsLoaded(previous) {
		currentRoot, err := previousStateRootForReplay(previous)
		if err != nil {
			return err
		}
		if err = cell.MayApplyMerkleUpdate(currentRoot, update); err != nil {
			return err
		}
	} else {
		if source != stateViewSourceUpdate {
			return fmt.Errorf("previous state root cells are not loaded")
		}
		if err = validateStateUpdateSourceHashes(previous, updateFrom); err != nil {
			return err
		}
		if err = cell.MayApplyMerkleUpdate(updateFrom, update); err != nil {
			return err
		}
	}

	if len(expected.StateRootHash) > 0 {
		updateToHash := updateTo.HashKey(0)
		if !bytes.Equal(updateToHash[:], expected.StateRootHash) {
			return fmt.Errorf("state update target hash mismatch: got=%x want=%x", updateToHash[:], expected.StateRootHash)
		}
	}
	return nil
}

func previousStateCellsLoaded(previous []*storage.BlockState) bool {
	if len(previous) != 1 && len(previous) != 2 {
		return false
	}
	for _, state := range previous {
		if state == nil || state.Cell == nil {
			return false
		}
	}
	return true
}

func validateStateUpdateSourceHashes(previous []*storage.BlockState, updateFrom *cell.Cell) error {
	switch len(previous) {
	case 1:
		state := previous[0]
		if len(state.StateRootHash) != 32 {
			return fmt.Errorf("current state %s has no root hash", storage.FormatBlockRef(state.Block))
		}
		updateFromHash := updateFrom.HashKey(0)
		if !bytes.Equal(updateFromHash[:], state.StateRootHash) {
			return fmt.Errorf("state update source hash mismatch: got=%x want=%x", updateFromHash[:], state.StateRootHash)
		}
		return nil
	case 2:
		left := previous[0]
		right := previous[1]
		if len(left.StateRootHash) != 32 || len(right.StateRootHash) != 32 {
			return fmt.Errorf("merge previous states have missing root hashes")
		}

		loader, err := updateFrom.BeginParse()
		if err != nil {
			return fmt.Errorf("parse split source state: %w", err)
		}
		tag, err := loader.LoadUInt(32)
		if err != nil {
			return fmt.Errorf("load split source magic: %w", err)
		}
		if tag != 0x5f327da5 {
			return fmt.Errorf("state update source is not ShardStateSplit: %x", tag)
		}
		leftRef, err := loader.PeekRefCellAt(0)
		if err != nil {
			return fmt.Errorf("load split source left state: %w", err)
		}
		rightRef, err := loader.PeekRefCellAt(1)
		if err != nil {
			return fmt.Errorf("load split source right state: %w", err)
		}
		leftHash := leftRef.HashKey(0)
		if !bytes.Equal(leftHash[:], left.StateRootHash) {
			return fmt.Errorf("split source left hash mismatch: got=%x want=%x", leftHash[:], left.StateRootHash)
		}
		rightHash := rightRef.HashKey(0)
		if !bytes.Equal(rightHash[:], right.StateRootHash) {
			return fmt.Errorf("split source right hash mismatch: got=%x want=%x", rightHash[:], right.StateRootHash)
		}
		return nil
	default:
		return fmt.Errorf("unsupported previous state count %d", len(previous))
	}
}

func shardStateViewsFromDB(previous []*storage.BlockState, expected *storage.BlockState) ([]*shardStateView, *shardStateView, error) {
	previousViews := make([]*shardStateView, 0, len(previous))
	for _, state := range previous {
		view, err := newShardStateView(state)
		if err != nil {
			return nil, nil, fmt.Errorf("parse previous state %s: %w", storage.FormatBlockRef(state.Block), err)
		}
		previousViews = append(previousViews, view)
	}

	expectedView, err := newShardStateView(expected)
	if err != nil {
		return nil, nil, fmt.Errorf("parse expected state %s: %w", storage.FormatBlockRef(expected.Block), err)
	}
	return previousViews, expectedView, nil
}

func previousStateRootForReplay(previous []*storage.BlockState) (*cell.Cell, error) {
	switch len(previous) {
	case 1:
		current := previous[0]
		if current.Cell == nil {
			return nil, fmt.Errorf("current state cell is missing")
		}
		return current.Cell.Virtualize(0), nil
	case 2:
		left := previous[0]
		right := previous[1]
		if left.Cell == nil || right.Cell == nil {
			return nil, fmt.Errorf("merge previous state cell is missing")
		}
		return cell.BeginCell().
			MustStoreUInt(0x5f327da5, 32).
			MustStoreRef(left.Cell.Virtualize(0)).
			MustStoreRef(right.Cell.Virtualize(0)).
			EndCell(), nil
	default:
		return nil, fmt.Errorf("unsupported previous state count %d", len(previous))
	}
}

func shardStateViewsFromUpdate(block ton.BlockIDExt, update *cell.Cell, base cell.LazyCellLoader) ([]*shardStateView, *shardStateView, error) {
	updateFrom, updateTo, err := merkleUpdateRootRefs(update)
	if err != nil {
		return nil, nil, err
	}

	records, err := prepareStateUpdateViewCells(updateFrom, updateTo)
	if err != nil {
		return nil, nil, err
	}
	loader := stateUpdateViewLoader(records, base)
	updateFrom, err = loadStateUpdateViewRoot(updateFrom, loader)
	if err != nil {
		return nil, nil, fmt.Errorf("load state update source root: %w", err)
	}
	updateTo, err = loadStateUpdateViewRoot(updateTo, loader)
	if err != nil {
		return nil, nil, fmt.Errorf("load state update target root: %w", err)
	}

	previousViews, err := previousShardStateViewsFromRoot(updateFrom)
	if err != nil {
		return nil, nil, err
	}

	var expected tlb.ShardStateUnsplit
	toLoader, err := updateTo.BeginParse()
	if err != nil {
		return nil, nil, fmt.Errorf("parse state update target root: %w", err)
	}
	if err = tlb.LoadFromCell(&expected, toLoader); err != nil {
		return nil, nil, fmt.Errorf("load state update target state: %w", err)
	}

	expectedView := newShardStateViewFromParsed(block, updateTo, &expected)
	return previousViews, expectedView, nil
}

func loadStateUpdateViewRoot(root *cell.Cell, loader cell.LazyCellLoader) (*cell.Cell, error) {
	hash := root.GetMetadata().Hash
	loaded, err := loader(hash)
	if err != nil {
		return nil, err
	}
	loadedHash := loaded.GetMetadata().Hash
	if loadedHash != hash {
		return nil, fmt.Errorf("loaded root hash mismatch: got=%x want=%x", loadedHash[:], hash[:])
	}
	return loaded, nil
}

func stateUpdateViewLoader(records map[cell.Hash][]byte, base cell.LazyCellLoader) cell.LazyCellLoader {
	var load cell.LazyCellLoader
	load = func(hash cell.Hash) (*cell.Cell, error) {
		if encoded := records[hash]; len(encoded) > 0 {
			record := storage.DecodeCellRecordTrusted(hash[:], encoded)
			return storage.LazyCellRecord(record, load)
		}
		if base != nil {
			return base(hash)
		}
		return nil, storage.ErrNotFound
	}
	return load
}

func prepareStateUpdateViewCells(roots ...*cell.Cell) (map[cell.Hash][]byte, error) {
	records := make(map[cell.Hash][]byte, 1024)
	for _, root := range roots {
		if root == nil {
			continue
		}
		if err := walkStateUpdateViewCells(root, func(current *cell.Cell, meta cell.Metadata) error {
			record, err := prepareStateUpdateViewCellRecord(current, meta)
			if err != nil {
				return err
			}
			records[record.Hash] = record.Data
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return records, nil
}

func prepareStateUpdateViewCellRecord(current *cell.Cell, meta cell.Metadata) (storage.EncodedCellRecord, error) {
	if current.GetType() == cell.PrunedCellType {
		var err error
		current, err = materializePrunedStateUpdateViewCell(current)
		if err != nil {
			return storage.EncodedCellRecord{}, err
		}
	}
	return storage.PrepareEncodedCellRecordFromCellMetadata(current, meta)
}

func materializePrunedStateUpdateViewCell(current *cell.Cell) (*cell.Cell, error) {
	if current == nil || current.GetType() != cell.PrunedCellType || !current.IsVirtualized() {
		return current, nil
	}

	loader, err := current.BeginParse()
	if err != nil {
		return nil, err
	}
	bits, data, err := loader.RestBits()
	if err != nil {
		return nil, err
	}

	builder := cell.BeginCell()
	if err = builder.StoreSlice(data, bits); err != nil {
		return nil, err
	}
	return builder.EndCellSpecial(true)
}

func walkStateUpdateViewCells(root *cell.Cell, visit func(*cell.Cell, cell.Metadata) error) error {
	stack := []*cell.Cell{root}
	seen := map[cell.Hash]struct{}{}
	for len(stack) > 0 {
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if current == nil {
			continue
		}
		if current.IsLazy() {
			loader, err := current.BeginParse()
			if err != nil {
				return err
			}
			current = loader.BaseCell()
		}
		if current.GetType() == cell.PrunedCellType && current.ActualLevel() == current.EffectiveLevel()+1 {
			continue
		}

		meta := current.GetMetadata()
		if _, ok := seen[meta.Hash]; ok {
			continue
		}
		seen[meta.Hash] = struct{}{}

		if err := visit(current, meta); err != nil {
			return err
		}
		if current.GetType() == cell.PrunedCellType {
			continue
		}
		loader, err := current.BeginParse()
		if err != nil {
			return err
		}
		for i := loader.RefsNum() - 1; i >= 0; i-- {
			ref, err := loader.PeekRefCellAt(i)
			if err != nil {
				return err
			}
			stack = append(stack, ref)
		}
	}
	return nil
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

func newShardStateView(state *storage.BlockState) (*shardStateView, error) {
	if state.Cell == nil {
		return nil, fmt.Errorf("state %s has no root cell", storage.FormatBlockRef(state.Block))
	}

	loader, err := state.Cell.BeginParse()
	if err != nil {
		return nil, err
	}
	var parsed tlb.ShardStateUnsplit
	if err = tlb.LoadFromCell(&parsed, loader); err != nil {
		return nil, err
	}
	return &shardStateView{
		block:  state.Block,
		root:   state.Cell,
		parsed: &parsed,
	}, nil
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

func merkleUpdateRootRefs(update *cell.Cell) (*cell.Cell, *cell.Cell, error) {
	loader, err := update.BeginParse()
	if err != nil {
		return nil, nil, fmt.Errorf("load merkle update cell: %w", err)
	}
	update = loader.BaseCell()
	if update.Level() != 0 {
		return nil, nil, fmt.Errorf("merkle update has non-zero level")
	}
	if update.GetType() != cell.MerkleUpdateCellType {
		return nil, nil, fmt.Errorf("not a MerkleUpdate cell")
	}
	if loader.RefsNum() != 2 {
		return nil, nil, fmt.Errorf("wrong references count for a merkle update special cell")
	}

	updateFrom, err := loader.PeekRefCellAt(0)
	if err != nil {
		return nil, nil, fmt.Errorf("load merkle update source ref: %w", err)
	}
	updateTo, err := loader.PeekRefCellAt(1)
	if err != nil {
		return nil, nil, fmt.Errorf("load merkle update target ref: %w", err)
	}
	return updateFrom.Virtualize(0), updateTo.Virtualize(0), nil
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

type replayShardResolver struct {
	validator   *replayValidator
	masterState *storage.BlockState
	master      ton.BlockIDExt
	masterSeqno uint32

	mu           sync.Mutex
	current      map[storage.BlockRootHash]*storage.BlockState
	cache        map[storage.BlockRootHash]*storage.BlockState
	masterStates map[storage.BlockRootHash]*storage.BlockState
	tasks        map[storage.BlockRootHash]*shardResolveTask
}

type shardResolveTask struct {
	done  chan struct{}
	state *storage.BlockState
	err   error
}

func newReplayShardResolver(validator *replayValidator, masterState *storage.BlockState, master ton.BlockIDExt, current map[storage.BlockRootHash]*storage.BlockState) *replayShardResolver {
	return &replayShardResolver{
		validator:   validator,
		masterState: masterState,
		master:      master,
		masterSeqno: master.SeqNo,
		current:     current,
		cache:       map[storage.BlockRootHash]*storage.BlockState{},
		masterStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(masterState.Block): masterState,
		},
		tasks: map[storage.BlockRootHash]*shardResolveTask{},
	}
}

func (r *replayShardResolver) resolve(ctx context.Context, block ton.BlockIDExt) (*storage.BlockState, error) {
	key := storage.BlockKey(block)

	r.mu.Lock()
	if state := r.current[key]; state != nil {
		r.mu.Unlock()
		return state, nil
	}
	if state := r.cache[key]; state != nil {
		r.mu.Unlock()
		return state, nil
	}
	if task := r.tasks[key]; task != nil {
		r.mu.Unlock()
		select {
		case <-task.done:
			return task.state, task.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	task := &shardResolveTask{done: make(chan struct{})}
	r.tasks[key] = task
	r.mu.Unlock()

	state, err := r.resolveOwned(ctx, block)

	r.mu.Lock()
	if err == nil {
		r.cache[key] = state
	}
	delete(r.tasks, key)
	task.state = state
	task.err = err
	close(task.done)
	r.mu.Unlock()
	return state, err
}

func (r *replayShardResolver) resolveOwned(ctx context.Context, block ton.BlockIDExt) (*storage.BlockState, error) {
	loaded, err := r.validator.loadBlock(ctx, block)
	if err != nil {
		return nil, fmt.Errorf("load shard block %s: %w", storage.FormatBlockRef(block), err)
	}
	if !loaded.ID.Equals(&block) {
		return nil, fmt.Errorf("loaded shard block %s instead of %s", storage.FormatBlockRef(loaded.ID), storage.FormatBlockRef(block))
	}

	prevRefs := loaded.Meta.PrevRefs
	if len(prevRefs) == 0 || len(prevRefs) > 2 {
		return nil, fmt.Errorf("shard block %s has %d previous refs", storage.FormatBlockRef(block), len(prevRefs))
	}

	previous := make([]*storage.BlockState, len(prevRefs))
	for i, prev := range prevRefs {
		if prev.Workchain != block.Workchain || prev.SeqNo >= block.SeqNo {
			return nil, fmt.Errorf("shard block %s has invalid previous ref %s", storage.FormatBlockRef(block), storage.FormatBlockRef(prev))
		}
		resolved, err := r.resolve(ctx, prev)
		if err != nil {
			return nil, err
		}
		previous[i] = resolved
	}

	expected := blockStateShellFromLoadedBlock(loaded)
	if r.validator.stateViews == stateViewSourceDB {
		loadedState, err := r.validator.loadState(ctx, block)
		if err != nil {
			return nil, fmt.Errorf("load expected shard state %s: %w", storage.FormatBlockRef(block), err)
		}
		expected = loadedState
	}

	master, err := replayExecutionMaster(loaded, r.master)
	if err != nil {
		return nil, err
	}
	masterState, err := r.masterStateFor(ctx, master)
	if err != nil {
		return nil, fmt.Errorf("load master state for shard block %s: %w", storage.FormatBlockRef(block), err)
	}

	execCtx, err := r.validator.cache.blockContext(ctx, r.validator.lookupMaster, masterState.Block, masterState.Cell, loaded.Parsed.BlockInfo.GenUtime)
	if err != nil {
		return nil, fmt.Errorf("prepare shard execution context %s: %w", storage.FormatBlockRef(block), err)
	}
	result, err := r.validator.validateBlock(ctx, r.masterSeqno, previous, expected, loaded, execCtx)
	if err != nil {
		return nil, err
	}
	return result.State, nil
}

func replayExecutionMaster(block loadedBlock, fallback ton.BlockIDExt) (ton.BlockIDExt, error) {
	if !block.Parsed.BlockInfo.NotMaster {
		return fallback, nil
	}
	if block.Parsed.BlockInfo.MasterRef == nil {
		return ton.BlockIDExt{}, fmt.Errorf("shard block %s has no header master ref", storage.FormatBlockRef(block.ID))
	}
	return runMethodExtBlkRef(*block.Parsed.BlockInfo.MasterRef), nil
}

func (r *replayShardResolver) masterStateFor(ctx context.Context, block ton.BlockIDExt) (*storage.BlockState, error) {
	key := storage.BlockKey(block)

	r.mu.Lock()
	state := r.masterStates[key]
	r.mu.Unlock()
	if state != nil {
		return state, nil
	}

	state, err := r.validator.loadStateRoot(ctx, block)
	if err != nil {
		return nil, fmt.Errorf("load master state root %s: %w", storage.FormatBlockRef(block), err)
	}

	r.mu.Lock()
	if existing := r.masterStates[key]; existing != nil {
		r.mu.Unlock()
		return existing, nil
	}
	r.masterStates[key] = state
	r.mu.Unlock()

	return state, nil
}

type executionConfigCache struct {
	log              zerolog.Logger
	prevBlocksSource prevBlocksSource
	mu               sync.Mutex

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

func newExecutionConfigCache(log zerolog.Logger, source prevBlocksSource) *executionConfigCache {
	return &executionConfigCache{
		log:              log,
		prevBlocksSource: source,
		masters:          map[string]*masterExecutionContext{},
		bundles:          map[string]*configBundle{},
	}
}

func (c *executionConfigCache) blockContext(ctx context.Context, lookupMaster func(context.Context, uint32) (ton.BlockIDExt, error), master ton.BlockIDExt, masterState *cell.Cell, now uint32) (*blockExecutionContext, error) {
	masterCtx, err := c.masterContext(ctx, lookupMaster, master, masterState)
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

func (c *executionConfigCache) masterContext(ctx context.Context, lookupMaster func(context.Context, uint32) (ton.BlockIDExt, error), master ton.BlockIDExt, masterState *cell.Cell) (*masterExecutionContext, error) {
	stateHash := masterState.HashKey(0)
	masterKey := fmt.Sprintf("%x:%x:%s", storage.BlockKey(master), stateHash[:], c.prevBlocksSource)

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

	prevBlocks, err := c.prevBlocksInfo(ctx, lookupMaster, master, masterState)
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

func (c *executionConfigCache) prevBlocksInfo(ctx context.Context, lookupMaster func(context.Context, uint32) (ton.BlockIDExt, error), master ton.BlockIDExt, masterState *cell.Cell) (tuple.Tuple, error) {
	switch c.prevBlocksSource {
	case prevBlocksSourceState:
		return runMethodPrevBlocksInfo(master, masterState)
	case prevBlocksSourceIndex:
		return runMethodPrevBlocksInfoFromIndex(ctx, master, masterState, lookupMaster)
	default:
		return tuple.Tuple{}, fmt.Errorf("invalid prev blocks source %q", c.prevBlocksSource)
	}
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

func runMethodPrevBlocksInfoFromIndex(ctx context.Context, master ton.BlockIDExt, masterState *cell.Cell, lookupMaster func(context.Context, uint32) (ton.BlockIDExt, error)) (tuple.Tuple, error) {
	info, err := runMethodMasterInfoFromState(masterState)
	if err != nil {
		return tuple.Tuple{}, err
	}
	oldBlock := func(seqno uint32) (ton.BlockIDExt, error) {
		if seqno == master.SeqNo {
			return master, nil
		}
		block, err := lookupMaster(ctx, seqno)
		if err != nil {
			return ton.BlockIDExt{}, fmt.Errorf("lookup old mc block in index: %w", err)
		}
		if block.Workchain != masterchainID || block.Shard != masterchainShard || block.SeqNo != seqno {
			return ton.BlockIDExt{}, fmt.Errorf("old mc block index returned %s for seqno=%d", storage.FormatBlockRef(block), seqno)
		}
		return block, nil
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
