package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"syscall"
	"time"

	"github.com/xssnick/gton/internal/logutil"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/storage/pebblestore"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	masterchainID    int32 = -1
	masterchainShard int64 = -1 << 63
	maxNodeBOCCells        = 4_000_000_000
)

type options struct {
	dbDir          string
	seqno          uint32
	blocks         uint
	holesPath      string
	holeLimit      int
	failFast       bool
	checkStateRoot bool
	checkSeqIndex  bool
	progress       uint
	logLevel       zerolog.Level
	logJSON        bool
}

type holeChecker struct {
	store *pebblestore.Store
	log   zerolog.Logger
	opts  options

	report       holeReport
	checkedShard map[string]struct{}
	stop         bool
}

type holeReport struct {
	DB                  string        `json:"db"`
	StartSeqno          uint32        `json:"start_seqno"`
	Blocks              uint          `json:"blocks"`
	CheckedUntilSeqno   uint32        `json:"checked_until_seqno"`
	CheckedMasterBlocks uint64        `json:"checked_master_blocks"`
	CheckedShardRefs    uint64        `json:"checked_shard_refs"`
	CheckedShardBlocks  uint64        `json:"checked_shard_blocks"`
	CheckedBlockData    uint64        `json:"checked_block_data"`
	CheckedStateRoots   uint64        `json:"checked_state_roots"`
	Holes               []storageHole `json:"holes"`
	Truncated           bool          `json:"truncated"`
	Elapsed             string        `json:"elapsed"`
	StartedAt           time.Time     `json:"started_at"`
	FinishedAt          time.Time     `json:"finished_at"`
}

type storageHole struct {
	MasterSeqno uint32    `json:"master_seqno"`
	Scope       string    `json:"scope"`
	Block       blockJSON `json:"block"`
	Error       string    `json:"error"`
}

type blockJSON struct {
	Workchain int32  `json:"workchain"`
	Shard     string `json:"shard"`
	Seqno     uint32 `json:"seqno"`
	RootHash  string `json:"root_hash,omitempty"`
	FileHash  string `json:"file_hash,omitempty"`
}

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
	logger := logs.Component("storage-holes")
	cell.MaxBOCCells = maxNodeBOCCells

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info().
		Str("dir", opts.dbDir).
		Uint32("seqno", opts.seqno).
		Uint("blocks", opts.blocks).
		Bool("read_only", true).
		Bool("check_state_root", opts.checkStateRoot).
		Bool("check_seq_index", opts.checkSeqIndex).
		Msg("opening node storage")

	store, err := pebblestore.Open(pebblestore.Options{
		Dir:      opts.dbDir,
		Logger:   logs.CategoryPtr("pebblestore"),
		ReadOnly: true,
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

	checker := &holeChecker{
		store:        store,
		log:          logger,
		opts:         opts,
		checkedShard: map[string]struct{}{},
		report: holeReport{
			DB:         opts.dbDir,
			StartSeqno: opts.seqno,
			Blocks:     opts.blocks,
			StartedAt:  time.Now(),
		},
	}

	report, err := checker.run(ctx)
	if writeErr := writeReport(opts.holesPath, report); writeErr != nil {
		logger.Error().Err(writeErr).Str("path", opts.holesPath).Msg("failed to write holes report")
		os.Exit(1)
	}
	if err != nil {
		logger.Error().Err(err).Str("path", opts.holesPath).Msg("storage hole check failed")
		os.Exit(1)
	}
	if len(report.Holes) > 0 {
		logger.Error().
			Int("holes", len(report.Holes)).
			Bool("truncated", report.Truncated).
			Str("path", opts.holesPath).
			Msg("storage holes found")
		os.Exit(1)
	}

	logger.Info().
		Uint64("masters", report.CheckedMasterBlocks).
		Uint64("shard_refs", report.CheckedShardRefs).
		Uint64("shard_blocks", report.CheckedShardBlocks).
		Uint64("block_data", report.CheckedBlockData).
		Uint64("state_roots", report.CheckedStateRoots).
		Dur("elapsed", report.FinishedAt.Sub(report.StartedAt)).
		Str("path", opts.holesPath).
		Msg("storage hole check finished")
}

func parseOptions() (options, error) {
	var seqnoSeen bool

	dbDir := flag.String("db", "data", "node storage directory")
	seqno := flag.Uint("seqno", 0, "masterchain seqno to start from")
	blocks := flag.Uint("blocks", 1, "number of masterchain blocks to check")
	holesPath := flag.String("holes", "storage-holes.json", "path to JSON holes report, empty disables writing")
	holeLimit := flag.Int("hole-limit", 1000, "maximum holes to keep in JSON report, 0 keeps all")
	failFast := flag.Bool("fail-fast", false, "stop after the first detected hole")
	checkStateRoot := flag.Bool("check-state-root", true, "check that block state metadata and lazy root cell are present")
	checkSeqIndex := flag.Bool("check-seq-index", true, "check shard seqno indexes for referenced shard blocks")
	progress := flag.Uint("progress", 1000, "log progress every N masterchain blocks, 0 disables progress logs")
	logLevel := flag.String("log-level", "info", "log level: trace, debug, info, warn, error")
	logJSON := flag.Bool("log-json", false, "write logs as JSON instead of pretty console")
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
	if *blocks == 0 {
		return options{}, fmt.Errorf("blocks must be positive")
	}
	if uint64(*seqno)+uint64(*blocks)-1 > math.MaxUint32 {
		return options{}, fmt.Errorf("seqno range exceeds uint32")
	}
	if *holeLimit < 0 {
		return options{}, fmt.Errorf("hole-limit cannot be negative")
	}

	level, err := logutil.ParseLevel(*logLevel)
	if err != nil {
		return options{}, fmt.Errorf("invalid log level %q: %w", *logLevel, err)
	}

	return options{
		dbDir:          *dbDir,
		seqno:          uint32(*seqno),
		blocks:         *blocks,
		holesPath:      *holesPath,
		holeLimit:      *holeLimit,
		failFast:       *failFast,
		checkStateRoot: *checkStateRoot,
		checkSeqIndex:  *checkSeqIndex,
		progress:       *progress,
		logLevel:       level,
		logJSON:        *logJSON,
	}, nil
}

func (c *holeChecker) run(ctx context.Context) (holeReport, error) {
	for offset := uint(0); offset < c.opts.blocks; offset++ {
		if c.stop {
			break
		}
		if err := ctx.Err(); err != nil {
			return c.finishReport(), err
		}

		seqno := c.opts.seqno + uint32(offset)
		if err := c.checkMaster(ctx, seqno, offset == 0); err != nil {
			return c.finishReport(), err
		}
		c.report.CheckedUntilSeqno = seqno
		c.report.CheckedMasterBlocks++

		if c.opts.progress > 0 && (offset+1)%c.opts.progress == 0 {
			c.log.Info().
				Uint32("master_seqno", seqno).
				Uint64("masters", c.report.CheckedMasterBlocks).
				Uint64("shard_refs", c.report.CheckedShardRefs).
				Uint64("shard_blocks", c.report.CheckedShardBlocks).
				Int("holes", len(c.report.Holes)).
				Bool("truncated", c.report.Truncated).
				Msg("storage hole check progress")
		}
	}
	return c.finishReport(), nil
}

func (c *holeChecker) finishReport() holeReport {
	c.report.FinishedAt = time.Now()
	c.report.Elapsed = c.report.FinishedAt.Sub(c.report.StartedAt).String()
	return c.report
}

func (c *holeChecker) checkMaster(ctx context.Context, seqno uint32, initial bool) error {
	master, err := c.store.LookupBlockBySeqNo(ctx, storage.BlockHistoryKey{
		Workchain: masterchainID,
		Shard:     masterchainShard,
	}, seqno)
	if err != nil {
		c.addHole(seqno, "master_seq_index", ton.BlockIDExt{
			Workchain: masterchainID,
			Shard:     masterchainShard,
			SeqNo:     seqno,
		}, err)
		return nil
	}

	_, parsed := c.checkBlock(ctx, seqno, "master", master, true)
	if parsed == nil {
		return nil
	}
	if parsed.Extra == nil || parsed.Extra.Custom == nil {
		c.addHole(seqno, "master_shard_hashes", master, fmt.Errorf("master block has no custom extra"))
		return nil
	}

	shards, err := ton.LoadShardsFromHashes(parsed.Extra.Custom.ShardHashes, false)
	if err != nil {
		c.addHole(seqno, "master_shard_hashes", master, err)
		return nil
	}
	sort.Slice(shards, func(i, j int) bool {
		if shards[i].Workchain != shards[j].Workchain {
			return shards[i].Workchain < shards[j].Workchain
		}
		if shards[i].Shard != shards[j].Shard {
			return uint64(shards[i].Shard) < uint64(shards[j].Shard)
		}
		return shards[i].SeqNo < shards[j].SeqNo
	})

	for _, shard := range shards {
		if c.stop {
			return nil
		}
		if shard == nil {
			c.addHole(seqno, "master_shard_hashes", master, fmt.Errorf("master shard hashes returned nil shard"))
			continue
		}
		c.report.CheckedShardRefs++
		c.checkShardChain(ctx, seqno, *shard, !initial)
	}
	return nil
}

func (c *holeChecker) checkShardChain(ctx context.Context, masterSeqno uint32, block ton.BlockIDExt, checkPrev bool) {
	if c.stop || ctx.Err() != nil {
		return
	}

	key := storage.BlockKey(block)
	if _, ok := c.checkedShard[key]; ok {
		return
	}
	c.checkedShard[key] = struct{}{}
	c.report.CheckedShardBlocks++

	meta, _ := c.checkBlock(ctx, masterSeqno, "shard", block, false)
	if meta == nil {
		return
	}
	if meta.MasterchainRef == nil {
		c.addHole(masterSeqno, "shard_master_ref", block, fmt.Errorf("shard block has no masterchain ref"))
	}
	if len(meta.PrevRefs) == 0 {
		c.addHole(masterSeqno, "shard_prev_refs", block, fmt.Errorf("shard block has no previous refs"))
	}

	if c.opts.checkSeqIndex {
		indexed, err := c.store.LookupBlockBySeqNo(ctx, storage.BlockHistoryKey{
			Workchain: block.Workchain,
			Shard:     block.Shard,
		}, block.SeqNo)
		if err != nil {
			c.addHole(masterSeqno, "shard_seq_index", block, err)
		} else if !indexed.Equals(&block) {
			c.addHole(masterSeqno, "shard_seq_index", block, fmt.Errorf("indexed block is %s", storage.FormatBlockRef(indexed)))
		}
	}

	if !checkPrev {
		return
	}
	for _, prev := range meta.PrevRefs {
		if c.stop {
			return
		}
		if prev.Workchain != block.Workchain || prev.SeqNo >= block.SeqNo {
			c.addHole(masterSeqno, "shard_prev_ref", block, fmt.Errorf("invalid previous ref %s", storage.FormatBlockRef(prev)))
			continue
		}
		c.checkShardChain(ctx, masterSeqno, prev, true)
	}
}

func (c *holeChecker) checkBlock(ctx context.Context, masterSeqno uint32, role string, block ton.BlockIDExt, parse bool) (*storage.BlockMeta, *tlb.Block) {
	meta, err := c.store.BlockMeta(ctx, block)
	if err != nil {
		c.addHole(masterSeqno, role+"_meta", block, err)
	} else {
		if len(meta.StateRootHash) != 32 {
			c.addHole(masterSeqno, role+"_state_hash_meta", block, fmt.Errorf("state root hash has size %d", len(meta.StateRootHash)))
		}
		if c.opts.checkStateRoot {
			rootHash := meta.StateRootHash
			if len(rootHash) != 32 {
				rootHash = nil
			}
			if _, err = c.store.LoadStateCellTree(ctx, block, rootHash); err != nil {
				c.addHole(masterSeqno, role+"_state_root_cell", block, err)
			} else {
				c.report.CheckedStateRoots++
			}
		}
	}

	data, err := c.store.BlockData(ctx, block)
	if err != nil {
		c.addHole(masterSeqno, role+"_block_data", block, err)
		return meta, nil
	}
	c.report.CheckedBlockData++

	root, err := cell.FromBOC(data)
	if err != nil {
		c.addHole(masterSeqno, role+"_block_boc", block, err)
		return meta, nil
	}
	rootHash := root.HashKey()
	if !bytes.Equal(rootHash[:], block.RootHash) {
		c.addHole(masterSeqno, role+"_block_root_hash", block, fmt.Errorf("got=%x want=%x", rootHash[:], block.RootHash))
	}
	fileHash := sha256.Sum256(data)
	if !bytes.Equal(fileHash[:], block.FileHash) {
		c.addHole(masterSeqno, role+"_block_file_hash", block, fmt.Errorf("got=%x want=%x", fileHash[:], block.FileHash))
	}

	if !parse {
		return meta, nil
	}
	parsed, err := storage.ParseVerifiedBlockCell(block, root)
	if err != nil {
		c.addHole(masterSeqno, role+"_block_parse", block, err)
		return meta, nil
	}
	return meta, parsed
}

func (c *holeChecker) addHole(masterSeqno uint32, scope string, block ton.BlockIDExt, err error) {
	message := err.Error()
	if errors.Is(err, storage.ErrNotFound) {
		message = storage.ErrNotFound.Error()
	}

	shouldLog := c.opts.holeLimit == 0 || len(c.report.Holes) < c.opts.holeLimit
	if c.opts.holeLimit == 0 || len(c.report.Holes) < c.opts.holeLimit {
		c.report.Holes = append(c.report.Holes, storageHole{
			MasterSeqno: masterSeqno,
			Scope:       scope,
			Block:       blockToJSON(block),
			Error:       message,
		})
	} else if !c.report.Truncated {
		c.report.Truncated = true
		shouldLog = true
	} else {
		c.report.Truncated = true
	}

	if shouldLog {
		event := c.log.Warn().
			Uint32("master_seqno", masterSeqno).
			Str("scope", scope).
			Str("block", storage.FormatBlockRef(block)).
			Bool("truncated", c.report.Truncated).
			Err(err)
		event.Msg("storage hole detected")
	}
	if c.opts.failFast {
		c.stop = true
	}
}

func blockToJSON(block ton.BlockIDExt) blockJSON {
	return blockJSON{
		Workchain: block.Workchain,
		Shard:     fmt.Sprintf("%016x", uint64(block.Shard)),
		Seqno:     block.SeqNo,
		RootHash:  hex.EncodeToString(block.RootHash),
		FileHash:  hex.EncodeToString(block.FileHash),
	}
}

func writeReport(path string, report holeReport) error {
	if path == "" {
		return nil
	}

	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}
