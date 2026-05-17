package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/xssnick/gton/internal/logutil"
	"github.com/xssnick/gton/service/state"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/storage/pebblestore"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	maxRollbackBOCCells = 4_000_000_000
	topShard            = int64(-1 << 63)
)

func main() {
	storageDirFlag := flag.String("storage-dir", "", "path to pebble storage directory")
	seqnoFlag := flag.Uint("seqno", 0, "masterchain seqno to rollback current state to")
	logLevelFlag := flag.String("log-level", "info", "log level: trace, debug, info, warn, error")
	logJSONFlag := flag.Bool("log-json", false, "write logs as JSON instead of pretty console")
	flag.Parse()

	if *storageDirFlag == "" {
		fmt.Fprintln(os.Stderr, "storage-dir is required")
		os.Exit(1)
	}
	rollbackSeqno := uint32(*seqnoFlag)
	if rollbackSeqno == 0 || uint(rollbackSeqno) != *seqnoFlag {
		fmt.Fprintf(os.Stderr, "invalid rollback seqno %d\n", *seqnoFlag)
		os.Exit(1)
	}

	level, err := logutil.ParseLevel(*logLevelFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid log level %q: %v\n", *logLevelFlag, err)
		os.Exit(1)
	}

	logs := logutil.NewFactory(os.Stdout, logutil.Config{
		Level: level,
		JSON:  *logJSONFlag,
	})
	logger := logs.Component("storage-rollback")
	cell.MaxBOCCells = maxRollbackBOCCells

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := pebblestore.Open(pebblestore.Options{
		Dir:    *storageDirFlag,
		Logger: logs.CategoryPtr("pebblestore"),
	})
	if err != nil {
		logger.Error().Err(err).Str("dir", *storageDirFlag).Msg("failed to open pebble storage")
		os.Exit(1)
	}
	defer closeStorage(logger, store)

	current, err := rollbackCurrentState(ctx, store, rollbackSeqno)
	if err != nil {
		logger.Error().Err(err).Uint32("rollback_seqno", rollbackSeqno).Msg("failed to prepare rollback")
		os.Exit(1)
	}

	stats, err := store.Rollback(ctx, current)
	if err != nil {
		logger.Error().Err(err).Uint32("rollback_seqno", rollbackSeqno).Msg("failed to rollback storage")
		os.Exit(1)
	}

	logger.Info().
		Uint32("rollback_seqno", rollbackSeqno).
		Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
		Int("shards", len(current.Shards)).
		Int("deleted_metadata_keys", stats.DeletedKeys).
		Msg("storage rolled back")
}

func rollbackCurrentState(ctx context.Context, store *pebblestore.Store, seqno uint32) (*storage.CurrentState, error) {
	master, err := store.LookupBlockBySeqNo(ctx, storage.BlockHistoryKey{Workchain: -1, Shard: topShard}, seqno)
	if err != nil {
		return nil, fmt.Errorf("lookup rollback masterchain block #%d: %w", seqno, err)
	}

	masterState, err := store.BlockState(ctx, master)
	if err != nil {
		return nil, fmt.Errorf("load rollback masterchain state %s: %w", storage.FormatBlockRef(master), err)
	}

	shardBlocks, err := state.ShardBlocksFromMasterState(masterState)
	if err != nil {
		return nil, fmt.Errorf("load rollback shard blocks from %s: %w", storage.FormatBlockRef(master), err)
	}

	current := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(masterState),
		Shards:           make(map[storage.ShardKey]storage.BlockState, len(shardBlocks)),
	}
	for _, shardBlock := range shardBlocks {
		shardState, err := store.BlockState(ctx, shardBlock)
		if err != nil {
			return nil, fmt.Errorf("load rollback shard state %s: %w", storage.FormatBlockRef(shardBlock), err)
		}
		current.Shards[storage.ShardKeyFromBlock(shardBlock)] = storage.BlockStateWithoutCells(shardState)
	}
	return current, nil
}

func closeStorage(logger zerolog.Logger, store interface{ Close() error }) {
	started := time.Now()
	logger.Info().Msg("closing storage")
	if err := store.Close(); err != nil {
		logger.Error().Err(err).Dur("elapsed", time.Since(started)).Msg("failed to close storage")
		return
	}
	logger.Info().Dur("elapsed", time.Since(started)).Msg("storage closed")
}
