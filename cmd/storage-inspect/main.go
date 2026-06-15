package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/xssnick/gton/internal/logutil"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/storage/pebblestore"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	masterchainID    int32 = -1
	masterchainShard int64 = -1 << 63
	maxNodeBOCCells        = 4_000_000_000
)

func main() {
	dbDir := flag.String("db", "data", "node storage directory")
	seqnos := flag.String("seqnos", "", "comma-separated masterchain seqnos to inspect")
	around := flag.Uint("around", 0, "inspect this seqno and one block before/after")
	logLevel := flag.String("log-level", "error", "log level: trace, debug, info, warn, error")
	flag.Parse()

	level, err := logutil.ParseLevel(*logLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid log level %q: %v\n", *logLevel, err)
		os.Exit(2)
	}

	targets, err := parseSeqnos(*seqnos, *around)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(2)
	}

	logs := logutil.NewFactory(os.Stdout, logutil.Config{Level: level})
	cell.MaxBOCCells = maxNodeBOCCells

	store, err := pebblestore.Open(pebblestore.Options{
		Dir:      *dbDir,
		Logger:   logs.CategoryPtr("pebblestore"),
		ReadOnly: true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "open storage: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	printCurrent(ctx, store)
	for _, seqno := range targets {
		inspectMaster(ctx, store, seqno)
	}
}

func parseSeqnos(raw string, around uint) ([]uint32, error) {
	seen := map[uint32]struct{}{}
	var seqnos []uint32
	add := func(value uint64) error {
		if value > math.MaxUint32 {
			return fmt.Errorf("seqno %d exceeds uint32", value)
		}
		seqno := uint32(value)
		if _, ok := seen[seqno]; ok {
			return nil
		}
		seen[seqno] = struct{}{}
		seqnos = append(seqnos, seqno)
		return nil
	}

	if raw != "" {
		for _, item := range strings.Split(raw, ",") {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}
			value, err := strconv.ParseUint(item, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("parse seqno %q: %w", item, err)
			}
			if err = add(value); err != nil {
				return nil, err
			}
		}
	}
	if around > 0 {
		if around > math.MaxUint32 {
			return nil, fmt.Errorf("around seqno %d exceeds uint32", around)
		}
		if around > 0 {
			if err := add(uint64(around - 1)); err != nil {
				return nil, err
			}
		}
		if err := add(uint64(around)); err != nil {
			return nil, err
		}
		if around < math.MaxUint32 {
			if err := add(uint64(around + 1)); err != nil {
				return nil, err
			}
		}
	}
	if len(seqnos) == 0 {
		return nil, fmt.Errorf("pass -seqnos or -around")
	}
	return seqnos, nil
}

func printCurrent(ctx context.Context, store *pebblestore.Store) {
	current, err := store.CurrentState(ctx)
	if err != nil {
		fmt.Printf("current_state error=%v\n", err)
		return
	}
	fmt.Printf("current_state master=%s shard_client_seqno=%d shards=%d\n",
		storage.FormatBlockRef(current.Masterchain.Block), current.ShardClientSeqno, len(current.Shards))

	progress, err := store.StateSyncProgress(ctx)
	if err == nil {
		fmt.Printf("state_sync_progress master=%s shard_client_seqno=%d shards=%d\n",
			storage.FormatBlockRef(progress.Masterchain.Block), progress.ShardClientSeqno, len(progress.Shards))
	} else if !errors.Is(err, storage.ErrNotFound) {
		fmt.Printf("state_sync_progress error=%v\n", err)
	}
}

func inspectMaster(ctx context.Context, store *pebblestore.Store, seqno uint32) {
	block, err := store.LookupBlockBySeqNo(ctx, storage.BlockHistoryKey{Workchain: masterchainID, Shard: masterchainShard}, seqno)
	if err != nil {
		fmt.Printf("master_seqno=%d lookup_error=%v\n", seqno, err)
		return
	}

	fmt.Printf("master_seqno=%d block=%s\n", seqno, storage.FormatBlockRef(block))
	printBlock(ctx, store, "master", seqno, block)

	data, err := store.BlockData(ctx, block)
	if err != nil {
		fmt.Printf("  shards error=load_master_block_data %v\n", err)
		return
	}
	root, err := cell.FromBOC(data)
	if err != nil {
		fmt.Printf("  shards error=parse_master_block %v\n", err)
		return
	}
	parsed, err := storage.ParseVerifiedBlockCell(block, root)
	if err != nil {
		fmt.Printf("  shards error=parse_verified_master_block %v\n", err)
		return
	}
	if parsed.Extra == nil || parsed.Extra.Custom == nil {
		fmt.Printf("  shards error=master_has_no_custom_extra\n")
		return
	}
	shards, err := ton.LoadShardsFromHashes(parsed.Extra.Custom.ShardHashes, false)
	if err != nil {
		fmt.Printf("  shards error=%v\n", err)
		return
	}
	for _, shard := range shards {
		if shard == nil {
			continue
		}
		printBlock(ctx, store, "shard", seqno, *shard)
	}
}

func printBlock(ctx context.Context, store *pebblestore.Store, label string, masterSeqno uint32, block ton.BlockIDExt) {
	meta, err := store.BlockMeta(ctx, block)
	if err != nil {
		fmt.Printf("  %s master_seqno=%d block=%s meta_error=%v\n", label, masterSeqno, storage.FormatBlockRef(block), err)
		return
	}

	stateErr := "ok"
	rootHash := meta.StateRootHash
	if len(rootHash) != 32 {
		rootHash = nil
	}
	if _, err = store.LoadStateCellTree(ctx, block, rootHash); err != nil {
		stateErr = err.Error()
	}

	masterRef := uint32(0)
	masterRefBlock := ""
	masterRefError := ""
	if meta.MasterchainRefKnown() {
		masterRef = meta.MasterchainRefSeqno
		ref, err := store.LookupBlockBySeqNo(ctx, storage.BlockHistoryKey{Workchain: masterchainID, Shard: masterchainShard}, meta.MasterchainRefSeqno)
		if err != nil {
			masterRefError = err.Error()
		} else {
			masterRefBlock = storage.FormatBlockRef(ref)
		}
	}
	fmt.Printf("  %s block=%s flags=%s state_root_hash=%d state_snapshot=%t state_cells=%t state_root=%s master_ref_seqno=%d master_ref=%s master_ref_error=%s prev_refs=%d\n",
		label,
		storage.FormatBlockRef(block),
		formatFlags(meta.Flags),
		len(meta.StateRootHash),
		meta.Has(storage.BlockMetaHasStateSnapshot),
		meta.Has(storage.BlockMetaHasStateCells),
		stateErr,
		masterRef,
		masterRefBlock,
		masterRefError,
		len(meta.PrevRefs),
	)
}

func formatFlags(flags storage.BlockMetaFlags) string {
	var names []string
	add := func(flag storage.BlockMetaFlags, name string) {
		if flags&flag != 0 {
			names = append(names, name)
		}
	}
	add(storage.BlockMetaHasServedFull, "served_full")
	add(storage.BlockMetaServedFullIsLink, "served_full_link")
	add(storage.BlockMetaHasBlockData, "block_data")
	add(storage.BlockMetaHasProofBlock, "proof_block")
	add(storage.BlockMetaHasProofBlockLink, "proof_block_link")
	add(storage.BlockMetaHasProofKeyBlock, "proof_key")
	add(storage.BlockMetaHasProofKeyBlockLink, "proof_key_link")
	add(storage.BlockMetaHasStateSnapshot, "state_snapshot")
	add(storage.BlockMetaHasStateCells, "state_cells")
	add(storage.BlockMetaIsKeyBlock, "key_block")
	if len(names) == 0 {
		return "none"
	}
	return strings.Join(names, "|")
}
