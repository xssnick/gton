package pebblestore

import (
	"fmt"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/storage"
)

const (
	metaDBMigrationHistoryIndexBatchBlocks = 500_000
	metaDBMigrationHistoryIndexLogEvery    = 1_000_000
)

type metaDBMigrationStats struct {
	scannedMeta      uint64
	indexedSeq       uint64
	indexedLT        uint64
	indexedUTime     uint64
	indexedKeyBlocks uint64
	elapsed          time.Duration
}

func runMetaDBMigrations(db *pebble.DB, from uint32, log zerolog.Logger) error {
	started := time.Now()
	log.Info().
		Uint32("from_version", from).
		Uint32("to_version", metaDBVersion).
		Time("started_at", started).
		Msg("starting metadb migrations")

	version := from
	for version < metaDBVersion {
		switch version {
		case 2:
			stats, err := migrateMetaDBToV3(db, log)
			if err != nil {
				return fmt.Errorf("migrate metadb v2 to v3: %w", err)
			}
			log.Info().
				Uint64("scanned_meta", stats.scannedMeta).
				Uint64("indexed_seq", stats.indexedSeq).
				Uint64("indexed_lt", stats.indexedLT).
				Uint64("indexed_utime", stats.indexedUTime).
				Uint64("indexed_key_blocks", stats.indexedKeyBlocks).
				Dur("elapsed", stats.elapsed).
				Msg("migrated metadb v2 to v3")
			version = 3
		default:
			return fmt.Errorf("unsupported metadb version %d, expected %d", from, metaDBVersion)
		}
	}
	if version != metaDBVersion {
		return fmt.Errorf("unsupported metadb version %d, expected %d", from, metaDBVersion)
	}
	finished := time.Now()
	log.Info().
		Uint32("from_version", from).
		Uint32("to_version", metaDBVersion).
		Time("started_at", started).
		Time("finished_at", finished).
		Dur("elapsed", finished.Sub(started)).
		Msg("finished metadb migrations")
	return nil
}

func migrateMetaDBToV3(db *pebble.DB, log zerolog.Logger) (metaDBMigrationStats, error) {
	started := time.Now()
	log.Info().
		Uint32("from_version", 2).
		Uint32("to_version", 3).
		Time("started_at", started).
		Msg("starting metadb v2 to v3 migration: rebuilding block history indexes")

	for _, prefix := range [][]byte{
		hotPrefixBlockSeq,
		hotPrefixBlockLT,
		hotPrefixBlockUTime,
		hotPrefixKeyBlockSeq,
	} {
		if err := db.DeleteRange(prefix, prefixUpperBound(prefix), pebble.NoSync); err != nil {
			return metaDBMigrationStats{}, fmt.Errorf("delete index prefix %x: %w", prefix, err)
		}
	}

	stats, err := rebuildBlockHistoryIndexes(db, log)
	if err != nil {
		return stats, err
	}
	stats.elapsed = time.Since(started)

	if err = db.Set(hotKeyMetaDBVersion(), encodeMetaDBVersion(metaDBVersion), pebble.Sync); err != nil {
		return stats, fmt.Errorf("persist metadb version %d: %w", metaDBVersion, err)
	}
	finished := time.Now()
	stats.elapsed = finished.Sub(started)
	log.Info().
		Uint32("from_version", 2).
		Uint32("to_version", 3).
		Time("started_at", started).
		Time("finished_at", finished).
		Dur("elapsed", stats.elapsed).
		Uint64("scanned_meta", stats.scannedMeta).
		Uint64("indexed_seq", stats.indexedSeq).
		Uint64("indexed_lt", stats.indexedLT).
		Uint64("indexed_utime", stats.indexedUTime).
		Uint64("indexed_key_blocks", stats.indexedKeyBlocks).
		Msg("finished metadb v2 to v3 migration")
	return stats, nil
}

func rebuildBlockHistoryIndexes(db *pebble.DB, log zerolog.Logger) (metaDBMigrationStats, error) {
	started := time.Now()
	log.Info().
		Time("started_at", started).
		Msg("starting metadb history index rebuild")

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: hotPrefixBlockMeta,
		UpperBound: prefixUpperBound(hotPrefixBlockMeta),
	})
	if err != nil {
		return metaDBMigrationStats{}, err
	}
	defer func() { _ = iter.Close() }()

	batch := db.NewBatch()
	defer func() { _ = batch.Close() }()

	var stats metaDBMigrationStats
	batchBlocks := 0
	flush := func() error {
		if batchBlocks == 0 {
			return nil
		}
		if err := batch.Commit(pebble.NoSync); err != nil {
			return err
		}
		if err := batch.Close(); err != nil {
			return err
		}
		batch = db.NewBatch()
		batchBlocks = 0
		return nil
	}

	for ok := iter.First(); ok; ok = iter.Next() {
		block, err := decodePrefixedBlockID(hotPrefixBlockMeta, iter.Key())
		if err != nil {
			return stats, err
		}
		meta, err := decodeBlockMeta(block, iter.Value())
		if err != nil {
			return stats, fmt.Errorf("decode block meta %s: %w", storage.FormatBlockRef(block), err)
		}
		stats.scannedMeta++

		if !blockMetaIndexedInHistory(meta) {
			continue
		}
		if err = validateBlockMetaBlockIDHashes(meta); err != nil {
			return stats, fmt.Errorf("validate block meta %s: %w", storage.FormatBlockRef(block), err)
		}
		if err = setBlockMetaHistoryIndexes(batch, nil, meta); err != nil {
			return stats, fmt.Errorf("index block meta %s: %w", storage.FormatBlockRef(block), err)
		}

		stats.indexedSeq++
		if meta.EndLT != 0 {
			stats.indexedLT++
		}
		if meta.GenUTime != 0 {
			stats.indexedUTime++
		}
		if isMasterchainBlock(meta.ID) && meta.Has(storage.BlockMetaIsKeyBlock) {
			stats.indexedKeyBlocks++
		}

		batchBlocks++
		if batchBlocks >= metaDBMigrationHistoryIndexBatchBlocks {
			if err = flush(); err != nil {
				return stats, err
			}
		}
		if stats.scannedMeta%metaDBMigrationHistoryIndexLogEvery == 0 {
			log.Info().
				Uint64("scanned_meta", stats.scannedMeta).
				Uint64("indexed_seq", stats.indexedSeq).
				Uint64("indexed_lt", stats.indexedLT).
				Uint64("indexed_utime", stats.indexedUTime).
				Uint64("indexed_key_blocks", stats.indexedKeyBlocks).
				Dur("elapsed", time.Since(started)).
				Msg("metadb history index rebuild progress")
		}
	}
	if err = iter.Error(); err != nil {
		return stats, err
	}
	if err = flush(); err != nil {
		return stats, err
	}
	finished := time.Now()
	log.Info().
		Time("started_at", started).
		Time("finished_at", finished).
		Dur("elapsed", finished.Sub(started)).
		Uint64("scanned_meta", stats.scannedMeta).
		Uint64("indexed_seq", stats.indexedSeq).
		Uint64("indexed_lt", stats.indexedLT).
		Uint64("indexed_utime", stats.indexedUTime).
		Uint64("indexed_key_blocks", stats.indexedKeyBlocks).
		Msg("finished metadb history index rebuild")
	return stats, nil
}
