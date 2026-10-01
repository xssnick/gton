package pebblestore

import (
	"context"
	"sort"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

type DBStatus struct {
	Meta            *MetaDBStatus
	CellGenerations []CellDBGenerationStatus
	LazyCellLoads   []storage.LazyCellLoadMetric
	// RecordCache is nil when the encoded cell record cache is disabled.
	RecordCache *RecordCacheStatus
}

// RecordCacheStatus reports the encoded cell record cache: residency, its two
// memory budgets (the arena the knob buys and the index derived on top), and
// the three write-path outcomes that are not hits — inserts, declined inserts
// (index probe budget exhausted) and salvage truncations (a rotation's 50%
// re-append budget hit). Hits are already counted by the record_cache layer of
// the lazy cell load metrics.
type RecordCacheStatus struct {
	Entries          int64
	ResidentBytes    int64
	CapacityBytes    int64
	IndexBytes       int64
	Inserts          uint64
	Declined         uint64
	SalvageTruncated uint64
}

type MetaDBStatus struct {
	Cache CellDBCacheStatus
	DB    CellDBShardStatus
}

type CellDBGenerationStatus struct {
	ID     uint64
	Role   string
	Origin ton.BlockIDExt
	Cache  CellDBCacheStatus
	Shards []CellDBShardStatus
	Total  CellDBShardStatus
}

type CellDBCacheStatus struct {
	BlockCacheSize   int64
	BlockCacheHits   int64
	BlockCacheMisses int64
	FileCacheSize    int64
}

type CellDBShardStatus struct {
	Shard                    int
	DiskSize                 uint64
	LiveSize                 uint64
	ReadAmp                  int64
	L0Files                  int64
	L0Sublevels              int64
	CompactionDebt           uint64
	CompactionsInProgress    int64
	CompactionInProgressSize int64
	MemTableSize             uint64
	TableIters               int64
	Flushes                  int64
	Ingests                  uint64
	ReadCells                uint64
	WrittenCells             uint64
	ReadCellsPerSecond       float64
	WrittenCellsPerSecond    float64
}

func (s *Store) DBStatus(ctx context.Context) (DBStatus, error) {
	select {
	case <-ctx.Done():
		return DBStatus{}, ctx.Err()
	default:
	}

	now := time.Now()
	generations := s.openCellGenerationStatusTargets()
	status := DBStatus{
		CellGenerations: make([]CellDBGenerationStatus, 0, len(generations)),
		LazyCellLoads:   s.lazyCellLoads.snapshot(),
	}
	if s.recordCache != nil {
		stats := s.recordCache.stats()
		status.RecordCache = &RecordCacheStatus{
			Entries:          stats.Entries,
			ResidentBytes:    stats.Bytes,
			CapacityBytes:    stats.CapacityBytes,
			IndexBytes:       stats.IndexBytes,
			Inserts:          stats.Inserts,
			Declined:         stats.Declined,
			SalvageTruncated: stats.SalvageTruncated,
		}
	}
	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return DBStatus{}, err
	}
	status.Meta = metaDBStatus(db)
	s.releaseHotDB()

	for _, generation := range generations {
		cells, err := s.acquireCellStore(ctx, generation.id)
		if err != nil {
			return DBStatus{}, err
		}
		status.CellGenerations = append(status.CellGenerations, cellGenerationDBStatus(cells, generation.role, generation.origin, now))
		cells.release()
	}
	return status, nil
}

func (s *Store) MaxReadAmp(ctx context.Context) (int64, error) {
	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return 0, err
	}
	metrics := db.Metrics()
	maxReadAmp := cellDBReadAmp(metrics.Levels)
	s.releaseHotDB()

	generations := s.openCellGenerationStatusTargets()
	for _, generation := range generations {
		cells, err := s.acquireCellStore(ctx, generation.id)
		if err != nil {
			return 0, err
		}
		readAmp := cellStoreMaxReadAmp(cells)
		cells.release()
		if readAmp > maxReadAmp {
			maxReadAmp = readAmp
		}
	}
	return maxReadAmp, nil
}

func (s *Store) CellGenerationDBMetrics(ctx context.Context, generation uint64) (storage.CellGenerationDBMetrics, error) {
	cells, err := s.acquireCellStore(ctx, generation)
	if err != nil {
		return storage.CellGenerationDBMetrics{}, err
	}
	defer cells.release()

	return cellStoreDBMetrics(cells), nil
}

type cellGenerationStatusTarget struct {
	id     uint64
	role   string
	origin ton.BlockIDExt
}

func (s *Store) openCellGenerationStatusTargets() []cellGenerationStatusTarget {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil
	}

	targets := make([]cellGenerationStatusTarget, 0, len(s.cellGenerations))
	for generation := range s.cellGenerations {
		target := cellGenerationStatusTarget{
			id:   generation,
			role: "open",
		}
		switch {
		case generation == s.activeCellGeneration:
			target.role = "active"
			target.origin = s.activeCellOrigin
		case s.pendingCellMigration != nil && generation == s.pendingCellMigration.generation:
			target.role = "pending"
			target.origin = s.pendingCellMigration.origin
		}
		targets = append(targets, target)
	}
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].id < targets[j].id
	})
	return targets
}

func cellGenerationDBStatus(cells *cellStore, role string, origin ton.BlockIDExt, now time.Time) CellDBGenerationStatus {
	status := CellDBGenerationStatus{
		ID:     cells.generation,
		Role:   role,
		Origin: origin,
		Shards: make([]CellDBShardStatus, 0, cellDBShardCount),
	}

	ioStatus := cells.ioStatus(now)
	for shardIdx, shard := range cells.shards {
		metrics := shard.db.Metrics()
		if shardIdx == 0 {
			status.Cache = pebbleDBCacheStatus(metrics)
		}
		shardStatus := pebbleDBShardStatus(shardIdx, metrics)
		shardStatus.ReadCells = ioStatus[shardIdx].readCells
		shardStatus.WrittenCells = ioStatus[shardIdx].writtenCells
		shardStatus.ReadCellsPerSecond = ioStatus[shardIdx].readCellsPerSecond
		shardStatus.WrittenCellsPerSecond = ioStatus[shardIdx].writtenCellsPerSecond
		status.Shards = append(status.Shards, shardStatus)
		status.Total.add(shardStatus)
	}
	status.Total.Shard = -1
	return status
}

func metaDBStatus(db *pebble.DB) *MetaDBStatus {
	metrics := db.Metrics()
	return &MetaDBStatus{
		Cache: pebbleDBCacheStatus(metrics),
		DB:    pebbleDBShardStatus(-1, metrics),
	}
}

func pebbleDBCacheStatus(metrics *pebble.Metrics) CellDBCacheStatus {
	return CellDBCacheStatus{
		BlockCacheSize:   metrics.BlockCache.Size,
		BlockCacheHits:   metrics.BlockCache.Hits,
		BlockCacheMisses: metrics.BlockCache.Misses,
		FileCacheSize:    metrics.FileCache.Size,
	}
}

func pebbleDBShardStatus(shard int, metrics *pebble.Metrics) CellDBShardStatus {
	return CellDBShardStatus{
		Shard:                    shard,
		DiskSize:                 metrics.DiskSpaceUsage(),
		LiveSize:                 metrics.Table.Local.LiveSize,
		ReadAmp:                  cellDBReadAmp(metrics.Levels),
		L0Files:                  metrics.Levels[0].TablesCount,
		L0Sublevels:              int64(metrics.Levels[0].Sublevels),
		CompactionDebt:           metrics.Compact.EstimatedDebt,
		CompactionsInProgress:    metrics.Compact.NumInProgress,
		CompactionInProgressSize: metrics.Compact.InProgressBytes,
		MemTableSize:             metrics.MemTable.Size,
		TableIters:               metrics.TableIters,
		Flushes:                  metrics.Flush.Count,
		Ingests:                  metrics.Ingest.Count,
	}
}

func cellDBReadAmp(levels [7]pebble.LevelMetrics) int64 {
	var amp int64
	amp += int64(levels[0].Sublevels)
	for i := 1; i < len(levels); i++ {
		if levels[i].TablesCount > 0 {
			amp++
		}
	}
	return amp
}

func cellStoreMaxReadAmp(cells *cellStore) int64 {
	var maxReadAmp int64
	for _, shard := range cells.shards {
		metrics := shard.db.Metrics()
		readAmp := cellDBReadAmp(metrics.Levels)
		if readAmp > maxReadAmp {
			maxReadAmp = readAmp
		}
	}
	return maxReadAmp
}

func cellStoreDBMetrics(cells *cellStore) storage.CellGenerationDBMetrics {
	var ret storage.CellGenerationDBMetrics
	for _, shard := range cells.shards {
		metrics := shard.db.Metrics()
		readAmp := cellDBReadAmp(metrics.Levels)
		if readAmp > ret.MaxReadAmp {
			ret.MaxReadAmp = readAmp
		}
		ret.L0Files += metrics.Levels[0].TablesCount
		ret.L0Sublevels += int64(metrics.Levels[0].Sublevels)
		ret.L0Size += metrics.Levels[0].TablesSize
		ret.CompactionDebt += metrics.Compact.EstimatedDebt
		ret.CompactionsInProgress += metrics.Compact.NumInProgress
		ret.CompactionInProgressSize += metrics.Compact.InProgressBytes
	}
	return ret
}

func (s *CellDBShardStatus) add(shard CellDBShardStatus) {
	s.DiskSize += shard.DiskSize
	s.LiveSize += shard.LiveSize
	if shard.ReadAmp > s.ReadAmp {
		s.ReadAmp = shard.ReadAmp
	}
	s.L0Files += shard.L0Files
	s.L0Sublevels += shard.L0Sublevels
	s.CompactionDebt += shard.CompactionDebt
	s.CompactionsInProgress += shard.CompactionsInProgress
	s.CompactionInProgressSize += shard.CompactionInProgressSize
	s.MemTableSize += shard.MemTableSize
	s.TableIters += shard.TableIters
	s.Flushes += shard.Flushes
	s.Ingests += shard.Ingests
	s.ReadCells += shard.ReadCells
	s.WrittenCells += shard.WrittenCells
	s.ReadCellsPerSecond += shard.ReadCellsPerSecond
	s.WrittenCellsPerSecond += shard.WrittenCellsPerSecond
}
