package pebblestore

import (
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

var (
	errPebbleClosed          = errors.New("pebble storage is closed")
	errCellGenerationNotOpen = errors.New("cell generation is not open")
)

type ArtifactMetricsObserver interface {
	AddArchivePackageBytes(delta int64)
	AddPersistentStateBytes(delta int64)
}

type Store struct {
	log zerolog.Logger

	hot                  *pebble.DB
	cells                *cellStore
	cellGenerations      map[uint64]*cellStore
	activeCellGeneration uint64
	activeCellOrigin     ton.BlockIDExt
	pendingCellMigration *cellGenerationPendingMigration
	retiredGenerations   []uint64
	nextCellGeneration   uint64
	// decodedCells is THE decoded cell cache, keyed by (generation, hash), and
	// there is exactly one for the whole process. Every consumer that decodes a
	// cell out of celldb shares it: the lightserver, proof building, the archive
	// importer, sync and download, collation and validation.
	//
	// One cache is not merely simpler, it is required. The collator and the
	// validator must receive the SAME *cell.Cell for a given parent —
	// ChainState.validatedCandidateState compares tip states by pointer and
	// silently falls back to a full re-apply otherwise — and a cache is what
	// supplies that identity for a store-backed read. Two caches cannot both
	// supply one object, so "give collation its own cache" and "keep one
	// materialization" are in direct opposition. Pinned by
	// TestOneDecodedCellCacheGivesPointerIdentity.
	//
	// Capacity is bounded in ENTRIES because each entry is live Go objects that
	// every GC mark scans; see decodedCellCacheConfig.
	decodedCells *decodedCellCache
	// decodedCellLoads collapses a cold miss for one (generation, hash) into one
	// record/Pebble read and one decode. It is separate from decodedCells so the
	// lock-free resident hit path remains unchanged.
	decodedCellLoads decodedCellLoadGroup
	// recordCache is the encoded cell RECORD tier under decodedCells: raw
	// celldb record bytes, pre-decode, keyed by hash alone (content-addressed,
	// so it needs no generation namespace and survives generation swaps). Its
	// arenas live outside the GC under cgo; see record_cache.go. Nil when
	// disabled.
	recordCache *cellRecordCache
	// activeCellLoader reads through decodedCells and threads ITSELF into every
	// cell it decodes, so a whole subtree reached from one decode resolves
	// through this same loader all the way down.
	activeCellLoader                cell.LazyCellLoader
	lazyCellLoads                   lazyCellLoadCounters
	dir                             string
	cellCacheSize                   int64
	cellShardMemTable               int
	cellMemTableStopWritesThreshold int
	largeBOCShardReadWorkers        int
	artifactFiles                   *artifactFileCache
	archivePackagesMu               sync.RWMutex
	archivePackages                 map[int64]archivePackageMeta
	bytesPerSync                    int
	fs                              vfs.FS
	hotOpts                         *pebble.Options
	hotCache                        *pebble.Cache
	artifactMetricsMu               sync.RWMutex
	artifactMetrics                 ArtifactMetricsObserver
	readOnly                        bool
	hotWriteMu                      sync.Mutex
	hotClosing                      atomic.Bool
	hotRefs                         atomic.Int64
	hotDrained                      chan struct{}
	hotDrainOnce                    sync.Once

	mu                  sync.RWMutex
	artifactMu          sync.Mutex
	artifactPublishMu   sync.Mutex
	artifactSyncSeq     uint64
	pendingArchiveSync  map[string]pendingPackWrite
	pendingKeyProofSync map[string]pendingPackWrite
	// prewrittenArtifacts and prewrittenPackRegs hold streamed pack append
	// results between checkpoints; both are guarded by artifactPublishMu.
	prewrittenArtifacts map[storage.BlockRootHash]prewrittenArtifactRecord
	prewrittenPackRegs  map[int64]archivePackRegistration
	closed              bool
}

func (s *Store) Close() error {
	started := time.Now()
	var firstErr error
	s.log.Info().
		Int64("hot_refs", s.hotRefs.Load()).
		Int("cell_generations", len(s.cellGenerations)).
		Msg("closing pebble storage")

	stageStarted := time.Now()
	s.artifactPublishMu.Lock()
	s.abandonPendingArtifactPacks()
	s.artifactPublishMu.Unlock()
	s.log.Debug().
		Dur("elapsed", time.Since(stageStarted)).
		Msg("abandoned pending artifact packs")

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.log.Info().Dur("elapsed", time.Since(started)).Msg("pebble storage already closed")
		return firstErr
	}
	s.closed = true
	s.hotClosing.Store(true)
	if s.hotRefs.Load() == 0 {
		s.signalHotDrained()
	}
	s.mu.Unlock()

	stageStarted = time.Now()
	if refs := s.hotRefs.Load(); refs > 0 {
		s.log.Warn().
			Int64("hot_refs", refs).
			Msg("waiting for pebble metadb refs before close")
	}
	<-s.hotDrained
	s.log.Debug().
		Dur("elapsed", time.Since(stageStarted)).
		Msg("pebble metadb refs drained")

	stageStarted = time.Now()
	s.log.Info().Msg("closing pebble metadb")
	if err := s.hot.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	s.log.Info().
		Dur("elapsed", time.Since(stageStarted)).
		Msg("closed pebble metadb")

	if err := s.closeCellGenerations(); err != nil && firstErr == nil {
		firstErr = err
	}
	// After the cell generations: no store read can start a record-cache
	// lookup once they are gone, and free() itself waits out any lookup still
	// in flight before returning the arenas to the allocator — under cgo this
	// is C memory, so the wait is what stands between Close and a
	// use-after-free no Go tool would catch.
	s.recordCache.free()
	stageStarted = time.Now()
	s.log.Info().Msg("closing artifact file cache")
	if err := s.artifactFiles.close(); err != nil && firstErr == nil {
		firstErr = err
	}
	s.log.Info().
		Dur("elapsed", time.Since(stageStarted)).
		Msg("closed artifact file cache")
	s.mu.Lock()
	s.cells = nil
	s.cellGenerations = nil
	s.mu.Unlock()
	s.hotCache.Unref()
	s.log.Info().
		Dur("elapsed", time.Since(started)).
		Msg("closed pebble storage")
	return firstErr
}

func (s *Store) SetArtifactMetricsObserver(observer ArtifactMetricsObserver) {
	s.artifactMetricsMu.Lock()
	defer s.artifactMetricsMu.Unlock()

	s.artifactMetrics = observer
}

func (s *Store) observeArchivePackageBytes(delta int64) {
	if delta == 0 {
		return
	}

	s.artifactMetricsMu.RLock()
	observer := s.artifactMetrics
	s.artifactMetricsMu.RUnlock()

	if observer != nil {
		observer.AddArchivePackageBytes(delta)
	}
}

func (s *Store) observePersistentStateBytes(delta int64) {
	if delta == 0 {
		return
	}

	s.artifactMetricsMu.RLock()
	observer := s.artifactMetrics
	s.artifactMetricsMu.RUnlock()

	if observer != nil {
		observer.AddPersistentStateBytes(delta)
	}
}

func (s *Store) StateFilesDir() string {
	return filepath.Join(s.dir, "archive", "states")
}
