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

	hot                             *pebble.DB
	cells                           *cellStore
	cellGenerations                 map[uint64]*cellStore
	activeCellGeneration            uint64
	activeCellOrigin                ton.BlockIDExt
	pendingCellMigration            *cellGenerationPendingMigration
	retiredGenerations              []uint64
	nextCellGeneration              uint64
	cellCache                       *decodedCellCache
	lazyCellLoaderZero              cell.LazyCellLoader
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
