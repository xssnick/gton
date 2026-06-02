package pebblestore

import (
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
)

var (
	errPebbleClosed          = errors.New("pebble storage is closed")
	errCellGenerationNotOpen = errors.New("cell generation is not open")
)

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
	dir                             string
	cellCacheSize                   int64
	cellShardMemTable               int
	cellMemTableStopWritesThreshold int
	artifactFiles                   *artifactFileCache
	bytesPerSync                    int
	fs                              vfs.FS
	hotOpts                         *pebble.Options
	hotCache                        *pebble.Cache
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
	dirtyArchivePacks   map[string]struct{}
	dirtyKeyProofPacks  map[string]struct{}
	closed              bool
}

func (s *Store) Close() error {
	var firstErr error
	s.artifactPublishMu.Lock()
	s.abandonPendingArtifactPacks()
	s.artifactPublishMu.Unlock()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return firstErr
	}
	s.closed = true
	s.hotClosing.Store(true)
	if s.hotRefs.Load() == 0 {
		s.signalHotDrained()
	}
	s.mu.Unlock()

	<-s.hotDrained
	if err := s.hot.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := s.closeCellGenerations(); err != nil && firstErr == nil {
		firstErr = err
	}
	if s.artifactFiles != nil {
		if err := s.artifactFiles.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.mu.Lock()
	s.cells = nil
	s.cellGenerations = nil
	s.mu.Unlock()
	if s.hotCache != nil {
		s.hotCache.Unref()
	}
	return firstErr
}

func (s *Store) StateFilesDir() string {
	return filepath.Join(s.dir, "archive", "states")
}
