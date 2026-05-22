package pebblestore

import (
	"runtime"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/rs/zerolog"
)

type pebbleOptionsTuning struct {
	blockSize                   int
	compression                 func() *sstable.CompressionProfile
	filterPolicy                pebble.FilterPolicy
	targetFileSize              int
	maxOpenFiles                int
	fileCache                   *pebble.FileCache
	maxConcurrentCompactions    func() int
	memTableStopWritesThreshold int
	l0CompactionThreshold       int
	l0CompactionFileThreshold   int
	l0StopWritesThreshold       int
	lBaseMaxBytes               int64
	disableWAL                  bool
	compactionScheduler         pebble.CompactionScheduler
}

func newPebbleDiskFullRetryFS(dir string, logger zerolog.Logger) vfs.FS {
	fs := vfs.Default
	return vfs.OnDiskFull(fs, func() {
		waitPebbleDiskSpace(fs, dir, logger)
	})
}

func waitPebbleDiskSpace(fs vfs.FS, dir string, logger zerolog.Logger) {
	nextLog := time.Time{}
	for {
		usage, err := fs.GetDiskUsage(dir)
		if err == nil && usage.AvailBytes >= pebbleDiskFullRetryMinBytes {
			logger.Warn().
				Str("dir", dir).
				Uint64("available_bytes", usage.AvailBytes).
				Uint64("required_bytes", pebbleDiskFullRetryMinBytes).
				Msg("free disk space recovered after pebble write hit ENOSPC")
			return
		}

		// Pebble treats MANIFEST write ENOSPC as fatal after a version edit starts.
		// Keep its write retry blocked here until the retry has room to complete.
		now := time.Now()
		if now.After(nextLog) {
			event := logger.Error().
				Str("dir", dir).
				Uint64("required_bytes", pebbleDiskFullRetryMinBytes).
				Dur("retry_delay", pebbleDiskFullRetryDelay)
			if err != nil {
				event.Err(err).Msg("pebble write hit ENOSPC, waiting for free disk space")
			} else {
				event.Uint64("available_bytes", usage.AvailBytes).
					Msg("pebble write hit ENOSPC, waiting for free disk space")
			}
			nextLog = now.Add(pebbleDiskFullRetryLogInterval)
		}

		time.Sleep(pebbleDiskFullRetryDelay)
	}
}

func newMetaPebbleOptions(cache *pebble.Cache, fs vfs.FS, memTableSize, bytesPerSync, walBytesPerSync int, logger zerolog.Logger) *pebble.Options {
	return newPebbleOptions(cache, memTableSize, bytesPerSync, walBytesPerSync, pebbleOptionsTuning{
		blockSize:                   4 << 10,
		compression:                 pebbleNoCompression,
		filterPolicy:                bloom.FilterPolicy(10),
		targetFileSize:              defaultPebbleMetaTargetFileSize,
		maxOpenFiles:                defaultPebbleMetaMaxOpenFiles,
		maxConcurrentCompactions:    pebbleMaxConcurrentCompactions,
		memTableStopWritesThreshold: defaultPebbleMetaMemTableStopThreshold,
		l0CompactionThreshold:       defaultPebbleMetaL0CompactionThreshold,
		l0CompactionFileThreshold:   defaultPebbleMetaL0FileThreshold,
		l0StopWritesThreshold:       defaultPebbleMetaL0StopWritesThreshold,
	}, fs, logger)
}

func newCellPebbleOptions(cache *pebble.Cache, fileCache *pebble.FileCache, fs vfs.FS, memTableSize, memTableStopWritesThreshold, bytesPerSync int, compactionScheduler pebble.CompactionScheduler, logger zerolog.Logger) *pebble.Options {
	return newPebbleOptions(cache, memTableSize, bytesPerSync, 0, pebbleOptionsTuning{
		blockSize:                   4 << 10,
		compression:                 pebbleNoCompression,
		filterPolicy:                bloom.FilterPolicy(10),
		targetFileSize:              defaultPebbleCellTargetFileSize,
		maxOpenFiles:                defaultPebbleCellMaxOpenFiles,
		fileCache:                   fileCache,
		maxConcurrentCompactions:    pebbleCellMaxConcurrentCompactions,
		memTableStopWritesThreshold: memTableStopWritesThreshold,
		l0CompactionThreshold:       defaultPebbleCellL0CompactionThreshold,
		l0CompactionFileThreshold:   defaultPebbleCellL0FileThreshold,
		l0StopWritesThreshold:       defaultPebbleCellL0StopWritesThreshold,
		lBaseMaxBytes:               defaultPebbleCellLBaseMaxBytes,
		disableWAL:                  true,
		compactionScheduler:         compactionScheduler,
	}, fs, logger)
}

func newPebbleOptions(cache *pebble.Cache, memTableSize, bytesPerSync, walBytesPerSync int, tuning pebbleOptionsTuning, fs vfs.FS, logger zerolog.Logger) *pebble.Options {
	filterPolicy := tuning.filterPolicy
	if filterPolicy == nil {
		filterPolicy = bloom.FilterPolicy(10)
	}

	var levels [7]pebble.LevelOptions
	for i := range levels {
		levels[i] = pebble.LevelOptions{
			BlockSize:      tuning.blockSize,
			IndexBlockSize: tuning.blockSize,
			FilterPolicy:   filterPolicy,
			FilterType:     pebble.TableFilter,
			Compression:    tuning.compression,
		}
	}
	var targetFileSizes [7]int64
	for i := range targetFileSizes {
		targetFileSizes[i] = int64(tuning.targetFileSize)
	}
	maxConcurrentCompactions := tuning.maxConcurrentCompactions
	if maxConcurrentCompactions == nil {
		maxConcurrentCompactions = pebbleMaxConcurrentCompactions
	}
	opts := &pebble.Options{
		Cache:                       cache,
		FileCache:                   tuning.fileCache,
		FormatMajorVersion:          defaultPebbleFormatMajorVersion,
		MaxOpenFiles:                tuning.maxOpenFiles,
		MemTableSize:                uint64(memTableSize),
		MemTableStopWritesThreshold: tuning.memTableStopWritesThreshold,
		BytesPerSync:                bytesPerSync,
		WALBytesPerSync:             walBytesPerSync,
		FlushSplitBytes:             int64(tuning.targetFileSize),
		TargetFileSizes:             targetFileSizes,
		L0CompactionThreshold:       tuning.l0CompactionThreshold,
		L0CompactionFileThreshold:   tuning.l0CompactionFileThreshold,
		L0StopWritesThreshold:       tuning.l0StopWritesThreshold,
		LBaseMaxBytes:               tuning.lBaseMaxBytes,
		DisableWAL:                  tuning.disableWAL,
		Logger:                      pebbleDebugLogger{log: logger},
		CompactionConcurrencyRange: func() (int, int) {
			maxCompactions := maxConcurrentCompactions()
			if maxCompactions < 1 {
				maxCompactions = 1
			}
			return 1, maxCompactions
		},
		Levels: levels,
	}
	opts.FS = fs
	opts.Experimental.EnableColumnarBlocks = func() bool { return false }
	opts.Experimental.EnableValueBlocks = func() bool { return false }
	if tuning.compactionScheduler != nil {
		opts.Experimental.CompactionScheduler = tuning.compactionScheduler
	}
	return opts
}

func pebbleNoCompression() *sstable.CompressionProfile {
	return sstable.NoCompression
}

func newPebbleFileCache(maxOpenFiles int) *pebble.FileCache {
	shards := runtime.GOMAXPROCS(0)
	if shards < 1 {
		shards = 1
	}
	return pebble.NewFileCache(shards, pebble.FileCacheSize(maxOpenFiles))
}

type pebbleDebugLogger struct {
	log zerolog.Logger
}

func (l pebbleDebugLogger) Infof(format string, args ...interface{}) {
	event := l.log.Debug()
	if !event.Enabled() {
		return
	}
	event.Msgf(format, args...)
}

func (l pebbleDebugLogger) Errorf(format string, args ...interface{}) {
	event := l.log.Error()
	if !event.Enabled() {
		return
	}
	event.Msgf(format, args...)
}

func (l pebbleDebugLogger) Fatalf(format string, args ...interface{}) {
	l.log.Fatal().Msgf(format, args...)
}

func pebbleMaxConcurrentCompactions() int {
	n := runtime.GOMAXPROCS(0) / 2
	if n < 1 {
		return 1
	}
	return n
}

func pebbleCellMaxConcurrentCompactions() int {
	return 2
}

func pebbleCellCompactionParallelism() int {
	n := runtime.GOMAXPROCS(0) * 2 / 3
	if n < 1 {
		return 1
	}
	max := cellDBShardCount * pebbleCellMaxConcurrentCompactions()
	if n > max {
		return max
	}
	return n
}
