package pebblestore

import (
	"time"

	"github.com/cockroachdb/pebble/v2"
)

const (
	defaultPebbleMetaCacheSize      = 64 << 20
	defaultPebbleCellTotalCacheSize = 8 << 30

	defaultPebbleMetaMemTableSize      = 16 << 20
	defaultPebbleCellShardMemTableSize = 256 << 20
	defaultPebbleCellTotalMemTableSize = defaultPebbleCellShardMemTableSize * cellDBShardCount

	defaultPebbleBytesPerSync = 8 << 20
	defaultPebbleWALBytesSync = 8 << 20

	defaultPebbleMetaTargetFileSize = 16 << 20
	defaultPebbleCellTargetFileSize = 256 << 20
	defaultPebbleMetaMaxOpenFiles   = 4096
	defaultPebbleCellMaxOpenFiles   = 16384
	defaultPebbleFormatMajorVersion = pebble.FormatFlushableIngest
	DefaultArtifactFileMaxOpen      = 512

	defaultPebbleMetaMemTableStopThreshold = 4
	defaultPebbleCellMemTableStopThreshold = 4

	defaultPebbleMetaL0CompactionThreshold = 4
	defaultPebbleCellL0CompactionThreshold = 4

	defaultPebbleMetaL0FileThreshold = 8
	defaultPebbleCellL0FileThreshold = 16

	defaultPebbleMetaL0StopWritesThreshold = 32
	defaultPebbleCellL0StopWritesThreshold = 64

	defaultPebbleCellLBaseMaxBytes = 1 << 30

	pebbleDiskFullRetryDelay       = 5 * time.Second
	pebbleDiskFullRetryLogInterval = 30 * time.Second
	pebbleDiskFullRetryMinBytes    = defaultPebbleCellTargetFileSize

	stateCellImportBatchTargetBytes = 128 << 20
	stateCellSaveProgressInterval   = 5 * time.Second
	archivePackageMasterchainBlocks = 20000
	archiveSliceMasterchainBlocks   = 100
	keyArchiveMasterchainBlocks     = 200000

	blockMetaVersion                 = 1
	currentStateVersion              = 1
	blockStateMetaVersion            = 1
	artifactRefVersion               = 1
	persistentStateVersion           = 1
	archivePackageVersion            = 1
	cellGenerationStateImportVersion = 1

	cellRecordCompactRefsFlag = 0x10
	cellRecordHashSize        = 32
	cellRecordDepthSize       = 2
)
