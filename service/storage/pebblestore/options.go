package pebblestore

import "github.com/rs/zerolog"

type Options struct {
	Dir                             string
	Logger                          *zerolog.Logger
	ReadOnly                        bool
	MetaCacheSize                   int64
	CellCacheSize                   int64
	DisableDecodedCellCache         bool
	DecodedCellCacheShards          int
	DecodedCellCacheBytesPerEntry   int64
	DecodedCellCacheMinEntries      int
	DecodedCellCacheMaxEntries      int
	MetaMemTableSize                int
	CellMemTableSize                int
	CellShardMemTableSize           int
	CellMemTableStopWritesThreshold int
	ArtifactFileMaxOpen             int
	BytesPerSync                    int
	WALBytesPerSync                 int
}
