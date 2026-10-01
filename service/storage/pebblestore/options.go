package pebblestore

import "github.com/rs/zerolog"

type Options struct {
	Dir      string
	Logger   *zerolog.Logger
	ReadOnly bool

	MetaCacheSize int64
	// CellCacheSize is the pebble BLOCK cache budget for celldb, in bytes. It is
	// an opaque allocation inside pebble, invisible to the Go GC, and it has
	// nothing to do with the decoded cell cache below, which is counted in Go
	// objects. The two have different cost models and are sized independently.
	CellCacheSize int64

	// CellRecordCacheBytes is the ARENA budget, in bytes, for the encoded cell
	// RECORD cache — the tier between the decoded cell cache and pebble that
	// holds raw celldb records pre-decode. Byte-denominated because its memory
	// is a ring of regions outside the GC's cost model (malloc'd under cgo,
	// noscan Go bytes under cgo=0); the derived index adds ~22-25% on top.
	// Zero disables the tier; negative is rejected; a positive dust value is
	// clamped up to the smallest workable ring.
	CellRecordCacheBytes int64

	// DisableDecodedCellCache turns the decoded cell cache off entirely. Reads
	// still return the same cells; every one of them is decoded fresh.
	DisableDecodedCellCache bool
	DecodedCellCacheShards  int
	// DecodedCellCacheEntries sizes the single decoded cell cache shared by
	// every consumer — lightserver, proofs, archive import, sync, collation and
	// validation. Counted in entries, not bytes: an entry is live Go objects
	// that every GC mark cycle scans.
	DecodedCellCacheEntries int

	MetaMemTableSize                int
	CellMemTableSize                int
	CellShardMemTableSize           int
	CellMemTableStopWritesThreshold int
	LargeBOCShardReadWorkers        int
	ArtifactFileMaxOpen             int
	BytesPerSync                    int
	WALBytesPerSync                 int
}
