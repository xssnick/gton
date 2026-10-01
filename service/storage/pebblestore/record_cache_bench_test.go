package pebblestore

import (
	"encoding/binary"
	"testing"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// Real encoded celldb records for the benchmark: a 64-bit payload cell with
// two refs encodes near the measured mainnet mean (104.5 B; see
// recordCacheIndexArenaBytesPerSlot).
func benchEncodedRecords(b *testing.B, n int) (hashes [][]byte, records [][]byte) {
	b.Helper()

	hashes = make([][]byte, n)
	records = make([][]byte, n)
	for i := 0; i < n; i++ {
		builder := cell.BeginCell().MustStoreUInt(uint64(i), 64)
		for r := 0; r < 2; r++ {
			builder.MustStoreRef(cell.BeginCell().MustStoreUInt(uint64(i)*2+uint64(r), 64).EndCell())
		}
		c := builder.EndCell()
		record, err := storage.CellRecordFromCell(c)
		if err != nil {
			b.Fatal(err)
		}
		hash := c.HashKey()
		hashes[i] = hash[:]
		records[i] = storage.EncodeCellRecord(record)
	}
	return hashes, records
}

// The whole record-cache hit as the read path pays it: index probe, arena
// copy, and the decode into a lazy cell. The store tiers it sits between cost
// 26 us (pebble block cache) and 90 us (page cache); the target band for this
// row is 0.3-0.5 us.
func BenchmarkCellRecordCacheHitEndToEnd(b *testing.B) {
	cache := newCellRecordCache(cellRecordCacheConfigFromBytes(64 << 20))
	defer cache.free()

	const keys = 4096
	hashes, records := benchEncodedRecords(b, keys)
	for i := range hashes {
		cache.put(hashes[i], records[i])
	}
	loader := cell.LazyCellLoader(func(h cell.Hash) (*cell.Cell, error) { return nil, storage.ErrNotFound })

	var buf []byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		hash := hashes[i&(keys-1)]
		buf = cache.get(hash, buf)
		if buf == nil {
			b.Fatal("miss on a warm key")
		}
		if _, err := storage.DecodeLazyCellRecordTrusted(hash, buf, loader); err != nil {
			b.Fatal(err)
		}
	}
}

// The probe-and-copy alone, without the decode, to keep the two halves of the
// end-to-end figure separately visible.
func BenchmarkCellRecordCacheHitProbeCopy(b *testing.B) {
	cache := newCellRecordCache(cellRecordCacheConfigFromBytes(64 << 20))
	defer cache.free()

	const keys = 4096
	hashes, records := benchEncodedRecords(b, keys)
	for i := range hashes {
		cache.put(hashes[i], records[i])
	}

	var buf []byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		if buf = cache.get(hashes[i&(keys-1)], buf); buf == nil {
			b.Fatal("miss on a warm key")
		}
	}
}

// The insert path, ring rotation and salvage walks included: hashes are
// unique per iteration so every put lands and the regions churn like a real
// read miss stream.
func BenchmarkCellRecordCachePut(b *testing.B) {
	cache := newCellRecordCache(cellRecordCacheConfigFromBytes(64 << 20))
	defer cache.free()

	_, records := benchEncodedRecords(b, 1)
	record := records[0]
	var hash [32]byte

	b.ReportAllocs()
	b.ResetTimer()
	for i := uint64(0); b.Loop(); i++ {
		binary.BigEndian.PutUint64(hash[0:8], i*0x9e3779b97f4a7c15+1)
		binary.BigEndian.PutUint64(hash[8:16], i*0xbf58476d1ce4e5b9+1)
		cache.put(hash[:], record)
	}
}
