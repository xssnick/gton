package service

import (
	"errors"
	"fmt"
	"testing"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// These benchmarks deliberately use only the cache API/record slices, so they
// can also run against the previous full-hash index through a Go file overlay.
func BenchmarkStateCellCacheIndexLoad(b *testing.B) {
	for _, count := range []int{100000, 1000000} {
		records := cacheIndexTestRecords(b, count, false)
		cache := newStateCellEncodedCache(count)
		cache.stageRecords(storage.NewStateCellRecords(records), nil)
		cache.foldLayers(1, count)
		queries := make([]cell.Hash, 65536)
		random := uint64(1)
		for i := range queries {
			random ^= random << 13
			random ^= random >> 7
			random ^= random << 17
			queries[i] = records[random%uint64(count)].Hash
			if _, err := cache.loadWith(queries[i], nil); err != nil {
				b.Fatal(err)
			}
		}
		b.Run(fmt.Sprintf("records=%d/memo-hit", count), func(b *testing.B) {
			i := 0
			b.ReportAllocs()
			for b.Loop() {
				if _, err := cache.loadWith(queries[i&65535], nil); err != nil {
					b.Fatal(err)
				}
				i++
			}
		})
		b.Run(fmt.Sprintf("records=%d/miss", count), func(b *testing.B) {
			misses := append([]cell.Hash(nil), queries...)
			for i := range misses {
				misses[i][0] ^= 0x80
			}
			i := 0
			b.ReportAllocs()
			for b.Loop() {
				if _, err := cache.loadWith(misses[i&65535], nil); !errors.Is(err, storage.ErrNotFound) {
					b.Fatalf("missing record: %v", err)
				}
				i++
			}
		})
	}
}

func BenchmarkStateCellCacheIndexFold(b *testing.B) {
	for _, count := range []int{16384, 100000} {
		flat := cacheIndexTestRecords(b, count, false)
		records := storage.NewStateCellRecords(flat)
		for _, capacity := range []int{4096, count} {
			b.Run(fmt.Sprintf("records=%d/capacity=%d/fresh", count, capacity), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					cache := newStateCellEncodedCache(capacity)
					cache.stageRecords(records, nil)
					for {
						if _, owed := cache.foldLayers(1, stateCellWindowFoldSliceRecords); !owed {
							break
						}
					}
				}
			})
		}
		b.Run(fmt.Sprintf("records=%d/refold-decoded", count), func(b *testing.B) {
			cache := newStateCellEncodedCache(count)
			cache.stageRecords(records, nil)
			cache.foldLayers(1, count)
			layer := newStateCellRecordLayer(records)
			for i, record := range flat {
				loaded, err := cache.loadWith(record.Hash, nil)
				if err != nil {
					b.Fatal(err)
				}
				layer.decoded[i].Store(loaded)
			}
			b.ReportAllocs()
			for b.Loop() {
				cache.stageLayer(layer, nil)
				for {
					if _, owed := cache.foldLayers(1, stateCellWindowFoldSliceRecords); !owed {
						break
					}
				}
			}
		})
	}
}
