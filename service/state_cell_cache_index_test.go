package service

import (
	"bytes"
	"errors"
	"sync"
	"testing"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func stateCellCacheRecordPosition(tb testing.TB, cache *stateCellEncodedCache, hash cell.Hash) int {
	tb.Helper()

	for i := range cache.records {
		if cache.records[i].Hash == hash {
			return i
		}
	}
	tb.Fatalf("record %x absent from folded cache", hash)
	return 0
}

func cacheIndexTestRecords(tb testing.TB, count int, colliding bool) []storage.EncodedCellRecord {
	tb.Helper()

	records := make([]storage.EncodedCellRecord, count)
	for i := range records {
		root := cell.BeginCell().MustStoreUInt(uint64(i), 64).EndCell()
		record, err := storage.PrepareEncodedCellRecordFromCellMetadata(root, root.GetMetadata())
		if err != nil {
			tb.Fatal(err)
		}
		if colliding {
			// Exercise index collisions without a production-only hash hook.
			// Trusted records allow testing cache ownership with synthetic keys.
			clear(record.Hash[:8])
		}
		records[i] = record
	}
	return records
}

func loadCacheIndexTestValue(tb testing.TB, cache *stateCellEncodedCache, hash cell.Hash) (*cell.Cell, uint64) {
	tb.Helper()

	loaded, err := cache.loadWith(hash, nil)
	if err != nil {
		tb.Fatalf("load record %x: %v", hash, err)
	}
	parsed, err := loaded.BeginParse()
	if err != nil {
		tb.Fatal(err)
	}
	value, err := parsed.LoadUInt(64)
	if err != nil {
		tb.Fatal(err)
	}
	return loaded, value
}

func TestStateCellCacheIndexCollisionsAndReplacements(t *testing.T) {
	t.Parallel()

	records := cacheIndexTestRecords(t, 256, true)
	cache := newStateCellEncodedCache(1)
	for i, record := range records {
		if !cache.setRecordLocked(record.Hash, record.Data) {
			t.Fatalf("record %d not inserted", i)
		}
	}
	for i, record := range records {
		first, value := loadCacheIndexTestValue(t, cache, record.Hash)
		if value != uint64(i) {
			t.Fatalf("record %d resolved to %d", i, value)
		}
		if cache.setRecordLocked(record.Hash, bytes.Clone(record.Data)) {
			t.Fatalf("identical record %d reported a replacement", i)
		}
		if again, _ := loadCacheIndexTestValue(t, cache, record.Hash); again != first {
			t.Fatalf("identical record %d lost its decoded memo", i)
		}
		if pos := stateCellCacheRecordPosition(t, cache, record.Hash); pos != i {
			t.Fatalf("record %d moved to position %d", i, pos)
		}
	}

	missing := records[0].Hash
	missing[31] ^= 0x80
	if _, err := cache.loadWith(missing, nil); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("same-fingerprint missing record: %v", err)
	}
	missing[0] = 1
	if _, err := cache.loadWith(missing, nil); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("different-fingerprint missing record: %v", err)
	}

	replacement := cell.BeginCell().MustStoreUInt(9999, 64).EndCell()
	encoded := mustPreparedReachableStateCells(t, replacement).Data(replacement.HashKey())
	for _, pos := range []int{0, len(records) / 2, len(records) - 1} {
		before, _ := loadCacheIndexTestValue(t, cache, records[pos].Hash)
		if !cache.setRecordLocked(records[pos].Hash, encoded) {
			t.Fatalf("replacement at %d reported no change", pos)
		}
		after, value := loadCacheIndexTestValue(t, cache, records[pos].Hash)
		if after == before || value != 9999 {
			t.Fatalf("replacement at %d kept old decoded data", pos)
		}
	}
	if cache.setRecordLocked(missing, nil) || cache.len() != len(records) {
		t.Fatal("empty encoding inserted a record or replacements changed record count")
	}
}

func TestStateCellCacheIndexCollidingLayersKeepNewestMemo(t *testing.T) {
	t.Parallel()

	older := cacheIndexTestRecords(t, 32, true)
	newer := append([]storage.EncodedCellRecord(nil), older...)
	for i := range newer {
		newer[i].Data = older[(i+1)%len(older)].Data
	}
	cache := newStateCellEncodedCache(1)
	cache.stageRecords(storage.NewStateCellRecords(older), nil)
	cache.stageRecords(storage.NewStateCellRecords(newer), nil)
	memos := make([]*cell.Cell, len(newer))
	for i := range newer {
		memos[i], _ = loadCacheIndexTestValue(t, cache, newer[i].Hash)
	}
	for {
		_, owed := cache.foldLayers(2, 3)
		for i := range newer {
			loaded, value := loadCacheIndexTestValue(t, cache, newer[i].Hash)
			if loaded != memos[i] || value != uint64((i+1)%len(newer)) {
				t.Fatalf("record %d lost newest bytes/memo while folding", i)
			}
		}
		if !owed {
			break
		}
	}
	if cache.len() != len(newer) || cache.stagedLayers() != 0 {
		t.Fatal("fold retained duplicate records or layers")
	}
}

func TestStateCellCacheIndexConcurrentCollidingGrowth(t *testing.T) {
	t.Parallel()

	records := cacheIndexTestRecords(t, 256, true)
	cache := newStateCellEncodedCache(1)
	cache.stageRecords(storage.NewStateCellRecords(records[:64]), nil)
	cache.foldLayers(1, 64)
	start := make(chan struct{})
	var workers sync.WaitGroup
	for reader := 0; reader < 4; reader++ {
		workers.Go(func() {
			<-start
			for i := 0; i < 512; i++ {
				record := records[(i+reader)%64]
				loaded, err := cache.loadWith(record.Hash, nil)
				if err != nil || loaded.HashKey() != record.Hash {
					t.Errorf("concurrent load of %x: %v", record.Hash, err)
					return
				}
			}
		})
	}
	workers.Go(func() {
		<-start
		for i := 64; i < len(records); i++ {
			cache.stageRecords(storage.NewStateCellRecords(records[i:i+1]), nil)
			cache.foldLayers(1, 1)
			cache.mu.Lock()
			cache.setRecordLocked(records[i%64].Hash, records[i].Data)
			cache.mu.Unlock()
		}
	})
	close(start)
	workers.Wait()
	if cache.len() != len(records) {
		t.Fatalf("record count after growth = %d, want %d", cache.len(), len(records))
	}
	for _, record := range records {
		if _, err := cache.loadWith(record.Hash, nil); err != nil {
			t.Fatalf("record lost during growth: %v", err)
		}
	}
}
