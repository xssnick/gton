package pebblestore

import (
	"errors"
	"testing"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestDecodedCellCacheConfigDefaults(t *testing.T) {
	cfg, err := decodedCellCacheConfigFromOptions(Options{CellCacheSize: defaultPebbleCellTotalCacheSize})
	if err != nil {
		t.Fatalf("decoded cell cache config: %v", err)
	}
	if !cfg.enabled {
		t.Fatal("decoded cell cache should be enabled by default")
	}
	if cfg.shards != DefaultDecodedCellCacheShards {
		t.Fatalf("shards = %d, want %d", cfg.shards, DefaultDecodedCellCacheShards)
	}
	if cfg.bytesPerEntry != DefaultDecodedCellCacheBytesPerEntry {
		t.Fatalf("bytes per entry = %d, want %d", cfg.bytesPerEntry, DefaultDecodedCellCacheBytesPerEntry)
	}
	if cfg.minEntries != DefaultDecodedCellCacheMinEntries {
		t.Fatalf("min entries = %d, want %d", cfg.minEntries, DefaultDecodedCellCacheMinEntries)
	}
	if cfg.maxEntries != DefaultDecodedCellCacheMaxEntries {
		t.Fatalf("max entries = %d, want %d", cfg.maxEntries, DefaultDecodedCellCacheMaxEntries)
	}

	cache := newDecodedCellCache(cfg)
	if cache == nil {
		t.Fatal("decoded cell cache is nil")
	}
	if len(cache.shards) != DefaultDecodedCellCacheShards {
		t.Fatalf("cache shards = %d, want %d", len(cache.shards), DefaultDecodedCellCacheShards)
	}
	if got := cache.shards[0].capacity * len(cache.shards); got != int(defaultPebbleCellTotalCacheSize/DefaultDecodedCellCacheBytesPerEntry) {
		t.Fatalf("decoded cell cache capacity = %d", got)
	}
}

func TestDecodedCellCacheDisabled(t *testing.T) {
	cfg, err := decodedCellCacheConfigFromOptions(Options{
		CellCacheSize:           defaultPebbleCellTotalCacheSize,
		DisableDecodedCellCache: true,
	})
	if err != nil {
		t.Fatalf("decoded cell cache config: %v", err)
	}
	if cfg.enabled {
		t.Fatal("decoded cell cache should be disabled")
	}
	if cache := newDecodedCellCache(cfg); cache != nil {
		t.Fatal("disabled decoded cell cache should be nil")
	}
}

func TestDecodedCellCacheHashLookupMatchesSliceLookup(t *testing.T) {
	loaded := cell.BeginCell().MustStoreUInt(42, 64).EndCell()
	hash := loaded.HashKey()

	var disabled *decodedCellCache
	if _, err := disabled.getHash(1, hash); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("disabled cache error = %v, want ErrNotFound", err)
	}

	cache := newDecodedCellCache(decodedCellCacheConfig{
		enabled:       true,
		shards:        1,
		cacheBytes:    1,
		bytesPerEntry: 1,
		minEntries:    1,
		maxEntries:    1,
	})
	cache.set(1, hash[:], loaded)

	fromHash, err := cache.getHash(1, hash)
	if err != nil {
		t.Fatalf("get cached cell by hash: %v", err)
	}
	fromSlice, err := cache.get(1, hash[:])
	if err != nil {
		t.Fatalf("get cached cell by slice: %v", err)
	}
	if fromHash != loaded || fromSlice != loaded {
		t.Fatal("hash and slice lookups should return the cached cell")
	}

	if _, err = cache.getHash(2, hash); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("different generation error = %v, want ErrNotFound", err)
	}
}

func TestDecodedCellCacheConfigRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		opts Options
	}{
		{
			name: "negative shards",
			opts: Options{DecodedCellCacheShards: -1},
		},
		{
			name: "negative bytes per entry",
			opts: Options{DecodedCellCacheBytesPerEntry: -1},
		},
		{
			name: "negative min entries",
			opts: Options{DecodedCellCacheMinEntries: -1},
		},
		{
			name: "negative max entries",
			opts: Options{DecodedCellCacheMaxEntries: -1},
		},
		{
			name: "min over max",
			opts: Options{DecodedCellCacheMinEntries: 20, DecodedCellCacheMaxEntries: 10},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := decodedCellCacheConfigFromOptions(tt.opts); err == nil {
				t.Fatal("expected invalid decoded cell cache options to fail")
			}
		})
	}
}
