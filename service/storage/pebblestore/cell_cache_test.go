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
		t.Fatal("the decoded cell cache should be enabled by default")
	}
	if cfg.shards != DefaultDecodedCellCacheShards {
		t.Fatalf("shards = %d, want %d", cfg.shards, DefaultDecodedCellCacheShards)
	}
	if cfg.entries != DefaultDecodedCellCacheEntries {
		t.Fatalf("entries = %d, want %d", cfg.entries, DefaultDecodedCellCacheEntries)
	}

	cache := newDecodedCellCache(cfg)
	if cache == nil {
		t.Fatal("decoded cell cache is nil")
	}
	if cache.shardCount() != DefaultDecodedCellCacheShards {
		t.Fatalf("cache shards = %d, want %d", cache.shardCount(), DefaultDecodedCellCacheShards)
	}
	if got := cache.capacity(); got != DefaultDecodedCellCacheEntries {
		t.Fatalf("decoded cell cache capacity = %d, want %d", got, DefaultDecodedCellCacheEntries)
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
		t.Fatal("the decoded cell cache should be disabled")
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
		enabled: true,
		shards:  1,
		entries: 1,
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
			name: "negative entries",
			opts: Options{DecodedCellCacheEntries: -1},
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
