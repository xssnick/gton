package pebblestore

import "testing"

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
