package collator

import (
	"errors"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/ton"
)

func cacheTestSource(id ton.BlockIDExt) *localBlockSource {
	return &localBlockSource{previous: PreviousBlock{ID: id}}
}

func storeCacheTestSource(t *testing.T, cache *localBlockCache, id ton.BlockIDExt) *localBlockSource {
	t.Helper()
	stored, err := cache.store(cacheTestSource(id))
	if err != nil {
		t.Fatalf("store %d: %v", id.SeqNo, err)
	}

	return stored
}

// A second store of the same block must hand back the incumbent rather than
// replacing it: the caller that lost the race keeps using the entry every other
// slot already shares, so the memoised derivations are computed once.
func TestLocalBlockCacheStoreKeepsTheIncumbent(t *testing.T) {
	var cache localBlockCache
	id := testBlockID(0, -1<<63, 7, 0x11)
	first := storeCacheTestSource(t, &cache, id)
	second := storeCacheTestSource(t, &cache, id)
	if first != second {
		t.Fatal("a second store replaced the incumbent entry")
	}

	found, err := cache.lookup(id)
	if err != nil {
		t.Fatal(err)
	}
	if found != first {
		t.Fatal("lookup returned an entry other than the stored one")
	}
}

// Content addressing is what makes a hit safe, so a root hash that arrives with
// a different block id is a contradiction, not a hit. Both entry points refuse
// it rather than serving a state that belongs to another block.
func TestLocalBlockCacheRejectsForeignBlockIDOnTheSameRoot(t *testing.T) {
	var cache localBlockCache
	stored := testBlockID(0, -1<<63, 7, 0x22)
	storeCacheTestSource(t, &cache, stored)

	foreign := stored
	foreign.SeqNo = 8

	if _, err := cache.lookup(foreign); !errors.Is(err, ErrInvalidInput) ||
		!strings.Contains(err.Error(), "belongs to another block id") {
		t.Fatalf("lookup of a foreign block id = %v", err)
	}
	if _, err := cache.store(cacheTestSource(foreign)); !errors.Is(err, ErrInvalidInput) ||
		!strings.Contains(err.Error(), "belongs to another block id") {
		t.Fatalf("store of a foreign block id = %v", err)
	}
}

func TestLocalBlockCacheRejectsMalformedRootHash(t *testing.T) {
	var cache localBlockCache
	id := testBlockID(0, -1<<63, 7, 0x33)
	id.RootHash = id.RootHash[:16]

	if _, err := cache.lookup(id); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("lookup with a short root hash = %v", err)
	}
	if _, err := cache.store(cacheTestSource(id)); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("store with a short root hash = %v", err)
	}
}

// The cache is bounded by memory, not freshness, so the bound has to hold even
// when nothing is ever looked up again.
func TestLocalBlockCacheEvictsPastItsBound(t *testing.T) {
	var cache localBlockCache
	for i := range maxLocalBlockSources + 64 {
		id := testBlockID(0, -1<<63, uint32(i), 0)
		id.RootHash[1], id.RootHash[2] = byte(i), byte(i>>8)
		storeCacheTestSource(t, &cache, id)
		if len(cache.entries) > maxLocalBlockSources {
			t.Fatalf("after %d stores the cache holds %d entries, want at most %d",
				i+1, len(cache.entries), maxLocalBlockSources)
		}
	}
	if len(cache.entries) != maxLocalBlockSources {
		t.Fatalf("cache holds %d entries, want %d", len(cache.entries), maxLocalBlockSources)
	}
}

// An entry has to survive the window where the collating slot still references
// the previous masterchain view, and must not survive longer than that.
func TestLocalBlockCacheRetiresEntriesAfterTwoViewInstallations(t *testing.T) {
	var cache localBlockCache
	id := testBlockID(0, -1<<63, 7, 0x44)
	storeCacheTestSource(t, &cache, id)

	cache.advance()
	if _, err := cache.lookup(id); err != nil {
		t.Fatalf("entry retired after one view installation: %v", err)
	}

	cache.advance()
	cache.advance()
	if _, err := cache.lookup(id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("lookup after the retention window = %v, want ErrNotFound", err)
	}
}

// A lookup renews the entry, so a block that stays referenced across view
// installations is never retired underneath its users.
func TestLocalBlockCacheLookupRenewsTheRetentionWindow(t *testing.T) {
	var cache localBlockCache
	id := testBlockID(0, -1<<63, 7, 0x55)
	storeCacheTestSource(t, &cache, id)

	for i := range 8 {
		cache.advance()
		if _, err := cache.lookup(id); err != nil {
			t.Fatalf("entry retired at view installation %d: %v", i, err)
		}
	}
}
