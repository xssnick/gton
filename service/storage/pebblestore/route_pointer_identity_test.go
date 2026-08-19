package pebblestore

import (
	"context"
	"testing"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// POINTER IDENTITY is a load-bearing property of the state route, and it is the
// reason there is ONE decoded cell cache rather than one per consumer.
//
// service/validator/chain_state.go's validatedCandidateState carries a verified
// candidate forward with
//
//	successor.Live.Over(s.root.WithoutTrace(), s.tipStates()...)
//
// which takes the tip states BY POINTER and refuses anything else. When it
// refuses, the candidate silently falls back to a full re-apply: no error, no
// metric, just several milliseconds per candidate, forever. Nothing in the unit
// suite notices, which is why this is pinned here rather than left to review.
//
// The decoded cell cache is what supplies that identity for a store-backed
// read: two reads of one cell return the SAME *cell.Cell, because the second is
// a cache hit on the object the first filed. The second half of this test builds
// what a SECOND cache would have produced — the same record decoded again — and
// shows it is a different object carrying identical bits. That is the whole
// argument against splitting the cache per consumer, kept pinned so a future
// attempt has to answer it rather than rediscover it in production: whoever
// wants two caches must first explain how the collator and the validator still
// receive one object for one parent.
func TestOneDecodedCellCacheGivesPointerIdentity(t *testing.T) {
	const depth = 4
	store, block, hashes := stateChainStore(t, depth, Options{})
	ctx := context.Background()

	// One cache, read twice: identical objects, all the way down.
	firstRoot, err := store.LoadStateCellTree(ctx, block, hashes[0][:])
	if err != nil {
		t.Fatalf("first state load: %v", err)
	}
	secondRoot, err := store.LoadStateCellTree(ctx, block, hashes[0][:])
	if err != nil {
		t.Fatalf("second state load: %v", err)
	}
	if firstRoot != secondRoot {
		t.Fatal("two reads returned different materializations of the root; " +
			"chain_state.go's live-successor carry-back compares tips by pointer and would stop matching")
	}
	for level := 1; level <= depth; level++ {
		first, loadErr := store.loadActiveLazyCell(ctx, hashes[level][:])
		if loadErr != nil {
			t.Fatalf("level %d first load: %v", level, loadErr)
		}
		second, loadErr := store.loadActiveLazyCell(ctx, hashes[level][:])
		if loadErr != nil {
			t.Fatalf("level %d second load: %v", level, loadErr)
		}
		if first != second {
			t.Fatalf("level %d: two reads returned different objects", level)
		}
	}

	// What a second cache would have produced, at every level: the same celldb
	// record decoded a second time. Equal cells, different objects — and the
	// difference is invisible to every check except pointer comparison, which is
	// exactly why the split failed quietly rather than loudly.
	for level := 0; level <= depth; level++ {
		cached, loadErr := store.loadActiveLazyCell(ctx, hashes[level][:])
		if loadErr != nil {
			t.Fatalf("level %d cached load: %v", level, loadErr)
		}
		second := decodeSecondMaterialization(t, store, hashes[level])

		if second.HashKey() != cached.HashKey() {
			t.Fatalf("level %d: the fixture decoded a different cell", level)
		}
		if second == cached {
			t.Fatalf("level %d: the fixture returned the cached object, so it models nothing", level)
		}
	}
}

// decodeSecondMaterialization decodes one celldb record outside the cache,
// producing the object a second decoded cell cache would have filed for that
// hash. It uses the same decode path the store uses, so the result differs from
// the cached cell in identity only.
func decodeSecondMaterialization(t *testing.T, store *Store, hash cell.Hash) *cell.Cell {
	t.Helper()

	cells, err := store.acquireActiveCellStore(context.Background())
	if err != nil {
		t.Fatalf("acquire active cell store: %v", err)
	}
	defer cells.release()

	raw, closer, err := cells.get(hash[:])
	if err != nil {
		t.Fatalf("read cell record %x: %v", hash[:4], err)
	}
	defer func() { _ = closer.Close() }()

	var loader cell.LazyCellLoader
	loader = func(child cell.Hash) (*cell.Cell, error) {
		return decodeSecondMaterializationWith(store, child, loader)
	}
	decoded, err := storage.DecodeLazyCellRecordTrusted(hash[:], raw, loader)
	if err != nil {
		t.Fatalf("decode cell record %x: %v", hash[:4], err)
	}
	return decoded
}

func decodeSecondMaterializationWith(store *Store, hash cell.Hash, loader cell.LazyCellLoader) (*cell.Cell, error) {
	cells, err := store.acquireActiveCellStore(context.Background())
	if err != nil {
		return nil, err
	}
	defer cells.release()

	raw, closer, err := cells.get(hash[:])
	if err != nil {
		return nil, err
	}
	defer func() { _ = closer.Close() }()

	return storage.DecodeLazyCellRecordTrusted(hash[:], raw, loader)
}
