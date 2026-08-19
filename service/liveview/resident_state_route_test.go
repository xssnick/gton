package liveview

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// The live view sits between the validator seam and pebble. What is pinned here
// is the case that actually runs in production: a state the live view holds is
// returned as the SAME object, and its unresolved tips still resolve through the
// loader that decoded it, never through anything the backing would have
// supplied.
//
// That second fact is not decoration. It is why a per-consumer decoded cell
// cache cannot work: the cache a tree belongs to is decided once, at the first
// cold decode, and every later reader inherits it along with the tree. A cache
// split that hoped to reroute collation would have had to rebuild the resident
// tree — losing the pointer identity chain_state.go compares on, and doing
// exactly the work residency exists to avoid.

type routeCountingBacking struct {
	noopBacking
	root         *cell.Cell
	serviceCalls int
}

func (b *routeCountingBacking) LoadStateCellTree(context.Context, ton.BlockIDExt, []byte) (*cell.Cell, error) {
	b.serviceCalls++
	if b.root == nil {
		return nil, storage.ErrNotFound
	}
	return b.root, nil
}

func operationRouteTestBlock() ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     12,
		RootHash:  bytes.Repeat([]byte{0x41}, 32),
		FileHash:  bytes.Repeat([]byte{0x42}, 32),
	}
}

func TestLiveStoreStateReadReachesTheBackingWhenNotResident(t *testing.T) {
	root := cell.BeginCell().MustStoreUInt(0x77, 8).EndCell()
	backing := &routeCountingBacking{root: root}
	live := New(backing)
	block := operationRouteTestBlock()

	loaded, err := live.LoadStateCellTree(context.Background(), block, nil)
	if err != nil {
		t.Fatalf("state load: %v", err)
	}
	if loaded != root {
		t.Fatal("state load returned the wrong cell")
	}
	if backing.serviceCalls != 1 {
		t.Fatalf("backing calls = %d, want 1", backing.serviceCalls)
	}
}

// lazyRootWithTip builds a root whose single reference is an UNRESOLVED lazy
// boundary: reading through that reference calls loader, exactly the way a state
// tree decoded out of celldb reaches the next cell. The root's own hash is the
// hash it would have had with the subtree stored directly, so it is a faithful
// stand-in for a real state root and not a special shape.
func lazyRootWithTip(t *testing.T, tag uint64, target *cell.Cell, loader cell.LazyCellLoader) *cell.Cell {
	t.Helper()

	carrier := cell.BeginCell().MustStoreRef(target).EndCell()
	lazy, err := cell.CreateWithLazyRefsUnsafe(
		0x0100,
		nil,
		carrier.Hash(),
		[]uint16{carrier.Depth()},
		[]cell.LazyRef{{
			LevelMask: target.LevelMask(),
			Hashes:    target.Hash(),
			Depths:    []uint16{target.Depth()},
		}},
		loader,
	)
	if err != nil {
		t.Fatalf("build lazy boundary: %v", err)
	}
	tip := lazy.MustPeekRef(0)
	if !tip.IsLazy() {
		t.Fatal("boundary is not lazy, the fixture proves nothing")
	}

	root := cell.BeginCell().MustStoreUInt(tag, 8).MustStoreRef(tip).EndCell()
	if root.HashKey() != cell.BeginCell().MustStoreUInt(tag, 8).MustStoreRef(target).EndCell().HashKey() {
		t.Fatal("lazy root does not hash like the tree it stands for")
	}
	return root
}

// A state resident in the live view is an already-decoded tree, and it must come
// back as that tree without consulting the backing. What matters, and what this
// pins, is what happens BELOW the root: the tree's unresolved tips carry the
// loader they were decoded with, and reading through them must land on that
// loader and no other.
//
// The backing is deliberately armed with a DIFFERENT tree of identical bits,
// whose tips resolve through a different loader. It stands in for a second
// decoded cell cache, or a second entry point, or any future attempt to answer
// this read from somewhere else: if the resident branch is ever bypassed or the
// resident tree rebuilt, the pointer check, the tip counters and the backing
// counter all say so at once, and seeing all three is what identifies it as a
// reroute rather than a copy.
//
// The fixture is deliberately LAZY. An earlier version installed a single fully
// materialized cell, which is the one shape with nothing below it to route: it
// could not distinguish any behaviour from any other, so it certified rather
// than probed.
func TestLiveStoreResidentStateTipsKeepTheirOwnLoader(t *testing.T) {
	target := cell.BeginCell().MustStoreUInt(0xa5, 8).EndCell()

	var residentTipLoads, backingTipLoads int
	countingLoader := func(counter *int) cell.LazyCellLoader {
		return func(hash cell.Hash) (*cell.Cell, error) {
			if hash != target.HashKey() {
				return nil, fmt.Errorf("unexpected lazy load %x", hash)
			}
			*counter++
			return target, nil
		}
	}

	// The resident tree is the one the applier produced.
	resident := lazyRootWithTip(t, 0x99, target, countingLoader(&residentTipLoads))
	// What the backing would have answered with: same bits, other loader.
	fromBacking := lazyRootWithTip(t, 0x99, target, countingLoader(&backingTipLoads))

	backing := &routeCountingBacking{root: fromBacking}
	live := New(backing)
	block := operationRouteTestBlock()
	rootHash := resident.HashKey(0)

	live.mu.Lock()
	key, ok := liveBlockLookupKeyFromBlock(block)
	if !ok {
		live.mu.Unlock()
		t.Fatal("block has no live lookup key")
	}
	live.states[key] = storage.BlockState{
		Block:         block,
		StateRootHash: rootHash[:],
		Cell:          resident,
	}
	live.mu.Unlock()

	// Read twice: both the tree AND its per-call stability are the property.
	// chain_state.go's live-successor carry-back compares tip states by pointer,
	// so a read that returns an equal-but-distinct materialization costs a full
	// re-apply per candidate and nothing fails loudly when it happens.
	//
	// Errorf rather than Fatalf: a regression that reroutes the resident tree
	// breaks pointer identity AND moves the tips, and seeing both in one run is
	// what tells you it was a reroute rather than a copy.
	first, err := live.LoadStateCellTree(context.Background(), block, rootHash[:])
	if err != nil {
		t.Fatalf("first state load: %v", err)
	}
	second, err := live.LoadStateCellTree(context.Background(), block, rootHash[:])
	if err != nil {
		t.Fatalf("second state load: %v", err)
	}
	if first != resident {
		t.Error("the state read did not return the resident tree itself")
	}
	if first != second {
		t.Error("two reads of one resident state returned different materializations")
	}

	// Cross the lazy boundary through BOTH handles. Each crossing must land on
	// the loader the resident tree was decoded with. PeekRef deliberately does
	// not resolve — it hands back the boundary — so the crossing is a parse.
	for _, route := range []struct {
		name string
		root *cell.Cell
	}{{"first", first}, {"second", second}} {
		boundary, refErr := route.root.PeekRef(0)
		if refErr != nil {
			t.Fatalf("%s read: peek tip: %v", route.name, refErr)
		}
		if !boundary.IsLazy() {
			t.Fatalf("%s read: the tip is already materialized, nothing was resolved", route.name)
		}

		slice, parseErr := route.root.BeginParse()
		if parseErr != nil {
			t.Fatalf("%s read: parse root: %v", route.name, parseErr)
		}
		if _, parseErr = slice.LoadUInt(8); parseErr != nil {
			t.Fatalf("%s read: parse root tag: %v", route.name, parseErr)
		}
		tip, loadErr := slice.LoadRef()
		if loadErr != nil {
			t.Fatalf("%s read: resolve lazy tip: %v", route.name, loadErr)
		}
		value, valueErr := tip.LoadUInt(8)
		if valueErr != nil {
			t.Fatalf("%s read: read resolved tip: %v", route.name, valueErr)
		}
		if value != 0xa5 {
			t.Fatalf("%s read: lazy tip resolved to the wrong cell: %#x", route.name, value)
		}
	}
	if residentTipLoads != 2 {
		t.Errorf("resident tips resolved through their own loader %d times, want 2", residentTipLoads)
	}
	if backingTipLoads != 0 {
		t.Errorf("a resident tip resolved through the backing's loader %d times, want 0", backingTipLoads)
	}
	if backing.serviceCalls != 0 {
		t.Errorf("a resident state consulted the backing %d times", backing.serviceCalls)
	}

	// A resident state that does not match the requested root is ErrNotFound,
	// and must not silently fall through to a backing lookup.
	wrong := bytes.Repeat([]byte{0xbb}, 32)
	if _, err = live.LoadStateCellTree(context.Background(), block, wrong); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("state load with wrong root = %v, want ErrNotFound", err)
	}
	if backing.serviceCalls != 0 {
		t.Fatalf("a mismatched resident state fell through to the backing %d times", backing.serviceCalls)
	}
	if backingTipLoads != 0 {
		t.Fatalf("the mismatch path resolved a tip through the backing's loader %d times", backingTipLoads)
	}
}
