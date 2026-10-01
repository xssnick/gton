package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"math/rand"
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestPrepareStateUpdateCellViewsMatchMaterialized(t *testing.T) {
	for seed := int64(0); seed < 32; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			root := randomStateCellViewGraph(t, seed)
			cases := []storagePreparedCellRecordTest{
				{name: "raw", root: root},
				{name: "virtual-zero", root: root.Virtualize(0)},
				{name: "virtual-one", root: root.Virtualize(1)},
				{name: "virtual-two", root: root.Virtualize(2)},
				{name: "traced", root: cell.NewReadSet(root).Root()},
				{name: "traced-virtual", root: cell.NewReadSet(root.Virtualize(1)).Root()},
				{name: "virtual-traced", root: cell.NewReadSet(root).Root().Virtualize(1)},
				{name: "lazy-root", root: stateCellViewLazyRoot(t, root, nil)},
				{name: "lazy-virtual-root", root: stateCellViewLazyRoot(t, root, nil).Virtualize(1)},
			}
			for _, tt := range cases {
				t.Run(tt.name, func(t *testing.T) {
					assertStateCellViewRecords(t, tt.root)
				})
			}
		})
	}
}

func TestPrepareStateUpdateCellViewsNestedProofMessage(t *testing.T) {
	// This real message caught the historical raw-parent logical-level bug.
	// GetMetadata is not an independent oracle for its nested proof refs.
	assertStateCellViewRecords(t, stateMessageWithMerkleProof(t))
}

func TestPrepareStateUpdateCellViewsSparseEffectiveLevel(t *testing.T) {
	pruned := stateCellViewPrunedMask(t, 0b101, 0x1234)
	projected := pruned.Virtualize(2)
	if projected.Level() != 1 || projected.EffectiveLevel() != 2 {
		t.Fatal("fixture must have visible level 1 but effective level 2")
	}
	if projected.Virtualize(1).EffectiveLevel() != 2 {
		t.Fatal("library must preserve an already sufficient sparse projection")
	}

	// Two nested proofs advance the traversal from level 0 to effective 2.
	// The sparse mask projects to 001, but treating its visible level 1 as
	// effective level would persist this level-3 boundary instead of skipping it.
	root := mustStorageMerkleProofBody(t, mustStorageMerkleProofBody(t, pruned))
	assertStateCellViewRecords(t, root)
	records, err := prepareReachableStateUpdateCells(root)
	if err != nil {
		t.Fatal(err)
	}
	if records.Len() != 2 || records.Has(projected.HashKey()) {
		t.Fatalf("effective-level boundary was persisted: %d records", records.Len())
	}
}

func TestPrepareStateUpdateCellViewsLazyErrors(t *testing.T) {
	root := cell.BeginCell().MustStoreUInt(0xAA, 8).
		MustStoreRef(cell.BeginCell().MustStoreUInt(0xBB, 8).EndCell()).EndCell()
	loadErr := errors.New("cell view test loader failure")
	tests := []storagePreparedCellRecordTest{
		{name: "root-load", root: stateCellViewLazyRoot(t, root, loadErr)},
		{name: "child-lazy", root: cell.BeginCell().
			MustStoreRef(stateCellViewLazyRoot(t, root, nil)).EndCell()},
		{name: "traced-child-lazy", root: cell.NewReadSet(cell.BeginCell().
			MustStoreRef(stateCellViewLazyRoot(t, root, loadErr)).EndCell()).Root()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotErr := prepareReachableStateUpdateCells(tt.root)
			want, wantErr := referenceReachableStateUpdateCells(tt.root)
			if wantErr == nil {
				t.Fatal("error fixture unexpectedly succeeded")
			}
			if gotErr == nil || gotErr.Error() != wantErr.Error() {
				t.Fatalf("error changed: got %v, want %v", gotErr, wantErr)
			}
			if got.Len() != want.Len() || got.ByteSize() != want.ByteSize() {
				t.Fatal("partial records returned differently on failure")
			}
			if errors.Is(wantErr, loadErr) != errors.Is(gotErr, loadErr) {
				t.Fatal("loader error wrapping changed")
			}
		})
	}
}

func assertStateCellViewRecords(t *testing.T, root *cell.Cell) {
	t.Helper()

	// Compare the complete released walker and byte encoder, not metadata
	// reconstruction: that has different nested Merkle logical-ref semantics.
	want, err := referenceReachableStateUpdateCells(root)
	if err != nil {
		t.Fatalf("materialized reference: %v", err)
	}
	got, err := prepareReachableStateUpdateCells(root)
	if err != nil {
		t.Fatalf("prepare cell views: %v", err)
	}
	wantRecords := want.AppendTo(nil)
	gotRecords := got.AppendTo(nil)
	if len(gotRecords) != len(wantRecords) || got.ByteSize() != want.ByteSize() {
		t.Fatalf("record sizes changed: got %d/%d bytes, want %d/%d bytes",
			len(gotRecords), got.ByteSize(), len(wantRecords), want.ByteSize())
	}
	for i, expected := range wantRecords {
		actual := gotRecords[i]
		if actual.Hash != expected.Hash {
			t.Fatalf("record %d order/hash changed: got %x, want %x", i, actual.Hash, expected.Hash)
		}
		if !bytes.Equal(actual.Data, expected.Data) {
			t.Fatalf("record %d (%x) bytes changed:\n got %x\nwant %x",
				i, actual.Hash, actual.Data, expected.Data)
		}
		idx, ok := got.IndexOf(expected.Hash)
		if !ok || idx != i || !bytes.Equal(got.Data(expected.Hash), expected.Data) {
			t.Fatalf("record %d index lookup changed", i)
		}
	}
}

func randomStateCellViewGraph(t *testing.T, seed int64) *cell.Cell {
	t.Helper()

	rng := rand.New(rand.NewSource(seed))
	nodes := make([]*cell.Cell, 0, 160)
	for mask := byte(1); mask <= 7; mask++ {
		nodes = append(nodes, stateCellViewPrunedMask(t, mask, uint64(seed)*8+uint64(mask)))
	}
	for i := 0; i < 96; i++ {
		switch rng.Intn(5) {
		case 0:
			nodes = append(nodes, mustStorageMerkleProofBody(t, nodes[rng.Intn(len(nodes))]))
		case 1:
			nodes = append(nodes, mustStorageMerkleUpdateBody(t,
				nodes[rng.Intn(len(nodes))], nodes[rng.Intn(len(nodes))]))
		default:
			dataBits := uint(1 + rng.Intn(63))
			builder := cell.BeginCell().MustStoreUInt(uint64(i), 16).
				MustStoreUInt(rng.Uint64()>>(64-dataBits), dataBits)
			refs := 1 + rng.Intn(4)
			for j := 0; j < refs; j++ {
				ref := nodes[rng.Intn(len(nodes))]
				if rng.Intn(8) == 0 {
					ref = ref.Virtualize(uint8(rng.Intn(3)))
				}
				builder.MustStoreRef(ref)
			}
			nodes = append(nodes, builder.EndCell())
		}
	}
	// Keep every generated node reachable, retain shared refs, and guarantee
	// sparse masks and multiply nested Merkle cells in every seeded fixture.
	nodes = append(nodes, mustStorageMerkleProofBody(t,
		mustStorageMerkleProofBody(t, nodes[4])))
	nodes = append(nodes, mustStorageMerkleUpdateBody(t,
		mustStorageMerkleProofBody(t, nodes[5]), nodes[len(nodes)-1]))
	for len(nodes) > 1 {
		next := make([]*cell.Cell, 0, (len(nodes)+3)/4)
		for i := 0; i < len(nodes); i += 4 {
			builder := cell.BeginCell().MustStoreUInt(uint64(i), 16)
			for j := i; j < min(i+4, len(nodes)); j++ {
				builder.MustStoreRef(nodes[j])
			}
			next = append(next, builder.EndCell())
		}
		nodes = next
	}
	return nodes[0]
}

func stateCellViewPrunedMask(tb testing.TB, mask byte, salt uint64) *cell.Cell {
	tb.Helper()

	count := bits.OnesCount8(mask)
	body := make([]byte, 2+count*34)
	body[0], body[1] = byte(cell.PrunedCellType), mask
	for i := 0; i < count; i++ {
		hash := cell.BeginCell().MustStoreUInt(salt, 64).MustStoreUInt(uint64(i), 8).EndCell().HashKey()
		copy(body[2+i*32:], hash[:])
		binary.BigEndian.PutUint16(body[2+count*32+i*2:], uint16(i+1))
	}
	pruned, err := cell.BeginCell().MustStoreSlice(body, uint(len(body)*8)).EndCellSpecial(true)
	if err != nil {
		tb.Fatal(err)
	}
	return pruned
}

func stateCellViewLazyRoot(t *testing.T, root *cell.Cell, loadErr error) *cell.Cell {
	t.Helper()

	parent := cell.BeginCell().MustStoreRef(root).EndCell()
	record, err := CellRecordFromCell(parent)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := LazyCellRecord(record, func(hash cell.Hash) (*cell.Cell, error) {
		if loadErr != nil {
			return nil, loadErr
		}
		if hash != root.HashKey() {
			return nil, fmt.Errorf("unexpected lazy hash %x", hash)
		}
		return root, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	lazy, err := loaded.PeekRef(0)
	if err != nil {
		t.Fatal(err)
	}
	if !lazy.IsLazy() {
		t.Fatal("fixture did not create a lazy root")
	}
	return lazy
}

func BenchmarkPrepareStateUpdateCellViews(b *testing.B) {
	for _, leaves := range []int{4096, 16384, 65536} {
		for _, levelled := range []bool{false, true} {
			name := "level-zero"
			if levelled {
				name = "pruned-level-one"
			}
			b.Run(fmt.Sprintf("%s/leaves=%d", name, leaves), func(b *testing.B) {
				root := benchmarkStateCellViewGraph(b, leaves, levelled)
				update := mustStorageMerkleUpdateBody(b, root, root)
				want, err := referenceReachableStateUpdateCells(root)
				if err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ReportMetric(float64(want.Len()), "records/op")
				b.ReportMetric(float64(want.ByteSize()), "encoded-B/op")
				for b.Loop() {
					got, err := PrepareStateUpdateCells(update)
					if err != nil {
						b.Fatal(err)
					}
					if got.Len() != want.Len() || got.ByteSize() != want.ByteSize() {
						b.Fatal("prepared record counts changed")
					}
				}
			})
		}
	}
}

func benchmarkStateCellViewGraph(tb testing.TB, leaves int, levelled bool) *cell.Cell {
	tb.Helper()

	nodes := make([]*cell.Cell, leaves)
	for i := range nodes {
		nodes[i] = cell.BeginCell().MustStoreUInt(uint64(i), 32).EndCell()
		if levelled && i%4 == 0 {
			nodes[i] = mustStoragePrunedBranchAtLevel(tb, nodes[i], 1)
		}
	}
	for len(nodes) > 1 {
		next := make([]*cell.Cell, 0, (len(nodes)+3)/4)
		for i := 0; i < len(nodes); i += 4 {
			builder := cell.BeginCell()
			for j := i; j < min(i+4, len(nodes)); j++ {
				builder.MustStoreRef(nodes[j])
			}
			next = append(next, builder.EndCell())
		}
		nodes = next
	}
	return nodes[0]
}

// Frozen pre-value-view traversal and encoding. Keep this independent of the
// optimized helpers: GetMetadata is not equivalent for raw nested Merkle refs.
type referenceStateCellRecordRef struct {
	cell    *cell.Cell
	logical *cell.Cell
}

type referenceStateCellRecordRefs struct {
	items [4]referenceStateCellRecordRef
	count int
}

func referenceReachableStateUpdateCells(root *cell.Cell) (StateCellRecords, error) {
	var builder stateCellRecordBuilder
	stack := []*cell.Cell{root.Virtualize(0)}
	for len(stack) > 0 {
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if current.IsLazy() {
			loader, err := current.BeginParse()
			if err != nil {
				return StateCellRecords{}, fmt.Errorf("load reachable state update cell %x: %w", current.Hash(), err)
			}
			current = loader.BaseCell()
		}

		if current.GetType() == cell.PrunedCellType && current.ActualLevel() == current.EffectiveLevel()+1 {
			continue
		}

		hash := current.HashKey()
		if _, ok := builder.index[hash]; ok {
			continue
		}

		var refs referenceStateCellRecordRefs
		if current.GetType() != cell.PrunedCellType {
			refs.count = int(current.RefsNum())
			for i := range refs.count {
				ref, err := current.PeekRef(i)
				if err != nil {
					return StateCellRecords{}, fmt.Errorf(
						"load reachable state update ref %d from %x: %w",
						i,
						hash,
						err,
					)
				}
				if ref.IsLazy() {
					return StateCellRecords{}, fmt.Errorf(
						"reachable state update ref %d from %x is lazy",
						i,
						hash,
					)
				}
				refs.items[i] = referenceStateCellRecordRef{
					cell:    ref,
					logical: referenceStateCellLogicalRef(current, ref),
				}
			}
		}

		// GetMetadata builds hash, depth and ref slices for every cell. The
		// state-update path only needs the encoded form, so write it directly.
		record, err := referenceReachableStateUpdateCellRecord(current, refs, builder.alloc)
		if err != nil {
			return StateCellRecords{}, fmt.Errorf("build reachable state update cell record %x: %w", hash, err)
		}
		builder.add(record)

		for i := range refs.count {
			stack = append(stack, refs.items[i].cell)
		}
	}
	return builder.build(), nil
}

func referenceReachableStateUpdateCellRecord(
	cl *cell.Cell,
	refs referenceStateCellRecordRefs,
	alloc func(int) []byte,
) (EncodedCellRecord, error) {
	body := cl
	if cl.GetType() == cell.PrunedCellType {
		var err error
		body, err = referenceMaterializePrunedStateCell(cl)
		if err != nil {
			return EncodedCellRecord{}, err
		}
	}

	record := referenceEncodedStateCellRecord(cl, body, refs, alloc)
	return record, nil
}

func referenceEncodedStateCellRecord(
	view *cell.Cell,
	body *cell.Cell,
	refs referenceStateCellRecordRefs,
	alloc func(int) []byte,
) EncodedCellRecord {
	cellBits := body.BitsSize()
	d1, d2 := cellRecordDescriptorsForLevelMask(body, view.LevelMask(), refs.count, cellBits)
	layout, refsSize := referenceStateCellRefLayout(refs)
	size := 2 + int(d2/2+d2%2) + refsSize

	encoded := alloc(size)
	referenceEncodeStateCellRecordTo(encoded, body, refs, d1, d2, layout)
	return EncodedCellRecord{
		Hash: view.HashKey(),
		Data: encoded,
	}
}

func referenceEncodeStateCellRecordTo(
	buf []byte,
	body *cell.Cell,
	refs referenceStateCellRecordRefs,
	d1 byte,
	d2 byte,
	layout encodedCellRecordRefLayout,
) {
	pos := 0
	if layout.compactRefs {
		d1 |= encodedCellRecordCompactRefsFlag
	}
	buf[pos] = d1
	buf[pos+1] = d2
	pos += 2

	pos += body.SerializeBOCBodyTo(buf[pos:])
	if layout.compactRefs {
		buf[pos] = layout.slowRefs
		pos++
	}
	for i := range refs.count {
		logicalRef := refs.items[i].logical
		levelMask := logicalRef.LevelMask()
		if layout.compactRefs && layout.slowRefs&(1<<uint(i)) == 0 {
			hash := logicalRef.HashKeyAt(0)
			copy(buf[pos:pos+encodedCellRecordHashSize], hash[:])
			pos += encodedCellRecordHashSize
			binary.BigEndian.PutUint16(
				buf[pos:pos+encodedCellRecordDepthSize],
				logicalRef.Depth(0),
			)
			pos += encodedCellRecordDepthSize
			continue
		}

		buf[pos] = levelMask.Mask
		pos++
		for level := 0; level <= levelMask.GetLevel(); level++ {
			if !levelMask.IsSignificant(level) {
				continue
			}
			hash := logicalRef.HashKeyAt(level)
			copy(buf[pos:pos+encodedCellRecordHashSize], hash[:])
			pos += encodedCellRecordHashSize
		}
		for level := 0; level <= levelMask.GetLevel(); level++ {
			if !levelMask.IsSignificant(level) {
				continue
			}
			binary.BigEndian.PutUint16(
				buf[pos:pos+encodedCellRecordDepthSize],
				logicalRef.Depth(level),
			)
			pos += encodedCellRecordDepthSize
		}
	}
}

func referenceStateCellRefLayout(refs referenceStateCellRecordRefs) (encodedCellRecordRefLayout, int) {
	if refs.count == 0 {
		return encodedCellRecordRefLayout{}, 0
	}

	refsSize := 0
	compactRefsSize := 1
	hasCommonRef := false
	var layout encodedCellRecordRefLayout
	for i := range refs.count {
		logicalRef := refs.items[i].logical
		levelMask := logicalRef.LevelMask()
		hashesCount := CellRefHashesCount(levelMask.Mask)
		refSize := 1 + hashesCount*(encodedCellRecordHashSize+encodedCellRecordDepthSize)
		refsSize += refSize
		if levelMask.Mask == 0 {
			hasCommonRef = true
			compactRefsSize += encodedCellRecordHashSize + encodedCellRecordDepthSize
			continue
		}
		layout.slowRefs |= 1 << uint(i)
		compactRefsSize += refSize
	}
	layout.compactRefs = hasCommonRef && compactRefsSize <= refsSize
	if layout.compactRefs {
		return layout, compactRefsSize
	}
	return layout, refsSize
}

func referenceStateCellLogicalRef(parent *cell.Cell, ref *cell.Cell) *cell.Cell {
	// Match tonutils cellRefView.logicalBoundaryRef: PeekRef already applies
	// the parent view, while non-virtual Merkle parents shift children by one.
	if parent.IsVirtualized() {
		return ref
	}

	// A raw parent is its own view at its own level, and its children live at
	// that level, not at zero. The distinction only exists inside a Merkle
	// proof carried by the state — a message body proving a dictionary entry,
	// say — where the walk reaches raw level-1 cells: PeekRef on the proof view
	// hands the interior root back raw, because a level-1 cell viewed at level
	// 1 is itself. Virtualizing its children to level 0 recorded every level-1
	// child as a level-0 ref: the level-1 hash and depth were dropped, the
	// parent's record then decoded to a depth at level 1 one deeper than the
	// cell's own, and every lazy read of the proof's interior failed with
	// "loaded lazy ref does not match placeholder: depth mismatch at level 1".
	// On the stand that cost a shard block its slot 65 times in a row, on one
	// inbound message whose body carried such a proof.
	level := parent.Level()
	switch parent.GetType() {
	case cell.MerkleProofCellType, cell.MerkleUpdateCellType:
		level++
	}
	return ref.Virtualize(uint8(level))
}

func referenceMaterializePrunedStateCell(cl *cell.Cell) (*cell.Cell, error) {
	if !cl.IsVirtualized() {
		return cl, nil
	}

	loader, err := cl.BeginParse()
	if err != nil {
		return nil, err
	}
	bits, data, err := loader.RestBits()
	if err != nil {
		return nil, err
	}

	builder := cell.BeginCell()
	if err = builder.StoreSlice(data, bits); err != nil {
		return nil, err
	}
	raw, err := builder.EndCellSpecial(true)
	if err != nil {
		return nil, err
	}
	return raw, nil
}
