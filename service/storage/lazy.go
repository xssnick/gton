package storage

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/bits"
	"sync"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	cellHashSize  = 32
	cellDepthSize = 2
)

type lazyCellRecordScratch struct {
	record    CellRecord
	refs      [4]CellRefRecord
	lazyRefs  [4]cell.LazyRef
	hashes    [4 * cellHashSize]byte
	depths    [4]uint16
	refDepths [4][4]uint16
}

var lazyCellRecordScratchPool = sync.Pool{
	New: func() any {
		return new(lazyCellRecordScratch)
	},
}

// DecodeLazyCellRecordTrusted decodes an encoded database record directly
// into a cell. The returned cell owns every byte it keeps from encoded.
func DecodeLazyCellRecordTrusted(hash []byte, encoded []byte, loader cell.LazyCellLoader) (*cell.Cell, error) {
	scratch := lazyCellRecordScratchPool.Get().(*lazyCellRecordScratch)
	defer func() {
		scratch.record = CellRecord{}
		clear(scratch.refs[:])
		clear(scratch.lazyRefs[:])
		lazyCellRecordScratchPool.Put(scratch)
	}()

	decodeCellRecordTrustedInto(hash, encoded, &scratch.record, scratch.refs[:])
	record := &scratch.record
	if lazyCellType(record) == cell.PrunedCellType {
		return prunedCellRecordView(record.Hash, record.D1, record.D2, record.Data)
	}

	levelMask := cell.LevelMask{Mask: record.D1 >> 5}
	hashesCount := CellRefHashesCount(levelMask.Mask)
	hashes := scratch.hashes[:hashesCount*cellHashSize]
	depths := scratch.depths[:hashesCount]
	lazyCellHashesDepthsInto(record, hashes, depths)

	refs := scratch.lazyRefs[:len(record.Refs)]
	for i, ref := range record.Refs {
		refHashesCount := CellRefHashesCount(ref.LevelMask)
		refDepths := scratch.refDepths[i][:refHashesCount]
		for j := range refDepths {
			refDepths[j] = binary.BigEndian.Uint16(ref.Depths[j*cellDepthSize : (j+1)*cellDepthSize])
		}
		refs[i] = cell.LazyRef{
			LevelMask: cell.LevelMask{Mask: ref.LevelMask},
			Hashes:    ref.Hashes,
			Depths:    refDepths,
		}
	}

	descriptors := uint16(record.D1)<<8 | uint16(record.D2)
	// CreateWithLazyRefsUnsafe copies data into its slab, so record.Data — a
	// view into encoded — needs no clone here.
	loaded, err := cell.CreateWithLazyRefsUnsafe(
		descriptors,
		record.Data,
		hashes,
		depths,
		refs,
		loader,
	)
	if err != nil {
		return nil, err
	}
	return cellViewForRecordHash(loaded, record.Hash)
}

// LazyCellRecord rebuilds only the requested cell and leaves refs as lazy placeholders.
func LazyCellRecord(record *CellRecord, loader cell.LazyCellLoader) (*cell.Cell, error) {
	if lazyCellType(record) == cell.PrunedCellType {
		return prunedCellRecordView(record.Hash, record.D1, record.D2, record.Data)
	}

	hashes, depths, err := lazyCellHashesDepths(record)
	if err != nil {
		return nil, err
	}
	descriptors := uint16(record.D1)<<8 | uint16(record.D2)
	loaded, err := cell.CreateWithLazyRefsUnsafe(descriptors, record.Data, hashes, depths, lazyRefsFromRecord(record), loader)
	if err != nil {
		return nil, err
	}
	return cellViewForRecordHash(loaded, record.Hash)
}

func prunedCellRecord(record *CellRecord) (*cell.Cell, error) {
	bits, err := cellRecordBits(record)
	if err != nil {
		return nil, err
	}

	builder := cell.BeginCell()
	if err = builder.StoreSlice(record.Data, bits); err != nil {
		return nil, err
	}
	return builder.EndCellSpecial(true)
}

func prunedCellRecordView(hash []byte, d1 byte, d2 byte, data []byte) (*cell.Cell, error) {
	if d1&8 == 0 {
		return nil, fmt.Errorf("pruned cell record is not special")
	}
	if d1&7 != 0 {
		return nil, fmt.Errorf("pruned cell record has refs")
	}
	if len(data) == 0 || cell.Type(data[0]) != cell.PrunedCellType {
		return nil, fmt.Errorf("pruned cell record has invalid body")
	}

	record := &CellRecord{
		Hash: hash,
		D1:   d1,
		D2:   d2,
		Data: data,
	}
	pruned, err := prunedCellRecord(record)
	if err != nil {
		return nil, err
	}
	view, err := cellViewForRecordHash(pruned, hash)
	if err != nil {
		return nil, err
	}
	if got, want := view.LevelMask().Mask, d1>>5; got != want {
		return nil, fmt.Errorf("pruned cell record level mask mismatch: got=%d want=%d", got, want)
	}
	return view, nil
}

func cellViewForRecordHash(cl *cell.Cell, hash []byte) (*cell.Cell, error) {
	if len(hash) != cellHashSize {
		return nil, fmt.Errorf("cell record hash size mismatch: %d", len(hash))
	}

	var want cell.Hash
	copy(want[:], hash)
	if cl.HashKey() == want {
		return cl, nil
	}

	for level := 0; level < cl.Level(); level++ {
		view := cl.Virtualize(uint8(level))
		if view.HashKey() == want {
			return view, nil
		}
	}
	return nil, fmt.Errorf("cell record hash mismatch: got=%x want=%x", cl.Hash(), hash)
}

func lazyRefsFromRecord(record *CellRecord) []cell.LazyRef {
	refs := make([]cell.LazyRef, len(record.Refs))
	for i, ref := range record.Refs {
		hashesCount := CellRefHashesCount(ref.LevelMask)
		depths := make([]uint16, hashesCount)
		for j := range depths {
			depths[j] = binary.BigEndian.Uint16(ref.Depths[j*cellDepthSize : (j+1)*cellDepthSize])
		}
		refs[i] = cell.LazyRef{
			LevelMask: cell.LevelMask{Mask: ref.LevelMask},
			Hashes:    ref.Hashes,
			Depths:    depths,
		}
	}
	return refs
}

func lazyCellHashesDepths(record *CellRecord) ([]byte, []uint16, error) {
	levelMask := cell.LevelMask{Mask: record.D1 >> 5}
	hashesCount := CellRefHashesCount(levelMask.Mask)
	typ := lazyCellType(record)

	if typ == cell.PrunedCellType {
		view, err := prunedCellRecordView(record.Hash, record.D1, record.D2, record.Data)
		if err != nil {
			return nil, nil, err
		}
		meta := view.GetMetadata()
		if len(meta.Hashes) != hashesCount || len(meta.Depths) != hashesCount {
			return nil, nil, fmt.Errorf("pruned cell metadata size mismatch: hashes=%d depths=%d want=%d", len(meta.Hashes), len(meta.Depths), hashesCount)
		}

		hashes := make([]byte, hashesCount*cellHashSize)
		depths := make([]uint16, hashesCount)
		for i := 0; i < hashesCount; i++ {
			copy(hashes[i*cellHashSize:(i+1)*cellHashSize], meta.Hashes[i][:])
			depths[i] = meta.Depths[i]
		}
		return hashes, depths, nil
	}

	hashes := make([]byte, hashesCount*cellHashSize)
	depths := make([]uint16, hashesCount)
	lazyCellHashesDepthsInto(record, hashes, depths)
	return hashes, depths, nil
}

func lazyCellHashesDepthsInto(record *CellRecord, hashes []byte, depths []uint16) {
	levelMask := cell.LevelMask{Mask: record.D1 >> 5}
	hashesCount := CellRefHashesCount(levelMask.Mask)
	typ := lazyCellType(record)
	isMerkle := typ == cell.MerkleProofCellType || typ == cell.MerkleUpdateCellType

	var prevHash [cellHashSize]byte
	hasPrevHash := false
	var hashBuf [2 + 128 + 4*cellDepthSize + 4*cellHashSize]byte
	hashIndex := 0

	for level := 0; level <= levelMask.GetLevel(); level++ {
		if !levelMask.IsSignificant(level) {
			continue
		}

		pos := 0
		hashBuf[pos] = (record.D1 & 0x0f) + levelMask.Apply(level).Mask*32
		hashBuf[pos+1] = record.D2
		pos += 2

		if hasPrevHash {
			pos += copy(hashBuf[pos:], prevHash[:])
		} else {
			pos += copy(hashBuf[pos:], record.Data)
		}

		childLevel := level
		if isMerkle {
			childLevel++
		}

		var depth uint16
		for _, ref := range record.Refs {
			childDepth := cellRefDepthAtLevel(ref, childLevel)
			binary.BigEndian.PutUint16(hashBuf[pos:pos+cellDepthSize], childDepth)
			pos += cellDepthSize
			if childDepth > depth {
				depth = childDepth
			}
		}
		if len(record.Refs) > 0 {
			depth++
		}

		hashOffset := hashIndex * cellHashSize
		depths[hashIndex] = depth
		if hashIndex == hashesCount-1 {
			copy(hashes[hashOffset:hashOffset+cellHashSize], record.Hash)
			hashIndex++
			continue
		}

		for _, ref := range record.Refs {
			pos += copy(hashBuf[pos:], cellRefHashAtLevel(ref, childLevel))
		}

		sum := sha256.Sum256(hashBuf[:pos])
		copy(prevHash[:], sum[:])
		hasPrevHash = true
		copy(hashes[hashOffset:hashOffset+cellHashSize], sum[:])
		hashIndex++
	}
}

func lazyCellType(record *CellRecord) cell.Type {
	if record.D1&8 == 0 {
		return cell.OrdinaryCellType
	}
	return cell.Type(record.Data[0])
}

func cellRefHashAtLevel(ref CellRefRecord, level int) []byte {
	index := cellRefHashIndex(ref.LevelMask, level)
	return ref.Hashes[index*cellHashSize : (index+1)*cellHashSize]
}

func cellRefDepthAtLevel(ref CellRefRecord, level int) uint16 {
	index := cellRefHashIndex(ref.LevelMask, level)
	return binary.BigEndian.Uint16(ref.Depths[index*cellDepthSize : (index+1)*cellDepthSize])
}

func cellRefHashIndex(levelMask byte, level int) int {
	if level <= 0 {
		return 0
	}
	if level >= 8 {
		return bits.OnesCount8(levelMask)
	}
	return bits.OnesCount8(levelMask & byte((1<<uint(level))-1))
}
