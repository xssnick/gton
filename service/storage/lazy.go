package storage

import (
	"crypto/sha256"
	"encoding/binary"
	"math/bits"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	cellHashSize  = 32
	cellDepthSize = 2
)

// LazyCellRecord rebuilds only the requested cell and leaves refs as lazy placeholders.
func LazyCellRecord(record *CellRecord) *cell.Cell {
	hashes, depths := lazyCellHashesDepths(record)
	descriptors := uint16(record.D1)<<8 | uint16(record.D2)
	return cell.CreateLazy(descriptors, record.Data, hashes, depths, lazyRefsFromRecord(record))
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

func lazyCellHashesDepths(record *CellRecord) ([]byte, []uint16) {
	levelMask := cell.LevelMask{Mask: record.D1 >> 5}
	hashesCount := CellRefHashesCount(levelMask.Mask)
	typ := lazyCellType(record)

	if typ == cell.PrunedCellType {
		hashes := make([]byte, hashesCount*cellHashSize)
		for off := 0; off < len(hashes); off += cellHashSize {
			copy(hashes[off:off+cellHashSize], record.Hash)
		}
		return hashes, make([]uint16, hashesCount)
	}

	hashes := make([]byte, hashesCount*cellHashSize)
	depths := make([]uint16, hashesCount)
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

	return hashes, depths
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
