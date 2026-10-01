package collator

import (
	"fmt"
	"math"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	maxCollatedBOCDepth = 1024
	// The canonical serializer reserves the top three uint32 indexes as visit
	// sentinels and rejects the next cell once this many were admitted.
	maxCollatedBOCCells = uint64(math.MaxUint32) - 2
)

type bocSizeWriter struct {
	size uint64
}

func (w *bocSizeWriter) Write(data []byte) (int, error) {
	bytes := uint64(len(data))
	if w.size > math.MaxUint64-bytes {
		return 0, fmt.Errorf("boc streamed size overflows uint64")
	}
	w.size += bytes
	return len(data), nil
}

type bocSizeCell struct {
	cell  *cell.Cell
	depth int
}

// collatedBOCSize measures exactly the CRC-only, unindexed BOC used by
// collated data admission. Resident cells need only one identity/shape walk:
// without an index, cache bits or stored hashes, cell ordering cannot change
// the encoded length, and writing descriptors, bodies and CRC adds no size
// information beyond the shape.
func collatedBOCSize(roots []*cell.Cell) (uint64, error) {
	size, resident, err := residentCollatedBOCSize(roots)
	if err != nil || resident {
		return size, err
	}

	// Lazy loading has validation and dedup ordering owned by the canonical
	// serializer. A first-seen lazy cell is uncommon in these locally built
	// proof roots; when one is supplied by a provider, preserve those semantics
	// by measuring the real stream instead of approximating its materialized
	// shape. Already-seen lazy hashes are skipped by the fast walk before this
	// fallback, exactly as the serializer skips them before loading.
	var streamed bocSizeWriter
	if err = cell.WriteBOCWithOptions(&streamed, roots, cell.BOCSerializeOptions{WithCRC32C: true}); err != nil {
		return 0, err
	}

	return streamed.size, nil
}

// residentCollatedBOCSize reports resident=false when exact measurement must
// be delegated to the serializer because a first-seen lazy cell was reached.
func residentCollatedBOCSize(roots []*cell.Cell) (size uint64, resident bool, err error) {
	if len(roots) == 0 {
		return 0, true, nil
	}

	var seen proofSeenHashSet
	seen.presize(len(roots))
	stack := make([]bocSizeCell, 0, len(roots)+16)
	for i := len(roots) - 1; i >= 0; i-- {
		stack = append(stack, bocSizeCell{cell: roots[i]})
	}

	var (
		cells     uint64
		refs      uint64
		dataBytes uint64
	)
	for len(stack) > 0 {
		last := len(stack) - 1
		item := stack[last]
		stack = stack[:last]

		if item.depth > maxCollatedBOCDepth {
			return 0, true, fmt.Errorf("cell depth too large")
		}
		if item.cell == nil {
			return 0, true, fmt.Errorf("cell is nil")
		}

		hash := item.cell.HashKey()
		if seen.observe(hash, proofSeenLoaded, 0) != 0 {
			continue
		}
		if item.cell.IsLazy() {
			return 0, false, nil
		}
		// Canonical serialization deduplicates before rejecting a virtualized
		// view, so a repeated hash above remains valid while a first occurrence
		// keeps the serializer's sentinel error.
		if item.cell.IsVirtualized() {
			return 0, true, cell.ErrVirtualizedCell
		}
		if cells == maxCollatedBOCCells {
			return 0, true, fmt.Errorf("too many cells to serialize")
		}
		cells++

		refsNum := item.cell.RefsNum()
		if refs > math.MaxUint64-uint64(refsNum) {
			return 0, true, fmt.Errorf("boc reference count overflows uint64")
		}
		refs += uint64(refsNum)

		bodyBytes := uint64(2 + item.cell.SerializedBOCBodySize())
		if dataBytes > math.MaxUint64-bodyBytes {
			return 0, true, fmt.Errorf("boc data size overflows uint64")
		}
		dataBytes += bodyBytes

		for i := int(refsNum) - 1; i >= 0; i-- {
			ref, refErr := item.cell.PeekRef(i)
			if refErr != nil {
				return 0, true, fmt.Errorf("failed to load ref %d: %w", i, refErr)
			}
			stack = append(stack, bocSizeCell{cell: ref, depth: item.depth + 1})
		}
	}

	refBytes := bocUintBytes(cells)
	refDataBytes, ok := checkedBOCMul(refs, refBytes)
	if !ok || dataBytes > math.MaxUint64-refDataBytes {
		return 0, true, fmt.Errorf("boc data size overflows uint64")
	}
	dataBytes += refDataBytes

	offsetBytes := bocUintBytes(dataBytes)
	rootBytes, ok := checkedBOCMul(uint64(len(roots)), refBytes)
	if !ok {
		return 0, true, fmt.Errorf("boc root index size overflows uint64")
	}

	// magic + flags + offset width, then cells/roots/absent counters, the data
	// size counter, root indexes, cell data and CRC32C.
	headerBytes := uint64(6) + 3*refBytes + offsetBytes
	total, ok := checkedBOCAdd(headerBytes, rootBytes)
	if ok {
		total, ok = checkedBOCAdd(total, dataBytes)
	}
	if ok {
		total, ok = checkedBOCAdd(total, 4)
	}
	if !ok {
		return 0, true, fmt.Errorf("boc total size overflows uint64")
	}

	return total, true, nil
}

func bocUintBytes(value uint64) uint64 {
	switch {
	case value < 1<<8:
		return 1
	case value < 1<<16:
		return 2
	case value < 1<<24:
		return 3
	case value < 1<<32:
		return 4
	case value < 1<<40:
		return 5
	case value < 1<<48:
		return 6
	case value < 1<<56:
		return 7
	default:
		return 8
	}
}

func checkedBOCAdd(left, right uint64) (uint64, bool) {
	if left > math.MaxUint64-right {
		return 0, false
	}
	return left + right, true
}

func checkedBOCMul(left, right uint64) (uint64, bool) {
	if left != 0 && right > math.MaxUint64/left {
		return 0, false
	}
	return left * right, true
}
