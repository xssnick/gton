package storage

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

func formatCellRebuildError(record *CellRecord, err error) error {
	cellBits, _ := cellRecordBits(record)
	return fmt.Errorf(
		"rebuild cell %x special=%v bits=%d refs=%d prefix=%x: %w",
		record.Hash,
		record.D1&8 != 0,
		cellBits,
		len(record.Refs),
		recordDataPrefix(record.Data, 16),
		err,
	)
}

func recordDataPrefix(data []byte, max int) []byte {
	if len(data) <= max {
		return data
	}
	return data[:max]
}

type cellLoadFrame struct {
	hash      cell.Hash
	record    *CellRecord
	refs      [4]cell.Hash
	refsCount int
	refsReady bool
}

type cellGraphCache struct {
	index map[cell.Hash]uint32
	cells []*cell.Cell
}

func newCellGraphCache() cellGraphCache {
	return cellGraphCache{index: map[cell.Hash]uint32{}}
}

func (c *cellGraphCache) get(hash cell.Hash) *cell.Cell {
	idx, ok := c.index[hash]
	if !ok {
		return nil
	}
	return c.cells[idx]
}

func (c *cellGraphCache) set(hash cell.Hash, cl *cell.Cell) {
	c.index[hash] = uint32(len(c.cells))
	c.cells = append(c.cells, cl)
}

func (c *cellGraphCache) len() int {
	return len(c.cells)
}

type LoadCellGraphProgress struct {
	RecordsLoaded int64
	CellsBuilt    int64
	StackDepth    int
	CacheSize     int
	Done          bool
}

func LoadCellGraph(ctx context.Context, hash []byte, loadRecord func(hash []byte) (*CellRecord, error), progress ...func(LoadCellGraphProgress)) (*cell.Cell, error) {
	rootHash, err := hashBytesToCellHash(hash)
	if err != nil {
		return nil, err
	}

	cache := newCellGraphCache()
	stack := []cellLoadFrame{{hash: rootHash}}
	var recordsLoaded int64
	var cellsBuilt int64
	var iterations uint64

	reportProgress := func(done bool) {
		if len(progress) == 0 || progress[0] == nil {
			return
		}
		progress[0](LoadCellGraphProgress{
			RecordsLoaded: recordsLoaded,
			CellsBuilt:    cellsBuilt,
			StackDepth:    len(stack),
			CacheSize:     cache.len(),
			Done:          done,
		})
	}

	for len(stack) > 0 {
		if iterations&0x3fff == 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
		}
		iterations++

		top := &stack[len(stack)-1]
		if cache.get(top.hash) != nil {
			stack = stack[:len(stack)-1]
			continue
		}

		if top.record == nil {
			record, err := loadRecord(top.hash[:])
			if err != nil {
				return nil, err
			}
			top.record = record
			recordsLoaded++
			if recordsLoaded&0x1ffff == 0 {
				reportProgress(false)
			}
		}

		if !top.refsReady {
			if len(top.record.Refs) > len(top.refs) {
				return nil, fmt.Errorf("invalid cell refs count %d", len(top.record.Refs))
			}
			for i := range top.record.Refs {
				refHashBytes, err := CellRefHash(top.record.Refs[i])
				if err != nil {
					return nil, err
				}
				refHash, err := hashBytesToCellHash(refHashBytes)
				if err != nil {
					return nil, err
				}
				top.refs[i] = refHash
			}
			top.refsCount = len(top.record.Refs)
			top.refsReady = true
		}

		var pushed bool
		for i := top.refsCount - 1; i >= 0; i-- {
			refHash := top.refs[i]
			if cache.get(refHash) != nil {
				continue
			}
			if refHash == top.hash {
				return nil, fmt.Errorf("recursive cell reference %x", refHash[:])
			}

			stack = append(stack, cellLoadFrame{hash: refHash})
			pushed = true
			break
		}
		if pushed {
			continue
		}

		builder := cell.BeginCell()
		cellBits, err := cellRecordBits(top.record)
		if err != nil {
			return nil, formatCellRebuildError(top.record, err)
		}
		if err := builder.StoreSlice(top.record.Data, cellBits); err != nil {
			return nil, err
		}
		for i := 0; i < top.refsCount; i++ {
			refHash := top.refs[i]
			ref := cache.get(refHash)
			if ref == nil {
				return nil, fmt.Errorf("missing child cell %x", refHash[:])
			}
			if err := builder.StoreRef(ref); err != nil {
				return nil, err
			}
		}

		rebuilt, err := builder.EndCellSpecial(top.record.D1&8 != 0)
		if err != nil {
			return nil, formatCellRebuildError(top.record, err)
		}
		gotHash := rebuilt.HashKey()
		if gotHash != top.hash {
			return nil, fmt.Errorf("rebuilt cell hash mismatch: got=%x want=%x", gotHash[:], top.hash[:])
		}

		cache.set(top.hash, rebuilt)
		cellsBuilt++
		if cellsBuilt&0x1ffff == 0 {
			reportProgress(false)
		}
		stack = stack[:len(stack)-1]
	}

	root := cache.get(rootHash)
	if root == nil {
		return nil, fmt.Errorf("missing root cell %x", hash)
	}
	reportProgress(true)
	return root, nil
}

func hashBytesToCellHash(hash []byte) (cell.Hash, error) {
	var key cell.Hash
	if len(hash) != len(key) {
		return key, fmt.Errorf("cell hash size mismatch: %d", len(hash))
	}
	copy(key[:], hash)
	return key, nil
}

func CollectCellRecords(root *cell.Cell) ([]*CellRecord, error) {
	if root == nil {
		return nil, nil
	}

	seen := map[cell.Hash]struct{}{}
	records := make([]*CellRecord, 0, 1024)
	stack := []*cell.Cell{root}

	for len(stack) > 0 {
		idx := len(stack) - 1
		current := stack[idx]
		stack = stack[:idx]

		hash := current.HashKey()
		if _, ok := seen[hash]; ok {
			continue
		}
		seen[hash] = struct{}{}

		record, err := CellRecordFromCell(current)
		if err != nil {
			return nil, err
		}
		records = append(records, record)

		for i := uint(0); i < current.RefsNum(); i++ {
			ref, err := current.PeekRef(int(i))
			if err != nil {
				return nil, err
			}
			stack = append(stack, ref)
		}
	}

	sort.Slice(records, func(i, j int) bool {
		return bytes.Compare(records[i].Hash, records[j].Hash) < 0
	})
	return records, nil
}
