package service

import (
	"bytes"
	"sort"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

func collectCellRecordsForTest(root *cell.Cell) ([]*storage.CellRecord, error) {
	if root == nil {
		return nil, nil
	}

	seen := map[cell.Hash]struct{}{}
	records := make([]*storage.CellRecord, 0, 1024)
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

		record, err := storage.CellRecordFromCell(current)
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
