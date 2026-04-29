package storage

import (
	"bytes"
	"context"
	"fmt"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type StateCellTreeImporter interface {
	ImportStateCellTree(ctx context.Context, block ton.BlockIDExt, root *cell.Cell, parsedCells []cell.Cell, totalCells uint64) (*cell.Cell, error)
	ImportStateCellTrees(ctx context.Context, trees []StateCellTreeImport) ([]*cell.Cell, error)
	LoadStateCellTree(ctx context.Context, block ton.BlockIDExt, rootHash []byte) (*cell.Cell, uint64, error)
}

type StateCellTreeImport struct {
	Block       ton.BlockIDExt
	Root        *cell.Cell
	ParsedCells []cell.Cell
	TotalCells  uint64
}

type StateStorage interface {
	StateCellTreeImporter
	SaveCurrentState(ctx context.Context, state *CurrentState) error
	CurrentState(ctx context.Context) (*CurrentState, error)
	SaveStateSyncProgress(ctx context.Context, state *CurrentState) error
	StateSyncProgress(ctx context.Context) (*CurrentState, error)
	ClearStateSyncProgress(ctx context.Context) error
	SaveBlockState(ctx context.Context, state *BlockState) error
	SaveBlockStateAndCurrentState(ctx context.Context, block *BlockState, current *CurrentState) error
	SaveBlockStatesAndCurrentState(ctx context.Context, blocks []*BlockState, current *CurrentState) error
	BlockState(ctx context.Context, block ton.BlockIDExt) (*BlockState, error)
}

func CloneCurrentState(state *CurrentState) *CurrentState {
	if state == nil {
		return nil
	}

	cloned := &CurrentState{
		SyncedAt:         state.SyncedAt,
		ShardClientSeqno: state.ShardClientSeqno,
		Masterchain:      *CloneBlockState(&state.Masterchain),
		Shards:           make(map[ShardKey]BlockState, len(state.Shards)),
	}
	for key, shard := range state.Shards {
		cloned.Shards[key] = *CloneBlockState(&shard)
	}
	return cloned
}

func CloneBlockState(state *BlockState) *BlockState {
	if state == nil {
		return nil
	}

	return &BlockState{
		Block:            state.Block,
		StateRootHash:    bytes.Clone(state.StateRootHash),
		StateCellHash:    bytes.Clone(state.StateCellHash),
		StateFileHash:    bytes.Clone(state.StateFileHash),
		CellsCount:       state.CellsCount,
		Cell:             state.Cell,
		Parsed:           state.Parsed,
		DownloadedAt:     state.DownloadedAt,
		ReusedStateCells: append([]cell.MerkleUpdateReusedCell(nil), state.ReusedStateCells...),
		ReusedStateRefs:  append([]cell.MerkleUpdateReusedRef(nil), state.ReusedStateRefs...),
	}
}

func BlockKey(block ton.BlockIDExt) string {
	return fmt.Sprintf(
		"%d:%016x:%d:%x:%x",
		block.Workchain,
		uint64(block.Shard),
		block.SeqNo,
		block.RootHash,
		block.FileHash,
	)
}
