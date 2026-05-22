package storage

import (
	"bytes"
	"context"
	"fmt"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type StateCellTreeImporter interface {
	ImportStateCellTree(ctx context.Context, block ton.BlockIDExt, root *cell.Cell, totalCells uint64) (*cell.Cell, error)
	ImportStateBOCView(ctx context.Context, block ton.BlockIDExt, view *cell.BOCView) (*cell.Cell, error)
	LoadStateCellTree(ctx context.Context, block ton.BlockIDExt, rootHash []byte) (*cell.Cell, error)
	TrustImportedStateCellHashes() bool
}

type DownloadedState interface {
	Block() ton.BlockIDExt
	Decode(ctx context.Context) (*BlockState, error)
	ImportCells(ctx context.Context, cells StateCellTreeImporter) (*BlockState, error)
	Cleanup() error
}

type StateStorage interface {
	StateCellTreeImporter
	SaveCurrentState(ctx context.Context, state *CurrentState) error
	CurrentState(ctx context.Context) (*CurrentState, error)
	SaveStateSyncProgress(ctx context.Context, state *CurrentState) error
	StateSyncProgress(ctx context.Context) (*CurrentState, error)
	ClearStateSyncProgress(ctx context.Context) error
	SaveVerifiedKeyBlockProgress(ctx context.Context, block ton.BlockIDExt) error
	VerifiedKeyBlockProgress(ctx context.Context) (ton.BlockIDExt, error)
	SaveBlockState(ctx context.Context, state *BlockState) error
	SaveStateCheckpoint(ctx context.Context, blocks []*BlockState, current *CurrentState) error
	SaveStateCheckpointWithCells(ctx context.Context, blocks []*BlockState, current *CurrentState, cells []EncodedCellRecord) error
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
		Block:          state.Block,
		StateRootHash:  bytes.Clone(state.StateRootHash),
		StateCellHash:  bytes.Clone(state.StateCellHash),
		StateFileHash:  bytes.Clone(state.StateFileHash),
		MasterchainRef: cloneBlockIDPtr(state.MasterchainRef),
		CellGeneration: state.CellGeneration,
		Cell:           state.Cell,
		Parsed:         state.Parsed,
	}
}

func BlockStateWithoutCells(state *BlockState) BlockState {
	if state == nil {
		return BlockState{}
	}

	return BlockState{
		Block:          state.Block,
		StateRootHash:  bytes.Clone(state.StateRootHash),
		StateCellHash:  bytes.Clone(state.StateCellHash),
		StateFileHash:  bytes.Clone(state.StateFileHash),
		MasterchainRef: cloneBlockIDPtr(state.MasterchainRef),
		CellGeneration: state.CellGeneration,
	}
}

func cloneBlockIDPtr(block *ton.BlockIDExt) *ton.BlockIDExt {
	if block == nil {
		return nil
	}
	cloned := *block
	return &cloned
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
