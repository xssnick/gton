package storage

import (
	"bytes"
	"context"
	"time"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type StateCellTreeImporter interface {
	ImportStateCellTree(ctx context.Context, block ton.BlockIDExt, root *cell.Cell, totalCells uint64) (*cell.Cell, error)
	ImportStateBOCView(ctx context.Context, block ton.BlockIDExt, view *cell.BOCView) (*cell.Cell, error)
	LoadStateCellTree(ctx context.Context, block ton.BlockIDExt, rootHash []byte) (*cell.Cell, error)
	TrustImportedStateCellHashes() bool
	ReuseImportedSplitStatePartCells() bool
}

type DownloadedState interface {
	Block() ton.BlockIDExt
	Decode(ctx context.Context) (*BlockState, error)
	ImportCells(ctx context.Context, cells StateCellTreeImporter) (*BlockState, error)
	Cleanup() error
}

type StateStorage interface {
	StateCellTreeImporter
	CurrentState(ctx context.Context) (*CurrentState, error)
	SaveStateSyncProgress(ctx context.Context, state *CurrentState) error
	StateSyncProgress(ctx context.Context) (*CurrentState, error)
	ClearStateSyncProgress(ctx context.Context) error
	SaveStateCellRecords(ctx context.Context, cells StateCellRecords) error
	FlushStateCells(ctx context.Context) error
	SaveZeroStateCheckpoint(ctx context.Context, blocks []*BlockState, current *CurrentState) error
	SaveStateCheckpointEntries(ctx context.Context, blocks []StateCheckpointBlock, cells StateCellRecords, current *CurrentState) (StateCheckpointTiming, error)
	BlockState(ctx context.Context, block ton.BlockIDExt) (*BlockState, error)
}

type StateCheckpointTiming struct {
	CellsWrite   time.Duration
	CellsFlush   time.Duration
	ArtifactSync time.Duration
	MetadataSync time.Duration
}

type BlockRootHash [32]byte

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
		Block:          cloneBlockID(state.Block),
		StateRootHash:  bytes.Clone(state.StateRootHash),
		StateFileHash:  bytes.Clone(state.StateFileHash),
		MasterchainRef: cloneBlockIDPtr(state.MasterchainRef),
		Cell:           state.Cell,
		Parsed:         state.Parsed,
	}
}

func BlockStateWithoutCells(state *BlockState) BlockState {
	return BlockState{
		Block:          cloneBlockID(state.Block),
		StateRootHash:  bytes.Clone(state.StateRootHash),
		StateFileHash:  bytes.Clone(state.StateFileHash),
		MasterchainRef: cloneBlockIDPtr(state.MasterchainRef),
	}
}

func cloneBlockID(block ton.BlockIDExt) ton.BlockIDExt {
	block.RootHash = bytes.Clone(block.RootHash)
	block.FileHash = bytes.Clone(block.FileHash)
	return block
}

func cloneBlockIDPtr(block *ton.BlockIDExt) *ton.BlockIDExt {
	if block == nil {
		return nil
	}
	cloned := cloneBlockID(*block)
	return &cloned
}

func BlockKey(block ton.BlockIDExt) BlockRootHash {
	var key BlockRootHash
	copy(key[:], block.RootHash)
	return key
}
