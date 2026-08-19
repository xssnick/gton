package storage

import (
	"bytes"
	"context"
	"time"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// LoadStateCellTree below is THE state read, and there is deliberately only one.
// Every consumer uses it: the lightserver, proof building, the archive importer,
// sync and download, and — the same call, not a parallel one — collation and
// validation.
//
// There was briefly a second, optional "operation" loader that returned the same
// cells out of a second decoded cell cache. It is gone. Two things killed it,
// and both still hold, so a per-consumer state read should not be reintroduced
// without answering them:
//
//   - The collator and the validator must hold the SAME *cell.Cell for a given
//     parent. ChainState.validatedCandidateState compares tip states by pointer
//     and silently degrades to a full re-apply per candidate otherwise, so two
//     consumers cannot be served different materializations of one state.
//   - It did not route what it appeared to route. A resident state tree's lazy
//     tips carry the loader that decoded them, so a read that lands on an
//     already-materialized tree — which in a steady-state node is essentially
//     every collation and validation read — resolves through that tree's
//     original loader no matter which entry point asked for it.
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
