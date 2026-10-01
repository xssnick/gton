package service

import (
	"context"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// durableMasterchainStore is the shared durable-state boundary used when a
// serializer or generation migration resolves an explicit masterchain target.
type durableMasterchainStore interface {
	CurrentState(ctx context.Context) (*storage.CurrentState, error)
	LookupBlockBySeqNo(ctx context.Context, ref storage.BlockSeqRef) (ton.BlockIDExt, error)
}

// stateCheckpointStore is owned by the normal state-checkpoint workflow. It
// deliberately excludes state serialization, generation rotation and storage
// lifecycle operations.
type stateCheckpointStore interface {
	FlushStateCells(ctx context.Context) error
	SaveStateCheckpointEntries(ctx context.Context, blocks []storage.StateCheckpointBlock, cells storage.StateCellRecords, current *storage.CurrentState) (storage.StateCheckpointTiming, error)
	LazyCellLoader() cell.LazyCellLoader
}

// stateMigrationBlockStore is the artifact surface needed to replay already
// stored blocks while a candidate cell generation catches up.
type stateMigrationBlockStore interface {
	LookupBlockBySeqNo(ctx context.Context, ref storage.BlockSeqRef) (ton.BlockIDExt, error)
	BlockFull(ctx context.Context, block ton.BlockIDExt) (*storage.ServedBlockFull, error)
}

type persistentStateArtifactStore interface {
	PersistentStateFile(ctx context.Context, block ton.BlockIDExt, masterchainBlock ton.BlockIDExt, effectiveShard int64) (*storage.PersistentStateFile, error)
}

// cellGenerationStore is owned by generation rotation. Candidate reads and
// writes use the selected exact generation; BlockState rehydrates the durable
// current state through the ordinary active loader before publication.
type cellGenerationStore interface {
	durableMasterchainStore
	persistentStateArtifactStore

	Cells(generation uint64) (storage.CellGeneration, error)
	BlockState(ctx context.Context, block ton.BlockIDExt) (*storage.BlockState, error)
	BlockMeta(ctx context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error)
	ActiveCellGeneration(ctx context.Context) (storage.CellGenerationInfo, error)
	PendingCellGenerationMigration(ctx context.Context) (storage.CellGenerationInfo, error)
	CellGenerationDBMetrics(ctx context.Context, generation uint64) (storage.CellGenerationDBMetrics, error)
	CellGenerationMigrationProgress(ctx context.Context, generation uint64) (*storage.CurrentState, error)
	SaveCellGenerationMigrationProgress(ctx context.Context, generation uint64, current *storage.CurrentState) error
	BeginCellGeneration(ctx context.Context, origin ton.BlockIDExt) (uint64, error)
	DropPendingCellGeneration(ctx context.Context, generation uint64) error
	CleanupCellGeneration(ctx context.Context, generation uint64) error
	ThrottleCellGenerationCompactions(ctx context.Context, generation uint64) (func(), error)
	DeleteStateMetadataBeforeCellGenerationSwitch(ctx context.Context, origin ton.BlockIDExt, current *storage.CurrentState, preserveStateMeta []ton.BlockIDExt) (int, error)
	SwitchCellGeneration(ctx context.Context, generation uint64, origin ton.BlockIDExt, expectedCurrent ton.BlockIDExt, current *storage.CurrentState) (uint64, error)
}

// stateLifecycleStore is used only while wiring StateLifecycle. Each workflow
// stores one of the narrower interfaces above, so peer serving, archive
// maintenance and Close cannot leak into runtime code through this boundary.
type stateLifecycleStore interface {
	stateCheckpointStore
	stateCellPrewriteStore
	stateSerializerStore
	cellGenerationStore
	stateMigrationBlockStore
}
