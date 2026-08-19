package storage

import (
	"context"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type MaintenanceStorage interface {
	MaxReadAmp(ctx context.Context) (int64, error)
	ThrottleCellCompactions() func()
	PruneArchivePackages(ctx context.Context, cutoffUnix uint32, maxStartGroups int) (ArchivePruneStats, error)
	PruneExpiredPersistentStateFiles(ctx context.Context, nowUnix uint64, keepRecentGroups int, maxFiles int) (PersistentStatePruneStats, error)
	PrunePreviousPersistentStateFiles(ctx context.Context, beforeMasterSeqno uint32) (PersistentStatePruneStats, error)
}

type PersistentStateSerializationStorage interface {
	DeletePersistentStateFile(ctx context.Context, block ton.BlockIDExt, masterchainBlock ton.BlockIDExt, effectiveShard int64) error
	PrunePersistentStateFilesToLimit(ctx context.Context, throughMasterSeqno uint32, keepRecentGroups int) (PersistentStatePruneStats, error)
	PersistentStateSerializerState(ctx context.Context) (*PersistentStateSerializerState, error)
	SavePersistentStateSerializerState(ctx context.Context, state *PersistentStateSerializerState) error
	ActivePersistentStateSerialization(ctx context.Context) (*PersistentStateSerializerActive, error)
	SaveActivePersistentStateSerialization(ctx context.Context, active *PersistentStateSerializerActive) error
	DeleteActivePersistentStateSerialization(ctx context.Context) error
	SavePersistentStateDescription(ctx context.Context, desc *PersistentStateDescription) error
	PersistentStateDescriptions(ctx context.Context) ([]PersistentStateDescription, error)
}

type CellGenerationStorage interface {
	ActiveCellGeneration(ctx context.Context) (CellGenerationInfo, error)
	PendingCellGenerationMigration(ctx context.Context) (CellGenerationInfo, error)
	CellGenerationDBMetrics(ctx context.Context, generation uint64) (CellGenerationDBMetrics, error)
	CellGenerationMigrationProgress(ctx context.Context, generation uint64) (*CurrentState, error)
	SaveCellGenerationMigrationProgress(ctx context.Context, generation uint64, current *CurrentState) error
	BeginCellGeneration(ctx context.Context, origin ton.BlockIDExt) (uint64, error)
	DropPendingCellGeneration(ctx context.Context, generation uint64) error
	CleanupCellGeneration(ctx context.Context, generation uint64) error
	ThrottleCellGenerationCompactions(ctx context.Context, generation uint64) (func(), error)
	DeleteStateMetadataBeforeCellGenerationSwitch(ctx context.Context, origin ton.BlockIDExt, current *CurrentState, preserveStateMeta []ton.BlockIDExt) (int, error)
	SwitchCellGeneration(ctx context.Context, generation uint64, origin ton.BlockIDExt, expectedCurrent ton.BlockIDExt, current *CurrentState) (uint64, error)
}

// CellGeneration owns all cell operations scoped to one concrete, non-zero
// generation. Selecting the generation once keeps active-generation policy out
// of serializers and migrations, and avoids a generation branch in each batch.
type CellGeneration interface {
	ID() uint64
	Save(ctx context.Context, records []*CellRecord) error
	SaveEncoded(ctx context.Context, records []EncodedCellRecord, sync bool) error
	Record(ctx context.Context, hash []byte) (*CellRecord, error)
	Load(ctx context.Context, hash []byte) (*cell.Cell, error)
	Loader() cell.LazyCellLoader
	LoadLargeBOCMeta(ctx context.Context, hashes []cell.Hash, dst []cell.LargeBOCMetaRecord) ([]cell.LargeBOCMetaRecord, error)
	LoadLargeBOCPayload(ctx context.Context, hashes []cell.Hash, dst []cell.LargeBOCPayloadRecord) ([]cell.LargeBOCPayloadRecord, error)
	LoadLargeBOCCells(ctx context.Context, hashes []cell.Hash, dst []cell.LargeBOCRecord) ([]cell.LargeBOCRecord, error)
	ImportStateCellTree(ctx context.Context, block ton.BlockIDExt, root *cell.Cell, totalCells uint64) (*cell.Cell, error)
	ImportStateBOCView(ctx context.Context, block ton.BlockIDExt, view *cell.BOCView) (*cell.Cell, error)
}

type BlockMetaStorage interface {
	SaveBlockMeta(meta *BlockMeta) error
	BlockMeta(ctx context.Context, block ton.BlockIDExt) (*BlockMeta, error)
	NextKeyBlocks(ctx context.Context, after uint32, limit int) ([]ton.BlockIDExt, error)
	// NextKeyBlockMetas iterates the same key-block index as NextKeyBlocks but
	// without the served-full requirement, returning the stored metadata.
	NextKeyBlockMetas(ctx context.Context, after uint32, limit int) ([]*BlockMeta, error)
	LookupBlockBySeqNo(ctx context.Context, ref BlockSeqRef) (ton.BlockIDExt, error)
	LookupBlockByLT(ctx context.Context, key BlockHistoryKey, lt uint64) (ton.BlockIDExt, error)
	LookupBlockByAccountLT(ctx context.Context, workchain int32, account []byte, lt uint64) (ton.BlockIDExt, error)
	LookupBlockByUnixTime(ctx context.Context, key BlockHistoryKey, utime uint32) (ton.BlockIDExt, error)
}

type CellStorage interface {
	ActiveCells() (CellGeneration, error)
	Cells(generation uint64) (CellGeneration, error)
	CellRecord(ctx context.Context, hash []byte) (*CellRecord, error)
	LoadCell(ctx context.Context, hash []byte) (*cell.Cell, error)
	LazyCellLoader() cell.LazyCellLoader
}
