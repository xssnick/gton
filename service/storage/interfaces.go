package storage

import (
	"context"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type Storage interface {
	PeerServingStorage
	PeerServingStorageWriter
	PersistentStateFileStorage
	StateStorage
	BlockMetaStorage
	CellStorage
	MaintenanceStorage
	PersistentStateSerializationStorage
	CellGenerationStorage
	Close() error
}

type MaintenanceStorage interface {
	MaxReadAmp(ctx context.Context) (int64, error)
	ThrottleCellCompactions() func()
	PruneArchivePackages(ctx context.Context, cutoffUnix uint32, maxStartGroups int) (ArchivePruneStats, error)
	PruneExpiredPersistentStateFiles(ctx context.Context, nowUnix uint64, keepRecentGroups int, maxFiles int) (PersistentStatePruneStats, error)
	PrunePreviousPersistentStateFiles(ctx context.Context, beforeMasterSeqno uint32) (PersistentStatePruneStats, error)
}

type PersistentStateSerializationStorage interface {
	DeletePersistentStateFile(ctx context.Context, block ton.BlockIDExt, masterchainBlock ton.BlockIDExt, effectiveShard int64) error
	SaveCellsInGeneration(ctx context.Context, generation uint64, records []*CellRecord) error
	CellRecordInGeneration(ctx context.Context, generation uint64, hash []byte) (*CellRecord, error)
	LargeBOCLoadMeta(ctx context.Context, hashes []cell.Hash, dst []cell.LargeBOCMetaRecord) ([]cell.LargeBOCMetaRecord, error)
	LargeBOCLoadPayload(ctx context.Context, hashes []cell.Hash, dst []cell.LargeBOCPayloadRecord) ([]cell.LargeBOCPayloadRecord, error)
	LargeBOCLoadCells(ctx context.Context, hashes []cell.Hash, dst []cell.LargeBOCRecord) ([]cell.LargeBOCRecord, error)
	LargeBOCLoadMetaInGeneration(ctx context.Context, generation uint64, hashes []cell.Hash, dst []cell.LargeBOCMetaRecord) ([]cell.LargeBOCMetaRecord, error)
	LargeBOCLoadPayloadInGeneration(ctx context.Context, generation uint64, hashes []cell.Hash, dst []cell.LargeBOCPayloadRecord) ([]cell.LargeBOCPayloadRecord, error)
	LargeBOCLoadCellsInGeneration(ctx context.Context, generation uint64, hashes []cell.Hash, dst []cell.LargeBOCRecord) ([]cell.LargeBOCRecord, error)
	PersistentStateSerializerState(ctx context.Context) (*PersistentStateSerializerState, error)
	SavePersistentStateSerializerState(ctx context.Context, state *PersistentStateSerializerState) error
	ActivePersistentStateSerialization(ctx context.Context) (*PersistentStateSerializerActive, error)
	SaveActivePersistentStateSerialization(ctx context.Context, active *PersistentStateSerializerActive) error
	DeleteActivePersistentStateSerialization(ctx context.Context) error
	SavePersistentStateDescription(ctx context.Context, desc *PersistentStateDescription) error
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
	ImportStateCellTreeInGeneration(ctx context.Context, generation uint64, block ton.BlockIDExt, root *cell.Cell, totalCells uint64) (*cell.Cell, error)
	ImportStateBOCViewInGeneration(ctx context.Context, generation uint64, block ton.BlockIDExt, view *cell.BOCView) (*cell.Cell, error)
	LazyCellLoaderInGeneration(generation uint64) cell.LazyCellLoader
	SaveEncodedCellsInGeneration(ctx context.Context, generation uint64, records []EncodedCellRecord, sync bool) error
	DeleteStateMetadataBeforeCellGenerationSwitch(ctx context.Context, origin ton.BlockIDExt, current *CurrentState, preserveStateMeta []ton.BlockIDExt) (int, error)
	SwitchCellGeneration(ctx context.Context, generation uint64, origin ton.BlockIDExt, expectedCurrent ton.BlockIDExt, current *CurrentState) (uint64, error)
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
	SaveCells(records []*CellRecord) error
	CellRecord(ctx context.Context, hash []byte) (*CellRecord, error)
	LoadCell(ctx context.Context, hash []byte) (*cell.Cell, error)
	LazyCellLoader() cell.LazyCellLoader
}
