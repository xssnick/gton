package service

import (
	"context"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

// stateSerializerStore is owned by the persistent-state serializer. It keeps
// serialization scheduling and cell reads independent from unrelated archive,
// peer-serving, maintenance and lifecycle operations.
type stateSerializerStore interface {
	durableMasterchainStore

	ActiveCells() (storage.CellGeneration, error)
	BlockState(ctx context.Context, block ton.BlockIDExt) (*storage.BlockState, error)
	BlockMeta(ctx context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error)
	NextKeyBlockMetas(ctx context.Context, after uint32, limit int) ([]*storage.BlockMeta, error)
	ThrottleCellCompactions() func()
	PrunePersistentStateFilesToLimit(ctx context.Context, throughMasterSeqno uint32, keepRecentGroups int) (storage.PersistentStatePruneStats, error)
	PrunePreviousPersistentStateFiles(ctx context.Context, beforeMasterSeqno uint32) (storage.PersistentStatePruneStats, error)

	PersistentStateSize(ctx context.Context, block ton.BlockIDExt, masterchainBlock ton.BlockIDExt, effectiveShard int64) (int64, error)
	PersistentStateFile(ctx context.Context, block ton.BlockIDExt, masterchainBlock ton.BlockIDExt, effectiveShard int64) (*storage.PersistentStateFile, error)
	SavePersistentStateFile(file *storage.PersistentStateFile) error
	DeletePersistentStateFile(ctx context.Context, block ton.BlockIDExt, masterchainBlock ton.BlockIDExt, effectiveShard int64) error

	PersistentStateSerializerState(ctx context.Context) (*storage.PersistentStateSerializerState, error)
	SavePersistentStateSerializerState(ctx context.Context, state *storage.PersistentStateSerializerState) error
	ActivePersistentStateSerialization(ctx context.Context) (*storage.PersistentStateSerializerActive, error)
	SaveActivePersistentStateSerialization(ctx context.Context, active *storage.PersistentStateSerializerActive) error
	DeleteActivePersistentStateSerialization(ctx context.Context) error
	SavePersistentStateDescription(ctx context.Context, desc *storage.PersistentStateDescription) error
	PersistentStateDescriptions(ctx context.Context) ([]storage.PersistentStateDescription, error)
}
