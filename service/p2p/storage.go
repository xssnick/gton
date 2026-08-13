package p2p

import (
	"context"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// PeerStorage is the read boundary for block, state-file, and archive data P2P
// serves to remote peers.
type PeerStorage interface {
	BlockMeta(ctx context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error)
	BlockFull(ctx context.Context, block ton.BlockIDExt) (*storage.ServedBlockFull, error)
	NextBlockFull(ctx context.Context, prev ton.BlockIDExt) (*storage.ServedBlockFull, error)
	BlockData(ctx context.Context, block ton.BlockIDExt) ([]byte, error)
	BlockProof(ctx context.Context, kind storage.ServedProofKind, block ton.BlockIDExt) ([]byte, error)
	NextKeyBlocks(ctx context.Context, after uint32, limit int) ([]ton.BlockIDExt, error)
	CurrentState(ctx context.Context) (*storage.CurrentState, error)
	ZeroStateSize(ctx context.Context, block ton.BlockIDExt) (int64, error)
	ZeroState(ctx context.Context, block ton.BlockIDExt) ([]byte, error)
	PersistentStateSize(ctx context.Context, block ton.BlockIDExt, masterchainBlock ton.BlockIDExt, effectiveShard int64) (int64, error)
	PersistentStateSlice(ctx context.Context, block ton.BlockIDExt, masterchainBlock ton.BlockIDExt, effectiveShard int64, offset int64, maxSize int64) ([]byte, error)
	ArchiveInfo(ctx context.Context, masterchainSeqno int32, workchain int32, shard int64) (int64, error)
	ArchiveSlice(ctx context.Context, archiveID, offset int64, maxSize int32) ([]byte, error)
}

// StateArtifactStorage is the optional local cell/state boundary used to
// import and persist downloaded snapshots and reuse already staged state
// files. Its lifecycle belongs to the composition root, not Node.
type StateArtifactStorage interface {
	BlockState(ctx context.Context, block ton.BlockIDExt) (*storage.BlockState, error)
	ImportStateBOCView(ctx context.Context, block ton.BlockIDExt, view *cell.BOCView) (*cell.Cell, error)
	LoadStateCellTree(ctx context.Context, block ton.BlockIDExt, rootHash []byte) (*cell.Cell, error)
	CellRecord(ctx context.Context, hash []byte) (*storage.CellRecord, error)
	LazyCellLoader() cell.LazyCellLoader
	TrustImportedStateCellHashes() bool
	PersistentStateFile(ctx context.Context, block ton.BlockIDExt, masterchainBlock ton.BlockIDExt, effectiveShard int64) (*storage.PersistentStateFile, error)
	SaveZeroState(block ton.BlockIDExt, data []byte, ref *storage.ArtifactRef) error
	SavePersistentStateFile(file *storage.PersistentStateFile) error
}

type importedStateHashPolicy interface {
	TrustImportedStateCellHashes() bool
}

type zeroStateWriter interface {
	SaveZeroState(block ton.BlockIDExt, data []byte, ref *storage.ArtifactRef) error
}
