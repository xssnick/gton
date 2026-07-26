package storage

import (
	"context"

	"github.com/xssnick/tonutils-go/ton"
)

type PeerServingStorage interface {
	BlockMeta(ctx context.Context, block ton.BlockIDExt) (*BlockMeta, error)
	BlockFull(ctx context.Context, block ton.BlockIDExt) (*ServedBlockFull, error)
	NextBlockFull(ctx context.Context, prev ton.BlockIDExt) (*ServedBlockFull, error)
	BlockData(ctx context.Context, block ton.BlockIDExt) ([]byte, error)
	BlockProof(ctx context.Context, kind ServedProofKind, block ton.BlockIDExt) ([]byte, error)
	ZeroStateSize(ctx context.Context, block ton.BlockIDExt) (int64, error)
	ZeroState(ctx context.Context, block ton.BlockIDExt) ([]byte, error)
	PersistentStateSize(ctx context.Context, block ton.BlockIDExt, masterchainBlock ton.BlockIDExt, effectiveShard int64) (int64, error)
	PersistentStateSlice(ctx context.Context, block ton.BlockIDExt, masterchainBlock ton.BlockIDExt, effectiveShard int64, offset int64, maxSize int64) ([]byte, error)
	ArchiveInfo(ctx context.Context, masterchainSeqno int32, workchain int32, shard int64) (int64, error)
	ArchiveSlice(ctx context.Context, archiveID, offset int64, maxSize int32) ([]byte, error)
}

type PeerServingStorageWriter interface {
	SaveZeroState(block ton.BlockIDExt, data []byte, ref *ArtifactRef) error
	SavePersistentStateFile(file *PersistentStateFile) error
}

type PersistentStateFileStorage interface {
	PersistentStateFile(ctx context.Context, block ton.BlockIDExt, masterchainBlock ton.BlockIDExt, effectiveShard int64) (*PersistentStateFile, error)
}

type ServedProofKind string

const (
	ServedProofBlock        ServedProofKind = "block"
	ServedProofBlockLink    ServedProofKind = "block_link"
	ServedProofKeyBlock     ServedProofKind = "key_block"
	ServedProofKeyBlockLink ServedProofKind = "key_block_link"
)

type ServedBlockFull struct {
	ID                     ton.BlockIDExt
	Proof                  []byte
	Block                  []byte
	Meta                   *BlockMeta
	IsLink                 bool
	ArchiveShardSplitDepth uint32
}

type ServedBlockLink struct {
	Prev ton.BlockIDExt
	Next ton.BlockIDExt
}

type ArchivePruneStats struct {
	CutoffUnix            uint32
	DeletedBeforeSeqno    uint32
	ScannedPackages       int
	DeletedPackages       int
	DeletedPackageFiles   int
	DeletedPackageBytes   uint64
	DeletedBlockMeta      int
	DeletedMetadataKeys   int
	RetainedBoundarySeqno uint32
}

type PersistentStatePruneStats struct {
	NowUnix                   uint64
	ScannedFiles              int
	DeletedFileRecords        int
	DeletedDiskFiles          int
	DeletedDiskBytes          uint64
	DeletedMasterSeqno        uint32
	RetainedRecentGroups      int
	OldestRetainedMasterSeqno uint32
}

type PersistentStateFile struct {
	Block            ton.BlockIDExt
	MasterchainBlock ton.BlockIDExt
	EffectiveShard   int64
	Ref              *ArtifactRef
	FileHash         []byte
	StateRootHash    []byte
}

type ArtifactRef struct {
	Path             string
	ArchivePackage   bool
	ArchivePackageID int64
	Offset           int64
	Size             int64
}

func (r *ArtifactRef) Clone() *ArtifactRef {
	if r == nil {
		return nil
	}
	return &ArtifactRef{
		Path:             r.Path,
		ArchivePackage:   r.ArchivePackage,
		ArchivePackageID: r.ArchivePackageID,
		Offset:           r.Offset,
		Size:             r.Size,
	}
}

func (b *ServedBlockFull) Clone() *ServedBlockFull {
	if b == nil {
		return nil
	}
	return &ServedBlockFull{
		ID:                     cloneBlockID(b.ID),
		Proof:                  append([]byte(nil), b.Proof...),
		Block:                  append([]byte(nil), b.Block...),
		Meta:                   b.Meta.Clone(),
		IsLink:                 b.IsLink,
		ArchiveShardSplitDepth: b.ArchiveShardSplitDepth,
	}
}
