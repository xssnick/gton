package storage

import (
	"context"

	"github.com/xssnick/tonutils-go/ton"
)

type PeerServingStorage interface {
	BlockFull(ctx context.Context, block ton.BlockIDExt) (*ServedBlockFull, error)
	NextBlockFull(ctx context.Context, prev ton.BlockIDExt) (*ServedBlockFull, error)
	BlockData(ctx context.Context, block ton.BlockIDExt) ([]byte, error)
	BlockProof(ctx context.Context, kind ServedProofKind, block ton.BlockIDExt) ([]byte, error)
	ZeroState(ctx context.Context, block ton.BlockIDExt) ([]byte, error)
	PersistentStateSize(ctx context.Context, block ton.BlockIDExt, masterchainBlock ton.BlockIDExt, effectiveShard int64) (int64, error)
	PersistentStateSlice(ctx context.Context, block ton.BlockIDExt, masterchainBlock ton.BlockIDExt, effectiveShard int64, offset int64, maxSize int64) ([]byte, error)
	ArchiveInfo(ctx context.Context, masterchainSeqno int32, workchain int32, shard int64) (int64, error)
	ArchiveSlice(ctx context.Context, archiveID, offset int64, maxSize int32) ([]byte, error)
}

type PeerServingStorageWriter interface {
	SaveBlockFull(block *ServedBlockFull) error
	SaveArchiveImport(imported *ServedArchiveImport) error
	LinkNextBlock(prev ton.BlockIDExt, next ton.BlockIDExt) error
	SaveBlockData(block ton.BlockIDExt, data []byte, ref *ArtifactRef) error
	SaveBlockProof(kind ServedProofKind, block ton.BlockIDExt, data []byte, ref *ArtifactRef) error
	SaveZeroState(block ton.BlockIDExt, data []byte, ref *ArtifactRef) error
	SavePersistentStateFile(file *PersistentStateFile) error
	SaveArchiveFile(masterchainSeqno int32, workchain int32, shard int64, archiveID int64, path string) (SavedArchiveFile, error)
}

type SavedArchiveFile struct {
	Path           string
	ReusedExisting bool
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
	ProofRef               *ArtifactRef
	BlockRef               *ArtifactRef
	Meta                   *BlockMeta
	IsLink                 bool
	ArchiveShardSplitDepth uint32
}

type ServedBlockData struct {
	ID   ton.BlockIDExt
	Data []byte
	Ref  *ArtifactRef
}

type ServedBlockProof struct {
	Kind ServedProofKind
	ID   ton.BlockIDExt
	Data []byte
	Ref  *ArtifactRef
}

type ServedBlockLink struct {
	Prev ton.BlockIDExt
	Next ton.BlockIDExt
}

type ServedArchiveImport struct {
	FullBlocks []*ServedBlockFull
	BlockData  []ServedBlockData
	Proofs     []ServedBlockProof
	Links      []ServedBlockLink
}

type ArchivePruneStats struct {
	CutoffUnix            uint32
	DeletedBeforeSeqno    uint32
	ScannedPackages       int
	DeletedPackages       int
	DeletedPackageFiles   int
	DeletedBlockMeta      int
	DeletedMetadataKeys   int
	RetainedBoundarySeqno uint32
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
	Path   string
	Offset int64
	Size   int64
}

func (r *ArtifactRef) Clone() *ArtifactRef {
	if r == nil {
		return nil
	}
	return &ArtifactRef{
		Path:   r.Path,
		Offset: r.Offset,
		Size:   r.Size,
	}
}

func (b *ServedBlockFull) Clone() *ServedBlockFull {
	if b == nil {
		return nil
	}
	return &ServedBlockFull{
		ID:                     b.ID,
		Proof:                  append([]byte(nil), b.Proof...),
		Block:                  append([]byte(nil), b.Block...),
		ProofRef:               b.ProofRef.Clone(),
		BlockRef:               b.BlockRef.Clone(),
		Meta:                   b.Meta.Clone(),
		IsLink:                 b.IsLink,
		ArchiveShardSplitDepth: b.ArchiveShardSplitDepth,
	}
}
