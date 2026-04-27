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
	ArchiveInfo(ctx context.Context, masterchainSeqno int32) (int64, error)
	ArchiveSlice(ctx context.Context, archiveID, offset int64, maxSize int32) ([]byte, error)
}

type PeerServingStorageWriter interface {
	SaveBlockFull(block *ServedBlockFull) error
	LinkNextBlock(prev ton.BlockIDExt, next ton.BlockIDExt)
	SaveBlockData(block ton.BlockIDExt, data []byte)
	SaveBlockProof(kind ServedProofKind, block ton.BlockIDExt, data []byte)
	SaveArchiveInfo(masterchainSeqno int32, archiveID int64)
	SaveArchiveSlice(archiveID, offset int64, data []byte)
}

type ServedProofKind string

const (
	ServedProofBlock        ServedProofKind = "block"
	ServedProofBlockLink    ServedProofKind = "block_link"
	ServedProofKeyBlock     ServedProofKind = "key_block"
	ServedProofKeyBlockLink ServedProofKind = "key_block_link"
)

type ServedBlockFull struct {
	ID     ton.BlockIDExt
	Proof  []byte
	Block  []byte
	Meta   *BlockMeta
	IsLink bool
}

func (b *ServedBlockFull) Clone() *ServedBlockFull {
	if b == nil {
		return nil
	}
	return &ServedBlockFull{
		ID:     b.ID,
		Proof:  append([]byte(nil), b.Proof...),
		Block:  append([]byte(nil), b.Block...),
		Meta:   b.Meta.Clone(),
		IsLink: b.IsLink,
	}
}
