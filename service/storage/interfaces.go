package storage

import (
	"context"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type Storage interface {
	PeerServingStorage
	PeerServingStorageWriter
	StateStorage
	BlockMetaStorage
	CellStorage
	Close() error
}

type BlockMetaStorage interface {
	SaveBlockMeta(meta *BlockMeta) error
	BlockMeta(ctx context.Context, block ton.BlockIDExt) (*BlockMeta, error)
	NextKeyBlocks(ctx context.Context, after uint32, limit int) ([]ton.BlockIDExt, error)
	LookupBlockBySeqNo(ctx context.Context, key BlockHistoryKey, seqno uint32) (ton.BlockIDExt, error)
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
