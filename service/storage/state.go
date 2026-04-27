package storage

import (
	"time"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type ShardKey struct {
	Workchain int32
	Shard     int64
}

func ShardKeyFromBlock(block ton.BlockIDExt) ShardKey {
	return ShardKey{
		Workchain: block.Workchain,
		Shard:     block.Shard,
	}
}

type BlockState struct {
	Block         ton.BlockIDExt
	StateRootHash []byte
	StateCellHash []byte
	StateFileHash []byte
	CellsCount    uint64
	Cell          *cell.Cell
	Parsed        *tlb.ShardStateUnsplit
	DownloadedAt  time.Time
	// ReusedStateCells contains old-state subtree roots reused by MERKLE_UPDATE application.
	ReusedStateCells []cell.MerkleUpdateReusedCell
	// ReusedStateRefs contains parent ref edges whose child was reused by MERKLE_UPDATE application.
	ReusedStateRefs []cell.MerkleUpdateReusedRef
}

type CurrentState struct {
	SyncedAt         time.Time
	ShardClientSeqno uint32
	Masterchain      BlockState
	Shards           map[ShardKey]BlockState
}
