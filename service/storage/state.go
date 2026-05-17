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
	// MasterchainRef matches C++ BlockHandle::masterchain_ref_block:
	// the masterchain block that first included this shard block.
	MasterchainRef *ton.BlockIDExt
	CellGeneration uint64
	Cell           *cell.Cell
	Parsed         *tlb.ShardStateUnsplit
}

type CellGenerationInfo struct {
	ID                    uint64
	OriginPersistentState ton.BlockIDExt
}

type CurrentState struct {
	SyncedAt         time.Time
	ShardClientSeqno uint32
	Masterchain      BlockState
	Shards           map[ShardKey]BlockState
}
