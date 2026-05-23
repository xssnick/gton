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
	StateFileHash []byte
	// MasterchainRef matches C++ BlockHandle::masterchain_ref_block:
	// the masterchain block that applied/included this shard block, not BlockInfo.MasterRef.
	MasterchainRef *ton.BlockIDExt
	CellGeneration uint64
	Cell           *cell.Cell
	Parsed         *tlb.ShardStateUnsplit
}

type CellGenerationInfo struct {
	ID                    uint64
	OriginPersistentState ton.BlockIDExt
}

type CellGenerationDBMetrics struct {
	MaxReadAmp               int64
	L0Files                  int64
	L0Sublevels              int64
	L0Size                   int64
	CompactionDebt           uint64
	CompactionsInProgress    int64
	CompactionInProgressSize int64
}

type CurrentState struct {
	SyncedAt         time.Time
	ShardClientSeqno uint32
	Masterchain      BlockState
	Shards           map[ShardKey]BlockState
}
