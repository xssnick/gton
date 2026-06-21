package archive

import (
	"errors"
	"fmt"
	"math/bits"
	"time"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const topShard = int64(-1 << 63)

var ErrNotAvailable = errors.New("archive slice is not available from fullnode")

type ShardID struct {
	Workchain int32 `tl:"int"`
	Shard     int64 `tl:"long"`
}

type Downloaded struct {
	MasterchainSeqno uint32
	Shard            ShardID
	ArchiveID        int64
	Peer             string
	Data             []byte
	Bytes            int64
	DownloadElapsed  time.Duration
}

type ImportStats struct {
	MasterchainSeqno       uint32
	ArchiveID              int64
	Peer                   string
	Bytes                  int64
	Entries                int
	IgnoredEntries         int
	Blocks                 int
	Proofs                 int
	ProofLinks             int
	FullBlocks             int
	Links                  int
	FirstSeqno             uint32
	LastSeqno              uint32
	MasterchainFirstSeqno  uint32
	MasterchainLastSeqno   uint32
	ShardArchives          int
	DownloadElapsed        time.Duration
	ImportElapsed          time.Duration
	ProcessingElapsed      time.Duration
	BlockPrepareElapsed    time.Duration
	StateUpdateCells       uint64
	StateUpdateCellBytes   uint64
	StateUpdateCellPrepare time.Duration
}

type PreparedBlock struct {
	Block                     *cell.Cell
	Parsed                    *tlb.Block
	Meta                      *storage.BlockMeta
	State                     *storage.BlockState
	StateUpdate               *cell.Cell
	StateUpdateToCells        storage.StateCellRecords
	StateUpdateToCellsElapsed time.Duration
}

func (s ShardID) IsMasterchain() bool {
	return s.Workchain == -1 && s.Shard == topShard
}

func (s ShardID) String() string {
	return fmt.Sprintf("wc=%d shard=%016x", s.Workchain, uint64(s.Shard))
}

func (s ShardID) ContainsBlock(block ton.BlockIDExt) bool {
	if block.Workchain != s.Workchain {
		return false
	}
	if s.IsMasterchain() {
		return block.Shard == s.Shard
	}

	shard := uint64(s.Shard)
	if shard == 0 {
		return uint64(block.Shard) == shard
	}

	depth := 63 - bits.TrailingZeros64(shard)
	if depth <= 0 {
		return true
	}

	mask := ^uint64(0) << (64 - depth)
	return uint64(block.Shard)&mask == shard&mask
}

func (s *ImportStats) observe(block ton.BlockIDExt) {
	if s.FirstSeqno == 0 || block.SeqNo < s.FirstSeqno {
		s.FirstSeqno = block.SeqNo
	}
	if block.SeqNo > s.LastSeqno {
		s.LastSeqno = block.SeqNo
	}
}
