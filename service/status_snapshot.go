package service

import (
	"github.com/xssnick/gton/service/blocksync"
	"github.com/xssnick/gton/service/p2p"

	"github.com/xssnick/tonutils-go/ton"
)

type StatusSnapshot struct {
	p2p.StatusSnapshot
	BlockSync               blocksync.StatusSnapshot
	AppliedMasterchain      *ton.BlockIDExt
	LocalMasterchain        *ton.BlockIDExt
	LocalBasechain          *ton.BlockIDExt
	LocalBasechainShards    []ShardStatusSnapshot
	LocalStateLoaded        bool
	LocalStateError         string
	AppliedMasterchainUtime int64
	LocalMasterchainUtime   int64
	LocalBasechainUtime     int64
	LocalMasterchainTx      uint32
	LocalBasechainTx        uint32
	LocalMasterchainHasTx   bool
	LocalBasechainHasTx     bool
	RecentTPS               StatusTPSSnapshot
	BackgroundTask          string
}

type ShardStatusSnapshot struct {
	Block           ton.BlockIDExt
	Utime           int64
	Transactions    uint32
	HasTransactions bool
}

type StatusTPSSnapshot struct {
	// WindowMasters is kept for compatibility with the released status API.
	//
	// Deprecated: use DurationSeconds to identify the sampling window.
	WindowMasters   int
	Transactions    uint64
	DurationSeconds int64
	TPS             float64
	Complete        bool
}
