package service

import (
	"github.com/xssnick/gton/service/p2p"
	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type BlockStats struct {
	ID           ton.BlockIDExt
	Transactions int
}

func StatsFromDownloadedBlock(downloaded p2p.DownloadedBlock) (BlockStats, error) {
	if downloaded.Parsed != nil {
		if err := tnstore.VerifyBlockIdentity(downloaded.ID, downloaded.Parsed); err != nil {
			return BlockStats{}, err
		}
		return statsFromParsedBlock(downloaded.ID, downloaded.Parsed)
	}

	root, err := downloadedBlockRoot(downloaded)
	if err != nil {
		return BlockStats{}, err
	}

	return StatsFromBlockCell(downloaded.ID, root)
}

func StatsFromBlockCell(id ton.BlockIDExt, root *cell.Cell) (BlockStats, error) {
	txCount, err := tnstore.BlockTransactionCountFromBlockCell(id, root)
	if err != nil {
		return BlockStats{}, err
	}

	return BlockStats{
		ID:           id,
		Transactions: int(txCount),
	}, nil
}

func statsFromParsedBlock(id ton.BlockIDExt, block *tlb.Block) (BlockStats, error) {
	txCount, err := tnstore.BlockTransactionCountFromParsed(id, block)
	if err != nil {
		return BlockStats{}, err
	}

	return BlockStats{
		ID:           id,
		Transactions: int(txCount),
	}, nil
}
