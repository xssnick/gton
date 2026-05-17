package service

import (
	"fmt"

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
	block, err := tnstore.ParseVerifiedBlockCell(id, root)
	if err != nil {
		return BlockStats{}, err
	}
	return statsFromParsedBlock(id, block)
}

func statsFromParsedBlock(id ton.BlockIDExt, block *tlb.Block) (BlockStats, error) {
	if block.Extra == nil || block.Extra.ShardAccountBlocks == nil {
		return BlockStats{}, fmt.Errorf("block %s does not contain shard account blocks", tnstore.FormatBlockRef(id))
	}

	txCount, err := countBlockTransactions(block.Extra.ShardAccountBlocks)
	if err != nil {
		return BlockStats{}, fmt.Errorf("count transactions in %s: %w", tnstore.FormatBlockRef(id), err)
	}

	return BlockStats{
		ID:           id,
		Transactions: txCount,
	}, nil
}

func countBlockTransactions(root *cell.Cell) (int, error) {
	var shardAccounts tlb.ShardAccountBlocks
	loader, err := root.BeginParse()
	if err != nil {
		return 0, fmt.Errorf("load shard account blocks: %w", err)
	}
	if err := tlb.LoadFromCell(&shardAccounts, loader); err != nil {
		return 0, fmt.Errorf("load shard account blocks: %w", err)
	}
	if shardAccounts.Accounts == nil {
		return 0, nil
	}

	accounts, err := shardAccounts.Accounts.Range(false, false)
	if err != nil {
		return 0, fmt.Errorf("load shard account dictionary: %w", err)
	}

	total := 0
	for _, kv := range accounts {
		if err := tlb.LoadFromCell(new(tlb.CurrencyCollection), kv.Value); err != nil {
			return 0, fmt.Errorf("load account currency collection: %w", err)
		}

		var accountBlock tlb.AccountBlock
		if err := tlb.LoadFromCell(&accountBlock, kv.Value); err != nil {
			return 0, fmt.Errorf("load account block: %w", err)
		}
		if accountBlock.Transactions == nil {
			continue
		}

		txs, err := accountBlock.Transactions.Range(false, false)
		if err != nil {
			return 0, fmt.Errorf("load account transactions: %w", err)
		}
		total += len(txs)
	}

	return total, nil
}
