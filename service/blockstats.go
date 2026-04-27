package service

import (
	"errors"
	"flexserver/service/p2p"
	tnstore "flexserver/service/storage"
	"fmt"

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
		return statsFromParsedBlock(downloaded.ID, downloaded.Parsed)
	}

	root := downloaded.Block
	if root == nil {
		return BlockStats{}, fmt.Errorf("downloaded block %s is missing parsed cell", downloaded.BlockRef())
	}

	var err error
	if downloaded.IsLink && root.GetType() == cell.MerkleProofCellType {
		root, err = cell.UnwrapProof(root, downloaded.ID.RootHash)
		if err != nil {
			return BlockStats{}, fmt.Errorf("unwrap merkle proof link for %s: %w", downloaded.BlockRef(), err)
		}
	}

	return StatsFromBlockCell(downloaded.ID, root)
}

func StatsFromBlockCell(id ton.BlockIDExt, root *cell.Cell) (BlockStats, error) {
	if root == nil {
		return BlockStats{}, errors.New("block root is nil")
	}

	var block tlb.Block
	if err := tlb.LoadFromCell(&block, root.BeginParse()); err != nil {
		return BlockStats{}, fmt.Errorf("load tlb block %s: %w", tnstore.FormatBlockRef(id), err)
	}

	if err := verifyBlockIdentity(id, &block); err != nil {
		return BlockStats{}, err
	}
	return statsFromParsedBlock(id, &block)
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

func verifyBlockIdentity(id ton.BlockIDExt, block *tlb.Block) error {
	workchain, shard := tlb.ConvertShardIdentToShard(block.BlockInfo.Shard)
	if block.BlockInfo.SeqNo != id.SeqNo || workchain != id.Workchain || shard != uint64(id.Shard) {
		return fmt.Errorf(
			"downloaded block identity mismatch: expected %s, got wc=%d shard=%016x seqno=%d",
			tnstore.FormatBlockRef(id),
			workchain,
			shard,
			block.BlockInfo.SeqNo,
		)
	}

	return nil
}

func countBlockTransactions(root *cell.Cell) (int, error) {
	var shardAccounts tlb.ShardAccountBlocks
	if err := tlb.LoadFromCell(&shardAccounts, root.BeginParse()); err != nil {
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
