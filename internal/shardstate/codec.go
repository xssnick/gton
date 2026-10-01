// Package shardstate owns the low-level cell representation of split shard
// states. It is intentionally independent from synchronization and transport
// policy so state workflows and P2P artifact transfer can share the codec.
package shardstate

import (
	"fmt"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func WrapAccountsRoot(root *cell.Cell) (*cell.Cell, error) {
	extra, err := root.AsAugDict(256, tlb.AugShardAccounts{}).LoadRootExtra()
	if err != nil {
		return nil, err
	}

	extraCell, err := extra.ToCell()
	if err != nil {
		return nil, err
	}

	return cell.BeginCell().
		MustStoreUInt(1, 1).
		MustStoreRef(root).
		MustStoreBuilder(extraCell.ToBuilder()).
		EndCell(), nil
}

func NewAccounts() (*cell.AugmentedDictionary, error) {
	return cell.NewAugDict(256, tlb.AugShardAccounts{})
}

func LoadAccountsRoot(root *cell.Cell) (*cell.AugmentedDictionary, error) {
	loader, err := root.BeginParse()
	if err != nil {
		return nil, err
	}
	return loader.LoadAugDict(256, cell.ReadOnlyAugmentation{
		SkipExtraFn: tlb.AugShardAccounts{}.SkipExtra,
	}, false)
}

func Merge(header *tlb.ShardStateUnsplit, parts []*cell.Cell) (*cell.Cell, error) {
	accounts, err := NewAccounts()
	if err != nil {
		return nil, err
	}

	for i, root := range parts {
		partAccounts, err := LoadAccountsRoot(root)
		if err != nil {
			return nil, fmt.Errorf("parse split state part %d accounts: %w", i+1, err)
		}

		merged, err := accounts.CombineWith(partAccounts)
		if err != nil {
			return nil, fmt.Errorf("merge split state part %d accounts: %w", i+1, err)
		}
		if !merged {
			return nil, fmt.Errorf("duplicate account in split state part %d", i+1)
		}
	}

	return MergeAccounts(header, accounts)
}

func MergeAccounts(header *tlb.ShardStateUnsplit, accounts *cell.AugmentedDictionary) (*cell.Cell, error) {
	full := *header
	full.Accounts.ShardAccounts = &tlb.ShardAccountsAugDict{AugmentedDictionary: accounts}
	return tlb.ToCell(&full)
}
