package blockproof

import (
	"errors"
	"fmt"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// VisitShardStateHeader parses the shard-state header (including the stats
// cell) from the loader, marking every visited cell for usage proofs.
func VisitShardStateHeader(loader *cell.Slice) (tlb.ShardStateUnsplit, error) {
	var state tlb.ShardStateUnsplit
	if err := tlb.LoadFromCell(&state, loader); err != nil {
		return tlb.ShardStateUnsplit{}, err
	}
	if err := visitShardStateStats(state.Stats); err != nil {
		return tlb.ShardStateUnsplit{}, err
	}
	return state, nil
}

func visitShardStateStats(stats *cell.Cell) error {
	if stats == nil {
		return fmt.Errorf("state is missing shard state stats")
	}

	loader, err := stats.BeginParse()
	if err != nil {
		return err
	}
	if _, err = loader.LoadUInt(64); err != nil {
		return err
	}
	if _, err = loader.LoadUInt(64); err != nil {
		return err
	}
	if err = SkipCurrencyCollection(loader); err != nil {
		return err
	}
	if err = SkipCurrencyCollection(loader); err != nil {
		return err
	}
	if _, err = loader.LoadDict(256); err != nil {
		return err
	}

	hasMasterRef, err := loader.LoadBoolBit()
	if err != nil {
		return err
	}
	if hasMasterRef {
		var masterRef tlb.ExtBlkRef
		if err = tlb.LoadFromCell(&masterRef, loader); err != nil {
			return err
		}
	}
	return nil
}

func SkipCurrencyCollection(loader *cell.Slice) error {
	if _, err := loader.LoadBigCoins(); err != nil {
		return err
	}
	_, err := loader.LoadMaybeRef()
	return err
}

func VisitSliceRefsRecursive(sl *cell.Slice) error {
	if sl == nil {
		return nil
	}

	loader := sl.Copy()
	seen := map[cell.Hash]struct{}{}
	for loader.RefsNum() > 0 {
		ref, err := loader.LoadRefCell()
		if err != nil {
			return fmt.Errorf("load proof ref: %w", err)
		}
		if err = visitCellRecursiveSeen(ref, seen); err != nil {
			return err
		}
	}
	return nil
}

func AccountKey(accountID []byte) *cell.Cell {
	return cell.BeginCell().MustStoreSlice(accountID, 256).EndCell()
}

// ShardHashesProof builds a usage proof of the shard-hash tree path for the
// given shard inside a masterchain state root.
func ShardHashesProof(stateRoot *cell.Cell, workchain int32, shard int64, exact bool) (*cell.Cell, error) {
	return CreateUsageProof(stateRoot, func(root *cell.Cell) error {
		loader, err := root.BeginParse()
		if err != nil {
			return err
		}

		state, err := VisitShardStateHeader(loader)
		if err != nil {
			return err
		}
		if state.McStateExtra == nil {
			return fmt.Errorf("state is missing mc_state_extra")
		}

		extraLoader, err := state.McStateExtra.BeginParse()
		if err != nil {
			return err
		}

		var extra tlb.McStateExtra
		if err = tlb.LoadFromCell(&extra, extraLoader); err != nil {
			return err
		}
		if extra.Info != nil {
			if err = VisitMcStateExtraInfo(extra.Info); err != nil {
				return err
			}
		}
		if extra.ShardHashes == nil {
			return fmt.Errorf("state is missing shard hashes")
		}

		value, err := extra.ShardHashes.LoadValue(cell.BeginCell().MustStoreInt(int64(workchain), 32).EndCell())
		if errors.Is(err, cell.ErrNoSuchKeyInDict) {
			return nil
		}
		if err != nil {
			return err
		}

		root, err = value.LoadRefCell()
		if err != nil {
			return err
		}

		trueShard, leaf, err := findShardLeaf(root, uint64(shard), exact)
		if errors.Is(err, storage.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		_, err = BlockIDFromShardLeaf(workchain, trueShard, leaf)
		return err
	})
}

func BlockContainsAccount(block ton.BlockIDExt, workchain int32, account []byte) bool {
	if block.Workchain != workchain || len(account) != 32 {
		return false
	}

	addr := address.NewAddress(0, byte(workchain), account)
	return tlb.ShardID(uint64(block.Shard)).ContainsAddress(addr)
}
