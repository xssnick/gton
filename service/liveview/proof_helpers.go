package liveview

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func stateRootHashFromBlock(id ton.BlockIDExt, root *cell.Cell) ([]byte, error) {
	if _, err := storage.ParseVerifiedBlockCell(id, root); err != nil {
		return nil, err
	}

	rootLoader, err := root.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("load block root %s: %w", storage.FormatBlockRef(id), err)
	}

	update, err := rootLoader.PeekRefCellAt(2)
	if err != nil {
		return nil, fmt.Errorf("block %s has no state update: %w", storage.FormatBlockRef(id), err)
	}
	updateLoader, err := update.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("load block state update %s: %w", storage.FormatBlockRef(id), err)
	}

	nextState, err := updateLoader.PeekRefCellAt(1)
	if err != nil {
		return nil, fmt.Errorf("load block state update target %s: %w", storage.FormatBlockRef(id), err)
	}

	hash := nextState.HashKeyAt(0)
	return bytes.Clone(hash[:]), nil
}

func accountStateProofAndCell(stateRoot *cell.Cell, accountID []byte) (*cell.Cell, *cell.Cell, error) {
	var accountCell *cell.Cell

	proof, err := blockproof.CreateUsageProof(stateRoot, func(root *cell.Cell) error {
		loader, err := root.BeginParse()
		if err != nil {
			return err
		}
		if _, err = blockproof.VisitShardStateHeader(loader); err != nil {
			return err
		}

		dictRoot, err := accountsDictRoot(root)
		if errors.Is(err, storage.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		value, extra, err := dictRoot.AsAugDict(256, tlb.AugShardAccounts{}).LoadValueExtra(blockproof.AccountKey(accountID))
		if errors.Is(err, cell.ErrNoSuchKeyInDict) {
			return nil
		}
		if err != nil {
			return err
		}

		if err = tlb.LoadFromCell(new(tlb.DepthBalanceInfo), extra); err != nil {
			return err
		}

		accountValue := value.Copy().WithoutTrace()
		var account tlb.ShardAccount
		if err = tlb.LoadFromCell(&account, accountValue); err != nil {
			return err
		}
		accountCell = account.Account
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return proof, accountCell, nil
}

func accountPrunedProof(account *cell.Cell) (*cell.Cell, error) {
	source, err := account.BeginParse()
	if err != nil {
		return nil, err
	}

	root := source.BaseCell().WithTrace(nil)
	builder := blockproof.NewProofBuilder(root)
	loader, err := builder.Root().BeginParse()
	if err != nil {
		return nil, err
	}
	if err = visitPrunedAccount(loader); err != nil {
		return nil, err
	}
	return builder.CreateProof()
}

func visitPrunedAccount(loader *cell.Slice) error {
	isAccount, err := loader.LoadBoolBit()
	if err != nil || !isAccount {
		return err
	}

	if _, err = loader.LoadAddr(); err != nil {
		return err
	}

	var storageInfo tlb.StorageInfo
	if err = tlb.LoadFromCell(&storageInfo, loader); err != nil {
		return err
	}

	if _, err = loader.LoadUInt(64); err != nil {
		return err
	}
	if _, err = loader.LoadBigCoins(); err != nil {
		return err
	}

	extraCurrencies, err := loader.LoadDict(32)
	if err != nil {
		return err
	}
	if extraCurrencies != nil && !extraCurrencies.IsEmpty() {
		if _, err = extraCurrencies.LoadAll(); err != nil {
			return err
		}
	}

	active, err := loader.LoadBoolBit()
	if err != nil {
		return err
	}
	if active {
		return skipPrunedStateInit(loader)
	}

	frozen, err := loader.LoadBoolBit()
	if err != nil || !frozen {
		return err
	}
	_, err = loader.LoadSlice(256)
	return err
}

func skipPrunedStateInit(loader *cell.Slice) error {
	hasDepth, err := loader.LoadBoolBit()
	if err != nil {
		return err
	}
	if hasDepth {
		if _, err = loader.LoadUInt(5); err != nil {
			return err
		}
	}

	hasTickTock, err := loader.LoadBoolBit()
	if err != nil {
		return err
	}
	if hasTickTock {
		if _, err = loader.LoadBoolBit(); err != nil {
			return err
		}
		if _, err = loader.LoadBoolBit(); err != nil {
			return err
		}
	}

	for range 3 {
		hasRef, err := loader.LoadBoolBit()
		if err != nil {
			return err
		}
		if hasRef {
			if err = loader.SkipBitsAndRefs(0, 1); err != nil {
				return err
			}
		}
	}
	return nil
}

func accountCellFromAccountsRoot(dictRoot *cell.Cell, accountID []byte) (*cell.Cell, error) {
	if dictRoot == nil {
		return nil, cell.ErrNoSuchKeyInDict
	}
	value, err := dictRoot.AsAugDict(256, tlb.AugShardAccounts{}).LoadValue(blockproof.AccountKey(accountID))
	if err != nil {
		return nil, fmt.Errorf("load account value from accounts dict: %w", err)
	}

	var account tlb.ShardAccount
	if err = tlb.LoadFromCell(&account, value); err != nil {
		return nil, fmt.Errorf("load shard account: %w", err)
	}

	return account.Account, nil
}

func accountsDictRoot(stateRoot *cell.Cell) (*cell.Cell, error) {
	stateLoader, err := stateRoot.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("parse shard state root: %w", err)
	}
	accounts, err := stateLoader.PeekRefCellAt(1)
	if err != nil {
		return nil, fmt.Errorf("load shard state accounts ref: %w", err)
	}

	loader, err := accounts.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("parse shard accounts cell: %w", err)
	}
	hasRoot, err := loader.LoadBoolBit()
	if err != nil {
		return nil, fmt.Errorf("load shard accounts dict flag: %w", err)
	}
	if !hasRoot {
		return nil, storage.ErrNotFound
	}

	root, err := loader.LoadRefCell()
	if err != nil {
		return nil, fmt.Errorf("load shard accounts dict root ref: %w", err)
	}
	return root, nil
}

func mcStateExtra(root *cell.Cell) (*tlb.McStateExtra, error) {
	loader, err := root.BeginParse()
	if err != nil {
		return nil, err
	}

	var state tlb.ShardStateUnsplit
	if err := tlb.LoadFromCell(&state, loader); err != nil {
		return nil, err
	}
	if state.McStateExtra == nil {
		return nil, fmt.Errorf("state is missing mc_state_extra")
	}

	extraLoader, err := state.McStateExtra.BeginParse()
	if err != nil {
		return nil, err
	}

	var extra tlb.McStateExtra
	if err := tlb.LoadFromCell(&extra, extraLoader); err != nil {
		return nil, err
	}
	return &extra, nil
}

func librariesDict(stateRoot *cell.Cell) (*cell.Dictionary, error) {
	stateLoader, err := stateRoot.BeginParse()
	if err != nil {
		return nil, err
	}

	var state tlb.ShardStateUnsplit
	if err := tlb.LoadFromCell(&state, stateLoader); err != nil {
		return nil, err
	}
	if state.Stats == nil {
		return nil, fmt.Errorf("state is missing shard state extras")
	}

	loader, err := state.Stats.BeginParse()
	if err != nil {
		return nil, err
	}
	if _, err := loader.LoadUInt(64); err != nil {
		return nil, err
	}
	if _, err := loader.LoadUInt(64); err != nil {
		return nil, err
	}
	if err := tlb.LoadFromCell(new(tlb.CurrencyCollection), loader); err != nil {
		return nil, err
	}
	if err := tlb.LoadFromCell(new(tlb.CurrencyCollection), loader); err != nil {
		return nil, err
	}
	return loader.LoadDict(256)
}
