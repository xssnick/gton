package liveview

import (
	"errors"
	"fmt"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const liveExternalMessageAccountCacheLimit = 64

type externalMessageAccountKey [32]byte

type externalMessageAccountValue struct {
	shard   *tlb.ShardAccount
	account *tlb.AccountState
}

type ExternalMessageSizeLimits struct {
	MaxSize  uint32
	MaxDepth uint16
}

func (f *BlockView) ExternalMessageLimits() (ExternalMessageSizeLimits, error) {
	return lazyLoadFragment(f, &f.extMsgLimitsLoad, struct{}{},
		func() (ExternalMessageSizeLimits, error) {
			if !f.extMsgLimitsLoaded {
				return ExternalMessageSizeLimits{}, storage.ErrNotFound
			}
			return f.extMsgLimits, nil
		},
		func(limits ExternalMessageSizeLimits) ExternalMessageSizeLimits {
			if !f.extMsgLimitsLoaded {
				f.extMsgLimits = limits
				f.extMsgLimitsLoaded = true
			}
			return f.extMsgLimits
		},
		func() (ExternalMessageSizeLimits, error) {
			base, err := f.runMethodBaseConfig()
			if err != nil {
				return ExternalMessageSizeLimits{}, err
			}
			return base.epoch.extMsgLimits, nil
		},
	)
}

func CheckExternalMessageLimits(limits ExternalMessageSizeLimits, data []byte, root *cell.Cell) error {
	if uint64(len(data)) > uint64(limits.MaxSize) {
		return errors.New("external message too large, rejecting")
	}
	if root.Level() != 0 {
		return errors.New("external message must have zero level")
	}
	if root.Depth() >= limits.MaxDepth {
		return errors.New("external message is too deep")
	}
	return nil
}

func (f *BlockView) ExternalMessageAccount(addr *address.Address) (*tlb.ShardAccount, *tlb.AccountState, error) {
	var key externalMessageAccountKey
	copy(key[:], addr.Data())

	f.mu.Lock()
	cached, ok := f.externalMsgAccounts[key]
	f.mu.Unlock()
	if ok {
		return cached.shard, cached.account, nil
	}

	shard, account, err := externalMessageAccountFromAccountsRoot(f.accountsRoot, addr)
	if err != nil {
		return nil, nil, err
	}

	f.mu.Lock()
	if cached, ok = f.externalMsgAccounts[key]; ok {
		f.mu.Unlock()
		return cached.shard, cached.account, nil
	}
	if !f.retainCurrentCaches {
		f.mu.Unlock()
		return shard, account, nil
	}
	if f.externalMsgAccounts == nil {
		f.externalMsgAccounts = make(map[externalMessageAccountKey]externalMessageAccountValue)
	} else if len(f.externalMsgAccounts) >= liveExternalMessageAccountCacheLimit {
		for evicted := range f.externalMsgAccounts {
			delete(f.externalMsgAccounts, evicted)
			break
		}
	}
	f.externalMsgAccounts[key] = externalMessageAccountValue{shard: shard, account: account}
	f.mu.Unlock()
	return shard, account, nil
}

// externalMessageLimitsFromConfigRoot resolves the ext-message size/depth
// limits from a blockchain config root (param 43, defaults applied when
// absent). Called once per config epoch.
func externalMessageLimitsFromConfigRoot(root *cell.Cell) (ExternalMessageSizeLimits, error) {
	limits, err := (tlb.BlockchainConfig{Root: root}).GetSizeLimitsConfig()
	if err != nil {
		return ExternalMessageSizeLimits{}, err
	}

	switch cfg := limits.Config.(type) {
	case tlb.SizeLimitsConfigV1:
		return ExternalMessageSizeLimits{MaxSize: cfg.MaxExtMsgSize, MaxDepth: cfg.MaxExtMsgDepth}, nil
	case tlb.SizeLimitsConfigV2:
		return ExternalMessageSizeLimits{MaxSize: cfg.MaxExtMsgSize, MaxDepth: cfg.MaxExtMsgDepth}, nil
	default:
		return ExternalMessageSizeLimits{}, fmt.Errorf("unsupported size limits config %T", limits.Config)
	}
}

func externalMessageAccountFromAccountsRoot(accountsRoot *cell.Cell, addr *address.Address) (*tlb.ShardAccount, *tlb.AccountState, error) {
	if accountsRoot == nil {
		shard := emptyShardAccount()
		account, err := accountStateFromShardAccount(shard)
		return shard, account, err
	}

	value, err := accountsRoot.AsDict(256).LoadValue(blockproof.AccountKey(addr.Data()))
	if errors.Is(err, cell.ErrNoSuchKeyInDict) {
		shard := emptyShardAccount()
		account, parseErr := accountStateFromShardAccount(shard)
		return shard, account, parseErr
	}
	if err != nil {
		return nil, nil, err
	}

	var extra tlb.DepthBalanceInfo
	if err = tlb.LoadFromCell(&extra, value); err != nil {
		return nil, nil, err
	}

	var account tlb.ShardAccount
	if err = tlb.LoadFromCell(&account, value); err != nil {
		return nil, nil, err
	}

	state, err := accountStateFromShardAccount(&account)
	if err != nil {
		return nil, nil, err
	}
	return &account, state, nil
}

func emptyShardAccount() *tlb.ShardAccount {
	return &tlb.ShardAccount{
		Account:       cell.BeginCell().MustStoreBoolBit(false).EndCell(),
		LastTransHash: make([]byte, 32),
	}
}

func accountStateFromShardAccount(shard *tlb.ShardAccount) (*tlb.AccountState, error) {
	loader, err := shard.Account.BeginParse()
	if err != nil {
		return nil, err
	}

	var account tlb.AccountState
	if err := tlb.LoadFromCell(&account, loader); err != nil {
		return nil, err
	}
	return &account, nil
}
