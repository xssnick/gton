package liveview

import (
	"context"
	"errors"
	"fmt"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type ExternalMessageSizeLimits struct {
	MaxSize  uint32
	MaxDepth uint16
}

func (f *BlockView) ExternalMessageLimits() (ExternalMessageSizeLimits, error) {
	f.mu.Lock()
	if f.extMsgLimitsLoaded {
		limits := f.extMsgLimits
		f.mu.Unlock()
		return limits, nil
	}
	f.mu.Unlock()

	value, err := f.lazyLoad.do(context.Background(), liveBlockFragmentLoadKey{kind: liveBlockFragmentLoadExternalMessageLimits}, func() (any, error) {
		f.mu.Lock()
		if f.extMsgLimitsLoaded {
			limits := f.extMsgLimits
			f.mu.Unlock()
			return limits, nil
		}
		f.mu.Unlock()

		base, err := f.runMethodBaseConfig()
		if err != nil {
			return ExternalMessageSizeLimits{}, err
		}
		limits, err := externalMessageLimitsFromBaseConfig(base)
		if err != nil {
			return ExternalMessageSizeLimits{}, err
		}

		f.mu.Lock()
		if !f.extMsgLimitsLoaded {
			f.extMsgLimits = limits
			f.extMsgLimitsLoaded = true
		} else {
			limits = f.extMsgLimits
		}
		f.mu.Unlock()
		return limits, nil
	})
	if err != nil {
		return ExternalMessageSizeLimits{}, err
	}
	limits, ok := value.(ExternalMessageSizeLimits)
	if !ok {
		return ExternalMessageSizeLimits{}, errors.New("invalid external message limits cache value")
	}
	return limits, nil
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
	return externalMessageAccountFromAccountsRoot(f.accountsRoot, addr)
}

func externalMessageLimitsFromBaseConfig(base *runMethodBaseConfig) (ExternalMessageSizeLimits, error) {
	param := base.Unpacked.Params[runMethodConfigParamSizeLimitsIndex]

	var limits tlb.SizeLimitsConfig
	if param == nil {
		var err error
		// Preserve tonutils-go's default size limits when config param 43 is absent.
		limits, err = base.Config.GetSizeLimitsConfig()
		if err != nil {
			return ExternalMessageSizeLimits{}, err
		}
	} else if err := tlb.Parse(&limits, param); err != nil {
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

	value, err := accountsRoot.AsDict(256).LoadValue(accountKey(addr.Data()))
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
