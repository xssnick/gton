package liteserver

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"

	"github.com/xssnick/gton/internal/extmsg"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"github.com/xssnick/tonutils-go/tvm/tuple"
)

const maxExternalMessageBroadcastDataSize = 16 << 20

func (s *Server) checkExternalMessage(ctx context.Context, data []byte, msgCell *cell.Cell, msg *tlb.ExternalMessage) error {
	current, err := s.store.CurrentState(ctx)
	if err != nil {
		return err
	}

	state, err := currentStateForAddress(current, msg.DstAddr)
	if err != nil {
		return err
	}

	stateFragments, err := s.blockFragments(ctx, state.Block)
	if err != nil {
		return err
	}

	masterFragments := stateFragments
	if !blockIDEqual(state.Block, current.Masterchain.Block) {
		masterFragments, err = s.blockFragments(ctx, current.Masterchain.Block)
		if err != nil {
			return err
		}
	}

	header := stateFragments.shardHeader

	shardAccount, err := shardAccountFromAccountsRoot(stateFragments.accountsRoot, msg.DstAddr)
	if err != nil {
		return err
	}

	accountCode, accountLibraries, duePayment, err := externalMessageAccountConfig(shardAccount)
	if err != nil {
		return err
	}

	config, err := masterFragments.runMethodConfig(header.GenUTime, accountCode)
	if err != nil {
		return err
	}
	if err = checkExternalMessageLimits(tlb.BlockchainConfig{Root: config.Root}, data, msgCell); err != nil {
		return err
	}

	libraries, err := masterFragments.runMethodLibraries(accountLibraries)
	if err != nil {
		return err
	}

	unpacked, _ := config.Unpacked.(tuple.Tuple)
	machine, err := s.tvm.WithGlobalVersion(config.GlobalVersion)
	if err != nil {
		return err
	}

	result, err := machine.EmulateTransaction(shardAccount, msgCell, tvm.TransactionEmulationConfig{
		Address:             msg.DstAddr,
		Now:                 header.GenUTime,
		BlockLT:             int64(header.GenLT),
		LogicalTime:         int64(header.GenLT + 2),
		RandSeed:            externalMessageRandSeed(),
		ConfigRoot:          config.Root,
		PrevBlocks:          config.PrevBlocks,
		UnpackedConfig:      unpacked,
		DuePayment:          duePayment,
		PrecompiledGasUsage: config.Precompiled,
		Libraries:           libraries,
		StopOnAccept:        true,
	})
	if err != nil {
		return fmt.Errorf("External message was not accepted: cannot run message on account: %w", err)
	}
	if !result.Accepted {
		return errors.New("External message was not accepted")
	}
	return nil
}

func (s *Server) checkExternalMessageAddressLimit(addr *address.Address) error {
	if s.externalMessageLimiter == nil {
		s.externalMessageLimiter = extmsg.NewDefaultAddressLimiter()
	}
	return externalMessageAddressLimitError(addr, s.externalMessageLimiter.Check(externalMessageAddressKey(addr), s.now()))
}

func (s *Server) addExternalMessageAddressLimit(addr *address.Address) error {
	if s.externalMessageLimiter == nil {
		s.externalMessageLimiter = extmsg.NewDefaultAddressLimiter()
	}
	return externalMessageAddressLimitError(addr, s.externalMessageLimiter.Add(externalMessageAddressKey(addr), s.now()))
}

func (s *Server) dropExternalMessageAddressLimit(addr *address.Address) {
	if s.externalMessageLimiter == nil {
		return
	}
	s.externalMessageLimiter.Remove(externalMessageAddressKey(addr), s.now())
}

func externalMessageAddressKey(addr *address.Address) extmsg.AddressKey {
	key := extmsg.AddressKey{Workchain: addr.Workchain()}
	copy(key.Account[:], addr.Data())
	return key
}

func externalMessageAddressLimitError(addr *address.Address, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w %d:%x", err, addr.Workchain(), addr.Data())
}

func parseExternalMessage(data []byte) (*cell.Cell, *tlb.ExternalMessage, error) {
	if len(data) == 0 {
		return nil, nil, errors.New("external message is empty")
	}

	root, err := cell.FromBOC(data)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot parse external message BOC: %w", err)
	}

	var msg tlb.ExternalMessage
	loader, err := root.BeginParse()
	if err != nil {
		return nil, nil, fmt.Errorf("cannot parse external message: %w", err)
	}
	magic, err := loader.PreloadUInt(2)
	if err != nil || magic != 0b10 {
		return nil, nil, errors.New("external message must begin with ext_in_msg_info$10")
	}
	if err = tlb.LoadFromCell(&msg, loader); err != nil {
		return nil, nil, fmt.Errorf("cannot parse external message: %w", err)
	}
	if msg.DstAddr == nil {
		return nil, nil, errors.New("external message has no destination address")
	}
	if msg.DstAddr.Type() != address.StdAddress || msg.DstAddr.BitsLen() != 256 {
		return nil, nil, errors.New("external message destination address is not a std 256-bit address")
	}

	return root, &msg, nil
}

func checkExternalMessageLimits(config tlb.BlockchainConfig, data []byte, root *cell.Cell) error {
	limits, err := config.GetSizeLimitsConfig()
	if err != nil {
		return err
	}

	var maxSize uint32
	var maxDepth uint16
	switch cfg := limits.Config.(type) {
	case tlb.SizeLimitsConfigV1:
		maxSize = cfg.MaxExtMsgSize
		maxDepth = cfg.MaxExtMsgDepth
	case tlb.SizeLimitsConfigV2:
		maxSize = cfg.MaxExtMsgSize
		maxDepth = cfg.MaxExtMsgDepth
	default:
		return fmt.Errorf("unsupported size limits config %T", limits.Config)
	}

	if uint64(len(data)) > uint64(maxSize) {
		return errors.New("external message too large, rejecting")
	}
	if root.Level() != 0 {
		return errors.New("external message must have zero level")
	}
	if root.Depth() >= maxDepth {
		return errors.New("external message is too deep")
	}
	return nil
}

func currentStateForAddress(current *storage.CurrentState, addr *address.Address) (storage.BlockState, error) {
	if current == nil {
		return storage.BlockState{}, storage.ErrNotFound
	}
	if addr.Workchain() == masterchainID {
		return current.Masterchain, nil
	}

	for _, shard := range current.Shards {
		if shard.Block.Workchain != addr.Workchain() {
			continue
		}
		if tlb.ShardID(uint64(shard.Block.Shard)).ContainsAddress(addr) {
			return shard, nil
		}
	}

	return storage.BlockState{}, storage.ErrNotFound
}

func shardAccountForExternalMessage(stateRoot *cell.Cell, addr *address.Address) (*tlb.ShardAccount, error) {
	dictRoot, err := accountsDictRoot(stateRoot)
	if errors.Is(err, storage.ErrNotFound) {
		return emptyShardAccount(), nil
	}
	if err != nil {
		return nil, err
	}

	return shardAccountFromAccountsRoot(dictRoot, addr)
}

func shardAccountFromAccountsRoot(accountsRoot *cell.Cell, addr *address.Address) (*tlb.ShardAccount, error) {
	if accountsRoot == nil {
		return emptyShardAccount(), nil
	}

	value, err := accountsRoot.AsDict(256).LoadValue(accountKey(addr.Data()))
	if errors.Is(err, cell.ErrNoSuchKeyInDict) {
		return emptyShardAccount(), nil
	}
	if err != nil {
		return nil, err
	}

	var extra tlb.DepthBalanceInfo
	if err = tlb.LoadFromCell(&extra, value); err != nil {
		return nil, err
	}

	var account tlb.ShardAccount
	if err = tlb.LoadFromCell(&account, value); err != nil {
		return nil, err
	}
	return &account, nil
}

func emptyShardAccount() *tlb.ShardAccount {
	return &tlb.ShardAccount{
		Account:       cell.BeginCell().MustStoreBoolBit(false).EndCell(),
		LastTransHash: make([]byte, 32),
	}
}

func externalMessageAccountConfig(shard *tlb.ShardAccount) (*cell.Cell, *cell.Dictionary, any, error) {
	var account tlb.AccountState
	loader, err := shard.Account.BeginParse()
	if err != nil {
		return nil, nil, nil, err
	}
	if err := tlb.LoadFromCell(&account, loader); err != nil {
		return nil, nil, nil, err
	}
	if !account.IsValid || account.StateInit == nil {
		return nil, nil, nil, nil
	}
	return account.StateInit.Code, account.StateInit.Lib, accountDuePayment(account), nil
}

func externalMessageRandSeed() []byte {
	var seed [32]byte
	if _, err := rand.Read(seed[:]); err != nil {
		return nil
	}
	return seed[:]
}
