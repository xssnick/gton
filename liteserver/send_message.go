package liteserver

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strings"

	"github.com/xssnick/gton/internal/extmsg"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"github.com/xssnick/tonutils-go/tvm/tuple"
	vmcore "github.com/xssnick/tonutils-go/tvm/vm"
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

	execConfig := tvm.TransactionEmulationConfig{
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
	}

	var trace []vmcore.TraceStep
	if s.sendMessageTVMTrace {
		execConfig.TraceHook = func(step vmcore.TraceStep) {
			trace = append(trace, step)
		}
	}
	result, err := machine.EmulateTransaction(shardAccount, msgCell, execConfig)
	if s.sendMessageTVMTrace && (err != nil || result == nil || !result.Accepted) {
		s.logSendMessageTVMTrace(err, result, trace, msg.DstAddr, current, state, masterFragments.shardHeader, header, execConfig, config.GlobalVersion)
	}
	if err != nil {
		return fmt.Errorf("external message was not accepted: cannot run message on account: %w", err)
	}
	if !result.Accepted {
		return errors.New("external message was not accepted")
	}
	return nil
}

func (s *Server) logSendMessageTVMTrace(err error, result *tvm.TransactionExecutionResult, trace []vmcore.TraceStep, addr *address.Address, current *storage.CurrentState, state storage.BlockState, masterHeader runMethodShardHeader, executionHeader runMethodShardHeader, cfg tvm.TransactionEmulationConfig, globalVersion int) {
	accepted := false
	if result != nil {
		accepted = result.Accepted
	}

	event := s.log.Warn().
		Bool("accepted", accepted).
		Int("vm_trace_steps", len(trace)).
		Str("address", addr.StringRaw()).
		Int32("address_workchain", addr.Workchain()).
		Str("master_block", storage.FormatBlockRef(current.Masterchain.Block)).
		Int32("master_workchain", current.Masterchain.Block.Workchain).
		Int64("master_shard", current.Masterchain.Block.Shard).
		Uint32("master_seqno", current.Masterchain.Block.SeqNo).
		Uint32("master_gen_utime", masterHeader.GenUTime).
		Uint64("master_gen_lt", masterHeader.GenLT).
		Str("execution_block", storage.FormatBlockRef(state.Block)).
		Int32("execution_workchain", state.Block.Workchain).
		Int64("execution_shard", state.Block.Shard).
		Uint32("execution_seqno", state.Block.SeqNo).
		Uint32("execution_gen_utime", executionHeader.GenUTime).
		Uint64("execution_gen_lt", executionHeader.GenLT).
		Int("global_version", globalVersion).
		Uint32("c7_now", cfg.Now).
		Int64("c7_block_lt", cfg.BlockLT).
		Int64("c7_logical_time", cfg.LogicalTime).
		Str("vm_trace", formatSendMessageTVMTrace(trace))
	if err != nil {
		event = event.Err(err)
	}
	event.Msg("sendMessage TVM trace")
}

func formatSendMessageTVMTrace(trace []vmcore.TraceStep) string {
	if len(trace) == 0 {
		return ""
	}

	lines := make([]string, 0, len(trace))
	for _, step := range trace {
		lines = append(lines, step.String())
	}
	return strings.Join(lines, "\n")
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
