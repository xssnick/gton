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
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"github.com/xssnick/tonutils-go/tvm/tuple"
	vmcore "github.com/xssnick/tonutils-go/tvm/vm"
)

const (
	maxExternalMessageBroadcastDataSize = 16 << 20
	maxExternalMessageBOCCells          = 1 << 17
)

var errExternalMessageRejected = errors.New("external message was not accepted")

type CurrentAccountBlockIDs struct {
	Master  ton.BlockIDExt
	Account ton.BlockIDExt
}

func (s *Server) checkExternalMessage(ctx context.Context, data []byte, msgCell *cell.Cell, msg *tlb.ExternalMessage) error {
	blocks, err := s.currentExternalMessageBlocks(ctx, msg.DstAddr)
	if err != nil {
		return err
	}

	stateFragments, err := s.blockFragments(ctx, blocks.Account)
	if err != nil {
		return err
	}

	masterFragments := stateFragments
	if !blockIDEqual(blocks.Account, blocks.Master) {
		masterFragments, err = s.blockFragments(ctx, blocks.Master)
		if err != nil {
			return err
		}
	}

	header := stateFragments.shardHeader

	shardAccount, accountState, err := externalMessageAccountFromAccountsRoot(stateFragments.accountsRoot, msg.DstAddr)
	if err != nil {
		return err
	}

	var accountCode *cell.Cell
	var accountLibraries *cell.Dictionary
	if accountState.IsValid && accountState.StateInit != nil {
		accountCode = accountState.StateInit.Code
		accountLibraries = accountState.StateInit.Lib
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

	checkConfig := tvm.CheckExternalMessageAcceptedConfig{
		Now:                 header.GenUTime,
		BlockLT:             int64(header.GenLT),
		LogicalTime:         int64(header.GenLT + 2),
		RandSeed:            externalMessageRandSeed(),
		ConfigRoot:          config.Root,
		PrevBlocks:          config.PrevBlocks,
		UnpackedConfig:      unpacked,
		DuePayment:          accountDuePayment(*accountState),
		PrecompiledGasUsage: config.Precompiled,
		Libraries:           libraries,
	}

	var trace []vmcore.TraceStep
	if s.sendMessageTVMTrace {
		checkConfig.TraceHook = func(step vmcore.TraceStep) {
			trace = append(trace, step)
		}
	}
	accepted, err := machine.CheckExternalMessageAccepted(shardAccount, accountState, msgCell, msg, checkConfig)
	if s.sendMessageTVMTrace && (err != nil || !accepted) {
		s.logSendMessageTVMTrace(err, accepted, trace, msg.DstAddr, blocks.Master, blocks.Account, masterFragments.shardHeader, header, checkConfig, config.GlobalVersion)
	}
	if err != nil {
		return fmt.Errorf("%w: cannot run message on account: %w", errExternalMessageRejected, err)
	}
	if !accepted {
		return errExternalMessageRejected
	}
	return nil
}

func (s *Server) currentExternalMessageBlocks(ctx context.Context, addr *address.Address) (CurrentAccountBlockIDs, error) {
	return s.store.CurrentAccountBlocks(ctx, addr.Workchain(), addr.Data())
}

func (s *Server) logSendMessageTVMTrace(err error, accepted bool, trace []vmcore.TraceStep, addr *address.Address, master ton.BlockIDExt, execution ton.BlockIDExt, masterHeader runMethodShardHeader, executionHeader runMethodShardHeader, cfg tvm.CheckExternalMessageAcceptedConfig, globalVersion int) {
	event := s.log.Warn().
		Bool("accepted", accepted).
		Int("vm_trace_steps", len(trace)).
		Str("address", addr.StringRaw()).
		Int32("address_workchain", addr.Workchain()).
		Str("master_block", storage.FormatBlockRef(master)).
		Int32("master_workchain", master.Workchain).
		Int64("master_shard", master.Shard).
		Uint32("master_seqno", master.SeqNo).
		Uint32("master_gen_utime", masterHeader.GenUTime).
		Uint64("master_gen_lt", masterHeader.GenLT).
		Str("execution_block", storage.FormatBlockRef(execution)).
		Int32("execution_workchain", execution.Workchain).
		Int64("execution_shard", execution.Shard).
		Uint32("execution_seqno", execution.SeqNo).
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

func (s *Server) checkExternalMessageAddressLimit(key extmsg.AddressKey) error {
	return externalMessageAddressLimitError(key, s.externalMessageLimiter.Check(key, s.now()))
}

func (s *Server) addExternalMessageAddressLimit(key extmsg.AddressKey) error {
	return externalMessageAddressLimitError(key, s.externalMessageLimiter.Add(key, s.now()))
}

func (s *Server) dropExternalMessageAddressLimit(key extmsg.AddressKey) {
	s.externalMessageLimiter.Remove(key, s.now())
}

func externalMessageAddressKey(addr *address.Address) extmsg.AddressKey {
	key := extmsg.AddressKey{Workchain: addr.Workchain()}
	copy(key.Account[:], addr.Data())
	return key
}

func externalMessageAddressLimitError(key extmsg.AddressKey, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w %d:%x", err, key.Workchain, key.Account[:])
}

func parseExternalMessage(data []byte) (*cell.Cell, *tlb.ExternalMessage, error) {
	if len(data) == 0 {
		return nil, nil, errors.New("external message is empty")
	}

	root, err := cell.FromBOCWithOptions(data, cell.BOCParseOptions{
		MaxCells: maxExternalMessageBOCCells,
	})
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
	var account tlb.AccountState
	if err := tlb.Parse(&account, shard.Account); err != nil {
		return nil, err
	}
	return &account, nil
}

func externalMessageRandSeed() []byte {
	var seed [32]byte
	if _, err := rand.Read(seed[:]); err != nil {
		return nil
	}
	return seed[:]
}
