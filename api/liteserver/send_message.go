package liteserver

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/internal/extmsg"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"github.com/xssnick/tonutils-go/tvm/tuple"
)

const (
	maxExternalMessageBroadcastDataSize = 16 << 20
	maxExternalMessageBOCCells          = 1 << 17
)

var ErrExternalMessageRejected = errors.New("external message was not accepted")

type CurrentAccountBlockIDs struct {
	Master  ton.BlockIDExt
	Account ton.BlockIDExt
}

type ExternalMessageCheckOptions struct {
	Logger *zerolog.Logger
	Store  Store
}

type ExternalMessageChecker struct {
	log   zerolog.Logger
	store Store
	tvm   *tvm.TVM
}

type ExternalMessageCheckResult struct {
	Root    *cell.Cell
	Message *tlb.ExternalMessage
	Blocks  CurrentAccountBlockIDs
}

func NewExternalMessageChecker(opts ExternalMessageCheckOptions) (*ExternalMessageChecker, error) {
	if opts.Store == nil {
		return nil, fmt.Errorf("external message checker store is required")
	}

	log := zerolog.Nop()
	if opts.Logger != nil {
		log = *opts.Logger
	}

	return &ExternalMessageChecker{
		log:   log,
		store: opts.Store,
		tvm:   tvm.NewTVM(),
	}, nil
}

func (c *ExternalMessageChecker) CheckBOC(ctx context.Context, data []byte) (ExternalMessageCheckResult, error) {
	root, msg, err := parseExternalMessage(data)
	if err != nil {
		return ExternalMessageCheckResult{}, err
	}
	return c.Check(ctx, data, root, msg)
}

func (c *ExternalMessageChecker) Check(ctx context.Context, data []byte, msgCell *cell.Cell, msg *tlb.ExternalMessage) (ExternalMessageCheckResult, error) {
	blocks, err := c.currentExternalMessageBlocks(ctx, msg.DstAddr)
	if err != nil {
		return ExternalMessageCheckResult{}, err
	}

	stateFragments, err := c.blockFragments(ctx, blocks.Account)
	if err != nil {
		return ExternalMessageCheckResult{}, err
	}

	masterFragments := stateFragments
	if !blockIDEqual(blocks.Account, blocks.Master) {
		masterFragments, err = c.blockFragments(ctx, blocks.Master)
		if err != nil {
			return ExternalMessageCheckResult{}, err
		}
	}

	header := stateFragments.shardHeader

	shardAccount, accountState, err := externalMessageAccountFromAccountsRoot(stateFragments.accountsRoot, msg.DstAddr)
	if err != nil {
		return ExternalMessageCheckResult{}, err
	}

	var accountCode *cell.Cell
	var accountLibraries *cell.Dictionary
	if accountState.IsValid && accountState.StateInit != nil {
		accountCode = accountState.StateInit.Code
		accountLibraries = accountState.StateInit.Lib
	}

	config, err := masterFragments.runMethodConfig(header.GenUTime, accountCode)
	if err != nil {
		return ExternalMessageCheckResult{}, err
	}
	limits, err := masterFragments.externalMessageLimits()
	if err != nil {
		return ExternalMessageCheckResult{}, err
	}
	if err = checkExternalMessageLimits(limits, data, msgCell); err != nil {
		return ExternalMessageCheckResult{}, err
	}

	libraries, err := masterFragments.runMethodLibraries(accountLibraries)
	if err != nil {
		return ExternalMessageCheckResult{}, err
	}

	unpacked, _ := config.Unpacked.(tuple.Tuple)
	machine, err := c.tvm.WithGlobalVersion(config.GlobalVersion)
	if err != nil {
		return ExternalMessageCheckResult{}, err
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

	accepted, err := machine.CheckExternalMessageAccepted(shardAccount, accountState, msgCell, msg, checkConfig)
	if err != nil {
		return ExternalMessageCheckResult{}, fmt.Errorf("%w: cannot run message on account: %w", ErrExternalMessageRejected, err)
	}
	if !accepted {
		return ExternalMessageCheckResult{}, ErrExternalMessageRejected
	}
	return ExternalMessageCheckResult{
		Root:    msgCell,
		Message: msg,
		Blocks:  blocks,
	}, nil
}

func (s *Server) checkExternalMessage(ctx context.Context, data []byte, msgCell *cell.Cell, msg *tlb.ExternalMessage) (ExternalMessageCheckResult, error) {
	checker := ExternalMessageChecker{
		log:   s.log,
		store: s.store,
		tvm:   s.tvm,
	}
	return checker.Check(ctx, data, msgCell, msg)
}

func (c *ExternalMessageChecker) currentExternalMessageBlocks(ctx context.Context, addr *address.Address) (CurrentAccountBlockIDs, error) {
	return c.store.CurrentAccountBlocks(ctx, addr.Workchain(), addr.Data())
}

func (c *ExternalMessageChecker) blockFragments(ctx context.Context, block ton.BlockIDExt) (*liveBlockFragments, error) {
	return c.store.BlockFragments(ctx, block)
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

type externalMessageSizeLimits struct {
	maxSize  uint32
	maxDepth uint16
}

func (f *liveBlockFragments) externalMessageLimits() (externalMessageSizeLimits, error) {
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
			return externalMessageSizeLimits{}, err
		}
		limits, err := externalMessageLimitsFromBaseConfig(base)
		if err != nil {
			return externalMessageSizeLimits{}, err
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
		return externalMessageSizeLimits{}, err
	}
	limits, ok := value.(externalMessageSizeLimits)
	if !ok {
		return externalMessageSizeLimits{}, errors.New("invalid external message limits cache value")
	}
	return limits, nil
}

func externalMessageLimitsFromBaseConfig(base *runMethodBaseConfig) (externalMessageSizeLimits, error) {
	param := base.Unpacked.Params[runMethodConfigParamSizeLimitsIndex]

	var limits tlb.SizeLimitsConfig
	if param == nil {
		var err error
		// Preserve tonutils-go's default size limits when config param 43 is absent.
		limits, err = base.Config.GetSizeLimitsConfig()
		if err != nil {
			return externalMessageSizeLimits{}, err
		}
	} else if err := tlb.Parse(&limits, param); err != nil {
		return externalMessageSizeLimits{}, err
	}

	switch cfg := limits.Config.(type) {
	case tlb.SizeLimitsConfigV1:
		return externalMessageSizeLimits{maxSize: cfg.MaxExtMsgSize, maxDepth: cfg.MaxExtMsgDepth}, nil
	case tlb.SizeLimitsConfigV2:
		return externalMessageSizeLimits{maxSize: cfg.MaxExtMsgSize, maxDepth: cfg.MaxExtMsgDepth}, nil
	default:
		return externalMessageSizeLimits{}, fmt.Errorf("unsupported size limits config %T", limits.Config)
	}
}

func checkExternalMessageLimits(limits externalMessageSizeLimits, data []byte, root *cell.Cell) error {
	if uint64(len(data)) > uint64(limits.maxSize) {
		return errors.New("external message too large, rejecting")
	}
	if root.Level() != 0 {
		return errors.New("external message must have zero level")
	}
	if root.Depth() >= limits.maxDepth {
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
