package externalmsg

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/liveview"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"github.com/xssnick/tonutils-go/tvm/tuple"
)

const maxBOCCells = 1 << 17

var ErrRejected = errors.New("external message was not accepted")

type Store interface {
	CurrentAccountBlocks(ctx context.Context, workchain int32, account []byte) (liveview.CurrentAccountBlockIDs, error)
	BlockFragments(ctx context.Context, block ton.BlockIDExt) (*liveview.BlockView, error)
}

type Options struct {
	Logger *zerolog.Logger
	Store  Store
	TVM    *tvm.TVM
}

type Checker struct {
	log   zerolog.Logger
	store Store
	tvm   *tvm.TVM
}

type CheckResult struct {
	Root    *cell.Cell
	Message *tlb.ExternalMessage
	Blocks  liveview.CurrentAccountBlockIDs
}

func NewChecker(opts Options) (*Checker, error) {
	if opts.Store == nil {
		return nil, fmt.Errorf("external message checker store is required")
	}

	log := zerolog.Nop()
	if opts.Logger != nil {
		log = *opts.Logger
	}

	machine := opts.TVM
	if machine == nil {
		machine = tvm.NewTVM()
	}

	return &Checker{
		log:   log,
		store: opts.Store,
		tvm:   machine,
	}, nil
}

func (c *Checker) CheckBOC(ctx context.Context, data []byte) (CheckResult, error) {
	root, msg, err := ParseMessage(data)
	if err != nil {
		return CheckResult{}, err
	}
	return c.Check(ctx, data, root, msg)
}

func (c *Checker) Check(ctx context.Context, data []byte, msgCell *cell.Cell, msg *tlb.ExternalMessage) (CheckResult, error) {
	blocks, err := c.currentBlocks(ctx, msg.DstAddr)
	if err != nil {
		return CheckResult{}, err
	}

	stateFragments, err := c.blockFragments(ctx, blocks.Account)
	if err != nil {
		return CheckResult{}, err
	}

	masterFragments := stateFragments
	if !blockIDEqual(blocks.Account, blocks.Master) {
		masterFragments, err = c.blockFragments(ctx, blocks.Master)
		if err != nil {
			return CheckResult{}, err
		}
	}

	header := stateFragments.Header()

	shardAccount, accountState, err := stateFragments.ExternalMessageAccount(msg.DstAddr)
	if err != nil {
		return CheckResult{}, err
	}

	var accountCode *cell.Cell
	var accountLibraries *cell.Dictionary
	if accountState.IsValid && accountState.StateInit != nil {
		accountCode = accountState.StateInit.Code
		accountLibraries = accountState.StateInit.Lib
	}

	config, err := masterFragments.RunMethodConfig(header.GenUTime, accountCode)
	if err != nil {
		return CheckResult{}, err
	}
	limits, err := masterFragments.ExternalMessageLimits()
	if err != nil {
		return CheckResult{}, err
	}
	if err = liveview.CheckExternalMessageLimits(limits, data, msgCell); err != nil {
		return CheckResult{}, err
	}

	libraries, err := masterFragments.RunMethodLibraries(accountLibraries)
	if err != nil {
		return CheckResult{}, err
	}

	unpacked, _ := config.Unpacked.(tuple.Tuple)
	machine, err := c.tvm.WithGlobalVersion(config.GlobalVersion)
	if err != nil {
		return CheckResult{}, err
	}

	checkConfig := tvm.CheckExternalMessageAcceptedConfig{
		Now:                 header.GenUTime,
		BlockLT:             int64(header.GenLT),
		LogicalTime:         int64(header.GenLT + 2),
		RandSeed:            randSeed(),
		ConfigRoot:          config.Root,
		PrevBlocks:          config.PrevBlocks,
		UnpackedConfig:      unpacked,
		DuePayment:          liveview.AccountDuePayment(*accountState),
		PrecompiledGasUsage: config.Precompiled,
		Libraries:           libraries,
	}

	accepted, err := machine.CheckExternalMessageAccepted(shardAccount, accountState, msgCell, msg, checkConfig)
	if err != nil {
		return CheckResult{}, fmt.Errorf("%w: cannot run message on account: %w", ErrRejected, err)
	}
	if !accepted {
		return CheckResult{}, ErrRejected
	}
	return CheckResult{
		Root:    msgCell,
		Message: msg,
		Blocks:  blocks,
	}, nil
}

func ParseMessage(data []byte) (*cell.Cell, *tlb.ExternalMessage, error) {
	if len(data) == 0 {
		return nil, nil, errors.New("external message is empty")
	}

	root, err := cell.FromBOCWithOptions(data, cell.BOCParseOptions{
		MaxCells: maxBOCCells,
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

func (c *Checker) currentBlocks(ctx context.Context, addr *address.Address) (liveview.CurrentAccountBlockIDs, error) {
	return c.store.CurrentAccountBlocks(ctx, addr.Workchain(), addr.Data())
}

func (c *Checker) blockFragments(ctx context.Context, block ton.BlockIDExt) (*liveview.BlockView, error) {
	return c.store.BlockFragments(ctx, block)
}

func blockIDEqual(a ton.BlockIDExt, b ton.BlockIDExt) bool {
	return a.Workchain == b.Workchain &&
		a.Shard == b.Shard &&
		a.SeqNo == b.SeqNo &&
		bytes.Equal(a.RootHash, b.RootHash) &&
		bytes.Equal(a.FileHash, b.FileHash)
}

func randSeed() []byte {
	var seed [32]byte
	if _, err := rand.Read(seed[:]); err != nil {
		return nil
	}
	return seed[:]
}
