package externalmsg

import (
	"bytes"
	"container/list"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/internal/extmsg"
	"github.com/xssnick/gton/service/liveview"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// blockContextCacheLimit bounds the prepared tvm block-context cache. A
// context is keyed by the (account block, master block) pair the checker
// validates against, so the live window needs one entry per current shard
// block plus the masterchain block itself.
const blockContextCacheLimit = 8

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

	mu       sync.Mutex
	contexts map[blockContextKey]*list.Element
	order    *list.List
}

// blockContextKey identifies the execution context of one accept check: the
// account shard block provides the block time and logical time, the master
// block provides the config epoch, prev-blocks info and global libraries.
type blockContextKey struct {
	account [32]byte
	master  [32]byte
}

type blockContextEntry struct {
	key   blockContextKey
	block *tvm.BlockContext
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
		log:      log,
		store:    opts.Store,
		tvm:      machine,
		contexts: map[blockContextKey]*list.Element{},
		order:    list.New(),
	}, nil
}

func (c *Checker) CheckBOC(ctx context.Context, data []byte) (CheckResult, error) {
	root, msg, err := extmsg.ParseMessage(data)
	if err != nil {
		return CheckResult{}, err
	}
	return c.Check(ctx, data, root, msg)
}

func (c *Checker) Check(ctx context.Context, data []byte, msgCell *cell.Cell, msg *tlb.ExternalMessage) (CheckResult, error) {
	blocks, err := c.store.CurrentAccountBlocks(ctx, msg.DstAddr.Workchain(), msg.DstAddr.Data())
	if err != nil {
		return CheckResult{}, err
	}

	stateFragments, err := c.store.BlockFragments(ctx, blocks.Account)
	if err != nil {
		return CheckResult{}, err
	}

	masterFragments := stateFragments
	if !blockIDEqual(blocks.Account, blocks.Master) {
		masterFragments, err = c.store.BlockFragments(ctx, blocks.Master)
		if err != nil {
			return CheckResult{}, err
		}
	}

	header := stateFragments.Header()

	shardAccount, accountState, err := stateFragments.ExternalMessageAccount(msg.DstAddr)
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

	block, err := c.blockContext(blocks, header, masterFragments)
	if err != nil {
		return CheckResult{}, err
	}

	accepted, err := c.checkAccepted(block, shardAccount, accountState, msgCell, msg, header)
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

func (c *Checker) checkAccepted(block *tvm.BlockContext, shardAccount *tlb.ShardAccount, accountState *tlb.AccountState, msgCell *cell.Cell, msg *tlb.ExternalMessage, header liveview.RunMethodShardHeader) (bool, error) {
	account, err := tvm.PrepareParsedAccount(shardAccount, accountState, msg.DstAddr)
	if err != nil {
		return false, err
	}
	message, err := tvm.PrepareParsedMessage(msgCell, &tlb.Message{
		MsgType: tlb.MsgTypeExternalIn,
		Msg:     msg,
	})
	if err != nil {
		return false, err
	}

	return c.tvm.CheckExternalMessageAccepted(block, account, message, tvm.TransactionOptions{
		LogicalTime: int64(header.GenLT + 2),
		RandSeed:    randSeed(),
	})
}

// blockContext returns the tvm execution context for the (account block,
// master block) pair, reusing a cached context when the checker already
// validated another message against the same pair. The context carries only
// block-scoped state (block time, prev blocks, config epoch, global
// libraries); everything per-account or per-message is passed per call.
func (c *Checker) blockContext(blocks liveview.CurrentAccountBlockIDs, header liveview.RunMethodShardHeader, master *liveview.BlockView) (*tvm.BlockContext, error) {
	key, ok := blockContextCacheKey(blocks)
	if !ok {
		return master.BlockContext(header.GenUTime, header.GenLT)
	}

	c.mu.Lock()
	if element, hit := c.contexts[key]; hit {
		c.order.MoveToFront(element)
		block := element.Value.(*blockContextEntry).block
		c.mu.Unlock()
		return block, nil
	}
	c.mu.Unlock()

	block, err := master.BlockContext(header.GenUTime, header.GenLT)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if element, hit := c.contexts[key]; hit {
		// A concurrent check built the same context; keep the published one.
		c.order.MoveToFront(element)
		return element.Value.(*blockContextEntry).block, nil
	}
	c.contexts[key] = c.order.PushFront(&blockContextEntry{key: key, block: block})
	for len(c.contexts) > blockContextCacheLimit {
		oldest := c.order.Back()
		c.order.Remove(oldest)
		delete(c.contexts, oldest.Value.(*blockContextEntry).key)
	}
	return block, nil
}

func blockContextCacheKey(blocks liveview.CurrentAccountBlockIDs) (blockContextKey, bool) {
	var key blockContextKey
	if len(blocks.Account.RootHash) != len(key.account) || len(blocks.Master.RootHash) != len(key.master) {
		return blockContextKey{}, false
	}
	copy(key.account[:], blocks.Account.RootHash)
	copy(key.master[:], blocks.Master.RootHash)
	return key, true
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
	rand.Read(seed[:])
	return seed[:]
}
