package collator

import (
	"fmt"
	"math/bits"
	"time"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	mergeMaxQueueSize   = uint64(2047)
	forceSplitQueueSize = uint64(4096)
	splitMaxQueueSize   = uint64(100000)

	// skipExternalsQueueSize keeps ordinary externals out of a block while the
	// outbound queue already holds this many entries. Each external admitted
	// here generates further outbound messages, so taking them on a deep queue
	// both deepens it and inflates the block whatever split comes next has to
	// encode: splitting rewrites the queue trie's spine in proportion to the
	// number of entries, and that rewrite lands in the state update, where no
	// byte budget can shed it because it is not made of transactions.
	//
	// Collator brakes at the same count (SKIP_EXTERNALS_QUEUE_SIZE,
	// collator.cpp:54) and skips per message rather than ending the phase, so a
	// queue that drains mid-collation reopens intake. It exempts messages from
	// high-priority senders; this pool has no priority notion, so the brake
	// applies to all of them.
	skipExternalsQueueSize = uint64(8000)
)

// blockParts contains the serialized candidate pieces assembled after
// execution: the next state, its Merkle update, and the block body parts.
type blockParts struct {
	state       *cell.Cell
	stateUpdate *cell.Cell
	valueFlow   *cell.Cell
	header      tlb.BlockHeader
	extra       *tlb.BlockExtra
	extraRoot   *cell.Cell
}

func (c *collation) buildStateAndBlockParts() (blockParts, error) {
	if c.master != nil {
		return c.buildMasterStateAndBlockParts()
	}

	queueInfo, err := c.buildQueueInfo()
	if err != nil {
		return blockParts{}, err
	}

	flow, err := c.buildValueFlow()
	if err != nil {
		return blockParts{}, err
	}
	overloadHistory, underloadHistory, wantSplit, wantMerge := c.loadHistory()

	statsCell, err := (tlb.ShardStateStats{
		OverloadHistory:    overloadHistory,
		UnderloadHistory:   underloadHistory,
		TotalBalance:       flow.totalBalance,
		TotalValidatorFees: flow.validatorFees,
		Libraries:          c.oldStats.Libraries,
		MasterRef:          c.header.MasterRef,
	}).ToCell()
	if err != nil {
		return blockParts{}, fmt.Errorf("serialize shard state statistics: %w", err)
	}

	minRefMCSeqno := min(c.header.MasterRef.SeqNo, c.processedMinMC)
	nextState := c.oldState
	nextState.Seqno = c.header.SeqNo
	nextState.VertSeqno = c.header.VertSeqNo
	nextState.GenUTime = c.header.GenUtime
	nextState.GenLT = c.maxLT
	nextState.MinRefMCSeqno = minRefMCSeqno
	nextState.OutMsgQueueInfo = queueInfo
	nextState.BeforeSplit = c.header.BeforeSplit
	nextState.Accounts.ShardAccounts = c.accounts
	nextState.Stats = statsCell
	nextState.McStateExtra = nil

	stateRoot, err := tlb.ToCell(&nextState)
	if err != nil {
		return blockParts{}, fmt.Errorf("serialize shard state: %w", err)
	}
	// The published state comes back from the update rather than from the tree
	// just serialized: unchanged subtrees are then the predecessor's own cells,
	// shared instead of duplicated, and free of the collation traversal trace.
	// Both come out of one pass — applying the update separately would rediscover
	// the source subtree behind every pruned boundary that the pass already knew.
	stateUpdate, stateRoot, err := c.usage.CreateMerkleUpdateApplied(stateRoot)
	if err != nil {
		return blockParts{}, fmt.Errorf("create shard state update: %w", err)
	}

	inMessages, err := c.inMessages.ToCell()
	if err != nil {
		return blockParts{}, fmt.Errorf("serialize inbound message descriptors: %w", err)
	}
	outMessages, err := c.outMessages.ToCell()
	if err != nil {
		return blockParts{}, fmt.Errorf("serialize outbound message descriptors: %w", err)
	}
	accountBlocks, err := c.accountBlocks.ToCell()
	if err != nil {
		return blockParts{}, fmt.Errorf("serialize account blocks: %w", err)
	}

	header := c.header
	header.EndLt = c.maxLT
	header.MinRefMcSeqno = minRefMCSeqno
	header.WantSplit = wantSplit
	header.WantMerge = wantMerge
	extra := &tlb.BlockExtra{
		InMsgDesc:          inMessages,
		OutMsgDesc:         outMessages,
		ShardAccountBlocks: accountBlocks,
		RandSeed:           c.req.randSeed[:],
		CreatedBy:          c.req.createdBy[:],
	}

	return blockParts{
		state:       stateRoot,
		stateUpdate: stateUpdate,
		valueFlow:   flow.root,
		header:      header,
		extra:       extra,
	}, nil
}

func (c *collation) buildQueueInfo() (*cell.Cell, error) {
	storeQueueSize := c.config.capabilities&capStoreOutMsgQueueSize != 0
	var extra *tlb.OutMsgQueueExtra
	if !c.dispatchQueue.IsEmpty() || storeQueueSize {
		var outQueueSize *uint64
		if storeQueueSize {
			outQueueSize = &c.queueSize
		}
		extra = &tlb.OutMsgQueueExtra{
			DispatchQueue: c.dispatchQueue,
			OutQueueSize:  outQueueSize,
		}
	}

	root, err := (tlb.OutMsgQueueInfo{
		OutQueue: c.outQueue,
		ProcInfo: c.processed,
		Extra:    extra,
	}).ToCell()
	if err != nil {
		return nil, fmt.Errorf("serialize outbound queue: %w", err)
	}
	return root, nil
}

// valueFlowParts contains the serialized value flow and the balance totals
// reused by the next-state statistics.
type valueFlowParts struct {
	root          *cell.Cell
	totalBalance  tlb.CurrencyCollection
	validatorFees tlb.CurrencyCollection
}

func (c *collation) buildValueFlow() (valueFlowParts, error) {
	if c.master != nil {
		return c.buildMasterValueFlow()
	}
	if !currencyZero(c.burned) {
		return valueFlowParts{}, fmt.Errorf("%w: shardchain transaction reported blackhole burn", ErrInvalidInput)
	}

	totalBalance, err := loadAccountsBalance(c.accounts)
	if err != nil {
		return valueFlowParts{}, err
	}
	transactionFees, err := loadCurrencyExtra(c.accountBlocks.AugmentedDictionary)
	if err != nil {
		return valueFlowParts{}, fmt.Errorf("load transaction fees: %w", err)
	}
	importFees, err := loadImportFees(c.inMessages.AugmentedDictionary)
	if err != nil {
		return valueFlowParts{}, fmt.Errorf("load import fees: %w", err)
	}
	exported, err := loadCurrencyExtra(c.outMessages.AugmentedDictionary)
	if err != nil {
		return valueFlowParts{}, fmt.Errorf("load exported value: %w", err)
	}

	createdNano := c.config.basechain.createFee.Nano()
	createdNano.Rsh(createdNano, uint(c.header.Shard.PrefixBits))
	created := tlb.CurrencyCollection{Coins: tlb.FromNanoTON(createdNano)}

	feesCollected, err := created.Add(transactionFees)
	if err != nil {
		return valueFlowParts{}, fmt.Errorf("add transaction fees: %w", err)
	}
	feesCollected, err = feesCollected.Add(tlb.CurrencyCollection{Coins: importFees.FeesCollected})
	if err != nil {
		return valueFlowParts{}, fmt.Errorf("add import fees: %w", err)
	}
	validatorFees, err := c.oldStats.TotalValidatorFees.Add(feesCollected)
	if err != nil {
		return valueFlowParts{}, fmt.Errorf("update validator fees: %w", err)
	}

	flow := tlb.ValueFlow{
		FromPrevBlock: c.oldStats.TotalBalance,
		ToNextBlock:   totalBalance,
		Imported:      importFees.ValueImported,
		Exported:      exported,
		FeesCollected: feesCollected,
		Created:       created,
	}
	if err = flow.Validate(); err != nil {
		return valueFlowParts{}, fmt.Errorf("validate value flow: %w", err)
	}
	root, err := flow.ToCell()
	if err != nil {
		return valueFlowParts{}, fmt.Errorf("serialize value flow: %w", err)
	}

	return valueFlowParts{root: root, totalBalance: totalBalance, validatorFees: validatorFees}, nil
}

func loadAccountsBalance(accounts *tlb.ShardAccountsAugDict) (tlb.CurrencyCollection, error) {
	extra, err := accounts.LoadRootExtra()
	if err != nil {
		return tlb.CurrencyCollection{}, fmt.Errorf("load accounts balance: %w", err)
	}
	var balance tlb.DepthBalanceInfo
	if err = loadExactSlice(&balance, extra); err != nil {
		return tlb.CurrencyCollection{}, fmt.Errorf("decode accounts balance: %w", err)
	}
	return balance.Currencies, nil
}

func loadCurrencyExtra(dict *cell.AugmentedDictionary) (tlb.CurrencyCollection, error) {
	extra, err := dict.LoadRootExtra()
	if err != nil {
		return tlb.CurrencyCollection{}, err
	}
	var value tlb.CurrencyCollection
	if err = loadExactSlice(&value, extra); err != nil {
		return tlb.CurrencyCollection{}, err
	}
	return value, nil
}

func loadImportFees(dict *cell.AugmentedDictionary) (tlb.ImportFees, error) {
	extra, err := dict.LoadRootExtra()
	if err != nil {
		return tlb.ImportFees{}, err
	}
	var value tlb.ImportFees
	if err = loadExactSlice(&value, extra); err != nil {
		return tlb.ImportFees{}, err
	}
	return value, nil
}

func (c *collation) loadHistory() (uint64, uint64, bool, bool) {
	c.updateCollatedEstimate()
	overload := c.oldStats.OverloadHistory << 1
	underload := c.oldStats.UnderloadHistory << 1
	load := max(c.peakLoad, c.limits.classify())
	// A shard that can no longer collate inside its slot has to split even when
	// every block-limit axis stayed below its soft threshold: the limits measure
	// the block, not what it cost to build it.
	longOverload, longUnderload := c.pace.longCollation(time.Now())

	if load >= LoadSoft || longOverload {
		if c.queueSize <= splitMaxQueueSize {
			overload |= 1
			c.stats.OverloadReason = OverloadBlockLimit
			if load < LoadSoft {
				c.stats.OverloadReason = OverloadLongCollation
			}
		}
	} else if load <= LoadUnderload && !longUnderload && c.queueSize <= mergeMaxQueueSize {
		// The suppression is checked before the queue size, and against a laxer
		// external-wait ratio than the overload trigger uses: between the two
		// ratios a block is neither overloaded nor allowed to claim it is idle.
		underload |= 1
	}
	if overload&1 == 0 && c.queueSize >= forceSplitQueueSize && c.queueSize <= splitMaxQueueSize {
		overload |= 1
		c.stats.OverloadReason = OverloadForceSplitQueue
	}

	wantSplit := historyWeight(overload) >= 0
	wantMerge := !wantSplit && historyWeight(underload) >= 0
	return overload, underload, wantSplit, wantMerge
}

func historyWeight(history uint64) int {
	return bits.OnesCount64(history&0xffff)*3 +
		bits.OnesCount64(history&0xffff0000)*2 +
		bits.OnesCount64(history&0xffff00000000) - 64
}
