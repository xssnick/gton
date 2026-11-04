package collator

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	sharddomain "github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/validator/msgpool"
)

// GlobalBalanceCalculator reads every top shard registered by the supplied
// masterchain state. The result includes accounts, dispatch queues, undelivered
// outbound messages and masterchain validator fees, independently of the stored
// global_balance. Implementations must authenticate acquired states by block ID.
type GlobalBalanceCalculator interface {
	CalculateGlobalBalance(context.Context, PreviousBlock, uint32) (*big.Int, error)
}

// Only message headers are retained. Keeping message cells here would pin
// arbitrary contract payloads across the block-source cache's generations.
type globalBalanceMessage struct {
	descr tlb.ProcessedMsgDescr
	grams big.Int
}

type globalBalanceShard struct {
	owner     msgpool.ShardIdent
	base      big.Int
	processed []tlb.ProcessedUptoRecord
	messages  []globalBalanceMessage
}

type localGlobalBalanceCalculator struct {
	acquisition *LocalAcquisition
	mode        acquisitionReadMode
	baseline    PreviousBlock
}

type globalBalanceLoad struct {
	done     chan struct{}
	parsed   *globalBalanceShard
	err      error
	canceled bool
}

type globalBalanceHistory struct {
	genLT  uint64
	shards []shardEndLT
}

// This is exactly int_msg_info, leaving StateInit and body unopened.
type globalBalanceMessageHeader struct {
	_           tlb.Magic              `tlb:"$0"`
	IHRDisabled bool                   `tlb:"bool"`
	Bounce      bool                   `tlb:"bool"`
	Bounced     bool                   `tlb:"bool"`
	Source      *address.Address       `tlb:"addr"`
	Destination *address.Address       `tlb:"addr"`
	Value       tlb.CurrencyCollection `tlb:"."`
	IHRFee      tlb.Coins              `tlb:"."`
	FwdFee      tlb.Coins              `tlb:"."`
	CreatedLT   uint64                 `tlb:"## 64"`
	CreatedAt   uint32                 `tlb:"## 32"`
}

func parseGlobalBalanceMessage(value *cell.Slice, version uint32, dispatch bool) (globalBalanceMessage, msgpool.QueueKey, error) {
	var enqueued tlb.EnqueuedMsg
	if err := tlb.LoadFromCell(&enqueued, value); err != nil {
		return globalBalanceMessage{}, msgpool.QueueKey{}, err
	}
	if value.BitsLeft() != 0 || value.RefsNum() != 0 {
		return globalBalanceMessage{}, msgpool.QueueKey{}, fmt.Errorf("%w: trailing enqueued message data", ErrInvalidInput)
	}
	var envelope tlb.MsgEnvelope
	if err := parseExact(&envelope, enqueued.Msg); err != nil {
		return globalBalanceMessage{}, msgpool.QueueKey{}, err
	}
	if err := requireCanonicalEnvelopeTag(envelope); err != nil {
		return globalBalanceMessage{}, msgpool.QueueKey{}, err
	}
	if envelope.CurAddr.Type != tlb.IntermediateAddressRegular || envelope.NextAddr.Type != tlb.IntermediateAddressRegular {
		return globalBalanceMessage{}, msgpool.QueueKey{}, fmt.Errorf("%w: global balance envelope uses non-regular routing addresses", ErrInvalidInput)
	}
	var header globalBalanceMessageHeader
	var s cell.Slice
	if err := envelope.Msg.BeginParseInto(&s); err != nil {
		return globalBalanceMessage{}, msgpool.QueueKey{}, err
	}
	if err := tlb.LoadFromCell(&header, &s); err != nil {
		return globalBalanceMessage{}, msgpool.QueueKey{}, err
	}
	source, err := semanticAccountPrefixFromAddress(header.Source)
	if err != nil {
		return globalBalanceMessage{}, msgpool.QueueKey{}, err
	}
	destination, err := semanticAccountPrefixFromAddress(header.Destination)
	if err != nil {
		return globalBalanceMessage{}, msgpool.QueueKey{}, err
	}
	current := semanticIntermediatePrefix(source, destination, envelope.CurAddr)
	next := semanticIntermediatePrefix(source, destination, envelope.NextAddr)
	emittedLT := header.CreatedLT
	if envelope.EmittedLT != nil {
		emittedLT = *envelope.EmittedLT
	}
	result := globalBalanceMessage{descr: tlb.ProcessedMsgDescr{
		CurWorkchain: current.Workchain, CurPrefix: current.Prefix,
		NextWorkchain: next.Workchain, NextPrefix: next.Prefix,
		LT: emittedLT, EnqueuedLT: enqueued.EnqueuedLT, Hash: envelope.Msg.HashKey(),
	}}
	result.grams.Set(header.Value.Coins.NanoRef())
	if dispatch {
		result.grams.Add(&result.grams, header.FwdFee.NanoRef())
	} else {
		result.grams.Add(&result.grams, envelope.FwdFeeRemaining.NanoRef())
	}
	if version < 12 {
		result.grams.Add(&result.grams, header.IHRFee.NanoRef())
	}

	return result, msgpool.MakeQueueKey(next, result.descr.Hash), nil
}

func parseGlobalBalanceShard(ctx context.Context, state *tlb.ShardStateUnsplit, owner msgpool.ShardIdent, version uint32) (*globalBalanceShard, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := &globalBalanceShard{owner: owner}
	var accounts tlb.DepthBalanceInfo
	if err := parseExact(&accounts, state.Accounts.ShardAccounts.GetRootExtra()); err != nil {
		return nil, fmt.Errorf("decode accounts balance: %w", err)
	}
	result.base.Set(accounts.Currencies.Coins.NanoRef())
	if owner.Workchain == masterchainWorkchainID {
		var stats tlb.ShardStateStats
		if err := parseExact(&stats, state.Stats); err != nil {
			return nil, err
		}
		result.base.Add(&result.base, stats.TotalValidatorFees.Coins.NanoRef())
	}
	var queue tlb.OutMsgQueueInfo
	if err := parseExact(&queue, state.OutMsgQueueInfo); err != nil {
		return nil, err
	}
	var err error
	result.processed, err = tlb.LoadProcessedUptoRecords(queue.ProcInfo, owner.Shard)
	if err != nil {
		return nil, err
	}
	if queue.Extra != nil && queue.Extra.DispatchQueue != nil {
		dispatch, err := globalDispatchBalance(ctx, queue.Extra.DispatchQueue, version)
		if err != nil {
			return nil, err
		}
		result.base.Add(&result.base, dispatch)
	}
	iter, err := queue.OutQueue.IteratorExtra(false, false)
	if err != nil {
		return nil, err
	}
	for iter.Next() {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		item := iter.View()
		message, key, err := parseGlobalBalanceMessage(&item.Value, version, false)
		if err != nil {
			return nil, err
		}
		var storedKey msgpool.QueueKey
		if err = item.Key.LoadSliceInto(storedKey[:], 352); err != nil {
			return nil, err
		}
		if key != storedKey {
			return nil, fmt.Errorf("%w: global balance queue key disagrees with envelope", ErrInvalidInput)
		}
		result.messages = append(result.messages, message)
	}
	if err = iter.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

func globalDispatchBalance(ctx context.Context, queue *tlb.DispatchQueueAugDict, version uint32) (*big.Int, error) {
	var extra tlb.DispatchQueueAugData
	if err := parseExact(&extra, queue.GetRootExtra()); err != nil {
		return nil, err
	}
	if extra.TotalBalance != nil {
		return new(big.Int).Set(extra.TotalBalance.Coins.NanoRef()), nil
	}
	// A pre-GV16 nonempty queue can retain the old constructor until all of
	// its original messages are removed. Such a queue has no stored total.
	total := new(big.Int)
	iter, err := queue.IteratorExtra(false, false)
	if err != nil {
		return nil, err
	}
	for iter.Next() {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		var account tlb.AccountDispatchQueue
		item := iter.View()
		if err = tlb.LoadFromCell(&account, &item.Value); err != nil {
			return nil, err
		}
		// Without a stored root total, the reference sums actual messages even
		// when an individual account already carries the new constructor.
		messages, err := account.Messages.Iterator(false, false)
		if err != nil {
			return nil, err
		}
		for messages.Next() {
			if err = ctx.Err(); err != nil {
				return nil, err
			}
			item := messages.View()
			message, _, err := parseGlobalBalanceMessage(&item.Value, version, true)
			if err != nil {
				return nil, err
			}
			total.Add(total, &message.grams)
		}
		if err = messages.Err(); err != nil {
			return nil, err
		}
	}
	if err = iter.Err(); err != nil {
		return nil, err
	}

	return total, nil
}

func sumGlobalBalance(ctx context.Context, states []*globalBalanceShard, resolve tlb.ShardEndLTFunc) (*big.Int, error) {
	total := new(big.Int)
	for _, state := range states {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		total.Add(total, &state.base)
		for i := range state.messages {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			message := &state.messages[i]
			processed := false
			for _, destination := range states {
				var err error
				processed, err = tlb.AlreadyProcessed(destination.processed, destination.owner.Workchain,
					destination.owner.Shard, &message.descr, resolve)
				if err != nil {
					return nil, err
				}
				if processed {
					break
				}
			}
			if !processed {
				total.Add(total, &message.grams)
			}
		}
	}

	return total, nil
}

// CalculateGlobalBalance uses exact block-source reads, including historical
// masterchain states selected by ProcessedUpto. Its cache lives with each
// bounded block source; it never pins message payloads or starts background work.
func (c localGlobalBalanceCalculator) CalculateGlobalBalance(ctx context.Context, master PreviousBlock, version uint32) (*big.Int, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	state, err := verifyPredecessor("global balance masterchain", &master)
	if err != nil {
		return nil, err
	}
	var extra tlb.McStateExtra
	if state.McStateExtra == nil {
		return nil, fmt.Errorf("%w: global balance requires a masterchain state", ErrInvalidInput)
	}
	if err = parseExact(&extra, state.McStateExtra); err != nil {
		return nil, err
	}
	registry, err := ParseShardRegistry(extra.ShardHashes)
	if err != nil {
		return nil, err
	}
	var info tlb.McStateExtraBlockInfo
	if err = parseExact(&info, extra.Info); err != nil {
		return nil, err
	}
	view := localMasterView{state: state, extra: extra, info: info, registry: registry,
		context: MasterchainContext{ID: master.ID}}
	transitions, err := c.transitions(ctx, master, version)
	if err != nil {
		return nil, err
	}
	if err = transitions.verify(ctx, &localBlockSource{previous: master, state: state}); err != nil {
		return nil, err
	}
	mc, err := parseGlobalBalanceShard(ctx, &state, blockShardIdent(master.ID), version)
	if err != nil {
		return nil, err
	}
	states := make([]*globalBalanceShard, 1, len(registry.leaves)+1)
	states[0] = mc
	for _, leaf := range registry.leaves {
		source, err := c.acquisition.blockSource(ctx, leaf.top.Block, c.mode)
		if err != nil {
			return nil, fmt.Errorf("acquire global balance shard %s: %w", fmt.Sprint(leaf.top.Block), err)
		}
		if err = transitions.verify(ctx, source); err != nil {
			return nil, err
		}
		parsed, err := source.globalBalance(ctx, version)
		if err != nil {
			return nil, err
		}
		states = append(states, parsed)
	}
	history := map[uint32]globalBalanceHistory{master.ID.SeqNo: {genLT: state.GenLT, shards: shardEndLTs(registry)}}
	for _, shard := range states {
		for _, record := range shard.processed {
			seqno := min(record.MCSeqno, master.ID.SeqNo)
			if _, exists := history[seqno]; exists {
				continue
			}
			id, err := view.blockAt(seqno)
			if err != nil {
				return nil, err
			}
			source, err := c.acquisition.blockSource(ctx, id, c.mode)
			if err != nil {
				return nil, err
			}
			genLT, shards, err := source.masterShardRegistry()
			if err != nil {
				return nil, err
			}
			history[seqno] = globalBalanceHistory{genLT: genLT, shards: shardEndLTs(shards)}
		}
	}
	resolve := func(seqno uint32, workchain int32, prefix uint64) uint64 {
		entry := history[min(seqno, master.ID.SeqNo)]
		if workchain == masterchainWorkchainID {
			return entry.genLT
		}
		for _, shard := range entry.shards {
			if shard.shard.Contains(workchain, prefix) {
				return shard.endLT
			}
		}
		return 0
	}

	return sumGlobalBalance(ctx, states, resolve)
}

func (s *localBlockSource) globalBalance(ctx context.Context, version uint32) (*globalBalanceShard, error) {
	index := 0
	if version < 12 {
		index = 1
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s.balanceMu.Lock()
		if parsed := s.balances[index]; parsed != nil {
			s.balanceMu.Unlock()
			return parsed, nil
		}
		if flight := s.balanceLoads[index]; flight != nil {
			s.balanceMu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-flight.done:
			}
			// A caller with a shorter deadline must not cancel another
			// validation that joined its read. Retry under the live context.
			if flight.canceled {
				continue
			}
			return flight.parsed, flight.err
		}
		flight := &globalBalanceLoad{done: make(chan struct{})}
		s.balanceLoads[index] = flight
		s.balanceMu.Unlock()

		flight.parsed, flight.err = parseGlobalBalanceShard(ctx, &s.state, blockShardIdent(s.previous.ID), version)
		flight.canceled = ctx.Err() != nil && errors.Is(flight.err, ctx.Err())
		s.balanceMu.Lock()
		if flight.err == nil {
			s.balances[index] = flight.parsed
		}
		s.balanceLoads[index] = nil
		close(flight.done)
		s.balanceMu.Unlock()
		return flight.parsed, flight.err
	}
}

func globalBalanceCreated(config *Config, previous, next *ShardRegistry) (*big.Int, error) {
	created := new(big.Int).Set(config.masterchain.createFee.NanoRef())
	workchains := make(map[int32]struct{})
	for key := range previous.leaves {
		workchains[key.workchain] = struct{}{}
	}
	for key, leaf := range next.leaves {
		depth, err := sharddomain.PrefixLength(key.shard)
		if err != nil {
			return nil, err
		}
		oldSeqno := uint32(0)
		if _, exists := workchains[key.workchain]; exists {
			old, same := previous.leaves[key]
			var parent, left, right shardRegistryLeaf
			var split, hasLeft, hasRight bool
			if !same && depth > 0 {
				parentID, err := sharddomain.Parent(key.shard)
				if err != nil {
					return nil, err
				}
				parent, split = previous.leaves[shardRegistryKey{key.workchain, parentID}]
			}
			if !same && !split {
				leftID, err := sharddomain.Child(key.shard, true)
				if err != nil {
					return nil, err
				}
				rightID, err := sharddomain.Child(key.shard, false)
				if err != nil {
					return nil, err
				}
				left, hasLeft = previous.leaves[shardRegistryKey{key.workchain, leftID}]
				right, hasRight = previous.leaves[shardRegistryKey{key.workchain, rightID}]
			}
			switch {
			case same:
				oldSeqno = old.top.Block.SeqNo
			case split:
				oldSeqno = parent.top.Block.SeqNo
			case hasLeft && hasRight:
				oldSeqno = max(left.top.Block.SeqNo, right.top.Block.SeqNo)
			default:
				return nil, fmt.Errorf("%w: invalid shard topology for global balance", ErrInvalidInput)
			}
		}
		if leaf.top.Block.SeqNo < oldSeqno {
			return nil, fmt.Errorf("%w: shard sequence number regressed in global balance", ErrInvalidInput)
		}
		fee := new(big.Int).Rsh(config.basechain.createFee.NanoRef(), uint(depth))
		fee.Mul(fee, new(big.Int).SetUint64(uint64(leaf.top.Block.SeqNo-oldSeqno)))
		created.Add(created, fee)
	}
	return created, nil
}

func nextGlobalBalanceGrams(previous *big.Int, created, imported, burned tlb.Coins) (tlb.Coins, error) {
	grams := new(big.Int).Add(previous, created.NanoRef())
	grams.Add(grams, imported.NanoRef())
	grams.Sub(grams, burned.NanoRef())
	if grams.Sign() < 0 || grams.BitLen() > 120 {
		return tlb.Coins{}, fmt.Errorf("%w: invalid resulting global balance", ErrInvalidInput)
	}
	return tlb.FromOwnedNanoTON(grams), nil
}
