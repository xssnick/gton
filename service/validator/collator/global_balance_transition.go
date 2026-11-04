package collator

import (
	"context"
	"fmt"
	"math"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	sharddomain "github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/validator/msgpool"
)

// A calculation trusts only the explicitly supplied, authenticated previous
// masterchain snapshot. Every descendant is checked back to its exact top IDs,
// regardless of which parsed states happen to survive in the block cache.
// Queue copies live only during this traversal; no additional payload trees are
// retained by the balance cache.
type globalBalanceTransitions struct {
	calculator localGlobalBalanceCalculator
	version    uint32
	anchors    map[cell.Hash]ton.BlockIDExt
	floors     map[shardRegistryKey]uint32
	verified   map[cell.Hash]struct{}
}

func (c localGlobalBalanceCalculator) transitions(ctx context.Context, master PreviousBlock, version uint32) (*globalBalanceTransitions, error) {
	baseline, err := verifyPredecessor("global balance baseline", &c.baseline)
	if err != nil {
		return nil, err
	}
	if c.baseline.ID.Workchain != masterchainWorkchainID || c.baseline.ID.Shard != math.MinInt64 {
		return nil, fmt.Errorf("%w: global balance baseline is not masterchain", ErrInvalidInput)
	}
	if !master.ID.Equals(&c.baseline.ID) &&
		(c.baseline.ID.SeqNo == math.MaxUint32 || master.ID.SeqNo != c.baseline.ID.SeqNo+1) {
		return nil, fmt.Errorf("%w: global balance requires the immediate masterchain successor", ErrInvalidInput)
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	var extra tlb.McStateExtra
	if baseline.McStateExtra == nil {
		return nil, fmt.Errorf("%w: global balance baseline has no masterchain extra", ErrInvalidInput)
	}
	if err = parseExact(&extra, baseline.McStateExtra); err != nil {
		return nil, err
	}
	registry, err := ParseShardRegistry(extra.ShardHashes)
	if err != nil {
		return nil, err
	}
	result := &globalBalanceTransitions{
		calculator: c, version: version,
		anchors:  make(map[cell.Hash]ton.BlockIDExt, len(registry.leaves)+1),
		floors:   make(map[shardRegistryKey]uint32),
		verified: make(map[cell.Hash]struct{}),
	}
	if err = result.addAnchor(c.baseline.ID); err != nil {
		return nil, err
	}
	for _, leaf := range registry.leaves {
		if err = result.addAnchor(leaf.top.Block); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// Prefix floors cover both ancestor and descendant overlap in at most the
// shard depth, without scanning every registered shard for every new block.
func (v *globalBalanceTransitions) addAnchor(id ton.BlockIDExt) error {
	v.anchors[cell.Hash(id.RootHash)] = id
	for shard := id.Shard; ; {
		key := shardRegistryKey{id.Workchain, shard}
		v.floors[key] = max(v.floors[key], id.SeqNo)
		if shard == math.MinInt64 {
			return nil
		}
		var err error
		shard, err = sharddomain.Parent(shard)
		if err != nil {
			return err
		}
	}
}

func (v *globalBalanceTransitions) verify(ctx context.Context, source *localBlockSource) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	id := source.previous.ID
	if _, exists := v.verified[cell.Hash(id.RootHash)]; exists {
		return nil
	}
	if anchor, exists := v.anchors[cell.Hash(id.RootHash)]; exists && id.Equals(&anchor) {
		return nil
	}
	for shard := id.Shard; ; {
		if floor, exists := v.floors[shardRegistryKey{id.Workchain, shard}]; exists {
			if id.SeqNo <= floor {
				return fmt.Errorf("%w: global balance predecessor does not reach the authenticated shard top", ErrInvalidInput)
			}
			break
		}
		if shard == math.MinInt64 {
			break
		}
		var err error
		shard, err = sharddomain.Parent(shard)
		if err != nil {
			return err
		}
	}
	// A newly activated workchain starts at its authenticated zerostate. It has
	// no transition, just like a shard in the initial masterchain snapshot.
	if id.SeqNo == 0 {
		v.verified[cell.Hash(id.RootHash)] = struct{}{}
		return nil
	}
	if source.previous.Block == nil {
		return fmt.Errorf("%w: global balance transition block is absent", ErrAcquisitionNotReady)
	}
	var block tlb.Block
	if err := parseExact(&block, source.previous.Block); err != nil {
		return err
	}
	parents, err := globalBalancePreviousIDs(id, block.BlockInfo)
	if err != nil {
		return err
	}
	previous := make([]*localBlockSource, len(parents))
	for i, parent := range parents {
		if parent.Equals(&v.calculator.baseline.ID) {
			state, err := verifyPredecessor("global balance baseline", &v.calculator.baseline)
			if err != nil {
				return err
			}
			previous[i] = &localBlockSource{previous: v.calculator.baseline, state: state}
		} else {
			previous[i], err = v.calculator.acquisition.blockSource(ctx, parent, v.calculator.mode)
			if err != nil {
				return fmt.Errorf("acquire global balance predecessor %v: %w", parent, err)
			}
		}
		if err = v.verify(ctx, previous[i]); err != nil {
			return err
		}
	}
	if err = verifyGlobalBalanceTransition(ctx, source, previous, &block, v.version); err != nil {
		return fmt.Errorf("global balance transition %v: %w", id, err)
	}
	v.verified[cell.Hash(id.RootHash)] = struct{}{}
	return nil
}

func globalBalancePreviousIDs(id ton.BlockIDExt, info tlb.BlockHeader) ([]ton.BlockIDExt, error) {
	if info.Version != 0 || info.NotMaster != (id.Workchain != masterchainWorkchainID) ||
		info.PrevRef.Pruned || info.AfterSplit && info.AfterMerge || info.AfterMerge != (info.PrevRef.Prev2 != nil) {
		return nil, fmt.Errorf("%w: invalid global balance predecessor topology", ErrInvalidInput)
	}
	shards := []int64{id.Shard}
	var err error
	if info.AfterSplit {
		shards[0], err = sharddomain.Parent(id.Shard)
	} else if info.AfterMerge {
		shards[0], err = sharddomain.Child(id.Shard, true)
		if err == nil {
			var right int64
			right, err = sharddomain.Child(id.Shard, false)
			shards = append(shards, right)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("%w: invalid predecessor shard: %v", ErrInvalidInput, err)
	}
	refs := []tlb.ExtBlkRef{info.PrevRef.Prev1}
	if info.PrevRef.Prev2 != nil {
		refs = append(refs, *info.PrevRef.Prev2)
	}
	result := make([]ton.BlockIDExt, len(refs))
	var maxSeqno uint32
	for i, ref := range refs {
		if len(ref.RootHash) != 32 || len(ref.FileHash) != 32 || ref.SeqNo >= id.SeqNo ||
			ref.SeqNo == 0 && (info.AfterSplit || info.AfterMerge) {
			return nil, fmt.Errorf("%w: invalid global balance predecessor reference", ErrInvalidInput)
		}
		maxSeqno = max(maxSeqno, ref.SeqNo)
		result[i] = ton.BlockIDExt{Workchain: id.Workchain, Shard: shards[i], SeqNo: ref.SeqNo, RootHash: ref.RootHash, FileHash: ref.FileHash}
	}
	if id.SeqNo == 0 || maxSeqno != id.SeqNo-1 {
		return nil, fmt.Errorf("%w: global balance predecessor sequence gap", ErrInvalidInput)
	}
	return result, nil
}

func verifyGlobalBalanceTransition(ctx context.Context, next *localBlockSource, previous []*localBlockSource, block *tlb.Block, version uint32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var target tlb.OutMsgQueueInfo
	if err := parseExact(&target, next.state.OutMsgQueueInfo); err != nil {
		return err
	}
	queues := make([]tlb.OutMsgQueueInfo, len(previous))
	allStored := true
	for i, source := range previous {
		if err := parseExact(&queues[i], source.state.OutMsgQueueInfo); err != nil {
			return err
		}
		stored, err := globalDispatchBalanceStored(&queues[i])
		if err != nil {
			return err
		}
		allStored = allStored && stored
	}
	stored, err := globalDispatchBalanceStored(&target)
	if err != nil {
		return err
	}
	if version >= 16 && allStored && !stored {
		return fmt.Errorf("%w: dispatch queue lost its stored total balance", ErrInvalidInput)
	}
	queue := &tlb.OutMsgQueueAugDict{AugmentedDictionary: queues[0].OutQueue.Copy()}
	if len(previous) == 2 {
		if ok, err := queue.CombineWith(queues[1].OutQueue.AugmentedDictionary); err != nil || !ok {
			return fmt.Errorf("%w: merge global balance queues: %v", ErrInvalidInput, err)
		}
	} else if previous[0].previous.ID.Shard != next.previous.ID.Shard {
		owner, target := blockShardIdent(previous[0].previous.ID), blockShardIdent(next.previous.ID)
		_, err = queue.Filter(func(value, _ *cell.Slice, key *cell.Cell) (cell.DictFilterAction, error) {
			if err := ctx.Err(); err != nil {
				return cell.DictFilterKeep, err
			}
			current, err := queuedCurrentPrefix(value, key)
			if err != nil {
				return cell.DictFilterKeep, err
			}
			if !owner.Contains(current.Workchain, current.Prefix) {
				return cell.DictFilterKeep, fmt.Errorf("%w: outbound message current prefix outside split parent", ErrInvalidInput)
			}
			if target.Contains(current.Workchain, current.Prefix) {
				return cell.DictFilterKeep, nil
			}
			return cell.DictFilterRemove, nil
		})
		if err != nil {
			return err
		}
	}
	var loader cell.Slice
	if err = block.Extra.OutMsgDesc.BeginParseInto(&loader); err != nil {
		return err
	}
	descriptors, err := tlb.LoadOutMsgDescrAugDict(&loader, version)
	if err != nil {
		return err
	}
	iter, err := descriptors.IteratorExtra(false, false)
	if err != nil {
		return err
	}
	for iter.Next() {
		if err = ctx.Err(); err != nil {
			return err
		}
		item := iter.View()
		hash, err := semanticDescriptorKey(&item.Key)
		if err != nil {
			return err
		}
		if err = applyGlobalBalanceQueueDescriptor(queue, item.Value, hash, block.BlockInfo.StartLt, version); err != nil {
			return err
		}
	}
	if err = iter.Err(); err != nil {
		return err
	}
	actual, err := queue.ToCell()
	if err != nil {
		return err
	}
	expected, err := target.OutQueue.ToCell()
	if err != nil {
		return err
	}
	if !equalCell(actual, expected) {
		return fmt.Errorf("%w: outbound queue differs from predecessor and OutMsgDescr delta", ErrInvalidInput)
	}
	return nil
}

func globalDispatchBalanceStored(queue *tlb.OutMsgQueueInfo) (bool, error) {
	if queue.Extra == nil || queue.Extra.DispatchQueue == nil || queue.Extra.DispatchQueue.IsEmpty() {
		return true, nil
	}
	var extra tlb.DispatchQueueAugData
	if err := parseExact(&extra, queue.Extra.DispatchQueue.GetRootExtra()); err != nil {
		return false, err
	}
	return extra.TotalBalance != nil, nil
}

// Only constructors that change OutMsgQueue are opened, exactly as in the
// reference calculator. Message StateInit, bodies and transaction cells remain
// unopened. They are committed by the reconstructed queue's full root hash.
func applyGlobalBalanceQueueDescriptor(queue *tlb.OutMsgQueueAugDict, value cell.Slice, hash [32]byte, startLT uint64, version uint32) error {
	tag, err := value.LoadUInt(3)
	if err != nil {
		return err
	}
	if tag == 0b101 {
		suffix, err := value.LoadUInt(2)
		if err != nil || suffix > 1 {
			return fmt.Errorf("%w: invalid deferred outbound constructor", ErrInvalidInput)
		}
		tag = semanticOutNewDeferred + suffix
	} else if tag == 0b110 {
		suffix, err := value.LoadUInt(1)
		if err != nil {
			return err
		}
		tag = semanticOutDequeue + suffix
	}
	if tag == semanticOutDequeueShort {
		if err = value.SkipBits(256); err != nil {
			return err
		}
		wc, err := value.LoadInt(32)
		if err != nil {
			return err
		}
		prefix, err := value.LoadUInt(64)
		if err != nil {
			return err
		}
		if _, err = value.LoadUInt(64); err != nil {
			return err
		}
		if err = requireEmptyDescriptor(&value); err != nil {
			return err
		}
		key := msgpool.MakeQueueKey(msgpool.AccountPrefix{Workchain: int32(wc), Prefix: prefix}, hash)
		var removed cell.Slice
		return queue.LoadValueAndDeleteByBytesKeyInto(key[:], &removed)
	}
	switch tag {
	case semanticOutNew, semanticOutTransit, semanticOutTransitRequest, semanticOutDeferredTransit,
		semanticOutDequeue, semanticOutDequeueImmediate:
	default:
		return nil
	}
	envelope, err := value.LoadRefCell()
	if err != nil {
		return err
	}
	var reimport *cell.Cell
	if tag == semanticOutDequeue {
		_, err = value.LoadUInt(63)
	} else {
		reimport, err = value.LoadRefCell()
	}
	if err != nil {
		return err
	}
	if err = requireEmptyDescriptor(&value); err != nil {
		return err
	}
	entry := cell.BeginCell().MustStoreUInt(startLT, 64).MustStoreRef(envelope)
	var slice cell.Slice
	if err = entry.EndCell().BeginParseInto(&slice); err != nil {
		return err
	}
	message, key, err := parseGlobalBalanceMessage(&slice, version, false)
	if err != nil {
		return err
	}
	if message.descr.Hash != hash {
		return fmt.Errorf("%w: global balance outbound descriptor message hash mismatch", ErrInvalidInput)
	}
	if tag == semanticOutTransitRequest {
		var inbound cell.Slice
		if err = reimport.BeginParseInto(&inbound); err != nil {
			return err
		}
		inTag, err := inbound.LoadUInt(3)
		if err != nil || inTag != semanticInTransit {
			return fmt.Errorf("%w: global balance requeue has no transit import", ErrInvalidInput)
		}
		old, _, err := semanticLoadTwoRefs(&inbound)
		if err != nil {
			return err
		}
		if _, err = semanticLoadCoins(&inbound); err != nil {
			return err
		}
		if err = requireEmptyDescriptor(&inbound); err != nil {
			return err
		}
		var oldEntry cell.Slice
		if err = cell.BeginCell().MustStoreUInt(0, 64).MustStoreRef(old).EndCell().BeginParseInto(&oldEntry); err != nil {
			return err
		}
		oldMessage, oldKey, err := parseGlobalBalanceMessage(&oldEntry, version, false)
		if err != nil {
			return err
		}
		if oldMessage.descr.Hash != hash {
			return fmt.Errorf("%w: global balance requeue message hash mismatch", ErrInvalidInput)
		}
		var removed cell.Slice
		if err = queue.LoadValueAndDeleteByBytesKeyInto(oldKey[:], &removed); err != nil {
			return err
		}
	}
	if tag == semanticOutDequeue || tag == semanticOutDequeueImmediate {
		var removed cell.Slice
		return queue.LoadValueAndDeleteByBytesKeyInto(key[:], &removed)
	}
	if tag == semanticOutNew || tag == semanticOutDeferredTransit {
		entry = cell.BeginCell().MustStoreUInt(message.descr.LT, 64).MustStoreRef(envelope)
	}
	added, err := queue.SetBuilderByBytesKeyWithMode(key[:], entry, cell.DictSetModeAdd)
	if err != nil {
		return err
	}
	if !added {
		return fmt.Errorf("%w: global balance outbound descriptor adds an existing message", ErrInvalidInput)
	}
	return nil
}
