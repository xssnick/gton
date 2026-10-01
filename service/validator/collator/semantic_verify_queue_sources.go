package collator

import (
	"fmt"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	sharddomain "github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/validator/msgpool"
)

// loadQueueSources builds the inbound-queue view of this block: one source per
// foreign neighbor plus the predecessor queue this block actually starts from.
// Every neighbor whose shard intersects ours is withheld and replaced by that
// predecessor view, and a still-registered ancestor additionally contributes
// the half of its queue that stayed with our sibling. This is the same
// trivial-neighbor normalization validation performs; without it the
// ancestor (after a split) or the merged children (after a merge) expose the
// same queued message a second time, and the duplicate is indistinguishable
// from a candidate importing one message twice.
func (v *semanticQueueValidation) loadQueueSources() error {
	registered := v.replay.transition.Neighbors
	parsed := make([]tlb.OutMsgQueueInfo, len(registered))
	for i := range registered {
		neighbor := &registered[i]
		queueRoot, err := v.authenticatedNeighborQueue(neighbor)
		if err != nil {
			return fmt.Errorf("neighbor %d: %w", i, err)
		}
		parsed[i], err = parseNeighborQueueInfo(queueRoot)
		if err != nil {
			return fmt.Errorf("decode neighbor %d outbound queue: %w", i, err)
		}
		records, err := tlb.LoadProcessedUptoRecords(parsed[i].ProcInfo, neighbor.Shard.Shard)
		if err != nil {
			return fmt.Errorf("%w: decode neighbor %d processed info: %v", ErrInvalidInput, i, err)
		}
		if !equalProcessedRecords(records, neighbor.Processed) {
			return fmt.Errorf("%w: neighbor %d processed info differs from its authenticated queue", ErrInvalidInput, i)
		}
	}

	block := cloneBlockID(v.replay.transition.Previous.ID)
	block.Shard = int64(v.target.Shard)
	entries, err := normalizeTrivialNeighbors(registered, v.target, Neighbor{
		Block:     block,
		Shard:     v.target,
		EndLT:     v.replay.previous.GenLT,
		Processed: v.oldRecords,
	}, v.replay.candidate.block.BlockInfo.AfterMerge)
	if err != nil {
		return err
	}

	v.sources = make([]semanticQueueSource, 0, len(entries))
	for i := range entries {
		entry := &entries[i]
		source := semanticQueueSource{neighbor: &entry.Neighbor, owner: entry.Shard}
		switch {
		case entry.origin < 0:
			source.queue = v.old
		case entry.Shard == registered[entry.origin].Shard:
			source.queue = parsed[entry.origin]
		default:
			queue, cutErr := siblingQueueCut(
				parsed[entry.origin].OutQueue,
				registered[entry.origin].Shard,
				v.target,
				entry.Shard,
			)
			if cutErr != nil {
				return cutErr
			}
			source.queue = tlb.OutMsgQueueInfo{OutQueue: queue}
		}
		v.sources = append(v.sources, source)
	}

	return nil
}

// siblingQueueCut carves out the half of a still-registered ancestor queue that
// stayed with our sibling when the ancestor split. Our own half is the
// predecessor queue, so only the sibling's messages may remain visible through
// the ancestor.
func siblingQueueCut(
	queue *tlb.OutMsgQueueAugDict,
	ancestor, target, sibling msgpool.ShardIdent,
) (*tlb.OutMsgQueueAugDict, error) {
	ancestorIdent, err := semanticShardIdent(ancestor)
	if err != nil {
		return nil, err
	}
	siblingIdent, err := semanticShardIdent(sibling)
	if err != nil {
		return nil, err
	}

	cut := &tlb.OutMsgQueueAugDict{AugmentedDictionary: queue.Copy()}
	prefix, err := outQueueShardPrefix(target)
	if err != nil {
		return nil, err
	}
	if ok, cutErr := cut.CutPrefixSubdict(prefix, false); cutErr != nil || !ok {
		return nil, fmt.Errorf("%w: cut virtual sibling outbound queue by target prefix", ErrInvalidInput)
	}
	if _, err = filterOutQueue(cut, ancestorIdent, siblingIdent); err != nil {
		return nil, err
	}

	return cut, nil
}

func outQueueShardPrefix(shard msgpool.ShardIdent) (*cell.Cell, error) {
	prefixBits, err := sharddomain.PrefixLength(int64(shard.Shard))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid shard %016x: %v", ErrInvalidInput, shard.Shard, err)
	}

	prefix := cell.BeginCell().MustStoreInt(int64(shard.Workchain), 32)
	if prefixBits != 0 {
		prefix.MustStoreUInt(shard.Shard>>(64-prefixBits), uint(prefixBits))
	}
	return prefix.EndCell(), nil
}

func semanticShardIdent(id msgpool.ShardIdent) (tlb.ShardIdent, error) {
	prefixBits, err := sharddomain.PrefixLength(int64(id.Shard))
	if err != nil {
		return tlb.ShardIdent{}, fmt.Errorf("%w: invalid shard %016x: %v", ErrInvalidInput, id.Shard, err)
	}

	return tlb.ShardIdent{
		PrefixBits:  int8(prefixBits),
		WorkchainID: id.Workchain,
		ShardPrefix: id.Shard &^ (uint64(1) << (63 - prefixBits)),
	}, nil
}

func (v *semanticQueueValidation) authenticatedNeighborQueue(neighbor *Neighbor) (*cell.Cell, error) {
	queueRoot := neighbor.OutMsgQueueInfo
	if v.replay.transition.Masterchain != nil &&
		neighbor.Block.Equals(&v.replay.transition.Masterchain.ID) {
		masterQueue := v.replay.transition.Masterchain.OutMsgQueueInfo
		if masterQueue == nil {
			return nil, fmt.Errorf("%w: masterchain context has no outbound queue", ErrInvalidInput)
		}
		if queueRoot != nil && !equalCell(queueRoot, masterQueue) {
			return nil, fmt.Errorf("%w: neighbor queue differs from masterchain context", ErrInvalidInput)
		}
		queueRoot = masterQueue
	}
	if !v.replay.candidate.block.BlockInfo.NotMaster && neighbor.Block.Equals(&v.replay.transition.Previous.ID) {
		previousQueue := v.replay.previous.OutMsgQueueInfo
		if queueRoot != nil && !equalCell(queueRoot, previousQueue) {
			return nil, fmt.Errorf("%w: masterchain neighbor queue differs from predecessor", ErrInvalidInput)
		}
		queueRoot = previousQueue
	}
	if queueRoot == nil {
		return nil, fmt.Errorf("%w: authenticated outbound queue is absent", ErrInvalidInput)
	}

	if v.replay.transition.FullCollatedData && neighbor.Block.Workchain != address.MasterchainID {
		stateRoot, err := v.replay.transition.CollatedProofs.StateRoot(neighbor.Block)
		if err != nil {
			return nil, fmt.Errorf("%w: load collated neighbor state: %v", ErrInvalidInput, err)
		}
		var state tlb.ShardStateUnsplit
		if err = parseProofExact(&state, stateRoot); err != nil {
			return nil, fmt.Errorf("%w: decode collated neighbor state: %v", ErrInvalidInput, err)
		}
		if !equalCell(queueRoot, state.OutMsgQueueInfo) {
			return nil, fmt.Errorf("%w: neighbor queue differs from collated state", ErrInvalidInput)
		}
	}

	return queueRoot, nil
}
