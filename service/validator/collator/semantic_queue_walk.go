package collator

import (
	"fmt"
	"math/bits"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// walkSemanticQueuePrefix follows the same non-mutating augmented-tree walk as
// C++ OutputQueueMerger. The target-prefix path is opened first; below it a
// branch is opened only when its minimum logical time can still precede bound,
// and equal-LT leaves are compared by message hash. Apart from matching the
// wire proof boundary, walking once avoids rebuilding a partial augmented
// dictionary after every leaf: deleting from such a proof can promote a
// legitimate pruned sibling to the root and make the next scan fail.
func walkSemanticQueuePrefix(
	queue *tlb.OutMsgQueueAugDict,
	target msgpool.ShardIdent,
	bound semanticMessageBound,
	visit func(semanticQueueEntry) error,
) error {
	targetPrefix, err := semanticQueueTargetPrefix(target)
	if err != nil {
		return err
	}
	_, _, err = queue.TraverseExtra(func(keyPrefix *cell.Cell, extra, value *cell.Slice) (int, error) {
		direction, prefixErr := semanticQueuePrefixDirection(keyPrefix, targetPrefix)
		if prefixErr != nil || direction == 0 {
			return 0, prefixErr
		}
		// OutputQueueMerger narrows every source to the destination shard before
		// consulting its minimum LT. Following just that child is both necessary
		// for C++ proof parity and what leaves the off-prefix sibling pruned.
		if direction != 6 {
			if value != nil {
				return 0, fmt.Errorf("outbound queue leaf ends before the target prefix")
			}
			return direction, nil
		}
		minimum := extra.Copy()
		minimumLT, loadErr := minimum.LoadUInt(64)
		if loadErr != nil || minimum.BitsLeft() != 0 || minimum.RefsNum() != 0 {
			return 0, fmt.Errorf("invalid outbound queue augmentation")
		}
		if minimumLT > bound.lt {
			return 0, nil
		}
		if value == nil {
			return 6, nil
		}
		entryBound, boundErr := semanticQueueLeafBound(keyPrefix, minimumLT)
		if boundErr != nil {
			return 0, boundErr
		}
		// C++ loads every leaf node in the last equal-LT heap batch, but only
		// unpacks EnqueuedMsg payloads through the claimed message hash.
		if !entryBound.lessEqual(bound) {
			return 0, nil
		}
		entry, parseErr := parseSemanticNeighborQueueEntry(keyPrefix, value, extra)
		if parseErr != nil {
			return 0, parseErr
		}
		if visit != nil {
			if visitErr := visit(entry); visitErr != nil {
				return 0, visitErr
			}
		}

		return 0, nil
	})

	return err
}

func semanticQueueLeafBound(key *cell.Cell, lt uint64) (semanticMessageBound, error) {
	if key.BitsSize() != 352 {
		return semanticMessageBound{}, fmt.Errorf("outbound queue leaf key has %d bits", key.BitsSize())
	}
	loader, err := key.BeginParse()
	if err != nil {
		return semanticMessageBound{}, err
	}
	if err = loader.SkipBits(96); err != nil {
		return semanticMessageBound{}, err
	}
	var hash [32]byte
	if err = loader.LoadSliceInto(hash[:], 256); err != nil {
		return semanticMessageBound{}, err
	}

	return semanticMessageBound{lt: lt, hash: hash}, nil
}

func semanticQueueTargetPrefix(target msgpool.ShardIdent) (*cell.Cell, error) {
	if target.Shard == 0 {
		return nil, fmt.Errorf("target shard is zero")
	}
	depth := 63 - bits.TrailingZeros64(target.Shard)
	builder := cell.BeginCell().MustStoreInt(int64(target.Workchain), 32)
	if depth != 0 {
		builder.MustStoreUInt(target.Shard>>(64-uint(depth)), uint(depth))
	}
	return builder.EndCell(), nil
}

// semanticQueuePrefixDirection maps the current Patricia prefix to a
// TraverseExtra directive. Before the target prefix is reached only its next
// child is opened; once reached, both children are eligible for the LT walk.
func semanticQueuePrefixDirection(current, target *cell.Cell) (int, error) {
	common := min(current.BitsSize(), target.BitsSize())
	currentLoader := current.MustBeginParse()
	targetLoader := target.MustBeginParse()
	for common > 0 {
		chunk := min(common, uint(64))
		currentBits, err := currentLoader.LoadUInt(chunk)
		if err != nil {
			return 0, err
		}
		targetBits, err := targetLoader.LoadUInt(chunk)
		if err != nil {
			return 0, err
		}
		if currentBits != targetBits {
			return 0, nil
		}
		common -= chunk
	}
	if current.BitsSize() >= target.BitsSize() {
		return 6, nil
	}
	next, err := targetLoader.LoadUInt(1)
	if err != nil {
		return 0, err
	}

	return int(next) + 1, nil
}
