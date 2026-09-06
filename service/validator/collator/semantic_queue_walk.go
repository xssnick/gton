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
	return walkSemanticQueuePrefixLeaves(queue, target, bound,
		func(key msgpool.QueueKey, value, extra *cell.Slice, leaf semanticQueueLeafCells) error {
			entry, parseErr := parseSemanticNeighborQueueEntryLoaded(key, value, extra, leaf)
			if parseErr != nil {
				return semanticQueueEntryVerdict{err: parseErr}
			}
			if visit != nil {
				return visit(entry)
			}
			return nil
		})
}

// walkSemanticQueuePrefixLeaves is the traversal half of walkSemanticQueuePrefix,
// shared with the claimed-prefix cleanup's narrower reader (walkClaimedQueuePrefix).
// The descent — which nodes are opened, in which order, which leaves are
// materialised and which failures stop the walk — has to be single-source: the
// cells cleanup's walk touches are handed to the validation closure as the
// validator's own read set, so two independently maintained descents could
// drift apart in exactly the way that ships a short proof. Only the leaf PARSE
// may differ between callers, and the parity tests over claimedPrefixCells pin
// that the narrow parse opens the same cells the full one does.
func walkSemanticQueuePrefixLeaves(
	queue *tlb.OutMsgQueueAugDict,
	target msgpool.ShardIdent,
	bound semanticMessageBound,
	visit func(key msgpool.QueueKey, value, extra *cell.Slice, leaf semanticQueueLeafCells) error,
) error {
	targetPrefix, err := semanticQueueTargetPrefix(target)
	if err != nil {
		return err
	}
	return queue.TraverseExtraBorrowed(func(keyPrefix, extra, value *cell.Slice) (int, error) {
		direction, prefixErr := semanticQueuePrefixDirectionBorrowed(keyPrefix, targetPrefix)
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
		var minimum cell.Slice
		extra.CopyInto(&minimum)
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

		var keyLoader cell.Slice
		keyPrefix.CopyInto(&keyLoader)
		var key msgpool.QueueKey
		if keyLoader.BitsLeft() != 352 {
			return 0, fmt.Errorf("outbound queue leaf key has %d bits", keyLoader.BitsLeft())
		}
		if loadErr = keyLoader.LoadSliceInto(key[:], 352); loadErr != nil {
			return 0, loadErr
		}
		entryBound := semanticMessageBound{lt: minimumLT}
		copy(entryBound.hash[:], key[12:])
		// C++ loads every leaf node in the last equal-LT batch, but only unpacks
		// EnqueuedMsg payloads through the claimed message hash.
		if !entryBound.lessEqual(bound) {
			return 0, nil
		}
		// The load and the parse are asked separately, and in that order,
		// because only one of the two answers may ever be swallowed by the
		// collator's proof-closure replay. See materialiseSemanticQueueLeaf.
		leaf, loadErr := materialiseSemanticQueueLeaf(value)
		if loadErr != nil {
			return 0, loadErr
		}
		if visitErr := visit(key, value, extra, leaf); visitErr != nil {
			return 0, visitErr
		}

		return 0, nil
	})
}

// semanticQueueLeafCells are the cells BELOW a queue leaf that the entry parse
// dereferences: the message envelope, which is the leaf value's only reference,
// and the message, which is the envelope's only reference. Nothing further down
// is opened — the neighbour parse stops after created_at, and the extra-currency
// dictionary, the StateInit roots and the body are taken as references and never
// followed (LoadDict takes the root cell without walking it) — so these two are
// the complete set.
type semanticQueueLeafCells struct {
	envelope *cell.Cell
	message  *cell.Cell
}

// materialiseSemanticQueueLeaf takes those two cells out of storage BEFORE the
// entry is parsed, and exists so that a storage fault and a verdict on the
// entry's content cannot arrive as the same error.
//
// They used to be indistinguishable. Both now stop collation, but preserving the
// distinction keeps the returned cause actionable: a content verdict identifies
// a candidate every validator would reject, while a load failure identifies the
// local storage/read path that prevented proof closure. Until this split, a lazy
// load raised strictly below the leaf surfaced from inside the parse because
// tonutils lazy loaders return the storage error unchanged and there is no I/O
// sentinel to inspect afterwards.
//
// The split is by CALL, not by error inspection. Cell.Prewarm does one thing —
// resolve a lazy reference — and every error it can return comes from the
// loader or from validating what the loader returned
// (ErrLazyLoaderNotSet, ErrLazyRefNotFound, ErrLazyRefMismatch, or the store's
// own error). Those are returned bare. Taking the reference itself is a separate
// step on cells already in hand: a leaf value with no reference, or an envelope
// with none, is malformed content that the validator's own parse rejects in the
// same place, so those are marked as verdicts here exactly as the parse would
// have marked them.
//
// It adds no I/O. Prewarm performs the load the parse would have performed on
// first touch, the loaded cells are then handed to the parse, and a
// non-lazy cell — every resident state, every proof-backed tree — returns from
// Prewarm untouched. Loader calls per leaf are the same two as before; see
// TestProcessedQueueTraceAddsNoLoadsOnEitherParentShape.
func materialiseSemanticQueueLeaf(value *cell.Slice) (semanticQueueLeafCells, error) {
	var view cell.Slice
	value.CopyInto(&view)

	envelope, err := view.PeekRefCell()
	if err != nil {
		return semanticQueueLeafCells{}, semanticQueueEntryVerdict{err: fmt.Errorf(
			"%w: outbound queue entry carries no envelope reference: %v", ErrInvalidInput, err)}
	}
	if envelope, err = envelope.Prewarm(); err != nil {
		return semanticQueueLeafCells{}, fmt.Errorf("load outbound queue envelope: %w", err)
	}
	var envelopeSlice cell.Slice
	if err = envelope.BeginParseInto(&envelopeSlice); err != nil {
		return semanticQueueLeafCells{}, fmt.Errorf("open outbound queue envelope: %w", err)
	}
	// The message is reference 0 of both envelope versions: neither emitted_lt
	// nor the metadata that may follow the v2 tag carries a reference.
	message, err := envelopeSlice.PeekRefCell()
	if err != nil {
		return semanticQueueLeafCells{}, semanticQueueEntryVerdict{err: fmt.Errorf(
			"%w: queued envelope carries no message reference: %v", ErrInvalidInput, err)}
	}
	if message, err = message.Prewarm(); err != nil {
		return semanticQueueLeafCells{}, fmt.Errorf("load queued message: %w", err)
	}

	return semanticQueueLeafCells{envelope: envelope, message: message}, nil
}

// semanticQueueEntryVerdict marks the failures walkSemanticQueuePrefix raises
// about the CONTENT of a leaf it already holds, as opposed to the many it
// raises about reaching one. There are exactly two sources, and both run on
// cells that are in hand by then: the header parse of an entry inside the
// bound, and the two structural checks materialiseSemanticQueueLeaf makes
// before it — a leaf value with no envelope reference, an envelope with no
// message reference — which the parse itself would have rejected in the same
// place had it got that far.
//
// It exists so callers and diagnostics can tell content from traversal/storage
// failures without parsing error text. Proof closure returns either class as an
// error: a verdict means the candidate's acceptance result is already known to
// be rejection, while any other failure means this node could not complete the
// validator's read set.
//
// Deliberately transparent. Error() is the wrapped message verbatim and
// Unwrap exposes the cause, so every existing errors.Is/As test and every
// error string on the validator's own path through this walk
// (verifySourceProcessed) is unchanged; the type is only ever consulted by
// errors.As.
type semanticQueueEntryVerdict struct{ err error }

func (e semanticQueueEntryVerdict) Error() string { return e.err.Error() }

func (e semanticQueueEntryVerdict) Unwrap() error { return e.err }

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

func semanticQueuePrefixDirectionBorrowed(current *cell.Slice, target *cell.Cell) (int, error) {
	currentSize := current.BitsLeft()
	targetSize := target.BitsSize()
	common := min(currentSize, targetSize)
	var currentLoader, targetLoader cell.Slice
	current.CopyInto(&currentLoader)
	if err := target.BeginParseInto(&targetLoader); err != nil {
		return 0, err
	}
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
	if currentSize >= targetSize {
		return 6, nil
	}
	next, err := targetLoader.LoadUInt(1)
	if err != nil {
		return 0, err
	}

	return int(next) + 1, nil
}

// drainedQueueScanBudget caps the queue entries a drained claim may walk per
// neighbour before the claim is abandoned. It is deliberately small: the claim
// is only worth making when the prefix under it is nearly empty, which is the
// case where the reference's own merger would have reached the end of the
// queues and made the same claim for free. A deeper prefix means there was work
// to import, and importing it advances the bound without a special claim.
const drainedQueueScanBudget = 64
