package collator

import (
	"fmt"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// deferQueueDelete queues one out-queue removal for the next flush. The
// pending set is what keeps deferred semantics identical to immediate ones: a
// second dequeue of the same key must fail as absent, and an enqueue landing
// on a pending key must see the delete applied first.
func (c *collation) deferQueueDelete(key msgpool.QueueKey, keyCell *cell.Cell) {
	if c.queuePendingSet == nil {
		c.queuePendingSet = make(map[msgpool.QueueKey]struct{}, 64)
	}
	c.queuePendingSet[key] = struct{}{}
	c.queuePendingDelete = append(c.queuePendingDelete, keyCell)
}

func (c *collation) queueDeletePending(key msgpool.QueueKey) bool {
	_, pending := c.queuePendingSet[key]
	return pending
}

// flushQueueDeletes applies the pending removals in one descent. It runs
// before every read that must see them: the root samples the limiter takes on
// the queue-op cadence, the closing proof of the cleanup phase, and finish.
func (c *collation) flushQueueDeletes() error {
	if len(c.queuePendingDelete) == 0 {
		return nil
	}
	if err := c.outQueue.DeleteMany(c.queuePendingDelete); err != nil {
		return fmt.Errorf("%w: outbound queue deletes did not apply: %v", ErrInvalidInput, err)
	}
	c.queuePendingDelete = c.queuePendingDelete[:0]
	clear(c.queuePendingSet)
	return nil
}

// traceOutQueueValidationClosure retains every predecessor cell that the
// reference validator reads while checking the old/new OutMsgQueue diff. Queue
// mutations already authenticate their direct trie paths; the structural diff
// additionally opens Patricia alignment boundaries and new fork augmentations.
// The callback opens both changed EnqueuedMsg payloads through Message body
// roots, exactly as the generated C++ validators do.
func (c *collation) traceOutQueueValidationClosure() error {
	return c.oldOutQueue.ScanDiff(c.outQueue.AugmentedDictionary, true, func(
		keyCell *cell.Cell,
		oldValueExtra, newValueExtra *cell.Slice,
	) error {
		var key msgpool.QueueKey
		keyLoader, err := keyCell.BeginParse()
		if err != nil {
			return fmt.Errorf("%w: load outbound queue diff key: %v", ErrInvalidInput, err)
		}
		if err = keyLoader.LoadSliceInto(key[:], 352); err != nil {
			return fmt.Errorf("%w: decode outbound queue diff key: %v", ErrInvalidInput, err)
		}

		if oldValueExtra != nil {
			oldValue := oldValueExtra.Copy()
			if err = (tlb.AugOutMsgQueue{}).SkipExtra(oldValue); err != nil {
				return fmt.Errorf("%w: decode predecessor outbound queue augmentation %x: %v", ErrInvalidInput, key, err)
			}
			if _, err = parseQueueEntry(oldValue, key); err != nil {
				return fmt.Errorf("trace predecessor outbound queue entry: %w", err)
			}
		}
		if newValueExtra != nil {
			newValue := newValueExtra.Copy()
			if err = (tlb.AugOutMsgQueue{}).SkipExtra(newValue); err != nil {
				return fmt.Errorf("%w: decode candidate outbound queue augmentation %x: %v", ErrInvalidInput, key, err)
			}
			if _, err = parseQueueEntry(newValue, key); err != nil {
				return fmt.Errorf("trace candidate outbound queue entry: %w", err)
			}
		}

		return nil
	})
}
