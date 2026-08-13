package collator

import (
	"fmt"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

// traceValidationClosure replays the predecessor reads a validator makes but
// collation does not, so the collated proof carries them. It is
// Collator::prepare_proofs (collator.cpp:6205-6265), and like it, it runs after
// the block is built — see the call site for why that ordering is load-bearing
// rather than tidy.
func (c *collation) traceValidationClosure() error {
	// Gated exactly where create_collated_data gates prepare_proofs
	// (collator.cpp:6287-6296). Without a proof to widen there is nothing for
	// these reads to reach: collatedProofEstimate is only built under the
	// capability, so off it they record into nothing at all. The masterchain is
	// excluded for the same reason it emits no state proof.
	if !c.fullCollated || c.master != nil {
		return nil
	}
	if err := c.traceAccountValidationClosure(); err != nil {
		return err
	}
	if err := c.traceOutQueueValidationClosure(); err != nil {
		return err
	}
	if err := c.traceImmediateQueueValidationClosure(); err != nil {
		return err
	}

	return c.traceDispatchQueueValidationClosure()
}

// traceImmediateQueueValidationClosure retains the outbound-queue paths of
// messages that were delivered inside this block without ever being queued.
//
// The validator proves each of them absent from the predecessor queue and from
// the candidate one, exactly as the reference does (ValidateQuery::check_out_msg
// looks the key up in both at validate-query.cpp:4790-4791 and rejects a
// msg_export_imm found in either at :4823-4827). Collation has no reason to go
// near those paths: the message never entered a queue, so nothing in the diff
// covers them.
//
// The reference collator does not replay this one, which is why it has not bitten
// anyone yet — the destination of an immediately delivered message is inside our
// own shard, and the queue holds nothing routed there, so the lookup usually
// diverges within a couple of nodes near the root that every queue operation has
// already loaded. "Usually" is the problem: on a deeply split shard the queue is
// full of keys sharing a long prefix with our own, and the same lookup descends
// far enough to meet a boundary. Proving them costs a handful of cells and
// removes the class.
func (c *collation) traceImmediateQueueValidationClosure() error {
	for _, key := range c.immediateQueueKeys {
		keyCell := cell.BeginCell().MustStoreSlice(key[:], 352).EndCell()
		if _, err := c.oldOutQueue.LoadValue(keyCell); err != nil && !isMissingKey(err) {
			return fmt.Errorf("trace predecessor queue absence %x: %w", key, err)
		}
		// The candidate queue is walked too, because the validator walks it and
		// its untouched nodes are the predecessor's own cells: a key whose path
		// the block reshaped resolves through cells only this walk reaches.
		if _, err := c.outQueue.LoadValue(keyCell); err != nil && !isMissingKey(err) {
			return fmt.Errorf("trace candidate queue absence %x: %w", key, err)
		}
	}

	return nil
}

// traceAccountValidationClosure retains the structural predecessor reads made
// by the reference collator's ShardAccounts scan_diff(..., mode=2). Direct
// account lookups are not sufficient: Patricia labels may change around a
// modified range, and validating new fork augmentations reads sibling roots at
// every changed structural boundary.
func (c *collation) traceAccountValidationClosure() error {
	return c.oldState.Accounts.ShardAccounts.ScanDiff(
		c.accounts.AugmentedDictionary,
		true,
		func(_ *cell.Cell, _, _ *cell.Slice) error { return nil },
	)
}

// traceDispatchQueueValidationClosure retains the predecessor dispatch-queue
// reads the validator makes but collation itself never performs. It is the
// third and fourth parts of Collator::prepare_proofs (collator.cpp:6238-6264);
// the account and outbound-queue parts already have their own shims.
//
// Two reads are involved and only the first follows from the diff. Validating a
// changed AccountDispatchQueue opens the boundary of each side — the greatest
// key still in the predecessor queue and the smallest key left in the candidate
// one — which the collator's own removals do not necessarily touch.
//
// The second has no relation to the diff at all: before replaying an account's
// transactions, the validator looks that account up in the *predecessor*
// dispatch queue to decide whether all of its new messages must be deferred
// (validate-query.cpp:6295-6306). It does this for every account that has
// transactions in the block, including the overwhelming majority that have
// never deferred anything, and for those the lookup is an absence path. Without
// it in the closure, a validator running on proofs meets a pruned branch on an
// ordinary account that merely happens to sit next to a deferring one.
func (c *collation) traceDispatchQueueValidationClosure() error {
	err := c.oldDispatchQueue.ScanDiff(c.dispatchQueue.AugmentedDictionary, true, func(
		_ *cell.Cell,
		oldValueExtra, newValueExtra *cell.Slice,
	) error {
		if oldValueExtra != nil {
			account, parseErr := dispatchDiffAccount(oldValueExtra)
			if parseErr != nil {
				return fmt.Errorf("%w: decode predecessor dispatch account: %v", ErrInvalidInput, parseErr)
			}
			if _, _, parseErr = account.Messages.LoadMax(); parseErr != nil {
				return fmt.Errorf("%w: load predecessor dispatch account maximum: %v", ErrInvalidInput, parseErr)
			}
		}
		if newValueExtra != nil {
			account, parseErr := dispatchDiffAccount(newValueExtra)
			if parseErr != nil {
				return fmt.Errorf("%w: decode candidate dispatch account: %v", ErrInvalidInput, parseErr)
			}
			if _, _, parseErr = account.Messages.LoadMin(); parseErr != nil {
				return fmt.Errorf("%w: load candidate dispatch account minimum: %v", ErrInvalidInput, parseErr)
			}
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("trace dispatch queue difference: %w", err)
	}

	for _, key := range sortedAccountKeys(c.lanes) {
		if c.lanes[key].transactions == nil {
			continue
		}
		if _, err = loadAccountDispatchQueue(c.oldDispatchQueue, key); err != nil && !isMissingKey(err) {
			return fmt.Errorf("trace predecessor dispatch account %x: %w", key, err)
		}
	}

	return nil
}
