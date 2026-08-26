package collator

import (
	"fmt"
	"sync"
	"time"

	"github.com/xssnick/tonutils-go/tlb"
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
	if !c.closureRecordsPredecessorReads() {
		return nil
	}

	// Spelled as calls rather than method values on purpose: the composition
	// is pinned by an AST guard that reads the call sites, and it should stay
	// readable as five calls.
	tasks := [...]struct {
		name string
		run  func() error
	}{
		{"accounts", func() error { return c.traceAccountValidationClosure() }},
		{"out_queue", func() error { return c.traceOutQueueValidationClosure() }},
		{"immediate_queue", func() error { return c.traceImmediateQueueValidationClosure() }},
		{"processed_queue", func() error { return c.traceProcessedQueueValidationClosure() }},
		{"dispatch_queue", func() error { return c.traceDispatchQueueValidationClosure() }},
	}
	var errs [len(tasks)]error
	// The stage is the slowest of the five, so the stage timer alone cannot
	// say which one to shorten. Each task's own span is recorded here — on the
	// worker, into its own slot — and merged into the build's durations once
	// they have all joined.
	var spans [len(tasks)]time.Duration
	var wait sync.WaitGroup
	for i, task := range tasks {
		wait.Go(func() {
			started := time.Now()
			errs[i] = task.run()
			spans[i] = time.Since(started)
		})
	}
	wait.Wait()
	for i, task := range tasks {
		c.req.assembly.substage(CollationStageValidationClosure, task.name, spans[i])
	}

	// Keep the old deterministic error priority even though the work is now
	// concurrent. A later failure must not hide the account or queue error the
	// sequential implementation returned first.
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// closureRecordsPredecessorReads is the gate above, asked separately because an
// earlier phase has to know the same answer: cleanupClaimedLocalDequeues keeps
// its parsed cells for the closure only when there is a closure to keep them
// for. The two must never disagree, or the collection is either dead weight or
// silently absent.
func (c *collation) closureRecordsPredecessorReads() bool {
	return c.fullCollated && c.master == nil
}

// traceProcessedQueueValidationClosure retains the predecessor outbound-queue
// cells a validator reads while checking our ProcessedInfo update, and it is the
// one queue source with no other reader on this side.
//
// verifySourceProcessed scans every inbound-queue source with
// walkSemanticQueuePrefix, and loadQueueSources always contributes the local
// predecessor queue as one of them. For every OTHER source that walk is made on
// the produce side too — traceInternalCut runs the identical function over each
// registered neighbour view — but the predecessor view is deliberately untraced
// (see loadExpectedNeighbors), because its state travels in the candidate's own
// predecessor proof rather than as a neighbour proof. So nothing widened that
// proof for this walk, and what collation happened to touch is not the same set:
// the walk descends into a subtree whenever the subtree's minimum lt is at or
// below the claimed bound — an lt-only comparison, so an entry that merely TIES
// the bound is descended to and only then dropped on the message-hash
// tie-break — while collation reaches the predecessor queue through the entries
// it imported (their delete paths and the diff over them) and through cleanup's
// prefix-restricted minimum-rank streams. An entry left behind at the bound's own
// lt is inside the first set and outside the second, and a validator meets a
// pruned branch there: "augmented dict has special cells in tree structure",
// raised by TraverseExtra on a node it must parse to descend.
//
// The reference has no shim for this because its collator makes the walk itself.
// Collator::request_neighbor_msg_queues fills neighbors_ from
// get_neighbor_shard_hash_ids(shard_), which lists our own shard
// (collator.cpp:936-953); create_output_queue_merger then builds one
// block::OutputQueueMerger over all of neighbors_ (collator.cpp:2269-2277) and
// process_inbound_internal_messages drives it (collator.cpp:4099-4160),
// unpacking even the entry it declines to import so its cells are in the proof
// (collator.cpp:4108-4109, "Visit cells to include it in proof").
// ValidateQuery::check_in_queue builds the same merger over the same neighbour
// set (validate-query.cpp:5389, :5402-5409) and walks it to claimed_proc_lt_ /
// claimed_proc_hash_ (:5441-5443) — one walk, two nodes. Collator::prepare_proofs
// (collator.cpp:6205-6264) therefore carries an account shim, a queue-DIFF shim
// and a dispatch shim, and no queue-SCAN shim at all. gton takes its inbound cut
// from the message pool instead of walking the queue, so the walk has to be
// replayed here, against the bound the block actually claims.
//
// Replaying it through walkSemanticQueuePrefix rather than an equivalent of it
// is the point: the cells recorded are then the validator's cells by
// construction, including the header-only leaf parse, and the two cannot drift
// apart the way a re-derived descent would.
//
// After a split and after a merge this is redundant rather than wrong: those
// predecessors are scanned whole before collation starts — filterOutQueue runs a
// Filter with a full parseQueueEntry over every parent entry, and
// cleanupMergedOutQueue an exhaustive queueCandidates scan — so every cell this
// walk can reach is already recorded and the read set drops the repeat.
//
// This pass must not take a truncating budget. Its extent is not an input: the
// bound is the one the block claims and the validator opens exactly that
// region. Cancellation is safe because a cancelled collation ships nothing.
//
// Every replay failure is fatal, including a semantic verdict on inherited
// content. The same verdict would make every validator reject this candidate;
// continuing would only spend serialization, signing, storage and broadcast on
// a block whose acceptance result is already known. Loading remains separated
// from parsing in walkSemanticQueuePrefix so the returned error still tells an
// operator whether storage or content stopped the closure.
func (c *collation) traceProcessedQueueValidationClosure() error {
	// The validator's own gate: verifyProcessedInfo returns before the source
	// loop unless the candidate moved ProcessedInfo, so an unchanged dictionary
	// starts no scan and needs no proof.
	if !c.processedClaimed {
		return nil
	}
	// cleanupClaimedLocalDequeues already made this exact walk, over the same
	// dictionary, to the same bound, with recording suspended because it ran
	// before the state update. Its cells are the cells this pass would parse,
	// so record those rather than resolving the whole prefix a second time.
	// Record is the path OnLoad takes, so what lands in the read set and in the
	// collated-size estimator is the same set either way; the stamp is what says
	// the collection is that walk's and not some other one's.
	if c.claimedPrefix.serves(c.oldOutQueue, c.shard, c.processedClaim) {
		c.usage.RecordMany(c.claimedPrefix.cells)
		c.claimedPrefix.release()

		return c.ctx.Err()
	}
	err := walkSemanticQueuePrefix(c.oldOutQueue, c.shard, c.processedClaim,
		func(semanticQueueEntry) error { return c.ctx.Err() })
	if err == nil {
		return nil
	}
	if cancelled := c.ctx.Err(); cancelled != nil {
		return cancelled
	}
	scanErr := fmt.Errorf("trace predecessor inbound queue scan to (%d,%x): %w",
		c.processedClaim.lt, c.processedClaim.hash, err)
	return scanErr
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
		var value cell.Slice
		if err := c.oldOutQueue.LoadValueByBytesKeyInto(key[:], &value); err != nil && !isMissingKey(err) {
			return fmt.Errorf("trace predecessor queue absence %x: %w", key, err)
		}
		// The candidate queue is walked too, because the validator walks it and
		// its untouched nodes are the predecessor's own cells: a key whose path
		// the block reshaped resolves through cells only this walk reaches.
		if err := c.outQueue.LoadValueByBytesKeyInto(key[:], &value); err != nil && !isMissingKey(err) {
			return fmt.Errorf("trace candidate queue absence %x: %w", key, err)
		}
	}

	return nil
}

// traceAccountValidationClosure retains the structural predecessor reads made
// by the reference collator's ShardAccounts scan_diff(..., mode=2). The normal
// bulk mutation captures its exact net structural closure and this late replay
// checks every changed candidate node through its final Patricia path. A block
// that destroys an account uses the canonical scan below because its later
// individual delete cannot be composed with the earlier receipt without
// retaining an intermediate trie.
func (c *collation) traceAccountValidationClosure() error {
	if c.accountMutationDiff != nil {
		// The replay is the slowest of the five closure tasks by a factor of
		// four on the testnet validator — 44 ms against 10 for the next — and
		// it is a sparse trie walk, so it splits by subtree. Half of its time
		// there is loading siblings the collation never touched, which the
		// split overlaps as well as the parsing.
		return c.accountMutationDiff.ReplayParallel(collationParallelism)
	}

	return c.oldState.Accounts.ShardAccounts.ScanDiffParallelRaw(
		c.accounts.AugmentedDictionary,
		true,
		func(cell.AugDictDiffRawView) error { return nil },
		collationParallelism,
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
	err := c.oldDispatchQueue.ScanDiffRaw(c.dispatchQueue.AugmentedDictionary, true, func(view cell.AugDictDiffRawView) error {
		var accountID [32]byte
		if view.KeyBits != 256 || len(view.Key) != len(accountID) {
			return fmt.Errorf("%w: dispatch diff key is malformed", ErrInvalidInput)
		}
		copy(accountID[:], view.Key)

		var oldAccount *tlb.AccountDispatchQueue
		if view.HasOld {
			var traced bool
			oldAccount, traced = c.oldDispatchAccounts[accountID]
			if !traced {
				var err error
				oldAccount, err = c.loadPredecessorDispatchAccount(accountID)
				if err != nil {
					return fmt.Errorf("%w: decode predecessor dispatch account: %v", ErrInvalidInput, err)
				}
			}
			if _, _, err := oldAccount.Messages.LoadMax(); err != nil {
				return fmt.Errorf("%w: load predecessor dispatch account maximum: %v", ErrInvalidInput, err)
			}
		}
		if view.HasNew {
			newAccount, err := dispatchDiffAccount(view.NewValueExtra)
			if err != nil {
				return fmt.Errorf("%w: decode candidate dispatch account: %v", ErrInvalidInput, err)
			}
			minimum, _, err := newAccount.Messages.LoadMin()
			if err != nil {
				return fmt.Errorf("%w: load candidate dispatch account minimum: %v", ErrInvalidInput, err)
			}
			// Mutating the nested dictionary rebuilds its Patricia path. Trace the
			// same minimum through the immutable predecessor source so a reused leaf
			// is retained in the previous-state proof.
			if oldAccount != nil {
				var value cell.Slice
				oldErr := oldAccount.Messages.LoadValueInto(minimum, &value)
				if oldErr != nil && !isMissingKey(oldErr) {
					return fmt.Errorf("%w: load candidate dispatch account minimum from predecessor: %v", ErrInvalidInput, oldErr)
				}
			}
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("trace dispatch queue difference: %w", err)
	}

	for _, accountID := range sortedAccountKeys(c.lanes) {
		if !c.lanes[accountID].touched {
			continue
		}
		if _, err = c.loadPredecessorDispatchValue(accountID); err != nil && !isMissingKey(err) {
			return fmt.Errorf("trace predecessor dispatch account %x: %w", accountID, err)
		}
	}

	return nil
}
