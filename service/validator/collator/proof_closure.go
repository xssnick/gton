package collator

import (
	"errors"
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
	if err := c.traceProcessedQueueValidationClosure(); err != nil {
		return err
	}

	return c.traceDispatchQueueValidationClosure()
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
// Two things this pass must NOT do, both learned the hard way.
//
// It must not take a truncating budget. Its extent is not an input: the bound
// is the one the block claims and the validator opens exactly that region, so a
// walk that stopped early ships a proof with a pruned branch inside the region
// the validator descends — the incident this shim exists to prevent, produced
// on a schedule instead of by coincidence. The only legal way to shorten it is
// to claim less, which is a decision for import, not for here. It is linear in
// the predecessor's own-prefix queue at roughly four cell loads per entry, with
// no cap anywhere (maxOutMsgQueueSize is 2^48-1), so the cost is real; what it
// gets instead of a budget is the per-entry cancellation check every sibling
// pass already has (cleanupOutQueue, cleanupMergedOutQueue,
// verifyMergedQueueCleanup). Cancellation truncates nothing that could ever
// ship — a cancelled collation produces no block — while today an abandoned
// slot keeps burning loads and allocation that the next slot needs.
//
// And it must not veto the block FOR ONE CLASS OF FAILURE, which is narrower
// than "any failure". The walk exists to record cells; its parse results are
// discarded. A semantic rejection of a PREDECESSOR entry — data we inherited,
// possibly from a persistent state or an archive import, and never validated
// ourselves — is a verdict on the predecessor, not on this candidate, and it
// reaches the validator through the very same parseSemanticNeighborQueueEntry
// on the very same dictionary at the very same bound. So the block's fate is
// already settled by that entry whatever we do here, and failing the collation
// only converts "produce a block others reject" into "produce nothing", which
// on a mixed network stalls every Go node every slot while the reference nodes
// keep collating. For that class the walk stops widening, records that it did,
// and the block goes out.
//
// Every OTHER failure must stop the block, because that reasoning does not
// reach it. A node whose storage could not open a queue node, whose child
// context was cancelled, or whose walk met a structural inconsistency of its
// own making has learned nothing about what the validator will find — it has
// only learned that its proof is short. Shipping there is defect 1 recreated
// on purpose: a candidate with a knowingly incomplete collated proof, rejected
// by every proof-backed peer for "augmented dict has special cells in tree
// structure", with no record of why.
//
// So the swallow is keyed on semanticQueueEntryVerdict, which
// walkSemanticQueuePrefix raises only about cells it already holds, and on
// ErrInvalidInput. Cancellation is read from the context rather than from the
// error chain, because a cancelled lazy load surfaces as whatever the storage
// returned and the entry parse re-wraps it with %v: the chain is not a reliable
// witness there while the context always is, and a cancelled collation ships
// nothing anyway.
//
// The storage fault raised strictly BELOW the leaf — inside the envelope
// reference the entry parse follows — used to be the one case this could not
// tell from a malformed entry, because tonutils lazy loaders return the
// underlying error unchanged and there is no I/O sentinel to test for. It is
// closed by separating the load from the parse rather than by inspecting
// errors: materialiseSemanticQueueLeaf resolves the envelope and the message
// through Cell.Prewarm, whose every error comes from the loader, and the parse
// then runs on those cells, so what it can still raise is a statement about
// content. See materialiseSemanticQueueLeaf for why the two structural checks
// it makes on the way are verdicts, and for why it costs no extra loads.
func (c *collation) traceProcessedQueueValidationClosure() error {
	// The validator's own gate: verifyProcessedInfo returns before the source
	// loop unless the candidate moved ProcessedInfo, so an unchanged dictionary
	// starts no scan and needs no proof.
	if !c.processedClaimed {
		return nil
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
	if !shippableQueueTraceFailure(err) {
		return scanErr
	}
	c.stats.ProcessedScanTraceError = scanErr.Error()

	return nil
}

// shippableQueueTraceFailure reports whether a replay failure is one the block
// may ship past. Both conditions are load-bearing and neither implies the
// other:
//
//   - the failure must be a semanticQueueEntryVerdict, which the walk raises
//     only about a leaf's content, never about reaching one;
//   - and it must carry ErrInvalidInput, which is what the entry parse marks a
//     rejection of predecessor DATA with. A verdict that does not is not a
//     rejection of the entry — it is this node's own machinery failing while
//     holding the entry, and the argument for shipping (the validator meets the
//     same bytes through the same parse) says nothing about that.
//
// It is a function rather than two lines at the call site so each condition can
// be pinned on its own; see TestShippableQueueTraceFailureNeedsBothGuards.
func shippableQueueTraceFailure(err error) bool {
	var verdict semanticQueueEntryVerdict
	return errors.As(err, &verdict) && errors.Is(err, ErrInvalidInput)
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
