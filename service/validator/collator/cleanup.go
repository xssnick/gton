package collator

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	sharddomain "github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/validator/msgpool"
)

// cleanupPart is one neighbor's cleanup frontier: the neighbor and a lazy
// stream of its own out-queue entries in canonical (lt, message hash) order.
// It is the Go form of C++'s `queue_parts` element, a
// `pair<OutputQueueMerger, const McShardDescr*>` (collator.cpp ~:2598-2612).
type cleanupPart struct {
	neighbor Neighbor
	stream   *cell.AugMinIterator
}

// outQueueMinLT reads the OutMsgQueue augmentation: the 64-bit minimum EMITTED
// lt over the subtree, and nothing else. Emitted, not enqueued — the leaf rule
// is MsgEnvelope::get_emitted_lt over the envelope reference
// (Aug_OutMsgQueue::eval_leaf, block-parse.cpp:2142-2147), so EnqueuedMsg's
// inline enqueued_lt never enters it; see cleanupOutQueue's residual for why
// the difference decides what the drain may stop at. The trailing-data check is
// the same one walkSemanticQueuePrefix applies on the inbound side; C++ gets it
// from the `size_ext() != 0x20000` fork check in MsgKeyValue::unpack_node.
func outQueueMinLT(extra *cell.Slice) (uint64, error) {
	lt, err := extra.LoadUInt(64)
	if err != nil {
		return 0, err
	}
	if extra.BitsLeft() != 0 || extra.RefsNum() != 0 {
		return 0, fmt.Errorf("invalid outbound queue augmentation")
	}
	return lt, nil
}

// neighborQueueStream opens the own out-queue restricted to a neighbor's shard
// in the canonical (lt, message hash) cleanup order. This is the C++
// OutputQueueMerger built over exactly one root — the collator's own queue —
// and narrowed to the neighbor's shard by replace_by_prefix.
//
// Restricting by prefix rather than probing every key against every neighbor is
// what makes the enumeration lazy: nothing outside the prefix is ever opened.
//
// What it does NOT buy is a cheap first pull. Before the stream may emit a leaf
// it has to expand every fork whose subtree minimum TIES that leaf's rank —
// otherwise an unopened fork could still hold a smaller suffix — so the first
// Next costs O(m·depth) node opens, where m is the number of entries under this
// prefix sharing the minimum emitted lt. And m is not necessarily one: a
// message leaving the dispatch queue is emitted at
// max(start_lt, last emission for its account) + 1 (collator.cpp:4605-4611), so
// every account emitting its first deferred message in a block takes the same
// start_lt+1 and the whole set ties. The saving is
// therefore over the tail — a neighbor with nothing deliverable pays one prefix
// descent and one tied cascade instead of a walk of the whole trie, and a
// cleanup that stops early never touches what it did not reach.
func (c *collation) neighborQueueStream(shard msgpool.ShardIdent) (*cell.AugMinIterator, error) {
	prefix, err := outQueueShardPrefix(shard)
	if err != nil {
		return nil, err
	}
	return c.outQueue.MinIterator(cell.AugMinIteratorOptions{
		Rank:   outQueueMinLT,
		Prefix: prefix,
		// The canonical tie-break is the 256-bit message hash, i.e. the key
		// past its 32-bit workchain and 64-bit next-hop prefix. Byte for byte
		// the C++ `key.cbits() + 96` comparison and the key[12:] the eager scan
		// used to sort on.
		TieBreakFrom: 96,
	})
}

// cleanupOutQueue dequeues entries a neighbor has already imported. Each topology
// neighbor has an independent canonical (lt, hash) frontier. Round-robin
// traversal prevents one busy shard from consuming the entire cleanup budget.
//
// Enumeration is lazy: one prefix-restricted stream per neighbor, advanced only
// as entries are consumed. The eager alternative — collect every key, sort, then
// partition — costs O(Q log Q) before the first dequeue and, under
// capFullCollatedData, drags the whole queue trie into the shipped proof.
//
// Three bounds end the loop, for every part, in the C++ order
// (collator.cpp:2616-2624): the block-limit gate, the wall-clock budget, then
// cancellation. Stopping there early is legal — the neighbour keeps its own
// entry until a later block of ours dequeues it, and nothing validates the
// completeness of that half.
//
// The predecessor's own part is subject to the same three stops. This is both
// the reference order (the local predecessor is one of C++ queue_parts) and the
// validator's actual rule: an unchanged ProcessedInfo makes check_in_queue /
// verifyProcessedInfo return before inspecting any source queue. Consequently
// an idle block may leave an old, already-covered local entry queued.
//
// If this block later advances ProcessedInfo, the rule becomes narrower and
// stronger: validators inspect exactly the claimed (lt, hash) prefix and demand
// dequeue descriptors for old-covered local entries inside that prefix. The
// claim does not exist yet during this phase, so cleanupClaimedLocalDequeues
// closes precisely that prefix after updateProcessedInfo. It does not turn the
// whole local frontier into mandatory work.
//
// The reference-budget pass keeps C++'s per-part stop at the first uncovered
// entry. The post-claim pass is intentionally different: it walks exactly the
// validator's claimed prefix, which is the only safe way to cover non-prefix
// ProcessedInfo shapes inherited after topology changes.
func (c *collation) cleanupOutQueue() error {
	// An empty registered set used to return here, before anything ran. The
	// predecessor is a neighbour of itself: normalizeTrivialNeighbors contributes
	// it whether or not the masterchain registered anyone, so an empty set still
	// has ordinary budgeted own-shard cleanup work and may later have an exact
	// claimed-prefix obligation.
	//
	// After a MERGE it is different, and the return stays for that one case. The
	// merged predecessor's identity is assembled out of the registered child
	// descriptors — normalizeTrivialNeighbors takes end_lt from
	// neighbors[firstCovering] and only its queue and frontier from the
	// predecessor — and end_lt is the import_block_lt written into every
	// msg_export_deq_short. With no descriptors there is no end_lt to write, so
	// there is no dequeue to make rather than a dequeue we are skipping;
	// normalizeTrivialNeighbors says the same thing by rejecting the input.
	if len(c.req.neighbors) == 0 && c.topology.kind == topologyAfterMerge {
		return nil
	}
	if c.config.capabilities&capShortDequeue == 0 {
		return fmt.Errorf("%w: queue cleanup requires the short dequeue capability", ErrUnsupported)
	}
	neighbors, err := c.effectiveNeighbors()
	if err != nil {
		return err
	}
	for i := range neighbors {
		if neighbors[i].Shard != c.shard {
			continue
		}
		c.localCleanup = &localCleanupFrontier{
			endLT:     neighbors[i].EndLT,
			processed: append([]tlb.ProcessedUptoRecord(nil), neighbors[i].Processed...),
		}
		break
	}
	if c.topology.kind == topologyAfterMerge {
		return c.cleanupMergedOutQueue(neighbors)
	}

	// Each stream captures the queue root as it stands now. Cleanup only ever
	// deletes entries a stream has already emitted, so the snapshot and the
	// live dictionary agree on everything still ahead of every stream — the
	// same aliasing C++ relies on when it hands OutputQueueMerger
	// out_msg_queue_->get_root_cell() and then mutates out_msg_queue_ under it.
	parts := make([]cleanupPart, 0, len(neighbors))
	for i := range neighbors {
		stream, streamErr := c.neighborQueueStream(neighbors[i].Shard)
		if streamErr != nil {
			return fmt.Errorf("%w: open outbound queue stream for neighbor %d: %v",
				ErrInvalidInput, i, streamErr)
		}
		parts = append(parts, cleanupPart{neighbor: neighbors[i], stream: stream})
	}

	// Every part, including the local predecessor, participates in the same
	// round-robin under the C++ bounds.
	stop := CleanupStopExhausted
	for i := 0; len(parts) > 0; {
		if c.blockFull {
			stop = CleanupStopBlockFull
			break
		}
		if c.queueCleanupExpired() {
			stop = CleanupStopBudget
			break
		}
		if err = c.ctx.Err(); err != nil {
			return err
		}
		if i == len(parts) {
			i = 0
		}

		more, stepErr := c.cleanupQueuePartStep(&parts[i], strconv.Itoa(i))
		if stepErr != nil {
			return stepErr
		}
		if !more {
			parts[i] = parts[len(parts)-1]
			parts = parts[:len(parts)-1]
			continue
		}
		i++
	}
	c.stats.QueueCleanupStop = stop
	// The final queue proof closes the cleanup.
	if err = c.flushQueueDeletes(); err != nil {
		return err
	}
	return c.limits.addProof(c.outQueue.RootCell())
}

type localCleanupFrontier struct {
	endLT     uint64
	processed []tlb.ProcessedUptoRecord
}

// cleanupClaimedLocalDequeues closes the only local work that is mandatory for
// validity: old-covered entries inside the ProcessedInfo prefix this block has
// actually claimed. Cleanup runs before imports and cannot know that prefix;
// this pass runs immediately after updateProcessedInfo.
func (c *collation) cleanupClaimedLocalDequeues() error {
	if !c.processedClaimed || c.localCleanup == nil || c.topology.kind == topologyAfterMerge {
		return nil
	}

	drained := uint32(0)
	dequeue := func(entry claimedQueueEntry) error {
		// entry.key is the dictionary key the walk visited, already checked
		// against the routing the envelope itself derives.
		key := entry.key
		var value cell.Slice
		err := c.outQueue.LoadValueByBytesKeyInto(key[:], &value)
		if isMissingKey(err) {
			// An old-covered entry cannot be removed by importInternal: that
			// path returns at its processed check before dequeueOwn. Therefore
			// absence here means the reference-budget cleanup already removed
			// this exact key and emitted its descriptor.
			return nil
		}
		if err != nil {
			return fmt.Errorf("load claimed local queue entry %x: %w", key, err)
		}
		current, err := parseQueueEntry(&value, key)
		if err != nil {
			return err
		}
		if current.envelope.HashKey() != entry.envelopeHash {
			return fmt.Errorf("%w: claimed local queue entry %x changed envelope", ErrInvalidInput, key)
		}
		if err = c.dequeueDelivered(current, c.localCleanup.endLT); err != nil {
			return err
		}
		c.stats.QueueCleaned++
		drained++
		if what, value, limit, over := c.limits.hardOverflow(); over {
			return fmt.Errorf(
				"%w: shard %d:%016x dequeued %d already-processed entries inside the claimed prefix and "+
					"%s reached %d against a hard limit of %d; the claimed prefix cannot be truncated",
				ErrMandatoryDequeueOverflow, c.shard.Workchain, c.shard.Shard, drained, what, value, limit)
		}

		return nil
	}

	// The scan below is DISCOVERY, and its reads must not be recorded. This pass
	// runs before create_shard_state generates the Merkle update, and the update
	// descends only through cells the read set holds, so a read taken here lands
	// in the update's OLD side — cells the reference collator never puts there.
	// It is the same rule finish() states at the traceValidationClosure call
	// site, and the cost is not theoretical: on the mainnet fixture this walk
	// dequeues nothing and still added 697 block bytes, and 9175 on the
	// three-times-traffic arm, because what it retains scales with the own-shard
	// queue prefix rather than with the work it finds.
	//
	// Nothing is lost by ignoring them. traceProcessedQueueValidationClosure
	// (proof_closure.go) records this identical region — same queue, same target
	// shard, same claimed bound — after the update exists, which is where these
	// cells belong: in the collated proof and nowhere else. It records the very
	// cells this walk parses, kept in claimedPrefixCells, so the split that has
	// to stay split is the recording and not the traversal.
	//
	// Recording is switched back on around the dequeue itself. An entry this
	// block actually removes is block content, and its cells have to reach both
	// the update and the proof; the traced c.outQueue lookup inside dequeue is
	// what puts them there.
	c.usage.IgnoreReads(true)
	defer c.usage.IgnoreReads(false)

	// Installed inside the scope and cleared before it closes — the deferred
	// order above is what guarantees the second half — so the observer fires at
	// this nesting depth only, and a dictionary opening its own ignore scope
	// underneath still drops its reads to nobody.
	//
	// The same answer gates the stamp below, out of one variable rather than two
	// calls. A stamp left on a collection nothing filled would tell the closure
	// to record an empty set, which is the under-recording that makes every
	// validator reject the candidate.
	collecting := c.closureRecordsPredecessorReads()
	if collecting {
		c.usage.SetIgnoredObserver(c.claimedPrefix.collect)
		defer c.usage.SetIgnoredObserver(nil)
	}

	err := walkClaimedQueuePrefix(c.oldOutQueue, c.shard, c.processedClaim, func(entry claimedQueueEntry) error {
		// Everything below is cleanup's own work and the closure's walk does
		// none of it, so its reads are not part of the set the closure records.
		c.claimedPrefix.suspend()
		defer c.claimedPrefix.resume()

		if err := c.ctx.Err(); err != nil {
			return err
		}
		if !c.shard.ContainsPrefix(entry.current) {
			return nil
		}
		processed, err := c.shardEndLT.alreadyProcessed(
			c.localCleanup.processed,
			c.shard.Workchain,
			c.shard.Shard,
			&entry.descr,
		)
		if err != nil {
			return fmt.Errorf("%w: check predecessor processed info: %v", ErrInvalidInput, err)
		}
		if !processed {
			return nil
		}

		c.usage.IgnoreReads(false)
		err = dequeue(entry)
		c.usage.IgnoreReads(true)

		return err
	})
	if err != nil {
		return fmt.Errorf("close claimed local queue prefix to (%d,%x): %w",
			c.processedClaim.lt, c.processedClaim.hash, err)
	}
	// Stamped only here, so a walk that stopped anywhere above leaves the
	// collection unusable and the closure walks for itself.
	if collecting {
		c.claimedPrefix.stamp(c.oldOutQueue, c.shard, c.processedClaim)
	}

	return nil
}

// cleanupQueuePartStep advances one stream by a single entry and dequeues it if
// the part's frontier covers it. It reports whether the part still has work: a
// stream that ran out or reached its first undelivered entry is finished, and
// C++ pops exactly those two cases off queue_parts.
//
// The label only names the part in errors; it is the current neighbor index in
// the round-robin set.
func (c *collation) cleanupQueuePartStep(part *cleanupPart, label string) (bool, error) {
	if !part.stream.Next() {
		if err := part.stream.Err(); err != nil {
			return false, fmt.Errorf("%w: scan outbound queue for neighbor %s: %v",
				ErrInvalidInput, label, err)
		}
		return false, nil
	}
	// The view is borrowed and dies at this stream's next Next. Everything kept
	// past this call is owned: parseQueueEntry extracts finalized cells, and
	// dequeueDelivered runs before the stream advances again.
	view := part.stream.View()
	var key msgpool.QueueKey
	if err := view.Key.LoadSliceInto(key[:], 352); err != nil {
		return false, fmt.Errorf("%w: decode queue entry key: %v", ErrInvalidInput, err)
	}
	entry, err := parseQueueEntry(&view.Value, key)
	if err != nil {
		return false, err
	}
	processed, err := c.shardEndLT.alreadyProcessed(
		part.neighbor.Processed,
		part.neighbor.Shard.Workchain,
		part.neighbor.Shard.Shard,
		&entry.descr,
	)
	if err != nil {
		return false, fmt.Errorf("%w: check neighbor processed info: %v", ErrInvalidInput, err)
	}
	if !processed {
		// C++ drops the part at its first undelivered entry without advancing
		// the merger; we advanced before the check. Unobservable: the part is
		// never consulted again.
		return false, nil
	}
	if err = c.dequeueDelivered(entry, part.neighbor.EndLT); err != nil {
		return false, err
	}
	c.stats.QueueCleaned++

	return true, nil
}

// queueCleanupUntil ports Collator::queue_cleanup_timeout_ (collator.cpp:85-97):
// with an external wait the budget runs to the later of that wait and the soft
// deadline, otherwise it takes a quarter of the remaining soft budget.
//
// A zero soft deadline leaves the budget inert. That is the convention every
// deterministic entry point relies on: BuildShard, restore requests and the
// whole test surface pass zero and therefore clean exactly as much as they did
// before the budget existed.
func queueCleanupUntil(now, softDeadline, externalWaitUntil time.Time) time.Time {
	return phaseTimeout(now, softDeadline, externalWaitUntil, 4)
}

// internalMsgUntil ports Collator::internal_msg_timeout_ from the same
// constructor block (collator.cpp:85-97). It differs from
// queue_cleanup_timeout_ in exactly one place: without an external wait the
// reference gives the internal phases half of the remaining soft budget where
// cleanup gets a quarter (collator.cpp:94-95). With an external wait both are
// max(wait_externals_until, soft_timeout), which is why the two share
// phaseTimeout rather than repeating the branch.
//
// The zero convention is the same one queueCleanupUntil documents: a zero soft
// deadline leaves the budget inert, so BuildShard, restore requests and the
// whole deterministic test surface reproduce the pre-budget candidate byte for
// byte.
func internalMsgUntil(now, softDeadline, externalWaitUntil time.Time) time.Time {
	return phaseTimeout(now, softDeadline, externalWaitUntil, 2)
}

// phaseTimeout is the shared shape of collator.cpp:85-97: with an external wait
// every phase budget runs to the later of that wait and the soft deadline;
// without one each phase takes its own fraction of the remaining soft budget.
func phaseTimeout(now, softDeadline, externalWaitUntil time.Time, divisor time.Duration) time.Time {
	if softDeadline.IsZero() {
		return time.Time{}
	}
	if !externalWaitUntil.IsZero() {
		if externalWaitUntil.After(softDeadline) {
			return externalWaitUntil
		}
		return softDeadline
	}
	if remaining := softDeadline.Sub(now); remaining > 0 {
		return now.Add(remaining / divisor)
	}
	return now
}

// internalMsgExpired is the per-iteration soft-timeout check the reference runs
// at collator.cpp:4141 (inbound internals), :4424 (dispatch queue), :4851 (new
// messages) and :3732 (after one delivered new message). Like
// queueCleanupExpired it samples the clock every iteration, because sampling
// every N would make the stop point depend on the iteration count.
//
// Reaching it is not an error: the reference truncates the block at the soft
// boundary and still publishes. Before this port the three internal phases had
// no time budget at all, so a slot that ran long was aborted wholesale by ctx
// cancellation and published nothing.
func (c *collation) internalMsgExpired() bool {
	if c.req.internalMsgUntil.IsZero() {
		return false
	}
	return !time.Now().Before(c.req.internalMsgUntil)
}

// queueCleanupExpired is the per-iteration budget check. C++ samples the clock
// on every loop iteration too; sampling every N would make the stop point
// depend on the iteration count, which is exactly the determinism the rest of
// this file works to avoid.
func (c *collation) queueCleanupExpired() bool {
	if c.req.queueCleanupUntil.IsZero() {
		return false
	}
	return !time.Now().Before(c.req.queueCleanupUntil)
}

// cleanupMergedOutQueue must inspect the complete merged queue. The two child
// frontiers are not a single monotonic stream, so stopping at one uncovered
// message could leave a later message covered by another neighbor.
//
// It deliberately takes neither the lazy stream nor the budget. C++ agrees
// (collator.cpp:2535-2597 is a plain check_for_each with no break), merge is
// refused above mergeMaxQueueSize = 2047 entries so the eager scan is bounded,
// and — decisively — verifyMergedQueueCleanup rejects any after-merge block
// that left a delivered message behind. A budget here would produce blocks our
// own validator refuses.
func (c *collation) cleanupMergedOutQueue(effective []Neighbor) error {
	candidates, err := c.queueCandidates()
	if err != nil {
		return err
	}
	for i := range candidates {
		if err = c.ctx.Err(); err != nil {
			return err
		}
		entry, loadErr := c.loadQueueEntry(candidates[i].key)
		if loadErr != nil {
			return loadErr
		}
		for j := range effective {
			neighbor := &effective[j]
			processed, processedErr := c.shardEndLT.alreadyProcessed(
				neighbor.Processed,
				neighbor.Shard.Workchain,
				neighbor.Shard.Shard,
				&entry.descr,
			)
			if processedErr != nil {
				return fmt.Errorf("%w: check neighbor processed info: %v", ErrInvalidInput, processedErr)
			}
			if !processed {
				continue
			}
			if err = c.dequeueDelivered(entry, neighbor.EndLT); err != nil {
				return err
			}
			c.stats.QueueCleaned++
			break
		}
	}

	if err = c.flushQueueDeletes(); err != nil {
		return err
	}
	return c.limits.addProof(c.outQueue.RootCell())
}

// dequeueCandidate is one own out-queue entry: its dictionary key and the
// canonical augmentation lt that orders the scan.
type dequeueCandidate struct {
	lt  uint64
	key msgpool.QueueKey
}

// queueCandidates collects every queue key in the canonical (augmentation lt,
// message hash) order queues are scanned in. Values stay unloaded until the
// scan reaches them.
//
// Only the after-merge path uses it. The round-robin path streams instead, see
// neighborQueueStream; the eager form is kept here because the merged scan is
// exhaustive by requirement, not by choice.
func (c *collation) queueCandidates() ([]dequeueCandidate, error) {
	var candidates []dequeueCandidate
	ok, err := c.outQueue.CheckForEachExtra(func(_, extra *cell.Slice, key *cell.Cell) (bool, error) {
		var keyLoader cell.Slice
		err := key.BeginParseInto(&keyLoader)
		if err != nil {
			return false, err
		}
		var entryKey msgpool.QueueKey
		if err = keyLoader.LoadSliceInto(entryKey[:], 352); err != nil {
			return false, err
		}
		extraValue := *extra
		lt, err := extraValue.LoadUInt(64)
		if err != nil {
			return false, fmt.Errorf("load queue entry %x augmentation: %w", entryKey, err)
		}
		candidates = append(candidates, dequeueCandidate{lt: lt, key: entryKey})
		return true, nil
	}, false)
	if err != nil {
		return nil, fmt.Errorf("scan outbound queue: %w", err)
	}
	if !ok {
		return nil, fmt.Errorf("%w: outbound queue scan stopped", ErrInvalidInput)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].lt != candidates[j].lt {
			return candidates[i].lt < candidates[j].lt
		}
		return bytes.Compare(candidates[i].key[12:], candidates[j].key[12:]) < 0
	})
	return candidates, nil
}

// queueEntry is one parsed out-queue entry: the queued envelope, the message
// inside it and the coverage-check view.
type queueEntry struct {
	key      msgpool.QueueKey
	envelope *cell.Cell
	msg      *cell.Cell
	descr    tlb.ProcessedMsgDescr
}

// loadQueueEntry parses one queue entry into the coverage-check view and
// verifies that its dictionary key matches the envelope it holds.
func (c *collation) loadQueueEntry(key msgpool.QueueKey) (queueEntry, error) {
	var value cell.Slice
	if err := c.outQueue.LoadValueByBytesKeyInto(key[:], &value); err != nil {
		return queueEntry{}, fmt.Errorf("load queue entry %x: %w", key, err)
	}

	return parseQueueEntry(&value, key)
}

// parseQueueEntry decodes one out-queue entry and re-derives its routing from
// the message it carries. The queue key is checked against that derivation, so
// an entry can never claim a next hop its envelope does not produce.
func parseQueueEntry(value *cell.Slice, key msgpool.QueueKey) (queueEntry, error) {
	var enqueued tlb.EnqueuedMsg
	if err := loadExactSlice(&enqueued, value); err != nil {
		return queueEntry{}, fmt.Errorf("%w: decode queue entry %x: %v", ErrInvalidInput, key, err)
	}
	var env tlb.MsgEnvelope
	if err := parseExact(&env, enqueued.Msg); err != nil {
		return queueEntry{}, fmt.Errorf("%w: decode queued envelope %x: %v", ErrInvalidInput, key, err)
	}
	if env.CurAddr.Type != tlb.IntermediateAddressRegular || env.NextAddr.Type != tlb.IntermediateAddressRegular {
		return queueEntry{}, fmt.Errorf("%w: queued envelope %x has a non-regular intermediate address", ErrInvalidInput, key)
	}
	var internal tlb.InternalMessage
	if err := parseExact(&internal, env.Msg); err != nil {
		return queueEntry{}, fmt.Errorf("%w: decode queued message %x: %v", ErrInvalidInput, key, err)
	}
	if err := validateQueuedExtraCurrencies(internal.ExtraCurrencies); err != nil {
		return queueEntry{}, fmt.Errorf("%w: decode queued message extra currencies %x: %v", ErrInvalidInput, key, err)
	}
	if internal.StateInit != nil {
		roots := [3]*cell.Cell{internal.StateInit.Code, internal.StateInit.Data}
		if internal.StateInit.Lib != nil && !internal.StateInit.Lib.IsEmpty() {
			roots[2] = internal.StateInit.Lib.AsCell()
		}
		for _, root := range roots {
			if root == nil {
				continue
			}
			var content cell.Slice
			if err := root.BeginParseInto(&content); err != nil {
				return queueEntry{}, fmt.Errorf("%w: decode queued message StateInit %x: %v", ErrInvalidInput, key, err)
			}
		}
	}
	// The reference validator's generated EnqueuedMsg validation opens an
	// indirect message body and every StateInit reference as Anything cells.
	// Parsing InternalMessage only loads those references, so explicitly
	// materialize their roots as part of the same validation closure.
	// Descendants stay opaque, matching Anything.
	var body cell.Slice
	if err := internal.Body.BeginParseInto(&body); err != nil {
		return queueEntry{}, fmt.Errorf("%w: decode queued message body %x: %v", ErrInvalidInput, key, err)
	}
	lt := internal.CreatedLT
	if env.EmittedLT != nil {
		lt = *env.EmittedLT
	}

	source, err := accountPrefixFromAddress(internal.SrcAddr)
	if err != nil {
		return queueEntry{}, fmt.Errorf("%w: queued message %x source: %v", ErrInvalidInput, key, err)
	}
	destination, err := accountPrefixFromAddress(internal.DstAddr)
	if err != nil {
		return queueEntry{}, fmt.Errorf("%w: queued message %x destination: %v", ErrInvalidInput, key, err)
	}
	cur := msgpool.InterpolatePrefix(source, destination, int(env.CurAddr.UseDestBits))
	next := msgpool.InterpolatePrefix(source, destination, int(env.NextAddr.UseDestBits))
	hash := env.Msg.HashKey()
	if msgpool.MakeQueueKey(next, hash) != key {
		return queueEntry{}, fmt.Errorf("%w: queue entry %x key differs from its envelope", ErrInvalidInput, key)
	}

	return queueEntry{
		key:      key,
		envelope: enqueued.Msg,
		msg:      env.Msg,
		descr: tlb.ProcessedMsgDescr{
			CurWorkchain:  cur.Workchain,
			CurPrefix:     cur.Prefix,
			NextWorkchain: next.Workchain,
			NextPrefix:    next.Prefix,
			LT:            lt,
			EnqueuedLT:    enqueued.EnqueuedLT,
			Hash:          hash,
		},
	}, nil
}

// validateQueuedExtraCurrencies covers the same HashmapE 32
// (VarUIntegerPos 32) traversal as the reference Message validator. Empty
// dictionaries are the common path; non-empty ones must be fully materialized
// so the collated predecessor proof is sufficient for independent validation.
func validateQueuedExtraCurrencies(extra *cell.Dictionary) error {
	if extra == nil || extra.IsEmpty() {
		return nil
	}

	items, err := extra.LoadAll()
	if err != nil {
		return err
	}
	for _, item := range items {
		length, err := item.Value.LoadUInt(5)
		if err != nil {
			return err
		}
		if length == 0 || length >= 32 {
			return fmt.Errorf("invalid value length %d", length)
		}
		first, err := item.Value.LoadUInt(8)
		if err != nil {
			return err
		}
		if first == 0 {
			return errors.New("non-canonical value with a leading zero byte")
		}
		if err = item.Value.SkipBits(uint(length-1) * 8); err != nil {
			return err
		}
		if item.Value.BitsLeft() != 0 || item.Value.RefsNum() != 0 {
			return errors.New("value has trailing data")
		}
	}
	return nil
}

// dequeueDelivered removes one covered entry and records
// msg_export_deq_short$1101: the envelope hash, the next-hop half of the queue
// key and the covering neighbor's end lt.
func (c *collation) dequeueDelivered(entry queueEntry, importBlockLT uint64) error {
	// The entry was loaded and verified by loadQueueEntry just above; the
	// pending-set check is what keeps a repeated dequeue failing as it did
	// when the delete looked the key up again.
	if c.queueDeletePending(entry.key) {
		return fmt.Errorf("dequeue delivered message %x: %w", entry.key, cell.ErrNoSuchKeyInDict)
	}
	if c.queueSize == 0 {
		return fmt.Errorf("%w: outbound queue size underflow", ErrInvalidInput)
	}
	if err := c.deferQueueDelete(entry.key); err != nil {
		return err
	}
	c.queueSize--

	out, err := descriptorDequeueShort(entry.envelope.HashKey(), entry.key, importBlockLT)
	if err != nil {
		return err
	}
	if err = c.insert(c.outMessages.AugmentedDictionary, &c.outDescr, entry.msg, out); err != nil {
		return err
	}
	if err = c.registerQueueOp(); err != nil {
		return err
	}
	c.updatePeakLoad()
	if !c.limits.fits(c.fullMark()) {
		c.blockFull = true
	}
	return nil
}

func validateNeighbors(neighbors []Neighbor) error {
	for i := range neighbors {
		neighbor := &neighbors[i]
		if neighbor.Shard.Shard == 0 {
			return fmt.Errorf("%w: neighbor %d has a zero shard", ErrInvalidInput, i)
		}
		if neighbor.Block.Workchain != neighbor.Shard.Workchain ||
			uint64(neighbor.Block.Shard) != neighbor.Shard.Shard ||
			len(neighbor.Block.RootHash) != 32 || len(neighbor.Block.FileHash) != 32 {
			return fmt.Errorf("%w: neighbor %d block id differs from its shard", ErrInvalidInput, i)
		}
		if len(neighbor.Processed) > 0 && neighbor.EndLT == 0 {
			return fmt.Errorf("%w: neighbor %d has processed records but a zero end lt", ErrInvalidInput, i)
		}
		for j := range neighbor.Processed {
			record := &neighbor.Processed[j]
			if record.ShardPrefix == 0 ||
				!sharddomain.Contains(int64(neighbor.Shard.Shard), int64(record.ShardPrefix)) {
				return fmt.Errorf("%w: processed record %d is outside neighbor %d", ErrInvalidInput, j, i)
			}
		}
		for j := 0; j < i; j++ {
			other := &neighbors[j]
			if neighbor.Shard.Workchain == other.Shard.Workchain &&
				sharddomain.Intersects(int64(neighbor.Shard.Shard), int64(other.Shard.Shard)) {
				return fmt.Errorf("%w: neighbors %d and %d overlap", ErrInvalidInput, j, i)
			}
		}
	}

	return nil
}
