package collator

import (
	"bytes"
	"errors"
	"fmt"
	"sort"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	sharddomain "github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/validator/msgpool"
)

// dequeueCandidate is one own out-queue entry routed to the masterchain
// neighbor: its dictionary key and the canonical augmentation lt that orders
// the scan.
type dequeueCandidate struct {
	lt  uint64
	key msgpool.QueueKey
}

type cleanupPart struct {
	neighbor   Neighbor
	candidates []dequeueCandidate
	next       int
}

// cleanupOutQueue dequeues entries a neighbor has already imported. Each topology
// neighbor has an independent canonical (lt, hash) frontier. Round-robin
// traversal prevents one busy shard from consuming the entire cleanup budget.
func (c *collation) cleanupOutQueue() error {
	if len(c.req.neighbors) == 0 {
		return nil
	}
	if c.config.capabilities&capShortDequeue == 0 {
		return fmt.Errorf("%w: queue cleanup requires the short dequeue capability", ErrUnsupported)
	}
	neighbors, err := c.effectiveNeighbors()
	if err != nil {
		return err
	}
	if c.topology.kind == topologyAfterMerge {
		return c.cleanupMergedOutQueue(neighbors)
	}

	parts := make([]cleanupPart, len(neighbors))
	for i := range neighbors {
		parts[i].neighbor = neighbors[i]
	}
	// One scan of the queue, partitioned by next hop. The canonical order is a
	// total order on queue keys, so each neighbor's slice of the global order is
	// exactly the order a per-neighbor scan would have produced. The parts keep
	// C++ neighbors_ order for the same round-robin cleanup without rescanning.
	candidates, err := c.queueCandidates()
	if err != nil {
		return err
	}
	partitionQueueCandidates(parts, candidates)

	for i := 0; len(parts) > 0; {
		if c.blockFull {
			break
		}
		if err := c.ctx.Err(); err != nil {
			return err
		}
		if i == len(parts) {
			i = 0
		}

		part := &parts[i]
		if part.next == len(part.candidates) {
			parts[i] = parts[len(parts)-1]
			parts = parts[:len(parts)-1]
			continue
		}

		entry, err := c.loadQueueEntry(part.candidates[part.next].key)
		if err != nil {
			return err
		}
		processed, err := c.shardEndLT.alreadyProcessed(
			part.neighbor.Processed,
			part.neighbor.Shard.Workchain,
			part.neighbor.Shard.Shard,
			&entry.descr,
		)
		if err != nil {
			return fmt.Errorf("%w: check neighbor processed info: %v", ErrInvalidInput, err)
		}
		if !processed {
			parts[i] = parts[len(parts)-1]
			parts = parts[:len(parts)-1]
			continue
		}
		if err = c.dequeueDelivered(entry, part.neighbor.EndLT); err != nil {
			return err
		}
		part.next++
		i++
	}
	// The final queue proof closes the cleanup.
	if err = c.flushQueueDeletes(); err != nil {
		return err
	}
	return c.limits.addProof(c.outQueue.RootCell())
}

// cleanupMergedOutQueue must inspect the complete merged queue. The two child
// frontiers are not a single monotonic stream, so stopping at one uncovered
// message could leave a later message covered by another neighbor.
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
			break
		}
	}

	if err = c.flushQueueDeletes(); err != nil {
		return err
	}
	return c.limits.addProof(c.outQueue.RootCell())
}

// partitionQueueCandidates buckets one canonical scan by next hop. The
// canonical order is a total order on queue keys, so a neighbor's slice of the
// global order is exactly the order a scan restricted to that neighbor would
// have produced. An entry no neighbor covers is dropped: it is not deliverable
// to anyone this block can clean up after.
func partitionQueueCandidates(parts []cleanupPart, candidates []dequeueCandidate) {
	for i := range candidates {
		for j := range parts {
			if parts[j].neighbor.Shard.ContainsPrefix(candidates[i].key.NextHop()) {
				parts[j].candidates = append(parts[j].candidates, candidates[i])
				break
			}
		}
	}
}

// queueCandidates collects every queue key in the canonical (augmentation lt,
// message hash) order queues are scanned in. Values stay unloaded until the
// scan reaches them.
func (c *collation) queueCandidates() ([]dequeueCandidate, error) {
	var candidates []dequeueCandidate
	ok, err := c.outQueue.CheckForEachExtra(func(_, extra *cell.Slice, key *cell.Cell) (bool, error) {
		keyLoader, err := key.BeginParse()
		if err != nil {
			return false, err
		}
		keyBits, err := keyLoader.LoadSlice(352)
		if err != nil {
			return false, err
		}
		var entryKey msgpool.QueueKey
		copy(entryKey[:], keyBits)
		lt, err := extra.Copy().LoadUInt(64)
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
	keyCell := cell.BeginCell().MustStoreSlice(key[:], 352).EndCell()
	value, err := c.outQueue.LoadValue(keyCell)
	if err != nil {
		return queueEntry{}, fmt.Errorf("load queue entry %x: %w", key, err)
	}

	return parseQueueEntry(value, key)
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
			if _, err := root.BeginParse(); err != nil {
				return queueEntry{}, fmt.Errorf("%w: decode queued message StateInit %x: %v", ErrInvalidInput, key, err)
			}
		}
	}
	// The reference validator's generated EnqueuedMsg validation opens an
	// indirect message body and every StateInit reference as Anything cells.
	// Parsing InternalMessage only loads those references, so explicitly
	// materialize their roots as part of the same validation closure.
	// Descendants stay opaque, matching Anything.
	if _, err := internal.Body.BeginParse(); err != nil {
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
	keyCell := cell.BeginCell().MustStoreSlice(entry.key[:], 352).EndCell()
	if c.queueSize == 0 {
		return fmt.Errorf("%w: outbound queue size underflow", ErrInvalidInput)
	}
	c.deferQueueDelete(entry.key, keyCell)
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
	if !c.limits.fits(LoadNormal) {
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
