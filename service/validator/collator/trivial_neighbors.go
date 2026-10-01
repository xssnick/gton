package collator

import (
	"fmt"

	sharddomain "github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/validator/msgpool"
)

// effectiveNeighbor is one entry of the normalized neighbor set together with
// the index of the registered neighbor whose queue it covers. origin is -1 for
// the predecessor entry, whose queue is the collation's own.
type effectiveNeighbor struct {
	Neighbor
	origin int
}

// normalizeTrivialNeighbors turns the registered neighbor set into the one both
// sides of the protocol reason about. Every neighbor whose shard intersects
// ours is withheld and replaced by the predecessor this block starts from; a
// still-registered ancestor additionally contributes the half of its queue that
// stayed with our sibling.
//
// The masterchain registers a shard only once it produces a block, so between a
// split and that registration our own ancestor is still listed and covers both
// halves. Reasoning from the raw set makes a dequeue of one of our own messages
// carry the ancestor's end_lt in msg_export_deq_short, while validation
// derives it from the predecessor entry and rejects the block (check_out_msg,
// msg_export_deq*). Entries retain the C++ neighbors_ order: cleanup visits
// those streams round-robin and its first processed neighbor supplies end_lt.
func normalizeTrivialNeighbors(
	neighbors []Neighbor,
	target msgpool.ShardIdent,
	predecessor Neighbor,
	afterMerge bool,
) ([]effectiveNeighbor, error) {
	effective := make([]effectiveNeighbor, 0, len(neighbors)+1)
	ancestor := -1
	firstCovering := -1
	covering := 0
	children := 0
	for i := range neighbors {
		neighbor := &neighbors[i]
		if neighbor.Shard.Workchain != target.Workchain ||
			!sharddomain.Intersects(int64(neighbor.Shard.Shard), int64(target.Shard)) {
			continue
		}

		covering++
		if firstCovering < 0 {
			firstCovering = i
		}
		if neighbor.Shard == target {
			continue
		}
		if sharddomain.IsDirectChild(int64(target.Shard), int64(neighbor.Shard.Shard)) {
			children++
			continue
		}
		if !sharddomain.Contains(int64(neighbor.Shard.Shard), int64(target.Shard)) {
			return nil, fmt.Errorf("%w: neighbor %016x neither is, contains nor descends from this shard",
				ErrInvalidInput, neighbor.Shard.Shard)
		}
		if ancestor >= 0 {
			return nil, fmt.Errorf("%w: two neighbors are ancestors of this shard", ErrInvalidInput)
		}
		ancestor = i
	}

	switch {
	case afterMerge:
		if covering != 2 || children != 2 || ancestor >= 0 {
			return nil, fmt.Errorf("%w: after-merge collation has %d self-covering neighbors, want the two predecessors",
				ErrInvalidInput, covering)
		}
		// C++ add_trivial_neighbor_after_merge mutates the first intersecting
		// child descriptor in place. Its queue and processed frontier become
		// the merged predecessor's, but its end_lt remains the registered
		// child's end_lt and is written into every dequeue descriptor.
		merged := neighbors[firstCovering]
		merged.Block = cloneBlockID(merged.Block)
		merged.Block.Shard = int64(target.Shard)
		merged.Shard = target
		merged.Processed = predecessor.Processed
		merged.OutMsgQueueInfo = predecessor.OutMsgQueueInfo
		predecessor = merged
	case covering == 2 && children == 2 && ancestor < 0:
		// C++ add_trivial_neighbor case 4. The masterchain may keep both child
		// descriptors until it registers a later block of the merged chain.
		// They are both replaced by the already-merged linear predecessor.
	case covering > 1:
		return nil, fmt.Errorf("%w: %d neighbors cover this shard, want at most one", ErrInvalidInput, covering)
	case children != 0:
		return nil, fmt.Errorf("%w: one merge child covers this shard without its sibling", ErrInvalidInput)
	}
	if covering == 0 {
		for i := range neighbors {
			effective = append(effective, effectiveNeighbor{Neighbor: neighbors[i], origin: i})
		}
		return append(effective, effectiveNeighbor{Neighbor: predecessor, origin: -1}), nil
	}

	if ancestor >= 0 {
		sibling, err := siblingTrivialNeighbor(&neighbors[ancestor], target)
		if err != nil {
			return nil, err
		}
		for i := range neighbors {
			if i == ancestor {
				effective = append(effective, effectiveNeighbor{Neighbor: sibling, origin: i})
				continue
			}
			effective = append(effective, effectiveNeighbor{Neighbor: neighbors[i], origin: i})
		}
		// Immediate and continued after-split both append the local predecessor
		// after replacing the registered ancestor with the virtual sibling.
		effective = append(effective, effectiveNeighbor{Neighbor: predecessor, origin: -1})
		return effective, nil
	}

	for i := range neighbors {
		neighbor := &neighbors[i]
		coveringTarget := neighbor.Shard.Workchain == target.Workchain &&
			sharddomain.Intersects(int64(neighbor.Shard.Shard), int64(target.Shard))
		if !coveringTarget {
			effective = append(effective, effectiveNeighbor{Neighbor: *neighbor, origin: i})
			continue
		}
		if i == firstCovering {
			// Normal, continued-after-merge and immediate-after-merge all replace
			// the first covering entry in place; any second merge child is disabled.
			effective = append(effective, effectiveNeighbor{Neighbor: predecessor, origin: -1})
		}
	}

	return effective, nil
}

// siblingTrivialNeighbor narrows a still-registered ancestor to our sibling.
// The ancestor's processed frontier is the one both children inherited at the
// split, so the sibling's view is that frontier restricted to its shard.
func siblingTrivialNeighbor(ancestor *Neighbor, target msgpool.ShardIdent) (Neighbor, error) {
	siblingID, err := directMasterShardSibling(int64(target.Shard))
	if err != nil {
		return Neighbor{}, fmt.Errorf("%w: derive sibling shard: %v", ErrInvalidInput, err)
	}
	block := cloneBlockID(ancestor.Block)
	block.Shard = siblingID

	return Neighbor{
		Block:           block,
		Shard:           msgpool.ShardIdent{Workchain: target.Workchain, Shard: uint64(siblingID)},
		EndLT:           ancestor.EndLT,
		Processed:       splitProcessedRecords(ancestor.Processed, siblingID),
		OutMsgQueueInfo: ancestor.OutMsgQueueInfo,
	}, nil
}

// effectiveNeighbors is the collation's own normalized neighbor set. Queue
// cleanup is the only phase that reasons about neighbors; inbound imports are
// classified by the message's own current hop instead.
func (c *collation) effectiveNeighbors() ([]Neighbor, error) {
	records, err := c.parentProcessedRecords()
	if err != nil {
		return nil, err
	}
	block := cloneBlockID(c.req.previous.ID)
	block.Shard = int64(c.shard.Shard)
	predecessor := Neighbor{
		Block:     block,
		Shard:     c.shard,
		EndLT:     c.oldState.GenLT,
		Processed: records,
	}

	entries, err := normalizeTrivialNeighbors(
		c.req.neighbors,
		c.shard,
		predecessor,
		c.topology.kind == topologyAfterMerge,
	)
	if err != nil {
		return nil, err
	}
	neighbors := make([]Neighbor, len(entries))
	for i := range entries {
		neighbors[i] = entries[i].Neighbor
	}

	return neighbors, nil
}
