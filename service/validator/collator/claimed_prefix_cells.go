package collator

import (
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// claimedPrefixCells carries the predecessor outbound-queue cells that
// cleanupClaimedLocalDequeues parsed over to
// traceProcessedQueueValidationClosure.
//
// The two passes walk the identical region — same dictionary, same target
// shard, same claimed bound, the same walkSemanticQueuePrefix call — and differ
// only in what may become of the reads. Cleanup runs before the Merkle update
// and must not record: a read taken there lands in the update's OLD side, which
// cost 697 block bytes on the mainnet fixture and 9175 on the three-times arm.
// The closure runs after it and must record, because that region is exactly
// what a validator opens and the collated proof has to carry it. So the
// RECORDING has to stay split, and the split is byte-load-bearing.
//
// The TRAVERSAL does not. Nothing about it depends on which side of the update
// it happens on, and doing it twice resolved every lazy placeholder over the
// prefix twice, because resolving one is never memoised back into the tree.
// Cleanup therefore keeps what its walk parsed here and the closure hands the
// same cells to ReadSet.Record once the update exists. Record is the entry
// point OnLoad itself uses, so the recorded set, its dedup and the
// collated-size estimator behind it cannot tell a cell that arrived this way
// from one a second walk would have produced.
//
// Only the walk's own reads belong here. Cleanup's visit callback does work the
// closure does not — the processed-info check and the dequeue — and a cell read
// there is not in the set the closure records, so collection is suspended
// around it.
type claimedPrefixCells struct {
	cells  []*cell.Cell
	paused bool

	// The stamp is what makes the replay a decision instead of an assumption.
	// It is taken from the arguments of the walk that produced the cells, and
	// the closure replays only when its own arguments match. A predecessor queue
	// that is not the one walked, a different target shard, a bound recomputed
	// after cleanup, a walk that stopped early, or a topology where cleanup
	// returns before walking at all — all of them fall through to the walk with
	// nothing assumed.
	root     cell.Hash
	shard    msgpool.ShardIdent
	bound    semanticMessageBound
	complete bool
}

// collect is the ReadSet ignored-cell observer. It runs on the collating
// goroutine inside the ignore scope, which is single-threaded by the same rule
// that makes the scope itself safe.
func (p *claimedPrefixCells) collect(parsed *cell.Cell) {
	if p.paused {
		return
	}
	p.cells = append(p.cells, parsed)
}

// suspend and resume bracket the reads that are cleanup's own rather than the
// walk's.
func (p *claimedPrefixCells) suspend() { p.paused = true }

func (p *claimedPrefixCells) resume() { p.paused = false }

// stamp marks the collection as the complete parse of a walk over these
// arguments. It is called only after the walk returned without error.
func (p *claimedPrefixCells) stamp(queue *tlb.OutMsgQueueAugDict, shard msgpool.ShardIdent, bound semanticMessageBound) {
	p.root = claimedPrefixQueueRoot(queue)
	p.shard = shard
	p.bound = bound
	p.complete = true
}

// serves reports whether these cells are what a walk over this queue, shard and
// bound would parse.
func (p *claimedPrefixCells) serves(queue *tlb.OutMsgQueueAugDict, shard msgpool.ShardIdent, bound semanticMessageBound) bool {
	return p.complete &&
		p.root == claimedPrefixQueueRoot(queue) &&
		p.shard == shard &&
		p.bound == bound
}

// release drops the cells once they are in the read set. They are retained
// there anyway; what this ends is the extra pin the collection added between
// cleanup and the update.
func (p *claimedPrefixCells) release() {
	p.cells = nil
	p.complete = false
}

// claimedPrefixQueueRoot identifies the trie the cells came out of. An absent
// root hashes to zero on both sides and describes the same walk: one that
// visits nothing.
func claimedPrefixQueueRoot(queue *tlb.OutMsgQueueAugDict) cell.Hash {
	if queue == nil {
		return cell.Hash{}
	}
	root := queue.RootCell()
	if root == nil {
		return cell.Hash{}
	}
	return root.HashKey()
}
