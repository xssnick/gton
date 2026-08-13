package collator

import (
	"fmt"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// Full collated data makes a shard candidate self-contained: every state cell
// its validation reads is proven by the candidate itself. This file is what
// makes the deterministic verifier execute against those proofs rather than
// against whatever resident state this node happens to hold.
//
// The difference is not academic. Reading the resident tree accepts a candidate
// whose collated data is missing a branch the replay walked, because the local
// tree answers where the proof would have stopped at a pruned boundary. The
// reference validator has no such fallback: it virtualizes the collated
// predecessor, applies the state update to that, and every read leaving the
// proven closure fails with "prunned branch". A candidate this node accepts and
// the reference node rejects is the worst outcome available, so the local tree
// is deliberately given up here.
//
// The masterchain deliberately keeps reading its resident state: collated data
// carries no masterchain state proof (see buildCollatedRoots), because every
// validator already holds that state. Only shard validation switches.

// proofBackedPredecessors re-points the ordered predecessors at the states
// proven by the candidate's own collated data. The proofs are already bound to
// these exact block ids — and, while a resident state is still available, to
// its content — by verifyCollatedPredecessors, so this substitutes the source
// of the cells without weakening what identifies them.
func proofBackedPredecessors(
	collated *verifiedCollatedData,
	first PreviousBlock,
	second *PreviousBlock,
) (PreviousBlock, *PreviousBlock, error) {
	state, err := collatedStateRoot(collated, first.ID)
	if err != nil {
		return PreviousBlock{}, nil, fmt.Errorf("%w: proven first predecessor state: %v", ErrInvalidInput, err)
	}
	first.State = state
	if second == nil {
		return first, nil, nil
	}

	state, err = collatedStateRoot(collated, second.ID)
	if err != nil {
		return PreviousBlock{}, nil, fmt.Errorf("%w: proven second predecessor state: %v", ErrInvalidInput, err)
	}
	// The caller borrows this pointer from its request, so the substitution
	// lands on a copy rather than on the request the session still owns.
	proven := *second
	proven.State = state

	return first, &proven, nil
}

// provenPredecessorStates re-points an ordered predecessor list at the states
// the candidate's own collated data proves.
//
// This is what lets a node validate a shard whose state it does not hold: the
// successor tree is applied on top of these roots, so nothing downstream — not
// the structural pass, not the replay — ever reaches for a resident cell. The
// roots are authenticated by the predecessor block ids, which arrive from
// consensus, and verifyCollatedPredecessors additionally binds them to the
// resident state whenever the node still happens to have one.
func provenPredecessorStates(
	collated *verifiedCollatedData,
	previous []PreviousBlock,
) ([]PreviousBlock, error) {
	proven := make([]PreviousBlock, len(previous))
	copy(proven, previous)
	for i := range proven {
		state, err := collatedStateRoot(collated, proven[i].ID)
		if err != nil {
			return nil, fmt.Errorf("%w: proven predecessor %d state: %v", ErrInvalidInput, i, err)
		}
		proven[i].State = state
	}

	return proven, nil
}

// bindProofBackedCandidateState rebuilds the candidate state on top of the
// proven predecessor and re-decodes every view the verifier and the replay take
// from it.
//
// The rebuilt root carries the same hash as the one restored from the resident
// predecessor — both are the state update's target — so every structural check
// already made against that root still holds. What changes is materialization:
// a subtree the block did not touch is now the pruned boundary the collated
// proof left there, unless the producer proved it. Re-decoding the header,
// statistics and queue here rather than trusting the earlier decode is the
// point: the reference validator reads them off the virtualized state too, so a
// proof that omits one has to fail on this node as well.
func bindProofBackedCandidateState(verified *verifiedCandidate, oldRoot *cell.Cell) error {
	stateRoot, err := cell.ApplyMerkleUpdate(oldRoot.WithoutTrace(), verified.block.StateUpdate)
	if err != nil {
		return fmt.Errorf(
			"%w: apply candidate state update to the proven predecessor: %v",
			ErrInvalidInput,
			err,
		)
	}

	var state tlb.ShardStateUnsplit
	if err = parseExact(&state, stateRoot); err != nil {
		return fmt.Errorf("%w: decode proven candidate state: %v", ErrInvalidInput, err)
	}
	var stats tlb.ShardStateStats
	if err = parseExact(&stats, state.Stats); err != nil {
		return fmt.Errorf("%w: decode proven candidate state statistics: %v", ErrInvalidInput, err)
	}
	var queue tlb.OutMsgQueueInfo
	if err = parseExact(&queue, state.OutMsgQueueInfo); err != nil {
		return fmt.Errorf("%w: decode proven candidate outbound queue: %v", ErrInvalidInput, err)
	}
	if queue.OutQueue == nil || queue.ProcInfo == nil || !queue.ProcInfo.ValidateAll() ||
		(queue.Extra != nil && queue.Extra.DispatchQueue == nil) {
		return fmt.Errorf("%w: invalid proven candidate outbound queue dictionaries", ErrInvalidInput)
	}
	if err = verifyAccountsShardPrefix(state.ShardIdent, state.Accounts.ShardAccounts); err != nil {
		return err
	}

	verified.state = state
	verified.stats = stats
	verified.queue = queue

	return nil
}
