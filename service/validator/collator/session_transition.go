package collator

import (
	"github.com/xssnick/tonutils-go/ton"

	"github.com/xssnick/gton/service/validator/simplex"
)

// PreserveProgress carries forward session-local observations which a newer
// masterchain snapshot cannot revoke. In particular, a split or merge target
// may have no exact shard descriptor yet. C++ skips notify_mc_finalized in that
// state, so absence means "no new observation", not that the last finalized
// block was cleared.
func (u *SessionUpdate) PreserveProgress(current SessionUpdate) {
	if current.HasFinalizedBlock && !u.HasFinalizedBlock {
		u.HasFinalizedBlock = true
		u.FinalizedBlock = cloneBlockID(current.FinalizedBlock)
	}
	if current.HasCurrentWindow && !u.HasCurrentWindow {
		u.HasCurrentWindow = true
		u.CurrentWindowStart = current.CurrentWindowStart
		u.CurrentWindowObservedSlot = current.CurrentWindowObservedSlot
		u.CurrentWindowStartAt = current.CurrentWindowStartAt
		u.CurrentBase = current.CurrentBase
	}
}

// ValidateSessionUpdateAdvance verifies that next can replace an already
// durable session view without rewriting authenticated chain history. It is
// shared by the runtime and storage so opaque pipeline state is never mutated
// for an update the WAL would reject.
func ValidateSessionUpdateAdvance(old, next SessionUpdate) error {
	if old.SessionID != next.SessionID {
		return ErrSessionConflict
	}
	if !blockAdvances(old.MasterchainBlock, next.MasterchainBlock) {
		return ErrSessionConflict
	}
	if old.HasFinalizedBlock && !next.HasFinalizedBlock {
		return ErrSessionConflict
	}
	if old.HasFinalizedBlock && next.HasFinalizedBlock &&
		!blockAdvances(old.FinalizedBlock, next.FinalizedBlock) {
		return ErrSessionConflict
	}
	if old.HasCurrentWindow && !next.HasCurrentWindow {
		return ErrSessionConflict
	}
	if old.HasCurrentWindow && next.HasCurrentWindow {
		if next.CurrentWindowStart < old.CurrentWindowStart {
			return ErrSessionConflict
		}
		if next.CurrentWindowStart == old.CurrentWindowStart {
			if !next.CurrentWindowStartAt.Equal(old.CurrentWindowStartAt) ||
				next.CurrentWindowObservedSlot < old.CurrentWindowObservedSlot {
				return ErrSessionConflict
			}
		}
	}
	if !parentAdvances(old.CurrentBase, next.CurrentBase) {
		return ErrSessionConflict
	}

	return nil
}

func blockAdvances(old, next ton.BlockIDExt) bool {
	if old.Workchain != next.Workchain || old.Shard != next.Shard || next.SeqNo < old.SeqNo {
		return false
	}
	if next.SeqNo == old.SeqNo {
		return sameBlockID(old, next)
	}

	return true
}

func parentAdvances(old, next simplex.ParentID) bool {
	if old.Exists && !next.Exists {
		return false
	}
	if !old.Exists || !next.Exists {
		return true
	}
	if next.ID.Slot < old.ID.Slot {
		return false
	}
	if next.ID.Slot == old.ID.Slot {
		return next.ID == old.ID
	}

	return true
}
