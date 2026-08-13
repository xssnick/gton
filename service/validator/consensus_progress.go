package validator

import (
	"errors"

	"github.com/xssnick/gton/service/validator/collator"
)

// collatorConsensusProgress is the one in-process boundary from the Simplex
// runtime to a local collator. Candidate buffers are immutable after decoding,
// so this projection deliberately keeps their ownership rather than copying
// block-sized payloads.
func collatorConsensusProgress(
	sessionID [32]byte,
	slotsPerLeaderWindow uint32,
	progress sessionConsensusProgress,
) (collator.ConsensusProgress, error) {
	converted := collator.ConsensusProgress{
		SessionID:  sessionID,
		Window:     progress.Window,
		StartAt:    progress.StartAt,
		Candidates: make([]collator.CandidateArtifact, len(progress.Candidates)),
	}
	if progress.FinalizedAnchor != nil {
		anchor := *progress.FinalizedAnchor.Copy()
		converted.FinalizedAnchor = &anchor
	}
	if progress.AppliedAnchorState != nil {
		if progress.FinalizedAnchor == nil {
			return collator.ConsensusProgress{}, errors.New("validator runtime: applied anchor state has no block id")
		}
		if len(progress.AppliedAnchorState.tips) != 1 {
			return collator.ConsensusProgress{}, errors.New("validator runtime: applied anchor state is not normal")
		}
		tip := progress.AppliedAnchorState.tips[0]
		if !sameBlockID(tip.ID, *progress.FinalizedAnchor) || tip.State == nil {
			return collator.ConsensusProgress{}, errors.New("validator runtime: applied anchor state differs from block id")
		}
		if tip.ID.SeqNo == 0 {
			if len(tip.BlockBOC) != 0 {
				return collator.ConsensusProgress{}, errors.New("validator runtime: zerostate anchor carries block data")
			}
		} else if len(tip.BlockBOC) == 0 {
			return collator.ConsensusProgress{}, errors.New("validator runtime: ordinary anchor block data is absent")
		}

		converted.FinalizedAnchorState = &collator.FinalizedAnchorState{
			Block:    *tip.ID.Copy(),
			BlockBOC: tip.BlockBOC,
			State:    tip.State,
		}
	}
	for i := range progress.Candidates {
		artifact := progress.Candidates[i]
		converted.Candidates[i] = collator.CandidateArtifact{
			SessionID: sessionID,
			WindowID: collator.WindowID{
				SessionID: sessionID,
				StartSlot: artifact.Candidate.ID.Slot - artifact.Candidate.ID.Slot%slotsPerLeaderWindow,
			},
			Candidate:    artifact.Candidate,
			BlockBOC:     artifact.BlockBOC,
			CollatedData: artifact.CollatedData,
		}
	}

	return converted, nil
}
