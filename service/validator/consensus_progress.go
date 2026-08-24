package validator

import (
	"errors"

	"github.com/xssnick/gton/service/validator/collator"
)

// collatorConsensusProgress is the one in-process boundary from the Simplex
// runtime to a local collator. The selected roots are immutable and remain
// resolver-owned; the collator receives an exact candidate-bound capability.
func collatorConsensusProgress(
	sessionID [32]byte,
	progress sessionConsensusProgress,
) (collator.ConsensusProgress, error) {
	converted := collator.ConsensusProgress{
		SessionID: sessionID,
		Window:    progress.Window,
		StartAt:   progress.StartAt,
	}
	if !progress.Window.Base.Exists {
		if progress.BaseState != nil {
			return collator.ConsensusProgress{}, errors.New("validator runtime: consensus genesis carries a selected base state")
		}

		return converted, nil
	}
	if progress.BaseState == nil || len(progress.BaseState.tips) != 1 {
		return collator.ConsensusProgress{}, errors.New("validator runtime: selected consensus base is not a normal state")
	}
	tip := progress.BaseState.tips[0]
	base, err := collator.NewSelectedBaseState(
		sessionID,
		progress.Window.Base.ID,
		tip.ID,
		tip.BlockBOC,
		tip.Block,
		tip.State,
	)
	if err != nil {
		return collator.ConsensusProgress{}, err
	}
	converted.Base = base

	return converted, nil
}
