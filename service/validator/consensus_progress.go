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

// collatorSpeculativeWindow binds one observer bet to the exact block and state
// the collator will build on. Like the progress conversion above, the roots stay
// resolver-owned and the collator receives a candidate-bound capability rather
// than raw cells.
func collatorSpeculativeWindow(
	sessionID [32]byte,
	window sessionSpeculativeWindow,
) (collator.SpeculativeWindowRequest, error) {
	if window.BaseState == nil || len(window.BaseState.tips) != 1 {
		return collator.SpeculativeWindowRequest{}, errors.New(
			"validator runtime: speculative base is not a normal state",
		)
	}
	tip := window.BaseState.tips[0]
	base, err := collator.NewSelectedBaseState(
		sessionID,
		window.Base,
		tip.ID,
		tip.BlockBOC,
		tip.Block,
		tip.State,
	)
	if err != nil {
		return collator.SpeculativeWindowRequest{}, err
	}

	return collator.SpeculativeWindowRequest{
		SessionID: sessionID,
		StartSlot: window.StartSlot,
		Leader:    window.Leader,
		Base:      base,
		StartAt:   window.StartAt,
		Deadline:  window.Deadline,
	}, nil
}
