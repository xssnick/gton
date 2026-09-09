package collator

import "context"

// Historical fixtures pin block bytes and proof read sets. These helpers run
// the same transition checks without today's wall-clock admission bound; the
// public entry points and the bound are covered by candidate_time_test.go.
func verifyShardCandidateForTest(ctx context.Context, req ShardVerificationRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	previous := []PreviousBlock{req.Previous}
	if req.Previous2 != nil {
		previous = append(previous, *req.Previous2)
	}
	prepared, err := prepareVerificationCandidate(ctx, req.Masterchain.Config, req.Candidate, previous)
	if err != nil {
		return err
	}

	return verifyPreparedShardCandidate(ctx, req, prepared)
}

func verifyMasterCandidateForTest(ctx context.Context, req MasterVerificationRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	prepared, err := prepareVerificationCandidate(ctx, req.Config, req.Candidate, []PreviousBlock{req.Previous})
	if err != nil {
		return err
	}

	return verifyPreparedMasterCandidate(ctx, req, prepared)
}
