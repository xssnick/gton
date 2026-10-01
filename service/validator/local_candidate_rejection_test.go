package validator

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/xssnick/gton/service/validator/collator"
)

// A candidate the collator refuses must reach consensus as ErrCandidateRejected:
// the session abstains from notarizing it and records a semantic rejection
// instead of treating the failure as a local fault. Falling through the default
// branch of classifyLocalCandidateError is therefore a consensus-visible
// regression, and it is exactly what a rewrap that drops one of collator's
// rejection sentinels would cause. The prose of the 169 distinct rejection
// reasons is not the invariant; the class is.
func TestClassifyLocalCandidateErrorRejectsCollatorRejections(t *testing.T) {
	ctx := context.Background()

	// Errors produced by the real entry points, not by rewrapping a sentinel.
	shardRejection := collator.VerifyShardCandidate(ctx, collator.ShardVerificationRequest{})
	if shardRejection == nil {
		t.Fatal("VerifyShardCandidate accepted an empty request")
	}
	masterRejection := collator.VerifyMasterCandidate(ctx, collator.MasterVerificationRequest{})
	if masterRejection == nil {
		t.Fatal("VerifyMasterCandidate accepted an empty request")
	}

	cases := []struct {
		name string
		err  error
		want error
	}{
		{name: "shard verification", err: shardRejection, want: ErrCandidateRejected},
		{name: "master verification", err: masterRejection, want: ErrCandidateRejected},
		// The remaining rejection classes need a prepared config and a full
		// candidate to reach through the real API, so they are pinned at the
		// sentinel: the point of the assertion is that classification covers
		// every class collator can reject with, not the wording behind it.
		{
			name: "unsupported block",
			err:  fmt.Errorf("%w: only basechain blocks are supported", collator.ErrUnsupported),
			want: ErrCandidateRejected,
		},
		{
			name: "size limit",
			err:  fmt.Errorf("%w: candidate block is too large", collator.ErrSizeLimit),
			want: ErrCandidateRejected,
		},
		{
			name: "collated root missing",
			err:  fmt.Errorf("%w: state proof is absent", collator.ErrCollatedRootNotFound),
			want: ErrCandidateRejected,
		},
		// Not a rejection: acquisition has not caught up, so the candidate is
		// unjudged and the session must wait rather than vote against it.
		{
			name: "acquisition not ready",
			err:  fmt.Errorf("%w: masterchain state", collator.ErrAcquisitionNotReady),
			want: ErrBlockNotReady,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			classified := classifyLocalCandidateError(tc.err)
			if !errors.Is(classified, tc.want) {
				t.Fatalf("classifyLocalCandidateError(%v) = %v, want %v", tc.err, classified, tc.want)
			}
			if !errors.Is(classified, tc.err) {
				t.Fatalf("classification dropped the original error: %v", classified)
			}
			if tc.want == ErrCandidateRejected && errors.Is(classified, ErrBlockNotReady) {
				t.Fatalf("a rejection was classified as not-ready: %v", classified)
			}
		})
	}
}

// The default branch must stay reachable for genuine local faults: a storage or
// context failure is not the candidate's fault and must not become a vote
// against it.
func TestClassifyLocalCandidateErrorPassesLocalFaultsThrough(t *testing.T) {
	local := errors.New("open state database")
	for _, err := range []error{local, context.Canceled, fmt.Errorf("collate: %w", local)} {
		classified := classifyLocalCandidateError(err)
		if errors.Is(classified, ErrCandidateRejected) || errors.Is(classified, ErrBlockNotReady) {
			t.Fatalf("local fault %v was classified as %v", err, classified)
		}
		if !errors.Is(classified, err) {
			t.Fatalf("classifyLocalCandidateError(%v) = %v, want it unchanged", err, classified)
		}
	}
}
