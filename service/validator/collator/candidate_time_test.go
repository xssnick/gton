package collator

import (
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xssnick/tonutils-go/tvm"
)

func TestCandidateGenerationTimeBoundary(t *testing.T) {
	now := time.Unix(1_700_000_000, 999_999_999)
	for _, test := range []struct {
		name   string
		delta  int64
		reject bool
	}{
		{name: "past", delta: -1},
		{name: "present"},
		{name: "thirty seconds", delta: 30},
		{name: "thirty one seconds", delta: 31, reject: true},
		{name: "one hour", delta: 3600, reject: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := verifyCandidateGenerationTime(uint32(now.Unix()+test.delta), now)
			if errors.Is(err, ErrInvalidInput) != test.reject {
				t.Fatalf("generation time delta %d: %v", test.delta, err)
			}
		})
	}
}

func TestPublicCandidateVerificationChecksFutureTime(t *testing.T) {
	for _, masterchain := range []bool{false, true} {
		name := "shardchain"
		if masterchain {
			name = "masterchain"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const now = int64(1_900_000_000)
				time.Sleep(time.Unix(now, 0).Sub(time.Now()))
				for _, delta := range []int64{30, 31} {
					var err error
					header := HeaderParams{GenUtime: uint32(now + delta), GenUtimeMS: uint64(now+delta) * 1000}
					if masterchain {
						fixture := newMasterBuildFixtureWith(t, masterBuildFixtureOptions{genUtime: uint32(now - 1)})
						fixture.request.Header = header
						candidate, buildErr := testBuilder().BuildMaster(t.Context(), fixture.request)
						if buildErr != nil {
							t.Fatal(buildErr)
						}
						err = VerifyMasterCandidate(t.Context(), MasterVerificationRequest{
							Previous: fixture.request.Previous, Config: fixture.request.Config, Groups: fixture.request.Groups,
							ShardTops: fixture.request.ShardTops, Neighbors: fixture.request.Neighbors,
							NeighborShardEndLT: fixture.request.NeighborShardEndLT,
							Semantics:          NewSemanticVerifier(tvm.NewTVM()), Candidate: candidate,
						})
					} else {
						req := emptyCandidateRequest(t)
						req.Header = header
						candidate, buildErr := testBuilder().BuildShard(t.Context(), req)
						if buildErr != nil {
							t.Fatal(buildErr)
						}
						verification := shardVerificationRequest(req, candidate)
						verification.Semantics = NewSemanticVerifier(tvm.NewTVM())
						err = VerifyShardCandidate(t.Context(), verification)
					}
					if delta == 30 && err != nil {
						t.Fatalf("candidate on the future boundary: %v", err)
					}
					if delta == 31 && (!errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "30 seconds")) {
						t.Fatalf("candidate beyond the future boundary: %v", err)
					}
				}
			})
		})
	}
}

func TestLocalCandidateValidationChecksFutureTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// The live fixture's candidate is at 1700000001. Keep it one second
		// beyond the admission window, then advance onto the exact boundary.
		time.Sleep(time.Unix(1_700_000_001-31, 0).Sub(time.Now()))
		acquisition, request, _ := advSessionValidation(t)
		acquisition.semantics = NewSemanticVerifier(tvm.NewTVM())
		if _, err := acquisition.ValidateCandidate(t.Context(), request); !errors.Is(err, ErrInvalidInput) ||
			!strings.Contains(err.Error(), "30 seconds") {
			t.Fatalf("live future candidate: %v", err)
		}
		time.Sleep(time.Second)
		result, err := acquisition.ValidateCandidate(t.Context(), request)
		if err != nil {
			t.Fatalf("live candidate on the future boundary: %v", err)
		}
		if result.ValidAfter.Unix() != 1_700_000_001 {
			t.Fatalf("valid-after = %v", result.ValidAfter)
		}
	})
}
