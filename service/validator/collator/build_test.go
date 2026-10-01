package collator

import (
	"errors"
	"math"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
)

func TestClampLocalHeaderTime(t *testing.T) {
	tests := []struct {
		name          string
		globalVersion uint32
		header        HeaderParams
		bound         uint32
		want          HeaderParams
		wantError     bool
	}{
		{
			name:          "v13 keeps later slot milliseconds",
			globalVersion: 13,
			header:        HeaderParams{GenUtime: 101, GenUtimeMS: 101_375},
			bound:         100,
			want:          HeaderParams{GenUtime: 101, GenUtimeMS: 101_375},
		},
		{
			name:          "v13 clamps behind slot to equal second",
			globalVersion: 13,
			header:        HeaderParams{GenUtime: 99, GenUtimeMS: 99_875},
			bound:         100,
			want:          HeaderParams{GenUtime: 100, GenUtimeMS: 100_000},
		},
		{
			name:          "v12 advances one full second",
			globalVersion: 12,
			header:        HeaderParams{GenUtime: 100, GenUtimeMS: 100_875},
			bound:         100,
			want:          HeaderParams{GenUtime: 101, GenUtimeMS: 101_000},
		},
		{
			name:          "v12 keeps an already valid fractional second",
			globalVersion: 12,
			header:        HeaderParams{GenUtime: 101, GenUtimeMS: 101_375},
			bound:         100,
			want:          HeaderParams{GenUtime: 101, GenUtimeMS: 101_375},
		},
		{
			name:          "strict successor overflows protocol time",
			globalVersion: 12,
			header:        HeaderParams{GenUtime: math.MaxUint32, GenUtimeMS: uint64(math.MaxUint32) * 1000},
			bound:         math.MaxUint32,
			wantError:     true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := clampLocalHeaderTime(test.header, test.globalVersion, test.bound)
			if (err != nil) != test.wantError {
				t.Fatalf("clamp error = %v, want error %t", err, test.wantError)
			}
			if err != nil {
				if !errors.Is(err, ErrInvalidInput) {
					t.Fatalf("clamp error = %v, want ErrInvalidInput", err)
				}
				return
			}
			if got != test.want {
				t.Fatalf("clamped header = %+v, want %+v", got, test.want)
			}
			if err = requireGenUtimeMonotonic(
				test.globalVersion,
				got.GenUtime,
				test.bound,
				"the bound",
			); err != nil {
				t.Fatalf("clamped header remains invalid: %v", err)
			}
		})
	}
}

// TestRequireGenUtimeMonotonic pins the gen_utime monotonicity rule at the
// version-13 boundary and shows that validateBlockTime — the shard collation
// entry point — returns the same verdict for the same triple.
//
// makeMasterHeader (collation) and verifyMasterHeaderAndGroups (validation)
// call the same helper, so they cannot disagree with it. verify.go's shard
// validation still carries its own inline copy of the comparison; it is the one
// site this package cannot route through the helper from here.
func TestRequireGenUtimeMonotonic(t *testing.T) {
	tests := []struct {
		name          string
		globalVersion uint32
		genUtime      uint32
		minGenUtime   uint32
		wantReject    bool
	}{
		{name: "v13 equal", globalVersion: 13, genUtime: 100, minGenUtime: 100},
		{name: "v13 ahead", globalVersion: 13, genUtime: 101, minGenUtime: 100},
		{name: "v13 behind", globalVersion: 13, genUtime: 99, minGenUtime: 100, wantReject: true},
		{name: "v14 equal", globalVersion: 14, genUtime: 100, minGenUtime: 100},
		{name: "v12 equal", globalVersion: 12, genUtime: 100, minGenUtime: 100, wantReject: true},
		{name: "v12 ahead", globalVersion: 12, genUtime: 101, minGenUtime: 100},
		{name: "v12 behind", globalVersion: 12, genUtime: 99, minGenUtime: 100, wantReject: true},
		{name: "v0 equal", globalVersion: 0, genUtime: 7, minGenUtime: 7, wantReject: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := requireGenUtimeMonotonic(test.globalVersion, test.genUtime, test.minGenUtime, "the bound")
			if (err != nil) != test.wantReject {
				t.Fatalf("helper verdict = %v, want reject %t", err, test.wantReject)
			}
			if err != nil && !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("helper error = %v, want ErrInvalidInput", err)
			}

			var header tlb.BlockHeader
			header.GenUtime = test.genUtime
			// The shard bound is max(predecessor, masterchain reference); feed the
			// same value through each of its two halves.
			for _, split := range [2][2]uint32{
				{test.minGenUtime, 0},
				{0, test.minGenUtime},
			} {
				shardErr := validateBlockTime(
					header,
					&tlb.ShardStateUnsplit{GenUTime: split[0]},
					split[1],
					test.globalVersion,
				)
				if (shardErr != nil) != test.wantReject {
					t.Fatalf("validateBlockTime verdict = %v, want reject %t", shardErr, test.wantReject)
				}
			}
		})
	}
}
