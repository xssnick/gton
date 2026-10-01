package groups

import (
	"encoding/binary"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/ton"
)

type testTentativeValidatorSetCase struct {
	name      string
	input     TentativeValidatorSetInput
	wantKey   byte
	wantError string
}

func TestValidatorSetPRNGGolden(t *testing.T) {
	want := []uint64{
		0x7c4a5f98284ba818,
		0x108ce50effa6b10a,
		0xe5e4c2da6ae84443,
		0x7b0f07a2145919cf,
		0xae6c841734fad774,
		0x33c98ed95cd3b403,
		0xa2d24b30954a22c3,
		0xec9ca1a5b59dbbaf,
		0x165eec092a69b1d3,
		0xd33409e6bc119bcc,
	}
	rng := ton.NewValidatorSetPRNG(math.MinInt64, -1, 0x11223344, nil)
	for i, expected := range want {
		if got := rng.NextUint64(); got != expected {
			t.Fatalf("PRNG word %d = %016x, want %016x", i, got, expected)
		}
	}

	rng = ton.NewValidatorSetPRNG(math.MinInt64, -1, 0x11223344, nil)
	if got := rng.NextRanged(7); got != 3 {
		t.Fatalf("PRNG ranged value = %d, want 3", got)
	}
}

func TestSelectMasterchainRosterGolden(t *testing.T) {
	validatorSet := testRosterValidatorSet()

	shuffled := selectRoster(RosterInput{
		Set:           validatorSet,
		Workchain:     -1,
		Shard:         0x8000000000000000,
		CatchainSeqno: 17,
		Catchain:      CatchainConfig{ShuffleMasterchain: true},
	})
	assertRosterKeys(t, shuffled, []byte{2, 1, 3, 4})
	for i := range shuffled {
		if shuffled[i].Weight != validatorSet.Validators[int([]byte{1, 0, 2, 3}[i])].Weight {
			t.Fatalf("shuffled validator %d weight = %d", i, shuffled[i].Weight)
		}
	}

	unshuffled := selectRoster(RosterInput{
		Set:       validatorSet,
		Workchain: -1,
		Shard:     0x8000000000000000,
	})
	assertRosterKeys(t, unshuffled, []byte{1, 2, 3, 4})
}

func TestSelectShardRosterGolden(t *testing.T) {
	roster := selectRoster(RosterInput{
		Set:           testRosterValidatorSet(),
		Workchain:     0,
		Shard:         0x4000000000000000,
		CatchainSeqno: 17,
		Catchain: CatchainConfig{
			ShardValidators: 3,
		},
	})

	assertRosterKeys(t, roster, []byte{4, 5, 2})
	for i := range roster {
		if roster[i].Weight != 1 {
			t.Fatalf("shard validator %d weight = %d, want 1", i, roster[i].Weight)
		}
	}
}

func TestFenwickShardRosterMatchesHoleSelection(t *testing.T) {
	tests := []struct {
		name  string
		count int
		pick  uint32
	}{
		{name: "threshold", count: 65, pick: 64},
		{name: "threshold plus one", count: 65, pick: 65},
		{name: "below power of two", count: 127, pick: 65},
		{name: "power of two", count: 128, pick: 128},
		{name: "above power of two", count: 257, pick: 129},
		{name: "large full roster", count: 1024, pick: 1024},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			validators := make([]Validator, test.count)
			cumulativeWeights := make([]uint64, test.count)
			totalWeight := uint64(0)
			for i := range validators {
				weight := uint64(i%23 + 1)
				if i%31 == 0 {
					weight *= 10_000
				}
				binary.BigEndian.PutUint32(validators[i].PublicKey[:4], uint32(i))
				validators[i].PublicKeyHash = validators[i].PublicKey
				validators[i].ADNL = validators[i].PublicKey
				validators[i].Weight = weight
				cumulativeWeights[i] = totalWeight
				totalWeight += weight
			}

			for catchainSeqno := uint32(0); catchainSeqno < 8; catchainSeqno++ {
				input := RosterInput{
					Set: ValidatorSet{
						Main:        uint16(test.count),
						TotalWeight: totalWeight,
						Validators:  validators,
					},
					Workchain:     0,
					Shard:         0x4000000000000000,
					CatchainSeqno: catchainSeqno,
					Catchain:      CatchainConfig{ShardValidators: test.pick},
				}
				count := shardRosterCount(input)
				want := selectShardRosterWithHoles(input, cumulativeWeights, count)
				got := selectShardRosterWithFenwick(input, count)
				if !slices.Equal(got, want) {
					t.Fatalf("catchain %d roster differs", catchainSeqno)
				}
				input.Set.cumulativeWeights = cumulativeWeights
				routed := selectShardRosterWithCumulativeWeights(input, cumulativeWeights)
				if !slices.Equal(routed, want) {
					t.Fatalf("catchain %d routed roster differs", catchainSeqno)
				}
			}
		})
	}
}

func TestSelectTentativeValidatorSetBoundary(t *testing.T) {
	current := testSingleValidatorSet(1, 100)
	next := testSingleValidatorSet(2, 400)

	tests := []testTentativeValidatorSetCase{
		{
			name: "next starts after boundary",
			input: TentativeValidatorSetInput{
				Current: current, Next: &next, GenUTime: 199, CatchainLifetime: 200,
			},
			wantKey: 1,
		},
		{
			name: "equality selects next",
			input: TentativeValidatorSetInput{
				Current: current, Next: &next, GenUTime: 200, CatchainLifetime: 200,
			},
			wantKey: 2,
		},
		{
			name: "absent next ignores zero lifetime",
			input: TentativeValidatorSetInput{
				Current: current,
			},
			wantKey: 1,
		},
		{
			name: "zero lifetime",
			input: TentativeValidatorSetInput{
				Current: current, Next: &next,
			},
			wantError: "lifetime must be positive",
		},
		{
			name: "uint32 boundary overflow",
			input: TentativeValidatorSetInput{
				Current: current, Next: &next, GenUTime: math.MaxUint32, CatchainLifetime: 200,
			},
			wantError: "overflows uint32",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			selected, err := SelectTentativeValidatorSet(test.input)
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("SelectTentativeValidatorSet error = %v, want %q", err, test.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("SelectTentativeValidatorSet: %v", err)
			}
			if got := selected.Validators[0].PublicKey[0]; got != test.wantKey {
				t.Fatalf("selected key prefix = %d, want %d", got, test.wantKey)
			}
		})
	}
}

func TestValidatorSetHashGolden(t *testing.T) {
	validators := []Validator{
		{PublicKey: groupTestBytes(1), ADNL: groupTestBytes(101), Weight: 10},
		{PublicKey: groupTestBytes(2), ADNL: groupTestBytes(102), Weight: 20},
	}

	got, err := ValidatorSetHash(ValidatorSetHashInput{
		CatchainSeqno: 0x11223344,
		Validators:    validators,
	})
	if err != nil {
		t.Fatalf("validator set hash: %v", err)
	}
	if want := uint32(0x1ea85bb2); got != want {
		t.Fatalf("validator set hash = %08x, want %08x", got, want)
	}
}

// buildPersistentOverlayMembers now receives the selected sets, so what it
// still has to get right is the ordering and the ADNL deduplication across
// them. That the persistent sets are the ones selected is pinned end-to-end by
// TestParseConfigPrefersTemporaryValidatorSets.
func TestPersistentOverlayMembersDeduplicatesAcrossSets(t *testing.T) {
	a := groupTestBytes(1)
	b := groupTestBytes(2)
	c := groupTestBytes(3)
	validatorSets := []ValidatorSet{
		{Validators: []Validator{{ADNL: b}}},
		{Validators: []Validator{{PublicKeyHash: a}}},
		{Validators: []Validator{{ADNL: c}, {ADNL: b}}},
	}
	config := &Config{persistentOverlayMembers: buildPersistentOverlayMembers(validatorSets)}

	members := config.PersistentOverlayMembers()
	want := [][32]byte{a, b, c}
	if len(members) != len(want) {
		t.Fatalf("overlay members = %x, want %x", members, want)
	}
	for i := range want {
		if members[i].ADNL != want[i] {
			t.Fatalf("overlay member %d = %x, want %x", i, members[i], want[i])
		}
	}
	if len(members[1].ValidatorKeyIDs) != 1 {
		t.Fatalf("ADNL %x key IDs = %x, want one deduplicated key", b, members[1].ValidatorKeyIDs)
	}
}

func TestPersistentOverlayMembersPreservesValidatorKeyMapping(t *testing.T) {
	sharedADNL := groupTestBytes(80)
	rotatedADNL := groupTestBytes(81)
	firstKey := groupTestBytes(10)
	secondKey := groupTestBytes(11)
	validatorSets := []ValidatorSet{
		{Validators: []Validator{{PublicKeyHash: firstKey, ADNL: sharedADNL}}},
		{Validators: []Validator{
			{PublicKeyHash: secondKey, ADNL: sharedADNL},
			{PublicKeyHash: firstKey, ADNL: rotatedADNL},
		}},
		{Validators: []Validator{{PublicKeyHash: secondKey, ADNL: sharedADNL}}},
	}

	members := buildPersistentOverlayMembers(validatorSets)
	if len(members) != 2 || members[0].ADNL != sharedADNL || members[1].ADNL != rotatedADNL {
		t.Fatalf("persistent overlay members = %+v", members)
	}
	if len(members[0].ValidatorKeyIDs) != 2 ||
		members[0].ValidatorKeyIDs[0] != firstKey || members[0].ValidatorKeyIDs[1] != secondKey {
		t.Fatalf("shared ADNL key IDs = %x", members[0].ValidatorKeyIDs)
	}
	if len(members[1].ValidatorKeyIDs) != 1 || members[1].ValidatorKeyIDs[0] != firstKey {
		t.Fatalf("rotated ADNL key IDs = %x", members[1].ValidatorKeyIDs)
	}
}

func testRosterValidatorSet() ValidatorSet {
	weights := [...]uint64{10, 20, 30, 40, 50}
	validators := make([]Validator, len(weights))
	cumulativeWeights := make([]uint64, len(weights))
	totalWeight := uint64(0)
	for i, weight := range weights {
		validators[i] = Validator{
			PublicKey:     groupTestBytes(byte(i + 1)),
			PublicKeyHash: groupTestBytes(byte(i + 31)),
			ADNL:          groupTestBytes(byte(i + 61)),
			Weight:        weight,
		}
		cumulativeWeights[i] = totalWeight
		totalWeight += weight
	}

	return ValidatorSet{Main: 4, TotalWeight: totalWeight, Validators: validators, cumulativeWeights: cumulativeWeights}
}

func testSingleValidatorSet(key byte, since uint32) ValidatorSet {
	return ValidatorSet{
		Since:       since,
		Main:        1,
		TotalWeight: 1,
		Validators: []Validator{{
			PublicKey:     groupTestBytes(key),
			PublicKeyHash: groupTestBytes(key + 40),
			Weight:        1,
		}},
	}
}

func assertRosterKeys(t *testing.T, roster []Validator, want []byte) {
	t.Helper()

	if len(roster) != len(want) {
		t.Fatalf("roster length = %d, want %d", len(roster), len(want))
	}
	for i := range want {
		if got := roster[i].PublicKey[0]; got != want[i] {
			t.Fatalf("roster key %d prefix = %d, want %d", i, got, want[i])
		}
	}
}
