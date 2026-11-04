package simplex

import (
	"math"
	"testing"
)

func TestRejectValidatorWeightOverflow(t *testing.T) {
	for _, tc := range []struct {
		name    string
		weights []uint64
	}{
		{"single excessive weight", []uint64{math.MaxUint64}},
		{"wrap to zero", []uint64{1, math.MaxUint64}},
		{"wrap after maximum total", []uint64{maxTotalWeight, math.MaxUint64 - maxTotalWeight + 1}},
		{"total above maximum", []uint64{maxTotalWeight, 1}},
		{"weight above maximum", []uint64{maxTotalWeight + 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			validators, _ := testValidators(len(tc.weights))
			for i, weight := range tc.weights {
				validators[i].Weight = weight
			}
			if verifier, err := NewCertificateVerifier([32]byte{}, validators); err == nil {
				t.Errorf("verifier accepted overflowing weights with threshold %d", verifier.threshold)
			}
			if _, err := NewEngine(Config{
				Validators: validators, LocalIndex: ObserverIndex,
				SlotsPerLeaderWindow: 4, ProtocolVersion: 3,
				Journal: NewMemoryJournal(len(validators)), Transport: &fakeTransport{}, Hooks: acceptingHooks{},
			}); err == nil {
				t.Error("engine accepted overflowing validator weights")
			}
		})
	}
}

func TestValidatorWeightMaximumRemainsValid(t *testing.T) {
	validators, _ := testValidators(2)
	validators[0].Weight = maxTotalWeight - 1
	validators[1].Weight = 1
	verifier, err := NewCertificateVerifier([32]byte{}, validators)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := verifier.threshold, maxTotalWeight*2/3+1; got != want {
		t.Fatalf("threshold=%d, want=%d", got, want)
	}
	eng, err := NewEngine(Config{
		Validators: validators, LocalIndex: ObserverIndex,
		SlotsPerLeaderWindow: 4, ProtocolVersion: 3,
		Journal: NewMemoryJournal(len(validators)), Transport: &fakeTransport{}, Hooks: acceptingHooks{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if eng.totalWeight != maxTotalWeight {
		t.Fatalf("total weight=%d, want=%d", eng.totalWeight, maxTotalWeight)
	}
}
