package groups

import (
	"encoding/binary"
	"testing"
)

var benchmarkRoster []Validator

func BenchmarkSelectParsedShardRoster(b *testing.B) {
	validators := make([]Validator, 100)
	cumulativeWeights := make([]uint64, len(validators))
	totalWeight := uint64(0)
	for i := range validators {
		weight := uint64(i%17 + 1)
		validators[i] = Validator{
			PublicKey:     groupTestBytes(byte(i)),
			PublicKeyHash: groupTestBytes(byte(i + 1)),
			ADNL:          groupTestBytes(byte(i + 2)),
			Weight:        weight,
		}
		binary.BigEndian.PutUint32(validators[i].PublicKey[:4], uint32(i))
		binary.BigEndian.PutUint32(validators[i].PublicKeyHash[:4], uint32(i))
		cumulativeWeights[i] = totalWeight
		totalWeight += weight
	}
	input := RosterInput{
		Set: ValidatorSet{
			Main:              100,
			TotalWeight:       totalWeight,
			Validators:        validators,
			cumulativeWeights: cumulativeWeights,
		},
		Workchain:     0,
		Shard:         0x4000000000000000,
		CatchainSeqno: 17,
		Catchain:      CatchainConfig{ShardValidators: 23},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		benchmarkRoster = selectRoster(input)
	}
}

func BenchmarkSelectParsedLargeShardRoster(b *testing.B) {
	const validatorCount = 4096

	validators := make([]Validator, validatorCount)
	cumulativeWeights := make([]uint64, len(validators))
	totalWeight := uint64(0)
	for i := range validators {
		weight := uint64(i%17 + 1)
		validators[i] = Validator{
			PublicKey:     groupTestBytes(byte(i)),
			PublicKeyHash: groupTestBytes(byte(i + 1)),
			ADNL:          groupTestBytes(byte(i + 2)),
			Weight:        weight,
		}
		binary.BigEndian.PutUint32(validators[i].PublicKey[:4], uint32(i))
		binary.BigEndian.PutUint32(validators[i].PublicKeyHash[:4], uint32(i))
		cumulativeWeights[i] = totalWeight
		totalWeight += weight
	}
	input := RosterInput{
		Set: ValidatorSet{
			Main:              validatorCount,
			TotalWeight:       totalWeight,
			Validators:        validators,
			cumulativeWeights: cumulativeWeights,
		},
		Workchain:     0,
		Shard:         0x4000000000000000,
		CatchainSeqno: 17,
		Catchain:      CatchainConfig{ShardValidators: validatorCount},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		benchmarkRoster = selectRoster(input)
	}
}
