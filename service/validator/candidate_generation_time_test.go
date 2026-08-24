package validator

import (
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestCandidateArtifactGenerationTimeUsesTrustedScalarWithoutBOCDecode(t *testing.T) {
	want := time.UnixMilli(1_765_432_109_321)
	artifact := &CandidateArtifact{
		CollatedData:        []byte("not a BOC"),
		generationTimeMS:    uint64(want.UnixMilli()),
		generationTimeKnown: true,
	}

	got, err := artifact.generationTime()
	if err != nil {
		t.Fatalf("trusted generation time decoded CollatedData: %v", err)
	}
	if !got.Equal(want) {
		t.Fatalf("generation time = %v, want %v", got, want)
	}
}

func TestCandidateArtifactGenerationTimeFallbackValidatesConsensusExtra(t *testing.T) {
	t.Run("derives exact milliseconds", func(t *testing.T) {
		want := time.UnixMilli(1_765_432_109_321)
		extra := cell.BeginCell().
			MustStoreUInt(consensusExtraDataTag, 32).
			MustStoreUInt(0, 32).
			MustStoreUInt(uint64(want.UnixMilli()), 64).
			EndCell()
		boc, err := cell.ToBOCWithOptionsErr([]*cell.Cell{extra}, cell.BOCSerializeOptions{WithCRC32C: true})
		if err != nil {
			t.Fatal(err)
		}

		source := &CandidateArtifact{CollatedData: boc}
		artifact, err := source.withGenerationTime()
		if err != nil {
			t.Fatal(err)
		}
		if artifact == source || source.generationTimeKnown {
			t.Fatal("fallback timestamp provenance mutated the externally owned artifact")
		}
		if !artifact.generationTimeKnown || artifact.generationTimeMS != uint64(want.UnixMilli()) {
			t.Fatalf("derived generation time = %d, %v; want %d, true",
				artifact.generationTimeMS, artifact.generationTimeKnown, want.UnixMilli())
		}
	})

	t.Run("rejects malformed consensus extra", func(t *testing.T) {
		root := cell.BeginCell().MustStoreUInt(0xdeadbeef, 32).EndCell()
		boc, err := cell.ToBOCWithOptionsErr([]*cell.Cell{root}, cell.BOCSerializeOptions{WithCRC32C: true})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = (&CandidateArtifact{CollatedData: boc}).withGenerationTime(); err == nil {
			t.Fatal("artifact without consensus extra acquired generation time provenance")
		}
	})
}

var benchmarkGenerationTime time.Time
var benchmarkGenerationTimeMS uint64

func BenchmarkCandidateGenerationTime(b *testing.B) {
	fixture := loadReceiveFixture(b, fixtureFullCollated)
	collatedData, err := cell.ToBOCWithOptionsErr(
		fixture.roots[1:],
		cell.BOCSerializeOptions{WithCRC32C: true},
	)
	if err != nil {
		b.Fatal(err)
	}
	trusted := &CandidateArtifact{
		CollatedData:        collatedData,
		generationTimeMS:    receiveFixtureGenUtimeM,
		generationTimeKnown: true,
	}
	fallback := *trusted
	fallback.generationTimeKnown = false

	b.Run("trusted-scalar", func(b *testing.B) {
		var got time.Time
		for b.Loop() {
			got, err = trusted.generationTime()
			if err != nil {
				b.Fatal(err)
			}
		}
		benchmarkGenerationTime = got
	})

	b.Run("decoded-roots", func(b *testing.B) {
		var got uint64
		for b.Loop() {
			got, err = candidateGenUtimeMSFromRoots(fixture.roots[1:])
			if err != nil {
				b.Fatal(err)
			}
		}
		benchmarkGenerationTimeMS = got
	})

	b.Run("boc-fallback", func(b *testing.B) {
		var got time.Time
		for b.Loop() {
			got, err = fallback.generationTime()
			if err != nil {
				b.Fatal(err)
			}
		}
		benchmarkGenerationTime = got
	})
}
