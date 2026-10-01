package collator

import (
	"crypto/sha256"
	"testing"

	"github.com/xssnick/gton/service/validator/simplex"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// payloadHintFixture is one finished mainnet candidate in the form the broadcast
// payload is built from: the roots, and the two BoCs the hint is read off.
type payloadHintFixture struct {
	blockRoot        *cell.Cell
	collatedRoots    []*cell.Cell
	blockBOC         []byte
	collatedData     []byte
	seqNo            uint32
	fileHash         [32]byte
	collatedFileHash [32]byte
}

func newPayloadHintFixture(tb testing.TB, repeat int, lazy bool) payloadHintFixture {
	tb.Helper()

	req := benchMainnetCollatedRequest(tb, repeat)
	if lazy {
		req = lazyParentPredecessor(tb, req)
	}
	candidate := lazyParentCandidate(tb, req)

	blockRoot, err := cell.FromBOC(candidate.BlockBOC)
	if err != nil {
		tb.Fatal(err)
	}
	collatedRoots, err := cell.FromBOCMultiRoot(candidate.CollatedData)
	if err != nil {
		tb.Fatal(err)
	}

	return payloadHintFixture{
		blockRoot:        blockRoot,
		collatedRoots:    collatedRoots,
		blockBOC:         candidate.BlockBOC,
		collatedData:     candidate.CollatedData,
		seqNo:            candidate.ID.SeqNo,
		fileHash:         sha256.Sum256(candidate.BlockBOC),
		collatedFileHash: candidate.CollatedFileHash,
	}
}

// The A/B for the broadcast payload hint, in one binary: the hint is a
// parameter of PrepareCandidate, so the two arms are the same code on the same
// roots with the same output, differing only in whether the serializer's dedup
// structures were presized. Run it with -benchmem.
//
// WHAT IS MEASURED, and it is only this. On the mainnet collated fixture at
// repeat=3 (Apple M1 Pro, go1.26.4, darwin/arm64), -benchtime 200x -benchmem,
// eight samples per arm: one -count 5 process plus three independent -count 1
// processes, so the spread covers process-to-process variation and not only
// within-process repeats (the fixtures are memoized per process, which is why
// -count 5 alone cannot show that).
//
//	B/op       7,915,941-7,923,608  ->  4,128,217-4,133,091   (-47.8%)
//	allocs/op                   50  ->                    18   (-32, exactly)
//	ns/op      7.35 ms - 7.72 ms    ->  6.13 ms - 6.32 ms     (-14% .. -21%)
//
// The allocation halves are the result and are the most stable figure here:
// 0.10% spread on B/op, and an allocation count identical in every one of the
// eight runs of each arm.
//
// The wall-clock halves are CLEANLY SEPARATED — the slowest hinted run (6.32 ms)
// is faster than the fastest no-hint one (7.35 ms), across all four processes —
// so a range may be quoted, and it is the range above rather than a single
// number. An earlier version of this comment booked overlapping halves
// (7.44-8.31 against 6.32-7.50) and concluded that no ns/op figure could be
// quoted; a re-measurement on the current tree does not reproduce that overlap,
// so that claim was withdrawn rather than carried forward. The box was under
// load average 13 on 10 cores while these ran, which widens the upper tails and
// makes the separation a conservative statement, not an optimistic one.
//
// Nor is any whole-collation figure booked. The serializer this row measures runs
// inside collation, but asynchronously (PrepareCandidateAsync, build.go:886-893),
// so whether its CPU appears in collation wall time depends on whether the join
// lands on the critical path — which this row does not measure and which no
// figure in this file may be read as measuring. What the hint is for is the
// allocation table above and the bound in candidate_payload_hint.go; the wire it
// produces is byte-identical at every hint, which TestL3HintChangesNoPayloadByte
// holds.
func BenchmarkPrepareCandidatePayloadMainnet(b *testing.B) {
	fixture := newPayloadHintFixture(b, benchMainnetExportRepeat(), false)
	hint := simplex.PayloadCellHint(fixture.blockBOC, fixture.collatedData)
	if hint <= 0 {
		b.Fatalf("the fixture produced no hint")
	}

	for _, arm := range []struct {
		name string
		hint int
	}{{"nohint", 0}, {"hint", hint}} {
		b.Run(arm.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				prepared, err := simplex.PrepareCandidate(
					fixture.seqNo,
					fixture.blockRoot,
					fixture.collatedRoots,
					fixture.fileHash,
					fixture.collatedFileHash,
					arm.hint,
				)
				if err != nil {
					b.Fatal(err)
				}
				if prepared == nil {
					b.Fatal("no payload")
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(arm.hint), "hint")
			b.ReportMetric(float64(len(fixture.blockBOC)+len(fixture.collatedData)), "inputB")
		})
	}
}
