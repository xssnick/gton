package validator

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/simplex"
)

// The receive path re-serializes both halves of every candidate it decodes,
// before the signature check, and until this file existed it was only ever
// benchmarked on the 60-byte four-cell artifacts the codec tests build. Those
// are dominated by fixed costs and hid the two findings measured here: the two
// serializations were unhinted and sequential.
//
// The fixture is a real mainnet candidate payload, not a shaped one: block
// 71398501 of the basechain and the Merkle proof of the state of its
// predecessor, which is exactly the pair a full-collated candidate carries
// (collator/build.go builds collated roots as the consensus extra followed by
// the previous-state proof). Both come out of the collator's own replay
// fixture, read here rather than copied so there is one mainnet block in the
// tree and not two.
const (
	receiveFixturePath      = "collator/testdata/tvm_replay_fat_block_66519406.json"
	receiveFixtureBlockSeq  = 71398501
	receiveFixtureGenUtimeM = 1778681725321
)

type receiveFixtureFile struct {
	BlockBOCBase64      string `json:"block_boc_base64"`
	PreviousStateProofs []struct {
		ProofBOCBase64 string `json:"proof_boc_base64"`
	} `json:"previous_state_proofs"`
}

// receiveFixture is what decodePayload hands finishPayload: the roots of one
// combined candidate BOC, parsed out of the buffer LZ4 produced. Holding the
// combined buffer too is what makes the hint measurable — it is the only place
// the receiver can read a cell count off for free.
type receiveFixture struct {
	combined      []byte
	roots         []*cell.Cell
	rootHash      []byte
	blockCells    int
	collatedCells int
	combinedCells int
	blockBytes    int
	collatedBytes int
}

// The two shapes a received candidate comes in, both of them production.
// Proved: a shard candidate carries the previous-state proof because
// capFullCollatedData is in the capability set every network this node launches
// starts with (genesis/config_cells.go:60-62), while a masterchain candidate
// takes the branch above that proof (collator/build.go buildCollatedRoots
// reaches the proof only when the collation is not a master one) and carries
// the shard-top descriptors and the consensus extra alone — tens of cells
// against a block of thousands.
//
// The lopsided shape is the one that decides whether a hint taken off the
// combined payload may be given to both halves: the combined count bounds each
// half from above, but on a master candidate it over-states the collated half
// by the whole block. Both shapes are measured, and
// TestCandidateFinishPayloadDoesNotRegressOnLopsidedCandidates is the gate.
type receiveFixtureShape int

const (
	fixtureFullCollated receiveFixtureShape = iota
	fixtureMarkerCollated
)

var (
	receiveFixtureOnce  [2]sync.Once
	receiveFixtureValue [2]receiveFixture
	receiveFixtureErr   [2]error
)

// loadReceiveFixture memoizes each fixture per process: the JSON is 2.7 MB and
// the combined serialization is the very work being measured, so paying for it
// once per benchmark iteration would measure the setup.
func loadReceiveFixture(tb testing.TB, shape receiveFixtureShape) receiveFixture {
	tb.Helper()

	receiveFixtureOnce[shape].Do(func() {
		receiveFixtureValue[shape], receiveFixtureErr[shape] = buildReceiveFixture(shape)
	})
	if receiveFixtureErr[shape] != nil {
		tb.Fatal(receiveFixtureErr[shape])
	}

	return receiveFixtureValue[shape]
}

func buildReceiveFixture(shape receiveFixtureShape) (receiveFixture, error) {
	raw, err := os.ReadFile(receiveFixturePath)
	if err != nil {
		return receiveFixture{}, err
	}
	var parsed receiveFixtureFile
	if err = json.Unmarshal(raw, &parsed); err != nil {
		return receiveFixture{}, err
	}

	blockBOC, err := base64.StdEncoding.DecodeString(parsed.BlockBOCBase64)
	if err != nil {
		return receiveFixture{}, err
	}
	blockRoot, err := cell.FromBOC(blockBOC)
	if err != nil {
		return receiveFixture{}, err
	}
	extra := cell.BeginCell().
		MustStoreUInt(consensusExtraDataTag, 32).
		MustStoreUInt(0, 32).
		MustStoreUInt(receiveFixtureGenUtimeM, 64).
		EndCell()
	collatedRoots := []*cell.Cell{extra}
	if shape == fixtureFullCollated {
		proofBOC, proofErr := base64.StdEncoding.DecodeString(parsed.PreviousStateProofs[0].ProofBOCBase64)
		if proofErr != nil {
			return receiveFixture{}, proofErr
		}
		proofRoots, proofErr := cell.FromBOCMultiRoot(proofBOC)
		if proofErr != nil {
			return receiveFixture{}, proofErr
		}
		collatedRoots = append(collatedRoots, proofRoots...)
	}

	// The two canonical component serializations, only to report the shape the
	// receiver meets — these are the outputs finishPayload rebuilds.
	blockCanonical, err := blockRoot.ToBOCWithOptionsErr(cell.BOCSerializeOptions{
		WithCRC32C:    true,
		WithIndex:     true,
		WithCacheBits: true,
		WithIntHashes: true,
	})
	if err != nil {
		return receiveFixture{}, err
	}
	collatedCanonical, err := cell.ToBOCWithOptionsErr(collatedRoots, cell.BOCSerializeOptions{WithCRC32C: true})
	if err != nil {
		return receiveFixture{}, err
	}

	// The combined BOC is built exactly as compressCandidatePayload builds it,
	// then parsed back: a candidate's block and collated cells are deduplicated
	// against each other on the wire, so the roots the receiver holds share
	// cells, and a fixture that parsed the two halves separately would not.
	combined, err := cell.ToBOCWithOptionsErr(
		append([]*cell.Cell{blockRoot}, collatedRoots...),
		cell.BOCSerializeOptions{WithCRC32C: true},
	)
	if err != nil {
		return receiveFixture{}, err
	}
	roots, err := cell.FromBOCMultiRoot(combined)
	if err != nil {
		return receiveFixture{}, err
	}

	return receiveFixture{
		combined:      combined,
		roots:         roots,
		rootHash:      roots[0].Hash(),
		blockCells:    receiveFixtureDeclaredCells(blockCanonical),
		collatedCells: receiveFixtureDeclaredCells(collatedCanonical),
		combinedCells: receiveFixtureDeclaredCells(combined),
		blockBytes:    len(blockCanonical),
		collatedBytes: len(collatedCanonical),
	}, nil
}

// receiveFixtureDeclaredCells is the header read simplex.bocDeclaredCells does,
// repeated here because the test only needs it to report the fixture's shape
// and the codec's own use of it goes through the exported, clamped helper.
func receiveFixtureDeclaredCells(boc []byte) int {
	sizeBytes := int(boc[4] & 0b111)
	cells := 0
	for _, b := range boc[6 : 6+sizeBytes] {
		cells = cells<<8 | int(b)
	}

	return cells
}

// receiveFixtureCodec is a codec whose limits admit the fixture; the protocol
// version selects which block branch finishPayload takes.
func receiveFixtureCodec(tb testing.TB, protocolVersion uint8) *candidateCodec {
	tb.Helper()

	config, _ := runtimeTestConfig(0x71, &runtimeTestJournal{})
	config.Protocol.ProtocolVersion = protocolVersion
	codec, err := newCandidateCodec(config, CandidateLimits{
		MaxBlockBytes:        1 << 22,
		MaxCollatedDataBytes: 1 << 22,
	})
	if err != nil {
		tb.Fatal(err)
	}

	return codec
}

// TestReceiveFixtureIsMainnetSized fails the whole file loudly if the fixture
// degenerates. Every number this file reports is meaningless on a small BOC,
// and the failure mode is silent: a fixture that lost its collated proof still
// benchmarks, at a number that means nothing.
func TestReceiveFixtureIsMainnetSized(t *testing.T) {
	full := loadReceiveFixture(t, fixtureFullCollated)
	marker := loadReceiveFixture(t, fixtureMarkerCollated)
	for name, fixture := range map[string]receiveFixture{"full": full, "marker": marker} {
		t.Logf("%s candidate fixture: block %d B / %d cells, collated %d B / %d cells, combined %d B / %d cells",
			name, fixture.blockBytes, fixture.blockCells,
			fixture.collatedBytes, fixture.collatedCells,
			len(fixture.combined), fixture.combinedCells)

		if fixture.blockBytes < 256<<10 || fixture.blockCells < 8000 {
			t.Fatalf("%s fixture block is %d B / %d cells, not a mainnet block",
				name, fixture.blockBytes, fixture.blockCells)
		}
	}

	if full.collatedBytes < 64<<10 || full.collatedCells < 4000 {
		t.Fatalf("full fixture collated data is %d B / %d cells, too small to be a state proof",
			full.collatedBytes, full.collatedCells)
	}
	if full.combinedCells >= full.blockCells+full.collatedCells {
		t.Fatal("combined BOC deduplicated nothing: the two halves share no cells, " +
			"so the fixture is not one candidate payload")
	}
	// The lopsided shape is only a test of the hint if the two halves really are
	// orders apart; a marker fixture that grew a proof would prove nothing.
	if marker.collatedCells > marker.blockCells/100 {
		t.Fatalf("marker fixture collated data is %d cells against a %d-cell block, not lopsided",
			marker.collatedCells, marker.blockCells)
	}
}

// receiveSerializeArms are the four ways the two receive-path serializations
// can be run. They are the A/B of this work: same roots, same two modes, same
// bytes out, differing only in whether the serializer's dedup structures were
// presized and whether the two halves overlap.
func receiveBlockBOC(root *cell.Cell, hint int) ([]byte, error) {
	return root.ToBOCWithOptionsErr(cell.BOCSerializeOptions{
		WithCRC32C:     true,
		WithIndex:      true,
		WithCacheBits:  true,
		WithIntHashes:  true,
		CellsCountHint: hint,
	})
}

func receiveCollatedBOC(roots []*cell.Cell, hint int) ([]byte, error) {
	return cell.ToBOCWithOptionsErr(roots, cell.BOCSerializeOptions{
		WithCRC32C:     true,
		CellsCountHint: hint,
	})
}

func receiveSerializeSerial(fixture receiveFixture, hint int) ([]byte, []byte, error) {
	blockBOC, err := receiveBlockBOC(fixture.roots[0], hint)
	if err != nil {
		return nil, nil, err
	}
	collated, err := receiveCollatedBOC(fixture.roots[1:], hint)
	if err != nil {
		return nil, nil, err
	}

	return blockBOC, collated, nil
}

func receiveSerializeConcurrent(fixture receiveFixture, hint int) ([]byte, []byte, error) {
	var collated []byte
	var collatedErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		collated, collatedErr = receiveCollatedBOC(fixture.roots[1:], hint)
	}()
	blockBOC, blockErr := receiveBlockBOC(fixture.roots[0], hint)
	<-done
	if blockErr != nil {
		return nil, nil, blockErr
	}
	if collatedErr != nil {
		return nil, nil, collatedErr
	}

	return blockBOC, collated, nil
}

// BenchmarkCandidateReceiveSerialize is the decomposition: the two levers,
// separately and together, over one already-parsed mainnet DAG. Run it with
// -benchmem; the hint arms are the allocation story and the concurrent arms the
// wall-clock one.
//
// WHAT WAS MEASURED, and it is only this. Apple M1 Pro, go1.26.4, darwin/arm64,
// -benchtime 150x -benchmem -count 7, best of the seven per arm, on a box at
// load average 13-14 on 10 cores throughout — the minimum is the statistic
// precisely because a neighbour stealing a core inflates the mean and cannot
// deflate the minimum. Fixture shapes are the two logged by
// TestReceiveFixtureIsMainnetSized.
//
//	full_collated    (block 600 KB / 18144 cells, collated 259 KB / 9566 cells)
//	  serial_unhinted       4.122 ms   5,767,845 B/op   69 allocs/op
//	  serial_hinted         3.318 ms   2,998,646 B/op   14 allocs/op   -19.5% / -48.0%
//	  concurrent_unhinted   2.831 ms   5,768,062 B/op   73 allocs/op   -31.3% /  +0.0%
//	  concurrent_hinted     2.102 ms   2,998,954 B/op   18 allocs/op   -49.0% / -48.0%
//
//	marker_collated  (same block, collated 33 B / 1 cell — the master shape)
//	  serial_unhinted       2.444 ms   3,732,496 B/op   42 allocs/op
//	  serial_hinted         1.969 ms   2,367,888 B/op   13 allocs/op   -19.4% / -36.6%
//	  concurrent_unhinted   2.477 ms   3,732,812 B/op   46 allocs/op    +1.4% /  +0.0%
//	  concurrent_hinted     1.931 ms   2,368,200 B/op   17 allocs/op   -21.0% / -36.6%
//
// Two things in there are worth more than the headline. The concurrency is
// worth nothing on the lopsided shape and is not expected to be — there is one
// cell in the other half — and the +1.4% is the goroutine, at the resolution
// this method has. And the hint is worth 36.6% of the allocations there anyway,
// which is the answer to the objection the shape exists to raise: presizing the
// collated bag for 18k cells to serialize one wastes about a megabyte, and the
// same hint saves the block half more than twice that by replacing the doubling
// growth of its cell list.
//
// The production function, with the sha256 passes and the protocol branch the
// arms leave out, is BenchmarkCandidateFinishPayload; its own before/after is
// recorded there.
func BenchmarkCandidateReceiveSerialize(b *testing.B) {
	for _, shape := range []struct {
		name  string
		shape receiveFixtureShape
	}{{"full_collated", fixtureFullCollated}, {"marker_collated", fixtureMarkerCollated}} {
		fixture := loadReceiveFixture(b, shape.shape)
		hint := simplex.PayloadCellHint(fixture.combined, nil)
		if hint <= 0 {
			b.Fatal("combined candidate BOC yielded no cell hint")
		}

		arms := []struct {
			name string
			run  func() ([]byte, []byte, error)
		}{
			{"serial_unhinted", func() ([]byte, []byte, error) { return receiveSerializeSerial(fixture, 0) }},
			{"serial_hinted", func() ([]byte, []byte, error) { return receiveSerializeSerial(fixture, hint) }},
			{"concurrent_unhinted", func() ([]byte, []byte, error) { return receiveSerializeConcurrent(fixture, 0) }},
			{"concurrent_hinted", func() ([]byte, []byte, error) { return receiveSerializeConcurrent(fixture, hint) }},
		}
		for _, arm := range arms {
			b.Run(shape.name+"/"+arm.name, func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if _, _, err := arm.run(); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkCandidateFinishPayload is the production function, on the same
// fixture: everything the receive path pays inside decodeVerified once the
// combined BOC is parsed, which is the part that runs before the signature
// check.
//
// Before and after the hint and the concurrency, same box and method as
// BenchmarkCandidateReceiveSerialize (-benchtime 120x -count 7, best of seven),
// on the full_collated fixture:
//
//	protocol 2   4.234 ms -> 2.369 ms  (-44.0%)   5,767,760 -> 2,998,938 B/op   70 -> 20 allocs
//	protocol 1   7.052 ms -> 5.468 ms  (-22.5%)  11,752,688 -> 11,027,874 B/op  217 -> 196 allocs
//
// Protocol 1 gains less because its block half does not take the hint: it goes
// through storage.PrepareBlockCandidate, which detaches a private copy of the
// block graph and serializes that, and the copy is 2.8 ms and 6 MB of the 7.05
// on its own. Nothing this node launches runs protocol 1 — genesis refuses any
// consensus protocol_version but 3 (genesis/spec.go:193) — so that branch was
// measured and left alone rather than optimized.
func BenchmarkCandidateFinishPayload(b *testing.B) {
	fixture := loadReceiveFixture(b, fixtureFullCollated)
	source := make([]byte, 32)
	hint := simplex.PayloadCellHint(fixture.combined, nil)

	for _, protocolVersion := range []uint8{2, 1} {
		codec := receiveFixtureCodec(b, protocolVersion)
		b.Run(map[uint8]string{1: "protocol1", 2: "protocol2"}[protocolVersion], func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := codec.finishPayload(
					source,
					receiveFixtureBlockSeq,
					fixture.rootHash,
					fixture.roots,
					hint,
				); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// bestOf is the shape every timing claim in this file is made in: the box is
// shared and noisy, so the statistic is the minimum over N runs, which is the
// one insensitive to a neighbour stealing a core mid-run.
func bestOf(runs int, once func()) time.Duration {
	best := time.Duration(1<<62 - 1)
	for range runs {
		start := time.Now()
		once()
		if elapsed := time.Since(start); elapsed < best {
			best = elapsed
		}
	}

	return best
}

func mustFinishPayload(tb testing.TB, codec *candidateCodec, fixture receiveFixture) decodedCandidatePayload {
	tb.Helper()

	payload, err := codec.finishPayload(
		make([]byte, 32),
		receiveFixtureBlockSeq,
		fixture.rootHash,
		fixture.roots,
		simplex.PayloadCellHint(fixture.combined, nil),
	)
	if err != nil {
		tb.Fatal(err)
	}

	return payload
}

// TestCandidateReceiveSerializeIsByteIdentical is the gate on both levers. The
// canonical BOCs a candidate is identified by — its file hash is the sha256 of
// one of them — must not depend on how the serializer was sized or on which
// goroutine ran it, so the production payload is compared against the arm this
// path used to be: sequential and unhinted.
func TestCandidateReceiveSerializeIsByteIdentical(t *testing.T) {
	for shapeName, shape := range map[string]receiveFixtureShape{
		"full":   fixtureFullCollated,
		"marker": fixtureMarkerCollated,
	} {
		fixture := loadReceiveFixture(t, shape)
		referenceBlock, referenceCollated, err := receiveSerializeSerial(fixture, 0)
		if err != nil {
			t.Fatal(err)
		}

		for _, protocolVersion := range []uint8{1, 2} {
			payload := mustFinishPayload(t, receiveFixtureCodec(t, protocolVersion), fixture)
			if payload.generationTimeMS != receiveFixtureGenUtimeM {
				t.Fatalf("%s protocol %d generation time = %d, want %d",
					shapeName, protocolVersion, payload.generationTimeMS, receiveFixtureGenUtimeM)
			}
			if !bytes.Equal(payload.blockBOC, referenceBlock) {
				t.Fatalf("%s protocol %d block BOC differs from the unhinted sequential serialization: %d vs %d bytes",
					shapeName, protocolVersion, len(payload.blockBOC), len(referenceBlock))
			}
			if !bytes.Equal(payload.collatedData, referenceCollated) {
				t.Fatalf("%s protocol %d collated BOC differs from the unhinted sequential serialization: %d vs %d bytes",
					shapeName, protocolVersion, len(payload.collatedData), len(referenceCollated))
			}
			if payload.collatedFileHash != sha256.Sum256(referenceCollated) {
				t.Fatalf("%s protocol %d collated file hash is not the digest of the collated BOC", shapeName, protocolVersion)
			}
		}

		// The same two levers, exercised directly over the mainnet DAG rather than
		// through the codec, so a byte difference is attributed to the lever and not
		// to a change in the codec around it.
		hint := simplex.PayloadCellHint(fixture.combined, nil)
		hintedBlock, hintedCollated, err := receiveSerializeSerial(fixture, hint)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(hintedBlock, referenceBlock) || !bytes.Equal(hintedCollated, referenceCollated) {
			t.Fatalf("%s: presizing the serializer changed the bytes it emitted", shapeName)
		}
		concurrentBlock, concurrentCollated, err := receiveSerializeConcurrent(fixture, hint)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(concurrentBlock, referenceBlock) || !bytes.Equal(concurrentCollated, referenceCollated) {
			t.Fatalf("%s: running the two serializations concurrently changed the bytes they emitted", shapeName)
		}
	}
}

// TestCandidateFinishPayloadIsHinted guards the hint by the only observable it
// has: a serializer that grows its dedup index and cell list from nothing
// allocates the discarded intermediate tables as well as the final ones. The
// threshold is measured against the unhinted arm in the same process on the
// same fixture rather than written down as a constant, so it cannot drift.
//
// It runs on the lopsided shape too, and that arm is the one that decides the
// design rather than merely defending it. A hint read off the combined payload
// over-states the collated half of a master candidate by the whole block, so
// the collated bag is presized for 18k cells to serialize one; the question is
// whether that waste outweighs what the same hint saves the block half, and the
// measurement says it does not come close — 2.37 MB against the 3.73 MB the
// unhinted pair costs, because a presized bag replaces the doubling growth of
// an 18k-cell list. Should that ever invert, this arm fails and the hint must
// be narrowed to the block half.
func TestCandidateFinishPayloadIsHinted(t *testing.T) {
	codec := receiveFixtureCodec(t, 2)
	for shapeName, shape := range map[string]receiveFixtureShape{
		"full":   fixtureFullCollated,
		"marker": fixtureMarkerCollated,
	} {
		fixture := loadReceiveFixture(t, shape)
		unhinted := allocatedBytes(t, func() {
			if _, _, err := receiveSerializeSerial(fixture, 0); err != nil {
				t.Fatal(err)
			}
		})
		production := allocatedBytes(t, func() {
			mustFinishPayload(t, codec, fixture)
		})
		t.Logf("%s: finishPayload allocates %d B against %d B for the unhinted serializations (%.1f%%)",
			shapeName, production, unhinted, 100*float64(production)/float64(unhinted))

		// The measured gap is 48% on the full shape and 37% on the lopsided one;
		// the gate is set at a tenth of the smaller, so it fails on a lost hint and
		// not on a change of allocation accounting elsewhere in the payload.
		if production > unhinted*96/100 {
			t.Fatalf("%s: finishPayload allocated %d B, no better than the %d B the unhinted "+
				"serializations cost — the combined cell hint is not reaching the serializer, "+
				"or it now costs the small half more than it saves the large one",
				shapeName, production, unhinted)
		}
	}
}

// allocatedBytes reports the bytes one call allocates, averaged over a few
// repeats so a single GC-assist accounting artefact cannot decide the result.
func allocatedBytes(tb testing.TB, once func()) uint64 {
	tb.Helper()

	const repeats = 5
	once()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range repeats {
		once()
	}
	runtime.ReadMemStats(&after)

	return (after.TotalAlloc - before.TotalAlloc) / repeats
}

// TestCandidateFinishPayloadOverlapsItsTwoSerializations guards the other lever.
// A goroutine leaves no trace in the payload it produced, so the observable is
// the only one there is: the whole call must finish in less than the two halves
// take one after the other. Both sides are minimums over several runs, and the
// margin is wide, because the claim being defended is "these overlap at all"
// and not any particular speedup.
//
// The reference halves are hinted, so this bar also moves if the hint is lost —
// measured: reverting the concurrency alone puts the call at 117% of the
// sequential pair, reverting the hint alone at 106%. That overlap between the
// two failures is why the message below names the discriminating test rather
// than asserting which lever went missing.
func TestCandidateFinishPayloadOverlapsItsTwoSerializations(t *testing.T) {
	if runtime.GOMAXPROCS(0) < 2 {
		t.Skip("overlap is unobservable on a single processor")
	}
	fixture := loadReceiveFixture(t, fixtureFullCollated)
	codec := receiveFixtureCodec(t, 2)
	hint := simplex.PayloadCellHint(fixture.combined, nil)

	const runs = 9
	blockOnly := bestOf(runs, func() {
		if _, err := receiveBlockBOC(fixture.roots[0], hint); err != nil {
			t.Fatal(err)
		}
	})
	collatedOnly := bestOf(runs, func() {
		if _, err := receiveCollatedBOC(fixture.roots[1:], hint); err != nil {
			t.Fatal(err)
		}
	})
	production := bestOf(runs, func() {
		mustFinishPayload(t, codec, fixture)
	})
	sequential := blockOnly + collatedOnly
	t.Logf("finishPayload %v against %v sequential (block %v + collated %v), %.0f%%",
		production, sequential, blockOnly, collatedOnly,
		100*float64(production)/float64(sequential))

	// finishPayload does more than the two serializations — two sha256 passes
	// over 700 KB, which is why the bar is not the sum itself. The slower half
	// alone plus everything else is well under this; the sum is not.
	if production > sequential*9/10 {
		t.Fatalf("finishPayload took %v, no better than the %v its two hinted "+
			"serializations take one after the other: the two halves are no longer "+
			"overlapped, or no longer presized — TestCandidateFinishPayloadIsHinted "+
			"tells the two apart",
			production, sequential)
	}
}

// The block half's size check and digest were moved above the join with the
// collated half, because both are finished the instant the block is serialized
// and neither reads anything the collated goroutine writes. This function runs
// on every candidate a validator is offered, before the signature check, and no
// notarization quorum can form until it returns, so the residue matters.
//
// What must not move with them is the return: the goroutine writes three
// variables of this function, so a failure discovered early is carried to the
// other side of the join rather than returned through it. That part is held by
// inspection — Go leaves an abandoned goroutine writing perfectly valid heap, so
// no test can see the difference. What a test can hold is the verdict: the two
// limits keep their precedence, block before collated, so a candidate that
// overflows both fails the same way it always did.
func TestFinishPayloadCarriesAnEarlyBlockFailurePastTheJoin(t *testing.T) {
	fixture := loadReceiveFixture(t, fixtureFullCollated)
	config, _ := runtimeTestConfig(0x71, &runtimeTestJournal{})
	config.Protocol.ProtocolVersion = 2

	for _, test := range []struct {
		name   string
		limits CandidateLimits
		want   string
	}{
		{
			name:   "only the block overflows",
			limits: CandidateLimits{MaxBlockBytes: 1, MaxCollatedDataBytes: 1 << 22},
			want:   "candidate block is too large",
		},
		{
			name:   "only the collated data overflows",
			limits: CandidateLimits{MaxBlockBytes: 1 << 22, MaxCollatedDataBytes: 1},
			want:   "candidate collated data is too large",
		},
		{
			// Both halves fail at once, which is the case that pins the
			// precedence: the block's verdict is the one returned, exactly as
			// when its check sat after the join.
			name:   "both overflow",
			limits: CandidateLimits{MaxBlockBytes: 1, MaxCollatedDataBytes: 1},
			want:   "candidate block is too large",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			codec, err := newCandidateCodec(config, test.limits)
			if err != nil {
				t.Fatal(err)
			}
			_, err = codec.finishPayload(
				make([]byte, 32),
				receiveFixtureBlockSeq,
				fixture.rootHash,
				fixture.roots,
				simplex.PayloadCellHint(fixture.combined, nil),
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want one containing %q", err, test.want)
			}
		})
	}
}
