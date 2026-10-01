package validator

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"testing"

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
// The lopsided shape is the one that decides where a hint taken off the
// combined payload may be used: the combined count is useful for the block,
// but on a master candidate it over-states the collated half by the whole
// block. Both shapes are covered by the allocation gate below.
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

// BenchmarkCandidateReceiveSerialize decomposes hinting and overlap over one
// already-parsed mainnet DAG. Run it with -benchmem. Since tonutils keeps
// serializer scratch in a pool, the steady-state numbers describe output
// ownership plus rare pool misses; use multiple counts rather than treating a
// single run as an allocation identity.
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
// Protocol 1 gains less because its block half goes through
// storage.PrepareBlockCandidate and detaches a private graph. It remains in the
// benchmark for byte-ownership and regression coverage.
func BenchmarkCandidateFinishPayload(b *testing.B) {
	for _, shape := range []struct {
		name  string
		shape receiveFixtureShape
	}{{"full_collated", fixtureFullCollated}, {"marker_collated", fixtureMarkerCollated}} {
		fixture := loadReceiveFixture(b, shape.shape)
		source := make([]byte, 32)
		hint := simplex.PayloadCellHint(fixture.combined, nil)

		b.Run(shape.name, func(b *testing.B) {
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
		})
	}
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

// TestCandidateFinishPayloadAllocationBudget holds the steady-state property
// that serializer traversal scratch is reused and only the returned BOCs stay
// owned by the payload. A relative comparison with sequential serialization is
// invalid here: it needs one scratch while production deliberately overlaps two
// serializations and therefore needs two.
func TestCandidateFinishPayloadAllocationBudget(t *testing.T) {
	codec := receiveFixtureCodec(t, 2)
	for shapeName, shape := range map[string]receiveFixtureShape{
		"full":   fixtureFullCollated,
		"marker": fixtureMarkerCollated,
	} {
		fixture := loadReceiveFixture(t, shape)
		production := steadyAllocatedBytes(t, func() {
			mustFinishPayload(t, codec, fixture)
		})
		ownedBytes := uint64(fixture.blockBytes + fixture.collatedBytes)
		// The race detector instruments the concurrent path and can raise this
		// just above 2x. A 2.5x ceiling still leaves a wide gap to the
		// allocation-heavy serializer, while keeping this gate valid under -race.
		budget := ownedBytes * 5 / 2
		t.Logf("%s: finishPayload allocates %d B for %d B of owned output (%.1f%%)",
			shapeName, production, ownedBytes, 100*float64(production)/float64(ownedBytes))
		if production > budget {
			t.Fatalf("%s: finishPayload allocated %d B for %d B of owned output; budget is %d B",
				shapeName, production, ownedBytes, budget)
		}
	}
}

// steadyAllocatedBytes reports bytes per call after both concurrent serializer
// scratches have been returned to the pool. It intentionally does not force a
// GC between warmup and measurement: a GC may evict sync.Pool entries, which is
// a cold-start property rather than the steady-state property this gate holds.
// GC is disabled only for this short, sequential test window and restored
// before the helper returns.
func steadyAllocatedBytes(tb testing.TB, once func()) uint64 {
	tb.Helper()

	previousGCPercent := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(previousGCPercent)

	const repeats = 16
	once()
	once()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range repeats {
		once()
	}
	runtime.ReadMemStats(&after)

	return (after.TotalAlloc - before.TotalAlloc) / repeats
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
