package validator

import (
	"bytes"
	"crypto/sha256"
	"sync"
	"testing"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/simplex"
)

type candidateWireBenchmarkFixture struct {
	candidate     simplex.Candidate
	blockBOC      []byte
	collatedData  []byte
	blockRoot     *cell.Cell
	collatedRoots []*cell.Cell
}

type candidateWireBenchmarkShape struct {
	name  string
	shape receiveFixtureShape
}

var candidateWireBenchmarkShapes = []candidateWireBenchmarkShape{
	{"full_collated", fixtureFullCollated},
	{"marker_collated", fixtureMarkerCollated},
}

func loadCandidateWireBenchmarkFixture(tb testing.TB, shape receiveFixtureShape) candidateWireBenchmarkFixture {
	tb.Helper()

	fixture := loadReceiveFixture(tb, shape)
	compressed := receiveFixtureCompressed(tb, fixture)
	codec := receiveFixtureCodec(tb, 2)
	artifact, _, err := codec.decodeDeferred(compressed.broadcast, nil)
	if err != nil {
		tb.Fatal(err)
	}

	return candidateWireBenchmarkFixture{
		candidate:     artifact.Candidate,
		blockBOC:      artifact.BlockBOC,
		collatedData:  artifact.CollatedData,
		blockRoot:     artifact.validationRoots.block,
		collatedRoots: artifact.validationRoots.collated,
	}
}

func (f candidateWireBenchmarkFixture) lazy() *lazyCandidateWire {
	return &lazyCandidateWire{
		candidate:    f.candidate,
		blockBOC:     f.blockBOC,
		collatedData: f.collatedData,
	}
}

// legacyCandidateWireBenchmark preserves the original first-use path so that
// the synchronization and root ownership change can be compared in one run.
// It is test-only: production has exactly one lazy wire implementation.
type legacyCandidateWireBenchmark struct {
	once sync.Once
	data candidateWireBenchmarkFixture
	wire []byte
	hash [32]byte
	err  error
}

func (w *legacyCandidateWireBenchmark) materialize() ([]byte, [32]byte, error) {
	w.once.Do(func() {
		var fileHash [32]byte
		copy(fileHash[:], w.data.candidate.Block.FileHash)

		prepared, err := simplex.PrepareCandidate(
			w.data.candidate.Block.SeqNo,
			w.data.blockRoot,
			w.data.collatedRoots,
			fileHash,
			w.data.candidate.CollatedFileHash,
			simplex.PayloadCellHint(w.data.blockBOC, w.data.collatedData),
		)
		if err == nil {
			w.wire, err = simplex.SerializeCandidatePrepared(w.data.candidate, prepared)
		}
		if err != nil {
			w.err = err
		} else {
			w.hash = sha256.Sum256(w.wire)
		}
		w.data.blockRoot = nil
		w.data.collatedRoots = nil
	})

	return w.wire, w.hash, w.err
}

func checkCandidateWireBenchmarkParity(tb testing.TB, f candidateWireBenchmarkFixture) {
	tb.Helper()

	want, err := simplex.SerializeCandidate(f.candidate, f.blockBOC, f.collatedData)
	if err != nil {
		tb.Fatal(err)
	}
	wantHash := sha256.Sum256(want)
	legacy := &legacyCandidateWireBenchmark{data: f}
	got, hash, err := legacy.materialize()
	if err != nil || !bytes.Equal(got, want) || hash != wantHash {
		tb.Fatalf("legacy wire differs from checked canonical wire: %v", err)
	}

	for _, hot := range []bool{true, false} {
		lazy := f.lazy()
		lazy.setRoots(f.blockRoot, f.collatedRoots)
		if !hot {
			lazy.releaseRoots()
		}
		got, hash, err := lazy.materialize()
		if err != nil || !bytes.Equal(got, want) || hash != wantHash {
			tb.Fatalf("lazy wire (hot=%t) differs from checked canonical wire: %v", hot, err)
		}
	}
}

func TestCandidateColdWireMainnetFixtureCanonicalParity(t *testing.T) {
	for _, shape := range candidateWireBenchmarkShapes {
		t.Run(shape.name, func(t *testing.T) {
			fixture := loadCandidateWireBenchmarkFixture(t, shape.shape)
			checkCandidateWireBenchmarkParity(t, fixture)
		})
	}
}

// BenchmarkCandidateWireMaterialize uses a real mainnet block and its full
// previous-state proof, plus the marker-only shape. Hot and cold both include
// canonical wire creation and its hash; cold alone reparses immutable BOCs.
// Fixture parsing and canonical parity checks happen outside the timed loop.
func BenchmarkCandidateWireMaterialize(b *testing.B) {
	for _, shape := range candidateWireBenchmarkShapes {
		fixture := loadCandidateWireBenchmarkFixture(b, shape.shape)
		checkCandidateWireBenchmarkParity(b, fixture)

		b.Run(shape.name+"/path=legacy_hot", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				wire := &legacyCandidateWireBenchmark{data: fixture}
				if _, _, err := wire.materialize(); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(shape.name+"/path=hot", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				wire := fixture.lazy()
				wire.setRoots(fixture.blockRoot, fixture.collatedRoots)
				if _, _, err := wire.materialize(); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(shape.name+"/path=cold", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				wire := fixture.lazy()
				if _, _, err := wire.materialize(); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(shape.name+"/path=checked_boc", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				wire, err := simplex.SerializeCandidate(fixture.candidate, fixture.blockBOC, fixture.collatedData)
				if err != nil {
					b.Fatal(err)
				}
				_ = sha256.Sum256(wire)
			}
		})
		b.Run(shape.name+"/path=cached", func(b *testing.B) {
			wire := fixture.lazy()
			wire.setRoots(fixture.blockRoot, fixture.collatedRoots)
			if _, _, err := wire.materialize(); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			for b.Loop() {
				if _, _, err := wire.materialize(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkCandidateWireRootRelease includes re-arming the root holder on each
// iteration. The set+release pair is an upper bound on the release itself and
// avoids timing millions of individual StopTimer/StartTimer calls. In
// particular, it must not grow with the full-collated graph size.
func BenchmarkCandidateWireRootRelease(b *testing.B) {
	for _, shape := range candidateWireBenchmarkShapes {
		fixture := loadCandidateWireBenchmarkFixture(b, shape.shape)

		b.Run(shape.name, func(b *testing.B) {
			wire := fixture.lazy()
			b.ReportAllocs()
			for b.Loop() {
				wire.setRoots(fixture.blockRoot, fixture.collatedRoots)
				wire.releaseRoots()
			}
		})
	}
}

// The sink makes the hash slices escape past identity construction, as they do
// in a resolver entry. Without it, escape analysis could turn both arms into
// stack-only work and hide the actual ownership cost.
var candidateIdentityBenchmarkSink ton.BlockIDExt

func BenchmarkCandidateIdentityOwnership(b *testing.B) {
	fixture := loadReceiveFixture(b, fixtureFullCollated)
	compressed := receiveFixtureCompressed(b, fixture)
	source, err := receiveFixtureCodec(b, 2).decodePayload(compressed.payload)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("path=interior_slices", func(b *testing.B) {
		b.Cleanup(func() { candidateIdentityBenchmarkSink = ton.BlockIDExt{} })
		b.ReportAllocs()
		for b.Loop() {
			payload := source
			candidateIdentityBenchmarkSink = ton.BlockIDExt{
				Workchain: 0,
				Shard:     -1 << 63,
				SeqNo:     payload.round,
				RootHash:  payload.rootHash[:],
				FileHash:  payload.fileHash[:],
			}
		}
		if !bytes.Equal(candidateIdentityBenchmarkSink.RootHash, source.rootHash[:]) ||
			!bytes.Equal(candidateIdentityBenchmarkSink.FileHash, source.fileHash[:]) {
			b.Fatal("interior identity changed the hashes")
		}
	})
	b.Run("path=detached_hashes", func(b *testing.B) {
		b.Cleanup(func() { candidateIdentityBenchmarkSink = ton.BlockIDExt{} })
		b.ReportAllocs()
		for b.Loop() {
			payload := source
			candidateIdentityBenchmarkSink = ton.BlockIDExt{
				Workchain: 0,
				Shard:     -1 << 63,
				SeqNo:     payload.round,
				RootHash:  bytes.Clone(payload.rootHash[:]),
				FileHash:  bytes.Clone(payload.fileHash[:]),
			}
		}
		if !bytes.Equal(candidateIdentityBenchmarkSink.RootHash, source.rootHash[:]) ||
			!bytes.Equal(candidateIdentityBenchmarkSink.FileHash, source.fileHash[:]) {
			b.Fatal("detached identity changed the hashes")
		}
	})
}
