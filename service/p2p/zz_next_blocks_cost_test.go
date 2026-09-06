package p2p

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/pierrec/lz4/v4"
	tnstore "github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func lz4BoundForBench(n int) int { return lz4.CompressBlockBound(n) }

func compressForBench(in, out []byte) (int, error) { return lz4.CompressBlock(in, out, nil) }

// benchRealBlock returns a real mainnet block BOC out of the collator's replay
// fixture, so the BOC and LZ4 costs below are measured on a real cell shape
// rather than on a synthetic tree that compresses to nothing.
func benchRealBlock(tb testing.TB) []byte {
	tb.Helper()

	raw, err := os.ReadFile("../validator/collator/testdata/tvm_replay_fat_block_66519406.json")
	if err != nil {
		tb.Skipf("replay fixture is unavailable: %v", err)
	}
	var fixture struct {
		BlockBOC string `json:"block_boc_base64"`
	}
	if err = json.Unmarshal(raw, &fixture); err != nil {
		tb.Fatal(err)
	}
	boc, err := base64.StdEncoding.DecodeString(fixture.BlockBOC)
	if err != nil {
		tb.Fatal(err)
	}

	return boc
}

// benchBlockProof stands in for the block's proof: the header subtree of the
// real block, which is the shape a proof actually has.
func benchBlockProof(tb testing.TB, blockBOC []byte) []byte {
	tb.Helper()

	root, err := cell.FromBOC(blockBOC)
	if err != nil {
		tb.Fatal(err)
	}
	info, err := root.PeekRef(0)
	if err != nil {
		tb.Fatal(err)
	}

	return info.ToBOC()
}

func BenchmarkServeNextBlockCompression(b *testing.B) {
	block := benchRealBlock(b)
	proof := benchBlockProof(b, block)
	full := &tnstore.ServedBlockFull{Proof: proof, Block: block}

	compressed, err := compressNextBlockFull(full)
	if err != nil {
		b.Fatal(err)
	}
	b.Logf("proof=%dB block=%dB wire=%dB", len(proof), len(block), len(compressed.Compressed))

	b.Run(fmt.Sprintf("raw%dKiB", (len(proof)+len(block))>>10), func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(proof) + len(block)))
		for i := 0; i < b.N; i++ {
			if _, err := compressNextBlockFull(full); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkServeNextBlockParts splits the same work into its three stages, so
// the answer to "what would caching the compressed form save" is not a guess.
func BenchmarkServeNextBlockParts(b *testing.B) {
	blockBOC := benchRealBlock(b)
	proofBOC := benchBlockProof(b, blockBOC)

	blockRoot, err := cell.FromBOC(blockBOC)
	if err != nil {
		b.Fatal(err)
	}
	proofRoot, err := cell.FromBOC(proofBOC)
	if err != nil {
		b.Fatal(err)
	}
	combined, err := cell.ToBOCWithOptionsErr(
		[]*cell.Cell{proofRoot, blockRoot},
		cell.BOCSerializeOptions{WithCRC32C: true},
	)
	if err != nil {
		b.Fatal(err)
	}
	b.Logf("combined=%dB", len(combined))

	b.Run("parse", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := cell.FromBOC(proofBOC); err != nil {
				b.Fatal(err)
			}
			if _, err := cell.FromBOC(blockBOC); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("serialize", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := cell.ToBOCWithOptionsErr(
				[]*cell.Cell{proofRoot, blockRoot},
				cell.BOCSerializeOptions{WithCRC32C: true},
			); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("lz4", func(b *testing.B) {
		b.ReportAllocs()
		out := make([]byte, lz4BoundForBench(len(combined)))
		for i := 0; i < b.N; i++ {
			if _, err := compressForBench(combined, out); err != nil {
				b.Fatal(err)
			}
		}
	})
}
