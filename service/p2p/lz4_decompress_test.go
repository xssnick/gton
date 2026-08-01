package p2p

import (
	"bytes"
	"testing"

	"github.com/pierrec/lz4/v4"
)

func lz4CompressBlockForTest(t *testing.T, data []byte) []byte {
	t.Helper()
	dst := make([]byte, lz4.CompressBlockBound(len(data)))
	n, err := lz4.CompressBlock(data, dst, nil)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	if n == 0 {
		t.Fatal("incompressible test payload")
	}
	return dst[:n]
}

func TestDecompressLZ4BlockSizes(t *testing.T) {
	patterned := func(size int) []byte {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i / 64)
		}
		return data
	}

	cases := []struct {
		name string
		size int
	}{
		{"tiny", 100},
		{"exactly1MiB", 1 << 20},
		{"above4xEstimate", 9 << 20},
		{"nearMax", maxDecompressedBlockSize - 1},
		{"exactlyMax", maxDecompressedBlockSize},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			original := patterned(tc.size)
			compressed := lz4CompressBlockForTest(t, original)

			got, err := decompressLZ4Block(compressed)
			if err != nil {
				t.Fatalf("decompress: %v", err)
			}
			if !bytes.Equal(got, original) {
				t.Fatalf("roundtrip mismatch: got %d bytes, want %d", len(got), len(original))
			}
		})
	}

	t.Run("exceedsMax", func(t *testing.T) {
		original := patterned(maxDecompressedBlockSize + 1)
		compressed := lz4CompressBlockForTest(t, original)

		if _, err := decompressLZ4Block(compressed); err == nil {
			t.Fatal("expected error for payload above the size limit")
		}
	})
}
