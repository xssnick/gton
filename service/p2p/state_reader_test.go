package p2p

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

var _ io.WriterTo = (*persistentStateChunkReader)(nil)

// newChunkReaderForTest builds a reader whose first chunk is pre-seeded (like
// the probe chunk) and whose remaining chunks arrive through the results
// channel out of order.
func newChunkReaderForTest(t *testing.T, chunks [][]byte, size int64) *persistentStateChunkReader {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	r := &persistentStateChunkReader{
		ctx:          ctx,
		cancel:       cancel,
		node:         &Node{log: zerolog.Nop()},
		peer:         &overlayPeer{addr: "test-peer"},
		blockRef:     "test-block",
		size:         size,
		chunks:       make([][]byte, len(chunks)),
		hash:         sha256.New(),
		lastProgress: time.Now(),
	}
	r.addDownloadedChunk(stateChunkResult{offset: 0, data: chunks[0]})

	results := make(chan stateChunkResult, len(chunks))
	for i := len(chunks) - 1; i >= 1; i-- {
		results <- stateChunkResult{offset: int64(i) * persistentStateChunkSize, data: chunks[i]}
	}
	close(results)
	r.results = results
	return r
}

func testChunkBytes(seed byte, size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = seed + byte(i%251)
	}
	return data
}

func TestPersistentStateChunkReaderWriteToMatchesRead(t *testing.T) {
	chunks := [][]byte{
		testChunkBytes(0x11, 64<<10),
		testChunkBytes(0x22, 32<<10),
		testChunkBytes(0x33, 5),
	}
	var want []byte
	for _, chunk := range chunks {
		want = append(want, chunk...)
	}
	size := int64(len(want))

	readReader := newChunkReaderForTest(t, chunks, size)
	readOut, err := io.ReadAll(readReader)
	if err != nil {
		t.Fatalf("sequential Read failed: %v", err)
	}
	if !bytes.Equal(readOut, want) {
		t.Fatalf("sequential Read produced %d bytes, want %d matching bytes", len(readOut), len(want))
	}

	writeReader := newChunkReaderForTest(t, chunks, size)
	var writeOut bytes.Buffer
	written, err := writeReader.WriteTo(&writeOut)
	if err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}
	if written != size {
		t.Fatalf("WriteTo wrote %d bytes, want %d", written, size)
	}
	if !bytes.Equal(writeOut.Bytes(), readOut) {
		t.Fatal("WriteTo output differs from sequential Read output")
	}
	if !bytes.Equal(writeReader.FileHash(), readReader.FileHash()) {
		t.Fatal("WriteTo and Read produced different file hashes")
	}
	if !bytes.Equal(writeReader.Prefix(), readReader.Prefix()) {
		t.Fatal("WriteTo and Read captured different prefixes")
	}

	if n, err := writeReader.WriteTo(io.Discard); err != nil || n != 0 {
		t.Fatalf("drained WriteTo = (%d, %v), want (0, nil)", n, err)
	}
	if _, err := writeReader.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("Read after WriteTo drain = %v, want io.EOF", err)
	}
}

func TestPersistentStateChunkReaderWriteToViaIOCopy(t *testing.T) {
	chunks := [][]byte{
		testChunkBytes(0x41, 4<<10),
		testChunkBytes(0x42, 4<<10),
	}
	var want []byte
	for _, chunk := range chunks {
		want = append(want, chunk...)
	}
	size := int64(len(want))

	reader := newChunkReaderForTest(t, chunks, size)
	var out bytes.Buffer
	copied, err := io.Copy(&out, reader)
	if err != nil {
		t.Fatalf("io.Copy failed: %v", err)
	}
	if copied != size {
		t.Fatalf("io.Copy copied %d bytes, want %d", copied, size)
	}
	if !bytes.Equal(out.Bytes(), want) {
		t.Fatal("io.Copy output does not match expected chunk bytes")
	}
}
