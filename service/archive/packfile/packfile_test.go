package packfile

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"
)

// buildPack assembles a package byte slice using the same framing Read/ReadBytes
// consume, so the two readers can be cross-checked on identical input.
func buildPack(entries []Entry) []byte {
	var buf []byte
	var magic [HeaderSize]byte
	binary.LittleEndian.PutUint32(magic[:], PackageMagic)
	buf = append(buf, magic[:]...)

	for _, e := range entries {
		var header [EntryHeaderSize]byte
		binary.LittleEndian.PutUint32(header[:4], uint32(EntryMagic)|uint32(len(e.Name))<<16)
		binary.LittleEndian.PutUint32(header[4:], uint32(len(e.Data)))
		buf = append(buf, header[:]...)
		buf = append(buf, []byte(e.Name)...)
		buf = append(buf, e.Data...)
	}
	return buf
}

func collect(t *testing.T, read func(func(Entry) error) error) []Entry {
	t.Helper()
	var got []Entry
	if err := read(func(e Entry) error {
		got = append(got, Entry{Name: e.Name, Data: e.Data, DataSize: e.DataSize})
		return nil
	}); err != nil {
		t.Fatalf("read returned error: %v", err)
	}
	return got
}

func TestReadBytesMatchesRead(t *testing.T) {
	entries := []Entry{
		{Name: "block_a", Data: []byte("hello")},
		{Name: "proof_b", Data: []byte{}}, // zero-length entry
		{Name: "prooflink_c", Data: bytes.Repeat([]byte{0xAB}, 300)},
	}
	pack := buildPack(entries)

	streamed := collect(t, func(h func(Entry) error) error {
		return Read(context.Background(), bytes.NewReader(pack), h)
	})
	inplace := collect(t, func(h func(Entry) error) error {
		return ReadBytes(context.Background(), pack, h)
	})

	if len(streamed) != len(entries) || len(inplace) != len(entries) {
		t.Fatalf("entry count mismatch: stream=%d inplace=%d want=%d", len(streamed), len(inplace), len(entries))
	}
	for i := range entries {
		if streamed[i].Name != entries[i].Name || inplace[i].Name != entries[i].Name {
			t.Fatalf("entry %d name mismatch: stream=%q inplace=%q want=%q", i, streamed[i].Name, inplace[i].Name, entries[i].Name)
		}
		if !bytes.Equal(streamed[i].Data, entries[i].Data) || !bytes.Equal(inplace[i].Data, entries[i].Data) {
			t.Fatalf("entry %d data mismatch", i)
		}
	}
}

func TestReadBytesIsZeroCopy(t *testing.T) {
	pack := buildPack([]Entry{{Name: "block_a", Data: []byte("payload-bytes")}})

	var entryData []byte
	if err := ReadBytes(context.Background(), pack, func(e Entry) error {
		entryData = e.Data
		return nil
	}); err != nil {
		t.Fatalf("ReadBytes returned error: %v", err)
	}
	if len(entryData) == 0 {
		t.Fatal("expected entry data")
	}
	// The entry must alias the input buffer, not a copy: mutating the underlying
	// pack must be visible through the returned slice.
	entryStart := int(binary.LittleEndian.Uint32(pack[HeaderSize:HeaderSize+4])>>16) + HeaderSize + EntryHeaderSize
	pack[entryStart] ^= 0xFF
	if entryData[0] != pack[entryStart] {
		t.Fatal("ReadBytes returned a copy; expected a zero-copy sub-slice of the input")
	}
}

func TestReadBytesRejectsBadMagic(t *testing.T) {
	pack := buildPack([]Entry{{Name: "block_a", Data: []byte("x")}})
	pack[0] ^= 0xFF
	if err := ReadBytes(context.Background(), pack, func(Entry) error { return nil }); err == nil {
		t.Fatal("expected magic mismatch error")
	}
}

func TestReadBytesRejectsTruncatedData(t *testing.T) {
	pack := buildPack([]Entry{{Name: "block_a", Data: []byte("full-payload")}})
	truncated := pack[:len(pack)-3] // cut into the entry data
	if err := ReadBytes(context.Background(), truncated, func(Entry) error { return nil }); err == nil {
		t.Fatal("expected truncated-data error")
	}
}

func TestReadBytesEmptyPackHasNoEntries(t *testing.T) {
	pack := buildPack(nil)
	got := collect(t, func(h func(Entry) error) error {
		return ReadBytes(context.Background(), pack, h)
	})
	if len(got) != 0 {
		t.Fatalf("expected no entries, got %d", len(got))
	}
}
