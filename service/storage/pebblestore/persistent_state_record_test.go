package pebblestore

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/xssnick/gton/service/storage"
)

func TestDecodePersistentStateFileRecordVersions(t *testing.T) {
	fileHash := bytes.Repeat([]byte{0x11}, 32)
	stateRootHash := bytes.Repeat([]byte{0x22}, 32)
	v1 := encodePersistentStateFileRecord(&storage.PersistentStateFile{
		Ref:           &storage.ArtifactRef{Size: 1234},
		FileHash:      fileHash,
		StateRootHash: stateRootHash,
	})
	v2 := append([]byte(nil), v1...)
	v2[0] = persistentStateCellsCountVersion
	v2 = binary.BigEndian.AppendUint64(v2, 5678)

	tests := []struct {
		name string
		data []byte
	}{
		{name: "v1", data: v1},
		{name: "v2", data: v2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record, err := decodePersistentStateFileRecord(test.data)
			if err != nil {
				t.Fatalf("decode persistent state record: %v", err)
			}
			if record.size != 1234 {
				t.Fatalf("record size = %d, want 1234", record.size)
			}
			if !bytes.Equal(record.fileHash, fileHash) {
				t.Fatal("file hash mismatch")
			}
			if !bytes.Equal(record.stateRootHash, stateRootHash) {
				t.Fatal("state root hash mismatch")
			}
		})
	}
}
