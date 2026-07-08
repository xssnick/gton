package storage

import (
	"bytes"
	"testing"

	"github.com/xssnick/tonutils-go/ton"
)

func TestServedBlockFullClonePreservesMessageEntriesPresence(t *testing.T) {
	tests := []struct {
		name      string
		entries   []MessageTransactionIndexEntry
		wantNil   bool
		wantCount int
	}{
		{
			name:    "missing index remains nil",
			entries: nil,
			wantNil: true,
		},
		{
			name:      "prepared empty index remains non-nil",
			entries:   []MessageTransactionIndexEntry{},
			wantCount: 0,
		},
		{
			name: "prepared index entries are copied",
			entries: []MessageTransactionIndexEntry{{
				Kind: MessageTransactionInbound,
				Key:  MessageTransactionKey{CreatedLT: 11},
				Ref:  MessageTransactionRef{LT: 22},
			}},
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cloned := (&ServedBlockFull{MessageEntries: tt.entries}).Clone()
			if gotNil := cloned.MessageEntries == nil; gotNil != tt.wantNil {
				t.Fatalf("cloned message entries nil = %v, want %v", gotNil, tt.wantNil)
			}
			if len(cloned.MessageEntries) != tt.wantCount {
				t.Fatalf("cloned message entries = %d, want %d", len(cloned.MessageEntries), tt.wantCount)
			}
		})
	}
}

func TestServedBlockFullCloneCopiesMessageEntries(t *testing.T) {
	original := &ServedBlockFull{
		MessageEntries: []MessageTransactionIndexEntry{{
			Kind: MessageTransactionInbound,
			Key:  MessageTransactionKey{CreatedLT: 11},
			Ref:  MessageTransactionRef{LT: 22},
		}},
	}

	cloned := original.Clone()
	cloned.MessageEntries[0].Key.CreatedLT = 33
	cloned.MessageEntries[0].Ref.LT = 44

	if original.MessageEntries[0].Key.CreatedLT != 11 {
		t.Fatalf("original key created lt = %d, want 11", original.MessageEntries[0].Key.CreatedLT)
	}
	if original.MessageEntries[0].Ref.LT != 22 {
		t.Fatalf("original ref lt = %d, want 22", original.MessageEntries[0].Ref.LT)
	}
}

func TestServedBlockFullCloneCopiesBlockIDHashes(t *testing.T) {
	original := &ServedBlockFull{
		ID: ton.BlockIDExt{
			Workchain: 0,
			Shard:     masterchainShard,
			SeqNo:     21,
			RootHash:  bytes.Repeat([]byte{0x11}, 32),
			FileHash:  bytes.Repeat([]byte{0x22}, 32),
		},
	}

	cloned := original.Clone()
	cloned.ID.RootHash[0] = 0xAA
	cloned.ID.FileHash[0] = 0xBB

	if original.ID.RootHash[0] == 0xAA {
		t.Fatal("clone shares block id root hash backing array")
	}
	if original.ID.FileHash[0] == 0xBB {
		t.Fatal("clone shares block id file hash backing array")
	}
}
