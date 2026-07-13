package storage

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestMessageTransactionEntriesMatchFullTransactionParse(t *testing.T) {
	id, block := loadMessageTransactionBlockFixture(t)

	got, err := MessageTransactionEntriesFromParsedBlock(id, block)
	if err != nil {
		t.Fatalf("extract trimmed message transaction entries: %v", err)
	}
	want, err := legacyMessageTransactionEntriesFromParsedBlock(id, block)
	if err != nil {
		t.Fatalf("extract full-parse message transaction entries: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("fixture contains no indexable messages")
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("trimmed entries differ from full transaction parse:\ngot:  %#v\nwant: %#v", got, want)
	}
}

func TestMessageTransactionEntryErrorsNameRequiredField(t *testing.T) {
	truncated := cell.BeginCell().
		MustStoreBoolBit(false).
		MustStoreUInt(0, 3).
		EndCell()

	_, _, err := messageTransactionEntryFromCell(MessageTransactionInbound, truncated, MessageTransactionRef{})
	if err == nil || !strings.Contains(err.Error(), "internal message source") {
		t.Fatalf("truncated source error = %v", err)
	}
}

func BenchmarkMessageTransactionEntriesFromParsedBlock(b *testing.B) {
	id, block := loadMessageTransactionBlockFixture(b)

	b.Run("trimmed", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := MessageTransactionEntriesFromParsedBlock(id, block); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("full_transaction_parse", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := legacyMessageTransactionEntriesFromParsedBlock(id, block); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func legacyMessageTransactionEntriesFromParsedBlock(id ton.BlockIDExt, block *tlb.Block) ([]MessageTransactionIndexEntry, error) {
	txs, err := block.ListTransactions()
	if err != nil {
		return nil, err
	}

	entries := make([]MessageTransactionIndexEntry, 0)
	for _, tx := range txs {
		account, err := MessageTransactionAddressFromRaw(id.Workchain, tx.AccountAddr)
		if err != nil {
			return nil, err
		}
		ref := MessageTransactionRef{
			Block:     id,
			Workchain: id.Workchain,
			Account:   account.Account,
			LT:        tx.LT,
		}
		copy(ref.Hash[:], tx.Hash)

		if tx.IO.In != nil {
			if entry, ok := legacyMessageTransactionEntryFromMessage(MessageTransactionInbound, tx.IO.In, ref); ok {
				entries = append(entries, entry)
			}
		}
		if tx.IO.Out == nil {
			continue
		}
		out, err := tx.IO.Out.ToSlice()
		if err != nil {
			return nil, err
		}
		for i := range out {
			if entry, ok := legacyMessageTransactionEntryFromMessage(MessageTransactionOutbound, &out[i], ref); ok {
				entries = append(entries, entry)
			}
		}
	}
	return entries, nil
}

func legacyMessageTransactionEntryFromMessage(kind MessageTransactionKind, msg *tlb.Message, ref MessageTransactionRef) (MessageTransactionIndexEntry, bool) {
	internal, ok := msg.Msg.(*tlb.InternalMessage)
	if !ok || internal.CreatedLT == 0 {
		return MessageTransactionIndexEntry{}, false
	}
	source, ok := messageTransactionAddressFromTON(internal.SrcAddr)
	if !ok {
		return MessageTransactionIndexEntry{}, false
	}
	destination, ok := messageTransactionAddressFromTON(internal.DstAddr)
	if !ok {
		return MessageTransactionIndexEntry{}, false
	}
	return MessageTransactionIndexEntry{
		Kind: kind,
		Key: MessageTransactionKey{
			Source:      source,
			Destination: destination,
			CreatedLT:   internal.CreatedLT,
		},
		Ref: ref,
	}, true
}

func loadMessageTransactionBlockFixture(tb testing.TB) (ton.BlockIDExt, *tlb.Block) {
	tb.Helper()

	raw, err := os.ReadFile("../testdata/masterchain_block_fixture.json")
	if err != nil {
		tb.Fatalf("read block fixture: %v", err)
	}
	var fixture struct {
		RawBOCBase64 string `json:"raw_boc_base64"`
		Block        struct {
			Workchain int32  `json:"workchain"`
			Shard     string `json:"shard"`
			SeqNo     uint32 `json:"seqno"`
			RootHash  string `json:"root_hash_hex"`
			FileHash  string `json:"file_hash_hex"`
		} `json:"block"`
	}
	if err = json.Unmarshal(raw, &fixture); err != nil {
		tb.Fatalf("decode block fixture: %v", err)
	}
	boc, err := base64.StdEncoding.DecodeString(fixture.RawBOCBase64)
	if err != nil {
		tb.Fatalf("decode block boc: %v", err)
	}
	root, err := cell.FromBOC(boc)
	if err != nil {
		tb.Fatalf("parse block boc: %v", err)
	}
	shard, err := strconv.ParseUint(fixture.Block.Shard, 16, 64)
	if err != nil {
		tb.Fatalf("parse block shard: %v", err)
	}
	rootHash, err := hex.DecodeString(fixture.Block.RootHash)
	if err != nil {
		tb.Fatalf("decode block root hash: %v", err)
	}
	fileHash, err := hex.DecodeString(fixture.Block.FileHash)
	if err != nil {
		tb.Fatalf("decode block file hash: %v", err)
	}
	id := ton.BlockIDExt{
		Workchain: fixture.Block.Workchain,
		Shard:     int64(shard),
		SeqNo:     fixture.Block.SeqNo,
		RootHash:  rootHash,
		FileHash:  fileHash,
	}
	block, err := ParseVerifiedBlockCell(id, root)
	if err != nil {
		tb.Fatalf("parse verified block: %v", err)
	}
	return id, block
}
