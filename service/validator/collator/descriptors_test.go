package collator

import (
	"fmt"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// insertDescriptor writes one descriptor on its own. Collation batches them
// through descriptorBatch; this is the single-entry form the fixtures and the
// duplicate-rejection test are written against.
func insertDescriptor(dict *cell.AugmentedDictionary, message, descriptor *cell.Cell) error {
	hash := message.HashKey()
	key := cell.BeginCell().MustStoreSlice(hash[:], 256).EndCell()
	inserted, err := dict.SetWithMode(key, descriptor, cell.DictSetModeAdd)
	if err != nil {
		return err
	}
	if !inserted {
		return fmt.Errorf("%w: duplicate message descriptor %x", ErrInvalidInput, hash)
	}
	return nil
}

func TestDescriptorInsertRejectsDuplicateMessage(t *testing.T) {
	dict, err := tlb.NewOutMsgDescrAugDict(0)
	if err != nil {
		t.Fatal(err)
	}
	msg := cell.BeginCell().MustStoreUInt(0b11, 2).EndCell()
	descriptor := cell.BeginCell().MustStoreUInt(0, 3).MustStoreRef(msg).MustStoreRef(msg).EndCell()

	if err = insertDescriptor(dict.AugmentedDictionary, msg, descriptor); err != nil {
		t.Fatal(err)
	}
	if err = insertDescriptor(dict.AugmentedDictionary, msg, descriptor); err == nil {
		t.Fatal("duplicate insert succeeded")
	}
}

// The batched path is what collation uses, so the duplicate rejection has to be
// tested there rather than only on the single-entry helper above — a bulk write
// that quietly accepted a repeat would ship a block with a descriptor missing.
func TestDescriptorBatchRejectsDuplicateMessage(t *testing.T) {
	dict, err := tlb.NewInMsgDescrAugDict(supportedSoftwareVersion)
	if err != nil {
		t.Fatal(err)
	}
	msg := cell.BeginCell().MustStoreUInt(0xfeed, 32).EndCell()
	value, err := descriptor(0b000, 3, msg, msg) // msg_import_ext$000
	if err != nil {
		t.Fatal(err)
	}

	var batch descriptorBatch
	batch.addKeyed(descriptorKey(msg), value)
	batch.addKeyed(descriptorKey(msg), value)
	err = batch.flush(dict.AugmentedDictionary)
	if err == nil {
		t.Fatal("a batch holding the same message twice must be rejected")
	}
	hash := msg.HashKey()
	if !strings.Contains(err.Error(), fmt.Sprintf("%x", hash[:])) {
		t.Fatalf("error does not name the duplicated message: %v", err)
	}

	// And against what the dictionary already holds, not just within the batch.
	var first descriptorBatch
	first.addKeyed(descriptorKey(msg), value)
	if err = first.flush(dict.AugmentedDictionary); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	var again descriptorBatch
	again.addKeyed(descriptorKey(msg), value)
	if err = again.flush(dict.AugmentedDictionary); err == nil {
		t.Fatal("re-inserting a message already in the dictionary must be rejected")
	}
}
