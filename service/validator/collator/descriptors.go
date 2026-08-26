package collator

import (
	"fmt"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// descriptor builds an InMsg/OutMsg descriptor value: a constructor tag of
// tagBits, then its cell references in wire order. The TL-B constructor each tag
// belongs to is named at the call site.
func descriptor(tag uint64, tagBits uint, refs ...*cell.Cell) (*cell.Cell, error) {
	b := cell.BeginCell()
	if err := b.StoreUInt(tag, tagBits); err != nil {
		return nil, err
	}
	for _, ref := range refs {
		if err := b.StoreRef(ref); err != nil {
			return nil, err
		}
	}

	return b.EndCell(), nil
}

// descriptorFee builds an import descriptor with a forward-fee tail:
// in_msg:^MsgEnvelope transaction:^Transaction fwd_fee:Grams.
func descriptorFee(
	tag uint64,
	tagBits uint,
	envelope, transaction *cell.Cell,
	fee tlb.Coins,
) (*cell.Cell, error) {
	b := cell.BeginCell()
	if err := b.StoreUInt(tag, tagBits); err != nil {
		return nil, err
	}
	if err := b.StoreRef(envelope); err != nil {
		return nil, err
	}
	if err := b.StoreRef(transaction); err != nil {
		return nil, err
	}
	if err := b.StoreBigCoins(fee.NanoRef()); err != nil {
		return nil, err
	}

	return b.EndCell(), nil
}

// descriptorDequeueShort builds msg_export_deq_short$1101 msg_env_hash:bits256
// next_workchain:int32 next_addr_pfx:uint64 import_block_lt:uint64. The
// next-hop fields are exactly the head of the out-queue key.
func descriptorDequeueShort(envHash [32]byte, key msgpool.QueueKey, importBlockLT uint64) (*cell.Cell, error) {
	b := cell.BeginCell()
	if err := b.StoreUInt(0b1101, 4); err != nil {
		return nil, err
	}
	if err := b.StoreSlice(envHash[:], 256); err != nil {
		return nil, err
	}
	if err := b.StoreSlice(key[:12], 96); err != nil {
		return nil, err
	}
	if err := b.StoreUInt(importBlockLT, 64); err != nil {
		return nil, err
	}

	return b.EndCell(), nil
}

// descriptorFlushMask flushes a batch every 64 descriptors, matching the cadence
// on which the limiter already sampled the dictionary root.
const descriptorFlushMask = 63

// descriptorBatch collects descriptor writes and applies them to their
// dictionary together. One write at a time rebuilds the whole path from leaf to
// root, recomputing the augmentation and rehashing every fork on the way; a
// batch walks the shared upper nodes once for the whole group. Nothing reads
// these dictionaries while the block is being executed — only the limiter, and
// only its root, on the cadence below — so the entries owe their presence to
// the flush rather than to each individual insert.
// The dictionary is passed in rather than held: a collation is built in several
// places, and a batch carrying its own reference is one more field each of them
// could forget to wire.
type descriptorBatch struct {
	ops     uint64
	pending []descriptorEntry
	entries []cell.AugmentedBytesEntry
}

type descriptorEntry struct {
	key   cell.Hash
	value *cell.Cell
}

// descriptorKey is the message-hash key both descriptor dictionaries use.
func descriptorKey(message *cell.Cell) cell.Hash {
	return message.HashKey()
}

func (b *descriptorBatch) addKeyed(key cell.Hash, descriptor *cell.Cell) {
	b.pending = append(b.pending, descriptorEntry{
		key:   key,
		value: descriptor,
	})
}

func (c *collation) flushDescriptors() error {
	if err := c.inDescr.flush(c.inMessages.AugmentedDictionary); err != nil {
		return fmt.Errorf("inbound %w", err)
	}
	if err := c.outDescr.flush(c.outMessages.AugmentedDictionary); err != nil {
		return fmt.Errorf("outbound %w", err)
	}
	return nil
}

// flush applies the pending writes. Add mode is what rejects a repeated
// message, both against the dictionary and within the batch itself, so the
// duplicate check the one-at-a-time path performed is preserved rather than
// dropped — it just reports on the group instead of on the single insert.
func (b *descriptorBatch) flush(dict *cell.AugmentedDictionary) error {
	if len(b.pending) == 0 {
		return nil
	}
	b.entries = b.entries[:0]
	for i := range b.pending {
		b.entries = append(b.entries, cell.AugmentedBytesEntry{
			Key:   b.pending[i].key[:],
			Value: b.pending[i].value,
			Mode:  cell.DictSetModeAdd,
		})
	}
	if err := dict.SetManyByBytes(b.entries, collationParallelism); err != nil {
		if hash, found := b.duplicateMessage(dict); found {
			return fmt.Errorf("%w: duplicate message descriptor %x", ErrInvalidInput, hash)
		}
		return fmt.Errorf("%w: message descriptors: %w", ErrInvalidInput, err)
	}
	b.pending = b.pending[:0]
	b.entries = b.entries[:0]
	return nil
}

// duplicateMessage names the message a rejected batch collided on. A bulk write
// reports that some key was already present without saying which, and the
// message hash is what an operator needs; recovering it costs a scan, which is
// affordable because collation is failing anyway.
func (b *descriptorBatch) duplicateMessage(dict *cell.AugmentedDictionary) ([]byte, bool) {
	seen := make(map[[32]byte]struct{}, len(b.pending))
	for _, entry := range b.pending {
		hash := [32]byte(entry.key)
		if _, repeated := seen[hash]; repeated {
			return hash[:], true
		}
		seen[hash] = struct{}{}
		var value cell.Slice
		if err := dict.LoadValueByBytesKeyInto(hash[:], &value); err == nil {
			return hash[:], true
		}
	}
	return nil, false
}
