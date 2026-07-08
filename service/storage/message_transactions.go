package storage

import (
	"fmt"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type MessageTransactionKind uint8

const (
	MessageTransactionInbound MessageTransactionKind = iota + 1
	MessageTransactionOutbound
)

type MessageTransactionAddress struct {
	Workchain int32
	Account   [32]byte
}

type MessageTransactionKey struct {
	Source      MessageTransactionAddress
	Destination MessageTransactionAddress
	CreatedLT   uint64
}

type MessageTransactionRef struct {
	Block     ton.BlockIDExt
	Workchain int32
	Account   [32]byte
	LT        uint64
	Hash      [32]byte
}

type MessageTransactionIndexEntry struct {
	Kind MessageTransactionKind
	Key  MessageTransactionKey
	Ref  MessageTransactionRef
}

func cloneMessageTransactionEntries(entries []MessageTransactionIndexEntry) []MessageTransactionIndexEntry {
	if entries == nil {
		return nil
	}

	cloned := make([]MessageTransactionIndexEntry, len(entries))
	copy(cloned, entries)
	return cloned
}

func MessageTransactionAddressFromTON(addr *address.Address) (MessageTransactionAddress, error) {
	value, ok := messageTransactionAddressFromTON(addr)
	if !ok {
		return MessageTransactionAddress{}, fmt.Errorf("message transaction address is not a standard 256-bit address")
	}
	return value, nil
}

func MessageTransactionAddressFromRaw(workchain int32, account []byte) (MessageTransactionAddress, error) {
	if len(account) != 32 {
		return MessageTransactionAddress{}, fmt.Errorf("message transaction account has invalid size %d", len(account))
	}

	value := MessageTransactionAddress{Workchain: workchain}
	copy(value.Account[:], account)
	return value, nil
}

func MessageTransactionEntriesFromBlockCell(id ton.BlockIDExt, root *cell.Cell) ([]MessageTransactionIndexEntry, error) {
	block, err := ParseVerifiedBlockCell(id, root)
	if err != nil {
		return nil, err
	}
	return MessageTransactionEntriesFromParsedBlock(id, block)
}

func MessageTransactionEntriesFromParsedBlock(id ton.BlockIDExt, block *tlb.Block) ([]MessageTransactionIndexEntry, error) {
	txs, err := block.ListTransactions()
	if err != nil {
		return nil, fmt.Errorf("list transactions in %s: %w", FormatBlockRef(id), err)
	}

	entries := make([]MessageTransactionIndexEntry, 0)
	for _, tx := range txs {
		ref, err := messageTransactionRefFromTLB(id, tx)
		if err != nil {
			return nil, err
		}

		if tx.IO.In != nil {
			entry, ok := messageTransactionEntryFromMessage(MessageTransactionInbound, tx.IO.In, ref)
			if ok {
				entries = append(entries, entry)
			}
		}

		if tx.IO.Out == nil {
			continue
		}
		outMessages, err := tx.IO.Out.ToSlice()
		if err != nil {
			return nil, fmt.Errorf("list transaction outgoing messages in %s: %w", FormatBlockRef(id), err)
		}
		for i := range outMessages {
			entry, ok := messageTransactionEntryFromMessage(MessageTransactionOutbound, &outMessages[i], ref)
			if ok {
				entries = append(entries, entry)
			}
		}
	}
	return entries, nil
}

func messageTransactionRefFromTLB(id ton.BlockIDExt, tx *tlb.Transaction) (MessageTransactionRef, error) {
	account, err := MessageTransactionAddressFromRaw(id.Workchain, tx.AccountAddr)
	if err != nil {
		return MessageTransactionRef{}, err
	}

	hash := tx.Hash
	if len(hash) == 0 {
		txCell, err := tx.ToCell()
		if err != nil {
			return MessageTransactionRef{}, fmt.Errorf("serialize transaction: %w", err)
		}
		hash = txCell.Hash()
	}
	if len(hash) != 32 {
		return MessageTransactionRef{}, fmt.Errorf("transaction hash has invalid size %d", len(hash))
	}

	ref := MessageTransactionRef{
		Block:     id,
		Workchain: id.Workchain,
		Account:   account.Account,
		LT:        tx.LT,
	}
	copy(ref.Hash[:], hash)
	return ref, nil
}

func messageTransactionEntryFromMessage(kind MessageTransactionKind, msg *tlb.Message, ref MessageTransactionRef) (MessageTransactionIndexEntry, bool) {
	if msg == nil || msg.Msg == nil {
		return MessageTransactionIndexEntry{}, false
	}

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

func messageTransactionAddressFromTON(addr *address.Address) (MessageTransactionAddress, bool) {
	if addr == nil || addr.Type() != address.StdAddress || addr.BitsLen() != 256 {
		return MessageTransactionAddress{}, false
	}
	data := addr.Data()
	if len(data) != 32 {
		return MessageTransactionAddress{}, false
	}

	value := MessageTransactionAddress{Workchain: addr.Workchain()}
	copy(value.Account[:], data)
	return value, true
}
