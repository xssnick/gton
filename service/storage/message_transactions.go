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
	if block == nil {
		return nil, fmt.Errorf("list transactions in %s: block is nil", FormatBlockRef(id))
	}
	if block.Extra == nil || block.Extra.ShardAccountBlocks == nil {
		return nil, fmt.Errorf("list transactions in %s: block has no shard account blocks", FormatBlockRef(id))
	}

	accounts, err := block.Extra.ShardAccountBlocks.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("list transactions in %s: failed to load shard account blocks: %w", FormatBlockRef(id), err)
	}
	hasAccounts, err := accounts.LoadBoolBit()
	if err != nil {
		return nil, fmt.Errorf("list transactions in %s: failed to load shard account blocks root flag: %w", FormatBlockRef(id), err)
	}
	if !hasAccounts {
		return []MessageTransactionIndexEntry{}, nil
	}
	root, err := accounts.LoadRefCell()
	if err != nil {
		return nil, fmt.Errorf("list transactions in %s: failed to load shard account blocks root: %w", FormatBlockRef(id), err)
	}
	accountList, err := root.AsDict(256).LoadAll()
	if err != nil {
		return nil, fmt.Errorf("list transactions in %s: failed to load account blocks: %w", FormatBlockRef(id), err)
	}

	entries := make([]MessageTransactionIndexEntry, 0)
	for _, accountKV := range accountList {
		if err = skipMessageTransactionCurrencyCollection(accountKV.Value); err != nil {
			return nil, fmt.Errorf("list transactions in %s: failed to load account block fees: %w", FormatBlockRef(id), err)
		}

		magic, err := accountKV.Value.LoadUInt(4)
		if err != nil {
			return nil, fmt.Errorf("list transactions in %s: failed to load account block magic: %w", FormatBlockRef(id), err)
		}
		if magic != 0x5 {
			return nil, fmt.Errorf("list transactions in %s: invalid account block magic %x", FormatBlockRef(id), magic)
		}
		if err = accountKV.Value.SkipBits(256); err != nil {
			return nil, fmt.Errorf("list transactions in %s: failed to load account block address: %w", FormatBlockRef(id), err)
		}
		var transactions tlb.AccountTransactionsAugDict
		if err = tlb.LoadFromCell(&transactions, accountKV.Value); err != nil {
			return nil, fmt.Errorf("list transactions in %s: failed to load account transactions root: %w", FormatBlockRef(id), err)
		}
		if transactions.IsEmpty() {
			continue
		}
		txRoot, err := transactions.InlineCell()
		if err != nil {
			return nil, fmt.Errorf("list transactions in %s: failed to load account transactions root: %w", FormatBlockRef(id), err)
		}
		txList, err := txRoot.AsDict(64).LoadAll()
		if err != nil {
			return nil, fmt.Errorf("list transactions in %s: failed to load account transactions: %w", FormatBlockRef(id), err)
		}

		for _, txKV := range txList {
			if err = skipMessageTransactionCurrencyCollection(txKV.Value); err != nil {
				return nil, fmt.Errorf("list transactions in %s: failed to load tx fees: %w", FormatBlockRef(id), err)
			}
			txCell, err := txKV.Value.LoadRefCell()
			if err != nil {
				return nil, fmt.Errorf("list transactions in %s: failed to load tx ref: %w", FormatBlockRef(id), err)
			}

			entries, err = appendMessageTransactionEntries(entries, id, txCell)
			if err != nil {
				return nil, fmt.Errorf("list transactions in %s: failed to load tx index fields: %w", FormatBlockRef(id), err)
			}
		}
	}
	return entries, nil
}

func appendMessageTransactionEntries(entries []MessageTransactionIndexEntry, id ton.BlockIDExt, txCell *cell.Cell) ([]MessageTransactionIndexEntry, error) {
	// The index needs only the transaction account/LT, its original cell hash,
	// and the IO reference. The remaining transaction fields are intentionally
	// left encoded in txCell.
	loader, err := txCell.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("begin parse transaction: %w", err)
	}
	magic, err := loader.LoadUInt(4)
	if err != nil {
		return nil, fmt.Errorf("load transaction magic: %w", err)
	}
	if magic != 0b0111 {
		return nil, fmt.Errorf("invalid transaction magic %04b", magic)
	}
	var account [32]byte
	if err = loader.LoadSliceInto(account[:], 256); err != nil {
		return nil, fmt.Errorf("load transaction account: %w", err)
	}
	lt, err := loader.LoadUInt(64)
	if err != nil {
		return nil, fmt.Errorf("load transaction lt: %w", err)
	}
	ioCell, err := loader.LoadRefCell()
	if err != nil {
		return nil, fmt.Errorf("load transaction io: %w", err)
	}

	ref := MessageTransactionRef{
		Block:     id,
		Workchain: id.Workchain,
		Account:   account,
		LT:        lt,
	}
	txHash := txCell.HashKey()
	copy(ref.Hash[:], txHash[:])

	io, err := ioCell.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("begin parse transaction io: %w", err)
	}
	hasInbound, err := io.LoadBoolBit()
	if err != nil {
		return nil, fmt.Errorf("load inbound message flag: %w", err)
	}
	if hasInbound {
		messageCell, err := io.LoadRefCell()
		if err != nil {
			return nil, fmt.Errorf("load inbound message ref: %w", err)
		}
		entry, ok, err := messageTransactionEntryFromCell(MessageTransactionInbound, messageCell, ref)
		if err != nil {
			return nil, fmt.Errorf("load inbound message: %w", err)
		}
		if ok {
			entries = append(entries, entry)
		}
	}

	outMessages, err := io.LoadDict(15)
	if err != nil {
		return nil, fmt.Errorf("load outgoing messages dictionary: %w", err)
	}
	if outMessages.IsEmpty() {
		return entries, nil
	}
	outList, err := outMessages.LoadAll()
	if err != nil {
		return nil, fmt.Errorf("list outgoing messages: %w", err)
	}
	for i, item := range outList {
		messageCell, err := item.Value.LoadRefCell()
		if err != nil {
			return nil, fmt.Errorf("load outgoing message %d ref: %w", i, err)
		}
		entry, ok, err := messageTransactionEntryFromCell(MessageTransactionOutbound, messageCell, ref)
		if err != nil {
			return nil, fmt.Errorf("load outgoing message %d: %w", i, err)
		}
		if ok {
			entries = append(entries, entry)
		}
	}
	return entries, nil
}

func messageTransactionEntryFromCell(kind MessageTransactionKind, message *cell.Cell, ref MessageTransactionRef) (MessageTransactionIndexEntry, bool, error) {
	loader, err := message.BeginParse()
	if err != nil {
		return MessageTransactionIndexEntry{}, false, fmt.Errorf("begin parse message: %w", err)
	}
	isExternal, err := loader.LoadBoolBit()
	if err != nil {
		return MessageTransactionIndexEntry{}, false, fmt.Errorf("load message type: %w", err)
	}
	if isExternal {
		return MessageTransactionIndexEntry{}, false, nil
	}
	if _, err = loader.LoadUInt(3); err != nil {
		return MessageTransactionIndexEntry{}, false, fmt.Errorf("load internal message flags: %w", err)
	}
	source, sourceOK, err := loadMessageTransactionAddress(loader)
	if err != nil {
		return MessageTransactionIndexEntry{}, false, fmt.Errorf("load internal message source: %w", err)
	}
	destination, destinationOK, err := loadMessageTransactionAddress(loader)
	if err != nil {
		return MessageTransactionIndexEntry{}, false, fmt.Errorf("load internal message destination: %w", err)
	}
	if err = skipMessageTransactionCoins(loader); err != nil {
		return MessageTransactionIndexEntry{}, false, fmt.Errorf("load internal message amount: %w", err)
	}
	if err = skipMessageTransactionMaybeRef(loader); err != nil {
		return MessageTransactionIndexEntry{}, false, fmt.Errorf("load internal message extra currencies: %w", err)
	}
	if err = skipMessageTransactionCoins(loader); err != nil {
		return MessageTransactionIndexEntry{}, false, fmt.Errorf("load internal message ihr fee: %w", err)
	}
	if err = skipMessageTransactionCoins(loader); err != nil {
		return MessageTransactionIndexEntry{}, false, fmt.Errorf("load internal message forward fee: %w", err)
	}
	createdLT, err := loader.LoadUInt(64)
	if err != nil {
		return MessageTransactionIndexEntry{}, false, fmt.Errorf("load internal message created lt: %w", err)
	}
	if createdLT == 0 {
		return MessageTransactionIndexEntry{}, false, nil
	}

	if !sourceOK {
		return MessageTransactionIndexEntry{}, false, nil
	}
	if !destinationOK {
		return MessageTransactionIndexEntry{}, false, nil
	}

	return MessageTransactionIndexEntry{
		Kind: kind,
		Key: MessageTransactionKey{
			Source:      source,
			Destination: destination,
			CreatedLT:   createdLT,
		},
		Ref: ref,
	}, true, nil
}

func skipMessageTransactionCurrencyCollection(loader *cell.Slice) error {
	if err := skipMessageTransactionCoins(loader); err != nil {
		return err
	}
	return skipMessageTransactionMaybeRef(loader)
}

func skipMessageTransactionCoins(loader *cell.Slice) error {
	byteLen, err := loader.LoadUInt(4)
	if err != nil {
		return err
	}
	return loader.SkipBits(uint(byteLen * 8))
}

func skipMessageTransactionMaybeRef(loader *cell.Slice) error {
	hasRef, err := loader.LoadBoolBit()
	if err != nil {
		return err
	}
	if !hasRef {
		return nil
	}
	_, err = loader.LoadRefCell()
	return err
}

func loadMessageTransactionAddress(loader *cell.Slice) (MessageTransactionAddress, bool, error) {
	kind, err := loader.LoadUInt(2)
	if err != nil {
		return MessageTransactionAddress{}, false, err
	}

	switch kind {
	case 0:
		return MessageTransactionAddress{}, false, nil
	case 1:
		bits, err := loader.LoadUInt(9)
		if err != nil {
			return MessageTransactionAddress{}, false, err
		}
		return MessageTransactionAddress{}, false, loader.SkipBits(uint(bits))
	case 2:
		if err = skipMessageTransactionAnycast(loader); err != nil {
			return MessageTransactionAddress{}, false, err
		}
		workchain, err := loader.LoadUInt(8)
		if err != nil {
			return MessageTransactionAddress{}, false, err
		}
		value := MessageTransactionAddress{Workchain: int32(int8(workchain))}
		if err = loader.LoadSliceInto(value.Account[:], 256); err != nil {
			return MessageTransactionAddress{}, false, err
		}
		return value, true, nil
	case 3:
		if err = skipMessageTransactionAnycast(loader); err != nil {
			return MessageTransactionAddress{}, false, err
		}
		bits, err := loader.LoadUInt(9)
		if err != nil {
			return MessageTransactionAddress{}, false, err
		}
		if err = loader.SkipBits(32); err != nil {
			return MessageTransactionAddress{}, false, err
		}
		return MessageTransactionAddress{}, false, loader.SkipBits(uint(bits))
	default:
		return MessageTransactionAddress{}, false, fmt.Errorf("unsupported message address kind %d", kind)
	}
}

func skipMessageTransactionAnycast(loader *cell.Slice) error {
	hasAnycast, err := loader.LoadBoolBit()
	if err != nil {
		return err
	}
	if !hasAnycast {
		return nil
	}
	depth, err := loader.LoadUInt(5)
	if err != nil {
		return err
	}
	if depth == 0 || depth > 30 {
		return fmt.Errorf("invalid anycast depth %d", depth)
	}
	return loader.SkipBits(uint(depth))
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
