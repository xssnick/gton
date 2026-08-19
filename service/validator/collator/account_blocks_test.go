package collator

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// accountBlockKeyCell is the dictionary key a keyed descent takes.
func accountBlockKeyCell(key [32]byte) *cell.Cell {
	return cell.BeginCell().MustStoreSlice(key[:], 256).EndCell()
}

// The index replaces a keyed descent for every account the candidate changed,
// so a binary search over it has to return exactly the leaf the descent would.
// If it ever does not, the precheck validates a different account block than the
// lanes replay.
func TestAccountBlockIndexMatchesKeyedDescent(t *testing.T) {
	replay := semanticMultiAccountReplay(t, 6)
	index := replay.accountBlockIndex
	if index == nil || len(index.entries) != 6 {
		t.Fatalf("structural pass recorded %v account blocks, want 6", index)
	}

	for i := range index.entries {
		key := index.entries[i].key
		entry, ok := index.find(key)
		if !ok {
			t.Fatalf("index does not hold account %x", key)
		}
		if entry != &index.entries[i] {
			t.Fatalf("find(%x) returned entry %p, want %p", key, entry, &index.entries[i])
		}

		descended, err := replay.accountBlocks.LoadValue(accountBlockKeyCell(key))
		if err != nil {
			t.Fatalf("descend to account %x: %v", key, err)
		}
		var want tlb.AccountBlock
		if err = loadExactSlice(&want, descended); err != nil {
			t.Fatalf("decode descended account %x: %v", key, err)
		}
		if !bytes.Equal(entry.block.Addr, want.Addr) {
			t.Fatalf("account %x: indexed address %x, descended %x", key, entry.block.Addr, want.Addr)
		}
		if entry.block.StateUpdate.HashKeyAt(0) != want.StateUpdate.HashKeyAt(0) {
			t.Fatalf("account %x: indexed state update differs from the descended one", key)
		}
		if entry.block.Transactions.RootCell().HashKeyAt(0) != want.Transactions.RootCell().HashKeyAt(0) {
			t.Fatalf("account %x: indexed transactions differ from the descended ones", key)
		}
		if entry.exactErr != nil || entry.updateErr != nil || entry.keyMalformed {
			t.Fatalf("account %x carries a deferred verdict on a valid candidate: %v / %v / %v",
				key, entry.exactErr, entry.updateErr, entry.keyMalformed)
		}

		// The state update is the parse both consumers used to make on their
		// own, so the recorded one has to be the same HashUpdate.
		var update tlb.HashUpdate
		if err = parseExact(&update, want.StateUpdate); err != nil {
			t.Fatalf("parse descended state update %x: %v", key, err)
		}
		if !bytes.Equal(entry.update.OldHash, update.OldHash) ||
			!bytes.Equal(entry.update.NewHash, update.NewHash) {
			t.Fatalf("account %x: indexed hash update differs from the parsed one", key)
		}
	}

	var absent [32]byte
	absent[0] = 0xfe
	if _, ok := index.find(absent); ok {
		t.Fatal("index answers for an account the candidate never touched")
	}
}

// keyed is the whole justification for the binary search: ValidateAll proves the
// trie is a well-formed 256-bit Patricia, so its walk is strictly ascending. A
// candidate that produced a non-ascending walk would silently send the precheck
// down the fallback, so the invariant is asserted rather than assumed.
func TestAccountBlockIndexKeysAreStrictlyAscending(t *testing.T) {
	index := semanticMultiAccountReplay(t, 6).accountBlockIndex
	if !index.keyed {
		t.Fatal("structural pass did not prove the account-block walk ascending")
	}
	for i := 1; i < len(index.entries); i++ {
		if bytes.Compare(index.entries[i-1].key[:], index.entries[i].key[:]) >= 0 {
			t.Fatalf("entry %d key %x does not exceed %x",
				i, index.entries[i].key, index.entries[i-1].key)
		}
	}
}

// tamperedAccountBlockReplay rebuilds the candidate's account-block dictionary
// with one entry replaced (or removed) and re-runs the structural pass over it,
// so the two consumers see exactly what they would see for a candidate that
// arrived that way.
func tamperedAccountBlockReplay(
	t *testing.T,
	accounts int,
	tamper func(*testing.T, [32]byte, *tlb.AccountBlock) *cell.Cell,
) (*semanticReplay, [32]byte) {
	t.Helper()

	replay := semanticMultiAccountReplay(t, accounts)
	target := replay.accountBlockIndex.entries[0].key
	block := replay.accountBlockIndex.entries[0].block

	tampered := &tlb.ShardAccountBlocksAugDict{AugmentedDictionary: replay.accountBlocks.Copy()}
	value := tamper(t, target, &block)
	if value == nil {
		if err := tampered.Delete(accountBlockKeyCell(target)); err != nil {
			t.Fatalf("remove account block %x: %v", target, err)
		}
	} else if err := tampered.Set(accountBlockKeyCell(target), value); err != nil {
		t.Fatalf("replace account block %x: %v", target, err)
	}

	index, err := buildAccountBlockIndex(tampered)
	if err != nil {
		t.Fatalf("structural pass over the tampered dictionary: %v", err)
	}
	replay.accountBlocks = tampered
	replay.accountBlockIndex = index
	replay.candidate.accountBlocks = tampered
	replay.candidate.accountBlockIndex = index

	return replay, target
}

// accountBlockCellWithTrailingBit re-encodes an AccountBlock with one bit of
// trailing data: the entry the structural pass decodes with LoadFromCell and
// both semantic consumers reject with loadExactSlice.
func accountBlockCellWithTrailingBit(t *testing.T, block *tlb.AccountBlock) *cell.Cell {
	t.Helper()

	encoded, err := tlb.ToCell(block)
	if err != nil {
		t.Fatalf("encode account block: %v", err)
	}
	loader := encoded.MustBeginParse()
	bits := loader.BitsLeft()
	data, err := loader.LoadSlice(bits)
	if err != nil {
		t.Fatalf("read account block bits: %v", err)
	}
	builder := cell.BeginCell().MustStoreSlice(data, bits).MustStoreBoolBit(true)
	for loader.RefsNum() > 0 {
		ref, refErr := loader.LoadRefCell()
		if refErr != nil {
			t.Fatalf("read account block ref: %v", refErr)
		}
		builder.MustStoreRef(ref)
	}

	return builder.EndCell()
}

// The point of recording the deferred verdicts instead of raising them in the
// structural pass is that a malformed candidate keeps being rejected by the pass
// that rejected it before, with the words that pass used. This walks the four
// verdicts the index carries and pins both wordings.
func TestAccountBlockDeferredVerdictsRejectAtTheirOldSites(t *testing.T) {
	cases := []struct {
		name     string
		tamper   func(*testing.T, [32]byte, *tlb.AccountBlock) *cell.Cell
		precheck string
		lane     string
	}{
		{
			name: "trailing data",
			tamper: func(t *testing.T, _ [32]byte, block *tlb.AccountBlock) *cell.Cell {
				return accountBlockCellWithTrailingBit(t, block)
			},
			precheck: "decode AccountBlock for changed account %x: trailing data: 1 bits, 0 refs",
			lane:     "decode semantic account block %x",
		},
		{
			name: "address differs from key",
			tamper: func(t *testing.T, _ [32]byte, block *tlb.AccountBlock) *cell.Cell {
				other := bytes.Repeat([]byte{0xab}, 32)
				encoded, err := tlb.ToCell(&tlb.AccountBlock{
					Addr:         other,
					Transactions: block.Transactions,
					StateUpdate:  block.StateUpdate,
				})
				if err != nil {
					t.Fatalf("encode account block: %v", err)
				}
				return encoded
			},
			precheck: "AccountBlock address differs from changed account %x",
			lane:     "semantic account block %x address differs from its key",
		},
		{
			name: "state update is not a HashUpdate",
			tamper: func(t *testing.T, key [32]byte, block *tlb.AccountBlock) *cell.Cell {
				encoded, err := tlb.ToCell(&tlb.AccountBlock{
					Addr:         key[:],
					Transactions: block.Transactions,
					StateUpdate:  cell.BeginCell().MustStoreUInt(0xff, 8).EndCell(),
				})
				if err != nil {
					t.Fatalf("encode account block: %v", err)
				}
				return encoded
			},
			precheck: "decode AccountBlock state update %x",
			lane:     "decode account block state update %x",
		},
		{
			name: "no AccountBlock for a changed account",
			tamper: func(*testing.T, [32]byte, *tlb.AccountBlock) *cell.Cell {
				return nil
			},
			precheck: "changed account %x has no AccountBlock: no such key in dict",
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			replay, target := tamperedAccountBlockReplay(t, 3, test.tamper)
			err := replay.precheckAccountUpdates()
			want := fmt.Sprintf(test.precheck, target)
			if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), want) {
				t.Fatalf("precheck error = %v, want one containing %q", err, want)
			}
			if test.lane == "" {
				return
			}

			// A fresh replay: the lane wording is only reachable when the
			// precheck has not already rejected the candidate, which is what
			// production does and what the lane tests drive directly.
			lanes, _ := tamperedAccountBlockReplay(t, 3, test.tamper)
			err = lanes.verifyAccounts()
			want = fmt.Sprintf(test.lane, target)
			if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), want) {
				t.Fatalf("lane error = %v, want one containing %q", err, want)
			}
		})
	}
}

// The index is an accelerator, never an authority: with keyed cleared, the
// precheck descends the dictionary and the lanes rebuild their duplicate set,
// and the candidate has to get the same verdict and the same replayed root. This
// is what makes "a trie the walk and the descent could disagree about cannot
// change the verdict" a property rather than an argument.
func TestAccountBlockIndexFallsBackWhenNotKeyed(t *testing.T) {
	keyed := semanticMultiAccountReplay(t, 6)
	if err := keyed.precheckAccountUpdates(); err != nil {
		t.Fatalf("precheck through the index: %v", err)
	}
	if err := keyed.verifyAccounts(); err != nil {
		t.Fatalf("replay through the index: %v", err)
	}

	degraded := semanticMultiAccountReplay(t, 6)
	degraded.accountBlockIndex.keyed = false
	if err := degraded.precheckAccountUpdates(); err != nil {
		t.Fatalf("precheck through the fallback: %v", err)
	}
	if err := degraded.verifyAccounts(); err != nil {
		t.Fatalf("replay through the fallback: %v", err)
	}

	if got, want := degraded.replayedAccounts.RootCell().HashKey(),
		keyed.replayedAccounts.RootCell().HashKey(); got != want {
		t.Fatalf("fallback replayed account root %x, indexed %x", got, want)
	}

	// A nil index is the same fallback one step further out: it is what a replay
	// assembled without the structural verifier has.
	absent := semanticMultiAccountReplay(t, 6)
	absent.accountBlockIndex = nil
	if err := absent.precheckAccountUpdates(); err != nil {
		t.Fatalf("precheck without an index: %v", err)
	}
	if err := absent.verifyAccounts(); err != nil {
		t.Fatalf("replay without an index: %v", err)
	}
	if got, want := absent.replayedAccounts.RootCell().HashKey(),
		keyed.replayedAccounts.RootCell().HashKey(); got != want {
		t.Fatalf("index-less replayed account root %x, indexed %x", got, want)
	}
}

// bumpCurrencyCollection re-encodes a CurrencyCollection with one more nanoton,
// which is the smallest augmentation that is still a well-formed one: the
// rejection under test has to be "this augmentation does not follow from the
// data below it", never "this augmentation does not parse".
func bumpCurrencyCollection(t *testing.T, loader *cell.Slice) *cell.Cell {
	t.Helper()

	var total tlb.CurrencyCollection
	if err := tlb.LoadFromCell(&total, loader); err != nil {
		t.Fatalf("decode augmentation: %v", err)
	}
	total.Coins = tlb.FromNanoTON(new(big.Int).Add(total.Coins.Nano(), big.NewInt(1)))
	bumped, err := tlb.ToCell(&total)
	if err != nil {
		t.Fatalf("encode raised augmentation: %v", err)
	}

	return bumped
}

// augNodeWithExtra re-emits a HashmapAug node with its trailing extraBits
// replaced by extra, keeping the label before it and every reference.
//
// It is only correct where the extra ends the node's data: a fork is
// left(^),right(^),extra, and a leaf is extra,value only when the value is
// itself a reference. A leaf with an inline value is not such a node, so
// callers pass forks or ^-valued leaves and check the result.
func augNodeWithExtra(t *testing.T, node *cell.Cell, extraBits uint, extra *cell.Cell) *cell.Cell {
	t.Helper()

	if node.BitsSize() < extraBits {
		t.Fatalf("node holds %d bits, augmentation is %d", node.BitsSize(), extraBits)
	}
	labelBits := node.BitsSize() - extraBits
	loader := node.MustBeginParse()
	label, err := loader.LoadSlice(labelBits)
	if err != nil {
		t.Fatalf("read node label: %v", err)
	}
	builder := cell.BeginCell().MustStoreSlice(label, labelBits).MustStoreBuilder(extra.ToBuilder())
	for i := range int(node.RefsNum()) {
		ref, refErr := node.PeekRef(i)
		if refErr != nil {
			t.Fatalf("read node ref %d: %v", i, refErr)
		}
		builder.MustStoreRef(ref)
	}

	return builder.EndCell()
}

// transactionsNodeWithBumpedExtra re-emits an account's transaction dictionary
// node with an extra that no longer follows from the transactions under it.
//
// HashmapAug 64 ^Transaction puts its value in a reference, so the node's data
// is label ++ extra for both shapes — ahmn_leaf is extra,value(^) and ahmn_fork
// is left(^),right(^),extra — and everything before the extra can be copied
// verbatim. The result is checked by re-parsing it, so a layout that ever stops
// ending in the extra fails here rather than silently producing a valid node.
func transactionsNodeWithBumpedExtra(t *testing.T, dict *tlb.AccountTransactionsAugDict) *cell.Cell {
	t.Helper()

	stored, err := dict.LoadRootExtra()
	if err != nil {
		t.Fatalf("read transaction augmentation: %v", err)
	}
	extraBits := stored.BitsLeft()
	bumped := bumpCurrencyCollection(t, stored)

	lying := augNodeWithExtra(t, dict.RootCell(), extraBits, bumped)

	var reloaded tlb.AccountTransactionsAugDict
	if err = reloaded.LoadFromCell(lying.MustBeginParse()); err != nil {
		t.Fatalf("re-parse the tampered transaction node: %v", err)
	}
	replaced, err := reloaded.LoadRootExtra()
	if err != nil {
		t.Fatalf("read the tampered transaction augmentation: %v", err)
	}
	if got, want := replaced.MustToCell().HashKey(), bumped.HashKey(); got != want {
		t.Fatalf("tampered node carries augmentation %x, stored %x", got, want)
	}

	return lying
}

// accountBlockCellWithTransactions re-emits an AccountBlock around a raw
// transaction-dictionary node. acc_trans#5 inlines that dictionary, so the
// node's data follows the address and its references precede the state update.
func accountBlockCellWithTransactions(
	t *testing.T,
	address [32]byte,
	transactions *cell.Cell,
	stateUpdate *cell.Cell,
) *cell.Cell {
	t.Helper()

	builder := cell.BeginCell().MustStoreUInt(0x5, 4).MustStoreSlice(address[:], 256)
	loader := transactions.MustBeginParse()
	bits := loader.BitsLeft()
	data, err := loader.LoadSlice(bits)
	if err != nil {
		t.Fatalf("read transaction node bits: %v", err)
	}
	builder.MustStoreSlice(data, bits)
	for loader.RefsNum() > 0 {
		ref, refErr := loader.LoadRefCell()
		if refErr != nil {
			t.Fatalf("read transaction node ref: %v", refErr)
		}
		builder.MustStoreRef(ref)
	}

	return builder.MustStoreRef(stateUpdate).EndCell()
}

// The two augmentations of ShardAccountBlocks are the only part of a candidate
// the semantic replay never recomputes: it rebuilds every account state and
// every transaction, but the totals carried by the account-block trie and by
// each account's transaction trie are only ever checked by the structural pass.
// Deleting either check therefore removes a check outright, and nothing else in
// the package notices — which is why each is pinned here, at the site that makes
// it, with the wording that site uses.
//
// The two corruptions are deliberately disjoint. The transaction case leaves the
// outer trie self-consistent, because the outer augmentation of a leaf is by
// definition the inner trie's root extra: raising that extra and re-inserting
// the account block makes the outer totals agree with the lie, so only
// recomputing the inner trie from the transactions under it can catch it.
func TestVerifyAccountBlocksRejectsForgedAugmentations(t *testing.T) {
	replay := semanticMultiAccountReplay(t, 3)
	valid, err := replay.accountBlocks.ToCell()
	if err != nil {
		t.Fatalf("serialize account blocks: %v", err)
	}
	if _, _, err = verifyAccountBlocks(valid); err != nil {
		t.Fatalf("verify an untouched account block dictionary: %v", err)
	}

	t.Run("transaction augmentation", func(t *testing.T) {
		target := replay.accountBlockIndex.entries[0].key
		block := replay.accountBlockIndex.entries[0].block

		forged := accountBlockCellWithTransactions(
			t,
			target,
			transactionsNodeWithBumpedExtra(t, block.Transactions),
			block.StateUpdate,
		)
		// Set recomputes the outer augmentation from the value it is given, so
		// the account-block trie ends up agreeing with the forged inner total.
		tampered := &tlb.ShardAccountBlocksAugDict{AugmentedDictionary: replay.accountBlocks.Copy()}
		if err := tampered.Set(accountBlockKeyCell(target), forged); err != nil {
			t.Fatalf("replace account block %x: %v", target, err)
		}
		if !tampered.ValidateAll() {
			t.Fatal("the account-block trie was expected to stay self-consistent with the forged total")
		}
		root, err := tampered.ToCell()
		if err != nil {
			t.Fatalf("serialize the tampered account blocks: %v", err)
		}

		_, _, err = verifyAccountBlocks(root)
		if err == nil || !strings.Contains(err.Error(), "account transaction dictionary augmentation is invalid") {
			t.Fatalf("verify error = %v, want the transaction augmentation rejection", err)
		}
	})

	// Raising only the HashmapAugE wrapper total is caught by the decoder, which
	// compares it against the root node — that rejection is the parse, not the
	// check under test. Both are raised together instead: the pair still agrees,
	// so the candidate decodes, and the lie survives until something recomputes
	// the root fork from the two subtries under it. ValidateAll is the only thing
	// that does.
	t.Run("account block augmentation", func(t *testing.T) {
		loader := valid.MustBeginParse()
		if !loader.MustLoadBoolBit() {
			t.Fatal("the fixture account block dictionary is empty")
		}
		extraBits := loader.BitsLeft()
		bumped := bumpCurrencyCollection(t, loader)
		node, err := valid.PeekRef(0)
		if err != nil {
			t.Fatalf("read the account block trie root: %v", err)
		}
		if node.RefsNum() != 2 {
			t.Fatalf("the fixture account block trie root has %d refs, want a fork", node.RefsNum())
		}
		forged := cell.BeginCell().
			MustStoreBoolBit(true).
			MustStoreBuilder(bumped.ToBuilder()).
			MustStoreRef(augNodeWithExtra(t, node, extraBits, bumped)).
			EndCell()

		_, _, err = verifyAccountBlocks(forged)
		if err == nil || !strings.Contains(err.Error(), "account block dictionary augmentation is invalid") {
			t.Fatalf("verify error = %v, want the account block augmentation rejection", err)
		}
	})
}

// accountBlockCellWithoutStateUpdate re-emits an AccountBlock with its
// state-update reference dropped. tlb.LoadFromCell fails on it, while the
// account-block augmentation still computes — AugShardAccountBlocks.LeafExtra
// reads no further than the transaction dictionary — so the entry can be
// inserted into a self-consistent dictionary and reach the replay.
func accountBlockCellWithoutStateUpdate(t *testing.T, key [32]byte, block *tlb.AccountBlock) *cell.Cell {
	t.Helper()

	transactions, err := block.Transactions.InlineCell()
	if err != nil {
		t.Fatalf("serialize account transactions: %v", err)
	}
	builder := cell.BeginCell().MustStoreUInt(0x5, 4).MustStoreSlice(key[:], 256)
	loader := transactions.MustBeginParse()
	bits := loader.BitsLeft()
	data, err := loader.LoadSlice(bits)
	if err != nil {
		t.Fatalf("read transaction node bits: %v", err)
	}
	builder.MustStoreSlice(data, bits)
	for loader.RefsNum() > 0 {
		ref, refErr := loader.LoadRefCell()
		if refErr != nil {
			t.Fatalf("read transaction node ref: %v", refErr)
		}
		builder.MustStoreRef(ref)
	}

	return builder.EndCell()
}

// A replay assembled without a structural verifier rebuilds the index itself.
// That rebuild must reach the same verdict the walk it replaced reached, or the
// same candidate is rejected in different words, and at a different rank, purely
// because of how the replay around it was put together. An undecodable account
// block is the case that tells the two apart: the structural pass raises it, and
// the pre-index lane walk recorded it against its account key like any replay
// failure.
func TestIndexlessReplayRanksUndecodableAccountBlockAsALane(t *testing.T) {
	replay := semanticMultiAccountReplay(t, 3)
	target := replay.accountBlockIndex.entries[1].key
	block := replay.accountBlockIndex.entries[1].block

	tampered := &tlb.ShardAccountBlocksAugDict{AugmentedDictionary: replay.accountBlocks.Copy()}
	forged := accountBlockCellWithoutStateUpdate(t, target, &block)
	if err := tampered.Set(accountBlockKeyCell(target), forged); err != nil {
		t.Fatalf("replace account block %x: %v", target, err)
	}
	var undecodable tlb.AccountBlock
	if err := tlb.LoadFromCell(&undecodable, forged.MustBeginParse()); err == nil {
		t.Fatal("the tampered account block still decodes, so this proves nothing")
	}

	replay.accountBlocks = tampered
	replay.candidate.accountBlocks = tampered
	replay.accountBlockIndex = nil
	replay.candidate.accountBlockIndex = nil

	lanes, err := replay.decodeAccountLanes()
	if err != nil {
		t.Fatalf("index-less projection raised instead of ranking: %v", err)
	}
	if len(lanes) != 2 {
		t.Fatalf("projection produced %d lanes, want the walk to stop at the second account", len(lanes))
	}
	if lanes[0].err != nil {
		t.Fatalf("the account before the tampered one failed: %v", lanes[0].err)
	}
	want := fmt.Sprintf("decode semantic account block %x", target)
	if lanes[1].err == nil || !errors.Is(lanes[1].err, ErrInvalidInput) ||
		!strings.Contains(lanes[1].err.Error(), want) {
		t.Fatalf("lane error = %v, want one containing %q", lanes[1].err, want)
	}

	if err = replay.verifyAccounts(); err == nil || !errors.Is(err, ErrInvalidInput) ||
		!strings.Contains(err.Error(), want) {
		t.Fatalf("verifyAccounts error = %v, want one containing %q", err, want)
	}
}
