package collator

import (
	"bytes"
	"fmt"

	"github.com/xssnick/tonutils-go/tlb"
)

// accountBlockEntry is one AccountBlock as the structural pass decoded it.
//
// Every field is written exactly once, by buildAccountBlockIndex, and is
// read-only for the rest of the candidate's life: precheckAccountUpdates reads
// it from its own goroutine while the queue preparation runs on the caller's,
// and the replay lanes read it from GOMAXPROCS goroutines at once. Nothing here
// is per-replay state, which is what lets one verifiedCandidate be replayed more
// than once — the retry after ErrSemanticExecution — against the same entries.
// Putting mutable lane state in here would break exactly that.
type accountBlockEntry struct {
	key   [32]byte
	block tlb.AccountBlock

	// decodeErr is the AccountBlock decode failure the structural pass raises,
	// deferred instead of raised on the index-less rebuild — see
	// buildAccountBlockIndexDeferred. It is always nil on an index built for a
	// candidate, because that build stops at the failure.
	decodeErr error

	// exactErr is what loadExactSlice would have returned for this entry. The
	// structural pass decodes with tlb.LoadFromCell, which tolerates trailing
	// data; both semantic consumers reject it, and each words the rejection
	// differently, so the verdict is recorded here and reported at the two sites
	// that report it today. A LoadFromCell failure is not recorded: it still
	// aborts the structural pass at the same entry with the same message, so the
	// only verdict this field can hold is trailing data.
	exactErr error

	// update is StateUpdate parsed as an exact tlb.HashUpdate — the parse both
	// precheckAccountUpdates and replayAccount make today, on the same cell.
	// updateErr carries the failure the two consumers re-word.
	update    tlb.HashUpdate
	updateErr error

	// keyMalformed records an iterator key that was not exactly 256 bits.
	// Unreachable through a dictionary ValidateAll has accepted; kept so the
	// lane walk can still report it in its own words.
	keyMalformed bool
}

// accountBlockIndex is the result of the single structural pass over
// ShardAccountBlocks. entries is in dictionary order, which is ascending by key.
type accountBlockIndex struct {
	entries []accountBlockEntry

	// keyed is set when entries are strictly ascending, which is what makes a
	// binary search equivalent to descending the dictionary for a key. The outer
	// ValidateAll has already proven the trie is a well-formed 256-bit Patricia,
	// so it is always true in practice; a false sends the keyed consumer back
	// down the dictionary, so a trie the walk and the descent could disagree
	// about cannot change the verdict.
	keyed bool
}

// find returns the entry for key, or false when the index cannot answer for it:
// a nil or degraded index, or a key the walk did not see. Both callers treat
// false as "take the path you took before this index existed", which is what
// makes "the index cannot change any verdict" provable rather than argued.
func (i *accountBlockIndex) find(key [32]byte) (*accountBlockEntry, bool) {
	if i == nil || !i.keyed {
		return nil, false
	}
	// Hand-rolled rather than slices.BinarySearchFunc: that one takes the
	// element by value, and an accountBlockEntry is a few hundred bytes to copy
	// per comparison.
	low, high := 0, len(i.entries)
	for low < high {
		middle := int(uint(low+high) >> 1)
		if bytes.Compare(i.entries[middle].key[:], key[:]) < 0 {
			low = middle + 1
		} else {
			high = middle
		}
	}
	if low >= len(i.entries) || i.entries[low].key != key {
		return nil, false
	}

	return &i.entries[low], true
}

// buildAccountBlockIndex is the single structural walk over the account-block
// dictionary. Reaching an entry's Transactions requires a full AccountBlock
// decode, so the decode is kept instead of thrown away, together with the two
// verdicts the semantic passes would otherwise re-derive from the same cells.
//
// It returns an error for exactly what the structural walk rejects today — an
// undecodable entry body and a bad transaction augmentation — so a malformed
// candidate is still rejected by the same pass, at the same entry, with the same
// message. Everything else is recorded, not raised.
//
// HARD INVARIANT: no pointer into entries may escape while this loop is still
// appending. append reallocates, and a pointer taken before the final append
// would address a stale array. The pointer below is taken after its own append
// and dropped before the next one; the lane projection and every find() run only
// once the slice is final.
func buildAccountBlockIndex(blocks *tlb.ShardAccountBlocksAugDict) (*accountBlockIndex, error) {
	return buildAccountBlockIndexWith(blocks, false)
}

// buildAccountBlockIndexDeferred is the same walk with the two verdicts above
// recorded rather than raised, and the walk stopping at the first of them.
//
// It exists for the one caller that rebuilds an index the structural pass never
// produced: decodeAccountLanes on a replay assembled without a structural
// verifier. For that caller the strict form would be wrong, not merely stricter.
// Before the index existed, that walk decoded entries itself and turned an
// undecodable one into a lane failure ranked by account key like any other, and
// it never looked at transaction augmentations at all — those belong to the
// structural pass, which such a replay by definition did not run. Raising here
// would give a candidate a different rejection, with different words and a
// different rank, depending on how the replay around it was assembled.
func buildAccountBlockIndexDeferred(blocks *tlb.ShardAccountBlocksAugDict) (*accountBlockIndex, error) {
	return buildAccountBlockIndexWith(blocks, true)
}

func buildAccountBlockIndexWith(
	blocks *tlb.ShardAccountBlocksAugDict,
	deferVerdicts bool,
) (*accountBlockIndex, error) {
	iterator, err := blocks.IteratorExtra(false, false)
	if err != nil {
		return nil, err
	}
	index := &accountBlockIndex{entries: make([]accountBlockEntry, 0, 64), keyed: true}
	for iterator.Next() {
		item := iterator.View()
		index.entries = append(index.entries, accountBlockEntry{})
		entry := &index.entries[len(index.entries)-1]

		if keyErr := item.Key.LoadSliceInto(entry.key[:], 256); keyErr != nil ||
			item.Key.BitsLeft() != 0 || item.Key.RefsNum() != 0 {
			entry.keyMalformed = true
			index.keyed = false
		}

		// Identical to the decode this walk has always made: LoadFromCell, not
		// loadExactSlice, so a failure aborts here and trailing data does not.
		value := item.Value
		if err = tlb.LoadFromCell(&entry.block, &value); err != nil {
			if !deferVerdicts {
				return nil, err
			}
			// entry.block is now partial, so nothing below may read it. The walk
			// ends here, which is where the pre-index walk ended too.
			entry.decodeErr = err
			return index, nil
		}
		if value.BitsLeft() != 0 || value.RefsNum() != 0 {
			entry.exactErr = fmt.Errorf("trailing data: %d bits, %d refs", value.BitsLeft(), value.RefsNum())
		}
		if !deferVerdicts {
			if err = verifyAccountTransactions(&entry.block); err != nil {
				return nil, err
			}
		}
		entry.updateErr = parseExact(&entry.update, entry.block.StateUpdate)

		if count := len(index.entries); count > 1 &&
			bytes.Compare(index.entries[count-2].key[:], entry.key[:]) >= 0 {
			index.keyed = false
		}
	}
	if err = iterator.Err(); err != nil {
		return nil, err
	}

	return index, nil
}
