package collator

import (
	"fmt"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// deferQueueDelete queues one out-queue removal for the next flush. The
// pending set is what keeps deferred semantics identical to immediate ones: a
// second dequeue of the same key must fail as absent, and an enqueue landing
// on a pending key must see the delete applied first.
func (c *collation) deferQueueDelete(key msgpool.QueueKey, keyCell *cell.Cell) {
	if c.queuePendingSet == nil {
		c.queuePendingSet = make(map[msgpool.QueueKey]struct{}, 64)
	}
	c.queuePendingSet[key] = struct{}{}
	c.queuePendingDelete = append(c.queuePendingDelete, keyCell)
}

func (c *collation) queueDeletePending(key msgpool.QueueKey) bool {
	_, pending := c.queuePendingSet[key]
	return pending
}

// flushQueueDeletes applies the pending removals in one descent. It runs
// before every read that must see them: the root samples the limiter takes on
// the queue-op cadence, the closing proof of the cleanup phase, and finish.
func (c *collation) flushQueueDeletes() error {
	if len(c.queuePendingDelete) == 0 {
		return nil
	}
	if err := c.outQueue.DeleteMany(c.queuePendingDelete, collationParallelism); err != nil {
		return fmt.Errorf("%w: outbound queue deletes did not apply: %v", ErrInvalidInput, err)
	}
	c.queuePendingDelete = c.queuePendingDelete[:0]
	clear(c.queuePendingSet)
	return nil
}

// traceOutQueueValidationClosure retains every predecessor cell that the
// reference validator reads while checking the old/new OutMsgQueue diff. Queue
// mutations already authenticate their direct trie paths; the structural diff
// additionally opens Patricia alignment boundaries and new fork augmentations.
// The callback opens both changed EnqueuedMsg payloads through Message body
// roots, exactly as the generated C++ validators do — see traceQueueEntryClosure
// for how, and for why it descends the entry instead of decoding it.
func (c *collation) traceOutQueueValidationClosure() error {
	// Scanned by subtree on workers: with the queue thousands of entries deep
	// this was the longest of the closure's five tasks once the account
	// replay was split, 37 ms on the testnet validator, and it has the same
	// shape — sibling loads and augmentation checks along changed paths.
	return c.oldOutQueue.ScanDiffParallel(c.outQueue.AugmentedDictionary, true, func(
		keyCell *cell.Cell,
		oldValueExtra, newValueExtra *cell.Slice,
	) error {
		var value cell.Slice
		if oldValueExtra != nil {
			oldValueExtra.CopyInto(&value)
			if err := (tlb.AugOutMsgQueue{}).SkipExtra(&value); err != nil {
				return fmt.Errorf("%w: decode predecessor outbound queue augmentation %s: %v",
					ErrInvalidInput, queueDiffKeyLabel(keyCell), err)
			}
			if err := traceQueueEntryClosure(&value); err != nil {
				return fmt.Errorf("trace predecessor outbound queue entry %s: %w", queueDiffKeyLabel(keyCell), err)
			}
		}
		if newValueExtra != nil {
			newValueExtra.CopyInto(&value)
			if err := (tlb.AugOutMsgQueue{}).SkipExtra(&value); err != nil {
				return fmt.Errorf("%w: decode candidate outbound queue augmentation %s: %v",
					ErrInvalidInput, queueDiffKeyLabel(keyCell), err)
			}
			if err := traceQueueEntryClosure(&value); err != nil {
				return fmt.Errorf("trace candidate outbound queue entry %s: %w", queueDiffKeyLabel(keyCell), err)
			}
		}

		return nil
	}, collationParallelism)
}

// queueDiffKeyLabel renders a diff key for an error message. Formatting is the
// only thing the key is wanted for on this path, so it is decoded on the
// failure path instead of once per entry on the hot one.
func queueDiffKeyLabel(keyCell *cell.Cell) string {
	if keyCell == nil {
		return "<absent>"
	}
	var key msgpool.QueueKey
	loader, err := keyCell.BeginParse()
	if err != nil {
		return "<unreadable>"
	}
	if err = loader.LoadSliceInto(key[:], 352); err != nil {
		return "<unreadable>"
	}
	return fmt.Sprintf("%x", key)
}

// traceQueueEntryClosure opens the predecessor cells the reference validator
// reads out of one out-queue entry, by descending the entry's structure rather
// than materializing a TL-B value the closure would immediately throw away.
// Every cell parseQueueEntry opens is opened here, in the same order.
//
// Order is reproduced but is not, on this consumer, load-bearing: cell.ReadSet
// is keyed by hash and ReadSet.Proof prunes the source tree on Contains, so the
// proof this feeds is a function of the recorded set alone. What the descent
// keeps is the parse's order anyway — a by-reference StateInit is opened while
// walking the message, before the extra-currency dictionary, because that is
// where the TL-B loader opens it, and the dictionary, the StateInit roots and an
// indirect body follow in that order, which is where parseQueueEntry reaches
// them. It costs three locals, and it lets the gate assert the stronger of the
// two properties: same cells, same order, same proof bytes. Should this ever be
// simplified, the set is the property to keep asserting.
//
// Part of the validation parseQueueEntry performs is deliberately not repeated:
// its key re-derivation and its regular-intermediate-address check, both of
// which are redundant on the trace path — the predecessor was validated when it
// was accepted and the candidate side is our own build — and neither of which
// the reference implementation's proof preparation performs either. Both run
// after the last read on that path, so dropping them cannot change what the
// trace records.
//
// Everything structural is kept, and kept for one reason: a shape the parse
// refuses must not become a shape this accepts, because what the descent walks
// into is what lands in the proof a remote validator checks. That means the
// entry's 64-bit/one-reference shape; the envelope's single reference and its
// tag, either of which refuses a pruned envelope boundary — the reference count
// reaches it first, since a boundary carries none; the message's exactness once
// a by-reference body is loaded; and the extra-currency dictionary, which is
// validated by the very function the parse validated it with. See
// traceQueuedMessageClosure for why that last one is not a descent.
func traceQueueEntryClosure(value *cell.Slice) error {
	if value.BitsLeft() != 64 || value.RefsNum() != 1 {
		return fmt.Errorf("%w: queue entry holds %d bits and %d refs, want 64 and 1",
			ErrInvalidInput, value.BitsLeft(), value.RefsNum())
	}

	var env cell.Slice
	if err := value.LoadRefInto(&env); err != nil { // opens the envelope
		return fmt.Errorf("%w: open queued envelope: %v", ErrInvalidInput, err)
	}
	// Both envelope versions hold exactly one reference, the message: neither
	// emitted_lt nor the metadata carries one, so a second is trailing data and
	// the parse refuses it. Refusing it here too is what keeps the descent from
	// accepting an envelope with a subtree hanging off it that nothing on this
	// path — or on the validator's — will ever look at.
	if env.RefsNum() != 1 {
		return fmt.Errorf("%w: queued envelope holds %d refs, want 1", ErrInvalidInput, env.RefsNum())
	}
	tag, err := env.LoadUInt(4)
	if err != nil {
		return fmt.Errorf("%w: load queued envelope tag: %v", ErrInvalidInput, err)
	}
	if tag != 4 && tag != 5 {
		return fmt.Errorf("%w: unsupported queued envelope tag %d", ErrInvalidInput, tag)
	}

	// The message is reference 0 of both envelope versions; neither emitted_lt
	// nor the metadata that may follow the tag carries a reference of its own,
	// so the descent never has to read past the tag.
	var msg cell.Slice
	if err = env.LoadRefInto(&msg); err != nil { // opens the message
		return fmt.Errorf("%w: open queued message: %v", ErrInvalidInput, err)
	}
	return traceQueuedMessageClosure(&msg)
}

// traceQueuedMessageClosure walks an int_msg_info by bits and opens exactly its
// references that the reference validator opens: every extra-currency node, a
// by-reference StateInit and its code, data and library roots, and an indirect
// body root. Descendants of those roots stay opaque, matching Anything.
//
// Every reference an int_msg_info cell holds is one of those, which is why the
// bit walk is enough: it exists only to label which reference is which, since
// the dictionary must be walked whole while the other roots are opened alone.
//
// Walking it whole is the one thing the bit walk hands back to the dictionary
// code rather than doing itself. Inside a dictionary the labels run out: a fork
// and a leaf are both just a cell with references, so a structural descent
// follows a leaf value's reference and a fork's third one, and records cells
// that belong in neither the parse's reading nor the proof.
func traceQueuedMessageClosure(msg *cell.Slice) error {
	kind, err := msg.LoadUInt(1)
	if err != nil {
		return fmt.Errorf("%w: load queued message tag: %v", ErrInvalidInput, err)
	}
	if kind != 0 {
		return fmt.Errorf("%w: queued message is not an internal message", ErrInvalidInput)
	}
	if err = msg.SkipBits(3); err != nil { // ihr_disabled, bounce, bounced
		return fmt.Errorf("%w: skip queued message flags: %v", ErrInvalidInput, err)
	}
	if err = skipMessageAddress(msg); err != nil {
		return fmt.Errorf("%w: skip queued message source: %v", ErrInvalidInput, err)
	}
	if err = skipMessageAddress(msg); err != nil {
		return fmt.Errorf("%w: skip queued message destination: %v", ErrInvalidInput, err)
	}
	if err = skipMessageGrams(msg); err != nil { // value.grams
		return fmt.Errorf("%w: skip queued message value: %v", ErrInvalidInput, err)
	}

	// The extra-currency root is taken but not opened yet: the parse it
	// replaces walks the dictionary after the whole message, so opening it here
	// would record it ahead of a by-reference StateInit.
	var extraRoot *cell.Cell
	hasExtra, err := msg.LoadBoolBit()
	if err != nil {
		return fmt.Errorf("%w: load queued message extra currencies flag: %v", ErrInvalidInput, err)
	}
	if hasExtra {
		if extraRoot, err = msg.LoadRefCell(); err != nil {
			return fmt.Errorf("%w: take queued message extra currencies: %v", ErrInvalidInput, err)
		}
	}

	if err = skipMessageGrams(msg); err != nil { // ihr_fee
		return fmt.Errorf("%w: skip queued message ihr fee: %v", ErrInvalidInput, err)
	}
	if err = skipMessageGrams(msg); err != nil { // fwd_fee
		return fmt.Errorf("%w: skip queued message forward fee: %v", ErrInvalidInput, err)
	}
	if err = msg.SkipBits(64 + 32); err != nil { // created_lt, created_at
		return fmt.Errorf("%w: skip queued message creation: %v", ErrInvalidInput, err)
	}

	// initRoots is left sitting on the code reference of whichever cell carries
	// the StateInit, so the three roots can be opened after the dictionary.
	var initRoots cell.Slice
	hasInit, err := msg.LoadBoolBit()
	if err != nil {
		return fmt.Errorf("%w: load queued message init flag: %v", ErrInvalidInput, err)
	}
	if hasInit {
		byRef, err := msg.LoadBoolBit()
		if err != nil {
			return fmt.Errorf("%w: load queued message init form: %v", ErrInvalidInput, err)
		}
		if byRef {
			if err = msg.LoadRefInto(&initRoots); err != nil { // opens the StateInit cell
				return fmt.Errorf("%w: open queued message StateInit: %v", ErrInvalidInput, err)
			}
			if err = skipStateInitHead(&initRoots); err != nil {
				return fmt.Errorf("%w: skip queued message StateInit head: %v", ErrInvalidInput, err)
			}
		} else {
			if err = skipStateInitHead(msg); err != nil {
				return fmt.Errorf("%w: skip queued message StateInit head: %v", ErrInvalidInput, err)
			}
			msg.CopyInto(&initRoots)
			// Step over code, data and the library root without opening them,
			// so the body's either bit is next in the message.
			for i := 0; i < 3; i++ {
				present, err := msg.LoadBoolBit()
				if err != nil {
					return fmt.Errorf("%w: skip queued message StateInit roots: %v", ErrInvalidInput, err)
				}
				if !present {
					continue
				}
				if err = msg.SkipBitsAndRefs(0, 1); err != nil {
					return fmt.Errorf("%w: skip queued message StateInit roots: %v", ErrInvalidInput, err)
				}
			}
		}
	}

	bodyByRef, err := msg.LoadBoolBit()
	if err != nil {
		return fmt.Errorf("%w: load queued message body form: %v", ErrInvalidInput, err)
	}

	// From here the order is the parse's: the extra-currency dictionary, then
	// the StateInit roots, then the body root.
	//
	// The dictionary is walked by the dictionary code and validated by the same
	// function the parse validated it with, and that is deliberate. It is the
	// only dictionary this path opens below its root, and a structural descent
	// cannot walk one safely: it has no way to tell a fork's two references from
	// a leaf value's, so a value carrying a reference gets opened and recorded —
	// a cell in the proof that the parse never put there — and a fork carrying a
	// third reference gets followed into a subtree the dictionary itself ignores.
	// Neither is hypothetical from a hostile sender: an extra-currency dictionary
	// arrives inside somebody else's message, and the second shape is accepted by
	// the parse, so a stray pruned branch there would fail this collation over an
	// entry every other node takes.
	if extraRoot != nil {
		if err = validateQueuedExtraCurrencies(extraRoot.AsDict(32)); err != nil {
			return fmt.Errorf("%w: walk queued message extra currencies: %v", ErrInvalidInput, err)
		}
	}
	if hasInit {
		var root cell.Slice
		for i := 0; i < 3; i++ { // code, data, library root
			if _, err = initRoots.LoadMaybeRefInto(&root); err != nil {
				return fmt.Errorf("%w: open queued message StateInit root %d: %v", ErrInvalidInput, i, err)
			}
		}
	}
	if bodyByRef {
		var body cell.Slice
		if err = msg.LoadRefInto(&body); err != nil {
			return fmt.Errorf("%w: open queued message body: %v", ErrInvalidInput, err)
		}
		// The parse requires the message to be exhausted once its body is
		// loaded. An inline body is Any and swallows everything left, so there
		// is nothing to check on that side; a by-reference body leaves the
		// message empty, or the entry carries data no reader accounts for.
		if msg.BitsLeft() != 0 || msg.RefsNum() != 0 {
			return fmt.Errorf("%w: queued message holds %d bits and %d refs after its body",
				ErrInvalidInput, msg.BitsLeft(), msg.RefsNum())
		}
	}
	return nil
}

// skipStateInitHead consumes split_depth and the tick/tock flag, leaving s on
// the code reference.
func skipStateInitHead(s *cell.Slice) error {
	hasDepth, err := s.LoadBoolBit()
	if err != nil {
		return err
	}
	if hasDepth {
		if err = s.SkipBits(5); err != nil {
			return err
		}
	}
	hasTickTock, err := s.LoadBoolBit()
	if err != nil {
		return err
	}
	if hasTickTock {
		return s.SkipBits(2)
	}
	return nil
}

// skipMessageAddress advances past one MsgAddress without materializing it.
func skipMessageAddress(s *cell.Slice) error {
	tag, err := s.LoadUInt(2)
	if err != nil {
		return err
	}
	switch tag {
	case 0: // addr_none$00
		return nil
	case 1: // addr_extern$01
		length, err := s.LoadUInt(9)
		if err != nil {
			return err
		}
		return s.SkipBits(uint(length))
	case 2: // addr_std$10
		if err = skipMessageAnycast(s); err != nil {
			return err
		}
		return s.SkipBits(8 + 256)
	default: // addr_var$11
		if err = skipMessageAnycast(s); err != nil {
			return err
		}
		length, err := s.LoadUInt(9)
		if err != nil {
			return err
		}
		if err = s.SkipBits(32); err != nil { // workchain_id
			return err
		}
		return s.SkipBits(uint(length))
	}
}

func skipMessageAnycast(s *cell.Slice) error {
	has, err := s.LoadBoolBit()
	if err != nil || !has {
		return err
	}
	depth, err := s.LoadUInt(5)
	if err != nil {
		return err
	}
	return s.SkipBits(uint(depth))
}

// skipMessageGrams advances past one VarUInteger 16.
func skipMessageGrams(s *cell.Slice) error {
	length, err := s.LoadUInt(4)
	if err != nil {
		return err
	}
	return s.SkipBits(uint(length) * 8)
}
