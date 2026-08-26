package collator

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type semanticDispatchOrder struct {
	created uint64
	emitted uint64
}

type semanticDispatchChange struct {
	hadOld       bool
	oldMax       uint64
	removed      bool
	maxRemoved   uint64
	added        bool
	minimumAdded uint64
}

func (v *semanticQueueValidation) applyDispatchChanges() error {
	orders := make(map[[32]byte][]semanticDispatchOrder)
	changes := make(map[[32]byte]*semanticDispatchChange)
	for _, entry := range v.inOrder {
		hash, descriptor := entry.hash, entry.descriptor
		if descriptor.tag != semanticInDeferredFinal && descriptor.tag != semanticInDeferredTransit {
			continue
		}
		envelope := descriptor.envelope
		if envelope.value.EmittedLT == nil {
			return fmt.Errorf("%w: deferred inbound message %x has no emitted lt", ErrInvalidInput, hash)
		}
		emittedLT := *envelope.value.EmittedLT
		header := &v.replay.candidate.block.BlockInfo
		if emittedLT < header.StartLt || emittedLT >= header.EndLt {
			return fmt.Errorf("%w: deferred inbound message %x emitted lt is outside the block", ErrInvalidInput, hash)
		}
		accountID, err := semanticAccountIDFromAddress(envelope.internal.SrcAddr)
		if err != nil || !v.target.ContainsPrefix(envelope.source) {
			return fmt.Errorf("%w: deferred inbound message %x source is outside its queue shard", ErrInvalidInput, hash)
		}
		change, err := v.dispatchChange(changes, accountID)
		if err != nil {
			return err
		}
		accountQueue, err := loadAccountDispatchQueue(v.dispatch, accountID)
		if err != nil {
			return fmt.Errorf("%w: load deferred source %x: %v", ErrInvalidInput, accountID, err)
		}
		var value cell.Slice
		if err := accountQueue.Messages.LoadValueAndDeleteByUintKeyInto(envelope.internal.CreatedLT, &value); err != nil {
			return fmt.Errorf("%w: deferred inbound message %x is absent from DispatchQueue", ErrInvalidInput, hash)
		}
		var enqueued tlb.EnqueuedMsg
		if err = loadExactSlice(&enqueued, &value); err != nil {
			return fmt.Errorf("%w: decode deferred inbound message %x: %v", ErrInvalidInput, hash, err)
		}
		withoutEmission := envelope.value
		withoutEmission.EmittedLT = nil
		// The envelope is repacked after clearing emitted_lt, and its constructor
		// follows from the remaining optional fields rather than from the decoded
		// v2 tag. Reset V2 so ToCell selects the same constructor.
		withoutEmission.V2 = false
		expectedEnvelope, err := withoutEmission.ToCell()
		if err != nil {
			return fmt.Errorf("serialize deferred inbound message %x: %w", hash, err)
		}
		if enqueued.EnqueuedLT != envelope.internal.CreatedLT {
			return fmt.Errorf(
				"%w: deferred inbound message %x creation lt differs from DispatchQueue",
				ErrInvalidInput,
				hash,
			)
		}
		if !equalCell(enqueued.Msg, expectedEnvelope) {
			return fmt.Errorf(
				"%w: deferred inbound message %x envelope differs from DispatchQueue",
				ErrInvalidInput,
				hash,
			)
		}
		if accountQueue.Count == 0 {
			return fmt.Errorf("%w: deferred source %x count underflow", ErrInvalidInput, accountID)
		}
		accountQueue.Count--
		change.removed = true
		change.maxRemoved = max(change.maxRemoved, envelope.internal.CreatedLT)
		if err = storeAccountDispatchQueue(v.dispatch.AugmentedDictionary, accountID, accountQueue); err != nil {
			return fmt.Errorf("%w: update deferred source %x: %v", ErrInvalidInput, accountID, err)
		}
		orders[accountID] = append(orders[accountID], semanticDispatchOrder{
			created: envelope.internal.CreatedLT,
			emitted: emittedLT,
		})
	}

	for _, entry := range v.outOrder {
		hash, descriptor := entry.hash, entry.descriptor
		if descriptor.tag != semanticOutNewDeferred {
			continue
		}
		envelope := descriptor.envelope
		if envelope.value.EmittedLT != nil ||
			envelope.value.CurAddr.Type != tlb.IntermediateAddressRegular || envelope.value.CurAddr.UseDestBits != 0 ||
			envelope.value.NextAddr.Type != tlb.IntermediateAddressRegular || envelope.value.NextAddr.UseDestBits != 0 {
			return fmt.Errorf("%w: new deferred message %x has non-zero dispatch routing", ErrInvalidInput, hash)
		}
		accountID, err := semanticAccountIDFromAddress(envelope.internal.SrcAddr)
		if err != nil || !v.target.ContainsPrefix(envelope.source) {
			return fmt.Errorf("%w: new deferred message %x source is outside this shard", ErrInvalidInput, hash)
		}
		if _, special := v.replay.specials.set[accountID]; special {
			return fmt.Errorf("%w: special masterchain account %x defers an outbound message", ErrInvalidInput, accountID)
		}
		change, err := v.dispatchChange(changes, accountID)
		if err != nil {
			return err
		}
		accountQueue, err := loadAccountDispatchQueue(v.dispatch, accountID)
		if isMissingKey(err) {
			accountQueue = &tlb.AccountDispatchQueue{Messages: cell.NewDict(64)}
		} else if err != nil {
			return fmt.Errorf("%w: load deferred destination %x: %v", ErrInvalidInput, accountID, err)
		}
		enqueued, err := (tlb.EnqueuedMsg{
			EnqueuedLT: envelope.internal.CreatedLT,
			Msg:        envelope.root,
		}).ToCell()
		if err != nil {
			return err
		}
		var value cell.Builder
		enqueued.ToBuilderInto(&value)
		inserted, err := accountQueue.Messages.SetBuilderByUintKeyWithMode(
			envelope.internal.CreatedLT,
			&value,
			cell.DictSetModeAdd,
		)
		if err != nil || !inserted {
			return fmt.Errorf("%w: duplicate deferred message %x", ErrInvalidInput, hash)
		}
		if accountQueue.Count == maxOutMsgQueueSize {
			return fmt.Errorf("%w: deferred source %x count overflow", ErrInvalidInput, accountID)
		}
		accountQueue.Count++
		change.added = true
		change.minimumAdded = min(change.minimumAdded, envelope.internal.CreatedLT)
		if err = storeAccountDispatchQueue(v.dispatch.AugmentedDictionary, accountID, accountQueue); err != nil {
			return fmt.Errorf("%w: store deferred source %x: %v", ErrInvalidInput, accountID, err)
		}
	}

	for _, accountID := range sortedAccountKeys(orders) {
		order := orders[accountID]
		slicesSortDispatchOrder(order)
		for i := 1; i < len(order); i++ {
			if order[i-1].emitted >= order[i].emitted {
				return fmt.Errorf("%w: deferred source %x emitted messages out of order", ErrInvalidInput, accountID)
			}
		}
	}
	if err := v.verifyDispatchOrder(changes); err != nil {
		return err
	}
	if err := v.verifyDispatchOutputPolicy(changes); err != nil {
		return err
	}

	return nil
}

func (v *semanticQueueValidation) dispatchChange(
	changes map[[32]byte]*semanticDispatchChange,
	accountID [32]byte,
) (*semanticDispatchChange, error) {
	if change := changes[accountID]; change != nil {
		return change, nil
	}

	change := &semanticDispatchChange{minimumAdded: ^uint64(0)}
	original, err := v.oldDispatchAccount(accountID)
	if err != nil {
		return nil, err
	}
	if original != nil {
		if original.Count == 0 || original.Messages.IsEmpty() {
			return nil, fmt.Errorf("%w: predecessor dispatch account %x is empty", ErrInvalidInput, accountID)
		}
		change.hadOld = true
		change.oldMax, err = dispatchDictionaryBoundary(original.Messages, true)
		if err != nil {
			return nil, fmt.Errorf("%w: predecessor dispatch account %x maximum: %v", ErrInvalidInput, accountID, err)
		}
	}
	changes[accountID] = change

	return change, nil
}

func (v *semanticQueueValidation) oldDispatchAccount(accountID [32]byte) (*tlb.AccountDispatchQueue, error) {
	if v.old.Extra == nil || v.old.Extra.DispatchQueue == nil {
		return nil, nil
	}
	queue, err := loadAccountDispatchQueue(v.old.Extra.DispatchQueue, accountID)
	if isMissingKey(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%w: load predecessor dispatch account %x: %v", ErrInvalidInput, accountID, err)
	}

	return queue, nil
}

func (v *semanticQueueValidation) verifyDispatchOrder(changes map[[32]byte]*semanticDispatchChange) error {
	processedAccounts := 0
	for _, accountID := range sortedAccountKeys(changes) {
		change := changes[accountID]
		if change.added && change.hadOld && change.minimumAdded <= change.oldMax {
			return fmt.Errorf("%w: dispatch account %x appends before a predecessor message", ErrInvalidInput, accountID)
		}
		if !change.removed {
			continue
		}
		if change.hadOld {
			processedAccounts++
		}

		remaining, err := loadAccountDispatchQueue(v.dispatch, accountID)
		if isMissingKey(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("%w: load candidate dispatch account %x: %v", ErrInvalidInput, accountID, err)
		}
		minimum, err := dispatchDictionaryBoundary(remaining.Messages, false)
		if err != nil {
			return fmt.Errorf("%w: candidate dispatch account %x minimum: %v", ErrInvalidInput, accountID, err)
		}
		if minimum <= change.maxRemoved {
			return fmt.Errorf("%w: dispatch account %x did not remove an oldest prefix", ErrInvalidInput, accountID)
		}
	}

	if v.old.Extra == nil || v.old.Extra.OutQueueSize == nil ||
		v.queueSize > v.replay.transition.Config.deferOutQueueSizeLimit {
		return nil
	}
	if v.everyDispatchAccountAdvanced(processedAccounts) {
		return nil
	}

	for _, entry := range v.inOrder {
		hash, descriptor := entry.hash, entry.descriptor
		switch descriptor.tag {
		case semanticInExternal, semanticInDeferredFinal, semanticInDeferredTransit:
			continue
		default:
			// The masterchain delivers its own fee-recovery and currency-mint
			// messages as msg_import_imm, and they are not something a collator
			// can postpone until the dispatch queues drain. C++ therefore ands
			// !is_special_in_msg() into the same gate
			// (validate-query.cpp:4053-4056, is_special_in_msg at :3901-3904).
			if v.isMasterSpecial(descriptor.root) {
				continue
			}
			return fmt.Errorf(
				"%w: inbound message %x is processed before every dispatch account advances",
				ErrInvalidInput,
				hash,
			)
		}
	}

	return nil
}

// everyDispatchAccountAdvanced answers ValidateQuery's
// have_unprocessed_account_dispatch_queue_ (validate-query.cpp:3768-3786): when
// the outbound queue is small enough, a collator must take at least one message
// out of every AccountDispatchQueue before it may import other internals.
//
// Two properties of the reference implementation are load-bearing and were both
// missing here. It counts with an early exit — the walk stops the moment the
// running total passes the number of accounts the block advanced, so it reads
// at most processed+1 leaves instead of the whole queue. And it treats a cell
// it cannot open as "not advanced" rather than as an invalid block, with the
// comment "VmVirtError can happen if we have only a proof of ShardState": under
// full collated data the untouched accounts are exactly the ones the proof
// prunes, so a full count cannot succeed and must not be attempted.
//
// Degrading to false is the safe direction. It only ever adds the restriction
// below — no other internal message may be imported — so a block that skipped
// dispatch processing cannot be accepted because its proof was too narrow to
// prove it.
func (v *semanticQueueValidation) everyDispatchAccountAdvanced(processed int) bool {
	queue := v.old.Extra.DispatchQueue
	if queue == nil {
		return processed == 0
	}

	total := 0
	if err := queue.ForEachValueExtra(func(_, _ *cell.Slice) (bool, error) {
		total++

		return total <= processed, nil
	}); err != nil {
		return false
	}

	return total == processed
}

func (v *semanticQueueValidation) verifyDispatchOutputPolicy(
	changes map[[32]byte]*semanticDispatchChange,
) error {
	for _, accountID := range sortedAccountKeys(v.replay.accounts) {
		account := v.replay.accounts[accountID]
		change := changes[accountID]
		deferAll := false
		if change == nil {
			oldQueue, err := v.oldDispatchAccount(accountID)
			if err != nil {
				return err
			}
			deferAll = oldQueue != nil
		} else {
			deferAll = change.hadOld && (!change.removed || change.maxRemoved != change.oldMax)
		}

		for transactionIndex := range account.outputTags {
			for outputIndex, tag := range account.outputTags[transactionIndex] {
				if tag == semanticOutExternal {
					continue
				}
				if tag != semanticOutNewDeferred {
					if deferAll {
						return fmt.Errorf(
							"%w: account %x emits a non-deferred message after its dispatch queue became pending",
							ErrInvalidInput,
							accountID,
						)
					}
					continue
				}

				if v.replay.transition.Config.capabilities&capDeferMessages == 0 && !deferAll {
					return fmt.Errorf(
						"%w: account %x defers a message while the capability is disabled",
						ErrInvalidInput,
						accountID,
					)
				}
				if outputIndex == 0 && !deferAll {
					return fmt.Errorf(
						"%w: account %x defers the first output without an earlier pending message",
						ErrInvalidInput,
						accountID,
					)
				}
				deferAll = true
			}
		}
	}

	return nil
}

// sortedAccountKeys orders account-keyed rejection loops the way the
// descriptor loops are ordered by their dictionary walk. These maps are built
// from map iteration upstream, so unlike inOrder/outOrder they have to be
// sorted here.
func sortedAccountKeys[V any](accounts map[[32]byte]V) [][32]byte {
	keys := make([][32]byte, 0, len(accounts))
	for accountID := range accounts {
		keys = append(keys, accountID)
	}
	slices.SortFunc(keys, func(left, right [32]byte) int {
		return bytes.Compare(left[:], right[:])
	})

	return keys
}

func dispatchDictionaryBoundary(messages *cell.Dictionary, maximum bool) (uint64, error) {
	var key *cell.Cell
	var err error
	if maximum {
		key, _, err = messages.LoadMax()
	} else {
		key, _, err = messages.LoadMin()
	}
	if err != nil {
		return 0, err
	}
	var loader cell.Slice
	if err = key.BeginParseInto(&loader); err != nil {
		return 0, err
	}
	lt, err := loader.LoadUInt(64)
	if err != nil || loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		return 0, fmt.Errorf("dispatch key is malformed")
	}

	return lt, nil
}

func slicesSortDispatchOrder(order []semanticDispatchOrder) {
	// Dispatch batches are small; insertion sort avoids an interface callback
	// and allocation on this per-account validation path.
	for i := 1; i < len(order); i++ {
		value := order[i]
		j := i
		for j > 0 && value.created < order[j-1].created {
			order[j] = order[j-1]
			j--
		}
		order[j] = value
	}
}
