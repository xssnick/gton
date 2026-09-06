package collator

import (
	"fmt"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// applyDispatchChangesPerMessage preserves the former per-message outer-tree
// reconstruction for rejection/root equivalence tests and focused benchmarks.
func (v *semanticQueueValidation) applyDispatchChangesPerMessage() error {
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
		change, err := v.dispatchChangePerMessage(changes, accountID)
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
		change, err := v.dispatchChangePerMessage(changes, accountID)
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

func (v *semanticQueueValidation) dispatchChangePerMessage(
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
