package collator

import (
	"fmt"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func (v *semanticQueueValidation) verifyProcessedInfo() error {
	newRecords, err := tlb.LoadProcessedUptoRecords(v.candidate.ProcInfo, v.target.Shard)
	if err != nil {
		return fmt.Errorf("%w: decode candidate processed info: %v", ErrInvalidInput, err)
	}
	claim, err := v.processedUpdate(v.oldRecords, newRecords)
	if err != nil {
		return err
	}
	if claim == nil {
		if v.hasProcessed {
			return fmt.Errorf("%w: inbound messages were processed without a ProcessedInfo update", ErrInvalidInput)
		}
		for _, descriptor := range v.in {
			if descriptor.tag == semanticInFinal || descriptor.tag == semanticInTransit {
				return fmt.Errorf("%w: queued inbound message has no ProcessedInfo update", ErrInvalidInput)
			}
		}
		return nil
	}

	bound := semanticMessageBound{lt: claim.LastMsgLT, hash: claim.LastMsgHash}
	if bound.lt == 0 || bound.lt >= v.replay.candidate.block.BlockInfo.EndLt {
		return fmt.Errorf("%w: candidate processed bound is outside the block", ErrInvalidInput)
	}
	if v.hasProcessed && bound.less(v.processedMax) {
		return fmt.Errorf("%w: ProcessedInfo ends before an imported message", ErrInvalidInput)
	}
	if v.hasMinimumQueued && !bound.less(v.minimumEnqueued) {
		return fmt.Errorf("%w: ProcessedInfo crosses a newly enqueued message", ErrInvalidInput)
	}

	for i := range v.sources {
		if err = v.verifySourceProcessed(&v.sources[i], v.oldRecords, newRecords, bound); err != nil {
			return err
		}
	}
	for _, entry := range v.inOrder {
		hash, descriptor := entry.hash, entry.descriptor
		if descriptor.tag != semanticInFinal && descriptor.tag != semanticInTransit {
			continue
		}
		if _, matched := v.matchedImports[hash]; !matched {
			return fmt.Errorf("%w: queued inbound message %x is absent from the processed queue prefix", ErrInvalidInput, hash)
		}
	}

	return nil
}

func (v *semanticQueueValidation) processedUpdate(
	oldRecords []tlb.ProcessedUptoRecord,
	newRecords []tlb.ProcessedUptoRecord,
) (*tlb.ProcessedUptoRecord, error) {
	reduced := tlb.CompactifyProcessedUpto(append([]tlb.ProcessedUptoRecord(nil), newRecords...))
	reducedDict, err := tlb.ProcessedUptoDict(reduced)
	if err != nil {
		return nil, fmt.Errorf("%w: reduce candidate ProcessedInfo: %v", ErrInvalidInput, err)
	}
	if !equalDictionary(reducedDict, v.candidate.ProcInfo) {
		return nil, fmt.Errorf("%w: candidate ProcessedInfo is not reduced", ErrInvalidInput)
	}
	if equalDictionary(v.old.ProcInfo, v.candidate.ProcInfo) {
		return nil, nil
	}

	header := &v.replay.candidate.block.BlockInfo
	referenceSeqno := header.SeqNo
	if header.NotMaster {
		referenceSeqno = header.MasterRef.SeqNo
	}
	for i := range newRecords {
		record := newRecords[i]
		if record.ShardPrefix != v.target.Shard || record.MCSeqno != referenceSeqno {
			continue
		}
		expected, insertErr := tlb.InsertProcessedUpto(
			append([]tlb.ProcessedUptoRecord(nil), oldRecords...),
			v.target.Shard,
			referenceSeqno,
			record.LastMsgLT,
			record.LastMsgHash,
		)
		if insertErr != nil {
			continue
		}
		expected = tlb.CompactifyProcessedUpto(expected)
		expectedDict, dictErr := tlb.ProcessedUptoDict(expected)
		if dictErr == nil && equalDictionary(expectedDict, v.candidate.ProcInfo) {
			result := record
			return &result, nil
		}
	}

	return nil, fmt.Errorf("%w: candidate ProcessedInfo is not a single reference update", ErrInvalidInput)
}

func (v *semanticQueueValidation) verifySourceProcessed(
	source *semanticQueueSource,
	oldRecords []tlb.ProcessedUptoRecord,
	newRecords []tlb.ProcessedUptoRecord,
	bound semanticMessageBound,
) error {
	err := walkSemanticQueuePrefix(source.queue.OutQueue, v.target, bound, func(entry semanticQueueEntry) error {
		if !source.owner.ContainsPrefix(entry.envelope.current) {
			return fmt.Errorf("%w: queued message current hop is outside its source shard", ErrInvalidInput)
		}
		wasProcessed, err := v.shardEndLT.alreadyProcessed(
			oldRecords,
			v.target.Workchain,
			v.target.Shard,
			&entry.descr,
		)
		if err != nil {
			return fmt.Errorf("%w: check predecessor ProcessedInfo: %v", ErrInvalidInput, err)
		}
		isProcessed, err := v.shardEndLT.alreadyProcessed(
			newRecords,
			v.target.Workchain,
			v.target.Shard,
			&entry.descr,
		)
		if err != nil {
			return fmt.Errorf("%w: check candidate ProcessedInfo: %v", ErrInvalidInput, err)
		}
		hash := entry.envelope.message.HashKey()
		inbound := v.in[hash]
		isQueuedImport := inbound != nil &&
			(inbound.tag == semanticInFinal || inbound.tag == semanticInTransit)
		if wasProcessed {
			if isQueuedImport {
				return fmt.Errorf("%w: candidate imports already processed message %x", ErrInvalidInput, hash)
			}
			if v.target.ContainsPrefix(entry.envelope.current) {
				if err = v.verifyProcessedLocalDequeue(hash, entry); err != nil {
					return err
				}
			}
			return nil
		}
		if !isProcessed {
			return fmt.Errorf(
				"%w: ProcessedInfo skips unprocessed message %x (lt=%d enqueued_lt=%d cur=%d:%016x next=%d:%016x bound=%d:%x records=%v)",
				ErrInvalidInput,
				hash,
				entry.descr.LT,
				entry.descr.EnqueuedLT,
				entry.descr.CurWorkchain,
				entry.descr.CurPrefix,
				entry.descr.NextWorkchain,
				entry.descr.NextPrefix,
				bound.lt,
				bound.hash,
				newRecords,
			)
		}
		if !isQueuedImport || !equalCell(inbound.envelope.root, entry.enqueued.Msg) {
			return fmt.Errorf("%w: newly processed queue message %x has no exact InMsg", ErrInvalidInput, hash)
		}
		if _, duplicate := v.matchedImports[hash]; duplicate {
			return fmt.Errorf("%w: inbound message %x occurs in multiple source queues", ErrInvalidInput, hash)
		}
		v.matchedImports[hash] = struct{}{}

		return nil
	})
	if err != nil {
		return fmt.Errorf(
			"%w: scan inbound queue from (%d,%016x): %v",
			ErrInvalidInput,
			source.owner.Workchain,
			source.owner.Shard,
			err,
		)
	}

	return nil
}

func (v *semanticQueueValidation) verifyProcessedLocalDequeue(
	hash cell.Hash,
	entry semanticQueueEntry,
) error {
	descriptor := v.out[hash]
	if descriptor == nil {
		return fmt.Errorf(
			"%w: processed message %x remains in the local predecessor queue without a dequeue descriptor",
			ErrInvalidInput,
			hash,
		)
	}

	switch descriptor.tag {
	case semanticOutDequeue:
		if descriptor.envelope == nil || !equalCell(descriptor.envelope.root, entry.enqueued.Msg) {
			return fmt.Errorf("%w: dequeue descriptor %x refers to another envelope", ErrInvalidInput, hash)
		}
	case semanticOutDequeueShort:
		if cell.Hash(descriptor.envelopeHash) != entry.enqueued.Msg.HashKey() {
			return fmt.Errorf("%w: short dequeue descriptor %x refers to another envelope", ErrInvalidInput, hash)
		}
	default:
		return fmt.Errorf(
			"%w: processed local queue message %x has outbound descriptor tag %d instead of dequeue",
			ErrInvalidInput,
			hash,
			descriptor.tag,
		)
	}

	return nil
}
