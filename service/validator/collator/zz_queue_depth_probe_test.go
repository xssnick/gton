package collator

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// TestZZQueueDepthProbe reports how each collation stage scales with the depth
// of the predecessor's outbound queue. It is a probe, not a gate: the numbers
// answer "what would a lifted external brake cost per block", by building the
// drain-shaped block — a backlog of own-queue internals imported up to the
// block limits — over queues an order of magnitude apart.
func TestZZQueueDepthProbe(t *testing.T) {
	if testing.Short() {
		t.Skip("probe")
	}
	stageNames := map[CollationStage]string{
		CollationStageAcquireInputs:           "acquire_inputs",
		CollationStagePrepareState:            "prepare_state",
		CollationStageCleanupOutQueue:         "cleanup_out_queue",
		CollationStageExecuteInternalMessages: "execute_internals",
		CollationStageExecuteExternalMessages: "execute_externals",
		CollationStageFinalizeAccounts:        "finalize_accounts",
		CollationStageBuildStateUpdate:        "build_state_update",
		CollationStageSerializeCandidate:      "serialize_candidate",
		CollationStageFinalizeCandidate:       "finalize_candidate",
		CollationStageProcessedInfo:           "processed_info",
		CollationStageClaimedLocalCleanup:     "claimed_local_cleanup",
		CollationStageFlushBatches:            "flush_batches",
		CollationStageValidationClosure:       "validation_closure",
		CollationStageSerializeBlock:          "serialize_block",
	}
	order := []CollationStage{
		CollationStageAcquireInputs, CollationStagePrepareState, CollationStageCleanupOutQueue,
		CollationStageExecuteInternalMessages, CollationStageExecuteExternalMessages,
		CollationStageFinalizeAccounts, CollationStageBuildStateUpdate,
		CollationStageSerializeCandidate, CollationStageFinalizeCandidate,
		CollationStageProcessedInfo, CollationStageClaimedLocalCleanup,
		CollationStageFlushBatches, CollationStageValidationClosure, CollationStageSerializeBlock,
	}

	for _, depth := range []int{2048, 8192, 32768} {
		t.Run("", func(t *testing.T) {
			req := queueDepthRequest(t, depth)
			var assembly candidateAssemblyDurations
			req.assembly = &assembly

			started := time.Now()
			candidate, err := testBuilder().BuildShard(context.Background(), req)
			if err != nil {
				t.Fatalf("depth %d: %v", depth, err)
			}
			total := time.Since(started)

			t.Logf("DEPTH %d: total=%.1fms tx=%d imported=%d cleaned=%d ext_inc=%d ext_skip=%d block=%dB queue_after=%d",
				depth, float64(total.Microseconds())/1000,
				candidate.Stats.Transactions, candidate.Stats.InternalsImported,
				candidate.Stats.QueueCleaned,
				candidate.Stats.ExternalIncluded, candidate.Stats.ExternalSkippedLimit,
				len(candidate.BlockBOC), candidate.Stats.OutQueueSize)
			for _, stage := range order {
				if d := assembly.stages[stage]; d > 500*time.Microsecond {
					t.Logf("  %-22s %8.2fms", stageNames[stage], float64(d.Microseconds())/1000)
				}
			}
		})
	}
}

// TestZZQueueDepthOpsProbe measures the two real costs of a deep outbound
// queue: the wall time of a drain-shaped block's worth of queue mutations, and
// the merkle-update bytes those mutations put into the state update. Untouched
// depth is free (the probe above shows a 32k queue collates in 0.2ms); these
// are the costs that would grow if the external brake were lifted and the
// queue settled an order of magnitude deeper.
func TestZZQueueDepthOpsProbe(t *testing.T) {
	if testing.Short() {
		t.Skip("probe")
	}
	const enqueues, dequeues = 400, 270

	makeKey := func(seed uint64) *cell.Cell {
		h := sha256.Sum256(binary.BigEndian.AppendUint64(nil, seed))
		return cell.BeginCell().
			MustStoreUInt(0, 32).                              // workchain
			MustStoreUInt(binary.BigEndian.Uint64(h[:8]), 64). // next-hop prefix
			MustStoreSlice(h[:], 256).                         // msg hash
			EndCell()
	}
	makeValue := func(lt uint64) *cell.Cell {
		// A real enqueued-message value: the augmentation parses the envelope
		// to compute the leaf extra, so the payload must be a valid MsgEnvelope
		// over a valid internal message. Shape borrowed from queuedInternal.
		message, err := tlb.ToCell(&tlb.InternalMessage{
			IHRDisabled: true,
			SrcAddr:     predecessorAddress(byte(lt % 200)),
			DstAddr:     predecessorAddress(byte((lt + 7) % 200)),
			Amount:      tlb.FromNanoTONU(1_000_000_000),
			FwdFee:      tlb.FromNanoTONU(100_000),
			CreatedLT:   lt,
			CreatedAt:   1_700_000_000,
			Body:        cell.BeginCell().MustStoreUInt(lt, 64).EndCell(),
		})
		if err != nil {
			t.Fatal(err)
		}
		envelope, err := (tlb.MsgEnvelope{
			CurAddr:         tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: 0},
			NextAddr:        tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: 96},
			FwdFeeRemaining: tlb.FromNanoTONU(100_000),
			Msg:             message,
		}).ToCell()
		if err != nil {
			t.Fatal(err)
		}
		return cell.BeginCell().MustStoreUInt(lt, 64).MustStoreRef(envelope).EndCell()
	}

	for _, depth := range []int{8192, 65536, 262144} {
		queue, err := tlb.NewOutMsgQueueAugDict()
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < depth; i++ {
			if _, err = queue.SetWithMode(makeKey(uint64(i)), makeValue(uint64(1000+i)), cell.DictSetModeAdd); err != nil {
				t.Fatalf("seed %d: %v", i, err)
			}
		}

		read := cell.NewReadSet(queue.RootCell())
		traced := queue.AugmentedDictionary.Copy().SetTrace(read.Trace())

		started := time.Now()
		for i := 0; i < enqueues; i++ {
			if _, err = traced.SetWithMode(makeKey(uint64(depth+i)), makeValue(uint64(depth+1000+i)), cell.DictSetModeAdd); err != nil {
				t.Fatal(err)
			}
		}
		enqueueWall := time.Since(started)

		started = time.Now()
		for i := 0; i < dequeues; i++ {
			if _, err = traced.LoadValueAndDelete(makeKey(uint64(i * 7 % depth))); err != nil {
				t.Fatalf("dequeue %d: %v", i, err)
			}
		}
		dequeueWall := time.Since(started)

		started = time.Now()
		update, err := read.CreateMerkleUpdate(traced.RootCell())
		if err != nil {
			t.Fatal(err)
		}
		updateWall := time.Since(started)
		updateBytes := len(update.ToBOC())

		t.Logf("DEPTH %6d: enqueue(400)=%6.2fms  dequeue(270)=%6.2fms  merkle=%6.2fms  state_update=%7dB",
			depth,
			float64(enqueueWall.Microseconds())/1000,
			float64(dequeueWall.Microseconds())/1000,
			float64(updateWall.Microseconds())/1000,
			updateBytes)
	}
}
