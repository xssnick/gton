package collator

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

type semanticDequeueCapabilityCase struct {
	name       string
	candidate  *Candidate
	capability bool
	wantError  string
}

// C++ check_out_msg requires msg_export_deq only without capShortDequeue and
// msg_export_deq_short only with it. Both records describe the same queue
// removal, so a valid queue delta alone cannot enforce the wire format.
func TestVerifyShardCandidateDequeueCapability(t *testing.T) {
	req := emptyCandidateRequest(t)
	if req.Masterchain.Config.capabilities&capShortDequeue == 0 {
		t.Fatal("fixture requires capShortDequeue")
	}
	start := requestStartLT(t, req)
	message := masterchainQueueMessage(t, &req,
		address.NewAddress(0, 0, bytes.Repeat([]byte{0xb1}, 32)),
		address.NewAddress(0, 0xff, bytes.Repeat([]byte{0xb3}, 32)), start-30, start-30)
	size := uint64(1)
	req.Previous.OutQueueSize = &size
	req.Internals = &msgpool.Cut{More: true}
	record := tlb.ProcessedUptoRecord{
		ShardPrefix: msgpool.ShardAll, MCSeqno: req.Masterchain.ID.SeqNo,
		LastMsgLT: start - 10, LastMsgHash: processedInfinityHash,
	}
	var mcQueue tlb.OutMsgQueueInfo
	if err := parseExact(&mcQueue, req.Masterchain.OutMsgQueueInfo); err != nil {
		t.Fatal(err)
	}
	var err error
	mcQueue.ProcInfo, err = tlb.ProcessedUptoDict([]tlb.ProcessedUptoRecord{record})
	if err != nil {
		t.Fatal(err)
	}
	req.Masterchain.OutMsgQueueInfo, err = mcQueue.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	req.Neighbors = masterchainNeighbor(req, record)
	req.Neighbors[0].OutMsgQueueInfo = req.Masterchain.OutMsgQueueInfo
	req.NeighborShardEndLT = func(uint32, int32, uint64) uint64 { return start }

	short, err := testBuilder().BuildShard(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	if short.Stats.OutQueueSize != 0 {
		t.Fatalf("message was not dequeued: %+v", short.Stats)
	}
	_, descriptors := candidateMessageDescriptors(t, short, req.Masterchain.Config.globalVersion)
	if tag := descriptorByHash(t, descriptors.AugmentedDictionary, message.Root.HashKey()).MustLoadUInt(4); tag != 0b1101 {
		t.Fatalf("built dequeue tag = %04b, want msg_export_deq_short$1101", tag)
	}

	full := cloneVerificationCandidate(short)
	rewriteVerificationShardBlock(t, full, func(block *tlb.Block) {
		out, err := loadOutMsgDescriptors(block.Extra.OutMsgDesc, req.Masterchain.Config.globalVersion)
		if err != nil {
			t.Fatal(err)
		}
		descriptor := cell.BeginCell().MustStoreUInt(0b1100, 4).
			MustStoreRef(message.EnvelopeCell).MustStoreUInt(req.Masterchain.EndLT, 63).EndCell()
		key := cell.BeginCell().MustStoreSlice(message.Root.Hash(), 256).EndCell()
		if err = out.Set(key, descriptor); err != nil {
			t.Fatal(err)
		}
		block.Extra.OutMsgDesc, err = out.ToCell()
		if err != nil {
			t.Fatal(err)
		}
	})

	tests := []semanticDequeueCapabilityCase{
		{name: "short with capability", candidate: short, capability: true},
		{name: "full without capability", candidate: full},
		{name: "full with capability", candidate: full, capability: true, wantError: "short dequeue capability is enabled"},
		{name: "short without capability", candidate: short, wantError: "short dequeue capability is disabled"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				time.Sleep(time.Unix(int64(req.Header.GenUtime), 0).Sub(time.Now()))
				verification := shardVerificationRequest(req, test.candidate)
				config := *req.Masterchain.Config
				if !test.capability {
					config.capabilities &^= capShortDequeue
				}
				verification.Masterchain.Config = &config
				verification.NeighborShardEndLT = req.NeighborShardEndLT
				verification.Semantics = NewSemanticVerifier(tvm.NewTVM())
				err := VerifyShardCandidate(t.Context(), verification)
				if test.wantError == "" {
					if err != nil {
						t.Fatalf("valid dequeue format rejected: %v", err)
					}
					return
				}
				if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("verification error = %v, want ErrInvalidInput containing %q", err, test.wantError)
				}
			})
		})
	}
}
