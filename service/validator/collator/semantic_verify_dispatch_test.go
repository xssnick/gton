package collator

import (
	"errors"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// TestSemanticDispatchOrderExemptsMasterSpecialImports pins the exemption C++
// spells as !is_special_in_msg() inside the undrained-DispatchQueue gate
// (validate-query.cpp:4053-4056, is_special_in_msg at :3901-3904). The
// masterchain delivers its own fee-recovery and currency-mint messages as
// msg_import_imm, which a collator cannot postpone until the dispatch queues
// drain, so without the exemption a valid masterchain block that recovers or
// mints while some account still has a backlog would be rejected.
func TestSemanticDispatchOrderExemptsMasterSpecialImports(t *testing.T) {
	recoverMsg := cell.BeginCell().MustStoreUInt(0xA1, 8).EndCell()
	mintMsg := cell.BeginCell().MustStoreUInt(0xA2, 8).EndCell()
	ordinaryMsg := cell.BeginCell().MustStoreUInt(0xA3, 8).EndCell()

	special := &tlb.McBlockExtra{}
	special.Details.RecoverCreateMsg = recoverMsg
	special.Details.MintMsg = mintMsg

	tests := []struct {
		name      string
		custom    *tlb.McBlockExtra
		imported  *cell.Cell
		tag       uint8
		wantError string
	}{
		{
			name:     "recover create message",
			custom:   special,
			imported: recoverMsg,
			tag:      semanticInImmediate,
		},
		{
			name:     "mint message",
			custom:   special,
			imported: mintMsg,
			tag:      semanticInImmediate,
		},
		{
			name:      "ordinary immediate message",
			custom:    special,
			imported:  ordinaryMsg,
			tag:       semanticInImmediate,
			wantError: "is processed before every dispatch account advances",
		},
		{
			// A shard block has no McBlockExtra at all, so nothing there is
			// special: the same message shape must still be rejected.
			name:      "shard block without masterchain extra",
			custom:    nil,
			imported:  recoverMsg,
			tag:       semanticInImmediate,
			wantError: "is processed before every dispatch account advances",
		},
		{
			name:     "external message",
			custom:   special,
			imported: ordinaryMsg,
			tag:      semanticInExternal,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			validation := newDispatchGateValidation(t, test.custom, test.tag, test.imported)
			err := validation.verifyDispatchOrder(map[[32]byte]*semanticDispatchChange{})
			if test.wantError == "" {
				if err != nil {
					t.Fatalf("dispatch gate rejected an admissible import: %v", err)
				}
				return
			}
			if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("dispatch gate error = %v, want ErrInvalidInput containing %q", err, test.wantError)
			}
		})
	}
}

// TestSemanticDispatchOrderGateStaysInactiveWhenDrained keeps the exemption from
// being read as a weakening of the gate itself: with every predecessor dispatch
// account left untouched there is no backlog to drain, and the loop the
// exemption lives in must not run at all.
func TestSemanticDispatchOrderGateStaysInactiveWhenDrained(t *testing.T) {
	ordinaryMsg := cell.BeginCell().MustStoreUInt(0xA3, 8).EndCell()
	validation := newDispatchGateValidation(t, nil, semanticInImmediate, ordinaryMsg)
	validation.old.Extra.DispatchQueue = nil

	if err := validation.verifyDispatchOrder(map[[32]byte]*semanticDispatchChange{}); err != nil {
		t.Fatalf("dispatch gate fired without a pending dispatch account: %v", err)
	}
}

// newDispatchGateValidation arms the undrained-DispatchQueue gate: the
// predecessor keeps one dispatch account, no change drains it, and the
// predecessor out queue is below the deferral limit.
func newDispatchGateValidation(
	t *testing.T,
	custom *tlb.McBlockExtra,
	tag uint8,
	imported *cell.Cell,
) *semanticQueueValidation {
	t.Helper()
	pending := makeDispatchQueue(t, dispatchFixtureAccount{
		accountID: [32]byte{0x81},
		lts:       []uint64{11},
	})
	queueSize := uint64(1)
	candidate := &verifiedCandidate{}
	candidate.block.Extra = &tlb.BlockExtra{Custom: custom}

	hash := cell.Hash{0x5a}
	descriptor := &semanticInDescriptor{tag: tag, root: imported}

	return &semanticQueueValidation{
		replay: &semanticReplay{
			transition: CandidateTransition{Config: &Config{deferOutQueueSizeLimit: 256}},
			candidate:  candidate,
		},
		old: tlb.OutMsgQueueInfo{Extra: &tlb.OutMsgQueueExtra{
			DispatchQueue: pending,
			OutQueueSize:  &queueSize,
		}},
		queueSize: queueSize,
		in:        map[cell.Hash]*semanticInDescriptor{hash: descriptor},
		inOrder:   []semanticInDescriptorEntry{{hash: hash, descriptor: descriptor}},
	}
}
