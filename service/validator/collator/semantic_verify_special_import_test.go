package collator

import (
	"context"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/validator/msgpool"
)

const (
	specialEnvelopeStartLT = uint64(100)
	specialEnvelopeGenTime = uint32(200)
)

// specialEnvelopeFixture builds the canonical fee recovery / mint system message
// the masterchain collator emits in processMasterSpecial, so every rejection
// below differs from a block gton itself produces by exactly one field.
func specialEnvelopeFixture(destination [32]byte, amount tlb.CurrencyCollection) tlb.InternalMessage {
	var zero [32]byte

	return tlb.InternalMessage{
		IHRDisabled:     true,
		Bounce:          true,
		SrcAddr:         address.NewAddress(0, 0xff, zero[:]),
		DstAddr:         address.NewAddress(0, 0xff, destination[:]),
		Amount:          amount.Coins,
		ExtraCurrencies: amount.ExtraCurrencies,
		CreatedLT:       specialEnvelopeStartLT,
		CreatedAt:       specialEnvelopeGenTime,
		Body:            cell.BeginCell().EndCell(),
	}
}

func specialEnvelopeCell(t *testing.T, envelope tlb.MsgEnvelope, message tlb.InternalMessage) *cell.Cell {
	t.Helper()

	messageRoot, err := message.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	envelope.CurAddr = tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: routingAddressBits}
	envelope.NextAddr = tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: routingAddressBits}
	envelope.Msg = messageRoot
	root, err := envelope.ToCell()
	if err != nil {
		t.Fatal(err)
	}

	return root
}

// TestMasterSpecialEnvelopeRejectsNonCanonicalSystemMessage covers the part of
// check_special_message that verifyMasterSpecialEnvelope used to skip entirely
// (validate-query.cpp:6574 and :6600-6608): the envelope may carry no metadata,
// and the wrapped message must be a non-bounced bounceable IHR-disabled message
// stamped with the logical time and the generation time of the very block that
// creates it. None of these were read before, so a masterchain collator emitting
// any of them split gton validators from the reference ones.
func TestMasterSpecialEnvelopeRejectsNonCanonicalSystemMessage(t *testing.T) {
	var destination [32]byte
	destination[0] = 0x51
	amount := tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(7)}
	target := msgpool.ShardIdent{Workchain: address.MasterchainID, Shard: msgpool.ShardAll}

	// Positive control: the shape processMasterSpecial actually emits stays
	// accepted, so every rejection below is attributable to its one field.
	canonical := specialEnvelopeFixture(destination, amount)
	if _, err := verifyMasterSpecialEnvelope(
		specialEnvelopeCell(t, tlb.MsgEnvelope{}, canonical),
		amount,
		destination[:],
		target,
		specialEnvelopeStartLT,
		specialEnvelopeGenTime,
	); err != nil {
		t.Fatalf("canonical special message rejected: %v", err)
	}

	for _, test := range []struct {
		name     string
		envelope tlb.MsgEnvelope
		mutate   func(message *tlb.InternalMessage)
	}{
		{
			name: "metadata",
			envelope: tlb.MsgEnvelope{Metadata: &tlb.MsgMetadata{
				InitiatorLT: specialEnvelopeStartLT,
				Initiator:   address.NewAddress(0, 0xff, destination[:]),
			}},
		},
		{
			name:   "ihr enabled",
			mutate: func(message *tlb.InternalMessage) { message.IHRDisabled = false },
		},
		{
			name:   "non bounceable",
			mutate: func(message *tlb.InternalMessage) { message.Bounce = false },
		},
		{
			name:   "bounced",
			mutate: func(message *tlb.InternalMessage) { message.Bounced = true },
		},
		{
			name:   "created before the block",
			mutate: func(message *tlb.InternalMessage) { message.CreatedLT = specialEnvelopeStartLT - 1 },
		},
		{
			name:   "created at another time",
			mutate: func(message *tlb.InternalMessage) { message.CreatedAt = specialEnvelopeGenTime + 1 },
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			message := specialEnvelopeFixture(destination, amount)
			if test.mutate != nil {
				test.mutate(&message)
			}
			_, err := verifyMasterSpecialEnvelope(
				specialEnvelopeCell(t, test.envelope, message),
				amount,
				destination[:],
				target,
				specialEnvelopeStartLT,
				specialEnvelopeGenTime,
			)
			if err == nil {
				t.Fatal("non-canonical special message was accepted")
			}
		})
	}
}

// localImportFixture enqueues one message into queue under its canonical
// out-queue key and returns the inbound descriptor importing it back.
func localImportFixture(
	t *testing.T,
	queue *tlb.OutMsgQueueAugDict,
	source, destination [32]byte,
	lt uint64,
) (cell.Hash, *semanticInDescriptor) {
	t.Helper()

	message := tlb.InternalMessage{
		IHRDisabled: true,
		Bounce:      true,
		SrcAddr:     address.NewAddress(0, 0, source[:]),
		DstAddr:     address.NewAddress(0, 0, destination[:]),
		Amount:      tlb.FromNanoTONU(1),
		CreatedLT:   lt,
		CreatedAt:   7,
		Body:        cell.BeginCell().EndCell(),
	}
	messageRoot, err := message.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	// cur_addr stays at the source prefix and next_addr is the final
	// destination, which is what makes the import local or not: the local branch
	// of findImportedMessage is chosen by the current hop being inside our own
	// shard.
	envelopeRoot, err := (tlb.MsgEnvelope{
		CurAddr:  tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular},
		NextAddr: tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: routingAddressBits},
		Msg:      messageRoot,
	}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := parseSemanticEnvelope(envelopeRoot)
	if err != nil {
		t.Fatal(err)
	}
	enqueued, err := (tlb.EnqueuedMsg{EnqueuedLT: lt, Msg: envelopeRoot}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	key := msgpool.MakeQueueKey(envelope.next, messageRoot.HashKey())
	keyCell := cell.BeginCell().MustStoreSlice(key[:], 352).EndCell()
	inserted, err := queue.SetWithMode(keyCell, enqueued, cell.DictSetModeAdd)
	if err != nil || !inserted {
		t.Fatalf("enqueue fixture message: inserted=%v err=%v", inserted, err)
	}

	return messageRoot.HashKey(), &semanticInDescriptor{
		tag:         semanticInFinal,
		root:        cell.BeginCell().EndCell(),
		message:     messageRoot,
		envelope:    envelope,
		transaction: cell.BeginCell().EndCell(),
	}
}

// TestVerifyInboundMessagesSkipsProcessedForLocalReimport pins the processed
// bound to genuine neighbor imports. C++ advances proc_lt_/proc_hash_ only in
// check_imported_message (validate-query.cpp:3884), which the msg_import_fin
// branch for a message whose current hop is already inside our own shard never
// reaches (validate-query.cpp:4256) -- the very rare post-merge re-dequeue. gton
// recorded that message too, so it demanded a ProcessedInfo update covering a
// logical time the reference validator never requires.
func TestVerifyInboundMessagesSkipsProcessedForLocalReimport(t *testing.T) {
	left, err := shard.Child(int64(shard.Root), true)
	if err != nil {
		t.Fatal(err)
	}
	right, err := shard.Child(int64(shard.Root), false)
	if err != nil {
		t.Fatal(err)
	}
	target := msgpool.ShardIdent{Workchain: 0, Shard: uint64(left)}
	sibling := msgpool.ShardIdent{Workchain: 0, Shard: uint64(right)}

	var localSource, neighborSource, destination [32]byte
	localSource[0] = 0x11
	neighborSource[0] = 0x91
	destination[0] = 0x22

	own, err := tlb.NewOutMsgQueueAugDict()
	if err != nil {
		t.Fatal(err)
	}
	neighborQueue, err := tlb.NewOutMsgQueueAugDict()
	if err != nil {
		t.Fatal(err)
	}
	// The local re-dequeue carries the higher logical time, so a bound recorded
	// from it is impossible to confuse with the neighbor import's own bound.
	localHash, localDescriptor := localImportFixture(t, own, localSource, destination, 900)
	neighborHash, neighborDescriptor := localImportFixture(t, neighborQueue, neighborSource, destination, 100)
	if !target.ContainsPrefix(localDescriptor.envelope.current) {
		t.Fatal("local fixture current hop is not inside the target shard")
	}
	if !sibling.ContainsPrefix(neighborDescriptor.envelope.current) {
		t.Fatal("neighbor fixture current hop is not inside the sibling shard")
	}

	validation := &semanticQueueValidation{
		replay: &semanticReplay{
			ctx:       context.Background(),
			candidate: &verifiedCandidate{block: tlb.Block{Extra: &tlb.BlockExtra{}}},
		},
		target:     target,
		old:        tlb.OutMsgQueueInfo{OutQueue: own},
		shardEndLT: &shardEndLTResolver{},
		in: map[cell.Hash]*semanticInDescriptor{
			localHash:    localDescriptor,
			neighborHash: neighborDescriptor,
		},
		out: map[cell.Hash]*semanticOutDescriptor{
			// A locally re-dequeued message is paired with msg_export_deq_imm.
			localHash: {tag: semanticOutDequeueImmediate},
		},
		inOrder: []cell.Hash{localHash, neighborHash},
		sources: []semanticQueueSource{{
			neighbor: &Neighbor{Shard: sibling},
			owner:    sibling,
			queue:    tlb.OutMsgQueueInfo{OutQueue: neighborQueue},
		}},
	}
	if err = validation.verifyInboundMessages(); err != nil {
		t.Fatalf("verify inbound messages: %v", err)
	}

	if !validation.hasProcessed {
		t.Fatal("neighbor import did not advance the processed bound")
	}
	if validation.processedMax.lt != 100 {
		t.Fatalf("processed bound lt = %d, want the neighbor import at 100", validation.processedMax.lt)
	}
	if validation.processedMax.hash != neighborDescriptor.envelope.message.HashKey() {
		t.Fatal("processed bound does not point at the neighbor import")
	}
}
