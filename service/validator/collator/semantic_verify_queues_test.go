package collator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

func TestSemanticVerifyDeliveredUsesFirstProcessedNeighbor(t *testing.T) {
	owner := msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll}
	records := []tlb.ProcessedUptoRecord{{
		ShardPrefix: owner.Shard,
		MCSeqno:     1,
		LastMsgLT:   100,
	}}
	validation := semanticQueueValidation{
		shardEndLT: newShardEndLTResolver(nil),
		sources: []semanticQueueSource{
			{owner: owner, neighbor: &Neighbor{Shard: owner, EndLT: 7_000, Processed: records}},
			{owner: owner, neighbor: &Neighbor{Shard: owner, EndLT: 8_000, Processed: records}},
		},
	}
	entry := semanticQueueEntry{
		envelope: semanticEnvelope{next: msgpool.AccountPrefix{Workchain: 0, Prefix: 0x11}},
		descr: tlb.ProcessedMsgDescr{
			CurWorkchain:  0,
			CurPrefix:     0x11,
			NextWorkchain: 0,
			NextPrefix:    0x11,
			LT:            50,
		},
	}

	if err := validation.verifyDelivered(entry, 7_000); err != nil {
		t.Fatalf("first processed neighbor end lt rejected: %v", err)
	}
	if err := validation.verifyDelivered(entry, 8_000); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("second processed neighbor end lt error = %v, want invalid input", err)
	}
}

func TestSemanticDispatchOutputPolicy(t *testing.T) {
	accountID := [32]byte{0x81}
	pending := makeDispatchQueue(t, dispatchFixtureAccount{
		accountID: accountID,
		lts:       []uint64{11},
	})
	tests := []struct {
		name         string
		capabilities uint64
		old          tlb.OutMsgQueueInfo
		changes      map[[32]byte]*semanticDispatchChange
		outputTags   [][]uint8
		wantError    string
	}{
		{
			name:       "unchanged pending queue requires deferral",
			old:        tlb.OutMsgQueueInfo{Extra: &tlb.OutMsgQueueExtra{DispatchQueue: pending}},
			outputTags: [][]uint8{{semanticOutNew}},
			wantError:  "non-deferred message",
		},
		{
			name:         "fully drained queue cannot defer first output",
			capabilities: capDeferMessages,
			old:          tlb.OutMsgQueueInfo{Extra: &tlb.OutMsgQueueExtra{DispatchQueue: pending}},
			changes: map[[32]byte]*semanticDispatchChange{
				accountID: {hadOld: true, oldMax: 11, removed: true, maxRemoved: 11},
			},
			outputTags: [][]uint8{{semanticOutNewDeferred}},
			wantError:  "first output",
		},
		{
			name:       "capability gates new deferral",
			outputTags: [][]uint8{{semanticOutExternal, semanticOutNewDeferred}},
			wantError:  "capability is disabled",
		},
		{
			name:         "deferral persists across later outputs",
			capabilities: capDeferMessages,
			outputTags:   [][]uint8{{semanticOutExternal, semanticOutNewDeferred}, {semanticOutNew}},
			wantError:    "non-deferred message",
		},
		{
			name:         "external first output permits enabled deferral",
			capabilities: capDeferMessages,
			outputTags:   [][]uint8{{semanticOutExternal, semanticOutNewDeferred, semanticOutNewDeferred}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			validation := semanticQueueValidation{
				replay: &semanticReplay{
					transition: CandidateTransition{Config: &Config{capabilities: test.capabilities}},
					accounts: map[[32]byte]*semanticAccountResult{
						accountID: {outputTags: test.outputTags},
					},
				},
				old: test.old,
			}
			changes := test.changes
			if changes == nil {
				changes = make(map[[32]byte]*semanticDispatchChange)
			}

			err := validation.verifyDispatchOutputPolicy(changes)
			if test.wantError == "" {
				if err != nil {
					t.Fatalf("policy rejected valid output sequence: %v", err)
				}
				return
			}
			if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("policy error = %v, want ErrInvalidInput containing %q", err, test.wantError)
			}
		})
	}
}

func TestSemanticProcessedLocalQueueRequiresExactDequeue(t *testing.T) {
	envelope := cell.BeginCell().MustStoreUInt(0x1234, 16).EndCell()
	otherEnvelope := cell.BeginCell().MustStoreUInt(0x4321, 16).EndCell()
	hash := cell.Hash{0x91}
	entry := semanticQueueEntry{enqueued: tlb.EnqueuedMsg{Msg: envelope}}
	validation := semanticQueueValidation{out: make(map[cell.Hash]*semanticOutDescriptor)}

	if err := validation.verifyProcessedLocalDequeue(hash, entry); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("missing dequeue error = %v", err)
	}

	validation.out[hash] = &semanticOutDescriptor{
		tag:      semanticOutDequeue,
		envelope: &semanticEnvelope{root: envelope},
	}
	if err := validation.verifyProcessedLocalDequeue(hash, entry); err != nil {
		t.Fatalf("exact full dequeue rejected: %v", err)
	}

	validation.out[hash] = &semanticOutDescriptor{
		tag:          semanticOutDequeueShort,
		envelopeHash: envelope.HashKey(),
	}
	if err := validation.verifyProcessedLocalDequeue(hash, entry); err != nil {
		t.Fatalf("exact short dequeue rejected: %v", err)
	}

	validation.out[hash].envelopeHash = otherEnvelope.HashKey()
	if err := validation.verifyProcessedLocalDequeue(hash, entry); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("wrong short dequeue error = %v", err)
	}
}

// A fee-carrying inbound descriptor whose Grams field is truncated must be
// rejected outright. The fee comparisons in verifyInboundEnvelope and
// verifyTransitRewrite rely on that: they test the tag alone, so a descriptor
// that reached them with an unparsed fee would silently skip the check.
//
// The control is what makes this a test rather than a shape assertion: the two
// descriptors are byte-identical up to the Grams tail, and the well-formed one
// PARSES. Dropping the coins load is then caught by the control rather than by
// the truncated case — the unread fee bits become trailing data and the valid
// descriptor stops parsing. The truncated case alone cannot pin the coins load,
// because the trailing-data check rejects it either way; asserting only that it
// fails would pass against a parser that never looked at the fee.
func TestSemanticInDescriptorRejectsTruncatedFee(t *testing.T) {
	source := address.NewAddress(0, 0, bytes.Repeat([]byte{0x63}, 32))
	destination := address.NewAddress(0, 0, bytes.Repeat([]byte{0x64}, 32))
	message, err := tlb.ToCell(&tlb.InternalMessage{
		IHRDisabled: true,
		SrcAddr:     source,
		DstAddr:     destination,
		Amount:      tlb.FromNanoTONU(2_000_000),
		IHRFee:      tlb.FromNanoTONU(30),
		FwdFee:      tlb.FromNanoTONU(20),
		CreatedLT:   2_000_000,
		CreatedAt:   1_900_000_000,
		Body:        cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := (tlb.MsgEnvelope{
		NextAddr:        tlb.IntermediateAddress{UseDestBits: 96},
		FwdFeeRemaining: tlb.FromNanoTONU(11),
		Msg:             message,
	}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	transaction := cell.BeginCell().MustStoreUInt(0, 8).EndCell()
	key := [32]byte(message.HashKey())

	// msg_import_imm$011 envelope, transaction, fwd_fee:Grams.
	head := func(t *testing.T) *cell.Builder {
		t.Helper()
		builder := cell.BeginCell()
		if err := builder.StoreUInt(uint64(semanticInImmediate), 3); err != nil {
			t.Fatalf("store tag: %v", err)
		}
		if err := builder.StoreRef(envelope); err != nil {
			t.Fatalf("store envelope ref: %v", err)
		}
		if err := builder.StoreRef(transaction); err != nil {
			t.Fatalf("store transaction ref: %v", err)
		}
		return builder
	}

	control := head(t)
	if err := control.StoreBigCoins(tlb.FromNanoTONU(4).NanoRef()); err != nil {
		t.Fatalf("store fee: %v", err)
	}
	if _, err := parseSemanticInDescriptor(*control.EndCell().MustBeginParse(), key, nil); err != nil {
		t.Fatalf("well-formed control descriptor must parse, got: %v", err)
	}

	// Grams announces four value bytes and then supplies one.
	truncated := head(t)
	if err := truncated.StoreUInt(4, 4); err != nil {
		t.Fatalf("store fee length: %v", err)
	}
	if err := truncated.StoreUInt(0xFF, 8); err != nil {
		t.Fatalf("store fee body: %v", err)
	}
	if _, err := parseSemanticInDescriptor(*truncated.EndCell().MustBeginParse(), key, nil); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("truncated fee error = %v, want invalid input", err)
	}
}

// routeFixtureEnvelope wraps one internal message between the two addresses in
// an envelope whose both hops are encoded with useDestBits. Only that field
// varies between the fixtures below: the message itself, and therefore its
// source and destination prefixes, stays byte-identical.
func routeFixtureEnvelope(t *testing.T, src, dst *address.Address, useDestBits uint8) *semanticEnvelope {
	t.Helper()

	message, err := tlb.ToCell(&tlb.InternalMessage{
		IHRDisabled: true,
		SrcAddr:     src,
		DstAddr:     dst,
		Amount:      tlb.FromNanoTONU(1_000_000),
		FwdFee:      tlb.FromNanoTONU(1_000),
		CreatedLT:   5_000,
		CreatedAt:   1_900_000_000,
		Body:        cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	root, err := (tlb.MsgEnvelope{
		CurAddr:         tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: useDestBits},
		NextAddr:        tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: useDestBits},
		FwdFeeRemaining: tlb.FromNanoTONU(1_000),
		Msg:             message,
	}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := parseSemanticEnvelope(root)
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}

// Comparing only the interpolated hop prefixes leaves a second encoding of the
// same route: once source and destination agree on the trailing address bits,
// several use_dest_bits values interpolate to one prefix. The reference
// validator pins the raw pair as well -- (96,96) for msg_export_imm
// (validate-query.cpp:4904), the hypercube routing output for msg_export_new
// and msg_export_deferred_tr (:4929) and for a rewritten transit envelope
// (:4386) -- and rejects everything else as a non-canonical raw route. gton's
// own collator always stores exactly those bits (execute.go, imports.go), so
// only a foreign or delegated collator can produce the rejected shape.
//
// The fixture addresses share their low 32 prefix bits, so use_dest_bits 64
// interpolates to the destination just as the canonical 96 does; the assertions
// below pin that premise, and each case carries the canonical envelope as a
// positive control so a rejection for any other reason cannot pass for
// protection.
func TestSemanticOutboundRejectsNonCanonicalRawRoute(t *testing.T) {
	shard := msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll}
	source := address.NewAddress(0, 0, append(
		append(bytes.Repeat([]byte{0xc1}, 4), bytes.Repeat([]byte{0xaa}, 4)...),
		bytes.Repeat([]byte{0x11}, 24)...,
	))
	destination := address.NewAddress(0, 0, append(
		append(bytes.Repeat([]byte{0xd1}, 4), bytes.Repeat([]byte{0xaa}, 4)...),
		bytes.Repeat([]byte{0x22}, 24)...,
	))
	sourcePrefix, err := msgpool.AccountPrefixFromAddress(source)
	if err != nil {
		t.Fatal(err)
	}
	destinationPrefix, err := msgpool.AccountPrefixFromAddress(destination)
	if err != nil {
		t.Fatal(err)
	}
	const alternativeBits = 64
	if msgpool.InterpolatePrefix(sourcePrefix, destinationPrefix, alternativeBits) !=
		msgpool.InterpolatePrefix(sourcePrefix, destinationPrefix, routingAddressBits) {
		t.Fatal("fixture addresses do not admit an alternative raw route")
	}
	curBits, nextBits, err := performHypercubeRouting(sourcePrefix, destinationPrefix, shard, 0)
	if err != nil {
		t.Fatal(err)
	}
	if curBits != routingAddressBits || nextBits != routingAddressBits {
		t.Fatalf("canonical route = (%d,%d), want (%d,%d)", curBits, nextBits, routingAddressBits, routingAddressBits)
	}

	canonical := routeFixtureEnvelope(t, source, destination, routingAddressBits)
	alternative := routeFixtureEnvelope(t, source, destination, alternativeBits)
	if canonical.current != alternative.current || canonical.next != alternative.next {
		t.Fatal("alternative raw route does not interpolate to the canonical hops")
	}
	hash := cell.Hash(canonical.message.HashKey())

	outboundEnvelope := func(t *testing.T, tag uint8, envelope *semanticEnvelope) error {
		t.Helper()

		validation := &semanticQueueValidation{target: shard}
		return validation.verifyOutboundEnvelope(hash, &semanticOutDescriptor{tag: tag, envelope: envelope})
	}
	// A deferred transit rewrite carries a dispatch-shaped inbound envelope,
	// whose zeroed hops make its next hop the message source -- the very pair
	// the reference routes a transit message from.
	transitRewrite := func(t *testing.T, envelope *semanticEnvelope) error {
		t.Helper()

		inbound := routeFixtureEnvelope(t, source, destination, 0)
		validation := &semanticQueueValidation{
			replay: &semanticReplay{},
			target: shard,
			in: map[cell.Hash]*semanticInDescriptor{hash: {
				tag:         semanticInDeferredTransit,
				envelope:    inbound,
				outEnvelope: envelope,
			}},
		}
		descriptor := &semanticOutDescriptor{tag: semanticOutDeferredTransit, envelope: envelope}
		return validation.verifyTransitRewrite(hash, descriptor, semanticInDeferredTransit)
	}

	tests := []struct {
		name   string
		verify func(*testing.T, *semanticEnvelope) error
	}{
		{
			name: "immediate",
			verify: func(t *testing.T, envelope *semanticEnvelope) error {
				return outboundEnvelope(t, semanticOutImmediate, envelope)
			},
		},
		{
			name: "new",
			verify: func(t *testing.T, envelope *semanticEnvelope) error {
				return outboundEnvelope(t, semanticOutNew, envelope)
			},
		},
		{
			name:   "deferred transit rewrite",
			verify: transitRewrite,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.verify(t, canonical); err != nil {
				t.Fatalf("canonical raw route rejected: %v", err)
			}
			err := test.verify(t, alternative)
			if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "non-canonical raw route") {
				t.Fatalf("alternative raw route error = %v, want invalid input naming a non-canonical raw route", err)
			}
		})
	}
}

// The queue phase reports the offending message hash, so its loops must run in
// dictionary order: two validators rejecting the same block, or one validator
// twice, have to name the same message. Go reshuffles map iteration on every
// range, so repeating the pass in one process is enough to catch a regression.
func TestSemanticQueueRejectionReasonIsDeterministic(t *testing.T) {
	inMessages, err := tlb.NewInMsgDescrAugDict(11)
	if err != nil {
		t.Fatal(err)
	}
	var keys [][32]byte
	for i := range 8 {
		message := cell.BeginCell().MustStoreUInt(uint64(i), 32).EndCell()
		descriptor, descriptorErr := descriptor(semanticInExternal, 3, message, cell.BeginCell().EndCell())
		if descriptorErr != nil {
			t.Fatal(descriptorErr)
		}
		if err = insertDescriptor(inMessages.AugmentedDictionary, message, descriptor); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, message.HashKey())
	}
	outMessages, err := tlb.NewOutMsgDescrAugDict(11)
	if err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(keys, func(left, right [32]byte) int {
		return bytes.Compare(left[:], right[:])
	})

	// No transaction was consumed, so every descriptor above trips coverage and
	// the reported one is whichever the loop reaches first.
	want := fmt.Sprintf("inbound descriptor %x transaction coverage mismatch", keys[0])
	for range 32 {
		validation := &semanticQueueValidation{
			replay: &semanticReplay{
				ctx:         context.Background(),
				inMessages:  inMessages,
				outMessages: outMessages,
				consumedIn:  make(map[cell.Hash]struct{}),
				consumedOut: make(map[cell.Hash]struct{}),
			},
			in:  make(map[cell.Hash]*semanticInDescriptor),
			out: make(map[cell.Hash]*semanticOutDescriptor),
		}
		if err = validation.loadDescriptors(); err != nil {
			t.Fatalf("load descriptors: %v", err)
		}
		if len(validation.inOrder) != len(validation.in) {
			t.Fatalf("inbound order = %d keys, map = %d", len(validation.inOrder), len(validation.in))
		}
		for i := 1; i < len(validation.inOrder); i++ {
			if bytes.Compare(validation.inOrder[i-1].hash[:], validation.inOrder[i].hash[:]) >= 0 {
				t.Fatalf("inbound order is not ascending at %d", i)
			}
		}
		err = validation.verifyDescriptorCoverage()
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("coverage rejection = %v, want %s", err, want)
		}
	}
}
