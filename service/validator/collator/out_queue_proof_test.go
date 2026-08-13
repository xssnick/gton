package collator

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

func TestFullCollatedDispatchQueueMoveRetainsReferencedBody(t *testing.T) {
	req := emptyCandidateRequest(t)
	startLT := requestStartLT(t, req)
	sourceID := [32]byte{0x48}
	lts := make([]uint64, 30)
	for i := range lts {
		lts[i] = startLT - 300 + uint64(i)*10
	}
	req.Previous.State = previousStateWithDispatchQueue(t, req.Previous.State, makeDispatchQueue(t,
		dispatchFixtureAccount{
			accountID: sourceID,
			lts:       lts,
			bodyInRef: true,
		},
	))
	req.Previous.State = previousStateWithLazyDispatchBodies(t, req.Previous.State, sourceID, lts)
	req.Previous.State = previousStateWithDenseOutQueue(t, req.Previous.State, 1_024)
	denseQueueSize := uint64(1_024)
	req.Previous.OutQueueSize = &denseQueueSize
	// The predecessor consumes only the first deferred message and delivers it
	// immediately. The second referenced body therefore survives solely through
	// DispatchQueue, matching the provenance of actor #102959.
	req.Internals = &msgpool.Cut{}
	req = advanceCandidateRequestApplyingUpdate(t, req)
	req.Internals = nil
	wantKeys := dispatchOutQueueKeys(t, req.Previous.State, sourceID, 20)
	req.Dispatch = DispatchPolicy{
		DeferringEnabled:      true,
		DeferMessagesAfter:    100,
		Phase2MaxTotal:        19,
		Phase2MaxPerInitiator: 100,
	}
	req.Masterchain.Config.capabilities |= capFullCollatedData
	attachFullCollatedTestNeighbors(t, &req)

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Stats.DispatchedMessages != 20 || candidate.Stats.EnqueuedMessages != 20 {
		t.Fatalf("dispatch/enqueue stats = %d/%d, want 20/20",
			candidate.Stats.DispatchedMessages, candidate.Stats.EnqueuedMessages)
	}
	var fullState tlb.ShardStateUnsplit
	if err = parseExact(&fullState, candidate.State); err != nil {
		t.Fatal(err)
	}
	var fullQueue tlb.OutMsgQueueInfo
	if err = parseExact(&fullQueue, fullState.OutMsgQueueInfo); err != nil {
		t.Fatal(err)
	}
	for i, key := range wantKeys {
		keyCell := cell.BeginCell().MustStoreSlice(key[:], 352).EndCell()
		if _, loadErr := fullQueue.OutQueue.LoadValue(keyCell); loadErr != nil {
			t.Fatalf("full state lacks expected outbound queue key %d %x: %v", i, key, loadErr)
		}
	}

	roots, err := cell.FromBOCMultiRoot(candidate.CollatedData)
	if err != nil {
		t.Fatal(err)
	}
	var provenPrevious *cell.Cell
	for _, root := range roots {
		proven, unwrapErr := unwrapCollatedProof(root)
		if unwrapErr == nil && proven.HashKeyAt(0) == req.Previous.State.HashKeyAt(0) {
			provenPrevious = proven
			break
		}
	}
	if provenPrevious == nil {
		t.Fatal("collated data has no predecessor state proof")
	}

	blockRoot, err := cell.FromBOC(candidate.BlockBOC)
	if err != nil {
		t.Fatal(err)
	}
	var block tlb.Block
	if err = parseExact(&block, blockRoot); err != nil {
		t.Fatal(err)
	}
	// Deserialize the update independently from the block graph. This prevents
	// the full Message copy in BlockExtra from satisfying a missing state-proof
	// cell by object sharing, which the receiving C++ validator cannot rely on.
	isolatedUpdate, err := cell.FromBOC(block.StateUpdate.ToBOC())
	if err != nil {
		t.Fatal(err)
	}
	applied, err := cell.ApplyMerkleUpdate(provenPrevious, isolatedUpdate)
	if err != nil {
		t.Fatal(err)
	}

	var state tlb.ShardStateUnsplit
	if err = parseProofExact(&state, applied); err != nil {
		t.Fatal(err)
	}
	var queue tlb.OutMsgQueueInfo
	if err = parseProofExact(&queue, state.OutMsgQueueInfo); err != nil {
		t.Fatal(err)
	}
	for i, key := range wantKeys {
		keyCell := cell.BeginCell().MustStoreSlice(key[:], 352).EndCell()
		value, loadErr := queue.OutQueue.LoadValue(keyCell)
		if loadErr != nil {
			t.Fatalf("load generated outbound queue key %d %x: %v", i, key, loadErr)
		}
		var enqueued tlb.EnqueuedMsg
		if loadErr = loadExactSlice(&enqueued, value); loadErr != nil {
			t.Fatal(loadErr)
		}
		var envelope tlb.MsgEnvelope
		if loadErr = parseExact(&envelope, enqueued.Msg); loadErr != nil {
			t.Fatal(loadErr)
		}
		var message tlb.InternalMessage
		if loadErr = parseExact(&message, envelope.Msg); loadErr != nil {
			t.Fatal(loadErr)
		}
		if message.Body.IsSpecial() {
			t.Fatalf("generated EnqueuedMsg body is pruned after applying the collated update: %x", message.Body.Hash())
		}
	}
}

func previousStateWithDenseOutQueue(t *testing.T, root *cell.Cell, count int) *cell.Cell {
	t.Helper()

	var state tlb.ShardStateUnsplit
	if err := parseExact(&state, root); err != nil {
		t.Fatal(err)
	}
	var queue tlb.OutMsgQueueInfo
	if err := parseExact(&queue, state.OutMsgQueueInfo); err != nil {
		t.Fatal(err)
	}
	fee := tlb.FromNanoTONU(100_000)
	for i := range count {
		var sourceID, destinationID [32]byte
		sourceID[0], sourceID[1], sourceID[31] = 0x20, byte(i>>8), byte(i)
		destinationID[0], destinationID[1], destinationID[31] = byte(i), byte(i>>8), 0x80
		message, enqueued := queuedInternalWithReferencedBody(
			t,
			address.NewAddress(0, 0, sourceID[:]),
			address.NewAddress(0, 0, destinationID[:]),
			1_000_000+uint64(i),
			1_900_000_000,
			fee,
			fee,
			0,
			msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll},
		)
		keyCell := cell.BeginCell().MustStoreSlice(message.Key[:], 352).EndCell()
		inserted, err := queue.OutQueue.SetWithMode(keyCell, enqueued, cell.DictSetModeAdd)
		if err != nil || !inserted {
			t.Fatalf("insert dense outbound queue entry %d: inserted=%t err=%v", i, inserted, err)
		}
	}
	var err error
	state.OutMsgQueueInfo, err = queue.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	root, err = tlb.ToCell(&state)
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func dispatchOutQueueKeys(t *testing.T, root *cell.Cell, accountID [32]byte, count int) []msgpool.QueueKey {
	t.Helper()

	var state tlb.ShardStateUnsplit
	if err := parseExact(&state, root); err != nil {
		t.Fatal(err)
	}
	var queue tlb.OutMsgQueueInfo
	if err := parseExact(&queue, state.OutMsgQueueInfo); err != nil {
		t.Fatal(err)
	}
	account, err := loadAccountDispatchQueue(queue.Extra.DispatchQueue, accountID)
	if err != nil {
		t.Fatal(err)
	}
	items, err := account.Messages.LoadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) < count {
		t.Fatalf("dispatch queue contains %d entries, want at least %d", len(items), count)
	}
	keys := make([]msgpool.QueueKey, 0, count)
	for _, item := range items[:count] {
		var enqueued tlb.EnqueuedMsg
		loadErr := loadExactSlice(&enqueued, item.Value)
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		var envelope tlb.MsgEnvelope
		if loadErr = parseExact(&envelope, enqueued.Msg); loadErr != nil {
			t.Fatal(loadErr)
		}
		var message tlb.InternalMessage
		if loadErr = parseExact(&message, envelope.Msg); loadErr != nil {
			t.Fatal(loadErr)
		}
		source, loadErr := accountPrefixFromAddress(message.SrcAddr)
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		destination, loadErr := accountPrefixFromAddress(message.DstAddr)
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		keys = append(keys, msgpool.MakeQueueKey(
			msgpool.InterpolatePrefix(source, destination, 96),
			envelope.Msg.HashKey(),
		))
	}
	return keys
}

func advanceCandidateRequestApplyingUpdate(t *testing.T, req ShardRequest) ShardRequest {
	t.Helper()

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	blockRoot, err := cell.FromBOC(candidate.BlockBOC)
	if err != nil {
		t.Fatal(err)
	}
	stateRoot, err := cell.ApplyMerkleUpdate(req.Previous.State.WithoutTrace(), candidate.StateUpdate)
	if err != nil {
		t.Fatal(err)
	}
	queueSize := candidate.Stats.OutQueueSize
	req.Previous = PreviousBlock{
		ID:           candidate.ID,
		Block:        blockRoot,
		State:        stateRoot,
		OutQueueSize: &queueSize,
	}
	req.Header.GenUtime++
	req.Header.GenUtimeMS = uint64(req.Header.GenUtime) * 1_000
	return req
}

func previousStateWithLazyDispatchBodies(
	t *testing.T,
	root *cell.Cell,
	accountID [32]byte,
	lts []uint64,
) *cell.Cell {
	t.Helper()

	var state tlb.ShardStateUnsplit
	if err := parseExact(&state, root); err != nil {
		t.Fatal(err)
	}
	var queue tlb.OutMsgQueueInfo
	if err := parseExact(&queue, state.OutMsgQueueInfo); err != nil {
		t.Fatal(err)
	}
	account, err := loadAccountDispatchQueue(queue.Extra.DispatchQueue, accountID)
	if err != nil {
		t.Fatal(err)
	}
	for _, lt := range lts {
		key := dispatchLTKey(lt)
		value, loadErr := account.Messages.LoadValue(key)
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		var enqueued tlb.EnqueuedMsg
		if loadErr = loadExactSlice(&enqueued, value); loadErr != nil {
			t.Fatal(loadErr)
		}
		var envelope tlb.MsgEnvelope
		if loadErr = parseExact(&envelope, enqueued.Msg); loadErr != nil {
			t.Fatal(loadErr)
		}
		var internal tlb.InternalMessage
		if loadErr = parseExact(&internal, envelope.Msg); loadErr != nil {
			t.Fatal(loadErr)
		}

		body := internal.Body
		carrier := cell.BeginCell().MustStoreRef(body).EndCell()
		lazyCarrier, lazyErr := cell.CreateWithLazyRefsUnsafe(
			0x0100,
			nil,
			carrier.Hash(),
			[]uint16{carrier.Depth()},
			[]cell.LazyRef{{
				LevelMask: body.LevelMask(),
				Hashes:    body.Hash(),
				Depths:    []uint16{body.Depth()},
			}},
			func(hash cell.Hash) (*cell.Cell, error) {
				if hash != body.HashKey() {
					return nil, fmt.Errorf("unexpected dispatch body hash %x", hash)
				}
				return body, nil
			},
		)
		if lazyErr != nil {
			t.Fatal(lazyErr)
		}
		internal.Body = lazyCarrier.MustPeekRef(0)
		messageBuilder := cell.BeginCell()
		if loadErr = tlb.StoreMessageWithLayout(messageBuilder, &tlb.Message{
			MsgType: tlb.MsgTypeInternal,
			Msg:     &internal,
		}, tlb.MessageLayout{BodyInRef: true}); loadErr != nil {
			t.Fatal(loadErr)
		}
		rebuiltMessage := messageBuilder.EndCell()
		if rebuiltMessage.HashKey() != envelope.Msg.HashKey() {
			t.Fatal("lazy body changed dispatch message hash")
		}
		envelope.Msg = rebuiltMessage
		envelopeCell, serializeErr := envelope.ToCell()
		if serializeErr != nil {
			t.Fatal(serializeErr)
		}
		enqueued.Msg = envelopeCell
		enqueuedCell, serializeErr := enqueued.ToCell()
		if serializeErr != nil {
			t.Fatal(serializeErr)
		}
		if serializeErr = account.Messages.Set(key, enqueuedCell); serializeErr != nil {
			t.Fatal(serializeErr)
		}
	}
	if err = storeAccountDispatchQueue(queue.Extra.DispatchQueue.AugmentedDictionary, accountID, account); err != nil {
		t.Fatal(err)
	}
	state.OutMsgQueueInfo, err = queue.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	lazyRoot, err := tlb.ToCell(&state)
	if err != nil {
		t.Fatal(err)
	}
	if lazyRoot.HashKey() != root.HashKey() {
		t.Fatal("lazy dispatch bodies changed predecessor state hash")
	}
	return lazyRoot
}

func TestTraceOutQueueValidationClosureLoadsDequeuedEnvelope(t *testing.T) {
	source := address.NewAddress(0, 0, bytes.Repeat([]byte{0x31}, 32))
	destination := address.NewAddress(0, 0, bytes.Repeat([]byte{0x32}, 32))
	fee := tlb.FromNanoTONU(100_000)
	message, enqueued := queuedInternalWithReferencedBody(
		t,
		source,
		destination,
		10_000_001,
		1_900_000_000,
		fee,
		fee,
		96,
		msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll},
	)

	queue, err := tlb.NewOutMsgQueueAugDict()
	if err != nil {
		t.Fatal(err)
	}
	keyCell := cell.BeginCell().MustStoreSlice(message.Key[:], 352).EndCell()
	inserted, err := queue.SetWithMode(keyCell, enqueued, cell.DictSetModeAdd)
	if err != nil || !inserted {
		t.Fatalf("insert predecessor queue entry: inserted=%v err=%v", inserted, err)
	}
	queueInfo, err := (tlb.OutMsgQueueInfo{
		OutQueue: queue,
		ProcInfo: cell.NewDict(processedInfoKeyBits),
	}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	unrelated := cell.BeginCell().MustStoreRef(
		cell.BeginCell().MustStoreUInt(0xdd, 8).EndCell(),
	).EndCell()
	root := cell.BeginCell().MustStoreRef(queueInfo).MustStoreRef(unrelated).EndCell()

	usage := cell.NewReadSet(root)
	rootLoader := usage.Root().MustBeginParse()
	tracedQueueInfo, err := rootLoader.LoadRefCell()
	if err != nil {
		t.Fatal(err)
	}
	var previous tlb.OutMsgQueueInfo
	if err = parseExact(&previous, tracedQueueInfo); err != nil {
		t.Fatal(err)
	}
	c := &collation{
		usage: usage,
		oldOutQueue: &tlb.OutMsgQueueAugDict{
			AugmentedDictionary: previous.OutQueue.Copy(),
		},
		outQueue: &tlb.OutMsgQueueAugDict{
			AugmentedDictionary: previous.OutQueue.Copy(),
		},
	}
	c.deferQueueDelete(message.Key, keyCell)
	if err = c.flushQueueDeletes(); err != nil {
		t.Fatal(err)
	}
	if err = c.traceOutQueueValidationClosure(); err != nil {
		t.Fatal(err)
	}

	proof, err := usage.Proof()
	if err != nil {
		t.Fatal(err)
	}
	proven, err := cell.UnwrapProof(proof, root.Hash())
	if err != nil {
		t.Fatal(err)
	}
	provenLoader := proven.MustBeginParse()
	provenQueueInfo, err := provenLoader.LoadRefCell()
	if err != nil {
		t.Fatal(err)
	}
	var provenQueue tlb.OutMsgQueueInfo
	if err = parseExact(&provenQueue, provenQueueInfo); err != nil {
		t.Fatal(err)
	}
	value, err := provenQueue.OutQueue.LoadValue(keyCell)
	if err != nil {
		t.Fatalf("load dequeued predecessor entry: %v", err)
	}
	entry, err := parseQueueEntry(value, message.Key)
	if err != nil {
		t.Fatalf("parse dequeued predecessor envelope: %v", err)
	}
	var envelope tlb.MsgEnvelope
	if err = parseExact(&envelope, entry.envelope); err != nil {
		t.Fatalf("parse proven envelope: %v", err)
	}
	var internal tlb.InternalMessage
	if err = parseExact(&internal, envelope.Msg); err != nil {
		t.Fatalf("parse proven message: %v", err)
	}
	if _, err = internal.Body.BeginParse(); err != nil {
		t.Fatalf("open proven referenced body: %v", err)
	}

	provenUnrelated, err := provenLoader.LoadRefCell()
	if err != nil {
		t.Fatal(err)
	}
	if provenUnrelated.GetType() != cell.PrunedCellType {
		t.Fatal("unrelated predecessor subtree is materialized")
	}
}

func TestTraceOutQueueValidationClosureRetainsReusedEnqueuedBody(t *testing.T) {
	source := address.NewAddress(0, 0, bytes.Repeat([]byte{0x41}, 32))
	destination := address.NewAddress(0, 0, bytes.Repeat([]byte{0x42}, 32))
	fee := tlb.FromNanoTONU(100_000)
	message, _ := queuedInternalWithReferencedBody(
		t,
		source,
		destination,
		20_000_001,
		1_900_000_100,
		fee,
		fee,
		96,
		msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll},
	)

	var internal tlb.InternalMessage
	if err := parseExact(&internal, message.Root); err != nil {
		t.Fatal(err)
	}
	bodyCarrier := cell.BeginCell().MustStoreRef(internal.Body).EndCell()
	oldQueue, err := tlb.NewOutMsgQueueAugDict()
	if err != nil {
		t.Fatal(err)
	}
	oldQueueInfo, err := (tlb.OutMsgQueueInfo{
		OutQueue: oldQueue,
		ProcInfo: cell.NewDict(processedInfoKeyBits),
	}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	oldRoot := cell.BeginCell().MustStoreRef(oldQueueInfo).MustStoreRef(bodyCarrier).EndCell()

	usage := cell.NewReadSet(oldRoot)
	oldLoader := usage.Root().MustBeginParse()
	tracedQueueInfo, err := oldLoader.LoadRefCell()
	if err != nil {
		t.Fatal(err)
	}
	var previous tlb.OutMsgQueueInfo
	if err = parseExact(&previous, tracedQueueInfo); err != nil {
		t.Fatal(err)
	}
	tracedCarrier, err := oldLoader.LoadRefCell()
	if err != nil {
		t.Fatal(err)
	}
	tracedBody, err := tracedCarrier.MustBeginParse().LoadRefCell()
	if err != nil {
		t.Fatal(err)
	}
	bodyHash := tracedBody.HashKeyAt(0)
	if _, read := usage.Contains(bodyHash); read {
		t.Fatal("reused body was loaded before tracing the queue validation closure")
	}

	internal.Body = tracedBody
	messageBuilder := cell.BeginCell()
	if err = tlb.StoreMessageWithLayout(messageBuilder, &tlb.Message{
		MsgType: tlb.MsgTypeInternal,
		Msg:     &internal,
	}, tlb.MessageLayout{BodyInRef: true}); err != nil {
		t.Fatal(err)
	}
	rebuiltMessage := messageBuilder.EndCell()
	envelope := message.Envelope
	envelope.Msg = rebuiltMessage
	envelopeCell, err := envelope.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	enqueued, err := (tlb.EnqueuedMsg{
		EnqueuedLT: message.EnqueuedLT,
		Msg:        envelopeCell,
	}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	key := msgpool.MakeQueueKey(message.Key.NextHop(), rebuiltMessage.HashKey())
	keyCell := cell.BeginCell().MustStoreSlice(key[:], 352).EndCell()

	newQueue := &tlb.OutMsgQueueAugDict{AugmentedDictionary: previous.OutQueue.Copy()}
	inserted, err := newQueue.SetWithMode(keyCell, enqueued, cell.DictSetModeAdd)
	if err != nil || !inserted {
		t.Fatalf("insert candidate queue entry: inserted=%v err=%v", inserted, err)
	}
	newQueueInfo, err := (tlb.OutMsgQueueInfo{
		OutQueue: newQueue,
		ProcInfo: cell.NewDict(processedInfoKeyBits),
	}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	newRoot := cell.BeginCell().MustStoreRef(newQueueInfo).MustStoreRef(bodyCarrier).EndCell()

	c := &collation{
		usage:       usage,
		oldOutQueue: previous.OutQueue,
		outQueue:    newQueue,
	}
	if err = c.traceOutQueueValidationClosure(); err != nil {
		t.Fatal(err)
	}
	if _, read := usage.Contains(bodyHash); !read {
		t.Fatal("queue validation closure did not load the C++ Anything body boundary")
	}

	update, _, err := usage.CreateMerkleUpdateApplied(newRoot)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := usage.Proof()
	if err != nil {
		t.Fatal(err)
	}
	proven, err := cell.UnwrapProof(proof, oldRoot.Hash())
	if err != nil {
		t.Fatal(err)
	}
	applied, err := cell.ApplyMerkleUpdate(proven, update)
	if err != nil {
		t.Fatal(err)
	}

	appliedLoader := applied.MustBeginParse()
	appliedQueueInfo, err := appliedLoader.LoadRefCell()
	if err != nil {
		t.Fatal(err)
	}
	var appliedQueue tlb.OutMsgQueueInfo
	if err = parseExact(&appliedQueue, appliedQueueInfo); err != nil {
		t.Fatal(err)
	}
	value, err := appliedQueue.OutQueue.LoadValue(keyCell)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := parseQueueEntry(value, key)
	if err != nil {
		t.Fatal(err)
	}
	var appliedEnvelope tlb.MsgEnvelope
	if err = parseExact(&appliedEnvelope, entry.envelope); err != nil {
		t.Fatal(err)
	}
	var appliedMessage tlb.InternalMessage
	if err = parseExact(&appliedMessage, appliedEnvelope.Msg); err != nil {
		t.Fatal(err)
	}
	if appliedMessage.Body.IsSpecial() {
		t.Fatalf("C++ Anything boundary is special after applying update: type=%v hash=%x",
			appliedMessage.Body.GetType(), appliedMessage.Body.Hash())
	}
	if appliedMessage.Body.HashKey() != internal.Body.HashKey() {
		t.Fatalf("applied body hash = %x, want %x", appliedMessage.Body.Hash(), internal.Body.Hash())
	}
}

func TestTraceOutQueueValidationClosureRetainsReusedStateInitCode(t *testing.T) {
	source := address.NewAddress(0, 0, bytes.Repeat([]byte{0x51}, 32))
	destination := address.NewAddress(0, 0, bytes.Repeat([]byte{0x52}, 32))
	fee := tlb.FromNanoTONU(100_000)
	message, _ := queuedInternalWithReferencedBody(
		t,
		source,
		destination,
		30_000_001,
		1_900_000_200,
		fee,
		fee,
		96,
		msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll},
	)

	codeLeaf := cell.BeginCell().MustStoreUInt(0x77, 8).EndCell()
	code := cell.BeginCell().MustStoreRef(codeLeaf).EndCell()
	codeCarrier := cell.BeginCell().MustStoreRef(code).EndCell()
	oldQueue, err := tlb.NewOutMsgQueueAugDict()
	if err != nil {
		t.Fatal(err)
	}
	oldQueueInfo, err := (tlb.OutMsgQueueInfo{
		OutQueue: oldQueue,
		ProcInfo: cell.NewDict(processedInfoKeyBits),
	}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	oldRoot := cell.BeginCell().MustStoreRef(oldQueueInfo).MustStoreRef(codeCarrier).EndCell()

	usage := cell.NewReadSet(oldRoot)
	oldLoader := usage.Root().MustBeginParse()
	tracedQueueInfo, err := oldLoader.LoadRefCell()
	if err != nil {
		t.Fatal(err)
	}
	var previous tlb.OutMsgQueueInfo
	if err = parseExact(&previous, tracedQueueInfo); err != nil {
		t.Fatal(err)
	}
	tracedCarrier, err := oldLoader.LoadRefCell()
	if err != nil {
		t.Fatal(err)
	}
	tracedCode, err := tracedCarrier.MustBeginParse().LoadRefCell()
	if err != nil {
		t.Fatal(err)
	}
	codeHash := tracedCode.HashKeyAt(0)
	if _, read := usage.Contains(codeHash); read {
		t.Fatal("reused StateInit code was loaded before tracing the queue validation closure")
	}

	var internal tlb.InternalMessage
	if err = parseExact(&internal, message.Root); err != nil {
		t.Fatal(err)
	}
	internal.StateInit = &tlb.StateInit{Code: tracedCode}
	messageBuilder := cell.BeginCell()
	if err = tlb.StoreMessageWithLayout(messageBuilder, &tlb.Message{
		MsgType: tlb.MsgTypeInternal,
		Msg:     &internal,
	}, tlb.MessageLayout{StateInitInRef: true, BodyInRef: true}); err != nil {
		t.Fatal(err)
	}
	rebuiltMessage := messageBuilder.EndCell()
	envelope := message.Envelope
	envelope.Msg = rebuiltMessage
	envelopeCell, err := envelope.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	enqueued, err := (tlb.EnqueuedMsg{
		EnqueuedLT: message.EnqueuedLT,
		Msg:        envelopeCell,
	}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	key := msgpool.MakeQueueKey(message.Key.NextHop(), rebuiltMessage.HashKey())
	keyCell := cell.BeginCell().MustStoreSlice(key[:], 352).EndCell()

	newQueue := &tlb.OutMsgQueueAugDict{AugmentedDictionary: previous.OutQueue.Copy()}
	inserted, err := newQueue.SetWithMode(keyCell, enqueued, cell.DictSetModeAdd)
	if err != nil || !inserted {
		t.Fatalf("insert candidate queue entry: inserted=%v err=%v", inserted, err)
	}
	c := &collation{
		usage:       usage,
		oldOutQueue: previous.OutQueue,
		outQueue:    newQueue,
	}
	if err = c.traceOutQueueValidationClosure(); err != nil {
		t.Fatal(err)
	}
	if _, read := usage.Contains(codeHash); !read {
		t.Fatal("queue validation closure did not load the C++ StateInit code boundary")
	}
}

func TestTraceOutQueueValidationClosureRetainsReusedExtraCurrencies(t *testing.T) {
	source := address.NewAddress(0, 0, bytes.Repeat([]byte{0x61}, 32))
	destination := address.NewAddress(0, 0, bytes.Repeat([]byte{0x62}, 32))
	fee := tlb.FromNanoTONU(100_000)
	message, _ := queuedInternalWithReferencedBody(
		t,
		source,
		destination,
		40_000_001,
		1_900_000_300,
		fee,
		fee,
		96,
		msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll},
	)

	extra := cell.NewDict(32)
	for id, amount := range map[uint32]uint64{1: 7, ^uint32(0): 11} {
		value := cell.BeginCell().MustStoreBigVarUInt(new(big.Int).SetUint64(amount), 32).EndCell()
		if err := extra.SetIntKey(new(big.Int).SetUint64(uint64(id)), value); err != nil {
			t.Fatal(err)
		}
	}
	extraRoot := extra.AsCell()
	extraCarrier := cell.BeginCell().MustStoreRef(extraRoot).EndCell()
	oldQueue, err := tlb.NewOutMsgQueueAugDict()
	if err != nil {
		t.Fatal(err)
	}
	oldQueueInfo, err := (tlb.OutMsgQueueInfo{
		OutQueue: oldQueue,
		ProcInfo: cell.NewDict(processedInfoKeyBits),
	}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	oldRoot := cell.BeginCell().MustStoreRef(oldQueueInfo).MustStoreRef(extraCarrier).EndCell()

	usage := cell.NewReadSet(oldRoot)
	oldLoader := usage.Root().MustBeginParse()
	tracedQueueInfo, err := oldLoader.LoadRefCell()
	if err != nil {
		t.Fatal(err)
	}
	var previous tlb.OutMsgQueueInfo
	if err = parseExact(&previous, tracedQueueInfo); err != nil {
		t.Fatal(err)
	}
	tracedCarrier, err := oldLoader.LoadRefCell()
	if err != nil {
		t.Fatal(err)
	}
	tracedExtraRoot, err := tracedCarrier.MustBeginParse().LoadRefCell()
	if err != nil {
		t.Fatal(err)
	}
	tracedExtraChild, err := tracedExtraRoot.PeekRef(0)
	if err != nil {
		t.Fatal(err)
	}
	childHash := tracedExtraChild.HashKeyAt(0)
	if _, read := usage.Contains(childHash); read {
		t.Fatal("reused extra-currency child was loaded before tracing the queue validation closure")
	}

	var internal tlb.InternalMessage
	if err = parseExact(&internal, message.Root); err != nil {
		t.Fatal(err)
	}
	internal.ExtraCurrencies = tracedExtraRoot.AsDict(32)
	messageBuilder := cell.BeginCell()
	if err = tlb.StoreMessageWithLayout(messageBuilder, &tlb.Message{
		MsgType: tlb.MsgTypeInternal,
		Msg:     &internal,
	}, tlb.MessageLayout{BodyInRef: true}); err != nil {
		t.Fatal(err)
	}
	rebuiltMessage := messageBuilder.EndCell()
	envelope := message.Envelope
	envelope.Msg = rebuiltMessage
	envelopeCell, err := envelope.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	enqueued, err := (tlb.EnqueuedMsg{
		EnqueuedLT: message.EnqueuedLT,
		Msg:        envelopeCell,
	}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	key := msgpool.MakeQueueKey(message.Key.NextHop(), rebuiltMessage.HashKey())
	keyCell := cell.BeginCell().MustStoreSlice(key[:], 352).EndCell()

	newQueue := &tlb.OutMsgQueueAugDict{AugmentedDictionary: previous.OutQueue.Copy()}
	inserted, err := newQueue.SetWithMode(keyCell, enqueued, cell.DictSetModeAdd)
	if err != nil || !inserted {
		t.Fatalf("insert candidate queue entry: inserted=%v err=%v", inserted, err)
	}
	c := &collation{
		usage:       usage,
		oldOutQueue: previous.OutQueue,
		outQueue:    newQueue,
	}
	if err = c.traceOutQueueValidationClosure(); err != nil {
		t.Fatal(err)
	}
	if _, read := usage.Contains(childHash); !read {
		t.Fatal("queue validation closure did not traverse the C++ ExtraCurrencyCollection")
	}
}
