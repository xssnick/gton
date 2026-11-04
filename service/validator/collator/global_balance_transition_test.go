package collator

import (
	"bytes"
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/groups"
)

func globalTransitionEnvelope(t testing.TB, source byte, next uint8, emitted *uint64) *cell.Cell {
	t.Helper()
	message, err := tlb.ToCell(globalBalanceMessageHeader{
		Source:      address.NewAddress(0, 0, bytes.Repeat([]byte{source}, 32)),
		Destination: address.NewAddress(0, 255, bytes.Repeat([]byte{2}, 32)),
		Value:       tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(100)},
		FwdFee:      tlb.FromNanoTONU(7), CreatedLT: 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := (tlb.MsgEnvelope{
		CurAddr:  tlb.IntermediateAddress{UseDestBits: 0},
		NextAddr: tlb.IntermediateAddress{UseDestBits: next},
		Msg:      message, FwdFeeRemaining: tlb.FromNanoTONU(5), EmittedLT: emitted,
	}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}

func globalTransitionQueue(t testing.TB, lt uint64, envelopes ...*cell.Cell) *tlb.OutMsgQueueAugDict {
	t.Helper()
	queue, err := tlb.NewOutMsgQueueAugDict()
	if err != nil {
		t.Fatal(err)
	}
	for _, envelope := range envelopes {
		entry := cell.BeginCell().MustStoreUInt(lt, 64).MustStoreRef(envelope).EndCell()
		_, key, err := parseGlobalBalanceMessage(entry.MustBeginParse(), 17, false)
		if err != nil {
			t.Fatal(err)
		}
		if err = queue.Set(cell.BeginCell().MustStoreSlice(key[:], 352).EndCell(), entry); err != nil {
			t.Fatal(err)
		}
	}
	return queue
}

func globalTransitionDescriptor(t testing.TB, tag uint8, envelope, old *cell.Cell) *cell.Cell {
	t.Helper()
	b := cell.BeginCell()
	switch tag {
	case semanticOutDeferredTransit, semanticOutNewDeferred:
		b.MustStoreUInt(uint64(tag), 5)
	case semanticOutDequeue, semanticOutDequeueShort:
		b.MustStoreUInt(uint64(tag), 4)
	default:
		b.MustStoreUInt(uint64(tag), 3)
	}
	if tag == semanticOutDequeueShort {
		_, key, err := parseGlobalBalanceMessage(cell.BeginCell().MustStoreUInt(0, 64).MustStoreRef(envelope).EndCell().MustBeginParse(), 17, false)
		if err != nil {
			t.Fatal(err)
		}
		return b.MustStoreSlice(envelope.Hash(), 256).MustStoreSlice(key[:12], 96).MustStoreUInt(99, 64).EndCell()
	}
	b.MustStoreRef(envelope)
	if tag == semanticOutDequeue {
		return b.MustStoreUInt(99, 63).EndCell()
	}
	if tag == semanticOutTransitRequest {
		in := cell.BeginCell().MustStoreUInt(semanticInTransit, 3).MustStoreRef(old).MustStoreRef(envelope).MustStoreUInt(0, 4).EndCell()
		return b.MustStoreRef(in).EndCell()
	}
	return b.MustStoreRef(cell.BeginCell().EndCell()).EndCell()
}

func globalTransitionBase(t testing.TB) (PreviousBlock, *localBlockSource) {
	t.Helper()
	master, store := globalBalanceAcquisitionFixture(t)
	state, err := verifyPredecessor("test", &PreviousBlock{ID: store.block, State: store.state})
	if err != nil {
		t.Fatal(err)
	}
	return master, &localBlockSource{previous: PreviousBlock{ID: store.block, State: store.state}, state: state}
}

func globalTransitionNext(t testing.TB, previous []*localBlockSource, shard int64, queue *tlb.OutMsgQueueAugDict, envelope, descriptor *cell.Cell, dispatch *tlb.DispatchQueueAugDict) (*localBlockSource, *tlb.Block) {
	t.Helper()
	state := previous[0].state
	state.Seqno++
	state.GenLT += 1_000
	for _, source := range previous[1:] {
		state.Seqno = max(state.Seqno, source.state.Seqno+1)
	}
	var err error
	state.ShardIdent = previous[0].state.ShardIdent
	if shard != previous[0].previous.ID.Shard {
		state.ShardIdent, err = topologyShardIdent(groups.ShardID{Workchain: previous[0].previous.ID.Workchain, Shard: shard})
		if err != nil {
			t.Fatal(err)
		}
	}
	var info tlb.OutMsgQueueInfo
	if err = parseExact(&info, state.OutMsgQueueInfo); err != nil {
		t.Fatal(err)
	}
	info.OutQueue = queue
	info.Extra = nil
	if dispatch != nil {
		info.Extra = &tlb.OutMsgQueueExtra{DispatchQueue: dispatch}
	}
	state.OutMsgQueueInfo, err = info.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	stateRoot, err := tlb.ToCell(&state)
	if err != nil {
		t.Fatal(err)
	}
	out, err := tlb.NewOutMsgDescrAugDict(17)
	if err != nil {
		t.Fatal(err)
	}
	if descriptor != nil {
		var env tlb.MsgEnvelope
		if err = parseExact(&env, envelope); err != nil {
			t.Fatal(err)
		}
		if err = out.Set(cell.BeginCell().MustStoreSlice(env.Msg.Hash(), 256).EndCell(), descriptor); err != nil {
			t.Fatal(err)
		}
	}
	outRoot, err := out.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	parent := previous[0].previous
	var header tlb.BlockHeader
	header.NotMaster = state.ShardIdent.WorkchainID != -1
	header.SeqNo = state.Seqno
	header.Shard = state.ShardIdent
	header.GenUtime = state.GenUTime
	header.StartLt = state.GenLT - 1_000
	header.EndLt = state.GenLT
	header.AfterSplit = len(previous) == 1 && shard != parent.ID.Shard
	header.AfterMerge = len(previous) == 2
	header.PrevRef = tlb.BlkPrevInfo{Prev1: tlb.ExtBlkRef{SeqNo: parent.ID.SeqNo, RootHash: parent.ID.RootHash, FileHash: parent.ID.FileHash}}
	if header.NotMaster {
		header.MasterRef = &tlb.ExtBlkRef{RootHash: make([]byte, 32), FileHash: make([]byte, 32)}
	}
	if len(previous) == 2 {
		second := previous[1].previous.ID
		header.PrevRef.Prev2 = &tlb.ExtBlkRef{SeqNo: second.SeqNo, RootHash: second.RootHash, FileHash: second.FileHash}
	}
	update, err := cell.CreateMerkleUpdate(parent.State, stateRoot)
	if err != nil {
		t.Fatal(err)
	}
	empty := cell.BeginCell().EndCell()
	block := &tlb.Block{GlobalID: state.GlobalID, BlockInfo: header, ValueFlow: empty, StateUpdate: update, Extra: &tlb.BlockExtra{
		OutMsgDesc: outRoot, InMsgDesc: empty, ShardAccountBlocks: empty, RandSeed: make([]byte, 32), CreatedBy: make([]byte, 32),
	}}
	blockRoot, err := tlb.ToCell(block)
	if err != nil {
		t.Fatal(err)
	}
	id := ton.BlockIDExt{Workchain: state.ShardIdent.WorkchainID, Shard: shard, SeqNo: state.Seqno, RootHash: blockRoot.Hash(), FileHash: bytes.Repeat([]byte{3}, 32)}
	return &localBlockSource{previous: PreviousBlock{ID: id, Block: blockRoot, State: stateRoot}, state: state}, block
}

type globalQueueDescriptorCase struct {
	name     string
	tag      uint8
	emitted  bool
	removes  bool
	requeues bool
}

func TestGlobalBalanceQueueDescriptorTransitions(t *testing.T) {
	for _, test := range []globalQueueDescriptorCase{
		{"new", semanticOutNew, false, false, false}, {"new emitted", semanticOutNew, true, false, false},
		{"transit", semanticOutTransit, false, false, false}, {"deferred transit", semanticOutDeferredTransit, true, false, false},
		{"dequeue", semanticOutDequeue, false, true, false}, {"dequeue immediate", semanticOutDequeueImmediate, false, true, false},
		{"dequeue short", semanticOutDequeueShort, false, true, false}, {"requeue", semanticOutTransitRequest, false, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, previous := globalTransitionBase(t)
			var emitted *uint64
			if test.emitted {
				value := uint64(70)
				emitted = &value
			}
			envelope := globalTransitionEnvelope(t, 1, 96, emitted)
			old := globalTransitionEnvelope(t, 1, 0, emitted)
			before := globalTransitionQueue(t, 20)
			if test.removes {
				before = globalTransitionQueue(t, 20, envelope)
			}
			if test.requeues {
				before = globalTransitionQueue(t, 20, old)
			}
			var info tlb.OutMsgQueueInfo
			if err := parseExact(&info, previous.state.OutMsgQueueInfo); err != nil {
				t.Fatal(err)
			}
			info.OutQueue = before
			previous.state.OutMsgQueueInfo, _ = info.ToCell()
			lt := previous.state.GenLT
			if test.tag == semanticOutNew || test.tag == semanticOutDeferredTransit {
				lt = 50
				if emitted != nil {
					lt = *emitted
				}
			}
			after := globalTransitionQueue(t, lt, envelope)
			if test.removes {
				after = globalTransitionQueue(t, 0)
			}
			descriptor := globalTransitionDescriptor(t, test.tag, envelope, old)
			next, block := globalTransitionNext(t, []*localBlockSource{previous}, math.MinInt64, after, envelope, descriptor, nil)
			if err := verifyGlobalBalanceTransition(context.Background(), next, []*localBlockSource{previous}, block, 17); err != nil {
				t.Fatal(err)
			}
			if test.removes {
				return
			}
			bad, _ := globalTransitionNext(t, []*localBlockSource{previous}, math.MinInt64, globalTransitionQueue(t, lt+1, envelope), nil, nil, nil)
			if err := verifyGlobalBalanceTransition(context.Background(), bad, []*localBlockSource{previous}, block, 17); err == nil {
				t.Fatal("incorrect enqueued LT accepted")
			}
			changed := globalTransitionEnvelope(t, 3, 96, emitted)
			bad, _ = globalTransitionNext(t, []*localBlockSource{previous}, math.MinInt64, globalTransitionQueue(t, lt, changed), nil, nil, nil)
			if err := verifyGlobalBalanceTransition(context.Background(), bad, []*localBlockSource{previous}, block, 17); err == nil {
				t.Fatal("same-grams message replacement accepted")
			}
		})
	}
}

func TestGlobalBalanceDispatchBalanceLoss(t *testing.T) {
	_, base := globalTransitionBase(t)
	legacy := makeDispatchQueue(t, dispatchFixtureAccount{accountID: [32]byte{1}, lts: []uint64{10}})
	for _, version := range []uint32{15, 16, 17} {
		next, block := globalTransitionNext(t, []*localBlockSource{base}, math.MinInt64, globalTransitionQueue(t, 0), nil, nil, legacy)
		err := verifyGlobalBalanceTransition(context.Background(), next, []*localBlockSource{base}, block, version)
		if (err != nil) != (version >= 16) {
			t.Fatalf("GV%d lost balance: %v", version, err)
		}
		if version >= 16 && !strings.Contains(err.Error(), "lost its stored total balance") {
			t.Fatal(err)
		}
	}
	old, _ := globalTransitionNext(t, []*localBlockSource{base}, math.MinInt64, globalTransitionQueue(t, 0), nil, nil, legacy)
	next, block := globalTransitionNext(t, []*localBlockSource{old}, math.MinInt64, globalTransitionQueue(t, 0), nil, nil, legacy)
	if err := verifyGlobalBalanceTransition(context.Background(), next, []*localBlockSource{old}, block, 17); err != nil {
		t.Fatalf("legacy queue continuation: %v", err)
	}
	for _, dispatch := range []*tlb.DispatchQueueAugDict{nil, globalTransitionEmptyDispatch(t)} {
		next, block = globalTransitionNext(t, []*localBlockSource{old}, math.MinInt64, globalTransitionQueue(t, 0), nil, nil, dispatch)
		if err := verifyGlobalBalanceTransition(context.Background(), next, []*localBlockSource{old}, block, 17); err != nil {
			t.Fatalf("legacy queue drained: %v", err)
		}
	}
}

func globalTransitionEmptyDispatch(t testing.TB) *tlb.DispatchQueueAugDict {
	t.Helper()
	queue, err := tlb.NewDispatchQueueAugDict()
	if err != nil {
		t.Fatal(err)
	}
	return queue
}

func TestGlobalBalanceQueueSplitMerge(t *testing.T) {
	_, zero := globalTransitionBase(t)
	base, _ := globalTransitionNext(t, []*localBlockSource{zero}, math.MinInt64, globalTransitionQueue(t, 0), nil, nil, nil)
	leftEnvelope, rightEnvelope := globalTransitionEnvelope(t, 1, 96, nil), globalTransitionEnvelope(t, 129, 96, nil)
	var queue tlb.OutMsgQueueInfo
	if err := parseExact(&queue, base.state.OutMsgQueueInfo); err != nil {
		t.Fatal(err)
	}
	queue.OutQueue = globalTransitionQueue(t, 10, leftEnvelope, rightEnvelope)
	base.state.OutMsgQueueInfo, _ = queue.ToCell()
	root := tlb.ShardID(uint64(1) << 63)
	left, leftBlock := globalTransitionNext(t, []*localBlockSource{base}, int64(root.GetChild(true)), globalTransitionQueue(t, 10, leftEnvelope), nil, nil, nil)
	right, rightBlock := globalTransitionNext(t, []*localBlockSource{base}, int64(root.GetChild(false)), globalTransitionQueue(t, 10, rightEnvelope), nil, nil, nil)
	for _, side := range []globalTransitionSide{{left, leftBlock}, {right, rightBlock}} {
		if err := verifyGlobalBalanceTransition(context.Background(), side.source, []*localBlockSource{base}, side.block, 17); err != nil {
			t.Fatal(err)
		}
		ids, err := globalBalancePreviousIDs(side.source.previous.ID, side.block.BlockInfo)
		if err != nil || len(ids) != 1 || !ids[0].Equals(&base.previous.ID) {
			t.Fatalf("split parents: %v %v", ids, err)
		}
	}
	merged, block := globalTransitionNext(t, []*localBlockSource{left, right}, math.MinInt64, queue.OutQueue, nil, nil, nil)
	if err := verifyGlobalBalanceTransition(context.Background(), merged, []*localBlockSource{left, right}, block, 17); err != nil {
		t.Fatal(err)
	}
	ids, err := globalBalancePreviousIDs(merged.previous.ID, block.BlockInfo)
	if err != nil || len(ids) != 2 || !ids[0].Equals(&left.previous.ID) || !ids[1].Equals(&right.previous.ID) {
		t.Fatalf("merge parents: %v %v", ids, err)
	}
	bad, _ := globalTransitionNext(t, []*localBlockSource{left, right}, math.MinInt64, globalTransitionQueue(t, 10, leftEnvelope), nil, nil, nil)
	if err := verifyGlobalBalanceTransition(context.Background(), bad, []*localBlockSource{left, right}, block, 17); err == nil {
		t.Fatal("merge lost a queued message")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := verifyGlobalBalanceTransition(ctx, left, []*localBlockSource{base}, leftBlock, 17); !errors.Is(err, context.Canceled) {
		t.Fatalf("split cancellation: %v", err)
	}
}

type globalTransitionSide struct {
	source *localBlockSource
	block  *tlb.Block
}

type globalTransitionStore struct {
	*localAcquisitionTestStore
	states  map[cell.Hash]*localBlockSource
	blocked ton.BlockIDExt
	ready   chan struct{}
	waiting chan struct{}
}

func (s *globalTransitionStore) BlockState(ctx context.Context, id ton.BlockIDExt) (*storage.BlockState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if id.Equals(&s.blocked) {
		select {
		case <-s.ready:
		default:
			return nil, storage.ErrNotFound
		}
	}
	source := s.states[cell.Hash(id.RootHash)]
	if source == nil || !source.previous.ID.Equals(&id) {
		return nil, storage.ErrNotFound
	}
	return &storage.BlockState{Block: id, Cell: source.previous.State, StateRootHash: source.previous.State.Hash()}, nil
}

func (s *globalTransitionStore) BlockRoot(ctx context.Context, id ton.BlockIDExt) (*cell.Cell, error) {
	if _, err := s.BlockState(ctx, id); err != nil {
		return nil, err
	}
	return s.states[cell.Hash(id.RootHash)].previous.Block, nil
}

func (s *globalTransitionStore) WaitBlockArtifacts(ctx context.Context, id ton.BlockIDExt) error {
	select {
	case s.waiting <- struct{}{}:
	default:
	}
	select {
	case <-s.ready:
		_, err := s.BlockState(ctx, id)
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func globalTransitionTraversal(t testing.TB, acquisition *LocalAcquisition, master PreviousBlock, base *localBlockSource, mode acquisitionReadMode) *globalBalanceTransitions {
	t.Helper()
	v := &globalBalanceTransitions{calculator: localGlobalBalanceCalculator{acquisition: acquisition, baseline: master, mode: mode},
		version: 17, anchors: make(map[cell.Hash]ton.BlockIDExt), floors: make(map[shardRegistryKey]uint32), verified: make(map[cell.Hash]struct{})}
	if err := v.addAnchor(master.ID); err != nil {
		t.Fatal(err)
	}
	if err := v.addAnchor(base.previous.ID); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestGlobalBalanceChecksIntermediateStatesAfterCacheEviction(t *testing.T) {
	master, base := globalTransitionBase(t)
	envelope := globalTransitionEnvelope(t, 1, 96, nil)
	middle, _ := globalTransitionNext(t, []*localBlockSource{base}, math.MinInt64, globalTransitionQueue(t, 50, envelope), nil, nil, nil)
	tip, _ := globalTransitionNext(t, []*localBlockSource{middle}, math.MinInt64, globalTransitionQueue(t, 50, envelope), nil, nil, nil)
	store := &globalTransitionStore{states: map[cell.Hash]*localBlockSource{
		cell.Hash(base.previous.ID.RootHash): base, cell.Hash(middle.previous.ID.RootHash): middle,
	}}
	acquisition := &LocalAcquisition{store: store}
	if _, err := acquisition.blocks.store(middle); err != nil {
		t.Fatal(err)
	}
	if _, err := middle.globalBalance(context.Background(), 17); err != nil {
		t.Fatal(err)
	}
	for _, evict := range []bool{false, true} {
		if evict {
			acquisition.blocks = localBlockCache{}
		}
		v := globalTransitionTraversal(t, acquisition, master, base, acquisitionReadImmediate)
		if err := v.verify(context.Background(), tip); err == nil || !strings.Contains(err.Error(), "OutMsgDescr delta") {
			t.Fatalf("cached %t malformed intermediate: %v", !evict, err)
		}
	}
	// A valid final transition cannot replace the exact authenticated boundary
	// with a different block at the same height.
	wrong := *base
	wrong.previous.ID = cloneBlockID(base.previous.ID)
	wrong.previous.ID.RootHash[0] ^= 1
	v := globalTransitionTraversal(t, acquisition, master, base, acquisitionReadImmediate)
	if err := v.verify(context.Background(), &wrong); err == nil {
		t.Fatal("wrong baseline fork accepted")
	}
}

func TestGlobalBalanceIntermediateStateWaitAndCancel(t *testing.T) {
	master, base := globalTransitionBase(t)
	middle, _ := globalTransitionNext(t, []*localBlockSource{base}, math.MinInt64, globalTransitionQueue(t, 0), nil, nil, nil)
	tip, _ := globalTransitionNext(t, []*localBlockSource{middle}, math.MinInt64, globalTransitionQueue(t, 0), nil, nil, nil)
	for _, cancelWait := range []bool{false, true} {
		store := &globalTransitionStore{states: map[cell.Hash]*localBlockSource{
			cell.Hash(base.previous.ID.RootHash): base, cell.Hash(middle.previous.ID.RootHash): middle,
		}, blocked: middle.previous.ID, ready: make(chan struct{}), waiting: make(chan struct{}, 1)}
		acquisition := &LocalAcquisition{store: store}
		v := globalTransitionTraversal(t, acquisition, master, base, acquisitionReadImmediate)
		if err := v.verify(context.Background(), tip); !errors.Is(err, ErrAcquisitionNotReady) {
			t.Fatalf("missing intermediate: %v", err)
		}
		wait := acquisitionInputWait{}
		v = globalTransitionTraversal(t, acquisition, master, base, acquisitionReadMode{waits: &wait})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- v.verify(ctx, tip) }()
		select {
		case <-store.waiting:
		case err := <-done:
			t.Fatalf("did not wait: %v", err)
		case <-time.After(time.Second):
			t.Fatal("intermediate wait not reached")
		}
		if cancelWait {
			cancel()
		} else {
			close(store.ready)
		}
		select {
		case err := <-done:
			if cancelWait && !errors.Is(err, context.Canceled) || !cancelWait && err != nil {
				t.Fatalf("intermediate wait result: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("intermediate wait did not finish")
		}
		cancel()
	}
}

func TestGlobalBalanceLegacyDispatchIgnoresPartialStoredTotals(t *testing.T) {
	known, legacy := [32]byte{1}, [32]byte{2}
	queue := makeDispatchQueue(t,
		dispatchFixtureAccount{accountID: known, lts: []uint64{10}},
		dispatchFixtureAccount{accountID: legacy, lts: []uint64{20}},
	)
	setDispatchBalance(t, queue, known, 999)
	var extra tlb.DispatchQueueAugData
	if err := parseExact(&extra, queue.GetRootExtra()); err != nil {
		t.Fatal(err)
	}
	if extra.TotalBalance != nil {
		t.Fatal("mixed legacy fixture must have no stored total")
	}
	got, err := globalDispatchBalance(context.Background(), queue, 17)
	if err != nil || got.Int64() != 2_002_000 {
		t.Fatalf("actual legacy dispatch balance: %v %v", got, err)
	}
}

func TestGlobalBalanceCalculationRequiresImportedTransitions(t *testing.T) {
	master, base := globalTransitionBase(t)
	envelope := globalTransitionEnvelope(t, 1, 96, nil)
	for _, valid := range []bool{false, true} {
		var descriptor *cell.Cell
		if valid {
			descriptor = globalTransitionDescriptor(t, semanticOutNew, envelope, nil)
		}
		top, _ := globalTransitionNext(t, []*localBlockSource{base}, math.MinInt64, globalTransitionQueue(t, 50, envelope), envelope, descriptor, nil)
		store := &globalTransitionStore{states: map[cell.Hash]*localBlockSource{
			cell.Hash(base.previous.ID.RootHash): base, cell.Hash(top.previous.ID.RootHash): top,
		}}
		state, err := verifyPredecessor("master test", &master)
		if err != nil {
			t.Fatal(err)
		}
		var extra tlb.McStateExtra
		if err = parseExact(&extra, state.McStateExtra); err != nil {
			t.Fatal(err)
		}
		extra.ShardHashes = masterBuildShardHashes(t, masterBuildShardDescriptor(t, top.previous.ID, 0, top.state.GenUTime))
		state.McStateExtra, err = tlb.ToCell(&extra)
		if err != nil {
			t.Fatal(err)
		}
		var queue tlb.OutMsgQueueInfo
		if err = parseExact(&queue, state.OutMsgQueueInfo); err != nil {
			t.Fatal(err)
		}
		mcBase := &localBlockSource{previous: master, state: state}
		next, _ := globalTransitionNext(t, []*localBlockSource{mcBase}, math.MinInt64, queue.OutQueue, nil, nil, nil)
		calculator := localGlobalBalanceCalculator{acquisition: &LocalAcquisition{store: store}, baseline: master}
		if _, err = calculator.CalculateGlobalBalance(context.Background(), master, 17); err != nil {
			t.Fatalf("initial snapshot: %v", err)
		}
		_, err = calculator.CalculateGlobalBalance(context.Background(), next.previous, 17)
		if (err == nil) != valid {
			t.Fatalf("valid imported transition %t: %v", valid, err)
		}
		if !valid && !strings.Contains(err.Error(), "OutMsgDescr delta") {
			t.Fatal(err)
		}
	}
}

func BenchmarkGlobalBalanceQueueTransition(b *testing.B) {
	_, base := globalTransitionBase(b)
	envelope := globalTransitionEnvelope(b, 1, 96, nil)
	descriptor := globalTransitionDescriptor(b, semanticOutNew, envelope, nil)
	next, block := globalTransitionNext(b, []*localBlockSource{base}, math.MinInt64, globalTransitionQueue(b, 50, envelope), envelope, descriptor, nil)
	previous := []*localBlockSource{base}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := verifyGlobalBalanceTransition(context.Background(), next, previous, block, 17); err != nil {
			b.Fatal(err)
		}
	}
}

type globalBalanceBadHeaderCase struct {
	name   string
	change func(*tlb.BlockHeader, *ton.BlockIDExt)
}

func TestGlobalBalancePredecessorHeaderBoundaries(t *testing.T) {
	_, base := globalTransitionBase(t)
	next, block := globalTransitionNext(t, []*localBlockSource{base}, math.MinInt64, globalTransitionQueue(t, 0), nil, nil, nil)
	for _, test := range []globalBalanceBadHeaderCase{
		{"split zerostate", func(h *tlb.BlockHeader, id *ton.BlockIDExt) {
			h.AfterSplit = true
			id.Shard = int64(tlb.ShardID(uint64(1) << 63).GetChild(true))
		}},
		{"merge zerostates", func(h *tlb.BlockHeader, _ *ton.BlockIDExt) {
			h.AfterMerge = true
			second := h.PrevRef.Prev1
			h.PrevRef.Prev2 = &second
		}},
		{"unknown version", func(h *tlb.BlockHeader, _ *ton.BlockIDExt) { h.Version = 1 }},
		{"masterchain identity", func(h *tlb.BlockHeader, _ *ton.BlockIDExt) { h.NotMaster = false }},
		{"parent at same height", func(h *tlb.BlockHeader, id *ton.BlockIDExt) { h.PrevRef.Prev1.SeqNo = id.SeqNo }},
		{"sequence gap", func(_ *tlb.BlockHeader, id *ton.BlockIDExt) { id.SeqNo += 2 }},
		{"short parent hash", func(h *tlb.BlockHeader, _ *ton.BlockIDExt) { h.PrevRef.Prev1.RootHash = nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			header, id := block.BlockInfo, next.previous.ID
			test.change(&header, &id)
			if _, err := globalBalancePreviousIDs(id, header); err == nil {
				t.Fatal("invalid header accepted")
			}
		})
	}
	// Merge children can have different heights; the parent advances the
	// maximum, not both child sequence numbers independently.
	header, id := block.BlockInfo, next.previous.ID
	header.AfterMerge, header.PrevRef.Prev1.SeqNo, id.SeqNo = true, 10, 21
	second := header.PrevRef.Prev1
	second.SeqNo = 20
	header.PrevRef.Prev2 = &second
	parents, err := globalBalancePreviousIDs(id, header)
	if err != nil || len(parents) != 2 || parents[0].SeqNo != 10 || parents[1].SeqNo != 20 {
		t.Fatalf("unequal merge parents: %v %v", parents, err)
	}
}

func TestGlobalBalancePrefixFloorsAndNewWorkchain(t *testing.T) {
	master, base := globalTransitionBase(t)
	v := globalTransitionTraversal(t, &LocalAcquisition{}, master, base, acquisitionReadImmediate)
	// No registered shard of workchain 1: the caller has authenticated its
	// configured zerostate, and there is no preceding transition to replay.
	newWorkchain := *base
	newWorkchain.previous.ID = cloneBlockID(base.previous.ID)
	newWorkchain.previous.ID.Workchain = 1
	newWorkchain.previous.ID.RootHash[0] ^= 1
	if err := v.verify(context.Background(), &newWorkchain); err != nil {
		t.Fatalf("new workchain zerostate: %v", err)
	}

	root := tlb.ShardID(uint64(1) << 63)
	left, right := cloneBlockID(base.previous.ID), cloneBlockID(base.previous.ID)
	left.Shard, left.SeqNo, left.RootHash[0] = int64(root.GetChild(true)), 10, 21
	right.Shard, right.SeqNo, right.RootHash[0] = int64(root.GetChild(false)), 20, 22
	v.floors = make(map[shardRegistryKey]uint32)
	v.anchors = make(map[cell.Hash]ton.BlockIDExt)
	if err := v.addAnchor(left); err != nil {
		t.Fatal(err)
	}
	if err := v.addAnchor(right); err != nil {
		t.Fatal(err)
	}
	child := *base
	child.previous.ID = cloneBlockID(left)
	child.previous.ID.Shard = int64(root.GetChild(true).GetChild(false))
	child.previous.ID.SeqNo = 11
	child.previous.ID.RootHash[0] = 23
	if err := v.verify(context.Background(), &child); !errors.Is(err, ErrAcquisitionNotReady) {
		t.Fatalf("left child incorrectly bounded by right height: %v", err)
	}
	child.previous.ID.Shard = math.MinInt64
	child.previous.ID.SeqNo = 20
	if err := v.verify(context.Background(), &child); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("merge must advance maximum child height: %v", err)
	}
}
