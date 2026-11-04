package collator

import (
	"bytes"
	"context"
	"errors"
	"math"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"

	"github.com/xssnick/gton/service/validator/msgpool"
)

type globalBalanceFeeCase struct {
	version  uint32
	dispatch bool
	want     int64
}

func TestGlobalBalanceMessageFeesAndHeaderOnlyRead(t *testing.T) {
	// There is deliberately no body or StateInit after this header. The
	// calculator must not open or retain either payload, just like C++.
	header := globalBalanceMessageHeader{
		Source:      address.NewAddress(0, 0, bytes.Repeat([]byte{1}, 32)),
		Destination: address.NewAddress(0, 255, bytes.Repeat([]byte{2}, 32)),
		Value:       tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(100)},
		IHRFee:      tlb.FromNanoTONU(3), FwdFee: tlb.FromNanoTONU(7), CreatedLT: 50,
	}
	message, err := tlb.ToCell(header)
	if err != nil {
		t.Fatal(err)
	}
	emitted := uint64(60)
	envelope, err := (tlb.MsgEnvelope{
		CurAddr:  tlb.IntermediateAddress{UseDestBits: 0},
		NextAddr: tlb.IntermediateAddress{UseDestBits: 96},
		Msg:      message, FwdFeeRemaining: tlb.FromNanoTONU(5), EmittedLT: &emitted,
	}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	queued, err := (tlb.EnqueuedMsg{EnqueuedLT: 70, Msg: envelope}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []globalBalanceFeeCase{{11, false, 108}, {12, false, 105}, {17, false, 105}, {11, true, 110}, {12, true, 107}, {17, true, 107}} {
		got, key, err := parseGlobalBalanceMessage(queued.MustBeginParse(), test.version, test.dispatch)
		if err != nil {
			t.Fatal(err)
		}
		if got.grams.Int64() != test.want || got.descr.LT != emitted || got.descr.EnqueuedLT != 70 ||
			got.descr.CurWorkchain != 0 || got.descr.NextWorkchain != -1 || !bytes.Equal(key[12:], message.Hash()) {
			t.Fatalf("version %d dispatch %t: %+v, balance %s", test.version, test.dispatch, got.descr, &got.grams)
		}
	}
}

func TestGlobalBalanceProcessedUnionAndStrictEnqueuedBound(t *testing.T) {
	root := uint64(1) << 63
	source := &globalBalanceShard{owner: msgpool.ShardIdent{Workchain: 0, Shard: root}}
	destination := &globalBalanceShard{owner: msgpool.ShardIdent{Workchain: -1, Shard: root}, processed: []tlb.ProcessedUptoRecord{{
		ShardPrefix: root, MCSeqno: 7, LastMsgLT: 100, LastMsgHash: processedInfinityHash,
	}}}
	source.base.SetInt64(20)
	destination.base.SetInt64(30)
	source.messages = []globalBalanceMessage{{descr: tlb.ProcessedMsgDescr{
		CurWorkchain: 0, NextWorkchain: -1, LT: 50, EnqueuedLT: 80,
	}, grams: *big.NewInt(9)}}
	endLT := uint64(80)
	resolve := func(seq uint32, wc int32, prefix uint64) uint64 {
		if seq != 7 || wc != 0 {
			t.Fatalf("wrong history request %d:%d", seq, wc)
		}
		return endLT
	}
	for _, expected := range []int64{59, 50} {
		got, err := sumGlobalBalance(context.Background(), []*globalBalanceShard{source, destination}, resolve)
		if err != nil || got.Int64() != expected {
			t.Fatalf("got %v (%v), want %d", got, err, expected)
		}
		endLT++
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := sumGlobalBalance(ctx, []*globalBalanceShard{source, destination}, resolve); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled calculation: %v", err)
	}
}

type globalBalanceCreationCase struct {
	name     string
	previous []ton.BlockIDExt
	next     []ton.BlockIDExt
	want     int64
	invalid  bool
}

func globalBalanceRegistry(blocks ...ton.BlockIDExt) *ShardRegistry {
	r := &ShardRegistry{leaves: make(map[shardRegistryKey]shardRegistryLeaf, len(blocks))}
	for _, block := range blocks {
		r.leaves[shardRegistryKey{block.Workchain, block.Shard}] = shardRegistryLeaf{top: ShardTop{Block: block}}
	}
	return r
}

func TestGlobalBalanceCreatedAcrossTopology(t *testing.T) {
	root := tlb.ShardID(uint64(1) << 63)
	id := func(shard tlb.ShardID, seq uint32) ton.BlockIDExt {
		return ton.BlockIDExt{Workchain: 0, Shard: int64(shard), SeqNo: seq}
	}
	config := &Config{masterchain: chainConfig{createFee: tlb.FromNanoTONU(7)}, basechain: chainConfig{createFee: tlb.FromNanoTONU(101)}}
	for _, test := range []globalBalanceCreationCase{
		{name: "unchanged", previous: []ton.BlockIDExt{id(root, 10)}, next: []ton.BlockIDExt{id(root, 10)}, want: 7},
		{name: "linear", previous: []ton.BlockIDExt{id(root, 10)}, next: []ton.BlockIDExt{id(root, 12)}, want: 209},
		{name: "split floors per child", previous: []ton.BlockIDExt{id(root, 10)}, next: []ton.BlockIDExt{id(root.GetChild(true), 11), id(root.GetChild(false), 12)}, want: 157},
		{name: "merge max child seqno", previous: []ton.BlockIDExt{id(root.GetChild(true), 10), id(root.GetChild(false), 12)}, next: []ton.BlockIDExt{id(root, 13)}, want: 108},
		{name: "new workchain", next: []ton.BlockIDExt{id(root, 3)}, want: 310},
		{name: "regressed", previous: []ton.BlockIDExt{id(root, 10)}, next: []ton.BlockIDExt{id(root, 9)}, invalid: true},
		{name: "missing sibling", previous: []ton.BlockIDExt{id(root.GetChild(true), 10)}, next: []ton.BlockIDExt{id(root, 11)}, invalid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := globalBalanceCreated(config, globalBalanceRegistry(test.previous...), globalBalanceRegistry(test.next...))
			if test.invalid {
				if err == nil {
					t.Fatal("invalid topology accepted")
				}
				return
			}
			if err != nil || got.Int64() != test.want {
				t.Fatalf("got %v (%v), want %d", got, err, test.want)
			}
		})
	}
}

func globalBalanceAcquisitionFixture(t testing.TB) (PreviousBlock, *localAcquisitionTestStore) {
	t.Helper()
	fixture := newMasterBuildFixture(t, false)
	shard := fixture.oldState
	shard.ShardIdent = tlb.ShardIdent{WorkchainID: 0}
	shard.McStateExtra = nil
	var err error
	shard.Accounts.ShardAccounts, err = tlb.NewShardAccountsAugDict()
	if err != nil {
		t.Fatal(err)
	}
	shard.Stats, err = (tlb.ShardStateStats{TotalValidatorFees: tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(999)}}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	shardRoot, err := tlb.ToCell(&shard)
	if err != nil {
		t.Fatal(err)
	}
	id := ton.BlockIDExt{Workchain: 0, Shard: math.MinInt64, RootHash: shardRoot.Hash(), FileHash: bytes.Repeat([]byte{1}, 32)}
	fixture.oldExtra.ShardHashes = masterBuildShardHashes(t, masterBuildShardDescriptor(t, id, 0, shard.GenUTime))
	fixture.oldState.McStateExtra, err = tlb.ToCell(&fixture.oldExtra)
	if err != nil {
		t.Fatal(err)
	}
	root, err := tlb.ToCell(&fixture.oldState)
	if err != nil {
		t.Fatal(err)
	}
	master := fixture.request.Previous
	master.State = root
	master.ID.RootHash = root.Hash()
	return master, &localAcquisitionTestStore{block: id, state: shardRoot}
}

func TestCalculateGlobalBalanceLoadsAuthenticatedShardAndExcludesShardFees(t *testing.T) {
	master, store := globalBalanceAcquisitionFixture(t)
	var masterState, shard tlb.ShardStateUnsplit
	if err := parseExact(&masterState, master.State); err != nil {
		t.Fatal(err)
	}
	if err := parseExact(&shard, store.state); err != nil {
		t.Fatal(err)
	}
	id := store.block
	acquisition := &LocalAcquisition{store: store}
	got, err := (localGlobalBalanceCalculator{acquisition: acquisition, baseline: master}).CalculateGlobalBalance(context.Background(), master, 17)
	if err != nil {
		t.Fatal(err)
	}
	want, err := loadAccountsBalance(masterState.Accounts.ShardAccounts)
	if err != nil {
		t.Fatal(err)
	}
	if got.Cmp(want.Coins.NanoRef()) != 0 {
		t.Fatalf("got %s want %s; shard validator fees must be excluded", got, want.Coins.NanoRef())
	}
	// A different state under the same declared block ID must never enter the cache.
	shard.Seqno++
	corrupted, err := tlb.ToCell(&shard)
	if err != nil {
		t.Fatal(err)
	}
	acquisition = &LocalAcquisition{store: &localAcquisitionTestStore{block: id, state: corrupted}}
	if _, err = (localGlobalBalanceCalculator{acquisition: acquisition, baseline: master}).CalculateGlobalBalance(context.Background(), master, 17); err == nil {
		t.Fatal("unauthenticated shard state accepted")
	}
}

func TestNextGlobalBalanceGramsBurning(t *testing.T) {
	got, err := nextGlobalBalanceGrams(big.NewInt(100), tlb.FromNanoTONU(7), tlb.FromNanoTONU(9), tlb.FromNanoTONU(20))
	if err != nil || got.NanoRef().Int64() != 96 {
		t.Fatalf("got %s (%v)", got.NanoRef(), err)
	}
	if _, err = nextGlobalBalanceGrams(big.NewInt(1), tlb.Coins{}, tlb.Coins{}, tlb.FromNanoTONU(2)); err == nil {
		t.Fatal("negative balance accepted")
	}
}

func TestGlobalBalanceClampsFutureProcessedFrontier(t *testing.T) {
	master, store := globalBalanceAcquisitionFixture(t)
	var shard, masterState tlb.ShardStateUnsplit
	if err := parseExact(&shard, store.state); err != nil {
		t.Fatal(err)
	}
	if err := parseExact(&masterState, master.State); err != nil {
		t.Fatal(err)
	}
	var queue tlb.OutMsgQueueInfo
	if err := parseExact(&queue, shard.OutMsgQueueInfo); err != nil {
		t.Fatal(err)
	}
	var err error
	queue.ProcInfo, err = tlb.ProcessedUptoDict([]tlb.ProcessedUptoRecord{{ShardPrefix: 1 << 63, MCSeqno: 99, LastMsgLT: 100}})
	if err != nil {
		t.Fatal(err)
	}
	shard.OutMsgQueueInfo, err = queue.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	store.state, err = tlb.ToCell(&shard)
	if err != nil {
		t.Fatal(err)
	}
	store.block.RootHash = store.state.Hash()
	var extra tlb.McStateExtra
	if err = parseExact(&extra, masterState.McStateExtra); err != nil {
		t.Fatal(err)
	}
	extra.ShardHashes = masterBuildShardHashes(t, masterBuildShardDescriptor(t, store.block, 0, shard.GenUTime))
	masterState.McStateExtra, err = tlb.ToCell(&extra)
	if err != nil {
		t.Fatal(err)
	}
	master.State, err = tlb.ToCell(&masterState)
	if err != nil {
		t.Fatal(err)
	}
	master.ID.RootHash = master.State.Hash()
	// The store has only the shard zerostate. A lookup of record seqno 99
	// instead of min(99, current master seqno 0) necessarily fails.
	calculator := localGlobalBalanceCalculator{acquisition: &LocalAcquisition{store: store}, baseline: master}
	if _, err = calculator.CalculateGlobalBalance(context.Background(), master, 17); err != nil {
		t.Fatal(err)
	}
}

func TestGlobalBalanceLegacyAndStoredDispatchTotals(t *testing.T) {
	id := [32]byte{1}
	queue := makeDispatchQueue(t, dispatchFixtureAccount{accountID: id, lts: []uint64{10, 20}})
	for _, stored := range []bool{false, true} {
		if stored {
			setDispatchBalance(t, queue, id, 2_002_000)
		}
		got, err := globalDispatchBalance(context.Background(), queue, 17)
		if err != nil || got.Int64() != 2_002_000 {
			t.Fatalf("stored=%t: got %v (%v)", stored, got, err)
		}
	}
}

func TestGlobalBalanceCacheConcurrentAndCancelable(t *testing.T) {
	_, store := globalBalanceAcquisitionFixture(t)
	var state tlb.ShardStateUnsplit
	if err := parseExact(&state, store.state); err != nil {
		t.Fatal(err)
	}
	source := &localBlockSource{previous: PreviousBlock{ID: store.block, State: store.state}, state: state}
	var workers sync.WaitGroup
	results := make(chan *globalBalanceShard, 8)
	for range 8 {
		workers.Go(func() {
			value, err := source.globalBalance(context.Background(), 17)
			if err != nil {
				t.Error(err)
				return
			}
			results <- value
		})
	}
	workers.Wait()
	close(results)
	for value := range results {
		if value != source.balances[0] {
			t.Fatal("concurrent parse results were not shared")
		}
	}
	blocked := &localBlockSource{balanceLoads: [2]*globalBalanceLoad{{done: make(chan struct{})}, nil}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := blocked.globalBalance(ctx, 17); done <- err }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled waiter: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled waiter stayed behind another calculation")
	}
}

func BenchmarkGlobalBalanceSum(b *testing.B) {
	source := &globalBalanceShard{owner: msgpool.ShardIdent{Workchain: 0, Shard: 1 << 63}}
	destination := &globalBalanceShard{owner: msgpool.ShardIdent{Workchain: -1, Shard: 1 << 63}, processed: []tlb.ProcessedUptoRecord{{
		ShardPrefix: 1 << 63, LastMsgLT: 9000, LastMsgHash: processedInfinityHash,
	}}}
	for i := range 18000 {
		source.messages = append(source.messages, globalBalanceMessage{
			descr: tlb.ProcessedMsgDescr{CurWorkchain: 0, NextWorkchain: -1, LT: uint64(i + 1), EnqueuedLT: uint64(i + 1)},
			grams: *big.NewInt(100),
		})
	}
	states := []*globalBalanceShard{source, destination}
	resolve := func(uint32, int32, uint64) uint64 { return 10000 }
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		value, err := sumGlobalBalance(context.Background(), states, resolve)
		if err != nil || value.Int64() != 900000 {
			b.Fatalf("balance %v: %v", value, err)
		}
	}
}
